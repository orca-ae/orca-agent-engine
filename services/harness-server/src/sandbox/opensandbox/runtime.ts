// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type {
  EnvironmentSpec,
  SandboxCapabilities,
  SandboxFileMode,
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SandboxRuntime,
  ToolCall,
  ToolResult,
  SandboxWritePolicy,
} from '../sandbox-runtime.js';
import {
  buildSandboxChmodManyCommand,
  SANDBOX_CHMOD_MANY_TIMEOUT_MS,
  serializeSandboxFileModes,
} from '../chmod-many.js';
import type { ReadPage, ReadPageInput } from '../read-page.js';
import {
  buildSandboxReadPageCommand,
  buildSandboxReadPrerequisiteProbeCommand,
  parseSandboxReadPageResult,
  SANDBOX_READ_COMMAND_ENVS,
  SANDBOX_READ_TIMEOUT_MS,
} from '../read-page.js';
import {
  buildBubblewrapCommand,
  buildSandboxFilesystemRootPreflightCommand,
  buildSkillsAliasProbeCommand,
  SANDBOX_FILESYSTEM_ROOT_PREFLIGHT_TIMEOUT_MS,
} from '../write-policy.js';

const DEFAULT_EXECD_PORT = 44772;
const DEFAULT_RESOURCE_LIMITS: Record<string, string> = {
  cpu: '1',
  memory: '2Gi',
};
const DEFAULT_ENTRYPOINT = ['tail', '-f', '/dev/null'];
const READY_TIMEOUT_MS = 60_000;
const READY_POLL_MS = 500;
const EXECD_ISOLATION_EXTENSION = 'bootstrap.execd.isolation';
const MAX_COMMAND_STREAM_BYTES = 1024 * 1024;
const FUSE_DEVICE_EXTENSION = 'orca.fuse.device';

export interface OpenSandboxRuntimeOptions {
  /** OpenSandbox server host[:port] or full URL. */
  domain: string;
  /** Protocol used when `domain` has no scheme. */
  protocol: 'http' | 'https';
  apiKey?: string;
  image: string;
  entrypoint?: string[];
  timeoutSeconds: number;
  useServerProxy: boolean;
  requestTimeoutSeconds: number;
  resourceLimits?: Record<string, string>;
}

interface SandboxEndpoint {
  endpoint: string;
  headers?: Record<string, string>;
}

interface CommandExecution {
  stdout: string;
  stderr: string;
  exitCode: number;
}

interface ServerStreamEvent extends Record<string, unknown> {
  type?: string;
  text?: string;
  error?: Record<string, unknown>;
}

/**
 * Remote sandbox runtime backed by an OpenSandbox server.
 *
 * This adapter deliberately speaks the OpenSandbox REST protocol directly
 * instead of importing the JS SDK. It keeps local debugging lightweight while
 * still matching the SDK's lifecycle/execd API shape:
 *   - lifecycle API under /v1 on opensandbox-server
 *   - execd command/files API through the resolved sandbox endpoint
 */
export class OpenSandboxRuntime implements SandboxRuntime {
  readonly capabilities: SandboxCapabilities;

  private readonly lifecycleBaseUrl: string;
  private readonly headers: Record<string, string>;

  constructor(private readonly opts: OpenSandboxRuntimeOptions) {
    this.lifecycleBaseUrl = `${normalizeServerBaseUrl(opts.domain, opts.protocol)}/v1`;
    this.headers = opts.apiKey ? { 'OPEN-SANDBOX-API-KEY': opts.apiKey } : {};
    this.capabilities = {
      supportsFuse: true,
      supportsWritePolicy: true,
    };
  }

  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    if (hasRequestedPackages(env)) {
      throw new Error(
        'OpenSandbox does not allow runtime package installation before the agent isolation boundary; use an operator-approved prebuilt image',
      );
    }
    const body = buildOpenSandboxAcquireBody(env, {
      image: this.opts.image,
      entrypoint: this.opts.entrypoint,
      timeoutSeconds: this.opts.timeoutSeconds,
      resourceLimits: this.opts.resourceLimits,
    });

    const created = await this.requestJson<{ id?: string }>('/sandboxes', {
      method: 'POST',
      body,
    });
    if (!created.id) {
      throw new Error('OpenSandbox create sandbox failed: response did not include id');
    }

    const handle = new OpenSandboxHandle({
      id: created.id,
      runtime: this,
      ...(env.fileUploadOwnership ? { fileUploadOwnership: env.fileUploadOwnership } : {}),
    });
    try {
      await handle.waitUntilReady();
      await handle.verifyFusePrerequisites();
      return handle;
    } catch (e) {
      await handle.destroy().catch(() => undefined);
      throw e;
    }
  }

  async lifecycleRequest(
    path: string,
    init: RequestInit & { body?: unknown } = {},
  ): Promise<Response> {
    const body = init.body === undefined ? undefined : JSON.stringify(init.body);
    const headers: Record<string, string> = { ...this.headers };
    if (body !== undefined) headers['content-type'] = 'application/json';
    const response = await fetchWithTimeout(
      `${this.lifecycleBaseUrl}${path}`,
      {
        ...init,
        headers: { ...headers, ...(init.headers as Record<string, string> | undefined) },
        body,
      },
      this.opts.requestTimeoutSeconds,
    );
    return response;
  }

  async requestJson<T>(path: string, init: RequestInit & { body?: unknown } = {}): Promise<T> {
    const response = await this.lifecycleRequest(path, init);
    if (!response.ok) {
      throw new Error(
        await formatHttpError(response, `OpenSandbox ${init.method ?? 'GET'} ${path} failed`),
      );
    }
    return (await response.json()) as T;
  }

  async requestOk(path: string, init: RequestInit & { body?: unknown } = {}): Promise<void> {
    const response = await this.lifecycleRequest(path, init);
    if (!response.ok) {
      throw new Error(
        await formatHttpError(response, `OpenSandbox ${init.method ?? 'GET'} ${path} failed`),
      );
    }
  }

  async getExecdEndpoint(sandboxId: string): Promise<SandboxEndpoint> {
    const query = this.opts.useServerProxy ? '?use_server_proxy=true' : '';
    return await this.requestJson<SandboxEndpoint>(
      `/sandboxes/${encodeURIComponent(sandboxId)}/endpoints/${DEFAULT_EXECD_PORT}${query}`,
    );
  }

  async getSandboxEndpoint(sandboxId: string, port: number): Promise<SandboxEndpoint> {
    const query = this.opts.useServerProxy ? '?use_server_proxy=true' : '';
    return await this.requestJson<SandboxEndpoint>(
      `/sandboxes/${encodeURIComponent(sandboxId)}/endpoints/${port}${query}`,
    );
  }

  buildExecdBaseUrl(endpoint: SandboxEndpoint): {
    baseUrl: string;
    headers: Record<string, string>;
  } {
    return {
      baseUrl: buildOpenSandboxEndpointUrl(endpoint.endpoint, this.opts.protocol),
      headers: { ...this.headers, ...(endpoint.headers ?? {}) },
    };
  }

  buildSandboxEndpoint(endpoint: SandboxEndpoint): {
    url: string;
    headers: Record<string, string>;
  } {
    return {
      url: buildOpenSandboxEndpointUrl(endpoint.endpoint, this.opts.protocol),
      headers: { ...this.headers, ...(endpoint.headers ?? {}) },
    };
  }

  requestTimeoutSeconds(): number {
    return this.opts.requestTimeoutSeconds;
  }
}

function hasRequestedPackages(env: EnvironmentSpec): boolean {
  if (!env.packages) return false;
  return Object.values(env.packages).some((packages) =>
    (packages ?? []).some((pkg) => pkg.trim().length > 0),
  );
}

/**
 * Build the POST /sandboxes request body for OpenSandbox `acquire`. Exported as
 * a pure function so it can be unit-tested without provisioning a real sandbox
 * (the production `acquire` calls this so test + prod share one implementation).
 *
 * Merges `env.harnessEnv` (gateway LLM vars) into
 * the container env map, honors a per-acquire `env.image` / `env.exposePorts`
 * override (falling back to the constructor image), and treats undefined
 * `harnessEnv` as `{}`. A per-acquire entrypoint travels with an in-sandbox
 * harness image instead of inheriting the constructor image's entrypoint.
 */
export function buildOpenSandboxAcquireBody(
  env: EnvironmentSpec,
  opts: {
    image: string;
    entrypoint?: string[];
    timeoutSeconds: number;
    resourceLimits?: Record<string, string>;
  },
): Record<string, unknown> {
  // Merge harnessEnv (gateway LLM vars, etc.) into the container env map.
  // Treat undefined as {} per the backward-compat contract.
  const containerEnv: Record<string, string> = { ...(env.harnessEnv ?? {}) };
  // Per-acquire image override takes precedence over the constructor image.
  const image = env.image ?? opts.image;
  const body: Record<string, unknown> = {
    image: { uri: image },
    entrypoint: env.entrypoint ?? opts.entrypoint ?? DEFAULT_ENTRYPOINT,
    resourceLimits: opts.resourceLimits ?? DEFAULT_RESOURCE_LIMITS,
    secureAccess: false,
    timeout: opts.timeoutSeconds,
    metadata: { owner: 'orca-managed-agents' },
    env: containerEnv,
    // OpenSandbox grants this operator-allowlisted workload only SETFCAP and
    // SYS_ADMIN for Bubblewrap and gVisor's in-sandbox FUSE. /dev/fuse comes
    // from gVisor; no host device or privileged Pod is used.
    extensions: {
      [EXECD_ISOLATION_EXTENSION]: 'enable',
      [FUSE_DEVICE_EXTENSION]: 'enable',
    },
  };
  if (env.exposePorts?.length) {
    body['exposePorts'] = env.exposePorts;
  }
  return body;
}

class OpenSandboxHandle implements SandboxHandle {
  readonly files: SandboxFiles;
  private destroyed = false;
  private execdBaseUrl = '';
  private execdHeaders: Record<string, string> = {};

  constructor(
    private readonly opts: {
      id: string;
      runtime: OpenSandboxRuntime;
      fileUploadOwnership?: { owner: string; group: string };
    },
  ) {
    this.files = new OpenSandboxFiles(this, opts.fileUploadOwnership);
  }

  get id(): string {
    return this.opts.id;
  }

  async waitUntilReady(): Promise<void> {
    const deadline = Date.now() + READY_TIMEOUT_MS;
    let lastError: unknown;
    while (Date.now() < deadline) {
      try {
        await this.refreshExecdEndpoint();
        const response = await this.execdFetch('/ping', { method: 'GET' });
        if (response.ok) return;
        lastError = new Error(`execd ping returned ${response.status}`);
      } catch (e) {
        lastError = e;
      }
      await sleep(READY_POLL_MS);
    }
    throw new Error(
      `OpenSandbox sandbox ${this.id} did not become ready: ${errorMessage(lastError)}`,
    );
  }

  async verifyFusePrerequisites(): Promise<void> {
    const result = await this.runCommand(
      [
        'set -euo pipefail',
        'test "$(id -u)" -eq 0',
        'command -v s3fs >/dev/null',
        'command -v fusermount3 >/dev/null',
        'command -v mount >/dev/null',
        'command -v umount >/dev/null',
        'test -c /dev/fuse',
        'exec 9<>/dev/fuse',
        'probe_dir="$(mktemp -d)"',
        'cleanup() { umount "$probe_dir" >/dev/null 2>&1 || true; rmdir "$probe_dir" >/dev/null 2>&1 || true; }',
        'trap cleanup EXIT',
        'mount -t tmpfs -o size=4096 tmpfs "$probe_dir"',
        'umount "$probe_dir"',
        'rmdir "$probe_dir"',
        'trap - EXIT',
      ].join('\n'),
    );
    if (result.exitCode !== 0) {
      throw new Error(
        `OpenSandbox gVisor FUSE prerequisite probe failed: requires root execd, s3fs, fusermount3, gVisor-provided /dev/fuse, and permission to mount/unmount filesystems (exit=${result.exitCode}). stderr=${summarizeCommandOutput(result.stderr)}; stdout=${summarizeCommandOutput(result.stdout)}`,
      );
    }
  }

  async run(call: ToolCall): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);

    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      const execution = await this.runCommand(args.command, {
        timeoutMs: args.timeout_ms,
      });
      return {
        stdout: execution.stdout,
        stderr: execution.stderr,
        exit_code: execution.exitCode,
      };
    }

    if (call.tool === 'glob') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ?? '.';
      const cmd = `cd '${escapeSingleQuotes(cwd)}' && compgen -G '${escapeSingleQuotes(args.pattern)}' || true`;
      const execution = await this.runCommand(cmd);
      const matches = execution.stdout.split('\n').filter((s) => s.length > 0);
      return { output: matches };
    }

    if (call.tool === 'grep') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ?? '.';
      const cmd = `cd '${escapeSingleQuotes(cwd)}' && grep -rn '${escapeSingleQuotes(args.pattern)}' . || true`;
      const execution = await this.runCommand(cmd);
      return { output: execution.stdout };
    }

    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
  }

  async prepareFilesystemRoots(paths: readonly string[]): Promise<void> {
    const result = await this.runCommand(buildSandboxFilesystemRootPreflightCommand(paths), {
      envs: { ...SANDBOX_READ_COMMAND_ENVS },
      timeoutMs: SANDBOX_FILESYSTEM_ROOT_PREFLIGHT_TIMEOUT_MS,
      requestTimeoutSeconds: Math.ceil(SANDBOX_FILESYSTEM_ROOT_PREFLIGHT_TIMEOUT_MS / 1_000) + 5,
    });
    if (result.exitCode !== 0) {
      throw new Error(
        `OpenSandbox filesystem-root preflight failed: ${summarizeCommandOutput(result.stderr)}`,
      );
    }
  }

  async prepareWritePolicy(policy: SandboxWritePolicy): Promise<void> {
    const readProbe = await this.runCommand(buildSandboxReadPrerequisiteProbeCommand(), {
      envs: { ...SANDBOX_READ_COMMAND_ENVS },
      timeoutMs: SANDBOX_READ_TIMEOUT_MS,
      requestTimeoutSeconds: Math.ceil(SANDBOX_READ_TIMEOUT_MS / 1_000) + 5,
    });
    if (readProbe.exitCode !== 0) {
      throw new Error(
        `OpenSandbox ranged-read prerequisite probe failed: ${summarizeCommandOutput(
          readProbe.stderr,
        )}`,
      );
    }
    const aliasProbe = await this.runCommand(buildSkillsAliasProbeCommand(policy));
    if (aliasProbe.exitCode !== 0) {
      throw new Error(`OpenSandbox filesystem-alias probe failed: ${aliasProbe.stderr}`);
    }
    const result = await this.runCommand(buildOpenSandboxToolCommand('true', policy));
    if (result.exitCode !== 0) {
      throw new Error(`OpenSandbox write-policy sandbox probe failed: ${result.stderr}`);
    }
  }

  async runWithWritePolicy(call: ToolCall, policy: SandboxWritePolicy): Promise<ToolResult> {
    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      const result = await this.runCommand(buildOpenSandboxToolCommand(args.command, policy), {
        timeoutMs: args.timeout_ms,
      });
      return { stdout: result.stdout, stderr: result.stderr, exit_code: result.exitCode };
    }
    if (call.tool === 'glob' || call.tool === 'grep') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ?? '.';
      const body =
        call.tool === 'glob'
          ? `cd '${escapeSingleQuotes(cwd)}' && compgen -G '${escapeSingleQuotes(args.pattern)}' || true`
          : `cd '${escapeSingleQuotes(cwd)}' && grep -rn '${escapeSingleQuotes(args.pattern)}' . || true`;
      const result = await this.runCommand(buildOpenSandboxToolCommand(body, policy));
      return call.tool === 'glob'
        ? { output: result.stdout.split('\n').filter((s) => s.length > 0) }
        : { output: result.stdout };
    }
    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
  }

  async canonicalizePathForPolicy(path: string): Promise<string> {
    const result = await this.runCommand(`realpath -m -- '${escapeSingleQuotes(path)}'`);
    if (result.exitCode !== 0) throw new Error(`realpath failed: ${result.stderr}`);
    return result.stdout.trim();
  }

  async runCommand(
    command: string,
    opts: {
      timeoutMs?: number;
      envs?: Record<string, string>;
      requestTimeoutSeconds?: number;
    } = {},
  ): Promise<CommandExecution> {
    const commandRequestTimeoutSeconds =
      opts.timeoutMs === undefined ? 0 : Math.ceil(opts.timeoutMs / 1_000) + 5;
    const requestTimeoutSeconds =
      opts.requestTimeoutSeconds ??
      Math.max(this.opts.runtime.requestTimeoutSeconds(), commandRequestTimeoutSeconds);
    const body: Record<string, unknown> = {
      command,
      background: false,
    };
    if (opts.timeoutMs !== undefined) body['timeout'] = Math.round(opts.timeoutMs);
    if (opts.envs !== undefined) body['envs'] = opts.envs;

    const response = await this.execdFetch(
      '/command',
      {
        method: 'POST',
        headers: {
          accept: 'text/event-stream',
          'content-type': 'application/json',
        },
        body: JSON.stringify(body),
      },
      requestTimeoutSeconds,
    );
    if (!response.ok) {
      throw new Error(await formatHttpError(response, 'OpenSandbox command failed'));
    }
    return await consumeCommandStream(response, requestTimeoutSeconds * 1_000);
  }

  async execdFetch(
    path: string,
    init: RequestInit,
    timeoutSeconds = this.opts.runtime.requestTimeoutSeconds(),
  ): Promise<Response> {
    if (!this.execdBaseUrl) await this.refreshExecdEndpoint();
    const headers = {
      ...this.execdHeaders,
      ...(init.headers as Record<string, string> | undefined),
    };
    return await fetchWithTimeout(
      `${this.execdBaseUrl}${path}`,
      { ...init, headers },
      timeoutSeconds,
    );
  }

  async runPrivileged(cmd: string, opts?: { envs?: Record<string, string> }): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const result = await this.runCommand(cmd.replace(/^\s*sudo\s+/, ''), { envs: opts?.envs });
    return {
      stdout: result.stdout,
      stderr: result.stderr,
      exit_code: result.exitCode,
    };
  }

  async pause(): Promise<void> {
    if (this.destroyed) return;
    throw new Error(
      'OpenSandbox pause is disabled for gVisor in-sandbox FUSE because snapshot resume cannot restore live mounts',
    );
  }

  async resume(): Promise<void> {
    if (this.destroyed) return;
    throw new Error(
      'OpenSandbox resume is disabled for gVisor in-sandbox FUSE because snapshot resume cannot restore live mounts',
    );
  }

  async destroy(): Promise<void> {
    if (this.destroyed) return;
    this.destroyed = true;
    try {
      await this.opts.runtime.requestOk(`/sandboxes/${encodeURIComponent(this.id)}`, {
        method: 'DELETE',
      });
    } catch (e) {
      // Idempotent destroy: ignore missing/deleted sandboxes, surface other errors.
      if (!String((e as Error).message).includes('status=404')) throw e;
    }
  }

  async endpoint(port: number): Promise<{ url: string; headers?: Record<string, string> }> {
    const endpoint = await this.opts.runtime.getSandboxEndpoint(this.id, port);
    return this.opts.runtime.buildSandboxEndpoint(endpoint);
  }

  private async refreshExecdEndpoint(): Promise<void> {
    const endpoint = await this.opts.runtime.getExecdEndpoint(this.id);
    const resolved = this.opts.runtime.buildExecdBaseUrl(endpoint);
    this.execdBaseUrl = resolved.baseUrl;
    this.execdHeaders = resolved.headers;
  }
}

class OpenSandboxFiles implements SandboxFiles {
  constructor(
    private readonly handle: OpenSandboxHandle,
    private readonly uploadOwnership?: { owner: string; group: string },
  ) {}

  async write(path: string, content: Buffer | NodeJS.ReadableStream): Promise<void> {
    const bytes = Buffer.isBuffer(content) ? content : await readStream(content);
    // Execd applies this metadata to the file and to parent directories it
    // creates. That preserves the in-sandbox runtime user's ownership at the
    // source instead of repairing a potentially large tree during startup.
    const metadata = JSON.stringify({ path, ...this.uploadOwnership });
    const form = new FormData();
    form.append('metadata', new Blob([metadata], { type: 'application/json' }), 'metadata');
    form.append('file', new Blob([bytes], { type: 'application/octet-stream' }), basename(path));

    const response = await this.handle.execdFetch('/files/upload', {
      method: 'POST',
      body: form,
    });
    if (!response.ok) {
      throw new Error(await formatHttpError(response, `OpenSandbox write ${path} failed`));
    }
  }

  async read(path: string): Promise<Buffer> {
    const response = await this.handle.execdFetch(
      `/files/download?path=${encodeURIComponent(path)}`,
      { method: 'GET' },
    );
    if (!response.ok) {
      throw new Error(await formatHttpError(response, `OpenSandbox read ${path} failed`));
    }
    return Buffer.from(await response.arrayBuffer());
  }

  async readUtf8Page(
    path: string,
    input: ReadPageInput,
    constraint?: SandboxReadConstraint,
  ): Promise<ReadPage> {
    const result = await this.handle.runCommand(
      buildSandboxReadPageCommand(path, constraint, input),
      {
        envs: { ...SANDBOX_READ_COMMAND_ENVS },
        timeoutMs: SANDBOX_READ_TIMEOUT_MS,
        requestTimeoutSeconds: Math.ceil(SANDBOX_READ_TIMEOUT_MS / 1_000) + 5,
      },
    );
    if (result.exitCode !== 0) {
      throw new Error(
        `OpenSandbox ranged read failed (exit=${result.exitCode}): ${summarizeCommandOutput(
          result.stderr,
        )}`,
      );
    }
    return parseSandboxReadPageResult(result.stdout, input);
  }

  async list(path: string): Promise<string[]> {
    const result = await this.handle.runCommand(`ls -1A '${escapeSingleQuotes(path)}'`);
    if (result.exitCode !== 0) {
      throw new Error(`ls ${path} failed (exit=${result.exitCode}): ${result.stderr}`);
    }
    return result.stdout.split('\n').filter((s) => s.length > 0);
  }

  async chmod(path: string, mode: number): Promise<void> {
    const normalizedMode = normalizeFileMode(mode);
    const result = await this.handle.runCommand(
      `chmod ${normalizedMode.toString(8)} -- '${escapeSingleQuotes(path)}'`,
    );
    if (result.exitCode !== 0) {
      throw new Error(`chmod ${path} failed (exit=${result.exitCode}): ${result.stderr}`);
    }
  }

  async chmodMany(root: string, entries: readonly SandboxFileMode[]): Promise<void> {
    if (entries.length === 0) return;
    const manifestPath = `/tmp/orca-chmod-${randomUUID()}.json`;
    await this.write(manifestPath, serializeSandboxFileModes(root, entries));
    const result = await this.handle.runCommand(buildSandboxChmodManyCommand(manifestPath, root), {
      envs: { ...SANDBOX_READ_COMMAND_ENVS },
      timeoutMs: SANDBOX_CHMOD_MANY_TIMEOUT_MS,
    });
    if (result.exitCode !== 0) {
      await this.delete(manifestPath).catch(() => undefined);
      throw new Error(
        `batch chmod failed (exit=${result.exitCode}): ${summarizeCommandOutput(result.stderr)}`,
      );
    }
  }

  async delete(path: string): Promise<void> {
    const response = await this.handle.execdFetch(`/files?path=${encodeURIComponent(path)}`, {
      method: 'DELETE',
    });
    if (!response.ok && response.status !== 404) {
      throw new Error(await formatHttpError(response, `OpenSandbox delete ${path} failed`));
    }
  }
}

async function consumeCommandStream(
  response: Response,
  streamTimeoutMs: number,
): Promise<CommandExecution> {
  const stdoutParts: string[] = [];
  const stderrParts: string[] = [];
  let complete = false;
  let exitCode: number | null = null;

  await readJsonEvents(response, streamTimeoutMs, (event) => {
    if (event.type === 'stdout' && event.text) stdoutParts.push(event.text);
    if (event.type === 'stderr' && event.text) stderrParts.push(event.text);
    if (event.type === 'execution_complete') complete = true;
    if (event.type === 'error') {
      const value = event.error?.['evalue'] ?? event.error?.['value'];
      const parsed =
        typeof value === 'string' && /^-?\d+$/.test(value.trim())
          ? Number(value.trim())
          : Number.NaN;
      // An explicit error event is always a failed command, even when a
      // buggy/malicious execd reports evalue=0.
      exitCode = Number.isFinite(parsed) && parsed !== 0 ? parsed : 1;
    }
  });

  return {
    stdout: stdoutParts.join('\n'),
    stderr: stderrParts.join('\n'),
    exitCode: exitCode ?? (complete ? 0 : 1),
  };
}

async function readJsonEvents(
  response: Response,
  streamTimeoutMs: number,
  onEvent: (event: ServerStreamEvent) => void,
): Promise<void> {
  const body = response.body;
  if (!body) return;
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let bytesRead = 0;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(
      () => reject(new Error(`OpenSandbox command stream timed out after ${streamTimeoutMs}ms`)),
      streamTimeoutMs,
    );
  });

  try {
    while (true) {
      const { done, value } = await Promise.race([reader.read(), timedOut]);
      if (done) break;
      bytesRead += value.byteLength;
      if (bytesRead > MAX_COMMAND_STREAM_BYTES) {
        throw new Error(
          `OpenSandbox command stream exceeded ${MAX_COMMAND_STREAM_BYTES}-byte limit`,
        );
      }
      buffer += decoder.decode(value, { stream: true });
      drainFrames(buffer, onEvent, (rest) => {
        buffer = rest;
      });
    }
    buffer += decoder.decode();
    parseFrame(buffer, onEvent);
  } catch (error) {
    await reader.cancel(error).catch(() => undefined);
    throw error;
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

function drainFrames(
  input: string,
  onEvent: (event: ServerStreamEvent) => void,
  setRest: (rest: string) => void,
): void {
  let buffer = input;
  while (true) {
    const lf = buffer.indexOf('\n\n');
    const crlf = buffer.indexOf('\r\n\r\n');
    const idx = lf === -1 ? crlf : crlf === -1 ? lf : Math.min(lf, crlf);
    if (idx === -1) {
      setRest(buffer);
      return;
    }
    const sepLen = buffer.startsWith('\r\n\r\n', idx) ? 4 : 2;
    const frame = buffer.slice(0, idx);
    parseFrame(frame, onEvent);
    buffer = buffer.slice(idx + sepLen);
  }
}

function parseFrame(frame: string, onEvent: (event: ServerStreamEvent) => void): void {
  const trimmed = frame.trim();
  if (!trimmed) return;
  const dataLines = trimmed
    .split(/\r?\n/)
    .filter((line) => line.startsWith('data:'))
    .map((line) => line.slice(5).trimStart());
  const payload = dataLines.length > 0 ? dataLines.join('\n') : trimmed;
  if (!payload || payload === '[DONE]') return;
  try {
    onEvent(JSON.parse(payload) as ServerStreamEvent);
  } catch {
    // Ignore keepalive/status frames that are not JSON.
  }
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutSeconds: number,
): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutSeconds * 1000);
  try {
    const signals = [controller.signal];
    if (init.signal) signals.push(init.signal);
    return await fetch(url, {
      ...init,
      signal: AbortSignal.any(signals),
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function formatHttpError(response: Response, prefix: string): Promise<string> {
  const body = await response.text().catch(() => '');
  const suffix = body ? `: ${body.slice(0, 500)}` : '';
  return `${prefix} (status=${response.status})${suffix}`;
}

async function readStream(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
  }
  return Buffer.concat(chunks);
}

function normalizeServerBaseUrl(domain: string, protocol: 'http' | 'https'): string {
  const raw = stripTrailingSlashes(domain);
  if (raw.startsWith('http://') || raw.startsWith('https://')) {
    return stripV1Suffix(raw);
  }
  return `${protocol}://${stripV1Suffix(raw)}`;
}

export function buildOpenSandboxEndpointUrl(endpoint: string, protocol: 'http' | 'https'): string {
  const stripped = stripTrailingSlashes(endpoint);
  if (stripped.startsWith('http://') || stripped.startsWith('https://')) return stripped;
  return `${protocol}://${stripped}`;
}

function stripTrailingSlashes(s: string): string {
  let out = s;
  while (out.endsWith('/')) out = out.slice(0, -1);
  return out;
}

function stripV1Suffix(s: string): string {
  const trimmed = stripTrailingSlashes(s);
  return trimmed.endsWith('/v1') ? trimmed.slice(0, -3) : trimmed;
}

function basename(path: string): string {
  const parts = path.split('/').filter(Boolean);
  return parts[parts.length - 1] ?? 'file';
}

function escapeSingleQuotes(s: string): string {
  return s.replace(/'/g, "'\\''");
}

function normalizeFileMode(mode: number): number {
  if (!Number.isInteger(mode) || mode < 0 || mode > 0o777) {
    throw new Error(`invalid file mode: ${mode}`);
  }
  return mode;
}

function summarizeCommandOutput(s: string): string {
  if (s.length === 0) return '<empty>';
  return JSON.stringify(s.length > 500 ? `${s.slice(0, 500)}…` : s);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** Match uploaded resources and the in-sandbox harness in the outer UID namespace. */
function buildOpenSandboxToolCommand(command: string, policy: SandboxWritePolicy): string {
  // execd runs as root. bwrap --uid 1000 alone maps that outer root to an inner
  // 1000, leaving node/ubuntu-owned files unmapped and unwritable. Drop the
  // actual identity first, then keep the user/PID/mount isolation and cap drop.
  return (
    'setpriv --reuid 1000 --regid 1000 --clear-groups --bounding-set=-all --no-new-privs -- ' +
    buildBubblewrapCommand(command, policy)
  );
}
