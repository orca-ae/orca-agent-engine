// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { posix as posixPath } from 'node:path';
import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import type {
  EnvironmentSpec,
  SandboxCapabilities,
  SandboxFileMode,
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SandboxRuntime,
  SandboxWritePolicy,
  ToolCall,
  ToolResult,
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

const ENVD_PORT = 49983;
const READY_TIMEOUT_MS = 60_000;
const READY_POLL_MS = 500;
const MAX_COMMAND_STREAM_BYTES = 1024 * 1024;
const CONNECT_DATA_FLAG = 0x00;
const CONNECT_END_FLAG = 0x02;
const DEFAULT_FILE_USERNAME = 'ubuntu';

export interface AgentEnvRuntimeOptions {
  /** AgentENV gateway URL, without a required version suffix. */
  baseUrl: string;
  apiKey: string;
  image: string;
  timeoutSeconds: number;
  requestTimeoutSeconds: number;
  cpuCount?: number;
  memoryMB?: number;
  diskSizeMB?: number;
}

interface AgentEnvSandboxResponse {
  sandboxID?: string;
  envdAccessToken?: string | null;
}

interface AgentEnvErrorBody {
  code?: number;
  message?: string;
}

interface CommandExecution {
  stdout: string;
  stderr: string;
  exitCode: number;
}

type DecodedProcessEvent =
  | { type: 'stdout'; bytes: Uint8Array }
  | { type: 'stderr'; bytes: Uint8Array }
  | { type: 'end'; exitCode: number; error?: string }
  | { type: 'other' };

/**
 * Remote Firecracker sandbox runtime backed by AgentENV.
 *
 * Lifecycle requests use the AgentENV REST API. Commands and file transfers
 * use envd through the gateway's header-routed reverse proxy. This adapter
 * deliberately does not advertise FUSE: memory and output resources use the
 * existing Files API fallback because the AgentENV guest image contract does
 * not provide a validated FUSE device and mount policy.
 */
export class AgentEnvRuntime implements SandboxRuntime {
  readonly capabilities: SandboxCapabilities = {
    supportsFuse: false,
    supportsLocalMemory: true,
    supportsWritePolicy: true,
  };

  private readonly baseUrl: string;

  constructor(private readonly opts: AgentEnvRuntimeOptions) {
    this.baseUrl = stripTrailingSlashes(opts.baseUrl);
  }

  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    if (hasRequestedPackages(env)) {
      throw new Error(
        'AgentENV does not allow runtime package installation before the agent isolation boundary; use an operator-approved prebuilt image',
      );
    }
    if (env.entrypoint?.length) {
      throw new Error('AgentENV does not support a per-acquire entrypoint override');
    }
    if (env.exposePorts?.length) {
      throw new Error('AgentENV does not support declaring exposed ports at acquire time');
    }

    const body = buildAgentEnvAcquireBody(env, this.opts);
    const created = await this.requestJson<AgentEnvSandboxResponse>('/sandboxes-cold', {
      method: 'POST',
      body,
    });
    if (!created.sandboxID) {
      throw new Error('AgentENV create sandbox failed: response did not include sandboxID');
    }
    if (!created.envdAccessToken) {
      await this.deleteSandbox(created.sandboxID).catch(() => undefined);
      throw new Error(
        'AgentENV create sandbox failed: secure sandbox response did not include envdAccessToken',
      );
    }

    const handle = new AgentEnvSandboxHandle({
      id: created.sandboxID,
      envdAccessToken: created.envdAccessToken,
      runtime: this,
      ...(env.fileUploadOwnership ? { fileUploadOwnership: env.fileUploadOwnership } : {}),
    });
    try {
      await handle.waitUntilReady();
      await handle.verifyPrerequisites();
      return handle;
    } catch (error) {
      await handle.destroy().catch(() => undefined);
      throw error;
    }
  }

  async requestJson<T>(path: string, init: RequestInit & { body?: unknown } = {}): Promise<T> {
    const response = await this.lifecycleRequest(path, init);
    if (!response.ok) {
      throw new Error(
        await formatHttpError(response, `AgentENV ${init.method ?? 'GET'} ${path} failed`),
      );
    }
    return (await response.json()) as T;
  }

  async requestOk(path: string, init: RequestInit & { body?: unknown } = {}): Promise<void> {
    const response = await this.lifecycleRequest(path, init);
    if (!response.ok) {
      throw new Error(
        await formatHttpError(response, `AgentENV ${init.method ?? 'GET'} ${path} failed`),
      );
    }
  }

  async proxyFetch(
    sandboxId: string,
    envdAccessToken: string,
    path: string,
    init: RequestInit,
    timeoutMs?: number,
  ): Promise<Response> {
    const headers = {
      'x-agentenv-sandbox-id': sandboxId,
      'x-agentenv-target-port': String(ENVD_PORT),
      'X-Access-Token': envdAccessToken,
      ...(init.headers as Record<string, string> | undefined),
    };
    return await fetchWithTimeout(
      `${this.baseUrl}${path}`,
      { ...init, headers },
      timeoutMs ?? this.opts.requestTimeoutSeconds * 1000,
    );
  }

  async pauseSandbox(sandboxId: string): Promise<void> {
    await this.requestOk(`/sandboxes/${encodeURIComponent(sandboxId)}/pause`, {
      method: 'POST',
    });
  }

  async connectSandbox(sandboxId: string): Promise<AgentEnvSandboxResponse> {
    return await this.requestJson<AgentEnvSandboxResponse>(
      `/sandboxes/${encodeURIComponent(sandboxId)}/connect`,
      {
        method: 'POST',
        body: { timeout: this.opts.timeoutSeconds },
      },
    );
  }

  async deleteSandbox(sandboxId: string): Promise<void> {
    const response = await this.lifecycleRequest(`/sandboxes/${encodeURIComponent(sandboxId)}`, {
      method: 'DELETE',
    });
    if (!response.ok && response.status !== 404) {
      throw new Error(await formatHttpError(response, 'AgentENV delete sandbox failed'));
    }
  }

  endpoint(
    sandboxId: string,
    envdAccessToken: string,
    port: number,
  ): { url: string; headers: Record<string, string> } {
    return {
      url: this.baseUrl,
      headers: {
        'x-agentenv-sandbox-id': sandboxId,
        'x-agentenv-target-port': String(port),
        'X-Access-Token': envdAccessToken,
      },
    };
  }

  requestTimeoutSeconds(): number {
    return this.opts.requestTimeoutSeconds;
  }

  private async lifecycleRequest(
    path: string,
    init: RequestInit & { body?: unknown } = {},
  ): Promise<Response> {
    const body = init.body === undefined ? undefined : JSON.stringify(init.body);
    const headers: Record<string, string> = { 'X-API-Key': this.opts.apiKey };
    if (body !== undefined) headers['content-type'] = 'application/json';
    return await fetchWithTimeout(
      `${this.baseUrl}${path}`,
      {
        ...init,
        headers: { ...headers, ...(init.headers as Record<string, string> | undefined) },
        body,
      },
      this.opts.requestTimeoutSeconds * 1000,
    );
  }
}

export function buildAgentEnvAcquireBody(
  env: EnvironmentSpec,
  opts: Pick<
    AgentEnvRuntimeOptions,
    'image' | 'timeoutSeconds' | 'cpuCount' | 'memoryMB' | 'diskSizeMB'
  >,
): Record<string, unknown> {
  const body: Record<string, unknown> = {
    image: env.image ?? opts.image,
    timeout: opts.timeoutSeconds,
    autoPause: false,
    secure: true,
    metadata: { owner: 'orca-managed-agents' },
  };
  if (env.harnessEnv && Object.keys(env.harnessEnv).length > 0) {
    body['envVars'] = { ...env.harnessEnv };
  }
  if (env.networking && Object.keys(env.networking).length > 0) {
    body['network'] = { ...env.networking };
  }
  if (opts.cpuCount !== undefined) body['cpuCount'] = opts.cpuCount;
  if (opts.memoryMB !== undefined) body['memoryMB'] = opts.memoryMB;
  if (opts.diskSizeMB !== undefined) body['diskSizeMB'] = opts.diskSizeMB;
  return body;
}

function hasRequestedPackages(env: EnvironmentSpec): boolean {
  if (!env.packages) return false;
  return Object.values(env.packages).some((packages) =>
    (packages ?? []).some((pkg) => pkg.trim().length > 0),
  );
}

class AgentEnvSandboxHandle implements SandboxHandle {
  readonly files: SandboxFiles;
  private destroyed = false;
  private envdAccessToken: string;

  constructor(
    private readonly opts: {
      id: string;
      envdAccessToken: string;
      runtime: AgentEnvRuntime;
      fileUploadOwnership?: { owner: string; group: string };
    },
  ) {
    this.envdAccessToken = opts.envdAccessToken;
    this.files = new AgentEnvFiles(this, opts.fileUploadOwnership);
  }

  get id(): string {
    return this.opts.id;
  }

  async waitUntilReady(): Promise<void> {
    const deadline = Date.now() + READY_TIMEOUT_MS;
    let lastError: unknown;
    while (Date.now() < deadline) {
      try {
        const result = await this.runCommand('true');
        if (result.exitCode === 0) return;
        lastError = new Error(`envd readiness command exited ${result.exitCode}`);
      } catch (error) {
        lastError = error;
      }
      await sleep(READY_POLL_MS);
    }
    throw new Error(`AgentENV sandbox ${this.id} did not become ready: ${errorMessage(lastError)}`);
  }

  async verifyPrerequisites(): Promise<void> {
    const result = await this.runCommand(
      [
        'set -euo pipefail',
        'test "$(id -u)" -eq 0',
        'command -v bash >/dev/null',
        'command -v bwrap >/dev/null',
        'command -v node >/dev/null',
        'command -v realpath >/dev/null',
        'command -v setpriv >/dev/null',
        'command -v timeout >/dev/null',
        'test "$(id -u ubuntu):$(id -g ubuntu)" = 1000:1000',
        'install -d -m 0777 /mnt/inputs /mnt/memory /mnt/session/outputs',
      ].join('\n'),
    );
    if (result.exitCode !== 0) {
      throw new Error(
        `AgentENV image prerequisite probe failed: requires root, bash, bubblewrap, Node, realpath, setpriv, timeout, and ubuntu uid/gid 1000 (exit=${result.exitCode}). stderr=${boundedDiagnostic(result.stderr)}; stdout=${boundedDiagnostic(result.stdout)}`,
      );
    }
  }

  async run(call: ToolCall): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);

    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      const result = await this.runCommand(args.command, {
        timeoutMs: args.timeout_ms,
      });
      return { stdout: result.stdout, stderr: result.stderr, exit_code: result.exitCode };
    }

    if (call.tool === 'glob') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ?? '.';
      const result = await this.runCommand(
        `cd '${escapeSingleQuotes(cwd)}' && compgen -G '${escapeSingleQuotes(args.pattern)}' || true`,
      );
      return { output: result.stdout.split('\n').filter((value) => value.length > 0) };
    }

    if (call.tool === 'grep') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ?? '.';
      const result = await this.runCommand(
        `cd '${escapeSingleQuotes(cwd)}' && grep -rn '${escapeSingleQuotes(args.pattern)}' . || true`,
      );
      return { output: result.stdout };
    }

    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
  }

  async prepareFilesystemRoots(paths: readonly string[]): Promise<void> {
    const result = await this.runCommand(buildSandboxFilesystemRootPreflightCommand(paths), {
      envs: { ...SANDBOX_READ_COMMAND_ENVS },
      timeoutMs: SANDBOX_FILESYSTEM_ROOT_PREFLIGHT_TIMEOUT_MS,
    });
    if (result.exitCode !== 0) {
      throw new Error(
        `AgentENV filesystem-root preflight failed: ${boundedDiagnostic(result.stderr)}`,
      );
    }
  }

  async prepareWritePolicy(policy: SandboxWritePolicy): Promise<void> {
    const readProbe = await this.runCommand(buildSandboxReadPrerequisiteProbeCommand(), {
      envs: { ...SANDBOX_READ_COMMAND_ENVS },
      timeoutMs: SANDBOX_READ_TIMEOUT_MS,
    });
    if (readProbe.exitCode !== 0) {
      throw new Error(
        `AgentENV ranged-read prerequisite probe failed: ${boundedDiagnostic(readProbe.stderr)}`,
      );
    }
    const aliasProbe = await this.runCommand(buildSkillsAliasProbeCommand(policy));
    if (aliasProbe.exitCode !== 0) {
      throw new Error(`AgentENV filesystem-alias probe failed: ${aliasProbe.stderr}`);
    }
    const result = await this.runCommand(buildBubblewrapCommand('true', policy));
    if (result.exitCode !== 0) {
      throw new Error(`AgentENV write-policy sandbox probe failed: ${result.stderr}`);
    }
  }

  async runWithWritePolicy(call: ToolCall, policy: SandboxWritePolicy): Promise<ToolResult> {
    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      const result = await this.runCommand(buildBubblewrapCommand(args.command, policy), {
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
      const result = await this.runCommand(buildBubblewrapCommand(body, policy));
      return call.tool === 'glob'
        ? { output: result.stdout.split('\n').filter((value) => value.length > 0) }
        : { output: result.stdout };
    }
    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
  }

  async canonicalizePathForPolicy(path: string): Promise<string> {
    const result = await this.runCommand(`realpath -m -- '${escapeSingleQuotes(path)}'`);
    if (result.exitCode !== 0) throw new Error(`realpath failed: ${result.stderr}`);
    return result.stdout.trim();
  }

  async runPrivileged(cmd: string, opts?: { envs?: Record<string, string> }): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const result = await this.runCommand(cmd.replace(/^\s*sudo\s+/, ''), {
      envs: opts?.envs,
    });
    return { stdout: result.stdout, stderr: result.stderr, exit_code: result.exitCode };
  }

  async pause(): Promise<void> {
    if (this.destroyed) return;
    await this.opts.runtime.pauseSandbox(this.id);
  }

  async resume(): Promise<void> {
    if (this.destroyed) return;
    const response = await this.opts.runtime.connectSandbox(this.id);
    if (response.envdAccessToken) this.envdAccessToken = response.envdAccessToken;
    await this.waitUntilReady();
  }

  async destroy(): Promise<void> {
    if (this.destroyed) return;
    await this.opts.runtime.deleteSandbox(this.id);
    this.destroyed = true;
  }

  async endpoint(port: number): Promise<{ url: string; headers?: Record<string, string> }> {
    return this.opts.runtime.endpoint(this.id, this.envdAccessToken, port);
  }

  async runCommand(
    command: string,
    opts: { timeoutMs?: number; envs?: Record<string, string> } = {},
  ): Promise<CommandExecution> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const timeoutMs =
      opts.timeoutMs === undefined
        ? this.opts.runtime.requestTimeoutSeconds() * 1000
        : opts.timeoutMs + 5_000;
    const guestCommand =
      opts.timeoutMs === undefined ? command : buildGuestTimeoutCommand(command, opts.timeoutMs);
    const payload = encodeStartRequest(guestCommand, opts.envs ?? {});
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await this.opts.runtime.proxyFetch(
        this.id,
        this.envdAccessToken,
        '/process.Process/Start',
        {
          method: 'POST',
          headers: {
            'Connect-Protocol-Version': '1',
            'content-type': 'application/connect+proto',
          },
          body: encodeConnectEnvelope(CONNECT_DATA_FLAG, payload),
          signal: controller.signal,
        },
        timeoutMs,
      );
      if (!response.ok) {
        throw new Error(await formatHttpError(response, 'AgentENV envd command failed'));
      }
      return await consumeProcessStream(response);
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error(`AgentENV command stream timed out after ${timeoutMs}ms`, {
          cause: error,
        });
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  proxyFetch(path: string, init: RequestInit, timeoutMs?: number): Promise<Response> {
    return this.opts.runtime.proxyFetch(this.id, this.envdAccessToken, path, init, timeoutMs);
  }
}

class AgentEnvFiles implements SandboxFiles {
  constructor(
    private readonly handle: AgentEnvSandboxHandle,
    private readonly uploadOwnership?: { owner: string; group: string },
  ) {}

  async write(path: string, content: Buffer | NodeJS.ReadableStream): Promise<void> {
    const bytes = Buffer.isBuffer(content) ? content : await readStream(content);
    const query = new URLSearchParams({
      path,
      username: this.uploadOwnership?.owner ?? DEFAULT_FILE_USERNAME,
    });
    const form = new FormData();
    form.append(
      'file',
      new Blob([bytes], { type: 'application/octet-stream' }),
      posixPath.basename(path) || 'file',
    );
    const response = await this.handle.proxyFetch(`/files?${query.toString()}`, {
      method: 'POST',
      body: form,
    });
    if (!response.ok) {
      throw new Error(await formatHttpError(response, `AgentENV write ${path} failed`));
    }
    if (isManagedWritablePath(path)) {
      const parent = posixPath.dirname(path);
      const result = await this.handle.runCommand(`chmod 0777 -- '${escapeSingleQuotes(parent)}'`);
      if (result.exitCode !== 0) {
        throw new Error(
          `chmod managed parent ${parent} failed (exit=${result.exitCode}): ${result.stderr}`,
        );
      }
    }
  }

  async read(path: string): Promise<Buffer> {
    const query = new URLSearchParams({
      path,
      username: this.uploadOwnership?.owner ?? DEFAULT_FILE_USERNAME,
    });
    const response = await this.handle.proxyFetch(`/files?${query.toString()}`, {
      method: 'GET',
    });
    if (!response.ok) {
      throw new Error(await formatHttpError(response, `AgentENV read ${path} failed`));
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
      },
    );
    if (result.exitCode !== 0) {
      throw new Error(
        `AgentENV ranged read failed (exit=${result.exitCode}): ${boundedDiagnostic(result.stderr)}`,
      );
    }
    return parseSandboxReadPageResult(result.stdout, input);
  }

  async list(path: string): Promise<string[]> {
    const result = await this.handle.runCommand(`ls -1A -- '${escapeSingleQuotes(path)}'`);
    if (result.exitCode !== 0) {
      throw new Error(`ls ${path} failed (exit=${result.exitCode}): ${result.stderr}`);
    }
    return result.stdout.split('\n').filter((value) => value.length > 0);
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
        `batch chmod failed (exit=${result.exitCode}): ${boundedDiagnostic(result.stderr)}`,
      );
    }
  }

  async delete(path: string): Promise<void> {
    const result = await this.handle.runCommand(`rm -rf -- '${escapeSingleQuotes(path)}'`);
    if (result.exitCode !== 0) {
      throw new Error(`delete ${path} failed (exit=${result.exitCode}): ${result.stderr}`);
    }
  }
}

function encodeStartRequest(command: string, envs: Record<string, string>): Uint8Array {
  const process = new BinaryWriter();
  process.uint32(10).string('/bin/bash');
  process.uint32(18).string('-lc');
  process.uint32(18).string(command);
  for (const [key, value] of Object.entries(envs).sort(([a], [b]) => a.localeCompare(b))) {
    const entry = new BinaryWriter();
    entry.uint32(10).string(key);
    entry.uint32(18).string(value);
    process.uint32(26).bytes(entry.finish());
  }

  const request = new BinaryWriter();
  request.uint32(10).bytes(process.finish());
  // proto3 optional bool: encode false explicitly so envd does not apply the
  // backwards-compatible default of keeping stdin open.
  request.uint32(32).bool(false);
  return request.finish();
}

function buildGuestTimeoutCommand(command: string, timeoutMs: number): string {
  const seconds = Math.max(0.001, timeoutMs / 1000).toFixed(3);
  return `timeout --signal=TERM --kill-after=1s ${seconds}s /bin/bash -lc '${escapeSingleQuotes(command)}'`;
}

function isManagedWritablePath(path: string): boolean {
  return path.startsWith('/mnt/memory/') || path.startsWith('/mnt/session/outputs/');
}

function encodeConnectEnvelope(flag: number, payload: Uint8Array): Uint8Array {
  const envelope = Buffer.allocUnsafe(5 + payload.byteLength);
  envelope[0] = flag;
  envelope.writeUInt32BE(payload.byteLength, 1);
  Buffer.from(payload).copy(envelope, 5);
  return envelope;
}

async function consumeProcessStream(response: Response): Promise<CommandExecution> {
  if (!response.body) throw new Error('AgentENV envd command returned an empty stream');
  const reader = response.body.getReader();
  let buffer = Buffer.alloc(0);
  let totalBytes = 0;
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  let exitCode: number | undefined;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    totalBytes += value.byteLength;
    if (totalBytes > MAX_COMMAND_STREAM_BYTES) {
      await reader.cancel().catch(() => undefined);
      throw new Error(
        `AgentENV envd command stream exceeded ${MAX_COMMAND_STREAM_BYTES}-byte limit`,
      );
    }
    buffer = Buffer.concat([buffer, Buffer.from(value)]);
    while (buffer.byteLength >= 5) {
      const flag = buffer[0]!;
      const length = buffer.readUInt32BE(1);
      if (buffer.byteLength < 5 + length) break;
      const payload = buffer.subarray(5, 5 + length);
      buffer = buffer.subarray(5 + length);
      if (flag === CONNECT_DATA_FLAG) {
        const event = decodeStartResponse(payload);
        if (event.type === 'stdout') stdout.push(Buffer.from(event.bytes));
        if (event.type === 'stderr') stderr.push(Buffer.from(event.bytes));
        if (event.type === 'end') {
          exitCode = event.exitCode;
          if (event.error) stderr.push(Buffer.from(event.error));
        }
      } else if (flag === CONNECT_END_FLAG) {
        const error = parseConnectEndError(payload);
        if (error) {
          await reader.cancel().catch(() => undefined);
          throw error;
        }
      } else {
        await reader.cancel().catch(() => undefined);
        throw new Error(`AgentENV envd returned unexpected Connect flag 0x${flag.toString(16)}`);
      }
    }
  }

  if (buffer.byteLength !== 0) {
    throw new Error('AgentENV envd command stream ended with a truncated Connect envelope');
  }
  if (exitCode === undefined) {
    throw new Error('AgentENV envd command stream ended without a process exit event');
  }
  return {
    stdout: Buffer.concat(stdout).toString('utf8'),
    stderr: Buffer.concat(stderr).toString('utf8'),
    exitCode,
  };
}

function decodeStartResponse(payload: Uint8Array): DecodedProcessEvent {
  const reader = new BinaryReader(payload);
  while (reader.pos < reader.len) {
    const tag = reader.uint32();
    if (tag >>> 3 === 1 && (tag & 7) === 2) return decodeProcessEvent(reader.bytes());
    reader.skip(tag & 7);
  }
  return { type: 'other' };
}

function decodeProcessEvent(payload: Uint8Array): DecodedProcessEvent {
  const reader = new BinaryReader(payload);
  while (reader.pos < reader.len) {
    const tag = reader.uint32();
    const field = tag >>> 3;
    if ((tag & 7) === 2 && field === 2) return decodeDataEvent(reader.bytes());
    if ((tag & 7) === 2 && field === 3) return decodeEndEvent(reader.bytes());
    reader.skip(tag & 7);
  }
  return { type: 'other' };
}

function decodeDataEvent(payload: Uint8Array): DecodedProcessEvent {
  const reader = new BinaryReader(payload);
  while (reader.pos < reader.len) {
    const tag = reader.uint32();
    const field = tag >>> 3;
    if ((tag & 7) === 2 && field === 1) return { type: 'stdout', bytes: reader.bytes() };
    if ((tag & 7) === 2 && (field === 2 || field === 3)) {
      return { type: 'stderr', bytes: reader.bytes() };
    }
    reader.skip(tag & 7);
  }
  return { type: 'other' };
}

function decodeEndEvent(payload: Uint8Array): DecodedProcessEvent {
  const reader = new BinaryReader(payload);
  let exitCode = 0;
  let error: string | undefined;
  while (reader.pos < reader.len) {
    const tag = reader.uint32();
    const field = tag >>> 3;
    if (field === 1 && (tag & 7) === 0) {
      exitCode = reader.sint32();
      continue;
    }
    if (field === 4 && (tag & 7) === 2) {
      error = reader.string();
      continue;
    }
    reader.skip(tag & 7);
  }
  return error === undefined ? { type: 'end', exitCode } : { type: 'end', exitCode, error };
}

function parseConnectEndError(payload: Uint8Array): Error | undefined {
  if (payload.byteLength === 0 || Buffer.from(payload).toString('utf8') === '{}') return undefined;
  try {
    const trailer = JSON.parse(Buffer.from(payload).toString('utf8')) as {
      error?: { code?: string; message?: string };
    };
    if (!trailer.error) return undefined;
    const code = trailer.error.code ?? 'unknown';
    const message = trailer.error.message ?? '';
    return new Error(message ? `${code}: ${message}` : code);
  } catch {
    return new Error('AgentENV envd returned an invalid Connect end envelope');
  }
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number,
): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const signals = [controller.signal];
    if (init.signal) signals.push(init.signal);
    return await fetch(url, { ...init, signal: AbortSignal.any(signals) });
  } finally {
    clearTimeout(timer);
  }
}

async function formatHttpError(response: Response, prefix: string): Promise<string> {
  const body = await response.text().catch(() => '');
  let detail = body.trim();
  try {
    const parsed = JSON.parse(body) as AgentEnvErrorBody;
    if (parsed.message?.trim()) detail = parsed.message.trim();
  } catch {
    // Keep the bounded plain-text response.
  }
  return `${prefix} (status=${response.status})${detail ? `: ${detail.slice(0, 500)}` : ''}`;
}

async function readStream(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
  }
  return Buffer.concat(chunks);
}

function normalizeFileMode(mode: number): number {
  if (!Number.isInteger(mode) || mode < 0 || mode > 0o777) {
    throw new Error(`invalid file mode: ${mode}`);
  }
  return mode;
}

function stripTrailingSlashes(value: string): string {
  let result = value;
  while (result.endsWith('/')) result = result.slice(0, -1);
  return result;
}

function escapeSingleQuotes(value: string): string {
  return value.replace(/'/g, "'\\''");
}

function boundedDiagnostic(value: string): string {
  if (value.length === 0) return '<empty>';
  return JSON.stringify(value.length > 500 ? `${value.slice(0, 500)}…` : value);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
