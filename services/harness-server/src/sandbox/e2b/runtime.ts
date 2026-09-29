// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { posix as posixPath } from 'node:path';
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

const SUDO_PRESERVED_ENV_NAMES = new Set([
  'ORCA_S3_ACCESS_KEY_ID',
  'ORCA_S3_SECRET_ACCESS_KEY',
  'ORCA_S3_SESSION_TOKEN',
]);
const PRIVILEGED_COMMAND_ENV_NAMES = new Set([
  ...SUDO_PRESERVED_ENV_NAMES,
  'BASH_ENV',
  'ENV',
  'PATH',
]);

export function buildE2BPrivilegedCommand(cmd: string, envNames: readonly string[] = []): string {
  const unsupported = envNames.filter((name) => !PRIVILEGED_COMMAND_ENV_NAMES.has(name));
  if (unsupported.length > 0) {
    throw new Error(`unsupported E2B privileged environment: ${unsupported.join(',')}`);
  }
  const preservedNames = envNames.filter((name) => SUDO_PRESERVED_ENV_NAMES.has(name));
  const preserve = preservedNames.length > 0 ? `--preserve-env=${preservedNames.join(',')} ` : '';
  return `sudo ${preserve}${cmd.replace(/^\s*sudo\s+/, '')}`;
}

/**
 * Production sandbox: E2B Cloud. Activated when `E2B_API_KEY` is set.
 *
 * Maps the SandboxRuntime / SandboxHandle interfaces onto the E2B SDK's
 * `Sandbox` class. The exact SDK option names may evolve; this adapter
 * isolates the harness from the SDK churn.
 *
 * Note: importing `@e2b/code-interpreter` is dynamic so the harness still
 * builds + runs (with InMemorySandboxRuntime) when the dep is unavailable
 * or when E2B_API_KEY is unset.
 */
export interface E2BSandboxRuntimeOptions {
  apiKey: string;
  /** Optional template id to provision; SDK default if unset. */
  templateId?: string;
  /** Optional E2B server URL override (e.g. for self-hosted). */
  baseURL?: string;
}

/**
 * Minimal structural type of the E2B `Sandbox` class. Mirrors only the
 * surface this adapter calls — keeps the file decoupled from the SDK's
 * full type tree (it ships as a CJS-flavored ESM dual build) and lets the
 * dynamic-import path stay typed.
 */
interface E2BSdkSandbox {
  sandboxId?: string;
  id?: string;
  commands: {
    run: (
      cmd: string,
      opts?: { timeoutMs?: number; envs?: Record<string, string> },
    ) => Promise<{
      stdout: string;
      stderr: string;
      exitCode: number;
    }>;
  };
  files: {
    write: (
      path: string,
      data: string | ArrayBuffer | Uint8Array | Blob | ReadableStream,
    ) => Promise<unknown>;
    read: (path: string, opts?: { format?: 'text' | 'bytes' }) => Promise<string | Uint8Array>;
    list: (path: string) => Promise<Array<{ name: string } | string>>;
    remove?: (path: string) => Promise<unknown>;
    delete?: (path: string) => Promise<unknown>;
  };
  pause?: () => Promise<unknown>;
  resume?: () => Promise<unknown>;
  kill?: () => Promise<unknown>;
  getHost?: (port: number) => string;
}

interface E2BSdkSandboxStatic {
  // The SDK ships two overloads — `create(opts)` (default template) and
  // `create(template, opts)` (explicit template id/name). The opts object has
  // no `template` field, so passing it via opts is silently dropped and the
  // SDK falls back to its built-in template. The harness MUST use the
  // positional form whenever a custom `templateId` is configured.
  create: ((template: string, opts: Record<string, unknown>) => Promise<E2BSdkSandbox>) &
    ((opts: Record<string, unknown>) => Promise<E2BSdkSandbox>);
}

export class E2BSandboxRuntime implements SandboxRuntime {
  /**
   * The `orca-default` custom template (see
   * `services/harness-server/sandbox-templates/orca-default/`) bakes in
   * `s3fs-fuse` + `fuse3` and a sudo-enabled constrained mount helper, so
   * privileged FUSE mounts work inside the sandbox. The template's recorded
   * spike run (see its README) confirmed `CAP_SYS_ADMIN` is available.
   * Strategy factory uses this to default memory_store resources to
   * `memory_fuse`; file resources are always delivered host-side via
   * `tarball_prefetch`.
   */
  readonly capabilities: SandboxCapabilities = {
    supportsFuse: true,
    supportsWritePolicy: true,
  };

  constructor(private readonly opts: E2BSandboxRuntimeOptions) {}

  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    // Dynamic import so this file's compile + the harness's startup work
    // even when @e2b/code-interpreter isn't actually used (InMemory path).
    const mod = (await import('@e2b/code-interpreter')) as unknown as {
      Sandbox: E2BSdkSandboxStatic;
    };
    const Sandbox = mod.Sandbox;
    // The SDK has two `create` overloads. The opts-only form has no
    // `template` field, so a misnamed `template`/`templateId` key in the opts
    // object is silently ignored and the SDK spawns from its default
    // template. Use the positional form whenever a templateId is configured
    // (either the per-acquire env.image override or the constructor option)
    // so the orca-default image's pre-created /mnt roots, sudoers, and
    // s3fs-fuse binary are actually present inside the sandbox.
    const createOpts: Record<string, unknown> = { apiKey: this.opts.apiKey };
    if (this.opts.baseURL !== undefined) createOpts['domain'] = this.opts.baseURL;
    // Pass harnessEnv into the sandbox environment (LITELLM_API_BASE, LITELLM_API_KEY, …).
    // Treat undefined as {} per the backward-compat contract.
    if (env.harnessEnv && Object.keys(env.harnessEnv).length > 0) {
      createOpts['envs'] = { ...(env.harnessEnv ?? {}) };
    }
    // Per-acquire image override takes precedence over the constructor templateId.
    const templateId = env.image ?? this.opts.templateId;
    const sb =
      templateId !== undefined
        ? await Sandbox.create(templateId, createOpts)
        : await Sandbox.create(createOpts);
    try {
      await verifyE2BTemplatePrerequisites(sb);
      return new E2BSandboxHandle(sb);
    } catch (error) {
      await sb.kill?.().catch(() => undefined);
      throw error;
    }
  }
}

async function verifyE2BTemplatePrerequisites(sb: E2BSdkSandbox): Promise<void> {
  const result = await sb.commands.run(buildE2BPrerequisiteProbeCommand());
  if (result.exitCode !== 0) {
    throw new Error(
      `E2B template prerequisite probe failed: requires UID/GID 1000, FUSE/bubblewrap tools, /dev/fuse, constrained helper sudo, and no general root shell (exit=${result.exitCode}). stderr=${result.stderr.trim()}; stdout=${result.stdout.trim()}`,
    );
  }
}

export function buildE2BPrerequisiteProbeCommand(): string {
  return [
    'set -euo pipefail',
    'test "$(id -u):$(id -g)" = 1000:1000',
    'command -v bwrap >/dev/null',
    'command -v s3fs >/dev/null',
    'command -v fusermount3 >/dev/null',
    'command -v realpath >/dev/null',
    'command -v setpriv >/dev/null',
    'test -c /dev/fuse',
    'sudo -n -l > /tmp/orca-sudo-list',
    `! grep -Eq 'NOPASSWD:[[:space:]]*ALL|NOPASSWD:SETENV' /tmp/orca-sudo-list`,
    'if sudo -n /bin/sh -c id >/dev/null 2>&1; then exit 91; fi',
    'set +e',
    'ORCA_S3_ACCESS_KEY_ID=x ORCA_S3_SECRET_ACCESS_KEY=y sudo -n --preserve-env=ORCA_S3_ACCESS_KEY_ID,ORCA_S3_SECRET_ACCESS_KEY /usr/local/bin/orca-s3fs-mount bucket /mnt/session/outputs url=http://example.invalid,credlib=/tmp/forbidden.so >/tmp/orca-helper-out 2>/tmp/orca-helper-err',
    'helper_rc=$?',
    'ORCA_S3_ACCESS_KEY_ID=x ORCA_S3_SECRET_ACCESS_KEY=y sudo -n --preserve-env=ORCA_S3_ACCESS_KEY_ID,ORCA_S3_SECRET_ACCESS_KEY /usr/local/bin/orca-s3fs-mount bucket /mnt/custom url=http://example.invalid >/tmp/orca-custom-mount-out 2>/tmp/orca-custom-mount-err',
    'custom_mount_rc=$?',
    'set -e',
    'test "$helper_rc" = 64',
    'test "$custom_mount_rc" = 64',
    `! grep -Fq 'password is required' /tmp/orca-helper-err`,
    `! grep -Fq 'password is required' /tmp/orca-custom-mount-err`,
    'rm -f /tmp/orca-sudo-list /tmp/orca-helper-out /tmp/orca-helper-err /tmp/orca-custom-mount-out /tmp/orca-custom-mount-err',
  ].join('\n');
}

class E2BFiles implements SandboxFiles {
  constructor(private readonly sb: E2BSdkSandbox) {}

  async write(path: string, content: Buffer | NodeJS.ReadableStream): Promise<void> {
    if (Buffer.isBuffer(content)) {
      // Buffer is a Uint8Array — pass it through. SDK accepts Uint8Array.
      await this.sb.files.write(path, content);
      return;
    }
    // Drain stream → buffer; E2B SDK accepts Uint8Array. Bounded by upstream caller.
    const chunks: Buffer[] = [];
    for await (const chunk of content) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
    }
    await this.sb.files.write(path, Buffer.concat(chunks));
  }

  async read(path: string): Promise<Buffer> {
    const data = await this.sb.files.read(path, { format: 'bytes' });
    if (typeof data === 'string') return Buffer.from(data, 'utf8');
    return Buffer.from(data);
  }

  async readUtf8Page(
    path: string,
    input: ReadPageInput,
    constraint?: SandboxReadConstraint,
  ): Promise<ReadPage> {
    const result = await this.sb.commands.run(
      buildSandboxReadPageCommand(path, constraint, input),
      {
        envs: { ...SANDBOX_READ_COMMAND_ENVS },
        timeoutMs: SANDBOX_READ_TIMEOUT_MS,
      },
    );
    if (result.exitCode !== 0) {
      throw new Error(
        `E2B ranged read failed (exit=${result.exitCode}): ${boundedDiagnostic(result.stderr)}`,
      );
    }
    return parseSandboxReadPageResult(result.stdout, input);
  }

  async list(path: string): Promise<string[]> {
    const entries = await this.sb.files.list(path);
    return entries.map((e) => (typeof e === 'string' ? e : e.name));
  }

  async chmod(path: string, mode: number): Promise<void> {
    const normalizedMode = normalizeFileMode(mode);
    const result = await this.sb.commands.run(
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
    const result = await this.sb.commands.run(buildSandboxChmodManyCommand(manifestPath, root), {
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
    try {
      if (this.sb.files.remove) {
        await this.sb.files.remove(path);
        return;
      }
      if (this.sb.files.delete) {
        await this.sb.files.delete(path);
        return;
      }
      throw new Error('E2B SDK has no remove/delete method');
    } catch (error) {
      // SandboxFiles.delete is idempotent. Some E2B SDK versions report a
      // missing target as an error, so confirm absence through the parent
      // listing before accepting it. Any failed/ambiguous probe stays closed.
      const parent = posixPath.dirname(path);
      const target = posixPath.basename(path);
      try {
        const siblings = await this.sb.files.list(parent);
        if (
          !siblings.some((entry) => (typeof entry === 'string' ? entry : entry.name) === target)
        ) {
          return;
        }
      } catch {
        // Preserve the original error; the adapter cannot prove idempotence.
      }
      throw error;
    }
  }
}

class E2BSandboxHandle implements SandboxHandle {
  readonly id: string;
  readonly files: SandboxFiles;
  private destroyed = false;

  constructor(private readonly sb: E2BSdkSandbox) {
    this.id = (sb.sandboxId ?? sb.id ?? `sbx_e2b_${Date.now().toString(36)}`).toString();
    this.files = new E2BFiles(sb);
  }

  async run(call: ToolCall): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);

    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      const runOpts = args.timeout_ms !== undefined ? { timeoutMs: args.timeout_ms } : undefined;
      const result = await this.sb.commands.run(args.command, runOpts);
      return {
        stdout: result.stdout,
        stderr: result.stderr,
        exit_code: result.exitCode,
      };
    }

    if (call.tool === 'glob') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ?? '.';
      const cmd = `cd '${escapeSingleQuotes(cwd)}' && compgen -G '${escapeSingleQuotes(args.pattern)}' || true`;
      const result = await this.sb.commands.run(cmd);
      const matches = result.stdout.split('\n').filter((s) => s.length > 0);
      return { output: matches };
    }

    if (call.tool === 'grep') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ?? '.';
      const cmd = `cd '${escapeSingleQuotes(cwd)}' && grep -rn '${escapeSingleQuotes(args.pattern)}' . || true`;
      const result = await this.sb.commands.run(cmd);
      return { output: result.stdout };
    }

    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
  }

  async prepareFilesystemRoots(paths: readonly string[]): Promise<void> {
    const result = await this.sb.commands.run(buildSandboxFilesystemRootPreflightCommand(paths), {
      envs: { ...SANDBOX_READ_COMMAND_ENVS },
      timeoutMs: SANDBOX_FILESYSTEM_ROOT_PREFLIGHT_TIMEOUT_MS,
    });
    if (result.exitCode !== 0) {
      throw new Error(`E2B filesystem-root preflight failed: ${boundedDiagnostic(result.stderr)}`);
    }
  }

  async prepareWritePolicy(policy: SandboxWritePolicy): Promise<void> {
    const readProbe = await this.sb.commands.run(buildSandboxReadPrerequisiteProbeCommand(), {
      envs: { ...SANDBOX_READ_COMMAND_ENVS },
      timeoutMs: SANDBOX_READ_TIMEOUT_MS,
    });
    if (readProbe.exitCode !== 0) {
      throw new Error(
        `E2B ranged-read prerequisite probe failed: ${boundedDiagnostic(readProbe.stderr)}`,
      );
    }
    const aliasProbe = await this.sb.commands.run(buildSkillsAliasProbeCommand(policy));
    if (aliasProbe.exitCode !== 0) {
      throw new Error(`E2B filesystem-alias probe failed: ${aliasProbe.stderr}`);
    }
    const result = await this.sb.commands.run(buildBubblewrapCommand('true', policy));
    if (result.exitCode !== 0) {
      throw new Error(`E2B write-policy sandbox probe failed: ${result.stderr}`);
    }
  }

  async runWithWritePolicy(call: ToolCall, policy: SandboxWritePolicy): Promise<ToolResult> {
    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      const opts = args.timeout_ms !== undefined ? { timeoutMs: args.timeout_ms } : undefined;
      const result = await this.sb.commands.run(buildBubblewrapCommand(args.command, policy), opts);
      return { stdout: result.stdout, stderr: result.stderr, exit_code: result.exitCode };
    }
    if (call.tool === 'glob' || call.tool === 'grep') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ?? '.';
      const body =
        call.tool === 'glob'
          ? `cd '${escapeSingleQuotes(cwd)}' && compgen -G '${escapeSingleQuotes(args.pattern)}' || true`
          : `cd '${escapeSingleQuotes(cwd)}' && grep -rn '${escapeSingleQuotes(args.pattern)}' . || true`;
      const result = await this.sb.commands.run(buildBubblewrapCommand(body, policy));
      return call.tool === 'glob'
        ? { output: result.stdout.split('\n').filter((s) => s.length > 0) }
        : { output: result.stdout };
    }
    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
  }

  async canonicalizePathForPolicy(path: string): Promise<string> {
    const result = await this.sb.commands.run(`realpath -m -- '${escapeSingleQuotes(path)}'`);
    if (result.exitCode !== 0) throw new Error(`realpath failed: ${result.stderr}`);
    return result.stdout.trim();
  }

  /**
   * Runs `cmd` under `sudo` inside the E2B sandbox. Memory/output mounts invoke
   * the image-baked `orca-s3fs-mount` helper, which consumes short-lived ORCA_*
   * transport envs, starts s3fs under a scrubbed environment, and removes its
   * temporary AWS profile after daemonization.
   *
   * sudo's default `env_reset` would strip every env var injected via the
   * SDK before the wrapped binary (e.g. s3fs) ever sees them. We add
   * `--preserve-env=` for the three sudoers-approved ORCA_S3_* names only.
   * PATH and shell startup-hook overrides protect the non-root command shell
   * but are deliberately not preserved across sudo. The list form is
   * preferred over `-E` so unrelated harness env never enters the root helper.
   *
   * Defensive: a leading `sudo ` is stripped so we never end up running
   * `sudo sudo …` if a caller pre-prefixes.
   */
  async runPrivileged(cmd: string, opts?: { envs?: Record<string, string> }): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const envs = opts?.envs;
    const envNames = envs ? Object.keys(envs) : [];
    const runOpts = envs ? { envs } : undefined;
    const result = await this.sb.commands.run(buildE2BPrivilegedCommand(cmd, envNames), runOpts);
    return {
      stdout: result.stdout,
      stderr: result.stderr,
      exit_code: result.exitCode,
    };
  }

  async pause(): Promise<void> {
    if (this.sb.pause) await this.sb.pause();
  }

  async resume(): Promise<void> {
    if (this.sb.resume) await this.sb.resume();
  }

  async destroy(): Promise<void> {
    if (this.destroyed) return;
    this.destroyed = true;
    if (this.sb.kill) await this.sb.kill();
  }

  async endpoint(port: number): Promise<{ url: string }> {
    if (!this.sb.getHost) {
      throw new Error('E2B SDK sandbox does not expose getHost(port)');
    }
    return { url: buildE2BEndpointUrl(this.sb.getHost(port)) };
  }
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

function boundedDiagnostic(value: string, maxLength = 2_000): string {
  return value.length <= maxLength ? value : `${value.slice(0, maxLength)}…`;
}

export function buildE2BEndpointUrl(host: string): string {
  if (host.startsWith('http://') || host.startsWith('https://')) return host;
  return `https://${host}`;
}
