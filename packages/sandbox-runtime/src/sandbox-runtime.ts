// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * SandboxRuntime is the abstraction over the per-session execution sandbox.
 * Reusable implementations ship in this package:
 *   - InMemorySandboxRuntime — tmpdir-backed; tests + dev.
 *   - LocalSandboxRuntime — `srt`-wrapped host processes; local-stack.
 * Cloud-only runtimes (E2B, OpenSandbox) live in `@orca/cloud-sandbox` and
 * import this interface from here.
 *
 * Above this boundary, harness code is sandbox-agnostic. The MountStrategy
 * + agent_toolset layer composes against `SandboxHandle`, not the runtime
 * impl.
 */

import type { ReadPage, ReadPageInput } from './read-page.js';

export interface ToolCall {
  tool: string;
  args: unknown;
}

export interface ToolResult {
  stdout?: string;
  stderr?: string;
  exit_code?: number;
  output?: unknown;
}

/** Validated arguments of a `bash` tool call. */
export interface BashToolArgs {
  command: string;
  timeout_ms?: number;
}

/** Validated arguments shared by the `glob` and `grep` tool calls. */
export interface SearchToolArgs {
  pattern: string;
  root?: string;
}

/**
 * Outcome of {@link parseToolCall}: the known-tool arms carry validated args,
 * `other` is the adapters' exit-127 fallback, `invalid` is a known tool whose
 * args failed validation (adapters surface it as an exit-2 ToolResult).
 */
export type ParsedToolCall =
  | { tool: 'bash'; args: BashToolArgs }
  | { tool: 'glob'; args: SearchToolArgs }
  | { tool: 'grep'; args: SearchToolArgs }
  | { tool: 'other'; name: string }
  | { tool: 'invalid'; error: string };

/**
 * Validate a {@link ToolCall}'s `args` shape ONCE, for every runtime adapter.
 *
 * `ToolCall.args` is `unknown` by design (the tool set is open), but each of
 * the four handles used to re-narrow it with an unchecked `as` cast — so a
 * caller passing `{tool: 'bash', args: {cmd: …}}` (wrong key) compiled clean
 * and shipped `undefined` as the command to a cloud exec API, and the known
 * arg shapes were duplicated as casts that could drift independently. All
 * adapters route through this parser and keep their exit-127 fallback for
 * `other`.
 */
export function parseToolCall(call: ToolCall): ParsedToolCall {
  const args =
    typeof call.args === 'object' && call.args !== null
      ? (call.args as Record<string, unknown>)
      : {};
  switch (call.tool) {
    case 'bash': {
      const command = args['command'];
      if (typeof command !== 'string') {
        return { tool: 'invalid', error: "bash tool call requires a string 'command'" };
      }
      const timeout = args['timeout_ms'];
      // Positive-finite only: NaN/Infinity/negative would pass a bare typeof
      // check and degrade into undefined transport behavior downstream (E2B
      // forwards it to the SDK; OpenSandbox derives a request timeout from it).
      if (
        timeout !== undefined &&
        (typeof timeout !== 'number' || !Number.isFinite(timeout) || timeout <= 0)
      ) {
        return {
          tool: 'invalid',
          error: "bash tool call 'timeout_ms' must be a positive finite number",
        };
      }
      return {
        tool: 'bash',
        args: { command, ...(timeout !== undefined ? { timeout_ms: timeout } : {}) },
      };
    }
    case 'glob':
    case 'grep': {
      const pattern = args['pattern'];
      if (typeof pattern !== 'string') {
        return { tool: 'invalid', error: `${call.tool} tool call requires a string 'pattern'` };
      }
      const root = args['root'];
      if (root !== undefined && typeof root !== 'string') {
        return { tool: 'invalid', error: `${call.tool} tool call 'root' must be a string` };
      }
      return { tool: call.tool, args: { pattern, ...(root !== undefined ? { root } : {}) } };
    }
    default:
      return { tool: 'other', name: call.tool };
  }
}

export type WritablePathKind = 'session_output' | 'memory_store' | 'github_repository';

export interface WritablePath {
  path: string;
  kind: WritablePathKind;
}

/** Immutable agent-facing filesystem write allowlist for one session runner. */
export interface SandboxWritePolicy {
  writablePaths: readonly WritablePath[];
  readonlyPaths: readonly string[];
  /** Exact hosts Bash may reach from the nested Claude SDK network sandbox. */
  networkAllowedDomains?: readonly string[];
  /** Explicit unrestricted egress for managed local tools; filesystem isolation still applies. */
  networkUnrestricted?: boolean;
}

declare const sandboxWritePolicyValidated: unique symbol;

/**
 * A {@link SandboxWritePolicy} that provably passed the builder's invariant
 * checks — canonical absolute roots, no writable/writable or writable/readonly
 * overlap, kind-unique writable paths, frozen.
 *
 * The brand is type-level only (the symbol has no runtime value) and is applied
 * solely by `buildSandboxWritePolicy` / `validateSandboxWritePolicy` in
 * `write-policy.ts`. Every enforcement surface (`prepareWritePolicy`,
 * `runWithWritePolicy`, `buildBubblewrapCommand`, `createPolicyEnforcedSandbox`,
 * the containment lookups) requires this type, so a hand-rolled literal — whose
 * non-canonical or overlapping roots would silently break containment matching
 * and bind unvetted paths into the bubblewrap namespace — cannot reach them
 * without going through validation first.
 */
export type ValidatedSandboxWritePolicy = SandboxWritePolicy & {
  readonly [sandboxWritePolicyValidated]: true;
};

/**
 * Trusted roots for one atomic agent-facing read. Raw runtime adapters require
 * this constraint; the policy wrapper injects it and ignores caller input.
 */
export interface SandboxReadConstraint {
  readableRoots: readonly string[];
}

export interface SandboxFileMode {
  path: string;
  mode: number;
}

export interface SandboxFiles {
  /** Write `content` to `path`. Creates parent dirs as needed. Overwrites. */
  write(path: string, content: Buffer | NodeJS.ReadableStream): Promise<void>;
  read(path: string): Promise<Buffer>;
  /**
   * Open, authorize, and pread one bounded UTF-8 page from the same descriptor.
   * Raw adapters fail closed when `constraint` is absent.
   */
  readUtf8Page(
    path: string,
    input: ReadPageInput,
    constraint?: SandboxReadConstraint,
  ): Promise<ReadPage>;
  /** List entries (one level deep). Returns relative names, not full paths. */
  list(path: string): Promise<string[]>;
  /** Apply POSIX permission bits to an existing file or directory. */
  chmod(path: string, mode: number): Promise<void>;
  /**
   * Apply many POSIX modes without one remote command round trip per path.
   * Runtime adapters may omit this when chmod is already an in-process call.
   */
  chmodMany?(root: string, entries: readonly SandboxFileMode[]): Promise<void>;
  /** Delete a file or tree. Must succeed when the target does not exist. */
  delete(path: string): Promise<void>;
}

/**
 * Long-lived process spawned inside the sandbox via {@link SandboxHandle.spawn}.
 * Unlike {@link SandboxHandle.run} (which buffers a command to completion),
 * this exposes the live stdio streams so callers can drive stdin and consume
 * streamed stdout incrementally, then terminate the process explicitly.
 */
export interface SpawnHandle {
  /** Streamed process stdout. */
  stdout: NodeJS.ReadableStream;
  /** Process stdin — write to feed the process. */
  stdin: NodeJS.WritableStream;
  /** Terminate the process. Defaults to `SIGTERM`. Idempotent. */
  kill(signal?: NodeJS.Signals): void;
  /**
   * Settles when the process exits: resolves with the exit code/signal, and
   * REJECTS when the process could not be spawned at all (e.g. a nonexistent
   * cwd). Without this channel a caller can only watch stdout end — it cannot
   * distinguish a clean exit from a process that died or never started.
   * Implementations pre-attach a no-op rejection observer, so a caller that
   * ignores this promise never turns a spawn failure into an unhandled
   * rejection.
   */
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
}

export interface SandboxHandle {
  /** Stable id; surfaces in audit + logs. */
  id: string;
  /** Run a tool call (bash, glob, grep, …). */
  run(call: ToolCall): Promise<ToolResult>;
  /**
   * Spawn a long-lived command inside the sandbox, returning live stdio
   * streams. Used by transports that need to keep a process attached (drive
   * stdin, read streamed stdout) rather than run-to-completion like
   * {@link run}. Honors the same sandbox boundary as `run`: InMemory spawns
   * on the host via `child_process.spawn`; Local wraps the command with the
   * `srt` sandbox profile first.
   *
   * Optional — like {@link endpoint}, not every runtime surfaces a live-stdio
   * spawn. The two reusable runtimes (InMemory, Local) implement it; cloud
   * runtimes that only expose a run-to-completion command API can omit it.
   * Callers MUST feature-detect (`if (handle.spawn) …`).
   */
  spawn?(cmd: string, opts?: { env?: Record<string, string>; cwd?: string }): Promise<SpawnHandle>;
  /**
   * Prove that planned resource/Skill roots are still plain, canonical paths
   * before any trusted materializer writes or mounts them. Remote image-backed
   * runtimes must reject symlink aliases and pre-existing mount points at or
   * below a root; private-workdir runtimes may implement this as a no-op.
   */
  prepareFilesystemRoots?(paths: readonly string[]): Promise<void>;
  /**
   * Probe/install the runtime's agent-subprocess write boundary. Present when
   * `capabilities.supportsWritePolicy=true`; failure must abort session setup.
   * Takes the branded policy so only builder-validated policies reach it.
   */
  prepareWritePolicy?(policy: ValidatedSandboxWritePolicy): Promise<void>;
  /** Execute an agent tool call through the installed filesystem boundary. */
  runWithWritePolicy?(call: ToolCall, policy: ValidatedSandboxWritePolicy): Promise<ToolResult>;
  /** Resolve symlinks and missing suffixes to a canonical sandbox-visible path. */
  canonicalizePathForPolicy?(path: string): Promise<string>;
  /** Filesystem operations — used by mount strategies + agent_toolset's read/write/edit/list/delete. */
  files: SandboxFiles;
  /**
   * Run a command with elevated privilege (sudo). Used by S3FuseStrategy
   * + output-mount to issue mount syscalls.
   *
   * Optional — like {@link spawn} and {@link endpoint}, absence is the
   * feature-detection signal: runtimes with no privilege boundary (InMemory,
   * Local) simply omit it rather than shipping an always-throwing stub that
   * the type system presents as callable. Cloud runtimes with a real sudo
   * boundary (E2B, OpenSandbox) implement it.
   *
   * Callers SHOULD pass the bare command (e.g. `mkdir -p /mnt/foo`); the
   * runtime layer prefixes `sudo`. Defensive: a leading `sudo ` is stripped
   * to avoid `sudo sudo …` if a caller pre-prefixes.
   */
  runPrivileged?(cmd: string, opts?: { envs?: Record<string, string> }): Promise<ToolResult>;
  /** Pause execution; the sandbox is preserved (FS, env). */
  pause(): Promise<void>;
  /** Resume after pause. */
  resume(): Promise<void>;
  /** Destroy the sandbox; release all resources. Idempotent. */
  destroy(): Promise<void>;
  /**
   * Resolve a reachable URL for a port exposed by the sandbox (colocated mode:
   * the harness HTTP server). Optional — only runtimes that boot a service +
   * expose a port implement it. Used by the in-sandbox bridge transport.
   */
  endpoint?(port: number): Promise<{ url: string; headers?: Record<string, string> }>;
}

/**
 * A {@link SandboxHandle} whose write-policy trio is statically present.
 *
 * `createPolicyEnforcedSandbox` accepts THIS type, so "the runtime supports
 * write-policy enforcement" is a compile-time obligation on the caller (narrow
 * via {@link hasWritePolicyEnforcement}) instead of a session-setup crash when
 * a handle advertised `supportsWritePolicy: true` but omitted a method.
 */
export interface WritePolicyCapableHandle extends SandboxHandle {
  prepareWritePolicy(policy: ValidatedSandboxWritePolicy): Promise<void>;
  runWithWritePolicy(call: ToolCall, policy: ValidatedSandboxWritePolicy): Promise<ToolResult>;
  canonicalizePathForPolicy(path: string): Promise<string>;
}

/** Narrowing guard for {@link WritePolicyCapableHandle} (checks the actual methods). */
export function hasWritePolicyEnforcement(
  handle: SandboxHandle,
): handle is WritePolicyCapableHandle {
  return (
    handle.prepareWritePolicy !== undefined &&
    handle.runWithWritePolicy !== undefined &&
    handle.canonicalizePathForPolicy !== undefined
  );
}

/**
 * Capabilities advertised by a SandboxRuntime instance. The strategy factory
 * reads these flags to pick `s3_fuse` vs. `tarball_prefetch` for file
 * resources, and the output-mount path consults `supportsFuse` to decide
 * between an S3 FUSE mount and a sandbox-local directory.
 */
export interface SandboxCapabilities {
  /**
   * Whether the runtime can mount FUSE filesystems inside the sandbox.
   * E2B with the `orca-default` custom template advertises true; the
   * in-memory test runtime advertises false (no privilege boundary,
   * no libfuse).
   */
  supportsFuse: boolean;
  /** Whether the runtime provides the test-only local memory mount fallback. */
  supportsLocalMemory?: boolean;
  /** Whether this adapter can enforce agent writes for the `separate` topology. */
  supportsWritePolicy?: boolean;
}

/**
 * Environment spec — minimal v1. Mirrors registry's `environments` shape so
 * harness can pass through `agent.environment_id` resolution. Package install
 * behavior is runtime-specific (OpenSandbox rejects it before creating an
 * isolated workload; E2B does not consume it yet).
 */
export const packageManagers = ['apt', 'cargo', 'gem', 'go', 'npm', 'pip'] as const;
export type PackageManager = (typeof packageManagers)[number];
export type Packages = Partial<Record<PackageManager, string[]>>;

export interface EnvironmentSpec {
  packages?: Packages;
  networking?: Record<string, unknown>;
  /** Container image to boot (colocated: the harness image). */
  image?: string;
  /** Container entrypoint (colocated: the harness server command). */
  entrypoint?: string[];
  /** Request outer-runtime FUSE privilege only when this acquisition mounts S3. */
  requiresFuse?: boolean;
  /** Ports the sandbox should expose (colocated: the harness HTTP port). */
  exposePorts?: number[];
  /** Extra env injected into the sandbox (e.g. gateway base URL + session token). */
  harnessEnv?: Record<string, string>;
  /**
   * Ownership applied by runtimes whose file-upload API supports it. In-sandbox
   * harness images use this so materialized resources are writable without a
   * recursive ownership repair on the startup path.
   */
  fileUploadOwnership?: { owner: string; group: string };
  /** Where the sandbox runs. */
  target?: 'cloud' | 'self_hosted';
}

export interface SandboxRuntime {
  /**
   * Static capability descriptor for this runtime. The strategy factory
   * inspects `capabilities.supportsFuse` to pick `s3_fuse` vs.
   * `tarball_prefetch` per resource. Implemented as a property so every
   * runtime instance can be queried.
   */
  readonly capabilities: SandboxCapabilities;
  /**
   * Acquire a sandbox. The returned handle is owned by the caller and must be
   * `destroy()`ed when the session ends.
   */
  acquire(env: EnvironmentSpec): Promise<SandboxHandle>;
}
