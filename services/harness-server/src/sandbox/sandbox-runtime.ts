// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * SandboxRuntime is the abstraction over the per-session execution sandbox.
 * Implementations include private-workdir local runtimes and remote E2B,
 * OpenSandbox, and AgentENV providers.
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
}

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

export interface SandboxHandle {
  /** Stable id; surfaces in audit + logs. */
  id: string;
  /** Run a tool call (bash, glob, grep, …). */
  run(call: ToolCall): Promise<ToolResult>;
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
   */
  prepareWritePolicy?(policy: SandboxWritePolicy): Promise<void>;
  /** Execute an agent tool call through the installed filesystem boundary. */
  runWithWritePolicy?(call: ToolCall, policy: SandboxWritePolicy): Promise<ToolResult>;
  /** Resolve symlinks and missing suffixes to a canonical sandbox-visible path. */
  canonicalizePathForPolicy?(path: string): Promise<string>;
  /** Filesystem operations — used by mount strategies + agent_toolset's read/write/edit/list/delete. */
  files: SandboxFiles;
  /**
   * Run a command with elevated privilege (sudo). Used by MemoryFuseStrategy
   * + output-mount to issue mount syscalls. InMemorySandboxRuntime and
   * LocalSandboxRuntime throw; AgentENV (`capabilities.supportsFuse=false`)
   * runs the command through envd without the `sudo` prefix.
   *
   * Callers SHOULD pass the bare command (e.g. `mkdir -p /mnt/foo`); the
   * runtime layer prefixes `sudo`. Defensive: a leading `sudo ` is stripped
   * to avoid `sudo sudo …` if a caller pre-prefixes.
   */
  runPrivileged(cmd: string, opts?: { envs?: Record<string, string> }): Promise<ToolResult>;
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
 * Capabilities advertised by a SandboxRuntime instance. The strategy factory
 * reads these flags to pick `memory_fuse` vs. `local_memory` for memory_store
 * resources (file resources always use `tarball_prefetch`), and the
 * output-mount path consults `supportsFuse` to decide between an S3 FUSE
 * mount and a sandbox-local directory.
 */
export interface SandboxCapabilities {
  /**
   * Whether the runtime can mount FUSE filesystems inside the sandbox.
   * E2B with `orca-default` advertises true. OpenSandbox advertises true only
   * when its fail-closed image/device probe is enabled. AgentENV, InMemory, and
   * Local advertise false and opt into the Files API/local-memory capability.
   */
  supportsFuse: boolean;
  /** Whether the runtime provides the non-FUSE Files API memory fallback. */
  supportsLocalMemory?: boolean;
  /** Whether this adapter can enforce agent writes for the `separate` topology. */
  supportsWritePolicy?: boolean;
}

/**
 * Environment spec — minimal v1. Mirrors registry's `environments` shape so
 * harness can pass through `agent.environment_id` resolution. The dispatcher
 * rejects Environment packages for every managed sandbox
 * (`environment-trust.ts`); OpenSandbox and AgentENV also reject them at
 * acquire, and E2B does not consume them.
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
   * inspects `capabilities.supportsFuse` to pick `memory_fuse` vs.
   * `local_memory` per memory_store resource. Implemented as a property so
   * every runtime instance can be queried.
   */
  readonly capabilities: SandboxCapabilities;
  /**
   * Acquire a sandbox. The returned handle is owned by the caller and must be
   * `destroy()`ed when the session ends.
   */
  acquire(env: EnvironmentSpec): Promise<SandboxHandle>;
}
