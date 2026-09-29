// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The runner's sandbox seam — where the session-runner links `@orca/sandbox-runtime`.
//
// The SandboxRuntime / SandboxHandle abstraction and the two reusable
// implementations (InMemory + Local) live in the shared `@orca/sandbox-runtime`
// package so BOTH the harness-server and this runner consume ONE definition of
// the sandbox boundary (the extraction's whole point). The harness-server links
// the package through its `src/sandbox/*` re-export shims; the runner links it
// HERE — this module is the runner's canonical import point for the sandbox
// types and the place it constructs a concrete runtime at boot.
//
// Why the runner needs a sandbox at all: the runner-role glue drives an
// {@link AgentHarness} per turn. The in-process claude/mock providers run the
// model in-process and never touch a sandbox, but the native-CLI providers
// (Claude Code / Codex / Cursor / Pi / custom — see
// `docs/managed-agents/session-runner-scope.md`, Native-CLI providers) launch a
// real CLI as a long-lived child and stream its stdio. That
// launch is exactly {@link SandboxHandle.spawn} — the streaming spawn primitive
// this package exposes. The runner constructs a runtime here and advertises it to
// providers via the provider context, so a native-CLI provider acquires a sandbox
// and calls `spawn` through the SAME interface the harness-server uses. The
// primitive is `spawn?` (optional): cloud-only runtimes can omit it, so a caller
// MUST feature-detect (`if (handle.spawn) …`) — both reusable runtimes implement it.
//
// Runtime selection: the runner can only construct a runtime with NO external
// launch dependency out of the box, so the default is {@link InMemorySandboxRuntime}
// (a tmpdir + `child_process.spawn`; `supportsFuse=false`). `SANDBOX_RUNTIME=local`
// selects {@link LocalSandboxRuntime} (the `srt`-wrapped host sandbox) — mirroring
// the harness-server's env var. The runner now WIRES that path itself via
// {@link resolveRunnerLocalSandbox} (`main.ts` calls it and hands the result to
// {@link createRunnerSandboxRuntime}): it probes the `srt` binary, then builds the
// concrete `SandboxManager` + work-dir + network allow-list the local runtime needs.
// Because a self-hosted runner must not crash on an operator's box that simply hasn't
// installed `srt` yet, resolution is BEST-EFFORT: when the probe fails it returns
// `undefined` (with a visible warning), and the seam degrades a `local` request to
// InMemory rather than boot-failing — so `SANDBOX_RUNTIME=local` yields a real
// srt-wrapped Local runtime when the host supports it, and an explained InMemory
// fallback (never a silent one, never a crash) when it does not. That is the deliberate
// difference from the harness-server, which hard-fails at boot on a missing `srt`.

import { execFileSync } from 'node:child_process';
import {
  InMemorySandboxRuntime,
  LocalSandboxRuntime,
  SandboxManager,
  createManagedToolSandboxManager,
  buildSandboxWritePolicy,
  type LocalSandboxRuntimeOptions,
  type SandboxRuntime,
} from '@orca/sandbox-runtime';

// Re-export the sandbox boundary types from the shared package so runner code
// (providers, the loop, tests) imports them from ONE runner-local path — the
// same pattern the harness-server's `sandbox/sandbox-runtime.ts` shim uses.
export type {
  EnvironmentSpec,
  ReadPage,
  ReadPageInput,
  SandboxCapabilities,
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SandboxRuntime,
  SpawnHandle,
  ToolCall,
  ToolResult,
  WritePolicyCapableHandle,
  ValidatedSandboxWritePolicy,
  ResourceManifest,
  GitProxyCapability,
  ResourceMountDescriptor,
  ResourceFileDescriptor,
  ResourcePush,
  ResourceCheckpoint,
  ChangedResourceFile,
  DeletedResourceFile,
} from '@orca/sandbox-runtime';

export {
  hasWritePolicyEnforcement,
  resolveOpenedDescriptorPath,
  buildSandboxWritePolicy,
  createPolicyEnforcedSandbox,
  parseResourceManifest,
  resourceManifestDigest,
  resourceFilePath,
  decodeResourceChunk,
  RESOURCE_CHUNK_BYTES,
  RESOURCE_MAX_FILE_BYTES,
  RESOURCE_MAX_TOTAL_BYTES,
  RESOURCE_MAX_FILES,
  RESOURCE_MAX_MOUNTS,
  RESOURCE_CHECKPOINT_EVENT,
  OUTPUT_RESOURCE_ID,
  GIT_PROXY_AUTH_ROOT,
  parseResourceCheckpoint,
  resourceCheckpointDigest,
} from '@orca/sandbox-runtime';

/** A dedicated model-tool runtime; the trusted native SDK worker uses a separate handle. */
export function createManagedToolSandboxRuntime(opts: {
  harnessWorkDir: string;
  networkAllowedDomains: readonly string[];
  networkUnrestricted?: boolean;
}): SandboxRuntime {
  if (process.platform !== 'linux') {
    throw new Error('managed model tools require Linux SRT and bubblewrap; fallback is disabled');
  }
  const policy = buildSandboxWritePolicy([], { networkAllowedDomains: opts.networkAllowedDomains });
  // These diagnostics are preliminary. prepareWritePolicy also executes the
  // actual nested SRT/managed-filesystem chain before a handle reaches tools.
  execFileSync('srt', ['--version'], { stdio: 'pipe', timeout: 10_000 });
  execFileSync('bwrap', ['--version'], { stdio: 'pipe', timeout: 10_000 });
  return new LocalSandboxRuntime({
    harnessWorkDir: opts.harnessWorkDir,
    allowedNetworkHosts: [...(policy.networkAllowedDomains ?? [])],
    managedToolFilesystem: true,
    ...(opts.networkUnrestricted === true ? { networkUnrestricted: true } : {}),
    manager: createManagedToolSandboxManager(),
  });
}

/**
 * The env var that selects the runner's sandbox runtime. Mirrors the
 * harness-server's `SANDBOX_RUNTIME` so an operator uses one name across both
 * processes. Unset / `in-memory` → {@link InMemorySandboxRuntime}; `local` →
 * {@link LocalSandboxRuntime} (only when a manager is supplied — see
 * {@link createRunnerSandboxRuntime}).
 */
export const SANDBOX_RUNTIME_ENV_VAR = 'SANDBOX_RUNTIME';

/** Options for {@link createRunnerSandboxRuntime}. */
export interface CreateRunnerSandboxRuntimeOptions {
  /**
   * Raw `SANDBOX_RUNTIME` value (defaults to the process env). Case-insensitive;
   * unset / blank / `in-memory` → InMemory, `local` → Local (when wired).
   */
  kind?: string | undefined;
  /**
   * The concrete Local-runtime wiring (a `SandboxManager` + a work-dir), normally
   * produced by {@link resolveRunnerLocalSandbox} and passed by `main.ts`. Only
   * consulted when `kind === 'local'`. Absent → a `local` request falls back to
   * InMemory — either the caller did not attempt to resolve it, or resolution failed
   * (e.g. the `srt` binary is not installed on this host, in which case
   * {@link resolveRunnerLocalSandbox} already logged the reason). Keeping the fallback
   * here means the runner never boot-fails over a `local` request it cannot honor.
   */
  local?: LocalSandboxRuntimeOptions | undefined;
}

/**
 * Construct the runner's {@link SandboxRuntime} from `@orca/sandbox-runtime`.
 *
 * This is the live link between the session-runner and the shared package: the
 * runner CONSTRUCTS a concrete runtime here (not merely a type import) and holds
 * it for the session, so a native-CLI provider can `acquire` a sandbox and drive
 * {@link SandboxHandle.spawn}.
 *
 * Defaults to {@link InMemorySandboxRuntime} — the only runtime the runner can
 * stand up with no external dependency (it just `mkdtemp`s on `acquire`, touching
 * nothing at construction). `SANDBOX_RUNTIME=local` yields a
 * {@link LocalSandboxRuntime} ONLY when `opts.local` supplies the `SandboxManager`
 * + work-dir it needs; without that wiring a `local` request falls back to
 * InMemory (a `local` request this host cannot honor must not fail the whole
 * runner boot).
 */
export function createRunnerSandboxRuntime(
  opts: CreateRunnerSandboxRuntimeOptions = {},
): SandboxRuntime {
  const kind = (opts.kind ?? process.env[SANDBOX_RUNTIME_ENV_VAR] ?? '').trim().toLowerCase();
  if (kind === 'local' && opts.local !== undefined) {
    return new LocalSandboxRuntime(opts.local);
  }
  return new InMemorySandboxRuntime();
}

/**
 * The `AI_GATEWAY_URL` env var — the ai-gateway base URL. Its host is added to the
 * local sandbox's network allow-list (the runner reaches MCP/LLM egress through it).
 * Mirrors the harness-server's `collectLocalSandboxAllowedHosts`.
 */
export const AI_GATEWAY_URL_ENV_VAR = 'AI_GATEWAY_URL';

/**
 * The `S3_ENDPOINT` env var — the object-store base URL. Its host is added to the
 * local sandbox's network allow-list (file/memory-store egress). Mirrors the
 * harness-server's `collectLocalSandboxAllowedHosts`.
 */
export const S3_ENDPOINT_ENV_VAR = 'S3_ENDPOINT';

/** Explicit provider API hosts for the trusted SDK/CLI worker. */
const PROVIDER_API_HOSTS = ['api.anthropic.com', 'api.openai.com'];

/** A minimal warn seam (a subset of the runner's structured logger). */
export interface SandboxResolveLogger {
  warn?(obj: unknown, msg?: string): void;
  info?(obj: unknown, msg?: string): void;
}

/** Options for {@link resolveRunnerLocalSandbox}. */
export interface ResolveRunnerLocalSandboxOptions {
  /**
   * The runner's host work-dir root — each session lives under
   * `{harnessWorkDir}/sessions/{id}` and that path is exposed read+write inside the
   * sandbox. In production this is the runner's `config.workspace`.
   */
  harnessWorkDir: string;
  /** Env source for the network allow-list hosts (defaults to `process.env`). */
  env?: NodeJS.ProcessEnv;
  /**
   * The `srt --version` probe (injectable for tests). Defaults to a real
   * `execFileSync('srt', ['--version'])`; must THROW when `srt` is unreachable.
   */
  probeSrt?: () => void;
  /** Warn/info sink (defaults to no logging). */
  logger?: SandboxResolveLogger;
}

/**
 * Resolve the concrete {@link LocalSandboxRuntimeOptions} the runner's Local runtime
 * needs, or `undefined` when this host cannot support it.
 *
 * This is the runner's own `srt` wiring — the piece that was previously missing, so a
 * `SANDBOX_RUNTIME=local` runner silently ran InMemory. It mirrors the harness-server's
 * `buildLocalSandboxRuntime`: probe the `srt` binary, build the network allow-list
 * (`api.anthropic.com` / `api.openai.com` + the hosts of `AI_GATEWAY_URL` / `S3_ENDPOINT`), and pass the
 * real {@link SandboxManager} + the runner work-dir.
 *
 * The one deliberate difference from the harness-server: a self-hosted runner runs on
 * an operator's box that may not have `srt` installed, and it must NOT crash over that.
 * So when the probe fails this returns `undefined` (logging the reason) instead of
 * throwing — {@link createRunnerSandboxRuntime} then degrades the `local` request to
 * InMemory. The result: `SANDBOX_RUNTIME=local` yields a real srt-wrapped Local runtime
 * where the host supports it, and an EXPLAINED InMemory fallback (never silent, never a
 * boot crash) where it does not.
 *
 * @returns The Local wiring to hand to {@link createRunnerSandboxRuntime} as `local`,
 *   or `undefined` when `srt` is unreachable (caller then gets InMemory).
 */
export function resolveRunnerLocalSandbox(
  opts: ResolveRunnerLocalSandboxOptions,
): LocalSandboxRuntimeOptions | undefined {
  const env = opts.env ?? process.env;
  const probe = opts.probeSrt ?? (() => execFileSync('srt', ['--version'], { stdio: 'pipe' }));
  try {
    probe();
  } catch (err) {
    // Best-effort: a self-hosted runner without `srt` degrades to InMemory rather than
    // boot-failing. The warning makes the fallback explicit (never silent).
    opts.logger?.warn?.(
      { err: (err as Error).message },
      `${SANDBOX_RUNTIME_ENV_VAR}=local requested but the 'srt' binary is not reachable ` +
        `(npm install -g @anthropic-ai/sandbox-runtime); falling back to the in-memory sandbox runtime`,
    );
    return undefined;
  }
  return {
    harnessWorkDir: opts.harnessWorkDir,
    allowedNetworkHosts: collectRunnerLocalSandboxAllowedHosts(env),
    manager: SandboxManager,
  };
}

/**
 * Build the local sandbox network allow-list: Anthropic and OpenAI API hosts plus the host
 * portions of `AI_GATEWAY_URL` (ai-gateway) and `S3_ENDPOINT` (object storage). The
 * locked design decision (mirrored from the harness-server) is "no wildcards" — each
 * origin is listed explicitly. A URL that fails to parse is dropped (the runner tolerates
 * placeholder values in dev).
 */
function collectRunnerLocalSandboxAllowedHosts(env: NodeJS.ProcessEnv): string[] {
  const hosts = new Set<string>(PROVIDER_API_HOSTS);
  const candidates: Array<string | undefined> = [
    env[AI_GATEWAY_URL_ENV_VAR],
    env[S3_ENDPOINT_ENV_VAR],
  ];
  for (const url of candidates) {
    if (url === undefined || url.trim().length === 0) {
      continue;
    }
    try {
      const parsed = new URL(url);
      if (parsed.hostname) {
        hosts.add(parsed.hostname);
      }
    } catch {
      // A malformed URL is ignored — the allow-list stays conservative.
    }
  }
  return [...hosts];
}

export {
  composeSkillsCatalog,
  isPinnedSkillsCatalog,
  assertSafeSkillName,
  isSafeSkillName,
  validateSkillBundlePath,
  validateSkillDescriptor,
  uniqueSkillMaterializations,
  validateSkillBundle,
  planSkillBundleChmod,
  MAX_SKILL_BINDINGS,
} from '@orca/sandbox-runtime';
export type { MaterializableSkillDescriptor, MaterializableBundle } from '@orca/sandbox-runtime';
export type { SandboxFileMode } from '@orca/sandbox-runtime';
