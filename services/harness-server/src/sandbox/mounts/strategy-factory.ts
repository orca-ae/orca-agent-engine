// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileStore } from '@orca/file-store';
import type { SessionS3Creds } from '../../auth/sts-creds.js';
import type { GitWorker } from '../../git/git-worker.js';
import type { WorkDirManager } from '../../git/work-dir.js';
import type { SandboxRuntime } from '../sandbox-runtime.js';
import { GitCloneStrategy } from './git-clone.js';
import { LocalMemoryStrategy } from './local-memory.js';
import { MemoryFuseStrategy } from './memory-fuse.js';
import type { MountResource, MountStrategy, MountStrategyName } from './mount-strategy.js';
import { TarballPrefetchStrategy } from './tarball-prefetch.js';

/**
 * Inputs for {@link pickStrategy}. Encapsulates everything the dispatcher
 * needs to resolve a strategy per resource: `tarball_prefetch` (the only
 * file path — sandbox execution credentials never cover the workspace
 * file-blob namespace, so file bytes are always copied host-side),
 * `local_memory`/`memory_fuse` (memory_store resources), and `git_clone`
 * (github_repository resources) — the per-resource override, the runtime's
 * advertised capabilities, and the wiring needed to build each strategy.
 *
 * The dispatcher builds one input per resource: file resources go through a
 * `chooseStrategy` closure it hands to `materializeResources`; memory_store
 * and github_repository resources call {@link pickStrategy} directly.
 */
export interface PickStrategyInput {
  resource: MountResource;
  /**
   * From `session_resources.mount_strategy` (registry contract). When `null`,
   * the factory falls back to runtime capability detection. When set, the
   * factory honors it strictly — including throwing if a FUSE-based strategy
   * was requested but the runtime can't satisfy it.
   *
   * For `file` resources: `tarball_prefetch` | `null`.
   * For `memory_store` resources: `local_memory` | `memory_fuse` | `null`.
   * Mismatches between resource type + override are validated per-branch
   * inside the factory.
   */
  overrideStrategy: MountStrategyName | null;
  runtime: SandboxRuntime;

  // ---- memory_fuse wiring ----
  workspaceId: string;
  sessionId: string;
  /** Per-session STS-scoped creds; required for any FUSE mount. */
  creds?: SessionS3Creds;
  bucket?: string;
  endpoint?: string;
  /** Defaults to true for MinIO compatibility. Set false for virtual-hosted addressing. */
  forcePathStyle?: boolean;
  /**
   * e.g. `memory/` — same prefix passed to `S3MemoryBlobStore`. Used by
   * `memory_fuse`. When omitted, the factory does not auto-build
   * `MemoryFuseStrategy`. It may use `LocalMemoryStrategy` only when the
   * runtime explicitly advertises the test/dev fallback; production runtimes
   * fail closed instead of silently weakening persistence semantics.
   */
  memoryKeyPrefix?: string;
  /**
   * Bucket region (e.g. `us-east-2`). Forwarded as the s3fs `endpoint=` flag
   * so SigV4 signing uses the correct region. Without this, s3fs defaults to
   * `us-east-1` and AWS rejects the request with `AuthorizationHeaderMalformed`
   * when the bucket actually lives elsewhere.
   */
  region?: string;

  // ---- tarball_prefetch wiring (required only for file resources) ----
  /** Memory/repository strategies do not need it. */
  fileStore?: FileStore;

  // ---- git_clone wiring (only required when resource.type='github_repository') ----
  /**
   * Host-side simple-git wrapper. Required for `github_repository` resources.
   * Tests inject a fake `GitWorker` so they can run against a `file://` remote
   * without taking a network round-trip.
   */
  gitWorker?: GitWorker;
  /**
   * Owns the per-session ephemeral host work dir (rm -rf'd at session stop).
   * Required for `github_repository` resources.
   */
  workDir?: WorkDirManager;
  /**
   * Resolves a git credential id to its bound PAT bytes. The PAT is only ever
   * used for the duration of a single clone; never cached, logged, or shipped
   * into the sandbox env.
   */
  resolvePat?: (gitCredentialId: string) => Promise<string>;
}

/**
 * Resolve the mount strategy for a single resource.
 *
 * For `file` resources there is exactly one strategy: `tarball_prefetch`.
 * Sandbox execution credentials never include the workspace file-blob
 * namespace (multi-workspace isolation), so file bytes are always copied
 * through the trusted host-side FileStore. Both `overrideStrategy=null`
 * (auto) and `overrideStrategy='tarball_prefetch'` resolve to it; any other
 * override throws.
 *
 * For `memory_store` resources:
 *   1. `overrideStrategy='memory_fuse'` against a `supportsFuse=false` runtime → throw.
 *   2. `overrideStrategy='local_memory'` requires explicit runtime support.
 *   3. `overrideStrategy=null` (auto): pick `memory_fuse` when the runtime
 *      advertises FUSE support AND the memory_fuse wiring is present; otherwise
 *      select `local_memory` only when the runtime explicitly supports it.
 *
 * Mismatched overrides (e.g. `mount_strategy='memory_fuse'` on a `file`
 * resource) throw — the registry's zod schema should already reject these,
 * but the factory is defensive at the TS boundary.
 */
export function pickStrategy(input: PickStrategyInput): MountStrategy {
  const { resource } = input;

  if (resource.type === 'file') {
    return pickFileStrategy(input);
  }

  if (resource.type === 'memory_store') {
    return pickMemoryStrategy(input);
  }

  if (resource.type === 'github_repository') {
    return pickGitStrategy(input);
  }

  throw new Error(`pickStrategy: unsupported resource type ${(resource as { type: string }).type}`);
}

/**
 * `github_repository` always uses {@link GitCloneStrategy} — there's
 * no FUSE alternative because the harness clones host-side and streams the
 * working tree (plus `.git/`) into the sandbox. No `mount_strategy` override
 * is honored; the registry's zod schema rejects them up-front, but we defend
 * at the TS boundary against a future contract drift.
 */
function pickGitStrategy(input: PickStrategyInput): MountStrategy {
  if (
    input.overrideStrategy !== null &&
    input.overrideStrategy !== undefined &&
    input.overrideStrategy !== 'git_clone'
  ) {
    throw new Error(
      `pickStrategy: mount_strategy=${input.overrideStrategy} is invalid for resource.type=github_repository`,
    );
  }
  if (!input.gitWorker || !input.workDir || !input.resolvePat) {
    throw new Error(
      'pickStrategy: github_repository resource requires gitWorker + workDir + resolvePat',
    );
  }
  return new GitCloneStrategy({
    workspaceId: input.workspaceId,
    sessionId: input.sessionId,
    worker: input.gitWorker,
    workDir: input.workDir,
    resolvePat: input.resolvePat,
  });
}

function pickFileStrategy(input: PickStrategyInput): MountStrategy {
  const override = input.overrideStrategy;

  if (override !== null && override !== undefined && override !== 'tarball_prefetch') {
    throw new Error(
      `pickStrategy: mount_strategy=${override} is invalid for resource.type=file (use tarball_prefetch)`,
    );
  }

  // tarball_prefetch works on every runtime — no capability check needed.
  return new TarballPrefetchStrategy(input.workspaceId, requireFileStore(input));
}

function pickMemoryStrategy(input: PickStrategyInput): MountStrategy {
  const supportsFuse = input.runtime.capabilities.supportsFuse;
  const override = input.overrideStrategy;

  if (override === 'tarball_prefetch' || override === 'git_clone') {
    throw new Error(
      `pickStrategy: mount_strategy=${override} is invalid for resource.type=memory_store (use local_memory or memory_fuse)`,
    );
  }

  if (override === 'memory_fuse') {
    if (!supportsFuse) {
      const runtimeName = input.runtime.constructor.name || 'SandboxRuntime';
      throw new Error(
        `mount_strategy=memory_fuse requested but runtime ${runtimeName} has supportsFuse=false`,
      );
    }
    return buildMemoryFuseStrategy(input);
  }

  if (override === 'local_memory') {
    return buildLocalMemoryStrategy(input);
  }

  // Auto-pick.
  if (supportsFuse) {
    if (hasMemoryFuseWiring(input)) {
      return buildMemoryFuseStrategy(input);
    }
    if (!input.runtime.capabilities.supportsLocalMemory) {
      throw new Error(
        'pickStrategy: supportsFuse=true but memory_fuse wiring is incomplete and runtime does not support local_memory',
      );
    }
    console.warn(
      'pickStrategy: supportsFuse=true but memory_fuse not configured; using local_memory',
    );
  }
  return buildLocalMemoryStrategy(input);
}

function buildLocalMemoryStrategy(input: PickStrategyInput): LocalMemoryStrategy {
  if (!input.runtime.capabilities.supportsLocalMemory) {
    const runtimeName = input.runtime.constructor.name || 'SandboxRuntime';
    throw new Error(
      `mount_strategy=local_memory requested but runtime ${runtimeName} has supportsLocalMemory=false`,
    );
  }
  return new LocalMemoryStrategy();
}

function requireFileStore(input: PickStrategyInput): FileStore {
  if (!input.fileStore) {
    throw new Error('pickStrategy: file resource requires fileStore');
  }
  return input.fileStore;
}

function hasMemoryFuseWiring(input: PickStrategyInput): input is PickStrategyInput & {
  creds: SessionS3Creds;
  bucket: string;
  endpoint: string;
  memoryKeyPrefix: string;
} {
  return (
    input.creds !== undefined &&
    input.bucket !== undefined &&
    input.endpoint !== undefined &&
    input.memoryKeyPrefix !== undefined
  );
}

function buildMemoryFuseStrategy(input: PickStrategyInput): MemoryFuseStrategy {
  if (!hasMemoryFuseWiring(input)) {
    throw new Error(
      'pickStrategy: memory_fuse requested but creds/bucket/endpoint/memoryKeyPrefix wiring is incomplete',
    );
  }
  const cfg: ConstructorParameters<typeof MemoryFuseStrategy>[0] = {
    workspaceId: input.workspaceId,
    sessionId: input.sessionId,
    bucket: input.bucket,
    endpoint: input.endpoint,
    keyPrefix: input.memoryKeyPrefix,
    creds: input.creds,
  };
  if (input.region !== undefined) cfg.region = input.region;
  if (input.forcePathStyle !== undefined) cfg.forcePathStyle = input.forcePathStyle;
  return new MemoryFuseStrategy(cfg);
}
