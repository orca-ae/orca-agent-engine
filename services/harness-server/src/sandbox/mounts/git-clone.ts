// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFile, readdir } from 'node:fs/promises';
import { join, relative, sep } from 'node:path';
import type { MountHandle, MountResource, MountStrategy, TornDownState } from './mount-strategy.js';
import type { SandboxHandle } from '../sandbox-runtime.js';
import type { GitWorker } from '../../git/git-worker.js';
import type { WorkDirManager } from '../../git/work-dir.js';
import { harnessGitCloneSeconds, harnessGitCloneTotal } from '../../metrics.js';

export interface GitCloneStrategyConfig {
  workspaceId: string;
  sessionId: string;
  worker: GitWorker;
  workDir: WorkDirManager;
  /**
   * Resolves the git credential secret → PAT bytes. Called once per repo at
   * activate time. The PAT lives in scope only for the duration of
   * `GitWorker.cloneInto` and is discarded immediately after — no caching,
   * no logging, no env propagation.
   */
  resolvePat: (gitCredentialId: string) => Promise<string>;
}

/**
 * v1 mount strategy for github_repository resources.
 *
 * Lifecycle:
 *   1. activate(): acquire host-side work dir → resolve PAT → `git clone
 *      --filter=blob:none --depth=1 [--branch <ref>]` into work dir → walk
 *      the work dir recursively (working tree + .git/) →
 *      `sandbox.files.write(mount_path/<rel>, bytes)` for each file. The
 *      sandbox FS layer creates parent dirs implicitly, so no explicit
 *      `mkdir -p mount_path` step is required. The .git/ dir IS shipped so
 *      in-sandbox `git status` and `git push` work without re-init.
 *   2. deactivate(): no-op. The sandbox is being torn down; the host-side
 *      work dir is cleaned up via WorkDirManager.releaseSession in the
 *      runner's stop path.
 *   3. teardownForSnapshot / restoreAfterSnapshot: no-ops, for the same
 *      reason as MemoryFuseStrategy's.
 *
 * The PAT NEVER reaches the sandbox: GitWorker.cloneInto embeds it in the
 * clone URL one-shot (visible only on the harness host's argv) and resets
 * the remote URL to the bare form before this strategy streams `.git/config`
 * into the sandbox. Per-call git operations from inside the sandbox (push,
 * fetch) flow through the `orca-git-creds` credential helper, which calls back
 * to the registry — the PAT is resolved per-call, never persisted.
 */
export class GitCloneStrategy implements MountStrategy {
  readonly name = 'git_clone' as const;
  readonly supports = ['github_repository'] as const;

  constructor(private readonly config: GitCloneStrategyConfig) {}

  async activate(sandbox: SandboxHandle, resource: MountResource): Promise<MountHandle> {
    if (resource.type !== 'github_repository') {
      throw new Error(`GitCloneStrategy: unsupported resource type ${resource.type}`);
    }

    // Capture wall-clock duration of the entire activate flow
    // (clone + walk + sandbox stream). The histogram observes for BOTH success
    // and failure so the error-path latency is graphable; the counter labels
    // with the result so dashboards can split ok/error volume.
    const start = Date.now();
    try {
      // 1. Acquire host-side work dir.
      const dest = await this.config.workDir.acquire(
        this.config.workspaceId,
        this.config.sessionId,
        resource.repoIdx,
      );

      // 2. Resolve the PAT just-in-time.
      const pat = await this.config.resolvePat(resource.gitCredentialId);

      // 3. Clone. v1 always uses --filter=blob:none for production github.com
      //    URLs; tests inject a fake worker that bypasses the filter logic.
      const cloneOpts: Parameters<GitWorker['cloneInto']>[0] = {
        url: resource.url,
        pat,
        dest,
      };
      if (resource.checkout?.type === 'branch') {
        cloneOpts.ref = resource.checkout.value;
      } else if (resource.checkout?.type === 'commit') {
        // Commit-pin requires a full-history clone (--depth=1 wouldn't include
        // an arbitrary commit). v1 limitation: clone shallow then `git checkout
        // <sha>` may fail if the SHA is older than the tip. Document.
        cloneOpts.shallow = false;
      }
      await this.config.worker.cloneInto(cloneOpts);

      // 4. Stream every file (tracked + .git/) into the sandbox. Parent dirs at
      //    `mountPath` are created implicitly by `sandbox.files.write` (E2B SDK
      //    + InMemoryFiles both `mkdir -p` the dirname before writing) — no
      //    explicit `mkdir -p` step is needed.
      const allFiles = await listAllFiles(dest);
      for (const rel of allFiles) {
        const buf = await readFile(join(dest, rel));
        const target = `${stripTrailingSlash(resource.mountPath)}/${rel}`;
        await sandbox.files.write(target, buf);
      }

      const handle: MountHandle = {
        id: `mh_git_${resource.id}_${Date.now().toString(36)}`,
        resourceId: resource.id,
        resourceType: 'github_repository',
        mountPath: resource.mountPath,
      };
      harnessGitCloneSeconds.observe((Date.now() - start) / 1000);
      harnessGitCloneTotal.inc({ workspace_id: this.config.workspaceId, result: 'ok' });
      return handle;
    } catch (e) {
      harnessGitCloneSeconds.observe((Date.now() - start) / 1000);
      harnessGitCloneTotal.inc({ workspace_id: this.config.workspaceId, result: 'error' });
      throw e;
    }
  }

  async deactivate(_sandbox: SandboxHandle, handle: MountHandle): Promise<void> {
    // No-op: the host-side work dir is cleaned up via WorkDirManager in the
    // runner's stop path; the sandbox is being torn down anyway. The handle
    // is referenced (via the early return) only to keep the signature aligned
    // with `MountStrategy` without tripping `no-unused-vars`.
    if (handle.resourceType !== 'github_repository') return;
  }

  async teardownForSnapshot(_sandbox: SandboxHandle, handle: MountHandle): Promise<TornDownState> {
    return {
      resourceId: handle.resourceId,
      resourceType: handle.resourceType,
      mountPath: handle.mountPath,
    };
  }

  async restoreAfterSnapshot(_sandbox: SandboxHandle, torn: TornDownState): Promise<MountHandle> {
    return {
      id: `mh_git_${torn.resourceId}_${Date.now().toString(36)}`,
      resourceId: torn.resourceId,
      resourceType: torn.resourceType,
      mountPath: torn.mountPath,
    };
  }
}

/**
 * Recursively list every file under `root`, returning paths relative to root.
 * Includes `.git/**`. Skips symlinks. Uses POSIX path separators in the
 * output regardless of the host platform — the sandbox is Linux.
 */
async function listAllFiles(root: string): Promise<string[]> {
  const out: string[] = [];
  const visit = async (dir: string): Promise<void> => {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const e of entries) {
      const full = join(dir, e.name);
      if (e.isSymbolicLink()) continue; // skip symlinks
      if (e.isDirectory()) {
        await visit(full);
      } else if (e.isFile()) {
        const rel = relative(root, full).split(sep).join('/');
        out.push(rel);
      }
    }
  };
  await visit(root);
  return out;
}

function stripTrailingSlash(s: string): string {
  return s.endsWith('/') ? s.slice(0, -1) : s;
}
