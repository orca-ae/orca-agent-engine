// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdir, rm } from 'node:fs/promises';
import { resolve, sep } from 'node:path';

export interface WorkDirManagerOptions {
  /** Base directory under which all per-session work dirs live. Defaults
   * to /var/tmp/orca-harness in production; tests pass a fresh tmpdir. */
  baseDir: string;
}

/**
 * Owns the per-session host-side ephemeral work dir for
 * `GitCloneStrategy`. Layout:
 *
 *   {baseDir}/sessions/{workspaceId}/{sessionId}/repo-{repoIdx}/
 *
 * - `acquire` creates the per-repo dir (and parents) and returns its absolute
 *   path. Idempotent: re-acquiring the same triplet returns the same path
 *   without rebuilding the dir.
 * - `releaseSession` rm -rf's the entire session subtree (every repo plus
 *   the session-level dir). Called from SessionRunner.stop().
 * - `releaseWorkspace` rm -rf's an entire workspace subtree.
 *
 * Identifiers are validated as single safe path segments and the resolved
 * target is checked against the sessions root before mkdir or recursive rm.
 * mkdir/rm are idempotent, so double acquire/release remains a no-op.
 */
export class WorkDirManager {
  private readonly sessionsRoot: string;

  constructor(opts: WorkDirManagerOptions) {
    this.sessionsRoot = resolve(opts.baseDir, 'sessions');
  }

  async acquire(workspaceId: string, sessionId: string, repoIdx: number): Promise<string> {
    if (!Number.isSafeInteger(repoIdx) || repoIdx < 0) {
      throw new Error(`invalid repository index: ${repoIdx}`);
    }
    const dir = this.scopedPath(workspaceId, sessionId, `repo-${repoIdx}`);
    await mkdir(dir, { recursive: true });
    return dir;
  }

  async releaseSession(workspaceId: string, sessionId: string): Promise<void> {
    const dir = this.scopedPath(workspaceId, sessionId);
    await rm(dir, { recursive: true, force: true });
  }

  async releaseWorkspace(workspaceId: string): Promise<void> {
    const dir = this.scopedPath(workspaceId);
    await rm(dir, { recursive: true, force: true });
  }

  private scopedPath(...segments: string[]): string {
    for (const segment of segments) {
      if (!SAFE_PATH_SEGMENT.test(segment)) {
        throw new Error(`invalid work-dir path segment: ${segment}`);
      }
    }
    const candidate = resolve(this.sessionsRoot, ...segments);
    if (!candidate.startsWith(`${this.sessionsRoot}${sep}`)) {
      throw new Error('work-dir path escapes the sessions root');
    }
    return candidate;
  }
}

const SAFE_PATH_SEGMENT = /^[A-Za-z0-9_-]{1,128}$/;
