// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { chmod, lstat, mkdir, mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';

/**
 * Layout helper for {@link LocalSandboxRuntime}.
 *
 * Each session gets its own host-side directory under
 * `{baseDir}/sessions/{sessionId}-{random}` that the in-sandbox process is
 * allowed to read and write. Mount strategies (`TarballPrefetchStrategy`,
 * `LocalMemoryStrategy`, `GitCloneStrategy`) write resource bytes through
 * `SandboxHandle.files.write` whose root is anchored at this directory — the
 * srt sandbox profile then exposes the same prefix to the agent process.
 *
 * The helper is thin on purpose: the heavy lifting (S3 fetch, git clone,
 * memory seed) is already covered by the existing strategy implementations
 * that E2B and InMemory share. There is deliberately no `local/`-only resource
 * materializer: the local runtime reuses MountStrategy rather than extending it.
 */
export interface SessionWorkDirLayout {
  /** Root directory the agent process reads/writes inside. */
  root: string;
  /** Sandbox-visible scratch dir, distinct from the system `/tmp`. */
  tmp: string;
}

export interface SessionWorkDirOptions {
  /**
   * Top-level base from `ServiceConfig.harnessWorkDir`. Defaults to
   * `/var/tmp/orca-harness` in production; tests pass a fresh tmpdir.
   */
  baseDir: string;
  sessionId: string;
}

/**
 * Create a fresh, private per-session host-side work dir and return its
 * absolute paths. The random, exclusive directory is never reused: a local
 * sandbox's filesystem-root preflight relies on this root being controlled
 * solely by the newly acquired handle.
 *
 * The caller MUST destroy the dir with {@link releaseSessionWorkDir} when the
 * SandboxHandle is destroyed; otherwise the host FS leaks bytes between
 * sessions.
 */
export async function acquireSessionWorkDir(
  opts: SessionWorkDirOptions,
): Promise<SessionWorkDirLayout> {
  if (!/^[A-Za-z0-9_-]+$/.test(opts.sessionId)) {
    throw new Error('local sandbox session id must contain only letters, numbers, "_" or "-"');
  }

  await ensurePrivateDirectory(opts.baseDir);
  const sessionsRoot = join(opts.baseDir, 'sessions');
  await ensurePrivateDirectory(sessionsRoot);

  let root: string | undefined;
  try {
    root = await mkdtemp(join(sessionsRoot, `${opts.sessionId}-`));
    await chmod(root, 0o700);
    const tmp = join(root, 'tmp');
    await mkdir(tmp, { mode: 0o700 });
    await chmod(tmp, 0o700);
    return { root, tmp };
  } catch (error) {
    if (root !== undefined) {
      await rm(root, { recursive: true, force: true }).catch(() => undefined);
    }
    throw error;
  }
}

async function ensurePrivateDirectory(path: string): Promise<void> {
  await mkdir(path, { recursive: true, mode: 0o700 });
  const stats = await lstat(path);
  if (stats.isSymbolicLink() || !stats.isDirectory()) {
    throw new Error(`local sandbox work directory is not a real directory: ${path}`);
  }
  await chmod(path, 0o700);
}

/**
 * Remove the exact randomized per-session work-dir. Cleanup failures are
 * surfaced so the caller can report and retry them; silently abandoning this
 * directory could retain tenant data.
 */
export async function releaseSessionWorkDir(layout: SessionWorkDirLayout): Promise<void> {
  await rm(layout.root, { recursive: true, force: true });
}
