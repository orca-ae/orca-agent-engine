// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker-thread offload for the host-side git worktree ops.
//
// Two layers are exercised here:
//
//   1. `runJob` — the thread-side body — run IN-PROCESS against real `git`, so
//      the clone-safe reply mapping (ok/value, worktreeError, unexpected) is
//      pinned without a thread.
//   2. `createWorktreeOffload` — the parent side — run against a REAL
//      `node:worker_threads` worker that loads the actual `worktree-thread.ts`
//      (through a tsx bootstrap, since the built `.js` sibling doesn't exist in a
//      source-only test run) and runs real `git` ON THAT THREAD. This proves the
//      offload starts the thread, passes the job, rehydrates a `WorktreeError`
//      across the structured-clone boundary, and — the load-bearing property —
//      keeps the MAIN event loop free while git runs on the thread.
//
// The worker.ts dispatch that calls this offload is covered in
// worker-fileops.spec.ts; here the contract under test is the offload mechanism.

import { afterEach, describe, expect, it } from 'vitest';
import { Worker } from 'node:worker_threads';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { WorktreeError } from '../../src/git-worktree.js';
import { runJob } from '../../src/worktree-thread.js';
import {
  createJob,
  createWorktreeOffload,
  removeJob,
  type WorktreeWorkerHandle,
  type WorktreeWorkerSpawn,
} from '../../src/worktree-offload.js';

const GIT_ENV: Record<string, string> = {
  GIT_AUTHOR_NAME: 't',
  GIT_AUTHOR_EMAIL: 't@t',
  GIT_COMMITTER_NAME: 't',
  GIT_COMMITTER_EMAIL: 't@t',
};

const tmpDirs: string[] = [];

afterEach(() => {
  for (const dir of tmpDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function git(repo: string, ...args: string[]): void {
  execFileSync('git', args, { cwd: repo, env: { ...process.env, ...GIT_ENV } });
}

function currentBranch(path: string): string {
  return execFileSync('git', ['rev-parse', '--abbrev-ref', 'HEAD'], {
    cwd: path,
    env: { ...process.env, ...GIT_ENV },
    encoding: 'utf-8',
  }).trim();
}

/** Create a one-commit git repo inside a fresh temp parent; return its resolved root. */
function makeGitRepo(): string {
  const parent = realpathSync(mkdtempSync(join(tmpdir(), 'orca-wto-')));
  tmpDirs.push(parent);
  const repo = join(parent, 'myrepo');
  mkdirSync(repo);
  git(repo, 'init', '-q', '-b', 'main');
  writeFileSync(join(repo, 'README.md'), 'hi');
  git(repo, 'add', '.');
  git(repo, 'commit', '-q', '-m', 'init');
  return repo;
}

/**
 * A real-worker-thread spawn that loads the actual `worktree-thread.ts` via a tsx
 * bootstrap (register the loader in-thread, then dynamic-import the TS entry). The
 * built `worktree-thread.js` sibling the production factory uses does not exist in
 * a source-only test run, so this stands in for it while still running the REAL
 * thread body on a REAL thread.
 */
const tsThreadEntryUrl = new URL('../../src/worktree-thread.ts', import.meta.url).href;
const tsxBootstrapSpawn: WorktreeWorkerSpawn = (job) => {
  const boot = `
import { register } from 'tsx/esm/api';
register();
await import(${JSON.stringify(tsThreadEntryUrl)});
`;
  return new Worker(boot, { eval: true, workerData: job });
};

describe('runJob (in-process thread body, real git)', () => {
  it('creates a worktree and returns the clone-safe value', () => {
    const repo = makeGitRepo();
    const reply = runJob(createJob({ repoPath: repo, branchName: 'feature/login' }));
    expect(reply.ok).toBe(true);
    if (!reply.ok) return;
    const expected = join(dirname(repo), 'myrepo-worktrees', 'feature-login');
    expect(reply.value).toEqual({ worktreePath: expected, branch: 'feature/login' });
    expect(existsSync(expected)).toBe(true);
    expect(currentBranch(expected)).toBe('feature/login');
  });

  it('returns ok with a null value for a remove job', () => {
    const repo = makeGitRepo();
    const created = runJob(createJob({ repoPath: repo, branchName: 'feature/rm' }));
    expect(created.ok).toBe(true);
    if (!created.ok || created.value === null) return;
    const reply = runJob(
      removeJob({
        worktreePath: created.value.worktreePath,
        branch: 'feature/rm',
        deleteBranch: true,
      }),
    );
    expect(reply).toEqual({ ok: true, value: null });
    expect(existsSync(created.value.worktreePath)).toBe(false);
  });

  it('flattens a WorktreeError to its message (clone-safe)', () => {
    const repo = makeGitRepo();
    // An invalid branch name raises a WorktreeError before any git runs.
    const reply = runJob(createJob({ repoPath: repo, branchName: 'a b' }));
    expect(reply).toEqual({
      ok: false,
      worktreeError: expect.stringContaining('invalid character'),
    });
  });
});

describe('createWorktreeOffload (real worker thread, real git)', () => {
  it('runs a create job on a thread and resolves the value', async () => {
    const offload = createWorktreeOffload(tsxBootstrapSpawn);
    const repo = makeGitRepo();

    const value = await offload(createJob({ repoPath: repo, branchName: 'feature/threaded' }));

    const expected = join(dirname(repo), 'myrepo-worktrees', 'feature-threaded');
    expect(value).toEqual({ worktreePath: expected, branch: 'feature/threaded' });
    // The worktree was really created by the thread (not a stub).
    expect(existsSync(expected)).toBe(true);
    expect(currentBranch(expected)).toBe('feature/threaded');
  });

  it('runs a remove job on a thread and resolves null', async () => {
    const offload = createWorktreeOffload(tsxBootstrapSpawn);
    const repo = makeGitRepo();
    const created = await offload(createJob({ repoPath: repo, branchName: 'feature/rm-thread' }));
    expect(created).not.toBeNull();
    if (created === null) return;

    const result = await offload(
      removeJob({
        worktreePath: created.worktreePath,
        branch: 'feature/rm-thread',
        deleteBranch: true,
      }),
    );

    expect(result).toBeNull();
    expect(existsSync(created.worktreePath)).toBe(false);
  });

  it('rehydrates a thread-side WorktreeError as a real WorktreeError', async () => {
    const offload = createWorktreeOffload(tsxBootstrapSpawn);
    const repo = makeGitRepo();

    // Missing worktree path → the thread raises WorktreeError; it must arrive on
    // the main thread as a WorktreeError instance (not a plain object), so the
    // handler's `instanceof WorktreeError` mapping fires.
    await expect(
      offload(removeJob({ worktreePath: join(dirname(repo), 'myrepo-worktrees', 'ghost') })),
    ).rejects.toBeInstanceOf(WorktreeError);
    await expect(
      offload(removeJob({ worktreePath: join(dirname(repo), 'myrepo-worktrees', 'ghost') })),
    ).rejects.toThrow(/does not exist/);
  });

  it('keeps the main event loop responsive while git runs on the thread', async () => {
    // The whole point of the worker thread: the main loop is free DURING git.
    // A 5ms-interval timer fired on the main loop must keep ticking while the
    // create job runs on the thread. If git ran on the main thread (blocking
    // spawnSync), the timer would be starved and tick ~0 times.
    const offload = createWorktreeOffload(tsxBootstrapSpawn);
    const repo = makeGitRepo();

    let ticks = 0;
    const timer = setInterval(() => {
      ticks += 1;
    }, 5);
    try {
      await offload(createJob({ repoPath: repo, branchName: 'feature/liveness' }));
    } finally {
      clearInterval(timer);
    }
    // Thread startup + a real `git worktree add` is comfortably more than a few
    // timer intervals; the main loop ran them. A blocked loop yields 0 ticks.
    expect(ticks).toBeGreaterThan(0);
  });

  it('rejects with a generic Error when the thread exits without a reply', async () => {
    // A worker that posts nothing and exits cleanly must not hang the offload —
    // the `exit` edge rejects.
    const silentExitSpawn: WorktreeWorkerSpawn = () =>
      new Worker('', { eval: true }) as unknown as WorktreeWorkerHandle;
    const offload = createWorktreeOffload(silentExitSpawn);

    await expect(offload(removeJob({ worktreePath: '/whatever' }))).rejects.toThrow(
      /exited without a result/,
    );
  });

  it('rejects with a generic Error for an unexpected (non-WorktreeError) thread failure', () => {
    // `unexpected` replies (a bug in the thread body, not a user-facing worktree
    // failure) become a generic Error so the handler re-throws them as a crash
    // rather than a `status: "failed"` worktree result.
    const offload = createWorktreeOffload(
      () =>
        ({
          once(event: string, listener: (arg: unknown) => void): void {
            if (event === 'message') {
              listener({ ok: false, unexpected: 'boom' });
            }
          },
          terminate: () => Promise.resolve(0),
        }) as unknown as WorktreeWorkerHandle,
    );

    return expect(offload(removeJob({ worktreePath: '/x' }))).rejects.toThrow(
      /worktree thread failed: boom/,
    );
  });
});
