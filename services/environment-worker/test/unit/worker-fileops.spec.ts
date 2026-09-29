// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// End-to-end-ish spec for the EnvironmentWorker filesystem + git-worktree
// dispatch surface.
//
// The worker is a CLIENT: it dials the in-process FAKE registry worker tunnel over
// a real `ws` WebSocket (the same fake used by the launch/stop spec, speaking the
// shared `@orca/harness-tunnel` worker-frame contract). This spec pushes the
// workspace-selection request frames the registry sends — worker.stat /
// worker.list_dir / worker.create_dir / worker.create_worktree / worker.remove_worktree —
// across the live socket and asserts the worker answers each with the right
// result frame, against a real temp filesystem and real `git`. It proves the
// dispatch wiring (not just the handlers in isolation): a frame the registry
// sends round-trips to a result on the same socket.

import { afterEach, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { EnvironmentWorker } from '../../src/worker.js';
import { WorktreeError } from '../../src/git-worktree.js';
import { runJob } from '../../src/worktree-thread.js';
import type { WorktreeOffload } from '../../src/worktree-offload.js';
import { FakeRegistryWorkerTunnel, type LiveWorker } from './support/fake-registry.js';

/**
 * In-process worktree offload for the dispatch tests: runs the SAME job the
 * production worker thread would (`runJob` → real `git`), and rehydrates a
 * thread-side `WorktreeError`/unexpected error exactly as the real worker-thread
 * offload does. This keeps the round-trip real (real git, real socket, real
 * dispatch) without depending on the built `worktree-thread.js` sibling; the
 * actual worker-thread mechanism is covered in worktree-offload.spec.ts.
 */
const inProcessOffload: WorktreeOffload = async (job) => {
  const reply = runJob(job);
  if (reply.ok) {
    return reply.value;
  }
  if ('worktreeError' in reply) {
    throw new WorktreeError(reply.worktreeError);
  }
  throw new Error(`worktree thread failed: ${reply.unexpected}`);
};

const ENV_ID = 'env_fileops_001';
const ENV_KEY = 'sk-test-env-key-fileopsfileopsfileopsfileops';

const GIT_ENV: Record<string, string> = {
  GIT_AUTHOR_NAME: 't',
  GIT_AUTHOR_EMAIL: 't@t',
  GIT_COMMITTER_NAME: 't',
  GIT_COMMITTER_EMAIL: 't@t',
};

const openServers: FakeRegistryWorkerTunnel[] = [];
const openWorkers: EnvironmentWorker[] = [];
const tmpDirs: string[] = [];

afterEach(async () => {
  for (const worker of openWorkers.splice(0)) {
    await worker.stop();
  }
  for (const server of openServers.splice(0)) {
    await server.close();
  }
  for (const dir of tmpDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

async function buildRegistry(): Promise<FakeRegistryWorkerTunnel> {
  const server = new FakeRegistryWorkerTunnel({ environmentId: ENV_ID, environmentKey: ENV_KEY });
  openServers.push(server);
  await server.listen();
  return server;
}

/** A scratch dir on disk (realpath so it matches the worker's canonical paths). */
function makeTmp(): string {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'orca-wfs-')));
  tmpDirs.push(dir);
  return dir;
}

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
  // makeTmp() already returns a realpath'd parent, so the child is canonical too.
  const parent = makeTmp();
  const repo = join(parent, 'myrepo');
  mkdirSync(repo);
  git(repo, 'init', '-q', '-b', 'main');
  writeFileSync(join(repo, 'README.md'), 'hi');
  git(repo, 'add', '.');
  git(repo, 'commit', '-q', '-m', 'init');
  return repo;
}

/** Start a real worker dialing the fake registry; resolve once it has registered. */
async function startWorker(
  server: FakeRegistryWorkerTunnel,
  offload: WorktreeOffload = inProcessOffload,
): Promise<LiveWorker> {
  const worker = new EnvironmentWorker({
    environmentId: ENV_ID,
    environmentKey: ENV_KEY,
    registryTunnelBaseUrl: server.baseUrl(),
    registryRunnerUrl: server.baseUrl(),
    workspaceDir: makeTmp(),
    // No runner is spawned in this spec; a placeholder argv keeps construction valid.
    runnerLaunchCommand: ['/bin/true'],
    name: 'test-worker',
    // Run worktree git in-process (the worker-thread sibling isn't built in the
    // unit run); the threaded offload itself is covered in worktree-offload.spec.ts.
    worktreeOffload: offload,
  });
  openWorkers.push(worker);
  void worker.run();
  return server.nextWorker();
}

describe('EnvironmentWorker — worker.stat', () => {
  it('answers a directory stat with exists + type + canonical path', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const tmp = makeTmp();
    const target = join(tmp, 'project');
    mkdirSync(target);

    const result = await worker.stat('req_stat_dir', target);

    expect(result.status).toBe('ok');
    expect(result.exists).toBe(true);
    expect(result.type).toBe('directory');
    expect(result.canonicalPath).toBe(realpathSync(target));
  });

  it('answers a missing-path stat with exists:false', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const tmp = makeTmp();

    const result = await worker.stat('req_stat_missing', join(tmp, 'nope'));

    expect(result.status).toBe('ok');
    expect(result.exists).toBe(false);
    expect(result.canonicalPath ?? null).toBeNull();
  });
});

describe('EnvironmentWorker — worker.list_dir', () => {
  it('answers with the directory contents sorted by name', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const tmp = makeTmp();
    mkdirSync(join(tmp, 'b-dir'));
    writeFileSync(join(tmp, 'a-file.txt'), 'hello');

    const result = await worker.listDir({ requestId: 'req_ls', path: tmp });

    expect(result.status).toBe('ok');
    expect((result.entries ?? []).map((e) => e.name)).toEqual(['a-file.txt', 'b-dir']);
    expect(result.hasMore).toBe(false);
  });

  it('paginates with an after cursor across the live tunnel', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const tmp = makeTmp();
    for (const name of ['a', 'b', 'c', 'd']) {
      writeFileSync(join(tmp, name), 'x');
    }

    const result = await worker.listDir({
      requestId: 'req_ls_page',
      path: tmp,
      limit: 2,
      after: join(tmp, 'a'),
    });

    expect(result.status).toBe('ok');
    expect((result.entries ?? []).map((e) => e.name)).toEqual(['b', 'c']);
    expect(result.hasMore).toBe(true);
  });
});

describe('EnvironmentWorker — worker.create_dir', () => {
  it('creates a directory and returns its path', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const tmp = makeTmp();
    const target = join(tmp, 'new-app');

    const result = await worker.createDir('req_mkdir', target);

    expect(result.status).toBe('ok');
    expect(result.path).toBe(target);
    expect(existsSync(target)).toBe(true);
  });

  it('reports an existing directory as status:ok with an error (not failed)', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const tmp = makeTmp();
    const existing = join(tmp, 'dup');
    mkdirSync(existing);

    const result = await worker.createDir('req_mkdir_dup', existing);

    expect(result.status).toBe('ok');
    expect(result.error).toBe('directory already exists');
    expect(result.path ?? null).toBeNull();
  });
});

describe('EnvironmentWorker — worker.create_worktree', () => {
  it('creates a sibling worktree with the branch checked out', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const repo = makeGitRepo();

    const result = await worker.createWorktree({
      requestId: 'req_wt',
      repoPath: repo,
      branchName: 'feature/login',
    });

    expect(result.status).toBe('ok');
    const expected = join(dirname(repo), 'myrepo-worktrees', 'feature-login');
    expect(result.worktreePath).toBe(expected);
    expect(result.branch).toBe('feature/login');
    expect(existsSync(expected)).toBe(true);
    expect(currentBranch(expected)).toBe('feature/login');
  });

  it('answers status:failed with the error for an invalid branch name', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const repo = makeGitRepo();

    const result = await worker.createWorktree({
      requestId: 'req_wt_bad',
      repoPath: repo,
      branchName: 'a b',
    });

    expect(result.status).toBe('failed');
    expect(result.error ?? '').not.toBe('');
    expect(result.worktreePath ?? null).toBeNull();
  });
});

describe('EnvironmentWorker — worker.remove_worktree', () => {
  it('removes the worktree directory and deletes the branch when requested', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const repo = makeGitRepo();
    const created = await worker.createWorktree({
      requestId: 'req_wt_for_rm',
      repoPath: repo,
      branchName: 'feature/rm',
    });
    expect(created.status).toBe('ok');

    const result = await worker.removeWorktree({
      requestId: 'req_wt_rm',
      worktreePath: created.worktreePath!,
      branch: 'feature/rm',
      deleteBranch: true,
    });

    expect(result.status).toBe('ok');
    expect(existsSync(created.worktreePath!)).toBe(false);
    const branches = execFileSync('git', ['branch', '--list', 'feature/rm'], {
      cwd: repo,
      env: { ...process.env, ...GIT_ENV },
      encoding: 'utf-8',
    }).trim();
    expect(branches).toBe('');
  });

  it('answers status:failed for a missing worktree path', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server);
    const repo = makeGitRepo();

    const result = await worker.removeWorktree({
      requestId: 'req_wt_rm_missing',
      worktreePath: join(dirname(repo), 'myrepo-worktrees', 'ghost'),
    });

    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain('does not exist');
  });
});

describe('EnvironmentWorker — an UNEXPECTED handler failure still answers the request', () => {
  // A handler that throws something other than a WorktreeError used to escape
  // the dispatch before its send: no result frame was built at all, and the
  // throw unwound through the serve loop into run()'s catch, where it was logged
  // as 'worker tunnel disconnected; reconnecting'. The registry's waiter then
  // rejected with its tunnel-closed error — the RETRYABLE classification — so a
  // permanent, actionable filesystem failure was delivered as a transient
  // disconnect and retried forever.
  const explodingOffload: WorktreeOffload = () => {
    throw Object.assign(new Error("EACCES: permission denied, mkdir '/read-only/x-worktrees'"), {
      code: 'EACCES',
    });
  };

  it('answers create_worktree with status:failed and keeps the tunnel up', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server, explodingOffload);
    const repo = makeGitRepo();

    const result = await worker.createWorktree({
      requestId: 'req_wt_boom',
      repoPath: repo,
      branchName: 'feature/boom',
    });

    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain('EACCES');
    expect(result.worktreePath ?? null).toBeNull();

    // The tunnel is unaffected — the failure was a REQUEST failure, not a
    // disconnect, so the same socket keeps serving.
    const pongTs = await worker.pingAndAwaitPong(4242);
    expect(pongTs).toBe(4242);
  });

  it('answers remove_worktree with status:failed and keeps the tunnel up', async () => {
    const server = await buildRegistry();
    const worker = await startWorker(server, explodingOffload);
    const repo = makeGitRepo();

    const result = await worker.removeWorktree({
      requestId: 'req_wt_rm_boom',
      worktreePath: join(dirname(repo), 'myrepo-worktrees', 'anything'),
    });

    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain('EACCES');

    const pongTs = await worker.pingAndAwaitPong(99);
    expect(pongTs).toBe(99);
  });
});

describe('EnvironmentWorker — a slow worktree op does not stall the serve loop', () => {
  it('answers a keepalive ping while the git job is still running', async () => {
    // GIT_TIMEOUT_MS bounds a worktree op at 120s while the engine declares a
    // worker dead after PING_INTERVAL_MS * PING_MISS_THRESHOLD = 90s. Running
    // the git shell-out on a worker thread frees the EVENT loop but not the
    // FRAME loop: with the dispatch awaited inline, `receive()` is not called
    // again until the job finishes, so the ping sits unread in the socket's
    // inbound queue and the worker is closed mid-operation.
    const server = await buildRegistry();
    let releaseJob: (() => void) | undefined;
    const blocked = new Promise<void>((resolve) => {
      releaseJob = resolve;
    });
    const slowOffload: WorktreeOffload = async () => {
      await blocked;
      return { worktreePath: '/tmp/slow-worktree', branch: 'feature/slow' };
    };
    const worker = await startWorker(server, slowOffload);

    const created = worker.createWorktree({
      requestId: 'req_wt_slow',
      repoPath: makeGitRepo(),
      branchName: 'feature/slow',
    });

    // The ping is answered WHILE the job is still blocked — the load-bearing
    // assertion: `releaseJob` has not been called yet.
    const pongTs = await worker.pingAndAwaitPong(2024);
    expect(pongTs).toBe(2024);

    releaseJob!();
    const result = await created;
    expect(result.status).toBe('ok');
    expect(result.worktreePath).toBe('/tmp/slow-worktree');
  });
});
