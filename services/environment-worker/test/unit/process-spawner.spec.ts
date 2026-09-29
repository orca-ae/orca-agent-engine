// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Spawn-failure diagnostics for the worker's runner-process seam.
//
// A runner that never STARTS produces no output, so the exit tail is empty and
// the exit code is synthesized — the OS error is the entire diagnostic that
// exists. These specs run the REAL `ChildProcessSpawner` against a genuinely
// missing binary and a genuinely non-executable one, and assert the composed
// `worker.launch_runner_result` error names the command and the errno: that
// string is what the registry stores and what the API user reads, and "exited
// with code 127" for every distinct cause is not a diagnostic.
//
// The second case here is the `error` event's OTHER meaning. Node emits it on a
// LIVE child when an operation on it fails (an EPERM `kill()`); treating that as
// an exit marks a running runner as dead, prunes it from the hello, and files a
// `worker.runner_exited` for a runner still holding its tunnel. That edge is
// driven through {@link runnerProcessFor}, since a real OS will not produce it
// on demand.

import { afterEach, describe, expect, it } from 'vitest';
import { EventEmitter } from 'node:events';
import type { ChildProcess } from 'node:child_process';
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ChildProcessSpawner, runnerProcessFor } from '../../src/process-spawner.js';
import { EnvironmentWorker } from '../../src/worker.js';

const tmpDirs: string[] = [];

afterEach(() => {
  for (const dir of tmpDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function makeTmp(): string {
  const dir = mkdtempSync(join(tmpdir(), 'orca-spawn-'));
  tmpDirs.push(dir);
  return dir;
}

/** A worker whose launch command is `command`, spawned by the REAL spawner. */
function buildWorker(command: string[]): EnvironmentWorker {
  return new EnvironmentWorker({
    environmentId: 'env_x',
    environmentKey: 'sk-env-key',
    registryTunnelBaseUrl: 'wss://registry.example.com',
    registryRunnerUrl: 'wss://registry.example.com',
    workspaceDir: makeTmp(),
    runnerLaunchCommand: command,
    name: 'worker',
  });
}

describe('ChildProcessSpawner — a runner that never starts', () => {
  it('keeps the ENOENT for a missing binary instead of collapsing it to code 127', async () => {
    const missing = join(makeTmp(), 'opt', 'orca', 'bin', 'session-runner');
    const proc = new ChildProcessSpawner().spawn({
      command: [missing],
      env: {},
      cwd: makeTmp(),
    });

    await proc.wait();

    expect(proc.poll()).toBe(127);
    const err = proc.spawnError?.();
    expect(err).toBeInstanceOf(Error);
    // The command AND the errno — the two things a bare 127 destroys.
    expect(err?.message ?? '').toContain(missing);
    expect((err as NodeJS.ErrnoException | undefined)?.code).toBe('ENOENT');
  });

  it('surfaces the command and errno in the launch result the registry receives', async () => {
    const missing = join(makeTmp(), 'opt', 'orca', 'bin', 'session-runner');
    const worker = buildWorker([missing]);

    const result = await worker.handleLaunchForTest({
      requestId: 'req_enoent',
      bindingToken: 'tok_enoent',
      workspace: makeTmp(),
    });

    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain(missing);
    expect(result.error ?? '').toContain('ENOENT');
  });

  it('distinguishes a non-executable binary (EACCES) from a missing one', async () => {
    // Both are "code 127" without the retained spawn error, yet they are
    // completely different operator fixes.
    const dir = makeTmp();
    const notExecutable = join(dir, 'session-runner');
    writeFileSync(notExecutable, '#!/bin/sh\necho hi\n');
    chmodSync(notExecutable, 0o644);
    const worker = buildWorker([notExecutable]);

    const result = await worker.handleLaunchForTest({
      requestId: 'req_eacces',
      bindingToken: 'tok_eacces',
      workspace: makeTmp(),
    });

    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain(notExecutable);
    expect(result.error ?? '').toContain('EACCES');
  });

  it('reports a bad cwd rather than a causeless exit code', async () => {
    const missingCwd = join(makeTmp(), 'no', 'such', 'dir');
    const worker = buildWorker(['/bin/sh', '-c', 'sleep 10']);
    const result = await worker.handleLaunchForTest({
      requestId: 'req_cwd',
      bindingToken: 'tok_cwd',
      workspace: missingCwd,
    });

    // An absolute frame workspace that does not exist is refused before the
    // spawn; the point is that the reason is named either way.
    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain(missingCwd);
  });
});

/** The slice of `ChildProcess` the handle listens on, as a bare emitter. */
class FakeChild extends EventEmitter {
  readonly pid: number | undefined = 4242;
  readonly stdout = null;
  readonly stderr = null;
  kill(): boolean {
    return true;
  }
}

describe('ChildProcessSpawner — a post-spawn error is not an exit', () => {
  it('leaves a running child running when the OS reports a failed kill', () => {
    const child = new FakeChild();
    const proc = runnerProcessFor(child as unknown as ChildProcess);

    child.emit('spawn');
    const eperm: NodeJS.ErrnoException = Object.assign(new Error('kill EPERM'), { code: 'EPERM' });
    child.emit('error', eperm);

    // Still alive: poll() is what aliveRunnerIds() prunes on and what the exit
    // watcher waits for, so a settled exit here would drop a LIVE runner from
    // the hello and file a bogus runner_exited for it.
    expect(proc.poll()).toBeNull();
    expect(proc.spawnError?.()).toBeUndefined();
    expect(proc.postSpawnError?.()).toBe(eperm);
  });

  it('settles the handle when the error arrives before the process ever started', () => {
    const child = new FakeChild();
    const proc = runnerProcessFor(child as unknown as ChildProcess);

    const enoent: NodeJS.ErrnoException = Object.assign(new Error('spawn /nope ENOENT'), {
      code: 'ENOENT',
    });
    child.emit('error', enoent);

    // No `spawn` fired, so no `exit` ever will: the handle must settle or the
    // launch would wait forever.
    expect(proc.poll()).toBe(127);
    expect(proc.spawnError?.()).toBe(enoent);
    expect(proc.postSpawnError?.()).toBeUndefined();
  });
});
