// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { EventEmitter } from 'node:events';
import type { ChildProcess } from 'node:child_process';
import { resolveWorkerEntry, workerCommand } from '../../src/commands/worker.js';

/**
 * A fake child process the worker command awaits; a test drives its exit.
 * `workerCommand` attaches its `exit` listener only after `await resolveEntry()`
 * resolves (a later microtask), so the exit is emitted on the next macrotask via
 * `setImmediate` to guarantee the listener is already registered — mirroring the
 * always-async timing of a real child exit.
 */
class FakeChild extends EventEmitter {
  exitWith(code: number | null, signal: NodeJS.Signals | null = null): void {
    setImmediate(() => this.emit('exit', code, signal));
  }
}

describe('workerCommand launch', () => {
  it('resolves the worker entry and spawns it with the mapped env', async () => {
    const child = new FakeChild();
    let spawnedEntry = '';
    let spawnedEnv: NodeJS.ProcessEnv = {};

    const done = workerCommand({
      args: [
        '--environment',
        'env_1',
        '--env-key',
        'k',
        '--registry',
        'wss://reg',
        '--workspace-dir',
        '/ws',
        '--runner-command',
        'run me',
      ],
      baseEnv: { PATH: '/usr/bin' },
      resolveEntry: async () => '/abs/path/to/environment-worker/dist/main.js',
      spawnWorker: (entry, env) => {
        spawnedEntry = entry;
        spawnedEnv = env;
        return child as unknown as ChildProcess;
      },
    });

    child.exitWith(0);
    const code = await done;

    expect(code).toBe(0);
    expect(spawnedEntry).toBe('/abs/path/to/environment-worker/dist/main.js');
    // Base env is preserved and the worker contract vars are layered on top.
    expect(spawnedEnv['PATH']).toBe('/usr/bin');
    expect(spawnedEnv['ENVIRONMENT_ID']).toBe('env_1');
    expect(spawnedEnv['ENVIRONMENT_KEY']).toBe('k');
    expect(spawnedEnv['REGISTRY_TUNNEL_BASE_URL']).toBe('wss://reg');
    expect(spawnedEnv['WORKSPACE_DIR']).toBe('/ws');
    expect(spawnedEnv['RUNNER_LAUNCH_COMMAND']).toBe('run me');
  });

  it('propagates a non-zero worker exit code', async () => {
    const child = new FakeChild();
    const done = workerCommand({
      args: [
        '--environment',
        'e',
        '--env-key',
        'k',
        '--registry',
        'wss://r',
        '--workspace-dir',
        '/w',
        '--runner-command',
        'c',
      ],
      baseEnv: {},
      resolveEntry: async () => '/entry.js',
      spawnWorker: () => child as unknown as ChildProcess,
    });
    child.exitWith(3);
    expect(await done).toBe(3);
  });

  it('maps a signal-only exit to code 1', async () => {
    const child = new FakeChild();
    const done = workerCommand({
      args: [
        '--environment',
        'e',
        '--env-key',
        'k',
        '--registry',
        'wss://r',
        '--workspace-dir',
        '/w',
        '--runner-command',
        'c',
      ],
      baseEnv: {},
      resolveEntry: async () => '/entry.js',
      spawnWorker: () => child as unknown as ChildProcess,
    });
    child.exitWith(null, 'SIGTERM');
    expect(await done).toBe(1);
  });
});

describe('resolveWorkerEntry', () => {
  it('returns the resolved dist entry when it exists on disk', () => {
    const entry = resolveWorkerEntry({
      resolve: (spec) => {
        expect(spec).toBe('@orca/environment-worker');
        return 'file:///abs/environment-worker/dist/main.js';
      },
      exists: () => true,
    });
    expect(entry).toBe('/abs/environment-worker/dist/main.js');
  });

  it('throws an actionable build hint when the entry is missing', () => {
    let checkedPath = '';
    expect(() =>
      resolveWorkerEntry({
        resolve: () => 'file:///abs/environment-worker/dist/main.js',
        exists: (path) => {
          checkedPath = path;
          return false;
        },
      }),
    ).toThrow(/environment-worker entry not found .*pnpm -r build/);
    // The guard checks the same path it would have spawned.
    expect(checkedPath).toBe('/abs/environment-worker/dist/main.js');
  });

  // `new URL(...).pathname` keeps percent-encoding, so a checkout under
  // `~/My Projects/` resolved to `.../My%20Projects/...`, which `existsSync`
  // cannot find: a correctly built worker was reported missing with "build it
  // first" on any path containing a space or a non-ASCII character.
  it.each([
    ['file:///home/a/My%20Projects/orca/dist/main.js', '/home/a/My Projects/orca/dist/main.js'],
    ['file:///srv/caf%C3%A9/orca/dist/main.js', '/srv/café/orca/dist/main.js'],
    ['file:///plain/path/dist/main.js', '/plain/path/dist/main.js'],
  ])('decodes %s to a real filesystem path', (url, expected) => {
    let checkedPath = '';
    const entry = resolveWorkerEntry({
      resolve: () => url,
      exists: (path) => {
        checkedPath = path;
        return true;
      },
    });
    expect(entry).toBe(expected);
    expect(checkedPath).toBe(expected);
  });
});
