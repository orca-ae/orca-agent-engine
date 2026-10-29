// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { InMemorySandboxRuntime } from '../../src/in-memory/runtime.js';
import {
  LocalSandboxRuntime,
  asLocalSandboxHandle,
  type SandboxManagerInitConfig,
  type SandboxManagerLike,
} from '../../src/local/runtime.js';
import type { SandboxHandle, SpawnHandle } from '../../src/sandbox-runtime.js';

/**
 * Read a readable stream to EOF, returning the accumulated utf8 string.
 */
function collect(stream: NodeJS.ReadableStream): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (b: Buffer) => chunks.push(Buffer.isBuffer(b) ? b : Buffer.from(b)));
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    stream.on('error', reject);
  });
}

/**
 * Read from a stream until `predicate` is satisfied by the accumulated text,
 * then resolve with what has arrived so far. Used to assert *streamed* output
 * without waiting for the (long-lived) process to exit.
 */
function readUntil(
  stream: NodeJS.ReadableStream,
  predicate: (acc: string) => boolean,
): Promise<string> {
  return new Promise((resolve, reject) => {
    let acc = '';
    const onData = (b: Buffer): void => {
      acc += (Buffer.isBuffer(b) ? b : Buffer.from(b)).toString('utf8');
      if (predicate(acc)) {
        stream.off('data', onData);
        stream.off('error', reject);
        resolve(acc);
      }
    };
    stream.on('data', onData);
    stream.on('error', reject);
  });
}

/**
 * Verbatim `SandboxManager` fake: `wrapWithSandbox` returns the command
 * unchanged so `child_process.spawn` runs the bare command — the same pattern
 * the LocalSandboxRuntime unit suite uses.
 */
class FakeSandboxManager implements SandboxManagerLike {
  async initialize(_config: SandboxManagerInitConfig): Promise<void> {}
  async wrapWithSandbox(command: string): Promise<string> {
    return command;
  }
}

/**
 * The spawn primitive is defined on `SandboxHandle` and implemented by both
 * reusable runtimes. This suite drives it against each: feed stdin, read
 * streamed stdout, and terminate with `kill`.
 */
describe.each<{
  name: string;
  acquire: () => Promise<{ sandbox: SandboxHandle; cleanup: () => Promise<void> }>;
}>([
  {
    name: 'InMemorySandboxRuntime',
    acquire: async () => {
      const rt = new InMemorySandboxRuntime();
      const sandbox = await rt.acquire({});
      return { sandbox, cleanup: () => sandbox.destroy() };
    },
  },
  {
    name: 'LocalSandboxRuntime',
    acquire: async () => {
      const baseDir = mkdtempSync(join(tmpdir(), 'orca-spawn-local-'));
      const rt = new LocalSandboxRuntime({
        harnessWorkDir: baseDir,
        allowedNetworkHosts: [],
        manager: new FakeSandboxManager(),
      });
      const sandbox = await rt.acquire({});
      return {
        sandbox,
        cleanup: async () => {
          await sandbox.destroy();
          rmSync(baseDir, { recursive: true, force: true });
        },
      };
    },
  },
])('SandboxHandle.spawn — $name', ({ acquire }) => {
  let sandbox: SandboxHandle;
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    ({ sandbox, cleanup } = await acquire());
  });

  afterEach(async () => {
    await cleanup();
  });

  it('round-trips: writes to stdin and reads it back off stdout', async () => {
    // `cat` echoes stdin to stdout. Close stdin so `cat` sees EOF and exits,
    // letting `collect` observe `end`.
    // `spawn` is optional on the interface (cloud runtimes may omit it); the two
    // reusable runtimes under test implement it, so assert-non-null to invoke.
    const proc = await sandbox.spawn!('cat');
    proc.stdin.write('ping\n');
    proc.stdin.end();
    const out = await collect(proc.stdout);
    expect(out).toBe('ping\n');
  });

  it('streams stdout incrementally (before the process exits)', async () => {
    // Emit a marker, then sleep — a run-to-completion API would block here,
    // but spawn hands back the live stream so we can read the marker now.
    const proc = await sandbox.spawn!('echo streamed-line; sleep 5');
    try {
      const seen = await readUntil(proc.stdout, (acc) => acc.includes('streamed-line'));
      expect(seen).toContain('streamed-line');
    } finally {
      proc.kill();
    }
  });

  it('kill terminates a long-lived process', async () => {
    const proc = await sandbox.spawn!('sleep 30');
    const exited = new Promise<void>((resolve) => {
      // `stdout` closes when the child (and thus the shell) is torn down.
      proc.stdout.on('close', () => resolve());
    });
    proc.kill('SIGKILL');
    await withTimeout(exited, 5000, 'process did not exit after kill()');
  }, 10_000);

  it('injects env vars into the spawned process', async () => {
    const proc = await sandbox.spawn!('printf "%s" "$ORCA_SPAWN_TEST"', {
      env: { ORCA_SPAWN_TEST: 'from-env' },
    });
    proc.stdin.end();
    const out = await collect(proc.stdout);
    expect(out).toBe('from-env');
  });
});

function withTimeout<T>(p: Promise<T>, ms: number, message: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, reject) => setTimeout(() => reject(new Error(message)), ms)),
  ]);
}

it('Local spawn drains large wrapper and command stderr, preserves stdout and releases resources', async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'orca-spawn-stderr-'));
  const releaseSandboxCommand = vi.fn(async (_command: string) => {});
  // Real child processes with a fake wrapper: this checks pipe handling, not SRT confinement.
  const noisy = `'${process.execPath}' -e 'require("node:fs").writeSync(2, Buffer.alloc(4 * 1024 * 1024))'`;
  const manager: SandboxManagerLike = {
    initialize: async () => {},
    wrapWithSandbox: async (command) => `${noisy}; ${command}`,
    releaseSandboxCommand,
  };
  const runtime = new LocalSandboxRuntime({
    harnessWorkDir: baseDir,
    allowedNetworkHosts: [],
    manager,
  });
  const sandbox = await runtime.acquire({});
  const root = asLocalSandboxHandle(sandbox).rootDir();
  const child = await sandbox.spawn!(`${noisy}; printf completed`);
  let timer: ReturnType<typeof setTimeout> | undefined;
  let exited = false;
  try {
    const [stdout, exit] = await Promise.race([
      Promise.all([collect(child.stdout), child.exited]),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('stderr pipe stalled')), 5000);
      }),
    ]);
    exited = true;
    expect(stdout).toBe('completed');
    expect(exit).toEqual({ code: 0, signal: null });
    expect(releaseSandboxCommand).toHaveBeenCalledOnce();
    await sandbox.destroy();
    expect(existsSync(root)).toBe(false);
  } finally {
    clearTimeout(timer);
    if (!exited) child.kill('SIGKILL');
    await child.exited;
    await sandbox.destroy();
    rmSync(baseDir, { recursive: true, force: true });
  }
}, 10000);

/**
 * spawn honors the destroyed guard the same way `run` does.
 */
describe('SandboxHandle.spawn — destroyed guard', () => {
  it('InMemory: rejects spawn after destroy', async () => {
    const rt = new InMemorySandboxRuntime();
    const sandbox = await rt.acquire({});
    await sandbox.destroy();
    await expect(sandbox.spawn!('echo hi')).rejects.toThrowError(/destroyed/);
  });

  it('Local: rejects spawn after destroy', async () => {
    const baseDir = mkdtempSync(join(tmpdir(), 'orca-spawn-guard-'));
    try {
      const rt = new LocalSandboxRuntime({
        harnessWorkDir: baseDir,
        allowedNetworkHosts: [],
        manager: new FakeSandboxManager(),
      });
      const sandbox = await rt.acquire({});
      await sandbox.destroy();
      await expect(sandbox.spawn!('echo hi')).rejects.toThrowError(/destroyed/);
    } finally {
      rmSync(baseDir, { recursive: true, force: true });
    }
  });
});

// Type-only reference so `SpawnHandle` import is exercised by the type checker.
const _typecheck: SpawnHandle | undefined = undefined;
void _typecheck;
