// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// TmuxSandboxHandle — a socket-scoped tmux session as a runner SandboxHandle.
//
// The runner's native-CLI providers launch a real CLI as a long-lived child and
// stream its stdio through the extracted `SandboxHandle.spawn` primitive. This
// suite pins the tmux-backed implementation of that boundary: each `spawn` runs
// the command in a tmux pane on a private socket, streams the process's raw
// stdout back (not the PTY-rendered pane), accepts stdin, and terminates on
// `kill`. Because an operator can `tmux -S <socket> attach` to the same socket to
// watch/drive the pane locally, this is the "free local attach" transport — no
// cloud sandbox required.
//
// The suite drives ONLY the shared `SandboxHandle` contract (spawn / run / files
// / pause / resume / destroy), so it is bit-compatible with the InMemory + Local
// runtimes the package already ships. It self-skips when the `tmux` binary is not
// on PATH so the unit run stays green on a box without tmux (CI installs it).

import { execFileSync } from 'node:child_process';
import { describe, it, expect } from 'vitest';
import { TmuxSandboxRuntime, type SandboxHandle } from '../../src/sandbox/tmux-sandbox.js';

/** True when a usable `tmux` binary is on PATH (the suite self-skips otherwise). */
function tmuxAvailable(): boolean {
  try {
    execFileSync('tmux', ['-V'], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

/** Read a readable stream to EOF, returning the accumulated utf8 string. */
function collect(stream: NodeJS.ReadableStream): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (b: Buffer) => chunks.push(Buffer.isBuffer(b) ? b : Buffer.from(b)));
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    stream.on('error', reject);
  });
}

/**
 * Read from a stream until `predicate` holds over the accumulated text, then
 * resolve with what has arrived so far — asserts *streamed* output without
 * waiting for a long-lived process to exit.
 */
function readUntil(
  stream: NodeJS.ReadableStream,
  predicate: (acc: string) => boolean,
  timeoutMs = 8000,
): Promise<string> {
  return new Promise((resolve, reject) => {
    let acc = '';
    const timer = setTimeout(() => {
      stream.off('data', onData);
      reject(new Error(`readUntil timed out; saw: ${JSON.stringify(acc)}`));
    }, timeoutMs);
    const onData = (b: Buffer): void => {
      acc += (Buffer.isBuffer(b) ? b : Buffer.from(b)).toString('utf8');
      if (predicate(acc)) {
        clearTimeout(timer);
        stream.off('data', onData);
        stream.off('error', reject);
        resolve(acc);
      }
    };
    stream.on('data', onData);
    stream.on('error', reject);
  });
}

/** Await `ms` milliseconds (used to let input round-trip before closing stdin). */
function delayMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const describeTmux = tmuxAvailable() ? describe : describe.skip;

describeTmux('TmuxSandboxHandle — SandboxHandle over a socket-scoped tmux session', () => {
  async function withSandbox(fn: (sandbox: SandboxHandle) => Promise<void>): Promise<void> {
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      await fn(sandbox);
    } finally {
      await sandbox.destroy();
    }
  }

  it('advertises a stable id and no FUSE (host-process transport)', async () => {
    await withSandbox(async (sandbox) => {
      expect(sandbox.id).toMatch(/^sbx_tmux_/);
      const runtime = new TmuxSandboxRuntime();
      expect(runtime.capabilities.supportsFuse).toBe(false);
    });
  });

  it('spawn round-trips: writes to stdin and reads it back off stdout', async () => {
    await withSandbox(async (sandbox) => {
      // `spawn` is optional on the interface (cloud runtimes may omit it); the
      // tmux runtime implements it, so assert-non-null to invoke.
      expect(sandbox.spawn).toBeTypeOf('function');
      const proc = await sandbox.spawn!('cat');
      proc.stdin.write('ping\n');
      proc.stdin.end();
      const out = await readUntil(proc.stdout, (acc) => acc.includes('ping'));
      expect(out).toContain('ping');
      proc.kill();
    });
  });

  it('spawn streams stdout incrementally (before the process exits)', async () => {
    await withSandbox(async (sandbox) => {
      // Emit a marker, then sleep — a run-to-completion API would block here,
      // but spawn hands back the live stream so the marker is readable now.
      const proc = await sandbox.spawn!('printf "streamed-line\\n"; sleep 5');
      try {
        const seen = await readUntil(proc.stdout, (acc) => acc.includes('streamed-line'));
        expect(seen).toContain('streamed-line');
      } finally {
        proc.kill();
      }
    });
  });

  it('spawn stdout carries clean raw bytes (no PTY carriage-return mangling)', async () => {
    await withSandbox(async (sandbox) => {
      // The transport must hand the stream-json normalizer the process's OWN
      // bytes — `\n`-terminated lines, not the pane's `\r\n` TTY rendering.
      const proc = await sandbox.spawn!('printf "a\\nb\\nc\\n"');
      const out = await collect(proc.stdout);
      expect(out).toBe('a\nb\nc\n');
    });
  });

  it('spawn injects env vars into the spawned process', async () => {
    await withSandbox(async (sandbox) => {
      const proc = await sandbox.spawn!('printf "%s\\n" "$ORCA_TMUX_TEST"', {
        env: { ORCA_TMUX_TEST: 'from-env' },
      });
      const out = await readUntil(proc.stdout, (acc) => acc.includes('from-env'));
      expect(out).toContain('from-env');
      proc.kill();
    });
  });

  it('kill terminates a long-lived process (stdout ends)', async () => {
    await withSandbox(async (sandbox) => {
      const proc = await sandbox.spawn!('sleep 30');
      const ended = new Promise<void>((resolve) => {
        proc.stdout.on('close', () => resolve());
        proc.stdout.on('end', () => resolve());
      });
      proc.kill('SIGKILL');
      await Promise.race([
        ended,
        new Promise<void>((_, reject) =>
          setTimeout(() => reject(new Error('stdout did not end after kill()')), 8000),
        ),
      ]);
    });
  }, 15_000);

  it('stdin end() delivers a real EOF: a read-to-EOF child terminates on its own', async () => {
    await withSandbox(async (sandbox) => {
      // `cat` copies stdin to stdout until EOF. Over the local-attach transport
      // `end()` types a `Ctrl-D`, which the pane's line discipline raises as EOF —
      // so `cat` exits WITHOUT a `kill`, and its stdout ends naturally (the FIFO
      // reader hits EOF and `collect` resolves). This pins that `end()` is a
      // genuine best-effort end-of-input, not just a local close.
      const proc = await sandbox.spawn!('cat');
      // `collect` drains stdout to EOF in flowing mode; capturing it as a promise
      // keeps the stream flowing so `end`/`close` actually fires. A paused FIFO
      // stream would never end, so a `kill`-free termination could not be observed.
      const collected = collect(proc.stdout);
      proc.stdin.write('line-before-eof\n');
      // Give the line time to round-trip so the pane is at a line boundary (empty
      // input buffer) when the EOF keystroke lands — the case where `C-d` raises
      // EOF rather than flushing a partial line — then close stdin.
      await delayMs(400);
      proc.stdin.end();
      // No proc.kill(): the child must terminate purely from the EOF, ending its
      // stdout (so `collect` resolves) and echoing the line it read.
      const out = await collected;
      expect(out).toContain('line-before-eof');
    });
  }, 15_000);

  it('stdin end() does not throw when the child has already exited', async () => {
    await withSandbox(async (sandbox) => {
      // A short-lived child that exits before we close stdin: the EOF keystroke
      // then targets a now-gone session. `end()` must still resolve cleanly (the
      // missing-session error is swallowed), never surfacing on `finish`/`error`.
      const proc = await sandbox.spawn!('printf "quick\\n"');
      // Drain stdout to EOF (a paused FIFO stream never ends); this also proves the
      // child ran to completion before we close stdin.
      const out = await collect(proc.stdout);
      expect(out).toContain('quick');
      const finished = new Promise<void>((resolve, reject) => {
        proc.stdin.on('finish', () => resolve());
        proc.stdin.on('error', reject);
      });
      expect(() => proc.stdin.end()).not.toThrow();
      await finished; // end() completed its _final without erroring
    });
  }, 15_000);

  it('stdin end() flushes + terminates a block-buffered (non-line-flushing) child', async () => {
    await withSandbox(async (sandbox) => {
      // `tr a-z A-Z` block-buffers when its stdout is a pipe (which it is here —
      // the pane pipes through `tee` into the FIFO), so it emits NOTHING per line;
      // its output only appears once it flushes at EOF. This is the documented
      // trade-off of reading clean raw bytes off a FIFO (vs. a TTY). The assertion
      // is the acceptable contract: the child's own bytes arrive intact and in
      // order once `end()`'s `Ctrl-D` raises EOF, which flushes tr and exits it.
      const proc = await sandbox.spawn!('tr a-z A-Z');
      const collected = collect(proc.stdout); // reads to EOF (stream end)
      proc.stdin.write('hello\nworld\n');
      proc.stdin.end();
      // No kill: EOF alone must flush the buffered output AND end tr, closing the
      // FIFO so `collect` resolves. Uppercased, newline-clean, no `\r` mangling.
      expect(await collected).toBe('HELLO\nWORLD\n');
    });
  }, 15_000);

  it('run executes a one-shot bash command to completion', async () => {
    await withSandbox(async (sandbox) => {
      const res = await sandbox.run({ tool: 'bash', args: { command: 'echo hello-run' } });
      expect(res.exit_code).toBe(0);
      expect(res.stdout ?? '').toContain('hello-run');
    });
  }, 15_000);

  it('run surfaces a non-zero exit code', async () => {
    await withSandbox(async (sandbox) => {
      const res = await sandbox.run({ tool: 'bash', args: { command: 'exit 3' } });
      expect(res.exit_code).toBe(3);
    });
  }, 15_000);

  it('run kills a command that overruns its timeout and reports exit code 124', async () => {
    await withSandbox(async (sandbox) => {
      // `sleep 30` never completes within the 200ms budget, so the poll loop
      // breaks on the deadline, kills the session, and returns the Local-runtime
      // timeout convention (124) with an explanatory stderr marker.
      const res = await sandbox.run({
        tool: 'bash',
        args: { command: 'sleep 30', timeout_ms: 200 },
      });
      expect(res.exit_code).toBe(124);
      expect(res.stderr ?? '').toContain('timed out after 200ms');
    });
  }, 15_000);

  it('run glob lists matching files under the work-dir', async () => {
    await withSandbox(async (sandbox) => {
      await sandbox.files.write('a.txt', Buffer.from('1'));
      await sandbox.files.write('b.txt', Buffer.from('2'));
      await sandbox.files.write('c.md', Buffer.from('3'));
      const res = await sandbox.run({ tool: 'glob', args: { pattern: '*.txt' } });
      const matches = res.output as string[];
      expect([...matches].sort()).toEqual(['a.txt', 'b.txt']);
    });
  }, 15_000);

  it('run grep returns matching lines under the work-dir', async () => {
    await withSandbox(async (sandbox) => {
      await sandbox.files.write('haystack.txt', Buffer.from('alpha\nneedle-here\nbeta\n'));
      const res = await sandbox.run({ tool: 'grep', args: { pattern: 'needle-here' } });
      expect(String(res.output)).toContain('needle-here');
    });
  }, 15_000);

  it('run reports an unknown tool', async () => {
    await withSandbox(async (sandbox) => {
      const res = await sandbox.run({ tool: 'nope', args: {} });
      expect(res.exit_code).toBe(127);
      expect(res.stderr ?? '').toContain('unknown tool');
    });
  });

  it('files reads back what it writes on the pane host fs', async () => {
    await withSandbox(async (sandbox) => {
      await sandbox.files.write('sub/note.txt', Buffer.from('tmux-fs'));
      const back = await sandbox.files.read('sub/note.txt');
      expect(back.toString('utf8')).toBe('tmux-fs');
      const entries = await sandbox.files.list('sub');
      expect(entries).toContain('note.txt');
      await sandbox.files.delete('sub/note.txt');
      await expect(sandbox.files.read('sub/note.txt')).rejects.toBeTruthy();
    });
  });

  it('pause/resume are no-ops that preserve the sandbox', async () => {
    await withSandbox(async (sandbox) => {
      await sandbox.files.write('keep.txt', Buffer.from('v1'));
      await sandbox.pause();
      await sandbox.resume();
      const back = await sandbox.files.read('keep.txt');
      expect(back.toString('utf8')).toBe('v1');
    });
  });

  it('runPrivileged throws (the tmux transport has no privilege boundary)', async () => {
    await withSandbox(async (sandbox) => {
      await expect(sandbox.runPrivileged!('mkdir -p /mnt/x')).rejects.toThrowError(/privileged/);
    });
  });

  it('destroy is idempotent and rejects further spawns', async () => {
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    await sandbox.destroy();
    await sandbox.destroy(); // second destroy must not throw
    await expect(sandbox.spawn!('echo hi')).rejects.toThrowError(/destroyed/);
  });
});
