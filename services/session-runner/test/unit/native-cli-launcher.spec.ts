// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// NativeCliLauncher — the provider-agnostic native-CLI launch framework.
//
// A native-CLI provider (Codex / Claude Code / Cursor, each wired per-CLI)
// boots its CLI as a long-lived child that speaks newline-delimited JSON
// ("stream-json") over stdio. The launcher is the transport-neutral core of that:
// given a launch config {cmd, args, env, cwd}, it boots the CLI inside ANY
// `SandboxHandle` via the extracted `spawn` primitive and exposes
//   - an async-iterable of stdout LINES (fed to a per-CLI stream-json normalizer),
//   - a stdin writer (the provider pushes user turns / control messages),
//   - lifecycle (kill on session end).
// It knows nothing about any specific CLI's arg grammar or JSON schema — those are
// the per-CLI adapters. This suite proves the transport with a FAKE CLI
// (a tiny stream-json echo script), driven through TWO real runtimes:
//   - InMemorySandboxRuntime (fast, no tmux) — proves provider-agnosticism, and
//   - TmuxSandboxRuntime (the local-attach transport) — the real target.
// The tmux leg self-skips when the `tmux` binary is absent.

import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { describe, it, expect, vi } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { TmuxSandboxRuntime } from '../../src/sandbox/tmux-sandbox.js';
import {
  cliExitFaultMessage,
  DEFAULT_TURN_DEADLINE_MS,
  EXIT_STATUS_GRACE_MS,
  launchNativeCli,
  type NativeCliExit,
  type NativeCliProcess,
} from '../../src/sandbox/native-cli-launcher.js';
import type { SandboxRuntime } from '../../src/sandbox/seam.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const FAKE_CLI = join(HERE, 'support', 'fake-native-cli.mjs');

function tmuxAvailable(): boolean {
  try {
    execFileSync('tmux', ['-V'], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

/** Collect the first `n` parsed JSON objects off the launcher's line stream. */
async function takeLines(
  lines: AsyncIterable<string>,
  n: number,
): Promise<Array<Record<string, unknown>>> {
  const out: Array<Record<string, unknown>> = [];
  for await (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      continue;
    }
    out.push(JSON.parse(trimmed) as Record<string, unknown>);
    if (out.length >= n) {
      break;
    }
  }
  return out;
}

/** The launch config that runs the fake stream-json CLI under `node`. */
function fakeCliConfig(): { cmd: string; args: string[] } {
  return { cmd: process.execPath, args: [FAKE_CLI] };
}

/**
 * The `SpawnHandle.exited` channel for a hand-rolled stdout double: settles with a clean
 * exit once the stream ends. Every real runtime supplies it (it is how the launcher tells
 * a crash apart from a clean exit), so the doubles here honor the same contract rather
 * than leaving the launcher reading `undefined.then`.
 */
function exitedOnEnd(
  stdout: NodeJS.ReadableStream,
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
  return new Promise((resolve) => {
    stdout.once('end', () => resolve({ code: 0, signal: null }));
    stdout.once('close', () => resolve({ code: 0, signal: null }));
  });
}

interface RuntimeCase {
  name: string;
  make: () => SandboxRuntime;
  enabled: boolean;
}

const cases: RuntimeCase[] = [
  { name: 'InMemorySandboxRuntime', make: () => new InMemorySandboxRuntime(), enabled: true },
  { name: 'TmuxSandboxRuntime', make: () => new TmuxSandboxRuntime(), enabled: tmuxAvailable() },
];

for (const rc of cases) {
  const maybe = rc.enabled ? describe : describe.skip;
  maybe(`launchNativeCli — over ${rc.name}`, () => {
    it('streams the CLI stdout as JSON lines (emits the boot line first)', async () => {
      const runtime = rc.make();
      const sandbox = await runtime.acquire({});
      const cli = launchNativeCli(sandbox, fakeCliConfig());
      try {
        const [first] = await takeLines(cli.lines(), 1);
        expect(first).toBeDefined();
        expect(first!['type']).toBe('ready');
      } finally {
        cli.kill();
        await sandbox.destroy();
      }
    }, 20_000);

    it('drives the CLI over stdin and reads its streamed replies', async () => {
      const runtime = rc.make();
      const sandbox = await runtime.acquire({});
      const cli = launchNativeCli(sandbox, fakeCliConfig());
      try {
        const iter = cli.lines()[Symbol.asyncIterator]();
        // Consume the boot line so the next reads observe replies to our input.
        // `lines()` yields raw line STRINGS (the normalizer parses them), so the
        // test parses here.
        const boot = await iter.next();
        expect(JSON.parse((boot.value as string).trim())['type']).toBe('ready');

        cli.write('alpha\n');
        cli.write('beta\n');

        const seen: Array<Record<string, unknown>> = [];
        while (seen.length < 2) {
          const next = await iter.next();
          if (next.done) {
            break;
          }
          const obj = JSON.parse((next.value as string).trim()) as Record<string, unknown>;
          if (obj['type'] === 'reply') {
            seen.push(obj);
          }
        }
        expect(seen.map((o) => o['text'])).toEqual(['alpha', 'beta']);
      } finally {
        cli.kill();
        await sandbox.destroy();
      }
    }, 20_000);

    it('ends the line stream when the CLI exits, and resolves done', async () => {
      const runtime = rc.make();
      const sandbox = await runtime.acquire({});
      const cli = launchNativeCli(sandbox, fakeCliConfig());
      try {
        // Tell the fake CLI to exit (`bye` → it prints `done` then exits 0).
        cli.write('bye\n');
        const collected: Array<Record<string, unknown>> = [];
        for await (const line of cli.lines()) {
          const trimmed = line.trim();
          if (trimmed.length > 0) {
            collected.push(JSON.parse(trimmed) as Record<string, unknown>);
          }
        }
        // The stream terminated on its own (the loop completed), and a `done`
        // line was observed before EOF.
        expect(collected.some((o) => o['type'] === 'done')).toBe(true);
        await cli.done;
      } finally {
        await sandbox.destroy();
      }
    }, 20_000);

    it('end() closes stdin as EOF: a read-to-EOF child exits, line stream ends, done resolves', async () => {
      // The launcher's `end()` closes the child's stdin. On InMemory that is a
      // real `child_process` stdin end; on tmux it is a typed `Ctrl-D`. Both must
      // deliver EOF to a child that reads its stdin to EOF — proven with `cat`
      // (transport-neutral: the launcher only streams stdout lines, and knows
      // nothing of stream-json). `end()` alone (no `kill`) terminates the child.
      const runtime = rc.make();
      const sandbox = await runtime.acquire({});
      const cli = launchNativeCli(sandbox, { cmd: 'cat' });
      try {
        cli.write('echo-me\n');
        // Confirm the line round-tripped (child is up + reading) before EOF, so
        // the pane is at a line boundary when the tmux `Ctrl-D` lands.
        const iter = cli.lines()[Symbol.asyncIterator]();
        const first = await iter.next();
        expect((first.value as string).trim()).toBe('echo-me');
        cli.end();
        // Drain the rest: the loop must COMPLETE (EOF ended cat) without a kill.
        const drained = (async () => {
          for await (const _line of { [Symbol.asyncIterator]: () => iter }) {
            void _line;
          }
        })();
        await Promise.race([
          drained,
          new Promise<void>((_, reject) =>
            setTimeout(() => reject(new Error('line stream did not end after end()')), 8000),
          ),
        ]);
        await cli.done; // stdout ended → the process is done
      } finally {
        cli.kill(); // belt-and-suspenders on the (unexpected) timeout path
        await sandbox.destroy();
      }
    }, 20_000);

    it('kill terminates the CLI (line stream ends promptly)', async () => {
      const runtime = rc.make();
      const sandbox = await runtime.acquire({});
      const cli = launchNativeCli(sandbox, fakeCliConfig());
      try {
        const iter = cli.lines()[Symbol.asyncIterator]();
        // Read the boot line so the process is definitely up.
        await iter.next();
        cli.kill('SIGKILL');
        // Draining the rest must terminate (no hang) once the child is killed.
        const drained = (async () => {
          for await (const _line of { [Symbol.asyncIterator]: () => iter }) {
            void _line; // discard
          }
        })();
        await Promise.race([
          drained,
          new Promise<void>((_, reject) =>
            setTimeout(() => reject(new Error('line stream did not end after kill()')), 8000),
          ),
        ]);
      } finally {
        await sandbox.destroy();
      }
    }, 20_000);
  });
}

describe('launchNativeCli — provider-agnostic contract', () => {
  it('rejects a sandbox that does not support spawn()', () => {
    // A cloud-only handle may omit `spawn`; a native-CLI launch cannot proceed
    // without it, so the launcher fails fast rather than silently no-op.
    const noSpawn = { id: 'sbx_nospawn' } as unknown as Parameters<typeof launchNativeCli>[0];
    expect(() => launchNativeCli(noSpawn, fakeCliConfig())).toThrowError(/spawn/);
  });

  it('carries a failed spawn on `failure` (an empty line stream cannot report it)', async () => {
    // `spawn` rejecting — a missing / misnamed binary on the sandbox PATH, the likeliest
    // production misconfiguration — ends the process before it began: `lines()` completes
    // with no output and `done` resolves, which is byte-for-byte indistinguishable from a
    // CLI that started and exited immediately. `failure` is the only channel that tells the
    // reader which happened, so the provider can report a launch fault instead of ending
    // the turn as a clean, silent no-op.
    const spawnError = new Error('spawn claude ENOENT');
    const sandbox = {
      id: 'sbx_enoent',
      spawn: () => Promise.reject(spawnError),
    } as unknown as Parameters<typeof launchNativeCli>[0];

    const cli = launchNativeCli(sandbox, fakeCliConfig());
    await expect(cli.failure).resolves.toBe(spawnError);
    // …and the rest of the surface still settles cleanly (no unhandled rejection anywhere).
    await expect(cli.done).resolves.toBeUndefined();
    const lines: string[] = [];
    for await (const line of cli.lines()) {
      lines.push(line);
    }
    expect(lines).toEqual([]);
    expect(cli.write('ignored\n')).toBe(false);
  });

  it('resolves `failure` with undefined once the CLI actually started', async () => {
    // The negative half: a successful launch must not look like a fault, or every read
    // loop would report a spurious `agent.error` on a perfectly healthy CLI.
    const { Readable, Writable: NodeWritable } = await import('node:stream');
    const stdout = new Readable({ read() {} });
    const stdin = new NodeWritable({ write: (_c, _e, cb) => cb() });
    const sandbox = {
      id: 'sbx_ok',
      async spawn() {
        return { stdout, stdin, kill: () => stdout.push(null), exited: exitedOnEnd(stdout) };
      },
    } as unknown as Parameters<typeof launchNativeCli>[0];

    const cli = launchNativeCli(sandbox, fakeCliConfig());
    await expect(cli.failure).resolves.toBeUndefined();
    stdout.push(null);
  });

  it('splits stdout on \\n only — a JSON frame with embedded U+2028/U+2029 stays ONE line', async () => {
    // The pi RPC contract requires `\n`-only framing: Node `readline` ALSO splits on the Unicode
    // line separators U+2028 / U+2029, which are valid inside JSON strings, so a generic line reader
    // would corrupt any frame carrying them. Prove the launcher delivers such a frame intact, and
    // strips a trailing `\r` on a `\r\n`-terminated line.
    const { Readable, Writable: NodeWritable } = await import('node:stream');
    const stdout = new Readable({ read() {} });
    const stdin = new NodeWritable({ write: (_c, _e, cb) => cb() });
    const sandbox = {
      id: 'sbx_u2028',
      async spawn() {
        return { stdout, stdin, kill: () => stdout.push(null), exited: exitedOnEnd(stdout) };
      },
    } as unknown as Parameters<typeof launchNativeCli>[0];

    const cli = launchNativeCli(sandbox, fakeCliConfig());
    const iter = cli.lines()[Symbol.asyncIterator]();
    // A JSON object whose string value contains U+2028 and U+2029, then a real `\n` terminator.
    const frame = JSON.stringify({ type: 'message', text: 'a b c' });
    // Emit the frame split ACROSS chunks (proving carry-buffer reassembly), then a `\r\n` line.
    stdout.push(frame.slice(0, 10));
    stdout.push(`${frame.slice(10)}\n`);
    stdout.push('{"type":"crlf"}\r\n');
    stdout.push(null);

    const first = await iter.next();
    expect(first.value).toBe(frame); // ONE line — the separators did NOT split it.
    expect(JSON.parse(first.value as string).text).toBe('a b c');
    const second = await iter.next();
    expect(second.value).toBe('{"type":"crlf"}'); // trailing `\r` stripped.
    const third = await iter.next();
    expect(third.done).toBe(true);
  });

  it('buffers stdin written before the async spawn resolves, replaying it in order', async () => {
    // The NativeCliProcess is synchronous but `spawn` is async; a write issued in
    // the same tick as the launch must not be lost — it is queued and replayed
    // once the SpawnHandle is live. Proven here with a hand-rolled sandbox whose
    // `spawn` resolves on a later tick.
    const writes: string[] = [];
    let endStream: (() => void) | undefined;
    const { Readable, Writable: NodeWritable } = await import('node:stream');
    const stdout = new Readable({ read() {} });
    const stdin = new NodeWritable({
      write(chunk, _enc, cb) {
        writes.push(Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk));
        cb();
      },
    });
    const sandbox = {
      id: 'sbx_deferred',
      async spawn() {
        // Resolve on a macrotask so a same-tick write lands in the pending queue.
        await new Promise((r) => setTimeout(r, 10));
        endStream = () => stdout.push(null);
        return {
          stdout,
          stdin,
          kill: () => stdout.push(null),
          exited: exitedOnEnd(stdout),
        };
      },
    } as unknown as Parameters<typeof launchNativeCli>[0];

    const cli = launchNativeCli(sandbox, fakeCliConfig());
    // Consume the line stream in the background so `stdout` drains + `done` can
    // resolve once we end it (a Readable only emits `end` after it is read).
    const drained = (async () => {
      for await (const _line of cli.lines()) {
        void _line;
      }
    })();
    // Write BEFORE spawn has resolved — must be buffered, not dropped.
    cli.write('early-line\n');
    // Let spawn resolve + the queue flush.
    await new Promise((r) => setTimeout(r, 30));
    expect(writes).toEqual(['early-line\n']);
    endStream?.();
    await cli.done;
    await drained;
  });
});

/**
 * A {@link NativeCliProcess} whose stdout has ENDED but whose `exit` is scripted — the
 * exact state a read loop's `finally` runs in. Everything else is inert: these tests only
 * exercise {@link cliExitFaultMessage}, which reads `exit` and nothing else.
 */
function cliWithExit(exit: Promise<NativeCliExit | undefined>): NativeCliProcess {
  return {
    lines: () => ({
      [Symbol.asyncIterator]: (): AsyncIterator<string> => ({
        next: () => Promise.resolve({ value: undefined, done: true }),
      }),
    }),
    write: () => false,
    end: () => {},
    kill: () => {},
    done: Promise.resolve(),
    failure: Promise.resolve(undefined),
    exit,
  };
}

describe('cliExitFaultMessage — the exit-status grace', () => {
  // The grace is the ONLY thing keeping a read loop's `finally` from parking forever on a
  // child that closed stdout and then LINGERED (never exits, never killed). That `finally`
  // is what releases the turn and ends the event stream, so an unbounded wait there is a
  // hang — the very defect the exit status exists to REPORT. Nothing measured it: every
  // scripted CLI fixture resolves `exit` inside the same call that ends its line stream, so
  // `exit` is always already settled and dropping the bound entirely stayed green.
  it('gives up on an exit status that never arrives, rather than parking the caller forever', async () => {
    vi.useFakeTimers();
    try {
      // An `exit` that NEVER settles: the lingering child.
      const cli = cliWithExit(new Promise<NativeCliExit | undefined>(() => undefined));
      let state: NativeCliExit | string | undefined | 'pending' = 'pending';
      const fault = cliExitFaultMessage(cli, 'claude-code', 'mid-turn').then((v) => {
        state = v;
        return v;
      });

      // Just BEFORE the grace it is still waiting — the bound is a real wait on the exit
      // status, not an unconditional give-up that would discard every crash report.
      await vi.advanceTimersByTimeAsync(EXIT_STATUS_GRACE_MS - 1);
      expect(state, 'the grace must not expire early').toBe('pending');

      // Past it, the caller is released with nothing to report, and the read loop goes on
      // to release the turn and end the stream. Asserted on the RECORDED state rather than
      // by awaiting `fault`, so a lost bound fails with this sentence instead of hanging
      // the suite until vitest's own timeout.
      await vi.advanceTimersByTimeAsync(EXIT_STATUS_GRACE_MS + 1);
      expect(
        state,
        'past the grace the caller MUST be released — an unbounded wait here parks the read ' +
          'loop`s finally, so the turn never reaches its terminal marker',
      ).not.toBe('pending');
      expect(state).toBeUndefined();
      void fault;
    } finally {
      vi.useRealTimers();
    }
  });

  it('reports a crash that arrives INSIDE the grace (the bound never eats a real status)', async () => {
    vi.useFakeTimers();
    try {
      let settle!: (e: NativeCliExit) => void;
      const cli = cliWithExit(
        new Promise<NativeCliExit | undefined>((resolve) => {
          settle = resolve as (e: NativeCliExit) => void;
        }),
      );
      const fault = cliExitFaultMessage(cli, 'claude-code', 'mid-turn');
      await vi.advanceTimersByTimeAsync(EXIT_STATUS_GRACE_MS - 1);
      settle({ code: null, signal: 'SIGKILL', requested: false });
      await expect(fault).resolves.toBe(
        'claude-code CLI was killed by SIGKILL before the turn completed',
      );
    } finally {
      vi.useRealTimers();
    }
  });

  // Why every caller wraps this in `.catch(() => undefined)`: a REJECTING `exit` throws out
  // of the read loop's `finally` BEFORE the turn is released and the stream ended — the hang
  // reintroduced through its own fix.
  it('REJECTS when the exit status itself rejects — the reason its callers must catch', async () => {
    const cli = cliWithExit(Promise.reject(new Error('spawn handle exploded')));
    await expect(cliExitFaultMessage(cli, 'claude-code', 'mid-turn')).rejects.toThrow(
      'spawn handle exploded',
    );
  });

  // An IDLE death is a real fault with no turn to attribute it to, and every read loop now
  // REPORTS it rather than dropping it — so the sentence had to stop claiming a turn was cut
  // short. Both wordings are pinned: same fault, different timing.
  it('names the IDLE timing when nothing was in flight, rather than claiming a cut-short turn', async () => {
    const cli = cliWithExit(Promise.resolve({ code: 137, signal: null, requested: false }));
    await expect(cliExitFaultMessage(cli, 'claude-code', 'idle')).resolves.toBe(
      'claude-code CLI exited with code 137 while the session was idle',
    );
    await expect(cliExitFaultMessage(cli, 'claude-code', 'mid-turn')).resolves.toBe(
      'claude-code CLI exited with code 137 before the turn completed',
    );
  });
});

describe('DEFAULT_TURN_DEADLINE_MS — the production bound', () => {
  // The value was a promise made ONLY in a comment. No test referenced the constant, and
  // every cell that reaches a deadline overrides `turnTimeoutMs` to 60ms — so
  // `DEFAULT_TURN_DEADLINE_MS = 2_000`, a two-second production bound that would truncate
  // essentially every real agentic turn, passed the entire suite unchanged.
  it('is far enough above a real agentic turn to bound a WEDGE, not the work', () => {
    // A tool-heavy turn runs single-digit to tens of minutes, so anything under ten minutes
    // bounds legitimate work rather than a wedge and would cut healthy turns short.
    expect(
      DEFAULT_TURN_DEADLINE_MS,
      'a bound this short truncates legitimate turns; it must sit above the longest real one',
    ).toBeGreaterThanOrEqual(10 * 60_000);
    // It must still RELEASE: an effectively-unbounded default is the hang this constant
    // exists to end (a parked turn also pins `activeTurns`, holding the idle watchdog open).
    expect(
      DEFAULT_TURN_DEADLINE_MS,
      'a bound this long is indistinguishable from none for an operator waiting on it',
    ).toBeLessThanOrEqual(60 * 60_000);
  });
});
