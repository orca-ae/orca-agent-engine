// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// {@link NativeCliProcess} stubs for the ways a native CLI FAILS.
//
// `launchNativeCli` returns synchronously but spawns asynchronously, so a launch that
// fails (a missing / misnamed binary on the sandbox PATH — the likeliest production
// misconfiguration for every native-CLI provider) surfaces only on the returned object:
// `lines()` ends immediately with no output, `done` resolves, and the error is carried on
// `failure`. That empty line stream is byte-for-byte what a CLI that started and exited
// straight away produces, so `failure` is the ONLY thing telling the two apart.
//
// The same blindness applies AFTER a successful launch: a CLI that finished its work, one
// that segfaulted, and one the harness killed on teardown all end the line stream
// identically — only {@link NativeCliProcess.exit} tells them apart. So these stubs script
// each of those endings too, letting a provider's turn-terminal behavior be driven without
// a sandbox runtime whose `spawn` has to be made to misbehave.
//
// Injected via each provider harness's `launch` option.

import type { NativeCliExit, NativeCliProcess } from '../../../src/sandbox/native-cli-launcher.js';

/** How a scripted CLI ENDS, minus the `requested` flag the stub tracks itself. */
export type ScriptedCliExit = Pick<NativeCliExit, 'code' | 'signal'>;

/** The ending of a scripted CLI that was not asked to die abnormally. */
const CLEAN_EXIT: ScriptedCliExit = { code: 0, signal: null };

/** How a scripted CLI reacts to one frame written to its stdin. */
export type ScriptedCliReply =
  /** Stdout lines to emit in reply (each written to the line stream verbatim). */
  | string[]
  /** End stdout: the child died / exited without answering this frame. */
  | 'end'
  /** Say nothing at all — a live-but-MUTE child that took the frame and went quiet. */
  | undefined;

/** Options for {@link scriptedNativeCli}. */
export interface ScriptedNativeCliOptions {
  /**
   * Called for each newline-terminated frame the harness writes to stdin, returning what
   * the CLI does about it. `ordinal` is the frame's 1-based position in the whole session,
   * so a script can make the SECOND frame behave differently from the first. Omitted → the
   * CLI answers NOTHING, ever (a mute child).
   */
  respond?: (frame: string, ordinal: number) => ScriptedCliReply;
  /** Stdout lines emitted as soon as the line stream is consumed (a banner / greeting). */
  initial?: string[];
  /** The status reported on {@link NativeCliProcess.exit} once stdout ends. */
  exit?: ScriptedCliExit;
}

/**
 * A launched-and-live {@link NativeCliProcess} whose stdout is driven by a script.
 *
 * The one stub behind every "the CLI misbehaved" shape: it starts cleanly (`failure`
 * resolves `undefined`), streams whatever `respond` returns for each stdin frame, and
 * reports `exit` once its stdout ends — as a REQUESTED exit when the harness `kill()`ed
 * or `end()`ed it, and as the scripted (possibly abnormal) status otherwise.
 */
export function scriptedNativeCli(opts: ScriptedNativeCliOptions = {}): NativeCliProcess {
  return scriptedNativeCliWithDeath(opts).cli;
}

/**
 * {@link scriptedNativeCli} plus a `die()` the TEST fires — an UNREQUESTED death at a moment
 * of the test's choosing.
 *
 * Every other stub ties the child's death to a stdin frame, so it can only die DURING a
 * turn. A CLI that dies while the session is IDLE (an OOM between turns, a one-shot child
 * that finished and exited) has no frame to hang off, and that is exactly the case a read
 * loop must not report as the NEXT turn's fault.
 */
export function scriptedNativeCliWithDeath(opts: ScriptedNativeCliOptions = {}): {
  cli: NativeCliProcess;
  die: () => void;
  /**
   * Push a stdout line at a moment of the TEST's choosing — the child speaking with no frame
   * to hang it off (an answer to a turn the harness has already given up on, arriving while
   * no turn is in flight).
   *
   * A NO-OP once stdout has ended, exactly as a real child's output is once it is dead. That
   * is load-bearing rather than incidental: it is what makes "the abandoned turn's answer can
   * never reach the next turn" an observable property of the harness killing the child, and
   * not of the fixture declining to speak.
   */
  say: (line: string) => void;
  /** Every frame the harness wrote to stdin, in order. */
  frames: string[];
} {
  const scriptedExit = opts.exit ?? CLEAN_EXIT;
  const frames: string[] = [];
  const queue: string[] = [...(opts.initial ?? [])];
  let waiting: ((r: IteratorResult<string>) => void) | undefined;
  let ended = false;
  let requested = false;
  let resolveExit!: (e: NativeCliExit) => void;
  const exit = new Promise<NativeCliExit>((resolve) => {
    resolveExit = resolve;
  });

  const endStream = (): void => {
    if (ended) {
      return;
    }
    ended = true;
    const resolver = waiting;
    waiting = undefined;
    resolver?.({ value: undefined, done: true });
    resolveExit({ ...scriptedExit, requested });
  };
  const push = (line: string): void => {
    if (ended) {
      return;
    }
    const resolver = waiting;
    if (resolver !== undefined) {
      waiting = undefined;
      resolver({ value: line, done: false });
      return;
    }
    queue.push(line);
  };

  const cli: NativeCliProcess = {
    lines: () => ({
      [Symbol.asyncIterator]: (): AsyncIterator<string> => ({
        next: (): Promise<IteratorResult<string>> => {
          const head = queue.shift();
          if (head !== undefined) {
            return Promise.resolve({ value: head, done: false });
          }
          if (ended) {
            return Promise.resolve({ value: undefined, done: true });
          }
          return new Promise<IteratorResult<string>>((resolve) => {
            waiting = resolve;
          });
        },
      }),
    }),
    write: (chunk: string | Buffer): boolean => {
      if (ended) {
        return false;
      }
      for (const frame of String(chunk).split('\n')) {
        if (frame.trim().length === 0) {
          continue;
        }
        frames.push(frame);
        const reply = opts.respond?.(frame, frames.length);
        if (reply === 'end') {
          // The frame was accepted and THEN the child ended — a crash mid-turn. Deferred
          // by a macrotask so the write itself returns first, as a real child's death does.
          setImmediate(endStream);
          return true;
        }
        for (const line of reply ?? []) {
          push(line);
        }
      }
      return true;
    },
    // Closing stdin is NOT a requested TERMINATION: the real `SpawnBackedNativeCli.end()`
    // only closes the child's stdin, and `killedByUs` is set solely by `kill()`. A stub
    // that also flipped `requested` here was MORE LENIENT than production — a CLI that
    // died after its stdin closed would report a requested exit in tests and an abnormal
    // one in the field, hiding exactly the fault the exit status exists to report.
    end: (): void => {
      endStream();
    },
    kill: (): void => {
      requested = true;
      endStream();
    },
    done: exit.then(() => undefined),
    failure: Promise.resolve(undefined),
    exit,
  };
  // `die` ends stdout WITHOUT setting `requested` — the child was not asked to stop, so the
  // scripted (abnormal) status stands.
  return { cli, die: endStream, say: push, frames };
}

/**
 * A live-but-MUTE {@link NativeCliProcess}: it starts cleanly and accepts stdin, but never
 * emits a single stdout line and never exits — a CLI that took the frame and went quiet (a
 * protocol mismatch, a wedged child). NOTHING on the stream reports that state, which is
 * why only a request deadline can end a wait on it. `kill()` / `end()` release the line
 * stream so a test can still unwind the harness.
 */
export function muteNativeCli(): NativeCliProcess {
  return scriptedNativeCli({ exit: { code: null, signal: 'SIGTERM' } });
}

/**
 * A CLI that accepted the turn frame and then DIED — a segfault / OOM-kill / non-zero
 * abort mid-turn. Its stdout ends with no terminal frame, exactly like a clean exit, so
 * only the scripted `exit` status tells the two apart.
 */
export function dyingNativeCli(exit: ScriptedCliExit): NativeCliProcess {
  return scriptedNativeCli({ respond: () => 'end', exit });
}

/** A never-started {@link NativeCliProcess} whose launch failed with `err`. */
export function failedNativeCli(err: Error): NativeCliProcess {
  return {
    // A failed launch produced no stdout at all: an already-ended line stream.
    lines: () => ({
      [Symbol.asyncIterator]: (): AsyncIterator<string> => ({
        next: () => Promise.resolve({ value: undefined, done: true }),
      }),
    }),
    write: () => false,
    end: () => {},
    kill: () => {},
    done: Promise.resolve(),
    failure: Promise.resolve(err),
    // The process never ran, so there is no exit status; `failure` is its whole story.
    exit: Promise.resolve(undefined),
  };
}
