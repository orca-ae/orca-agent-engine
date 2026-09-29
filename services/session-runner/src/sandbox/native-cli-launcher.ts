// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// NativeCliLauncher — the provider-agnostic native-CLI launch framework.
//
// The runner drives two kinds of harness: in-process providers (the model runs
// inside the runner) and native-CLI providers, which boot a real coding CLI
// (Claude Code / Codex / Cursor / Pi / custom, each wired by its own provider) as
// a long-lived child that speaks newline-delimited JSON — "stream-json" — over its
// stdio. This module is the transport-neutral core of the native-CLI path: given a
// launch config {cmd, args, env, cwd}, it boots the CLI inside ANY
// {@link SandboxHandle} through the extracted streaming {@link SandboxHandle.spawn}
// primitive and exposes exactly three things a provider needs:
//   - {@link NativeCliProcess.lines}: an async-iterable of stdout LINES, to be fed
//     to a per-CLI stream-json normalizer (which turns each JSON line into the
//     runner's transport-neutral agent events);
//   - {@link NativeCliProcess.write}: a stdin writer, so the provider pushes user
//     turns / control messages into the CLI;
//   - lifecycle: {@link NativeCliProcess.kill} (terminate on session end), a
//     {@link NativeCliProcess.done} promise that resolves when stdout ends, and a
//     {@link NativeCliProcess.failure} promise carrying the spawn error when the CLI
//     never started (so a missing binary is a reported fault, not an empty stream).
//
// It is deliberately provider-agnostic: it knows NOTHING about any specific CLI's
// argument grammar or JSON schema. Per-CLI argument builders and stream-json
// normalizers are separate per-provider adapters that sit on top of this launcher. By
// composing against `SandboxHandle` (not a concrete runtime) the same launcher
// drives a CLI over the InMemory runtime (dev/tests), the tmux transport (local
// attach), or the srt-wrapped Local runtime — one launch path for all of them.
//
// Line framing: stdout is split on `\n` ONLY (an optional trailing `\r` is stripped),
// NOT via Node's `readline`. readline additionally splits on the Unicode line
// separators U+2028 / U+2029, which are VALID inside JSON strings — a generic line
// reader therefore corrupts any stream-json frame carrying those code points (e.g.
// model output or file contents). The `pi` RPC protocol calls this out explicitly
// (`docs/rpc.md`: "split records on `\n` only … do not use generic line readers"), and
// it is the correct framing for every newline-delimited-JSON CLI here, so the split is
// done manually with a `\n`-only scanner shared across all native-CLI providers.

import type { SandboxHandle, SpawnHandle } from './seam.js';

/**
 * How a native CLI ENDED — the signal that tells a crash apart from a clean exit.
 *
 * Stdout ending is not that signal: a CLI that finished its work, one that segfaulted,
 * and one this harness killed on teardown all end the line stream identically. Without
 * the exit status a provider's read loop cannot know which happened, so every one of
 * them resolved the in-flight turn as if the CLI had simply finished — the turn then
 * reached the client as a bare `agent.turn_completed` with no answer and no reason.
 */
export interface NativeCliExit {
  /** Process exit code, or `null` when it died on a signal. */
  code: number | null;
  /** The signal that killed it, or `null` on a normal exit. */
  signal: NodeJS.Signals | null;
  /**
   * Whether THIS harness asked for the termination ({@link NativeCliProcess.kill}). A
   * requested exit is the expected end of a session teardown / interrupt, not a fault;
   * an UNREQUESTED death (an OOM-kill, a segfault, a non-zero exit) is.
   */
  requested: boolean;
}

/**
 * Where the native CLI binary is and how to invoke it. Transport-neutral: `cmd`
 * + `args` are joined into a single shell command for {@link SandboxHandle.spawn}
 * (which runs it inside whatever sandbox the runtime backs). `env` layers over
 * the sandbox process environment; `cwd` is a sandbox-visible working directory.
 */
export interface NativeCliLaunchConfig {
  /** The CLI executable (an absolute path, or a name resolved on the sandbox PATH). */
  cmd: string;
  /** Arguments passed to the CLI (each is shell-quoted before being joined). */
  args?: string[];
  /** Extra environment for the CLI process (merged over the sandbox environment). */
  env?: Record<string, string>;
  /** Sandbox-visible working directory for the CLI. */
  cwd?: string;
}

/**
 * A booted native CLI: its streamed stdout lines, a stdin writer, and lifecycle.
 * Returned by {@link launchNativeCli}. The provider consumes {@link lines}
 * through a stream-json normalizer, pushes input via {@link write}, and
 * {@link kill}s the process when the session ends.
 */
export interface NativeCliProcess {
  /**
   * The CLI's stdout as an async-iterable of lines (newline stripped, blank
   * trailing line dropped). Iterating consumes the live stdout stream; the
   * iterator completes when the CLI's stdout ends (the process exited or was
   * killed). A single consumer is expected — the runner's normalizer.
   */
  lines(): AsyncIterable<string>;
  /**
   * Write raw bytes to the CLI's stdin. A stream-json driver writes one JSON
   * object per line, each terminated by `\n`. Returns the stream's `write`
   * backpressure signal (`false` when the buffer is full).
   */
  write(chunk: string | Buffer): boolean;
  /** Close the CLI's stdin (signals EOF to the child, if it reads to EOF). */
  end(): void;
  /** Terminate the CLI. Defaults to `SIGTERM`. Idempotent. */
  kill(signal?: NodeJS.Signals): void;
  /** Resolves when the CLI's stdout stream ends (process exit / kill). */
  readonly done: Promise<void>;
  /**
   * The LAUNCH outcome: the error the sandbox's `spawn` rejected with, or `undefined`
   * once the process is up. Settles at exactly the moment {@link lines} would start
   * yielding, so a reader awaits this FIRST and can tell a failed launch apart from a
   * CLI that started and exited with no output — otherwise indistinguishable, because
   * both end the line stream immediately.
   *
   * A missing / misnamed CLI binary on the sandbox PATH is the likeliest production
   * misconfiguration for a native-CLI provider, so this is the one signal that keeps
   * it from being invisible: every provider's read loop turns it into a terminal
   * `agent.error` rather than an empty, silent turn.
   */
  readonly failure: Promise<Error | undefined>;
  /**
   * How the CLI ENDED, or `undefined` when it never started (a failed spawn — see
   * {@link failure}). Settles once the child has exited, so a read loop reads it from
   * the `finally` that runs when {@link lines} completes.
   *
   * This is the ONLY channel that distinguishes a CLI which finished cleanly from one
   * that CRASHED: both end the line stream with no further output. A provider turns an
   * UNREQUESTED non-zero / signalled exit into a terminal `agent.error` naming the
   * status, so a child that died mid-turn is reported instead of silently ending the
   * turn as a successful empty answer.
   */
  readonly exit: Promise<NativeCliExit | undefined>;
}

/**
 * How long a read loop waits for the exit status after the CLI's stdout ENDED.
 *
 * A read loop reads {@link NativeCliProcess.exit} from the `finally` that releases the
 * turn and ends the event stream, so an UNBOUNDED wait there would be a new way for a
 * turn to hang: a child that closes stdout but lingers (never exits, never killed) would
 * park that `finally` forever and the turn would never reach its terminal marker — the
 * exact defect the exit status exists to report. The two are simultaneous for a real
 * process (stdout closes as it dies), so this grace is a backstop, not a timing
 * dependency: past it the loop reports no exit fault and ends the turn on the runner
 * loop's own "stream ended before the turn completed" instead.
 */
export const EXIT_STATUS_GRACE_MS = 1_000;

/**
 * WHEN the CLI died, relative to the turn accounting — the half of the message a read loop
 * must supply because the exit status cannot know it.
 *
 * `'mid-turn'`: a turn was still parked on its answer, so the death is that turn's terminal
 * explanation. `'idle'`: nothing was in flight (an OOM between turns, a one-shot child that
 * emitted its terminal frame and then exited non-zero) — a real fault, but no turn's cause.
 * The distinction is in the WORDING because both reach the transcript: saying "before the
 * turn completed" about a death between turns is simply false.
 */
export type CliExitTiming = 'mid-turn' | 'idle';

/**
 * The `agent.error` message for a CLI that ended ABNORMALLY, or `undefined` when it did not
 * — the shared rule every native-CLI provider's read loop applies.
 *
 * `undefined` (no fault to report) for: a CLI that never started (its {@link
 * NativeCliProcess.failure} is the fault, already reported by the read loop's launch
 * check), a termination THIS harness requested (`kill()` on teardown / interrupt), a
 * status that did not arrive within {@link EXIT_STATUS_GRACE_MS}, and a clean `exit 0`
 * (a one-shot CLI ends its turn by exiting; the runner loop's own "stream ended before
 * the turn completed" terminal error covers a turn cut short by one). Everything else —
 * a non-zero code, or death by a signal we did not send — is a CRASH, and the message
 * names the status so the transcript records what happened.
 *
 * `when` picks the wording (see {@link CliExitTiming}); it never changes WHETHER there is a
 * fault. An idle death used to be dropped before the message was ever built, which lost the
 * only record that the child had died at all — see the read loops' own comment on why the
 * fault is now always computed and only the `terminal` flag is conditional.
 */
export async function cliExitFaultMessage(
  cli: NativeCliProcess,
  cliLabel: string,
  when: CliExitTiming,
): Promise<string | undefined> {
  const exit = await exitWithinGrace(cli.exit);
  if (exit === undefined || exit.requested) {
    return undefined;
  }
  const timing = when === 'idle' ? 'while the session was idle' : 'before the turn completed';
  if (exit.signal !== null) {
    return `${cliLabel} CLI was killed by ${exit.signal} ${timing}`;
  }
  if (exit.code !== null && exit.code !== 0) {
    return `${cliLabel} CLI exited with code ${exit.code} ${timing}`;
  }
  return undefined;
}

/**
 * Await an exit status, giving up after {@link EXIT_STATUS_GRACE_MS}. `undefined` for a
 * stub that predates the `exit` channel, for a failed launch, and for a status that did
 * not arrive in time — all "nothing to report" for {@link cliExitFaultMessage}. The
 * timer is unref'd (it must never hold the process open) and cleared as soon as the
 * status lands, so the common path costs nothing.
 */
async function exitWithinGrace(
  exit: Promise<NativeCliExit | undefined> | undefined,
): Promise<NativeCliExit | undefined> {
  if (exit === undefined) {
    return undefined;
  }
  let timer: NodeJS.Timeout | undefined;
  const grace = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => resolve(undefined), EXIT_STATUS_GRACE_MS);
    timer.unref?.();
  });
  try {
    return await Promise.race([exit, grace]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * A structured-logger seam for the native-CLI harnesses (a subset of the runner's logger,
 * mirroring the coordinator's). All methods optional.
 *
 * It exists for faults that have NO other channel. The `codex`, `pi` and `custom` approval
 * protocols answer a gated tool call with a bare deny VALUE and carry no message, so when
 * the approval GATE ITSELF faults — a permission-store outage denying every tool call in
 * the session — the reason reaches neither the model nor the transcript. Binding the error
 * makes it recoverable; this is where it goes. (`claude-code` and `cursor` need no logger
 * for it: their protocols carry a deny MESSAGE, so the reason already reaches the CLI.)
 */
export interface NativeCliLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/**
 * The default per-turn WALL-CLOCK bound every native-CLI harness drives its turn under
 * (each accepts its own `turnTimeoutMs` override; a test shortens it).
 *
 * A native-CLI `submit` writes a command frame and then parks on the CLI's own terminal
 * notification. NOTHING on the stream reports a child that took the frame and went QUIET
 * — a protocol mismatch, a wedged child, a model call that never returns — so without a
 * bound that park is FOREVER: `submit` never resolves, the runner loop parks on both legs
 * of its turn race, the consumer never sees an `agent.turn_completed`, and the registry
 * treats the session as pending for good. The CLI exiting is reported (the read loop's
 * exit-status check); the CLI staying ALIVE and mute is not, which is why only a clock
 * can end it.
 *
 * Thirty minutes: comfortably above the longest legitimate agentic turn (a tool-heavy one
 * runs single-digit to tens of minutes) so it never truncates real work, and short enough
 * that a wedged session releases rather than pinning `activeTurns` — which also holds the
 * runner's idle watchdog open — indefinitely. It is a BACKSTOP, not a timing dependency:
 * every healthy turn ends on the CLI's own terminal frame long before it.
 */
export const DEFAULT_TURN_DEADLINE_MS = 30 * 60_000;

/**
 * The terminal error message a turn that blew its wall-clock bound is reported as — one
 * wording for all five native-CLI harnesses, so an operator reads the same sentence
 * whichever CLI wedged. Returns the message only (never an event): this module sits BELOW
 * the harness layer and must not depend on its event shapes, exactly as
 * {@link cliExitFaultMessage} does.
 */
export function turnDeadlineFaultMessage(cliLabel: string, deadlineMs: number): string {
  return `${cliLabel} CLI did not complete the turn within ${deadlineMs}ms`;
}

/**
 * Await a native-CLI turn's terminal notification under a bounded wall clock, calling
 * `onDeadline` if the bound expires first.
 *
 * One implementation for all five native-CLI harnesses: the question "did this turn ever
 * end?" is the same question in every one of them, and the shape it is answered in is the
 * shape codex already uses for its JSON-RPC request deadline — a timer that settles the
 * park, unref'd so it can never hold the process open, cleared the moment the turn lands
 * so the healthy path costs nothing.
 *
 * `onDeadline` is the harness's own terminal handling: name the timeout on the wire, TEAR
 * THE CHILD DOWN, and release the parked turn. The teardown is not optional — a deadline
 * that ends the turn but leaves the CLI alive desynchronizes the harness from the child
 * for good, and the next turn is then answered by the abandoned one's late frame. It runs
 * at most once — after `turnDone` settles the timer is cleared — and never on the healthy
 * path.
 */
export async function awaitTurnWithinDeadline(
  turnDone: Promise<void>,
  deadlineMs: number,
  onDeadline: () => void,
): Promise<void> {
  const timer = setTimeout(onDeadline, deadlineMs);
  timer.unref?.();
  try {
    await turnDone;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Boot the native CLI described by `config` inside `sandbox` and return a live
 * {@link NativeCliProcess}. The command is `config.cmd` plus shell-quoted
 * `config.args`, launched through {@link SandboxHandle.spawn} so it honors the
 * sandbox boundary of whatever runtime backs the handle.
 *
 * Requires a handle that implements the optional `spawn` primitive (the InMemory,
 * Local, and tmux runtimes all do); a handle without it throws — a native-CLI
 * provider must be given a spawn-capable sandbox.
 */
export function launchNativeCli(
  sandbox: SandboxHandle,
  config: NativeCliLaunchConfig,
): NativeCliProcess {
  if (typeof sandbox.spawn !== 'function') {
    throw new Error(
      'launchNativeCli requires a sandbox that supports spawn(); this runtime does not',
    );
  }
  const command = buildCommand(config);
  const spawnOpts = toSpawnOpts(config);
  // `spawn` is async (the sandbox may need a round-trip to start the process);
  // the returned NativeCliProcess is synchronous, so we bridge over a promise for
  // the underlying SpawnHandle and queue writes/kills until it resolves.
  const handlePromise: Promise<SpawnHandle> =
    spawnOpts === undefined ? sandbox.spawn(command) : sandbox.spawn(command, spawnOpts);
  return new SpawnBackedNativeCli(handlePromise);
}

/**
 * Bridges the synchronous {@link NativeCliProcess} surface onto the async
 * {@link SandboxHandle.spawn} result. stdin writes and a kill issued before the
 * spawn resolves are buffered and replayed once the {@link SpawnHandle} is ready.
 */
class SpawnBackedNativeCli implements NativeCliProcess {
  readonly done: Promise<void>;
  readonly failure: Promise<Error | undefined>;
  readonly exit: Promise<NativeCliExit | undefined>;

  private handle: SpawnHandle | undefined;
  private spawnError: Error | undefined;
  /**
   * Set by {@link kill} — whether THIS harness asked the child to die. It is the only
   * way to tell a teardown/interrupt apart from an OOM-kill or a segfault: both arrive
   * as the same `SIGTERM`/`SIGKILL` exit and the same premature stdout close.
   */
  private killedByUs = false;
  /** Writes issued before the SpawnHandle resolved, replayed in order after. */
  private readonly pendingWrites: Array<string | Buffer> = [];
  private pendingEnd = false;
  private pendingKill: NodeJS.Signals | 'default' | undefined;
  private linesConsumed = false;
  private resolveDone!: () => void;
  /**
   * Resolves as soon as the SpawnHandle is available (the process has started),
   * or `undefined` if the spawn failed. Distinct from {@link done} (which resolves
   * only when stdout ENDS): `lines()` must begin reading the moment the process is
   * up, long before it exits — a long-lived CLI never ends until told to.
   */
  private readonly ready: Promise<SpawnHandle | undefined>;

  constructor(handlePromise: Promise<SpawnHandle>) {
    this.done = new Promise<void>((resolve) => {
      this.resolveDone = resolve;
    });
    this.ready = handlePromise.then(
      (handle) => {
        this.onSpawned(handle);
        return handle;
      },
      (err: Error) => {
        // A failed spawn ends the process immediately: `done` resolves and
        // `lines()` completes with no output. `ready` yields `undefined` so the
        // line iterator returns at once.
        this.spawnError = err;
        this.resolveDone();
        return undefined;
      },
    );
    // The launch outcome, readable by the provider's read loop: `spawnError` is
    // assigned before `ready` settles on the failure branch, so reading it here is
    // ordered. This is the ONLY path by which a failed spawn escapes this object —
    // `lines()` deliberately ends cleanly so a killed process is not a fault, which
    // would otherwise make a never-started CLI silently indistinguishable from one
    // that exited straight away.
    this.failure = this.ready.then(() => this.spawnError);
    // How the child ENDED. `SpawnHandle.exited` already carries the code/signal (and
    // REJECTS when the process could not be spawned at all — the `failure` channel's
    // case, mapped to `undefined` here so a failed launch is reported once, by
    // `failure`). `requested` is read at settle time, so a `kill()` issued at any point
    // before the exit marks it expected.
    this.exit = this.ready.then((handle) => {
      if (handle === undefined) {
        return undefined;
      }
      return handle.exited.then(
        ({ code, signal }): NativeCliExit => ({ code, signal, requested: this.killedByUs }),
        (): undefined => undefined,
      );
    });
  }

  lines(): AsyncIterable<string> {
    if (this.linesConsumed) {
      throw new Error('NativeCliProcess.lines() may only be consumed once');
    }
    this.linesConsumed = true;
    return this.iterateLines();
  }

  write(chunk: string | Buffer): boolean {
    if (this.handle !== undefined) {
      return this.handle.stdin.write(chunk);
    }
    if (this.spawnError !== undefined) {
      return false;
    }
    this.pendingWrites.push(chunk);
    return true;
  }

  end(): void {
    if (this.handle !== undefined) {
      this.handle.stdin.end();
      return;
    }
    this.pendingEnd = true;
  }

  kill(signal?: NodeJS.Signals): void {
    // Record the intent BEFORE either branch: a kill issued before the spawn resolved
    // is replayed by `onSpawned`, and the resulting exit is still one we asked for.
    this.killedByUs = true;
    if (this.handle !== undefined) {
      this.handle.kill(signal);
      return;
    }
    this.pendingKill = signal ?? 'default';
  }

  /** Replay any buffered stdin / end / kill once the SpawnHandle is live. */
  private onSpawned(handle: SpawnHandle): void {
    this.handle = handle;
    // stdout ending is the authoritative "process is done" signal for `done`.
    handle.stdout.once('end', () => this.resolveDone());
    handle.stdout.once('close', () => this.resolveDone());

    for (const chunk of this.pendingWrites) {
      handle.stdin.write(chunk);
    }
    this.pendingWrites.length = 0;
    if (this.pendingEnd) {
      handle.stdin.end();
    }
    if (this.pendingKill !== undefined) {
      handle.kill(this.pendingKill === 'default' ? undefined : this.pendingKill);
    }
  }

  /**
   * Yield stdout lines. Waits for the SpawnHandle, then consumes its stdout as raw
   * chunks and splits them on `\n` ONLY (stripping an optional trailing `\r`),
   * handling chunk boundaries via a carry buffer. This is deliberately NOT Node
   * `readline`: readline also splits on U+2028 / U+2029, which are valid inside JSON
   * strings and would corrupt a stream-json frame carrying them (the `pi` RPC
   * contract requires `\n`-only framing). Completes when stdout ends; a trailing
   * partial line (no final `\n`) is flushed. If the spawn failed, completes at once.
   *
   * The stream's async iterator completes on `end`. But a `kill` tears the stream
   * down with `destroy()`, which emits `close` WITHOUT `end` (no clean EOF arrives)
   * — the destroyed stream's iterator then rejects with a premature-close error,
   * which is the expected terminal state of a killed process (not a fault) and is
   * swallowed so the line stream ends cleanly for both a natural exit and a kill.
   */
  private async *iterateLines(): AsyncIterable<string> {
    const handle = await this.awaitHandle();
    if (handle === undefined) {
      return; // spawn failed; no output.
    }
    handle.stdout.setEncoding('utf8');
    let carry = '';
    try {
      for await (const chunk of handle.stdout as AsyncIterable<string>) {
        carry += chunk;
        let nl: number;
        // Split on `\n` only — never on U+2028 / U+2029 (valid inside JSON strings).
        while ((nl = carry.indexOf('\n')) >= 0) {
          yield stripTrailingCr(carry.slice(0, nl));
          carry = carry.slice(nl + 1);
        }
      }
      // Flush a trailing partial line the stream ended without a final `\n`.
      if (carry.length > 0) {
        yield stripTrailingCr(carry);
      }
    } catch (err) {
      if (!isPrematureClose(err)) {
        throw err;
      }
    }
  }

  /** Resolve the live SpawnHandle, or undefined if the spawn failed. */
  private awaitHandle(): Promise<SpawnHandle | undefined> {
    return this.ready;
  }
}

/** Join `cmd` + shell-quoted `args` into a single command for `spawn`. */
function buildCommand(config: NativeCliLaunchConfig): string {
  const parts = [config.cmd, ...(config.args ?? []).map(shellQuote)];
  return parts.join(' ');
}

/** Map the launch config's env/cwd onto the `spawn` opts shape (only when set). */
function toSpawnOpts(
  config: NativeCliLaunchConfig,
): { env?: Record<string, string>; cwd?: string } | undefined {
  const opts: { env?: Record<string, string>; cwd?: string } = {};
  if (config.env !== undefined) {
    opts.env = config.env;
  }
  if (config.cwd !== undefined) {
    opts.cwd = config.cwd;
  }
  return Object.keys(opts).length > 0 ? opts : undefined;
}

/** POSIX single-quote a string for safe interpolation into a shell command. */
function shellQuote(s: string): string {
  return `'${s.replace(/'/g, "'\\''")}'`;
}

/**
 * Strip a single optional trailing carriage return from a line. The framing splits on `\n`; a
 * `\r\n`-terminated producer leaves the `\r` as the line's last char, so it is removed to yield the
 * bare record (the stream-json contract: split on `\n`, strip a trailing `\r`).
 */
function stripTrailingCr(line: string): string {
  return line.endsWith('\r') ? line.slice(0, -1) : line;
}

/**
 * True when `err` is Node's premature-close error — the signal that a stream was
 * `destroy()`ed before it ended. For a killed CLI that is the expected terminal
 * state (not a fault), so the line iterator treats it as a clean end.
 */
function isPrematureClose(err: unknown): boolean {
  return (
    typeof err === 'object' &&
    err !== null &&
    (err as { code?: string }).code === 'ERR_STREAM_PREMATURE_CLOSE'
  );
}
