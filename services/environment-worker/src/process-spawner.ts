// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The worker's runner-process seam.
//
// The worker spawns a session runner per `worker.launch_runner`. This module is
// the process seam — a `ProcessSpawner` that starts one runner and the
// `RunnerProcess` handle the worker monitors (poll for an exit, await it,
// terminate / kill on stop or shutdown). The worker depends only on the seam, so
// tests inject a fake with no real processes.
//
// The launch command is CONFIGURABLE (injected argv — the worker's configured
// runner command in production, a stub in tests); the spawner just runs whatever
// it is given.
//
// Diagnostics: a runner that dies before connecting its tunnel (a bad env, an
// import error, a rejected handshake) leaves its cause only in its own output.
// The production spawner captures a bounded tail of the child's stdout+stderr in
// memory and exposes it via {@link RunnerProcess.outputTail}, so the worker can
// ship the cause in the `worker.runner_exited` report. The tail lives only in
// memory — there are no per-runner log files or state directory to manage.
//
// A process that never STARTS has no output at all, so the tail is empty and the
// exit code is synthesized: the OS error is the only diagnostic that exists, and
// it is kept in {@link RunnerProcess.spawnError} rather than discarded (see the
// `error` handler below). Signal delivery is likewise reported, not swallowed —
// {@link RunnerProcess.terminate} / {@link RunnerProcess.kill} return whether the
// signal landed, so the worker can tell a stopped runner from one that refused
// to die.

import { spawn, type ChildProcess } from 'node:child_process';

/** A spawned runner process handle the worker monitors. */
export interface RunnerProcess {
  /** OS process id, for log lines. */
  readonly pid: number | undefined;
  /**
   * The exit code if the process has already exited, otherwise `null`: a
   * non-null result means the process is gone.
   */
  poll(): number | null;
  /** Register a listener fired once when the process exits. Fires immediately if already exited. */
  onExit(listener: () => void): void;
  /**
   * Request graceful termination (SIGTERM). Returns whether the signal was
   * delivered — `false` means the process is still running and did NOT receive
   * it (e.g. EPERM), which the caller must report rather than assume success.
   */
  terminate(): boolean;
  /** Force kill (SIGKILL). Returns whether the signal was delivered — see {@link terminate}. */
  kill(): boolean;
  /** Resolve once the process has exited. */
  wait(): Promise<void>;
  /** A bounded tail of the process's captured stdout+stderr, for exit diagnostics. */
  outputTail?(): string;
  /**
   * The error that prevented the process from EVER starting (ENOENT for a bad
   * command path, EACCES for a non-executable file, ENOTDIR for a bad cwd,
   * EMFILE/ENFILE, E2BIG, ENOMEM), or `undefined` when the process did start.
   *
   * Node reports these on the child's `error` event and never produces an
   * `exit`, so without this the whole diagnostic would be the synthesized exit
   * code — every one of those distinct causes flattened into a bare
   * "code 127". The worker ships this in the launch result / exit report.
   */
  spawnError?(): Error | undefined;
  /**
   * An `error` the child emitted AFTER it started — Node emits one when an
   * operation on a still-running child fails, in practice an EPERM from
   * `kill()`. It is NOT an exit (the process is still alive), so it is reported
   * separately and never settles the handle.
   */
  postSpawnError?(): Error | undefined;
}

/** What to spawn for one runner launch. */
export interface SpawnRequest {
  /** Argv: element 0 is the executable, the rest are its arguments. */
  readonly command: readonly string[];
  /** The runner subprocess environment (already filtered + wired). */
  readonly env: Record<string, string>;
  /** Absolute working directory for the runner. */
  readonly cwd: string;
}

/** Spawns one runner process per launch. */
export interface ProcessSpawner {
  spawn(request: SpawnRequest): RunnerProcess;
}

/**
 * Synthesized exit code for a process that never started. There is no real exit
 * status to report (Node emits `error`, never `exit`), and 127 is the shell's
 * conventional "command not found". The CAUSE travels with it in
 * {@link RunnerProcess.spawnError} — the code alone is not a diagnostic.
 */
const SPAWN_FAILURE_EXIT_CODE = 127;

/** Max bytes of captured child output retained for the exit tail (~last 40-60 lines). */
const OUTPUT_TAIL_MAX_BYTES = 4096;
/** Max tail lines included in a composed exit error (keeps the report short). */
const OUTPUT_TAIL_MAX_LINES = 15;

/** A `node:child_process`-backed {@link RunnerProcess}. */
class ChildRunnerProcess implements RunnerProcess {
  private exitCode: number | null = null;
  private readonly exitWaiters: Array<() => void> = [];
  private tailBuffer = Buffer.alloc(0);
  /** Set on the child's `spawn` event: the process really started. */
  private started = false;
  /** The `error` that arrived BEFORE `spawn` — the process never started. */
  private startFailure: Error | undefined;
  /** An `error` that arrived AFTER `spawn` — e.g. an EPERM `kill()`. */
  private runningError: Error | undefined;

  constructor(private readonly child: ChildProcess) {
    const capture = (chunk: Buffer): void => {
      this.tailBuffer = Buffer.concat([this.tailBuffer, chunk]);
      if (this.tailBuffer.length > OUTPUT_TAIL_MAX_BYTES) {
        this.tailBuffer = this.tailBuffer.subarray(this.tailBuffer.length - OUTPUT_TAIL_MAX_BYTES);
      }
    };
    child.stdout?.on('data', capture);
    child.stderr?.on('data', capture);
    child.on('spawn', () => {
      this.started = true;
    });
    // `exit` fires when the process ends; `code` is null when it was signalled,
    // in which case the signal is mapped to the conventional 128+signal code so
    // the worker always has a numeric exit code to report.
    child.on('exit', (code: number | null, signal: NodeJS.Signals | null) => {
      this.settleExit(code ?? (signal !== null ? 128 + signalNumber(signal) : 1));
    });
    // `error` is NOT spawn-only. Before `spawn` it means the process never
    // started (ENOENT/EACCES/ENOTDIR/EMFILE/E2BIG/ENOMEM) and no `exit` will
    // ever arrive, so the handle is settled with a synthesized code — but the
    // error itself is RETAINED, because the code alone reduces every one of
    // those causes to an indistinguishable "127". After `spawn`, the same event
    // means an operation on a LIVE child failed (an EPERM `kill()`): settling
    // there would mark a running runner as exited, prune it from the hello, and
    // file a bogus `runner_exited` for a runner still holding its tunnel.
    child.on('error', (err: Error) => {
      if (this.started) {
        this.runningError = err;
        return;
      }
      this.startFailure = err;
      this.settleExit(SPAWN_FAILURE_EXIT_CODE);
    });
  }

  private settleExit(code: number): void {
    if (this.exitCode !== null) {
      return;
    }
    this.exitCode = code;
    for (const waiter of this.exitWaiters.splice(0)) {
      waiter();
    }
  }

  get pid(): number | undefined {
    return this.child.pid;
  }

  poll(): number | null {
    return this.exitCode;
  }

  onExit(listener: () => void): void {
    if (this.exitCode !== null) {
      listener();
      return;
    }
    this.exitWaiters.push(listener);
  }

  terminate(): boolean {
    return this.signal('SIGTERM');
  }

  kill(): boolean {
    return this.signal('SIGKILL');
  }

  /**
   * Deliver `signal`, reporting whether it landed. An already-exited child is
   * "delivered" (there is nothing left to signal). A refusal is NOT swallowed:
   * `child.kill` returns `false` when the signal could not be sent, and an EPERM
   * additionally arrives on the `error` event — both reach the caller instead of
   * looking like a successful kill.
   *
   * The `catch` covers only `child.kill`'s throw on an UNKNOWN signal name, which
   * the two call sites (`terminate` → SIGTERM, `kill` → SIGKILL) cannot produce:
   * both are literals, so the branch is unreachable by construction. It is folded
   * into the same `false` the caller already handles rather than rethrown,
   * because a caller that has just been told "not delivered" acts identically
   * either way — the value is discarded only in the sense that no distinct signal
   * exists to carry.
   */
  private signal(signal: NodeJS.Signals): boolean {
    if (this.exitCode !== null) {
      return true;
    }
    try {
      return this.child.kill(signal);
    } catch {
      return false;
    }
  }

  wait(): Promise<void> {
    if (this.exitCode !== null) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.exitWaiters.push(resolve));
  }

  outputTail(): string {
    const text = this.tailBuffer.toString('utf-8');
    if (text.trim() === '') {
      return '';
    }
    return text.trim().split('\n').slice(-OUTPUT_TAIL_MAX_LINES).join('\n');
  }

  spawnError(): Error | undefined {
    return this.startFailure;
  }

  postSpawnError(): Error | undefined {
    return this.runningError;
  }
}

/**
 * Wrap an already-spawned child as a {@link RunnerProcess}.
 *
 * Exported so a spec can drive the child's event edges directly. The one that
 * matters is a POST-spawn `error`: the OS only produces it when an operation on
 * a live child fails (an EPERM `kill()`), which is not reproducible from a test
 * on a normal machine, yet mishandling it marks a running runner as exited.
 */
export function runnerProcessFor(child: ChildProcess): RunnerProcess {
  return new ChildRunnerProcess(child);
}

/** The production `node:child_process`-backed {@link ProcessSpawner}. */
export class ChildProcessSpawner implements ProcessSpawner {
  spawn(request: SpawnRequest): RunnerProcess {
    const [command, ...args] = request.command;
    if (command === undefined) {
      throw new Error('runner launch command must not be empty');
    }
    const child = spawn(command, args, {
      cwd: request.cwd,
      env: request.env,
      // Runners are WS-tunnel clients with no interactive input: a clean ignored
      // stdin (not the worker's, which a long-lived daemon may have closed),
      // piped stdout/stderr so the spawner can capture the exit-diagnostic tail.
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    return runnerProcessFor(child);
  }
}

/** Map a signal name to its number for the conventional 128+signal exit code. */
function signalNumber(signal: NodeJS.Signals): number {
  // The two signals the worker itself raises (terminate/kill). Anything else
  // falls back to a stable placeholder so the exit code stays numeric.
  const known: Partial<Record<NodeJS.Signals, number>> = { SIGTERM: 15, SIGKILL: 9, SIGINT: 2 };
  return known[signal] ?? 0;
}
