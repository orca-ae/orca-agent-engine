// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The Local environment-launcher's process seam.
//
// `LocalEnvironmentLauncher.startWorker` spawns one `environment-worker`
// process per environment. This module is that process seam — a
// `ProcessSpawner` that starts the worker and the `SpawnedProcess` handle the
// launcher polls / terminates — mirroring `environment-worker`'s own
// `ProcessSpawner`/`ChildProcessSpawner` split in
// `services/environment-worker/src/process-spawner.ts` one level up the process
// tree: that seam lets `environment-worker` spawn session runners without
// touching a real process in its unit tests; this seam lets the launcher spawn
// `environment-worker` itself without touching a real process in ITS unit
// tests. The launcher depends only on the seam, so tests inject a fake.

import { spawn, type ChildProcess } from 'node:child_process';

/** What to spawn for one `environment-worker` launch. */
export interface SpawnRequest {
  /** Argv: element 0 is the executable, the rest are its arguments. */
  readonly command: readonly string[];
  /** The child process environment (already wired with the dial-back vars). */
  readonly env: Record<string, string>;
  /** Absolute working directory for the child. */
  readonly cwd: string;
}

/** A spawned process handle the launcher tracks. */
export interface SpawnedProcess {
  /** OS process id, for log lines. */
  readonly pid: number | undefined;
  /**
   * The exit code if the process has already exited, otherwise `null`: a
   * non-null result means the process is gone.
   */
  poll(): number | null;
  /** Request graceful termination (SIGTERM). */
  terminate(): void;
  /** Force kill (SIGKILL). */
  kill(): void;
  /** Resolve once the process has exited. */
  wait(): Promise<void>;
}

/** Spawns one `environment-worker` process per launch. */
export interface ProcessSpawner {
  spawn(request: SpawnRequest): SpawnedProcess;
}

/** A `node:child_process`-backed {@link SpawnedProcess}. */
class ChildSpawnedProcess implements SpawnedProcess {
  private exitCode: number | null = null;
  private readonly exitWaiters: Array<() => void> = [];

  constructor(private readonly child: ChildProcess) {
    // `exit` fires when the process ends; `code` is null when it was
    // signalled, in which case the signal is mapped to the conventional
    // 128+signal code so callers always see a numeric exit code.
    child.on('exit', (code: number | null, signal: NodeJS.Signals | null) => {
      this.settleExit(code ?? (signal !== null ? 128 + signalNumber(signal) : 1));
    });
    // A spawn error (e.g. ENOENT for a bad command) never produces an `exit`;
    // surface it as a non-zero exit so the caller sees a failure rather than
    // hanging forever on `wait()`.
    child.on('error', () => {
      this.settleExit(127);
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

  terminate(): void {
    try {
      this.child.kill('SIGTERM');
    } catch {
      // already gone
    }
  }

  kill(): void {
    try {
      this.child.kill('SIGKILL');
    } catch {
      // already gone
    }
  }

  wait(): Promise<void> {
    if (this.exitCode !== null) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.exitWaiters.push(resolve));
  }
}

/** The production `node:child_process`-backed {@link ProcessSpawner}. */
export class ChildProcessSpawner implements ProcessSpawner {
  spawn(request: SpawnRequest): SpawnedProcess {
    const [command, ...args] = request.command;
    if (command === undefined) {
      throw new Error('environment-worker launch command must not be empty');
    }
    const child = spawn(command, args, {
      cwd: request.cwd,
      env: request.env,
      // environment-worker is a long-running daemon with no interactive
      // input; inherit stdout/stderr so its logs land wherever the launcher
      // process's own logs do (matches start-self-hosted.sh redirecting the
      // worker's output to a log file the operator can tail).
      stdio: ['ignore', 'inherit', 'inherit'],
    });
    return new ChildSpawnedProcess(child);
  }
}

/** Map a signal name to its number for the conventional 128+signal exit code. */
function signalNumber(signal: NodeJS.Signals): number {
  // The two signals the launcher itself raises (terminate/kill). Anything else
  // falls back to a stable placeholder so the exit code stays numeric.
  const known: Partial<Record<NodeJS.Signals, number>> = { SIGTERM: 15, SIGKILL: 9, SIGINT: 2 };
  return known[signal] ?? 0;
}
