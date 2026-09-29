// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Run a host-side git worktree op on a dedicated worker thread.
//
// This is the production offload the worktree handlers use: it starts the
// `worktree-thread.ts` entry as a `node:worker_threads` Worker, hands it the job
// via `workerData`, awaits the single reply, and rehydrates it on the main
// thread. While the thread runs the blocking `spawnSync('git', …)`, the main
// thread's EVENT loop stays free.
//
// That is one half of keeping the tunnel answered during git, not the whole of
// it: a free event loop does nothing for a serve loop that is awaiting this
// promise, because it will not call `receive()` again until the promise settles
// and the ping just queues. The worker therefore also dispatches the worktree
// request OFF the serve loop (`EnvironmentWorker.serveRequestOffLoop`), which is
// what actually lets a keepalive ping be read and ponged while git runs.
//
// The worker is one-shot (spawned per op, terminated on reply): worktree
// create/remove is an infrequent session-start operation, so the thread-startup
// cost is paid rarely and the lifecycle stays trivially correct (no pool to drain
// on shutdown). A {@link WorktreeError} raised in the thread is flattened to a
// message across the structured-clone boundary and rebuilt here so the handler's
// `instanceof WorktreeError` mapping is preserved.

import { Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import { WorktreeError, type CreateWorktreeArgs, type RemoveWorktreeArgs } from './git-worktree.js';
import type { CreatedWorktreeValue, WorktreeJob, WorktreeThreadReply } from './worktree-thread.js';

/**
 * Absolute path to the BUILT worktree worker-thread entry
 * (`worktree-thread.js`), resolved relative to THIS module at runtime.
 *
 * tsup emits `main.ts` and `worktree-thread.ts` as sibling files in `dist/`, so
 * `new URL('./worktree-thread.js', import.meta.url)` resolves to
 * `dist/worktree-thread.js` next to the rest of the bundle. We go through
 * `import.meta.url` (not `__dirname`, undefined in ESM) so the path is correct no
 * matter the embedding process's cwd. The worker offload defaults to this; a test
 * injects a different `offload` seam instead of relying on a build.
 */
export const defaultWorktreeThreadEntry: string = fileURLToPath(
  new URL('./worktree-thread.js', import.meta.url),
);

/** A function that runs a {@link WorktreeJob} off the main thread. */
export type WorktreeOffload = (job: WorktreeJob) => Promise<CreatedWorktreeValue | null>;

/**
 * The slice of `node:worker_threads.Worker` the offload drives: it listens for
 * the single reply (and the error/exit failure edges) and terminates the
 * one-shot worker. A seam so a test can run the worker through a TS loader
 * (the built `.js` sibling does not exist under a source-only test run) without
 * changing the production reply-mapping logic below.
 */
export interface WorktreeWorkerHandle {
  once(event: 'message', listener: (reply: WorktreeThreadReply) => void): void;
  once(event: 'error', listener: (err: Error) => void): void;
  once(event: 'exit', listener: (code: number) => void): void;
  terminate(): Promise<number>;
}

/** Starts a worker thread to run `job`, returning its {@link WorktreeWorkerHandle}. */
export type WorktreeWorkerSpawn = (job: WorktreeJob) => WorktreeWorkerHandle;

/**
 * Build the production {@link WorktreeOffload}: each call starts a worker thread,
 * runs the job there, and resolves the (clone-safe) result. A thread-side
 * {@link WorktreeError} is re-thrown here as a real `WorktreeError` (so the
 * handler maps it to a `status: "failed"` frame); an unexpected thread error or a
 * worker that dies without replying rejects with a generic `Error` (re-thrown by
 * the handler, surfaced as a crash).
 *
 * @param spawn Worker-thread factory. Defaults to one that starts the built
 *   `worktree-thread.js` sibling ({@link defaultWorktreeThreadEntry}). A test
 *   injects a factory that loads the `.ts` entry through a loader instead.
 */
export function createWorktreeOffload(
  spawn: WorktreeWorkerSpawn = defaultWorktreeWorkerSpawn,
): WorktreeOffload {
  return (job) =>
    new Promise<CreatedWorktreeValue | null>((resolve, reject) => {
      const worker = spawn(job);
      let settled = false;

      worker.once('message', (reply: WorktreeThreadReply) => {
        settled = true;
        // Stop the one-shot worker as soon as the reply lands; ignore the
        // terminate promise (the result is already in hand).
        void worker.terminate();
        if (reply.ok) {
          resolve(reply.value);
        } else if ('worktreeError' in reply) {
          reject(new WorktreeError(reply.worktreeError));
        } else {
          reject(new Error(`worktree thread failed: ${reply.unexpected}`));
        }
      });

      worker.once('error', (err) => {
        // A throw that escaped the thread's own handling, or a failure to start
        // the worker module at all. Reject once.
        if (settled) {
          return;
        }
        settled = true;
        void worker.terminate();
        reject(err);
      });

      worker.once('exit', (code) => {
        // The worker exited before posting a reply (e.g. it crashed during
        // module load). Without this the promise would hang forever.
        if (settled) {
          return;
        }
        settled = true;
        reject(new Error(`worktree thread exited without a result (code ${code})`));
      });
    });
}

/** Default factory: start the built `worktree-thread.js` sibling as a worker. */
function defaultWorktreeWorkerSpawn(job: WorktreeJob): WorktreeWorkerHandle {
  return new Worker(defaultWorktreeThreadEntry, { workerData: job });
}

/** Build the worktree create job for {@link WorktreeOffload}. */
export function createJob(args: CreateWorktreeArgs): WorktreeJob {
  return { op: 'create', args };
}

/** Build the worktree remove job for {@link WorktreeOffload}. */
export function removeJob(args: RemoveWorktreeArgs): WorktreeJob {
  return { op: 'remove', args };
}
