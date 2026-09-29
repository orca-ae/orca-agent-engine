// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker-thread entrypoint for the host-side git worktree operations.
//
// The serve loop in `worker.ts` keeps the registry tunnel alive by answering
// keepalive pings on the event loop. A worktree op shells out to `git`, which
// can take seconds on a large repo (fetch/add) — running it on the main thread
// would block the loop for that whole window and miss the pings. This module
// runs the BLOCKING git work on a dedicated worker thread: `git-worktree.ts` calls
// `spawnSync`, which parks THIS thread while git runs, leaving the main thread's
// EVENT loop free.
//
// Freeing the event loop is necessary but not sufficient: the worker also
// dispatches the worktree request off the SERVE loop, so `receive()` keeps
// draining while this thread works. See the note in `worktree-offload.ts`.
//
// It is a SEPARATE tsup entry from `main.ts` so the parent can start it by a
// stable sibling filename (`dist/worktree-thread.js`) rather than re-entering its
// own bundle. The dependency surface is intentionally tiny — only the
// self-contained `git-worktree.ts` (node builtins, no workspace deps) — so the
// fresh thread context loads fast.
//
// Protocol: the parent posts exactly one {@link WorktreeJob} over the
// `workerData`/`parentPort` channel; this entry runs it and posts back exactly
// one {@link WorktreeThreadReply}, then lets the thread exit. A {@link
// WorktreeError} is flattened to `{ worktree: true, message }` so it survives the
// structured-clone boundary (a thrown class instance would arrive as a plain
// object and lose its `instanceof` identity); the parent rehydrates it.

import { parentPort, workerData } from 'node:worker_threads';
import {
  CreatedWorktree,
  WorktreeError,
  createWorktree,
  removeWorktree,
  type CreateWorktreeArgs,
  type RemoveWorktreeArgs,
} from './git-worktree.js';

/** Discriminated git job the parent asks the thread to run. */
export type WorktreeJob =
  | { readonly op: 'create'; readonly args: CreateWorktreeArgs }
  | { readonly op: 'remove'; readonly args: RemoveWorktreeArgs };

/** The flattened, clone-safe shape of a successful create result. */
export interface CreatedWorktreeValue {
  readonly worktreePath: string;
  readonly branch: string;
}

/**
 * The single reply posted back to the parent. `ok` carries the create result
 * (`null` for a remove, which has no value); a failure carries `worktreeError`
 * (a user-facing {@link WorktreeError} message) when the op failed expectedly, or
 * `unexpected` for any other thrown value (a bug — surfaced, not swallowed).
 */
export type WorktreeThreadReply =
  | { readonly ok: true; readonly value: CreatedWorktreeValue | null }
  | { readonly ok: false; readonly worktreeError: string }
  | { readonly ok: false; readonly unexpected: string };

/** Run the requested job synchronously, mapping it to a clone-safe reply. */
export function runJob(job: WorktreeJob): WorktreeThreadReply {
  try {
    if (job.op === 'create') {
      const created: CreatedWorktree = createWorktree(job.args);
      return { ok: true, value: { worktreePath: created.worktreePath, branch: created.branch } };
    }
    removeWorktree(job.args);
    return { ok: true, value: null };
  } catch (exc) {
    if (exc instanceof WorktreeError) {
      // Flatten to the message: a WorktreeError instance would arrive at the
      // parent as a plain object (structured clone drops the prototype), losing
      // its identity. The parent rebuilds a WorktreeError from this string.
      return { ok: false, worktreeError: exc.message };
    }
    // Any non-WorktreeError is a real bug; carry its message so the parent can
    // re-throw rather than silently turning it into a worktree failure.
    return { ok: false, unexpected: exc instanceof Error ? exc.message : String(exc) };
  }
}

// When loaded as a worker thread, `workerData` carries the job and `parentPort`
// is non-null. Running this file outside a worker (e.g. an accidental direct
// `node` invocation) is a no-op rather than a crash.
if (parentPort !== null) {
  const reply = runJob(workerData as WorktreeJob);
  parentPort.postMessage(reply);
}
