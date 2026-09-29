// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The runner-side registry of parked tool-confirmation verdicts — the client half
// of the uniform transcript approval.
//
// When the harness needs the user to confirm a gated tool call (the SDK's
// `canUseTool` fires), the runner PARKS a verdict here keyed by the tool's stable
// `tool_use_id` and waits. The verdict is delivered out-of-band: the registry pushes
// the client's `user.tool_confirmation` (which rode the transcript — the durable
// source of truth) to the runner's confirmation route, whose handler calls
// {@link PendingApprovals.resolve}. Resolving settles the parked Promise, so the
// harness either proceeds (allow) or returns a clean denial (deny) to the model.
//
// The parking model follows directly from Orca's uniform transcript approval:
//
//   - KEYED BY tool-use id. The verdict's source of truth is the transcript
//     `user.tool_confirmation` event, which correlates to the in-flight tool call by
//     its `tool_use_id` (the same id the agent's tool-use block carries). The runner
//     does not mint a separate correlation id, so the tool-use id IS the parking key.
//   - PURELY a routing table + a bounded wait. Parking emits NO out-of-band event:
//     a verdict resolves the parked promise (allow/deny), and that is the whole
//     contract. The "is this session awaiting a human approval" signal a UI badge /
//     a mid-turn ingest guard needs is exposed DIRECTLY in-process via
//     {@link PendingApprovals.hasPending} / {@link PendingApprovals.pendingCount},
//     which the loop surfaces as {@link SessionLoop.hasPendingApproval}. No
//     per-approval "resolved" notification is published: such a notification is
//     only needed when the pending-approval index (a UI awaiting-approval
//     indicator) lives in a DIFFERENT process from the awaiter, whereas here the
//     awaiter and the signal are the SAME process, so the count IS the lockstep —
//     no event to emit, no consumer to drift. The lockstep guarantee: the pending
//     count decrements on EVERY exit path — a delivered verdict ({@link resolve}), a
//     timeout (the park's deadline), and a teardown / cancel ({@link reset}) — so
//     `hasPending` never strands a "still awaiting" signal after the awaiter is gone
//     (asserted in the pending-approvals spec's lockstep test).
//
// The table is per-{@link PendingApprovals} instance (one per session loop), not a
// process global: a runner serves one session, but scoping the table to the loop
// keeps it testable and lets a teardown ({@link reset}) deny every outstanding
// verdict so a stopping harness never leaves a tool call blocked forever.
//
// EXACTLY-ONCE / IDEMPOTENT: the verdict source of truth is the durable transcript,
// so a re-delivered confirmation (a flapping reconnect re-pushing the same
// `user.tool_confirmation`) must be harmless. {@link resolve} is therefore
// idempotent — a second resolve for an id whose verdict already landed (or that was
// never parked) is a no-op that returns `false` — so the parked promise's first
// verdict always wins and a duplicate delivery cannot flip it.

/**
 * The outcome of a parked tool-confirmation verdict.
 *
 * `approved` is the load-bearing decision the harness maps to the SDK's
 * `PermissionResult` (`true` → `behavior: 'allow'`, `false` → `behavior: 'deny'`).
 * `timedOut` is set only when the bounded wait elapsed with no verdict (the wait
 * collapsed to a denial); a delivered verdict (allow or deny) never sets it.
 */
export interface ApprovalVerdict {
  /** `true` = the user allowed the tool call; `false` = denied (or timed out). */
  approved: boolean;
  /** `true` only when the verdict is a timeout-induced denial (no verdict arrived). */
  timedOut?: boolean;
}

/** Options for {@link PendingApprovals}. */
export interface PendingApprovalsOptions {
  /**
   * Default wait budget (ms) for a verdict when {@link ParkOptions.timeoutMs} is
   * omitted. Bounded so a user who walked away does not pin a parked tool call
   * forever; on timeout the wait collapses to a denial. Defaults to
   * {@link DEFAULT_APPROVAL_WAIT_MS}. A non-positive value disables the deadline.
   */
  defaultTimeoutMs?: number;
}

/** Options for one {@link PendingApprovals.park}. */
export interface ParkOptions {
  /** The in-flight tool call's stable `tool_use_id` — the routing + verdict key. */
  toolUseId: string;
  /**
   * Max ms to wait for a verdict before collapsing to a denial. Omitted → the
   * registry's {@link PendingApprovalsOptions.defaultTimeoutMs}. A non-positive
   * value disables the deadline (wait indefinitely for a delivered verdict).
   */
  timeoutMs?: number;
}

/**
 * Default wait budget for a tool-confirmation verdict, in milliseconds: 120s —
 * long enough for a human to answer, bounded so a walked-away user never pins a
 * parked tool call. Tunable per registry via
 * {@link PendingApprovalsOptions.defaultTimeoutMs} or per park via
 * {@link ParkOptions.timeoutMs}.
 */
export const DEFAULT_APPROVAL_WAIT_MS = 120_000;

/** One parked verdict: its resolver, its (optional) deadline timer, and a done flag. */
interface ParkedEntry {
  /** Settle the parked promise with a verdict. Called at most once (the `done` guard). */
  settle(verdict: ApprovalVerdict): void;
  /** The deadline timer to clear when the verdict lands first; `undefined` when no deadline. */
  timer: ReturnType<typeof setTimeout> | undefined;
  /** `true` once the promise has been settled — makes settle idempotent. */
  done: boolean;
}

/**
 * The runner-side routing table of parked tool-confirmation verdicts.
 *
 * One per {@link SessionLoop}. {@link park} returns a Promise the harness awaits;
 * {@link resolve} (driven by the confirmation route) settles it. {@link hasPending}
 * / {@link pendingCount} expose the outstanding-verdict bookkeeping; {@link reset}
 * denies every outstanding verdict on teardown.
 */
export class PendingApprovals {
  /** tool_use_id → the parked entry awaiting a verdict. */
  private readonly parked = new Map<string, ParkedEntry>();
  private readonly defaultTimeoutMs: number;

  constructor(opts: PendingApprovalsOptions = {}) {
    this.defaultTimeoutMs =
      opts.defaultTimeoutMs !== undefined ? opts.defaultTimeoutMs : DEFAULT_APPROVAL_WAIT_MS;
  }

  /**
   * Park a verdict for an in-flight tool call and return a Promise that settles when
   * the user's confirmation arrives (via {@link resolve}) or the wait budget elapses.
   *
   * The returned promise NEVER rejects: a delivered verdict resolves `{ approved }`;
   * a timeout resolves `{ approved: false, timedOut: true }` (a clean denial). The
   * caller (the harness's `canUseTool`) maps `approved` to the SDK permission result.
   *
   * Re-parking the same `toolUseId` overwrites the prior entry's routing slot (the
   * earlier park is abandoned — a re-issued confirmation supersedes a stale one); the
   * abandoned promise simply never settles from a verdict (its timer, if any, still
   * fires it as a denial so it cannot leak).
   *
   * @param opts The tool-use id to key on + an optional per-park timeout.
   */
  park(opts: ParkOptions): Promise<ApprovalVerdict> {
    const { toolUseId } = opts;
    const effectiveTimeout = opts.timeoutMs !== undefined ? opts.timeoutMs : this.defaultTimeoutMs;
    return new Promise<ApprovalVerdict>((resolve) => {
      const entry: ParkedEntry = {
        done: false,
        timer: undefined,
        settle: (verdict: ApprovalVerdict): void => {
          if (entry.done) {
            return;
          }
          entry.done = true;
          if (entry.timer !== undefined) {
            clearTimeout(entry.timer);
            entry.timer = undefined;
          }
          // Drop our routing slot, but ONLY if it still points at this entry — a
          // re-park for the same id replaced it, and we must not evict the newer one.
          if (this.parked.get(toolUseId) === entry) {
            this.parked.delete(toolUseId);
          }
          resolve(verdict);
        },
      };
      // A positive deadline collapses the wait to a denial; a non-positive timeout
      // disables it (wait indefinitely for a delivered verdict).
      if (effectiveTimeout > 0) {
        entry.timer = setTimeout(() => {
          entry.settle({ approved: false, timedOut: true });
        }, effectiveTimeout);
        // Do not keep the event loop alive solely for a parked approval deadline.
        entry.timer.unref?.();
      }
      this.parked.set(toolUseId, entry);
    });
  }

  /**
   * Deliver a verdict for a parked tool call.
   *
   * Called by the confirmation route when a `user.tool_confirmation` arrives.
   * Idempotent: returns `false` for an unknown id or an entry whose verdict already
   * landed (a duplicate / re-delivered confirmation), so the first verdict always
   * wins and a flapping reconnect's re-push cannot flip it. Returns `true` only when
   * this call is the one that settled a still-pending park.
   *
   * @param toolUseId The tool-use id the confirmation correlates to.
   * @param approved `true` on allow, `false` on deny.
   */
  resolve(toolUseId: string, approved: boolean): boolean {
    const entry = this.parked.get(toolUseId);
    if (entry === undefined || entry.done) {
      return false;
    }
    entry.settle({ approved });
    return true;
  }

  /**
   * Whether a verdict is outstanding.
   *
   * With a `toolUseId`, whether THAT id is parked (the per-call check the harness can
   * use). Without one, whether ANY verdict is parked — the "session is awaiting a
   * human approval" signal a mid-turn ingest guard reads so it does not steer a turn
   * past the human gate.
   */
  hasPending(toolUseId?: string): boolean {
    if (toolUseId !== undefined) {
      const entry = this.parked.get(toolUseId);
      return entry !== undefined && !entry.done;
    }
    return this.pendingCount() > 0;
  }

  /** The number of outstanding (still-pending) verdicts. */
  pendingCount(): number {
    let count = 0;
    for (const entry of this.parked.values()) {
      if (!entry.done) {
        count += 1;
      }
    }
    return count;
  }

  /**
   * Deny every outstanding verdict and clear the table.
   *
   * For teardown: a stopping harness must not leave a parked tool call blocked
   * forever, so each pending verdict resolves to a clean denial (`approved: false`).
   * Idempotent — a second reset finds nothing to deny.
   */
  reset(): void {
    const entries = [...this.parked.values()];
    this.parked.clear();
    for (const entry of entries) {
      entry.settle({ approved: false });
    }
  }
}
