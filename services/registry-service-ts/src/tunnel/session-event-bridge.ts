// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Owner-pod session event bridge — wires the runner tunnel (request/response
// over WS) to the session transcript for ONE self-hosted session whose
// environment is claimed by THIS registry replica (the "owner pod").
//
// The owner pod is the SINGLE WRITER of that session's AGENT events. The flow
// (persist-before-forward):
//
//   (a) USER turn. A client `POST /v1/sessions/:id/events` lands on ANY replica
//       and is appended to the transcript (the existing public path). The OWNER
//       pod follows the transcript for `user.*` events and, for each un-driven
//       one, sends a turn request to the runner over the runner tunnel — a
//       streaming HTTP request marshalled onto the WS by {@link TunnelTransport}.
//   (b) AGENT output. The runner answers the turn by streaming agent events back
//       as newline-delimited JSON over the tunnel. The OWNER pod PERSISTS each to
//       the transcript via {@link TranscriptStore.append} BEFORE moving on — the
//       persist-before-forward (I1) invariant — as a strict, in-order single
//       writer (`producedBy=harness`). The existing SSE path tails the SAME
//       transcript to clients, so a client only ever observes persisted agent
//       events; there is no cross-replica HTTP forwarding (the transcript is the
//       cross-pod bus).
//
// Why this is correct in a multi-replica registry: only the owner pod holds the
// environment's host tunnel + durable claim, so only the owner pod constructs
// and starts a bridge for that environment's sessions. Every agent event for the
// session is therefore appended by exactly this one bridge instance, in the order
// the runner streamed it — single-writer ordering by construction.
//
// First-turn catch-up + gapless boundary (why this is NOT a from-now tail). The
// bridge is started by the owner pod only AFTER the runner dials (the manager's
// `onRunnerConnect`), but a client can `POST` the first `user.message` BEFORE the
// runner is online — the public append path lands on any replica and does not
// wait for distribution. A pure from-now tail (`fromCursor: ''`, subscribe at the
// present head) would step PAST any `user.*` event already persisted at start, so
// that first turn would never be driven; there is likewise a race between the
// connect hook firing and the tail actually seeking to head. Closing that hole
// needs no per-session input queue (buffering input across reconnect) and no
// ready-ack gate before forwarding, because the TRANSCRIPT itself is the durable,
// ordered buffer: at {@link start} the bridge
//   1. reads the transcript from the beginning (a bounded {@link TranscriptStore.read}),
//      finding the un-driven `user.*` turns — those with NO agent (`harness`) event
//      after them — and the head seq at that moment;
//   2. drives those pending turns in order (the catch-up), then
//   3. follows live from an EXPLICIT cursor `head + 1` (a bounded resume), NOT
//      from-now. An explicit numeric tail cursor is gapless across every backend
//      (Kafka `seek`, Pulsar, Postgres): any event that lands between the bounded
//      read draining and the live tail joining has seq > head and is delivered by
//      the resume, so no input falls in a join-window gap and no ready-ack gate is
//      needed. An already-answered turn (its agent events sit after it in the
//      transcript) is NOT re-driven — the catch-up skips it — so a newest-wins
//      reconnect never re-drives a prompt a prior owner generation answered.
//
// Lossy-on-drop vs. lossless-across-reconnect — the unified-stream tradeoff. The
// turn request and its agent-event stream are ONE tunneled streaming POST, rather
// than a separate submit plus a long-lived stream backed by a per-session queue.
// On a mid-turn tunnel
// drop the un-streamed remainder of that turn is lost; only events persisted
// before the drop survive (persist-before-forward holds). Catch-up makes this
// self-healing for the meaningful case: a turn that produced NO agent event
// (runner offline, or a drop before the first persist) has nothing after it in
// the transcript, so the next bridge start (reconnect) re-drives and completes it.
// A turn that dropped AFTER partial output has agent events after it and is
// treated as answered — re-driving would duplicate the persisted prefix — so the
// persisted prefix is kept and the turn is not auto-resumed (a client re-prompt
// drives a fresh turn). The conditions that would reopen this tradeoff are in
// docs/managed-agents/roadmap.md ("Self-hosted runner transport").
//
// Composition with reverse-lookup recovery (the SAME owner pod, the SAME connect
// hook). {@link SessionEventBridgeManager.onRunnerConnect} serves a {@link
// SessionRecovery} resume replay BEFORE starting the bridge: recovery reads the
// session's persisted events and pushes them down the tunnel so the runner rebuilds
// its state (deduping by event id). Recovery only SERVES — it never appends and
// never drives a turn. The bridge below is the SOLE turn driver, so the two never
// both drive a turn (the answered-up-to rule here is the one authority on what is
// already answered). That keeps exactly-once across a reconnect: state is replayed,
// a turn is driven at most once.
//
// Reused, not re-implemented: `@orca/transcript-store` for `append`/`read`/`tail`,
// the shared {@link TunnelRegistry} + {@link TunnelTransport} for the runner
// tunnel, and `httpEventToProto` to map an agent-event JSON line into a transcript
// event. The bridge owns no sockets and no DB — the registry owns the tunnel and
// the store owns durability — so the whole module is unit-testable in-process
// against a fake runner (a ws peer speaking the tunnel protocol) and an in-memory
// store.
//
// Concurrency model: Node is single-threaded with one event loop. The bridge runs
// THREE concurrent followers over the same transcript + abort signal: the TURN loop
// (catch-up, then live tail) processes user turns serially (one in-flight turn at a
// time), so two turns never interleave their agent-event appends — the single
// writer; the CONFIRMATION loop pushes `user.tool_confirmation` verdicts to the
// runner's confirmation route; and the INTERRUPT loop pushes `user.interrupt` control
// signals to the runner's interrupt route. The control loops MUST be concurrent with
// the turn loop because a turn parked on a gated tool call — or blocked on the model —
// blocks the turn loop inside its streaming POST, so the verdict that unblocks it (or
// the interrupt that preempts it) can only be delivered by a SEPARATE loop. Both
// control loops are READ-ONLY on the transcript (they never append — the gated tool's
// output flows back through the turn's own stream, and an interrupt produces no agent
// events of its own), so neither contends with the single-writer turn loop. A single
// shared `AbortController` cancels all three loops and any in-flight turn on {@link stop}.

import { eventSourceTurnId } from './event-source-turn.js';
import {
  TunnelTransport,
  ConnectError,
  decodeBody,
  HARNESS_CHECKPOINT_EVENT,
  type TunnelResponse,
  type TransportRegistry,
} from '@orca/harness-tunnel';
import { setTimeout as delay } from 'node:timers/promises';
import type { Event, ReadOptions, TranscriptStore } from '@orca/transcript-store';
import { httpEventToProto, transcriptEventVisibility } from '../domain/events.js';
import {
  bridgeConfirmationsPushedTotal,
  bridgeInterruptsPushedTotal,
  bridgeTurnsDrivenTotal,
} from '../metrics.js';
import { SessionRecovery } from './session-recovery.js';
import { SessionSnapshotDelivery, type SnapshotProvider } from './session-snapshot-delivery.js';
import type { AgentSnapshot } from '../domain/agent-snapshot.js';
import { SessionSkillsDelivery, type SkillsProvider } from './session-skills-delivery.js';
import type { SkillStore } from '@orca/skill-store';
import { TunnelRegistry } from './tunnel-registry.js';
import { RESOURCE_CHECKPOINT_EVENT } from '@orca/sandbox-runtime';
import type { SessionResourcesDelivery } from './session-resources-delivery.js';
import {
  RunnerRequestDenied,
  type RunnerTurnAccounting,
  type RunnerAccountingContext,
} from '../domain/codex-runner-accounting.js';
import { sessionErrorPayload, sessionIdlePayload } from '@orca/agent-event-contract';
import { pinRunnerGeneration } from './runner-generation.js';

/**
 * Runner route the owner pod POSTs a turn request to. The runner serves this
 * locally (its tunnel adapter dispatches the framed request) and streams agent
 * events back as the response body. Orca-native path; the cross-component
 * contract with the runner's turn handler.
 */
export const RUNNER_TURN_PATH = '/v1/runner/turn';

/**
 * Runner route the owner pod PUSHES a tool-confirmation verdict to. Distinct from
 * the turn route because a `user.tool_confirmation` is NOT a new turn — it is the
 * client's allow/deny verdict for an in-flight gated tool call WITHIN the current
 * turn (the harness's `canUseTool` is parked on it). The bridge forwards the
 * verdict here so the runner resolves the parked approval (allow → the tool
 * proceeds; deny → a clean denial back to the model); the runner acks 2xx. Orca-
 * native path; the cross-component contract with the runner's confirmation handler.
 * Single-sourced here on the owner-pod side to match the runner's
 * `RUNNER_CONFIRMATION_PATH` EXACTLY (same path, same session header).
 *
 * The verdict's durable source of truth is the transcript `user.tool_confirmation`
 * event (it survives a restart and is re-served by recovery); this push is just the
 * LIVE delivery that unblocks the parked tool call. A re-pushed verdict (a flapping
 * reconnect re-driving the catch-up) is idempotent on the runner — the parking's
 * first verdict wins — so the bridge can safely re-forward on every (re)connect.
 */
export const RUNNER_CONFIRMATION_PATH = '/v1/runner/confirmation';
/** Independent result route; the public custom callback is not a new turn. */
export const RUNNER_CUSTOM_TOOL_RESULT_PATH = '/v1/runner/custom-tool-result';
export const CUSTOM_TOOL_RESULT_EVENT_KIND = 'user.custom_tool_result';

/**
 * The transcript event kind a client posts to deliver a tool-confirmation verdict.
 * A `user.tool_confirmation` is a `user.*` event (so the public-event filters treat
 * it as a turn-adjacent client event) but it is NOT a turn — the bridge routes it
 * to {@link RUNNER_CONFIRMATION_PATH} instead of driving it as a turn. Matches the
 * client wire shape (`{ type: 'user.tool_confirmation', tool_use_id, result }`,
 * `result` being the authoritative managed-agents-2026-04-01 `'allow' | 'deny'`
 * field — `validateClientEvent` in `src/domain/events.ts` also accepts the
 * deprecated boolean `approved` alias) and the runner's `parseToolConfirmation`
 * correlation key (`tool_use_id`).
 */
export const TOOL_CONFIRMATION_EVENT_KIND = 'user.tool_confirmation';

/**
 * Runner route the owner pod PUSHES a `user.interrupt` to. Distinct from the turn
 * route because a `user.interrupt` is NOT a new turn — it is an out-of-band control
 * signal that must PREEMPT the turn currently in flight (abort the in-flight turn
 * WITHOUT tearing down the harness). The bridge forwards it here so the runner aborts
 * the live turn and acks 2xx; routing it independently of the turn drive is what lets
 * it reach a turn the turn loop is parked/blocked on inside its streaming POST (an
 * interrupt delivered as a turn body would queue BEHIND the very turn it is meant to
 * unblock — a deadlock). Orca-native path; the cross-component contract with the
 * runner's interrupt handler. Single-sourced here on the owner-pod side to match the
 * runner's `RUNNER_INTERRUPT_PATH` EXACTLY (same path, same session header).
 *
 * The interrupt's durable source of truth is the transcript `user.interrupt` event
 * (it survives a restart and is re-served by recovery); this push is just the LIVE
 * delivery that preempts the in-flight turn. A re-pushed interrupt (a flapping
 * reconnect re-driving the catch-up) is idempotent on the runner — aborting an
 * already-finished or already-aborted turn is a harmless no-op — so the bridge can
 * safely re-forward an un-answered interrupt on every (re)connect.
 */
export const RUNNER_INTERRUPT_PATH = '/v1/runner/interrupt';

/**
 * The transcript event kind a client posts to interrupt the in-flight turn. A
 * `user.interrupt` is a `user.*` event (so the public-event filters treat it as a
 * turn-adjacent client event) but it is NOT a turn — the bridge routes it to
 * {@link RUNNER_INTERRUPT_PATH} instead of driving it as a turn. Matches the client
 * wire shape (`{ type: 'user.interrupt' }`) the runner's `parseUserInterrupt` reads
 * as a bare, parameterless control signal.
 */
export const INTERRUPT_EVENT_KIND = 'user.interrupt';

/**
 * Header carrying the session id on a turn request, so the runner can scope the
 * turn to the right conversation. Lower-cased on the wire by the frame codec; the
 * canonical-case constant is exported for the runner side to match.
 */
export const RUNNER_SESSION_HEADER = 'X-Orca-Session-Id';

/**
 * `producedBy` provenance stamped on every transcript event the bridge persists
 * from the runner's stream. Agent output is harness-produced (the same
 * provenance the in-process harness path uses), which is what the SSE
 * public-event filter and the read-model index key on.
 */
export const AGENT_EVENT_PRODUCED_BY = 'harness';

/**
 * Event-type prefix that marks a transcript event as a USER turn — the events the
 * owner pod forwards to the runner. Matches the public `user.*` convention the
 * session-create + append path emits (`producedBy=client`).
 */
export const USER_EVENT_PREFIX = 'user.';

/** Structured logger seam (a subset of the usual `req.log`). All optional. */
export interface SessionEventBridgeLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Options for {@link SessionEventBridge}. */
export interface SessionEventBridgeOptions {
  /** Recheck before forwarding turns or control events; unset only in isolated tests. */
  ownsSession?: () => Promise<boolean>;
  saveHarnessState?: (state: unknown) => Promise<void>;
  resources?: Pick<SessionResourcesDelivery, 'commit'>;
  /** Owning workspace (the tenant scope) for the transcript + tunnel owner. */
  workspaceId: string;
  /** Session id this bridge serves, e.g. `"ses_a1b2..."`. */
  sessionId: string;
  /**
   * Runner id the session is bound to — the tunnel the owner pod sends turn
   * requests over. The bridge resolves the live tunnel through the registry on
   * each turn, so a runner reconnect (newest-wins) is picked up transparently.
   */
  runnerId: string;
  /** Transcript store: tail user events, append agent events (single writer). */
  store: TranscriptStore;
  /** Shared runner-tunnel registry the {@link TunnelTransport} routes through. */
  registry: TransportRegistry;
  /**
   * Refresh policy immediately before a user turn is sent. The manager uses
   * this to detect a changed guardrail fold and re-deliver the snapshot before
   * the runner can observe the new message. A rejection fails the turn closed.
   */
  beforeTurn?: (event: Event) => Promise<void>;
  accounting?: RunnerTurnAccounting;
  /** Optional structured logger. */
  logger?: SessionEventBridgeLogger;
}

/** The minimal agent-event line the runner streams (one JSON object per line). */
interface AgentEventLine {
  /** Harness-defined event type, e.g. `"response.output_text.delta"`. */
  type: string;
  /** Optional stable event id; a fresh `evt_` id is minted when absent. */
  id?: string;
  /** Optional subagent subpath, e.g. `"subagents/<id>"`. */
  subpath?: string;
  [k: string]: unknown;
}

/**
 * The owner-pod event bridge for one self-hosted session.
 *
 * Construct one per (owner pod, session) — i.e. the replica holding the
 * session's environment claim builds and {@link start}s a bridge when the
 * session's runner is bound, and {@link stop}s it on teardown. The bridge tails
 * the transcript for user turns, drives the runner over the tunnel, and persists
 * the streamed agent events back to the transcript as the single writer.
 */
export class SessionEventBridge {
  private readonly ownsSession: SessionEventBridgeOptions['ownsSession'];
  private readonly workspaceId: string;
  private readonly sessionId: string;
  private readonly runnerId: string;
  private readonly store: TranscriptStore;
  private readonly transport: TunnelTransport;
  private readonly logger: SessionEventBridgeLogger | undefined;
  private readonly saveHarnessState: ((state: unknown) => Promise<void>) | undefined;
  private readonly beforeTurn: ((event: Event) => Promise<void>) | undefined;
  private readonly accounting: RunnerTurnAccounting | undefined;
  private readonly resources: SessionEventBridgeOptions['resources'];

  /** The tail loop's abort controller; `undefined` while stopped. */
  private abort: AbortController | undefined;
  // A failed checkpoint invalidates this runner generation, even after stop/start.
  private checkpointFailed = false;
  /** The running tail loop promise, awaited by {@link stop}. */
  private loop: Promise<void> | undefined;
  /** The in-flight turn's byte stream, closed by {@link stop} to cancel it. */
  private activeTurn: TunnelResponse | undefined;
  private latestInterruptSeq = -1;
  private activeSourceTurn: Event | undefined;

  constructor(opts: SessionEventBridgeOptions) {
    this.ownsSession = opts.ownsSession;
    this.workspaceId = opts.workspaceId;
    this.sessionId = opts.sessionId;
    this.runnerId = opts.runnerId;
    this.store = opts.store;
    this.transport = new TunnelTransport(opts.registry, opts.runnerId);
    this.logger = opts.logger;
    this.beforeTurn = opts.beforeTurn;
    this.accounting = opts.accounting;
    this.resources = opts.resources;
    this.saveHarnessState = opts.saveHarnessState;
  }

  private async stillOwnsSession(signal: AbortSignal): Promise<boolean> {
    let backoffMs = 100;
    while (!signal.aborted) {
      try {
        const owned = (await this.ownsSession?.()) ?? true;
        return !signal.aborted && owned;
      } catch (err) {
        this.logger?.error?.(
          { err, sessionId: this.sessionId },
          'session execution ownership unavailable; retrying current event',
        );
        // A failed read is not a different owner. Keep this event pending so a
        // transient DB failure cannot advance the follower past an unread turn.
        try {
          await delay(backoffMs, undefined, { signal });
        } catch {
          return false; // stop() cancels the wait.
        }
        backoffMs = Math.min(backoffMs * 2, 5000);
      }
    }
    return false;
  }

  /** Whether the bridge's tail loop is currently running. */
  get running(): boolean {
    return this.abort !== undefined && !this.abort.signal.aborted;
  }

  /**
   * Start following the transcript for user turns and driving the runner.
   *
   * Idempotent: a second call while already running is a no-op (it does not spawn
   * a second loop). The loop first CATCHES UP — a bounded read from the beginning
   * drives any `user.*` turn that has no agent (`harness`) event after it (a
   * pre-connect first turn, or an un-completed turn after a reconnect), in order —
   * and then FOLLOWS LIVE from an explicit cursor `head + 1`, gaplessly (NOT
   * from-now). An already-answered turn (agent events already sit after it) is
   * skipped, so a (re)started bridge never re-drives a prompt a prior owner
   * generation answered, while a genuinely un-driven first turn is no longer lost.
   * The loop runs detached; failures are funneled to the logger and never reject
   * out of `start`.
   */
  start(): void {
    if (this.checkpointFailed || this.abort !== undefined) {
      return;
    }
    const ac = new AbortController();
    this.abort = ac;
    // Three CONCURRENT followers over the same transcript + abort signal:
    //   - the TURN loop ({@link runLoop}) drives un-driven user turns serially,
    //     blocking on each turn's stream (single writer, no interleaved appends);
    //   - the CONFIRMATION loop ({@link runConfirmationLoop}) pushes
    //     `user.tool_confirmation` verdicts to the runner's confirmation route;
    //   - the INTERRUPT loop ({@link runInterruptLoop}) pushes `user.interrupt`
    //     control signals to the runner's interrupt route.
    // The control loops MUST run concurrently with the turn loop: a turn parked on a
    // gated tool call — or blocked on the model — blocks the turn loop inside its
    // streaming POST, so the verdict that unblocks it (or the interrupt that preempts
    // it) can only be delivered by a SEPARATE loop. Both control loops are read-only
    // on the transcript (they never append — the tool's agent output flows back
    // through the turn's own stream, and an interrupt produces no agent events), so
    // neither contends with the single-writer turn loop. All are detached; failures
    // funnel to the logger and never reject out of `start`. {@link stop} aborts the
    // shared signal and awaits all three.
    this.loop = Promise.all([
      this.runLoop(ac.signal),
      this.runConfirmationLoop(ac.signal),
      this.runInterruptLoop(ac.signal),
    ]).then(() => undefined);
  }

  /**
   * Stop following the transcript and cancel any in-flight turn.
   *
   * Aborts the loop's signal (ending the catch-up read or `tail` iterator and any
   * append wait), closes the in-flight turn's stream (sending a `request.cancel`
   * to the runner so it can stop the turn), and awaits the loop's clean exit. Safe
   * to call without a prior {@link start} and idempotent across repeated calls.
   */
  async stop(): Promise<void> {
    const ac = this.abort;
    if (ac === undefined) {
      return;
    }
    this.abort = undefined;
    ac.abort();
    const active = this.activeTurn;
    if (active !== undefined) {
      // Best-effort cancel of the in-flight turn so the runner can abort it.
      try {
        await closeTurnStream(active);
      } catch {
        // best-effort; the loop's own teardown handles the rest.
      }
    }
    const loop = this.loop;
    this.loop = undefined;
    if (loop !== undefined) {
      await loop;
    }
  }

  /**
   * The TURN loop: CATCH UP on un-driven user turns, then FOLLOW LIVE.
   *
   * One of the two concurrent followers {@link start} launches (the other is
   * {@link runConfirmationLoop}). Two contiguous, non-overlapping phases over the
   * same transcript:
   *   1. {@link catchUp} — a bounded read from the beginning that drives every
   *      `user.*` turn with no agent event after it (the pre-connect first turn,
   *      and any turn left un-completed by a previous owner generation), in order,
   *      and returns the head seq it read up to.
   *   2. live follow — {@link TranscriptStore.tail} from the EXPLICIT cursor
   *      `head + 1` (a bounded resume, NOT from-now). This is gapless: an event
   *      appended between the catch-up read draining and the tail joining has seq
   *      > head and is delivered here, so no input falls in a join-window gap.
   *
   * Serial by construction across BOTH phases — each turn is fully driven (and its
   * agent events persisted) before the next user event is pulled — so two turns
   * never interleave their appends (the single writer). A `user.tool_confirmation`
   * is NOT a turn ({@link isUserTurnEvent} excludes it); it is handled by the
   * concurrent confirmation loop, which is what lets a verdict reach a turn this
   * loop is parked on inside its streaming POST. A per-turn failure is logged and
   * swallowed so one bad turn (runner offline, tunnel drop, non-2xx) does not tear
   * down the loop; it keeps following the transcript for the next user event.
   */
  private async runLoop(signal: AbortSignal): Promise<void> {
    let head: number;
    let catchUpFailed = false;
    try {
      head = await this.catchUp(signal);
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      // The catch-up read itself failed (the store surfaced an error). We could
      // NOT establish the answered-up-to boundary, so we must follow from-now and
      // NOT from a numeric cursor: on a populated transcript the resume cursor
      // would be `"0"` (from `head = -1`), which on the real Kafka/Postgres/Pulsar
      // backends is an explicit seek to offset/seq 0 — a from-BEGINNING replay that
      // would re-drive already-answered turns (catch-up's answered-turn skip never
      // ran). From-now (`''`) instead seeks to the present head, so it drives only
      // subsequent turns without that replay. The owning caller decides whether to
      // rebuild the bridge to re-attempt the catch-up.
      this.logger?.error?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId },
        'session event bridge catch-up read failed; following from-now',
      );
      catchUpFailed = true;
      head = -1;
    }
    if (signal.aborted) {
      return;
    }
    // Pick the live-tail resume cursor:
    //   - catch-up SUCCEEDED → resume strictly after the boundary at the explicit,
    //     gapless numeric cursor `String(head + 1)`. For an EMPTY transcript
    //     (`head === -1`) that is `"0"` — an explicit seek to offset/seq 0 inclusive
    //     — which is gapless from the very first append and, the transcript being
    //     empty, replays nothing. For a populated transcript it resumes just past
    //     the last caught-up event.
    //   - catch-up FAILED → from-now `''` (the boundary is unknown; a numeric `"0"`
    //     here would be a from-beginning replay on the real backends, see above).
    const fromCursor = catchUpFailed ? '' : String(head + 1);
    try {
      for await (const event of this.store.tail(this.workspaceId, this.sessionId, {
        // Resume cursor chosen above: an explicit numeric cursor after a successful
        // catch-up (gapless), or from-now `''` after a catch-up read failure (to
        // avoid a from-beginning replay). Parent-agent user turns only; subagent
        // turns ride their own subpath and are driven by their own runner, not this
        // session bridge.
        fromCursor,
        subpath: '',
        signal,
      })) {
        if (signal.aborted) {
          return;
        }
        if (!isUserTurnEvent(event)) {
          continue;
        }
        bridgeTurnsDrivenTotal.inc({ source: 'live' });
        await this.runTurn(event, signal);
      }
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      // The tail iterator itself failed (the store surfaced an error). Log it;
      // the owning caller decides whether to rebuild the bridge.
      this.logger?.error?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId },
        'session event bridge tail loop failed',
      );
    }
  }

  /**
   * Catch-up phase: drive the un-driven user turns already in the transcript.
   *
   * A bounded {@link TranscriptStore.read} from the beginning (`fromCursor: ''`,
   * which for `read` means from offset 0 and ends at the high-watermark at call
   * time) walks the whole transcript once, tracking:
   *   - `head`: the highest seq seen — the boundary the live tail resumes after;
   *   - answered turn IDs from Registry-stamped output/completion events.
   *   - a legacy sequence boundary only for old output without a source ID.
   *
   * New output answers only its source user event. A later queued message can
   * precede that output in the transcript and must still be driven on reconnect.
   * Partial output also marks its own turn answered to avoid repeating side effects.
   *
   * Returns the head seq read up to (or -1 when the transcript was empty), which
   * {@link runLoop} turns into the live tail's explicit resume cursor.
   */
  private async catchUp(signal: AbortSignal): Promise<number> {
    const pending: Event[] = [];
    const pendingIds = new Set<string>();
    let orphanCallback: { id: string; sourceTurnId: string | undefined } | undefined;
    let head = -1;
    let legacyLastAgentSeq = -1;
    let unlinkedPrefixSeq = -1;
    const answered = new Set<string>();
    const opts: ReadOptions = {
      // From-beginning bounded read (read's `''` = from offset 0; it drains at the
      // high-watermark at call time). Parent-agent events only — subagent turns
      // are not driven by this bridge.
      fromCursor: '',
      maxEvents: 0,
      subpath: '',
    };
    for await (const event of this.store.read(this.workspaceId, this.sessionId, opts)) {
      if (signal.aborted) {
        return head;
      }
      if (event.seq > head) {
        head = event.seq;
      }
      if (!event.subpath && isAgentEvent(event)) {
        const claimedSource = eventSourceTurnId(event);
        const sourceTurnId =
          claimedSource && pendingIds.has(claimedSource) ? claimedSource : undefined;
        if (sourceTurnId) {
          answered.add(sourceTurnId);
          // Older bridges stamped only callbacks/completions. Their preceding
          // unlinked stream prefix belongs to this same serial turn, too.
          unlinkedPrefixSeq = -1;
        } else if (event.kind === 'agent.turn_completed' || event.kind === 'response.completed') {
          legacyLastAgentSeq = Math.max(legacyLastAgentSeq, event.seq);
          unlinkedPrefixSeq = -1;
        } else unlinkedPrefixSeq = Math.max(unlinkedPrefixSeq, event.seq);
        if (event.kind === 'agent.custom_tool_use') {
          orphanCallback = { id: event.id, sourceTurnId };
        }
        if (
          event.kind === 'agent.turn_completed' &&
          (!sourceTurnId || sourceTurnId === orphanCallback?.sourceTurnId)
        )
          orphanCallback = undefined;
        continue;
      }
      if (isUserTurnEvent(event)) {
        pending.push(event);
        pendingIds.add(event.id);
      }
    }
    const orphanTurn = pending.find((event) => event.id === orphanCallback?.sourceTurnId);
    if (orphanCallback) {
      // Snapshot refresh destroys the old SDK continuation. A persisted client
      // result does not prove that continuation consumed it. Close the incomplete
      // turn explicitly; replaying its result cannot resume or re-run the callback.
      await this.persistRequestDenied(
        new RunnerRequestDenied(
          `Custom tool callback ${orphanCallback.id} was abandoned when the runner disconnected; submit a new message to retry`,
          'custom_tool_callback_abandoned',
        ),
        orphanTurn?.id ?? '',
        signal,
      );
    }
    const undriven = pending.filter(
      (event) =>
        !answered.has(event.id) && event.seq > Math.max(legacyLastAgentSeq, unlinkedPrefixSeq),
    );
    for (const userEvent of undriven) {
      if (signal.aborted) {
        return head;
      }
      bridgeTurnsDrivenTotal.inc({ source: 'catchup' });
      await this.runTurn(userEvent, signal);
    }
    return head;
  }

  /**
   * The control follower: push tool-confirmation verdicts and custom-tool results.
   *
   * Runs CONCURRENTLY with {@link runLoop} (the turn loop) so a verdict can reach a
   * turn the turn loop is parked on inside its streaming POST. Two contiguous,
   * gapless phases over the same transcript, mirroring the turn loop's structure:
   *   1. {@link catchUpConfirmations} — a bounded read from the beginning that
   *      pushes every UN-ANSWERED `user.tool_confirmation` (one past the last
   *      completed turn boundary, i.e. its turn is being re-driven on catch-up),
   *      in order, and returns the head seq it read up to. A confirmation for an
   *      already-completed turn is moot and is NOT re-pushed.
   *   2. live follow — {@link TranscriptStore.tail} from the EXPLICIT cursor
   *      `head + 1` (gapless, NOT from-now), pushing each `user.tool_confirmation`
   *      as it arrives.
   *
   * Read-only on the transcript: this loop never appends (the gated tool's agent
   * output flows back through the TURN's own stream, persisted by the turn loop —
   * the single writer). A push failure is contained + logged (the verdict's durable
   * source of truth is the transcript, re-served on the next reconnect), so one bad
   * push never tears down the loop. The loop runs detached; on a catch-up READ
   * failure it follows from-now `''` (same rationale as the turn loop: a numeric
   * `"0"` would be a from-beginning replay on the real backends).
   */
  private async runConfirmationLoop(signal: AbortSignal): Promise<void> {
    let head: number;
    let catchUpFailed = false;
    try {
      head = await this.catchUpConfirmations(signal);
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      this.logger?.error?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId },
        'session event bridge confirmation catch-up read failed; following from-now',
      );
      catchUpFailed = true;
      head = -1;
    }
    if (signal.aborted) {
      return;
    }
    const fromCursor = catchUpFailed ? '' : String(head + 1);
    try {
      for await (const event of this.store.tail(this.workspaceId, this.sessionId, {
        fromCursor,
        subpath: '',
        signal,
      })) {
        if (signal.aborted) {
          return;
        }
        if (!isToolConfirmationEvent(event) && !isCustomToolResultEvent(event)) {
          continue;
        }
        await this.pushConfirmation(event, 'live', signal);
      }
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      this.logger?.error?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId },
        'session event bridge confirmation tail loop failed',
      );
    }
  }

  /**
   * Confirmation catch-up: push the UN-ANSWERED tool-confirmation verdicts already
   * in the transcript so a re-driven, re-parked gated tool call gets its verdict.
   *
   * A bounded {@link TranscriptStore.read} from the beginning walks the whole
   * transcript once, tracking the last agent (`harness`) event's seq (the "answered
   * up to here" marker, same boundary the turn catch-up uses) and collecting each
   * `user.tool_confirmation`. Only confirmations PAST the last agent event are
   * pushed: a confirmation whose turn already completed (an agent event sits after
   * it) is moot — the turn is not re-driven, so nothing is parked on that verdict —
   * while a confirmation after the last completion belongs to the turn the turn
   * loop is re-driving on catch-up, so its verdict must be re-delivered. Re-pushing
   * is harmless regardless (the runner no-ops an unknown/settled tool-use id), so
   * this filter is an optimization, not a correctness gate. Returns the head seq
   * read up to (or -1 when empty) for the live follow's gapless resume cursor.
   * Custom-tool results are always eligible for catch-up, because only their
   * canonical callback id can identify the waiter; unrelated agent output is not
   * evidence that a callback result was consumed. Unknown ids are runner no-ops.
   */
  private async catchUpConfirmations(signal: AbortSignal): Promise<number> {
    const confirmations: Event[] = [];
    let head = -1;
    let lastAgentSeq = -1;
    const opts: ReadOptions = {
      fromCursor: '',
      maxEvents: 0,
      subpath: '',
    };
    for await (const event of this.store.read(this.workspaceId, this.sessionId, opts)) {
      if (signal.aborted) {
        return head;
      }
      if (event.seq > head) {
        head = event.seq;
      }
      if (isAgentEvent(event)) {
        if (event.seq > lastAgentSeq) {
          lastAgentSeq = event.seq;
        }
        continue;
      }
      if (isToolConfirmationEvent(event) || isCustomToolResultEvent(event)) {
        confirmations.push(event);
      }
    }
    // Results correlate by canonical callback id, not by the latest arbitrary
    // agent output. Re-delivery is harmless: the runner never buffers unknown ids.
    const unanswered = confirmations.filter(
      (e) => isCustomToolResultEvent(e) || e.seq > lastAgentSeq,
    );
    for (const confirmation of unanswered) {
      if (signal.aborted) {
        return head;
      }
      await this.pushConfirmation(confirmation, 'catchup', signal);
    }
    return head;
  }

  /**
   * Push a confirmation or custom-tool result to its independent runner route.
   *
   * Sends the confirmation event's transcript payload (the client's
   * `{ type: 'user.tool_confirmation', tool_use_id, result }` JSON) as a POST to
   * {@link RUNNER_CONFIRMATION_PATH}; the runner resolves the parked approval keyed
   * by `tool_use_id` and acks 2xx. This is NOT a turn — it produces no agent-event
   * stream to persist (the gated tool's output flows back through the turn's own
   * stream), so the body is drained without persisting. Every failure is contained
   * to this push:
   *   - the runner is offline ({@link ConnectError}) → log + return;
   *   - the runner answers non-2xx → drain/close + log + return;
   *   - the tunnel drops mid-push → the drain throws → log + return.
   * A contained failure self-heals: the verdict's durable source of truth is the
   * transcript, re-served by recovery + re-pushed by catch-up on the next reconnect.
   */
  private async pushConfirmation(
    confirmation: Event,
    source: 'catchup' | 'live',
    signal: AbortSignal,
  ): Promise<void> {
    if (!(await this.stillOwnsSession(signal))) return;
    const confirmationId = confirmation.id;
    let response: TunnelResponse;
    try {
      response = await this.transport.handleRequest({
        method: 'POST',
        path: isCustomToolResultEvent(confirmation)
          ? RUNNER_CUSTOM_TOOL_RESULT_PATH
          : RUNNER_CONFIRMATION_PATH,
        headers: [[RUNNER_SESSION_HEADER, this.sessionId]],
        body: confirmation.payload,
        contentType: 'application/json',
      });
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      const offline = err instanceof ConnectError;
      bridgeConfirmationsPushedTotal.inc({ source, result: 'undelivered' });
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, confirmationId, offline },
        offline
          ? 'session event bridge could not reach runner to push tool confirmation (offline)'
          : 'session event bridge failed to push tool confirmation to runner',
      );
      return;
    }

    if (response.status < 200 || response.status >= 300) {
      await drainAndClose(response);
      bridgeConfirmationsPushedTotal.inc({ source, result: 'undelivered' });
      this.logger?.warn?.(
        {
          sessionId: this.sessionId,
          runnerId: this.runnerId,
          confirmationId,
          status: response.status,
        },
        'session event bridge got non-2xx tool confirmation ack from runner',
      );
      return;
    }

    // Drain the ack body so the request slot is released; a mid-drain drop means the
    // runner did not fully ack (contained — it self-heals on the next reconnect).
    try {
      for await (const _chunk of response.stream) {
        void _chunk;
      }
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      bridgeConfirmationsPushedTotal.inc({ source, result: 'undelivered' });
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, confirmationId },
        'session event bridge tool confirmation push ended early (runner tunnel drop)',
      );
      return;
    }
    bridgeConfirmationsPushedTotal.inc({ source, result: 'delivered' });
  }

  /**
   * The interrupt follower: push `user.interrupt` control signals to the runner.
   *
   * The sibling of {@link runConfirmationLoop} — the THIRD concurrent follower
   * {@link start} launches. Runs CONCURRENTLY with {@link runLoop} (the turn loop) so
   * an interrupt can PREEMPT a turn the turn loop is blocked on inside its streaming
   * POST (a turn parked on a gated tool call, or blocked on the model). Two contiguous,
   * gapless phases over the same transcript, mirroring the confirmation loop's
   * structure:
   *   1. {@link catchUpInterrupts} — a bounded read from the beginning that pushes
   *      every UN-ANSWERED `user.interrupt` (one past the last completed turn boundary,
   *      i.e. the turn it preempts is being re-driven on catch-up), in order, and
   *      returns the head seq it read up to. An interrupt for an already-completed turn
   *      is moot (no turn is in flight to abort) and is NOT re-pushed.
   *   2. live follow — {@link TranscriptStore.tail} from the EXPLICIT cursor
   *      `head + 1` (gapless, NOT from-now), pushing each `user.interrupt` as it
   *      arrives so it reaches the turn currently in flight.
   *
   * Read-only on the transcript: this loop never appends (an interrupt produces no
   * agent-event stream — the runner aborts the in-flight turn, whose own stream the
   * turn loop is draining, and keeps the harness alive). A push failure is contained +
   * logged (the interrupt's durable source of truth is the transcript, re-served on the
   * next reconnect), so one bad push never tears down the loop. The loop runs detached;
   * on a catch-up READ failure it follows from-now `''` (same rationale as the turn and
   * confirmation loops: a numeric `"0"` would be a from-beginning replay on the real
   * backends).
   */
  private async runInterruptLoop(signal: AbortSignal): Promise<void> {
    let head: number;
    let catchUpFailed = false;
    try {
      head = await this.catchUpInterrupts(signal);
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      this.logger?.error?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId },
        'session event bridge interrupt catch-up read failed; following from-now',
      );
      catchUpFailed = true;
      head = -1;
    }
    if (signal.aborted) {
      return;
    }
    const fromCursor = catchUpFailed ? '' : String(head + 1);
    try {
      for await (const event of this.store.tail(this.workspaceId, this.sessionId, {
        fromCursor,
        subpath: '',
        signal,
      })) {
        if (signal.aborted) {
          return;
        }
        if (!isInterruptEvent(event)) {
          continue;
        }
        await this.pushInterrupt(event, 'live', signal);
      }
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      this.logger?.error?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId },
        'session event bridge interrupt tail loop failed',
      );
    }
  }

  /**
   * Interrupt catch-up: push the UN-ANSWERED `user.interrupt` signals already in the
   * transcript so a re-driven turn that should be aborted gets its interrupt.
   *
   * A bounded {@link TranscriptStore.read} from the beginning walks the whole
   * transcript once, tracking the last agent (`harness`) event's seq (the "answered up
   * to here" marker, the same boundary the turn + confirmation catch-ups use) and
   * collecting each `user.interrupt`. Only interrupts PAST the last agent event are
   * pushed: an interrupt whose turn already completed (an agent event sits after it) is
   * moot — that turn is not re-driven, so nothing is in flight to abort, and re-pushing
   * it could abort a LATER re-driven turn it was never meant to interrupt — while an
   * interrupt after the last completion belongs to the turn the turn loop is re-driving
   * on catch-up, so it must be re-delivered to preempt it. Returns the head seq read up
   * to (or -1 when empty) for the live follow's gapless resume cursor.
   */
  private async catchUpInterrupts(signal: AbortSignal): Promise<number> {
    const interrupts: Event[] = [];
    let head = -1;
    let lastAgentSeq = -1;
    const opts: ReadOptions = {
      fromCursor: '',
      maxEvents: 0,
      subpath: '',
    };
    for await (const event of this.store.read(this.workspaceId, this.sessionId, opts)) {
      if (signal.aborted) {
        return head;
      }
      if (event.seq > head) {
        head = event.seq;
      }
      if (isAgentEvent(event)) {
        if (event.seq > lastAgentSeq) {
          lastAgentSeq = event.seq;
        }
        continue;
      }
      if (isInterruptEvent(event)) {
        interrupts.push(event);
      }
    }
    const unanswered = interrupts.filter((e) => e.seq > lastAgentSeq);
    for (const interrupt of unanswered) {
      if (signal.aborted) {
        return head;
      }
      await this.pushInterrupt(interrupt, 'catchup', signal);
    }
    return head;
  }

  /**
   * Push one `user.interrupt` control signal to the runner's interrupt route.
   *
   * Sends the interrupt event's transcript payload (the client's
   * `{ type: 'user.interrupt' }` JSON) as a POST to {@link RUNNER_INTERRUPT_PATH}; the
   * runner aborts the in-flight turn (keeping the harness alive) and acks 2xx. This is
   * NOT a turn — it produces no agent-event stream to persist (the aborted turn's own
   * stream is drained by the turn loop), so the body is drained without persisting.
   * Every failure is contained to this push:
   *   - the runner is offline ({@link ConnectError}) → log + return;
   *   - the runner answers non-2xx → drain/close + log + return;
   *   - the tunnel drops mid-push → the drain throws → log + return.
   * A contained failure self-heals: the interrupt's durable source of truth is the
   * transcript, re-served by recovery + re-pushed by catch-up on the next reconnect.
   */
  private async pushInterrupt(
    interrupt: Event,
    source: 'catchup' | 'live',
    signal: AbortSignal,
  ): Promise<void> {
    if (!(await this.stillOwnsSession(signal))) return;
    this.latestInterruptSeq = Math.max(this.latestInterruptSeq, interrupt.seq);
    // Independent followers can catch up out of order. An older interrupt must
    // not reach a later preparing/running turn or poison its accounting.
    if (this.activeSourceTurn && interrupt.seq <= this.activeSourceTurn.seq) return;
    this.accounting?.interrupt();
    const interruptId = interrupt.id;
    let response: TunnelResponse;
    try {
      response = await this.transport.handleRequest({
        method: 'POST',
        path: RUNNER_INTERRUPT_PATH,
        headers: [[RUNNER_SESSION_HEADER, this.sessionId]],
        body: interrupt.payload,
        contentType: 'application/json',
      });
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      const offline = err instanceof ConnectError;
      bridgeInterruptsPushedTotal.inc({ source, result: 'undelivered' });
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, interruptId, offline },
        offline
          ? 'session event bridge could not reach runner to push interrupt (offline)'
          : 'session event bridge failed to push interrupt to runner',
      );
      return;
    }

    if (response.status < 200 || response.status >= 300) {
      await drainAndClose(response);
      bridgeInterruptsPushedTotal.inc({ source, result: 'undelivered' });
      this.logger?.warn?.(
        {
          sessionId: this.sessionId,
          runnerId: this.runnerId,
          interruptId,
          status: response.status,
        },
        'session event bridge got non-2xx interrupt ack from runner',
      );
      return;
    }

    // Drain the ack body so the request slot is released; a mid-drain drop means the
    // runner did not fully ack (contained — it self-heals on the next reconnect).
    try {
      for await (const _chunk of response.stream) {
        void _chunk;
      }
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      bridgeInterruptsPushedTotal.inc({ source, result: 'undelivered' });
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, interruptId },
        'session event bridge interrupt push ended early (runner tunnel drop)',
      );
      return;
    }
    bridgeInterruptsPushedTotal.inc({ source, result: 'delivered' });
  }

  /**
   * Drive one user turn against the runner and persist its agent events.
   *
   * Sends the user event JSON as a streaming POST to the runner's turn route,
   * then drains the NDJSON response body, appending each well-formed agent event
   * to the transcript (awaiting each append: persist-before-forward, in order).
   * Every failure mode is contained to this turn:
   *   - the runner is offline ({@link ConnectError}) → log + return;
   *   - the runner answers non-2xx → log + drain/close without persisting;
   *   - the tunnel drops mid-stream → the body iterator throws; whatever was
   *     persisted before the drop survives (persist-before-forward), and the
   *     partial turn is logged + returned;
   *   - a single agent-event append fails → log + skip that event, continue with
   *     the rest of the stream.
   */
  private async runTurn(userEvent: Event, signal: AbortSignal): Promise<void> {
    this.activeSourceTurn = userEvent;
    try {
      await this.runSourceTurn(userEvent, signal);
    } finally {
      this.activeSourceTurn = undefined;
    }
  }

  private async runSourceTurn(userEvent: Event, signal: AbortSignal): Promise<void> {
    if (!(await this.stillOwnsSession(signal))) return;
    const userEventId = userEvent.id;
    try {
      await this.beforeTurn?.(userEvent);
    } catch (err) {
      if (signal.aborted) return;
      if (this.latestInterruptSeq > userEvent.seq) {
        await this.persistPreparationInterrupted(userEventId, signal);
        return;
      }
      if (err instanceof RunnerRequestDenied) {
        await this.persistRequestDenied(err, userEventId, signal);
        return;
      }
      this.logger?.error?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, userEventId },
        'session event bridge refused turn because runner preparation failed',
      );
      if (this.resources) {
        this.checkpointFailed = true;
        this.abort?.abort();
        if (this.activeTurn) await closeTurnStream(this.activeTurn).catch(() => {});
      }
      return;
    }
    if (!(await this.stillOwnsSession(signal))) return;
    if (this.latestInterruptSeq > userEvent.seq) {
      await this.persistPreparationInterrupted(userEventId, signal);
      return;
    }
    let response: TunnelResponse;
    try {
      response = await this.transport.handleRequest({
        method: 'POST',
        path: RUNNER_TURN_PATH,
        headers: [[RUNNER_SESSION_HEADER, this.sessionId]],
        body: userEvent.payload,
        contentType: 'application/json',
      });
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      const offline = err instanceof ConnectError;
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, userEventId, offline },
        offline
          ? 'session event bridge could not reach runner for turn (offline)'
          : 'session event bridge failed to send turn to runner',
      );
      return;
    }

    if (response.status < 200 || response.status >= 300) {
      // Non-2xx: the runner refused / errored the turn. Drain + close the stream
      // so the request slot is released, and DO NOT persist its body as agent
      // events (it is not an agent-event stream).
      await drainAndClose(response);
      this.logger?.warn?.(
        {
          sessionId: this.sessionId,
          runnerId: this.runnerId,
          userEventId,
          status: response.status,
        },
        'session event bridge got non-2xx turn response from runner',
      );
      return;
    }

    this.activeTurn = response;
    try {
      await this.persistAgentStream(response, userEventId, signal);
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      // The tunnel dropped (or the body iterator otherwise failed) mid-turn.
      // Events persisted before the drop are already durable; report the partial
      // turn and let the tail loop continue to the next user event.
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, userEventId },
        'session event bridge turn stream ended early (runner tunnel drop)',
      );
    } finally {
      this.activeTurn = undefined;
    }
  }

  /**
   * Drain the turn's NDJSON body, persisting one agent event per line in order.
   *
   * Lines are framed across body chunks (a chunk can split a line, or carry
   * several), so a running buffer reassembles complete lines on `\n` boundaries.
   * Each complete line is parsed; a malformed or typeless line is dropped (logged
   * at debug-less `warn`) without aborting the turn; a well-formed line is mapped
   * to a transcript event and appended — awaited before the next line is handled
   * (the persist-before-forward / single-writer guarantee). The trailing partial
   * line (no terminating newline) is flushed once the stream ends.
   */
  private async persistAgentStream(
    response: TunnelResponse,
    userEventId: string,
    signal: AbortSignal,
  ): Promise<void> {
    let buffer = '';
    for await (const chunk of response.stream) {
      if (signal.aborted) {
        return;
      }
      buffer += Buffer.from(chunk).toString('utf8');
      let newlineIdx = buffer.indexOf('\n');
      while (newlineIdx !== -1) {
        const line = buffer.slice(0, newlineIdx);
        buffer = buffer.slice(newlineIdx + 1);
        await this.persistAgentLine(line, userEventId, signal);
        if (signal.aborted) {
          return;
        }
        newlineIdx = buffer.indexOf('\n');
      }
    }
    // Flush a trailing line that had no terminating newline.
    if (buffer.length > 0) {
      await this.persistAgentLine(buffer, userEventId, signal);
    }
  }

  /**
   * Parse one NDJSON line and, if it is a well-formed agent event, persist it.
   *
   * A blank line, non-JSON line, non-object line, or one missing a string `type`
   * is dropped (logged) — never persisted, never aborts the turn. A valid line is
   * mapped via `httpEventToProto` (which mints a fresh `evt_` id when the line
   * carries none) with `producedBy=harness`, then appended; an append failure is
   * logged and the event skipped, leaving the rest of the turn to continue.
   */
  private async persistPreparationInterrupted(
    userEventId: string,
    signal: AbortSignal,
  ): Promise<void> {
    // Preflight can initialize accounting after the interrupt follower runs.
    // Mark that new accounting turn too; any committed guarded marker survives.
    this.accounting?.interrupt();
    await this.persistRequestDenied(
      new RunnerRequestDenied('Turn interrupted before SDK submission', 'interrupted'),
      userEventId,
      signal,
    );
  }

  private async persistRequestDenied(
    error: RunnerRequestDenied,
    userEventId: string,
    signal: AbortSignal,
  ): Promise<void> {
    if (!(await this.stillOwnsSession(signal))) return;
    const events = [
      {
        type: 'session.error',
        ...sessionErrorPayload({ type: error.errorType, message: error.message, willRetry: false }),
      },
      { type: 'session.status_idle', ...sessionIdlePayload('end_turn') },
      { type: 'agent.turn_completed', ...(userEventId ? { turn_event_id: userEventId } : {}) },
    ].map((input) =>
      httpEventToProto({
        workspaceId: this.workspaceId,
        sessionId: this.sessionId,
        producedBy: AGENT_EVENT_PRODUCED_BY,
        idempotencyKey: '',
        input,
      }),
    );
    await this.store.append(this.workspaceId, this.sessionId, events);
  }

  private async persistAgentLine(
    line: string,
    userEventId: string,
    signal: AbortSignal,
  ): Promise<void> {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      this.logger?.warn?.(
        { sessionId: this.sessionId, userEventId },
        'session event bridge dropped a non-JSON agent line',
      );
      return;
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      this.logger?.warn?.(
        { sessionId: this.sessionId, userEventId },
        'session event bridge dropped a non-object agent line',
      );
      return;
    }
    let lineObj = parsed as AgentEventLine;
    if (typeof lineObj.type !== 'string' || lineObj.type.length === 0) {
      this.logger?.warn?.(
        { sessionId: this.sessionId, userEventId },
        'session event bridge dropped an agent line with no type',
      );
      return;
    }

    // Private control record. Native SDK history is never a public transcript event.
    if (lineObj.type === HARNESS_CHECKPOINT_EVENT || lineObj.type === RESOURCE_CHECKPOINT_EVENT) {
      try {
        if (signal.aborted || !(await this.stillOwnsSession(signal)))
          throw new Error('checkpoint runner generation is no longer current');
        if (lineObj.type === RESOURCE_CHECKPOINT_EVENT) {
          if (!this.resources || lineObj.subpath) throw new Error('unexpected resource checkpoint');
          await this.resources.commit(lineObj.checkpoint_id, lineObj.manifest_sha256);
        } else {
          if (
            (lineObj.provider !== 'codex-sdk' && lineObj.provider !== 'pi-sdk') ||
            lineObj.subpath
          )
            throw new Error('unsupported harness checkpoint');
          if (!this.saveHarnessState && !this.accounting)
            throw new Error('harness checkpoint persistence is unavailable');
          if (this.accounting) await this.accounting.checkpoint(userEventId, lineObj.state);
          else await this.saveHarnessState!(lineObj.state);
        }
      } catch (err) {
        // Native history has advanced beyond durable state. Stop all followers
        // before another user turn/control can reach this generation. Do not
        // await stop() here: it joins this very turn loop and would deadlock.
        this.checkpointFailed = true;
        this.abort?.abort();
        this.logger?.error?.(
          { err, sessionId: this.sessionId, runnerId: this.runnerId, userEventId },
          'session event bridge retired after checkpoint persistence failed',
        );
        if (this.activeTurn) await closeTurnStream(this.activeTurn).catch(() => {});
        throw err;
      }
      return;
    }
    if (!lineObj.subpath) {
      // The request, never the SDK, owns source identity. Persist it for every
      // parent output so partial streams cannot swallow later queued messages.
      lineObj = {
        ...lineObj,
        source_event_id: userEventId,
        ...(lineObj.type === 'agent.turn_completed' ? { turn_event_id: userEventId } : {}),
      };
    }
    if (this.accounting) {
      try {
        if (!(await this.stillOwnsSession(signal))) throw new Error('runner generation retired');
        if (lineObj.type === 'agent.usage')
          lineObj = (await this.accounting.usage(userEventId, lineObj)) as AgentEventLine;
        if (lineObj.type === 'agent.error' || lineObj.type === 'session.error')
          this.accounting.fail();
        if (lineObj.type === 'agent.turn_completed') await this.accounting.complete(userEventId);
      } catch (err) {
        // The SDK may already have advanced history. Preserve the pending usage
        // marker and fence every follower until reconnect recovery owns the turn.
        this.checkpointFailed = true;
        this.abort?.abort();
        this.logger?.error?.(
          { err, sessionId: this.sessionId, runnerId: this.runnerId, userEventId },
          'session event bridge retired after usage accounting failed',
        );
        if (this.activeTurn) await closeTurnStream(this.activeTurn).catch(() => {});
        throw err;
      }
    }
    let proto: Event;
    try {
      proto = httpEventToProto({
        workspaceId: this.workspaceId,
        sessionId: this.sessionId,
        producedBy: AGENT_EVENT_PRODUCED_BY,
        // Agent events are not client-idempotent; the runner stream is the
        // source, and the per-event evt_ id carries identity for replay dedup.
        idempotencyKey: '',
        input: lineObj,
      });
    } catch (err) {
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, userEventId, type: lineObj.type },
        'session event bridge could not map an agent line to a transcript event',
      );
      return;
    }

    if (signal.aborted) {
      return;
    }
    try {
      // Persist BEFORE moving on (await): the single-writer, persist-before-
      // forward append. The SSE path tails the same transcript, so this is the
      // only forward — the bridge never writes to a client socket.
      await this.store.append(this.workspaceId, this.sessionId, [proto]);
    } catch (err) {
      this.logger?.error?.(
        { err, sessionId: this.sessionId, userEventId, type: proto.kind },
        'session event bridge failed to persist an agent event',
      );
      if (this.resources) {
        this.checkpointFailed = true;
        this.abort?.abort();
        if (this.activeTurn) await closeTurnStream(this.activeTurn).catch(() => {});
        throw err;
      }
    }
  }
}

/**
 * Resolves a runner id to the (workspace, session) it is bound to.
 *
 * The owner-pod manager needs this to build a bridge for a runner that just
 * connected. {@link DistributionSessionStore} already exposes `findBoundSessionId`
 * + `load`; this narrower seam is what the manager depends on so it is unit-
 * testable against an in-memory fake without the whole distribution store.
 */
export interface BoundSessionResolver {
  /**
   * Resolve the runner's bound session, or `null` when no session is bound to it
   * (a runner this replica did not distribute, or one already cleaned up).
   */
  resolveBoundSession(runnerId: string): Promise<{ workspaceId: string; sessionId: string } | null>;
}

/**
 * The serving-side snapshot of one {@link SessionRecovery} the manager ran on a
 * runner (re)connect, BEFORE starting the bridge.
 *
 * This is the manager-boundary form of recovery's serving-side observation: the
 * owner pod hands back what it replayed and whether a user turn looked pending
 * (the `agent.turn_completed` rule) for diagnostics and an explicit-resume seam.
 * It is NOT a turn-dispatch signal — the {@link SessionEventBridge} started right
 * after is the SOLE turn driver, with its own "any agent event after = answered"
 * rule, so `pendingUserTurnId` never feeds a second driver (that would risk
 * duplicating a persisted prefix; see the exactly-once note on
 * {@link SessionRecovery}). It is exposed so the connect sequence consumes the
 * observation (rather than discarding it) and ops / tests can see what recovery
 * served for a given (re)connect.
 */
export interface RecoveryObservation {
  /** Number of transcript events the owner pushed to the runner this recovery. */
  replayedCount: number;
  /** Whether the runner accepted the replay (a 2xx ack, fully drained). */
  replayDelivered: boolean;
  /** `true` when the transcript exceeded the read cap and only a prefix replayed. */
  replayTruncated: boolean;
  /**
   * The id of the user turn that looked pending past the last completed
   * `agent.turn_completed`, or `null` when none did. A SERVING-SIDE OBSERVATION
   * only — the bridge (not this) drives turns.
   */
  pendingUserTurnId: string | null;
}

/** Options for {@link SessionEventBridgeManager}. */
export interface ManagedRunnerResourcesContext {
  workspaceId: string;
  sessionId: string;
  runnerId: string;
  registry: TransportRegistry;
  isCurrent(): boolean;
}

export type ManagedRunnerResourcesFactory = (context: ManagedRunnerResourcesContext) => Promise<
  | (Pick<SessionResourcesDelivery, 'deliver' | 'commit'> & {
      publishMounted?(): Promise<void>;
      refreshGitCapabilities?(): Promise<void>;
    })
  | null
>;

export interface SessionEventBridgeManagerOptions {
  accountingFactory?: (context: RunnerAccountingContext) => RunnerTurnAccounting;
  resourcesFactory?: ManagedRunnerResourcesFactory;
  ownsSession?: (workspaceId: string, sessionId: string) => Promise<boolean>;
  saveHarnessState?: (
    workspaceId: string,
    sessionId: string,
    runnerId: string,
    state: unknown,
  ) => Promise<void>;
  /** Transcript store every bridge tails + appends through. */
  store: TranscriptStore;
  /** Shared runner-tunnel registry every bridge's transport routes through. */
  registry: TunnelRegistry;
  /** Resolve a connecting runner's bound (workspace, session). */
  resolver: BoundSessionResolver;
  /** Optional structured logger, threaded into each bridge + the manager. */
  logger?: SessionEventBridgeLogger;
  /**
   * Composes + delivers the credential-free agent snapshot to a (re)connecting
   * runner BEFORE recovery + the bridge. When wired (the `AgentSnapshotResolver`
   * in production), the owner pod pushes the snapshot — model + provider +
   * composed system + tool allowlists + egress config — over the runner tunnel so
   * the runner spins up the right harness/provider and reaches credentialed
   * upstreams the right way before the first turn. Absent → no snapshot is
   * delivered (a deployment whose runner is configured another way); the bridge
   * still runs. A delivery failure is contained (logged, the bridge still starts)
   * and self-heals on the next reconnect.
   */
  snapshotProvider?: SnapshotProvider;
  /**
   * Composes the SESSION-WIDE Skill-bundle union for a (re)connecting runner. When
   * wired ALONGSIDE {@link skillStore} (the `AgentSnapshotResolver` in production), the
   * owner pod PUSHES the session's pinned Skill bundle bytes over the tunnel BEFORE the
   * snapshot, so the colocated runner materializes them as a native `--plugin-dir`
   * plugin (it holds no `@orca/skill-store` + no object-store credentials of its own).
   * Absent → no skills push (a session with no Skills, or a deployment that delivers
   * them another way); the snapshot + bridge run unchanged. A push failure is contained.
   */
  skillsProvider?: SkillsProvider;
  /**
   * Opens the exact Registry-pinned Skill bundles the {@link skillsProvider} names —
   * the owner pod's `@orca/skill-store`. Required for the skills push (paired with
   * {@link skillsProvider}); absent → no skills push.
   */
  skillStore?: SkillStore;
  /**
   * Whether to serve a reverse-lookup resume replay ({@link SessionRecovery}) to
   * the runner on (re)connect, BEFORE starting the bridge. Defaults to `true` —
   * the owner pod looks up the session's persisted events and pushes them forward
   * over the tunnel so the runner rebuilds its state, deduping by event id. The
   * bridge (started after) remains the single turn DRIVER; recovery only serves
   * state, so the two never both drive a turn. Set `false` only to disable the
   * resume serve (e.g. a deployment whose runner reconstructs state another way).
   */
  recoverOnConnect?: boolean;
}

/**
 * Owns one {@link SessionEventBridge} per connected runner ON THIS replica.
 *
 * Wired to the runner-tunnel route's connect / disconnect hooks. When a runner
 * connects, THIS replica is the owner pod for that runner's session (it holds the
 * environment's host tunnel + claim, which is what drove the launch), so the
 * manager resolves the runner's bound session, serves a reverse-lookup resume
 * replay ({@link SessionRecovery} — the owner pod pushes the session's persisted
 * events forward over the tunnel so the runner rebuilds its state, deduping by
 * event id), and THEN starts a bridge: the single writer of that session's agent
 * events and the sole turn driver. Recovery only serves state; the bridge alone
 * drives turns, so the two never both drive a turn (exactly-once). When the
 * runner's tunnel closes, the manager stops the bridge. A reconnect (newest-wins)
 * recovers + starts a fresh bridge, replacing any prior one for the same runner id.
 *
 * Cloud sessions and runners this replica did not distribute resolve to no bound
 * session, so {@link onRunnerConnect} is a no-op for them — the manager is safe to
 * wire for every runner tunnel.
 */
export class SessionEventBridgeManager {
  private readonly ownsSession: SessionEventBridgeManagerOptions['ownsSession'];
  private readonly saveHarnessState: SessionEventBridgeManagerOptions['saveHarnessState'];
  private readonly store: TranscriptStore;
  private readonly registry: TunnelRegistry;
  private readonly resolver: BoundSessionResolver;
  private readonly resourcesFactory: ManagedRunnerResourcesFactory | undefined;
  private readonly accountingFactory: SessionEventBridgeManagerOptions['accountingFactory'];
  private readonly connects = new Map<string, symbol>();
  /** Drain preparation side effects even before a bridge enters the live map. */
  private readonly preparing = new Map<string, Promise<void>>();
  private readonly logger: SessionEventBridgeLogger | undefined;
  private readonly recoverOnConnect: boolean;
  private readonly snapshotProvider: SnapshotProvider | undefined;
  private readonly skillsProvider: SkillsProvider | undefined;
  private readonly skillStore: SkillStore | undefined;
  /** Live bridges keyed by runner id. */
  private readonly bridges = new Map<string, SessionEventBridge>();
  /** Preserve the drain barrier even after a bridge leaves the live map. */
  private readonly retiring = new Map<string, Promise<void>>();
  /**
   * The serving-side observation from the most recent recovery per runner id —
   * what the owner pod replayed on that runner's last (re)connect, including
   * whether a turn looked pending. Diagnostics / explicit-resume seam ONLY; the
   * bridge is the sole turn driver, so this never drives a turn. Retained per
   * live runner and cleared when the runner's bridge is torn down.
   */
  private readonly lastRecovery = new Map<string, RecoveryObservation>();

  constructor(opts: SessionEventBridgeManagerOptions) {
    this.ownsSession = opts.ownsSession;
    this.saveHarnessState = opts.saveHarnessState;
    this.store = opts.store;
    this.registry = opts.registry;
    this.resolver = opts.resolver;
    this.resourcesFactory = opts.resourcesFactory;
    this.accountingFactory = opts.accountingFactory;
    this.logger = opts.logger;
    this.recoverOnConnect = opts.recoverOnConnect ?? true;
    this.snapshotProvider = opts.snapshotProvider;
    this.skillsProvider = opts.skillsProvider;
    this.skillStore = opts.skillStore;
  }

  /**
   * Recover + start a bridge for a runner that just connected, if it is bound to a
   * session.
   *
   * The owner-pod (re)connect sequence (the reconnect catch-up, performed
   * owner-side):
   *   1. Resolve the runner's bound (workspace, session); a no-op when none is
   *      bound (a cloud / not-distributed-here runner).
   *   2. Stop any prior bridge for this runner id (newest-wins reconnect) so the
   *      owner pod runs exactly one bridge (one single writer) per generation.
   *   3. SNAPSHOT DELIVERY (when a snapshot provider is wired): compose the
   *      credential-free agent snapshot (model + provider + composed system + tool
   *      allowlists + egress config) and PUSH it over the tunnel so the runner
   *      spins up the right harness/provider and reaches credentialed upstreams the
   *      right way. This runs BEFORE recovery + the bridge so the runner is
   *      configured before any state is replayed or any turn is driven. A delivery
   *      failure is contained (logged, the bridge still starts) and self-heals on
   *      the next reconnect.
   *   4. REVERSE-LOOKUP RECOVERY (when enabled): construct a {@link SessionRecovery}
   *      and serve a resume replay — the owner pod reads the session's persisted
   *      events and PUSHES them forward over the tunnel so the runner rebuilds its
   *      state (the runner dedups by event id). The replay slice is INCREMENTAL
   *      `after={cursor}` when the runner presented a last-consumed cursor for this
   *      session in its hello, and a FRESH FULL replay otherwise (no cursor / a
   *      fresh runner). This runs BEFORE the bridge so the runner has its context
   *      back before any pending turn is driven. Recovery never drives a turn and
   *      never appends, so it cannot duplicate state; a delivery failure is
   *      contained (logged, the bridge still starts) and self-heals on the next
   *      reconnect.
   *   5. Start the bridge — the SINGLE WRITER + sole turn driver. Its catch-up
   *      drives the un-driven turns (by the bridge's answered-up-to rule, which
   *      skips a turn that already has agent output), then it follows live.
   *
   * @param runnerId The connecting runner's id.
   * @param resumeCursors Per-session last-consumed cursors the runner presented in
   *   its hello (`sessionId → eventId`). The cursor for the resolved bound session
   *   selects an incremental `after={cursor}` replay; a session absent from the map
   *   (or an empty map) yields a fresh full replay. Defaults to `{}`.
   *
   * Never throws — a resolve / recovery / start failure is logged so the
   * runner-tunnel connect hook (which awaits this) is not torn down.
   */
  async onRunnerConnect(
    runnerId: string,
    resumeCursors: Readonly<Record<string, string>> = {},
  ): Promise<void> {
    const connect = Symbol(runnerId);
    this.connects.set(runnerId, connect);
    const generation = pinRunnerGeneration(this.registry, runnerId);
    const isCurrent = () => this.connects.get(runnerId) === connect && generation.isCurrent();
    const previous = this.preparing.get(runnerId);
    const preparing = (async () => {
      // Retire the previous generation immediately, then join any append already
      // in flight. Its receipt must be visible before the new generation reads it.
      await previous?.catch(() => {});
      if (this.connects.get(runnerId) !== connect) return;
      await this.prepareRunnerConnection(runnerId, resumeCursors, generation.registry, isCurrent);
    })();
    this.preparing.set(runnerId, preparing);
    try {
      await preparing;
    } finally {
      if (this.preparing.get(runnerId) === preparing) this.preparing.delete(runnerId);
    }
  }

  private async prepareRunnerConnection(
    runnerId: string,
    resumeCursors: Readonly<Record<string, string>>,
    registry: TransportRegistry,
    isCurrent: () => boolean,
  ): Promise<void> {
    let bound: { workspaceId: string; sessionId: string } | null;
    try {
      bound = await this.resolver.resolveBoundSession(runnerId);
    } catch (err) {
      this.logger?.error?.(
        { err, runnerId },
        'session event bridge manager failed to resolve runner',
      );
      return;
    }
    if (bound === null || !isCurrent()) {
      return;
    }
    // Replace any prior bridge for this runner id (newest-wins reconnect). Drop
    // the prior recovery observation too, so a fresh recovery that throws does not
    // leave a stale observation from an earlier generation behind.
    await this.retireBridge(runnerId);
    if (!isCurrent()) return;
    try {
      if (this.ownsSession && !(await this.ownsSession(bound.workspaceId, bound.sessionId))) return;
    } catch (err) {
      this.logger?.error?.({ err, runnerId }, 'session execution ownership unavailable');
      return;
    }
    let resources: Awaited<ReturnType<ManagedRunnerResourcesFactory>> = null;
    let managed: AgentSnapshot['managed_resources'];
    let guardrailFingerprint: string | undefined;
    try {
      if (!isCurrent()) return;
      resources =
        (await this.resourcesFactory?.({
          ...bound,
          runnerId,
          registry,
          isCurrent,
        })) ?? null;
      if (resources) managed = await resources.deliver();
      if (!isCurrent()) return;
      await this.deliverSkills(runnerId, bound, registry, resources !== null);
      if (!isCurrent()) return;
      await resources?.refreshGitCapabilities?.();
      guardrailFingerprint = await this.deliverSnapshot(runnerId, bound, registry, managed);
      if (!isCurrent()) return;
    } catch (err) {
      this.logger?.error?.(
        { err, runnerId, sessionId: bound.sessionId },
        'managed runner preparation failed; bridge not started',
      );
      return;
    }
    // Serve the reverse-lookup resume replay BEFORE starting the bridge so the
    // runner rebuilds its state before any pending turn is driven. Recovery only
    // serves (reads + pushes); the bridge below is the sole turn driver, so the
    // two never both drive a turn. Managed resources require a complete replay ACK;
    // legacy providers retain their existing reconnect recovery behavior.
    // The serving-side observation is retained per runner (diagnostics / explicit-
    // resume seam) so the pending-turn signal is consumed at the manager boundary,
    // never fed into a second driver.
    if (this.recoverOnConnect) {
      // Select the cursor the runner presented for THIS bound session (if any).
      // An absent/empty cursor means a fresh full replay; a present one means an
      // incremental `after={cursor}` slice — the runner's per-session
      // last-consumed id, sourced from its hello.
      const cursor = resumeCursors[bound.sessionId] ?? '';
      const observation = await this.recoverSession(runnerId, bound, cursor, registry);
      if (!isCurrent()) return;
      if (observation !== null) {
        this.lastRecovery.set(runnerId, observation);
      }
      if (
        resources &&
        (!observation || !observation.replayDelivered || observation.replayTruncated)
      ) {
        this.logger?.error?.(
          { runnerId, sessionId: bound.sessionId },
          'managed runner recovery was not acknowledged; bridge not started',
        );
        return;
      }
    }
    if (!isCurrent()) return;
    try {
      await resources?.publishMounted?.();
    } catch (err) {
      this.logger?.error?.(
        { err, runnerId, sessionId: bound.sessionId },
        'managed resource publication failed; bridge not started',
      );
      return;
    }
    if (!isCurrent()) return;
    const accounting = resources
      ? this.accountingFactory?.({ ...bound, runnerId, isCurrent })
      : undefined;
    const bridge = new SessionEventBridge({
      ...(accounting ? { accounting } : {}),
      ownsSession: async () => {
        if (!isCurrent()) return false;
        const current = await this.resolver.resolveBoundSession(runnerId);
        return (
          current?.workspaceId === bound.workspaceId &&
          current.sessionId === bound.sessionId &&
          ((await this.ownsSession?.(bound.workspaceId, bound.sessionId)) ?? true) &&
          isCurrent()
        );
      },
      ...(resources ? { resources } : {}),
      ...(this.saveHarnessState
        ? {
            saveHarnessState: (state: unknown) =>
              this.saveHarnessState!(bound.workspaceId, bound.sessionId, runnerId, state),
          }
        : {}),
      workspaceId: bound.workspaceId,
      sessionId: bound.sessionId,
      runnerId,
      store: this.store,
      registry,
      ...(this.snapshotProvider !== undefined
        ? {
            beforeTurn: this.guardrailRefreshHook(
              runnerId,
              bound,
              guardrailFingerprint,
              registry,
              resources,
              isCurrent,
              accounting,
            ),
          }
        : {}),
      ...(this.logger !== undefined ? { logger: this.logger } : {}),
    });
    this.bridges.set(runnerId, bridge);
    bridge.start();
    this.logger?.info?.(
      { runnerId, sessionId: bound.sessionId },
      'session event bridge started (owner pod)',
    );
  }

  /**
   * Compose + deliver the credential-free agent snapshot to a (re)connecting
   * runner, before recovery + the bridge.
   *
   * Constructs a {@link SessionSnapshotDelivery} over the runner tunnel and pushes
   * the snapshot the provider composes. Every outcome is contained so the bridge
   * always gets to start:
   *   - the provider returns `null` (nothing to deliver) → skipped, no push;
   *   - the provider THROWS (a snapshot that could not be built — e.g. a missing
   *     skill_version) → caught + logged here (the connect hook must not be torn
   *     down); the bridge still starts and the next reconnect re-attempts;
   *   - a transport failure (offline / non-2xx / drop) → the delivery contains it
   *     and reports `delivered: false`, self-healing on the next reconnect.
   *
   * Called only when a {@link SnapshotProvider} is wired (the guard is in
   * {@link onRunnerConnect}).
   */
  /**
   * Resolve + open + push the session's Skill bundle bytes to a (re)connecting runner,
   * BEFORE the snapshot. Constructs a {@link SessionSkillsDelivery} over the runner
   * tunnel and pushes the bundles the provider names. Every outcome is contained so the
   * bridge (and the subsequent snapshot delivery) always get to run:
   *   - the session has no Skills → skipped, no push;
   *   - a bundle open / integrity failure → caught + logged here (a fault must not tear
   *     down the connect hook); the snapshot + bridge still proceed and the next reconnect
   *     re-attempts;
   *   - a transport failure (offline / non-2xx / drop) → the delivery contains it and
   *     reports `delivered: false`, self-healing on the next reconnect.
   *
   * Called only when BOTH a {@link SkillsProvider} and a {@link SkillStore} are wired
   * (the guard is in {@link onRunnerConnect}).
   */
  private async deliverSkills(
    runnerId: string,
    bound: { workspaceId: string; sessionId: string },
    registry: TransportRegistry,
    strict = false,
  ): Promise<void> {
    const provider = this.skillsProvider;
    const skillStore = this.skillStore;
    if (provider === undefined || skillStore === undefined) {
      if (strict) throw new Error('managed Skills delivery is unavailable');
      return;
    }
    const delivery = new SessionSkillsDelivery({
      workspaceId: bound.workspaceId,
      sessionId: bound.sessionId,
      runnerId,
      registry,
      provider,
      skillStore,
      deliverEmpty: strict,
      ...(this.logger !== undefined ? { logger: this.logger } : {}),
    });
    try {
      const outcome = await delivery.deliver();
      if (strict && !outcome.delivered) throw new Error('managed Skills were not acknowledged');
    } catch (err) {
      if (strict) throw err;
      // A Skill union that could not be resolved (a missing binding) or a bundle that
      // could not be opened/validated is a real fault, but it must not tear down the
      // connect hook — log + continue to snapshot delivery + recovery + the bridge.
      this.logger?.error?.(
        { err, runnerId, sessionId: bound.sessionId },
        'skills delivery failed before snapshot (continuing)',
      );
    }
  }

  private async deliverSnapshot(
    runnerId: string,
    bound: { workspaceId: string; sessionId: string },
    registry: TransportRegistry,
    managed?: AgentSnapshot['managed_resources'],
  ): Promise<string | undefined> {
    const provider = this.snapshotProvider;
    if (provider === undefined) {
      if (managed) throw new Error('managed snapshot provider is unavailable');
      return undefined;
    }
    let snapshot: AgentSnapshot | null;
    try {
      snapshot = await provider.resolve(bound.sessionId);
    } catch (err) {
      if (managed) throw err;
      this.logger?.error?.(
        { err, runnerId, sessionId: bound.sessionId },
        'snapshot delivery failed before bridge start (continuing to start bridge)',
      );
      return undefined;
    }
    if (snapshot === null) {
      if (managed) throw new Error('managed snapshot no longer resolves');
      return undefined;
    }
    if (managed)
      snapshot = {
        ...snapshot,
        managed_resources: managed,
        ...(this.accountingFactory &&
        (snapshot.provider === 'codex-sdk' || snapshot.provider === 'pi-sdk')
          ? { request_guardrails_owner: 'registry' as const }
          : {}),
      };
    const delivery = new SessionSnapshotDelivery({
      workspaceId: bound.workspaceId,
      sessionId: bound.sessionId,
      runnerId,
      registry,
      provider: { resolve: async () => snapshot },
      ...(this.logger !== undefined ? { logger: this.logger } : {}),
    });
    try {
      const outcome = await delivery.deliver();
      if (managed && !outcome.delivered) throw new Error('managed snapshot was not acknowledged');
      return outcome.delivered ? guardrailSnapshotFingerprint(snapshot) : undefined;
    } catch (err) {
      if (managed) throw err;
      // A snapshot that could not be composed (a missing skill_version, etc.) is a
      // real fault, but it must not tear down the connect hook or stop the bridge —
      // the runner can still be driven (it may have a prior snapshot, or the next
      // reconnect re-attempts). Log + continue to recovery + the bridge.
      this.logger?.error?.(
        { err, runnerId, sessionId: bound.sessionId },
        'snapshot delivery failed before bridge start (continuing to start bridge)',
      );
      return undefined;
    }
  }

  /**
   * Poll the Registry-composed guardrail fold before every turn and re-deliver
   * the full snapshot only when that fold changed. Guardrail edits therefore
   * take effect on the next message without restarting the runner, while an
   * unchanged policy does not tear down a persistent harness each turn.
   *
   * A changed snapshot that cannot be delivered rejects the hook. The bridge
   * then refuses the turn rather than running it under the stale policy.
   */
  private guardrailRefreshHook(
    runnerId: string,
    bound: { workspaceId: string; sessionId: string },
    initialFingerprint: string | undefined,
    registry: TransportRegistry,
    resources: Awaited<ReturnType<ManagedRunnerResourcesFactory>>,
    isCurrent: () => boolean,
    accounting: RunnerTurnAccounting | undefined,
  ): (event: Event) => Promise<void> {
    let deliveredFingerprint = initialFingerprint;
    return async (event) => {
      const provider = this.snapshotProvider;
      if (provider === undefined) return;
      if (!isCurrent()) throw new Error('runner generation retired');
      const managed = resources ? await resources.deliver() : undefined;
      if (resources) await this.deliverSkills(runnerId, bound, registry, true);
      if (!isCurrent()) throw new Error('runner generation retired');
      let snapshot = await provider.resolve(bound.sessionId);
      if (snapshot === null) {
        throw new Error('guardrail snapshot no longer resolves for the bound session');
      }
      if (managed)
        snapshot = {
          ...snapshot,
          managed_resources: managed,
          ...(this.accountingFactory &&
          (snapshot.provider === 'codex-sdk' || snapshot.provider === 'pi-sdk')
            ? { request_guardrails_owner: 'registry' as const }
            : {}),
        };
      if (!isCurrent()) throw new Error('runner generation retired');
      const nextFingerprint = guardrailSnapshotFingerprint(snapshot);
      if (
        !resources &&
        snapshot.provider !== 'codex-sdk' &&
        snapshot.provider !== 'pi-sdk' &&
        nextFingerprint === deliveredFingerprint
      )
        return;
      const delivery = new SessionSnapshotDelivery({
        workspaceId: bound.workspaceId,
        sessionId: bound.sessionId,
        runnerId,
        registry,
        provider: { resolve: async () => snapshot },
        ...(this.logger !== undefined ? { logger: this.logger } : {}),
      });
      await resources?.refreshGitCapabilities?.();
      const outcome = await delivery.deliver();
      if (!outcome.delivered) {
        throw new Error('changed guardrail snapshot was not accepted by the runner');
      }
      await resources?.publishMounted?.();
      deliveredFingerprint = nextFingerprint;
      await accounting?.prepare(event, snapshot);
      await resources?.refreshGitCapabilities?.();
    };
  }

  /**
   * Serve a reverse-lookup resume replay to a (re)connecting runner.
   *
   * The owner pod reads the session's persisted parent-agent events and PUSHES
   * them forward over the tunnel so the runner rebuilds its state (the runner
   * dedups by event id against whatever it already holds). The slice is keyed on
   * the `cursor` the runner presented for this session in its hello:
   *   - a NON-EMPTY cursor → an incremental `after={cursor}` slice (only the events
   *     the runner has not yet consumed), the resume win on a reconnect;
   *   - an EMPTY cursor (a fresh runner, or one whose in-memory state is gone) → a
   *     FRESH full replay from the start of the transcript.
   * The cursor is selected by {@link onRunnerConnect} from the per-session hello
   * map — the runner's per-session last-consumed id, sourced from the runner side.
   *
   * Recovery reads + pushes only; it never drives a turn or appends (the bridge is
   * the single writer + sole driver), so the replay cannot duplicate state. Every
   * failure is contained so the bridge always gets to start:
   *   - a transcript READ failure throws out of `recover` → caught + logged here,
   *     and this returns `null` (no observation to record);
   *   - a DELIVERY failure (offline / non-2xx / drop) does not throw — `recover`
   *     returns `replayDelivered: false`, which self-heals on the next reconnect.
   *
   * Returns the serving-side {@link RecoveryObservation} (what was replayed +
   * whether a turn looked pending) so the connect sequence consumes it as a
   * diagnostic / explicit-resume seam, or `null` when recovery threw before it
   * could compute one. The returned `pendingUserTurnId` is NEVER used to drive a
   * turn — the bridge started right after is the sole driver.
   *
   * @param cursor The runner's last-consumed event id for this session, or `""`
   *   for a fresh full replay.
   */
  private async recoverSession(
    runnerId: string,
    bound: { workspaceId: string; sessionId: string },
    cursor: string,
    registry: TransportRegistry,
  ): Promise<RecoveryObservation | null> {
    const recovery = new SessionRecovery({
      workspaceId: bound.workspaceId,
      sessionId: bound.sessionId,
      runnerId,
      store: this.store,
      registry,
      ...(this.logger !== undefined ? { logger: this.logger } : {}),
    });
    try {
      const outcome = await recovery.recover(cursor);
      this.logger?.info?.(
        {
          runnerId,
          sessionId: bound.sessionId,
          cursor,
          fresh: outcome.fresh,
          replayedCount: outcome.replayedCount,
          replayDelivered: outcome.replayDelivered,
          pending: outcome.pendingUserTurn?.id ?? null,
        },
        'session recovery served resume replay (owner pod)',
      );
      return {
        replayedCount: outcome.replayedCount,
        replayDelivered: outcome.replayDelivered,
        replayTruncated: outcome.replayTruncated,
        pendingUserTurnId: outcome.pendingUserTurn?.id ?? null,
      };
    } catch (err) {
      // The manager requires a complete recovery ACK for managed resources.
      // Legacy providers preserve their existing catch-up/reconnect behavior.
      this.logger?.error?.(
        { err, runnerId, sessionId: bound.sessionId },
        'session recovery failed before bridge start',
      );
      return null;
    }
  }

  /**
   * Stop the bridge for a runner whose tunnel closed.
   *
   * A no-op when no bridge is live for the runner id. Idempotent: a second close
   * (or a close for a runner the manager never bridged) does nothing.
   *
   * Reconnect guard: the runner-tunnel route fires the disconnect hook for the
   * OLD generation when a runner reconnects (newest-wins). If a newer tunnel is
   * already online for this runner id, the close was a replacement, not a death —
   * the live bridge (started or replaced by the matching connect hook) must keep
   * running, so this skips the stop. Only a runner that is truly gone (offline in
   * the registry) tears its bridge down. This mirrors the distributor's
   * `markRunnerDisconnected` reconnect guard, so a flap never strands a session
   * without its single writer.
   */
  async onRunnerDisconnect(runnerId: string): Promise<void> {
    if (this.registry.has(runnerId)) {
      // A newer tunnel is live for this runner id; the close was a reconnect
      // replacement. Leave the live bridge in place.
      return;
    }
    this.connects.delete(runnerId);
    await Promise.all([this.retireBridge(runnerId), this.preparing.get(runnerId)]);
    this.logger?.info?.({ runnerId }, 'session event bridge stopped (runner tunnel closed)');
  }

  /** Stop every live bridge (server shutdown). Idempotent. */
  async stopAll(): Promise<void> {
    this.connects.clear();
    const runners = new Set([...this.bridges.keys(), ...this.retiring.keys()]);
    await Promise.all([
      ...[...runners].map((runnerId) => this.retireBridge(runnerId)),
      ...this.preparing.values(),
    ]);
    this.lastRecovery.clear();
  }

  private async retireBridge(runnerId: string): Promise<void> {
    const bridge = this.bridges.get(runnerId);
    let pending = this.retiring.get(runnerId);
    if (bridge) {
      this.bridges.delete(runnerId);
      this.lastRecovery.delete(runnerId);
      pending = Promise.all([pending, bridge.stop()]).then(() => {});
      this.retiring.set(runnerId, pending);
    }
    if (!pending) return;
    try {
      await pending;
    } finally {
      if (this.retiring.get(runnerId) === pending) this.retiring.delete(runnerId);
    }
  }

  /** Number of live bridges. Intended for tests + diagnostics. */
  get size(): number {
    return this.bridges.size;
  }

  /**
   * The serving-side observation from the most recent recovery for `runnerId`, or
   * `null` when none ran (recovery disabled, runner unbound, recovery threw, or
   * the bridge was torn down). Diagnostics / explicit-resume seam ONLY — reading
   * `pendingUserTurnId` here never drives a turn; the bridge is the sole driver.
   * Intended for tests + ops to see what the owner pod replayed on (re)connect.
   */
  recoveryObservation(runnerId: string): RecoveryObservation | null {
    return this.lastRecovery.get(runnerId) ?? null;
  }
}

/**
 * The policy portion of a snapshot. Deliberately excludes model/egress JWTs:
 * those are refreshed on reconnect by the existing delivery lifecycle, while
 * this comparison answers only whether running the next turn under the current
 * guardrail fold would be stale.
 */
function guardrailSnapshotFingerprint(snapshot: AgentSnapshot): string {
  return JSON.stringify({
    guardrails: snapshot.guardrails ?? [],
  });
}

/**
 * Whether a transcript event is a user TURN the owner pod drives on the runner.
 *
 * It must be a public `user.*` event produced by a client. This excludes the
 * agent events the bridge itself persists (`producedBy=harness`) and internal
 * `harness.*` events — neither is a turn — so the bridge never echoes its own
 * output back to the runner as a new turn. It ALSO excludes
 * {@link TOOL_CONFIRMATION_EVENT_KIND}: a `user.tool_confirmation` is a `user.*`
 * client event but is NOT a turn — it is the verdict for an in-flight gated tool
 * call within the CURRENT turn, routed to {@link RUNNER_CONFIRMATION_PATH} (see
 * {@link isToolConfirmationEvent}). It ALSO excludes {@link INTERRUPT_EVENT_KIND}: a
 * `user.interrupt` is a `user.*` client event but is NOT a turn — it is an
 * out-of-band control signal that PREEMPTS the in-flight turn, routed to
 * {@link RUNNER_INTERRUPT_PATH} (see {@link isInterruptEvent}). Driving either as a
 * turn would POST a non-turn body to the turn route (which the runner would reject)
 * and, worse, make catch-up treat it as an un-answered pending turn.
 */
function isUserTurnEvent(event: Event): boolean {
  if (!isClientUserEvent(event)) {
    return false;
  }
  return (
    !isToolConfirmationEvent(event) && !isCustomToolResultEvent(event) && !isInterruptEvent(event)
  );
}

/** A client callback result has its own route and never enters the turn queue. */
function isCustomToolResultEvent(event: Event): boolean {
  return isClientUserEvent(event) && event.kind === CUSTOM_TOOL_RESULT_EVENT_KIND;
}

/**
 * Whether a transcript event is a tool-confirmation VERDICT the owner pod pushes to
 * the runner's confirmation route (NOT the turn route).
 *
 * A public, client-produced `user.tool_confirmation` ({@link
 * TOOL_CONFIRMATION_EVENT_KIND}). The bridge forwards it to the runner's parked
 * approval so a gated tool call proceeds (allow) or is cleanly denied (deny). Kept
 * separate from {@link isUserTurnEvent} so the two routings are mutually exclusive:
 * an event is either a turn or a confirmation, never both.
 */
function isToolConfirmationEvent(event: Event): boolean {
  return isClientUserEvent(event) && event.kind === TOOL_CONFIRMATION_EVENT_KIND;
}

/**
 * Whether a transcript event is an interrupt SIGNAL the owner pod pushes to the
 * runner's interrupt route (NOT the turn route).
 *
 * A public, client-produced `user.interrupt` ({@link INTERRUPT_EVENT_KIND}). The
 * bridge forwards it to the runner so the in-flight turn is aborted (the harness stays
 * alive — the next turn reuses it). Kept separate from {@link isUserTurnEvent} and
 * {@link isToolConfirmationEvent} so the three routings are mutually exclusive: an
 * event is a turn, a confirmation, or an interrupt — never more than one.
 */
function isInterruptEvent(event: Event): boolean {
  return isClientUserEvent(event) && event.kind === INTERRUPT_EVENT_KIND;
}

/**
 * Whether a transcript event is a public `user.*` event produced by a client — the
 * shared precondition for a turn ({@link isUserTurnEvent}), a confirmation verdict
 * ({@link isToolConfirmationEvent}), and an interrupt signal
 * ({@link isInterruptEvent}). Excludes harness-produced agent events and internal
 * `harness.*` events.
 */
function isClientUserEvent(event: Event): boolean {
  if (event.subpath || event.producedBy !== 'client') {
    return false;
  }
  if (transcriptEventVisibility(event.kind) !== 'public') {
    return false;
  }
  return event.kind.startsWith(USER_EVENT_PREFIX);
}

/**
 * Whether a transcript event is an AGENT event the bridge itself persisted.
 *
 * Agent output is stamped `producedBy=harness` ({@link AGENT_EVENT_PRODUCED_BY}).
 * The catch-up scan uses this to find the "answered up to here" marker: a user
 * turn followed by at least one agent event was already driven, so it is not
 * re-driven. Parent-agent events only here — the catch-up read is scoped to the
 * parent subpath, matching the turns the bridge drives.
 */
function isAgentEvent(event: Event): boolean {
  // Preparation receipts can follow a queued user message before the model
  // starts. They must not make catch-up treat that message as answered.
  return (
    event.producedBy === AGENT_EVENT_PRODUCED_BY &&
    event.kind !== 'session.resource_mounted' &&
    event.kind !== 'session.output_indexed'
  );
}

/**
 * Fully drain a response body and close its request slot.
 *
 * Used on the non-2xx branch to release the in-flight request without persisting
 * any of the body. Draining errors (a mid-drain tunnel drop) are swallowed — the
 * caller already decided this turn produces no agent events.
 */
async function drainAndClose(response: TunnelResponse): Promise<void> {
  try {
    for await (const _chunk of response.stream) {
      void _chunk;
    }
  } catch {
    // best-effort drain; the stream's own finally closes the request slot.
  }
}

/**
 * Close an in-flight turn's stream from the caller side (a `request.cancel` to
 * the runner), tolerating a stream that does not expose `close`.
 *
 * The {@link TunneledByteStream} returned by the transport exposes `close()`; the
 * type seam is `AsyncIterable<Uint8Array>`, so guard the cast before calling it.
 */
async function closeTurnStream(response: TunnelResponse): Promise<void> {
  const stream = response.stream as { close?: () => Promise<void> };
  if (typeof stream.close === 'function') {
    await stream.close();
  }
}

// Re-export the body decoder so a consumer that wants to interpret a raw turn
// body the same way the bridge does has the exact helper to hand. Kept here (not
// re-implemented) so the encoding contract stays single-sourced in the codec.
export { decodeBody };
