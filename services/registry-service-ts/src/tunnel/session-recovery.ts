// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Owner-pod reverse-lookup session recovery — the registry/owner-pod side of an
// exactly-once resume when a runner (re)connects.
//
// This is the SERVING half of the catch-up the event bridge already DRIVES. When
// a runner dials, the owner pod (the single writer of that session's agent events,
// and a reader of the cross-pod transcript ANY pod persisted) must put the runner
// back exactly where it left off, without re-running a turn that already finished.
// The decision, performed owner-side:
//
//   1. CURSOR. The runner presents its cursor on connect — the id of the last
//      transcript event it consumed. Recovery serves the events `after={cursor}`
//      IN ORDER from the transcript and PUSHES them down the runner tunnel so the
//      runner rebuilds from there. The runner dedups by event id.
//   2. FRESH REPLAY. A fresh runner presents an EMPTY cursor and gets a FULL
//      replay from the start of the transcript, to rebuild its whole state.
//   3. START-TURN-IF-PENDING. Recovery surfaces a turn to start ONLY IF a
//      `user.message` has no matching `agent.turn_completed` —
//      it never identifies a finished turn as pending. The observation is what the
//      caller drives (via the owner-pod event bridge), keeping recovery (the read/
//      serve side) separate from turn execution (the bridge's write side).
//   4. EXACTLY-ONCE. Recovery NEVER appends to the transcript and NEVER drives a
//      turn. The replay push is idempotent on the runner (dedup by id), and turn
//      execution is the {@link SessionEventBridge}'s job (the single writer), so a
//      flap / re-run replays state without duplicating it. Crucially, recovery and
//      the bridge run on the SAME (re)connect (see SessionEventBridgeManager): the
//      OWNER POD serves the replay (this module), THEN starts the bridge, whose
//      catch-up is the SOLE turn driver. Because recovery never starts a turn,
//      there is exactly one driver with exactly one definition of "answered" (the
//      bridge's: a user turn with ANY agent event after it is already answered and
//      is NOT re-driven, so a turn that streamed partial output before a drop keeps
//      its persisted prefix rather than being re-run). Recovery's pending-turn
//      field (rule 3, keyed on the `agent.turn_completed` marker) is therefore a
//      SERVING-SIDE OBSERVATION handed back for diagnostics / an explicit-resume
//      seam — never fed into a second driver — so the two rules cannot conflict at
//      the system level. See docs/managed-agents/services/registry-service.md
//      ("Owner-pod reverse-lookup recovery").
//
// Runner-side contract (the cross-component partner, implemented by
// `services/session-runner` — symmetric with the bridge's POST /v1/runner/turn
// partner). On a replay push the runner serves {@link
// RUNNER_REPLAY_PATH} locally: it applies each NDJSON event line, DEDUPING BY the
// stable transcript `id` (re-applying an id it already holds is a no-op), and acks
// with a 2xx once the body is drained. That dedup is the other half of rule 4's
// exactly-once: it makes a re-push (a flapping reconnect, or an overlapping
// chunk) idempotent on the runner. A non-2xx ack or a mid-push drop is reported
// by recovery as a non-delivered replay; the decision still stands so the owner
// retries the whole recovery when the runner returns.
//
// Recovery applies UNIFORMLY to all harness modes, native CLIs included. A
// catch-up over a transcript MIRRORED from a native CLI would have to skip those
// sessions, because a trailing user item there may be a real failed native turn
// rather than an unanswered task — replaying it would synthesize a spurious turn.
// That false positive does not structurally arise here: the transcript is AUTHORED
// by the owner-pod bridge with an explicit `agent.turn_completed` marker (it is not
// a mirror), so a pending user.message past the last completion is genuinely
// unanswered. And because recovery only SERVES (the bridge drives), even a
// surfaced pending turn is not auto-executed here.
//
// Why this is correct in a multi-replica registry: only the owner pod holds the
// environment's host tunnel + durable claim, so only the owner pod runs recovery
// for that runner's session — but the TRANSCRIPT it reads is the cross-pod bus, so
// the slice it serves is whatever any pod durably persisted, independent of which
// generation produced it. The after-cursor slice is computed by walking the
// transcript once from the beginning (a bounded-to-head {@link TranscriptStore.read}) and
// keeping the events strictly after the cursor event; a non-empty cursor whose id
// is not present yields an EMPTY slice (the runner is at/ahead of what this pod can
// see) rather than a from-beginning replay — only an explicitly EMPTY cursor means
// "fresh, full replay". That keeps a stale/foreign cursor from re-pushing the whole
// history.
//
// The pending decision spans the WHOLE transcript, not just the after-cursor slice:
// a user.message the runner already consumed (cursor at or past it) but that has no
// matching `agent.turn_completed` is STILL pending in the diagnostic — so
// an un-answered turn is never dropped just because the runner had already seen its
// user message. Conversely, if the owner pod's transcript shows the turn has SINCE
// completed (another generation answered it), nothing is pending and recovery does
// not re-run it.
//
// Reused, not re-implemented: `@orca/transcript-store` `read` for the cursor/after
// slice, the shared {@link TunnelRegistry} + {@link TunnelTransport} for the runner
// tunnel (the same transport the event bridge drives turns over), `protoToHttpEvent`
// to serialize each replayed event with its STABLE transcript id (what the runner
// dedups by), and the visibility helper to scope public parent-agent events. The
// runner-facing route + header constants are single-sourced from the event bridge
// where they already exist. Recovery owns no sockets and no DB, so it is fully
// unit-testable in-process against a fake runner (a ws peer speaking the tunnel
// protocol) and an in-memory store.

import { eventSourceTurnId } from './event-source-turn.js';
import { ConnectError, type TunnelResponse, TunnelTransport } from '@orca/harness-tunnel';
import type { Event, ReadOptions, TranscriptStore } from '@orca/transcript-store';
import { isPublicTranscriptEvent, protoToHttpEvent } from '../domain/events.js';
import { sessionRecoveryReplayedTotal } from '../metrics.js';
import {
  AGENT_EVENT_PRODUCED_BY,
  RUNNER_SESSION_HEADER,
  USER_EVENT_PREFIX,
} from './session-event-bridge.js';
import type { TransportRegistry } from '@orca/harness-tunnel';

// Re-export the session header from its single source (the event bridge) so a
// recovery consumer + the runner side match the exact same on-the-wire name.
export { RUNNER_SESSION_HEADER };

/**
 * Runner route the owner pod PUSHES a resume replay to. The runner serves this
 * locally (its tunnel adapter dispatches the framed request), applies each
 * replayed event (deduping by id), and acks with a 2xx. Orca-native path; the
 * cross-component contract with the runner's replay handler — distinct from the
 * bridge's turn route so a runner can route a replay and a turn independently.
 */
export const RUNNER_REPLAY_PATH = '/v1/runner/replay';

/**
 * Header carrying the runner's presented cursor on a replay push, so the runner
 * (and any observer) sees exactly which `after={cursor}` slice the owner served.
 * Empty string denotes a fresh full replay. Lower-cased on the wire by the frame
 * codec; the canonical-case constant is exported for the runner side to match.
 */
export const RUNNER_RESUME_CURSOR_HEADER = 'X-Orca-Resume-Cursor';

/**
 * Completion closes only the matching source turn. Legacy markers without an
 * identity retain their sequence boundary. Partial output does not close a turn
 * for this diagnostic; the bridge separately skips re-running its persisted prefix.
 */
export const COMPLETED_TURN_EVENT_KIND = 'agent.turn_completed';

/** Structured logger seam (a subset of the usual `req.log`). All optional. */
export interface SessionRecoveryLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Default upper bound for an in-memory replay page; never a total history cap. */
export const DEFAULT_MAX_REPLAY_EVENTS = 10_000;

/**
 * Default number of events per replay push frame. The owner serves the
 * `after={cursor}` slice in bounded chunks of this many events (each a separate
 * {@link RUNNER_REPLAY_PATH} POST that the runner dedups + acks), rather than one
 * frame carrying the whole slice — bounded pagination. Tunable via
 * {@link SessionRecoveryOptions.replayChunkSize}.
 */
export const DEFAULT_REPLAY_CHUNK_SIZE = 500;

/** Options for {@link SessionRecovery}. */
export interface SessionRecoveryOptions {
  /** Owning workspace (the tenant scope) for the transcript + tunnel owner. */
  workspaceId: string;
  /** Session id this recovery serves, e.g. `"ses_a1b2..."`. */
  sessionId: string;
  /**
   * Runner id the session is bound to — the tunnel the owner pod pushes the
   * resume replay over. The transport resolves the live tunnel through the
   * registry, so a runner reconnect (newest-wins) is picked up transparently.
   */
  runnerId: string;
  /** Transcript store: read the cursor/after slice (read-only; never appended). */
  store: TranscriptStore;
  /** Shared runner-tunnel registry the {@link TunnelTransport} routes through. */
  registry: TransportRegistry;
  /** Optional structured logger. */
  logger?: SessionRecoveryLogger;
  /**
   * Maximum public events buffered in one replay page (also bounded by replayChunkSize).
   * The complete bounded-to-head transcript is drained. Defaults to
   * {@link DEFAULT_MAX_REPLAY_EVENTS}; values <= 0 fall back to the default.
   */
  maxReplayEvents?: number;
  /**
   * Events per replay push frame (bounded pagination of the after-cursor slice).
   * Defaults to {@link DEFAULT_REPLAY_CHUNK_SIZE}; values <= 0 fall back to the
   * default.
   */
  replayChunkSize?: number;
}

/**
 * The outcome of a single {@link SessionRecovery.recover} call.
 *
 * Carries what the owner served and the resume decision so the caller (the
 * owner-pod bridge wiring) can drive the pending turn — recovery itself reads +
 * serves but does not execute turns.
 */
export interface RecoveryOutcome {
  /** `true` when the runner presented an empty cursor (a fresh, full replay). */
  fresh: boolean;
  /** Number of transcript events the owner pushed to the runner this recovery. */
  replayedCount: number;
  /**
   * Whether the replay push was accepted by the runner (a 2xx ack, fully drained).
   * `false` when the runner was offline, refused with a non-2xx, or dropped the
   * tunnel mid-push — the {@link pendingUserTurn} decision still stands so the
   * caller can act once the runner is back.
   */
  replayDelivered: boolean;
  /**
   * The user turn without a matching `agent.turn_completed`, or
   * `null` when none is. When set, it is the LATEST such pending message (a resume
   * picks up the last item). This is a SERVING-SIDE OBSERVATION the owner
   * pod hands back for diagnostics + an explicit-resume seam — recovery does NOT
   * itself execute it. In the owner-pod composition the turn DRIVER is the
   * {@link SessionEventBridge} catch-up, which runs after this replay and is the
   * single writer; recovery never drives a turn, so this field cannot cause a
   * second turn to run (that would risk duplicating a persisted prefix — see the
   * exactly-once note on the module). The transcript event is handed back whole.
   */
  pendingUserTurn: Event | null;
  /**
   * Compatibility field: complete bounded-to-head streaming returns false.
   * Read failures throw; delivery failures set replayDelivered=false.
   */
  replayTruncated: boolean;
}

/**
 * The owner-pod reverse-lookup recovery for one self-hosted session.
 *
 * Construct one per (owner pod, session) and call {@link recover} with the cursor
 * the runner presented on (re)connect. It serves the `after={cursor}` slice down
 * the tunnel (full replay for a fresh runner) and returns the pending-turn
 * decision. Stateless apart from the last cursor it served (diagnostics); safe to
 * call repeatedly across reconnects (each call recomputes from the transcript).
 */
export class SessionRecovery {
  private readonly workspaceId: string;
  private readonly sessionId: string;
  private readonly runnerId: string;
  private readonly store: TranscriptStore;
  private readonly transport: TunnelTransport;
  private readonly logger: SessionRecoveryLogger | undefined;
  private readonly maxReplayEvents: number;
  private readonly replayChunkSize: number;

  /** The cursor presented on the most recent {@link recover} (diagnostics). */
  private lastCursor = '';

  constructor(opts: SessionRecoveryOptions) {
    this.workspaceId = opts.workspaceId;
    this.sessionId = opts.sessionId;
    this.runnerId = opts.runnerId;
    this.store = opts.store;
    this.transport = new TunnelTransport(opts.registry, opts.runnerId);
    this.logger = opts.logger;
    this.maxReplayEvents =
      opts.maxReplayEvents !== undefined && opts.maxReplayEvents > 0
        ? opts.maxReplayEvents
        : DEFAULT_MAX_REPLAY_EVENTS;
    this.replayChunkSize =
      opts.replayChunkSize !== undefined && opts.replayChunkSize > 0
        ? opts.replayChunkSize
        : DEFAULT_REPLAY_CHUNK_SIZE;
  }

  /** The cursor presented on the most recent {@link recover} (diagnostics). */
  get lastReplayCursor(): string {
    return this.lastCursor;
  }

  /**
   * Resume the session for a runner that presented `cursor` on (re)connect.
   *
   * Reads the transcript once from the beginning, computes the `after={cursor}`
   * slice (the FULL transcript for an empty cursor — a fresh runner), pushes that
   * slice down the runner tunnel (each event serialized with its stable id so the
   * runner dedups), and returns the pending-turn decision (the latest
   * `user.message` past the last completed `agent.turn_completed`, or `null`).
   *
   * Read-only on the transcript: recovery never appends. A transcript READ failure
   * throws (the caller decides whether to retry the whole recovery) — recovery
   * could not even compute the slice, so it must not claim success. A DELIVERY
   * failure (runner offline / non-2xx / mid-push drop) is contained: it is logged,
   * the outcome's {@link RecoveryOutcome.replayDelivered} is `false`, and the
   * pending decision is still returned so the caller can drive it once the runner
   * is back. This call therefore never throws for a delivery problem.
   *
   * @param cursor The runner's last-consumed event id, or `""` for a fresh runner.
   */
  async recover(cursor: string): Promise<RecoveryOutcome> {
    this.lastCursor = cursor;
    const fresh = cursor === '';

    // A single bounded-to-head iterator preserves one backend high-watermark.
    // Stream it in acknowledged pages instead of materializing or truncating the
    // whole transcript. Private events advance the scan but never enter replay.
    let pastCursor = fresh;
    let replayedCount = 0;
    let replayDelivered = true;
    let frameCursor = cursor;
    let pendingUserTurn: Event | null = null;
    const pendingTurns = new Map<string, Event>();
    const pageSize = Math.min(this.maxReplayEvents, this.replayChunkSize);
    let page: Event[] = [];
    const flush = async (): Promise<void> => {
      if (replayDelivered) replayDelivered = await this.pushReplayChunk(frameCursor, page);
      frameCursor = page.at(-1)?.id ?? frameCursor;
      page = [];
    };
    const opts: ReadOptions = { fromCursor: '', maxEvents: 0, subpath: '' };
    for await (const event of this.store.read(this.workspaceId, this.sessionId, opts)) {
      if (!isPublicTranscriptEvent(event)) continue;
      if (isUserTurnEvent(event)) pendingTurns.set(event.id, event);
      if (isCompletedTurnEvent(event)) {
        const sourceTurnId = eventSourceTurnId(event);
        if (sourceTurnId) pendingTurns.delete(sourceTurnId);
        else pendingTurns.clear(); // Historical uncorrelated completion boundary.
      }
      if (!pastCursor) {
        if (event.id === cursor) pastCursor = true;
        continue;
      }
      replayedCount += 1;
      if (replayDelivered) {
        page.push(event);
        if (page.length >= pageSize) await flush();
      }
    }
    pendingUserTurn = [...pendingTurns.values()].at(-1) ?? null;
    if (page.length > 0 || replayedCount === 0) await flush();

    sessionRecoveryReplayedTotal.inc({ mode: fresh ? 'fresh' : 'resume' }, replayedCount);
    this.logger?.info?.(
      {
        sessionId: this.sessionId,
        runnerId: this.runnerId,
        cursor,
        fresh,
        replayedCount,
        replayDelivered,
        replayTruncated: false,
        pending: pendingUserTurn?.id ?? null,
      },
      'session recovery served resume replay',
    );

    return {
      fresh,
      replayedCount,
      replayDelivered,
      pendingUserTurn,
      replayTruncated: false,
    };
  }

  /**
   * Push one replay chunk as a single streaming POST and await its ack.
   *
   * Every delivery failure is contained + logged:
   *   - the runner is offline ({@link ConnectError}) → log + `false`;
   *   - the runner answers non-2xx → drain/close + log + `false`;
   *   - the tunnel drops mid-push → the body iterator throws → log + `false`.
   * Recovery is read-only, so a failed push duplicates nothing.
   */
  private async pushReplayChunk(cursor: string, chunk: Event[]): Promise<boolean> {
    const body = encodeReplayBody(chunk);
    let response: TunnelResponse;
    try {
      response = await this.transport.handleRequest({
        method: 'POST',
        path: RUNNER_REPLAY_PATH,
        headers: [
          [RUNNER_SESSION_HEADER, this.sessionId],
          [RUNNER_RESUME_CURSOR_HEADER, cursor],
        ],
        body,
        contentType: 'application/x-ndjson',
      });
    } catch (err) {
      const offline = err instanceof ConnectError;
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, cursor, offline },
        offline
          ? 'session recovery could not reach runner to push resume replay (offline)'
          : 'session recovery failed to push resume replay to runner',
      );
      return false;
    }

    if (response.status < 200 || response.status >= 300) {
      await drainAndClose(response);
      this.logger?.warn?.(
        { sessionId: this.sessionId, runnerId: this.runnerId, cursor, status: response.status },
        'session recovery got non-2xx ack for resume replay from runner',
      );
      return false;
    }

    // Drain the ack body so the request slot is released; a mid-drain tunnel drop
    // means the runner did not fully receive/ack the replay.
    try {
      for await (const _chunk of response.stream) {
        void _chunk;
      }
    } catch (err) {
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, cursor },
        'session recovery resume replay push ended early (runner tunnel drop)',
      );
      return false;
    }
    return true;
  }
}

/** Serialize the replay slice as NDJSON bytes (one event JSON per line). */
function encodeReplayBody(replay: Event[]): Uint8Array {
  if (replay.length === 0) {
    return new Uint8Array(0);
  }
  const text = replay.map((e) => JSON.stringify(protoToHttpEvent(e))).join('\n');
  // Terminating newline so the runner's NDJSON reader flushes the last line as a
  // complete record, not a trailing partial.
  return Buffer.from(`${text}\n`, 'utf8');
}

/**
 * Whether a transcript event is a public `user.*` event produced by a client.
 *
 * Excludes the agent events the bridge persists (`producedBy=harness`) and internal
 * `harness.*` events. Deliberately a BROAD `user.*` predicate: unlike the event
 * bridge's `isUserTurnEvent` (which additionally excludes `user.tool_confirmation`
 * and `user.interrupt` because it ROUTES them to dedicated runner routes), this helper
 * only feeds {@link SessionRecovery.recover}, whose result is a SERVING-SIDE
 * OBSERVATION ({@link RecoveryOutcome.pendingUserTurn}) that recovery never drives —
 * the bridge is the sole turn driver, with its own answered-up-to rule. So recovery
 * has no need to mirror the bridge's turn/confirmation/interrupt split, and keeping
 * this predicate local + broad avoids behavioural coupling to the bridge's private
 * helpers. Kept local for that reason.
 */
function isUserTurnEvent(event: Event): boolean {
  if (event.producedBy !== 'client') {
    return false;
  }
  if (!isPublicTranscriptEvent(event)) {
    return false;
  }
  return event.kind.startsWith(USER_EVENT_PREFIX);
}

/**
 * Whether a transcript event is the COMPLETED-turn marker
 * ({@link COMPLETED_TURN_EVENT_KIND}).
 *
 * Agent-produced ({@link AGENT_EVENT_PRODUCED_BY}) so a client cannot forge the
 * boundary, and of the completed-turn kind. This is the "answered up to here"
 * marker the pending-turn rule keys on — strictly the completed marker, not any
 * agent event, so a partial (then dropped) turn stays pending.
 */
function isCompletedTurnEvent(event: Event): boolean {
  return event.producedBy === AGENT_EVENT_PRODUCED_BY && event.kind === COMPLETED_TURN_EVENT_KIND;
}

/**
 * Fully drain a response body and close its request slot.
 *
 * Used on the non-2xx branch to release the in-flight request without interpreting
 * the body. Draining errors (a mid-drain tunnel drop) are swallowed — the caller
 * already decided this push was not delivered.
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
