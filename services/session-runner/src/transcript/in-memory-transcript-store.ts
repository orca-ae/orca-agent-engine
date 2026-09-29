// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// An in-memory, tunnel-fed {@link TranscriptStore} — the self-hosted runner's
// conversation-history substrate, with NO Kafka / Postgres / network behind it.
//
// A self-hosted runner is OUTBOUND-WSS-ONLY behind a NAT: it has no direct
// transcript backend. The registry is the single writer of the durable transcript
// (the runner streams new agent events UP the tunnel and the owner-pod bridge
// persists them), and the registry PUSHES the recovery replay DOWN the tunnel on
// (re)connect. So the runner needs a local, process-lifetime history log — not a
// broker client — for the one thing it reads back in-process: the Claude Agent
// SDK's per-turn conversation history (`SessionStore.load()`), which the claude
// provider's adapter persists + reloads through a `TranscriptStore`.
//
// This store is that log. It is populated by two sources and serves `read()` from
// the union:
//   1. {@link append} — the agent's OWN emitted turns. The SDK adapter wraps each
//      conversation entry as a `harness.claude.session_entry` event and appends it;
//      the next turn's `load()` reads them back, so cross-turn continuity holds
//      within the live runner without any broker.
//   2. {@link ingest} — the registry-pushed RECOVERY REPLAY. The session loop's
//      replay-apply feeds each replayed transcript event in, so the runner's local
//      log mirrors what the registry served (the tunnel feed, the "registry pushes
//      the replay down" half of the model).
//
// Scope (deliberate, matching the substrate it replaces): it is the SDK history
// substrate, not a durable store. It holds the session's events for the runner's
// lifetime; the DURABLE transcript is the registry's (the runner emits up the
// tunnel, the bridge persists, recovery re-serves). It is therefore unbounded only
// within a single short-lived runner process — the registry's recovery, not this
// log, is the cross-restart source of truth.
//
// The contract is the same {@link TranscriptStore} the Kafka/Postgres/Pulsar
// backends implement (so the adapter is backend-agnostic): `append` dedups by id
// and round-trips existing ids; `read` honors the cursor / subpath / bound; `tail`
// follows from-now; `archive` emits a sentinel; `close` is a no-op (nothing to
// release). Single-threaded by construction (Node's event loop), so the in-memory
// structures need no locking.

import type {
  Event,
  ReadOptions,
  TailOptions,
  TranscriptStore,
} from '@orca/transcript-store-types';

/** The sentinel kind {@link InMemoryTranscriptStore.archive} appends. */
export const SESSION_ARCHIVED_EVENT_KIND = 'session.archived';

/** A from-now tail subscriber: the predicate it follows + its delivery callback. */
interface TailSubscriber {
  /** Whether a newly-appended event matches this tail's subpath filter. */
  matches: (event: Event) => boolean;
  /** Deliver a matching event (resolves the tail generator's next pull). */
  deliver: (event: Event) => void;
}

/** One session's ordered log plus its live tail subscribers. */
interface SessionLog {
  /** The appended events in store (append/ingest) order, each with a store seq. */
  readonly events: Event[];
  /** Stable ids already stored — the idempotency index (dedup on append/ingest). */
  readonly ids: Set<string>;
  /** Live from-now tail subscribers awaiting the next matching append. */
  readonly tails: Set<TailSubscriber>;
}

/**
 * A {@link TranscriptStore} whose events live in process memory, keyed by
 * `(workspaceId, sessionId)`. The runner constructs one and shares it with the
 * claude providers (the SDK `SessionStore` adapter reads + writes it) and the
 * session loop (which {@link ingest}s the registry-pushed replay).
 */
export class InMemoryTranscriptStore implements TranscriptStore {
  private readonly logs = new Map<string, SessionLog>();

  /**
   * Append a batch to the `(workspaceId, sessionId)` log, in order.
   *
   * Dedups by stable event id: an id already in the log is NOT re-stored (its
   * existing id round-trips), matching the store contract every backend honors so
   * the SDK adapter's re-append of an entry it already wrote is idempotent. Returns
   * one id per input position (the input id for a fresh event, the same id for a
   * deduped one). A store-assigned monotonic `seq` is stamped on each newly-stored
   * event (the inputs carry `seq: 0`) so {@link read}'s cursor is a stable offset.
   */
  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    const log = this.logFor(workspaceId, sessionId);
    return events.map((event) => this.store(log, event));
  }

  /**
   * Feed a registry-pushed (recovery-replayed) event into the session's log — the
   * tunnel-fed half of the population. Deduped by id like {@link append}, so a
   * re-pushed replay event (a flapping reconnect re-serving an overlapping slice) is
   * a no-op. Synchronous: the replay-apply path is synchronous bookkeeping.
   */
  ingest(event: Event): void {
    const log = this.logFor(event.workspaceId, event.sessionId);
    this.store(log, event);
  }

  /**
   * Bounded read of the `(workspaceId, sessionId)` log.
   *
   * Yields the stored events whose store `seq` is strictly greater than the cursor
   * (`fromCursor: ''` = from the beginning), filtered by `subpath` (`''` = parent
   * only, `'*'` = all subpaths, an exact string = that one subpath), in append
   * order, capped at `maxEvents` (`0` = unbounded). Ends on its own when the log is
   * drained — the bounded-read semantics the SDK adapter's `load()` relies on.
   */
  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    const log = this.logs.get(keyOf(workspaceId, sessionId));
    if (log === undefined) {
      return;
    }
    const after = parseCursor(opts.fromCursor);
    const matches = subpathMatcher(opts.subpath);
    let yielded = 0;
    // Snapshot the length so a concurrent append during iteration does not extend
    // this bounded read past the watermark at call time (the contract: "ends when
    // the high-watermark at call time is drained").
    const watermark = log.events.length;
    for (let i = 0; i < watermark; i += 1) {
      const event = log.events[i]!;
      if (event.seq <= after || !matches(event)) {
        continue;
      }
      yield event;
      yielded += 1;
      if (opts.maxEvents > 0 && yielded >= opts.maxEvents) {
        return;
      }
    }
  }

  /**
   * Unbounded from-now follow of the `(workspaceId, sessionId)` log.
   *
   * Subscribes at the current head (`fromCursor: ''` — the from-now contract: it
   * does NOT replay history) and delivers each subsequently-appended event matching
   * the `subpath` filter, in append order, until `opts.signal` aborts. Implemented
   * as a real follow (a queue fed by {@link append} / {@link ingest}), not a poll.
   * The SDK adapter does not use `tail` (it reads via `load`), but the
   * `TranscriptStore` contract includes it, so it is a faithful follow, not a stub.
   */
  async *tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event> {
    const log = this.logFor(workspaceId, sessionId);
    const matches = subpathMatcher(opts.subpath);
    const queue: Event[] = [];
    let wake: (() => void) | undefined;
    const subscriber: TailSubscriber = {
      matches: (event) => matches(event),
      deliver: (event) => {
        queue.push(event);
        wake?.();
      },
    };
    log.tails.add(subscriber);
    const signal = opts.signal;
    const onAbort = (): void => wake?.();
    signal?.addEventListener('abort', onAbort, { once: true });
    try {
      for (;;) {
        while (queue.length > 0) {
          if (signal?.aborted) {
            return;
          }
          yield queue.shift()!;
        }
        if (signal?.aborted) {
          return;
        }
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
        wake = undefined;
      }
    } finally {
      log.tails.delete(subscriber);
      signal?.removeEventListener('abort', onAbort);
    }
  }

  /**
   * Append a `session.archived` sentinel to the parent log. Topic deletion is a
   * platform concern with a broker; here it is just the sentinel event, so a reader
   * observes the archive marker exactly as it would on a durable backend.
   */
  async archive(workspaceId: string, sessionId: string): Promise<void> {
    const log = this.logFor(workspaceId, sessionId);
    this.store(log, {
      id: `archive-${workspaceId}-${sessionId}-${log.events.length}`,
      workspaceId,
      sessionId,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'transcript-store',
      kind: SESSION_ARCHIVED_EVENT_KIND,
      payload: new Uint8Array(0),
      idempotencyKey: '',
    });
  }

  /** No-op: the in-memory store holds no connections to release. */
  async close(): Promise<void> {
    // Nothing to release — the store is pure process memory.
  }

  /** The (creating-if-absent) session log for a `(workspaceId, sessionId)`. */
  private logFor(workspaceId: string, sessionId: string): SessionLog {
    const key = keyOf(workspaceId, sessionId);
    let log = this.logs.get(key);
    if (log === undefined) {
      log = { events: [], ids: new Set<string>(), tails: new Set<TailSubscriber>() };
      this.logs.set(key, log);
    }
    return log;
  }

  /**
   * Store one event in `log` (dedup by id), stamping the store `seq`, and notify any
   * live from-now tail whose filter matches. Returns the event id (the existing id
   * on a dedup, so the caller's id round-trips).
   */
  private store(log: SessionLog, event: Event): string {
    if (log.ids.has(event.id)) {
      return event.id; // dedup: a re-append/ingest of a known id is a no-op.
    }
    const stored: Event = { ...event, seq: log.events.length };
    log.events.push(stored);
    log.ids.add(stored.id);
    for (const tail of log.tails) {
      if (tail.matches(stored)) {
        tail.deliver(stored);
      }
    }
    return stored.id;
  }
}

/** The map key for a `(workspaceId, sessionId)` log. */
function keyOf(workspaceId: string, sessionId: string): string {
  return `${workspaceId} ${sessionId}`;
}

/**
 * Parse a read cursor into the exclusive store-seq lower bound. `''` (the
 * from-beginning cursor) yields `-1` so every event (seq >= 0) is read; otherwise
 * the numeric seq the cursor names (read serves strictly AFTER it). A malformed
 * cursor falls back to from-beginning rather than dropping events.
 */
function parseCursor(fromCursor: string): number {
  if (fromCursor === '') {
    return -1;
  }
  const seq = Number(fromCursor);
  return Number.isFinite(seq) ? seq : -1;
}

/**
 * Build the subpath predicate for a read/tail: `''` matches the parent log only
 * (subpath `''`), `'*'` matches every subpath, and any other string matches that
 * exact subpath — the cross-backend subpath contract.
 */
function subpathMatcher(subpath: string): (event: Event) => boolean {
  if (subpath === '*') {
    return () => true;
  }
  return (event) => event.subpath === subpath;
}
