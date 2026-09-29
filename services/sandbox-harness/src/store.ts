// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-memory session records + per-session event history with live pub/sub
// fan-out.
//
// This store is deliberately EPHEMERAL: both session records and event history
// live in plain `Map`s for the lifetime of the process. Durability is an
// external concern — a bridge tails the SSE stream and persists events
// elsewhere — so there is intentionally NO Kafka / DB / persistence here.
import { genId, nowIso } from './core.js';

/**
 * Lifecycle states a session can be in. The store itself never validates the
 * value passed to `setStatus`, but the public surface is typed to the states
 * the harness actually uses.
 */
export type SessionStatus = 'idle' | 'running' | 'error';

/** A session record as returned over the wire. */
export interface Session {
  /** `session_<32 hex>`. */
  id: string;
  object: 'session';
  /** Provider/harness identifier this session runs against. */
  agent: string;
  status: SessionStatus;
  /** RFC3339 / ISO-8601 creation timestamp. */
  created_at: string;
}

/** Arguments accepted by {@link SessionStore.create}. */
export interface CreateSessionInput {
  agent: string;
}

/**
 * A bare event as produced by a caller (e.g. the `core` event factories): a
 * `type` discriminator plus an arbitrary payload. The store stamps envelope
 * fields (`id` / `session_id` / `created_at`) at publish time; callers should
 * generally avoid those keys, but explicit bare-event values win on collision
 * for tests and replay/idempotency adapters.
 */
export interface BareEvent {
  type: string;
  [key: string]: unknown;
}

/** Envelope fields stamped onto every event by {@link EventStore.publish}. */
export interface EventEnvelope {
  /** `evt_<32 hex>`. */
  id: string;
  session_id: string;
  /** RFC3339 / ISO-8601 publish timestamp. */
  created_at: string;
}

/** A {@link BareEvent} after the store has stamped its envelope onto it. */
export type StoredEvent = EventEnvelope & BareEvent;

/** Receives each event published to a session AFTER the listener subscribed. */
export type EventListener = (event: StoredEvent) => void;

/** Removes a previously-registered {@link EventListener}. Idempotent. */
export type Unsubscribe = () => void;

/** In-memory store of session records keyed by id. */
export interface SessionStore {
  /** Create and persist a new session in the `idle` state. */
  create(input: CreateSessionInput): Session;
  /** Look up a session by id, or `undefined` if unknown. */
  get(id: string): Session | undefined;
  /** Set a session's status. No-op if the id is unknown. */
  setStatus(id: string, status: SessionStatus): void;
  /** Drop a session. Returns whether a record was removed. */
  delete(id: string): boolean;
}

/** Per-session event history plus a live subscriber fan-out. */
export interface EventStore {
  /**
   * Stamp `id` / `session_id` / `created_at` onto a bare event, append it to
   * the session's history, then broadcast it to every live subscriber. A throw
   * from one listener is swallowed so it cannot break the rest of the fan-out.
   * Returns the stamped event.
   */
  publish(sessionId: string, bareEvent: BareEvent): StoredEvent;
  /** Stored events for a session in insertion order. Returns a copy. */
  list(sessionId: string): StoredEvent[];
  /** Drop private SDK frames already received by the owning adapter. */
  acknowledgeSdkEvents(sessionId: string, sequence: number): void;
  /**
   * Register a listener for events published AFTER this call (history is not
   * replayed — that is the route's job). Returns an `unsubscribe` callback.
   */
  subscribe(sessionId: string, listener: EventListener): Unsubscribe;
  /** Drop all history and subscribers for a session. */
  deleteSession(sessionId: string): void;
}

/** Create an in-memory {@link SessionStore}. */
export function createSessionStore(): SessionStore {
  const sessions = new Map<string, Session>();

  return {
    create({ agent }) {
      const session: Session = {
        id: genId('session'),
        object: 'session',
        agent,
        status: 'idle',
        created_at: nowIso(),
      };
      sessions.set(session.id, session);
      return session;
    },

    get(id) {
      return sessions.get(id);
    },

    setStatus(id, status) {
      const session = sessions.get(id);
      if (session) session.status = status;
    },

    delete(id) {
      return sessions.delete(id);
    },
  };
}

interface SessionEvents {
  history: StoredEvent[];
  subscribers: Set<EventListener>;
}

/** Create an in-memory {@link EventStore}. */
export function createEventStore(): EventStore {
  const sessions = new Map<string, SessionEvents>();

  function getOrCreate(sessionId: string): SessionEvents {
    let entry = sessions.get(sessionId);
    if (!entry) {
      entry = { history: [], subscribers: new Set() };
      sessions.set(sessionId, entry);
    }
    return entry;
  }

  return {
    publish(sessionId, bareEvent) {
      // Stamp the envelope first; spreading the bare event last means a
      // caller-supplied field wins on collision.
      const stamped: StoredEvent = {
        id: genId('evt'),
        session_id: sessionId,
        created_at: nowIso(),
        ...bareEvent,
      };

      const entry = getOrCreate(sessionId);
      entry.history.push(stamped);

      for (const listener of entry.subscribers) {
        try {
          listener(stamped);
        } catch {
          // One bad subscriber must not break the rest of the broadcast.
        }
      }

      return stamped;
    },

    list(sessionId) {
      const entry = sessions.get(sessionId);
      return entry ? entry.history.slice() : [];
    },

    acknowledgeSdkEvents(sessionId, sequence) {
      const entry = sessions.get(sessionId);
      if (entry)
        entry.history = entry.history.filter(
          (event) =>
            event.type !== 'harness.sdk_event' ||
            typeof event.sequence !== 'number' ||
            event.sequence > sequence,
        );
    },

    subscribe(sessionId, listener) {
      const entry = getOrCreate(sessionId);
      entry.subscribers.add(listener);
      return () => {
        entry.subscribers.delete(listener);
      };
    },

    deleteSession(sessionId) {
      sessions.delete(sessionId);
    },
  };
}
