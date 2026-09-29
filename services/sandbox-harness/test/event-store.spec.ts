// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit test for the in-memory event store (store.ts#createEventStore).
//
// The store is deliberately ephemeral — a plain Map, no Kafka/DB — so these
// tests assert exactly the contract the route layer and the durability bridge
// rely on:
//   - publish() stamps the envelope (id / session_id / created_at) and appends
//     to per-session history in insertion order;
//   - list() returns history in insertion order, isolated per session, and as a
//     copy (mutating the result must not corrupt the store);
//   - subscribe() only sees events published AFTER it subscribed (history replay
//     is the route's job, not the store's), in order, and unsubscribe stops the
//     feed; one throwing listener must not break the rest of the fan-out;
//   - deleteSession() drops both history and subscribers for that session only.

import { describe, it, expect, vi } from 'vitest';

import { createEventStore, type StoredEvent } from '../src/store.js';

describe('createEventStore', () => {
  it('prunes acknowledged SDK checkpoints without dropping unread events or public history', () => {
    const store = createEventStore();
    store.publish('s', { type: 'harness.sdk_event', sequence: 1, event: { type: 'checkpoint' } });
    const unread = store.publish('s', {
      type: 'harness.sdk_event',
      sequence: 2,
      event: { type: 'done' },
    });
    const publicEvent = store.publish('s', { type: 'agent.message', content: [] });
    const otherSession = store.publish('other', { type: 'harness.sdk_event', sequence: 1 });
    store.acknowledgeSdkEvents('s', 1);
    expect(store.list('s')).toEqual([unread, publicEvent]);
    expect(store.list('other')).toEqual([otherSession]);
    store.acknowledgeSdkEvents('s', 2);
    expect(store.list('s')).toEqual([publicEvent]);
  });

  describe('publish + list ordering', () => {
    it('stamps the envelope and preserves insertion order', () => {
      const store = createEventStore();

      const a = store.publish('session_1', { type: 'user.message', content: [] });
      const b = store.publish('session_1', { type: 'agent.message', content: [] });

      // Envelope stamped on each event.
      expect(a.session_id).toBe('session_1');
      expect(a.id).toMatch(/^evt_[0-9a-f]{32}$/);
      expect(typeof a.created_at).toBe('string');
      expect(a.type).toBe('user.message');

      // History is in insertion order and carries the stamped events.
      const history = store.list('session_1');
      expect(history.map((e) => e.id)).toEqual([a.id, b.id]);
      expect(history.map((e) => e.type)).toEqual(['user.message', 'agent.message']);
    });

    it('returns the stamped event from publish()', () => {
      const store = createEventStore();
      const stamped = store.publish('session_1', {
        type: 'session.status_idle',
        usage: {},
        total_cost_usd: 0,
      });
      expect(stamped).toMatchObject({ session_id: 'session_1', type: 'session.status_idle' });
      expect(stamped.id).toMatch(/^evt_/);
    });

    it('lets a caller-supplied field win over the stamped envelope', () => {
      // store.ts spreads the bare event AFTER the envelope, so a bare event that
      // carries its own id/created_at overrides the stamp (documented behavior).
      const store = createEventStore();
      const stamped = store.publish('session_1', { type: 'user.message', id: 'evt_custom' });
      expect(stamped.id).toBe('evt_custom');
    });

    it('keeps history isolated per session', () => {
      const store = createEventStore();
      store.publish('session_a', { type: 'user.message', content: [] });
      store.publish('session_b', { type: 'agent.message', content: [] });

      expect(store.list('session_a').map((e) => e.type)).toEqual(['user.message']);
      expect(store.list('session_b').map((e) => e.type)).toEqual(['agent.message']);
    });

    it('returns an empty list for an unknown session', () => {
      const store = createEventStore();
      expect(store.list('nope')).toEqual([]);
    });

    it('returns a copy from list() so mutating it cannot corrupt the store', () => {
      const store = createEventStore();
      store.publish('session_1', { type: 'user.message', content: [] });

      const first = store.list('session_1');
      first.push({
        id: 'evt_x',
        session_id: 'session_1',
        created_at: 'now',
        type: 'agent.message',
      });

      expect(store.list('session_1')).toHaveLength(1);
    });
  });

  describe('subscribe ordering', () => {
    it('delivers only events published after subscription, in order', () => {
      const store = createEventStore();

      // Published BEFORE subscribe — must not be replayed to the listener.
      store.publish('session_1', { type: 'user.message', content: [] });

      const received: StoredEvent[] = [];
      store.subscribe('session_1', (e) => received.push(e));

      const live1 = store.publish('session_1', { type: 'agent.message', content: [] });
      const live2 = store.publish('session_1', {
        type: 'session.status_idle',
        usage: {},
        total_cost_usd: 0,
      });

      expect(received.map((e) => e.id)).toEqual([live1.id, live2.id]);
      // Full history still has all three (the pre-subscribe one included).
      expect(store.list('session_1')).toHaveLength(3);
    });

    it('stops delivering after unsubscribe', () => {
      const store = createEventStore();
      const received: StoredEvent[] = [];
      const unsubscribe = store.subscribe('session_1', (e) => received.push(e));

      store.publish('session_1', { type: 'user.message', content: [] });
      unsubscribe();
      store.publish('session_1', { type: 'agent.message', content: [] });

      expect(received).toHaveLength(1);
      expect(received[0]?.type).toBe('user.message');
    });

    it('only notifies subscribers of the matching session', () => {
      const store = createEventStore();
      const a: StoredEvent[] = [];
      const b: StoredEvent[] = [];
      store.subscribe('session_a', (e) => a.push(e));
      store.subscribe('session_b', (e) => b.push(e));

      store.publish('session_a', { type: 'user.message', content: [] });

      expect(a).toHaveLength(1);
      expect(b).toHaveLength(0);
    });

    it('isolates a throwing listener from the rest of the fan-out', () => {
      const store = createEventStore();
      const good = vi.fn();
      store.subscribe('session_1', () => {
        throw new Error('bad subscriber');
      });
      store.subscribe('session_1', good);

      expect(() => store.publish('session_1', { type: 'user.message', content: [] })).not.toThrow();
      expect(good).toHaveBeenCalledTimes(1);
    });

    it('supports multiple independent subscribers on one session', () => {
      const store = createEventStore();
      const one: StoredEvent[] = [];
      const two: StoredEvent[] = [];
      store.subscribe('session_1', (e) => one.push(e));
      store.subscribe('session_1', (e) => two.push(e));

      store.publish('session_1', { type: 'user.message', content: [] });

      expect(one).toHaveLength(1);
      expect(two).toHaveLength(1);
    });
  });

  describe('deleteSession', () => {
    it('drops history and stops live delivery for that session', () => {
      const store = createEventStore();
      const received: StoredEvent[] = [];
      store.subscribe('session_1', (e) => received.push(e));
      store.publish('session_1', { type: 'user.message', content: [] });

      store.deleteSession('session_1');

      // History gone.
      expect(store.list('session_1')).toEqual([]);
      // A post-delete publish starts a brand-new (empty) history and does NOT
      // reach the old subscriber, which was dropped with the session.
      store.publish('session_1', { type: 'agent.message', content: [] });
      expect(received).toHaveLength(1);
      expect(store.list('session_1')).toHaveLength(1);
    });

    it('only deletes the named session', () => {
      const store = createEventStore();
      store.publish('session_a', { type: 'user.message', content: [] });
      store.publish('session_b', { type: 'user.message', content: [] });

      store.deleteSession('session_a');

      expect(store.list('session_a')).toEqual([]);
      expect(store.list('session_b')).toHaveLength(1);
    });

    it('is a no-op for an unknown session', () => {
      const store = createEventStore();
      expect(() => store.deleteSession('nope')).not.toThrow();
    });
  });
});
