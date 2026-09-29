// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { InMemoryGuardrailStateStore, applyStateUpdate } from '../../src/state.js';
import type { StateScope, StateUpdate } from '../../src/types.js';

/**
 * A guardrail never writes state directly — it emits updates that the runtime
 * applies. These tests pin the semantics of that application, because the same
 * semantics have to hold for an in-memory store and for a durable one.
 */
describe('applying a single update', () => {
  it('sets a value at an existing key', () => {
    const target: Record<string, unknown> = { approved: false };
    applyStateUpdate(target, { scope: 'session', key: 'approved', action: 'set', value: true });
    expect(target).toEqual({ approved: true });
  });

  it('sets a value at a key that does not exist yet', () => {
    const target: Record<string, unknown> = {};
    applyStateUpdate(target, { scope: 'session', key: 'tier', action: 'set', value: 'high' });
    expect(target).toEqual({ tier: 'high' });
  });

  it('removes the key when a set carries no value, so state never holds undefined', () => {
    const target: Record<string, unknown> = { tier: 'high' };
    applyStateUpdate(target, { scope: 'session', key: 'tier', action: 'set' });
    expect('tier' in target).toBe(false);
  });

  it('increments an existing number', () => {
    const target: Record<string, unknown> = { toolCalls: 4 };
    applyStateUpdate(target, { scope: 'session', key: 'toolCalls', action: 'increment', value: 3 });
    expect(target.toolCalls).toBe(7);
  });

  it('treats a missing key as zero when incrementing', () => {
    const target: Record<string, unknown> = {};
    applyStateUpdate(target, { scope: 'session', key: 'toolCalls', action: 'increment', value: 5 });
    expect(target.toolCalls).toBe(5);
  });

  it('increments by one when the update carries no value', () => {
    const target: Record<string, unknown> = { toolCalls: 1 };
    applyStateUpdate(target, { scope: 'session', key: 'toolCalls', action: 'increment' });
    expect(target.toolCalls).toBe(2);
  });

  it('decrements when the value is negative', () => {
    const target: Record<string, unknown> = { budget: 10 };
    applyStateUpdate(target, { scope: 'session', key: 'budget', action: 'increment', value: -4 });
    expect(target.budget).toBe(6);
  });

  it('replaces a non-numeric value with the increment rather than throwing', () => {
    const target: Record<string, unknown> = { toolCalls: 'many' };
    expect(() =>
      applyStateUpdate(target, {
        scope: 'session',
        key: 'toolCalls',
        action: 'increment',
        value: 2,
      }),
    ).not.toThrow();
    expect(target.toolCalls).toBe(2);
  });

  it('normalizes a key to a number when an increment carries a non-numeric value', () => {
    const target: Record<string, unknown> = { toolCalls: 'many' };
    applyStateUpdate(target, {
      scope: 'session',
      key: 'toolCalls',
      action: 'increment',
      value: 'lots',
    });
    expect(target.toolCalls).toBe(0);
  });

  it('replaces a non-finite value with the increment', () => {
    const target: Record<string, unknown> = { toolCalls: Number.NaN };
    applyStateUpdate(target, { scope: 'session', key: 'toolCalls', action: 'increment', value: 3 });
    expect(target.toolCalls).toBe(3);
  });

  it('deletes a key', () => {
    const target: Record<string, unknown> = { approved: true, tier: 'high' };
    applyStateUpdate(target, { scope: 'session', key: 'approved', action: 'delete' });
    expect(target).toEqual({ tier: 'high' });
  });

  it('deletes a key that is not present without throwing', () => {
    const target: Record<string, unknown> = {};
    expect(() =>
      applyStateUpdate(target, { scope: 'session', key: 'absent', action: 'delete' }),
    ).not.toThrow();
    expect(target).toEqual({});
  });

  it('appends to an existing array', () => {
    const target: Record<string, unknown> = { tools: ['read'] };
    applyStateUpdate(target, { scope: 'session', key: 'tools', action: 'append', value: 'write' });
    expect(target.tools).toEqual(['read', 'write']);
  });

  it('starts a new array when appending to a missing key', () => {
    const target: Record<string, unknown> = {};
    applyStateUpdate(target, { scope: 'session', key: 'tools', action: 'append', value: 'read' });
    expect(target.tools).toEqual(['read']);
  });

  it('replaces a non-array value with a single-element array when appending', () => {
    const target: Record<string, unknown> = { tools: 'read' };
    expect(() =>
      applyStateUpdate(target, {
        scope: 'session',
        key: 'tools',
        action: 'append',
        value: 'write',
      }),
    ).not.toThrow();
    expect(target.tools).toEqual(['write']);
  });

  it('leaves the key untouched when an append carries no value', () => {
    const target: Record<string, unknown> = { tools: ['read'] };
    applyStateUpdate(target, { scope: 'session', key: 'tools', action: 'append' });
    expect(target.tools).toEqual(['read']);
  });

  it('does not create a key when an append carries no value', () => {
    const target: Record<string, unknown> = {};
    applyStateUpdate(target, { scope: 'session', key: 'tools', action: 'append' });
    expect('tools' in target).toBe(false);
  });

  it('ignores an unrecognized action rather than throwing', () => {
    const target: Record<string, unknown> = { toolCalls: 1 };
    const malformed = {
      scope: 'session',
      key: 'toolCalls',
      action: 'multiply',
      value: 3,
    } as unknown as StateUpdate;
    expect(() => applyStateUpdate(target, malformed)).not.toThrow();
    expect(target).toEqual({ toolCalls: 1 });
  });

  it('drops an unsupported function value rather than retaining a live reference', () => {
    const target: Record<string, unknown> = { existing: 'value' };
    const mutable = Object.assign(() => undefined, { marker: 'stored' });
    expect(() =>
      applyStateUpdate(target, {
        scope: 'session',
        key: 'existing',
        action: 'set',
        value: mutable,
      }),
    ).not.toThrow();
    expect('existing' in target).toBe(false);
  });
});

describe('in-memory guardrail state', () => {
  it('reads an empty snapshot for a scope nothing has written', () => {
    const store = new InMemoryGuardrailStateStore();
    for (const scope of ['turn', 'session', 'subject_window'] as StateScope[]) {
      expect(store.read(scope)).toEqual({});
    }
  });

  it('restores state seeded at construction', () => {
    const store = new InMemoryGuardrailStateStore({ session: { toolCalls: 12 } });
    expect(store.read('session')).toEqual({ toolCalls: 12 });
  });

  it('seeds each scope independently', () => {
    const store = new InMemoryGuardrailStateStore({
      session: { toolCalls: 12 },
      subject_window: { spendUsd: 4.5 },
    });
    expect(store.read('session')).toEqual({ toolCalls: 12 });
    expect(store.read('subject_window')).toEqual({ spendUsd: 4.5 });
    expect(store.read('turn')).toEqual({});
  });

  it('does not alias the seed it was constructed with', () => {
    const seed = { session: { toolCalls: 12 } };
    const store = new InMemoryGuardrailStateStore(seed);
    seed.session.toolCalls = 99;
    expect(store.read('session')).toEqual({ toolCalls: 12 });
  });

  it('applies updates in array order', () => {
    const store = new InMemoryGuardrailStateStore();
    store.apply([
      { scope: 'session', key: 'toolCalls', action: 'set', value: 10 },
      { scope: 'session', key: 'toolCalls', action: 'increment', value: 1 },
    ]);
    expect(store.read('session').toolCalls).toBe(11);
  });

  it('applies a later set over an earlier increment', () => {
    const store = new InMemoryGuardrailStateStore();
    store.apply([
      { scope: 'session', key: 'toolCalls', action: 'increment', value: 7 },
      { scope: 'session', key: 'toolCalls', action: 'set', value: 0 },
    ]);
    expect(store.read('session').toolCalls).toBe(0);
  });

  it('keeps scopes isolated even for the same key name', () => {
    const store = new InMemoryGuardrailStateStore();
    store.apply([{ scope: 'session', key: 'calls', action: 'increment', value: 3 }]);
    expect(store.read('session').calls).toBe(3);
    expect(store.read('turn')).toEqual({});
    expect(store.read('subject_window')).toEqual({});
  });

  it('writes each scope only where the update points', () => {
    const store = new InMemoryGuardrailStateStore();
    store.apply([
      { scope: 'turn', key: 'calls', action: 'increment', value: 1 },
      { scope: 'session', key: 'calls', action: 'increment', value: 2 },
      { scope: 'subject_window', key: 'calls', action: 'increment', value: 3 },
    ]);
    expect(store.read('turn').calls).toBe(1);
    expect(store.read('session').calls).toBe(2);
    expect(store.read('subject_window').calls).toBe(3);
  });

  it('clears turn state at a turn boundary', () => {
    const store = new InMemoryGuardrailStateStore();
    store.apply([{ scope: 'turn', key: 'calls', action: 'increment', value: 4 }]);
    store.resetTurn();
    expect(store.read('turn')).toEqual({});
  });

  it('leaves session and subject window intact when the turn resets', () => {
    const store = new InMemoryGuardrailStateStore();
    store.apply([
      { scope: 'turn', key: 'calls', action: 'increment', value: 4 },
      { scope: 'session', key: 'calls', action: 'increment', value: 5 },
      { scope: 'subject_window', key: 'spendUsd', action: 'set', value: 1.25 },
    ]);
    store.resetTurn();
    expect(store.read('session')).toEqual({ calls: 5 });
    expect(store.read('subject_window')).toEqual({ spendUsd: 1.25 });
  });

  it('accepts writes again after a turn reset', () => {
    const store = new InMemoryGuardrailStateStore();
    store.apply([{ scope: 'turn', key: 'calls', action: 'increment', value: 4 }]);
    store.resetTurn();
    store.apply([{ scope: 'turn', key: 'calls', action: 'increment' }]);
    expect(store.read('turn').calls).toBe(1);
  });

  it('hands out a snapshot whose mutation cannot reach the store', () => {
    const store = new InMemoryGuardrailStateStore({ session: { toolCalls: 1 } });
    const snapshot = store.read('session') as Record<string, unknown>;
    snapshot.toolCalls = 999;
    snapshot.injected = true;
    expect(store.read('session')).toEqual({ toolCalls: 1 });
  });

  it('hands out a snapshot whose nested arrays cannot be mutated into the store', () => {
    const store = new InMemoryGuardrailStateStore();
    store.apply([{ scope: 'session', key: 'tools', action: 'append', value: 'read' }]);
    const tools = store.read('session').tools as string[];
    tools.push('write');
    expect(store.read('session').tools).toEqual(['read']);
  });

  it('hands out a snapshot whose nested objects cannot be mutated into the store', () => {
    const store = new InMemoryGuardrailStateStore({ session: { limits: { calls: 5 } } });
    const limits = store.read('session').limits as Record<string, unknown>;
    limits.calls = 500;
    expect(store.read('session')).toEqual({ limits: { calls: 5 } });
  });

  it('does not expose a reference beyond the former copy-depth limit', () => {
    const root: Record<string, unknown> = {};
    let cursor = root;
    for (let depth = 0; depth < 24; depth++) {
      const next: Record<string, unknown> = {};
      cursor['next'] = next;
      cursor = next;
    }
    cursor['value'] = 'stored';

    const store = new InMemoryGuardrailStateStore({ session: { root } });
    let snapshotCursor = store.read('session').root as Record<string, unknown>;
    for (let depth = 0; depth < 24; depth++) {
      snapshotCursor = snapshotCursor['next'] as Record<string, unknown>;
    }
    snapshotCursor['value'] = 'mutated';

    let storedCursor = store.read('session').root as Record<string, unknown>;
    for (let depth = 0; depth < 24; depth++) {
      storedCursor = storedCursor['next'] as Record<string, unknown>;
    }
    expect(storedCursor['value']).toBe('stored');
  });

  it('clones cyclic and built-in containers without exposing live state', () => {
    const cyclic: Record<string, unknown> = { label: 'stored' };
    cyclic['self'] = cyclic;
    const store = new InMemoryGuardrailStateStore({
      session: { cyclic, map: new Map([['key', { value: 'stored' }]]) },
    });

    const snapshot = store.read('session');
    const snapshotCycle = snapshot.cyclic as Record<string, unknown>;
    snapshotCycle['label'] = 'mutated';
    (snapshot.map as Map<string, { value: string }>).get('key')!.value = 'mutated';

    const stored = store.read('session');
    expect((stored.cyclic as Record<string, unknown>)['label']).toBe('stored');
    expect((stored.map as Map<string, { value: string }>).get('key')?.value).toBe('stored');
  });

  it('clones values on apply so the update object cannot mutate stored state', () => {
    const value = { nested: { status: 'stored' } };
    const store = new InMemoryGuardrailStateStore();
    store.apply([{ scope: 'session', key: 'value', action: 'set', value }]);
    value.nested.status = 'mutated';
    expect(store.read('session').value).toEqual({ nested: { status: 'stored' } });
  });

  it('returns a fresh snapshot on every read', () => {
    const store = new InMemoryGuardrailStateStore({ session: { toolCalls: 1 } });
    expect(store.read('session')).not.toBe(store.read('session'));
  });

  it('reflects an applied update in the next read', () => {
    const store = new InMemoryGuardrailStateStore();
    const before = store.read('session');
    store.apply([{ scope: 'session', key: 'toolCalls', action: 'increment' }]);
    expect(before).toEqual({});
    expect(store.read('session')).toEqual({ toolCalls: 1 });
  });

  it('ignores an update pointing at an unrecognized scope rather than throwing', () => {
    const store = new InMemoryGuardrailStateStore();
    const malformed = {
      scope: 'galaxy',
      key: 'calls',
      action: 'increment',
      value: 1,
    } as unknown as StateUpdate;
    expect(() => store.apply([malformed])).not.toThrow();
    expect(store.read('session')).toEqual({});
  });

  it('applies the rest of a batch when one update is malformed', () => {
    const store = new InMemoryGuardrailStateStore();
    const malformed = {
      scope: 'galaxy',
      key: 'calls',
      action: 'increment',
      value: 1,
    } as unknown as StateUpdate;
    store.apply([malformed, { scope: 'session', key: 'calls', action: 'increment', value: 2 }]);
    expect(store.read('session').calls).toBe(2);
  });

  it('accepts an empty batch without changing anything', () => {
    const store = new InMemoryGuardrailStateStore({ session: { toolCalls: 3 } });
    store.apply([]);
    expect(store.read('session')).toEqual({ toolCalls: 3 });
  });
});
