// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Context } from '@deepseek-ai/cordis';
import { SessionId, SessionStore, type Session } from '@deepseek-ai/dsh-session';
import { SessionQueryEngine } from '@deepseek-ai/dsh-session-query';
import { afterEach, expect, it, vi } from 'vitest';
import { ChildInspectionRuntime, type DeclaredChild } from '../../src/inspection-runtime.js';

class Query extends SessionQueryEngine {
  async searchSessions(): Promise<never> {
    throw new Error('Search is not inspection');
  }
  async searchEvents(): Promise<never> {
    throw new Error('Search is not inspection');
  }
}

const roots: Context[] = [];
afterEach(async () => {
  for (const ctx of roots.splice(0)) await ctx.fiber.dispose();
  vi.restoreAllMocks();
});

function fixture() {
  const ctx = new Context();
  roots.push(ctx);
  const sessions = new SessionStore(ctx);
  const persistence = {
    identity: 'fixture',
    stat: vi.fn(() => {
      throw new Error('Persistence stat forbidden');
    }),
    open: vi.fn(() => {
      throw new Error('Persistence load forbidden');
    }),
    list: vi.fn(() => {
      throw new Error('Persistence list forbidden');
    }),
  };
  ctx.provide('sessionPersistence', persistence);
  const sessionQuery = new Query(ctx);
  const agents = { get: vi.fn() };
  const children = new Map<string, DeclaredChild>([
    ['a', { parent: 'root', key: 0, label: 'child' }],
    ['b', { parent: 'root', key: 1 }],
    ['nested', { parent: 'a', key: 2 }],
  ]);
  const stops = new Map<string, string>();
  const runtime = new ChildInspectionRuntime(
    { sessions, sessionQuery, agents } as unknown as Context,
    children,
    (id) => id === 'b',
    stops,
  );
  const check = vi.fn();
  const controller = new AbortController();
  const read = (target = 'a', continuation?: string, caller = 'root') =>
    runtime.read(caller, target, check, controller.signal, continuation).then(JSON.parse);
  const attach = (id = 'a') => {
    const session = sessions.prepare(SessionId(id));
    const detach = sessions.enter(session);
    return { session, detach };
  };
  const noPersistence = () => {
    expect(persistence.stat).not.toHaveBeenCalled();
    expect(persistence.open).not.toHaveBeenCalled();
    expect(persistence.list).not.toHaveBeenCalled();
  };
  return {
    ctx,
    sessions,
    sessionQuery,
    agents,
    children,
    stops,
    runtime,
    check,
    controller,
    read,
    attach,
    noPersistence,
  };
}

function addRecords(session: Session, count: number) {
  for (let i = 0; i < count; i++)
    session.append('tool/call', {
      turn: 1,
      step: 1,
      callId: ('call-' + session.seq) as never,
      name: 'read',
      arguments: '{}',
    });
}

it('lists the established descendant catalog without observing, and keeps dormant history unavailable', async () => {
  const f = fixture();
  const observation = vi.spyOn(f.sessionQuery, 'observeSession');
  f.attach();
  f.agents.get.mockImplementation((id) => (id === 'a' ? { status: 'running' } : undefined));
  const list = JSON.parse(f.runtime.list('root', f.check));
  expect(list.children).toMatchObject([
    { agent_id: 'a', parent_agent_id: 'root', state: 'running', history_availability: 'available' },
    { agent_id: 'b', state: 'dormant', archived: true, history_availability: 'unavailable' },
    { agent_id: 'nested', state: 'dormant' },
  ]);
  expect(
    JSON.parse(f.runtime.list('a', f.check)).children.map((c: { agent_id: string }) => c.agent_id),
  ).toEqual(['nested']);
  expect(JSON.parse(f.runtime.list('nested', f.check)).children).toEqual([]);
  const missing = await f.read('b', '{}');
  expect(missing).toMatchObject({
    child: { history_availability: 'unavailable' },
    history_source: null,
    records: [],
  });
  expect(missing.child).not.toHaveProperty('last_stop_reason');
  expect(observation).not.toHaveBeenCalled();
  f.noPersistence();
});

it('reads an archived descendant while its source remains attached without persistence', async () => {
  const f = fixture();
  const { session } = f.attach('b');
  addRecords(session, 1);
  const page = await f.read('b');
  expect(page).toMatchObject({
    child: { archived: true, history_availability: 'available' },
    history_source: 'live',
    continuation: null,
  });
  expect(page.records).toHaveLength(1);
  expect(page.records[0].kind).toBe('tool/call');
  f.noPersistence();
});

it('denies missing, sibling, ancestor and self targets before any state or source lookup', async () => {
  const f = fixture();
  const lookup = vi.spyOn(f.sessions, 'get');
  for (const [caller, target] of [
    ['a', 'b'],
    ['a', 'root'],
    ['root', 'missing'],
    ['a', 'a'],
  ])
    await expect(f.read(target, 'forged', caller)).rejects.toThrow(
      'DSH inspection target outside owned authority',
    );
  expect(lookup).not.toHaveBeenCalled();
  expect(f.agents.get).not.toHaveBeenCalled();
  f.noPersistence();
});

it('denies an unrelated attached Session in the same store before target lookup or acquisition', async () => {
  const f = fixture();
  const { session } = f.attach('unrelated');
  addRecords(session, 1);
  expect(f.sessions.get(SessionId('unrelated'))).toBe(session);
  expect(f.children.has('unrelated')).toBe(false);
  const lookup = vi.spyOn(f.sessions, 'get');
  const observation = vi.spyOn(f.sessionQuery, 'observeSession');
  await expect(f.read('unrelated')).rejects.toThrow(
    'DSH inspection target outside owned authority',
  );
  expect(lookup).not.toHaveBeenCalled();
  expect(observation).not.toHaveBeenCalled();
  expect(f.agents.get).not.toHaveBeenCalled();
  f.noPersistence();
});

it('agrees on list/read eligibility at the real native 65,536-event boundary', async () => {
  const f = fixture();
  const { session } = f.attach();
  addRecords(session, 65_536);
  expect(session.seq).toBe(65_536);
  const observe = vi.spyOn(f.sessionQuery, 'observeSession');
  const materialize = vi.spyOn(session, 'snapshotEvents');
  expect(JSON.parse(f.runtime.list('root', f.check)).children[0].history_availability).toBe(
    'available',
  );
  const page = await f.read();
  expect(page.child.history_availability).toBe('available');
  expect(page.records).toHaveLength(50);
  expect((await observe.mock.results[0]!.value).cursor).toBe(65_535);
  expect(materialize).toHaveBeenCalledOnce();
  materialize.mockClear();
  addRecords(session, 1);
  expect(JSON.parse(f.runtime.list('root', f.check)).children[0].history_reason).toBe(
    'source_too_large',
  );
  expect(await f.read()).toMatchObject({
    child: { history_reason: 'source_too_large' },
    history_source: null,
    records: [],
    continuation: null,
  });
  expect(observe).toHaveBeenCalledOnce();
  expect(materialize).not.toHaveBeenCalled();
  f.noPersistence();
});

it('reports detached completion as dormant with only retained turn-local stop evidence', () => {
  const f = fixture();
  f.attach().detach();
  f.stops.set('a', 'completed');
  expect(JSON.parse(f.runtime.list('root', f.check)).children[0]).toMatchObject({
    state: 'dormant',
    last_stop_reason: 'completed',
    history_reason: 'source_not_attached',
  });
  f.stops.clear();
  expect(JSON.parse(f.runtime.list('root', f.check)).children[0]).not.toHaveProperty(
    'last_stop_reason',
  );
  f.noPersistence();
});

it('checks the live event cap before acquisition and the captured cap before materialization', async () => {
  const f = fixture();
  const { session } = f.attach();
  const observe = vi.spyOn(f.sessionQuery, 'observeSession');
  const seq = vi.spyOn(session, 'seq', 'get').mockReturnValue(65_537 as never);
  expect(await f.read()).toMatchObject({
    child: { history_reason: 'source_too_large' },
    records: [],
  });
  expect(observe).not.toHaveBeenCalled();
  seq.mockReturnValue(65_536 as never);
  // The real native reader captures seq immediately, but events must remain lazy.
  const materialize = vi.spyOn(session, 'snapshotEvents').mockImplementation(() => {
    throw new Error('must remain lazy');
  });
  seq
    .mockReturnValueOnce(65_536 as never)
    .mockReturnValueOnce(65_536 as never)
    .mockReturnValue(65_537 as never);
  expect(await f.read()).toMatchObject({ child: { history_reason: 'source_too_large' } });
  expect(observe).toHaveBeenCalledOnce();
  expect(materialize).not.toHaveBeenCalled();
  f.noPersistence();
});

it.each(['detach', 'replace', 'retire', 'abort'])(
  'rejects %s while awaiting a live observation, disposing its lease without cold fallback',
  async (change) => {
    const f = fixture();
    const { session, detach } = f.attach();
    const materialize = vi.spyOn(session, 'snapshotEvents');
    const observe = f.sessionQuery.observeSession.bind(f.sessionQuery);
    const dispose = vi.fn();
    vi.spyOn(f.sessionQuery, 'observeSession').mockImplementation(async (...args) => {
      const observation = await observe(...args);
      const release = observation[Symbol.dispose].bind(observation);
      observation[Symbol.dispose] = () => {
        dispose();
        release();
      };
      if (change === 'detach' || change === 'replace') detach();
      if (change === 'replace') f.attach();
      if (change === 'retire')
        f.check.mockImplementation(() => {
          throw new Error('caller retired');
        });
      if (change === 'abort') f.controller.abort();
      return observation;
    });
    await expect(f.read()).rejects.toThrow();
    expect(dispose).toHaveBeenCalledOnce();
    expect(materialize).not.toHaveBeenCalled();
    f.noPersistence();
  },
);

it('keeps live generations over appends and invalidates them on Session replacement', async () => {
  const f = fixture();
  const { session, detach } = f.attach();
  addRecords(session, 60);
  const first = await f.read();
  expect(first.records).toHaveLength(50);
  expect(first.continuation).toEqual(expect.any(String));
  addRecords(session, 1);
  expect((await f.read('a', first.continuation)).records).toHaveLength(11);
  detach();
  addRecords(f.attach().session, 60);
  await expect(f.read('a', first.continuation)).rejects.toThrow(/continuation/i);
  expect((await f.read()).records).toHaveLength(50);
  f.noPersistence();
});

it('rejects a caller fence before list or source acquisition', async () => {
  const f = fixture();
  f.check.mockImplementation(() => {
    throw new Error('archived caller');
  });
  const lookup = vi.spyOn(f.sessions, 'get');
  expect(() => f.runtime.list('root', f.check)).toThrow('archived caller');
  await expect(f.read()).rejects.toThrow('archived caller');
  expect(lookup).not.toHaveBeenCalled();
  f.noPersistence();
});
