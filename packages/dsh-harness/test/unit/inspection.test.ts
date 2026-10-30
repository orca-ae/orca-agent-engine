// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { listChildPage, readChildPage, type ChildInspection } from '../../src/inspection.js';

const child: ChildInspection = {
  agent_id: 'child',
  parent_agent_id: 'root',
  state: 'idle',
  archived: null,
  history_availability: 'available',
};
type Event = NonNullable<Parameters<typeof readChildPage>[0]['source']>['events'][number];
type RecordResult = {
  source_id: string;
  kind: string;
  tool_call_id?: string;
  content: unknown;
  truncated?: boolean;
  original_bytes?: number | null;
};
type Page = {
  child: ChildInspection;
  children: (ChildInspection & { truncated?: boolean })[];
  history_source: string | null;
  records: RecordResult[];
  continuation: string | null;
  catalog_complete: boolean;
};
function decode(value: string): Page {
  expect(Buffer.byteLength(value)).toBeLessThanOrEqual(32_768);
  return JSON.parse(value) as Page;
}
function message(text: string): Omit<Event, 'seq'> {
  return { type: 'assistant/message', data: { message: { content: [{ type: 'text', text }] } } };
}
function events(...values: Omit<Event, 'seq'>[]): Event[] {
  return values.map((value, seq) =>
    Object.defineProperty(
      Object.create(null, Object.getOwnPropertyDescriptors(value)) as Event,
      'seq',
      { value: seq },
    ),
  );
}
function read(
  log: readonly Event[],
  options: Partial<Omit<Parameters<typeof readChildPage>[0], 'source'>> & {
    source?: Parameters<typeof readChildPage>[0]['source'] | null;
  } = {},
) {
  const {
    source = {
      generation: 'live-nonce',
      events: log,
      inheritedEventCount: 0,
      cursor: log.length - 1,
    },
    ...rest
  } = options;
  return decode(
    readChildPage({
      callerId: 'root',
      child,
      ...(source ? { source } : {}),
      ...rest,
    }),
  );
}
function trap(): never {
  throw new Error('Excluded payload accessed');
}

describe('readChildPage', () => {
  it('projects owned visible records, source identity and tool correlation without private fields', () => {
    const log = events(
      message('inherited'),
      {
        type: 'user/message',
        data: { source: { kind: 'user' }, content: [{ type: 'text', text: 'task' }] },
      },
      { type: 'tool/call', data: { callId: 'call-1', name: 'shell', arguments: '{unparsed' } },
      {
        type: 'tool/result',
        data: {
          get meta() {
            return trap();
          },
          message: {
            toolCallId: 'call-1',
            content: [{ type: 'text', text: 'result' }],
            get private() {
              return trap();
            },
          },
        },
      },
      {
        type: 'assistant/message',
        data: {
          get stream() {
            return trap();
          },
          message: {
            get source() {
              return trap();
            },
            content: [
              {
                type: 'reasoning',
                get text() {
                  return trap();
                },
              },
              { type: 'text', text: 'visible' },
              {
                type: 'tool-call',
                get arguments() {
                  return trap();
                },
              },
            ],
          },
        },
      },
      {
        type: 'turn/end',
        data: {
          turn: 1,
          reason: {
            kind: 'completed',
            get error() {
              return trap();
            },
          },
        },
      },
    );
    const result = read(log, {
      source: { generation: 'live-nonce', events: log, inheritedEventCount: 1, cursor: 5 },
    });
    expect(result.history_source).toBe('live');
    expect(result.records.map((row) => row.source_id)).toEqual([
      'child:1',
      'child:2',
      'child:3',
      'child:4',
      'child:5',
    ]);
    expect(result.records[1]).toMatchObject({
      tool_call_id: 'call-1',
      content: { name: 'shell', arguments: '{unparsed' },
    });
    expect(result.records[2]).toMatchObject({ tool_call_id: 'call-1' });
    expect(result.records[3]!.content).toEqual([{ type: 'text', text: 'visible' }]);
    expect(result.records[4]!.content).toEqual({ turn: 1, reason: 'completed' });
  });

  it('filters injected instructions and hidden events without touching their payloads', () => {
    const log = events(
      {
        type: 'system/message',
        get data() {
          return trap();
        },
      },
      {
        type: 'developer/message',
        get data() {
          return trap();
        },
      },
      {
        type: 'assistant/attempt',
        get data() {
          return trap();
        },
      },
      {
        type: 'user/message',
        data: {
          source: {
            kind: 'runtime-context',
            get private() {
              return trap();
            },
          },
          get content() {
            return trap();
          },
        },
      },
      message('visible'),
    );
    expect(read(log).records).toHaveLength(1);
  });

  it('includes routed messages using the same text allowlist', () => {
    const log = events(
      ...['agent-message', 'subagent-settled'].map((kind) => ({
        type: 'user/message',
        data: { source: { kind }, content: [{ type: 'text', text: kind }] },
      })),
    );
    expect(read(log).records.map((row) => row.content)).toEqual([
      [{ type: 'text', text: 'agent-message' }],
      [{ type: 'text', text: 'subagent-settled' }],
    ]);
  });

  it('caps raw scans at 200 and advances even through an empty visible page', () => {
    const log = events(
      ...Array.from({ length: 201 }, () => ({
        type: 'request/header',
        get data() {
          return trap();
        },
      })),
      message('tail'),
    );
    const first = read(log);
    expect(first.records).toEqual([]);
    expect(JSON.parse(first.continuation!).nextSeq).toBe(200);
    const next = read(log, { continuation: first.continuation! });
    expect(next.records[0]!.source_id).toBe('child:201');
    expect(next.continuation).toBeNull();
  });

  it('caps output at 50 records and admits appends without changing generations', () => {
    const log = events(...Array.from({ length: 51 }, () => message('hello')));
    const first = read(log);
    expect(first.records).toHaveLength(50);
    log.push({ ...message('append'), seq: 51 });
    const next = read(log, { continuation: first.continuation! });
    expect(next.records.map((row) => row.source_id)).toEqual(['child:50', 'child:51']);
  });

  it('defers a late large record that fits alone, without previewing or skipping it', () => {
    const log = events(message('a'.repeat(20_000)), message('b'.repeat(20_000)));
    const first = read(log);
    expect(first.records).toHaveLength(1);
    expect(first.records[0]!.truncated).toBeUndefined();
    expect(JSON.parse(first.continuation!).nextSeq).toBe(1);
    const second = read(log, { continuation: first.continuation! });
    expect(second.records[0]!.content).toEqual([{ type: 'text', text: 'b'.repeat(20_000) }]);
    expect(second.records[0]!.truncated).toBeUndefined();
  });

  it('defers on work exhaustion after a filtered record, then previews on a fresh page', () => {
    const log = events(
      {
        type: 'request/header',
        get data() {
          return trap();
        },
      },
      message('a'.repeat(1_000_000)),
      message('tail'),
    );
    const first = read(log);
    expect(first.records).toEqual([]);
    expect(JSON.parse(first.continuation!).nextSeq).toBe(1);
    const second = read(log, { continuation: first.continuation! });
    expect(second.records[0]).toMatchObject({
      source_id: 'child:1',
      truncated: true,
      original_bytes: null,
    });
    expect(JSON.parse(second.continuation!).nextSeq).toBe(2);
    expect(read(log, { continuation: second.continuation! }).records[0]!.source_id).toBe('child:2');
  });

  it.each(['汉😀', '\u0000\n"\\', '\ud800'])('bounds escaped Unicode previews: %j', (unit) => {
    const result = read(events(message(unit.repeat(20_000))));
    expect(result.records[0]!.truncated).toBe(true);
    expect(result.continuation).toBeNull();
  });

  it('reports exact visible bytes when projection completed before output exhaustion', () => {
    const text = '汉'.repeat(15_000);
    const result = read(events(message(text)));
    expect(result.records[0]).toMatchObject({
      truncated: true,
      original_bytes: Buffer.byteLength(JSON.stringify([{ type: 'text', text }])),
    });
  });

  it('counts excluded content blocks against the shared node budget', () => {
    let visited = 0;
    const blocks = Array.from({ length: 10_000 }, () => ({
      get type() {
        visited++;
        return 'reasoning';
      },
      get text() {
        return trap();
      },
    }));
    const result = read(
      events(
        { type: 'assistant/message', data: { message: { content: blocks } } },
        message('tail'),
      ),
    );
    expect(visited).toBeGreaterThan(500);
    expect(visited).toBeLessThan(4_096);
    expect(result.records[0]).toMatchObject({ truncated: true, original_bytes: null });
    expect(JSON.parse(result.continuation!).nextSeq).toBe(1);
  });

  it('accumulates node costs across filtered records and leaves the exhausted record for a fresh page', () => {
    const excluded = {
      type: 'assistant/message',
      data: {
        message: {
          content: Array.from({ length: 400 }, () => ({
            type: 'reasoning',
            get text() {
              return trap();
            },
          })),
        },
      },
    };
    const log = events(excluded, excluded, excluded, message('tail'));
    const first = read(log);
    expect(first.records).toEqual([]);
    const nextSeq = JSON.parse(first.continuation!).nextSeq as number;
    expect(nextSeq).toBeGreaterThan(0);
    expect(nextSeq).toBeLessThan(3);
    const second = read(log, { continuation: first.continuation! });
    expect(second.records).toHaveLength(1);
    expect(second.records[0]!.source_id).toBe('child:3');
    expect(second.records[0]!.truncated).toBeUndefined();
  });

  it('stops property inspection at character exhaustion, before reading excluded block payloads', () => {
    let visited = 0;
    const log = events({
      type: 'assistant/message',
      data: {
        message: {
          content: Array.from({ length: 100 }, () => ({
            get type() {
              visited++;
              return 'x'.repeat(10_000);
            },
            get text() {
              return trap();
            },
          })),
        },
      },
    });
    const result = read(log);
    expect(visited).toBeLessThanOrEqual(7);
    expect(result.records[0]).toMatchObject({ truncated: true, original_bytes: null });
    expect(result.continuation).toBeNull();
  });

  it('defers at exact node exhaustion without inspecting the next event type or retrying it', () => {
    let nextTypeReads = 0;
    const log = events(
      {
        type: 'assistant/message',
        data: {
          message: {
            content: Array.from({ length: 1012 }, () => ({
              type: 'reasoning',
              get text() {
                return trap();
              },
            })),
          },
        },
      },
      {
        get type() {
          nextTypeReads++;
          return 'assistant/message';
        },
        data: { message: { content: [{ type: 'text', text: 'tail' }] } },
      },
    );
    const first = read(log);
    expect(first.records).toEqual([]);
    expect(JSON.parse(first.continuation!).nextSeq).toBe(1);
    expect(nextTypeReads).toBe(0);
    const next = read(log, { continuation: first.continuation! });
    expect(next.records[0]!.content).toEqual([{ type: 'text', text: 'tail' }]);
    expect(next.records[0]!.truncated).toBeUndefined();
    expect(nextTypeReads).toBe(1);
  });

  it('shares filter character costs across records and defers without retrying within the call', () => {
    let visits = 0;
    const hugeTag = {
      type: 'user/message',
      data: {
        source: {
          get kind() {
            visits++;
            return 'x'.repeat(40_000);
          },
          get content() {
            return trap();
          },
        },
        get content() {
          return trap();
        },
      },
    };
    const log = events(hugeTag, hugeTag, message('tail'));
    const first = read(log);
    expect(visits).toBe(2);
    expect(first.records).toEqual([]);
    expect(JSON.parse(first.continuation!).nextSeq).toBe(1);
    const second = read(log, { continuation: first.continuation! });
    expect(second.records.at(-1)?.source_id).toBe('child:2');
    expect(visits).toBe(3);
  });

  it('never follows arbitrary nested payloads or enumerates their keys', () => {
    let nested: unknown = {
      get secret() {
        return trap();
      },
    };
    for (let i = 0; i < 100; i++) nested = { nested };
    const data = new Proxy(
      { message: { content: [{ type: 'text', text: 'okay', nested }], nested }, nested },
      { ownKeys: trap },
    );
    expect(read(events({ type: 'assistant/message', data })).records[0]!.content).toEqual([
      { type: 'text', text: 'okay' },
    ]);
  });

  it('uses optional metadata only after records and bounds its previews', () => {
    const result = read(events(message('a'.repeat(25_000))), {
      child: { ...child, label: 'z'.repeat(1_000_000) },
    });
    expect(result.records[0]!.truncated).toBeUndefined();
    expect(result.child.label!.length).toBeLessThan(8_000);
    expect((result.child as unknown as { truncated: boolean }).truncated).toBe(true);
  });

  it('uses a consistent required reserve on final and intermediate pages', () => {
    const log = events(message('x'.repeat(50_000)));
    const final = read(log);
    const intermediate = read([...log, { ...message('tail'), seq: 1 }]);
    expect(final.records[0]).toEqual(intermediate.records[0]);
  });

  it('keeps record and preview decisions stable across required enum changes', () => {
    const log = events(message('x'.repeat(32_395)));
    const idle = read(log, {
      child: { ...child, state: 'idle', archived: true, history_availability: 'partial' },
    });
    const waiting = read(log, {
      child: {
        ...child,
        state: 'waiting_for_approval',
        archived: false,
        history_availability: 'unavailable',
      },
    });
    expect(idle.child.state).toBe('idle');
    expect(waiting.child.state).toBe('waiting_for_approval');
    expect(idle.records).toEqual(waiting.records);
  });

  it.each(['tool/call', 'tool/result'])(
    'rejects huge required correlation IDs in %s before previewing',
    (type) => {
      const id = 'x'.repeat(100_000);
      const data =
        type === 'tool/call'
          ? { callId: id, name: 'a', arguments: '' }
          : { message: { toolCallId: id, content: [{ type: 'text', text: 'result' }] } };
      expect(() => read(events({ type, data }))).toThrow(
        'Inspection required metadata exceeds bounds',
      );
    },
  );

  it.each([undefined, null, '', 42, {}])(
    'rejects malformed correlation IDs without losing identity: %j',
    (id) => {
      for (const event of [
        { type: 'tool/call', data: { callId: id, name: 'read', arguments: '{}' } },
        { type: 'tool/result', data: { message: { toolCallId: id, content: [] } } },
      ]) {
        expect(() => read(events(event, message('later')))).toThrow(
          'Inspection required metadata exceeds bounds',
        );
      }
    },
  );

  it('rejects malformed, foreign, stale and out-of-range continuations with restart instructions', () => {
    const log = events(message('hello'));
    const valid = { caller: 'root', target: 'child', generation: 'live-nonce', nextSeq: 0 };
    for (const continuation of [
      'bad',
      '[]',
      'x'.repeat(2_049),
      ...[
        { caller: 'other' },
        { target: 'other' },
        { generation: 'pre-restore' },
        { nextSeq: -1 },
        { nextSeq: 2 },
        { nextSeq: 0.5 },
      ].map((change) => JSON.stringify({ ...valid, ...change })),
    ])
      expect(() => read(log, { continuation })).toThrow('retry without continuation');
  });

  it('validates inherited lower bound and stops at the captured cursor', () => {
    const log = events(message('prefix'), message('own'), message('outside cut'));
    const source = { generation: 'live-nonce', events: log, inheritedEventCount: 1, cursor: 1 };
    expect(read(log, { source }).records).toHaveLength(1);
    expect(() =>
      read(log, {
        source,
        continuation: JSON.stringify({
          caller: 'root',
          target: 'child',
          generation: 'live-nonce',
          nextSeq: 0,
        }),
      }),
    ).toThrow('retry without continuation');
  });

  it('returns unavailable before target-dependent continuation checks', () => {
    const result = read([], {
      source: null,
      continuation: JSON.stringify({ generation: 'old', nextSeq: -999 }),
    });
    expect(result).toMatchObject({
      child: { history_availability: 'unavailable', history_reason: 'not_attached' },
      history_source: null,
      records: [],
      continuation: null,
    });
    expect(() => read([], { source: null, continuation: 'malformed' })).toThrow(
      'retry without continuation',
    );
    const oversized = read([], {
      source: null,
      child: { ...child, history_reason: 'source_too_large', label: 'z'.repeat(1_000_000) },
    });
    expect(oversized.child.history_reason).toBe('source_too_large');
  });

  it('rejects corrupt source positions and unrepresentable required identifiers', () => {
    expect(() => read([{ ...message('bad'), seq: 2 }])).toThrow('corrupt');
    expect(() => read([], { child: { ...child, agent_id: 'x'.repeat(100_000) } })).toThrow(
      'metadata exceeds',
    );
    expect(() =>
      read(
        events({
          type: 'tool/call',
          data: { callId: 'x'.repeat(3_000), name: 'a', arguments: '' },
        }),
      ),
    ).toThrow('metadata exceeds');
  });
});

describe('listChildPage', () => {
  function list(
    children: { key: number; child: ChildInspection }[],
    continuation?: string,
    generation = 'catalog-nonce',
  ) {
    return decode(
      listChildPage({
        callerId: 'root',
        generation,
        children,
        catalogComplete: false,
        ...(continuation === undefined ? {} : { continuation }),
      }),
    );
  }

  it('lists the bounded catalog in creation order with explicit completeness and archive evidence', () => {
    const result = list(
      Array.from({ length: 127 }, (_, key) => ({
        key,
        child: { ...child, agent_id: `child-${key}` },
      })),
    );
    expect(result.children).toHaveLength(127);
    expect(result.catalog_complete).toBe(false);
    expect(result.continuation).toBeNull();
    expect(result.children[126]).toMatchObject({ agent_id: 'child-126', archived: null });
  });

  it('pages by stable keys across removals and additions and invalidates changed nonces', () => {
    const catalog = Array.from({ length: 127 }, (_, key) => ({
      key: key * 2,
      child: { ...child, agent_id: `child-${key}-` + 'x'.repeat(240) },
    }));
    const first = list(catalog);
    expect(first.continuation).not.toBeNull();
    const lastKey: number = JSON.parse(first.continuation!).lastKey;
    const changed = catalog.filter((entry) => entry.key > lastKey);
    changed.push({ key: 999, child: { ...child, agent_id: 'appended' } });
    const next = list(changed, first.continuation!);
    expect(next.children[0]!.agent_id).toBe(changed[0]!.child.agent_id);
    expect(next.children.at(-1)?.agent_id).toBe('appended');
    expect(() => list(changed, first.continuation!, 'rebuilt')).toThrow(
      'retry without continuation',
    );
  });

  it('keeps mandatory catalog entries visible before previewing oversized labels', () => {
    const result = list(
      Array.from({ length: 127 }, (_, key) => ({
        key,
        child: {
          ...child,
          agent_id: `child-${key}`,
          label: 'z'.repeat(100_000),
          history_reason: 'retained',
        },
      })),
    );
    expect(result.children).toHaveLength(127);
    expect(result.children[0]!.truncated).toBe(true);
    expect(result.continuation).toBeNull();
  });

  it('does not let the first child label displace the second child required fields', () => {
    const result = list([
      { key: 0, child: { ...child, agent_id: 'first', label: 'z'.repeat(100_000) } },
      { key: 1, child: { ...child, agent_id: 'second' } },
    ]);
    expect(result.children).toHaveLength(2);
    expect(result.children[0]!.truncated).toBe(true);
    expect(result.children[1]).toEqual({ ...child, agent_id: 'second' });
    expect(result.continuation).toBeNull();
  });

  it('rejects invalid continuation and catalog order, while keeping empty catalogs valid', () => {
    expect(list([]).children).toEqual([]);
    expect(() =>
      list([], JSON.stringify({ caller: 'other', generation: 'catalog-nonce', lastKey: 0 })),
    ).toThrow('retry without continuation');
    expect(() =>
      list([], JSON.stringify({ caller: 'root', generation: 'catalog-nonce', lastKey: -1 })),
    ).toThrow('retry without continuation');
    expect(() =>
      list([
        { key: 1, child },
        { key: 1, child },
      ]),
    ).toThrow('catalog order');
    expect(() => list(Array.from({ length: 128 }, (_, key) => ({ key, child })))).toThrow(
      'catalog exceeds',
    );
  });
});
