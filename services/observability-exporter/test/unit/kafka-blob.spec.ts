// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { decodeKafkaBlob, encodeKafkaBlob, kafkaBlobKeys } from '../../src/kafka-blob.js';
import {
  initialKafkaCheckpoint,
  parseKafkaCheckpoint,
  parseKafkaDeliveryRecord,
  projectKafkaEvents,
} from '../../src/kafka-state.js';
import { event, SESSION_ID, TRANSCRIPT_SECRET, WORKSPACE_ID } from '../support/events.js';
import { mixedDeliveryContext, PRIVATE_OUTCOME } from '../support/mixed-observations.js';

function encoded(value: unknown = '界😀\ud800'.repeat(20_000), maxRecordBytes?: number) {
  return encodeKafkaBlob(value, 'scope:reducer', {
    inlineBytes: 0,
    ...(maxRecordBytes === undefined ? {} : { maxRecordBytes }),
  });
}

function wire(data: Buffer) {
  return {
    blob: {
      kind: 'chunks',
      keyPrefix: 'scope:reducer',
      count: 1,
      bytes: data.length,
      sha256: createHash('sha256').update(data).digest('hex'),
    },
    values: [JSON.stringify({ version: 1, data: data.toString('base64') })],
  };
}

describe('bounded Kafka blob codec', () => {
  it.each([null, true, 123, 'hello', { nested: ['界😀\ud800'] }])(
    'roundtrips inline JSON %j',
    (value) => {
      const { blob, records } = encodeKafkaBlob(value, 'scope:reducer');
      expect(blob.kind).toBe('inline');
      expect(records).toEqual([]);
      expect(kafkaBlobKeys(blob, 'scope:reducer')).toEqual([]);
      expect(decodeKafkaBlob(blob, [])).toEqual(value);
      expect(() => decodeKafkaBlob(blob, [null])).toThrow();
    },
  );

  it('roundtrips split UTF8 and escaped lone surrogates with bounded records', () => {
    // Opening quote + 47,998 ASCII bytes puts the CJK code point across a chunk boundary.
    const value = 'x'.repeat(47_998) + '界😀\ud800'.repeat(20_000);
    const { blob, records } = encoded(value);
    expect(blob.kind).toBe('chunks');
    expect(records.map((r) => r.key)).toEqual(kafkaBlobKeys(blob, 'scope:reducer'));
    expect(records.every((r) => Buffer.byteLength(r.value) < 64 * 1024)).toBe(true);
    expect(
      decodeKafkaBlob(
        blob,
        records.map((r) => r.value),
      ),
    ).toEqual(value);
  });

  it('adapts payloads to small record budgets including UTF8 keys and framing', () => {
    const value = '界'.repeat(1000);
    const { blob, records } = encodeKafkaBlob(value, '作用域', {
      maxRecordBytes: 512,
      inlineBytes: 0,
    });
    for (const r of records)
      expect(Buffer.byteLength(r.key) + Buffer.byteLength(r.value) + 128).toBeLessThanOrEqual(512);
    expect(
      decodeKafkaBlob(
        blob,
        records.map((r) => r.value),
        { maxRecordBytes: 512 },
      ),
    ).toEqual(value);
    expect(() => encoded(value, 100)).toThrow();
    expect(() =>
      decodeKafkaBlob(
        blob,
        records.map((r) => r.value),
        { maxRecordBytes: 200 },
      ),
    ).toThrow();
  });

  it('rejects missing, extra, reordered, corrupt and malformed chunks', () => {
    const { blob, records } = encoded('a'.repeat(48_000) + 'b'.repeat(48_000));
    const values = records.map((r) => r.value);
    for (const invalid of [
      values.slice(1),
      [...values, values[0]!],
      [null, ...values.slice(1)],
      [...values].reverse(),
      ['{}', ...values.slice(1)],
      ['{', ...values.slice(1)],
    ]) {
      expect(() => decodeKafkaBlob(blob, invalid)).toThrow();
    }
    const chunk = JSON.parse(values[0]!);
    const bytes = Buffer.from(chunk.data, 'base64');
    bytes[10] = 98;
    expect(() =>
      decodeKafkaBlob(blob, [
        JSON.stringify({ version: 1, data: bytes.toString('base64') }),
        ...values.slice(1),
      ]),
    ).toThrow(/sha256/);
  });

  it.each(['YQ', 'YQ==\n', 'YR==', 'YQ===', '_w==', ''])(
    'rejects noncanonical base64 %j',
    (data) => {
      const { blob } = wire(Buffer.from('1'));
      expect(() => decodeKafkaBlob(blob, [JSON.stringify({ version: 1, data })])).toThrow();
    },
  );

  it.each([
    Buffer.from([0x22, 0xc0, 0xaf, 0x22]),
    Buffer.from([0x22, 0xed, 0xa0, 0x80, 0x22]),
    Buffer.from([0xef, 0xbb, 0xbf, 0x31]),
    Buffer.from('not json'),
  ])('rejects authenticated invalid UTF8 or JSON', (data) => {
    const { blob, values } = wire(data);
    expect(() => decodeKafkaBlob(blob, values)).toThrow();
  });

  it('validates hostile metadata before constructing keys or assemblies', () => {
    const { blob, records } = encoded();
    const values = records.map((r) => r.value);
    for (const patch of [
      { count: 2 ** 32 },
      { count: Number.MAX_SAFE_INTEGER },
      { count: 0 },
      { count: 1.5 },
      { bytes: 65 * 1024 * 1024 },
      { bytes: -1 },
      { bytes: NaN },
      { bytes: 1 },
      { sha256: 'z'.repeat(64) },
      { sha256: 'a'.repeat(63) },
      { keyPrefix: '' },
      { keyPrefix: '\ud800' },
      { keyPrefix: 'x'.repeat(4097) },
    ]) {
      const invalid = { ...blob, ...patch };
      expect(() => kafkaBlobKeys(invalid, 'scope:reducer')).toThrow();
      expect(() => decodeKafkaBlob(invalid, values)).toThrow();
    }
    expect(() => kafkaBlobKeys(blob, 'scope:delivery')).toThrow(/prefix/);
    expect(() =>
      decodeKafkaBlob({ ...blob, bytes: (blob.kind === 'chunks' ? blob.bytes : 0) + 1 }, values),
    ).toThrow();
    expect(() => decodeKafkaBlob({ ...blob, sha256: '0'.repeat(64) }, values)).toThrow(/sha256/);
  });

  it('enforces assembly and option bounds on both representations', () => {
    for (const maxAssemblyBytes of [0, -1, 1.1, Infinity, 64 * 1024 * 1024 + 1]) {
      expect(() => encodeKafkaBlob(null, 'scope', { maxAssemblyBytes })).toThrow();
      expect(() =>
        decodeKafkaBlob({ kind: 'inline', value: null }, [], { maxAssemblyBytes }),
      ).toThrow();
      expect(() =>
        kafkaBlobKeys({ kind: 'inline', value: null }, 'scope', maxAssemblyBytes),
      ).toThrow();
    }
    expect(() => encodeKafkaBlob('large', 'scope', { maxAssemblyBytes: 1 })).toThrow();
    expect(() =>
      decodeKafkaBlob({ kind: 'inline', value: 'large' }, [], { maxAssemblyBytes: 1 }),
    ).toThrow();
    expect(encodeKafkaBlob(1, 'scope', { maxAssemblyBytes: 1 }).blob.kind).toBe('inline');
    expect(() => encodeKafkaBlob(undefined, 'scope')).toThrow();
    expect(() => encodeKafkaBlob(1n, 'scope')).toThrow();
    expect(() => encodeKafkaBlob(null, 'scope', { maxRecordBytes: NaN })).toThrow();
    expect(() => encodeKafkaBlob(null, 'scope', { inlineBytes: -1 })).toThrow();
  });

  it('supports large-small-large mutable slots without reading stale tails', () => {
    const state = new Map<string, string>();
    for (const value of ['large'.repeat(40_000), { small: true }, 'different'.repeat(50_000)]) {
      const { blob, records } = encodeKafkaBlob(value, 'scope:reducer');
      for (const r of records) state.set(r.key, r.value);
      expect(
        decodeKafkaBlob(
          blob,
          kafkaBlobKeys(blob, 'scope:reducer').map((key) => state.get(key) ?? null),
        ),
      ).toEqual(value);
    }
  });

  it('roundtrips real large reducer and delivery fixtures', () => {
    // Same bounded model/tool/evaluation lifecycle as the Kafka v2 integration test,
    // generated from committed event helpers, never local ablation JSON.
    const events: ReturnType<typeof event>[] = [];
    const add = (
      kind: string,
      payload: Record<string, unknown> = {},
      options: Parameters<typeof event>[3] = {},
    ) => {
      const seq = events.length;
      const next = event(seq, kind, payload, {
        id: ('evt_' + String(seq).padStart(8, '0') + '_').padEnd(512, 'x'),
        ...options,
      });
      events.push(next);
      return next.id;
    };
    const user = add(
      'user.message',
      { content: TRANSCRIPT_SECRET },
      { producedBy: 'client', userId: '\ud800'.repeat(512) },
    );
    add('session.user_event_processed', { user_event_id: user });
    add('session.status_running');
    for (let i = 0; i < 256; i++) {
      const model = add('span.model_request_start', {
        model_observation_kind: 'turn_model_summary',
        provider: 'p'.repeat(256),
        model: '\ud800'.repeat(256),
      });
      add('span.model_request_end', {
        model_observation_kind: 'turn_model_summary',
        model_request_start_id: model,
        is_error: false,
        model_usage: { input_tokens: 11, output_tokens: 7 },
      });
      const tool = add('agent.custom_tool_use', {
        name: TRANSCRIPT_SECRET,
        input: TRANSCRIPT_SECRET,
      });
      const result = add(
        'user.custom_tool_result',
        { custom_tool_use_id: tool, content: TRANSCRIPT_SECRET },
        { producedBy: 'client' },
      );
      add('session.status_idle', { stop_reason: { type: 'requires_action' } });
      add('session.user_event_processed', { user_event_id: result });
      add('session.status_running');
      const evaluation = add('span.outcome_evaluation_start', {
        outcome_id: PRIVATE_OUTCOME,
        iteration: i,
      });
      add('span.outcome_evaluation_end', {
        outcome_id: PRIVATE_OUTCOME,
        iteration: i,
        outcome_evaluation_start_id: evaluation,
        result: 'satisfied',
        explanation: TRANSCRIPT_SECRET,
      });
    }
    for (let i = 0; i < 1024; i++)
      add(
        'user.message',
        { content: TRANSCRIPT_SECRET },
        { producedBy: 'client', userId: 'u'.repeat(512) },
      );

    const route = { topic: 'transcript', workspaceId: WORKSPACE_ID, sessionId: SESSION_ID };
    const projected = projectKafkaEvents(
      initialKafkaCheckpoint(route, mixedDeliveryContext),
      events.map((event, i) => ({ event, offset: String(i) })),
    );
    expect(projected.deliveries).toEqual([]);
    const checkpoint = parseKafkaCheckpoint(
      JSON.parse(JSON.stringify(projected.checkpoint)),
      route,
    );
    add('session.status_idle', { stop_reason: { type: 'end_turn' } });
    const completed = projectKafkaEvents(checkpoint, [
      { event: events.at(-1)!, offset: checkpoint.nextOffset },
    ]);
    expect(completed.deliveries).toHaveLength(1);
    const delivery = parseKafkaDeliveryRecord(JSON.parse(JSON.stringify(completed.deliveries[0])));

    for (const [slot, value] of [
      ['reducer', checkpoint.reducer],
      ['delivery', delivery],
    ] as const) {
      const json = JSON.stringify(value);
      expect(Buffer.byteLength(json)).toBeGreaterThan(512 * 1024);
      expect(json).not.toContain(TRANSCRIPT_SECRET);
      expect(json).not.toContain(PRIVATE_OUTCOME);
      const prefix = `scope:${slot}`;
      const { blob, records } = encodeKafkaBlob(value, prefix);
      expect(blob.kind).toBe('chunks');
      expect(records.length).toBeGreaterThan(1);
      expect(records.map((r) => r.key)).toEqual(kafkaBlobKeys(blob, prefix));
      expect(records.every((r) => Buffer.byteLength(r.value) < 64 * 1024)).toBe(true);
      const restored = decodeKafkaBlob(
        blob,
        records.map((r) => r.value),
      );
      expect(restored).toEqual(value);
      if (slot === 'reducer')
        expect(parseKafkaCheckpoint({ ...checkpoint, reducer: restored }, route)).toEqual(
          checkpoint,
        );
      else expect(parseKafkaDeliveryRecord(restored)).toEqual(delivery);
    }
  });
});
