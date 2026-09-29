// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Transaction } from 'kafkajs';
import { KafkaDiskIndex } from '../../src/kafka-disk-index.js';
import {
  decodeKafkaDelivery,
  encodeKafkaDelivery,
  kafkaRecordBytes,
  sendKafkaStateRecords,
} from '../../src/kafka-state-io.js';
import { initialKafkaCheckpoint, projectKafkaEvents } from '../../src/kafka-state.js';
import { kafkaSourceGroupId } from '../../src/kafka-runtime.js';
import { completedPrimaryTurnEvents, event, SESSION_ID, WORKSPACE_ID } from '../support/events.js';
import { parseKafkaDeliveryContext } from '../../src/kafka-validation.js';

const stores: KafkaDiskIndex[] = [];
afterEach(async () => {
  await Promise.all(stores.splice(0).map((store) => store.close()));
});
const route = {
  workspaceId: WORKSPACE_ID,
  sessionId: SESSION_ID,
  topic: `orca.${WORKSPACE_ID}.sessions.${SESSION_ID}.events`,
};
const key = kafkaSourceGroupId('test-namespace', route.topic);
const context = parseKafkaDeliveryContext({
  organizationId: 'org_test',
  bindingId: 'aob_test',
  bindingVersion: 1,
  adapterType: 'otlp_http',
  endpointKind: 'traces_endpoint',
  endpointClass: 'public',
  endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
  semanticProfile: 'langfuse',
  protocol: 'http/json',
  compression: 'none',
  timeoutMs: 1000,
  captureMode: 'metadata_only',
  sampleRate: 1,
  configSchemaVersion: 1,
});
const checkpoint = initialKafkaCheckpoint(route, context);
const record = projectKafkaEvents(
  checkpoint,
  completedPrimaryTurnEvents().map((event) => ({ offset: String(event.seq), event })),
).deliveries[0]!;

describe('Kafka state writes and dual-format delivery', () => {
  it('reports exact assembly bytes and numeric-only budget errors without delivery secrets', () => {
    const actual = Buffer.byteLength(JSON.stringify(record));
    expect(encodeKafkaDelivery(record, key).assemblyBytes).toBe(actual);
    expect(() =>
      encodeKafkaDelivery(record, key, { maxRecordBytes: 2048, maxAssemblyBytes: actual - 1 }),
    ).toThrow(
      `Kafka delivery assembly size budget exceeded: kind=delivery-assembly actual=${actual} limit=${actual - 1}`,
    );
  });
  it('uses the checkpoint budget for chunks even with a larger delivery budget', () => {
    const events = completedPrimaryTurnEvents().slice(0, 3);
    for (let i = 0; i < 100; i++) {
      const id = `evt_${'x'.repeat(480)}_${i}`;
      events.push(
        event(
          4 + i * 2,
          'span.model_request_start',
          { observation_type: 'turn_model_summary', provider: 'anthropic', model: 'model' },
          { id },
        ),
      );
      events.push(
        event(5 + i * 2, 'span.model_request_end', {
          observation_type: 'turn_model_summary',
          model_request_start_id: id,
          is_error: false,
        }),
      );
    }
    events.push(event(204, 'session.status_idle', { stop_reason: { type: 'end_turn' } }));
    const large = projectKafkaEvents(
      checkpoint,
      events.map((event) => ({ offset: String(event.seq), event })),
    ).deliveries[0]!;
    const encoded = encodeKafkaDelivery(large, key, {
      maxRecordBytes: 512 * 1024,
      maxChunkBytes: 48 * 1024,
    });
    expect(encoded.chunks.length).toBeGreaterThan(2);
    for (const chunk of encoded.chunks)
      expect(kafkaRecordBytes(chunk)).toBeLessThanOrEqual(48 * 1024);
    expect(kafkaRecordBytes(encoded.delivery)).toBeLessThanOrEqual(512 * 1024);
  });
  it('continues to decode old inline delivery without any state query', async () => {
    const index = await KafkaDiskIndex.open();
    stores.push(index);
    const read = vi.spyOn(index, 'read');
    const onAssemblyBytes = vi.fn();
    expect(
      await decodeKafkaDelivery(JSON.stringify(record), index, 'test-namespace', {
        onAssemblyBytes,
      }),
    ).toEqual(record);
    expect(onAssemblyBytes).toHaveBeenCalledWith(Buffer.byteLength(JSON.stringify(record)));
    expect(read).not.toHaveBeenCalled();
  });

  it('waits for missing chunks and authenticates immutable content independent of a later source head', async () => {
    const index = await KafkaDiskIndex.open();
    stores.push(index);
    const encoded = encodeKafkaDelivery(record, key, { maxRecordBytes: 2048 });
    const onAssemblyBytes = vi.fn();
    expect(encoded.chunks.length).toBeGreaterThan(1);
    expect(
      await decodeKafkaDelivery(encoded.delivery.value, index, 'test-namespace', {
        maxRecordBytes: 2048,
        onAssemblyBytes,
      }),
    ).toBeNull();
    expect(onAssemblyBytes).not.toHaveBeenCalled();
    await index.apply([
      { key, value: JSON.stringify(checkpoint), offset: '0' },
      ...encoded.chunks.map((entry, i) => ({ ...entry, offset: String(i + 1) })),
    ]);
    expect(
      await decodeKafkaDelivery(encoded.delivery.value, index, 'test-namespace', {
        maxRecordBytes: 2048,
        onAssemblyBytes,
      }),
    ).toEqual(record);
    expect(onAssemblyBytes).toHaveBeenCalledWith(encoded.assemblyBytes);
    await index.apply([
      {
        key,
        value: JSON.stringify({ ...checkpoint, version: 2, nextOffset: '1000' }),
        offset: '1000',
      },
    ]);
    expect(
      await decodeKafkaDelivery(encoded.delivery.value, index, 'test-namespace', {
        maxRecordBytes: 2048,
      }),
    ).toEqual(record);
    await index.apply([
      {
        key,
        value: JSON.stringify({ ...checkpoint, route: { ...route, workspaceId: 'ws_foreign' } }),
        offset: '1001',
      },
    ]);
    await expect(
      decodeKafkaDelivery(encoded.delivery.value, index, 'test-namespace', {
        maxRecordBytes: 2048,
      }),
    ).rejects.toThrow('scope mismatch');
  });

  it('rejects forged prefixes and corrupted content rather than treating corruption as lag', async () => {
    const index = await KafkaDiskIndex.open();
    stores.push(index);
    const encoded = encodeKafkaDelivery(record, key, { maxRecordBytes: 2048 });
    const manifest = JSON.parse(encoded.delivery.value);
    await expect(
      decodeKafkaDelivery(
        JSON.stringify({ ...manifest, checkpointKey: 'foreign-source-' + 'a'.repeat(64) }),
        index,
        'test-namespace',
      ),
    ).rejects.toThrow('manifest');
    await index.apply([
      { key, value: JSON.stringify(checkpoint), offset: '0' },
      ...encoded.chunks.map((entry, i) => ({ ...entry, offset: String(i + 1) })),
    ]);
    const chunk = JSON.parse(encoded.chunks[0]!.value);
    chunk.data = Buffer.from('corrupt').toString('base64');
    await index.apply([{ ...encoded.chunks[0]!, value: JSON.stringify(chunk), offset: '200' }]);
    await expect(
      decodeKafkaDelivery(encoded.delivery.value, index, 'test-namespace', {
        maxRecordBytes: 2048,
      }),
    ).rejects.toThrow();
  });

  it('uses multiple bounded Produce batches without committing the caller transaction', async () => {
    const records = Array.from({ length: 1200 }, (_, i) => ({
      key: `key-${i}`,
      value: 'a'.repeat(100),
    }));
    let next = 0;
    const send = vi.fn(
      async ({ messages }: { messages: Array<{ key: string; value: string }> }) => {
        expect(messages.reduce((sum, row) => sum + kafkaRecordBytes(row), 0)).toBeLessThanOrEqual(
          4096,
        );
        const baseOffset = String(next);
        next += messages.length + 5;
        return [{ partition: 0, baseOffset }];
      },
    );
    const commit = vi.fn();
    const assertOwner = vi.fn();
    const last = await sendKafkaStateRecords(
      { send, commit } as unknown as Transaction,
      'state',
      records,
      assertOwner,
      4096,
      0,
    );
    expect(send.mock.calls.length).toBeGreaterThan(2);
    expect(commit).not.toHaveBeenCalled();
    expect(last).toBe(String(next - 6));
    expect(send.mock.calls.flatMap(([input]) => input.messages).map(({ key }) => key)).toEqual(
      records.map(({ key }) => key),
    );
    expect(assertOwner).toHaveBeenCalledTimes(send.mock.calls.length * 2);
  });

  it('does not send after ownership is lost and rejects an oversized first record', async () => {
    const send = vi.fn();
    await expect(
      sendKafkaStateRecords(
        { send } as unknown as Transaction,
        'state',
        [{ key: 'k', value: 'value' }],
        () => {
          throw new Error('lost');
        },
      ),
    ).rejects.toThrow('lost');
    await expect(
      sendKafkaStateRecords(
        { send } as unknown as Transaction,
        'state',
        [{ key: 'k', value: 'oversized' }],
        () => {},
        1,
      ),
    ).rejects.toThrow('Kafka exporter record size budget exceeded: kind=record actual=138 limit=1');
    expect(send).not.toHaveBeenCalled();
  });
});
