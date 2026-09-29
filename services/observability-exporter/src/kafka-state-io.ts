// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type { Transaction } from 'kafkajs';
import { encodeKafkaBlob, type KafkaBlobValue, type KafkaStateRecord } from './kafka-blob.js';
import { parseKafkaDeliveryRecord, type KafkaDeliveryRecord } from './kafka-state.js';
import { kafkaStateKeys, readKafkaStateBlob } from './kafka-state-v2.js';
import type { KafkaDiskIndex } from './kafka-disk-index.js';
import { parseKafkaDeliveryContext } from './kafka-validation.js';

export function encodeKafkaDelivery(
  record: KafkaDeliveryRecord,
  checkpointKey: string,
  options: { maxRecordBytes?: number; maxAssemblyBytes?: number; maxChunkBytes?: number } = {},
): { delivery: KafkaStateRecord; chunks: KafkaStateRecord[]; assemblyBytes: number } {
  const serialized = JSON.stringify(record);
  const assemblyBytes = Buffer.byteLength(serialized);
  const maxRecord = options.maxRecordBytes ?? 512 * 1024;
  if (
    assemblyBytes + Buffer.byteLength(record.trace.traceId) + 128 <=
    Math.min(maxRecord, 64 * 1024)
  )
    return {
      delivery: { key: record.trace.traceId, value: serialized },
      chunks: [],
      assemblyBytes,
    };
  const maxAssembly = options.maxAssemblyBytes ?? 32 * 1024 * 1024;
  if (assemblyBytes > maxAssembly)
    throw new Error(
      `Kafka delivery assembly size budget exceeded: kind=delivery-assembly actual=${assemblyBytes} limit=${maxAssembly}`,
    );
  const digest = createHash('sha256').update(serialized).digest('hex');
  const prefix = `${kafkaStateKeys(checkpointKey).deliveryPrefix}${digest}`;
  const encoded = encodeKafkaBlob(record, prefix, {
    ...options,
    maxRecordBytes: Math.min(maxRecord, options.maxChunkBytes ?? maxRecord),
    inlineBytes: 0,
  });
  return {
    delivery: {
      key: record.trace.traceId,
      value: JSON.stringify({
        version: 2,
        checkpointKey,
        traceId: record.trace.traceId,
        digest,
        blob: encoded.blob,
      }),
    },
    chunks: encoded.records,
    assemblyBytes,
  };
}

/** Legacy inline deliveries remain valid. Manifest references cannot escape their source namespace. */
export async function decodeKafkaDelivery(
  raw: string,
  index: KafkaDiskIndex,
  namespace: string,
  options: {
    maxRecordBytes?: number;
    maxAssemblyBytes?: number;
    onAssemblyBytes?: (bytes: number) => void;
  } = {},
): Promise<KafkaDeliveryRecord | null> {
  const value: unknown = JSON.parse(raw);
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('invalid Kafka delivery record');
  const manifest = value as Record<string, unknown>;
  if (manifest.version !== 2) {
    const record = parseKafkaDeliveryRecord(value);
    options.onAssemblyBytes?.(Buffer.byteLength(raw));
    return record;
  }
  if (
    typeof manifest.checkpointKey !== 'string' ||
    !manifest.checkpointKey.startsWith(`${namespace}-source-`) ||
    !/^[a-f0-9]{64}$/u.test(manifest.checkpointKey.slice(`${namespace}-source-`.length)) ||
    typeof manifest.traceId !== 'string' ||
    !/^[a-f0-9]{32}$/u.test(manifest.traceId) ||
    typeof manifest.digest !== 'string' ||
    !/^[a-f0-9]{64}$/u.test(manifest.digest)
  )
    throw new Error('invalid Kafka delivery manifest');
  const prefix = `${kafkaStateKeys(manifest.checkpointKey).deliveryPrefix}${manifest.digest}`;
  if (
    !manifest.blob ||
    typeof manifest.blob !== 'object' ||
    (manifest.blob as KafkaBlobValue).kind !== 'chunks'
  )
    throw new Error('invalid Kafka delivery manifest kind');
  let missing = false;
  let decoded: unknown;
  try {
    decoded = await readKafkaStateBlob(
      {
        read: async (requested) => {
          const values = await index.read(requested);
          if (values.some((entry) => entry === null)) {
            missing = true;
            throw new Error('Kafka delivery content pending');
          }
          return values;
        },
      },
      manifest.blob as KafkaBlobValue,
      prefix,
      undefined,
      options,
    );
    // readKafkaStateBlob has validated this chunk descriptor; no payload serialization for stats.
    options.onAssemblyBytes?.((manifest.blob as Extract<KafkaBlobValue, { kind: 'chunks' }>).bytes);
  } catch (error) {
    if (missing) return null;
    throw error;
  }
  if (createHash('sha256').update(JSON.stringify(decoded)).digest('hex') !== manifest.digest)
    throw new Error('Kafka delivery content digest mismatch');
  const record = parseKafkaDeliveryRecord(decoded);
  if (record.trace.traceId !== manifest.traceId) throw new Error('Kafka delivery trace mismatch');
  const [headValue] = await index.read([manifest.checkpointKey]);
  if (headValue === null) return null;
  const head = JSON.parse(headValue!) as {
    version?: unknown;
    route?: { workspaceId?: unknown; sessionId?: unknown };
    deliveryContext?: unknown;
  };
  if (
    (head.version !== 1 && head.version !== 2) ||
    head.route?.workspaceId !== record.trace.workspaceId ||
    head.route.sessionId !== record.trace.sessionId ||
    JSON.stringify(parseKafkaDeliveryContext(head.deliveryContext)) !==
      JSON.stringify(record.deliveryContext)
  )
    throw new Error('Kafka delivery scope mismatch');
  return record;
}

export function kafkaRecordBytes(record: KafkaStateRecord): number {
  return Buffer.byteLength(record.key) + Buffer.byteLength(record.value) + 128;
}

/** Multiple bounded Produce batches, ONE caller-owned Kafka transaction. */
export async function sendKafkaStateRecords(
  transaction: Transaction,
  topic: string,
  records: readonly KafkaStateRecord[],
  assertOwner: () => void,
  maxBatchBytes = 512 * 1024,
  partition?: number,
): Promise<string | undefined> {
  let finalOffset: string | undefined;
  for (let start = 0; start < records.length; ) {
    const messages: Array<{ key: string; value: string; partition?: number }> = [];
    let bytes = 0;
    while (start < records.length && messages.length < 500) {
      const entry = records[start]!;
      const size = kafkaRecordBytes(entry);
      if (size > maxBatchBytes)
        throw new Error(
          `Kafka exporter record size budget exceeded: kind=record actual=${size} limit=${maxBatchBytes}`,
        );
      if (messages.length > 0 && bytes + size > maxBatchBytes) break;
      messages.push({ ...entry, ...(partition === undefined ? {} : { partition }) });
      start++;
      bytes += size;
    }
    assertOwner();
    const sent = await transaction.send({ topic, messages });
    assertOwner();
    if (partition !== undefined) {
      const base = sent.find((entry) => entry.partition === partition)?.baseOffset;
      if (base === undefined || !/^(0|[1-9][0-9]*)$/u.test(base))
        throw new Error('Kafka state commit offset missing');
      finalOffset = (BigInt(base) + BigInt(messages.length) - 1n).toString();
    }
  }
  return finalOffset;
}
