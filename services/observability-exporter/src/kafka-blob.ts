// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { TextDecoder } from 'node:util';

export type KafkaStateRecord = { key: string; value: string };
export type KafkaBlobValue =
  | { kind: 'inline'; value: unknown }
  | { kind: 'chunks'; keyPrefix: string; count: number; bytes: number; sha256: string };

const MIB = 1024 * 1024;
const DEFAULT_ASSEMBLY_BYTES = 32 * MIB;
const MAX_ASSEMBLY_BYTES = 64 * MIB;
const DEFAULT_RECORD_BYTES = 512 * 1024;
const RAW_CHUNK_BYTES = 48_000;
// Also bound metadata/key allocations when a caller chooses very small records.
const MAX_CHUNKS = 65_536;
const MAX_PREFIX_BYTES = 4096;
// Conservative allowance for a Kafka record and its batch framing, without headers.
// The caller still budgets the enclosing head, headers and entire transaction.
const RECORD_FRAMING_BYTES = 128;
const CHUNK_ENVELOPE_BYTES = Buffer.byteLength(JSON.stringify({ version: 1, data: '' }));

function fail(message: string): never {
  throw new Error(`Invalid Kafka blob: ${message}`);
}

function limit(value: number | undefined, fallback: number, name: string, minimum = 1): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < minimum || result > MAX_ASSEMBLY_BYTES) fail(name);
  return result;
}

function prefix(value: unknown): asserts value is string {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > MAX_PREFIX_BYTES ||
    Buffer.byteLength(value) > MAX_PREFIX_BYTES ||
    Array.from(value).some((char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127) ||
    Buffer.from(value).toString('utf8') !== value
  )
    fail('key prefix');
}

function json(value: unknown, maxBytes: number): string {
  const serialized = JSON.stringify(value);
  if (serialized === undefined || Buffer.byteLength(serialized) > maxBytes)
    fail('JSON assembly size');
  return serialized;
}

function manifest(blob: unknown, maxBytes: number): KafkaBlobValue {
  if (!blob || typeof blob !== 'object' || Array.isArray(blob)) fail('manifest');
  const b = blob as Record<string, unknown>;
  if (b.kind === 'inline' && Object.hasOwn(b, 'value')) {
    json(b.value, maxBytes);
    return b as KafkaBlobValue;
  }
  if (b.kind !== 'chunks') fail('kind');
  prefix(b.keyPrefix);
  if (
    typeof b.bytes !== 'number' ||
    !Number.isSafeInteger(b.bytes) ||
    b.bytes < 1 ||
    b.bytes > maxBytes
  ) {
    fail('assembly bytes');
  }
  if (
    typeof b.count !== 'number' ||
    !Number.isSafeInteger(b.count) ||
    b.count < 1 ||
    b.count > MAX_CHUNKS ||
    b.count > b.bytes ||
    b.bytes > b.count * RAW_CHUNK_BYTES
  )
    fail('chunk count');
  if (typeof b.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(b.sha256)) fail('sha256');
  return b as KafkaBlobValue;
}

function fits(key: string, value: string, maxBytes: number): boolean {
  return Buffer.byteLength(key) + Buffer.byteLength(value) + RECORD_FRAMING_BYTES <= maxBytes;
}

export function kafkaBlobKeys(
  blob: unknown,
  expectedPrefix: string,
  maxAssemblyBytes?: number,
): string[] {
  const maxBytes = limit(maxAssemblyBytes, DEFAULT_ASSEMBLY_BYTES, 'maxAssemblyBytes');
  prefix(expectedPrefix);
  const b = manifest(blob, maxBytes);
  if (b.kind === 'inline') return [];
  if (b.keyPrefix !== expectedPrefix) fail('unexpected key prefix');
  return Array.from({ length: b.count }, (_, index) => `${b.keyPrefix}:${index}`);
}

export function encodeKafkaBlob(
  value: unknown,
  keyPrefix: string,
  options: { maxRecordBytes?: number; maxAssemblyBytes?: number; inlineBytes?: number } = {},
): { blob: KafkaBlobValue; records: KafkaStateRecord[] } {
  prefix(keyPrefix);
  const maxBytes = limit(options.maxAssemblyBytes, DEFAULT_ASSEMBLY_BYTES, 'maxAssemblyBytes');
  const maxRecord = limit(options.maxRecordBytes, DEFAULT_RECORD_BYTES, 'maxRecordBytes');
  const inlineBytes = limit(options.inlineBytes, 64 * 1024, 'inlineBytes', 0);
  const serialized = json(value, maxBytes);
  const bytes = Buffer.byteLength(serialized);
  if (bytes <= inlineBytes) {
    const blob: KafkaBlobValue = { kind: 'inline', value: JSON.parse(serialized) };
    if (fits(keyPrefix, JSON.stringify(blob), maxRecord)) return { blob, records: [] };
  }
  // Reserve the longest possible index before selecting a payload size.
  const available =
    maxRecord -
    RECORD_FRAMING_BYTES -
    Buffer.byteLength(`${keyPrefix}:${MAX_CHUNKS - 1}`) -
    CHUNK_ENVELOPE_BYTES;
  const chunkBytes = Math.min(RAW_CHUNK_BYTES, Math.floor(available / 4) * 3);
  if (chunkBytes < 1) fail('record budget');
  const count = Math.ceil(bytes / chunkBytes);
  if (count > MAX_CHUNKS) fail('chunk count');
  const data = Buffer.from(serialized, 'utf8');
  const blob: KafkaBlobValue = {
    kind: 'chunks',
    keyPrefix,
    count,
    bytes,
    sha256: createHash('sha256').update(data).digest('hex'),
  };
  if (!fits(keyPrefix, JSON.stringify(blob), maxRecord)) fail('manifest record budget');
  const records: KafkaStateRecord[] = [];
  for (let index = 0; index < count; index++) {
    const key = `${keyPrefix}:${index}`;
    const value = JSON.stringify({
      version: 1,
      data: data.subarray(index * chunkBytes, (index + 1) * chunkBytes).toString('base64'),
    });
    if (!fits(key, value, maxRecord)) fail('chunk record budget');
    records.push({ key, value });
  }
  return { blob, records };
}

export function decodeKafkaBlob(
  blob: unknown,
  values: readonly (string | null)[],
  options: { maxAssemblyBytes?: number; maxRecordBytes?: number } = {},
): unknown {
  const maxBytes = limit(options.maxAssemblyBytes, DEFAULT_ASSEMBLY_BYTES, 'maxAssemblyBytes');
  const maxRecord = limit(options.maxRecordBytes, DEFAULT_RECORD_BYTES, 'maxRecordBytes');
  const b = manifest(blob, maxBytes);
  if (!Array.isArray(values)) fail('chunk values');
  if (b.kind === 'inline') {
    if (values.length !== 0 || !fits('', JSON.stringify(b), maxRecord)) fail('inline record');
    return JSON.parse(json(b.value, maxBytes));
  }
  if (!fits(b.keyPrefix, JSON.stringify(b), maxRecord)) fail('manifest record budget');
  if (values.length !== b.count) fail('chunk count mismatch');
  const data = Buffer.alloc(b.bytes);
  let offset = 0;
  for (let index = 0; index < b.count; index++) {
    const value = values[index];
    if (typeof value !== 'string' || !fits(`${b.keyPrefix}:${index}`, value, maxRecord))
      fail('missing or oversized chunk');
    const chunk: unknown = JSON.parse(value);
    if (!chunk || typeof chunk !== 'object' || Array.isArray(chunk)) fail('chunk envelope');
    const c = chunk as Record<string, unknown>;
    if (
      c.version !== 1 ||
      typeof c.data !== 'string' ||
      c.data.length === 0 ||
      c.data.length > 4 * Math.ceil(RAW_CHUNK_BYTES / 3)
    )
      fail('chunk envelope');
    // Buffer base64 decoding is permissive; roundtrip enforces padding, alphabet and pad bits.
    const part = Buffer.from(c.data, 'base64');
    if (part.toString('base64') !== c.data || part.length === 0 || part.length > RAW_CHUNK_BYTES)
      fail('canonical base64');
    if (offset + part.length > b.bytes) fail('assembly bytes mismatch');
    part.copy(data, offset);
    offset += part.length;
  }
  if (offset !== b.bytes) fail('assembly bytes mismatch');
  if (createHash('sha256').update(data).digest('hex') !== b.sha256) fail('sha256 mismatch');
  // Decode only the whole assembly; JSON escapes preserve lone UTF-16 surrogates.
  return JSON.parse(new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(data));
}
