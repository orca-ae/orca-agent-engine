// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { boundedAgentEventId, eventIdentityKey, isBoundedAgentEventId } from './event-identity.js';
import {
  encodeKafkaBlob,
  decodeKafkaBlob,
  kafkaBlobKeys,
  type KafkaBlobValue,
} from './kafka-blob.js';
import type { KafkaDiskIndex } from './kafka-disk-index.js';
import {
  initialKafkaCheckpoint,
  parseKafkaCheckpoint,
  projectKafkaEvents,
  validateKafkaTranscriptOffsets,
  type KafkaCheckpoint,
  type KafkaSessionRoute,
  type KafkaDeliveryRecord,
  type DecodedKafkaTranscript,
} from './kafka-state.js';
import type { CanonicalProjectionState } from './projector.js';
import type { CaptureMode, PinnedDeliveryContext } from './types.js';

type Index = Pick<KafkaDiskIndex, 'read' | 'countPrefix'>;
type RecordValue = { key: string; value: string };
type BlobOptions = Parameters<typeof encodeKafkaBlob>[2];
const LEGACY_BYTES = 524_288;
const READ_BATCH = 256;

export interface KafkaStateHead {
  version: 2;
  route: KafkaSessionRoute;
  nextOffset: string;
  deliveryContext: PinnedDeliveryContext | null;
  identityCount: number;
  acceptedCount: number;
  reducer: KafkaBlobValue;
  /** Bounded v1 evidence, not an unbounded copy of the growing v2 ledger. */
  importBaseline?: {
    fingerprint: string;
    identityCount: number;
    acceptedCount: number;
    digest: string;
    nextOffset: string;
    checkpoint: KafkaBlobValue;
  };
}
export interface LoadedKafkaState {
  head: KafkaStateHead;
  headValue: string;
  reducer: CanonicalProjectionState;
}
export interface KafkaStateProjection {
  head: KafkaStateHead;
  headValue: string;
  records: RecordValue[];
  deliveries: KafkaDeliveryRecord[];
}
interface Identity {
  key: string;
  hash: string;
  offset: string;
  visibleOffset: string;
  imported: boolean;
}
interface Accepted {
  id: string;
  visibleOffset: string;
  imported: boolean;
}

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}
/** All auxiliary keys have disjoint leading namespaces; the legacy key remains the head. */
export function kafkaStateKeys(checkpointKey: string) {
  const scope = hash(checkpointKey);
  return {
    identityPrefix: `I/${scope}/`,
    acceptedPrefix: `A/${scope}/`,
    reducerPrefix: `R/${scope}/state/`,
    baselinePrefix: `R/${scope}/import/`,
    deliveryPrefix: `D/${scope}/`,
    identity: (key: string) => `I/${scope}/${key}`,
    accepted: (id: string) => `A/${scope}/${eventIdentityKey(id)}`,
  };
}
function decimal(value: unknown): value is string {
  return (
    typeof value === 'string' && /^(0|[1-9][0-9]{0,18})$/u.test(value) && BigInt(value) < 1n << 63n
  );
}
function count(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}
function digest(value: unknown): value is string {
  return typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function parseKafkaStateHead(raw: string, route: KafkaSessionRoute): KafkaStateHead {
  if (Buffer.byteLength(raw) > LEGACY_BYTES) throw new Error('Kafka state head too large');
  const value: unknown = JSON.parse(raw);
  if (
    !object(value) ||
    value.version !== 2 ||
    !count(value.identityCount) ||
    !count(value.acceptedCount)
  )
    throw new Error('invalid Kafka state head');
  // Reuse the existing scope/pin/offset parser rather than introducing a second contract.
  const validated = parseKafkaCheckpoint(
    {
      ...value,
      version: 1,
      reducer: initialKafkaCheckpoint(route, null).reducer,
      identities: [],
      acceptedSourceIds: [],
    },
    route,
  );
  let importBaseline: KafkaStateHead['importBaseline'];
  if (value.importBaseline !== undefined) {
    const baseline = value.importBaseline;
    if (
      !object(baseline) ||
      !digest(baseline.fingerprint) ||
      !digest(baseline.digest) ||
      !count(baseline.identityCount) ||
      !count(baseline.acceptedCount) ||
      !decimal(baseline.nextOffset) ||
      baseline.identityCount > value.identityCount ||
      baseline.acceptedCount > value.acceptedCount ||
      BigInt(baseline.nextOffset) > BigInt(validated.nextOffset)
    )
      throw new Error('invalid Kafka import baseline');
    importBaseline = {
      fingerprint: baseline.fingerprint,
      identityCount: baseline.identityCount,
      acceptedCount: baseline.acceptedCount,
      digest: baseline.digest,
      nextOffset: baseline.nextOffset,
      checkpoint: baseline.checkpoint as KafkaBlobValue,
    };
  }
  return {
    version: 2,
    route: validated.route,
    nextOffset: validated.nextOffset,
    deliveryContext: validated.deliveryContext,
    identityCount: value.identityCount,
    acceptedCount: value.acceptedCount,
    reducer: value.reducer as KafkaBlobValue,
    ...(importBaseline === undefined ? {} : { importBaseline }),
  };
}

function identityRecords(
  checkpoint: KafkaCheckpoint,
  checkpointKey: string,
  imported: boolean,
): RecordValue[] {
  const keys = kafkaStateKeys(checkpointKey);
  return [
    ...checkpoint.identities.map((entry) => ({
      key: keys.identity(entry.key),
      value: JSON.stringify({
        ...entry,
        visibleOffset: imported ? checkpoint.nextOffset : (BigInt(entry.offset) + 1n).toString(),
        imported,
      } satisfies Identity),
    })),
    ...checkpoint.acceptedSourceIds.map((id) => ({
      key: keys.accepted(id),
      value: JSON.stringify({
        id,
        visibleOffset: checkpoint.nextOffset,
        imported,
      } satisfies Accepted),
    })),
  ];
}
function finish(
  head: KafkaStateHead,
  checkpointKey: string,
  records: RecordValue[],
  deliveries: KafkaDeliveryRecord[],
): KafkaStateProjection {
  const headValue = JSON.stringify(head);
  parseKafkaStateHead(headValue, head.route);
  return {
    head,
    headValue,
    records: [...records, { key: checkpointKey, value: headValue }],
    deliveries,
  };
}

/** Caller must quiesce v1 writers and commit all records plus source offsets in ONE transaction. */
export function migrateKafkaCheckpoint(
  raw: string,
  route: KafkaSessionRoute,
  checkpointKey: string,
  options?: BlobOptions,
): KafkaStateProjection {
  if (Buffer.byteLength(raw) > LEGACY_BYTES)
    throw new Error('Kafka v1 checkpoint exceeds migration limit');
  const checkpoint = parseKafkaCheckpoint(JSON.parse(raw), route);
  const keys = kafkaStateKeys(checkpointKey);
  const ledger = identityRecords(checkpoint, checkpointKey, true);
  const reducer = encodeKafkaBlob(checkpoint.reducer, keys.reducerPrefix, options);
  // Keeping only the bounded original evidence permits exact baseline verification via point reads,
  // even after millions of new ledger entries; countPrefix alone cannot authenticate import content.
  const evidence = encodeKafkaBlob(raw, keys.baselinePrefix, options);
  const head: KafkaStateHead = {
    version: 2,
    route: checkpoint.route,
    nextOffset: checkpoint.nextOffset,
    deliveryContext: checkpoint.deliveryContext,
    identityCount: checkpoint.identities.length,
    acceptedCount: checkpoint.acceptedSourceIds.length,
    reducer: reducer.blob,
    importBaseline: {
      fingerprint: hash(raw),
      digest: hash(JSON.stringify(ledger)),
      identityCount: checkpoint.identities.length,
      acceptedCount: checkpoint.acceptedSourceIds.length,
      nextOffset: checkpoint.nextOffset,
      checkpoint: evidence.blob,
    },
  };
  return finish(head, checkpointKey, [...ledger, ...evidence.records, ...reducer.records], []);
}

/** Fresh scope only; the runtime must authenticate absence before calling this. */
export function initialKafkaState(
  route: KafkaSessionRoute,
  deliveryContext: PinnedDeliveryContext | null,
  checkpointKey: string,
  options?: BlobOptions,
): KafkaStateProjection {
  const checkpoint = initialKafkaCheckpoint(route, deliveryContext);
  const reducer = encodeKafkaBlob(
    checkpoint.reducer,
    kafkaStateKeys(checkpointKey).reducerPrefix,
    options,
  );
  return finish(
    {
      version: 2,
      route,
      nextOffset: '0',
      deliveryContext,
      identityCount: 0,
      acceptedCount: 0,
      reducer: reducer.blob,
    },
    checkpointKey,
    reducer.records,
    [],
  );
}

/** Bounded preflight during I/O; the shared codec still owns final digest/JSON validation.
 * One untrusted record is unavoidable with the index API. Never prefetch an entire
 * manifest's alleged chunks before checking them against its declared assembly.
 */
export async function readKafkaStateBlob(
  index: Pick<KafkaDiskIndex, 'read'>,
  blob: KafkaBlobValue,
  prefix: string,
  expected?: RecordValue,
  options?: BlobOptions,
): Promise<unknown> {
  const keys = kafkaBlobKeys(blob, prefix, options?.maxAssemblyBytes);
  const values: (string | null)[] = [];
  await index.read([], expected);
  if (blob.kind === 'chunks') {
    const maxRecordBytes = options?.maxRecordBytes ?? LEGACY_BYTES;
    if (
      !Number.isSafeInteger(maxRecordBytes) ||
      maxRecordBytes < 1 ||
      maxRecordBytes > 64 * 1024 * 1024
    )
      throw new Error('Invalid Kafka blob: maxRecordBytes');
    const envelopeBytes = Buffer.byteLength(JSON.stringify({ version: 1, data: '' }));
    // Padding costs at most four additional characters per independently encoded chunk.
    const encodedBudget = 4 * Math.ceil(blob.bytes / 3) + (4 + envelopeBytes) * blob.count;
    let rawBytes = 0;
    let encodedBytes = 0;
    for (let start = 0; start < keys.length; ) {
      const batchSize = Math.max(
        1,
        Math.min(
          READ_BATCH,
          Math.floor(Math.min(1024 * 1024, encodedBudget - encodedBytes) / maxRecordBytes),
        ),
      );
      const batchKeys = keys.slice(start, start + batchSize);
      const batch = await index.read(batchKeys, expected);
      if (batch.length !== batchKeys.length)
        throw new Error('Invalid Kafka blob: chunk count mismatch');
      for (const value of batch) {
        // Bound before JSON.parse/base64 allocation, including hostile extra envelope fields.
        const remainingBytes = blob.bytes - rawBytes;
        const maxEnvelopeBytes =
          envelopeBytes + 4 * Math.ceil(Math.min(48_000, remainingBytes) / 3);
        if (typeof value !== 'string' || value.length > Math.min(maxRecordBytes, maxEnvelopeBytes))
          throw new Error('Invalid Kafka blob: missing or oversized chunk');
        encodedBytes += Buffer.byteLength(value);
        if (encodedBytes > encodedBudget)
          throw new Error('Invalid Kafka blob: encoded assembly size');
        const chunk: unknown = JSON.parse(value);
        if (
          !object(chunk) ||
          chunk.version !== 1 ||
          typeof chunk.data !== 'string' ||
          value !== JSON.stringify({ version: 1, data: chunk.data })
        )
          throw new Error('Invalid Kafka blob: canonical chunk envelope');
        const part = Buffer.from(chunk.data, 'base64');
        if (part.length === 0 || part.toString('base64') !== chunk.data)
          throw new Error('Invalid Kafka blob: canonical base64');
        rawBytes += part.length;
        if (rawBytes > blob.bytes || rawBytes + (blob.count - values.length - 1) > blob.bytes)
          throw new Error('Invalid Kafka blob: assembly bytes mismatch');
        values.push(value);
      }
      start += batchKeys.length;
    }
  }
  await index.read([], expected);
  return decodeKafkaBlob(blob, values, options);
}
/** The index must already be restored through the owner's authenticated barrier. */
export async function loadKafkaState(
  raw: string,
  route: KafkaSessionRoute,
  checkpointKey: string,
  index: Index,
  options?: BlobOptions,
): Promise<LoadedKafkaState> {
  const head = parseKafkaStateHead(raw, route);
  const reducer = await readKafkaStateBlob(
    index,
    head.reducer,
    kafkaStateKeys(checkpointKey).reducerPrefix,
    { key: checkpointKey, value: raw },
    options,
  );
  const validated = parseKafkaCheckpoint(
    { ...head, version: 1, reducer, identities: [], acceptedSourceIds: [] },
    route,
  );
  return { head, headValue: raw, reducer: validated.reducer };
}

/** Full import authentication is bounded by the 512KiB v1 limit, never by total v2 history.
 * countPrefix checks cardinality only; it deliberately does not claim a digest over native v2 history.
 */
export async function validateKafkaState(
  state: LoadedKafkaState,
  checkpointKey: string,
  index: Index,
  options?: BlobOptions,
): Promise<void> {
  const { head, headValue } = state;
  const expected = { key: checkpointKey, value: headValue };
  const keys = kafkaStateKeys(checkpointKey);
  await index.read([], expected);
  const identities = await index.countPrefix(keys.identityPrefix);
  const accepted = await index.countPrefix(keys.acceptedPrefix);
  if (identities !== head.identityCount || accepted !== head.acceptedCount)
    throw new Error('incomplete Kafka state ledger');
  const baseline = head.importBaseline;
  if (baseline !== undefined) {
    const raw = await readKafkaStateBlob(
      index,
      baseline.checkpoint,
      keys.baselinePrefix,
      expected,
      options,
    );
    if (
      typeof raw !== 'string' ||
      Buffer.byteLength(raw) > LEGACY_BYTES ||
      hash(raw) !== baseline.fingerprint
    )
      throw new Error('invalid Kafka import fingerprint');
    const old = parseKafkaCheckpoint(JSON.parse(raw), head.route);
    const ledger = identityRecords(old, checkpointKey, true);
    if (
      old.identities.length !== baseline.identityCount ||
      old.acceptedSourceIds.length !== baseline.acceptedCount ||
      old.nextOffset !== baseline.nextOffset ||
      hash(JSON.stringify(ledger)) !== baseline.digest ||
      JSON.stringify(old.deliveryContext) !== JSON.stringify(head.deliveryContext)
    )
      throw new Error('invalid Kafka import baseline');
    for (let start = 0; start < ledger.length; start += READ_BATCH) {
      const batch = ledger.slice(start, start + READ_BATCH);
      const values = await index.read(
        batch.map((entry) => entry.key),
        expected,
      );
      if (batch.some((entry, i) => values[i] !== entry.value))
        throw new Error('incomplete Kafka import ledger');
    }
  }
  await index.read([], expected);
}

/** Normalize raw IDs/references once. Pending IDs are ALREADY normalized reducer state. */
export function collectKafkaMembership(
  reducer: CanonicalProjectionState,
  messages: readonly DecodedKafkaTranscript[],
) {
  const identities = new Set<string>();
  const accepted = new Set(reducer.pendingInputs.map((input) => input.eventId));
  for (const { event } of messages) {
    if (event === null) continue;
    identities.add(eventIdentityKey(event.id));
    const id = boundedAgentEventId(event.id);
    if (id !== undefined) accepted.add(id);
    if (event.kind === 'session.user_event_processed') {
      let payload: unknown;
      try {
        payload = JSON.parse(Buffer.from(event.payload).toString('utf8'));
      } catch {
        continue;
      }
      const reference = object(payload) ? boundedAgentEventId(payload.user_event_id) : undefined;
      if (reference !== undefined) accepted.add(reference);
    }
  }
  return { identities: [...identities], accepted: [...accepted] };
}

export async function projectKafkaState(
  state: LoadedKafkaState,
  messages: readonly DecodedKafkaTranscript[],
  checkpointKey: string,
  index: Index,
  options?: BlobOptions,
  captureMode: CaptureMode = 'metadata_only',
): Promise<KafkaStateProjection> {
  validateKafkaTranscriptOffsets(messages);
  const { head, headValue } = state;
  if (JSON.stringify(parseKafkaStateHead(headValue, head.route)) !== JSON.stringify(head))
    throw new Error('Kafka state head mismatch');
  const active = messages.filter((message) => BigInt(message.offset) >= BigInt(head.nextOffset));
  const candidates = collectKafkaMembership(state.reducer, active);
  const keys = kafkaStateKeys(checkpointKey);
  const requested = [
    ...candidates.identities.map(keys.identity),
    ...candidates.accepted.map(keys.accepted),
  ];
  const values: (string | null)[] = [];
  const expected = { key: checkpointKey, value: headValue };
  await index.read([], expected);
  for (let start = 0; start < requested.length; start += READ_BATCH) {
    const keys = requested.slice(start, start + READ_BATCH);
    const rows = await index.read(keys, expected);
    if (!Array.isArray(rows) || rows.length !== keys.length)
      throw new Error('incomplete Kafka membership read');
    values.push(...rows);
  }
  const identities: KafkaCheckpoint['identities'] = [];
  const acceptedSourceIds: string[] = [];
  for (let i = 0; i < values.length; i++) {
    const raw = values[i];
    if (raw === null) continue;
    const entry: unknown = JSON.parse(raw!);
    if (!object(entry) || !decimal(entry.visibleOffset) || typeof entry.imported !== 'boolean')
      throw new Error('invalid Kafka membership entry');
    // A future record indicates an uncertified mixed index view. Never treat it as history or overwrite it.
    if (BigInt(entry.visibleOffset) > BigInt(head.nextOffset))
      throw new Error('future Kafka membership entry');
    if (i < candidates.identities.length) {
      if (
        entry.key !== candidates.identities[i] ||
        !digest(entry.key) ||
        !digest(entry.hash) ||
        !decimal(entry.offset) ||
        BigInt(entry.offset) >= BigInt(entry.visibleOffset)
      )
        throw new Error('invalid Kafka identity entry');
      identities.push({ key: entry.key, hash: entry.hash, offset: entry.offset });
    } else {
      if (
        entry.id !== candidates.accepted[i - candidates.identities.length] ||
        !isBoundedAgentEventId(entry.id)
      )
        throw new Error('invalid Kafka accepted entry');
      acceptedSourceIds.push(entry.id);
    }
  }
  const result = projectKafkaEvents(
    {
      version: 1,
      route: head.route,
      nextOffset: head.nextOffset,
      reducer: state.reducer,
      deliveryContext: head.deliveryContext,
      identities,
      acceptedSourceIds,
    },
    active,
    captureMode,
  );
  const added = {
    ...result.checkpoint,
    identities: result.checkpoint.identities.slice(identities.length),
    acceptedSourceIds: result.checkpoint.acceptedSourceIds.slice(acceptedSourceIds.length),
  };
  const reducer = encodeKafkaBlob(result.checkpoint.reducer, keys.reducerPrefix, options);
  await index.read([], expected);
  return finish(
    {
      ...head,
      nextOffset: result.checkpoint.nextOffset,
      reducer: reducer.blob,
      identityCount: head.identityCount + added.identities.length,
      acceptedCount: head.acceptedCount + added.acceptedSourceIds.length,
    },
    checkpointKey,
    [...identityRecords(added, checkpointKey, false), ...reducer.records],
    result.deliveries,
  );
}
