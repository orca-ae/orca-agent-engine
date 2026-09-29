// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type { KafkaMessage } from 'kafkajs';
import type { Event } from '@orca/transcript-store-types';
import { eventIdentityKey, isBoundedAgentEventId } from './event-identity.js';
import {
  CanonicalProjectionError,
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
  type CanonicalProjectionState,
} from './projector.js';
import { parseKafkaDeliveryContext, parseKafkaProjectedTrace } from './kafka-validation.js';
import { TRACE_SAMPLING_VERSION } from './sampling.js';
import type { CaptureMode, PinnedDeliveryContext, ProjectedTrace } from './types.js';

export interface KafkaSessionRoute {
  topic: string;
  workspaceId: string;
  sessionId: string;
}
export interface KafkaCheckpoint {
  version: 1;
  route: KafkaSessionRoute;
  nextOffset: string;
  reducer: CanonicalProjectionState;
  identities: Array<{ key: string; hash: string; offset: string }>;
  acceptedSourceIds: string[];
  /** null is a terminal, immutable suppression decision for this Session. */
  deliveryContext: PinnedDeliveryContext | null;
}
export interface KafkaDeliveryRecord {
  version: 1;
  trace: ProjectedTrace;
  deliveryContext: PinnedDeliveryContext;
}

export function initialKafkaCheckpoint(
  route: KafkaSessionRoute,
  deliveryContext: PinnedDeliveryContext | null,
): KafkaCheckpoint {
  return {
    version: 1,
    route,
    nextOffset: '0',
    reducer: initialCanonicalProjectionState(),
    identities: [],
    acceptedSourceIds: [],
    deliveryContext,
  };
}

export function parseKafkaCheckpoint(value: unknown, route: KafkaSessionRoute): KafkaCheckpoint {
  if (
    !record(value) ||
    value.version !== 1 ||
    !record(value.route) ||
    value.route.topic !== route.topic ||
    value.route.workspaceId !== route.workspaceId ||
    value.route.sessionId !== route.sessionId ||
    !decimal(value.nextOffset) ||
    !Array.isArray(value.identities) ||
    !Array.isArray(value.acceptedSourceIds)
  ) {
    throw new Error('invalid Kafka checkpoint');
  }
  const nextOffset = value.nextOffset;
  const identities = value.identities.map((entry: unknown) => {
    if (
      !record(entry) ||
      !digest(entry.key) ||
      !digest(entry.hash) ||
      !decimal(entry.offset) ||
      BigInt(entry.offset) >= BigInt(nextOffset)
    )
      throw new Error('invalid Kafka identity checkpoint');
    return { key: entry.key, hash: entry.hash, offset: entry.offset };
  });
  if (
    new Set(identities.map((entry) => entry.key)).size !== identities.length ||
    !value.acceptedSourceIds.every(isBoundedAgentEventId) ||
    new Set(value.acceptedSourceIds).size !== value.acceptedSourceIds.length
  ) {
    throw new Error('invalid Kafka identity checkpoint');
  }
  const reducer = parseCanonicalProjectionState(value.reducer);
  if (
    reducer.activeTurn !== null &&
    (reducer.activeTurn.workspaceId !== route.workspaceId ||
      reducer.activeTurn.sessionId !== route.sessionId)
  )
    throw new Error('Kafka checkpoint scope mismatch');
  const suppressed = reducer.sampling?.suppressed;
  if (
    suppressed != null &&
    (suppressed.workspaceId !== route.workspaceId || suppressed.sessionId !== route.sessionId)
  ) {
    throw new Error('Kafka checkpoint scope mismatch');
  }
  return {
    version: 1,
    route: { ...route },
    nextOffset,
    reducer,
    identities,
    acceptedSourceIds: [...value.acceptedSourceIds],
    deliveryContext:
      value.deliveryContext === null ? null : parseKafkaDeliveryContext(value.deliveryContext),
  };
}

export function parseKafkaDeliveryRecord(value: unknown): KafkaDeliveryRecord {
  if (!record(value) || value.version !== 1) throw new Error('invalid Kafka delivery record');
  return {
    version: 1,
    trace: parseKafkaProjectedTrace(value.trace),
    deliveryContext: parseKafkaDeliveryContext(value.deliveryContext),
  };
}

function transcriptHeader(message: KafkaMessage, name: string): string {
  const value = message.headers?.[name];
  if (Array.isArray(value)) throw new Error('ambiguous Kafka Transcript header');
  return value === undefined ? '' : value.toString();
}

/** Preserve exporter header strictness and the legacy route-check short circuit before codec I/O. */
export function validateKafkaTranscriptHeaders(
  message: KafkaMessage,
  route: KafkaSessionRoute,
): boolean {
  if (
    transcriptHeader(message, 'workspace_id') !== route.workspaceId ||
    transcriptHeader(message, 'session_id') !== route.sessionId
  )
    return false;
  for (const name of [
    'user_id',
    'id',
    'subpath',
    'produced_at',
    'produced_by',
    'kind',
    'idempotency_key',
  ]) {
    transcriptHeader(message, name);
  }
  return true;
}

export interface DecodedKafkaTranscript {
  offset: string;
  event: Event | null;
}

/** Validate the entire broker batch before skipping replay or doing asynchronous lookups. */
export function validateKafkaTranscriptOffsets(messages: readonly { offset: string }[]): void {
  let previousOffset = -1n;
  for (const message of messages) {
    if (
      !decimal(message.offset) ||
      !Number.isSafeInteger(Number(message.offset)) ||
      BigInt(message.offset) <= previousOffset
    )
      throw new Error('invalid Kafka Transcript ordering');
    previousOffset = BigInt(message.offset);
  }
}

/** Pure projection over restored Events: neither wire frames nor schema IDs enter the hash. */
export function projectKafkaEvents(
  checkpoint: KafkaCheckpoint,
  messages: readonly DecodedKafkaTranscript[],
  captureMode: CaptureMode = 'metadata_only',
): {
  checkpoint: KafkaCheckpoint;
  deliveries: KafkaDeliveryRecord[];
} {
  const next = parseKafkaCheckpoint(checkpoint, checkpoint.route);
  const identities = new Map(next.identities.map((entry) => [entry.key, entry]));
  const events: Event[] = [];
  validateKafkaTranscriptOffsets(messages);
  for (const message of messages) {
    if (BigInt(message.offset) < BigInt(next.nextOffset)) continue;
    const event = message.event;
    next.nextOffset = (BigInt(message.offset) + 1n).toString();
    if (event === null) continue;
    const key = eventIdentityKey(event.id);
    const hash = kafkaTranscriptHash(event);
    const existing = identities.get(key);
    if (existing !== undefined) {
      if (existing.hash !== hash) throw new Error('Kafka Transcript identity conflict');
      continue;
    }
    identities.set(key, { key, hash, offset: message.offset });
    events.push(event);
  }
  next.identities = [...identities.values()];
  if (next.deliveryContext === null) return { checkpoint: next, deliveries: [] };
  const context = next.deliveryContext;
  const result = reduceCanonicalEventBatch(
    next.reducer,
    events,
    new Set(next.acceptedSourceIds),
    {
      algorithmVersion: TRACE_SAMPLING_VERSION,
      bindingId: context.bindingId,
      bindingVersion: context.bindingVersion,
      sampleRate: context.sampleRate,
    },
    context.captureMode === 'raw_io' ? captureMode : 'metadata_only',
  );
  if (result.issues.length > 0) throw new CanonicalProjectionError(result.issues);
  next.reducer = result.state;
  next.acceptedSourceIds.push(...result.acceptedSourceIds);
  return {
    checkpoint: next,
    deliveries: result.completedTraces.map((trace) => ({
      version: 1,
      trace: parseKafkaProjectedTrace(trace),
      deliveryContext: context,
    })),
  };
}

/** Same v1 envelope hash as the legacy repository, excluding broker-assigned seq. */
export function kafkaTranscriptHash(event: Event): string {
  const values = {
    hashSchema: 'orca.observability.transcript-envelope-hash.v1',
    id: event.id,
    workspaceId: event.workspaceId,
    sessionId: event.sessionId,
    subpath: event.subpath,
    producedAt: event.producedAt,
    producedBy: event.producedBy,
    kind: event.kind,
    idempotencyKey: event.idempotencyKey,
    ...(event.userId === undefined ? {} : { userId: event.userId }),
    payloadSha256: createHash('sha256').update(event.payload).digest('hex'),
  };
  return createHash('sha256')
    .update(
      JSON.stringify(
        Object.fromEntries(Object.entries(values).sort(([a], [b]) => a.localeCompare(b))),
      ),
    )
    .digest('hex');
}
function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function decimal(value: unknown): value is string {
  return (
    typeof value === 'string' && /^(0|[1-9][0-9]{0,18})$/u.test(value) && BigInt(value) < 1n << 63n
  );
}
function digest(value: unknown): value is string {
  return typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
}
