// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from '@orca/transcript-store-types';
import type { KafkaMessage } from 'kafkajs';
import { describe, expect, it } from 'vitest';
import {
  initialKafkaCheckpoint,
  kafkaTranscriptHash,
  parseKafkaCheckpoint,
  projectKafkaEvents,
  validateKafkaTranscriptHeaders,
  validateKafkaTranscriptOffsets,
} from '../../src/kafka-state.js';
import { hashTranscriptEnvelope } from '../../src/persistence.js';
import type { PinnedDeliveryContext } from '../../src/types.js';
import {
  completedPrimaryTurnEvents,
  event,
  SESSION_ID,
  TRANSCRIPT_SECRET,
  WORKSPACE_ID,
} from '../support/events.js';

const route = {
  topic: `orca.${WORKSPACE_ID}.sessions.${SESSION_ID}.events`,
  workspaceId: WORKSPACE_ID,
  sessionId: SESSION_ID,
};
const context: PinnedDeliveryContext = {
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
};
function decoded(event: Event) {
  return { offset: String(event.seq), event };
}

describe('Kafka-only durable reducer', () => {
  it('restores an open turn and publishes once without retaining payload', () => {
    const messages = completedPrimaryTurnEvents().map(decoded);
    const initial = initialKafkaCheckpoint(route, context);
    const before = JSON.stringify(initial);
    const first = projectKafkaEvents(initial, messages.slice(0, 4));
    expect(JSON.stringify(initial)).toBe(before);
    expect(first.deliveries).toEqual([]);
    const restored = parseKafkaCheckpoint(JSON.parse(JSON.stringify(first.checkpoint)), route);
    const completed = projectKafkaEvents(restored, messages.slice(4));
    expect(completed.deliveries).toHaveLength(1);
    expect(completed.checkpoint.acceptedSourceIds).toEqual(['evt_user_turn']);
    expect(JSON.stringify(completed)).not.toContain(TRANSCRIPT_SECRET);
    expect(projectKafkaEvents(completed.checkpoint, messages).deliveries).toEqual([]);
  });

  it('preserves duplicate identity and conflict detection across checkpoints', () => {
    const source = completedPrimaryTurnEvents()[0]!;
    const first = projectKafkaEvents(initialKafkaCheckpoint(route, context), [decoded(source)]);
    const restored = parseKafkaCheckpoint(JSON.parse(JSON.stringify(first.checkpoint)), route);
    const duplicate = decoded({ ...source, seq: 10 });
    expect(projectKafkaEvents(restored, [duplicate]).checkpoint.identities).toHaveLength(1);
    expect(() =>
      projectKafkaEvents(restored, [
        decoded({ ...source, seq: 10, payload: Buffer.from('changed') }),
      ]),
    ).toThrow('identity conflict');
    expect(restored.nextOffset).toBe('2');
  });

  it('retains accepted identity after turn closure and suppresses replayed acceptance markers', () => {
    const completed = projectKafkaEvents(
      initialKafkaCheckpoint(route, context),
      completedPrimaryTurnEvents().map(decoded),
    );
    const replay = event(
      20,
      'session.user_event_processed',
      { user_event_id: 'evt_user_turn' },
      { id: 'evt_reaccept' },
    );
    const result = projectKafkaEvents(completed.checkpoint, [decoded(replay)]);
    expect(result.deliveries).toEqual([]);
    expect(result.checkpoint.reducer.activeTurn).toBeNull();
  });

  it('pins sampling across batches and persists a sampled-out watermark', () => {
    const messages = completedPrimaryTurnEvents().map(decoded);
    const first = projectKafkaEvents(
      initialKafkaCheckpoint(route, { ...context, sampleRate: 0 }),
      messages.slice(0, 4),
    );
    const result = projectKafkaEvents(first.checkpoint, messages.slice(4));
    expect(result.deliveries).toEqual([]);
    expect(result.checkpoint.reducer.sampling?.suppressed?.turnCount).toBe('1');
    expect(JSON.stringify(result)).not.toContain('claude-test');
    expect(() =>
      projectKafkaEvents({ ...first.checkpoint, deliveryContext: context }, messages.slice(4)),
    ).toThrow();
  });

  it('fails a malformed acceptance atomically and skips forged route headers', () => {
    const initial = initialKafkaCheckpoint(route, context);
    expect(() =>
      projectKafkaEvents(initial, [decoded(event(1, 'session.user_event_processed'))]),
    ).toThrow();
    expect(initial.nextOffset).toBe('0');
    const poison = {
      headers: { workspace_id: 'ws_foreign', session_id: route.sessionId },
    } as KafkaMessage;
    expect(validateKafkaTranscriptHeaders(poison, route)).toBe(false);
    const result = projectKafkaEvents(initial, [{ offset: '1', event: null }]);
    expect(result.checkpoint.nextOffset).toBe('2');
    expect(result.checkpoint.identities).toEqual([]);
  });

  it('validates restored scopes and identity ledgers', () => {
    const checkpoint = projectKafkaEvents(
      initialKafkaCheckpoint(route, context),
      completedPrimaryTurnEvents().slice(0, 4).map(decoded),
    ).checkpoint;
    expect(() => parseKafkaCheckpoint(checkpoint, { ...route, sessionId: 'ses_other' })).toThrow();
    expect(() =>
      parseKafkaCheckpoint(
        { ...checkpoint, identities: [...checkpoint.identities, checkpoint.identities[0]] },
        route,
      ),
    ).toThrow();
    expect(() =>
      parseKafkaCheckpoint({ ...checkpoint, acceptedSourceIds: ['private data'] }, route),
    ).toThrow();
    expect(() => validateKafkaTranscriptOffsets([{ offset: '9007199254740992' }])).toThrow(
      'ordering',
    );
  });

  it('rejects invalid or non-monotonic broker offsets without advancing state', () => {
    const initial = initialKafkaCheckpoint(route, context);
    const message = decoded(event(2, 'agent.message'));
    for (const offset of ['-1', '01', '9007199254740992']) {
      expect(() => projectKafkaEvents(initial, [{ ...message, offset }])).toThrow('ordering');
    }
    expect(() => projectKafkaEvents(initial, [message, { ...message, offset: '1' }])).toThrow(
      'ordering',
    );
    expect(initial.nextOffset).toBe('0');
  });

  it('rejects a sampled-out checkpoint watermark from a different Session', () => {
    const checkpoint = projectKafkaEvents(
      initialKafkaCheckpoint(route, { ...context, sampleRate: 0 }),
      completedPrimaryTurnEvents().map(decoded),
    ).checkpoint;
    checkpoint.reducer.sampling!.suppressed!.sessionId = 'ses_foreign';
    expect(() => parseKafkaCheckpoint(checkpoint, route)).toThrow('scope mismatch');
  });

  it('uses the existing content-free v1 envelope hash', () => {
    for (const source of completedPrimaryTurnEvents()) {
      expect(kafkaTranscriptHash(source)).toBe(hashTranscriptEnvelope(source));
      expect(kafkaTranscriptHash({ ...source, seq: source.seq + 100 })).toBe(
        kafkaTranscriptHash(source),
      );
    }
  });
});
