// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { eventToKafkaMessage, kafkaMessageToEvent } from '../../src/kafka/serialize.js';
import type { Event } from '../../src/types.js';
import type { KafkaMessage } from 'kafkajs';

describe('Event ↔ Kafka message', () => {
  const baseEvent: Event = {
    id: '01933d4a-1234-7000-8000-abcdef000001',
    workspaceId: 'ws_x',
    sessionId: 'ses_y',
    subpath: 'subagents/a1',
    seq: 0,
    producedAt: '2026-05-01T12:34:56Z',
    producedBy: 'harness',
    kind: 'agent.tool_use',
    payload: new Uint8Array([0xde, 0xad, 0xbe, 0xef]),
    idempotencyKey: 'idem-1',
    userId: 'user_kafka_1',
  };

  it('encodes payload as Kafka message value and metadata as headers', () => {
    const msg = eventToKafkaMessage(baseEvent);
    expect(msg.key).toBe(baseEvent.id);
    expect(msg.value).toEqual(Buffer.from(baseEvent.payload));
    const headers = msg.headers ?? {};
    expect((headers['id'] as Buffer).toString()).toBe(baseEvent.id);
    expect((headers['workspace_id'] as Buffer).toString()).toBe(baseEvent.workspaceId);
    expect((headers['session_id'] as Buffer).toString()).toBe(baseEvent.sessionId);
    expect((headers['subpath'] as Buffer).toString()).toBe(baseEvent.subpath);
    expect((headers['produced_at'] as Buffer).toString()).toBe(baseEvent.producedAt);
    expect((headers['produced_by'] as Buffer).toString()).toBe(baseEvent.producedBy);
    expect((headers['kind'] as Buffer).toString()).toBe(baseEvent.kind);
    expect((headers['idempotency_key'] as Buffer).toString()).toBe(baseEvent.idempotencyKey);
    expect((headers['user_id'] as Buffer).toString()).toBe(baseEvent.userId);
  });

  it('round-trips Event ↔ Kafka', () => {
    const msg = eventToKafkaMessage(baseEvent);
    const reconstructed = kafkaMessageToEvent({
      offset: '7',
      key: msg.key as Buffer,
      value: msg.value as Buffer,
      headers: msg.headers as KafkaMessage['headers'],
      timestamp: '0',
      attributes: 0,
      size: 0,
    } as KafkaMessage);
    expect(reconstructed.id).toBe(baseEvent.id);
    expect(reconstructed.workspaceId).toBe(baseEvent.workspaceId);
    expect(reconstructed.sessionId).toBe(baseEvent.sessionId);
    expect(reconstructed.subpath).toBe(baseEvent.subpath);
    expect(reconstructed.kind).toBe(baseEvent.kind);
    expect(reconstructed.producedBy).toBe(baseEvent.producedBy);
    expect(reconstructed.producedAt).toBe(baseEvent.producedAt);
    expect(reconstructed.idempotencyKey).toBe(baseEvent.idempotencyKey);
    expect(reconstructed.userId).toBe(baseEvent.userId);
    expect(Buffer.from(reconstructed.payload)).toEqual(Buffer.from(baseEvent.payload));
    expect(reconstructed.seq.toString()).toBe('7');
  });

  it('omits empty optional fields from headers', () => {
    const e: Event = { ...baseEvent, subpath: '', idempotencyKey: '', userId: '' };
    const msg = eventToKafkaMessage(e);
    expect(msg.headers).not.toHaveProperty('subpath');
    expect(msg.headers).not.toHaveProperty('idempotency_key');
    expect(msg.headers).not.toHaveProperty('user_id');
  });

  it('decodes missing or empty user attribution as absent', () => {
    const withoutUser = eventToKafkaMessage({ ...baseEvent, userId: undefined });
    const emptyUser = {
      ...withoutUser,
      headers: { ...(withoutUser.headers ?? {}), user_id: Buffer.alloc(0) },
    };

    for (const message of [withoutUser, emptyUser]) {
      const reconstructed = kafkaMessageToEvent({
        offset: '7',
        key: message.key as Buffer,
        value: message.value as Buffer,
        headers: message.headers as KafkaMessage['headers'],
        timestamp: '0',
        attributes: 0,
        size: 0,
      } as KafkaMessage);
      expect(reconstructed).not.toHaveProperty('userId');
    }
  });
});
