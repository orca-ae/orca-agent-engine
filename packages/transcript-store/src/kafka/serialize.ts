// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Message, KafkaMessage } from 'kafkajs';
import type { Event } from '../types.js';
import { eventMatchesRoute, type SessionRoute } from '../route.js';

const STRING_HEADERS = [
  'id',
  'workspace_id',
  'session_id',
  'subpath',
  'produced_at',
  'produced_by',
  'kind',
  'idempotency_key',
  'user_id',
] as const;

type HeaderKey = (typeof STRING_HEADERS)[number];

const EVENT_FIELD_BY_HEADER: Record<HeaderKey, keyof Event> = {
  id: 'id',
  workspace_id: 'workspaceId',
  session_id: 'sessionId',
  subpath: 'subpath',
  produced_at: 'producedAt',
  produced_by: 'producedBy',
  kind: 'kind',
  idempotency_key: 'idempotencyKey',
  user_id: 'userId',
};

export function eventToKafkaMessage(event: Event): Message {
  const headers: Record<string, Buffer> = {};
  for (const h of STRING_HEADERS) {
    const value = event[EVENT_FIELD_BY_HEADER[h]] as string;
    if (value && value.length > 0) {
      headers[h] = Buffer.from(value, 'utf8');
    }
  }
  return {
    key: event.id,
    value: Buffer.from(event.payload),
    headers,
  };
}

export function kafkaMessageToEvent(msg: KafkaMessage): Event {
  const headers = (msg.headers ?? {}) as Record<string, Buffer | string | undefined>;
  const get = (k: HeaderKey): string => {
    const raw = headers[k];
    if (raw === undefined || raw === null) return '';
    return Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
  };
  const userId = get('user_id');
  return {
    id: get('id'),
    workspaceId: get('workspace_id'),
    sessionId: get('session_id'),
    subpath: get('subpath'),
    seq: Number(msg.offset),
    producedAt: get('produced_at'),
    producedBy: get('produced_by'),
    kind: get('kind'),
    payload: msg.value ? new Uint8Array(msg.value) : new Uint8Array(),
    idempotencyKey: get('idempotency_key'),
    ...(userId.length > 0 ? { userId } : {}),
  };
}

/** Decode only when the untrusted route headers agree with the broker topic. */
export function kafkaMessageToEventForRoute(msg: KafkaMessage, route: SessionRoute): Event | null {
  const event = kafkaMessageToEvent(msg);
  if (!eventMatchesRoute(event, route)) return null;
  return event;
}
