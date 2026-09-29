// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { Event } from '@orca/transcript-store';
import { newSessionInitialEventsOutboxRow } from '../../src/domain/session-lifecycle-outbox.js';

const event: Event = {
  id: 'evt_initial',
  workspaceId: 'ws_initial',
  sessionId: 'ses_initial',
  subpath: '',
  seq: 1,
  producedAt: '2026-07-24T00:00:00.000Z',
  producedBy: 'client',
  kind: 'user.message',
  payload: new TextEncoder().encode(
    JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'start' }] }),
  ),
  idempotencyKey: 'ses_initial:initial:0',
  userId: 'user_initial',
};

describe('initial session event persistence boundary', () => {
  it('serializes an ordered event batch into one durable outbox row', () => {
    const now = new Date('2026-07-24T00:00:01.000Z');
    const row = newSessionInitialEventsOutboxRow(event.workspaceId, event.sessionId, [event], now);

    expect(row).toMatchObject({
      id: event.id,
      workspaceId: event.workspaceId,
      sessionId: event.sessionId,
      kind: 'session.initial_events',
      createdAt: now,
      events: [
        {
          id: event.id,
          kind: event.kind,
          payloadBase64: Buffer.from(event.payload).toString('base64'),
          idempotencyKey: event.idempotencyKey,
          userId: event.userId,
        },
      ],
    });
  });

  it('omits absent and empty optional user attribution from the durable row', () => {
    const row = newSessionInitialEventsOutboxRow(event.workspaceId, event.sessionId, [
      { ...event, id: 'evt_initial_missing_user', userId: undefined },
      { ...event, id: 'evt_initial_empty_user', userId: '' },
    ]);

    for (const serialized of row.events ?? []) {
      expect(serialized).not.toHaveProperty('userId');
    }
  });
});
