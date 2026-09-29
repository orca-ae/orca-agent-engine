// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { publicEntryToEvent } from '../../src/harness/claude/event-mapper.js';

describe('publicEntryToEvent idempotencyKey', () => {
  const base = {
    workspaceId: 'ws',
    sessionId: 'ses',
    subpath: '' as const,
    producedBy: 'harness' as const,
  };
  it('defaults idempotencyKey to empty string', () => {
    const e = publicEntryToEvent({
      ...base,
      eventId: 'evt_default_metadata',
      entry: { type: 'agent.message', content: [] },
    });
    expect(e.idempotencyKey).toBe('');
  });
  it('uses the provided idempotencyKey', () => {
    const e = publicEntryToEvent({
      ...base,
      eventId: 'evt_metadata',
      entry: { type: 'agent.message', content: [] },
      idempotencyKey: 'evt_abc',
    });
    expect(e.idempotencyKey).toBe('evt_abc');
    expect(e.kind).toBe('agent.message');
  });
  it('keeps explicit envelope identity separate from idempotency metadata', () => {
    const e = publicEntryToEvent({
      ...base,
      entry: { type: 'agent.message', id: 'evt_payload' },
      eventId: 'evt_envelope',
      idempotencyKey: 'raw-envelope-id',
    });
    expect(e.id).toBe('evt_envelope');
    expect(e.idempotencyKey).toBe('raw-envelope-id');
  });
});
