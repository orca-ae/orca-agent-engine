// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { subagentMessageRate, registry } from '../../src/metrics.js';
import { KafkaTranscriptStore } from '../../src/kafka-store.js';

describe('subagent_message_rate (KafkaTranscriptStore.append)', () => {
  beforeEach(() => subagentMessageRate.reset());

  it('does not increment for empty subpath', async () => {
    const store = Object.create(KafkaTranscriptStore.prototype) as KafkaTranscriptStore;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (store as any).producer = { append: vi.fn().mockResolvedValue(['id1']) };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (store as any).connected = true;
    await store.append('ws_x', 'ses_y', [
      {
        id: 'id1',
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        subpath: '',
        seq: 0,
        producedAt: '',
        producedBy: 'client',
        kind: 'user.message',
        payload: new Uint8Array(),
        idempotencyKey: '',
      },
    ]);
    const out = await registry.getSingleMetricAsString('transcript_store_subagent_message_rate');
    expect(out).not.toMatch(/workspace_id="ws_x"[^}]*\}\s+1/);
  });

  it('increments per non-empty subpath, labeled', async () => {
    const store = Object.create(KafkaTranscriptStore.prototype) as KafkaTranscriptStore;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (store as any).producer = { append: vi.fn().mockResolvedValue(['a', 'b']) };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (store as any).connected = true;
    await store.append('ws_x', 'ses_y', [
      {
        id: 'a',
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        subpath: 'subagents/a/0',
        seq: 0,
        producedAt: '',
        producedBy: 'harness',
        kind: 'agent.message',
        payload: new Uint8Array(),
        idempotencyKey: '',
      },
      {
        id: 'b',
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        subpath: 'subagents/a/0',
        seq: 0,
        producedAt: '',
        producedBy: 'harness',
        kind: 'agent.message',
        payload: new Uint8Array(),
        idempotencyKey: '',
      },
    ]);
    const out = await registry.getSingleMetricAsString('transcript_store_subagent_message_rate');
    expect(out).toMatch(/workspace_id="ws_x"[^}]*produced_by="harness"[^}]*\}\s+2/);
  });
});
