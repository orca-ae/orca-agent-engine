// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import type { SessionKey, SessionStoreEntry } from '@anthropic-ai/claude-agent-sdk';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';

const newKey = (sessionId: string, subpath?: string): SessionKey => {
  const key: SessionKey = { projectKey: 'orca-test', sessionId };
  if (subpath !== undefined) key.subpath = subpath;
  return key;
};

const entry = (type = 'assistant'): SessionStoreEntry => ({
  type,
  uuid: `01933d4a-1234-7000-8000-${Math.random().toString(16).slice(2, 14).padEnd(12, '0')}`,
  content: [{ type: 'text', text: 'ok' }],
});

describe('ClaudeAgentSdkAdapter session threads', () => {
  it('rejects creating a 26th concurrent session thread', async () => {
    const store = new InMemoryTranscriptStore();
    const adapter = new ClaudeAgentSdkAdapter(store, 'ws_threads');
    const sessionId = 'ses_threads';

    for (let i = 0; i < 25; i++) {
      await adapter.append(newKey(sessionId, `threads/${i}`), [entry()]);
    }

    await expect(adapter.append(newKey(sessionId, 'threads/25'), [entry()])).rejects.toThrow(
      /maximum concurrent session threads/i,
    );
    expect(store.readCalls).toBe(2);
  });

  it('caches active thread subpaths across hot appends', async () => {
    const store = new InMemoryTranscriptStore();
    const adapter = new ClaudeAgentSdkAdapter(store, 'ws_threads');
    const sessionId = 'ses_threads';

    await adapter.append(newKey(sessionId, 'threads/worker'), [entry()]);
    await adapter.append(newKey(sessionId, 'threads/worker'), [entry()]);
    await adapter.append(newKey(sessionId, 'threads/another-worker'), [entry()]);

    expect(store.readCalls).toBe(1);
  });

  it('allows a new session thread after a previous subpath terminates', async () => {
    const store = new InMemoryTranscriptStore();
    const adapter = new ClaudeAgentSdkAdapter(store, 'ws_threads');
    const sessionId = 'ses_threads';

    for (let i = 0; i < 25; i++) {
      await adapter.append(newKey(sessionId, `threads/${i}`), [entry()]);
    }
    await store.append('ws_threads', sessionId, [
      {
        id: 'evt_thread_terminated',
        workspaceId: 'ws_threads',
        sessionId,
        subpath: 'threads/0',
        seq: 0,
        producedAt: new Date(0).toISOString(),
        producedBy: 'registry',
        kind: 'session.thread_status_terminated',
        payload: Buffer.from(JSON.stringify({ type: 'session.thread_status_terminated' }), 'utf8'),
        idempotencyKey: '',
      },
    ]);

    await expect(
      adapter.append(newKey(sessionId, 'threads/25'), [entry()]),
    ).resolves.toBeUndefined();
  });

  it('serializes concurrent subagent appends before enforcing the 25 thread limit', async () => {
    const store = new InMemoryTranscriptStore();
    const adapter = new ClaudeAgentSdkAdapter(store, 'ws_threads');
    const sessionId = 'ses_threads';

    for (let i = 0; i < 24; i++) {
      await adapter.append(newKey(sessionId, `threads/${i}`), [entry()]);
    }
    store.appendDelayMs = 25;

    const results = await Promise.allSettled([
      adapter.append(newKey(sessionId, 'threads/24'), [entry()]),
      adapter.append(newKey(sessionId, 'threads/25'), [entry()]),
    ]);

    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.find((result) => result.status === 'rejected');
    expect(rejected).toMatchObject({
      status: 'rejected',
      reason: expect.objectContaining({
        message: expect.stringMatching(/maximum concurrent session threads/i),
      }),
    });
  });
});

class InMemoryTranscriptStore implements TranscriptStore {
  private readonly events: Event[] = [];
  appendDelayMs = 0;
  readCalls = 0;

  async append(_workspaceId: string, _sessionId: string, events: Event[]): Promise<string[]> {
    if (this.appendDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.appendDelayMs));
    }
    for (const event of events) {
      this.events.push({ ...event, seq: this.events.length });
    }
    return events.map((event) => event.id);
  }

  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    this.readCalls++;
    for (const event of this.events) {
      if (event.workspaceId !== workspaceId || event.sessionId !== sessionId) continue;
      if (opts.subpath !== '*' && event.subpath !== opts.subpath) continue;
      yield event;
    }
  }

  async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {}

  async archive(_workspaceId: string, _sessionId: string): Promise<void> {}

  async close(): Promise<void> {}
}
