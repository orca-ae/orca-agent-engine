// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import type { SessionKey, SessionStoreEntry } from '@anthropic-ai/claude-agent-sdk';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { entryToEvent } from '../../src/harness/claude/event-mapper.js';

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

describe('ClaudeAgentSdkAdapter query-scoped resume reads', () => {
  const workspaceId = 'ws_snapshot';
  const sessionId = 'ses_snapshot';

  async function seed(store: InMemoryTranscriptStore, subpath = ''): Promise<SessionStoreEntry> {
    const value = entry();
    await store.append(workspaceId, sessionId, [
      entryToEvent({ workspaceId, sessionId, subpath, producedBy: 'harness', entry: value }),
    ]);
    return value;
  }

  it('shares one complete read across the probe, SDK loads, and subkey lookup', async () => {
    const store = new InMemoryTranscriptStore();
    const main = await seed(store);
    const thread = await seed(store, 'threads/worker');
    const adapter = new ClaudeAgentSdkAdapter(store, workspaceId);
    adapter.registerSession('sdk_uuid', sessionId);
    const release = adapter.beginReadScope(sessionId);
    try {
      const [has, subkeys, loaded, subagent] = await Promise.all([
        adapter.hasClaudeTranscript(sessionId),
        adapter.listSubkeys(newKey('sdk_uuid')),
        adapter.load(newKey('sdk_uuid')),
        adapter.load(newKey('sdk_uuid', 'threads/worker')),
      ]);
      expect(has).toBe(true);
      expect(subkeys).toEqual(['threads/worker']);
      expect(loaded).toEqual([main]);
      expect(subagent).toEqual([thread]);
      expect(await adapter.load(newKey('sdk_uuid', 'missing'))).toBeNull();
      expect(store.readCalls).toBe(1);

      (loaded![0]!.content as Array<{ text: string }>)[0]!.text = 'SDK mutation';
      expect(await adapter.load(newKey('sdk_uuid'))).toEqual([main]);
    } finally {
      release();
    }
  });

  it('keeps non-SDK and terminated subpaths listed without treating them as SDK history', async () => {
    const store = new InMemoryTranscriptStore();
    await store.append(workspaceId, sessionId, [
      {
        ...entryToEvent({
          workspaceId,
          sessionId,
          subpath: 'threads/ended',
          producedBy: 'harness',
          entry: entry(),
        }),
        kind: 'session.thread_status_terminated',
        producedBy: 'registry',
      },
    ]);
    const adapter = new ClaudeAgentSdkAdapter(store, workspaceId);
    const release = adapter.beginReadScope(sessionId);
    try {
      expect(await adapter.listSubkeys(newKey(sessionId))).toEqual(['threads/ended']);
      expect(await adapter.hasClaudeTranscript(sessionId, 'threads/ended')).toBe(false);
      expect(await adapter.load(newKey(sessionId, 'threads/ended'))).toBeNull();
      expect(store.readCalls).toBe(1);
    } finally {
      release();
    }
  });

  it('refreshes each new scope and an old release cannot clear a newer scope', async () => {
    const store = new InMemoryTranscriptStore();
    const first = await seed(store);
    const adapter = new ClaudeAgentSdkAdapter(store, workspaceId);
    const release = adapter.beginReadScope(sessionId);
    expect(await adapter.load(newKey(sessionId))).toEqual([first]);
    expect(() => adapter.beginReadScope(sessionId)).toThrow(/active read scope/);
    release();
    const second = await seed(store);
    const nextRelease = adapter.beginReadScope(sessionId);
    try {
      expect(await adapter.load(newKey(sessionId))).toEqual([first, second]);
      release();
      expect(await adapter.hasClaudeTranscript(sessionId)).toBe(true);
      expect(store.readCalls).toBe(2);
    } finally {
      nextRelease();
    }
    await adapter.hasClaudeTranscript(sessionId);
    await adapter.load(newKey(sessionId));
    expect(store.readCalls).toBe(4);
  });

  it('invalidates the snapshot after a successful durable append', async () => {
    const store = new InMemoryTranscriptStore();
    const first = await seed(store);
    const adapter = new ClaudeAgentSdkAdapter(store, workspaceId);
    const release = adapter.beginReadScope(sessionId);
    try {
      expect(await adapter.load(newKey(sessionId))).toEqual([first]);
      const second = entry();
      await adapter.append(newKey(sessionId), [second]);
      expect(await adapter.load(newKey(sessionId))).toEqual([first, second]);
      expect(store.readCalls).toBe(2);
    } finally {
      release();
    }
  });

  it.each(['before', 'after'] as const)(
    'rereads authoritative state when append fails %s persistence',
    async (failure) => {
      const store = new InMemoryTranscriptStore();
      const first = await seed(store);
      const adapter = new ClaudeAgentSdkAdapter(store, workspaceId);
      const release = adapter.beginReadScope(sessionId);
      try {
        expect(await adapter.load(newKey(sessionId))).toEqual([first]);
        const second = entry();
        store.appendFailure = failure;
        await expect(adapter.append(newKey(sessionId), [second])).rejects.toThrow('append failed');
        expect(await adapter.load(newKey(sessionId))).toEqual(
          failure === 'after' ? [first, second] : [first],
        );
        expect(store.readCalls).toBe(2);
      } finally {
        release();
      }
    },
  );

  it('does not republish an in-flight stale snapshot after append invalidation', async () => {
    const store = new InMemoryTranscriptStore();
    const first = await seed(store);
    let unblock!: () => void;
    store.readGate = new Promise<void>((resolve) => {
      unblock = resolve;
    });
    const adapter = new ClaudeAgentSdkAdapter(store, workspaceId);
    const release = adapter.beginReadScope(sessionId);
    try {
      const oldRead = adapter.load(newKey(sessionId));
      await vi.waitFor(() => expect(store.readCalls).toBe(1));
      const second = entry();
      await adapter.append(newKey(sessionId), [second]);
      store.readGate = undefined;
      expect(await adapter.load(newKey(sessionId))).toEqual([first, second]);
      unblock();
      expect(await oldRead).toEqual([first]);
      expect(await adapter.load(newKey(sessionId))).toEqual([first, second]);
      expect(store.readCalls).toBe(2);
    } finally {
      unblock();
      release();
    }
  });

  it('does not retain a failed read as the query snapshot', async () => {
    const store = new InMemoryTranscriptStore();
    await seed(store);
    const adapter = new ClaudeAgentSdkAdapter(store, workspaceId);
    const release = adapter.beginReadScope(sessionId);
    try {
      store.readFailures = 1;
      await expect(adapter.hasClaudeTranscript(sessionId)).rejects.toThrow('read failed');
      expect(await adapter.hasClaudeTranscript(sessionId)).toBe(true);
      expect(store.readCalls).toBe(2);
    } finally {
      release();
    }
  });

  it('does not share a scope across sessions or workspace-scoped adapters', async () => {
    const store = new InMemoryTranscriptStore();
    const value = await seed(store);
    const adapter = new ClaudeAgentSdkAdapter(store, workspaceId);
    const other = new ClaudeAgentSdkAdapter(store, 'ws_other');
    const release = adapter.beginReadScope(sessionId);
    const otherRelease = other.beginReadScope(sessionId);
    try {
      expect(await adapter.load(newKey(sessionId))).toEqual([value]);
      expect(await adapter.load(newKey('ses_other'))).toBeNull();
      expect(await other.load(newKey(sessionId))).toBeNull();
      expect(store.readCalls).toBe(3);
    } finally {
      release();
      otherRelease();
    }
  });
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
  readFailures = 0;
  readGate: Promise<void> | undefined;
  appendFailure: 'before' | 'after' | undefined;

  async append(_workspaceId: string, _sessionId: string, events: Event[]): Promise<string[]> {
    const failure = this.appendFailure;
    this.appendFailure = undefined;
    if (failure === 'before') throw new Error('append failed');
    if (this.appendDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.appendDelayMs));
    }
    for (const event of events) {
      this.events.push({ ...event, seq: this.events.length });
    }
    if (failure === 'after') throw new Error('append failed');
    return events.map((event) => event.id);
  }

  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    this.readCalls++;
    if (this.readFailures > 0) {
      this.readFailures--;
      throw new Error('read failed');
    }
    const snapshot = [...this.events];
    await this.readGate;
    for (const event of snapshot) {
      if (event.workspaceId !== workspaceId || event.sessionId !== sessionId) continue;
      if (opts.subpath !== '*' && event.subpath !== opts.subpath) continue;
      yield event;
    }
  }

  async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {}

  async archive(_workspaceId: string, _sessionId: string): Promise<void> {}

  async close(): Promise<void> {}
}
