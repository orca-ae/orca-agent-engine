// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { SessionKey, SessionStoreEntry } from '@anthropic-ai/claude-agent-sdk';
import { v7 as uuidv7 } from 'uuid';
import type { KafkaTranscriptStore } from '@orca/transcript-store';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { entryToEvent, publicEntryToEvent } from '../../src/harness/claude/event-mapper.js';
import { buildTestStore, uniqueIds } from './setup.js';

let store: KafkaTranscriptStore;

beforeAll(() => {
  store = buildTestStore();
});

afterAll(async () => {
  await store.close();
});

const newKey = (sessionId: string, subpath?: string): SessionKey => {
  const key: SessionKey = { projectKey: 'orca-test', sessionId };
  if (subpath !== undefined) key.subpath = subpath;
  return key;
};

const entry = (overrides: Partial<SessionStoreEntry> = {}): SessionStoreEntry => ({
  type: 'user',
  uuid: uuidv7(),
  timestamp: new Date().toISOString(),
  content: [{ type: 'text', text: `msg-${Math.random().toString(36).slice(2, 6)}` }],
  ...overrides,
});

describe('ClaudeAgentSdkAdapter SessionStore conformance', () => {
  it('append + load round-trips entries deeply', async () => {
    const { ws, ses } = uniqueIds('confrt');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const entries: SessionStoreEntry[] = [entry(), entry({ type: 'assistant' }), entry()];
    await adapter.append(newKey(ses), entries);
    const loaded = await adapter.load(newKey(ses));
    expect(loaded).not.toBeNull();
    expect(loaded).toHaveLength(3);
    for (let i = 0; i < entries.length; i++) {
      expect(loaded![i]).toEqual(entries[i]);
    }
  }, 30000);

  it('load returns null for never-written keys', async () => {
    const { ws, ses } = uniqueIds('confempty');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const loaded = await adapter.load(newKey(ses));
    expect(loaded).toBeNull();
  }, 30000);

  it('multiple append calls are concatenated in order', async () => {
    const { ws, ses } = uniqueIds('confmulti');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const a = entry({ type: 'user' });
    const b = entry({ type: 'assistant' });
    const c = entry({ type: 'user' });
    await adapter.append(newKey(ses), [a]);
    await adapter.append(newKey(ses), [b, c]);
    const loaded = await adapter.load(newKey(ses));
    expect(loaded).toHaveLength(3);
    expect(loaded![0]).toEqual(a);
    expect(loaded![1]).toEqual(b);
    expect(loaded![2]).toEqual(c);
  }, 30000);

  it('replaying append with the same uuid does NOT duplicate', async () => {
    const { ws, ses } = uniqueIds('confdedup');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const e = entry();
    await adapter.append(newKey(ses), [e]);
    await adapter.append(newKey(ses), [e]);
    await adapter.append(newKey(ses), [e]);
    const loaded = await adapter.load(newKey(ses));
    expect(loaded).toHaveLength(1);
    expect(loaded![0]).toEqual(e);
  }, 30000);

  it('subpath isolates subagent transcripts from the main transcript', async () => {
    const { ws, ses } = uniqueIds('confsub');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const main = entry({ type: 'user' });
    const sub = entry({ type: 'user' });
    await adapter.append(newKey(ses), [main]);
    await adapter.append(newKey(ses, 'subagents/code-reviewer/0'), [sub]);

    const mainLoaded = await adapter.load(newKey(ses));
    expect(mainLoaded).toHaveLength(1);
    expect(mainLoaded![0]).toEqual(main);

    const subLoaded = await adapter.load(newKey(ses, 'subagents/code-reviewer/0'));
    expect(subLoaded).toHaveLength(1);
    expect(subLoaded![0]).toEqual(sub);
  }, 30000);

  it('appending an empty array is a no-op', async () => {
    const { ws, ses } = uniqueIds('confempbatch');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    await adapter.append(newKey(ses), []);
    const loaded = await adapter.load(newKey(ses));
    expect(loaded).toBeNull();
  }, 30000);

  it('entries without uuid are appended without dedup', async () => {
    const { ws, ses } = uniqueIds('confnouuid');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const noUuid: SessionStoreEntry = { type: 'system.title', title: 'My session' };
    // Two appends of the same content; each gets a fresh uuid synthesized
    // inside event-mapper.entryToEvent (uuidv7), so they don't dedup.
    await adapter.append(newKey(ses), [noUuid]);
    await adapter.append(newKey(ses), [noUuid]);
    const loaded = await adapter.load(newKey(ses));
    expect(loaded).toHaveLength(2);
    expect(loaded![0]).toEqual(noUuid);
    expect(loaded![1]).toEqual(noUuid);
  }, 30000);

  it('large batches preserve ordering across many appends', async () => {
    const { ws, ses } = uniqueIds('conflarge');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const batch1 = Array.from({ length: 20 }, (_, i) => entry({ type: 'user', n: i }));
    const batch2 = Array.from({ length: 20 }, (_, i) => entry({ type: 'assistant', n: i + 100 }));
    await adapter.append(newKey(ses), batch1);
    await adapter.append(newKey(ses), batch2);
    const loaded = await adapter.load(newKey(ses));
    expect(loaded).toHaveLength(40);
    for (let i = 0; i < 20; i++) {
      expect((loaded![i] as { n: number }).n).toBe(i);
    }
    for (let i = 0; i < 20; i++) {
      expect((loaded![20 + i] as { n: number }).n).toBe(i + 100);
    }
  }, 60000);

  it('load ignores public transcript events that are not SDK session entries', async () => {
    const { ws, ses } = uniqueIds('confisolated');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const sdkEntry = entry({ type: 'assistant' });
    const publicEvent = publicEntryToEvent({
      workspaceId: ws,
      sessionId: ses,
      subpath: '',
      producedBy: 'harness',
      eventId: 'evt_public_wrapper',
      entry: { type: 'agent.message', content: [{ type: 'text', text: 'public wrapper' }] },
    });

    await adapter.append(newKey(ses), [sdkEntry]);
    await store.append(ws, ses, [publicEvent]);

    const loaded = await adapter.load(newKey(ses));
    expect(loaded).toEqual([sdkEntry]);
  }, 30000);

  it('load ignores client-authored SDK replay envelopes', async () => {
    const { ws, ses } = uniqueIds('confclient');
    const adapter = new ClaudeAgentSdkAdapter(store, ws);
    const sdkEntry = entry({ type: 'assistant' });
    const clientReplayEvent = entryToEvent({
      workspaceId: ws,
      sessionId: ses,
      subpath: '',
      producedBy: 'client',
      entry: sdkEntry,
    });

    await store.append(ws, ses, [clientReplayEvent]);

    const loaded = await adapter.load(newKey(ses));
    expect(loaded).toBeNull();
  }, 30000);
});
