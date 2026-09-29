// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { Kafka } from 'kafkajs';
import { KafkaTranscriptStore, sessionTopicName } from '@orca/transcript-store';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import { FakeHarness } from './fake-harness.js';
import { RegistryClient } from '../../src/clients/registry.js';
import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';
import { withCanonicalAgentEventEnvelope } from '../../src/harness/agent-harness.js';

// Cross-workspace TS imports — Vitest transforms them.
import { buildCombinedTestApp } from '../../../registry-service-ts/src/server.ts';
import { getTestDb, closeTestDb } from '../../../registry-service-ts/test/integration/setup.ts';
import {
  uniqueWorkspace,
  createTestApiKey,
  createTestAgent,
  createTestSession,
} from '../../../registry-service-ts/test/integration/fixtures.ts';

describe('chat round-trip via harness-server (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let store: KafkaTranscriptStore;
  let kafka: Kafka;

  beforeAll(async () => {
    kafka = new Kafka({
      clientId: 'rt-it',
      brokers: [process.env['KAFKA_BROKERS'] ?? 'localhost:9092'],
      // Default is 5 minutes — too long for integration tests. Drop it so
      // metadata refreshes happen quickly.
      metadataMaxAge: 1000,
    });
    store = new KafkaTranscriptStore({ kafka });
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: { bufferSize: 256, dropAgeMs: 5000, heartbeatMs: 15000 },
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('rt');
    apiKey = await createTestApiKey(db, workspaceId);
  }, 60000);

  afterAll(async () => {
    await store.close();
    await app.close();
    await closeTestDb();
  }, 60000);

  /** Pre-create the session topic via the Admin API for deterministic broker readiness. */
  async function preCreateTopic(ws: string, ses: string): Promise<void> {
    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: sessionTopicName(ws, ses), numPartitions: 1 }],
      });
    } finally {
      await admin.disconnect();
    }
  }

  function exactSessionTopicPattern(ws: string, ses: string): RegExp {
    return new RegExp(`^${escapeRegex(sessionTopicName(ws, ses))}$`);
  }

  it('user.message → agent.message via harness', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    // Pre-create the topic so the regex consumer picks it up at subscribe time.
    await preCreateTopic(workspaceId, sessionId);

    const dispatcher = new Dispatcher({
      kafka,
      groupId: `harness-rt-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      topicPattern: exactSessionTopicPattern(workspaceId, sessionId),
      store,
      anthropicApiKey: 'unused-test-key',
      modelDefault: 'fake',
      harnessFactory: () => new FakeHarness(),
    });
    await dispatcher.start();
    // Allow consumer-group rebalance to settle (kafkajs JOIN takes ~3s on
    // local broker; wait an extra second so the partitions are fetchable).
    await new Promise((r) => setTimeout(r, 4500));

    const sseAbort = new AbortController();
    let sseReader: ReadableStreamDefaultReader<Uint8Array> | null = null;
    try {
      // Open SSE first so we don't miss the reply.
      const ssePromise = (async (): Promise<unknown> => {
        const r = await fetch(`${baseURL}/v1/sessions/${sessionId}/events/stream`, {
          headers: { 'x-api-key': apiKey },
          signal: sseAbort.signal,
        });
        sseReader = r.body!.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        const start = Date.now();
        while (Date.now() - start < 30000) {
          let chunk: ReadableStreamReadResult<Uint8Array>;
          try {
            chunk = await sseReader.read();
          } catch {
            return null;
          }
          if (chunk.done) return null;
          buf += decoder.decode(chunk.value);
          let idx;
          while ((idx = buf.indexOf('\n\n')) >= 0) {
            const block = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            if (block.includes('event: agent.message')) {
              const dataLine = block.split('\n').find((l) => l.startsWith('data: '))!;
              return JSON.parse(dataLine.slice(6));
            }
          }
        }
        return null;
      })();

      // Give the SSE consumer a moment to subscribe.
      await new Promise((r) => setTimeout(r, 500));

      const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          events: [{ type: 'user.message', content: [{ type: 'text', text: 'hi' }] }],
        }),
      });
      expect(post.status).toBe(200);

      const replied = (await ssePromise) as { type?: string; content?: unknown[] } | null;
      expect(replied).not.toBeNull();
      expect(replied?.type).toBe('agent.message');
      expect(JSON.stringify(replied)).toContain('echo: hi');
    } finally {
      // Tear the SSE stream down so the server's tail() consumer disconnects
      // before app.close() in afterAll, which keeps cleanup fast.
      sseAbort.abort();
      if (sseReader) {
        try {
          await (sseReader as ReadableStreamDefaultReader<Uint8Array>).cancel();
        } catch {
          /* ignore */
        }
      }
      await dispatcher.stop();
    }
  }, 60000);

  it('streams agent.custom_tool_use and resumes after POST /events user.custom_tool_result', async () => {
    const inputSchema = {
      type: 'object',
      properties: { ticket_id: { type: 'string' } },
      required: ['ticket_id'],
    };
    const agentRes = await fetch(`${baseURL}/v1/agents`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        name: `custom-tool-flow-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
        system: 'Use lookup_ticket when asked for a ticket.',
        tools: [
          {
            type: 'custom',
            name: 'lookup_ticket',
            description: 'Look up a support ticket.',
            input_schema: inputSchema,
          },
        ],
        mcp_servers: [],
        skills: [],
        metadata: {},
      }),
    });
    expect(agentRes.status).toBe(200);
    const agentId = ((await agentRes.json()) as { id: string }).id;
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    await preCreateTopic(workspaceId, sessionId);

    let liveHarness: CustomToolRoundTripHarness | undefined;
    const dispatcher = new Dispatcher({
      kafka,
      groupId: `harness-custom-tool-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      topicPattern: exactSessionTopicPattern(workspaceId, sessionId),
      store,
      anthropicApiKey: 'unused-test-key',
      modelDefault: 'fake',
      harnessFactory: () => {
        liveHarness = new CustomToolRoundTripHarness();
        return liveHarness;
      },
      registry: new RegistryClient(
        baseURL,
        async () => 'test-internal-service-token-at-least-32-chars',
      ),
    });
    await dispatcher.start();
    await new Promise((r) => setTimeout(r, 4500));

    try {
      const firstFramesPromise = collectSseFrames(baseURL, apiKey, sessionId, {
        deadlineMs: 30000,
        until: (frames) => frames.some((frame) => frame.type === 'agent.custom_tool_use'),
      });

      await new Promise((r) => setTimeout(r, 500));
      const post = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          events: [
            {
              type: 'user.message',
              content: [{ type: 'text', text: 'Please look up ticket T-123.' }],
            },
          ],
          request_id: `custom-tool-request-${Date.now()}`,
        }),
      });
      expect(post.status).toBe(200);

      const firstFrames = await firstFramesPromise;
      const customToolUse = firstFrames.find((frame) => frame.type === 'agent.custom_tool_use');
      expect(customToolUse).toMatchObject({
        type: 'agent.custom_tool_use',
        name: 'lookup_ticket',
        input: { ticket_id: 'T-123' },
      });
      expect(customToolUse?.id).toMatch(/^evt_/);
      expect(liveHarness?.startInput?.agentSnapshot.custom_tools).toEqual([
        {
          name: 'lookup_ticket',
          description: 'Look up a support ticket.',
          input_schema: inputSchema,
        },
      ]);

      const resultPost = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          events: [
            {
              type: 'user.custom_tool_result',
              custom_tool_use_id: customToolUse!.id,
              content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
            },
          ],
          request_id: `custom-tool-result-${Date.now()}`,
        }),
      });
      expect(resultPost.status).toBe(200);
      expect(((await resultPost.json()) as { data: Array<{ id: string }> }).data[0]?.id).toMatch(
        /^evt_/,
      );

      const finalFrames = await collectSseFrames(baseURL, apiKey, sessionId, {
        // Canonical append responses omit the internal transcript cursor.
        // Replay from the beginning; the completion predicate ignores earlier frames.
        fromCursor: '0',
        deadlineMs: 30000,
        until: (frames) => assistantTextFromFrames(frames).includes('Ticket T-123 is open.'),
      });
      expect(assistantTextFromFrames(finalFrames)).toContain('Ticket T-123 is open.');
    } finally {
      await dispatcher.stop();
    }
  }, 60000);
});

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

interface SseFrame {
  id: string;
  type: string;
  seq?: string;
  content?: unknown;
  message?: {
    role?: string;
    content?: Array<{ type: string; text?: string }>;
  };
  [k: string]: unknown;
}

async function collectSseFrames(
  baseURL: string,
  apiKey: string,
  sessionId: string,
  opts: { deadlineMs: number; until: (frames: SseFrame[]) => boolean; fromCursor?: string },
): Promise<SseFrame[]> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), opts.deadlineMs + 5000);
  try {
    const streamUrl = new URL(`/v1/sessions/${sessionId}/events/stream`, baseURL);
    if (opts.fromCursor !== undefined) streamUrl.searchParams.set('from_cursor', opts.fromCursor);
    const res = await fetch(streamUrl.toString(), {
      headers: { 'x-api-key': apiKey, accept: 'text/event-stream' },
      signal: ac.signal,
    });
    expect(res.status).toBe(200);
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffered = '';
    const frames: SseFrame[] = [];
    const deadline = Date.now() + opts.deadlineMs;
    try {
      while (Date.now() < deadline && !opts.until(frames)) {
        const chunk = await reader.read();
        if (chunk.done) break;
        buffered += decoder.decode(chunk.value, { stream: true });
        let idx;
        while ((idx = buffered.indexOf('\n\n')) >= 0) {
          const block = buffered.slice(0, idx);
          buffered = buffered.slice(idx + 2);
          if (block.startsWith(':')) continue;
          const dataLine = block.split('\n').find((line) => line.startsWith('data: '));
          if (!dataLine) continue;
          frames.push(JSON.parse(dataLine.slice(6)) as SseFrame);
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
    }
    return frames;
  } finally {
    clearTimeout(timer);
    ac.abort();
  }
}

function assistantTextFromFrames(frames: SseFrame[]): string {
  return frames
    .flatMap((frame) => {
      const blocks = (Array.isArray(frame.content) ? frame.content : []) as Array<{
        type: string;
        text?: string;
      }>;
      return blocks
        .filter((block) => block?.type === 'text' && typeof block.text === 'string')
        .map((block) => block.text!);
    })
    .join('\n');
}

class CustomToolRoundTripHarness implements AgentHarness {
  startInput: SessionStartInput | undefined;
  private pendingId: string | null = null;
  private q: AgentEvent[] = [];
  private resolvers: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;

  async start(input: SessionStartInput): Promise<void> {
    this.startInput = input;
  }

  async submit(event: UserEvent): Promise<void> {
    if (this.done) return;
    if (event.kind === 'user.message' && this.pendingId === null) {
      this.pendingId = `evt_custom_tool_${Date.now()}`;
      this.emit({
        kind: 'agent.custom_tool_use',
        id: this.pendingId,
        payload: {
          id: this.pendingId,
          name: 'lookup_ticket',
          input: { ticket_id: 'T-123' },
        },
      });
      this.emit({
        kind: 'session.status_idle',
        payload: { stop_reason: { type: 'requires_action', event_ids: [this.pendingId] } },
      });
      return;
    }
    if (event.kind !== 'user.custom_tool_result') return;
    const payload = event.payload as {
      custom_tool_use_id?: unknown;
      content?: Array<{ type: string; text?: string }>;
    };
    if (payload.custom_tool_use_id !== this.pendingId) return;
    const text = payload.content
      ?.filter((block) => block.type === 'text' && typeof block.text === 'string')
      .map((block) => block.text)
      .join('\n');
    this.pendingId = null;
    this.emit({
      kind: 'agent.message',
      payload: { type: 'assistant', content: [{ type: 'text', text: `resolved: ${text}` }] },
    });
  }

  hasPendingRequiredAction(): boolean {
    return this.pendingId !== null;
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason;
    this.done = true;
    for (const resolve of this.resolvers.splice(0)) {
      resolve({ value: undefined, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.done || this.q.length > 0) {
      if (this.q.length > 0) {
        const head = this.q.shift();
        if (head !== undefined) yield head;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.resolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  private emit(event: AgentEventInput): void {
    const enveloped = withCanonicalAgentEventEnvelope(event);
    const resolve = this.resolvers.shift();
    if (resolve) resolve({ value: enveloped, done: false });
    else this.q.push(enveloped);
  }
}
