// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Real-bridge + real-Kafka integration spec for the owner-pod session event
// bridge.
//
// The owner-pod single-writer turn guarantee hinges on the boundary the bridge
// draws at start: it CATCHES UP on un-driven user turns (a `user.*` event with no
// agent event after it) and then follows live from an explicit cursor `head + 1`.
// Two properties this spec proves against the REAL `KafkaTranscriptStore` (the
// default backend), wired to the REAL `SessionEventBridge`, `TunnelRegistry`, and
// `TunnelTransport`, with only the runner end of the tunnel faked (an in-process
// ws peer speaking the tunnel frame protocol — network-free, the same approach the
// unit spec uses):
//   - a `user.*` event already persisted BEFORE the bridge starts, with no agent
//     event after it, IS caught up and driven (the first-turn case a from-now tail
//     would have dropped); and
//   - a turn a prior owner generation already ANSWERED (its agent events sit after
//     it) is NOT re-forwarded as a brand-new turn on a fresh bridge / reconnect.
//
// Requires the dev Kafka broker (KAFKA_BROKERS, default localhost:9092).

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  FrameKind,
  decodeFrame,
  encodeFrame,
  decodeBody,
  type Frame,
  type RequestFrame,
  type HelloFrame,
} from '@orca/harness-tunnel';
import { KafkaTranscriptStore, type Event } from '@orca/transcript-store';
import { Kafka } from 'kafkajs';
import { createReadyKafkaTopic } from '../helpers/kafka-topic.js';
import {
  TunnelRegistry,
  type RegistrySession,
  type RegistryWebSocketLike,
} from '../../src/tunnel/tunnel-registry.js';
import { httpEventToProto } from '../../src/domain/events.js';
import {
  SessionEventBridge,
  AGENT_EVENT_PRODUCED_BY,
} from '../../src/tunnel/session-event-bridge.js';

const RUNNER_ID = 'runner_token_bridgekafkabridgekafkabridgekaf';

function makeKafka(): Kafka {
  return new Kafka({
    clientId: 'registry-bridge-kafka-it',
    brokers: [process.env['KAFKA_BROKERS'] ?? 'localhost:9092'],
  });
}

async function deleteTopic(ws: string, ses: string): Promise<void> {
  const admin = makeKafka().admin();
  await admin.connect();
  try {
    await admin.deleteTopics({ topics: [`orca.${ws}.sessions.${ses}.events`], timeout: 5000 });
  } catch {
    /* topic might not exist */
  } finally {
    await admin.disconnect();
  }
}

/**
 * Pre-create the session topic so the bridge's from-now tail joins against
 * stable metadata, without depending on broker auto-creation and metadata
 * propagation during subscription. In production the topic already exists by the
 * time a turn is driven (session-create + the public append path created it);
 * creating it up front here makes that ordering deterministic for the test.
 */
async function createTopic(ws: string, ses: string): Promise<void> {
  const admin = makeKafka().admin();
  await admin.connect();
  try {
    await createReadyKafkaTopic(admin, `orca.${ws}.sessions.${ses}.events`);
  } finally {
    await admin.disconnect();
  }
}

function uniqueIds(prefix: string): { ws: string; ses: string } {
  const suffix = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  return { ws: `ws_${prefix}_${suffix}`, ses: `ses_${prefix}_${suffix}` };
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 15000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error('waitFor: condition not met within timeout');
    await new Promise<void>((resolve) => setTimeout(resolve, 25));
  }
}

function userEvent(ws: string, ses: string, id: string, text: string): Event {
  return httpEventToProto({
    workspaceId: ws,
    sessionId: ses,
    producedBy: 'client',
    idempotencyKey: '',
    input: { id, type: 'user.message', content: [{ type: 'text', text }] },
  });
}

async function waitForAgentEvents(
  store: KafkaTranscriptStore,
  ws: string,
  ses: string,
  count: number,
  afterSnapshot?: () => void,
): Promise<Event[]> {
  const signal = AbortSignal.timeout(15000);
  let events: Event[] = [];
  await waitFor(async () => {
    // maxEvents: 0 removes the count cap, not the high-watermark boundary.
    // Await each finite snapshot (including consumer cleanup), then re-read
    // while the bridge is still asynchronously appending the runner's output.
    events = [];
    for await (const event of store.read(ws, ses, {
      fromCursor: '',
      maxEvents: 0,
      subpath: '',
      signal,
    })) {
      if (event.producedBy === AGENT_EVENT_PRODUCED_BY) events.push(event);
    }
    afterSnapshot?.();
    return events.length === count;
  });
  return events;
}

// ── Minimal in-process fake runner (a ws peer speaking the tunnel protocol) ──

interface TurnScript {
  lines: Array<Record<string, unknown>>;
  beforeResponse?: Promise<void>;
}

class FakeRunner {
  readonly turns: Array<Record<string, unknown>> = [];
  private readonly scripts = new Map<string, TurnScript>();
  private readonly socket: RunnerSocket;

  constructor(
    private readonly registry: TunnelRegistry,
    private readonly runnerId: string,
    private readonly owner: string,
  ) {
    this.socket = new RunnerSocket((raw) => this.onBridgeFrame(raw));
  }

  scriptTurn(userEventId: string, script: TurnScript): this {
    this.scripts.set(userEventId, script);
    return this;
  }

  connect(): void {
    const hello: HelloFrame = {
      kind: FrameKind.Hello,
      runnerVersion: '0.1.0-test',
      frameProtocolVersion: 1,
      harnesses: [],
      envs: [],
    };
    const session = this.registry.register(this.runnerId, this.socket, hello, {
      owner: this.owner,
    });
    this.startOutboundDrain(session);
  }

  disconnect(): void {
    this.registry.deregister(this.runnerId);
  }

  private startOutboundDrain(session: RegistrySession): void {
    void (async () => {
      for (;;) {
        const data = await session.outboundQueue.get();
        if (data === null) return;
        await this.socket.sendText(data);
      }
    })();
  }

  private onBridgeFrame(raw: string): void {
    let frame: Frame;
    try {
      frame = decodeFrame(raw);
    } catch {
      return;
    }
    if (frame.kind === FrameKind.Ping) {
      this.route(encodeFrame({ kind: FrameKind.Pong, ts: frame.ts }));
      return;
    }
    if (frame.kind === FrameKind.RequestCancel) {
      this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
      return;
    }
    if (frame.kind !== FrameKind.Request) return;
    void this.handleTurn(frame);
  }

  private async handleTurn(frame: RequestFrame): Promise<void> {
    const body = decodeRequestBody(frame);
    this.turns.push(body);
    const id = typeof body['id'] === 'string' ? body['id'] : '';
    const script = this.scripts.get(id) ?? { lines: [] };
    if (script.beforeResponse) await script.beforeResponse;
    this.route(
      encodeFrame({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status: 200,
        headers: [['content-type', 'application/x-ndjson']],
      }),
    );
    for (const line of script.lines) {
      this.route(
        encodeFrame({
          kind: FrameKind.ResponseBody,
          id: frame.id,
          body: `${JSON.stringify(line)}\n`,
          encoding: 'utf-8',
        }),
      );
    }
    this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
  }

  private route(raw: string): void {
    const frame = decodeFrame(raw);
    if (
      frame.kind === FrameKind.ResponseHead ||
      frame.kind === FrameKind.ResponseBody ||
      frame.kind === FrameKind.ResponseEnd
    ) {
      this.registry.routeResponseFrame(this.runnerId, frame);
    }
  }
}

class RunnerSocket implements RegistryWebSocketLike {
  closed: { code: number; reason: string } | undefined;
  constructor(private readonly onFrame: (raw: string) => void) {}
  async sendText(data: string): Promise<void> {
    queueMicrotask(() => this.onFrame(data));
  }
  receiveText(): Promise<string> {
    return new Promise<string>(() => {});
  }
  async close(opts?: { code?: number; reason?: string }): Promise<void> {
    this.closed = { code: opts?.code ?? 1000, reason: opts?.reason ?? '' };
  }
}

function decodeRequestBody(frame: RequestFrame): Record<string, unknown> {
  if (frame.body === null || frame.body === undefined) return {};
  const bytes = decodeBody(frame.body, frame.encoding ?? 'utf-8');
  try {
    const parsed = JSON.parse(Buffer.from(bytes).toString('utf8')) as unknown;
    return parsed !== null && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

// ── The spec ──

describe('SessionEventBridge over real Kafka (integration)', () => {
  let store: KafkaTranscriptStore;

  beforeAll(() => {
    store = new KafkaTranscriptStore({ kafka: makeKafka() });
  });
  afterAll(async () => {
    await store.close();
  });

  it.each([false, true])(
    'catches up a user turn appended BEFORE the bridge starts (first-turn; gated response: %s)',
    async (gatedResponse) => {
      // The pre-connect first-turn case against real Kafka: the user.message is
      // persisted BEFORE the bridge starts. A from-now tail would seek past it and
      // never drive it; the catch-up read picks it up because it has no agent event
      // after it. No pre-join sleep is needed — the append precedes start.
      const { ws, ses } = uniqueIds('bridge_catchup');
      await createTopic(ws, ses);
      const registry = new TunnelRegistry();
      const runner = new FakeRunner(registry, RUNNER_ID, ws);
      runner.connect();
      const uId = 'evt_user_catchup';
      let releaseResponse!: () => void;
      const beforeResponse = new Promise<void>((resolve) => {
        releaseResponse = resolve;
      });
      runner.scriptTurn(uId, {
        lines: [{ type: 'response.created' }, { type: 'response.completed' }],
        ...(gatedResponse ? { beforeResponse } : {}),
      });
      await store.append(ws, ses, [userEvent(ws, ses, uId, 'first turn before connect')]);
      const bridge = new SessionEventBridge({
        workspaceId: ws,
        sessionId: ses,
        runnerId: RUNNER_ID,
        store,
        registry,
      });
      bridge.start();
      try {
        await waitFor(() => runner.turns.some((t) => t['id'] === uId));
        // In the gated case the runner has accepted the turn, but cannot send
        // output until the assertion's first real Kafka snapshot has ended.
        const persisted = await waitForAgentEvents(
          store,
          ws,
          ses,
          2,
          gatedResponse ? releaseResponse : undefined,
        );
        const kinds = persisted
          .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
          .map((e) => e.kind);
        expect(kinds).toEqual(['response.created', 'response.completed']);
      } finally {
        releaseResponse();
        await bridge.stop();
        await deleteTopic(ws, ses);
      }
    },
    30000,
  );

  it('drives a new user turn and persists the streamed agent events (single writer)', async () => {
    const { ws, ses } = uniqueIds('bridge_drive');
    await createTopic(ws, ses);
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID, ws);
    runner.connect();
    const uId = 'evt_user_drive';
    runner.scriptTurn(uId, {
      lines: [{ type: 'response.created' }, { type: 'response.completed' }],
    });
    const bridge = new SessionEventBridge({
      workspaceId: ws,
      sessionId: ses,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    bridge.start();
    try {
      // Catch-up (over the empty topic) draws the boundary, then the live tail
      // resumes at head+1. Let that resume seek + join before appending the live
      // turn so this test exercises the LIVE path (the catch-up path is covered by
      // the dedicated first-turn test above).
      await new Promise((resolve) => setTimeout(resolve, 5000));
      await store.append(ws, ses, [userEvent(ws, ses, uId, 'hi')]);

      await waitFor(() => runner.turns.some((t) => t['id'] === uId));

      // The agent events were persisted by the bridge (single writer, harness).
      const persisted = await waitForAgentEvents(store, ws, ses, 2);
      const kinds = persisted
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
        .map((e) => e.kind);
      expect(kinds).toEqual(['response.created', 'response.completed']);
    } finally {
      await bridge.stop();
      await deleteTopic(ws, ses);
    }
  }, 30000);

  it('does NOT re-forward an already-answered user turn when a fresh bridge starts (reconnect)', async () => {
    const { ws, ses } = uniqueIds('bridge_noreplay');
    await createTopic(ws, ses);
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID, ws);
    runner.connect();
    const answered = 'evt_user_answered';
    runner.scriptTurn(answered, { lines: [{ type: 'a.1' }, { type: 'a.2' }] });

    // First bridge generation: drive + answer the turn fully.
    const first = new SessionEventBridge({
      workspaceId: ws,
      sessionId: ses,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    first.start();
    try {
      // Catch-up over the empty topic, then let the live tail resume at head+1
      // before appending the turn this generation will answer.
      await new Promise((resolve) => setTimeout(resolve, 5000));
      await store.append(ws, ses, [userEvent(ws, ses, answered, 'answer me')]);
      await waitFor(() => runner.turns.filter((t) => t['id'] === answered).length === 1);

      // Confirm the two agent events landed before tearing the first bridge down.
      await waitForAgentEvents(store, ws, ses, 2);
    } finally {
      await first.stop();
    }
    const turnsAfterFirst = runner.turns.filter((t) => t['id'] === answered).length;
    expect(turnsAfterFirst).toBe(1);

    // Fresh bridge (newest-wins reconnect): catch-up sees the answered turn has
    // agent events after it, so it is NOT re-driven; the live tail then resumes at
    // head+1.
    const second = new SessionEventBridge({
      workspaceId: ws,
      sessionId: ses,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    second.start();
    try {
      // Let catch-up run + the new tail resume. A from-beginning replay would have
      // re-driven the answered turn within this window; catch-up must not.
      await new Promise((resolve) => setTimeout(resolve, 4000));
      expect(runner.turns.filter((t) => t['id'] === answered).length).toBe(turnsAfterFirst);

      // A genuinely new turn after the reconnect IS still driven.
      const next = 'evt_user_next';
      runner.scriptTurn(next, { lines: [{ type: 'b.1' }] });
      await store.append(ws, ses, [userEvent(ws, ses, next, 'new')]);
      await waitFor(() => runner.turns.some((t) => t['id'] === next));
      // Still exactly one occurrence of the answered turn — no replay.
      expect(runner.turns.filter((t) => t['id'] === answered).length).toBe(1);
    } finally {
      await second.stop();
      await deleteTopic(ws, ses);
    }
  }, 40000);
});
