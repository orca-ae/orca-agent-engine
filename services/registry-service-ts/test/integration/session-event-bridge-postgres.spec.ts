// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Real-backend, BUS-FREE integration spec for the owner-pod session event
// bridge: the registry as the in-process single writer on the
// Postgres transcript backend, no Kafka anywhere in this file (no `kafkajs`
// import, no broker of any kind).
//
// This is the Postgres sibling of session-event-bridge-kafka.spec.ts, proving
// the exact same single-writer / persist-before-forward invariant against the
// REAL `PostgresTranscriptStore` instead of Kafka:
//   - a `user.*` event already persisted BEFORE the bridge starts, with no agent
//     event after it, IS caught up and driven (the first-turn case a from-now
//     tail would have dropped); and
//   - a turn a prior owner generation already ANSWERED (its agent events sit
//     after it) is NOT re-forwarded as a brand-new turn on a fresh bridge
//     (reconnect).
//
// `SessionEventBridge` itself is 100% backend-agnostic — it only calls
// `TranscriptStore.append/read/tail` (see src/tunnel/session-event-bridge.ts).
// This spec exists to PROVE that on the wire, not just assert it by reading the
// source: the bridge, the tunnel registry/transport, and the transcript store
// here are all real production code; only the runner end of the tunnel is
// faked (an in-process ws peer speaking the tunnel frame protocol — network-
// free, the same approach the Kafka spec uses).
//
// Requires a live Postgres reachable at TRANSCRIPT_STORE_DATABASE_URL (or
// DATABASE_URL) — the same database the rest of this package's integration
// suite already uses (see test/integration/setup.ts). No Kafka broker is
// started or required for this spec to pass.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Pool } from 'pg';
import {
  FrameKind,
  decodeFrame,
  encodeFrame,
  decodeBody,
  type Frame,
  type RequestFrame,
  type HelloFrame,
} from '@orca/harness-tunnel';
import type { Event, PostgresTranscriptStore } from '@orca/transcript-store';
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
import { buildTestPostgresStore, deleteTranscriptRowsForWorkspace } from './setup.js';

const RUNNER_ID = 'runner_token_bridgepostgresbridgepostgresbrpg';

function uniqueIds(prefix: string): { ws: string; ses: string } {
  const suffix = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  return { ws: `ws_${prefix}_${suffix}`, ses: `ses_${prefix}_${suffix}` };
}

/**
 * Poll until `predicate` holds.
 *
 * The bound is generous on purpose. What is being waited on is a REAL cross-process
 * delivery — a turn travelling through Postgres and the bridge — and the budget has
 * to cover the slowest CI runner, not a quiet laptop. At the original 15s (inside a
 * 20s per-test timeout, so less than that once setup had run) this spec failed twice
 * on two DIFFERENT tests while two integration suites shared a runner: a bound that
 * tight tests the runner's spare capacity as much as the code.
 *
 * A late failure here is still a real failure — nothing is retried and nothing is
 * swallowed — it just no longer fires because the machine was busy.
 */
async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 45_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error('waitFor: condition not met within timeout');
    await new Promise<void>((resolve) => setTimeout(resolve, 25));
  }
}

async function readEvents(
  store: PostgresTranscriptStore,
  workspaceId: string,
  sessionId: string,
): Promise<Event[]> {
  const events: Event[] = [];
  for await (const event of store.read(workspaceId, sessionId, {
    fromCursor: '',
    maxEvents: 0,
    subpath: '',
  })) {
    events.push(event);
  }
  return events;
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

// ── Minimal in-process fake runner (a ws peer speaking the tunnel protocol) ──
// Identical to session-event-bridge-kafka.spec.ts's fake runner — only the
// transcript backend differs between the two specs.

interface TurnScript {
  lines: Array<Record<string, unknown>>;
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
    this.handleTurn(frame);
  }

  private handleTurn(frame: RequestFrame): void {
    const body = decodeRequestBody(frame);
    this.turns.push(body);
    const id = typeof body['id'] === 'string' ? body['id'] : '';
    const script = this.scripts.get(id) ?? { lines: [] };
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

describe('SessionEventBridge over real Postgres, bus-free (integration)', () => {
  let pool: Pool;
  let store: PostgresTranscriptStore;

  beforeAll(async () => {
    ({ pool, store } = await buildTestPostgresStore());
  });
  afterAll(async () => {
    await store?.close();
  });

  it('catches up a user turn appended BEFORE the bridge starts (first-turn)', async () => {
    // The pre-connect first-turn case against real Postgres: the user.message is
    // persisted BEFORE the bridge starts. A from-now tail would seek past it and
    // never drive it; the catch-up read picks it up because it has no agent event
    // after it. No pre-join sleep is needed — the append precedes start.
    const { ws, ses } = uniqueIds('bridge_catchup');
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID, ws);
    runner.connect();
    const uId = 'evt_user_catchup';
    runner.scriptTurn(uId, {
      lines: [{ type: 'response.created' }, { type: 'response.completed' }],
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
      let persisted: Event[] = [];
      await waitFor(async () => {
        persisted = await readEvents(store, ws, ses);
        return persisted.filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 2;
      });
      const kinds = persisted
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
        .map((e) => e.kind);
      expect(kinds).toEqual(['response.created', 'response.completed']);
    } finally {
      await bridge.stop();
      await deleteTranscriptRowsForWorkspace(pool, ws);
    }
  }, 60_000);

  it('drives a new user turn and persists the streamed agent events (single writer)', async () => {
    const { ws, ses } = uniqueIds('bridge_drive');
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
      // Catch-up (over the empty session) draws the boundary, then the live tail
      // resumes at head+1. Let that resume start polling before appending the
      // live turn so this test exercises the LIVE path (the catch-up path is
      // covered by the dedicated first-turn test above). Postgres `tail` is a
      // local poll loop (tailPollIntervalMs=50, no broker consumer-group join),
      // so a short settle is enough — unlike the Kafka sibling's 5s join wait.
      await new Promise((resolve) => setTimeout(resolve, 500));
      await store.append(ws, ses, [userEvent(ws, ses, uId, 'hi')]);

      await waitFor(() => runner.turns.some((t) => t['id'] === uId));

      // The agent events were persisted by the bridge (single writer, harness).
      let persisted: Event[] = [];
      await waitFor(async () => {
        persisted = await readEvents(store, ws, ses);
        return persisted.filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 2;
      });
      const kinds = persisted
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
        .map((e) => e.kind);
      expect(kinds).toEqual(['response.created', 'response.completed']);
    } finally {
      await bridge.stop();
      await deleteTranscriptRowsForWorkspace(pool, ws);
    }
  }, 60_000);

  it('does NOT re-forward an already-answered user turn when a fresh bridge starts (reconnect)', async () => {
    const { ws, ses } = uniqueIds('bridge_noreplay');
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
      // Catch-up over the empty session, then let the live tail resume at
      // head+1 before appending the turn this generation will answer.
      await new Promise((resolve) => setTimeout(resolve, 500));
      await store.append(ws, ses, [userEvent(ws, ses, answered, 'answer me')]);
      await waitFor(() => runner.turns.filter((t) => t['id'] === answered).length === 1);

      // Confirm the two agent events landed before tearing the first bridge down.
      let after: Event[] = [];
      await waitFor(async () => {
        after = await readEvents(store, ws, ses);
        return after.filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 2;
      });
    } finally {
      await first.stop();
    }
    const turnsAfterFirst = runner.turns.filter((t) => t['id'] === answered).length;
    expect(turnsAfterFirst).toBe(1);

    // Fresh bridge (newest-wins reconnect): catch-up sees the answered turn has
    // agent events after it, so it is NOT re-driven; the live tail then resumes
    // at head+1.
    const second = new SessionEventBridge({
      workspaceId: ws,
      sessionId: ses,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    second.start();
    try {
      // Let catch-up run + the new tail resume. A from-beginning replay would
      // have re-driven the answered turn within this window; catch-up must not.
      await new Promise((resolve) => setTimeout(resolve, 500));
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
      await deleteTranscriptRowsForWorkspace(pool, ws);
    }
  }, 60_000);
});
