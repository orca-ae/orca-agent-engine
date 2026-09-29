// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for snapshot delivery over the runner tunnel + its composition into
// the owner-pod connect handshake.
//
// At session start the registry composes a credential-free snapshot and must
// hand it to the runner BEFORE the first turn is driven. Delivery is a sibling of
// SessionRecovery: a streaming POST to the runner's snapshot route over the SAME
// tunnel the bridge drives turns on. The runner applies it (configures its model
// / provider / tools / egress) and acks 200.
//
// Everything is in-process: the REAL TunnelRegistry + TunnelTransport (network-
// free), a FAKE runner (a ws peer speaking the tunnel frame protocol), and an
// in-memory TranscriptStore with real append/tail ordering. The assertions pin:
//   - the snapshot is delivered to the runner (the exact credential-free body);
//   - delivery happens on connect, BEFORE recovery and BEFORE the first turn;
//   - a delivery failure (offline / non-2xx / drop) is contained — the bridge
//     still starts, but refuses turns until delivery self-heals on reconnect;
//   - a changed guardrail fold is delivered before the next turn, while an
//     unchanged fold does not restart the runner harness;
//   - the manager wires snapshot → recovery → bridge in that order.

import { describe, it, expect } from 'vitest';
import { parseSnapshotBody } from '../../../session-runner/src/snapshot.js';
import { buildSessionStartInput } from '../../../session-runner/src/harness/provider.js';
import {
  FrameKind,
  decodeFrame,
  encodeFrame,
  decodeBody,
  type Frame,
  type HelloFrame,
  type RequestFrame,
} from '@orca/harness-tunnel';
import type { Event, TranscriptStore, ReadOptions, TailOptions } from '@orca/transcript-store';
import {
  TunnelRegistry,
  type RegistrySession,
  type RegistryWebSocketLike,
} from '../../src/tunnel/tunnel-registry.js';
import { httpEventToProto } from '../../src/domain/events.js';
import {
  SessionSnapshotDelivery,
  RUNNER_SNAPSHOT_PATH,
  RUNNER_SESSION_HEADER,
  type SnapshotProvider,
} from '../../src/tunnel/session-snapshot-delivery.js';
import {
  SessionEventBridgeManager,
  RUNNER_TURN_PATH,
  type BoundSessionResolver,
} from '../../src/tunnel/session-event-bridge.js';
import { RUNNER_REPLAY_PATH } from '../../src/tunnel/session-recovery.js';
import { buildGatewayEgress } from '../../src/domain/credential-egress.js';
import { buildAgentSnapshot, type AgentSnapshot } from '../../src/domain/agent-snapshot.js';
import { assertSnapshotCredentialFree } from '../../src/domain/egress-credential-free.js';

const WORKSPACE_ID = 'ws_acme';
const SESSION_ID = 'ses_snapshot_1';
const RUNNER_ID = 'runner_token_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

function sampleSnapshot(): AgentSnapshot {
  return buildAgentSnapshot({
    agent: {
      model: { provider: 'anthropic', id: 'claude-opus-4' },
      system: 'You are a helpful agent.',
      tools: [{ type: 'agent_toolset' }, { type: 'mcp_toolset', mcp_server_name: 'github' }],
    },
    skills: [],
    provider: 'claude',
    egress: buildGatewayEgress({
      sessionId: SESSION_ID,
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      sessionJwt: 'eyJ.gateway.jwt',
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
      vaultByUrl: new Map([['https://github.example/mcp', 'vlt_gh']]),
    }),
  });
}

// ── In-memory transcript store with real append/tail ordering ──

class InMemoryTranscriptStore implements TranscriptStore {
  private readonly logs = new Map<string, Event[]>();
  private readonly waiters = new Map<string, Set<() => void>>();

  private key(workspaceId: string, sessionId: string): string {
    return `${workspaceId}/${sessionId}`;
  }

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    const key = this.key(workspaceId, sessionId);
    const log = this.logs.get(key) ?? [];
    this.logs.set(key, log);
    const ids: string[] = [];
    for (const event of events) {
      const stored: Event = { ...event, seq: log.length + 1 };
      log.push(stored);
      ids.push(stored.id);
    }
    this.wake(key);
    return ids;
  }

  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    const log = this.logs.get(this.key(workspaceId, sessionId)) ?? [];
    const from = opts.fromCursor === '' ? 0 : Number(opts.fromCursor);
    for (const event of log) {
      if (event.seq < from) continue;
      if (!subpathMatches(opts.subpath, event.subpath)) continue;
      yield { ...event };
    }
  }

  async *tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event> {
    const key = this.key(workspaceId, sessionId);
    let cursor =
      opts.fromCursor === '' ? (this.logs.get(key)?.length ?? 0) : Number(opts.fromCursor) - 1;
    const signal = opts.signal;
    for (;;) {
      if (signal?.aborted) return;
      const log = this.logs.get(key) ?? [];
      let advanced = false;
      for (const event of log) {
        if (event.seq <= cursor) continue;
        if (!subpathMatches(opts.subpath, event.subpath)) {
          cursor = event.seq;
          continue;
        }
        cursor = event.seq;
        advanced = true;
        yield { ...event };
        if (signal?.aborted) return;
      }
      if (advanced) continue;
      await this.parkForAppend(key, signal);
    }
  }

  async archive(): Promise<void> {}
  async close(): Promise<void> {}

  snapshot(workspaceId: string, sessionId: string): Event[] {
    return (this.logs.get(this.key(workspaceId, sessionId)) ?? []).map((e) => ({ ...e }));
  }

  private wake(key: string): void {
    const set = this.waiters.get(key);
    if (set === undefined) return;
    for (const w of [...set]) w();
  }

  private parkForAppend(key: string, signal: AbortSignal | undefined): Promise<void> {
    return new Promise<void>((resolve) => {
      const set = this.waiters.get(key) ?? new Set<() => void>();
      this.waiters.set(key, set);
      const wake = (): void => {
        set.delete(wake);
        if (signal !== undefined) signal.removeEventListener('abort', wake);
        resolve();
      };
      set.add(wake);
      if (signal !== undefined) {
        if (signal.aborted) {
          wake();
          return;
        }
        signal.addEventListener('abort', wake, { once: true });
      }
    });
  }
}

function subpathMatches(filter: string, subpath: string): boolean {
  if (filter === '*') return true;
  if (filter === '') return subpath === '';
  return subpath === filter;
}

// ── Fake in-process runner that records snapshot/replay/turn requests by path ──

interface RecordedRequest {
  path: string;
  cursor: string;
  body: Array<Record<string, unknown>>;
}

class FakeRunner {
  /** Every request the owner pod pushed, in arrival order, tagged by path. */
  readonly requests: RecordedRequest[] = [];
  /** Status to answer a snapshot POST with (default 200). */
  snapshotStatus = 200;
  /** When true, drop the tunnel on the snapshot POST instead of acking. */
  dropOnSnapshot = false;
  private readonly socket: RunnerSocket;
  private connected = false;

  constructor(
    private readonly registry: TunnelRegistry,
    private readonly runnerId: string,
  ) {
    this.socket = new RunnerSocket((raw) => this.onFrame(raw));
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
      owner: WORKSPACE_ID,
    });
    this.connected = true;
    this.startOutboundDrain(session);
  }

  disconnect(): void {
    if (!this.connected) return;
    this.registry.deregister(this.runnerId);
    this.connected = false;
  }

  /** Paths recorded, in order — lets a spec assert snapshot precedes replay/turn. */
  get pathOrder(): string[] {
    return this.requests.map((r) => r.path);
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

  private onFrame(raw: string): void {
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
    this.handleRequest(frame);
  }

  /** Hook for subclasses to capture per-request headers. Default no-op. */
  protected recordHeaders(_frame: RequestFrame): void {
    /* overridden by header-capturing subclasses */
  }

  private handleRequest(frame: RequestFrame): void {
    const headers = new Map((frame.headers ?? []).map(([k, v]) => [k.toLowerCase(), v]));
    const cursor = headers.get('x-orca-resume-cursor') ?? '';
    this.requests.push({ path: frame.path, cursor, body: decodeNdjsonBody(frame) });
    this.recordHeaders(frame);

    if (frame.path === RUNNER_SNAPSHOT_PATH && this.dropOnSnapshot) {
      // Send the head, then drop the tunnel mid-ack (deferred so the push is sent).
      this.route(
        encodeFrame({
          kind: FrameKind.ResponseHead,
          id: frame.id,
          status: 200,
          headers: [['content-type', 'application/json']],
        }),
      );
      setTimeout(() => this.disconnect(), 5);
      return;
    }

    const status = frame.path === RUNNER_SNAPSHOT_PATH ? this.snapshotStatus : 200;
    this.route(
      encodeFrame({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status,
        headers: [['content-type', 'application/json']],
      }),
    );
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

function decodeNdjsonBody(frame: RequestFrame): Array<Record<string, unknown>> {
  if (frame.body === null || frame.body === undefined) return [];
  const bytes = decodeBody(frame.body, frame.encoding ?? 'utf-8');
  const text = Buffer.from(bytes).toString('utf8');
  return text
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => l.length > 0)
    .map((l) => JSON.parse(l) as Record<string, unknown>);
}

async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (predicate()) return;
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 5));
  }
}

// ── SessionSnapshotDelivery (the tunnel push) ──

describe('SessionSnapshotDelivery', () => {
  it('pushes the credential-free snapshot to the runner and reports delivered', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();

    const provider: SnapshotProvider = { resolve: async () => sampleSnapshot() };
    const delivery = new SessionSnapshotDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider,
    });

    const outcome = await delivery.deliver();
    expect(outcome.delivered).toBe(true);
    expect(outcome.skipped).toBe(false);

    const snapReq = runner.requests.find((r) => r.path === RUNNER_SNAPSHOT_PATH)!;
    expect(snapReq).toBeDefined();
    // The body is the snapshot JSON (one object).
    expect(snapReq.body).toHaveLength(1);
    const delivered = snapReq.body[0] as unknown as AgentSnapshot;
    expect(delivered.provider).toBe('claude');
    expect(delivered.model).toEqual({ provider: 'anthropic', id: 'claude-opus-4' });
    expect(delivered.allowed_mcp_server_names).toEqual(['github']);
    expect(delivered.egress.mode).toBe('gateway');
    // Credential-free on the wire — STRUCTURALLY (the delivery path enforces this
    // invariant at the trust boundary before the push; the body that landed on the
    // runner conforms exactly to the secret-free snapshot shape).
    expect(() => assertSnapshotCredentialFree(delivered)).not.toThrow();
  });

  it('delivers managed Codex accounting and custom callbacks through the credential guard to the runner parser', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const snapshot = buildAgentSnapshot({
      agent: {
        model: { provider: 'openai', id: 'gpt-5.4' },
        system: 'Use lookup.',
        tools: [
          {
            type: 'agent_toolset',
            default_config: { enabled: false },
            configs: {
              read: { enabled: true, permission_policy: 'always_allow' },
              write: { enabled: true, permission_policy: 'always_ask' },
            },
          },
          {
            type: 'mcp_toolset',
            mcp_server_name: 'github',
            default_config: { permission_policy: 'always_allow' },
            configs: [{ name: 'delete', enabled: false }],
          },
          {
            type: 'custom',
            name: 'lookup',
            description: 'Look up a record',
            input_schema: { type: 'object', properties: { query: { type: 'string' } } },
          },
        ],
      },
      provider: 'codex-sdk',
      skills: [],
      egress: sampleSnapshot().egress,
    });
    snapshot.managed_resources = { version: 1, revision: 'a'.repeat(64) };
    snapshot.request_guardrails_owner = 'registry';
    const delivery = new SessionSnapshotDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: { resolve: async () => snapshot },
    });
    expect((await delivery.deliver()).delivered).toBe(true);
    const wire = runner.requests.find((request) => request.path === RUNNER_SNAPSHOT_PATH)!.body[0];
    expect(wire).toEqual(snapshot);
    const parsed = parseSnapshotBody(Buffer.from(JSON.stringify(wire)));
    expect(parsed.request_guardrails_owner).toBe('registry');
    expect(parsed.custom_tools).toEqual(snapshot.custom_tools);
    expect(parsed.managed_resources).toEqual(snapshot.managed_resources);
    const permissions = buildSessionStartInput(parsed, {
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
    }).toolPermissions!;
    expect(permissions.policyFor('mcp__orca__read')).toBe('always_allow');
    expect(permissions.policyFor('mcp__orca__write')).toBe('always_ask');
    expect(permissions.policyFor('mcp__orca__bash')).toBe('always_deny');
    expect(permissions.policyFor('mcp__github__search')).toBe('always_allow');
    expect(permissions.policyFor('mcp__github__delete')).toBe('always_deny');
    expect(permissions.policyFor('unclassified')).toBe('always_ask');
  });

  it('throws (does not push) when the resolver yields a non-credential-free snapshot', async () => {
    // Defense-in-depth at the trust boundary: a snapshot carrying a smuggled
    // secret-shaped field — one a string-match for known patterns would miss —
    // must NOT leave the registry. Delivery throws rather than pushing it.
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const tainted = sampleSnapshot();
    if (tainted.egress.mode !== 'gateway') throw new Error('unreachable');
    (tainted.egress.gateway.mcp_servers['github']! as unknown as Record<string, unknown>).api_key =
      'leaked-but-unpatterned';
    const delivery = new SessionSnapshotDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: { resolve: async () => tainted },
    });
    await expect(delivery.deliver()).rejects.toThrow(/not credential-free/);
    // Nothing was pushed to the runner.
    expect(runner.requests.find((r) => r.path === RUNNER_SNAPSHOT_PATH)).toBeUndefined();
  });

  it('stamps the session header on the snapshot push', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunnerCapturingHeaders(registry, RUNNER_ID);
    runner.connect();
    const delivery = new SessionSnapshotDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: { resolve: async () => sampleSnapshot() },
    });
    await delivery.deliver();
    expect(runner.lastSessionHeader).toBe(SESSION_ID);
  });

  it('skips delivery (not an error) when the resolver returns null', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const delivery = new SessionSnapshotDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: { resolve: async () => null },
    });
    const outcome = await delivery.deliver();
    expect(outcome.skipped).toBe(true);
    expect(outcome.delivered).toBe(false);
    expect(runner.requests.find((r) => r.path === RUNNER_SNAPSHOT_PATH)).toBeUndefined();
  });

  it('contains a non-2xx ack: reports not delivered, does not throw', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.snapshotStatus = 503;
    runner.connect();
    const delivery = new SessionSnapshotDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: { resolve: async () => sampleSnapshot() },
    });
    const outcome = await delivery.deliver();
    expect(outcome.delivered).toBe(false);
    expect(outcome.skipped).toBe(false);
  });

  it('contains an offline runner: reports not delivered, does not throw', async () => {
    const registry = new TunnelRegistry();
    // No runner connected.
    const delivery = new SessionSnapshotDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: { resolve: async () => sampleSnapshot() },
    });
    const outcome = await delivery.deliver();
    expect(outcome.delivered).toBe(false);
  });

  it('propagates a resolver throw (a snapshot that could not be built is a fault)', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const delivery = new SessionSnapshotDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: {
        resolve: async () => {
          throw new Error('skill_version sklv_x not found');
        },
      },
    });
    await expect(delivery.deliver()).rejects.toThrow(/skill_version/);
  });
});

// ── Manager composition: snapshot → recovery → bridge, in that order ──

describe('SessionEventBridgeManager with snapshot delivery', () => {
  const resolver: BoundSessionResolver = {
    resolveBoundSession: async (runnerId) =>
      runnerId === RUNNER_ID ? { workspaceId: WORKSPACE_ID, sessionId: SESSION_ID } : null,
  };

  it('delivers the snapshot BEFORE recovery and BEFORE the first turn on connect', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();

    // A pending user turn already in the transcript — the bridge catch-up drives it.
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: { type: 'user.message', id: 'evt_user_1', content: [{ type: 'text', text: 'hi' }] },
      }),
    ]);

    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver,
      snapshotProvider: { resolve: async () => sampleSnapshot() },
    });

    await manager.onRunnerConnect(RUNNER_ID, {});
    // Wait until the bridge catch-up drove the turn (a turn POST landed).
    await waitFor(() => runner.pathOrder.includes(RUNNER_TURN_PATH));

    const order = runner.pathOrder;
    const snapIdx = order.indexOf(RUNNER_SNAPSHOT_PATH);
    const replayIdx = order.indexOf(RUNNER_REPLAY_PATH);
    const turnIdx = order.indexOf(RUNNER_TURN_PATH);

    expect(snapIdx).toBeGreaterThanOrEqual(0);
    expect(replayIdx).toBeGreaterThanOrEqual(0);
    expect(turnIdx).toBeGreaterThanOrEqual(0);
    // snapshot first, then recovery replay, then the driven turn.
    expect(snapIdx).toBeLessThan(replayIdx);
    expect(replayIdx).toBeLessThan(turnIdx);

    await manager.stopAll();
  });

  it('starts the bridge but fails turns closed until snapshot delivery self-heals', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.snapshotStatus = 500; // snapshot push gets a non-2xx
    runner.connect();

    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: { type: 'user.message', id: 'evt_user_1', content: [{ type: 'text', text: 'hi' }] },
      }),
    ]);

    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver,
      snapshotProvider: { resolve: async () => sampleSnapshot() },
    });

    await manager.onRunnerConnect(RUNNER_ID, {});
    // The bridge starts so reconnect recovery remains available, but the pending
    // turn is NOT allowed past a policy snapshot the runner did not accept.
    await waitFor(
      () => runner.requests.filter((request) => request.path === RUNNER_SNAPSHOT_PATH).length >= 2,
    );
    expect(manager.size).toBe(1);
    expect(runner.pathOrder).not.toContain(RUNNER_TURN_PATH);

    // A healthy reconnect re-delivers the snapshot, then catch-up drives the
    // still-unanswered turn. No user input was silently lost.
    runner.disconnect();
    runner.snapshotStatus = 200;
    runner.connect();
    await manager.onRunnerConnect(RUNNER_ID, {});
    await waitFor(() => runner.pathOrder.includes(RUNNER_TURN_PATH));

    await manager.stopAll();
  });

  it('re-delivers a changed guardrail fold before the next turn only', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    let snapshot = sampleSnapshot();

    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver,
      snapshotProvider: { resolve: async () => snapshot },
    });

    await manager.onRunnerConnect(RUNNER_ID, {});
    await store.append(WORKSPACE_ID, SESSION_ID, [userMessage('evt_user_1', 'first')]);
    await waitFor(
      () => runner.requests.filter((request) => request.path === RUNNER_TURN_PATH).length === 1,
    );
    expect(runner.requests.filter((request) => request.path === RUNNER_SNAPSHOT_PATH)).toHaveLength(
      1,
    );

    snapshot = {
      ...snapshot,
      guardrails: [
        {
          id: 'grd_email',
          name: 'deny email',
          tier: 'workspace',
          phases: ['request'],
          rule: {
            kind: 'builtin',
            builtin: 'deny_pii_in_llm_request',
            params: { pii_types: ['email'] },
          },
          stateful: false,
        },
      ],
      guardrail_state: {},
    };
    await store.append(WORKSPACE_ID, SESSION_ID, [userMessage('evt_user_2', 'second')]);
    await waitFor(
      () => runner.requests.filter((request) => request.path === RUNNER_TURN_PATH).length === 2,
    );

    const secondTurnIndex = runner.pathOrder.lastIndexOf(RUNNER_TURN_PATH);
    const refreshedSnapshotIndex = runner.pathOrder.lastIndexOf(RUNNER_SNAPSHOT_PATH);
    expect(refreshedSnapshotIndex).toBeGreaterThan(runner.pathOrder.indexOf(RUNNER_TURN_PATH));
    expect(refreshedSnapshotIndex).toBeLessThan(secondTurnIndex);
    expect(runner.requests.filter((request) => request.path === RUNNER_SNAPSHOT_PATH)).toHaveLength(
      2,
    );

    await manager.stopAll();
  });

  it('refuses a turn when a changed guardrail fold cannot be delivered', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    let snapshot = sampleSnapshot();

    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver,
      snapshotProvider: { resolve: async () => snapshot },
    });
    await manager.onRunnerConnect(RUNNER_ID, {});
    await store.append(WORKSPACE_ID, SESSION_ID, [userMessage('evt_user_1', 'first')]);
    await waitFor(
      () => runner.requests.filter((request) => request.path === RUNNER_TURN_PATH).length === 1,
    );

    snapshot = {
      ...snapshot,
      guardrails: [
        {
          id: 'grd_changed',
          name: 'changed request policy',
          tier: 'workspace',
          phases: ['request'],
          rule: {
            kind: 'builtin',
            builtin: 'deny_pii_in_llm_request',
            params: { pii_types: ['phone'] },
          },
          stateful: false,
        },
      ],
    };
    runner.snapshotStatus = 500;
    await store.append(WORKSPACE_ID, SESSION_ID, [userMessage('evt_user_2', 'second')]);
    await waitFor(
      () => runner.requests.filter((request) => request.path === RUNNER_SNAPSHOT_PATH).length === 2,
    );
    expect(runner.requests.filter((request) => request.path === RUNNER_TURN_PATH)).toHaveLength(1);

    await manager.stopAll();
  });

  it('is a no-op for an unbound runner (no snapshot pushed)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, 'runner_token_unbound_bbbbbbbbbbbbbbbbbbbbbbbbbbbb');
    runner.connect();

    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver, // only RUNNER_ID resolves; this runner is unbound
      snapshotProvider: { resolve: async () => sampleSnapshot() },
    });

    await manager.onRunnerConnect('runner_token_unbound_bbbbbbbbbbbbbbbbbbbbbbbbbbbb', {});
    expect(runner.requests.find((r) => r.path === RUNNER_SNAPSHOT_PATH)).toBeUndefined();
    expect(manager.size).toBe(0);
  });

  it('works with no snapshotProvider wired (delivery is skipped, bridge still runs)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();

    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    await manager.onRunnerConnect(RUNNER_ID, {});
    expect(manager.size).toBe(1);
    // No snapshot push when no provider is wired.
    await waitFor(() => runner.pathOrder.includes(RUNNER_REPLAY_PATH));
    expect(runner.requests.find((r) => r.path === RUNNER_SNAPSHOT_PATH)).toBeUndefined();

    await manager.stopAll();
  });
});

function userMessage(id: string, text: string): Event {
  return httpEventToProto({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    producedBy: 'client',
    idempotencyKey: '',
    input: { type: 'user.message', id, content: [{ type: 'text', text }] },
  });
}

/** A FakeRunner variant that captures the session header off the snapshot POST. */
class FakeRunnerCapturingHeaders extends FakeRunner {
  lastSessionHeader: string | undefined;
  protected override recordHeaders(frame: RequestFrame): void {
    const headers = new Map((frame.headers ?? []).map(([k, v]) => [k.toLowerCase(), v]));
    if (frame.path === RUNNER_SNAPSHOT_PATH) {
      this.lastSessionHeader = headers.get(RUNNER_SESSION_HEADER.toLowerCase());
    }
  }
}
