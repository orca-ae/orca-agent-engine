// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the owner-pod SKILLS delivery over the runner tunnel + its
// composition into the connect handshake (skills pushed + acked BEFORE the snapshot).
//
// The colocated runner holds no `@orca/skill-store` + no object-store credentials, so
// the owner pod opens the session's pinned Skill bundles from `@orca/skill-store` and
// PUSHES their verified bytes down the tunnel; the runner materializes them as a native
// `--plugin-dir` plugin. Delivery is a sibling of SessionSnapshotDelivery: a streaming
// POST to the runner's skills route over the SAME tunnel, sequenced FIRST (before the
// snapshot). Everything is in-process: the REAL TunnelRegistry + TunnelTransport, a FAKE
// runner (a ws peer speaking the tunnel frame protocol), and an InMemorySkillStore.

import { describe, it, expect } from 'vitest';
import {
  FrameKind,
  decodeFrame,
  encodeFrame,
  decodeBody,
  type Frame,
  type HelloFrame,
  type RequestFrame,
} from '@orca/harness-tunnel';
import { InMemorySkillStore } from '@orca/skill-store';
import type { Event, TranscriptStore, ReadOptions, TailOptions } from '@orca/transcript-store';
import {
  TunnelRegistry,
  type RegistrySession,
  type RegistryWebSocketLike,
} from '../../src/tunnel/tunnel-registry.js';
import { httpEventToProto } from '../../src/domain/events.js';
import {
  SessionSkillsDelivery,
  RUNNER_SKILLS_PATH,
  RUNNER_SKILLS_PLUGIN_DIRNAME,
  type SkillsProvider,
} from '../../src/tunnel/session-skills-delivery.js';
import {
  SessionEventBridgeManager,
  RUNNER_TURN_PATH,
  type BoundSessionResolver,
} from '../../src/tunnel/session-event-bridge.js';
import { RUNNER_REPLAY_PATH } from '../../src/tunnel/session-recovery.js';
import { RUNNER_SNAPSHOT_PATH } from '../../src/tunnel/session-snapshot-delivery.js';
import type { PreparedSkillDescriptor } from '../../src/contracts/internal.contract.js';
import { buildGatewayEgress } from '../../src/domain/credential-egress.js';
import { buildAgentSnapshot, type AgentSnapshot } from '../../src/domain/agent-snapshot.js';

const WORKSPACE_ID = 'ws_acme';
const SESSION_ID = 'ses_skills_1';
const RUNNER_ID = 'runner_token_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

/** Seed one Skill bundle into the store and return its exact-pin descriptor. */
async function seedSkill(
  store: InMemorySkillStore,
  name: string,
  files: Array<{ path: string; content: Buffer; mode?: number }>,
): Promise<PreparedSkillDescriptor> {
  const versionId = `sklv_${name}`;
  const record = await store.put(
    WORKSPACE_ID,
    versionId,
    files.map((f) => ({
      path: f.path,
      content: f.content,
      ...(f.mode !== undefined ? { mode: f.mode } : {}),
    })),
  );
  return {
    id: versionId,
    skill_id: `skl_${name}`,
    source: 'anthropic',
    version_identifier: '1',
    name,
    description: `${name} skill.`,
    entrypoint: 'SKILL.md',
    package_sha256: record.sha256,
    package_size_bytes: record.sizeBytes,
  };
}

function skillsProviderOf(skills: PreparedSkillDescriptor[]): SkillsProvider {
  return { resolveSkillBundles: async () => skills };
}

function sampleSnapshot(): AgentSnapshot {
  return buildAgentSnapshot({
    agent: {
      model: { provider: 'anthropic', id: 'claude-opus-4' },
      system: 'You are a helpful agent.',
      tools: [{ type: 'agent_toolset' }],
    },
    skills: [],
    provider: 'claude',
    egress: buildGatewayEgress({
      sessionId: SESSION_ID,
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      sessionJwt: 'eyJ.gateway.jwt',
      mcpServers: [],
      vaultByUrl: new Map(),
    }),
  });
}

// ── Fake in-process runner that records skills/snapshot/turn requests by path ──

interface RecordedRequest {
  path: string;
  body: Array<Record<string, unknown>>;
}

class FakeRunner {
  readonly requests: RecordedRequest[] = [];
  /** Status to answer a skills POST with (default 200). */
  skillsStatus = 200;
  replayStatus = 200;
  /** When true, drop the tunnel mid-ack on the skills POST. */
  dropOnSkills = false;
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

  private handleRequest(frame: RequestFrame): void {
    this.requests.push({ path: frame.path, body: decodeNdjsonBody(frame) });

    if (frame.path === RUNNER_SKILLS_PATH && this.dropOnSkills) {
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

    const status =
      frame.path === RUNNER_SKILLS_PATH
        ? this.skillsStatus
        : frame.path === RUNNER_REPLAY_PATH
          ? this.replayStatus
          : 200;
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
  return Buffer.from(bytes)
    .toString('utf8')
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

// ── SessionSkillsDelivery (the tunnel push) ──

describe('SessionSkillsDelivery', () => {
  it('opens bundles from the store and pushes the NDJSON manifest + files (delivered)', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const store = new InMemorySkillStore();
    const alpha = await seedSkill(store, 'alpha', [
      { path: 'SKILL.md', content: Buffer.from('# Alpha\n') },
      { path: 'scripts/run.py', content: Buffer.from('print(1)\n'), mode: 0o755 },
    ]);

    const delivery = new SessionSkillsDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: skillsProviderOf([alpha]),
      skillStore: store,
    });
    const outcome = await delivery.deliver();
    expect(outcome).toEqual({ skipped: false, delivered: true });

    const req = runner.requests.find((r) => r.path === RUNNER_SKILLS_PATH)!;
    expect(req).toBeDefined();
    // Line 1 = the manifest (target dir + names); then one line per file.
    const manifest = req.body[0]!;
    expect(manifest.type).toBe('skills_manifest');
    expect(manifest.dir).toBe(RUNNER_SKILLS_PLUGIN_DIRNAME);
    expect(manifest.skills).toEqual(['alpha']);
    const fileLines = req.body.slice(1);
    expect(fileLines.map((l) => l.path).sort()).toEqual(['SKILL.md', 'scripts/run.py']);
    for (const line of fileLines) {
      expect(line.type).toBe('skill_file');
      expect(line.skill).toBe('alpha');
      expect(typeof line.content_base64).toBe('string');
    }
    const skillMd = fileLines.find((l) => l.path === 'SKILL.md')!;
    expect(Buffer.from(String(skillMd.content_base64), 'base64').toString('utf8')).toBe(
      '# Alpha\n',
    );
  });

  it('skips delivery (not an error) when the session has no skills', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const delivery = new SessionSkillsDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: skillsProviderOf([]),
      skillStore: new InMemorySkillStore(),
    });
    const outcome = await delivery.deliver();
    expect(outcome).toEqual({ skipped: true, delivered: false });
    expect(runner.requests.find((r) => r.path === RUNNER_SKILLS_PATH)).toBeUndefined();
  });

  it('contains a non-2xx ack: reports not delivered, does not throw', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.skillsStatus = 500;
    runner.connect();
    const store = new InMemorySkillStore();
    const alpha = await seedSkill(store, 'alpha', [
      { path: 'SKILL.md', content: Buffer.from('# A\n') },
    ]);
    const delivery = new SessionSkillsDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: skillsProviderOf([alpha]),
      skillStore: store,
    });
    expect(await delivery.deliver()).toEqual({ skipped: false, delivered: false });
  });

  it('contains an offline runner: reports not delivered, does not throw', async () => {
    const registry = new TunnelRegistry();
    const store = new InMemorySkillStore();
    const alpha = await seedSkill(store, 'alpha', [
      { path: 'SKILL.md', content: Buffer.from('# A\n') },
    ]);
    const delivery = new SessionSkillsDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: skillsProviderOf([alpha]),
      skillStore: store,
    });
    expect(await delivery.deliver()).toEqual({ skipped: false, delivered: false });
  });

  it('contains a mid-push tunnel drop: reports not delivered, does not throw', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.dropOnSkills = true;
    runner.connect();
    const store = new InMemorySkillStore();
    const alpha = await seedSkill(store, 'alpha', [
      { path: 'SKILL.md', content: Buffer.from('# A\n') },
    ]);
    const delivery = new SessionSkillsDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: skillsProviderOf([alpha]),
      skillStore: store,
    });
    expect((await delivery.deliver()).delivered).toBe(false);
  });

  it('propagates a bundle open failure (a Skill that could not be opened is a fault)', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    // A descriptor whose bundle is NOT in the store → skillStore.open throws → propagate.
    const missing: PreparedSkillDescriptor = {
      id: 'sklv_missing',
      skill_id: 'skl_missing',
      source: 'anthropic',
      version_identifier: '1',
      name: 'missing',
      description: 'Missing skill.',
      entrypoint: 'SKILL.md',
      package_sha256: 'a'.repeat(64),
      package_size_bytes: 8,
    };
    const delivery = new SessionSkillsDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      provider: skillsProviderOf([missing]),
      skillStore: new InMemorySkillStore(),
    });
    await expect(delivery.deliver()).rejects.toThrow(/failed to open skill bundle/);
    expect(runner.requests.find((r) => r.path === RUNNER_SKILLS_PATH)).toBeUndefined();
  });
});

// ── Manager composition: skills → snapshot, in that order ──

describe('SessionEventBridgeManager with skills delivery', () => {
  const resolver: BoundSessionResolver = {
    resolveBoundSession: async (runnerId) =>
      runnerId === RUNNER_ID ? { workspaceId: WORKSPACE_ID, sessionId: SESSION_ID } : null,
  };

  it('pushes + acks the skills BEFORE the snapshot on connect', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const skillStore = new InMemorySkillStore();
    const alpha = await seedSkill(skillStore, 'alpha', [
      { path: 'SKILL.md', content: Buffer.from('# Alpha\n') },
    ]);

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
      skillsProvider: skillsProviderOf([alpha]),
      skillStore,
    });

    await manager.onRunnerConnect(RUNNER_ID, {});
    await waitFor(() => runner.pathOrder.includes(RUNNER_TURN_PATH));

    const order = runner.pathOrder;
    const skillsIdx = order.indexOf(RUNNER_SKILLS_PATH);
    const snapIdx = order.indexOf(RUNNER_SNAPSHOT_PATH);
    expect(skillsIdx).toBeGreaterThanOrEqual(0);
    expect(snapIdx).toBeGreaterThanOrEqual(0);
    // Skills pushed + acked BEFORE the snapshot.
    expect(skillsIdx).toBeLessThan(snapIdx);

    await manager.stopAll();
  });

  it('pushes no skills when the session has none (bridge + snapshot unchanged)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();

    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver,
      snapshotProvider: { resolve: async () => sampleSnapshot() },
      skillsProvider: skillsProviderOf([]),
      skillStore: new InMemorySkillStore(),
    });
    await manager.onRunnerConnect(RUNNER_ID, {});
    await waitFor(() => runner.pathOrder.includes(RUNNER_SNAPSHOT_PATH));
    expect(runner.requests.find((r) => r.path === RUNNER_SKILLS_PATH)).toBeUndefined();

    await manager.stopAll();
  });
});

// ── In-memory transcript store (append/tail ordering) ──

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

describe('managed runner preparation sequencing', () => {
  it.each(['none', 'resources', 'skills', 'snapshot', 'replay', 'read'] as const)(
    'fails closed at %s',
    async (failure) => {
      const store = new InMemoryTranscriptStore();
      const registry = new TunnelRegistry();
      const runner = new FakeRunner(registry, RUNNER_ID);
      runner.connect();
      runner.skillsStatus = failure === 'skills' ? 503 : 200;
      runner.replayStatus = failure === 'replay' ? 503 : 200;
      if (failure === 'read')
        store.read = () => ({
          [Symbol.asyncIterator]: () => ({
            next: async () => {
              throw new Error('transcript unavailable');
            },
          }),
        });
      const revision = 'a'.repeat(64);
      let resourceDeliveries = 0;
      const manager = new SessionEventBridgeManager({
        store,
        registry,
        resolver: {
          resolveBoundSession: async () => ({ workspaceId: WORKSPACE_ID, sessionId: SESSION_ID }),
        },
        resourcesFactory: async () => ({
          deliver: async () => {
            resourceDeliveries++;
            if (failure === 'resources') throw new Error('resources unavailable');
            return { version: 1, revision };
          },
          commit: async () => {},
        }),
        snapshotProvider: {
          resolve: async () => {
            if (failure === 'snapshot') throw new Error('snapshot unavailable');
            return { ...sampleSnapshot(), provider: 'codex-sdk' };
          },
        },
        skillsProvider: skillsProviderOf([]),
        skillStore: new InMemorySkillStore(),
      });
      try {
        await store.append(WORKSPACE_ID, SESSION_ID, [
          httpEventToProto({
            workspaceId: WORKSPACE_ID,
            sessionId: SESSION_ID,
            producedBy: 'client',
            idempotencyKey: '',
            input: {
              type: 'user.message',
              id: 'evt_managed',
              content: [{ type: 'text', text: 'hi' }],
            },
          }),
        ]);
        await manager.onRunnerConnect(RUNNER_ID);
        expect(manager.size).toBe(failure === 'none' ? 1 : 0);
        if (failure === 'none') {
          await waitFor(() => runner.pathOrder.includes(RUNNER_TURN_PATH));
          expect(resourceDeliveries).toBe(2); // connect and before the pending turn
          expect(runner.pathOrder.slice(0, 2)).toEqual([RUNNER_SKILLS_PATH, RUNNER_SNAPSHOT_PATH]);
          expect(
            runner.requests.find((request) => request.path === RUNNER_SKILLS_PATH)!.body[0]!.skills,
          ).toEqual([]);
          for (const request of runner.requests.filter(
            (request) => request.path === RUNNER_SNAPSHOT_PATH,
          ))
            expect(request.body[0]!.managed_resources).toEqual({ version: 1, revision });
        } else expect(runner.pathOrder).not.toContain(RUNNER_TURN_PATH);
      } finally {
        await manager.stopAll();
      }
    },
  );

  it('retires a delayed connect without sending its snapshot to a replacement tunnel', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const first = new FakeRunner(registry, RUNNER_ID);
    first.connect();
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    let factories = 0;
    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver: {
        resolveBoundSession: async () => ({ workspaceId: WORKSPACE_ID, sessionId: SESSION_ID }),
      },
      resourcesFactory: async () => {
        if (++factories === 1) await held;
        return {
          deliver: async () => ({ version: 1, revision: 'a'.repeat(64) }),
          commit: async () => {},
        };
      },
      snapshotProvider: { resolve: async () => ({ ...sampleSnapshot(), provider: 'codex-sdk' }) },
      skillsProvider: skillsProviderOf([]),
      skillStore: new InMemorySkillStore(),
    });
    try {
      const stale = manager.onRunnerConnect(RUNNER_ID);
      await waitFor(() => factories === 1);
      const replacement = new FakeRunner(registry, RUNNER_ID);
      replacement.connect();
      const current = manager.onRunnerConnect(RUNNER_ID);
      await new Promise((resolve) => setTimeout(resolve, 25));
      expect(replacement.requests).toEqual([]);
      release();
      await Promise.all([stale, current]);
      expect(
        replacement.requests.filter((request) => request.path === RUNNER_SNAPSHOT_PATH),
      ).toHaveLength(1);
      expect(manager.size).toBe(1);
    } finally {
      release();
      await manager.stopAll();
    }
  });

  it('retains complete pins while sending identical named Skill bytes only once', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const skillStore = new InMemorySkillStore();
    const skill = await seedSkill(skillStore, 'alpha', [
      { path: 'SKILL.md', content: Buffer.from('# Alpha') },
    ]);
    const pins = [skill, { ...skill }];
    const delivery = new SessionSkillsDelivery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      registry,
      skillStore,
      provider: skillsProviderOf(pins),
    });
    try {
      expect((await delivery.deliver()).delivered).toBe(true);
      const lines = runner.requests[0]!.body;
      expect(lines).toHaveLength(2);
      expect(lines[0]!.skills).toEqual(['alpha']);
      expect(lines[0]!.descriptors).toEqual(pins);
    } finally {
      runner.disconnect();
    }
  });
});
