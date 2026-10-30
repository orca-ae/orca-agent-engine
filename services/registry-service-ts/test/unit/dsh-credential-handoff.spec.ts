// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { decodeJwt, importPKCS8, SignJWT } from 'jose';
import { FrameKind, decodeFrame, decodeBody } from '@orca/harness-tunnel';
import { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import {
  AgentSnapshotResolver,
  type ResolverAgentRecord,
  type ResolverSessionRecord,
} from '../../src/domain/agent-snapshot-resolver.js';
import type { AgentSnapshot } from '../../src/domain/agent-snapshot.js';
import { TunnelRegistry } from '../../src/tunnel/tunnel-registry.js';
import {
  SessionSnapshotDelivery,
  RUNNER_SNAPSHOT_PATH,
  RUNNER_SESSION_HEADER,
  type DshCredentialAcknowledgement,
} from '../../src/tunnel/session-snapshot-delivery.js';

const route = 'llm-pi-deepseek-anthropic-messages';
const model = { provider: 'deepseek', id: 'deepseek-v4-flash' };

function issuer() {
  const minter = new SessionJwtMinter({
    privateKeyPem: readFileSync(
      new URL('../fixtures/session-jwt-private.pem', import.meta.url),
      'utf8',
    ),
    issuer: 'orca-registry',
    audience: 'ai-gateway',
    ttlSecs: 30,
  });
  const session: ResolverSessionRecord = {
    id: 'ses_dsh',
    workspaceId: 'ws_test',
    organizationId: 'org_test',
    agentId: 'agt_test',
    environmentId: 'env_test',
    vaultIds: ['vlt_not_granted'],
    agentVersion: 3,
    runtimeRevision: 7,
    guardrailIds: ['grd_session', 'grd_shared'],
  };
  const agent: ResolverAgentRecord = {
    id: 'agt_test',
    model,
    system: '',
    tools: [],
    mcpServers: [],
    skills: [],
    metadata: { harness: 'dsh_sdk', mode: 'colocated' },
    guardrailIds: ['grd_shared', 'grd_agent'],
  };
  const environment = {
    id: 'env_test',
    target: 'self_hosted' as 'cloud' | 'self_hosted',
    egressMode: 'gateway' as const,
  };
  const policy = { routes: [route], models: ['*'] };
  const resolver = new AgentSnapshotResolver({
    minter,
    gatewayLlmUrl: 'https://gateway.example',
    llmJwtTtlSecs: 1800,
    llmPolicy: policy,
    loader: {
      loadSession: async () => session,
      loadAgent: async () => {
        throw new Error('must use pinned agent');
      },
      loadAgentVersion: async (_workspace, _agent, version) => (version === 3 ? agent : null),
      loadEnvironment: async () => environment,
      loadSkillVersion: async () => null,
      loadVault: async () => null,
    },
  });
  return { minter, resolver, session, agent, environment, policy };
}

async function fixture() {
  const data = issuer();
  const credential = await data.resolver.mintSelfHostedDshCredential('ses_dsh');
  const registry = new TunnelRegistry();
  const requests: Array<{ path: string; headers: unknown; body: Record<string, unknown> }> = [];
  let current = true;
  let monotonic = 100;
  let wall = Date.now();
  vi.spyOn(Date, 'now').mockImplementation(() => wall);
  vi.spyOn(performance, 'now').mockImplementation(() => monotonic);
  const delays = { outbound: 0, inbound: 0 };
  const cancels: string[] = [];
  let stall: { index: number; at: 'head' | 'end' } | undefined;
  let markStalled!: () => void;
  const stalled = new Promise<void>((resolve) => {
    markStalled = resolve;
  });
  let onAck: (ack: DshCredentialAcknowledgement, index: number) => unknown = (ack) => ack;
  let status = 200;
  const socket = {
    async sendText(raw: string) {
      const frame = decodeFrame(raw);
      if (frame.kind === FrameKind.RequestCancel) cancels.push(frame.id);
      if (frame.kind !== FrameKind.Request) return;
      monotonic += delays.outbound;
      wall += delays.outbound;
      const body = JSON.parse(
        Buffer.from(decodeBody(frame.body!, frame.encoding ?? 'utf-8')).toString(),
      );
      requests.push({ path: frame.path, headers: frame.headers, body });
      const {
        expires_at_ms: _expiry,
        remaining_at_anchor_ms: _remaining,
        ...binding
      } = body.credential_handoff;
      const ack = onAck({ ...binding, phase: 'acknowledged' }, requests.length);
      monotonic += delays.inbound;
      wall += delays.inbound;
      if (stall?.index === requests.length && stall.at === 'head') {
        markStalled();
        return;
      }
      registry.routeResponseFrame('runner_test', {
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status,
        headers: [],
      });
      if (ack !== undefined)
        registry.routeResponseFrame('runner_test', {
          kind: FrameKind.ResponseBody,
          id: frame.id,
          body: JSON.stringify(ack),
          encoding: 'utf-8',
        });
      if (stall?.index === requests.length && stall.at === 'end') {
        markStalled();
        return;
      }
      registry.routeResponseFrame('runner_test', { kind: FrameKind.ResponseEnd, id: frame.id });
    },
    receiveText: () => new Promise<string>(() => {}),
  };
  const connection = registry.register(
    'runner_test',
    socket,
    {
      kind: FrameKind.Hello,
      runnerVersion: 'test',
      frameProtocolVersion: 1,
      harnesses: [],
      envs: [],
    },
    { owner: 'ws_test' },
  );
  void (async () => {
    for (;;) {
      const raw = await connection.outboundQueue.get();
      if (raw === null) return;
      await socket.sendText(raw);
    }
  })();
  const delivery = new SessionSnapshotDelivery({
    workspaceId: 'ws_test',
    sessionId: 'ses_dsh',
    runnerId: 'runner_test',
    registry,
    provider: { resolve: async () => null },
  });
  const snapshot: AgentSnapshot = {
    provider: 'dsh-sdk',
    model,
    system: '',
    allowed_tool_names: [],
    allowed_mcp_server_names: [],
    egress: {
      mode: 'gateway',
      gateway: {
        mcp_base_url: 'https://gateway.example/mcp',
        llm_base_url: 'https://gateway.example',
        session_jwt: '',
        mcp_servers: {},
      },
    },
  };
  const opts = {
    snapshot,
    credential: { token: credential.token, expiresAt: credential.expiresAt },
    expectedContext: credential.context,
    minter: data.minter,
    executionId: 'execution_test',
    isCurrent: () => current,
  };
  return {
    ...data,
    registry,
    connection,
    cancels,
    stalled,
    requests,
    delays,
    delivery,
    opts,
    advance(ms: number, wallJump = 0) {
      monotonic += ms;
      wall += ms + wallJump;
    },
    retire() {
      current = false;
    },
    setAck(handler: typeof onAck) {
      onAck = handler;
    },
    setStatus(value: number) {
      status = value;
    },
    setStall(value: typeof stall) {
      stall = value;
    },
  };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('dormant DSH credential issuer', () => {
  it('signs exactly 720s with pinned policy and no MCP/vault grants, without admitting DSH', async () => {
    const { resolver, minter } = issuer();
    const minted = await resolver.mintSelfHostedDshCredential('ses_dsh');
    const decoded = decodeJwt(minted.token);
    expect(decoded.exp! - decoded.iat!).toBe(720);
    expect(await minter.verify(minted.token, { expectedAudience: 'ai-gateway' })).toMatchObject({
      workspaceId: 'ws_test',
      sessionId: 'ses_dsh',
      orgId: 'org_test',
      agentId: 'agt_test',
      runtimeConfigRevision: '7',
      llmRoutes: [route],
      llmModels: [model.id],
      mcpServerNames: [],
      vaultIds: [],
      credentialIds: [],
      expiresAt: minted.expiresAt,
    });
    await expect(resolver.resolve('ses_dsh')).rejects.toThrow('self_hosted');
  });
  it.each(['cloud', 'model', 'route', 'mode', 'agent', 'pin'] as const)(
    'rejects %s binding/policy',
    async (fault) => {
      const f = issuer();
      if (fault === 'cloud') f.environment.target = 'cloud';
      if (fault === 'model') f.session.agentOverrides = { model: 'deepseek/deepseek-flash' };
      if (fault === 'route') f.policy.routes = ['llm-responses'];
      if (fault === 'mode') f.agent.metadata.mode = 'separate';
      if (fault === 'agent') f.agent.metadata.harness = 'pi_sdk';
      if (fault === 'pin') f.session.agentVersion = 4;
      await expect(f.resolver.mintSelfHostedDshCredential('ses_dsh')).rejects.toThrow();
    },
  );
});

describe('DSH credential snapshot handoff over real TunnelTransport', () => {
  it.each([
    [1, 'head'],
    [1, 'end'],
    [2, 'head'],
    [2, 'end'],
  ] as const)('bounds a stalled step %i ACK %s and permits retry', async (index, at) => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    // Leave a short safety window; step two must consume the same deadline.
    f.advance(55_000);
    const budget = f.opts.credential.expiresAt * 1000 - Date.now() - 660_000;
    f.setStall({ index, at });
    if (index === 2) {
      f.setAck((ack, step) => {
        if (step === 1) {
          f.advance(1000);
          vi.advanceTimersByTime(1000);
        }
        return ack;
      });
    }
    let outcome: unknown;
    const pending = f.delivery.deliverDshCredentials(f.opts).then((value) => {
      outcome = value;
    });
    await f.stalled;
    expect(f.requests).toHaveLength(index);
    expect(f.connection.inFlight.size).toBe(1);
    const requestId = [...f.connection.inFlight.keys()][0];
    const elapsed = index === 2 ? 1000 : 0;
    f.advance(budget - elapsed - 1);
    await vi.advanceTimersByTimeAsync(budget - elapsed - 1);
    expect(outcome).toBeUndefined();
    // The connection stays registered: healthy heartbeats cannot extend this deadline.
    expect(f.registry.markFrameSeen(f.connection)).toBe(true);
    expect(f.registry.get('runner_test')).toBe(f.connection);
    f.advance(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(outcome).toEqual({ skipped: false, delivered: false });
    expect(f.cancels).toEqual([requestId]);
    expect(f.connection.inFlight.size).toBe(0);
    await pending;
    f.setStall(undefined);
    f.setAck((ack) => ack);
    f.opts.credential = await f.resolver.mintSelfHostedDshCredential('ses_dsh');
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(true);
    expect(f.connection.inFlight.size).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
    f.registry.deregister('runner_test');
  });
  it.each([
    [1200, 0],
    [0, 1200],
    [400, 800],
  ])(
    'includes outbound %ims and inbound %ims in the anchor deduction',
    async (outbound, inbound) => {
      const f = await fixture();
      f.delays.outbound = outbound;
      f.delays.inbound = inbound;
      const initialWall = Date.now();
      expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(true);
      const confirmation = f.requests[1]!.body.credential_handoff as Record<string, unknown>;
      expect(confirmation.remaining_at_anchor_ms).toBe(
        f.opts.credential.expiresAt * 1000 - initialWall - outbound - inbound,
      );
      expect(f.requests).toHaveLength(2);
      f.registry.deregister('runner_test');
    },
  );
  it.each([
    'session',
    'workspace',
    'audience',
    'ttl',
    'model',
    'signature',
    'org',
    'agent',
    'guardrails',
    'revision',
    'repoUrls',
    'gitProxy',
  ] as const)('rejects signed credential %s mismatch without a snapshot request', async (fault) => {
    const f = await fixture();
    f.opts.credential = await f.minter.mint(
      {
        org_id: fault === 'org' ? 'org_other' : 'org_test',
        agent_id: fault === 'agent' ? 'agt_other' : 'agt_test',
        guardrail_ids: fault === 'guardrails' ? ['grd_other'] : f.opts.expectedContext.guardrailIds,
        runtime_config_revision: fault === 'revision' ? '8' : '7',
        workspace_id: fault === 'workspace' ? 'ws_other' : 'ws_test',
        session_id: fault === 'session' ? 'ses_other' : 'ses_dsh',
        mcp_server_names: [],
        vault_ids: [],
        credential_ids: [],
        llm_routes: [route],
        llm_models: [fault === 'model' ? 'deepseek-flash' : model.id],
      },
      {
        audience: fault === 'audience' ? 'llm-proxy' : 'ai-gateway',
        ttlSecs: fault === 'ttl' ? 660 : 720,
        ...(fault === 'repoUrls' ? { repoUrls: ['https://example.com/repo.git'] } : {}),
        ...(fault === 'gitProxy'
          ? {
              gitProxy: {
                resourceId: 'res_test',
                repoUrl: 'https://example.com/repo.git',
                credentialId: 'cred_test',
                credentialRevision: '1',
              },
            }
          : {}),
      },
    );
    if (fault === 'signature') {
      const segments = f.opts.credential.token.split('.');
      segments[2] = (segments[2]!.startsWith('A') ? 'B' : 'A') + segments[2]!.slice(1);
      f.opts.credential.token = segments.join('.');
    }
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(0);
    f.registry.deregister('runner_test');
  });
  it('preserves authorized guardrails and runtime revision from trusted records', async () => {
    const f = await fixture();
    expect(f.opts.expectedContext).toEqual({
      orgId: 'org_test',
      agentId: 'agt_test',
      guardrailIds: ['grd_session', 'grd_shared', 'grd_agent'],
      runtimeConfigRevision: '7',
    });
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(true);
    const egress = f.requests[0]!.body.egress as AgentSnapshot['egress'];
    if (egress.mode !== 'gateway') throw new Error('expected gateway');
    expect(await f.minter.verify(egress.gateway.llm_jwt!)).toMatchObject(f.opts.expectedContext);
    f.registry.deregister('runner_test');
  });
  it.each([
    'absent-iat',
    'invalid-iat',
    'fractional',
    'unsafe',
    'absent-exp',
    'invalid-exp',
  ] as const)('rejects signed %s timestamps before any request', async (fault) => {
    const f = await fixture();
    const payload = decodeJwt(f.opts.credential.token);
    if (fault === 'absent-iat') delete payload.iat;
    if (fault === 'invalid-iat') Object.assign(payload, { iat: 'invalid' });
    if (fault === 'absent-exp') delete payload.exp;
    if (fault === 'invalid-exp') Object.assign(payload, { exp: 'invalid' });
    if (fault === 'fractional') {
      payload.iat! += 0.5;
      payload.exp! += 0.5;
    }
    if (fault === 'unsafe') {
      payload.iat = 2 ** 53;
      payload.exp = 2 ** 53 + 720;
    }
    const key = await importPKCS8(
      readFileSync(new URL('../fixtures/session-jwt-private.pem', import.meta.url), 'utf8'),
      'RS256',
    );
    f.opts.credential.token = await new SignJWT(payload)
      .setProtectedHeader({ alg: 'RS256' })
      .sign(key);
    f.opts.credential.expiresAt = payload.exp!;
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(0);
    f.registry.deregister('runner_test');
  });
  it('clears the active flag when generation setup throws', async () => {
    const f = await fixture();
    vi.spyOn(f.registry, 'get').mockImplementationOnce(() => {
      throw new Error('setup failed');
    });
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(true);
    f.registry.deregister('runner_test');
  });
  it.each([1, 2])('rejects a same-runnerId replacement during ACK %i', async (step) => {
    const f = await fixture();
    const openRequest = vi.spyOn(f.registry, 'openRequest');
    let replacement: typeof f.connection | undefined;
    f.setAck((ack, index) => {
      if (index === step)
        replacement = f.registry.register('runner_test', f.connection.ws, f.connection.hello, {
          owner: 'ws_test',
        });
      return ack;
    });
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(replacement).toBeDefined();
    expect(replacement).not.toBe(f.connection);
    expect(f.requests).toHaveLength(step);
    expect(replacement!.inFlight.size).toBe(0);
    expect(openRequest).toHaveBeenCalledTimes(step);
    f.registry.deregister('runner_test');
  });
  it.each([1, 2])('rejects defective ACK responses at step %i', async (step) => {
    const f = await fixture();
    f.setAck((ack, index) => (index === step ? { ...ack, extra: true } : ack));
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(step);
    expect(f.connection.inFlight.size).toBe(0);
    f.registry.deregister('runner_test');
  });
  it.each(['empty-2xx', 'non-2xx'] as const)('rejects %s without confirmation', async (fault) => {
    const f = await fixture();
    if (fault === 'empty-2xx') f.setAck(() => undefined);
    else f.setStatus(503);
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(1);
    expect(f.connection.inFlight.size).toBe(0);
    f.registry.deregister('runner_test');
  });
  it('refuses overlapping handoffs on one delivery instance', async () => {
    const f = await fixture();
    const pending = f.delivery.deliverDshCredentials(f.opts);
    await expect(f.delivery.deliverDshCredentials(f.opts)).rejects.toThrow('already active');
    expect((await pending).delivered).toBe(true);
    expect(f.requests).toHaveLength(2);
    f.registry.deregister('runner_test');
  });
  it('subtracts the full first RTT, sends credential-only confirmation and requires both ACKs', async () => {
    const f = await fixture();
    f.setAck((ack, index) => {
      f.advance(index === 1 ? 1234 : 200);
      return ack;
    });
    expect(await f.delivery.deliverDshCredentials(f.opts)).toEqual({
      skipped: false,
      delivered: true,
    });
    expect(f.requests).toHaveLength(2);
    const provisional = f.requests[0]!.body.credential_handoff as Record<string, unknown>;
    const confirmed = f.requests[1]!.body.credential_handoff as Record<string, unknown>;
    expect(f.requests.every((r) => r.path === RUNNER_SNAPSHOT_PATH)).toBe(true);
    expect(f.requests[0]!.headers).toContainEqual([RUNNER_SESSION_HEADER, 'ses_dsh']);
    expect(provisional).toMatchObject({
      phase: 'provisional',
      token_digest: createHash('sha256').update(f.opts.credential.token).digest('hex'),
    });
    expect(
      Number(provisional.expires_at_ms) - Number(confirmed.remaining_at_anchor_ms) - Date.now(),
    ).toBe(-200);
    expect(Object.keys(f.requests[1]!.body)).toEqual(['credential_handoff']);
    expect(confirmed).toMatchObject({ phase: 'confirmed', handoff_id: provisional.handoff_id });
    expect(f.opts.snapshot.egress).not.toHaveProperty('gateway.llm_jwt');
    f.registry.deregister('runner_test');
  });
  it.each([
    'handoff_id',
    'token_digest',
    'workspace_id',
    'session_id',
    'execution_id',
    'phase',
  ] as const)('rejects wrong %s before confirmation', async (field) => {
    const f = await fixture();
    f.setAck((ack) => ({ ...ack, [field]: 'wrong' }));
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(1);
    f.registry.deregister('runner_test');
  });
  it.each([
    'near-expiry',
    'clock-forward',
    'clock-backward',
    'retire',
    'reconnect',
    'second-ack',
    'second-delay',
    'oversized',
  ] as const)('fails closed on %s', async (fault) => {
    const f = await fixture();
    f.setAck((ack, index) => {
      if (index === 1) {
        if (fault === 'near-expiry') f.advance(61_000);
        if (fault === 'clock-forward') f.advance(100, 2000);
        if (fault === 'clock-backward') f.advance(100, -2000);
        if (fault === 'retire') f.retire();
        if (fault === 'reconnect') f.registry.deregister('runner_test');
        if (fault === 'oversized') return { ...ack, extra: 'x'.repeat(4096) };
      }
      if (index === 2 && fault === 'second-ack') return { ...ack, handoff_id: 'stale' };
      if (index === 2 && fault === 'second-delay') f.advance(61_000);
      return ack;
    });
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(fault.startsWith('second-') ? 2 : 1);
    f.registry.deregister('runner_test');
  });
  it('rejects expired preparation and wrong JWT identity before any request', async () => {
    const f = await fixture();
    f.opts.credential.expiresAt += 1;
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(0);
    f.opts.credential.expiresAt -= 1;
    f.advance(61_000);
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(0);
    f.registry.deregister('runner_test');
  });
  it('tolerates bounded wall-clock slew and uses fresh single-use IDs on another handoff', async () => {
    const f = await fixture();
    f.setAck((ack) => {
      f.advance(100, 200);
      return ack;
    });
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(true);
    const firstAck = f.requests[0]!.body.credential_handoff as DshCredentialAcknowledgement;
    f.setAck(() => ({ ...firstAck, phase: 'acknowledged' }));
    expect((await f.delivery.deliverDshCredentials(f.opts)).delivered).toBe(false);
    expect(f.requests).toHaveLength(3);
    expect(f.requests[2]!.body.credential_handoff).not.toMatchObject({
      handoff_id: firstAck.handoff_id,
    });
    f.registry.deregister('runner_test');
  });
});
