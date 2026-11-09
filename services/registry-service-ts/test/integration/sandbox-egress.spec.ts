// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { eq } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import Fastify from 'fastify';
import { buildInternalAuth } from '../../src/auth/internal-auth.js';
import * as schema from '../../src/persistence/postgres/schema.js';
import { SandboxNetworkService } from '../../src/domain/sandbox-network.js';
import { SandboxNetworkAuthenticationError } from '../../src/domain/sandbox-network-authority.js';
import { SandboxEgressService } from '../../src/domain/sandbox-egress.js';
import { registerSandboxEgressRoutes } from '../../src/api/sandbox-egress.routes.js';
import { registerGitCredsRoutes } from '../../src/api/git-creds.routes.js';
import { registerGitProxyRoutes } from '../../src/api/git-proxy.routes.js';
import {
  sessionUsesSandboxEgress,
  mintGitProxyCapability,
  loadGitProxyBinding,
} from '../../src/domain/git-proxy-capability.js';
import type { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import { newId } from '../../src/domain/versioning.js';
import { EnvironmentTokenStore } from '../../src/domain/environment-token-store.js';
import { hashEnvironmentToken } from '../../src/domain/environment-token.js';
import { EnvironmentLaunchLifecycle } from '../../src/environment/launch/environment-launch-lifecycle.js';
import { buildDistributionSessionStore } from '../../src/api/sessions.routes.js';
import { SessionDistributor } from '../../src/tunnel/session-distributor.js';
import { WorkerRegistry } from '../../src/tunnel/worker-registry.js';
import { TunnelRegistry } from '../../src/tunnel/tunnel-registry.js';
import { WorkerFrameKind, decodeWorkerFrame } from '@orca/harness-tunnel';
import type { WorkerLaunchAuthority } from '../../src/domain/runner-launch-authority.js';
import type { EgressOwnerAuthority } from '../../src/contracts/sandbox-egress.contract.js';
import { parseSessionEgressGrant } from '@orca/sandbox-runtime';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL ?? 'postgres://orca:orca@localhost:5432/registry',
  max: 10,
});
const db = drizzle(pool, { schema });
const live = vi.fn(async () => true);
const secret = vi.fn(async () => 'upstream-fixture-secret');
const networks = new SandboxNetworkService(db, {
  authority: {
    bootstrap: async () => ({
      pod: owner.sandbox_pod,
      workload: { ...owner.sandbox_pod, name: owner.sandbox_handle_id },
    }),
    current: async () => true,
    ownerLive: async () => true,
  },
  proxyUrl: 'https://proxy.example:8443',
  internalBypass: [],
});
const options = {
  networks,
  githubRestReadEnabled: true,
  environmentClaimTtlMs: 90000,
  verifyLiveness: live,
  locateSandbox: async () => owner.sandbox_pod,
  proxyUrl: 'https://proxy.example:8443',
  internalBypass: ['registry.orca.svc.cluster.local'],
};
const service = () => new SandboxEgressService(db, { resolve: secret }, options);
let oldRunnerId: string;
let workspaceId: string;
let sessionId: string;
let environmentId: string;
let resourceId: string;
let credentialId: string;
let owner: Extract<EgressOwnerAuthority, { kind: 'harness' }>;

describe('sandbox egress persisted authority', () => {
  beforeAll(async () => {
    await migrate(db, { migrationsFolder: 'src/persistence/postgres/migrations' });
  });
  afterAll(async () => {
    await pool.end();
  });
  beforeEach(async () => {
    live.mockReset().mockResolvedValue(true);
    secret.mockReset().mockResolvedValue('upstream-fixture-secret');
    workspaceId = newId('ws');
    sessionId = newId('ses');
    oldRunnerId = 'runner_' + sessionId;
    environmentId = newId('env');
    resourceId = newId('sesrsc');
    credentialId = newId('gitcred');
    const organizationId = newId('org');
    const agentId = newId('agt');
    await db.insert(schema.organizations).values({ id: organizationId, name: organizationId });
    await db
      .insert(schema.workspaces)
      .values({ id: workspaceId, organizationId, name: workspaceId, createdBy: 'fixture' });
    await db.insert(schema.environments).values({
      id: environmentId,
      workspaceId,
      name: environmentId,
      target: 'cloud',
      networking: { type: 'unrestricted' },
    });
    await db.insert(schema.agents).values({
      id: agentId,
      workspaceId,
      name: agentId,
      version: 1,
      metadata: { harness: 'codex_sdk', mode: 'separate' },
      harnessType: 'codex_sdk',
      modelProvider: 'openai',
      modelId: 'fixture',
    });
    await db.insert(schema.agentVersions).values({
      id: newId('agtv'),
      workspaceId,
      agentId,
      version: 1,
      snapshot: {
        metadata: { harness: 'codex_sdk', mode: 'separate' },
        harness_type: 'codex_sdk',
      },
    });
    await db
      .insert(schema.sessions)
      .values({ id: sessionId, workspaceId, agentId, agentVersion: 1, environmentId });
    await db.insert(schema.sessionResources).values({
      id: resourceId,
      workspaceId,
      sessionId,
      type: 'github_repository',
      mountPath: '/repo',
      access: 'read_only',
      repoRef: { url: 'https://github.com/fixture/repo.git', git_credential_id: credentialId },
    });
    await db.insert(schema.gitCredentials).values({
      id: credentialId,
      workspaceId,
      repoUrl: 'https://github.com/fixture/repo.git',
      secretRef: 'env:FIXTURE_SECRET',
      sessionResourceId: resourceId,
    });
    owner = {
      kind: 'harness',
      owner_pod: { namespace: 'orca', name: 'harness', uid: 'harness-uid' },
      sandbox_pod: { namespace: 'sandbox', name: 'sandbox', uid: 'sandbox-uid' },
      sandbox_handle_id: 'sandbox',
    };
    const network = await networks.registerHarnessNetwork(workspaceId, sessionId, owner.owner_pod);
    await networks.bootstrap(network.binding_id, network.generation, 'pod-token');
  });

  const target = { host: 'github.com', port: 443 as const };
  async function registryWorker() {
    const network = await networks.registerEnvironmentNetwork(environmentId);
    await networks.bootstrap(network.binding_id, network.generation, 'pod-token');
    const agentId = newId('agt');
    await db.insert(schema.agents).values({
      id: agentId,
      workspaceId,
      name: agentId,
      metadata: { harness: 'pi', mode: 'colocated' },
      modelProvider: 'openai',
      modelId: 'fixture',
    });
    await db.insert(schema.agentVersions).values({
      id: newId('agtv'),
      workspaceId,
      agentId,
      version: 1,
      snapshot: { metadata: { harness: 'pi', mode: 'colocated' }, harness_type: 'pi' },
    });
    await db.update(schema.sessions).set({ agentId }).where(eq(schema.sessions.id, sessionId));
    const token = await new EnvironmentTokenStore(db).mintEnvironmentToken(
      environmentId,
      undefined,
      undefined,
      owner.sandbox_handle_id,
    );
    await db
      .insert(schema.environmentClaims)
      .values({ environmentId, ownerPod: 'registry', workerConnId: 'original-connection' });
    const source = await service().authenticatedWorkerLaunch(
      environmentId,
      token,
      'registry',
      'original-connection',
    );
    return { token, source, network };
  }
  async function bindAssigned(
    runnerId: string,
    source: WorkerLaunchAuthority,
    replica = service(),
  ) {
    const send = vi.fn();
    await replica.bindRunnerLaunch(sessionId, runnerId, source, send);
    expect(send).toHaveBeenCalledOnce();
    expect(
      await buildDistributionSessionStore(db, replica).applyDistributionPatch(
        sessionId,
        { distributionState: 'assigned' },
        runnerId,
      ),
    ).toBe(true);
  }
  function connectedWorker(
    source: WorkerLaunchAuthority,
    stopStatus: 'stopped' | 'failed' = 'stopped',
  ) {
    const registry = new WorkerRegistry();
    registry.register(
      environmentId,
      {
        receive: async () => ({ type: 'close' as const, code: 1000, reason: 'fixture' }),
        sendText: async () => {},
        close: () => {},
      },
      {
        kind: WorkerFrameKind.Hello,
        version: 'fixture',
        frameProtocolVersion: 1,
        name: 'fixture',
        runners: [],
        configuredHarnesses: null,
      },
      { owner: workspaceId, launchAuthority: source },
    );
    const enqueue = registry.sendText.bind(registry);
    vi.spyOn(registry, 'sendText').mockImplementation((connection, data) => {
      const frame = decodeWorkerFrame(data);
      if (frame.kind === WorkerFrameKind.StopRunner)
        connection.pendingStops.get(frame.requestId)!.resolve({
          requestId: frame.requestId,
          status: stopStatus,
          error: stopStatus === 'failed' ? 'process survived kill' : null,
        });
      else enqueue(connection, data);
    });
    return registry;
  }
  const resolve = (
    grant: Awaited<ReturnType<SandboxEgressService['issueEgress']>>,
    value = `Basic ${Buffer.from(`x-access-token:${grant.git!.marker}`).toString('base64')}`,
  ) => ({
    scope: 'session' as const,
    binding_id: grant.binding_id,
    generation: grant.generation,
    target,
    method: 'GET',
    path_and_query: '/fixture/repo.git/info/refs?service=git-upload-pack',
    candidate_headers: [{ name: 'authorization', value }],
  });

  it('persists digest only, resolves across replicas and observes rotation', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    const [stored] = await db
      .select()
      .from(schema.sandboxEgressBindings)
      .where(eq(schema.sandboxEgressBindings.id, grant.binding_id));
    expect(JSON.stringify(stored)).not.toContain(grant.proxy_capability);
    expect(JSON.stringify(stored)).not.toContain('upstream-fixture-secret');
    expect((stored!.credentials as { mappings: unknown[] }).mappings[0]).not.toHaveProperty(
      'transforms',
    );
    const pass = await service().authorizeProxy(grant.proxy_capability, {
      host: 'example.org',
      port: 443,
    });
    expect(pass).toMatchObject({
      decision: 'passthrough',
      managed_auth: false,
    });
    expect(pass).not.toHaveProperty('authentication_header_names');
    expect(
      await service().resolveRequest({
        ...resolve(grant),
        target: { host: 'example.org', port: 443 },
        path_and_query: '/fixture',
        candidate_headers: [],
      }),
    ).toEqual({ replacements: [], credential_version: null, request_body_policy: 'stream' });
    expect(secret).not.toHaveBeenCalled();
    expect(await service().authorizeProxy(grant.proxy_capability, target)).toMatchObject({
      decision: 'mitm',
      managed_auth: true,
    });
    const first = await service().resolveRequest(resolve(grant));
    expect(first.request_body_policy).toBe('stream');
    const upload = await service().resolveRequest({
      ...resolve(grant),
      method: 'POST',
      path_and_query: '/fixture/repo.git/git-upload-pack',
    });
    expect(upload.request_body_policy).toBe('git_upload_pack');
    await expect(
      service().resolveRequest({
        ...resolve(grant),
        method: 'POST',
        path_and_query: '/different/repo.git/git-upload-pack',
      }),
    ).rejects.toMatchObject({ type: 'permission_denied' });
    expect(first.replacements[0]!.value).toBe(
      `Basic ${Buffer.from('x-access-token:upstream-fixture-secret').toString('base64')}`,
    );
    secret.mockResolvedValue('rotated-secret');
    await db
      .update(schema.gitCredentials)
      .set({ secretRef: 'env:ROTATED', updatedAt: new Date() })
      .where(eq(schema.gitCredentials.id, credentialId));
    const rotated = await service().resolveRequest(resolve(grant));
    expect(rotated.replacements[0]!.value).toContain(
      Buffer.from('x-access-token:rotated-secret').toString('base64'),
    );
    expect(rotated.credential_version).not.toBe(first.credential_version);
  });
  it('bootstraps an Environment without worker or Session authority and survives Registry restart', async () => {
    await db
      .update(schema.sessions)
      .set({ deletedAt: new Date() })
      .where(eq(schema.sessions.id, sessionId));
    const request = await networks.registerEnvironmentNetwork(environmentId);
    const context = await networks.bootstrap(request.binding_id, request.generation, 'pod-token');
    const [stored] = await db
      .select()
      .from(schema.sandboxNetworkBindings)
      .where(eq(schema.sandboxNetworkBindings.id, request.binding_id));
    expect(JSON.stringify(stored)).not.toContain(context.proxy_capability);
    const replicaDb = drizzle(pool, { schema });
    const replicaNetworks = new SandboxNetworkService(replicaDb, {
      authority: {
        bootstrap: async () => {
          throw new Error('restart must not bootstrap');
        },
        current: async () => true,
        ownerLive: async () => true,
      },
      proxyUrl: options.proxyUrl,
      internalBypass: [],
    });
    const replica = new SandboxEgressService(
      replicaDb,
      { resolve: secret },
      { ...options, networks: replicaNetworks },
    );
    expect(await replica.authorizeProxy(context.proxy_capability, target)).toMatchObject({
      scope: 'sandbox',
      decision: 'passthrough',
      managed_auth: false,
    });
    expect(secret).not.toHaveBeenCalled();
  });
  it('rejects a capability rotated between digest lookup and current authority validation', async () => {
    const request = await networks.registerHarnessNetwork(workspaceId, sessionId, owner.owner_pod);
    const context = await networks.bootstrap(request.binding_id, request.generation, 'pod-token');
    const current = networks.current.bind(networks);
    let rotated: Awaited<ReturnType<SandboxNetworkService['bootstrap']>> | undefined;
    const validation = vi.spyOn(networks, 'current').mockImplementationOnce(async (...args) => {
      rotated = await networks.bootstrap(request.binding_id, request.generation, 'pod-token');
      return current(...args);
    });
    try {
      await expect(
        service().authorizeProxy(context.proxy_capability, target),
      ).rejects.toMatchObject({
        type: 'invalid_capability',
      });
      expect(validation).toHaveBeenCalledOnce();
      expect(rotated).toBeDefined();
      expect(rotated!.proxy_capability).not.toBe(context.proxy_capability);
      expect(await service().authorizeProxy(rotated!.proxy_capability, target)).toMatchObject({
        binding_id: request.binding_id,
        scope: 'sandbox',
      });
      expect(secret).not.toHaveBeenCalled();
    } finally {
      validation.mockRestore();
    }
  });
  it('authenticates only the exact bootstrap route and keeps Pod tokens outside general internal auth', async () => {
    const authority = {
      bootstrap: async (token: string) => {
        if (token !== 'projected-pod-token')
          throw new SandboxNetworkAuthenticationError('invalid token');
        return {
          pod: owner.sandbox_pod,
          workload: { ...owner.sandbox_pod, name: owner.sandbox_handle_id },
        };
      },
      current: async () => true,
      ownerLive: async () => true,
    };
    const isolated = new SandboxNetworkService(db, {
      authority,
      proxyUrl: options.proxyUrl,
      internalBypass: [],
    });
    const egress = new SandboxEgressService(
      db,
      { resolve: secret },
      { ...options, networks: isolated },
    );
    const app = Fastify();
    app.addHook(
      'preHandler',
      buildInternalAuth(
        {
          verify: async (token) =>
            token === 'harness-token' ? { caller: 'harness', subject: 'harness' } : null,
        },
        true,
      ),
    );
    registerSandboxEgressRoutes(app, egress);
    const request = await isolated.registerHarnessNetwork(workspaceId, sessionId, owner.owner_pod);
    const route = '/internal/v1/sandbox-networks/' + request.binding_id + '/bootstrap';
    try {
      for (const token of ['', 'harness-token']) {
        const response = await app.inject({
          method: 'POST',
          url: route,
          headers: token ? { authorization: 'Bearer ' + token } : {},
          payload: { generation: request.generation },
        });
        expect(response.statusCode).toBe(401);
      }
      const response = await app.inject({
        method: 'POST',
        url: route,
        headers: { authorization: 'Bearer projected-pod-token' },
        payload: { generation: request.generation },
      });
      expect(response.statusCode).toBe(200);
      expect(response.json()).toMatchObject({
        binding_id: request.binding_id,
        generation: request.generation,
        proxy_url: options.proxyUrl,
      });
      expect(response.headers['cache-control']).toBe('private, no-store');
      expect(
        (
          await app.inject({
            method: 'POST',
            url: '/internal/v1/sandbox-egress/authorize',
            headers: { authorization: 'Bearer projected-pod-token' },
            payload: { capability: response.json().proxy_capability, target },
          })
        ).statusCode,
      ).toBe(401);
      expect(secret).not.toHaveBeenCalled();
    } finally {
      await app.close();
    }
  });
  it('rechecks the parent after asynchronous Session secret resolution', async () => {
    const request = await networks.registerHarnessNetwork(workspaceId, sessionId, owner.owner_pod);
    await networks.bootstrap(request.binding_id, request.generation, 'pod-token');
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    secret.mockImplementationOnce(async () => {
      await networks.revokeHarnessNetwork(
        workspaceId,
        sessionId,
        request.binding_id,
        request.generation,
      );
      return 'upstream-fixture-secret';
    });
    await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
      type: 'invalid_capability',
    });
  });
  it('fences worker-token mint against the provisioning network generation', async () => {
    const tokens = new EnvironmentTokenStore(db);
    const first = await networks.registerEnvironmentNetwork(environmentId);
    await networks.bootstrap(first.binding_id, first.generation, 'pod-token');
    await tokens.mintEnvironmentToken(
      environmentId,
      undefined,
      undefined,
      owner.sandbox_handle_id,
      first,
    );
    const next = await networks.registerEnvironmentNetwork(environmentId);
    await networks.bootstrap(next.binding_id, next.generation, 'pod-token');
    const token = await tokens.mintEnvironmentToken(
      environmentId,
      undefined,
      undefined,
      owner.sandbox_handle_id,
      next,
    );
    await expect(
      tokens.mintEnvironmentToken(
        environmentId,
        undefined,
        undefined,
        owner.sandbox_handle_id,
        first,
      ),
    ).rejects.toThrow('network generation was replaced');
    await networks.revokeEnvironmentNetwork(environmentId, first.binding_id, first.generation);
    expect(await tokens.resolveEnvironmentToken(environmentId, token)).toBe(workspaceId);
    expect((await networks.current(next.binding_id)).binding.id).toBe(next.binding_id);
  });
  it('keeps worker-token authentication separate from revoked network authority', async () => {
    const { token, source, network } = await registryWorker();
    const tokens = new EnvironmentTokenStore(db);
    const runnerId = 'worker-token-' + sessionId;
    await bindAssigned(runnerId, source);
    const grant = await service().issueRunnerEgress(workspaceId, sessionId, runnerId);
    await networks.revokeEnvironmentNetwork(environmentId, network.binding_id, network.generation);
    expect(await tokens.resolveEnvironmentToken(environmentId, token)).toBe(workspaceId);
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    await expect(
      service().issueRunnerEgress(workspaceId, sessionId, runnerId),
    ).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    expect(secret).not.toHaveBeenCalled();
    await tokens.revokeEnvironmentToken(environmentId, token);
    expect(await tokens.resolveEnvironmentToken(environmentId, token)).toBeNull();
  });
  it('rejects crossed scopes and stale generations while both capabilities are live', async () => {
    const request = await networks.registerHarnessNetwork(workspaceId, sessionId, owner.owner_pod);
    const context = await networks.bootstrap(request.binding_id, request.generation, 'pod-token');
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    const base = {
      ...resolve(grant),
      binding_id: context.binding_id,
      generation: context.generation,
      scope: 'sandbox' as const,
    };
    for (const input of [
      { ...resolve(grant), scope: 'sandbox' as const },
      { ...base, scope: 'session' as const },
      { ...base, generation: 'stale' },
      { ...resolve(grant), generation: 'stale' },
    ]) {
      await expect(service().resolveRequest(input)).rejects.toMatchObject({
        type: 'invalid_capability',
      });
    }
    expect(secret).not.toHaveBeenCalled();
    expect((await service().authorizeProxy(context.proxy_capability, target)).scope).toBe(
      'sandbox',
    );
    expect((await service().authorizeProxy(grant.proxy_capability, target)).scope).toBe('session');
  });
  it('never resolves Session markers with a base capability and retains base on grant teardown', async () => {
    const request = await networks.registerHarnessNetwork(workspaceId, sessionId, owner.owner_pod);
    const context = await networks.bootstrap(request.binding_id, request.generation, 'pod-token');
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    for (const port of [80, 443] as const) {
      expect(
        await service().resolveRequest({
          ...resolve(grant),
          binding_id: context.binding_id,
          generation: context.generation,
          scope: 'sandbox',
          target: { ...target, port },
        }),
      ).toEqual({ replacements: [], credential_version: null, request_body_policy: 'stream' });
    }
    expect(secret).not.toHaveBeenCalled();
    await service().revokeEgress(workspaceId, sessionId, grant.binding_id, grant.generation);
    expect((await service().authorizeProxy(context.proxy_capability, target)).scope).toBe(
      'sandbox',
    );
    await expect(
      service().resolveRequest({
        ...resolve(grant),
        binding_id: context.binding_id,
        generation: context.generation,
      }),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
  });
  it('fences parent replacement and rejects replay from a replacement Pod', async () => {
    const first = await networks.registerHarnessNetwork(workspaceId, sessionId, owner.owner_pod);
    const context = await networks.bootstrap(first.binding_id, first.generation, 'pod-token');
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    owner.sandbox_pod = { ...owner.sandbox_pod, uid: 'replacement' };
    await expect(
      networks.bootstrap(first.binding_id, first.generation, 'pod-token'),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
    owner.sandbox_pod = { ...owner.sandbox_pod, uid: 'sandbox-uid' };
    const successor = await networks.registerHarnessNetwork(
      workspaceId,
      sessionId,
      owner.owner_pod,
    );
    const next = await networks.bootstrap(successor.binding_id, successor.generation, 'pod-token');
    await networks.revokeHarnessNetwork(workspaceId, sessionId, first.binding_id, first.generation);
    await expect(service().authorizeProxy(context.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    expect((await service().authorizeProxy(next.proxy_capability, target)).scope).toBe('sandbox');
  });
  it('keeps another Session grant and Environment base alive during Session teardown', async () => {
    const { source } = await registryWorker();
    await bindAssigned('first-' + sessionId, source);
    const first = await service().issueRunnerEgress(workspaceId, sessionId, 'first-' + sessionId);
    const [original] = await db
      .select()
      .from(schema.sessions)
      .where(eq(schema.sessions.id, sessionId));
    const otherSession = newId('ses');
    await db.insert(schema.sessions).values({
      ...original!,
      id: otherSession,
      runnerId: null,
      runnerLaunchAuthority: null,
      distributionState: 'pending',
    });
    await service().bindRunnerLaunch(otherSession, 'second-' + otherSession, source, () => {});
    await db
      .update(schema.sessions)
      .set({ distributionState: 'assigned' })
      .where(eq(schema.sessions.id, otherSession));
    const other = await service().issueRunnerEgress(
      workspaceId,
      otherSession,
      'second-' + otherSession,
    );
    await service().revokeEgress(workspaceId, sessionId, first.binding_id, first.generation);
    expect((await service().authorizeProxy(other.proxy_capability, target)).scope).toBe('session');
    expect((await networks.current(first.network_binding_id)).binding.id).toBe(
      other.network_binding_id,
    );
  });
  it.each(['GitHub.com', 'GITHUB.COM'])(
    'preserves case-insensitive Environment hostname matching for %s',
    async (allowedHost) => {
      const networking = { type: 'limited', allowed_hosts: [allowedHost] };
      await db
        .update(schema.environments)
        .set({ networking })
        .where(eq(schema.environments.id, environmentId));
      expect(await loadGitProxyBinding(db, { workspaceId, sessionId, resourceId })).not.toBeNull();
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      expect(await service().authorizeProxy(grant.proxy_capability, target)).toMatchObject({
        decision: 'mitm',
        managed_auth: true,
      });
      const resolved = await service().resolveRequest(resolve(grant));
      expect(resolved.replacements).toEqual([
        {
          header: 'authorization',
          value:
            'Basic ' + Buffer.from('x-access-token:upstream-fixture-secret').toString('base64'),
        },
      ]);
      await expect(
        service().authorizeProxy(grant.proxy_capability, {
          host: 'github.com.attacker.example',
          port: 443,
        }),
      ).rejects.toMatchObject({ type: 'permission_denied' });
      const [environment] = await db
        .select({ networking: schema.environments.networking })
        .from(schema.environments)
        .where(eq(schema.environments.id, environmentId));
      expect(environment!.networking).toEqual(networking);
    },
  );
  it('serializes concurrent issuance; stale revoke cannot revoke successor', async () => {
    const grants = await Promise.all(
      Array.from({ length: 12 }, () => service().issueEgress(workspaceId, sessionId, owner)),
    );
    const outcomes = await Promise.allSettled(
      grants.map((grant) => service().authorizeProxy(grant.proxy_capability, target)),
    );
    expect(outcomes.filter((outcome) => outcome.status === 'fulfilled')).toHaveLength(1);
    const active = grants[outcomes.findIndex((outcome) => outcome.status === 'fulfilled')]!;
    const stale = grants.find((grant) => grant !== active)!;
    await service().revokeEgress(workspaceId, sessionId, stale.binding_id, stale.generation);
    expect((await service().authorizeProxy(active.proxy_capability, target)).generation).toBe(
      active.generation,
    );
    await service().revokeEgress(workspaceId, sessionId, active.binding_id, active.generation);
    await expect(service().authorizeProxy(active.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    expect(await sessionUsesSandboxEgress(db, sessionId)).toBe(true);
    await expect(
      mintGitProxyCapability(db, {} as SessionJwtMinter, {
        workspaceId,
        sessionId,
        resourceId,
        registryBaseUrl: 'https://registry.example',
      }),
    ).rejects.toThrow('legacy');
  });
  it('selects the exact repository when one marker covers several resources', async () => {
    const secondResource = newId('sesrsc');
    const secondCredential = newId('gitcred');
    await db.insert(schema.sessionResources).values({
      id: secondResource,
      workspaceId,
      sessionId,
      type: 'github_repository',
      mountPath: '/second',
      access: 'read_only',
      repoRef: {
        url: 'https://github.com/fixture/second.git',
        git_credential_id: secondCredential,
      },
    });
    await db.insert(schema.gitCredentials).values({
      id: secondCredential,
      workspaceId,
      repoUrl: 'https://github.com/fixture/second.git',
      secretRef: 'env:SECOND_REPO',
      sessionResourceId: secondResource,
    });
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    await db
      .update(schema.gitCredentials)
      .set({ archivedAt: new Date() })
      .where(eq(schema.gitCredentials.id, credentialId));
    expect((await service().authorizeProxy(grant.proxy_capability, target)).decision).toBe('mitm');
    const response = await service().resolveRequest({
      ...resolve(grant),
      path_and_query: '/fixture/second.git/info/refs?service=git-upload-pack',
    });
    expect(response.replacements).toHaveLength(1);
    expect(secret).toHaveBeenCalledWith('env:SECOND_REPO');
    await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
      type: 'permission_denied',
    });
  });
  it('rejects legacy helper and proxy handlers even after binding revocation', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    await service().revokeEgress(workspaceId, sessionId, grant.binding_id, grant.generation);
    const app = Fastify();
    const minter = {
      verify: async () => ({
        workspaceId,
        sessionId,
        gitProxy: { resourceId },
        repoUrls: ['https://github.com/fixture/repo.git'],
      }),
    } as unknown as SessionJwtMinter;
    const deps = { db, jwtMinter: minter, secretProvider: { resolve: secret } };
    registerGitCredsRoutes(app, deps);
    registerGitProxyRoutes(app, deps);
    const headers = { authorization: 'Bearer verified-fixture-token' };
    expect(
      (
        await app.inject({
          method: 'POST',
          url: '/v1/git-creds',
          headers,
          payload: { protocol: 'https', host: 'github.com', path: 'fixture/repo.git' },
        })
      ).statusCode,
    ).toBe(403);
    expect(
      (
        await app.inject({
          method: 'GET',
          url: `/v1/git-proxy/${resourceId}/info/refs?service=git-upload-pack`,
          headers,
        })
      ).statusCode,
    ).toBe(404);
    expect(secret).not.toHaveBeenCalled();
    await app.close();
  });
  it('rejects invalid markers, duplicate headers, cross scope and write operations', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    for (const request of [
      resolve(grant, 'Bearer invalid-marker'),
      {
        ...resolve(grant),
        candidate_headers: [
          ...resolve(grant).candidate_headers,
          ...resolve(grant).candidate_headers,
        ],
      },
      { ...resolve(grant), path_and_query: '/fixture/repo.git/git-receive-pack' },
      { ...resolve(grant), generation: 'superseded' },
    ])
      await expect(service().resolveRequest(request)).rejects.toBeDefined();
    await expect(
      service().resolveRequest({ ...resolve(grant), binding_id: 'egb_foreign' }),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
    expect(secret).not.toHaveBeenCalled();
  });
  it('keeps Git markers restricted to exact Authorization forms and requires Git authentication', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    for (const candidate_headers of [
      [],
      [{ name: 'x-api-key', value: grant.git!.marker }],
      [{ name: 'authorization', value: 'prefix ' + grant.git!.marker }],
      [{ name: 'authorization', value: 'Bearer ' + grant.git!.marker + ', extra' }],
      [
        {
          name: 'authorization',
          value: 'Basic ' + Buffer.from('wrong-user:' + grant.git!.marker).toString('base64'),
        },
      ],
    ]) {
      await expect(
        service().resolveRequest({ ...resolve(grant), candidate_headers }),
      ).rejects.toMatchObject({ type: 'permission_denied' });
    }
    expect(secret).not.toHaveBeenCalled();
    for (const scheme of ['Bearer', 'token']) {
      expect(
        (await service().resolveRequest(resolve(grant, scheme + ' ' + grant.git!.marker)))
          .replacements,
      ).toEqual([{ header: 'authorization', value: scheme + ' upstream-fixture-secret' }]);
    }
  });
  it('fails closed for owner loss, dependency failure, policy changes and resource revocation', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    live.mockResolvedValue(false);
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    live.mockRejectedValue(new Error('Kubernetes unavailable'));
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'dependency_unavailable',
    });
    live.mockResolvedValue(true);
    await db
      .update(schema.environments)
      .set({ networking: { type: 'limited', allowed_hosts: [] } })
      .where(eq(schema.environments.id, environmentId));
    await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
      type: 'permission_denied',
    });
    await db
      .update(schema.environments)
      .set({ networking: { type: 'unrestricted' } })
      .where(eq(schema.environments.id, environmentId));
    await db
      .update(schema.gitCredentials)
      .set({ archivedAt: new Date() })
      .where(eq(schema.gitCredentials.id, credentialId));
    await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
      type: 'permission_denied',
    });
    expect(secret).not.toHaveBeenCalled();
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'permission_denied',
    });
  });
  it('rechecks revocation after asynchronous secret resolution', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    secret.mockImplementation(async () => {
      await service().revokeEgress(workspaceId, sessionId, grant.binding_id, grant.generation);
      return 'secret';
    });
    await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
      type: 'invalid_capability',
    });
  });
  it('rejects sandbox UID replacement, terminated Sessions and inactive workspaces', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    owner.sandbox_pod = { ...owner.sandbox_pod, uid: 'replacement-pod-uid' };
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    owner.sandbox_pod = { ...owner.sandbox_pod, uid: 'sandbox-uid' };
    await db
      .update(schema.workspaces)
      .set({ status: 'archived', archivedAt: new Date() })
      .where(eq(schema.workspaces.id, workspaceId));
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    await db
      .update(schema.workspaces)
      .set({ status: 'active', archivedAt: null })
      .where(eq(schema.workspaces.id, workspaceId));
    await db
      .update(schema.sessions)
      .set({ status: 'terminated' })
      .where(eq(schema.sessions.id, sessionId));
    await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
      type: 'invalid_capability',
    });
  });
  it('rejects a replaced Harness sandbox association', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    await db
      .update(schema.sessions)
      .set({ sandboxHandleId: 'successor-sandbox' })
      .where(eq(schema.sessions.id, sessionId));
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
  });
  it('rechecks Session termination under the issuance lock', async () => {
    const racingDb = drizzle(pool, { schema });
    const transact = racingDb.transaction.bind(racingDb);
    vi.spyOn(racingDb, 'transaction').mockImplementation(async (callback, config) => {
      await db
        .update(schema.sessions)
        .set({ status: 'terminated' })
        .where(eq(schema.sessions.id, sessionId));
      return transact(callback, config);
    });
    const replica = new SandboxEgressService(racingDb, { resolve: secret }, options);
    await expect(replica.issueEgress(workspaceId, sessionId, owner)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    expect(
      await db
        .select()
        .from(schema.sandboxEgressBindings)
        .where(eq(schema.sandboxEgressBindings.sessionId, sessionId)),
    ).toHaveLength(0);
  });
  it('checks assigned Registry runner, cloud launch digest and current claim TTL', async () => {
    const agentId = newId('agt');
    await db.insert(schema.agents).values({
      id: agentId,
      workspaceId,
      name: agentId,
      metadata: { harness: 'pi', mode: 'colocated' },
      modelProvider: 'openai',
      modelId: 'fixture',
    });
    await db.insert(schema.agentVersions).values({
      id: newId('agtv'),
      workspaceId,
      agentId,
      version: 1,
      snapshot: { metadata: { harness: 'pi', mode: 'colocated' }, harness_type: 'pi' },
    });
    await db
      .update(schema.sessions)
      .set({
        agentId,
        runnerId: 'runner',
        hostEnvironmentId: environmentId,
        distributionState: 'assigned',
      })
      .where(eq(schema.sessions.id, sessionId));
    await db
      .update(schema.environments)
      .set({
        environmentTokenDigest: 'digest',
        cloudSandboxHandleId: owner.sandbox_handle_id,
        environmentTokenExpiresAt: new Date(Date.now() + 60000),
      })
      .where(eq(schema.environments.id, environmentId));
    await db
      .insert(schema.environmentClaims)
      .values({ environmentId, ownerPod: 'registry', workerConnId: 'connection' });
    const authority = {
      kind: 'registry',
      runner_id: 'runner',
      environment_id: environmentId,
      owner_pod: 'registry',
      worker_conn_id: 'connection',
      launch_digest: 'digest',
      sandbox_handle_id: owner.sandbox_handle_id,
      sandbox_pod: owner.sandbox_pod,
    } as const;
    await db
      .update(schema.sessions)
      .set({ runnerLaunchAuthority: authority })
      .where(eq(schema.sessions.id, sessionId));
    const network = await networks.registerEnvironmentNetwork(environmentId);
    await networks.bootstrap(network.binding_id, network.generation, 'pod-token');
    const grant = await service().issueRunnerEgress(workspaceId, sessionId, authority.runner_id);
    expect((await service().authorizeProxy(grant.proxy_capability, target)).decision).toBe('mitm');
    await db
      .update(schema.environmentClaims)
      .set({ workerConnId: 'replacement' })
      .where(eq(schema.environmentClaims.environmentId, environmentId));
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    await db
      .update(schema.environmentClaims)
      .set({ workerConnId: 'connection', lastPing: new Date(Date.now() - 100000) })
      .where(eq(schema.environmentClaims.environmentId, environmentId));
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    await db
      .update(schema.environmentClaims)
      .set({ lastPing: new Date() })
      .where(eq(schema.environmentClaims.environmentId, environmentId));
    await db
      .update(schema.environments)
      .set({ environmentTokenDigest: 'replacement-launch' })
      .where(eq(schema.environments.id, environmentId));
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
  });
  describe('Vault preparation and marker semantics', () => {
    let vaultCredential: typeof schema.vaultCredentials.$inferSelect;
    beforeEach(async () => {
      const vaultId = newId('vlt');
      await db.insert(schema.vaults).values({ id: vaultId, workspaceId, displayName: vaultId });
      const [row] = await db
        .insert(schema.vaultCredentials)
        .values({
          id: newId('vcrd'),
          workspaceId,
          vaultId,
          authType: 'environment_variable',
          secretName: 'API_KEY',
          networking: { type: 'limited', allowed_hosts: ['api.example.com'] },
          authConfig: { injection_location: { header: true, body: false } },
          accessSecretRef: 'env:VAULT_API_KEY',
        })
        .returning();
      vaultCredential = row!;
      await db
        .update(schema.sessions)
        .set({ vaultIds: [vaultId] })
        .where(eq(schema.sessions.id, sessionId));
    });
    const vaultRequest = (grant: Awaited<ReturnType<SandboxEgressService['issueEgress']>>) => ({
      scope: 'session' as const,
      binding_id: grant.binding_id,
      generation: grant.generation,
      target: { host: 'api.example.com', port: 443 as const },
      method: 'GET',
      path_and_query: '/fixture',
      candidate_headers: [{ name: 'x-custom-key', value: `Key ${grant.placeholders.API_KEY}` }],
    });
    it.each([
      ['reserved GITHUB_TOKEN', 'GITHUB_TOKEN', ['api.example.com']],
      ['reserved GH_TOKEN', 'GH_TOKEN', ['api.example.com']],
      ['reserved HTTP_PROXY', 'HTTP_PROXY', ['api.example.com']],
      ['reserved PIP_CERT', 'PIP_CERT', ['api.example.com']],
      ['reserved REQUESTS_CA_BUNDLE', 'REQUESTS_CA_BUNDLE', ['api.example.com']],
      ['reserved NPM_CONFIG_CAFILE', 'NPM_CONFIG_CAFILE', ['api.example.com']],
      ['reserved npm_config_proxy', 'npm_config_proxy', ['api.example.com']],
      ['reserved CARGO_HTTP_CAINFO', 'CARGO_HTTP_CAINFO', ['api.example.com']],
      ['reserved CARGO_HTTP_PROXY', 'CARGO_HTTP_PROXY', ['api.example.com']],
      ['reserved APT_CONFIG', 'APT_CONFIG', ['api.example.com']],
      [
        'reserved NODE_TLS_REJECT_UNAUTHORIZED',
        'NODE_TLS_REJECT_UNAUTHORIZED',
        ['api.example.com'],
      ],
      ['reserved SSH_ASKPASS', 'SSH_ASKPASS', ['api.example.com']],
      ['reserved ORCA_EGRESS_BOOTSTRAP_PATH', 'ORCA_EGRESS_BOOTSTRAP_PATH', ['api.example.com']],
      ['missing name', null, ['api.example.com']],
      ['empty name', '', ['api.example.com']],
      ['invalid name', 'bad-name', ['api.example.com']],
      ['empty targets', 'API_KEY', []],
      ['wildcard target', 'API_KEY', ['*.example.com']],
      ['IP target', 'API_KEY', ['127.0.0.1']],
    ] as const)(
      'rejects issuance for %s without reading secrets or persisting a binding',
      async (_case, secretName, hosts) => {
        await db
          .update(schema.vaultCredentials)
          .set({ secretName, networking: { type: 'limited', allowed_hosts: hosts } })
          .where(eq(schema.vaultCredentials.id, vaultCredential.id));
        const replica = service();
        await expect(replica.issueEgress(workspaceId, sessionId, owner)).rejects.toMatchObject({
          type: 'permission_denied',
        });
        expect(secret).not.toHaveBeenCalled();
        expect(
          await db
            .select()
            .from(schema.sandboxEgressBindings)
            .where(eq(schema.sandboxEgressBindings.sessionId, sessionId)),
        ).toEqual([]);
      },
    );
    it('prepares valid credentials without templates and substitutes arbitrary original headers', async () => {
      await db
        .update(schema.vaultCredentials)
        .set({ secretName: 'OPENAI_API_KEY' })
        .where(eq(schema.vaultCredentials.id, vaultCredential.id));
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      const marker = grant.placeholders.OPENAI_API_KEY!;
      expect(parseSessionEgressGrant(grant)).toEqual(grant);
      const candidate_headers = [
        { name: 'authorization', value: 'Bearer ' + marker },
        { name: 'x-api-key', value: marker },
        { name: 'x-arbitrary', value: 'prefix ' + marker + ', ' + marker + ' suffix' },
        { name: 'x-adjacent', value: 'prefix' + marker + marker + 'suffix' },
      ];
      const response = await service().resolveRequest({
        ...vaultRequest(grant),
        candidate_headers,
      });
      expect(response.replacements).toEqual([
        { header: 'authorization', value: 'Bearer upstream-fixture-secret' },
        { header: 'x-api-key', value: 'upstream-fixture-secret' },
        {
          header: 'x-arbitrary',
          value: 'prefix upstream-fixture-secret, upstream-fixture-secret suffix',
        },
        {
          header: 'x-adjacent',
          value: 'prefixupstream-fixture-secretupstream-fixture-secretsuffix',
        },
      ]);
      expect(secret).toHaveBeenCalledOnce();
    });
    it('substitutes multiple sources once and never scans inserted secret contents', async () => {
      await db.insert(schema.vaultCredentials).values({
        id: newId('vcrd'),
        workspaceId,
        vaultId: vaultCredential.vaultId,
        authType: 'environment_variable',
        secretName: 'SECOND_KEY',
        networking: vaultCredential.networking,
        authConfig: vaultCredential.authConfig,
        accessSecretRef: 'env:SECOND_KEY',
      });
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      const first = grant.placeholders.API_KEY!;
      const second = grant.placeholders.SECOND_KEY!;
      secret.mockImplementation(async (ref?: string) =>
        ref === 'env:SECOND_KEY' ? 'second-secret' : second,
      );
      const response = await service().resolveRequest({
        ...vaultRequest(grant),
        candidate_headers: [{ name: 'x-many', value: first + ':' + second + ':' + first }],
      });
      expect(response.replacements).toEqual([
        { header: 'x-many', value: second + ':second-secret:' + second },
      ]);
      expect(secret).toHaveBeenCalledTimes(2);
    });
    it.each(['username', 'password', 'both'])(
      'substitutes Basic %s preserving surrounding bytes',
      async (part) => {
        const grant = await service().issueEgress(workspaceId, sessionId, owner);
        const marker = grant.placeholders.API_KEY!;
        const username = part === 'password' ? '\xffuser' : '\xff' + marker;
        const password = part === 'username' ? 'pass:\xfe' : marker + ':\xfe';
        const value =
          'bAsIc ' + Buffer.from(username + ':' + password, 'latin1').toString('base64');
        const response = await service().resolveRequest({
          ...vaultRequest(grant),
          candidate_headers: [{ name: 'authorization', value }],
        });
        const decoded = Buffer.from(response.replacements[0]!.value.slice(6), 'base64');
        expect(decoded).toEqual(
          Buffer.from(
            (username + ':' + password).replaceAll(marker, 'upstream-fixture-secret'),
            'latin1',
          ),
        );
        expect(secret).toHaveBeenCalledOnce();
      },
    );
    it('denies colon introduced into Basic username', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      secret.mockResolvedValue('injected:separator');
      await expect(
        service().resolveRequest({
          ...vaultRequest(grant),
          candidate_headers: [
            {
              name: 'authorization',
              value:
                'Basic ' + Buffer.from(grant.placeholders.API_KEY + ':password').toString('base64'),
            },
          ],
        }),
      ).rejects.toMatchObject({ type: 'permission_denied' });
    });
    it.each(['Basic !!!', 'Basic dXNlcg==', 'Basic dTpw=', 'Basic dTpw ignored'])(
      'denies malformed Basic %s without reading secrets',
      async (value) => {
        const grant = await service().issueEgress(workspaceId, sessionId, owner);
        await expect(
          service().resolveRequest({
            ...vaultRequest(grant),
            candidate_headers: [{ name: 'authorization', value }],
          }),
        ).rejects.toMatchObject({ type: 'permission_denied' });
        expect(secret).not.toHaveBeenCalled();
      },
    );
    it('leaves nonmarkers and unrelated Basic credentials unchanged', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      const response = await service().resolveRequest({
        ...vaultRequest(grant),
        candidate_headers: [
          {
            name: 'authorization',
            value: 'Basic ' + Buffer.from('user:password').toString('base64'),
          },
          { name: 'x-custom', value: 'orca_vault_not-a-complete-marker' },
        ],
      });
      expect(response.replacements).toEqual([]);
      expect(secret).not.toHaveBeenCalled();
    });
    it('does not treat whitespace-prefixed malformed Basic as a raw Vault header', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      await expect(
        service().resolveRequest({
          ...vaultRequest(grant),
          candidate_headers: [
            { name: 'authorization', value: ' Basic ' + grant.placeholders.API_KEY },
          ],
        }),
      ).rejects.toMatchObject({ type: 'permission_denied' });
      expect(secret).not.toHaveBeenCalled();
    });
    it('denies complete unknown and prior-generation markers before secret reads', async () => {
      const previous = await service().issueEgress(workspaceId, sessionId, owner);
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      for (const marker of [
        previous.placeholders.API_KEY!,
        'orca_vault_' + 'a'.repeat(43),
        'orca_git_' + 'b'.repeat(43),
      ]) {
        await expect(
          service().resolveRequest({
            ...vaultRequest(grant),
            candidate_headers: [{ name: 'x-key', value: marker }],
          }),
        ).rejects.toMatchObject({ type: 'permission_denied' });
      }
      expect(secret).not.toHaveBeenCalled();
    });
    it('rejects another live Session marker without looking up its source', async () => {
      const [original] = await db
        .select()
        .from(schema.sessions)
        .where(eq(schema.sessions.id, sessionId));
      const otherSession = newId('ses');
      await db.insert(schema.sessions).values({ ...original!, id: otherSession });
      const otherNetwork = await networks.registerHarnessNetwork(
        workspaceId,
        otherSession,
        owner.owner_pod,
      );
      await networks.bootstrap(otherNetwork.binding_id, otherNetwork.generation, 'pod-token');
      const foreign = await service().issueEgress(workspaceId, otherSession, owner);
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      await expect(
        service().resolveRequest({
          ...vaultRequest(grant),
          candidate_headers: [{ name: 'x-key', value: foreign.placeholders.API_KEY! }],
        }),
      ).rejects.toMatchObject({ type: 'permission_denied' });
      expect(secret).not.toHaveBeenCalled();
    });
    it('rejects duplicate candidates even when only one contains a marker', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      await expect(
        service().resolveRequest({
          ...vaultRequest(grant),
          candidate_headers: [
            { name: 'x-key', value: grant.placeholders.API_KEY! },
            { name: 'x-key', value: 'unrelated' },
          ],
        }),
      ).rejects.toMatchObject({ type: 'permission_denied' });
      expect(secret).not.toHaveBeenCalled();
    });
    it('enforces candidate and body bounds through the workload-authenticated HTTP route', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      const app = Fastify();
      app.addHook(
        'preHandler',
        buildInternalAuth({ verify: async () => ({ caller: 'ai-gateway', subject: 'gateway' }) }),
      );
      registerSandboxEgressRoutes(app, service());
      const request = vaultRequest(grant);
      const { candidate_headers, ...missing } = request;
      try {
        for (const payload of [
          missing,
          { ...missing, authentication_headers: candidate_headers },
          {
            ...request,
            candidate_headers: Array.from({ length: 33 }, (_, i) => ({
              name: 'x-' + i,
              value: 'orca_vault_',
            })),
          },
          { ...request, candidate_headers: [{ name: 'x-key', value: 'x'.repeat(16385) }] },
          { ...request, candidate_headers: [{ name: 'x-key', value: 'line\r\nbreak' }] },
          {
            ...request,
            candidate_headers: Array.from({ length: 5 }, (_, i) => ({
              name: 'x-' + i,
              value: 'x'.repeat(16384),
            })),
          },
          { ...request, body: grant.placeholders.API_KEY },
        ]) {
          const response = await app.inject({
            method: 'POST',
            url: '/internal/v1/sandbox-egress/resolve',
            headers: { authorization: 'Bearer gateway-token' },
            payload,
          });
          expect(response.statusCode).toBe(400);
          expect(response.json().error.type).toBe('invalid_request');
        }
      } finally {
        await app.close();
      }
      expect(secret).not.toHaveBeenCalled();
    });
    it('rejects expanded replacements beyond the existing value bound', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      secret.mockResolvedValue('s'.repeat(16384));
      await expect(service().resolveRequest(vaultRequest(grant))).rejects.toMatchObject({
        type: 'permission_denied',
      });
    });
    it.each([80, 443] as const)(
      'denies a revoked known source on an unrelated HTTP%s host',
      async (port) => {
        const grant = await service().issueEgress(workspaceId, sessionId, owner);
        await db
          .update(schema.vaultCredentials)
          .set({ archivedAt: new Date() })
          .where(eq(schema.vaultCredentials.id, vaultCredential.id));
        await expect(
          service().resolveRequest({
            ...vaultRequest(grant),
            target: { host: 'other.example.com', port },
          }),
        ).rejects.toMatchObject({ type: 'permission_denied' });
        expect(secret).not.toHaveBeenCalled();
      },
    );
    it.each([
      'host',
      'connection',
      'content-length',
      'transfer-encoding',
      'proxy-authorization',
      'proxy-authenticate',
      'proxy-connection',
      'upgrade',
      'trailer',
      'te',
      'keep-alive',
      'cookie',
      'set-cookie',
    ])('denies protected header %s without reading secrets', async (name) => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      await expect(
        service().resolveRequest({
          ...vaultRequest(grant),
          candidate_headers: [{ name, value: grant.placeholders.API_KEY! }],
        }),
      ).rejects.toMatchObject({ type: 'permission_denied' });
      expect(secret).not.toHaveBeenCalled();
    });
    it('retains known markers at wrong hosts or header injection-location mismatch', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      expect(
        (
          await service().resolveRequest({
            ...vaultRequest(grant),
            target: { host: 'other.example.com', port: 443 },
          })
        ).replacements,
      ).toEqual([]);
      await db
        .update(schema.vaultCredentials)
        .set({ authConfig: { injection_location: { header: false, body: true } } })
        .where(eq(schema.vaultCredentials.id, vaultCredential.id));
      expect((await service().resolveRequest(vaultRequest(grant))).replacements).toEqual([]);
      expect(secret).not.toHaveBeenCalled();
    });
    it.each(['binding', 'source', 'environment', 'selection', 'rotation', 'location'])(
      'rechecks Vault %s changes during secret reads',
      async (change) => {
        const grant = await service().issueEgress(workspaceId, sessionId, owner);
        secret.mockImplementation(async () => {
          if (change === 'binding') await service().issueEgress(workspaceId, sessionId, owner);
          if (change === 'source')
            await db
              .update(schema.vaultCredentials)
              .set({ archivedAt: new Date() })
              .where(eq(schema.vaultCredentials.id, vaultCredential.id));
          if (change === 'environment')
            await db
              .update(schema.environments)
              .set({ networking: { type: 'limited', allowed_hosts: [] } })
              .where(eq(schema.environments.id, environmentId));
          if (change === 'selection')
            await db
              .update(schema.sessions)
              .set({ vaultIds: [] })
              .where(eq(schema.sessions.id, sessionId));
          if (change === 'rotation')
            await db
              .update(schema.vaultCredentials)
              .set({ accessSecretRef: 'env:ROTATED' })
              .where(eq(schema.vaultCredentials.id, vaultCredential.id));
          if (change === 'location')
            await db
              .update(schema.vaultCredentials)
              .set({ authConfig: { injection_location: { header: false, body: true } } })
              .where(eq(schema.vaultCredentials.id, vaultCredential.id));
          return 'secret';
        });
        await expect(service().resolveRequest(vaultRequest(grant))).rejects.toMatchObject({
          type: change === 'binding' ? 'invalid_capability' : 'permission_denied',
        });
      },
    );
    it('excludes provider credentials from environment-variable preparation checks', async () => {
      await db
        .update(schema.vaultCredentials)
        .set({
          authType: 'provider',
          provider: 'openai',
          scheme: 'bearer',
          logicalId: 'llm:openai',
          resolutionVersion: 'fixture',
          secretName: null,
          networking: {},
        })
        .where(eq(schema.vaultCredentials.id, vaultCredential.id));
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      expect(grant.placeholders).toEqual({});
      expect(grant.binding_id).toBeDefined();
      expect(secret).not.toHaveBeenCalled();
    });
    it('retains the marker without reading a secret when live targets narrow before resolve', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      await db
        .update(schema.vaultCredentials)
        .set({ networking: { type: 'limited', allowed_hosts: ['other.example.com'] } })
        .where(eq(schema.vaultCredentials.id, vaultCredential.id));
      expect(await service().resolveRequest(vaultRequest(grant))).toEqual({
        replacements: [],
        credential_version: null,
        request_body_policy: 'stream',
      });
      expect(secret).not.toHaveBeenCalled();
    });
    it('denies target narrowing during secret resolution even without a credential version change', async () => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      secret.mockImplementation(async () => {
        const [updated] = await db
          .update(schema.vaultCredentials)
          .set({ networking: { type: 'limited', allowed_hosts: ['other.example.com'] } })
          .where(eq(schema.vaultCredentials.id, vaultCredential.id))
          .returning();
        expect(updated!.updatedAt).toEqual(vaultCredential.updatedAt);
        expect(updated!.accessSecretRef).toBe(vaultCredential.accessSecretRef);
        return 'upstream-fixture-secret';
      });
      await expect(service().resolveRequest(vaultRequest(grant))).rejects.toMatchObject({
        type: 'permission_denied',
      });
      expect(secret).toHaveBeenCalledOnce();
      expect(secret).toHaveBeenCalledWith(vaultCredential.accessSecretRef);
    });
    it.each(['known marker', 'wrong marker', 'no header', 'unknown header'])(
      'does not read secrets for HTTP80 with %s',
      async (variant) => {
        const grant = await service().issueEgress(workspaceId, sessionId, owner);
        const request = {
          ...vaultRequest(grant),
          target: { host: 'api.example.com', port: 80 as const },
          candidate_headers:
            variant === 'no header'
              ? []
              : [
                  {
                    name: variant === 'unknown header' ? 'x-unknown-key' : 'x-custom-key',
                    value:
                      variant === 'wrong marker'
                        ? 'wrong-marker'
                        : `Key ${grant.placeholders.API_KEY}`,
                  },
                ],
        };
        const response = service().resolveRequest(request);
        expect(await response).toEqual({
          replacements: [],
          credential_version: null,
          request_body_policy: 'stream',
        });
        expect(secret).not.toHaveBeenCalled();
      },
    );
  });
  it('selects duplicate Vault names in Session order, resolves custom header and observes rotation/revocation', async () => {
    const vaultIds = [newId('vlt'), newId('vlt')];
    const credentialIds = [newId('vcrd'), newId('vcrd')];
    for (let index = 0; index < 2; index++) {
      await db
        .insert(schema.vaults)
        .values({ id: vaultIds[index]!, workspaceId, displayName: vaultIds[index]! });
      await db.insert(schema.vaultCredentials).values({
        id: credentialIds[index]!,
        workspaceId,
        vaultId: vaultIds[index]!,
        authType: 'environment_variable',
        secretName: 'API_KEY',
        networking: { type: 'limited', allowed_hosts: ['api.example.com'] },
        authConfig: { injection_location: { header: true, body: false } },
        accessSecretRef: `env:VAULT_${index}`,
      });
    }
    await db.update(schema.sessions).set({ vaultIds }).where(eq(schema.sessions.id, sessionId));
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    const request = {
      scope: 'session' as const,
      binding_id: grant.binding_id,
      generation: grant.generation,
      target: { host: 'api.example.com', port: 443 as const },
      method: 'GET',
      path_and_query: '/fixture',
      candidate_headers: [{ name: 'x-custom-key', value: `Key ${grant.placeholders.API_KEY}` }],
    };
    expect(
      (await service().authorizeProxy(grant.proxy_capability, request.target)).managed_auth,
    ).toBe(true);
    const first = await service().resolveRequest(request);
    expect(first.request_body_policy).toBe('stream');
    expect(
      (
        await service().resolveRequest({
          ...request,
          method: 'POST',
          path_and_query: '/fixture/repo.git/git-upload-pack',
        })
      ).request_body_policy,
    ).toBe('stream');
    expect(first.replacements).toEqual([
      { header: 'x-custom-key', value: 'Key upstream-fixture-secret' },
    ]);
    expect(secret).toHaveBeenCalledWith('env:VAULT_0');
    secret.mockResolvedValue('rotated');
    await db
      .update(schema.vaultCredentials)
      .set({ accessSecretRef: 'env:VAULT_ROTATED', updatedAt: new Date() })
      .where(eq(schema.vaultCredentials.id, credentialIds[0]!));
    expect((await service().resolveRequest(request)).replacements[0]!.value).toBe('Key rotated');
    const http = await service().resolveRequest({
      ...request,
      target: { host: 'api.example.com', port: 80 },
    });
    expect(http.replacements).toEqual([]);
    expect(
      (
        await service().resolveRequest({
          ...request,
          candidate_headers: [{ name: 'x-custom-key', value: 'wrong-marker' }],
        })
      ).replacements,
    ).toEqual([]);
    await db
      .update(schema.sessions)
      .set({ vaultIds: [...vaultIds].reverse() })
      .where(eq(schema.sessions.id, sessionId));
    await expect(service().resolveRequest(request)).rejects.toMatchObject({
      type: 'permission_denied',
    });
    await db.update(schema.sessions).set({ vaultIds }).where(eq(schema.sessions.id, sessionId));
    await db
      .update(schema.vaults)
      .set({ archivedAt: new Date() })
      .where(eq(schema.vaults.id, vaultIds[0]!));
    await expect(service().resolveRequest(request)).rejects.toMatchObject({
      type: 'permission_denied',
    });
  });
  it('fences old assigned runner across launch replacement and rebinds durably across replicas', async () => {
    const original = await registryWorker();
    await bindAssigned(oldRunnerId, original.source);
    const oldGrant = await service().issueRunnerEgress(workspaceId, sessionId, oldRunnerId);
    const freshToken = await new EnvironmentTokenStore(db).mintEnvironmentToken(
      environmentId,
      undefined,
      undefined,
      'new-sandbox',
    );
    owner.sandbox_pod = { ...owner.sandbox_pod, uid: 'new-pod-uid' };
    await expect(
      service().issueRunnerEgress(workspaceId, sessionId, oldRunnerId),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
    await expect(service().authorizeProxy(oldGrant.proxy_capability, target)).rejects.toMatchObject(
      { type: 'invalid_capability' },
    );
    await db
      .update(schema.environmentClaims)
      .set({ ownerPod: 'new-registry', workerConnId: 'new-connection' })
      .where(eq(schema.environmentClaims.environmentId, environmentId));
    const source = await service().authenticatedWorkerLaunch(
      environmentId,
      freshToken,
      'new-registry',
      'new-connection',
    );
    await expect(
      service().issueRunnerEgress(workspaceId, sessionId, oldRunnerId),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
    await expect(
      service().issueEgress(workspaceId, sessionId, {
        kind: 'registry',
        runner_id: oldRunnerId,
        ...source,
      }),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
    const [unchanged] = await db
      .select()
      .from(schema.sessions)
      .where(eq(schema.sessions.id, sessionId));
    expect(unchanged!.runnerLaunchAuthority).toMatchObject({
      runner_id: oldRunnerId,
      launch_digest: original.source.launch_digest,
    });
    expect(await service().retireStaleRunnerLaunches(environmentId, source)).toEqual([oldRunnerId]);
    expect(
      await buildDistributionSessionStore(db).applyDistributionPatch(
        sessionId,
        { runnerId: null, hostEnvironmentId: null, distributionState: 'pending' },
        oldRunnerId,
      ),
    ).toBe(true);
    expect(
      await buildDistributionSessionStore(db).loadPendingForEnvironment(environmentId),
    ).toContainEqual({ id: sessionId });
    const replica = new SandboxEgressService(
      drizzle(pool, { schema }),
      { resolve: secret },
      options,
    );
    owner.sandbox_handle_id = 'new-sandbox';
    const freshNetwork = await networks.registerEnvironmentNetwork(environmentId);
    await networks.bootstrap(freshNetwork.binding_id, freshNetwork.generation, 'pod-token');
    await bindAssigned('fresh-runner', source, replica);
    const grant = await service().issueRunnerEgress(workspaceId, sessionId, 'fresh-runner');
    expect(grant.network_binding_id).toBe(freshNetwork.binding_id);
    expect(grant).not.toHaveProperty('proxy_url');
    expect(grant).not.toHaveProperty('ca_bundle_path');
    expect((await service().authorizeProxy(grant.proxy_capability, target)).decision).toBe('mitm');
    expect(
      await buildDistributionSessionStore(db, replica).applyDistributionPatch(
        sessionId,
        { runnerId: null, hostEnvironmentId: null, distributionState: 'failed' },
        oldRunnerId,
      ),
    ).toBe(false);
    await expect(
      replica.bindRunnerLaunch(sessionId, 'fresh-runner', source, () => {}),
    ).rejects.toThrow('fresh binding');
    const [current] = await db
      .select()
      .from(schema.sessions)
      .where(eq(schema.sessions.id, sessionId));
    expect(current!.runnerLaunchAuthority).toMatchObject({
      runner_id: 'fresh-runner',
      launch_digest: source.launch_digest,
    });
    await expect(
      service().retireStaleRunnerLaunches(environmentId, original.source),
    ).rejects.toThrow('replaced');
    expect((await service().authorizeProxy(grant.proxy_capability, target)).decision).toBe('mitm');
  });
  it('retires stale assigned provenance on same-launch worker reconnect and makes it pending for fresh dispatch', async () => {
    const { token, source } = await registryWorker();
    await bindAssigned(oldRunnerId, source);
    const grant = await service().issueRunnerEgress(workspaceId, sessionId, oldRunnerId);
    await db
      .update(schema.environmentClaims)
      .set({ workerConnId: 'reconnected' })
      .where(eq(schema.environmentClaims.environmentId, environmentId));
    const replacement = await service().authenticatedWorkerLaunch(
      environmentId,
      token,
      'registry',
      'reconnected',
    );
    await expect(
      service().issueRunnerEgress(workspaceId, sessionId, oldRunnerId),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
    const distributor = new SessionDistributor({
      sessions: buildDistributionSessionStore(db, service()),
      environments: { loadTarget: async () => 'cloud' },
      tunnelRegistry: new TunnelRegistry(),
      workerRegistry: connectedWorker(replacement),
    });
    const dispatch = vi.spyOn(distributor, 'dispatch').mockResolvedValue({
      state: 'pending',
      runnerId: null,
      launchSettled: Promise.resolve({ status: 'no_worker' }),
    });
    await distributor.onWorkerConnect(environmentId);
    expect(dispatch).toHaveBeenCalledWith({
      sessionId,
      environment: { id: environmentId, target: 'cloud' },
    });
    const [pending] = await db
      .select()
      .from(schema.sessions)
      .where(eq(schema.sessions.id, sessionId));
    expect(pending).toMatchObject({
      runnerId: null,
      runnerLaunchAuthority: null,
      distributionState: 'pending',
    });
    await bindAssigned('new-runner', replacement);
    expect(
      (await service().issueRunnerEgress(workspaceId, sessionId, 'new-runner')).generation,
    ).toBeTruthy();
    await expect(service().authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    await db
      .update(schema.environments)
      .set({ environmentTokenExpiresAt: new Date(Date.now() - 1) })
      .where(eq(schema.environments.id, environmentId));
    await expect(
      service().issueRunnerEgress(workspaceId, sessionId, 'new-runner'),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
  });
  it('rejects authenticated-source races before binding and between commit/send with scoped cleanup', async () => {
    const { token, source } = await registryWorker();
    const newerToken = await new EnvironmentTokenStore(db).mintEnvironmentToken(
      environmentId,
      undefined,
      undefined,
      owner.sandbox_handle_id,
    );
    const send = vi.fn();
    await expect(service().bindRunnerLaunch(sessionId, 'raced', source, send)).rejects.toThrow(
      'replaced',
    );
    expect(send).not.toHaveBeenCalled();
    await expect(
      service().authenticatedWorkerLaunch(environmentId, token, 'registry', 'original-connection'),
    ).rejects.toThrow('current launch-token');
    const currentSource = await service().authenticatedWorkerLaunch(
      environmentId,
      newerToken,
      'registry',
      'original-connection',
    );
    const racingDb = drizzle(pool, { schema });
    const transact = racingDb.transaction.bind(racingDb);
    let committed = false;
    vi.spyOn(racingDb, 'transaction').mockImplementation(async (callback, config) => {
      const result = await transact(callback, config);
      if (!committed) {
        committed = true;
        await db
          .update(schema.environmentClaims)
          .set({ workerConnId: 'racing-connection' })
          .where(eq(schema.environmentClaims.environmentId, environmentId));
      }
      return result;
    });
    const replica = new SandboxEgressService(racingDb, { resolve: secret }, options);
    const workers = connectedWorker(currentSource);
    const enqueue = vi.spyOn(workers, 'sendText');
    const distributor = new SessionDistributor({
      sessions: buildDistributionSessionStore(db, replica),
      environments: { loadTarget: async () => 'cloud' },
      tunnelRegistry: new TunnelRegistry(),
      workerRegistry: workers,
    });
    const outcome = await distributor.dispatch({
      sessionId,
      environment: { id: environmentId, target: 'cloud' },
    });
    expect(await outcome.launchSettled).toMatchObject({
      status: 'failed',
      errorCode: 'sandbox_egress_provenance_required',
    });
    expect(enqueue).not.toHaveBeenCalled();
    const [failed] = await db
      .select()
      .from(schema.sessions)
      .where(eq(schema.sessions.id, sessionId));
    expect(failed).toMatchObject({
      runnerId: null,
      runnerLaunchAuthority: null,
      distributionState: 'failed',
    });
    await db
      .update(schema.sessions)
      .set({
        runnerId: 'legacy-runner',
        hostEnvironmentId: environmentId,
        distributionState: 'assigned',
      })
      .where(eq(schema.sessions.id, sessionId));
    await expect(
      service().issueRunnerEgress(workspaceId, sessionId, 'legacy-runner'),
    ).rejects.toThrow('reprovision Environment');
  });
  it('preserves stale provenance after teardown failure and retries before fresh dispatch', async () => {
    const { token, source } = await registryWorker();
    await bindAssigned(oldRunnerId, source);
    await db
      .update(schema.environmentClaims)
      .set({ workerConnId: 'replacement' })
      .where(eq(schema.environmentClaims.environmentId, environmentId));
    const replacement = await service().authenticatedWorkerLaunch(
      environmentId,
      token,
      'registry',
      'replacement',
    );
    const store = buildDistributionSessionStore(db, service());
    const failing = new SessionDistributor({
      sessions: store,
      environments: { loadTarget: async () => 'cloud' },
      tunnelRegistry: new TunnelRegistry(),
      workerRegistry: connectedWorker(replacement, 'failed'),
    });
    const dispatch = vi.spyOn(failing, 'dispatch');
    await expect(failing.onWorkerConnect(environmentId)).rejects.toThrow('reprovision Environment');
    expect(dispatch).not.toHaveBeenCalled();
    const [failed] = await db
      .select()
      .from(schema.sessions)
      .where(eq(schema.sessions.id, sessionId));
    expect(failed).toMatchObject({
      runnerId: oldRunnerId,
      distributionState: 'failed',
      runnerLaunchAuthority: { runner_id: oldRunnerId, worker_conn_id: source.worker_conn_id },
    });
    const retry = new SessionDistributor({
      sessions: store,
      environments: { loadTarget: async () => 'cloud' },
      tunnelRegistry: new TunnelRegistry(),
      workerRegistry: connectedWorker(replacement),
    });
    const recovered = vi.spyOn(retry, 'dispatch').mockResolvedValue({
      state: 'pending',
      runnerId: null,
      launchSettled: Promise.resolve({ status: 'no_worker' }),
    });
    await retry.onWorkerConnect(environmentId);
    expect(recovered).toHaveBeenCalledOnce();
    const [pending] = await db
      .select()
      .from(schema.sessions)
      .where(eq(schema.sessions.id, sessionId));
    expect(pending).toMatchObject({
      runnerId: null,
      runnerLaunchAuthority: null,
      distributionState: 'pending',
    });
  });
  it('persists sandbox association before worker start and fences stale cross-replica cleanup', async () => {
    let online = false;
    const tokens = new EnvironmentTokenStore(db);
    const lifecycle = new EnvironmentLaunchLifecycle({
      networks,
      loadProvisionSpec: async () => ({}),
      tokens,
      registryTunnelUrl: 'https://registry.example',
      workerOnline: { get: () => (online ? {} : undefined) },
      launcher: {
        provision: async (_name, spec) => {
          const request = spec!.sandboxNetwork!;
          owner.sandbox_handle_id = 'sandbox-from-launch';
          await networks.bootstrap(request.binding_id, request.generation, 'pod-token');
          return 'sandbox-from-launch';
        },
        startWorker: async (sandboxId, input) => {
          expect(input.sandboxEgress).toBe(true);
          const [row] = await db
            .select()
            .from(schema.environments)
            .where(eq(schema.environments.id, environmentId));
          expect(row!.cloudSandboxHandleId).toBe(sandboxId);
          expect(row!.environmentTokenDigest).toBe(hashEnvironmentToken(input.token));
          online = true;
        },
        terminate: async () => {},
        isRunning: async () => true,
      },
    });
    expect((await lifecycle.ensureLaunched(environmentId, { name: 'fixture' })).status).toBe(
      'launched',
    );
    const first = await tokens.mintEnvironmentToken(
      environmentId,
      undefined,
      undefined,
      'old-sandbox',
    );
    // A new store/service has no launcher Map; its only source is the durable row.
    const anotherReplica = new EnvironmentTokenStore(drizzle(pool, { schema }));
    const second = await anotherReplica.mintEnvironmentToken(
      environmentId,
      undefined,
      undefined,
      'new-sandbox',
    );
    await tokens.revokeEnvironmentToken(environmentId, first);
    const [latest] = await db
      .select()
      .from(schema.environments)
      .where(eq(schema.environments.id, environmentId));
    expect(latest!.cloudSandboxHandleId).toBe('new-sandbox');
    expect(latest!.environmentTokenDigest).toBe(hashEnvironmentToken(second));
    await anotherReplica.revokeEnvironmentToken(environmentId, second);
    const [revoked] = await db
      .select()
      .from(schema.environments)
      .where(eq(schema.environments.id, environmentId));
    expect(revoked!.cloudSandboxHandleId).toBeNull();
    expect(revoked!.environmentTokenDigest).toBeNull();
  });
  it.each(['', 'bad\rheader', 'bad\nheader', 'bad\0header'])(
    'rejects invalid resolved secret %j at the resolver boundary',
    async (value) => {
      const grant = await service().issueEgress(workspaceId, sessionId, owner);
      secret.mockResolvedValue(value);
      await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
        type: 'dependency_unavailable',
      });
    },
  );
  it('denies credential revocation during secret read and secret-store outage', async () => {
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    secret.mockRejectedValue(new Error('secret storage unavailable'));
    await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
      type: 'dependency_unavailable',
    });
    secret.mockImplementation(async () => {
      await db
        .update(schema.gitCredentials)
        .set({ archivedAt: new Date() })
        .where(eq(schema.gitCredentials.id, credentialId));
      return 'secret';
    });
    await expect(service().resolveRequest(resolve(grant))).rejects.toMatchObject({
      type: 'permission_denied',
    });
  });
  it('enforces gateway workload role and fixed error envelope without secrets', async () => {
    const app = Fastify();
    app.addHook(
      'preHandler',
      buildInternalAuth({
        verify: async (token) =>
          token === 'gateway-token'
            ? { caller: 'ai-gateway', subject: 'gateway' }
            : token === 'harness-token'
              ? { caller: 'harness', subject: 'harness' }
              : null,
      }),
    );
    registerSandboxEgressRoutes(app, service());
    const grant = await service().issueEgress(workspaceId, sessionId, owner);
    const authorize = await app.inject({
      method: 'POST',
      url: '/internal/v1/sandbox-egress/authorize',
      headers: { authorization: 'Bearer gateway-token' },
      payload: { capability: grant.proxy_capability, target },
    });
    expect(authorize.statusCode).toBe(200);
    expect(authorize.headers['cache-control']).toBe('private, no-store');
    const issue = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/session-egress/issue`,
      headers: { authorization: 'Bearer gateway-token' },
      payload: { sandbox_id: owner.sandbox_handle_id, owner: owner.owner_pod },
    });
    expect(issue.statusCode).toBe(403);
    const invalid = await app.inject({
      method: 'POST',
      url: '/internal/v1/sandbox-egress/authorize',
      headers: { authorization: 'Bearer gateway-token' },
      payload: { capability: 'x'.repeat(43), target },
    });
    expect(invalid.statusCode).toBe(401);
    expect(invalid.json()).toEqual({
      error: { type: 'invalid_capability', message: 'invalid capability' },
    });
    const workloadDenied = await app.inject({
      method: 'POST',
      url: '/internal/v1/sandbox-egress/authorize',
      payload: { capability: grant.proxy_capability, target },
    });
    expect(workloadDenied.statusCode).toBe(401);
    expect(workloadDenied.json().error).toBe('unauthenticated');
    const harnessIssue = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/session-egress/issue`,
      headers: { authorization: 'Bearer harness-token' },
      payload: { sandbox_id: owner.sandbox_handle_id, owner: owner.owner_pod },
    });
    expect(harnessIssue.statusCode).toBe(200);
    expect(harnessIssue.json().placeholders).not.toHaveProperty('GH_TOKEN');
    expect(harnessIssue.json().git).toMatchObject({ username: 'x-access-token' });
    await app.close();
  });
});
