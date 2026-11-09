// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { FrameKind } from '@orca/harness-tunnel';
import { InMemoryBlobStore, InMemoryFileMetadataStore, LocalFileStore } from '@orca/file-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { TranscriptStore } from '@orca/transcript-store';
import * as schema from '../../src/persistence/postgres/schema.js';
import { SandboxNetworkService } from '../../src/domain/sandbox-network.js';
import { SandboxEgressService } from '../../src/domain/sandbox-egress.js';
import { newId } from '../../src/domain/versioning.js';
import { SessionEventBridgeManager } from '../../src/tunnel/session-event-bridge.js';
import { SessionResourcesDelivery } from '../../src/tunnel/session-resources-delivery.js';
import { TunnelRegistry } from '../../src/tunnel/tunnel-registry.js';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL ?? 'postgres://orca:orca@localhost:5432/registry',
});
const db = drizzle(pool, { schema });
beforeAll(async () => {
  await migrate(db, { migrationsFolder: 'src/persistence/postgres/migrations' });
});
afterAll(async () => {
  await pool.end();
});

it('revokes a delivered capability when subsequent Skills preparation fails', async () => {
  const workspaceId = newId('ws'),
    sessionId = newId('ses'),
    organizationId = newId('org'),
    agentId = newId('agt'),
    environmentId = newId('env');
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
    metadata: { harness: 'codex_sdk', mode: 'separate' },
    modelProvider: 'openai',
    modelId: 'fixture',
  });
  await db.insert(schema.agentVersions).values({
    id: newId('agtv'),
    workspaceId,
    agentId,
    version: 1,
    snapshot: { metadata: { harness: 'codex_sdk', mode: 'separate' }, harness_type: 'codex_sdk' },
  });
  await db.insert(schema.sessions).values({
    id: sessionId,
    workspaceId,
    agentId,
    agentVersion: 1,
    environmentId,
    runnerId: 'runner-fixture',
    distributionState: 'assigned',
  });
  const pod = { namespace: 'sandboxes', name: 'sandbox', uid: 'sandbox-uid' };
  const networks = new SandboxNetworkService(db, {
    authority: {
      bootstrap: async () => ({ pod, workload: pod }),
      current: async () => true,
      ownerLive: async () => true,
    },
    proxyUrl: 'https://proxy.example:8443',
    internalBypass: [],
  });
  const network = await networks.registerHarnessNetwork(workspaceId, sessionId, {
    ...pod,
    name: 'harness',
    uid: 'harness-uid',
  });
  await networks.bootstrap(network.binding_id, network.generation, 'pod-token');
  const service = new SandboxEgressService(
    db,
    { resolve: async () => null },
    {
      networks,
      githubRestReadEnabled: false,
      environmentClaimTtlMs: 90000,
      verifyLiveness: async () => true,
      locateSandbox: async () => pod,
    },
  );
  const grant = await service.issueEgress(workspaceId, sessionId, {
    kind: 'harness',
    owner_pod: { ...pod, name: 'harness', uid: 'harness-uid' },
    sandbox_pod: pod,
    sandbox_handle_id: 'sandbox',
  });
  const target = { host: 'example.org', port: 443 as const };
  expect((await service.authorizeProxy(grant.proxy_capability, target)).decision).toBe(
    'passthrough',
  );
  const files = new LocalFileStore({
    blobStore: new InMemoryBlobStore(),
    metadataStore: new InMemoryFileMetadataStore(),
  });
  const store: TranscriptStore = {
    append: async () => [],
    read: async function* () {},
    tail: async function* () {},
    archive: async () => {},
    close: async () => {},
  };
  const delivered = grant;
  const manifestSent = vi.fn();
  const resources = new SessionResourcesDelivery({
    workspaceId,
    sessionId,
    fileStore: files,
    store,
    isCurrent: async () => true,
    prepare: async () => ({
      sessionEgress: delivered,
      manifest: { version: 1, revision: 'a'.repeat(64), resources: [] },
      memoryStores: new Map(),
      networkAllowedDomains: [],
      open: async () => {
        throw new Error('unused');
      },
      close: async () => {},
    }),
    request: async (_path, body) => {
      if ((body as { type?: string }).type === 'manifest') manifestSent(body);
      return { committed: true, checkpoint: null };
    },
    onPreparationFailure: async (failed) => {
      if (failed)
        await service.revokeEgress(workspaceId, sessionId, failed.binding_id, failed.generation);
    },
  });
  const registry = new TunnelRegistry();
  registry.register(
    'runner-fixture',
    {
      sendText: async () => {},
      receiveText: async () => {
        throw new Error('unused');
      },
    },
    { kind: FrameKind.Hello, runnerVersion: 'fixture', frameProtocolVersion: 1 },
  );
  const manager = new SessionEventBridgeManager({
    store,
    registry,
    recoverOnConnect: false,
    resolver: { resolveBoundSession: async () => ({ workspaceId, sessionId }) },
    resourcesFactory: async () => resources,
    skillsProvider: {
      resolveSkillBundles: async () => {
        throw new Error('Skills fixture unavailable');
      },
    },
    skillStore: new InMemorySkillStore(),
  });
  try {
    await manager.onRunnerConnect('runner-fixture');
    expect(manifestSent).toHaveBeenCalledOnce();
    expect(registry.has('runner-fixture')).toBe(false);
    expect(manager.size).toBe(0);
    await expect(service.authorizeProxy(grant.proxy_capability, target)).rejects.toMatchObject({
      type: 'invalid_capability',
    });
    await expect(
      service.resolveRequest({
        scope: 'session',
        binding_id: grant.binding_id,
        generation: grant.generation,
        target,
        method: 'GET',
        path_and_query: '/fixture',
        candidate_headers: [],
      }),
    ).rejects.toMatchObject({ type: 'invalid_capability' });
    const successor = await service.issueEgress(workspaceId, sessionId, {
      kind: 'harness',
      owner_pod: { ...pod, name: 'harness', uid: 'harness-uid' },
      sandbox_pod: pod,
      sandbox_handle_id: 'sandbox',
    });
    await resources.abortPreparation();
    expect((await service.authorizeProxy(successor.proxy_capability, target)).generation).toBe(
      successor.generation,
    );
  } finally {
    await manager.stopAll();
  }
});
