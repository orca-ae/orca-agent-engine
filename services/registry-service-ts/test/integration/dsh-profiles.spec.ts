// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq } from 'drizzle-orm';
import { LocalFileStore, InMemoryBlobStore, InMemoryFileMetadataStore } from '@orca/file-store';
import { dshProfileConfigSha256 } from '@orca/harness-catalog';
import { buildCombinedTestApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  environments,
  agentTriggers,
  agentTriggerFires,
  sessions,
} from '../../src/persistence/postgres/schema.js';
import {
  reconcileDueAgentTriggers,
  dispatchPendingAgentTriggerFires,
} from '../../src/domain/trigger-reconciler.js';
import {
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import { nativeProfileFiles } from '../helpers/dsh-profile.js';

describe('immutable Agent-bound native DSH profiles (Registry integration)', () => {
  let app: FastifyInstance;
  let revoked: FastifyInstance;
  let db: DbClient;
  let workspaceId: string;
  let key: string;
  let foreignKey: string;
  let environmentId: string;
  const fileStore = new LocalFileStore({
    blobStore: new InMemoryBlobStore(),
    metadataStore: new InMemoryFileMetadataStore(),
  });
  const files = nativeProfileFiles();
  const digest = dshProfileConfigSha256(files);
  let approved: Record<string, string[]>;
  const selection = { harness: 'dsh_sdk', mode: 'colocated' };
  const headers = () => ({ 'x-api-key': key, 'orca-beta': '1' });
  const post = (url: string, payload: object, target = app) =>
    target.inject({ method: 'POST', url, headers: headers(), payload });

  beforeAll(async () => {
    ({ db } = await getTestDb());
    workspaceId = uniqueWorkspace('dsh_profiles');
    key = await createTestApiKey(db, workspaceId);
    foreignKey = await createTestApiKey(db, uniqueWorkspace('dsh_foreign'));
    approved = { [workspaceId]: [digest] };
    const options = {
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore,
    };
    app = buildCombinedTestApp({ ...options, dshApprovedProfiles: approved });
    revoked = buildCombinedTestApp(options);
    await Promise.all([app.ready(), revoked.ready()]);
    const environment = await post('/v1/environments', {
      name: 'DSH profile environment',
      config: { type: 'cloud', networking: { type: 'unrestricted' } },
    });
    expect(environment.statusCode, environment.body).toBe(200);
    environmentId = environment.json().id;
  });

  afterAll(async () => {
    await Promise.all([app?.close(), revoked?.close()]);
    await fileStore.close();
    await closeTestDb();
  });

  async function upload(text = JSON.stringify({ version: 1, files })) {
    const boundary = 'orca-dsh-profile-boundary';
    const payload = [
      '--' + boundary,
      'Content-Disposition: form-data; name="file"; filename="profile.json"',
      'Content-Type: application/json',
      '',
      text,
      '--' + boundary + '--',
      '',
    ].join(String.fromCharCode(13, 10));
    const result = await app.inject({
      method: 'POST',
      url: '/v1/files',
      headers: { ...headers(), 'content-type': 'multipart/form-data; boundary=' + boundary },
      payload,
    });
    expect(result.statusCode, result.body).toBe(200);
    return result.json().id as string;
  }

  async function createAgent(fileId: string, extra = {}) {
    const result = await post('/v1/agents', {
      name: 'Profile agent',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
      metadata: { ...selection, dsh_profile: fileId },
      ...extra,
    });
    expect(result.statusCode, result.body).toBe(200);
    return result.json() as { id: string; version: number; metadata: Record<string, string> };
  }

  const createSession = (agentId: string, version = 1, extra = {}, target = app) =>
    post(
      '/v1/sessions',
      { agent: { type: 'agent', id: agentId, version }, environment_id: environmentId, ...extra },
      target,
    );
  const prepare = (id: string, target = app, capabilities: unknown = ['dsh_profile_v1']) =>
    post(
      '/internal/v1/workspaces/' + workspaceId + '/sessions/' + id + '/executions:prepare',
      capabilities === undefined ? {} : { capabilities },
      target,
    );
  async function snapshot(agentId: string, version: number) {
    return (
      await db
        .select()
        .from(agentVersions)
        .where(
          and(
            eq(agentVersions.workspaceId, workspaceId),
            eq(agentVersions.agentId, agentId),
            eq(agentVersions.version, version),
          ),
        )
    )[0]!.snapshot as Record<string, unknown>;
  }

  it('preserves toolset defaults across non-beta projection and round-trip updates', async () => {
    const agent = await createAgent(await upload(), {
      tools: [
        {
          type: 'agent_toolset',
          configs: [
            { name: 'business_probe', enabled: true },
            { name: 'web_fetch', enabled: false },
          ],
        },
      ],
    });
    const url = '/v1/agents/' + agent.id;
    const nativeHeaders = { 'x-api-key': key };
    const read = await app.inject({ method: 'GET', url, headers: nativeHeaders });
    expect(read.statusCode, read.body).toBe(200);
    const tools = read.json().tools;
    expect(tools[0].configs[0]).toMatchObject({
      name: 'business_probe',
      enabled: true,
      permission_policy: { type: 'always_allow' },
    });
    const same = await app.inject({
      method: 'POST',
      url,
      headers: nativeHeaders,
      payload: { tools },
    });
    expect(same.statusCode, same.body).toBe(200);
    expect(same.json().version).toBe(1);
    const renamed = await app.inject({
      method: 'POST',
      url,
      headers: nativeHeaders,
      payload: { name: 'Profile round trip', tools },
    });
    expect(renamed.statusCode, renamed.body).toBe(200);
    expect(renamed.json().version).toBe(2);
    tools[0].configs[0].permission_policy = { type: 'always_ask' };
    const ask = await app.inject({
      method: 'POST',
      url,
      headers: nativeHeaders,
      payload: { tools },
    });
    expect(ask.statusCode, ask.body).toBe(200);
    expect(ask.json().version).toBe(3);
    expect(ask.json().tools[0].configs[0].permission_policy).toEqual({ type: 'always_ask' });
    const original = await snapshot(agent.id, 1);
    const configured = await snapshot(agent.id, 3);
    expect(original).toMatchObject({
      tools: [
        {
          configs: [
            { name: 'business_probe', enabled: true },
            { name: 'web_fetch', enabled: false },
          ],
        },
      ],
    });
    expect(JSON.stringify(original.tools)).not.toContain('permission_policy');
    expect(configured).toMatchObject({
      tools: [
        {
          configs: [
            { permission_policy: { type: 'always_ask' } },
            { name: 'web_fetch', enabled: false },
          ],
        },
      ],
    });
  });

  it('uploads, atomically retains, reuses after deletion, and strips every public projection', async () => {
    const fileId = await upload();
    const agent = await createAgent(fileId);
    const descriptor = (await snapshot(agent.id, 1)).dsh_profile;
    expect(descriptor).toMatchObject({
      version: 1,
      source_file_id: fileId,
      config_sha256: digest,
      files,
    });
    const noDownload = await app.inject({
      method: 'GET',
      url: '/v1/files/' + fileId + '/content',
      headers: headers(),
    });
    expect(noDownload.statusCode).toBe(403);
    const deleted = await app.inject({
      method: 'DELETE',
      url: '/v1/files/' + fileId,
      headers: headers(),
    });
    expect(deleted.statusCode, deleted.body).toBe(200);
    const opened = vi.spyOn(fileStore, 'open');
    try {
      const noOp = await post('/v1/agents/' + agent.id, { metadata: agent.metadata });
      expect(noOp.statusCode, noOp.body).toBe(200);
      expect(noOp.json().version).toBe(1);
      const updates = await Promise.all([
        post('/v1/agents/' + agent.id, { name: 'Retained rename' }),
        post('/v1/agents/' + agent.id, { description: 'Retained description' }),
      ]);
      for (const update of updates) expect(update.statusCode, update.body).toBe(200);
      expect((await snapshot(agent.id, 2)).dsh_profile).toEqual(descriptor);
      expect((await snapshot(agent.id, 3)).dsh_profile).toEqual(descriptor);
      const modelEdit = await post('/v1/agents/' + agent.id, {
        model: { provider: 'anthropic', id: 'claude-sonnet-4-6', effort: 'high' },
        system: 'Retained system',
        tools: [],
        metadata: { tag: 'preserved' },
      });
      expect(modelEdit.statusCode, modelEdit.body).toBe(200);
      expect((await snapshot(agent.id, 4)).dsh_profile).toEqual(descriptor);
      const session = await createSession(agent.id, 1, {
        agent: {
          type: 'agent_with_overrides',
          id: agent.id,
          version: 1,
          system: 'Session override',
        },
      });
      expect(session.statusCode, session.body).toBe(200);
      const execution = await prepare(session.json().id);
      expect(execution.statusCode, execution.body).toBe(200);
      expect(execution.json()).toMatchObject({
        workspace_id: workspaceId,
        session: { agent_id: agent.id, agent_version: 1 },
        primary_agent: {
          id: agent.id,
          version: 1,
          workspace_id: workspaceId,
          system: 'Session override',
          dsh_profile: descriptor,
        },
      });
      expect(execution.json().resources).toEqual([]);
      for (const url of [
        '/v1/agents/' + agent.id,
        '/v1/agents/' + agent.id + '?version=1',
        '/v1/agents/' + agent.id + '/versions',
        '/v1/agents',
        '/v1/sessions/' + session.json().id,
        '/v1/sessions/' + session.json().id + '/threads',
      ]) {
        const response = await app.inject({ method: 'GET', url, headers: headers() });
        expect(response.statusCode, response.body).toBe(200);
        expect(response.body).not.toContain('pnpm-lock.yaml');
        expect(response.body).not.toContain('source_sha256');
      }
      expect(opened).not.toHaveBeenCalled();
    } finally {
      opened.mockRestore();
    }
  });

  it('gates old-version preparation and launch on current approval/networking without fallback', async () => {
    const agent = await createAgent(await upload());
    const session = await createSession(agent.id);
    expect(session.statusCode, session.body).toBe(200);
    const id = session.json().id;
    expect((await prepare(id, app, [])).statusCode).toBe(409);
    expect((await prepare(id, app, 'dsh_profile_v1')).statusCode).toBe(400);
    expect((await prepare(id, revoked)).statusCode).toBe(409);
    expect((await createSession(agent.id, 1, {}, revoked)).statusCode).toBe(400);
    const rename = await post('/v1/agents/' + agent.id, { name: 'Revoked rename' }, revoked);
    expect(rename.statusCode).toBe(400);
    expect(
      (await db.select().from(agentVersions).where(eq(agentVersions.agentId, agent.id))).length,
    ).toBe(1);
    await db
      .update(environments)
      .set({ networking: { type: 'limited' } })
      .where(eq(environments.id, environmentId));
    try {
      expect((await createSession(agent.id)).statusCode).toBe(400);
      const refused = await prepare(id);
      expect(refused.statusCode, refused.body).toBe(409);
      expect(refused.json().resource_type).toBe('dsh_profile');
    } finally {
      await db
        .update(environments)
        .set({ networking: { type: 'unrestricted' } })
        .where(eq(environments.id, environmentId));
    }
    await db
      .update(environments)
      .set({ target: 'self_hosted' })
      .where(eq(environments.id, environmentId));
    try {
      expect((await createSession(agent.id)).statusCode).toBe(400);
      expect((await prepare(id)).statusCode).toBe(409);
    } finally {
      await db
        .update(environments)
        .set({ target: 'cloud' })
        .where(eq(environments.id, environmentId));
    }
    const cleared = await post(
      '/v1/agents/' + agent.id,
      { name: 'Default again', metadata: { dsh_profile: null } },
      revoked,
    );
    expect(cleared.statusCode, cleared.body).toBe(200);
    expect((await snapshot(agent.id, 2)).dsh_profile).toBeUndefined();
    expect((await snapshot(agent.id, 1)).dsh_profile).toBeDefined();
    expect((await prepare(id, revoked)).statusCode).toBe(409);
    const defaults = await createSession(agent.id, 2, {}, revoked);
    expect(defaults.statusCode, defaults.body).toBe(200);
    const oldBody = await post(
      '/internal/v1/workspaces/' +
        workspaceId +
        '/sessions/' +
        defaults.json().id +
        '/executions:prepare',
      {},
      revoked,
    );
    expect(oldBody.statusCode, oldBody.body).toBe(200);
    const archived = await post('/v1/agents/' + agent.id + '/archive', {}, revoked);
    expect(archived.statusCode, archived.body).toBe(200);
    const deleted = await revoked.inject({
      method: 'DELETE',
      url: '/v1/agents/' + agent.id,
      headers: headers(),
    });
    expect(deleted.statusCode, deleted.body).toBe(200);
  });

  it('migrates only explicit legacy reassertion; malformed readers fail closed', async () => {
    const agent = await createAgent(await upload());
    const session = await createSession(agent.id);
    expect(session.statusCode, session.body).toBe(200);
    const legacy = await snapshot(agent.id, 1);
    delete legacy.dsh_profile;
    await db
      .update(agentVersions)
      .set({ snapshot: legacy })
      .where(and(eq(agentVersions.agentId, agent.id), eq(agentVersions.version, 1)));
    expect((await post('/v1/agents/' + agent.id, { name: 'Must reassert' })).statusCode).toBe(400);
    expect((await createSession(agent.id)).statusCode).toBe(503);
    expect((await prepare(session.json().id)).statusCode).toBe(409);
    const reassert = await post('/v1/agents/' + agent.id, { metadata: agent.metadata });
    expect(reassert.statusCode, reassert.body).toBe(200);
    expect(reassert.json().version).toBe(2);
    expect((await snapshot(agent.id, 2)).dsh_profile).toBeDefined();
    const unsupported = { ...(await snapshot(agent.id, 2)), dsh_profile: { version: 99 } };
    await db
      .update(agentVersions)
      .set({ snapshot: unsupported })
      .where(and(eq(agentVersions.agentId, agent.id), eq(agentVersions.version, 2)));
    expect((await createSession(agent.id, 2)).statusCode).toBe(503);
    expect(
      (await post('/v1/agents/' + agent.id, { metadata: { dsh_profile: null } }, revoked))
        .statusCode,
    ).toBe(200);
  });

  it('replays ingestion idempotently, ingests changed sources and rolls back denied updates', async () => {
    const fileId = await upload();
    const payload = {
      name: 'Idempotent profile',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
      metadata: { ...selection, dsh_profile: fileId },
    };
    const request = {
      method: 'POST' as const,
      url: '/v1/agents',
      headers: { ...headers(), 'idempotency-key': 'dsh-ingestion-' + fileId },
      payload,
    };
    const first = await app.inject(request);
    const second = await app.inject(request);
    expect(first.statusCode, first.body).toBe(200);
    expect(second.json()).toEqual(first.json());
    const id = first.json().id;
    const original = await snapshot(id, 1);
    const changedSource = await upload(JSON.stringify({ files, version: 1 }, null, 2));
    expect(changedSource).not.toBe(fileId);
    const reassigned = await post('/v1/agents/' + id, { metadata: { dsh_profile: changedSource } });
    expect(reassigned.statusCode, reassigned.body).toBe(200);
    expect((await snapshot(id, 2)).dsh_profile).toMatchObject({
      source_file_id: changedSource,
      config_sha256: digest,
    });
    expect((await snapshot(id, 1)).dsh_profile).toEqual(original.dsh_profile);
    const changedConfig = await upload(
      JSON.stringify({ version: 1, files: { ...files, 'cordis.patch.yml': ' []' } }),
    );
    const denied = await post('/v1/agents/' + id, {
      metadata: { dsh_profile: changedConfig },
      name: 'Must not persist',
    });
    expect(denied.statusCode).toBe(400);
    expect(
      (await db.select().from(agentVersions).where(eq(agentVersions.agentId, id))).length,
    ).toBe(2);
    expect((await post('/v1/agents/' + id, { version: 1, name: 'Stale write' })).statusCode).toBe(
      409,
    );
    const current = await app.inject({
      method: 'GET',
      url: '/v1/agents/' + id,
      headers: headers(),
    });
    expect(current.json()).toMatchObject({ name: 'Idempotent profile', version: 2 });
  });

  it('denies foreign/invalid input, reserved metadata and public parser diagnostics atomically', async () => {
    const fileId = await upload();
    const foreign = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { ...headers(), 'x-api-key': foreignKey },
      payload: {
        name: 'Foreign',
        model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
        metadata: { ...selection, dsh_profile: fileId },
      },
    });
    expect(foreign.statusCode).toBe(400);
    const invalidHarness = await post('/v1/agents', {
      name: 'Invalid harness',
      model: 'claude-sonnet-4-6',
      metadata: { dsh_profile: fileId },
    });
    expect(invalidHarness.statusCode).toBe(400);
    const secret = 'private-profile-sentinel';
    const malformed = await upload('{ ' + secret);
    const invalidInput = await post('/v1/agents', {
      name: 'Invalid input',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
      metadata: { ...selection, dsh_profile: malformed },
    });
    expect(invalidInput.statusCode).toBe(400);
    expect(invalidInput.body).not.toContain(secret);
    expect(
      await db
        .select()
        .from(agents)
        .where(and(eq(agents.workspaceId, workspaceId), eq(agents.name, 'Invalid input'))),
    ).toEqual([]);
    const agent = await createAgent(fileId);
    const roster = await post('/v1/agents', {
      name: 'Unsupported DSH roster member',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
      multiagent: { type: 'coordinator', agents: [agent.id] },
    });
    expect(roster.statusCode, roster.body).toBe(400);
    expect(
      (await createSession(agent.id, 1, { metadata: { dsh_profile: fileId } })).statusCode,
    ).toBe(400);
    const session = await createSession(agent.id);
    expect(
      (await post('/v1/sessions/' + session.json().id, { metadata: { dsh_profile: fileId } }))
        .statusCode,
    ).toBe(400);
    const trigger = await post('/v1/triggers', {
      name: 'Reserved profile',
      agent: agent.id,
      session_mode: 'SESSION_PER_EVENT',
      source: { type: 'cron', schedule: '* * * * *', payload: 'test' },
      session: { environment_id: environmentId, metadata: { dsh_profile: fileId } },
    });
    expect(trigger.statusCode).toBe(400);
  });

  it('launches pinned Trigger profiles after source deletion, then rejects revocation/network drift', async () => {
    const fileId = await upload();
    const agent = await createAgent(fileId);
    const newer = await post('/v1/agents/' + agent.id, { system: 'Newer version' });
    expect(newer.statusCode, newer.body).toBe(200);
    expect(newer.json().version).toBe(2);
    await fileStore.delete(workspaceId, fileId);
    async function trigger(name: string) {
      const result = await post('/v1/triggers', {
        name,
        agent: { type: 'agent', id: agent.id, version: 1 },
        session_mode: 'SESSION_PER_EVENT',
        source: { type: 'cron', schedule: '* * * * *', payload: 'test' },
        session: { environment_id: environmentId },
      });
      expect(result.statusCode, result.body).toBe(200);
      expect(
        (
          await post('/v1/triggers/' + result.json().id, {
            session: { metadata: { dsh_profile: fileId } },
          })
        ).statusCode,
      ).toBe(400);
      return result.json().id as string;
    }
    async function fire(id: string, grants = approved) {
      const now = new Date();
      await db.update(agentTriggers).set({ nextFireAt: now }).where(eq(agentTriggers.id, id));
      await reconcileDueAgentTriggers(db, { now, batchSize: 1000 });
      await dispatchPendingAgentTriggerFires(db, {
        now,
        batchSize: 1000,
        dshApprovedProfiles: grants,
      });
      return (
        await db.select().from(agentTriggerFires).where(eq(agentTriggerFires.triggerId, id))
      )[0]!;
    }
    const success = await fire(await trigger('Approved retained profile'));
    expect(success.status).toBe('enqueued');
    expect((await prepare(success.sessionId!)).statusCode).toBe(200);
    const revokedFire = await fire(await trigger('Revoked pinned profile'), {});
    expect(revokedFire).toMatchObject({
      status: 'failed',
      sessionId: null,
      lastError: expect.stringMatching(/approved/),
    });
    const networkTrigger = await trigger('Network drift');
    await db
      .update(environments)
      .set({ networking: { type: 'limited' } })
      .where(eq(environments.id, environmentId));
    try {
      expect((await fire(networkTrigger)).status).toBe('failed');
    } finally {
      await db
        .update(environments)
        .set({ networking: { type: 'unrestricted' } })
        .where(eq(environments.id, environmentId));
    }
    expect(
      (await db.select().from(sessions).where(eq(sessions.id, success.sessionId!)))[0]!
        .agentVersion,
    ).toBe(1);
    const legacyTrigger = await trigger('Missing retained descriptor');
    const legacy = await snapshot(agent.id, 1);
    const retained = structuredClone(legacy);
    delete legacy.dsh_profile;
    await db
      .update(agentVersions)
      .set({ snapshot: legacy })
      .where(and(eq(agentVersions.agentId, agent.id), eq(agentVersions.version, 1)));
    const pending = await fire(legacyTrigger);
    expect(pending).toMatchObject({
      status: 'pending',
      attemptCount: 1,
      sessionId: null,
      lastError: expect.stringMatching(/unavailable/),
    });
    await db
      .update(agentVersions)
      .set({ snapshot: retained })
      .where(and(eq(agentVersions.agentId, agent.id), eq(agentVersions.version, 1)));
    await dispatchPendingAgentTriggerFires(db, {
      now: new Date(pending.nextAttemptAt.getTime() + 1),
      batchSize: 1000,
      dshApprovedProfiles: approved,
    });
    const [recovered] = await db
      .select()
      .from(agentTriggerFires)
      .where(eq(agentTriggerFires.id, pending.id));
    expect(recovered).toMatchObject({ status: 'enqueued', attemptCount: 2 });
    expect((await prepare(recovered!.sessionId!)).statusCode).toBe(200);
  });
});
