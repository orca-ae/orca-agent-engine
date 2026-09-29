// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { eq } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  fingerprintPlatformApiKey,
  generatePlatformApiKey,
  hashPlatformApiKey,
  partialPlatformApiKeyHint,
} from '../../src/auth/platform-api-key.js';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import {
  adminApiKeys,
  agentObservabilityBindings,
  agentObservabilityBindingVersions,
  agentObservabilityBindingCredentials,
  agentObservabilityOrganizationSettings,
  agentObservabilityPlatformPolicy,
  agentObservabilityWorkspaceSettings,
  organizations,
  platformApiKeys,
  platformAuditEvents,
  platformIdempotencyKeys,
  sessionObservabilityBindings,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import { loadAgentObservabilitySessionContext } from '../../src/domain/agent-observability-context-resolver.js';
import { selectSessionObservabilityBindingInTransaction } from '../../src/domain/agent-observability-session-selection.js';
import { newSessionObservabilityBindingRow } from '../../src/domain/session-creation.js';
import { buildAdminApp } from '../../src/server.js';
import { buildStubStore } from './setup.js';
import {
  createFreshWorkspaceMutationTestDb,
  type FreshWorkspaceMutationTestDb,
} from './agent-observability-workspace-mutation-test-db.js';

const URL = '/v1/platform/agent_observability';
const rawPolicy = {
  allowed_adapters: ['otlp_http'],
  allowed_endpoint_classes: ['public'],
  max_capture_mode: 'raw_io',
};

describe('platform agent observability policy', () => {
  let fixture: FreshWorkspaceMutationTestDb;
  let app: FastifyInstance;
  let platformKey: string;
  let adminKey: string;
  let original: typeof agentObservabilityPlatformPolicy.$inferSelect;

  beforeAll(async () => {
    fixture = await createFreshWorkspaceMutationTestDb('platform_policy');
    const { db } = fixture;
    original = (await db.select().from(agentObservabilityPlatformPolicy))[0]!;
    platformKey = generatePlatformApiKey();
    adminKey = generateAdminApiKey();
    await db.insert(platformApiKeys).values({
      id: 'platformkey_policy',
      name: 'Policy test',
      hashedKey: await hashPlatformApiKey(platformKey),
      keyFingerprint: fingerprintPlatformApiKey(platformKey),
      partialKeyHint: partialPlatformApiKeyHint(platformKey),
      scopes: ['platform:admin'],
      status: 'active',
      createdBy: 'test',
    });
    await db.insert(organizations).values({ id: 'org_policy', name: 'Policy test' });
    await db.insert(adminApiKeys).values({
      id: 'adminkey_policy',
      organizationId: 'org_policy',
      name: 'Org admin test',
      hashedKey: await hashAdminApiKey(adminKey),
      keyFingerprint: fingerprintAdminApiKey(adminKey),
      partialKeyHint: partialAdminApiKeyHint(adminKey),
      scopes: ['org:admin'],
      status: 'active',
      createdBy: 'test',
    });
    app = buildAdminApp({
      db,
      store: buildStubStore(),
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
    });
    await app.ready();
  });

  beforeEach(async () => {
    await fixture.db
      .insert(agentObservabilityPlatformPolicy)
      .values(original)
      .onConflictDoUpdate({ target: agentObservabilityPlatformPolicy.id, set: original });
    await fixture.db.delete(platformAuditEvents);
    await fixture.db.delete(platformIdempotencyKeys);
  });

  afterAll(async () => {
    await app?.close();
    await fixture?.close();
  });

  const read = (url = URL) =>
    app.inject({ method: 'GET', url, headers: { 'x-api-key': platformKey } });
  const put = (etag: string, payload = rawPolicy, key = randomUUID(), url = URL) =>
    app.inject({
      method: 'PUT',
      url,
      payload,
      headers: { 'x-api-key': platformKey, 'if-match': etag, 'idempotency-key': key },
    });

  it('reads the seeded singleton with a strong ETag, including the API alias', async () => {
    const response = await read();
    expect(response.statusCode, response.body).toBe(200);
    expect(response.json()).toMatchObject({
      type: 'agent_observability_platform_policy',
      allowed_adapters: ['otlp_http'],
      allowed_endpoint_classes: ['public'],
      max_capture_mode: 'metadata_only',
      capture_restriction_epoch: 0,
    });
    expect(response.headers.etag).toMatch(/^"[^" ]+"$/);
    expect(response.headers['cache-control']).toBe('private, no-store');
    const alias = await read('/api' + URL);
    expect(alias.body).toBe(response.body);
    expect(alias.headers.etag).toBe(response.headers.etag);
  });

  it('rejects organization credentials and keeps all errors uncacheable', async () => {
    for (const key of [undefined, adminKey, 'orca_platform_invalid']) {
      for (const method of ['GET', 'PUT'] as const) {
        const response = await app.inject({
          method,
          url: '/api' + URL,
          headers: key ? { 'x-api-key': key } : {},
          ...(method === 'PUT' ? { payload: rawPolicy } : {}),
        });
        expect(response.statusCode).toBe(401);
        expect(response.headers['cache-control']).toContain('no-store');
      }
    }
    const malformed = await app.inject({
      method: 'PUT',
      url: URL,
      payload: '{',
      headers: { 'x-api-key': platformKey, 'content-type': 'application/json' },
    });
    expect(malformed.statusCode).toBe(400);
    expect(malformed.headers['cache-control']).toContain('no-store');
  });

  it('requires strict preconditions and rejects unsupported or server-owned fields', async () => {
    const etag = (await read()).headers.etag!;
    for (const [extra, expected] of [
      [{ 'idempotency-key': 'missing-etag' }, 428],
      [{ 'if-match': etag }, 400],
      [{ 'if-match': '*', 'idempotency-key': 'wildcard' }, 400],
      [{ 'if-match': 'W/' + etag, 'idempotency-key': 'weak' }, 400],
      [{ 'if-match': etag + ', "other"', 'idempotency-key': 'list' }, 400],
      [{ 'if-match': '"stale"', 'idempotency-key': 'stale' }, 412],
    ] as const) {
      const response = await app.inject({
        method: 'PUT',
        url: URL,
        payload: rawPolicy,
        headers: { 'x-api-key': platformKey, ...extra },
      });
      expect(response.statusCode, response.body).toBe(expected);
      expect(response.headers['cache-control']).toContain('no-store');
    }
    for (const payload of [
      {},
      [],
      null,
      { ...rawPolicy, capture_restriction_epoch: 0 },
      { ...rawPolicy, organization_id: 'org_policy' },
      { ...rawPolicy, allowed_adapters: [] },
      { ...rawPolicy, allowed_adapters: ['otlp_http', 'otlp_http'] },
      { ...rawPolicy, allowed_endpoint_classes: ['invalid'] },
      { ...rawPolicy, max_capture_mode: 'raw' },
    ]) {
      const response = await app.inject({
        method: 'PUT',
        url: URL,
        payload: JSON.stringify(payload),
        headers: {
          'x-api-key': platformKey,
          'content-type': 'application/json',
          'if-match': etag,
          'idempotency-key': randomUUID(),
        },
      });
      expect(response.statusCode, response.body).toBe(400);
    }
    expect(await fixture.db.select().from(platformAuditEvents)).toHaveLength(0);
  });

  it('validates the policy body before reporting a missing If-Match', async () => {
    for (const url of [URL, '/api' + URL]) {
      const response = await app.inject({
        method: 'PUT',
        url,
        payload: { ...rawPolicy, max_capture_mode: 'invalid' },
        headers: { 'x-api-key': platformKey, 'idempotency-key': randomUUID() },
      });
      expect(response.statusCode).toBe(400);
      expect(response.json()).toEqual({
        error: 'invalid agent observability platform policy request',
      });
      expect(response.headers['cache-control']).toContain('no-store');
    }
    expect(await fixture.db.select().from(platformAuditEvents)).toHaveLength(0);
    expect(await fixture.db.select().from(platformIdempotencyKeys)).toHaveLength(0);
  });

  it('atomically raises policy, audits it and replays the original result after later changes', async () => {
    const old = await read();
    const key = randomUUID();
    const [first, duplicate] = await Promise.all([
      put(old.headers.etag!, rawPolicy, key),
      put(old.headers.etag!, rawPolicy, key, '/api' + URL),
    ]);
    expect(first.statusCode, first.body).toBe(200);
    expect(first.json()).toMatchObject({
      max_capture_mode: 'raw_io',
      capture_restriction_epoch: 0,
    });
    expect(first.headers.etag).not.toBe(old.headers.etag);
    expect(duplicate.body).toBe(first.body);
    expect(duplicate.headers.etag).toBe(first.headers.etag);
    expect((await read()).headers.etag).toBe(first.headers.etag);
    const lower = await put(first.headers.etag!, {
      ...rawPolicy,
      max_capture_mode: 'metadata_only',
    });
    expect(lower.statusCode).toBe(200);
    const replay = await put(old.headers.etag!, rawPolicy, key);
    expect(replay.body).toBe(first.body);
    expect(replay.headers.etag).toBe(first.headers.etag);
    expect((await read()).json().max_capture_mode).toBe('metadata_only');
    for (const conflict of [
      await put(first.headers.etag!, rawPolicy, key),
      await put(old.headers.etag!, { ...rawPolicy, max_capture_mode: 'redacted_io' }, key),
    ]) {
      expect(conflict.statusCode).toBe(409);
      expect(conflict.json()).toEqual({ error: 'idempotency-key reused with different request' });
    }
    const audit = await fixture.db.select().from(platformAuditEvents);
    expect(audit).toHaveLength(2);
    expect(audit[0]).toMatchObject({
      action: 'agent_observability.platform_policy.updated',
      organizationId: null,
      workspaceId: null,
      targetId: 'default',
    });
    expect(audit[0]?.metadata).toMatchObject({
      before: { max_capture_mode: 'metadata_only' },
      after: { max_capture_mode: 'raw_io' },
    });
    expect(JSON.stringify(audit)).not.toContain(platformKey);
  });

  it('increments the sticky epoch only for each actual capture reduction', async () => {
    let response = await read();
    for (const [mode, epoch] of [
      ['raw_io', 0],
      ['redacted_io', 1],
      ['raw_io', 1],
      ['metadata_only', 2],
      ['raw_io', 2],
      ['metadata_only', 3],
      ['metadata_only', 3],
    ] as const) {
      response = await put(response.headers.etag!, { ...rawPolicy, max_capture_mode: mode });
      expect(response.statusCode, response.body).toBe(200);
      expect(response.json().capture_restriction_epoch).toBe(epoch);
    }
    const noop = await put(response.headers.etag!, {
      ...rawPolicy,
      max_capture_mode: 'metadata_only',
    });
    expect(noop.body).toBe(response.body);
    expect(noop.headers.etag).toBe(response.headers.etag);
  });

  it('allows only one competing replacement of an ETag', async () => {
    const etag = (await read()).headers.etag!;
    const responses = await Promise.all([
      put(etag),
      put(etag, { ...rawPolicy, max_capture_mode: 'redacted_io' }),
    ]);
    expect(responses.map((r) => r.statusCode).sort()).toEqual([200, 412]);
    expect(await fixture.db.select().from(platformAuditEvents)).toHaveLength(1);
    expect(await fixture.db.select().from(platformIdempotencyKeys)).toHaveLength(1);
  });

  it('canonicalizes allowlists and detects allowlist ABA replacements', async () => {
    const all = {
      ...rawPolicy,
      allowed_adapters: ['otlp_http', 'langfuse_sdk'],
      allowed_endpoint_classes: ['public', 'private'],
    };
    const first = await put((await read()).headers.etag!, all);
    expect(first.statusCode, first.body).toBe(200);
    expect(first.json().allowed_adapters).toEqual(['langfuse_sdk', 'otlp_http']);
    const noop = await put(first.headers.etag!, {
      ...all,
      allowed_adapters: [...all.allowed_adapters].reverse(),
      allowed_endpoint_classes: [...all.allowed_endpoint_classes].reverse(),
    });
    expect(noop.body).toBe(first.body);
    const narrow = await put(noop.headers.etag!, rawPolicy);
    const restored = await put(narrow.headers.etag!, all);
    expect(restored.headers.etag).not.toBe(first.headers.etag);
    expect(restored.json().capture_restriction_epoch).toBe(0);
    expect((await put(first.headers.etag!)).statusCode).toBe(412);
  });

  it('waits for the same shared authority lock used by Session pinning and context resolution', async () => {
    const etag = (await read()).headers.etag!;
    const holder = await fixture.pool.connect();
    let pending: ReturnType<typeof put> | undefined;
    try {
      await holder.query('BEGIN');
      const pid = (await holder.query('SELECT pg_backend_pid() AS pid')).rows[0].pid as number;
      await holder.query(
        "SELECT id FROM agent_observability_platform_policy WHERE id = 'default' FOR SHARE",
      );
      pending = put(etag);
      // Start injection explicitly; observe the server-side lock wait rather than relying on sleeps.
      void pending.then(() => {});
      await vi.waitFor(
        async () => {
          const waiters = await fixture.pool.query(
            'SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))',
            [pid],
          );
          expect(waiters.rowCount).toBeGreaterThan(0);
        },
        { timeout: 5000 },
      );
      expect((await read()).json().max_capture_mode).toBe('metadata_only');
    } finally {
      await holder.query('ROLLBACK');
      holder.release();
    }
    expect((await pending!).statusCode).toBe(200);
  });

  it.each(['platform_audit_events', 'platform_idempotency_keys'])(
    'rolls back policy and epoch if %s insertion fails, then permits a clean retry',
    async (table) => {
      const before = await put((await read()).headers.etag!);
      expect(before.statusCode).toBe(200);
      const restriction = { ...rawPolicy, max_capture_mode: 'metadata_only' };
      await fixture.db.delete(platformAuditEvents);
      await fixture.db.delete(platformIdempotencyKeys);
      const key = randomUUID();
      // Both identifiers come only from the fixed test cases above, never request data.
      await fixture.pool.query(
        "CREATE FUNCTION reject_policy_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private-database-canary'; END $$",
      );
      try {
        await fixture.pool.query(
          'CREATE TRIGGER reject_policy_write BEFORE INSERT ON ' +
            table +
            ' FOR EACH ROW EXECUTE FUNCTION reject_policy_write()',
        );
        const failed = await put(before.headers.etag!, restriction, key);
        expect(failed.statusCode).toBe(503);
        expect(failed.body).not.toContain('private-database-canary');
        expect((await read()).body).toBe(before.body);
        expect(await fixture.db.select().from(platformAuditEvents)).toHaveLength(0);
        expect(await fixture.db.select().from(platformIdempotencyKeys)).toHaveLength(0);
      } finally {
        await fixture.pool.query('DROP TRIGGER IF EXISTS reject_policy_write ON ' + table);
        await fixture.pool.query('DROP FUNCTION reject_policy_write()');
      }
      const retry = await put(before.headers.etag!, restriction, key);
      expect(retry.statusCode).toBe(200);
      expect(retry.json()).toMatchObject({
        max_capture_mode: 'metadata_only',
        capture_restriction_epoch: 1,
      });
    },
  );

  it('keeps old pins restricted after re-expansion while new pins capture raw I/O', async () => {
    const db = fixture.db;
    const workspaceId = 'ws_platform_policy_pins';
    const bindingId = 'aob_platform_policy_pins';
    await db.insert(workspaces).values({
      id: workspaceId,
      organizationId: 'org_policy',
      name: 'Pinned workspace',
      createdBy: 'test',
    });
    await db.transaction(async (tx) => {
      await tx.insert(agentObservabilityBindings).values({
        id: bindingId,
        organizationId: 'org_policy',
        workspaceId,
        scopeType: 'workspace',
        adapterType: 'otlp_http',
        endpointKind: 'traces_endpoint',
        endpointClass: 'public',
        endpoint: 'https://collector.example/v1/traces',
        externalProjectId: 'project-policy-test',
        createdBy: 'test',
        updatedBy: 'test',
      });
      await tx.insert(agentObservabilityBindingVersions).values({
        bindingId,
        version: 1,
        adapterType: 'otlp_http',
        semanticProfile: 'langfuse',
        protocol: 'http/json',
        captureMode: 'raw_io',
        createdBy: 'test',
      });
      await tx
        .insert(agentObservabilityBindingCredentials)
        .values({ bindingId, secretRef: 'opaque-test-reference', updatedBy: 'test' });
      await tx
        .update(agentObservabilityOrganizationSettings)
        .set({ captureCeiling: 'raw_io' })
        .where(eq(agentObservabilityOrganizationSettings.organizationId, 'org_policy'));
      await tx
        .update(agentObservabilityWorkspaceSettings)
        .set({ mode: 'custom', bindingId, captureCeiling: 'raw_io' })
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
    });
    const pin = (sessionId: string) =>
      db.transaction(async (tx) => {
        const selection = await selectSessionObservabilityBindingInTransaction(tx, workspaceId);
        await tx.insert(sessionObservabilityBindings).values(
          newSessionObservabilityBindingRow(
            {
              id: sessionId,
              workspaceId,
              agentId: 'agt_policy',
              agentVersion: 1,
              now: new Date(),
            },
            selection,
            { harness: 'claude_agent_sdk', mode: 'separate' },
          ),
        );
      });
    const context = (sessionId: string) =>
      loadAgentObservabilitySessionContext({ db, workspaceId, sessionId });
    await pin('ses_before_raw');
    let response = await put((await read()).headers.etag!);
    expect(response.statusCode).toBe(200);
    expect((await context('ses_before_raw')).capture.effective_mode).toBe('metadata_only');
    await pin('ses_raw_old');
    expect((await context('ses_raw_old')).capture.effective_mode).toBe('raw_io');
    response = await put(response.headers.etag!, { ...rawPolicy, max_capture_mode: 'redacted_io' });
    expect(response.statusCode).toBe(200);
    expect((await context('ses_raw_old')).capture.effective_mode).toBe('metadata_only');
    response = await put(response.headers.etag!);
    expect(response.statusCode).toBe(200);
    expect((await context('ses_raw_old')).capture.effective_mode).toBe('metadata_only');
    await pin('ses_raw_new');
    expect((await context('ses_raw_new')).capture.effective_mode).toBe('raw_io');
    expect((await context('ses_raw_new')).epochs.pinned.platform_capture_restriction_epoch).toBe(1);
    const pinsBefore = await db.select().from(sessionObservabilityBindings);
    for (const capture_ceiling of ['metadata_only', 'raw_io']) {
      const orgState = await app.inject({
        method: 'GET',
        url: '/v1/organizations/agent_observability',
        headers: { 'x-api-key': adminKey },
      });
      const changed = await app.inject({
        method: 'PUT',
        url: '/v1/organizations/agent_observability/capture_ceiling',
        headers: {
          'x-api-key': adminKey,
          'if-match': orgState.headers.etag!,
          'idempotency-key': randomUUID(),
        },
        payload: { capture_ceiling },
      });
      expect(changed.statusCode, changed.body).toBe(200);
      expect((await context('ses_raw_new')).capture.effective_mode).toBe('metadata_only');
    }
    expect(await db.select().from(sessionObservabilityBindings)).toEqual(pinsBefore);
    await pin('ses_after_org_reexpansion');
    expect((await context('ses_after_org_reexpansion')).capture.effective_mode).toBe('raw_io');
    expect(
      (await context('ses_after_org_reexpansion')).epochs.pinned
        .organization_capture_restriction_epoch,
    ).toBe(1);
    // Allowlists are live platform gates too; widening them does not bypass exporter capabilities.
    response = await put(response.headers.etag!, {
      ...rawPolicy,
      allowed_adapters: ['langfuse_sdk'],
    });
    expect(response.statusCode).toBe(200);
    expect((await context('ses_raw_new')).reason).toBe('platform_adapter_disallowed');
    response = await put(response.headers.etag!, {
      ...rawPolicy,
      allowed_endpoint_classes: ['private'],
    });
    expect(response.statusCode).toBe(200);
    expect((await context('ses_raw_new')).reason).toBe('platform_endpoint_class_disallowed');
  });

  it('fails closed on missing or unsafe policy state without silently recreating it', async () => {
    const etag = (await read()).headers.etag!;
    await fixture.db
      .update(agentObservabilityPlatformPolicy)
      .set({ captureRestrictionEpoch: Number.MAX_SAFE_INTEGER });
    const atLimit = await put((await read()).headers.etag!);
    expect(atLimit.statusCode).toBe(200);
    const overflow = await put(atLimit.headers.etag!, {
      ...rawPolicy,
      max_capture_mode: 'metadata_only',
    });
    expect(overflow.statusCode).toBe(503);
    await fixture.pool.query(
      'UPDATE agent_observability_platform_policy SET capture_restriction_epoch = 9007199254740992',
    );
    expect((await read()).statusCode).toBe(503);
    await fixture.db.delete(agentObservabilityPlatformPolicy);
    expect((await read()).statusCode).toBe(503);
    const missing = await put(etag);
    expect(missing.statusCode).toBe(503);
    expect(missing.json()).toEqual({ error: 'agent observability platform policy unavailable' });
    expect(await fixture.db.select().from(agentObservabilityPlatformPolicy)).toHaveLength(0);
  });
});
