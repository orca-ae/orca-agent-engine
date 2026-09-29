// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { and, eq } from 'drizzle-orm';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { buildAdminApp } from '../../src/server.js';
import {
  generateAdminApiKey,
  hashAdminApiKey,
  fingerprintAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import {
  adminApiKeys,
  adminAuditEvents,
  agentObservabilityOrganizationSettings,
  agentObservabilityBindings,
  organizations,
} from '../../src/persistence/postgres/schema.js';
import {
  createFreshWorkspaceMutationTestDb,
  type FreshWorkspaceMutationTestDb,
} from './agent-observability-workspace-mutation-test-db.js';
import { buildStubStore } from './setup.js';

const readURL = '/v1/organizations/agent_observability';
const putURL = readURL + '/capture_ceiling';
describe('organization capture ceiling admin contract', () => {
  let fixture: FreshWorkspaceMutationTestDb;
  let app: ReturnType<typeof buildAdminApp>;
  let org: string;
  let key: string;
  beforeAll(async () => {
    fixture = await createFreshWorkspaceMutationTestDb('org_ceiling');
    app = buildAdminApp({
      db: fixture.db,
      store: buildStubStore(),
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
    });
    await app.ready();
  });
  beforeEach(async () => {
    org = 'org_' + randomUUID();
    key = generateAdminApiKey();
    await fixture.db.insert(organizations).values({ id: org, name: org });
    await fixture.db.insert(adminApiKeys).values({
      id: 'adminkey_' + randomUUID(),
      organizationId: org,
      name: 'ceiling test',
      hashedKey: await hashAdminApiKey(key),
      keyFingerprint: fingerprintAdminApiKey(key),
      partialKeyHint: partialAdminApiKeyHint(key),
      scopes: ['org:admin'],
      status: 'active',
      createdBy: 'test',
    });
  });
  afterAll(async () => {
    await app?.close();
    await fixture?.close();
  });
  const read = () => app.inject({ method: 'GET', url: readURL, headers: { 'x-api-key': key } });
  const put = (etag: string, capture = 'raw_io', idem = randomUUID(), url = putURL) =>
    app.inject({
      method: 'PUT',
      url,
      headers: { 'x-api-key': key, 'if-match': etag, 'idempotency-key': idem },
      payload: { capture_ceiling: capture },
    });
  it('changes a ceiling without inventing a default and replays the original request after later changes', async () => {
    const etag = (await read()).headers.etag as string;
    const idem = randomUUID();
    const first = await put(etag, 'raw_io', idem);
    expect(first.statusCode).toBe(200);
    expect(first.json().configured).toEqual({ capture_ceiling: 'raw_io', default_binding: null });
    expect(first.json().effective.capture_mode).toBe('metadata_only');
    expect((await put((await read()).headers.etag as string, 'metadata_only')).statusCode).toBe(
      200,
    );
    expect((await put((await read()).headers.etag as string, 'raw_io')).statusCode).toBe(200);
    const replay = await put(etag, 'raw_io', idem);
    expect(replay.statusCode).toBe(200);
    expect(replay.body).toBe(first.body);
    expect((await put((await read()).headers.etag as string, 'raw_io', idem)).statusCode).toBe(409);
    expect((await put(etag, 'metadata_only', idem)).statusCode).toBe(409);
    const [setting] = await fixture.db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, org));
    expect(setting).toMatchObject({
      captureRestrictionEpoch: 1,
      selectionEpoch: 0,
      defaultRevocationEpoch: 0,
      activeDefaultBindingId: null,
    });
    expect(
      await fixture.db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.organizationId, org)),
    ).toHaveLength(0);
    expect(
      await fixture.db
        .select()
        .from(adminAuditEvents)
        .where(
          and(
            eq(adminAuditEvents.organizationId, org),
            eq(adminAuditEvents.action, 'organization.agent_observability.capture_ceiling.updated'),
          ),
        ),
    ).toHaveLength(3);
  });
  it('requires org admin, strong If-Match, exact body, and idempotency including on the alias', async () => {
    const etag = (await read()).headers.etag as string;
    expect(
      (await app.inject({ method: 'PUT', url: putURL, payload: { capture_ceiling: 'raw_io' } }))
        .statusCode,
    ).toBe(401);
    expect(
      (
        await app.inject({
          method: 'PUT',
          url: putURL,
          headers: { 'x-api-key': key, 'idempotency-key': 'key' },
          payload: { capture_ceiling: 'raw_io' },
        })
      ).statusCode,
    ).toBe(428);
    for (const bad of ['*', 'W/' + etag, etag + ',' + etag])
      expect((await put(bad)).statusCode).toBe(400);
    expect((await put('"stale"')).statusCode).toBe(412);
    expect((await put(etag, 'raw_io', randomUUID(), '/api' + putURL)).statusCode).toBe(200);
    await fixture.db
      .update(adminApiKeys)
      .set({ scopes: ['observability:write'] })
      .where(eq(adminApiKeys.organizationId, org));
    expect((await put(etag)).statusCode).toBe(403);
  });
  it('does not touch another organization or accept a caller-supplied organization or target', async () => {
    const etag = (await read()).headers.etag as string;
    for (const extra of [{ organization_id: 'foreign' }, { target: {} }, { credentials: {} }]) {
      const response = await app.inject({
        method: 'PUT',
        url: putURL,
        headers: { 'x-api-key': key, 'if-match': etag, 'idempotency-key': randomUUID() },
        payload: { capture_ceiling: 'raw_io', ...extra },
      });
      expect(response.statusCode).toBe(400);
    }
    expect((await read()).json().configured.capture_ceiling).toBe('metadata_only');
  });
  it('serializes competing ceilings on the same organization authority', async () => {
    const etag = (await read()).headers.etag as string;
    const results = await Promise.all([put(etag, 'raw_io'), put(etag, 'redacted_io')]);
    expect(results.map((r) => r.statusCode).sort()).toEqual([200, 412]);
  });
});
