// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import type { FastifyInstance } from 'fastify';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import {
  AgentObservabilityStateAvailabilityError,
  loadWorkspaceAgentObservabilityState,
} from '../../src/domain/agent-observability-state.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import * as schema from '../../src/persistence/postgres/schema.js';
import { adminApiKeys, organizations, workspaces } from '../../src/persistence/postgres/schema.js';
import { buildAdminApp } from '../../src/server.js';
import { buildStubStore } from './setup.js';

const REGISTRY_DATABASE_URL =
  process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
const ADMIN_DATABASE_URL = (() => {
  const url = new URL(REGISTRY_DATABASE_URL);
  url.pathname = '/postgres';
  return url.toString();
})();
const MIGRATIONS_FOLDER = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../src/persistence/postgres/migrations',
);

describe('agent observability admin state reads', () => {
  let db: DbClient;
  let pool: Pool;
  let adminPool: Pool | undefined;
  let app: FastifyInstance | undefined;
  let adminUrl: string;
  let zeroKey: string;
  let readKey: string;
  let limitedKey: string;
  let orgAdminKey: string;
  const suffix = `${process.pid}_${randomBytes(5).toString('hex')}`;
  const zeroOrganizationId = `org_observability_zero_${suffix}`;
  const zeroWorkspaceId = `ws_observability_zero_${suffix}`;
  const organizationId = `org_observability_state_${suffix}`;
  const crossOrganizationId = `org_observability_cross_${suffix}`;
  const inheritedWorkspaceId = `ws_observability_inherit_${suffix}`;
  const customWorkspaceId = `ws_observability_custom_${suffix}`;
  const disabledWorkspaceId = `ws_observability_disabled_${suffix}`;
  const policyWorkspaceId = `ws_observability_policy_${suffix}`;
  const bindingDisabledWorkspaceId = `ws_observability_binding_disabled_${suffix}`;
  const credentialWorkspaceId = `ws_observability_credential_${suffix}`;
  const archivedWorkspaceId = `ws_observability_archived_${suffix}`;
  const missingSettingWorkspaceId = `ws_observability_missing_setting_${suffix}`;
  const malformedWorkspaceId = `ws_observability_malformed_${suffix}`;
  const crossWorkspaceId = `ws_observability_cross_workspace_${suffix}`;
  const defaultBindingId = `aob_observability_default_${suffix}`;
  const customBindingId = `aob_observability_custom_${suffix}`;
  const policyBindingId = `aob_observability_policy_${suffix}`;
  const bindingDisabledId = `aob_observability_disabled_${suffix}`;
  const missingCredentialBindingId = `aob_observability_missing_credential_${suffix}`;
  const malformedBindingId = `aob_observability_malformed_${suffix}`;
  const secretRefSentinel = `secret_ref_sentinel_${suffix}`;
  const databaseName = `registry_agent_observability_state_${process.pid}_${randomBytes(4).toString('hex')}`;

  beforeAll(async () => {
    adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    await adminPool.query(`CREATE DATABASE "${databaseName}"`);
    pool = new Pool({ connectionString: databaseUrl.toString(), max: 5 });
    db = drizzle(pool, { schema });
    await migrate(db, { migrationsFolder: MIGRATIONS_FOLDER });
    await createOrganization(db, zeroOrganizationId, 'Zero-state organization');
    await createWorkspace(db, zeroWorkspaceId, zeroOrganizationId, 'Zero-state workspace');
    zeroKey = await createAdminKey(db, zeroOrganizationId, ['observability:read']);

    await createOrganization(db, organizationId, 'State organization');
    await createOrganization(db, crossOrganizationId, 'Cross organization');
    for (const workspaceId of [
      inheritedWorkspaceId,
      customWorkspaceId,
      disabledWorkspaceId,
      policyWorkspaceId,
      bindingDisabledWorkspaceId,
      credentialWorkspaceId,
      archivedWorkspaceId,
      missingSettingWorkspaceId,
      malformedWorkspaceId,
    ]) {
      await createWorkspace(db, workspaceId, organizationId, workspaceId);
    }
    await createWorkspace(db, crossWorkspaceId, crossOrganizationId, 'Cross-org workspace');

    readKey = await createAdminKey(db, organizationId, ['observability:read']);
    limitedKey = await createAdminKey(db, organizationId, ['workspaces:read']);
    orgAdminKey = await createAdminKey(db, organizationId, ['org:admin']);

    await insertBinding(pool, {
      id: defaultBindingId,
      organizationId,
      workspaceId: null,
      secretRef: secretRefSentinel,
      credentialVersion: 4,
      keyHint: '...1234',
    });
    await pool.query(
      `
        UPDATE agent_observability_organization_settings
        SET active_default_binding_id = $1, active_default_binding_scope = 'organization'
        WHERE organization_id = $2
      `,
      [defaultBindingId, organizationId],
    );

    await insertBinding(pool, {
      id: customBindingId,
      organizationId,
      workspaceId: customWorkspaceId,
    });
    await setWorkspaceMode(pool, customWorkspaceId, organizationId, 'custom', customBindingId);

    await setWorkspaceMode(pool, disabledWorkspaceId, organizationId, 'disabled', null);

    await insertBinding(pool, {
      id: policyBindingId,
      organizationId,
      workspaceId: policyWorkspaceId,
    });
    await setWorkspaceMode(pool, policyWorkspaceId, organizationId, 'custom', policyBindingId);

    await insertBinding(pool, {
      id: bindingDisabledId,
      organizationId,
      workspaceId: bindingDisabledWorkspaceId,
      status: 'disabled',
    });
    await setWorkspaceMode(
      pool,
      bindingDisabledWorkspaceId,
      organizationId,
      'custom',
      bindingDisabledId,
    );

    await insertBinding(pool, {
      id: missingCredentialBindingId,
      organizationId,
      workspaceId: credentialWorkspaceId,
      credential: false,
    });
    await setWorkspaceMode(
      pool,
      credentialWorkspaceId,
      organizationId,
      'custom',
      missingCredentialBindingId,
    );

    await insertBinding(pool, {
      id: malformedBindingId,
      organizationId,
      workspaceId: malformedWorkspaceId,
      secretRef: secretRefSentinel,
    });
    await setWorkspaceMode(
      pool,
      malformedWorkspaceId,
      organizationId,
      'custom',
      malformedBindingId,
    );

    await pool.query(
      `UPDATE workspaces SET status = 'archived', archived_at = now() WHERE id = $1 AND organization_id = $2`,
      [archivedWorkspaceId, organizationId],
    );
    await pool.query(
      `DELETE FROM agent_observability_workspace_settings WHERE workspace_id = $1 AND organization_id = $2`,
      [missingSettingWorkspaceId, organizationId],
    );

    app = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    await app.ready();
    adminUrl = await app.listen({ port: 0, host: '127.0.0.1' });
  });

  afterAll(async () => {
    await app?.close();
    await pool?.end().catch(() => {});
    await adminPool
      ?.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1', [
        databaseName,
      ])
      .catch(() => {});
    await adminPool?.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
    await adminPool?.end().catch(() => {});
  });

  it('returns default disabled organization and workspace state with opaque ETags', async () => {
    const [organization, workspace] = await Promise.all([
      get('/v1/organizations/agent_observability', zeroKey),
      get(`/v1/organizations/workspaces/${zeroWorkspaceId}/agent_observability`, zeroKey),
    ]);

    expect(organization.statusCode).toBe(200);
    expect(organization.headers['cache-control']).toBe('private, no-store');
    expect(organization.headers.etag).toMatch(/^"orca-aos-v1-[A-Za-z0-9_-]{43}"$/);
    expect(organization.json()).toMatchObject({
      type: 'agent_observability',
      scope: 'organization',
      organization_id: zeroOrganizationId,
      workspace_id: null,
      configured: { capture_ceiling: 'metadata_only', default_binding: null },
      effective: { source: 'none', status: 'disabled', disabled_reason: 'no_default_binding' },
    });
    expect(workspace.statusCode).toBe(200);
    expect(workspace.headers['cache-control']).toBe('private, no-store');
    expect(workspace.headers.etag).toMatch(/^"orca-aos-v1-[A-Za-z0-9_-]{43}"$/);
    expect(workspace.json()).toMatchObject({
      type: 'agent_observability',
      scope: 'workspace',
      organization_id: zeroOrganizationId,
      workspace_id: zeroWorkspaceId,
      configured: { mode: 'inherit', capture_ceiling: 'metadata_only', binding: null },
      effective: { source: 'none', status: 'disabled', disabled_reason: 'no_organization_default' },
    });
  });

  it('inherits organization default, omits secret reference, and refreshes ETag for credential state', async () => {
    const first = await get(
      `/v1/organizations/workspaces/${inheritedWorkspaceId}/agent_observability`,
      readKey,
    );
    const organization = await get('/v1/organizations/agent_observability', readKey);
    expect(first.statusCode).toBe(200);
    expect(organization.statusCode).toBe(200);
    expect(first.json()).toMatchObject({
      configured: { mode: 'inherit', binding: null },
      effective: {
        source: 'organization_default',
        status: 'enabled',
        capture_mode: 'metadata_only',
        binding: {
          id: defaultBindingId,
          credential: {
            configured: true,
            version: 4,
            key_hint: '...1234',
          },
        },
      },
    });
    expect(first.body).not.toContain(secretRefSentinel);
    expect(organization.body).not.toContain(secretRefSentinel);
    expect(Object.values(first.headers).join('\n')).not.toContain(secretRefSentinel);

    await pool.query(
      `
        UPDATE agent_observability_binding_credentials
        SET credential_version = 5, key_hint = '...5678', rotated_at = now()
        WHERE binding_id = $1
      `,
      [defaultBindingId],
    );
    const second = await get(
      `/v1/organizations/workspaces/${inheritedWorkspaceId}/agent_observability`,
      readKey,
    );
    expect(second.statusCode).toBe(200);
    expect(second.headers.etag).not.toBe(first.headers.etag);
    expect(second.json().effective.binding.credential).toMatchObject({
      configured: true,
      version: 5,
      key_hint: '...5678',
    });
  });

  it('reports a workspace custom binding and explicit workspace disable', async () => {
    const [custom, disabled] = await Promise.all([
      get(`/v1/organizations/workspaces/${customWorkspaceId}/agent_observability`, readKey),
      get(`/v1/organizations/workspaces/${disabledWorkspaceId}/agent_observability`, readKey),
    ]);

    expect(custom.statusCode).toBe(200);
    expect(custom.json()).toMatchObject({
      configured: { mode: 'custom', binding: { id: customBindingId, scope: 'workspace' } },
      effective: {
        source: 'workspace_custom',
        status: 'enabled',
        binding: { id: customBindingId },
      },
    });
    expect(disabled.statusCode).toBe(200);
    expect(disabled.json()).toMatchObject({
      configured: { mode: 'disabled', binding: null },
      effective: {
        source: 'none',
        status: 'disabled',
        disabled_reason: 'workspace_disabled',
        binding: null,
      },
    });
  });

  it('returns explicit platform-policy, binding, and credential disabled reasons', async () => {
    await pool.query(
      `UPDATE agent_observability_platform_policy SET allowed_adapters = ARRAY['langfuse_sdk']::text[] WHERE id = 'default'`,
    );
    try {
      const policy = await get(
        `/v1/organizations/workspaces/${policyWorkspaceId}/agent_observability`,
        readKey,
      );
      expect(policy.statusCode).toBe(200);
      expect(policy.json().effective).toMatchObject({
        source: 'workspace_custom',
        status: 'disabled',
        disabled_reason: 'platform_adapter_disallowed',
        binding: { id: policyBindingId },
      });
    } finally {
      await pool.query(
        `UPDATE agent_observability_platform_policy SET allowed_adapters = ARRAY['otlp_http']::text[] WHERE id = 'default'`,
      );
    }

    const [bindingDisabled, credentialMissing] = await Promise.all([
      get(
        `/v1/organizations/workspaces/${bindingDisabledWorkspaceId}/agent_observability`,
        readKey,
      ),
      get(`/v1/organizations/workspaces/${credentialWorkspaceId}/agent_observability`, readKey),
    ]);
    expect(bindingDisabled.json().effective).toMatchObject({
      status: 'disabled',
      disabled_reason: 'binding_disabled',
      binding: { id: bindingDisabledId },
    });
    expect(credentialMissing.json().effective).toMatchObject({
      status: 'disabled',
      disabled_reason: 'credential_not_configured',
      binding: { id: missingCredentialBindingId },
    });
  });

  it('requires observability:read or org:admin', async () => {
    const [denied, delegated, administrator] = await Promise.all([
      get('/v1/organizations/agent_observability', limitedKey),
      get('/v1/organizations/agent_observability', readKey),
      get('/v1/organizations/agent_observability', orgAdminKey),
    ]);
    expect(denied.statusCode).toBe(403);
    expect(denied.json()).toEqual({ error: 'missing required scope: observability:read' });
    expect(delegated.statusCode).toBe(200);
    expect(administrator.statusCode).toBe(200);
  });

  it('makes archived, missing, and cross-organization workspace state indistinguishable', async () => {
    const [archived, missing, crossOrganization] = await Promise.all([
      get(`/v1/organizations/workspaces/${archivedWorkspaceId}/agent_observability`, readKey),
      get('/v1/organizations/workspaces/ws_observability_missing/agent_observability', readKey),
      get(`/v1/organizations/workspaces/${crossWorkspaceId}/agent_observability`, readKey),
    ]);

    for (const response of [archived, missing, crossOrganization]) {
      expect(response.statusCode).toBe(404);
      expect(response.json()).toEqual({ error: 'workspace not found' });
    }
  });

  it('fails closed with sanitized 503 when a setting row is missing', async () => {
    const response = await get(
      `/v1/organizations/workspaces/${missingSettingWorkspaceId}/agent_observability`,
      readKey,
    );

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({ error: 'agent observability state unavailable' });
  });

  it('does not expose secret reference through malformed state response or error object', async () => {
    await pool.query(
      `UPDATE agent_observability_bindings SET endpoint = 'not a URL' WHERE id = $1`,
      [malformedBindingId],
    );

    const response = await get(
      `/v1/organizations/workspaces/${malformedWorkspaceId}/agent_observability`,
      readKey,
    );
    expect(response.statusCode).toBe(503);
    expect(response.body).not.toContain(secretRefSentinel);
    expect(Object.values(response.headers).join('\n')).not.toContain(secretRefSentinel);

    let error: unknown;
    try {
      await loadWorkspaceAgentObservabilityState({
        db,
        organizationId,
        workspaceId: malformedWorkspaceId,
      });
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(AgentObservabilityStateAvailabilityError);
    expect(
      `${(error as Error).name}:${(error as Error).message}:${JSON.stringify(error)}`,
    ).not.toContain(secretRefSentinel);
  });

  it('fails closed for noncanonical endpoints and bounded public metadata in stored rows', async () => {
    const cases: Array<{
      label: string;
      apply: () => Promise<unknown>;
      restore: () => Promise<unknown>;
    }> = [
      ...[
        'https://collector.example/v1/traces?',
        'https://collector.example/v1/traces#',
        'https://@collector.example/v1/traces',
        'https://:@collector.example/v1/traces',
        'https://collector.example/v1/traces/',
        'https://collector.example:443/v1/traces',
        'HTTPS://COLLECTOR.EXAMPLE/v1/traces',
      ].map((endpoint) => ({
        label: `endpoint ${endpoint}`,
        apply: () =>
          pool.query(`UPDATE agent_observability_bindings SET endpoint = $1 WHERE id = $2`, [
            endpoint,
            policyBindingId,
          ]),
        restore: () =>
          pool.query(`UPDATE agent_observability_bindings SET endpoint = $1 WHERE id = $2`, [
            'https://collector.example/v1/traces',
            policyBindingId,
          ]),
      })),
      {
        label: 'untrimmed key hint',
        apply: () =>
          pool.query(
            `UPDATE agent_observability_binding_credentials SET key_hint = ' ...abcd' WHERE binding_id = $1`,
            [policyBindingId],
          ),
        restore: () =>
          pool.query(
            `UPDATE agent_observability_binding_credentials SET key_hint = '...abcd' WHERE binding_id = $1`,
            [policyBindingId],
          ),
      },
      {
        label: 'untrimmed external project id',
        apply: () =>
          pool.query(
            `UPDATE agent_observability_bindings SET external_project_id = ' pk-observability' WHERE id = $1`,
            [policyBindingId],
          ),
        restore: () =>
          pool.query(
            `UPDATE agent_observability_bindings SET external_project_id = 'pk-observability' WHERE id = $1`,
            [policyBindingId],
          ),
      },
      {
        label: 'timeout above shared maximum',
        apply: () =>
          pool.query(
            `UPDATE agent_observability_binding_versions SET timeout_ms = 120001 WHERE binding_id = $1 AND version = 1`,
            [policyBindingId],
          ),
        restore: () =>
          pool.query(
            `UPDATE agent_observability_binding_versions SET timeout_ms = 5000 WHERE binding_id = $1 AND version = 1`,
            [policyBindingId],
          ),
      },
    ];

    for (const testCase of cases) {
      await testCase.apply();
      try {
        const response = await get(
          `/v1/organizations/workspaces/${policyWorkspaceId}/agent_observability`,
          readKey,
        );
        expect(response.statusCode, testCase.label).toBe(503);
        expect(response.json(), testCase.label).toEqual({
          error: 'agent observability state unavailable',
        });
      } finally {
        await testCase.restore();
      }
    }
  });

  it('serves admin state over bound HTTP listener', async () => {
    const response = await fetch(`${adminUrl}/v1/organizations/agent_observability`, {
      headers: { 'x-api-key': readKey },
    });

    expect(response.status).toBe(200);
    expect(response.headers.get('etag')).toMatch(/^"orca-aos-v1-[A-Za-z0-9_-]{43}"$/);
    await expect(response.json()).resolves.toMatchObject({
      type: 'agent_observability',
      scope: 'organization',
      organization_id: organizationId,
    });
  });

  function get(url: string, key: string) {
    return app!.inject({ method: 'GET', url, headers: { 'x-api-key': key } });
  }
});

async function createOrganization(db: DbClient, id: string, name: string): Promise<void> {
  await db.insert(organizations).values({ id, name, status: 'active' });
}

async function createWorkspace(
  db: DbClient,
  id: string,
  organizationId: string,
  name: string,
): Promise<void> {
  await db.insert(workspaces).values({
    id,
    organizationId,
    name,
    status: 'active',
    createdBy: 'integration-test',
  });
}

async function createAdminKey(
  db: DbClient,
  organizationId: string,
  scopes: string[],
): Promise<string> {
  const plaintext = generateAdminApiKey();
  await db.insert(adminApiKeys).values({
    id: `adminkey_observability_${randomBytes(5).toString('hex')}`,
    organizationId,
    name: 'Observability integration key',
    hashedKey: await hashAdminApiKey(plaintext),
    keyFingerprint: fingerprintAdminApiKey(plaintext),
    partialKeyHint: partialAdminApiKeyHint(plaintext),
    scopes,
    status: 'active',
    createdBy: 'integration-test',
  });
  return plaintext;
}

async function setWorkspaceMode(
  pool: Pool,
  workspaceId: string,
  organizationId: string,
  mode: 'inherit' | 'disabled' | 'custom',
  bindingId: string | null,
): Promise<void> {
  await pool.query(
    `
      UPDATE agent_observability_workspace_settings
      SET mode = $1, binding_id = $2
      WHERE workspace_id = $3 AND organization_id = $4
    `,
    [mode, bindingId, workspaceId, organizationId],
  );
}

async function insertBinding(
  pool: Pool,
  input: {
    id: string;
    organizationId: string;
    workspaceId: string | null;
    status?: 'active' | 'disabled';
    credential?: boolean;
    credentialVersion?: number;
    keyHint?: string | null;
    secretRef?: string;
  },
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const scopeType = input.workspaceId === null ? 'organization' : 'workspace';
    const status = input.status ?? 'active';
    await client.query(
      `
        INSERT INTO agent_observability_bindings (
          id, organization_id, workspace_id, scope_type, adapter_type, endpoint_kind, endpoint_class,
          endpoint, external_project_id, current_version, status, revocation_epoch, created_by, updated_by
        ) VALUES ($1, $2, $3, $4, 'otlp_http', 'traces_endpoint', 'public',
                  'https://collector.example/v1/traces', 'pk-observability', 1, $5, 0,
                  'integration-test', 'integration-test')
      `,
      [input.id, input.organizationId, input.workspaceId, scopeType, status],
    );
    await client.query(
      `
        INSERT INTO agent_observability_binding_versions (
          binding_id, version, adapter_type, semantic_profile, protocol, compression, timeout_ms,
          environment, release, capture_mode, sample_rate, config_schema_version, created_by
        ) VALUES ($1, 1, 'otlp_http', 'langfuse', 'http/protobuf', 'none', 5000,
                  'integration', 'v1', 'redacted_io', 0.5000, 1, 'integration-test')
      `,
      [input.id],
    );
    if (input.credential !== false) {
      await client.query(
        `
          INSERT INTO agent_observability_binding_credentials (
            binding_id, secret_ref, credential_version, key_hint, rotated_at, updated_by
          ) VALUES ($1, $2, $3, $4, now(), 'integration-test')
        `,
        [
          input.id,
          input.secretRef ?? `secret-ref-${input.id}`,
          input.credentialVersion ?? 1,
          input.keyHint ?? '...abcd',
        ],
      );
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
