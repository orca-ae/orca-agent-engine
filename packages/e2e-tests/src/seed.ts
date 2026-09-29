// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import * as argon2 from 'argon2';
import { createHash } from 'node:crypto';
import { customAlphabet } from 'nanoid';
import { Pool } from 'pg';

/**
 * Deterministic workspace + principal used by every e2e-tests run. Keeping
 * these stable means re-running the suite locally (or on a CI box that reuses
 * a Postgres volume) reuses the same row instead of accumulating hundreds of
 * `api_keys`. The actual key plaintext rotates on every call — argon2 hashes
 * are one-way so we can't reuse a previously-issued plaintext, but we can
 * idempotently update the same row's `hashed_key`.
 */
const E2E_WORKSPACE_ID = 'ws_e2e_tests';
const E2E_ORGANIZATION_ID = 'org_e2e_tests';
const E2E_PRINCIPAL = 'e2e-tests';
const E2E_API_KEY_ID = 'apikey_e2e_tests';
const E2E_ADMIN_API_KEY_ID = 'adminkey_e2e_tests';

/**
 * Scopes the e2e-tests api key gets. Mirrors the unit-test scopes in
 * `services/registry-service-ts/test/integration/fixtures.ts` plus the file +
 * memory-store scopes the wire-conformance specs need. We err generous: the
 * e2e-tests workspace is its own tenant on the dev stack, so over-broad
 * scopes don't bleed into other tests.
 */
const E2E_SCOPES = [
  'workspace.agents.create',
  'workspace.agents.alter',
  'workspace.agents.describe',
  'workspace.agents.delete',
  'workspace.sessions.create',
  'workspace.sessions.alter',
  'workspace.sessions.describe',
  'workspace.sessions.delete',
  'workspace.agentTriggers.create',
  'workspace.agentTriggers.alter',
  'workspace.agentTriggers.describe',
  'workspace.agentTriggers.delete',
  'workspace.files.create',
  'workspace.files.alter',
  'workspace.files.describe',
  'workspace.files.delete',
  'workspace.memory_stores.create',
  'workspace.memory_stores.alter',
  'workspace.memory_stores.describe',
  'workspace.memory_stores.delete',
];

const nano = customAlphabet('abcdefghijklmnopqrstuvwxyz0123456789', 32);

/**
 * Generate a workspace api key plaintext that matches the prefix the registry
 * api-key middleware looks for. Mirrors `services/registry-service-ts/src/auth/api-key.ts`
 * (`header.startsWith('orca_')`).
 *
 * Honors `ORCA_E2E_PLAINTEXT_KEY` for deterministic plaintext — useful for
 * cross-run idempotency on a CI box that reuses the Postgres volume. This
 * credential belongs only to the e2e API client and is never shared with the
 * Harness.
 */
function generateE2EApiKey(): string {
  return process.env['ORCA_E2E_PLAINTEXT_KEY'] ?? `orca_e2e_${nano()}`;
}

function fingerprintApiKey(plaintext: string): string {
  return createHash('sha256').update('orca-api-key\0').update(plaintext).digest('hex');
}

function generateE2EAdminApiKey(): string {
  return `orca_admin_e2e_${nano()}`;
}

function fingerprintAdminApiKey(plaintext: string): string {
  return createHash('sha256').update('orca-admin-api-key\0').update(plaintext).digest('hex');
}

export interface SeededApiKey {
  /** The plaintext api-key — pass this in the `x-api-key` header. */
  apiKey: string;
  /** Workspace the key is scoped to. Same on every call (idempotent). */
  workspaceId: string;
}

export interface SeededAdminApiKey {
  /** The plaintext organization-admin key — pass this only to the admin listener. */
  apiKey: string;
  /** Organization the admin key is scoped to. */
  organizationId: string;
}

/**
 * Idempotently provisions the organization-admin credential used by e2e
 * tests. This models the output of the offline bootstrap command; workspace
 * creation and workspace-key issuance still go through the live admin API.
 */
export async function seedOrganizationAdminApiKey(): Promise<SeededAdminApiKey> {
  const url = process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
  const pool = new Pool({ connectionString: url, max: 1 });
  try {
    const plaintext = generateE2EAdminApiKey();
    const hashed = await argon2.hash(plaintext, { type: argon2.argon2id });
    const keyFingerprint = fingerprintAdminApiKey(plaintext);
    await pool.query('BEGIN');
    await pool.query(
      `INSERT INTO organizations (id, name, status)
       VALUES ($1, $2, 'active')
       ON CONFLICT (id) DO UPDATE SET status = 'active', updated_at = now()`,
      [E2E_ORGANIZATION_ID, 'E2E tests'],
    );
    await pool.query(
      `INSERT INTO admin_api_keys
         (id, organization_id, name, hashed_key, key_fingerprint, partial_key_hint,
          scopes, status, revoked_at, created_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', NULL, $8)
       ON CONFLICT (id) DO UPDATE SET
         hashed_key = EXCLUDED.hashed_key,
         key_fingerprint = EXCLUDED.key_fingerprint,
         partial_key_hint = EXCLUDED.partial_key_hint,
         scopes = EXCLUDED.scopes,
         status = 'active',
         expires_at = NULL,
         revoked_at = NULL,
         updated_at = now()`,
      [
        E2E_ADMIN_API_KEY_ID,
        E2E_ORGANIZATION_ID,
        'E2E organization admin',
        hashed,
        keyFingerprint,
        `...${plaintext.slice(-4)}`,
        ['org:admin'],
        E2E_PRINCIPAL,
      ],
    );
    await pool.query('COMMIT');
    return { apiKey: plaintext, organizationId: E2E_ORGANIZATION_ID };
  } catch (error) {
    await pool.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    await pool.end().catch(() => {});
  }
}

/**
 * Idempotently provisions a workspace + api key in the registry Postgres DB.
 *
 * Behavior:
 *  - Inserts a fresh `api_keys` row (id `apikey_e2e_tests`, workspace_id
 *    `ws_e2e_tests`) on first invocation.
 *  - On subsequent invocations rotates the `hashed_key` (argon2 verify is
 *    one-way so we can't reissue the previous plaintext), but reuses the
 *    same row + workspace_id so the e2e-tests workspace's data accumulates
 *    in one logical tenant across runs.
 *
 * Connection: reads `DATABASE_URL` from env, defaults to the dev compose
 * Postgres at `postgres://orca:orca@localhost:5432/registry`. Caller
 * receives the plaintext key once; subsequent calls return a NEW plaintext
 * (the database row's hashed_key is rotated). Caller must either cache the
 * returned plaintext or treat each `seedWorkspaceApiKey()` call as the
 * single point of truth for that test run.
 */
export async function seedWorkspaceApiKey(): Promise<SeededApiKey> {
  const url = process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
  const pool = new Pool({ connectionString: url, max: 1 });
  try {
    const plaintext = generateE2EApiKey();
    const hashed = await argon2.hash(plaintext, { type: argon2.argon2id });
    const keyFingerprint = fingerprintApiKey(plaintext);
    await pool.query('BEGIN');
    await pool.query(
      `INSERT INTO organizations (id, name, status)
       VALUES ($1, $2, 'active')
       ON CONFLICT (id) DO UPDATE SET status = 'active', updated_at = now()`,
      [E2E_ORGANIZATION_ID, 'E2E tests'],
    );
    await pool.query(
      `INSERT INTO workspaces (id, organization_id, name, status, created_by)
       VALUES ($1, $2, $3, 'active', $4)
       ON CONFLICT (id) DO UPDATE SET
         status = 'active',
         archived_at = NULL,
         updated_at = now()`,
      [E2E_WORKSPACE_ID, E2E_ORGANIZATION_ID, 'E2E tests', E2E_PRINCIPAL],
    );
    // ON CONFLICT (id) DO UPDATE rotates the hashed_key + clears any prior
    // revocation + refreshes the principal/scopes. The unique constraint on
    // `id` is the primary-key clustered index — no extra index needed.
    await pool.query(
      `INSERT INTO api_keys
         (id, workspace_id, hashed_key, key_fingerprint, name, partial_key_hint,
          principal, scopes, status, revoked_at, created_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', NULL, $9)
       ON CONFLICT (id) DO UPDATE SET
         hashed_key = EXCLUDED.hashed_key,
         key_fingerprint = EXCLUDED.key_fingerprint,
         name = EXCLUDED.name,
         partial_key_hint = EXCLUDED.partial_key_hint,
         principal = EXCLUDED.principal,
         scopes = EXCLUDED.scopes,
         status = 'active',
         revoked_at = NULL,
         updated_at = now()`,
      [
        E2E_API_KEY_ID,
        E2E_WORKSPACE_ID,
        hashed,
        keyFingerprint,
        'E2E tests',
        `...${plaintext.slice(-4)}`,
        E2E_PRINCIPAL,
        E2E_SCOPES,
        E2E_PRINCIPAL,
      ],
    );
    await pool.query('COMMIT');
    return { apiKey: plaintext, workspaceId: E2E_WORKSPACE_ID };
  } catch (error) {
    await pool.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    await pool.end().catch(() => {});
  }
}
