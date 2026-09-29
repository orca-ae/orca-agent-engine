// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import * as argon2 from 'argon2';
import { createHash, randomUUID } from 'node:crypto';
import { Pool } from 'pg';

export interface IsolatedOrganizationAdmin {
  organizationId: string;
  apiKey: string;
  additionalApiKeys: Record<string, string>;
  /** Call after deleting HTTP resources and archiving the organization's workspaces. */
  archive: () => Promise<void>;
}

/**
 * The offline-bootstrap prerequisite for an isolated organization. Unlike the
 * shared seed helper, this gives each organization a unique name and never
 * rotates another spec's credential. Only the organization and admin keys touch
 * Postgres; workspaces, runtime keys, prices, guardrails, and sessions must still
 * be created through the live HTTP API.
 */
export async function seedIsolatedOrganizationAdmin(
  name: string,
  additionalKeyScopes: Record<string, readonly string[]> = {},
  databaseUrl?: string,
): Promise<IsolatedOrganizationAdmin> {
  const connectionString =
    databaseUrl ?? process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
  const organizationId = `org_e2e_${randomUUID().replaceAll('-', '')}`;
  const keyScopes: Array<[string, readonly string[]]> = [
    ['owner', ['org:admin']],
    ...Object.entries(additionalKeyScopes),
  ];
  const credentials = await Promise.all(
    keyScopes.map(async ([label, scopes]) => {
      const plaintext = `orca_admin_e2e_${randomUUID().replaceAll('-', '')}`;
      return {
        label,
        scopes,
        plaintext,
        id: `adminkey_e2e_${randomUUID().replaceAll('-', '')}`,
        hash: await argon2.hash(plaintext, { type: argon2.argon2id }),
        fingerprint: createHash('sha256')
          .update('orca-admin-api-key\0')
          .update(plaintext)
          .digest('hex'),
      };
    }),
  );
  const pool = new Pool({ connectionString, max: 1 });
  try {
    await pool.query('BEGIN');
    await pool.query(`INSERT INTO organizations (id, name, status) VALUES ($1, $2, 'active')`, [
      organizationId,
      `${name} ${organizationId}`,
    ]);
    for (const key of credentials) {
      await pool.query(
        `INSERT INTO admin_api_keys
           (id, organization_id, name, hashed_key, key_fingerprint, partial_key_hint,
            scopes, status, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', 'e2e-tests')`,
        [
          key.id,
          organizationId,
          `${name} ${key.label}`,
          key.hash,
          key.fingerprint,
          `...${key.plaintext.slice(-4)}`,
          key.scopes,
        ],
      );
    }
    await pool.query('COMMIT');
  } catch (error) {
    await pool.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    await pool.end();
  }

  return {
    organizationId,
    apiKey: credentials[0]!.plaintext,
    additionalApiKeys: Object.fromEntries(
      credentials.slice(1).map((key) => [key.label, key.plaintext]),
    ),
    async archive() {
      // There is no organization-admin endpoint for revoking bootstrap admin
      // keys or archiving an organization. Retain audit rows, retire credentials,
      // and touch only the bootstrap state this helper owns.
      const cleanupPool = new Pool({ connectionString, max: 1 });
      try {
        await cleanupPool.query('BEGIN');
        await cleanupPool.query(
          `UPDATE admin_api_keys SET status = 'archived', revoked_at = now(), updated_at = now()
           WHERE organization_id = $1`,
          [organizationId],
        );
        await cleanupPool.query(
          `UPDATE organizations SET status = 'archived', updated_at = now() WHERE id = $1`,
          [organizationId],
        );
        await cleanupPool.query('COMMIT');
      } catch (error) {
        await cleanupPool.query('ROLLBACK').catch(() => undefined);
        throw error;
      } finally {
        await cleanupPool.end();
      }
    },
  };
}
