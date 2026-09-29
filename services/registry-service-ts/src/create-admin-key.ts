// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq } from 'drizzle-orm';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  isAdminApiKey,
  parseAdminApiKeyScopes,
  partialAdminApiKeyHint,
} from './auth/admin-api-key.js';
import { newId } from './domain/versioning.js';
import { buildDb } from './persistence/postgres/client.js';
import { adminApiKeys, adminAuditEvents, organizations } from './persistence/postgres/schema.js';

const databaseUrl = process.env['DATABASE_URL'];
if (!databaseUrl) throw new Error('DATABASE_URL is required');
const organizationId = process.env['ORCA_ADMIN_ORGANIZATION_ID'];
if (!organizationId) throw new Error('ORCA_ADMIN_ORGANIZATION_ID is required');

const name = process.env['ORCA_ADMIN_KEY_NAME'] ?? 'Rotated admin key';
const archiveKeyId = process.env['ORCA_ARCHIVE_ADMIN_KEY_ID'];
const suppliedKey = process.env['ORCA_NEW_ADMIN_API_KEY'];
if (suppliedKey !== undefined && !isAdminApiKey(suppliedKey)) {
  throw new Error(
    'ORCA_NEW_ADMIN_API_KEY must start with orca_admin_ followed by at least 32 URL-safe characters',
  );
}
const scopes = parseAdminApiKeyScopes(process.env['ORCA_ADMIN_KEY_SCOPES']);

const plaintext = suppliedKey ?? generateAdminApiKey();
const hashedKey = await hashAdminApiKey(plaintext);
const keyId = newId('adminkey');
const now = new Date();
const { db, pool } = buildDb({ url: databaseUrl, poolSize: 1 });
try {
  const organization = await db.query.organizations.findFirst({
    where: eq(organizations.id, organizationId),
  });
  if (!organization || organization.status !== 'active') {
    throw new Error('active organization not found');
  }

  await db.transaction(async (tx) => {
    await tx.insert(adminApiKeys).values({
      id: keyId,
      organizationId,
      name,
      hashedKey,
      keyFingerprint: fingerprintAdminApiKey(plaintext),
      partialKeyHint: partialAdminApiKeyHint(plaintext),
      scopes,
      status: 'active',
      createdBy: 'offline-admin-cli',
      createdAt: now,
      updatedAt: now,
    });
    if (archiveKeyId) {
      const archived = await tx
        .update(adminApiKeys)
        .set({ status: 'archived', revokedAt: now, updatedAt: now })
        .where(
          and(
            eq(adminApiKeys.id, archiveKeyId),
            eq(adminApiKeys.organizationId, organizationId),
            eq(adminApiKeys.status, 'active'),
          ),
        )
        .returning({ id: adminApiKeys.id });
      if (archived.length !== 1) throw new Error('active admin key to archive not found');
      await tx.insert(adminAuditEvents).values({
        id: newId('audit'),
        organizationId,
        actor: 'offline-admin-cli',
        authMethod: 'offline-database',
        action: 'admin_api_key.archived',
        targetType: 'admin_api_key',
        targetId: archiveKeyId,
        requestId: newId('offline'),
        result: 'success',
        metadata: { replacement_admin_api_key_id: keyId },
      });
    }
    await tx.insert(adminAuditEvents).values({
      id: newId('audit'),
      organizationId,
      actor: 'offline-admin-cli',
      authMethod: 'offline-database',
      action: 'admin_api_key.created',
      targetType: 'admin_api_key',
      targetId: keyId,
      requestId: newId('offline'),
      result: 'success',
      metadata: { name, scopes },
    });
  });

  const result: Record<string, string> = {
    organization_id: organizationId,
    admin_api_key_id: keyId,
  };
  if (suppliedKey === undefined) result['admin_api_key'] = plaintext;
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await pool.end();
}
