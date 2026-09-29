// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq } from 'drizzle-orm';
import {
  fingerprintPlatformApiKey,
  generatePlatformApiKey,
  hashPlatformApiKey,
  isPlatformApiKey,
  partialPlatformApiKeyHint,
} from './auth/platform-api-key.js';
import { newId } from './domain/versioning.js';
import { buildDb } from './persistence/postgres/client.js';
import { platformApiKeys, platformAuditEvents } from './persistence/postgres/schema.js';

const databaseUrl = process.env['DATABASE_URL'];
if (!databaseUrl) throw new Error('DATABASE_URL is required');

const name = process.env['ORCA_PLATFORM_KEY_NAME'] ?? 'Rotated platform key';
const archiveKeyId = process.env['ORCA_ARCHIVE_PLATFORM_KEY_ID'];
const suppliedKey = process.env['ORCA_NEW_PLATFORM_API_KEY'];
if (suppliedKey !== undefined && !isPlatformApiKey(suppliedKey)) {
  throw new Error(
    'ORCA_NEW_PLATFORM_API_KEY must start with orca_platform_ followed by at least 32 URL-safe characters',
  );
}

const plaintext = suppliedKey ?? generatePlatformApiKey();
const hashedKey = await hashPlatformApiKey(plaintext);
const keyId = newId('platformkey');
const now = new Date();
const { db, pool } = buildDb({ url: databaseUrl, poolSize: 1 });

try {
  await db.transaction(async (tx) => {
    await tx.insert(platformApiKeys).values({
      id: keyId,
      name,
      hashedKey,
      keyFingerprint: fingerprintPlatformApiKey(plaintext),
      partialKeyHint: partialPlatformApiKeyHint(plaintext),
      scopes: ['platform:admin'],
      status: 'active',
      createdBy: 'offline-platform-cli',
      createdAt: now,
      updatedAt: now,
    });
    if (archiveKeyId) {
      const archived = await tx
        .update(platformApiKeys)
        .set({ status: 'archived', revokedAt: now, updatedAt: now })
        .where(and(eq(platformApiKeys.id, archiveKeyId), eq(platformApiKeys.status, 'active')))
        .returning({ id: platformApiKeys.id });
      if (archived.length !== 1) throw new Error('active platform key to archive not found');
      await tx.insert(platformAuditEvents).values({
        id: newId('audit'),
        actor: 'offline-platform-cli',
        authMethod: 'offline-database',
        action: 'platform_api_key.archived',
        targetType: 'platform_api_key',
        targetId: archiveKeyId,
        requestId: newId('offline'),
        result: 'success',
        metadata: { replacement_platform_api_key_id: keyId },
      });
    }
    await tx.insert(platformAuditEvents).values({
      id: newId('audit'),
      actor: 'offline-platform-cli',
      authMethod: 'offline-database',
      action: 'platform_api_key.created',
      targetType: 'platform_api_key',
      targetId: keyId,
      requestId: newId('offline'),
      result: 'success',
      metadata: { name, scopes: ['platform:admin'] },
    });
  });

  const result: Record<string, string> = { platform_api_key_id: keyId };
  if (suppliedKey === undefined) result['platform_api_key'] = plaintext;
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await pool.end();
}
