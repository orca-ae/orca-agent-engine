// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { sql } from 'drizzle-orm';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  isAdminApiKey,
  partialAdminApiKeyHint,
} from './auth/admin-api-key.js';
import {
  fingerprintPlatformApiKey,
  generatePlatformApiKey,
  hashPlatformApiKey,
  isPlatformApiKey,
  partialPlatformApiKeyHint,
} from './auth/platform-api-key.js';
import {
  newOrganizationObservabilitySettings,
  newWorkspaceObservabilitySettings,
} from './domain/agent-observability-settings.js';
import { newId } from './domain/versioning.js';
import { buildDb } from './persistence/postgres/client.js';
import {
  adminApiKeys,
  agentObservabilityOrganizationSettings,
  agentObservabilityWorkspaceSettings,
  organizations,
  platformApiKeys,
  workspaces,
} from './persistence/postgres/schema.js';

// Keep this command offline: possession of Registry database credentials is
// the one-time installation bootstrap authority. The Registry server itself
// never reads ORCA_BOOTSTRAP_ADMIN_API_KEY.
const databaseUrl = process.env['DATABASE_URL'];
if (!databaseUrl) throw new Error('DATABASE_URL is required');
const organizationName = process.env['ORCA_BOOTSTRAP_ORGANIZATION_NAME'] ?? 'Default organization';
const workspaceName = process.env['ORCA_BOOTSTRAP_WORKSPACE_NAME'] ?? 'Default workspace';
const adminKeyName = process.env['ORCA_BOOTSTRAP_ADMIN_KEY_NAME'] ?? 'Bootstrap admin key';
const platformKeyName = process.env['ORCA_BOOTSTRAP_PLATFORM_KEY_NAME'] ?? 'Bootstrap platform key';
const suppliedKey = process.env['ORCA_BOOTSTRAP_ADMIN_API_KEY'];
const suppliedPlatformKey = process.env['ORCA_BOOTSTRAP_PLATFORM_API_KEY'];
// Optional OIDC audience for the bootstrap organization, for a deployment that
// intends to run with OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE. An organization's
// audience is set at creation and there is no update route in this phase, so
// without this the one organization every installation starts with could only
// acquire an audience by recreating the database. Unset leaves the column null,
// exactly as before, and an organization with no audience takes part in no
// audience-based resolution.
const organizationAudience = process.env['ORCA_BOOTSTRAP_ORGANIZATION_AUDIENCE'];

if (organizationAudience !== undefined && organizationAudience.trim().length === 0) {
  // Empty is refused rather than read as unset: `''` is a value the unique
  // index would accept, and it authenticates nothing, so it is a silent
  // misconfiguration for a deployment that set the variable on purpose.
  throw new Error(
    'ORCA_BOOTSTRAP_ORGANIZATION_AUDIENCE must not be blank; leave it unset to create the ' +
      'organization without an audience',
  );
}
if (suppliedKey !== undefined && !isAdminApiKey(suppliedKey)) {
  throw new Error(
    'ORCA_BOOTSTRAP_ADMIN_API_KEY must start with orca_admin_ followed by at least 32 URL-safe characters',
  );
}
if (suppliedPlatformKey !== undefined && !isPlatformApiKey(suppliedPlatformKey)) {
  throw new Error(
    'ORCA_BOOTSTRAP_PLATFORM_API_KEY must start with orca_platform_ followed by at least 32 URL-safe characters',
  );
}

const plaintext = suppliedKey ?? generateAdminApiKey();
const hashedKey = await hashAdminApiKey(plaintext);
const platformPlaintext = suppliedPlatformKey ?? generatePlatformApiKey();
const platformHashedKey = await hashPlatformApiKey(platformPlaintext);
const organizationId = process.env['ORCA_BOOTSTRAP_ORGANIZATION_ID'] ?? newId('org');
const workspaceId = process.env['ORCA_BOOTSTRAP_WORKSPACE_ID'] ?? newId('wrkspc');
const adminKeyId = newId('adminkey');
const platformKeyId = newId('platformkey');
const now = new Date();
const { db, pool } = buildDb({ url: databaseUrl, poolSize: 1 });

try {
  await db.transaction(
    async (tx) => {
      // Serializes two bootstrap jobs even while the organizations table is empty.
      await tx.execute(sql`select pg_advisory_xact_lock(705646025101507)`);
      const existing = await tx.select({ id: organizations.id }).from(organizations).limit(1);
      if (existing.length > 0) {
        throw new Error('bootstrap refused: an organization already exists');
      }

      await tx.insert(organizations).values({
        id: organizationId,
        name: organizationName,
        status: 'active',
        ...(organizationAudience === undefined ? {} : { audience: organizationAudience.trim() }),
        createdAt: now,
        updatedAt: now,
      });
      // Intentionally redundant with migration 0048's mixed-version trigger;
      // retained as the durable writer path after that trigger is removed.
      await tx
        .insert(agentObservabilityOrganizationSettings)
        .values(newOrganizationObservabilitySettings(organizationId, now))
        .onConflictDoNothing();
      await tx.insert(platformApiKeys).values({
        id: platformKeyId,
        name: platformKeyName,
        hashedKey: platformHashedKey,
        keyFingerprint: fingerprintPlatformApiKey(platformPlaintext),
        partialKeyHint: partialPlatformApiKeyHint(platformPlaintext),
        scopes: ['platform:admin'],
        status: 'active',
        createdBy: 'installation-bootstrap',
        createdAt: now,
        updatedAt: now,
      });
      await tx.insert(workspaces).values({
        id: workspaceId,
        organizationId,
        name: workspaceName,
        status: 'active',
        createdBy: 'installation-bootstrap',
        createdAt: now,
        updatedAt: now,
      });
      // Intentionally redundant with migration 0048's mixed-version trigger;
      // retained as the durable writer path after that trigger is removed.
      await tx
        .insert(agentObservabilityWorkspaceSettings)
        .values(newWorkspaceObservabilitySettings(organizationId, workspaceId, now))
        .onConflictDoNothing();
      await tx.insert(adminApiKeys).values({
        id: adminKeyId,
        organizationId,
        name: adminKeyName,
        hashedKey,
        keyFingerprint: fingerprintAdminApiKey(plaintext),
        partialKeyHint: partialAdminApiKeyHint(plaintext),
        scopes: ['org:admin'],
        status: 'active',
        createdBy: 'installation-bootstrap',
        createdAt: now,
        updatedAt: now,
      });
    },
    { isolationLevel: 'serializable' },
  );

  const result: Record<string, string> = {
    organization_id: organizationId,
    workspace_id: workspaceId,
    admin_api_key_id: adminKeyId,
    platform_api_key_id: platformKeyId,
  };
  if (suppliedKey === undefined) result['admin_api_key'] = plaintext;
  if (suppliedPlatformKey === undefined) result['platform_api_key'] = platformPlaintext;
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await pool.end();
}
