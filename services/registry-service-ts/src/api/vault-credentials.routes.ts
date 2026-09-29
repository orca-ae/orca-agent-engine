// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type { FastifyBaseLogger, FastifyInstance } from 'fastify';
import { and, count, desc, eq, inArray, isNull, lt, ne, or, sql } from 'drizzle-orm';
import { canonicalMcpServerUrl } from '../domain/mcp-destination.js';
import {
  isCompatibleProviderCredentialScheme,
  isLogicalCredentialId,
  isProviderCredentialProvider,
  isProviderCredentialScheme,
  LOGICAL_CREDENTIAL_ID_PATTERN,
  type ProviderCredentialProvider,
  type ProviderCredentialScheme,
} from '../domain/provider-credential.js';
import { newId } from '../domain/versioning.js';
import type { DbClient } from '../persistence/postgres/client.js';
import { vaultCredentials, vaults } from '../persistence/postgres/schema.js';
import type { SecretProvider, SecretStore } from '../secrets/secret-provider.js';
import { createGuardedFetch } from './egress-guard.js';

export type CredentialRow = typeof vaultCredentials.$inferSelect;
type CredentialReadDb = Pick<DbClient, 'select'>;
type CredentialWriteDb = Pick<DbClient, 'select' | 'update' | 'delete'>;

interface RegisterVaultCredentialOptions {
  secretStore?: SecretStore | undefined;
  /**
   * Outbound HTTP seam for the mcp_oauth_validate diagnostic; tests inject a
   * stub. When absent, the global fetch is used wrapped in an SSRF egress
   * guard (mcp_server_url / token_endpoint are user-controlled).
   */
  fetchImpl?: typeof fetch | undefined;
}

interface ValidationResult<T> {
  ok: boolean;
  value?: T;
  error?: string;
}

interface StaticBearerCreateAuth {
  type: 'static_bearer';
  token: string;
  mcp_server_url: string;
}

interface McpOauthCreateAuth {
  type: 'mcp_oauth';
  access_token: string;
  mcp_server_url: string;
  expires_at: string | null;
  refresh: {
    refresh_token: string;
    token_endpoint: string;
    client_id: string;
    token_endpoint_auth:
      | { type: 'none' }
      | { type: 'client_secret_basic'; client_secret: string }
      | { type: 'client_secret_post'; client_secret: string };
    resource: string | null;
    scope: string | null;
  } | null;
}

type EnvVarNetworking = { type: 'limited'; allowed_hosts: string[] } | { type: 'unrestricted' };
type InjectionLocation = { header: boolean; body: boolean };
const DEFAULT_INJECTION_LOCATION: InjectionLocation = { header: true, body: false };
const RFC3339_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

interface CredentialAuthConfig {
  injection_location?: InjectionLocation;
  expires_at?: string | null;
  resource?: string | null;
  scope?: string | null;
}

interface EnvironmentVariableCreateAuth {
  type: 'environment_variable';
  secret_name: string;
  secret_value: string;
  networking: EnvVarNetworking;
  injection_location: InjectionLocation;
}

interface ProviderCreateAuth {
  type: 'provider';
  provider: ProviderCredentialProvider;
  scheme: ProviderCredentialScheme;
  logical_id: string;
  secret_value: string;
}

type CredentialCreateAuth =
  | StaticBearerCreateAuth
  | McpOauthCreateAuth
  | EnvironmentVariableCreateAuth
  | ProviderCreateAuth;

interface CredentialCreateBody {
  display_name?: string | null | undefined;
  metadata: Record<string, string>;
  auth: CredentialCreateAuth;
}

interface CredentialUpdateBody {
  display_name?: string | null | undefined;
  metadata?: Record<string, string | null> | null;
  auth?: Record<string, unknown>;
}

interface CredentialListPage {
  limit: number;
  page?: CredentialCursor;
  includeArchived: boolean;
}

interface CredentialCursor {
  createdAt: Date;
  id: string;
}

function isOrcaBetaRequest(headers: Record<string, string | string[] | undefined>): boolean {
  const value = headers['orca-beta'];
  return Array.isArray(value)
    ? value.some((entry) => entry.trim().length > 0)
    : typeof value === 'string' && value.trim().length > 0;
}

function isCredentialVisible(row: CredentialRow, orcaBeta: boolean): boolean {
  return orcaBeta || row.authType !== 'provider';
}

export function registerVaultCredentialRoutes(
  app: FastifyInstance,
  db: DbClient,
  opts: RegisterVaultCredentialOptions = {},
): void {
  app.get('/v1/vaults/:vault_id/credentials', async (req, reply) => {
    const auth = req.auth!;
    const orcaBeta = isOrcaBetaRequest(req.headers);
    const { vault_id: vaultId } = req.params as { vault_id: string };
    const page = parseCredentialListQuery(req.query);
    if (!page.ok) return reply.code(400).send({ error: page.error });
    const vault = await loadVault(db, auth.workspaceId, vaultId);
    if (!vault) return reply.code(404).send({ error: 'not found' });

    const cursor = page.value!.page;
    const filters = and(
      eq(vaultCredentials.workspaceId, auth.workspaceId),
      eq(vaultCredentials.vaultId, vaultId),
      orcaBeta ? undefined : ne(vaultCredentials.authType, 'provider'),
      page.value!.includeArchived ? undefined : isNull(vaultCredentials.archivedAt),
      cursor
        ? or(
            lt(vaultCredentials.createdAt, cursor.createdAt),
            and(
              eq(vaultCredentials.createdAt, cursor.createdAt),
              lt(vaultCredentials.id, cursor.id),
            ),
          )
        : undefined,
    );

    const rows = await db
      .select()
      .from(vaultCredentials)
      .where(and(isNull(vaultCredentials.deletedAt), filters))
      .orderBy(desc(vaultCredentials.createdAt), desc(vaultCredentials.id))
      .limit(page.value!.limit + 1);
    const credentials = rows.slice(0, page.value!.limit);
    reply.send({
      data: credentials.map(toCredentialApi),
      next_page:
        rows.length > page.value!.limit
          ? encodeCredentialCursor(credentials[credentials.length - 1]!)
          : null,
    });
  });

  app.post('/v1/vaults/:vault_id/credentials', async (req, reply) => {
    const auth = req.auth!;
    const { vault_id: vaultId } = req.params as { vault_id: string };
    const body = validateCreateBody(req.body);
    if (!body.ok) return reply.code(400).send({ error: body.error });

    const create = body.value!;
    if (create.auth.type === 'provider' && !isOrcaBetaRequest(req.headers)) {
      return reply.code(400).send({ error: "auth.type 'provider' requires orca-beta" });
    }
    const vault = await loadActiveVault(db, auth.workspaceId, vaultId);
    if (!vault) return reply.code(404).send({ error: 'not found' });

    const activeCount = await countActiveCredentials(db, auth.workspaceId, vaultId);
    if (activeCount >= 20) return reply.code(409).send({ error: 'credential limit exceeded' });

    const duplicateError = duplicateErrorForCreateAuth(create.auth);
    const duplicate = await loadDuplicateCredentialForCreate(
      db,
      auth.workspaceId,
      vaultId,
      create.auth,
    );
    if (duplicate) {
      return reply.code(409).send({ error: duplicateError });
    }
    if (!opts.secretStore) return reply.code(503).send({ error: 'secret store unavailable' });

    const credentialId = newId('vcrd');
    const now = new Date();
    const refs = credentialSecretRefs(auth.workspaceId, credentialId);

    const values: typeof vaultCredentials.$inferInsert = {
      id: credentialId,
      workspaceId: auth.workspaceId,
      vaultId,
      displayName: create.display_name ?? null,
      authType: create.auth.type,
      provider: create.auth.type === 'provider' ? create.auth.provider : null,
      scheme: create.auth.type === 'provider' ? create.auth.scheme : null,
      logicalId: create.auth.type === 'provider' ? create.auth.logical_id : null,
      resolutionVersion: create.auth.type === 'provider' ? randomUUID() : null,
      mcpServerUrl:
        create.auth.type === 'static_bearer' || create.auth.type === 'mcp_oauth'
          ? create.auth.mcp_server_url
          : null,
      secretName: create.auth.type === 'environment_variable' ? create.auth.secret_name : null,
      networking: create.auth.type === 'environment_variable' ? create.auth.networking : {},
      accessSecretRef: refs.access,
      refreshSecretRef: null,
      tokenEndpoint: null,
      clientId: null,
      tokenEndpointAuthType: null,
      clientSecretRef: null,
      authConfig:
        create.auth.type === 'environment_variable'
          ? { injection_location: create.auth.injection_location }
          : create.auth.type === 'mcp_oauth'
            ? {
                expires_at: create.auth.expires_at,
                resource: create.auth.refresh?.resource ?? null,
                scope: create.auth.refresh?.scope ?? null,
              }
            : {},
      metadata: create.metadata,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    };

    const stagedRefs: string[] = [];
    try {
      if (create.auth.type === 'environment_variable') {
        await putStagedSecret(opts.secretStore, stagedRefs, refs.access, create.auth.secret_value);
      } else if (create.auth.type === 'provider') {
        await putStagedSecret(opts.secretStore, stagedRefs, refs.access, create.auth.secret_value);
      } else if (create.auth.type === 'static_bearer') {
        await putStagedSecret(opts.secretStore, stagedRefs, refs.access, create.auth.token);
      } else {
        await putStagedSecret(opts.secretStore, stagedRefs, refs.access, create.auth.access_token);
        if (create.auth.refresh) {
          await putStagedSecret(
            opts.secretStore,
            stagedRefs,
            refs.refresh,
            create.auth.refresh.refresh_token,
          );
          values.refreshSecretRef = refs.refresh;
          values.tokenEndpoint = create.auth.refresh.token_endpoint;
          values.clientId = create.auth.refresh.client_id;
          values.tokenEndpointAuthType = create.auth.refresh.token_endpoint_auth.type;
        }
        if (create.auth.refresh && create.auth.refresh.token_endpoint_auth.type !== 'none') {
          await putStagedSecret(
            opts.secretStore,
            stagedRefs,
            refs.clientSecret,
            create.auth.refresh.token_endpoint_auth.client_secret,
          );
          values.clientSecretRef = refs.clientSecret;
        }
      }
    } catch (err) {
      await purgeSecretRefs(opts.secretStore, stagedRefs).catch((purgeErr: unknown) => {
        req.log.warn(
          { err: purgeErr, credentialId },
          'failed to purge staged secrets after failed credential secret write',
        );
      });
      throw err;
    }

    try {
      const result = await retrySerializationFailure(() =>
        db.transaction(
          async (tx) => {
            const lockedVault = await loadActiveVaultForUpdate(tx, auth.workspaceId, vaultId);
            if (!lockedVault) return 'not-found' as const;
            const nextActiveCount = await countActiveCredentials(tx, auth.workspaceId, vaultId);
            if (nextActiveCount >= 20) return 'limit' as const;
            const nextDuplicate = await loadDuplicateCredentialForCreate(
              tx,
              auth.workspaceId,
              vaultId,
              create.auth,
            );
            if (nextDuplicate) return 'duplicate' as const;
            await tx.insert(vaultCredentials).values(values);
            return 'created' as const;
          },
          { isolationLevel: 'serializable' },
        ),
      );
      if (result !== 'created') {
        await purgeSecretRefs(opts.secretStore, [
          refs.access,
          refs.refresh,
          refs.clientSecret,
        ]).catch((err: unknown) => {
          req.log.warn({ err, credentialId }, 'failed to purge secrets after rejected insert');
        });
        if (result === 'not-found') return reply.code(404).send({ error: 'not found' });
        if (result === 'limit') return reply.code(409).send({ error: 'credential limit exceeded' });
        return reply.code(409).send({ error: duplicateError });
      }
    } catch (err) {
      await purgeSecretRefs(opts.secretStore, [refs.access, refs.refresh, refs.clientSecret]).catch(
        (purgeErr: unknown) => {
          req.log.warn(
            { err: purgeErr, credentialId },
            'failed to purge secrets after failed insert',
          );
        },
      );
      if (isUniqueViolation(err)) {
        return reply.code(409).send({ error: duplicateError });
      }
      throw err;
    }

    const out = await loadCredential(db, auth.workspaceId, vaultId, credentialId);
    return reply.code(200).send(toCredentialApi(out!));
  });

  app.get('/v1/vaults/:vault_id/credentials/:credential_id', async (req, reply) => {
    const auth = req.auth!;
    const { vault_id: vaultId, credential_id: credentialId } = req.params as {
      vault_id: string;
      credential_id: string;
    };
    const vault = await loadVault(db, auth.workspaceId, vaultId);
    if (!vault) return reply.code(404).send({ error: 'not found' });
    const row = await loadCredential(db, auth.workspaceId, vaultId, credentialId);
    if (!row || !isCredentialVisible(row, isOrcaBetaRequest(req.headers))) {
      return reply.code(404).send({ error: 'not found' });
    }
    reply.send(toCredentialApi(row));
  });

  app.post('/v1/vaults/:vault_id/credentials/:credential_id', async (req, reply) => {
    const auth = req.auth!;
    const { vault_id: vaultId, credential_id: credentialId } = req.params as {
      vault_id: string;
      credential_id: string;
    };
    const body = validateUpdateBody(req.body);
    if (!body.ok) return reply.code(400).send({ error: body.error });
    const vault = await loadActiveVault(db, auth.workspaceId, vaultId);
    if (!vault) return reply.code(404).send({ error: 'not found' });
    const row = await loadCredential(db, auth.workspaceId, vaultId, credentialId);
    if (!row || row.archivedAt || !isCredentialVisible(row, isOrcaBetaRequest(req.headers))) {
      return reply.code(404).send({ error: 'not found' });
    }

    const next = body.value!;
    const stagedRefs: string[] = [];
    let oldRefs: string[] = [];
    try {
      const result = await db.transaction(
        async (tx) => {
          const locked = await loadActiveCredentialForUpdate(
            tx,
            auth.workspaceId,
            vaultId,
            credentialId,
          );
          if (!locked) return { status: 'not-found' as const };
          const { row: lockedRow, hasLiveRefreshLease } = locked;

          const update: Partial<typeof vaultCredentials.$inferInsert> = { updatedAt: new Date() };
          if ('display_name' in next) update.displayName = next.display_name ?? null;
          if (next.metadata !== undefined) {
            const metadata =
              next.metadata === null ? {} : applyMetadataPatch(lockedRow.metadata, next.metadata);
            const metadataError = validateStoredMetadata(metadata);
            if (metadataError) return { status: 'invalid' as const, error: metadataError };
            update.metadata = metadata;
          }

          if (next.auth !== undefined) {
            const authUpdate = validateAuthUpdate(lockedRow, next.auth);
            if (!authUpdate.ok) return { status: 'invalid' as const, error: authUpdate.error! };
            const rotationPlan = authUpdate.value!;
            const rotatesSecrets = requiresSecretStore(rotationPlan);
            if (rotatesSecrets && !opts.secretStore) {
              return { status: 'secret-store-unavailable' as const };
            }
            if (rotatesSecrets && hasLiveRefreshLease && !supersedesRefreshLease(rotationPlan)) {
              // The lease holder may already have consumed a one-time refresh
              // token. Let it persist that result before an access/client-only
              // rotation changes its pointer CAS inputs.
              return { status: 'refresh-in-progress' as const };
            }
            if (opts.secretStore) {
              const rotation = await stageSecretRotations(
                opts.secretStore,
                lockedRow,
                rotationPlan,
                stagedRefs,
              );
              oldRefs = rotation.oldRefs;
              Object.assign(update, rotation.rowUpdates);
            }
            Object.assign(update, rotationPlan.rowUpdates);
          }

          await tx
            .update(vaultCredentials)
            .set(update)
            .where(
              and(
                isNull(vaultCredentials.deletedAt),
                eq(vaultCredentials.id, credentialId),
                eq(vaultCredentials.vaultId, vaultId),
                eq(vaultCredentials.workspaceId, auth.workspaceId),
                isNull(vaultCredentials.archivedAt),
              ),
            );
          const out = await loadCredential(tx, auth.workspaceId, vaultId, credentialId);
          return { status: 'updated' as const, out: out! };
        },
        { isolationLevel: 'serializable' },
      );

      if (result.status === 'not-found') {
        await purgeSecretRefs(opts.secretStore, stagedRefs).catch((err: unknown) => {
          req.log.warn(
            { err, credentialId },
            'failed to purge staged secrets after rejected update',
          );
        });
        return reply.code(404).send({ error: 'not found' });
      }
      if (result.status === 'invalid') return reply.code(400).send({ error: result.error });
      if (result.status === 'secret-store-unavailable') {
        return reply.code(503).send({ error: 'secret store unavailable' });
      }
      if (result.status === 'refresh-in-progress') {
        return reply.code(409).send({ error: 'credential refresh in progress' });
      }

      await purgeSecretRefs(opts.secretStore, oldRefs).catch((err: unknown) => {
        req.log.warn({ err, credentialId }, 'failed to purge old secrets after update');
      });
      return reply.send(toCredentialApi(result.out));
    } catch (err) {
      await purgeSecretRefs(opts.secretStore, stagedRefs).catch((purgeErr: unknown) => {
        req.log.warn(
          { err: purgeErr, credentialId },
          'failed to purge staged secrets after failed update',
        );
      });
      if (row.authType === 'provider' && isUniqueViolation(err)) {
        return reply.code(409).send({ error: 'credential already exists for logical_id' });
      }
      throw err;
    }
  });

  app.post('/v1/vaults/:vault_id/credentials/:credential_id/archive', async (req, reply) => {
    const auth = req.auth!;
    const { vault_id: vaultId, credential_id: credentialId } = req.params as {
      vault_id: string;
      credential_id: string;
    };
    const vault = await loadActiveVault(db, auth.workspaceId, vaultId);
    if (!vault) return reply.code(404).send({ error: 'not found' });
    const row = await loadCredential(db, auth.workspaceId, vaultId, credentialId);
    if (!row || !isCredentialVisible(row, isOrcaBetaRequest(req.headers))) {
      return reply.code(404).send({ error: 'not found' });
    }

    const now = new Date();
    await db
      .update(vaultCredentials)
      .set({ archivedAt: row.archivedAt ?? now, updatedAt: now })
      .where(
        and(
          isNull(vaultCredentials.deletedAt),
          eq(vaultCredentials.id, credentialId),
          eq(vaultCredentials.vaultId, vaultId),
          eq(vaultCredentials.workspaceId, auth.workspaceId),
        ),
      );
    const out = await loadCredential(db, auth.workspaceId, vaultId, credentialId);
    await purgeCredentialSecrets(opts.secretStore, [row]).catch((err: unknown) => {
      req.log.warn({ err, credentialId }, 'failed to purge credential secrets after archive');
    });
    return reply.send(toCredentialApi(out!));
  });

  app.post(
    '/v1/vaults/:vault_id/credentials/:credential_id/mcp_oauth_validate',
    async (req, reply) => {
      const auth = req.auth!;
      const { vault_id: vaultId, credential_id: credentialId } = req.params as {
        vault_id: string;
        credential_id: string;
      };
      const vault = await loadActiveVault(db, auth.workspaceId, vaultId);
      if (!vault) return reply.code(404).send({ error: 'not found' });
      const row = await loadCredential(db, auth.workspaceId, vaultId, credentialId);
      // Archived credentials have had their secret payloads purged, so a live
      // refresh/probe diagnostic is meaningless — treat them as gone.
      if (!row || row.archivedAt || !isCredentialVisible(row, isOrcaBetaRequest(req.headers))) {
        return reply.code(404).send({ error: 'not found' });
      }
      if (row.authType !== 'mcp_oauth') {
        return reply.code(400).send({ error: 'credential is not mcp_oauth' });
      }
      if (!opts.secretStore) return reply.code(503).send({ error: 'secret store unavailable' });
      const secretStore = opts.secretStore;
      const fetchImpl = opts.fetchImpl ?? createGuardedFetch(globalThis.fetch);

      const refreshResult = await refreshCredentialWithLease(
        db,
        secretStore,
        secretStore,
        fetchImpl,
        req.log,
        row,
        auth.workspaceId,
        credentialId,
        [vaultId],
      );
      if (refreshResult.status === 'not_found') {
        return reply.code(404).send({ error: 'not found' });
      }
      if (refreshResult.status === 'conflict') {
        return reply.code(409).send({ error: 'credential was rotated concurrently' });
      }

      const probeToken = refreshResult.accessToken ?? '';
      const refresh: RefreshOutcome = {
        status:
          refreshResult.status === 'secret_unavailable' ? 'connect_error' : refreshResult.status,
        http_response: refreshResult.httpResponse,
      };

      const probeResponse = await probeMcpInitialize(fetchImpl, row.mcpServerUrl!, probeToken);

      let status: 'valid' | 'invalid' | 'unknown';
      if (refresh.status === 'failed') status = 'invalid';
      // Transient refresh breakage must surface even if the old access token
      // still probes 2xx — otherwise refresh failures stay hidden until that
      // token eventually expires.
      else if (refresh.status === 'connect_error') status = 'unknown';
      else if (probeResponse && probeResponse.status_code >= 200 && probeResponse.status_code < 300)
        status = 'valid';
      else if (
        probeResponse &&
        (probeResponse.status_code === 401 || probeResponse.status_code === 403)
      )
        status = 'invalid';
      else status = 'unknown';

      return reply.send({
        type: 'vault_credential_validation',
        credential_id: row.id,
        vault_id: row.vaultId,
        validated_at: new Date().toISOString(),
        has_refresh_token: row.refreshSecretRef !== null,
        status,
        mcp_probe: { method: 'initialize', http_response: probeResponse },
        refresh,
      });
    },
  );

  app.delete('/v1/vaults/:vault_id/credentials/:credential_id', async (req, reply) => {
    const auth = req.auth!;
    const { vault_id: vaultId, credential_id: credentialId } = req.params as {
      vault_id: string;
      credential_id: string;
    };
    const vault = await loadVault(db, auth.workspaceId, vaultId);
    if (!vault) return reply.code(404).send({ error: 'not found' });
    const row = await loadCredential(db, auth.workspaceId, vaultId, credentialId);
    if (!row || !isCredentialVisible(row, isOrcaBetaRequest(req.headers))) {
      return reply.code(404).send({ error: 'not found' });
    }

    await db
      .update(vaultCredentials)
      .set({ deletedAt: new Date() })
      .where(
        and(
          isNull(vaultCredentials.deletedAt),
          eq(vaultCredentials.id, credentialId),
          eq(vaultCredentials.vaultId, vaultId),
          eq(vaultCredentials.workspaceId, auth.workspaceId),
        ),
      );
    await purgeCredentialSecrets(opts.secretStore, [row]).catch((err: unknown) => {
      req.log.warn({ err, credentialId }, 'failed to purge credential secrets after delete');
    });
    return reply.send({ id: credentialId, type: 'vault_credential_deleted' });
  });
}

export function toCredentialApi(row: CredentialRow) {
  const authConfig = toCredentialAuthConfig(row.authConfig);
  const base = {
    id: row.id,
    type: 'vault_credential' as const,
    vault_id: row.vaultId,
    display_name: row.displayName ?? null,
    metadata: toStringMetadata(row.metadata),
    archived_at: row.archivedAt?.toISOString() ?? null,
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  };
  if (row.authType === 'static_bearer') {
    return {
      ...base,
      auth: { type: 'static_bearer' as const, mcp_server_url: row.mcpServerUrl },
    };
  }
  if (row.authType === 'environment_variable') {
    return {
      ...base,
      auth: {
        type: 'environment_variable' as const,
        secret_name: row.secretName!,
        networking: row.networking as EnvVarNetworking,
        injection_location: authConfig.injection_location ?? DEFAULT_INJECTION_LOCATION,
      },
    };
  }
  if (row.authType === 'provider') {
    return {
      ...base,
      auth: {
        type: 'provider' as const,
        provider: row.provider as ProviderCredentialProvider,
        scheme: row.scheme as ProviderCredentialScheme,
        logical_id: row.logicalId!,
        version: row.resolutionVersion!,
      },
    };
  }
  const auth = {
    type: 'mcp_oauth' as const,
    mcp_server_url: row.mcpServerUrl,
    ...(Object.prototype.hasOwnProperty.call(authConfig, 'expires_at')
      ? { expires_at: authConfig.expires_at ?? null }
      : {}),
    ...(row.tokenEndpoint !== null
      ? {
          refresh: {
            token_endpoint: row.tokenEndpoint,
            client_id: row.clientId!,
            token_endpoint_auth: { type: row.tokenEndpointAuthType! },
            ...(Object.prototype.hasOwnProperty.call(authConfig, 'resource')
              ? { resource: authConfig.resource ?? null }
              : {}),
            ...(Object.prototype.hasOwnProperty.call(authConfig, 'scope')
              ? { scope: authConfig.scope ?? null }
              : {}),
          },
        }
      : {}),
  };
  return {
    ...base,
    auth,
  };
}

export async function purgeCredentialSecrets(
  secretStore: SecretStore | undefined,
  rows: CredentialRow[],
): Promise<void> {
  if (!secretStore) return;
  const refs = new Set<string>();
  for (const row of rows) {
    refs.add(row.accessSecretRef);
    if (row.refreshSecretRef) refs.add(row.refreshSecretRef);
    if (row.clientSecretRef) refs.add(row.clientSecretRef);
  }
  await purgeSecretRefs(secretStore, [...refs]);
}

async function purgeSecretRefs(
  secretStore: SecretStore | undefined,
  refs: Array<string | null | undefined>,
): Promise<void> {
  if (!secretStore) return;
  const uniqueRefs = [...new Set(refs.filter((ref): ref is string => Boolean(ref)))];
  const results = await Promise.allSettled(uniqueRefs.map((ref) => secretStore.delete(ref)));
  const failures: unknown[] = [];
  for (const [index, result] of results.entries()) {
    if (result.status === 'rejected') {
      failures.push({ ref: uniqueRefs[index], cause: result.reason });
    }
  }
  if (failures.length > 0)
    throw new AggregateError(failures, 'failed to purge one or more secrets');
}

async function putStagedSecret(
  secretStore: SecretStore,
  stagedRefs: string[],
  ref: string,
  value: string,
): Promise<void> {
  stagedRefs.push(ref);
  await secretStore.put(ref, value);
}

export async function archiveActiveCredentialsForVault(
  db: CredentialWriteDb,
  workspaceId: string,
  vaultId: string,
  archivedAt: Date,
): Promise<CredentialRow[]> {
  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
        isNull(vaultCredentials.archivedAt),
      ),
    );
  if (rows.length === 0) return [];
  await db
    .update(vaultCredentials)
    .set({ archivedAt, updatedAt: archivedAt })
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
        isNull(vaultCredentials.archivedAt),
      ),
    );
  return rows;
}

export async function deleteCredentialsForVault(
  db: CredentialWriteDb,
  workspaceId: string,
  vaultId: string,
): Promise<CredentialRow[]> {
  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
      ),
    );
  if (rows.length === 0) return [];
  await db
    .update(vaultCredentials)
    .set({ deletedAt: new Date() })
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
      ),
    );
  return rows;
}

function parseCredentialListQuery(input: unknown): ValidationResult<CredentialListPage> {
  if (!isObject(input)) return { ok: true, value: { limit: 100, includeArchived: false } };
  const rawLimit = input['limit'];
  let limit = 100;
  if (rawLimit !== undefined) {
    const parsed =
      typeof rawLimit === 'number'
        ? rawLimit
        : typeof rawLimit === 'string'
          ? Number(rawLimit)
          : NaN;
    if (!Number.isInteger(parsed) || parsed < 1) return invalid('limit must be a positive integer');
    limit = Math.min(parsed, 100);
  }

  const rawIncludeArchived = input['include_archived'];
  let includeArchived = false;
  if (rawIncludeArchived !== undefined) {
    if (typeof rawIncludeArchived === 'boolean') includeArchived = rawIncludeArchived;
    else if (rawIncludeArchived === 'true') includeArchived = true;
    else if (rawIncludeArchived === 'false') includeArchived = false;
    else return invalid('include_archived must be a boolean');
  }

  const rawPage = input['page'];
  if (rawPage === undefined || rawPage === '') {
    return { ok: true, value: { limit, includeArchived } };
  }
  if (typeof rawPage !== 'string') return invalid('page must be a string');
  const page = decodeCredentialCursor(rawPage);
  if (!page) return invalid('invalid page');
  return { ok: true, value: { limit, includeArchived, page } };
}

function encodeCredentialCursor(row: CredentialRow): string {
  return `${row.createdAt.toISOString()}|${row.id}`;
}

function decodeCredentialCursor(cursor: string): CredentialCursor | null {
  const sep = cursor.lastIndexOf('|');
  if (sep <= 0 || sep === cursor.length - 1) return null;
  const createdAt = new Date(cursor.slice(0, sep));
  if (Number.isNaN(createdAt.getTime())) return null;
  return { createdAt, id: cursor.slice(sep + 1) };
}

function validateCreateBody(input: unknown): ValidationResult<CredentialCreateBody> {
  if (!isObject(input)) return invalid('invalid body');
  const displayName = readOptionalNullableString(input, 'display_name');
  if (!displayName.ok) return invalid(displayName.error!);
  const metadata = readCreateMetadata(input);
  if (!metadata.ok) return invalid(metadata.error!);
  const auth = validateCreateAuth(input['auth']);
  if (!auth.ok) return invalid(auth.error!);
  return {
    ok: true,
    value: { display_name: displayName.value, metadata: metadata.value!, auth: auth.value! },
  };
}

function validateCreateAuth(input: unknown): ValidationResult<CredentialCreateAuth> {
  if (!isObject(input)) return invalid('auth is required');
  const type = input['type'];
  if (type === 'static_bearer') {
    const token = readRequiredString(input, 'token');
    if (!token.ok) return invalid(token.error!);
    const mcpServerUrl = readRequiredUrlString(input, 'mcp_server_url');
    if (!mcpServerUrl.ok) return invalid(mcpServerUrl.error!);
    return { ok: true, value: { type, token: token.value!, mcp_server_url: mcpServerUrl.value! } };
  }
  if (type === 'mcp_oauth') {
    const accessToken = readRequiredString(input, 'access_token');
    if (!accessToken.ok) return invalid(accessToken.error!);
    const mcpServerUrl = readRequiredUrlString(input, 'mcp_server_url');
    if (!mcpServerUrl.ok) return invalid(mcpServerUrl.error!);
    const expiresAt = readOptionalNullableTimestamp(input, 'expires_at');
    if (!expiresAt.ok) return invalid(expiresAt.error!);
    if (!('refresh' in input)) {
      return {
        ok: true,
        value: {
          type,
          access_token: accessToken.value!,
          mcp_server_url: mcpServerUrl.value!,
          expires_at: expiresAt.value ?? null,
          refresh: null,
        },
      };
    }
    if (input['refresh'] === null) {
      return {
        ok: true,
        value: {
          type,
          access_token: accessToken.value!,
          mcp_server_url: mcpServerUrl.value!,
          expires_at: expiresAt.value ?? null,
          refresh: null,
        },
      };
    }
    if (!isObject(input['refresh'])) return invalid('auth.refresh must be an object');
    const refresh = input['refresh'];
    const refreshToken = readRequiredString(refresh, 'refresh_token');
    if (!refreshToken.ok) return invalid(refreshToken.error!);
    // token_endpoint is dialed outbound by mcp_oauth_validate, so constrain
    // it to http(s) at write time just like mcp_server_url.
    const tokenEndpoint = readRequiredUrlString(refresh, 'token_endpoint');
    if (!tokenEndpoint.ok) return invalid(tokenEndpoint.error!);
    const clientId = readRequiredString(refresh, 'client_id');
    if (!clientId.ok) return invalid(clientId.error!);
    const endpointAuth = validateTokenEndpointAuthCreate(refresh['token_endpoint_auth']);
    if (!endpointAuth.ok) return invalid(endpointAuth.error!);
    const resource = readOptionalNullableStringValue(refresh, 'resource');
    if (!resource.ok) return invalid(resource.error!);
    const scope = readOptionalNullableStringValue(refresh, 'scope');
    if (!scope.ok) return invalid(scope.error!);
    return {
      ok: true,
      value: {
        type,
        access_token: accessToken.value!,
        mcp_server_url: mcpServerUrl.value!,
        expires_at: expiresAt.value ?? null,
        refresh: {
          refresh_token: refreshToken.value!,
          token_endpoint: tokenEndpoint.value!,
          client_id: clientId.value!,
          token_endpoint_auth: endpointAuth.value!,
          resource: resource.value ?? null,
          scope: scope.value ?? null,
        },
      },
    };
  }
  if (type === 'environment_variable') {
    const secretName = readRequiredString(input, 'secret_name');
    if (!secretName.ok) return invalid(secretName.error!);
    if (secretName.value!.length > 255) return invalid('secret_name must be 1-255 characters');
    const secretValue = readRequiredString(input, 'secret_value');
    if (!secretValue.ok) return invalid(secretValue.error!);
    const networking = validateNetworking(input['networking']);
    if (!networking.ok) return invalid(networking.error!);
    const injectionLocation = validateInjectionLocation(input['injection_location'], false);
    if (!injectionLocation.ok) return invalid(injectionLocation.error!);
    return {
      ok: true,
      value: {
        type,
        secret_name: secretName.value!,
        secret_value: secretValue.value!,
        networking: networking.value!,
        injection_location: injectionLocation.value!,
      },
    };
  }
  if (type === 'provider') {
    const provider = input['provider'];
    if (!isProviderCredentialProvider(provider)) {
      return invalid('auth.provider is not supported');
    }
    const scheme = input['scheme'];
    if (!isProviderCredentialScheme(scheme)) {
      return invalid('auth.scheme is not supported');
    }
    if (!isCompatibleProviderCredentialScheme(provider, scheme)) {
      return invalid(`auth.scheme ${scheme} is not compatible with provider ${provider}`);
    }
    const logicalId = readRequiredString(input, 'logical_id');
    if (!logicalId.ok) return invalid(logicalId.error!);
    if (!isLogicalCredentialId(logicalId.value!)) {
      return invalid(`auth.logical_id must match ${LOGICAL_CREDENTIAL_ID_PATTERN.source}`);
    }
    const secretValue = readRequiredString(input, 'secret_value');
    if (!secretValue.ok) return invalid(secretValue.error!);
    return {
      ok: true,
      value: {
        type,
        provider,
        scheme,
        logical_id: logicalId.value!,
        secret_value: secretValue.value!,
      },
    };
  }
  return invalid('invalid auth.type');
}

function validateNetworking(input: unknown): ValidationResult<EnvVarNetworking> {
  if (!isObject(input)) return invalid('auth.networking is required');
  const type = input['type'];
  if (type === 'unrestricted') {
    return { ok: true, value: { type: 'unrestricted' } };
  }
  if (type === 'limited') {
    const hosts = input['allowed_hosts'];
    if (!Array.isArray(hosts) || hosts.length > 16) {
      return invalid('auth.networking.allowed_hosts must contain at most 16 hosts');
    }
    if (!hosts.every((host) => typeof host === 'string' && isAllowedCredentialHost(host))) {
      return invalid(
        'auth.networking.allowed_hosts entries must be bare hostnames, IPv4 addresses, or *. wildcards',
      );
    }
    return { ok: true, value: { type: 'limited', allowed_hosts: hosts as string[] } };
  }
  return invalid('auth.networking.type must be limited or unrestricted');
}

function validateInjectionLocation(
  input: unknown,
  merge: boolean,
  current: InjectionLocation = DEFAULT_INJECTION_LOCATION,
): ValidationResult<InjectionLocation> {
  if (input === undefined && !merge) {
    return { ok: true, value: { ...DEFAULT_INJECTION_LOCATION } };
  }
  if (!isObject(input)) return invalid('auth.injection_location must be an object');
  const header = input['header'];
  const body = input['body'];
  if (header !== undefined && typeof header !== 'boolean') {
    return invalid('auth.injection_location.header must be a boolean');
  }
  if (body !== undefined && typeof body !== 'boolean') {
    return invalid('auth.injection_location.body must be a boolean');
  }
  const value = {
    header: typeof header === 'boolean' ? header : merge ? current.header : false,
    body: typeof body === 'boolean' ? body : merge ? current.body : false,
  };
  if (!value.header && !value.body) {
    return invalid('auth.injection_location must enable header or body');
  }
  return { ok: true, value };
}

function validateTokenEndpointAuthCreate(
  input: unknown,
): ValidationResult<
  | { type: 'none' }
  | { type: 'client_secret_basic'; client_secret: string }
  | { type: 'client_secret_post'; client_secret: string }
> {
  if (!isObject(input)) return invalid('auth.refresh.token_endpoint_auth is required');
  const type = input['type'];
  if (type === 'none') return { ok: true, value: { type } };
  if (type !== 'client_secret_basic' && type !== 'client_secret_post') {
    return invalid(
      'auth.refresh.token_endpoint_auth.type must be none, client_secret_basic, or client_secret_post',
    );
  }
  const secret = readRequiredString(input, 'client_secret');
  if (!secret.ok) return invalid('auth.refresh.token_endpoint_auth.client_secret is required');
  return { ok: true, value: { type, client_secret: secret.value! } };
}

function isAllowedCredentialHost(value: string): boolean {
  if (
    value.length < 1 ||
    value.length > 253 ||
    /\s/.test(value) ||
    value.includes('/') ||
    value.includes(':') ||
    value.includes('[') ||
    value.includes(']')
  ) {
    return false;
  }
  const host = value.startsWith('*.') ? value.slice(2) : value;
  if (!host || host.startsWith('*.')) return false;
  if (/^\d+(?:\.\d+){3}$/.test(host)) {
    if (value.startsWith('*.')) return false;
    return host.split('.').every((part) => Number(part) >= 0 && Number(part) <= 255);
  }
  return host
    .split('.')
    .every(
      (label) =>
        label.length >= 1 &&
        label.length <= 63 &&
        /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/.test(label),
    );
}

function validateUpdateBody(input: unknown): ValidationResult<CredentialUpdateBody> {
  if (!isObject(input)) return invalid('invalid body');
  const displayName = readOptionalNullableString(input, 'display_name');
  if (!displayName.ok) return invalid(displayName.error!);
  const metadata = readMetadataPatch(input);
  if (!metadata.ok) return invalid(metadata.error!);
  if ('auth' in input && !isObject(input['auth'])) return invalid('auth must be an object');
  return {
    ok: true,
    value: {
      ...('display_name' in input ? { display_name: displayName.value } : {}),
      ...(metadata.value !== undefined ? { metadata: metadata.value } : {}),
      ...('auth' in input ? { auth: input['auth'] as Record<string, unknown> } : {}),
    },
  };
}

interface SecretRotationPlan {
  accessToken?: string;
  refreshToken?: string;
  clientSecret?: string;
  clearRefresh?: boolean;
  rowUpdates: Partial<typeof vaultCredentials.$inferInsert>;
}

function requiresSecretStore(plan: SecretRotationPlan): boolean {
  return (
    plan.accessToken !== undefined ||
    plan.refreshToken !== undefined ||
    plan.clientSecret !== undefined ||
    plan.clearRefresh === true
  );
}

function supersedesRefreshLease(plan: SecretRotationPlan): boolean {
  return plan.refreshToken !== undefined || plan.clearRefresh === true;
}

function validateAuthUpdate(
  row: CredentialRow,
  input: Record<string, unknown>,
): ValidationResult<SecretRotationPlan> {
  if ('type' in input && input['type'] !== row.authType) return invalid('auth.type is immutable');
  if ('mcp_server_url' in input && input['mcp_server_url'] !== row.mcpServerUrl) {
    return invalid('mcp_server_url is immutable');
  }

  const plan: SecretRotationPlan = { rowUpdates: {} };
  if (row.authType === 'static_bearer') {
    const token = readOptionalNullableSecret(input, 'token');
    if (!token.ok) return invalid(token.error!);
    if (typeof token.value === 'string') plan.accessToken = token.value;
    return { ok: true, value: plan };
  }

  if (row.authType === 'environment_variable') {
    if ('secret_name' in input && input['secret_name'] !== row.secretName) {
      return invalid('secret_name is immutable');
    }
    if ('networking' in input && input['networking'] !== null) {
      const networking = validateNetworking(input['networking']);
      if (!networking.ok) return invalid(networking.error!);
      plan.rowUpdates.networking = networking.value!;
    }
    if ('injection_location' in input) {
      const current =
        toCredentialAuthConfig(row.authConfig).injection_location ?? DEFAULT_INJECTION_LOCATION;
      const injection = validateInjectionLocation(input['injection_location'], true, current);
      if (!injection.ok) return invalid(injection.error!);
      plan.rowUpdates.authConfig = {
        ...toCredentialAuthConfig(row.authConfig),
        injection_location: injection.value!,
      };
    }
    // secret_value rotates into the existing access ref slot.
    const secretValue = readOptionalNullableSecret(input, 'secret_value');
    if (!secretValue.ok) return invalid(secretValue.error!);
    if (typeof secretValue.value === 'string') plan.accessToken = secretValue.value;
    return { ok: true, value: plan };
  }

  if (row.authType === 'provider') {
    if ('provider' in input && input['provider'] !== row.provider) {
      return invalid('provider is immutable');
    }
    if ('scheme' in input && input['scheme'] !== row.scheme) {
      return invalid('scheme is immutable');
    }
    const logicalId = readOptionalString(input, 'logical_id');
    if (!logicalId.ok) return invalid(logicalId.error!);
    if (logicalId.value !== undefined && !isLogicalCredentialId(logicalId.value)) {
      return invalid(`logical_id must match ${LOGICAL_CREDENTIAL_ID_PATTERN.source}`);
    }
    if (logicalId.value !== undefined && logicalId.value !== row.logicalId) {
      plan.rowUpdates.logicalId = logicalId.value;
    }
    const secretValue = readOptionalNullableSecret(input, 'secret_value');
    if (!secretValue.ok) return invalid(secretValue.error!);
    if (typeof secretValue.value === 'string') plan.accessToken = secretValue.value;
    if (plan.rowUpdates.logicalId !== undefined || plan.accessToken !== undefined) {
      plan.rowUpdates.resolutionVersion = randomUUID();
    }
    return { ok: true, value: plan };
  }

  const accessToken = readOptionalNullableSecret(input, 'access_token');
  if (!accessToken.ok) return invalid(accessToken.error!);
  if (typeof accessToken.value === 'string') plan.accessToken = accessToken.value;
  const authConfig = toCredentialAuthConfig(row.authConfig);
  const expiresAt = readOptionalNullableTimestamp(input, 'expires_at');
  if (!expiresAt.ok) return invalid(expiresAt.error!);
  if ('expires_at' in input) {
    plan.rowUpdates.authConfig = { ...authConfig, expires_at: expiresAt.value ?? null };
  }
  if ('refresh' in input) {
    if (input['refresh'] === null) {
      plan.clearRefresh = true;
      plan.rowUpdates.authConfig = {
        ...(plan.rowUpdates.authConfig as CredentialAuthConfig | undefined),
        ...authConfig,
        resource: null,
        scope: null,
        ...('expires_at' in input ? { expires_at: expiresAt.value ?? null } : {}),
      };
      return { ok: true, value: plan };
    }
    if (!isObject(input['refresh'])) return invalid('auth.refresh must be an object');
    const refresh = input['refresh'];
    const refreshToken = readOptionalNullableSecret(refresh, 'refresh_token');
    if (!refreshToken.ok) return invalid(refreshToken.error!);
    if (typeof refreshToken.value === 'string') {
      if (
        row.tokenEndpoint === null ||
        row.clientId === null ||
        row.tokenEndpointAuthType === null
      ) {
        return invalid(
          'auth.refresh cannot add a refresh token without existing refresh configuration',
        );
      }
      plan.refreshToken = refreshToken.value;
    }
    const tokenEndpoint = readOptionalString(refresh, 'token_endpoint');
    if (!tokenEndpoint.ok) return invalid(tokenEndpoint.error!);
    if (tokenEndpoint.value !== undefined && tokenEndpoint.value !== row.tokenEndpoint) {
      return invalid('token_endpoint is immutable');
    }
    const clientId = readOptionalString(refresh, 'client_id');
    if (!clientId.ok) return invalid(clientId.error!);
    if (clientId.value !== undefined && clientId.value !== row.clientId) {
      return invalid('client_id is immutable');
    }
    const scope = readOptionalNullableStringValue(refresh, 'scope');
    if (!scope.ok) return invalid(scope.error!);
    if ('scope' in refresh) {
      plan.rowUpdates.authConfig = {
        ...authConfig,
        ...(plan.rowUpdates.authConfig as CredentialAuthConfig | undefined),
        scope: scope.value ?? null,
      };
    }
    if ('token_endpoint_auth' in refresh) {
      if (!isObject(refresh['token_endpoint_auth'])) {
        return invalid('auth.refresh.token_endpoint_auth must be an object');
      }
      const endpointAuth = refresh['token_endpoint_auth'];
      const endpointAuthType = endpointAuth['type'];
      if (endpointAuthType !== 'client_secret_basic' && endpointAuthType !== 'client_secret_post') {
        return invalid(
          'auth.refresh.token_endpoint_auth.type must be client_secret_basic or client_secret_post',
        );
      }
      plan.rowUpdates.tokenEndpointAuthType = endpointAuthType;
      const clientSecret = readOptionalNullableSecret(endpointAuth, 'client_secret');
      if (!clientSecret.ok) return invalid(clientSecret.error!);
      if (clientSecret.value === null) {
        return invalid(
          'auth.refresh.token_endpoint_auth.client_secret cannot be null for client authentication',
        );
      }
      if (
        (row.tokenEndpointAuthType === 'none' || row.clientSecretRef === null) &&
        typeof clientSecret.value !== 'string'
      ) {
        return invalid(
          'auth.refresh.token_endpoint_auth.client_secret is required when enabling client authentication',
        );
      }
      if (typeof clientSecret.value === 'string') plan.clientSecret = clientSecret.value;
    }
  }
  return { ok: true, value: plan };
}

interface StagedSecretRotation {
  rowUpdates: Partial<typeof vaultCredentials.$inferInsert>;
  oldRefs: string[];
}

async function stageSecretRotations(
  secretStore: SecretStore,
  row: CredentialRow,
  plan: SecretRotationPlan,
  stagedRefs: string[],
): Promise<StagedSecretRotation> {
  const refs = credentialRotationSecretRefs(row.workspaceId, row.id, newId('vsec'));
  const rotation: StagedSecretRotation = { rowUpdates: {}, oldRefs: [] };
  if (plan.accessToken !== undefined) {
    await putStagedSecret(secretStore, stagedRefs, refs.access, plan.accessToken);
    rotation.oldRefs.push(row.accessSecretRef);
    rotation.rowUpdates.accessSecretRef = refs.access;
  }
  if (plan.refreshToken !== undefined) {
    await putStagedSecret(secretStore, stagedRefs, refs.refresh, plan.refreshToken);
    if (row.refreshSecretRef) rotation.oldRefs.push(row.refreshSecretRef);
    rotation.rowUpdates.refreshSecretRef = refs.refresh;
  }
  if (plan.clientSecret !== undefined) {
    await putStagedSecret(secretStore, stagedRefs, refs.clientSecret, plan.clientSecret);
    if (row.clientSecretRef) rotation.oldRefs.push(row.clientSecretRef);
    rotation.rowUpdates.clientSecretRef = refs.clientSecret;
  }
  if (plan.clearRefresh) {
    if (row.refreshSecretRef) rotation.oldRefs.push(row.refreshSecretRef);
    if (row.clientSecretRef) rotation.oldRefs.push(row.clientSecretRef);
    rotation.rowUpdates.refreshSecretRef = null;
    rotation.rowUpdates.tokenEndpoint = null;
    rotation.rowUpdates.clientId = null;
    rotation.rowUpdates.tokenEndpointAuthType = null;
    rotation.rowUpdates.clientSecretRef = null;
  }
  if (supersedesRefreshLease(plan)) {
    // Replacing or clearing the refresh token makes it safe to supersede an
    // in-flight refresh. The holder's pointer/owner CAS then fails without
    // leaving the database pointed at the consumed refresh token.
    rotation.rowUpdates.oauthRefreshLeaseOwner = null;
    rotation.rowUpdates.oauthRefreshLeaseExpiresAt = null;
  }
  return rotation;
}

const OUTBOUND_TIMEOUT_MS = 10_000;
const MAX_CAPTURED_BODY_CHARS = 2048;
const OAUTH_REFRESH_LEASE_TTL_MS = 30_000;
const OAUTH_REFRESH_WAIT_TIMEOUT_MS = 15_000;
const OAUTH_REFRESH_WAIT_INITIAL_MS = 25;
const OAUTH_REFRESH_WAIT_MAX_MS = 250;
const PERMANENT_SLACK_REFRESH_ERRORS: ReadonlySet<string> = new Set([
  'bad_client_secret',
  'invalid_auth',
  'invalid_client_id',
  'invalid_grant',
  'invalid_grant_type',
  'invalid_refresh_token',
  'not_authed',
  'token_expired',
  'token_revoked',
]);
// exchangeRefreshToken needs the decoded body to JSON.parse access_token /
// refresh_token out of it, not just to display it. A real token response
// (access_token + refresh_token + token_type + expires_in + scope) is well
// under 2KB, but give it a bit more headroom than the display cap so a
// slightly larger-than-typical response still parses; a token endpoint that
// returns more than this is treated as unsupported (falls back to
// status: 'connect_error', same as a JSON.parse failure today).
const MAX_REFRESH_PARSE_CHARS = 8192;

export interface CapturedHttpResponse {
  status_code: number;
  content_type: string;
  body: string;
  body_truncated: boolean;
}

interface RefreshOutcome {
  status: 'succeeded' | 'connect_error' | 'failed' | 'no_refresh_token';
  http_response: CapturedHttpResponse | null;
}

export interface RefreshExchangeResult {
  status: 'succeeded' | 'connect_error' | 'failed';
  httpResponse: CapturedHttpResponse | null;
  newAccessToken?: string;
  newRefreshToken?: string;
  newExpiresAt?: string | null;
}

export interface CredentialRefreshLeaseTiming {
  leaseTtlMs: number;
  heartbeatIntervalMs: number;
  waitTimeoutMs: number;
  waitInitialMs: number;
  waitMaxMs: number;
}

export interface CredentialRefreshResult {
  status:
    | 'succeeded'
    | 'not_found'
    | 'no_refresh_token'
    | 'secret_unavailable'
    | 'connect_error'
    | 'failed'
    | 'conflict';
  accessToken: string | null;
  httpResponse: CapturedHttpResponse | null;
}

/**
 * Serializes every rotating OAuth refresh entry point through one Postgres
 * lease. The UUID owner is also the persistence fencing token: a stale holder
 * cannot swap secret refs after another replica takes over. A heartbeat keeps
 * the lease alive while secret resolution and staged SecretStore writes are in
 * flight, both of which may outlive the bounded token-endpoint request.
 */
export async function refreshCredentialWithLease(
  db: DbClient,
  secretStore: SecretStore,
  secrets: SecretProvider,
  fetchImpl: typeof fetch,
  log: FastifyBaseLogger,
  initialRow: CredentialRow,
  workspaceId: string,
  credentialId: string,
  vaultIds: string[],
  timingOverrides: Partial<CredentialRefreshLeaseTiming> = {},
): Promise<CredentialRefreshResult> {
  const timing = resolveCredentialRefreshLeaseTiming(timingOverrides);
  const leaseOwner = randomUUID();
  const waitDeadline = Date.now() + timing.waitTimeoutMs;
  let waitMs = timing.waitInitialMs;
  let row: CredentialRow | null = initialRow;

  while (true) {
    if (row.accessSecretRef !== initialRow.accessSecretRef) {
      const winner = await secrets.resolve(row.accessSecretRef);
      return winner === null
        ? { status: 'not_found', accessToken: null, httpResponse: null }
        : { status: 'succeeded', accessToken: winner, httpResponse: null };
    }
    if (row.refreshSecretRef !== initialRow.refreshSecretRef) {
      return { status: 'conflict', accessToken: null, httpResponse: null };
    }
    if (!row.refreshSecretRef) {
      return {
        status: 'no_refresh_token',
        accessToken: await secrets.resolve(row.accessSecretRef),
        httpResponse: null,
      };
    }

    const claimed = await claimCredentialRefreshLease(
      db,
      workspaceId,
      credentialId,
      vaultIds,
      initialRow,
      leaseOwner,
      timing.leaseTtlMs,
    );
    if (claimed) {
      row = claimed;
      break;
    }

    row = await loadActiveRefreshCredential(db, workspaceId, credentialId, vaultIds);
    if (!row) return { status: 'not_found', accessToken: null, httpResponse: null };
    if (row.accessSecretRef !== initialRow.accessSecretRef) {
      const winner = await secrets.resolve(row.accessSecretRef);
      return winner === null
        ? { status: 'not_found', accessToken: null, httpResponse: null }
        : { status: 'succeeded', accessToken: winner, httpResponse: null };
    }
    if (row.refreshSecretRef !== initialRow.refreshSecretRef) {
      return { status: 'conflict', accessToken: null, httpResponse: null };
    }
    if (!row.refreshSecretRef) {
      return {
        status: 'no_refresh_token',
        accessToken: await secrets.resolve(row.accessSecretRef),
        httpResponse: null,
      };
    }
    if (Date.now() >= waitDeadline) {
      return {
        status: 'connect_error',
        accessToken: await secrets.resolve(row.accessSecretRef),
        httpResponse: null,
      };
    }
    await new Promise((resolve) => setTimeout(resolve, waitMs));
    waitMs = Math.min(waitMs * 2, timing.waitMaxMs);
  }

  // `claimCredentialRefreshLease` only claims a row whose `refresh_secret_ref`
  // still equals `initialRow.refreshSecretRef` (its WHERE clause), which the
  // loop above already proved truthy — so this is unreachable in practice.
  // TS can't see that DB-level invariant across the loop/reassignment, so
  // guard explicitly (mirrors the two identical checks above) rather than
  // asserting past a `string | null`.
  if (!row.refreshSecretRef) {
    return {
      status: 'no_refresh_token',
      accessToken: await secrets.resolve(row.accessSecretRef),
      httpResponse: null,
    };
  }

  const stopHeartbeat = startCredentialRefreshLeaseHeartbeat(
    db,
    log,
    row,
    workspaceId,
    credentialId,
    vaultIds,
    leaseOwner,
    timing,
  );
  try {
    const [accessToken, refreshToken, clientSecret] = await Promise.all([
      secrets.resolve(row.accessSecretRef),
      secrets.resolve(row.refreshSecretRef),
      row.clientSecretRef ? secrets.resolve(row.clientSecretRef) : Promise.resolve(null),
    ]);
    if (accessToken === null) {
      return { status: 'not_found', accessToken: null, httpResponse: null };
    }
    if (refreshToken === null || (row.clientSecretRef !== null && clientSecret === null)) {
      return { status: 'secret_unavailable', accessToken, httpResponse: null };
    }

    const exchange = await exchangeRefreshToken(
      fetchImpl,
      row,
      refreshToken,
      clientSecret,
      accessToken,
    );
    if (exchange.status !== 'succeeded' || exchange.newAccessToken === undefined) {
      return {
        status: exchange.status,
        accessToken,
        httpResponse: exchange.httpResponse,
      };
    }

    const persisted = await persistRotatedTokens(
      db,
      secretStore,
      log,
      row,
      exchange.newAccessToken,
      exchange.newRefreshToken,
      exchange.newExpiresAt,
      { refreshLeaseOwner: leaseOwner },
    );
    if (persisted) {
      return {
        status: 'succeeded',
        accessToken: exchange.newAccessToken,
        httpResponse: exchange.httpResponse,
      };
    }

    const current = await loadActiveRefreshCredential(db, workspaceId, credentialId, vaultIds);
    if (!current) return { status: 'not_found', accessToken: null, httpResponse: null };
    if (current.accessSecretRef !== row.accessSecretRef) {
      return {
        status: 'conflict',
        accessToken: await secrets.resolve(current.accessSecretRef),
        httpResponse: exchange.httpResponse,
      };
    }
    return { status: 'conflict', accessToken: null, httpResponse: exchange.httpResponse };
  } finally {
    await stopHeartbeat();
    await releaseCredentialRefreshLease(db, workspaceId, credentialId, leaseOwner).catch(
      (err: unknown) => {
        log.warn({ err, credentialId }, 'failed to release OAuth refresh lease');
      },
    );
  }
}

function resolveCredentialRefreshLeaseTiming(
  overrides: Partial<CredentialRefreshLeaseTiming>,
): CredentialRefreshLeaseTiming {
  const leaseTtlMs = positiveMilliseconds(overrides.leaseTtlMs, OAUTH_REFRESH_LEASE_TTL_MS);
  const defaultHeartbeatMs = Math.max(1, Math.floor(leaseTtlMs / 3));
  const heartbeatIntervalMs = Math.min(
    positiveMilliseconds(overrides.heartbeatIntervalMs, defaultHeartbeatMs),
    Math.max(1, leaseTtlMs - 1),
  );
  return {
    leaseTtlMs,
    heartbeatIntervalMs,
    waitTimeoutMs: positiveMilliseconds(overrides.waitTimeoutMs, OAUTH_REFRESH_WAIT_TIMEOUT_MS),
    waitInitialMs: positiveMilliseconds(overrides.waitInitialMs, OAUTH_REFRESH_WAIT_INITIAL_MS),
    waitMaxMs: positiveMilliseconds(overrides.waitMaxMs, OAUTH_REFRESH_WAIT_MAX_MS),
  };
}

function positiveMilliseconds(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isFinite(value) && value > 0
    ? Math.max(1, Math.floor(value))
    : fallback;
}

function startCredentialRefreshLeaseHeartbeat(
  db: DbClient,
  log: FastifyBaseLogger,
  row: CredentialRow,
  workspaceId: string,
  credentialId: string,
  vaultIds: string[],
  leaseOwner: string,
  timing: CredentialRefreshLeaseTiming,
): () => Promise<void> {
  let stopped = false;
  let renewal: Promise<void> | null = null;
  const timer = setInterval(() => {
    if (stopped || renewal !== null) return;
    renewal = renewCredentialRefreshLease(
      db,
      workspaceId,
      credentialId,
      vaultIds,
      row,
      leaseOwner,
      timing.leaseTtlMs,
    )
      .then((renewed) => {
        if (!renewed) {
          stopped = true;
          clearInterval(timer);
        }
      })
      .catch((err: unknown) => {
        // Persistence remains fenced by leaseOwner even if one heartbeat fails.
        // Keep retrying while DB availability recovers.
        log.warn({ err, credentialId }, 'failed to renew OAuth refresh lease');
      })
      .finally(() => {
        renewal = null;
      });
  }, timing.heartbeatIntervalMs);
  timer.unref();

  return async () => {
    stopped = true;
    clearInterval(timer);
    if (renewal !== null) await renewal;
  };
}

async function loadActiveRefreshCredential(
  db: CredentialReadDb,
  workspaceId: string,
  credentialId: string,
  vaultIds: string[],
): Promise<CredentialRow | null> {
  if (vaultIds.length === 0) return null;
  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.id, credentialId),
        inArray(vaultCredentials.vaultId, vaultIds),
        eq(vaultCredentials.authType, 'mcp_oauth'),
        isNull(vaultCredentials.archivedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function claimCredentialRefreshLease(
  db: DbClient,
  workspaceId: string,
  credentialId: string,
  vaultIds: string[],
  initialRow: CredentialRow,
  leaseOwner: string,
  leaseTtlMs: number,
): Promise<CredentialRow | null> {
  if (vaultIds.length === 0) return null;
  const rows = await db
    .update(vaultCredentials)
    .set({
      oauthRefreshLeaseOwner: leaseOwner,
      oauthRefreshLeaseExpiresAt: sql`now() + ${leaseTtlMs} * interval '1 millisecond'`,
    })
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.id, credentialId),
        inArray(vaultCredentials.vaultId, vaultIds),
        isNull(vaultCredentials.archivedAt),
        eq(vaultCredentials.authType, 'mcp_oauth'),
        eq(vaultCredentials.accessSecretRef, initialRow.accessSecretRef),
        initialRow.refreshSecretRef
          ? eq(vaultCredentials.refreshSecretRef, initialRow.refreshSecretRef)
          : isNull(vaultCredentials.refreshSecretRef),
        or(
          isNull(vaultCredentials.oauthRefreshLeaseOwner),
          isNull(vaultCredentials.oauthRefreshLeaseExpiresAt),
          sql`${vaultCredentials.oauthRefreshLeaseExpiresAt} <= now()`,
        ),
      ),
    )
    .returning();
  return rows[0] ?? null;
}

async function renewCredentialRefreshLease(
  db: DbClient,
  workspaceId: string,
  credentialId: string,
  vaultIds: string[],
  row: CredentialRow,
  leaseOwner: string,
  leaseTtlMs: number,
): Promise<boolean> {
  if (vaultIds.length === 0) return false;
  const renewed = await db
    .update(vaultCredentials)
    .set({
      oauthRefreshLeaseExpiresAt: sql`now() + ${leaseTtlMs} * interval '1 millisecond'`,
    })
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.id, credentialId),
        inArray(vaultCredentials.vaultId, vaultIds),
        isNull(vaultCredentials.archivedAt),
        eq(vaultCredentials.authType, 'mcp_oauth'),
        eq(vaultCredentials.accessSecretRef, row.accessSecretRef),
        row.refreshSecretRef
          ? eq(vaultCredentials.refreshSecretRef, row.refreshSecretRef)
          : isNull(vaultCredentials.refreshSecretRef),
        eq(vaultCredentials.oauthRefreshLeaseOwner, leaseOwner),
      ),
    )
    .returning({ id: vaultCredentials.id });
  return renewed.length > 0;
}

async function releaseCredentialRefreshLease(
  db: DbClient,
  workspaceId: string,
  credentialId: string,
  leaseOwner: string,
): Promise<void> {
  await db
    .update(vaultCredentials)
    .set({ oauthRefreshLeaseOwner: null, oauthRefreshLeaseExpiresAt: null })
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.id, credentialId),
        eq(vaultCredentials.oauthRefreshLeaseOwner, leaseOwner),
      ),
    );
}

/**
 * Reads at most `maxChars` decoded characters from a Response body and
 * cancels the stream once that much has been read, so a hostile or huge
 * upstream body is never fully buffered — at most `maxChars` plus one chunk
 * is ever held in memory. TextDecoder is fed with `stream: true` so a
 * multi-byte UTF-8 sequence split across chunk boundaries decodes correctly
 * instead of producing replacement characters.
 */
export async function readBoundedText(
  res: Response,
  maxChars: number,
): Promise<{ text: string; truncated: boolean }> {
  const body = res.body;
  if (!body) {
    // No readable stream (e.g. a 204/304 response, or a test double that
    // only implements res.text()). Bodies here are expected to be empty.
    let text = '';
    try {
      text = await res.text();
    } catch {
      return { text: '', truncated: false };
    }
    return { text: text.slice(0, maxChars), truncated: text.length > maxChars };
  }
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let text = '';
  try {
    while (text.length <= maxChars) {
      const { done, value } = await reader.read();
      if (done) break;
      text += decoder.decode(value, { stream: true });
    }
    // Flush any UTF-8 bytes TextDecoder is still holding onto because the
    // final chunk ended mid-sequence — otherwise those bytes are silently
    // dropped instead of appearing as replacement characters or text.
    text += decoder.decode();
  } catch {
    return { text: '', truncated: false };
  } finally {
    // Release the underlying connection instead of draining/buffering
    // whatever the upstream still wants to send.
    await reader.cancel().catch(() => {});
  }
  const truncated = text.length > maxChars;
  return { text: truncated ? text.slice(0, maxChars) : text, truncated };
}

async function captureHttpResponse(
  res: Response,
  maxParseChars: number = MAX_CAPTURED_BODY_CHARS,
): Promise<{ capture: CapturedHttpResponse; text: string }> {
  const { text, truncated } = await readBoundedText(res, maxParseChars);
  // body/body_truncated always reflect the (smaller) display cap, even when
  // the caller asked for more text to parse — see MAX_REFRESH_PARSE_CHARS.
  const displayTruncated = truncated || text.length > MAX_CAPTURED_BODY_CHARS;
  return {
    text,
    capture: {
      status_code: res.status,
      content_type: res.headers.get('content-type') ?? '',
      body: text.slice(0, MAX_CAPTURED_BODY_CHARS),
      body_truncated: displayTruncated,
    },
  };
}

export async function exchangeRefreshToken(
  fetchImpl: typeof fetch,
  row: CredentialRow,
  refreshToken: string,
  clientSecret: string | null,
  accessToken: string,
): Promise<RefreshExchangeResult> {
  const form = new URLSearchParams();
  form.set('grant_type', 'refresh_token');
  form.set('refresh_token', refreshToken);
  form.set('client_id', row.clientId ?? '');
  const authConfig = toCredentialAuthConfig(row.authConfig);
  if (authConfig.resource) form.set('resource', authConfig.resource);
  if (authConfig.scope) form.set('scope', authConfig.scope);
  const headers: Record<string, string> = {
    'content-type': 'application/x-www-form-urlencoded',
    accept: 'application/json',
  };
  if (row.tokenEndpointAuthType === 'client_secret_basic') {
    const basic = Buffer.from(`${row.clientId ?? ''}:${clientSecret ?? ''}`).toString('base64');
    headers['authorization'] = `Basic ${basic}`;
  } else if (row.tokenEndpointAuthType === 'client_secret_post') {
    form.set('client_secret', clientSecret ?? '');
  }

  let res: Response;
  try {
    res = await fetchImpl(row.tokenEndpoint!, {
      method: 'POST',
      headers,
      body: form.toString(),
      signal: AbortSignal.timeout(OUTBOUND_TIMEOUT_MS),
    });
  } catch {
    return { status: 'connect_error', httpResponse: null };
  }

  const { capture, text } = await captureHttpResponse(res, MAX_REFRESH_PARSE_CHARS);
  const safeCapture = scrubCapturedHttpResponse(capture, [
    accessToken,
    refreshToken,
    clientSecret,
    clientSecret === null ? null : `${row.clientId ?? ''}:${clientSecret}`,
  ]);
  if (res.status >= 200 && res.status < 300) {
    // A successful token response carries fresh secrets; never echo it back.
    const redacted: CapturedHttpResponse = {
      ...capture,
      body: '[redacted]',
      body_truncated: false,
    };
    const parsed = parseJsonObject(text);
    // Slack reports both permanent grant failures and transient backend errors
    // as HTTP 200 with `{ ok: false, error }`. Only known auth/grant failures
    // should invalidate the credential; unknown or transient application
    // errors (for example `internal_error`) remain retryable.
    if (parsed?.['ok'] === false) {
      const error = readNonEmptyString(parsed, 'error');
      return {
        status:
          error !== undefined && PERMANENT_SLACK_REFRESH_ERRORS.has(error)
            ? 'failed'
            : 'connect_error',
        httpResponse: redacted,
      };
    }

    // Standard OAuth token responses place tokens at the top level. Slack's
    // user OAuth payload may instead place the rotating credential under
    // `authed_user`; use it only when no top-level access token is present so
    // bot-token responses retain their standard interpretation.
    const authedUser = isObject(parsed?.['authed_user']) ? parsed['authed_user'] : null;
    const tokenPayload =
      readNonEmptyString(parsed, 'access_token') !== undefined
        ? parsed
        : readNonEmptyString(authedUser, 'access_token') !== undefined
          ? authedUser
          : parsed;
    const newAccessToken = readNonEmptyString(tokenPayload, 'access_token');
    const newRefreshToken = readNonEmptyString(tokenPayload, 'refresh_token');
    // A 2xx body missing access_token is malformed, not a definitive grant
    // rejection — there is no dedicated enum member for it, so it maps to
    // connect_error (inconclusive/retry) rather than failed, which would
    // wrongly force the top-level status to invalid/re-authorize.
    if (newAccessToken === undefined) return { status: 'connect_error', httpResponse: redacted };
    return {
      status: 'succeeded',
      httpResponse: redacted,
      newAccessToken,
      newExpiresAt: expiresAtFromExpiresIn(tokenPayload?.['expires_in']),
      ...(newRefreshToken !== undefined ? { newRefreshToken } : {}),
    };
  }
  if (res.status === 429 || res.status >= 500) {
    return { status: 'connect_error', httpResponse: safeCapture };
  }
  if (res.status >= 400) return { status: 'failed', httpResponse: safeCapture };
  // Redirects and other unexpected classes are inconclusive.
  return { status: 'connect_error', httpResponse: safeCapture };
}

async function probeMcpInitialize(
  fetchImpl: typeof fetch,
  mcpServerUrl: string,
  token: string,
): Promise<CapturedHttpResponse | null> {
  let res: Response;
  try {
    res = await fetchImpl(mcpServerUrl, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
        accept: 'application/json, text/event-stream',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2025-06-18',
          capabilities: {},
          clientInfo: { name: 'orca-registry', version: '1.0' },
        },
      }),
      signal: AbortSignal.timeout(OUTBOUND_TIMEOUT_MS),
    });
  } catch {
    return null;
  }
  return scrubCapturedHttpResponse((await captureHttpResponse(res)).capture, [token]);
}

function scrubCapturedHttpResponse(
  capture: CapturedHttpResponse,
  secrets: Array<string | null | undefined>,
): CapturedHttpResponse {
  let body = capture.body;
  const variants = new Set<string>();
  for (const secret of secrets) {
    if (!secret) continue;
    variants.add(secret);
    variants.add(encodeURIComponent(secret));
    variants.add(new URLSearchParams([['secret', secret]]).toString().slice('secret='.length));
    variants.add(Buffer.from(secret, 'utf8').toString('base64'));
    variants.add(JSON.stringify(secret).slice(1, -1));
  }
  for (const value of [...variants].sort((left, right) => right.length - left.length)) {
    if (value.length > 0) body = body.split(value).join('[redacted]');
  }
  return { ...capture, body };
}

interface PersistRotatedTokenOptions {
  refreshLeaseOwner?: string;
}

/**
 * Persists a rotated access token (and refresh token when the OAuth server
 * returned one) using the staged rotation pattern: stage new refs, commit the
 * DB pointer swap, then delete the old refs. The pointer swap is a
 * compare-and-swap on the secret refs read at the start of the request, so a
 * concurrent rotation (another validate call or a credential update) makes
 * this one lose cleanly instead of last-writer-wins orphaning the winner's
 * refs. Returns false when the CAS did not match (credential archived,
 * deleted, or concurrently rotated); staged refs are purged. Runtime refresh
 * callers also CAS the lease owner and clear that lease with the pointer swap.
 */
export async function persistRotatedTokens(
  db: DbClient,
  secretStore: SecretStore,
  log: FastifyBaseLogger,
  row: CredentialRow,
  newAccessToken: string,
  newRefreshToken: string | undefined,
  newExpiresAt: string | null | undefined,
  options: PersistRotatedTokenOptions = {},
): Promise<boolean> {
  const refs = credentialRotationSecretRefs(row.workspaceId, row.id, newId('vsec'));
  const stagedRefs: string[] = [];
  const oldRefs: string[] = [row.accessSecretRef];
  const update: Partial<typeof vaultCredentials.$inferInsert> = {
    accessSecretRef: refs.access,
    updatedAt: new Date(),
    ...(options.refreshLeaseOwner !== undefined
      ? { oauthRefreshLeaseOwner: null, oauthRefreshLeaseExpiresAt: null }
      : {}),
    ...(newExpiresAt !== undefined
      ? {
          // Expiry belongs to the newly rotated access token. Update only
          // that JSON field so a concurrent metadata-only scope/resource
          // edit is not overwritten by this request's stale row snapshot.
          authConfig: sql`jsonb_set(${vaultCredentials.authConfig}, '{expires_at}', ${JSON.stringify(newExpiresAt)}::jsonb, true)`,
        }
      : {}),
  };
  try {
    await putStagedSecret(secretStore, stagedRefs, refs.access, newAccessToken);
    if (newRefreshToken !== undefined) {
      await putStagedSecret(secretStore, stagedRefs, refs.refresh, newRefreshToken);
      if (row.refreshSecretRef) oldRefs.push(row.refreshSecretRef);
      update.refreshSecretRef = refs.refresh;
    }
    const updated = await db
      .update(vaultCredentials)
      .set(update)
      .where(
        and(
          isNull(vaultCredentials.deletedAt),
          eq(vaultCredentials.id, row.id),
          eq(vaultCredentials.vaultId, row.vaultId),
          eq(vaultCredentials.workspaceId, row.workspaceId),
          isNull(vaultCredentials.archivedAt),
          eq(vaultCredentials.accessSecretRef, row.accessSecretRef),
          row.refreshSecretRef
            ? eq(vaultCredentials.refreshSecretRef, row.refreshSecretRef)
            : isNull(vaultCredentials.refreshSecretRef),
          options.refreshLeaseOwner !== undefined
            ? eq(vaultCredentials.oauthRefreshLeaseOwner, options.refreshLeaseOwner)
            : undefined,
        ),
      )
      .returning({ id: vaultCredentials.id });
    if (updated.length === 0) {
      await purgeSecretRefs(secretStore, stagedRefs).catch((err: unknown) => {
        log.warn(
          { err, credentialId: row.id },
          'failed to purge staged secrets after rejected token rotation',
        );
      });
      return false;
    }
  } catch (err) {
    await purgeSecretRefs(secretStore, stagedRefs).catch((purgeErr: unknown) => {
      log.warn(
        { err: purgeErr, credentialId: row.id },
        'failed to purge staged secrets after failed token rotation',
      );
    });
    throw err;
  }
  await purgeSecretRefs(secretStore, oldRefs).catch((err: unknown) => {
    log.warn({ err, credentialId: row.id }, 'failed to purge old secrets after token rotation');
  });
  return true;
}

function readNonEmptyString(
  value: Record<string, unknown> | null,
  key: string,
): string | undefined {
  const item = value?.[key];
  return typeof item === 'string' && item.length > 0 ? item : undefined;
}

function expiresAtFromExpiresIn(value: unknown): string | null {
  const seconds =
    typeof value === 'number'
      ? value
      : typeof value === 'string' && value.trim().length > 0
        ? Number(value)
        : Number.NaN;
  if (!Number.isFinite(seconds) || seconds < 0) return null;
  const expiresAtMs = Date.now() + seconds * 1000;
  if (!Number.isFinite(expiresAtMs)) return null;
  const expiresAt = new Date(expiresAtMs);
  return Number.isNaN(expiresAt.getTime()) ? null : expiresAt.toISOString();
}

function parseJsonObject(text: string): Record<string, unknown> | null {
  try {
    const value: unknown = JSON.parse(text);
    return isObject(value) ? value : null;
  } catch {
    return null;
  }
}

async function loadActiveVault(db: DbClient, workspaceId: string, vaultId: string) {
  const rows = await db
    .select()
    .from(vaults)
    .where(
      and(
        isNull(vaults.deletedAt),
        eq(vaults.id, vaultId),
        eq(vaults.workspaceId, workspaceId),
        isNull(vaults.archivedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadVault(db: DbClient, workspaceId: string, vaultId: string) {
  const rows = await db
    .select()
    .from(vaults)
    .where(
      and(isNull(vaults.deletedAt), eq(vaults.id, vaultId), eq(vaults.workspaceId, workspaceId)),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadActiveVaultForUpdate(
  db: CredentialReadDb,
  workspaceId: string,
  vaultId: string,
) {
  const rows = await db
    .select()
    .from(vaults)
    .where(
      and(
        isNull(vaults.deletedAt),
        eq(vaults.id, vaultId),
        eq(vaults.workspaceId, workspaceId),
        isNull(vaults.archivedAt),
      ),
    )
    .for('update')
    .limit(1);
  return rows[0] ?? null;
}

async function loadCredential(
  db: CredentialReadDb,
  workspaceId: string,
  vaultId: string,
  credentialId: string,
): Promise<CredentialRow | null> {
  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.id, credentialId),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadActiveCredentialForUpdate(
  db: CredentialReadDb,
  workspaceId: string,
  vaultId: string,
  credentialId: string,
): Promise<{ row: CredentialRow; hasLiveRefreshLease: boolean } | null> {
  const rows = await db
    .select({
      row: vaultCredentials,
      hasLiveRefreshLease: sql<boolean>`
        ${vaultCredentials.oauthRefreshLeaseOwner} is not null
        and ${vaultCredentials.oauthRefreshLeaseExpiresAt} is not null
        and ${vaultCredentials.oauthRefreshLeaseExpiresAt} > now()
      `,
    })
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.id, credentialId),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
        isNull(vaultCredentials.archivedAt),
      ),
    )
    .for('update')
    .limit(1);
  return rows[0] ?? null;
}

function duplicateErrorForCreateAuth(auth: CredentialCreateAuth): string {
  if (auth.type === 'environment_variable') {
    return 'credential already exists for secret_name';
  }
  if (auth.type === 'provider') return 'credential already exists for logical_id';
  return 'credential already exists for mcp_server_url';
}

async function loadDuplicateCredentialForCreate(
  db: CredentialReadDb,
  workspaceId: string,
  vaultId: string,
  auth: CredentialCreateAuth,
): Promise<CredentialRow | null> {
  if (auth.type === 'environment_variable') {
    return loadActiveCredentialBySecretName(db, workspaceId, vaultId, auth.secret_name);
  }
  if (auth.type === 'provider') {
    return loadActiveCredentialByLogicalId(db, workspaceId, vaultId, auth.logical_id);
  }
  return loadActiveCredentialByCanonicalUrl(db, workspaceId, vaultId, auth.mcp_server_url);
}

async function loadActiveCredentialByCanonicalUrl(
  db: CredentialReadDb,
  workspaceId: string,
  vaultId: string,
  mcpServerUrl: string,
): Promise<CredentialRow | null> {
  const canonicalUrl = canonicalMcpServerUrl(mcpServerUrl);
  if (canonicalUrl === null) return null;

  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
        isNull(vaultCredentials.archivedAt),
      ),
    );
  return (
    rows.find(
      (row) =>
        row.mcpServerUrl !== null && canonicalMcpServerUrl(row.mcpServerUrl) === canonicalUrl,
    ) ?? null
  );
}

async function loadActiveCredentialByLogicalId(
  db: CredentialReadDb,
  workspaceId: string,
  vaultId: string,
  logicalId: string,
): Promise<CredentialRow | null> {
  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
        eq(vaultCredentials.logicalId, logicalId),
        isNull(vaultCredentials.archivedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadActiveCredentialBySecretName(
  db: CredentialReadDb,
  workspaceId: string,
  vaultId: string,
  secretName: string,
): Promise<CredentialRow | null> {
  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
        eq(vaultCredentials.secretName, secretName),
        isNull(vaultCredentials.archivedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function countActiveCredentials(
  db: CredentialReadDb,
  workspaceId: string,
  vaultId: string,
): Promise<number> {
  const rows = await db
    .select({ value: count() })
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.vaultId, vaultId),
        isNull(vaultCredentials.archivedAt),
      ),
    );
  return rows[0]?.value ?? 0;
}

function credentialSecretRefs(workspaceId: string, credentialId: string) {
  // `local:` is a legacy opaque-reference prefix, not a backend selector.
  // Keep it stable so persistent SecretStore cutovers do not rewrite DB refs.
  const base = `local:vault_credentials/${workspaceId}/${credentialId}`;
  return {
    access: `${base}/access_token`,
    refresh: `${base}/refresh_token`,
    clientSecret: `${base}/client_secret`,
  };
}

function credentialRotationSecretRefs(
  workspaceId: string,
  credentialId: string,
  rotationId: string,
) {
  const base = `local:vault_credentials/${workspaceId}/${credentialId}/rotations/${rotationId}`;
  return {
    access: `${base}/access_token`,
    refresh: `${base}/refresh_token`,
    clientSecret: `${base}/client_secret`,
  };
}

function readCreateMetadata(
  input: Record<string, unknown>,
): ValidationResult<Record<string, string>> {
  if (!('metadata' in input)) return { ok: true, value: {} };
  if (!isPlainRecord(input['metadata'])) return invalid('metadata must be an object');
  const error = validateStoredMetadata(input['metadata']);
  if (error) return invalid(error);
  return { ok: true, value: input['metadata'] as Record<string, string> };
}

function readMetadataPatch(
  input: Record<string, unknown>,
): ValidationResult<Record<string, string | null> | null | undefined> {
  if (!('metadata' in input)) return { ok: true, value: undefined };
  if (input['metadata'] === null) return { ok: true, value: null };
  if (!isPlainRecord(input['metadata'])) return invalid('metadata must be an object or null');
  for (const [key, value] of Object.entries(input['metadata'])) {
    if (key.length < 1 || key.length > 64) return invalid('metadata keys must be 1-64 characters');
    if (value !== null && (typeof value !== 'string' || value.length > 512)) {
      return invalid(`metadata.${key} must be a string up to 512 characters or null`);
    }
  }
  return { ok: true, value: input['metadata'] as Record<string, string | null> };
}

function applyMetadataPatch(
  existing: unknown,
  patch: Record<string, string | null>,
): Record<string, string> {
  const next = toStringMetadata(existing);
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete next[key];
    else next[key] = value;
  }
  return next;
}

function validateStoredMetadata(value: unknown): string | null {
  if (!isPlainRecord(value)) return 'metadata must be an object of string values';
  const entries = Object.entries(value);
  if (entries.length > 16) return 'metadata must contain at most 16 pairs';
  for (const [key, item] of entries) {
    if (key.length < 1 || key.length > 64) return 'metadata keys must be 1-64 characters';
    if (typeof item !== 'string' || item.length > 512) {
      return `metadata.${key} must be a string up to 512 characters`;
    }
  }
  return null;
}

function readRequiredString(input: Record<string, unknown>, key: string): ValidationResult<string> {
  const value = input[key];
  if (typeof value !== 'string' || value.trim().length === 0) return invalid(`${key} is required`);
  return { ok: true, value };
}

function readRequiredUrlString(
  input: Record<string, unknown>,
  key: string,
): ValidationResult<string> {
  const value = readRequiredString(input, key);
  if (!value.ok) return value;
  try {
    const url = new URL(value.value!);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      return invalid(`${key} must be a valid http(s) URL`);
    }
  } catch {
    return invalid(`${key} must be a valid http(s) URL`);
  }
  return value;
}

function readOptionalString(
  input: Record<string, unknown>,
  key: string,
): ValidationResult<string | undefined> {
  if (!(key in input)) return { ok: true, value: undefined };
  const value = input[key];
  if (typeof value !== 'string') return invalid(`${key} must be a string`);
  if (value.trim().length === 0) return invalid(`${key} must be a non-empty string`);
  return { ok: true, value };
}

function readOptionalNullableSecret(
  input: Record<string, unknown>,
  key: string,
): ValidationResult<string | null | undefined> {
  if (!(key in input)) return { ok: true, value: undefined };
  const value = input[key];
  if (value === null) return { ok: true, value: null };
  if (typeof value !== 'string' || value.length === 0) {
    return invalid(`${key} must be a non-empty string or null`);
  }
  return { ok: true, value };
}

function readOptionalNullableStringValue(
  input: Record<string, unknown>,
  key: string,
): ValidationResult<string | null | undefined> {
  if (!(key in input)) return { ok: true, value: undefined };
  const value = input[key];
  if (value === null || typeof value === 'string') return { ok: true, value };
  return invalid(`${key} must be a string or null`);
}

function readOptionalNullableTimestamp(
  input: Record<string, unknown>,
  key: string,
): ValidationResult<string | null | undefined> {
  const value = readOptionalNullableStringValue(input, key);
  if (!value.ok || value.value === undefined || value.value === null) return value;
  if (!RFC3339_RE.test(value.value)) return invalid(`${key} must be an RFC 3339 timestamp or null`);
  const date = new Date(value.value);
  if (Number.isNaN(date.getTime())) return invalid(`${key} must be an RFC 3339 timestamp or null`);
  return { ok: true, value: date.toISOString() };
}

function readOptionalNullableString(
  input: Record<string, unknown>,
  key: string,
): ValidationResult<string | null | undefined> {
  if (!(key in input)) return { ok: true, value: undefined };
  const value = input[key];
  if (value === null) return { ok: true, value: null };
  if (typeof value !== 'string') return invalid(`${key} must be a string`);
  if (value.length < 1 || value.length > 255) return invalid(`${key} must be 1-255 characters`);
  return { ok: true, value };
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return isObject(value);
}

function toStringMetadata(value: unknown): Record<string, string> {
  if (!isPlainRecord(value)) return {};
  const out: Record<string, string> = {};
  for (const [key, item] of Object.entries(value)) {
    if (typeof item === 'string') out[key] = item;
  }
  return out;
}

function toCredentialAuthConfig(value: unknown): CredentialAuthConfig {
  if (!isPlainRecord(value)) return {};
  const config: CredentialAuthConfig = {};
  const injection = value['injection_location'];
  if (
    isPlainRecord(injection) &&
    typeof injection['header'] === 'boolean' &&
    typeof injection['body'] === 'boolean'
  ) {
    config.injection_location = {
      header: injection['header'],
      body: injection['body'],
    };
  }
  if (value['expires_at'] === null || typeof value['expires_at'] === 'string') {
    config.expires_at = value['expires_at'];
  }
  if (value['resource'] === null || typeof value['resource'] === 'string') {
    config.resource = value['resource'];
  }
  if (value['scope'] === null || typeof value['scope'] === 'string') {
    config.scope = value['scope'];
  }
  return config;
}

function invalid<T>(error: string): ValidationResult<T> {
  return { ok: false, error };
}

function isUniqueViolation(err: unknown): boolean {
  return (
    isObject(err) && (err['code'] === '23505' || String(err['message'] ?? '').includes('23505'))
  );
}

async function retrySerializationFailure<T>(operation: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await operation();
    } catch (err) {
      // A waiter may begin its SERIALIZABLE snapshot before the Vault row lock
      // is released. Retry so it can observe the winning normalized binding.
      if (attempt >= 2 || !isSerializationFailure(err)) throw err;
    }
  }
}

function isSerializationFailure(err: unknown): boolean {
  return (
    isObject(err) && (err['code'] === '40001' || String(err['message'] ?? '').includes('40001'))
  );
}
