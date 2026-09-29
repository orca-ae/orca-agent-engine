// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { InMemorySkillStore } from '@orca/skill-store';
import { eq, sql } from 'drizzle-orm';
import { request as httpRequest } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { PoolClient } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { KubernetesServiceAccountAuthVerifier } from '../../src/auth/internal-auth.js';
import {
  AGENT_OBSERVABILITY_SECRET_RESOLUTION_DEADLINE_MS,
  AgentObservabilitySecretResolutionDeniedError,
  AgentObservabilitySecretResolutionUnavailableError,
  resolveAgentObservabilitySessionSecret,
} from '../../src/domain/agent-observability-secret-resolver.js';
import { loadAgentObservabilitySessionContext } from '../../src/domain/agent-observability-context-resolver.js';
import {
  AGENT_OBSERVABILITY_SECRET_BUNDLE_MAX_BYTES,
  encodeAgentObservabilitySecretBundle,
  newAgentObservabilitySecretReference,
} from '../../src/domain/agent-observability-secrets.js';
import * as schema from '../../src/persistence/postgres/schema.js';
import type { SecretProvider, SecretStore } from '../../src/secrets/secret-provider.js';
import { LocalSecretStore } from '../../src/secrets/local-store.js';
import {
  buildAdminApp,
  buildCombinedTestApp,
  buildInternalApp,
  buildPublicApp,
} from '../../src/server.js';
import { createFreshWorkspaceMutationTestDb } from './agent-observability-workspace-mutation-test-db.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  STUB_SSE_CONFIG,
} from './setup.js';

const organizationId = 'org_secret_resolver';
const workspaceId = 'ws_secret_resolver';
const bindingId = 'aob_secret_resolver';
const sessionId = 'ses_secret_resolver';
const initialSecretRef = newAgentObservabilitySecretReference();
const bearerWorkspaceId = 'ws_secret_resolver_bearer';
const bearerBindingId = 'aob_secret_resolver_bearer';
const bearerSessionId = 'ses_secret_resolver_bearer';
const bearerSecretRef = newAgentObservabilitySecretReference();
const headersWorkspaceId = 'ws_secret_resolver_headers';
const headersBindingId = 'aob_secret_resolver_headers';
const headersSessionId = 'ses_secret_resolver_headers';
const headersSecretRef = newAgentObservabilitySecretReference();
const langfuseOrganizationWorkspaceId = 'ws_secret_resolver_langfuse_org';
const langfuseOrganizationBindingId = 'aob_secret_resolver_langfuse_org';
const langfuseOrganizationSessionId = 'ses_secret_resolver_langfuse_org';
const langfuseOrganizationSecretRef = newAgentObservabilitySecretReference();
const langfuseWorkspaceId = 'ws_secret_resolver_langfuse_workspace';
const langfuseWorkspaceBindingId = 'aob_secret_resolver_langfuse_workspace';
const langfuseWorkspaceSessionId = 'ses_secret_resolver_langfuse_workspace';
const langfuseWorkspaceSecretRef = newAgentObservabilitySecretReference();
const disabledWorkspaceId = 'ws_secret_resolver_disabled';
const disabledSessionId = 'ses_secret_resolver_disabled';

function deferred<T = void>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

/** Polls an observable DB/event condition instead of guessing a sleep duration. */
async function waitForCondition(
  description: string,
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${description}`);
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}

class RecordingSecretStore extends LocalSecretStore {
  readonly resolvedReferences: string[] = [];

  override async resolve(
    reference: string,
    options?: { signal?: AbortSignal },
  ): Promise<string | null> {
    this.resolvedReferences.push(reference);
    return super.resolve(reference, options);
  }
}

describe('agent observability audited secret resolver', () => {
  let fixture: Awaited<ReturnType<typeof createFreshWorkspaceMutationTestDb>>;
  let secretStore: RecordingSecretStore;

  async function insertPinnedBinding(input: {
    workspaceId: string;
    sessionId: string;
    bindingId: string;
    secretRef: string;
    scope: 'organization' | 'workspace';
    adapterType: 'otlp_http' | 'langfuse_sdk';
    endpointKind: 'traces_endpoint' | 'base_endpoint';
    endpoint: string;
    externalProjectId: string | null;
    semanticProfile: 'otel_genai' | 'langfuse';
    protocol: 'http/protobuf' | 'http/json' | 'sdk';
  }): Promise<void> {
    const { db } = fixture;
    await db.insert(schema.workspaces).values({
      id: input.workspaceId,
      organizationId,
      name: input.workspaceId,
      status: 'active',
      createdBy: 'integration-test',
    });
    await db.transaction(async (tx) => {
      await tx.insert(schema.agentObservabilityBindings).values({
        id: input.bindingId,
        organizationId,
        workspaceId: input.scope === 'workspace' ? input.workspaceId : null,
        scopeType: input.scope,
        adapterType: input.adapterType,
        endpointKind: input.endpointKind,
        endpointClass: 'public',
        endpoint: input.endpoint,
        externalProjectId: input.externalProjectId,
        currentVersion: 1,
        status: 'active',
        revocationEpoch: 0,
        createdBy: 'integration-test',
        updatedBy: 'integration-test',
      });
      await tx.insert(schema.agentObservabilityBindingVersions).values({
        bindingId: input.bindingId,
        version: 1,
        adapterType: input.adapterType,
        semanticProfile: input.semanticProfile,
        protocol: input.protocol,
        compression: 'none',
        timeoutMs: 5_000,
        environment: 'integration',
        release: 'v1',
        captureMode: 'redacted_io',
        sampleRate: '1',
        configSchemaVersion: 1,
        createdBy: 'integration-test',
      });
      await tx.insert(schema.agentObservabilityBindingCredentials).values({
        bindingId: input.bindingId,
        secretRef: input.secretRef,
        credentialVersion: 1,
        keyHint: null,
        rotatedAt: new Date(),
        updatedBy: 'integration-test',
      });
      await tx
        .insert(schema.agentObservabilityWorkspaceSettings)
        .values({
          workspaceId: input.workspaceId,
          organizationId,
          mode: input.scope === 'workspace' ? 'custom' : 'inherit',
          bindingId: input.scope === 'workspace' ? input.bindingId : null,
          selectionEpoch: 0,
          revocationEpoch: 0,
          captureCeiling: 'redacted_io',
          captureRestrictionEpoch: 0,
        })
        .onConflictDoUpdate({
          target: schema.agentObservabilityWorkspaceSettings.workspaceId,
          set: {
            mode: input.scope === 'workspace' ? 'custom' : 'inherit',
            bindingId: input.scope === 'workspace' ? input.bindingId : null,
            captureCeiling: 'redacted_io',
          },
        });
      await tx.insert(schema.sessionObservabilityBindings).values({
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        organizationId,
        bindingId: input.bindingId,
        bindingVersion: 1,
        bindingScope: input.scope,
        bindingWorkspaceId: input.scope === 'workspace' ? input.workspaceId : null,
        selectionSource: input.scope === 'workspace' ? 'workspace_custom' : 'organization_default',
        status: 'active',
        organizationSelectionEpoch: 0,
        workspaceSelectionEpoch: 0,
        organizationDefaultRevocationEpoch: 0,
        organizationRevocationEpoch: 0,
        workspaceRevocationEpoch: 0,
        bindingRevocationEpoch: 0,
        platformCaptureRestrictionEpoch: 0,
        organizationCaptureRestrictionEpoch: 0,
        workspaceCaptureRestrictionEpoch: 0,
        effectiveCaptureMode: 'redacted_io',
        sessionRevocationEpoch: 0,
        agentId: `agt_${input.bindingId.slice(4)}`,
        agentVersion: 1,
        harness: 'codex',
        harnessMode: 'colocated',
      });
    });
  }

  beforeAll(async () => {
    fixture = await createFreshWorkspaceMutationTestDb('agent_observability_secret_resolver');
    const { db } = fixture;
    await db.insert(schema.organizations).values({
      id: organizationId,
      name: 'Secret resolver organization',
      status: 'active',
    });
    await db.insert(schema.workspaces).values({
      id: workspaceId,
      organizationId,
      name: 'Secret resolver workspace',
      status: 'active',
      createdBy: 'integration-test',
    });
    await db.transaction(async (tx) => {
      await tx.insert(schema.agentObservabilityBindings).values({
        id: bindingId,
        organizationId,
        workspaceId: null,
        scopeType: 'organization',
        adapterType: 'otlp_http',
        endpointKind: 'traces_endpoint',
        endpointClass: 'public',
        endpoint: 'https://collector.example/v1/traces',
        externalProjectId: null,
        currentVersion: 1,
        status: 'active',
        revocationEpoch: 0,
        createdBy: 'integration-test',
        updatedBy: 'integration-test',
      });
      await tx.insert(schema.agentObservabilityBindingVersions).values({
        bindingId,
        version: 1,
        adapterType: 'otlp_http',
        semanticProfile: 'otel_genai',
        protocol: 'http/protobuf',
        compression: 'none',
        timeoutMs: 5_000,
        environment: null,
        release: null,
        captureMode: 'redacted_io',
        sampleRate: '1',
        configSchemaVersion: 1,
        createdBy: 'integration-test',
      });
      await tx.insert(schema.agentObservabilityBindingCredentials).values({
        bindingId,
        secretRef: initialSecretRef,
        credentialVersion: 1,
        keyHint: null,
        rotatedAt: new Date(),
        updatedBy: 'integration-test',
      });
    });
    await db
      .update(schema.agentObservabilityOrganizationSettings)
      .set({
        activeDefaultBindingId: bindingId,
        activeDefaultBindingScope: 'organization',
        captureCeiling: 'redacted_io',
      })
      .where(eq(schema.agentObservabilityOrganizationSettings.organizationId, organizationId));
    await db
      .update(schema.agentObservabilityWorkspaceSettings)
      .set({ captureCeiling: 'redacted_io' })
      .where(eq(schema.agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
    await db
      .update(schema.agentObservabilityPlatformPolicy)
      .set({
        allowedAdapters: ['otlp_http', 'langfuse_sdk'],
        allowedEndpointClasses: ['public', 'private'],
        maxCaptureMode: 'redacted_io',
      })
      .where(eq(schema.agentObservabilityPlatformPolicy.id, 'default'));
    await db.insert(schema.sessionObservabilityBindings).values({
      workspaceId,
      sessionId,
      organizationId,
      bindingId,
      bindingVersion: 1,
      bindingScope: 'organization',
      bindingWorkspaceId: null,
      selectionSource: 'organization_default',
      status: 'active',
      organizationSelectionEpoch: 0,
      workspaceSelectionEpoch: 0,
      organizationDefaultRevocationEpoch: 0,
      organizationRevocationEpoch: 0,
      workspaceRevocationEpoch: 0,
      bindingRevocationEpoch: 0,
      platformCaptureRestrictionEpoch: 0,
      organizationCaptureRestrictionEpoch: 0,
      workspaceCaptureRestrictionEpoch: 0,
      effectiveCaptureMode: 'redacted_io',
      sessionRevocationEpoch: 0,
      agentId: 'agt_secret_resolver',
      agentVersion: 1,
      harness: 'codex',
      harnessMode: 'colocated',
    });
    await insertPinnedBinding({
      workspaceId: bearerWorkspaceId,
      sessionId: bearerSessionId,
      bindingId: bearerBindingId,
      secretRef: bearerSecretRef,
      scope: 'workspace',
      adapterType: 'otlp_http',
      endpointKind: 'traces_endpoint',
      endpoint: 'https://bearer-collector.example/v1/traces',
      externalProjectId: null,
      semanticProfile: 'otel_genai',
      protocol: 'http/protobuf',
    });
    await insertPinnedBinding({
      workspaceId: headersWorkspaceId,
      sessionId: headersSessionId,
      bindingId: headersBindingId,
      secretRef: headersSecretRef,
      scope: 'workspace',
      adapterType: 'otlp_http',
      endpointKind: 'traces_endpoint',
      endpoint: 'https://headers-collector.example/v1/traces',
      externalProjectId: null,
      semanticProfile: 'otel_genai',
      protocol: 'http/json',
    });
    await insertPinnedBinding({
      workspaceId: langfuseOrganizationWorkspaceId,
      sessionId: langfuseOrganizationSessionId,
      bindingId: langfuseOrganizationBindingId,
      secretRef: langfuseOrganizationSecretRef,
      scope: 'organization',
      adapterType: 'langfuse_sdk',
      endpointKind: 'base_endpoint',
      endpoint: 'https://langfuse-org.example',
      externalProjectId: 'pk_langfuse_org',
      semanticProfile: 'langfuse',
      protocol: 'sdk',
    });
    await insertPinnedBinding({
      workspaceId: langfuseWorkspaceId,
      sessionId: langfuseWorkspaceSessionId,
      bindingId: langfuseWorkspaceBindingId,
      secretRef: langfuseWorkspaceSecretRef,
      scope: 'workspace',
      adapterType: 'langfuse_sdk',
      endpointKind: 'base_endpoint',
      endpoint: 'https://langfuse-workspace.example',
      externalProjectId: 'pk_langfuse_workspace',
      semanticProfile: 'langfuse',
      protocol: 'sdk',
    });
    await db.insert(schema.workspaces).values({
      id: disabledWorkspaceId,
      organizationId,
      name: 'Disabled resolver workspace',
      status: 'active',
      createdBy: 'integration-test',
    });
    await db.insert(schema.sessionObservabilityBindings).values({
      workspaceId: disabledWorkspaceId,
      sessionId: disabledSessionId,
      organizationId,
      bindingId: null,
      bindingVersion: null,
      bindingScope: null,
      bindingWorkspaceId: null,
      selectionSource: 'disabled',
      status: 'disabled',
      organizationSelectionEpoch: 0,
      workspaceSelectionEpoch: 0,
      organizationDefaultRevocationEpoch: 0,
      organizationRevocationEpoch: 0,
      workspaceRevocationEpoch: 0,
      bindingRevocationEpoch: 0,
      platformCaptureRestrictionEpoch: 0,
      organizationCaptureRestrictionEpoch: 0,
      workspaceCaptureRestrictionEpoch: 0,
      effectiveCaptureMode: 'metadata_only',
      sessionRevocationEpoch: 0,
      agentId: 'agt_secret_resolver_disabled',
      agentVersion: 1,
      harness: 'codex',
      harnessMode: 'colocated',
    });
  });

  beforeEach(async () => {
    secretStore = new RecordingSecretStore();
    await secretStore.put(
      initialSecretRef,
      encodeAgentObservabilitySecretBundle({
        bindingId,
        credentialVersion: 1,
        adapterType: 'otlp_http',
        auth: { type: 'basic', username: 'test-user', password: 'test-password-v1' },
      }),
    );
    await Promise.all([
      secretStore.put(
        bearerSecretRef,
        encodeAgentObservabilitySecretBundle({
          bindingId: bearerBindingId,
          credentialVersion: 1,
          adapterType: 'otlp_http',
          auth: { type: 'bearer', token: 'bearer-token-workspace' },
        }),
      ),
      secretStore.put(
        headersSecretRef,
        encodeAgentObservabilitySecretBundle({
          bindingId: headersBindingId,
          credentialVersion: 1,
          adapterType: 'otlp_http',
          auth: {
            type: 'custom_headers',
            headers: {
              'X-Second': 'headers-secret-second',
              'x-First': 'headers-secret-first',
            },
          },
        }),
      ),
      secretStore.put(
        langfuseOrganizationSecretRef,
        encodeAgentObservabilitySecretBundle({
          bindingId: langfuseOrganizationBindingId,
          credentialVersion: 1,
          adapterType: 'langfuse_sdk',
          publicKey: 'pk-langfuse-organization',
          secretKey: 'sk-langfuse-organization',
        }),
      ),
      secretStore.put(
        langfuseWorkspaceSecretRef,
        encodeAgentObservabilitySecretBundle({
          bindingId: langfuseWorkspaceBindingId,
          credentialVersion: 1,
          adapterType: 'langfuse_sdk',
          publicKey: 'pk-langfuse-workspace',
          secretKey: 'sk-langfuse-workspace',
        }),
      ),
    ]);
    const { db } = fixture;
    await db
      .update(schema.agentObservabilityBindings)
      .set({
        adapterType: 'otlp_http',
        endpointKind: 'traces_endpoint',
        endpointClass: 'public',
        endpoint: 'https://collector.example/v1/traces',
        externalProjectId: null,
        currentVersion: 1,
        status: 'active',
        archivedAt: null,
        revocationEpoch: 0,
        updatedAt: sql`now()`,
      })
      .where(eq(schema.agentObservabilityBindings.id, bindingId));
    await db
      .update(schema.agentObservabilityBindingVersions)
      .set({
        semanticProfile: 'otel_genai',
        protocol: 'http/protobuf',
        compression: 'none',
        timeoutMs: 5_000,
        environment: null,
        release: null,
        captureMode: 'redacted_io',
        sampleRate: '1',
        configSchemaVersion: 1,
      })
      .where(
        sql`${schema.agentObservabilityBindingVersions.bindingId} = ${bindingId} and ${schema.agentObservabilityBindingVersions.version} = 1`,
      );
    await db
      .insert(schema.agentObservabilityBindingCredentials)
      .values({
        bindingId,
        secretRef: initialSecretRef,
        credentialVersion: 1,
        keyHint: null,
        rotatedAt: new Date(),
        updatedBy: 'integration-test',
        updatedAt: sql`now()`,
      })
      .onConflictDoUpdate({
        target: schema.agentObservabilityBindingCredentials.bindingId,
        set: {
          secretRef: initialSecretRef,
          credentialVersion: 1,
          keyHint: null,
          rotatedAt: new Date(),
          updatedBy: 'integration-test',
          updatedAt: sql`now()`,
        },
      });
    await db
      .update(schema.sessionObservabilityBindings)
      .set({ status: 'active', sessionRevocationEpoch: 0, archivedAt: null, deletedAt: null })
      .where(
        sql`${schema.sessionObservabilityBindings.workspaceId} = ${workspaceId} and ${schema.sessionObservabilityBindings.sessionId} = ${sessionId}`,
      );
    await db
      .update(schema.agentObservabilityOrganizationSettings)
      .set({
        defaultRevocationEpoch: 0,
        organizationRevocationEpoch: 0,
        captureCeiling: 'redacted_io',
      })
      .where(eq(schema.agentObservabilityOrganizationSettings.organizationId, organizationId));
    await db
      .delete(schema.agentObservabilityWorkspaceArchiveRevocations)
      .where(
        eq(schema.agentObservabilityWorkspaceArchiveRevocations.organizationId, organizationId),
      );
    await db
      .update(schema.agentObservabilityWorkspaceSettings)
      .set({ revocationEpoch: 0, captureCeiling: 'redacted_io' })
      .where(eq(schema.agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
    await db
      .update(schema.agentObservabilityPlatformPolicy)
      .set({
        allowedAdapters: ['otlp_http', 'langfuse_sdk'],
        allowedEndpointClasses: ['public', 'private'],
        maxCaptureMode: 'redacted_io',
        captureRestrictionEpoch: 0,
      })
      .where(eq(schema.agentObservabilityPlatformPolicy.id, 'default'));
    await db
      .update(schema.organizations)
      .set({ status: 'active' })
      .where(eq(schema.organizations.id, organizationId));
    await db
      .update(schema.workspaces)
      .set({ status: 'active', archivedAt: null })
      .where(eq(schema.workspaces.organizationId, organizationId));
    await db
      .delete(schema.adminAuditEvents)
      .where(eq(schema.adminAuditEvents.organizationId, organizationId));
  });

  afterAll(async () => {
    await fixture.close();
  });

  function resolve(
    input: Partial<{
      secretStore: SecretStore | undefined;
      requestId: string;
      workspaceId: string;
      sessionId: string;
      signal: AbortSignal;
      deadlineMs: number;
      maxAttempts: number;
    }> = {},
  ) {
    return resolveAgentObservabilitySessionSecret({
      db: fixture.db,
      secretStore: Object.hasOwn(input, 'secretStore') ? input.secretStore : secretStore,
      workspaceId: input.workspaceId ?? workspaceId,
      sessionId: input.sessionId ?? sessionId,
      actor: 'system:serviceaccount:orca:observability-exporter',
      requestId: input.requestId ?? 'req_secret_resolver',
      ...(input.signal ? { signal: input.signal } : {}),
      ...(input.deadlineMs !== undefined ? { deadlineMs: input.deadlineMs } : {}),
      ...(input.maxAttempts !== undefined ? { maxAttempts: input.maxAttempts } : {}),
    });
  }

  async function auditRows() {
    return fixture.db
      .select()
      .from(schema.adminAuditEvents)
      .where(eq(schema.adminAuditEvents.organizationId, organizationId));
  }

  async function resolveHttp(
    input: { workspaceId?: string; sessionId?: string; secretStore?: SecretStore | undefined } = {},
  ) {
    const app = buildCombinedTestApp({
      db: fixture.db,
      oidc: { allowedIssuers: [], audience: 'test' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      skillStore: new InMemorySkillStore(),
      secretStore: Object.hasOwn(input, 'secretStore') ? input.secretStore : secretStore,
    });
    try {
      return await app.inject({
        method: 'POST',
        url:
          `/internal/v1/workspaces/${input.workspaceId ?? workspaceId}` +
          `/sessions/${input.sessionId ?? sessionId}/agent-observability/secret/resolve`,
        payload: {},
      });
    } finally {
      await app.close();
    }
  }

  async function rotateHead(version: number, password: string): Promise<string> {
    const secretRef = newAgentObservabilitySecretReference();
    await secretStore.put(
      secretRef,
      encodeAgentObservabilitySecretBundle({
        bindingId,
        credentialVersion: version,
        adapterType: 'otlp_http',
        auth: { type: 'basic', username: 'test-user', password },
      }),
    );
    await fixture.db
      .update(schema.agentObservabilityBindingCredentials)
      .set({ secretRef, credentialVersion: version, rotatedAt: new Date(), updatedAt: sql`now()` })
      .where(eq(schema.agentObservabilityBindingCredentials.bindingId, bindingId));
    return secretRef;
  }

  async function waitForBlockedBackend(blockerPid: number): Promise<number> {
    let blockedPid = 0;
    await waitForCondition(`a resolver backend blocked by ${blockerPid} in pg_locks`, async () => {
      blockedPid = (await blockedBackendPids(blockerPid))[0] ?? 0;
      return blockedPid !== 0;
    });
    return blockedPid;
  }

  async function blockedBackendPids(blockerPid: number): Promise<number[]> {
    const result = await fixture.pool.query<{ pid: number }>(
      `
        SELECT pid
        FROM pg_locks
        WHERE NOT granted
          AND $1 = ANY(pg_blocking_pids(pid))
        ORDER BY pid
      `,
      [blockerPid],
    );
    return result.rows.map((row) => row.pid);
  }

  async function beginCredentialHeadLock(): Promise<PoolClient> {
    const client = await fixture.pool.connect();
    await client.query('BEGIN');
    await client.query(
      'SELECT binding_id FROM agent_observability_binding_credentials WHERE binding_id = $1 FOR UPDATE',
      [bindingId],
    );
    return client;
  }

  async function backendPid(client: PoolClient): Promise<number> {
    const result = await client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
    const pid = result.rows[0]?.pid;
    if (pid === undefined) throw new Error('Postgres did not return a backend PID');
    return pid;
  }

  function resolverOnlyStore(
    resolve: (reference: string, options?: { signal?: AbortSignal }) => Promise<string | null>,
  ): SecretStore {
    return {
      resolve,
      put: async () => {},
      delete: async () => {},
    };
  }

  it('releases a canonical bundle only after success audit commits', async () => {
    await expect(
      loadAgentObservabilitySessionContext({ db: fixture.db, workspaceId, sessionId }),
    ).resolves.toMatchObject({ status: 'enabled' });
    const result = await resolve();

    expect(result).toMatchObject({
      schema_version: 1,
      authorization_id: expect.stringMatching(/^obsauth_[0-9A-HJ-NP-TV-Z]{20}$/),
      binding_id: bindingId,
      binding_version: 1,
      credential_version: 1,
      effective_capture_mode: 'redacted_io',
      bundle: {
        adapter_type: 'otlp_http',
        auth: { type: 'basic', username: 'test-user', password: 'test-password-v1' },
      },
    });
    expect(secretStore.resolvedReferences).toEqual([initialSecretRef]);

    const [audit] = await auditRows();
    expect(audit).toMatchObject({
      actor: 'system:serviceaccount:orca:observability-exporter',
      authMethod: 'internal-service',
      action: 'agent_observability.credential_resolved',
      targetType: 'agent_observability_binding',
      targetId: bindingId,
      workspaceId,
      requestId: 'req_secret_resolver',
      result: 'success',
      metadata: {
        authorization_id: result.authorization_id,
        session_id: sessionId,
        binding_version: 1,
        credential_version: 1,
        selection_source: 'organization_default',
        effective_capture_mode: 'redacted_io',
      },
    });
    expect(JSON.stringify(audit)).not.toContain(initialSecretRef);
    expect(JSON.stringify(audit)).not.toContain('test-password-v1');
  });

  it('keeps a pre-restriction pin metadata-only after the current ceiling expands', async () => {
    await fixture.db
      .update(schema.agentObservabilityPlatformPolicy)
      .set({ maxCaptureMode: 'metadata_only', captureRestrictionEpoch: 1 })
      .where(eq(schema.agentObservabilityPlatformPolicy.id, 'default'));
    await fixture.db
      .update(schema.agentObservabilityPlatformPolicy)
      .set({ maxCaptureMode: 'redacted_io', captureRestrictionEpoch: 2 })
      .where(eq(schema.agentObservabilityPlatformPolicy.id, 'default'));

    const result = await resolve({ requestId: 'req_secret_resolver_sticky_capture' });
    expect(result).toMatchObject({
      credential_version: 1,
      effective_capture_mode: 'metadata_only',
      bundle: { auth: { type: 'basic', password: 'test-password-v1' } },
    });
    const [audit] = await auditRows();
    expect(audit).toMatchObject({
      requestId: 'req_secret_resolver_sticky_capture',
      metadata: { effective_capture_mode: 'metadata_only' },
    });
    expect(JSON.stringify(audit)).not.toContain(initialSecretRef);
    expect(JSON.stringify(audit)).not.toContain('test-password-v1');
  });

  it.each([
    {
      name: 'organization default basic',
      workspaceId,
      sessionId,
      bindingId,
      secretRef: initialSecretRef,
      bindingScope: 'organization',
      adapterType: 'otlp_http',
      protocol: 'http/protobuf',
      bundle: {
        adapter_type: 'otlp_http',
        auth: { type: 'basic', username: 'test-user', password: 'test-password-v1' },
      },
      secretFragments: ['test-user', 'test-password-v1'],
    },
    {
      name: 'workspace custom bearer',
      workspaceId: bearerWorkspaceId,
      sessionId: bearerSessionId,
      bindingId: bearerBindingId,
      secretRef: bearerSecretRef,
      bindingScope: 'workspace',
      adapterType: 'otlp_http',
      protocol: 'http/protobuf',
      bundle: {
        adapter_type: 'otlp_http',
        auth: { type: 'bearer', token: 'bearer-token-workspace' },
      },
      secretFragments: ['bearer-token-workspace'],
    },
    {
      name: 'workspace custom canonical headers',
      workspaceId: headersWorkspaceId,
      sessionId: headersSessionId,
      bindingId: headersBindingId,
      secretRef: headersSecretRef,
      bindingScope: 'workspace',
      adapterType: 'otlp_http',
      protocol: 'http/json',
      bundle: {
        adapter_type: 'otlp_http',
        auth: {
          type: 'custom_headers',
          headers: {
            'x-first': 'headers-secret-first',
            'x-second': 'headers-secret-second',
          },
        },
      },
      secretFragments: ['headers-secret-first', 'headers-secret-second'],
    },
    {
      name: 'organization pinned langfuse SDK',
      workspaceId: langfuseOrganizationWorkspaceId,
      sessionId: langfuseOrganizationSessionId,
      bindingId: langfuseOrganizationBindingId,
      secretRef: langfuseOrganizationSecretRef,
      bindingScope: 'organization',
      adapterType: 'langfuse_sdk',
      protocol: 'sdk',
      bundle: {
        adapter_type: 'langfuse_sdk',
        public_key: 'pk-langfuse-organization',
        secret_key: 'sk-langfuse-organization',
      },
      secretFragments: ['pk-langfuse-organization', 'sk-langfuse-organization'],
    },
    {
      name: 'workspace custom langfuse SDK',
      workspaceId: langfuseWorkspaceId,
      sessionId: langfuseWorkspaceSessionId,
      bindingId: langfuseWorkspaceBindingId,
      secretRef: langfuseWorkspaceSecretRef,
      bindingScope: 'workspace',
      adapterType: 'langfuse_sdk',
      protocol: 'sdk',
      bundle: {
        adapter_type: 'langfuse_sdk',
        public_key: 'pk-langfuse-workspace',
        secret_key: 'sk-langfuse-workspace',
      },
      secretFragments: ['pk-langfuse-workspace', 'sk-langfuse-workspace'],
    },
  ])(
    'releases $name through direct and HTTP paths without a fallback provider',
    async (testCase) => {
      const context = await loadAgentObservabilitySessionContext({
        db: fixture.db,
        workspaceId: testCase.workspaceId,
        sessionId: testCase.sessionId,
      });
      expect(context).toMatchObject({
        status: 'enabled',
        organization_id: organizationId,
        workspace_id: testCase.workspaceId,
        selection_source:
          testCase.bindingScope === 'workspace' ? 'workspace_custom' : 'organization_default',
        binding: {
          id: testCase.bindingId,
          version: 1,
          scope: testCase.bindingScope,
          target: { adapter_type: testCase.adapterType },
          config: { protocol: testCase.protocol },
          current_credential_version: 1,
        },
        capture: { effective_mode: 'redacted_io' },
      });
      expect(JSON.stringify(context)).not.toContain(testCase.secretRef);
      expect(JSON.stringify(context)).not.toContain('secret_ref');

      const direct = await resolve({
        workspaceId: testCase.workspaceId,
        sessionId: testCase.sessionId,
        requestId: `req_direct_${testCase.bindingId}`,
      });
      expect(direct).toMatchObject({
        binding_id: testCase.bindingId,
        binding_version: 1,
        credential_version: 1,
        effective_capture_mode: 'redacted_io',
        bundle: testCase.bundle,
      });
      expect(JSON.stringify(direct)).not.toContain(testCase.secretRef);
      expect(JSON.stringify(direct)).not.toContain('secret_ref');

      const fallback: SecretProvider = {
        resolve: vi.fn(async () => {
          throw new Error('fallback provider must never receive observability secret resolution');
        }),
      };
      const app = buildCombinedTestApp({
        db: fixture.db,
        oidc: { allowedIssuers: [], audience: 'test' },
        store: buildStubStore(),
        sse: STUB_SSE_CONFIG,
        jwtMinter: buildTestJwtMinter(),
        fileStore: buildStubFileStore(),
        skillStore: new InMemorySkillStore(),
        secretProvider: fallback,
        secretStore,
      });
      try {
        const response = await app.inject({
          method: 'POST',
          url:
            `/internal/v1/workspaces/${testCase.workspaceId}/sessions/${testCase.sessionId}` +
            '/agent-observability/secret/resolve',
          payload: {},
        });
        expect(response.statusCode).toBe(200);
        expect(response.headers['cache-control']).toBe('private, no-store');
        expect(response.json()).toMatchObject({
          binding_id: testCase.bindingId,
          binding_version: 1,
          credential_version: 1,
          effective_capture_mode: 'redacted_io',
          bundle: testCase.bundle,
        });
        expect(response.body).not.toContain(testCase.secretRef);
        expect(fallback.resolve).not.toHaveBeenCalled();
      } finally {
        await app.close();
      }

      const rows = await auditRows();
      expect(rows).toHaveLength(2);
      expect(rows).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            organizationId,
            workspaceId: testCase.workspaceId,
            actor: 'system:serviceaccount:orca:observability-exporter',
            targetId: testCase.bindingId,
            metadata: expect.objectContaining({
              binding_version: 1,
              credential_version: 1,
              session_id: testCase.sessionId,
              selection_source:
                testCase.bindingScope === 'workspace' ? 'workspace_custom' : 'organization_default',
              effective_capture_mode: 'redacted_io',
            }),
          }),
          expect.objectContaining({
            organizationId,
            workspaceId: testCase.workspaceId,
            actor: 'combined-test-internal',
            targetId: testCase.bindingId,
          }),
        ]),
      );
      const serializedAudit = JSON.stringify(rows);
      expect(serializedAudit).not.toContain(testCase.secretRef);
      for (const secretFragment of testCase.secretFragments) {
        expect(serializedAudit).not.toContain(secretFragment);
      }
    },
  );

  it('hides a cross-workspace pin without a store lookup or audit', async () => {
    await expect(resolve({ workspaceId: bearerWorkspaceId, sessionId })).rejects.toMatchObject({
      name: 'AgentObservabilitySecretResolutionNotFoundError',
    });
    const response = await resolveHttp({ workspaceId: bearerWorkspaceId, sessionId });
    expect(response.statusCode).toBe(404);
    expect(response.headers['cache-control']).toBe('private, no-store');
    expect(secretStore.resolvedReferences).toEqual([]);
    expect(await auditRows()).toEqual([]);
  });

  it.each([
    {
      name: 'disabled pin',
      workspaceId: disabledWorkspaceId,
      sessionId: disabledSessionId,
      apply: async () => {},
    },
    {
      name: 'archived Session pin',
      apply: async () => {
        await fixture.db
          .update(schema.sessionObservabilityBindings)
          .set({ status: 'archived', archivedAt: new Date() })
          .where(
            sql`${schema.sessionObservabilityBindings.workspaceId} = ${workspaceId} and ${schema.sessionObservabilityBindings.sessionId} = ${sessionId}`,
          );
      },
    },
    {
      name: 'deleted Session pin',
      apply: async () => {
        await fixture.db
          .update(schema.sessionObservabilityBindings)
          .set({ status: 'deleted', deletedAt: new Date(), sessionRevocationEpoch: 1 })
          .where(
            sql`${schema.sessionObservabilityBindings.workspaceId} = ${workspaceId} and ${schema.sessionObservabilityBindings.sessionId} = ${sessionId}`,
          );
      },
    },
    {
      name: 'organization archive',
      apply: async () => {
        await fixture.db
          .update(schema.organizations)
          .set({ status: 'archived' })
          .where(eq(schema.organizations.id, organizationId));
      },
    },
    {
      name: 'workspace archive',
      apply: async () => {
        await fixture.db
          .update(schema.workspaces)
          .set({ status: 'archived', archivedAt: new Date() })
          .where(eq(schema.workspaces.id, workspaceId));
      },
    },
    {
      name: 'organization-default revocation',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityOrganizationSettings)
          .set({ defaultRevocationEpoch: 1 })
          .where(eq(schema.agentObservabilityOrganizationSettings.organizationId, organizationId));
      },
    },
    {
      name: 'organization-wide revocation',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityOrganizationSettings)
          .set({ organizationRevocationEpoch: 1 })
          .where(eq(schema.agentObservabilityOrganizationSettings.organizationId, organizationId));
      },
    },
    {
      name: 'workspace revocation',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityWorkspaceSettings)
          .set({ revocationEpoch: 1 })
          .where(eq(schema.agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
      },
    },
    {
      name: 'binding revocation',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityBindings)
          .set({ revocationEpoch: 1, updatedAt: sql`now()` })
          .where(eq(schema.agentObservabilityBindings.id, bindingId));
      },
    },
    {
      name: 'disabled binding',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityBindings)
          .set({ status: 'disabled', updatedAt: sql`now()` })
          .where(eq(schema.agentObservabilityBindings.id, bindingId));
      },
    },
    {
      name: 'archived binding',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityBindings)
          .set({ status: 'archived', archivedAt: new Date(), updatedAt: sql`now()` })
          .where(eq(schema.agentObservabilityBindings.id, bindingId));
      },
    },
    {
      name: 'Session revocation',
      apply: async () => {
        await fixture.db
          .update(schema.sessionObservabilityBindings)
          .set({ sessionRevocationEpoch: 1 })
          .where(
            sql`${schema.sessionObservabilityBindings.workspaceId} = ${workspaceId} and ${schema.sessionObservabilityBindings.sessionId} = ${sessionId}`,
          );
      },
    },
    {
      name: 'platform adapter restriction',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityPlatformPolicy)
          .set({ allowedAdapters: ['langfuse_sdk'] })
          .where(eq(schema.agentObservabilityPlatformPolicy.id, 'default'));
      },
    },
    {
      name: 'platform endpoint-class restriction',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityPlatformPolicy)
          .set({ allowedEndpointClasses: ['private'] })
          .where(eq(schema.agentObservabilityPlatformPolicy.id, 'default'));
      },
    },
    {
      name: 'invalid pinned configuration',
      apply: async () => {
        await fixture.db
          .update(schema.agentObservabilityBindingVersions)
          .set({ semanticProfile: 'langfuse' })
          .where(
            sql`${schema.agentObservabilityBindingVersions.bindingId} = ${bindingId} and ${schema.agentObservabilityBindingVersions.version} = 1`,
          );
      },
    },
    {
      name: 'missing credential head',
      apply: async () => {
        await fixture.db
          .delete(schema.agentObservabilityBindingCredentials)
          .where(eq(schema.agentObservabilityBindingCredentials.bindingId, bindingId));
      },
    },
  ])('returns 409 for $name before SecretStore and audit', async (testCase) => {
    await testCase.apply();
    const input = {
      ...(testCase.workspaceId ? { workspaceId: testCase.workspaceId } : {}),
      ...(testCase.sessionId ? { sessionId: testCase.sessionId } : {}),
    };
    await expect(resolve(input)).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionDeniedError,
    );
    const response = await resolveHttp(input);
    expect(response.statusCode).toBe(409);
    expect(response.headers['cache-control']).toBe('private, no-store');
    expect(secretStore.resolvedReferences).toEqual([]);
    expect(await auditRows()).toEqual([]);
  });

  it('allows a draining binding before SecretStore resolution', async () => {
    await fixture.db
      .update(schema.agentObservabilityBindings)
      .set({ status: 'draining', updatedAt: sql`now()` })
      .where(eq(schema.agentObservabilityBindings.id, bindingId));
    await expect(resolve()).resolves.toMatchObject({
      binding_id: bindingId,
      credential_version: 1,
    });
    expect(secretStore.resolvedReferences).toEqual([initialSecretRef]);
    expect(await auditRows()).toHaveLength(1);
  });

  it('returns 409 before SecretStore for disabled or revoked authority', async () => {
    await fixture.db
      .update(schema.agentObservabilityBindings)
      .set({ status: 'disabled', updatedAt: sql`now()` })
      .where(eq(schema.agentObservabilityBindings.id, bindingId));

    await expect(resolve({ secretStore: undefined })).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionDeniedError,
    );
    expect(secretStore.resolvedReferences).toEqual([]);
    expect(await auditRows()).toEqual([]);
  });

  it('fails closed for absent SecretStore or malformed bundle without an audit', async () => {
    await expect(resolve({ secretStore: undefined })).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionUnavailableError,
    );
    expect(await auditRows()).toEqual([]);

    await secretStore.put(initialSecretRef, '{"not":"a canonical bundle"}');
    await expect(resolve()).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionUnavailableError,
    );
    expect(await auditRows()).toEqual([]);
  });

  it.each([
    {
      name: 'no SecretStore',
      store: undefined,
      marker: 'no-secret-store-marker',
    },
    {
      name: 'null SecretStore value',
      store: resolverOnlyStore(async () => null),
      marker: 'null-secret-store-marker',
    },
    {
      name: 'raw SecretStore rejection',
      store: resolverOnlyStore(async () => {
        throw new Error('raw-store-secret-marker');
      }),
      marker: 'raw-store-secret-marker',
    },
    {
      name: 'malformed bundle',
      store: resolverOnlyStore(async () => '{"not":"a canonical bundle"}'),
      marker: 'malformed-secret-marker',
    },
    {
      name: 'noncanonical bundle',
      store: resolverOnlyStore(
        async () =>
          '{"version":1,"adapter_type":"otlp_http","binding_id":"aob_secret_resolver","credential_version":1,"auth":{"type":"basic","username":"test-user","password":"noncanonical-secret-marker"}}',
      ),
      marker: 'noncanonical-secret-marker',
    },
    {
      name: 'oversized bundle',
      store: resolverOnlyStore(
        async () =>
          `{"secret":"${'oversized-secret-marker'.repeat(
            AGENT_OBSERVABILITY_SECRET_BUNDLE_MAX_BYTES,
          )}"}`,
      ),
      marker: 'oversized-secret-marker',
    },
    {
      name: 'wrong binding bundle',
      store: resolverOnlyStore(async () =>
        encodeAgentObservabilitySecretBundle({
          bindingId: 'aob_wrong_binding',
          credentialVersion: 1,
          adapterType: 'otlp_http',
          auth: { type: 'bearer', token: 'wrong-binding-secret-marker' },
        }),
      ),
      marker: 'wrong-binding-secret-marker',
    },
    {
      name: 'wrong credential generation bundle',
      store: resolverOnlyStore(async () =>
        encodeAgentObservabilitySecretBundle({
          bindingId,
          credentialVersion: 2,
          adapterType: 'otlp_http',
          auth: { type: 'bearer', token: 'wrong-generation-secret-marker' },
        }),
      ),
      marker: 'wrong-generation-secret-marker',
    },
    {
      name: 'wrong adapter bundle',
      store: resolverOnlyStore(async () =>
        encodeAgentObservabilitySecretBundle({
          bindingId,
          credentialVersion: 1,
          adapterType: 'langfuse_sdk',
          publicKey: 'pk-wrong-adapter',
          secretKey: 'wrong-adapter-secret-marker',
        }),
      ),
      marker: 'wrong-adapter-secret-marker',
    },
  ])('sanitizes $name without a success audit', async ({ store, marker }) => {
    const error = await resolve({ secretStore: store }).then(
      () => new Error('unexpected successful release'),
      (reason: unknown) => reason,
    );
    expect(error).toBeInstanceOf(AgentObservabilitySecretResolutionUnavailableError);
    expect(String((error as Error).message)).not.toContain(marker);
    expect(String((error as Error).message)).not.toContain(initialSecretRef);

    const response = await resolveHttp({ secretStore: store });
    expect(response.statusCode).toBe(503);
    expect(response.headers['cache-control']).toBe('private, no-store');
    expect(response.body).not.toContain(marker);
    expect(response.body).not.toContain(initialSecretRef);
    expect(await auditRows()).toEqual([]);
  });

  it('retries one coherent head rotation and audits only the new generation', async () => {
    let rotated = false;
    const originalResolve = secretStore.resolve.bind(secretStore);
    secretStore.resolve = async (reference) => {
      const value = await originalResolve(reference);
      if (!rotated) {
        rotated = true;
        await rotateHead(2, 'test-password-v2');
      }
      return value;
    };

    const result = await resolve();
    expect(result).toMatchObject({
      credential_version: 2,
      bundle: { auth: { type: 'basic', password: 'test-password-v2' } },
    });
    expect(secretStore.resolvedReferences).toHaveLength(2);
    const rows = await auditRows();
    expect(rows).toHaveLength(1);
    expect(rows[0]!.metadata).toMatchObject({ credential_version: 2 });
    expect(JSON.stringify(rows)).not.toContain('test-password-v1');
  });

  it('stops after three continuously changing heads without release or audit', async () => {
    let nextVersion = 2;
    const originalResolve = secretStore.resolve.bind(secretStore);
    secretStore.resolve = async (reference) => {
      const value = await originalResolve(reference);
      await rotateHead(nextVersion, `test-password-v${nextVersion}`);
      nextVersion += 1;
      return value;
    };

    await expect(resolve()).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionUnavailableError,
    );
    expect(secretStore.resolvedReferences).toHaveLength(3);
    expect(await auditRows()).toEqual([]);
  });

  it('denies a revocation that commits after resolve and before reauthorization', async () => {
    let revoked = false;
    const originalResolve = secretStore.resolve.bind(secretStore);
    secretStore.resolve = async (reference) => {
      const value = await originalResolve(reference);
      if (!revoked) {
        revoked = true;
        await fixture.db
          .update(schema.agentObservabilityOrganizationSettings)
          .set({ defaultRevocationEpoch: 1 })
          .where(eq(schema.agentObservabilityOrganizationSettings.organizationId, organizationId));
      }
      return value;
    };

    await expect(resolve()).rejects.toBeInstanceOf(AgentObservabilitySecretResolutionDeniedError);
    expect(secretStore.resolvedReferences).toEqual([initialSecretRef]);
    expect(await auditRows()).toEqual([]);
  });

  it.each([
    {
      name: 'target',
      mutate: async () => {
        await fixture.db
          .update(schema.agentObservabilityBindings)
          .set({ endpoint: 'https://changed-target.example/v1/traces', updatedAt: sql`now()` })
          .where(eq(schema.agentObservabilityBindings.id, bindingId));
      },
    },
    {
      name: 'pinned configuration',
      mutate: async () => {
        await fixture.db
          .update(schema.agentObservabilityBindingVersions)
          .set({ release: 'changed-without-version' })
          .where(
            sql`${schema.agentObservabilityBindingVersions.bindingId} = ${bindingId} and ${schema.agentObservabilityBindingVersions.version} = 1`,
          );
      },
    },
  ])(
    'fails closed when immutable $name changes without a pin-version change',
    async ({ mutate }) => {
      let changed = false;
      const originalResolve = secretStore.resolve.bind(secretStore);
      secretStore.resolve = async (reference, options) => {
        const value = await originalResolve(reference, options);
        if (!changed) {
          changed = true;
          await mutate();
        }
        return value;
      };

      await expect(resolve()).rejects.toBeInstanceOf(
        AgentObservabilitySecretResolutionUnavailableError,
      );
      expect(secretStore.resolvedReferences).toEqual([initialSecretRef]);
      expect(await auditRows()).toEqual([]);
    },
  );

  it('treats a partial credential-head change as corruption rather than a retry', async () => {
    const originalResolve = secretStore.resolve.bind(secretStore);
    secretStore.resolve = async (reference) => {
      const value = await originalResolve(reference);
      await fixture.db
        .update(schema.agentObservabilityBindingCredentials)
        .set({ credentialVersion: 2, rotatedAt: new Date(), updatedAt: sql`now()` })
        .where(eq(schema.agentObservabilityBindingCredentials.bindingId, bindingId));
      return value;
    };

    await expect(resolve()).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionUnavailableError,
    );
    expect(secretStore.resolvedReferences).toEqual([initialSecretRef]);
    expect(await auditRows()).toEqual([]);
  });

  it('does not release bytes when audit insertion rolls back', async () => {
    await fixture.db.execute(sql`
      create or replace function reject_observability_secret_audit() returns trigger as $$
      begin
        raise exception 'injected audit failure';
      end;
      $$ language plpgsql
    `);
    await fixture.db.execute(sql`
      create trigger reject_observability_secret_audit_trigger
      before insert on admin_audit_events
      for each row execute function reject_observability_secret_audit()
    `);
    try {
      await expect(resolve()).rejects.toBeInstanceOf(
        AgentObservabilitySecretResolutionUnavailableError,
      );
      expect(await auditRows()).toEqual([]);
    } finally {
      await fixture.db.execute(
        sql`drop trigger if exists reject_observability_secret_audit_trigger on admin_audit_events`,
      );
      await fixture.db.execute(sql`drop function if exists reject_observability_secret_audit()`);
    }
  });

  it('returns by its total deadline when a SecretStore ignores cancellation', async () => {
    const entered = deferred<void>();
    const neverSettles: SecretStore = {
      resolve: async () => {
        entered.resolve();
        return new Promise<string | null>(() => {});
      },
      put: async () => {},
      delete: async () => {},
    };
    const startedAt = Date.now();
    const release = resolve({ secretStore: neverSettles, deadlineMs: 50 });
    await entered.promise;
    await expect(release).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionUnavailableError,
    );
    expect(Date.now() - startedAt).toBeLessThan(500);
    expect(await auditRows()).toEqual([]);
  });

  it('honors an abort signal during SecretStore lookup without committing an audit', async () => {
    const entered = deferred<void>();
    const settleStore = deferred<string | null>();
    const ignoredSignalStore: SecretStore = {
      resolve: async () => {
        entered.resolve();
        return settleStore.promise;
      },
      put: async () => {},
      delete: async () => {},
    };
    const controller = new AbortController();
    const release = resolve({
      secretStore: ignoredSignalStore,
      signal: controller.signal,
      deadlineMs: 1_000,
    });
    await entered.promise;
    controller.abort(new Error('test client disconnected'));
    await expect(release).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionUnavailableError,
    );
    expect(await auditRows()).toEqual([]);
    settleStore.resolve(
      encodeAgentObservabilitySecretBundle({
        bindingId,
        credentialVersion: 1,
        adapterType: 'otlp_http',
        auth: { type: 'basic', username: 'test-user', password: 'late-secret' },
      }),
    );
    await Promise.resolve();
  });

  it('does not enter the first snapshot when its signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort(new Error('test client disconnected before authorization'));
    await expect(resolve({ signal: controller.signal })).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionUnavailableError,
    );
    expect(secretStore.resolvedReferences).toEqual([]);
    expect(await auditRows()).toEqual([]);
  });

  it.each([
    0,
    -1,
    1.5,
    Number.POSITIVE_INFINITY,
    AGENT_OBSERVABILITY_SECRET_RESOLUTION_DEADLINE_MS + 1,
  ])('rejects invalid bounded deadline override %s before resolution', async (deadlineMs) => {
    await expect(resolve({ deadlineMs })).rejects.toBeInstanceOf(
      AgentObservabilitySecretResolutionUnavailableError,
    );
    expect(secretStore.resolvedReferences).toEqual([]);
    expect(await auditRows()).toEqual([]);
  });

  it('bounds a real held Postgres lock, leaves no audit, and releases the pool', async () => {
    const holder = await beginCredentialHeadLock();
    try {
      const holderPid = await backendPid(holder);
      const startedAt = Date.now();
      const release = resolve({ deadlineMs: 500 });
      const blockedPid = await waitForBlockedBackend(holderPid);
      expect(blockedPid).toBeGreaterThan(0);
      await expect(release).rejects.toBeInstanceOf(
        AgentObservabilitySecretResolutionUnavailableError,
      );
      expect(Date.now() - startedAt).toBeLessThan(1_500);
      expect(secretStore.resolvedReferences).toEqual([]);
      expect(await auditRows()).toEqual([]);
      await waitForCondition(
        'the timed-out resolver transaction to release its PG lock wait',
        async () => {
          return (await blockedBackendPids(holderPid)).length === 0;
        },
      );
    } finally {
      await holder.query('ROLLBACK').catch(() => {});
      holder.release();
    }
    await expect(resolve()).resolves.toMatchObject({ credential_version: 1 });
  });

  it('retries a serialized first snapshot before any SecretStore lookup', async () => {
    const holder = await fixture.pool.connect();
    try {
      await holder.query('BEGIN');
      await holder.query(
        `
          UPDATE agent_observability_binding_credentials
          SET updated_at = now()
          WHERE binding_id = $1
        `,
        [bindingId],
      );
      const release = resolve({ deadlineMs: 2_000 });
      const blockedPid = await waitForBlockedBackend(await backendPid(holder));
      expect(blockedPid).toBeGreaterThan(0);
      await holder.query('COMMIT');

      await expect(release).resolves.toMatchObject({ credential_version: 1 });
      expect(secretStore.resolvedReferences).toEqual([initialSecretRef]);
      expect(await auditRows()).toHaveLength(1);
    } finally {
      await holder.query('ROLLBACK').catch(() => {});
      holder.release();
    }
  });

  it('retries a serialized reauthorization and releases only the rotated head', async () => {
    const rotatedRef = newAgentObservabilitySecretReference();
    await secretStore.put(
      rotatedRef,
      encodeAgentObservabilitySecretBundle({
        bindingId,
        credentialVersion: 2,
        adapterType: 'otlp_http',
        auth: { type: 'basic', username: 'test-user', password: 'test-password-v2' },
      }),
    );
    const holder = await fixture.pool.connect();
    const rotationReady = deferred<void>();
    let rotated = false;
    const originalResolve = secretStore.resolve.bind(secretStore);
    secretStore.resolve = async (reference, options) => {
      const value = await originalResolve(reference, options);
      if (!rotated) {
        rotated = true;
        await holder.query('BEGIN');
        await holder.query(
          `
            UPDATE agent_observability_binding_credentials
            SET secret_ref = $1,
                credential_version = 2,
                rotated_at = now(),
                updated_at = now()
            WHERE binding_id = $2
          `,
          [rotatedRef, bindingId],
        );
        rotationReady.resolve();
      }
      return value;
    };
    try {
      const release = resolve({ deadlineMs: 2_000 });
      await rotationReady.promise;
      const blockedPid = await waitForBlockedBackend(await backendPid(holder));
      expect(blockedPid).toBeGreaterThan(0);
      await holder.query('COMMIT');

      await expect(release).resolves.toMatchObject({
        credential_version: 2,
        bundle: { auth: { type: 'basic', password: 'test-password-v2' } },
      });
      expect(secretStore.resolvedReferences).toEqual([initialSecretRef, rotatedRef]);
      const rows = await auditRows();
      expect(rows).toHaveLength(1);
      expect(rows[0]!.metadata).toMatchObject({ credential_version: 2 });
      expect(JSON.stringify(rows)).not.toContain('test-password-v1');
    } finally {
      await holder.query('ROLLBACK').catch(() => {});
      holder.release();
    }
  });

  it.each([
    {
      name: 'organization-default revocation',
      mutate: (client: PoolClient) =>
        client.query(
          `
            UPDATE agent_observability_organization_settings
            SET default_revocation_epoch = 1
            WHERE organization_id = $1
          `,
          [organizationId],
        ),
    },
    {
      name: 'workspace revocation',
      mutate: (client: PoolClient) =>
        client.query(
          `
            UPDATE agent_observability_workspace_settings
            SET revocation_epoch = 1
            WHERE workspace_id = $1
          `,
          [workspaceId],
        ),
    },
    {
      name: 'binding revocation',
      mutate: (client: PoolClient) =>
        client.query(
          `
            UPDATE agent_observability_bindings
            SET revocation_epoch = 1, updated_at = now()
            WHERE id = $1
          `,
          [bindingId],
        ),
    },
    {
      name: 'Session revocation',
      mutate: (client: PoolClient) =>
        client.query(
          `
            UPDATE session_observability_bindings
            SET session_revocation_epoch = 1
            WHERE workspace_id = $1 AND session_id = $2
          `,
          [workspaceId, sessionId],
        ),
    },
    {
      name: 'workspace archive',
      mutate: (client: PoolClient) =>
        client.query(
          `UPDATE workspaces SET status = 'archived', archived_at = now() WHERE id = $1`,
          [workspaceId],
        ),
    },
  ])('linearizes a serialized $name as 409 with no release/audit', async ({ mutate }) => {
    const holder = await fixture.pool.connect();
    const mutationReady = deferred<void>();
    let mutationStarted = false;
    const originalResolve = secretStore.resolve.bind(secretStore);
    secretStore.resolve = async (reference, options) => {
      const value = await originalResolve(reference, options);
      if (!mutationStarted) {
        mutationStarted = true;
        await holder.query('BEGIN');
        try {
          await mutate(holder);
          mutationReady.resolve();
        } catch (error) {
          mutationReady.reject(error);
          throw error;
        }
      }
      return value;
    };
    try {
      const release = resolve({ deadlineMs: 2_000 });
      void release.catch(() => undefined);
      await mutationReady.promise;
      const blockedPid = await waitForBlockedBackend(await backendPid(holder));
      expect(blockedPid).toBeGreaterThan(0);
      await holder.query('COMMIT');

      await expect(release).rejects.toBeInstanceOf(AgentObservabilitySecretResolutionDeniedError);
      expect(secretStore.resolvedReferences).toEqual([initialSecretRef]);
      expect(await auditRows()).toEqual([]);
    } finally {
      await holder.query('ROLLBACK').catch(() => {});
      holder.release();
    }
  });

  it('uses strict route schema and never puts a secret resolver response in a cache', async () => {
    const app = buildCombinedTestApp({
      db: fixture.db,
      oidc: { allowedIssuers: [], audience: 'test' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      skillStore: new InMemorySkillStore(),
      secretStore,
    });
    const path =
      `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}` +
      '/agent-observability/secret/resolve';
    try {
      const missing = await app.inject({
        method: 'POST',
        url: path.replace(sessionId, 'ses_secret_resolver_missing'),
        payload: {},
      });
      expect(missing.statusCode).toBe(404);
      expect(missing.headers['cache-control']).toBe('private, no-store');
      expect(secretStore.resolvedReferences).toEqual([]);

      const response = await app.inject({ method: 'POST', url: path, payload: {} });
      expect(response.statusCode).toBe(200);
      expect(response.headers['cache-control']).toBe('private, no-store');
      expect(response.body).not.toContain(initialSecretRef);
      expect(response.json()).toMatchObject({ binding_id: bindingId, credential_version: 1 });

      const invalid = await app.inject({
        method: 'POST',
        url: path,
        payload: { secret_ref: 'nope' },
      });
      expect(invalid.statusCode).toBe(400);
      expect(invalid.headers['cache-control']).toBe('private, no-store');
      expect(invalid.json()).toEqual({ error: 'invalid agent observability secret request' });
    } finally {
      await app.close();
    }
  });

  it('exposes secret release only to exporter identity across real listener sockets', async () => {
    const audience = 'secret-resolver-socket-audience';
    const subjects = {
      exporter: 'system:serviceaccount:orca:observability-exporter',
      harness: 'system:serviceaccount:orca:harness',
      gateway: 'system:serviceaccount:orca:ai-gateway',
    };
    const verifier = new KubernetesServiceAccountAuthVerifier(
      {
        audience,
        harnessSubject: subjects.harness,
        aiGatewaySubject: subjects.gateway,
        observabilityExporterSubject: subjects.exporter,
      },
      async (token, requestedAudience) => {
        const username =
          token === 'exporter-token'
            ? subjects.exporter
            : token === 'harness-token'
              ? subjects.harness
              : token === 'gateway-token'
                ? subjects.gateway
                : undefined;
        return {
          authenticated: requestedAudience === audience && username !== undefined,
          ...(username ? { username } : {}),
          audiences: requestedAudience === audience ? [audience] : [],
        };
      },
    );
    const internal = buildInternalApp(
      {
        db: fixture.db,
        oidc: { allowedIssuers: [], audience: 'test' },
        store: buildStubStore(),
        sse: STUB_SSE_CONFIG,
        jwtMinter: buildTestJwtMinter(),
        fileStore: buildStubFileStore(),
        secretStore,
      },
      verifier,
    );
    const publicApp = buildPublicApp({
      db: fixture.db,
      oidc: { allowedIssuers: [], audience: 'test' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      skillStore: new InMemorySkillStore(),
      secretStore,
    });
    const adminApp = buildAdminApp({
      db: fixture.db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    const path =
      `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}` +
      '/agent-observability/secret/resolve';
    const baseUrl = (app: { server: { address(): string | AddressInfo | null } }) => {
      const address = app.server.address();
      if (address === null || typeof address === 'string') throw new Error('missing TCP address');
      return `http://127.0.0.1:${address.port}`;
    };
    const request = (base: string, token?: string, body: unknown = {}) =>
      fetch(`${base}${path}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          ...(token ? { authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(body),
      });
    try {
      await Promise.all([
        internal.listen({ host: '127.0.0.1', port: 0 }),
        publicApp.listen({ host: '127.0.0.1', port: 0 }),
        adminApp.listen({ host: '127.0.0.1', port: 0 }),
      ]);

      const exporter = await request(baseUrl(internal), 'exporter-token');
      expect(exporter.status).toBe(200);
      expect(exporter.headers.get('cache-control')).toBe('private, no-store');

      const unauthenticated = await request(baseUrl(internal));
      expect(unauthenticated.status).toBe(401);
      expect(unauthenticated.headers.get('cache-control')).toBe('private, no-store');

      for (const token of ['harness-token', 'gateway-token']) {
        const denied = await request(baseUrl(internal), token);
        expect(denied.status, token).toBe(403);
        expect(denied.headers.get('cache-control'), token).toBe('private, no-store');
      }
      const wrongMethod = await fetch(`${baseUrl(internal)}${path}`, {
        headers: { authorization: 'Bearer exporter-token' },
      });
      expect(wrongMethod.status).toBe(403);
      expect(wrongMethod.headers.get('cache-control')).toBe('private, no-store');
      const malformed = await request(baseUrl(internal), 'exporter-token', {
        selector: 'forbidden',
      });
      expect(malformed.status).toBe(400);
      expect(malformed.headers.get('cache-control')).toBe('private, no-store');
      const malformedJson = await fetch(`${baseUrl(internal)}${path}`, {
        method: 'POST',
        headers: {
          authorization: 'Bearer exporter-token',
          'content-type': 'application/json',
        },
        body: '{',
      });
      expect(malformedJson.status).toBe(400);
      expect(malformedJson.headers.get('cache-control')).toBe('private, no-store');

      for (const app of [publicApp, adminApp]) {
        const wrongListener = await request(baseUrl(app));
        expect(wrongListener.status).toBe(404);
        expect(wrongListener.headers.get('cache-control')).toBe('private, no-store');
      }
    } finally {
      await Promise.all([internal.close(), publicApp.close(), adminApp.close()]);
    }
  });

  it('aborts an actual exporter socket before audit release', async () => {
    const lookupStarted = deferred<void>();
    const abortObserved = deferred<void>();
    const lateStoreResult = deferred<string | null>();
    const blockingStore: SecretStore = {
      resolve: async (_reference, options) => {
        options?.signal?.addEventListener('abort', () => abortObserved.resolve(), { once: true });
        lookupStarted.resolve();
        return lateStoreResult.promise;
      },
      put: async () => {},
      delete: async () => {},
    };
    const internal = buildInternalApp(
      {
        db: fixture.db,
        oidc: { allowedIssuers: [], audience: 'test' },
        store: buildStubStore(),
        sse: STUB_SSE_CONFIG,
        jwtMinter: buildTestJwtMinter(),
        fileStore: buildStubFileStore(),
        secretStore: blockingStore,
      },
      {
        verify: async (token) =>
          token === 'exporter-abort-token'
            ? {
                caller: 'observability-exporter',
                subject: 'system:serviceaccount:orca:observability-exporter',
              }
            : null,
      },
    );
    try {
      await internal.listen({ host: '127.0.0.1', port: 0 });
      const address = internal.server.address();
      if (address === null || typeof address === 'string') throw new Error('missing TCP address');
      const clientClosed = deferred<void>();
      const request = httpRequest({
        host: '127.0.0.1',
        port: address.port,
        method: 'POST',
        path:
          `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}` +
          '/agent-observability/secret/resolve',
        headers: {
          authorization: 'Bearer exporter-abort-token',
          'content-type': 'application/json',
        },
      });
      request.once('error', () => {});
      request.once('close', () => clientClosed.resolve());
      request.end('{}');

      await lookupStarted.promise;
      request.destroy();
      await Promise.all([abortObserved.promise, clientClosed.promise]);
      expect(await auditRows()).toEqual([]);

      lateStoreResult.resolve(
        encodeAgentObservabilitySecretBundle({
          bindingId,
          credentialVersion: 1,
          adapterType: 'otlp_http',
          auth: { type: 'basic', username: 'test-user', password: 'late-socket-secret' },
        }),
      );
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(await auditRows()).toEqual([]);
    } finally {
      await internal.close();
    }
  });
});
