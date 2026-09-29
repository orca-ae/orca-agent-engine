// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { and, eq, sql } from 'drizzle-orm';
import {
  adminAuditEvents,
  agentObservabilityBindingCredentials,
  agentObservabilityBindingVersions,
  agentObservabilityBindings,
  agentObservabilityCredentialStagingIntents,
  agentObservabilityIdempotencyKeys,
  agentObservabilityMutationReservations,
  agentObservabilityOrganizationSettings,
  agentObservabilitySecretCleanupOutbox,
  organizations,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import {
  OrganizationAgentObservabilityMutationInvariantError,
  OrganizationAgentObservabilityMutationUnavailableError,
  loadOrganizationAgentObservabilityCredentialRotationHead,
  lockOrganizationAgentObservabilityMutationAuthority,
  parseOrganizationAgentObservabilityCredentialRotationRequest,
  parseOrganizationAgentObservabilityDisableRequest,
  parseOrganizationAgentObservabilityPutRequest,
  type NormalizedOrganizationAgentObservabilityPutRequest,
} from '../../src/domain/agent-observability-organization-mutation.js';
import { executeOrganizationAgentObservabilityPut } from '../../src/domain/agent-observability-organization-service.js';
import { executeOrganizationAgentObservabilityCaptureCeiling } from '../../src/domain/agent-observability-organization-ceiling.js';
import { executeOrganizationAgentObservabilityCredentialRotation } from '../../src/domain/agent-observability-organization-rotation-service.js';
import { executeOrganizationAgentObservabilityDisable } from '../../src/domain/agent-observability-organization-disable-service.js';
import {
  type OrganizationAgentObservabilityMutationExecutionHooks,
  type OrganizationAgentObservabilityMutationFailureEvent,
  type OrganizationAgentObservabilityMutationReporter,
} from '../../src/domain/agent-observability-organization-service-common.js';
import { agentObservabilityStateEtag } from '../../src/domain/agent-observability-etag.js';
import { loadOrganizationAgentObservabilityState } from '../../src/domain/agent-observability-state.js';
import {
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  agentObservabilityMutationTargetKey,
  reconcileAgentObservabilityStagingIntents,
  type AgentObservabilityMutationTarget,
} from '../../src/domain/agent-observability-mutations.js';
import { newAgentObservabilitySecretReference } from '../../src/domain/agent-observability-secrets.js';
import type { DbClient, DbTransaction } from '../../src/persistence/postgres/client.js';
import type { SecretStore } from '../../src/secrets/secret-provider.js';
import { closeTestDb, getTestDb } from './setup.js';

describe('organization agent observability mutation service', () => {
  let db: DbClient;
  let organizationId: string;
  let secretStore: TrackingSecretStore;

  beforeAll(async () => {
    ({ db } = await getTestDb());
  });

  beforeEach(async () => {
    organizationId = nextOrganizationId();
    await insertOrganization(db, organizationId);
    secretStore = new TrackingSecretStore();
  });

  afterAll(async () => {
    await closeTestDb();
  });

  it('updates only the ceiling while preserving an existing default and its credential head', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-ceiling-preservation')),
      'default-before-ceiling',
      null,
    );
    expect(created.kind).toBe('success');
    if (created.kind !== 'success') throw new Error('default not created');
    const writes = secretStore.putCalls;
    const changed = await executeOrganizationAgentObservabilityCaptureCeiling({
      db,
      organizationId,
      principal: 'test-admin',
      authMethod: 'api_key',
      requestId: 'ceiling-preservation',
      captureCeiling: 'raw_io',
      ifMatch: await currentEtag(),
      idempotencyKey: 'ceiling-only',
    });
    expect(changed.kind).toBe('success');
    if (changed.kind !== 'success') throw new Error('ceiling not changed');
    expect(changed.body.configured.default_binding).toEqual(
      created.body.configured.default_binding,
    );
    expect(changed.body.configured.capture_ceiling).toBe('raw_io');
    expect(secretStore.putCalls).toBe(writes);
  });

  it('replays after a duplicate begins while first finalization owns authority', async () => {
    const finalizationLocked = deferred<void>();
    const releaseFinalization = deferred<void>();
    const duplicateStarted = deferred<void>();
    const request = parseOrganizationAgentObservabilityPutRequest(
      requestBody('secret-finalization-race'),
    );

    const first = execute(request, 'finalization-race-key', null, {
      afterFinalizationAuthorityLocked: async () => {
        finalizationLocked.resolve();
        await releaseFinalization.promise;
      },
    });
    await finalizationLocked.promise;
    const duplicate = execute(request, 'finalization-race-key', null, {
      beforeAcquisitionAuthorityLock: async () => {
        duplicateStarted.resolve();
      },
    });
    await duplicateStarted.promise;
    releaseFinalization.resolve();

    const [created, replayed] = await Promise.all([first, duplicate]);
    expect(created).toMatchObject({ kind: 'success', status: 201 });
    expect(replayed).toEqual(created);
    expect(secretStore.putCalls).toBe(1);
  });

  it('does not serialize independent organizations behind platform policy', async () => {
    const firstLocked = deferred<void>();
    const releaseFirst = deferred<void>();
    const secondLocked = deferred<void>();
    const releaseSecond = deferred<void>();
    const otherOrganizationId = nextOrganizationId();
    await insertOrganization(db, otherOrganizationId);

    const first = execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-org-a')),
      'cross-org-a',
      null,
      {
        afterAcquisitionAuthorityLocked: async () => {
          firstLocked.resolve();
          await releaseFirst.promise;
        },
      },
    );
    await firstLocked.promise;

    const second = executeForOrganization(
      otherOrganizationId,
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-org-b')),
      'cross-org-b',
      null,
      {
        afterAcquisitionAuthorityLocked: async () => {
          secondLocked.resolve();
          await releaseSecond.promise;
        },
      },
    );
    await secondLocked.promise;
    releaseSecond.resolve();
    releaseFirst.resolve();

    await expect(Promise.all([first, second])).resolves.toEqual([
      expect.objectContaining({ kind: 'success', status: 201 }),
      expect.objectContaining({ kind: 'success', status: 201 }),
    ]);
  });

  it('reports correlated unexpected finalization failures without reflecting provider or secret data', async () => {
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const result = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-finalization-failure')),
      'finalization-failure',
      null,
      {
        afterFinalizationAuthorityLocked: async () => {
          throw new Error('provider echoed secret-finalization-failure');
        },
      },
      { report: (event) => reports.push(event) },
    );

    expect(result).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'finalization',
        code: 'unexpected_database_or_programmer',
        requestId: 'request_finalization-failure',
        organizationId,
        message:
          'registry-service-ts failed organization agent observability mutation finalization',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-finalization-failure');
    expect(JSON.stringify(reports)).not.toContain('provider echoed');
  });

  it('reports once after bounded finalization serialization retries exhaust', async () => {
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const attempts: number[] = [];
    const key = 'finalization-serialization-exhausted';
    const result = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-finalization-serialization-exhausted'),
      ),
      key,
      null,
      {
        beforeFinalizationTransaction: async ({ attempt }) => {
          attempts.push(attempt);
          throw postgresError('40001');
        },
      },
      { report: (event) => reports.push(event) },
    );

    expect(result).toEqual({ kind: 'unavailable' });
    expect(attempts).toEqual([1, 2, 3]);
    expect(reports).toEqual([
      {
        phase: 'finalization',
        code: 'unexpected_database_or_programmer',
        requestId: `request_${key}`,
        organizationId,
        message:
          'registry-service-ts failed organization agent observability mutation finalization',
      },
    ]);
    expect(await reservationForIdempotencyKey(key)).toMatchObject({ status: 'fenced' });
  });

  it('reports a disable finalization hook failure in the finalization phase', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-finalization-failure'),
      ),
      'disable-finalization-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];

    const result = await disable(
      'disable-finalization-failure',
      null,
      {
        afterFinalizationAuthorityLocked: async () => {
          throw new Error('provider echoed secret-disable-finalization-failure');
        },
      },
      { report: (event) => reports.push(event) },
    );

    expect(result).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'finalization',
        code: 'unexpected_database_or_programmer',
        requestId: 'request_disable-finalization-failure',
        organizationId,
        message:
          'registry-service-ts failed organization agent observability mutation finalization',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-disable-finalization-failure');
  });

  it('does not suppress a finalization availability exception from disable telemetry', async () => {
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];

    const result = await disable(
      'disable-finalization-unavailable',
      null,
      {
        afterFinalizationAuthorityLocked: async () => {
          throw new OrganizationAgentObservabilityMutationUnavailableError();
        },
      },
      { report: (event) => reports.push(event) },
    );

    expect(result).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'finalization',
        code: 'unexpected_database_or_programmer',
        requestId: 'request_disable-finalization-unavailable',
        organizationId,
        message:
          'registry-service-ts failed organization agent observability mutation finalization',
      },
    ]);
  });

  it('resets disable failure phase for each serialization retry', async () => {
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const attempts: number[] = [];

    const result = await disable(
      'disable-retry-phase-reset',
      null,
      {
        beforeAcquisitionAuthorityLock: async () => {
          attempts.push(attempts.length + 1);
          if (attempts.length === 2) {
            throw new Error('second retry acquisition failure');
          }
        },
        afterFinalizationAuthorityLocked: async () => {
          if (attempts.length === 1) throw postgresError('40001');
        },
      },
      { report: (event) => reports.push(event) },
    );

    expect(result).toEqual({ kind: 'unavailable' });
    expect(attempts).toEqual([1, 2]);
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'unexpected_database_or_programmer',
        requestId: 'request_disable-retry-phase-reset',
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);
  });

  it('emits a structured default operator signal without provider or secret data', async () => {
    const operatorSignal = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    try {
      const result = await execute(
        parseOrganizationAgentObservabilityPutRequest(requestBody('secret-default-reporter')),
        'default-reporter',
        null,
        {
          afterFinalizationAuthorityLocked: async () => {
            throw new Error('provider echoed secret-default-reporter');
          },
        },
      );

      expect(result).toEqual({ kind: 'unavailable' });
      expect(operatorSignal).toHaveBeenCalledOnce();
      const [signal] = operatorSignal.mock.calls[0]!;
      expect(typeof signal).toBe('string');
      expect(JSON.parse(signal as string)).toEqual({
        phase: 'finalization',
        code: 'unexpected_database_or_programmer',
        requestId: 'request_default-reporter',
        organizationId,
        message:
          'registry-service-ts failed organization agent observability mutation finalization',
      });
      expect(signal).not.toContain('secret-default-reporter');
      expect(signal).not.toContain('provider echoed');
    } finally {
      operatorSignal.mockRestore();
    }
  });

  it('fences a timed-out staging write without waiting for a non-cooperative SecretStore', async () => {
    const hangingStore = new HangingSecretStore();
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const result = await executeOrganizationAgentObservabilityPut({
      db,
      secretStore: hangingStore,
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request-staging-timeout',
      request: parseOrganizationAgentObservabilityPutRequest(requestBody('secret-staging-timeout')),
      idempotencyKey: 'staging-timeout',
      ifMatch: null,
      stagingWriteTimeoutMs: 50,
      reporter: { report: (event) => reports.push(event) },
    });

    expect(result).toEqual({ kind: 'unavailable' });
    expect(hangingStore.signal?.aborted).toBe(true);
    expect(reports).toEqual([
      {
        phase: 'staging_write',
        code: 'staging_timeout',
        requestId: 'request-staging-timeout',
        organizationId,
        message: 'registry-service-ts failed organization agent observability staging write',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-staging-timeout');
    const [reservation] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.organizationId, organizationId));
    const [staging] = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation!.id));
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
  });

  it('reports failed staging writes with a safe code and correlation', async () => {
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const result = await executeOrganizationAgentObservabilityPut({
      db,
      secretStore: new FailingSecretStore(),
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request-staging-failure',
      request: parseOrganizationAgentObservabilityPutRequest(requestBody('secret-staging-failure')),
      idempotencyKey: 'staging-failure',
      ifMatch: null,
      reporter: { report: (event) => reports.push(event) },
    });

    expect(result).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'staging_write',
        code: 'staging_failure',
        requestId: 'request-staging-failure',
        organizationId,
        message: 'registry-service-ts failed organization agent observability staging write',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-staging-failure');
  });

  it('reports fence failures without replacing the unavailable typed outcome', async () => {
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const result = await executeOrganizationAgentObservabilityPut({
      db,
      secretStore: new FailingSecretStore(),
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request-fence-failure',
      request: parseOrganizationAgentObservabilityPutRequest(requestBody('secret-fence-failure')),
      idempotencyKey: 'fence-failure',
      ifMatch: null,
      hooks: {
        beforeFencing: async () => {
          throw new Error('provider echoed secret-fence-failure');
        },
      },
      reporter: { report: (event) => reports.push(event) },
    });

    expect(result).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'staging_write',
        code: 'staging_failure',
        requestId: 'request-fence-failure',
        organizationId,
        message: 'registry-service-ts failed organization agent observability staging write',
      },
      {
        phase: 'fence',
        code: 'fence_failure',
        requestId: 'request-fence-failure',
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation fence',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-fence-failure');
    expect(JSON.stringify(reports)).not.toContain('provider echoed');
  });

  it('treats a corrupt cached response as unavailable instead of a request error', async () => {
    const request = parseOrganizationAgentObservabilityPutRequest(
      requestBody('secret-corrupt-cache'),
    );
    await expect(execute(request, 'corrupt-cache', null)).resolves.toMatchObject({
      kind: 'success',
      status: 201,
    });
    await db
      .update(agentObservabilityIdempotencyKeys)
      .set({ responseBody: { corrupt: true } })
      .where(eq(agentObservabilityIdempotencyKeys.organizationId, organizationId));

    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    await expect(
      execute(request, 'corrupt-cache', null, undefined, {
        report: (event) => reports.push(event),
      }),
    ).resolves.toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'corrupt_cache',
        requestId: 'request_corrupt-cache',
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-corrupt-cache');
  });

  it('reports invalid authoritative cached responses as unavailable', async () => {
    const request = parseOrganizationAgentObservabilityPutRequest(
      requestBody('secret-invalid-authoritative-response'),
    );
    await expect(execute(request, 'invalid-authoritative-response', null)).resolves.toMatchObject({
      kind: 'success',
      status: 201,
    });
    await db
      .update(agentObservabilityIdempotencyKeys)
      .set({ responseStatus: 204 })
      .where(eq(agentObservabilityIdempotencyKeys.organizationId, organizationId));

    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    await expect(
      execute(request, 'invalid-authoritative-response', null, undefined, {
        report: (event) => reports.push(event),
      }),
    ).resolves.toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'invalid_authoritative_response',
        requestId: 'request_invalid-authoritative-response',
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-invalid-authoritative-response');
  });

  it('maps internal staging invariants to a safe unavailable outcome', async () => {
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const result = await executeOrganizationAgentObservabilityPut({
      db,
      secretStore,
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request-invariant-violation',
      request: parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-invariant-violation'),
      ),
      idempotencyKey: 'invariant-violation',
      ifMatch: null,
      stagingWriteTimeoutMs: 0,
      reporter: { report: (event) => reports.push(event) },
    });

    expect(result).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'staging_write',
        code: 'invariant_violation',
        requestId: 'request-invariant-violation',
        organizationId,
        message: 'registry-service-ts failed organization agent observability staging write',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-invariant-violation');
  });

  it('reports malformed authoritative credential references as an invariant without staging', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-invalid-head-create')),
      'invalid-head-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    const etag = await currentEtag();
    await db
      .update(agentObservabilityBindingCredentials)
      .set({ secretRef: 'not-a-valid-agent-observability-reference' })
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const result = await rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-invalid-head-next' },
      }),
      'invalid-head',
      etag,
      undefined,
      { report: (event) => reports.push(event) },
    );

    expect(result).toEqual({ kind: 'unavailable' });
    expect(secretStore.putCalls).toBe(1);
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'invariant_violation',
        requestId: 'request_invalid-head',
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-invalid-head-next');
    expect(JSON.stringify(reports)).not.toContain('not-a-valid-agent-observability-reference');
  });

  it('reports a credential generation that cannot advance in PostgreSQL as an invariant', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-maximum-head-create')),
      'maximum-head-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    await db
      .update(agentObservabilityBindingCredentials)
      .set({ credentialVersion: 2_147_483_647, updatedAt: new Date() })
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const result = await rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-maximum-head-next' },
      }),
      'maximum-head',
      await currentEtag(),
      undefined,
      { report: (event) => reports.push(event) },
    );

    expect(result).toEqual({ kind: 'unavailable' });
    expect(secretStore.putCalls).toBe(1);
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'invariant_violation',
        requestId: 'request_maximum-head',
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);
  });

  it('keeps a valid default without a configured credential head as a rotation conflict', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-unconfigured-head-create')),
      'unconfigured-head-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    await db
      .delete(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const result = await rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-unconfigured-head-next' },
      }),
      'unconfigured-head',
      await currentEtag(),
      undefined,
      { report: (event) => reports.push(event) },
    );

    expect(result).toEqual({ kind: 'conflict' });
    expect(secretStore.putCalls).toBe(1);
    expect(reports).toEqual([]);
  });

  it('rejects a missing exact credential row when an authority snapshot claims a head', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-missing-exact-head-create'),
      ),
      'missing-exact-head-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    const authority = await db.transaction((tx) =>
      lockOrganizationAgentObservabilityMutationAuthority(tx, organizationId),
    );
    await db
      .delete(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    await expect(
      db.transaction((tx) =>
        loadOrganizationAgentObservabilityCredentialRotationHead(tx, authority),
      ),
    ).rejects.toBeInstanceOf(OrganizationAgentObservabilityMutationInvariantError);
  });

  it('rejects a credential generation that diverges from its authority snapshot', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-divergent-exact-head-create'),
      ),
      'divergent-exact-head-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    const authority = await db.transaction((tx) =>
      lockOrganizationAgentObservabilityMutationAuthority(tx, organizationId),
    );
    await db
      .update(agentObservabilityBindingCredentials)
      .set({ credentialVersion: 2, updatedAt: new Date() })
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    await expect(
      db.transaction((tx) =>
        loadOrganizationAgentObservabilityCredentialRotationHead(tx, authority),
      ),
    ).rejects.toBeInstanceOf(OrganizationAgentObservabilityMutationInvariantError);
  });

  it('classifies a corrupt route-scoped replay reservation as corrupt_cache', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-corrupt-rotation-cache-create'),
      ),
      'corrupt-rotation-cache-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const request = parseOrganizationAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'secret-corrupt-rotation-cache-next' },
    });
    const key = 'corrupt-rotation-cache';
    const rotated = await rotate(request, key, await currentEtag());
    expect(rotated).toMatchObject({ kind: 'success', status: 200 });
    const [idempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, key),
        ),
      );
    await db
      .update(agentObservabilityMutationReservations)
      .set({ ownerPrincipal: 'corrupt-route-scoped-owner' })
      .where(eq(agentObservabilityMutationReservations.id, idempotency!.reservationId));

    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const replay = await rotate(request, key, null, undefined, {
      report: (event) => reports.push(event),
    });

    expect(replay).toEqual({ kind: 'unavailable' });
    expect(secretStore.putCalls).toBe(2);
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'corrupt_cache',
        requestId: `request_${key}`,
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-corrupt-rotation-cache-next');
  });

  it('classifies noncanonical persisted route-scoped target keys as corrupt_cache', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-corrupt-target-key-create'),
      ),
      'corrupt-target-key-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const request = parseOrganizationAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'secret-corrupt-target-key-next' },
    });
    const key = 'corrupt-target-key';
    const rotated = await rotate(request, key, await currentEtag());
    expect(rotated).toMatchObject({ kind: 'success', status: 200 });
    const [idempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, key),
        ),
      );
    await Promise.all([
      db
        .update(agentObservabilityIdempotencyKeys)
        .set({ targetKey: 'corrupt-route-scoped-target-key' })
        .where(
          and(
            eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
            eq(agentObservabilityIdempotencyKeys.key, key),
          ),
        ),
      db
        .update(agentObservabilityMutationReservations)
        .set({ targetKey: 'corrupt-route-scoped-target-key' })
        .where(eq(agentObservabilityMutationReservations.id, idempotency!.reservationId)),
    ]);

    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const replay = await rotate(request, key, null, undefined, {
      report: (event) => reports.push(event),
    });

    expect(replay).toEqual({ kind: 'unavailable' });
    expect(secretStore.putCalls).toBe(2);
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'corrupt_cache',
        requestId: `request_${key}`,
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);
  });

  it('rejects a pending idempotency row paired with a committed reservation as corrupt_cache', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-pending-committed-create')),
      'pending-committed-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const request = parseOrganizationAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'secret-pending-committed-next' },
    });
    const key = 'pending-committed';
    const gate = deferred<void>();
    const stagingStarted = deferred<void>();
    secretStore.beforePut = async () => {
      if (secretStore.putCalls === 2) {
        stagingStarted.resolve();
        await gate.promise;
      }
    };
    const first = rotate(request, key, await currentEtag(), undefined, { report: () => {} });
    await stagingStarted.promise;
    const [idempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, key),
        ),
      );
    await db
      .update(agentObservabilityMutationReservations)
      .set({ status: 'committed', committedAt: new Date(), updatedAt: new Date() })
      .where(eq(agentObservabilityMutationReservations.id, idempotency!.reservationId));

    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const retry = await rotate(request, key, null, undefined, {
      report: (event) => reports.push(event),
    });
    expect(retry).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'corrupt_cache',
        requestId: `request_${key}`,
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);

    await db
      .update(agentObservabilityMutationReservations)
      .set({ status: 'pending', committedAt: null, updatedAt: new Date() })
      .where(eq(agentObservabilityMutationReservations.id, idempotency!.reservationId));
    gate.resolve();
    await expect(first).resolves.toMatchObject({ kind: 'success', status: 200 });
  });

  it('fences a staged rotation when a competing credential generation wins', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-credential-race-create')),
      'credential-race-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    const gate = deferred<void>();
    const stagingStarted = deferred<void>();
    secretStore.beforePut = async () => {
      if (secretStore.putCalls === 2) {
        stagingStarted.resolve();
        await gate.promise;
      }
    };
    const rotation = rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-credential-race-next' },
      }),
      'credential-race',
      await currentEtag(),
    );
    await stagingStarted.promise;
    await db
      .update(agentObservabilityBindingCredentials)
      .set({ credentialVersion: 2, updatedAt: new Date() })
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    gate.resolve();

    await expect(rotation).resolves.toEqual({ kind: 'conflict' });
    const [head] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    expect(head).toMatchObject({ credentialVersion: 2 });
    const [reservation] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.targetBindingId, bindingId));
    expect(reservation).toMatchObject({ status: 'fenced' });
  });

  it('returns conflict before a missing If-Match for an optimistic live-pending rotation retry', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-rotation-pending-create')),
      'rotation-pending-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const etag = await currentEtag();
    const gate = deferred<void>();
    const started = deferred<void>();
    secretStore.beforePut = async () => {
      started.resolve();
      await gate.promise;
    };
    const request = parseOrganizationAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'secret-rotation-pending-next' },
    });

    const first = rotate(request, 'rotation-pending-key', etag);
    await started.promise;
    const duplicate = await rotate(request, 'rotation-pending-key', null);
    expect(duplicate).toEqual({ kind: 'conflict' });
    gate.resolve();

    await expect(first).resolves.toMatchObject({ kind: 'success', status: 200 });
    const replay = await rotate(request, 'rotation-pending-key', null);
    expect(replay).toMatchObject({ kind: 'success', status: 200 });
    expect(secretStore.putCalls).toBe(2);
  });

  it('returns conflict before a stale If-Match when a live rotation appears after optimistic lookup', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-rotation-inner-pending-create'),
      ),
      'rotation-inner-pending-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const etag = await currentEtag();
    const request = parseOrganizationAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'secret-rotation-inner-pending-next' },
    });
    const optimisticLookupComplete = deferred<void>();
    const releaseRetry = deferred<void>();
    const stagingStarted = deferred<void>();
    const releaseStaging = deferred<void>();
    secretStore.beforePut = async () => {
      stagingStarted.resolve();
      await releaseStaging.promise;
    };

    const retry = rotate(request, 'rotation-inner-pending-key', '"stale"', {
      beforeAcquisitionAuthorityLock: async () => {
        optimisticLookupComplete.resolve();
        await releaseRetry.promise;
      },
    });
    await optimisticLookupComplete.promise;

    const first = rotate(request, 'rotation-inner-pending-key', etag);
    await stagingStarted.promise;
    releaseRetry.resolve();
    await expect(retry).resolves.toEqual({ kind: 'conflict' });

    releaseStaging.resolve();
    await expect(first).resolves.toMatchObject({ kind: 'success', status: 200 });
  });

  it('rebinds an expired pending rotation after its precondition succeeds', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-rotation-expired-create')),
      'rotation-expired-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const etag = await currentEtag();
    const request = parseOrganizationAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'secret-rotation-expired-next' },
    });
    const stagingStarted = deferred<void>();
    const releaseStaging = deferred<void>();
    let firstWrite = true;
    secretStore.beforePut = async () => {
      if (!firstWrite) return;
      firstWrite = false;
      stagingStarted.resolve();
      await releaseStaging.promise;
    };

    const first = rotate(request, 'rotation-expired-key', etag, undefined, { report: () => {} });
    await stagingStarted.promise;
    const [pendingIdempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, 'rotation-expired-key'),
        ),
      );
    expect(pendingIdempotency).toBeDefined();
    const expiredReservationId = pendingIdempotency!.reservationId;
    const [pendingReservation] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.id, expiredReservationId));
    expect(pendingReservation).toBeDefined();
    await db
      .update(agentObservabilityMutationReservations)
      .set({
        expiresAt: new Date(pendingReservation!.createdAt.getTime() + 1),
        updatedAt: new Date(),
      })
      .where(eq(agentObservabilityMutationReservations.id, expiredReservationId));
    await new Promise((resolve) => setTimeout(resolve, 5));

    try {
      const recovered = await rotate(request, 'rotation-expired-key', etag);
      expect(recovered).toMatchObject({ kind: 'success', status: 200 });
      const [reboundIdempotency] = await db
        .select()
        .from(agentObservabilityIdempotencyKeys)
        .where(
          and(
            eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
            eq(agentObservabilityIdempotencyKeys.key, 'rotation-expired-key'),
          ),
        );
      expect(reboundIdempotency!.reservationId).not.toBe(expiredReservationId);
    } finally {
      releaseStaging.resolve();
    }
    await expect(first).resolves.toEqual({ kind: 'conflict' });
  });

  it('fences a staged rotation when same-target PUT commits a newer config generation', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-rotation-policy-create')),
      'rotation-policy-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    const etag = await currentEtag();
    const gate = deferred<void>();
    const started = deferred<void>();
    secretStore.beforePut = async () => {
      if (secretStore.putCalls === 2) {
        started.resolve();
        await gate.promise;
      }
    };
    const rotation = rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: {
          type: 'basic',
          username: 'project',
          password: 'secret-rotation-policy-next',
        },
      }),
      'rotation-policy-key',
      etag,
    );
    await started.promise;

    const policy = await execute(
      parseOrganizationAgentObservabilityPutRequest({
        ...requestBody('ignored'),
        credentials: undefined,
        config: { ...requestBody('ignored').config, compression: 'gzip' },
      }),
      'rotation-policy-winner',
      etag,
    );
    expect(policy).toMatchObject({ kind: 'success', status: 200 });
    gate.resolve();
    await expect(rotation).resolves.toEqual({ kind: 'conflict' });

    const [head] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    expect(head).toMatchObject({ credentialVersion: 1 });
    const [reservation] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.targetBindingId, bindingId));
    const [staging] = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation!.id));
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
    expect(secretStore.deleteCalls).toBe(0);
  });

  it('fences a staged rotation when PUT replaces the current default binding', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-rotation-replace-create')),
      'rotation-replace-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const oldBindingId = created.body.configured.default_binding!.id;
    const etag = await currentEtag();
    const gate = deferred<void>();
    const started = deferred<void>();
    secretStore.beforePut = async () => {
      if (secretStore.putCalls === 2) {
        started.resolve();
        await gate.promise;
      }
    };
    const rotation = rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-rotation-replace-next' },
      }),
      'rotation-replace-key',
      etag,
    );
    await started.promise;

    const replacement = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody(
          'secret-rotation-replace-winner',
          'https://collector.example/v1/traces-replaced',
        ),
      ),
      'rotation-replace-winner',
      etag,
    );
    expect(replacement).toMatchObject({ kind: 'success', status: 200 });
    gate.resolve();
    await expect(rotation).resolves.toEqual({ kind: 'conflict' });

    const [head] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, oldBindingId));
    expect(head).toMatchObject({ credentialVersion: 1 });
    const [reservation] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.targetBindingId, oldBindingId));
    const [staging] = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation!.id));
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
    expect(secretStore.deleteCalls).toBe(0);
    const stagedRef = staging!.secretRef;
    await reconcileAgentObservabilityStagingIntents(db, secretStore, {
      now: new Date(Date.now() + 21 * 60 * 1_000),
    });
    expect(secretStore.deleteCalls).toBeGreaterThanOrEqual(1);
    expect(await secretStore.resolve(stagedRef)).toBeNull();
    expect(
      await db
        .select()
        .from(agentObservabilitySecretCleanupOutbox)
        .where(eq(agentObservabilitySecretCleanupOutbox.bindingId, oldBindingId)),
    ).toEqual([]);
  });

  it('sanitizes missing, failed, and timed-out rotation SecretStore writes', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-rotation-store-create')),
      'rotation-store-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const request = parseOrganizationAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'secret-rotation-store-next' },
    });
    const etag = await currentEtag();
    const before = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.organizationId, organizationId));

    const missing = await executeOrganizationAgentObservabilityCredentialRotation({
      db,
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request-rotation-store-missing',
      request,
      idempotencyKey: 'rotation-store-missing',
      ifMatch: etag,
    });
    expect(missing).toEqual({ kind: 'unavailable' });
    expect(
      (
        await db
          .select()
          .from(agentObservabilityMutationReservations)
          .where(eq(agentObservabilityMutationReservations.organizationId, organizationId))
      ).map((row) => row.id),
    ).toEqual(before.map((row) => row.id));

    const failing = new FailingSecretStore();
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const failed = await executeOrganizationAgentObservabilityCredentialRotation({
      db,
      secretStore: failing,
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request-rotation-store-failed',
      request,
      idempotencyKey: 'rotation-store-failed',
      ifMatch: etag,
      reporter: { report: (event) => reports.push(event) },
    });
    expect(failed).toEqual({ kind: 'unavailable' });
    expect(failing.deleteCalls).toBe(0);
    expect(reports).toEqual([
      {
        phase: 'staging_write',
        code: 'staging_failure',
        requestId: 'request-rotation-store-failed',
        organizationId,
        message: 'registry-service-ts failed organization agent observability staging write',
      },
    ]);
    expect(JSON.stringify(reports)).not.toContain('secret-rotation-store-next');

    const hanging = new HangingSecretStore();
    const timedOut = await executeOrganizationAgentObservabilityCredentialRotation({
      db,
      secretStore: hanging,
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request-rotation-store-timeout',
      request,
      idempotencyKey: 'rotation-store-timeout',
      ifMatch: etag,
      stagingWriteTimeoutMs: 50,
    });
    expect(timedOut).toEqual({ kind: 'unavailable' });
    expect(hanging.signal?.aborted).toBe(true);
  });

  it('disables an active default atomically, retains its credential, and replays after re-enable', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-disable-active')),
      'disable-active-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    const [[beforeSetting], [beforeBinding], [beforeCredential]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityOrganizationSettings)
        .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId)),
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, bindingId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
    ]);

    const disabled = await disable('disable-active-key', await currentEtag());
    expect(disabled).toMatchObject({
      kind: 'success',
      status: 200,
      body: {
        configured: { default_binding: null },
        effective: {
          source: 'none',
          status: 'disabled',
          disabled_reason: 'no_default_binding',
          binding: null,
        },
      },
    });
    const [[afterSetting], [afterBinding], [afterCredential], cleanup] = await Promise.all([
      db
        .select()
        .from(agentObservabilityOrganizationSettings)
        .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId)),
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, bindingId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
      db
        .select()
        .from(agentObservabilitySecretCleanupOutbox)
        .where(eq(agentObservabilitySecretCleanupOutbox.bindingId, bindingId)),
    ]);
    expect(afterSetting).toMatchObject({
      activeDefaultBindingId: null,
      activeDefaultBindingScope: null,
      selectionEpoch: beforeSetting!.selectionEpoch + 1,
      defaultRevocationEpoch: beforeSetting!.defaultRevocationEpoch + 1,
      organizationRevocationEpoch: beforeSetting!.organizationRevocationEpoch,
      captureRestrictionEpoch: beforeSetting!.captureRestrictionEpoch,
    });
    expect(afterBinding).toMatchObject({
      status: 'draining',
      currentVersion: beforeBinding!.currentVersion,
      revocationEpoch: beforeBinding!.revocationEpoch,
    });
    expect(afterCredential).toEqual(beforeCredential);
    expect(cleanup).toEqual([]);
    expect(secretStore.deleteCalls).toBe(0);

    const reenabled = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-reenable', 'https://collector.example/v1/traces-reenabled'),
      ),
      'disable-reenable',
      null,
    );
    expect(reenabled).toMatchObject({ kind: 'success', status: 201 });
    const replay = await disable('disable-active-key', null);
    expect(replay).toEqual(disabled);
    expect(secretStore.putCalls).toBe(2);
  });

  it('clears a selected default whose credential head is missing', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-disable-missing-head')),
      'disable-missing-head-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    const [before] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId));
    await db
      .delete(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    expect(await disable('disable-missing-head', await currentEtag())).toMatchObject({
      kind: 'success',
      status: 200,
      body: { configured: { default_binding: null } },
    });
    const [[after], [binding], credentials] = await Promise.all([
      db
        .select()
        .from(agentObservabilityOrganizationSettings)
        .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId)),
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, bindingId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
    ]);
    expect(after).toMatchObject({
      activeDefaultBindingId: null,
      activeDefaultBindingScope: null,
      selectionEpoch: before!.selectionEpoch + 1,
      defaultRevocationEpoch: before!.defaultRevocationEpoch + 1,
    });
    expect(binding).toMatchObject({ status: 'draining' });
    expect(credentials).toEqual([]);
  });

  it.each(['draining', 'disabled', 'archived'] as const)(
    'clears a selected %s binding without changing its status or credential head',
    async (status) => {
      const created = await execute(
        parseOrganizationAgentObservabilityPutRequest(requestBody(`secret-disable-${status}`)),
        `disable-${status}-create`,
        null,
      );
      if (created.kind !== 'success') throw new Error('expected initial configuration');
      const bindingId = created.body.configured.default_binding!.id;
      const [before] = await db
        .select()
        .from(agentObservabilityOrganizationSettings)
        .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId));
      const archivedAt = status === 'archived' ? new Date() : null;
      await db
        .update(agentObservabilityBindings)
        .set({ status, archivedAt, updatedAt: new Date() })
        .where(eq(agentObservabilityBindings.id, bindingId));
      const [credentialBefore] = await db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

      expect(await disable(`disable-${status}`, await currentEtag())).toMatchObject({
        kind: 'success',
        status: 200,
        body: { configured: { default_binding: null } },
      });
      const [[after], [binding], [credentialAfter]] = await Promise.all([
        db
          .select()
          .from(agentObservabilityOrganizationSettings)
          .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId)),
        db
          .select()
          .from(agentObservabilityBindings)
          .where(eq(agentObservabilityBindings.id, bindingId)),
        db
          .select()
          .from(agentObservabilityBindingCredentials)
          .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
      ]);
      expect(after).toMatchObject({
        activeDefaultBindingId: null,
        activeDefaultBindingScope: null,
        selectionEpoch: before!.selectionEpoch + 1,
        defaultRevocationEpoch: before!.defaultRevocationEpoch + 1,
      });
      expect(binding).toMatchObject({ status, archivedAt });
      expect(credentialAfter).toEqual(credentialBefore);
    },
  );

  it('records an idempotent no-op disable without changing default epochs or bindings', async () => {
    const [before] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId));
    const disabled = await disable('disable-no-default', null);
    expect(disabled).toMatchObject({ kind: 'success', status: 200 });
    const [[after], audits, idempotency] = await Promise.all([
      db
        .select()
        .from(agentObservabilityOrganizationSettings)
        .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId)),
      db.select().from(adminAuditEvents).where(eq(adminAuditEvents.organizationId, organizationId)),
      db
        .select()
        .from(agentObservabilityIdempotencyKeys)
        .where(eq(agentObservabilityIdempotencyKeys.organizationId, organizationId)),
    ]);
    expect(after).toMatchObject({
      activeDefaultBindingId: null,
      activeDefaultBindingScope: null,
      selectionEpoch: before!.selectionEpoch,
      defaultRevocationEpoch: before!.defaultRevocationEpoch,
      organizationRevocationEpoch: before!.organizationRevocationEpoch,
      captureRestrictionEpoch: before!.captureRestrictionEpoch,
    });
    expect(audits).toHaveLength(1);
    expect(audits[0]).toMatchObject({
      action: 'organization.agent_observability.default_disabled',
      targetType: 'organization_setting',
    });
    expect(idempotency).toHaveLength(1);
    expect(idempotency[0]).toMatchObject({ status: 'completed', responseStatus: 200 });
  });

  it('checks stale disable before fencing a blocked PUT, then emergency disable fences it', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-disable-race-create')),
      'disable-race-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const originalEtag = await currentEtag();
    const stagingStarted = deferred<void>();
    const releaseStaging = deferred<void>();
    secretStore.beforePut = async () => {
      if (secretStore.putCalls === 2) {
        stagingStarted.resolve();
        await releaseStaging.promise;
      }
    };
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const blockedPut = execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-race-put', 'https://collector.example/v1/traces-disable-race'),
      ),
      'disable-race-put',
      originalEtag,
      undefined,
      { report: (event) => reports.push(event) },
    );
    await stagingStarted.promise;

    expect(await disable('disable-race-stale', '"stale"')).toEqual({ kind: 'stale' });
    const [pendingIdempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, 'disable-race-put'),
        ),
      );
    const [stillPending] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.id, pendingIdempotency!.reservationId));
    expect(stillPending).toMatchObject({ status: 'pending' });

    const emergency = await disable('disable-race-emergency', null);
    expect(emergency).toMatchObject({ kind: 'success', status: 200 });
    const [blockedIdempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, 'disable-race-put'),
        ),
      );
    const [fenced] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.id, blockedIdempotency!.reservationId));
    expect(fenced).toMatchObject({ status: 'fenced' });

    releaseStaging.resolve();
    await expect(blockedPut).resolves.toEqual({ kind: 'conflict' });
    expect(reports).toEqual([]);
    const [staging] = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, fenced!.id));
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
  });

  it('preempts a blocked current-binding credential rotation without touching another target', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-disable-rotation-create')),
      'disable-rotation-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const bindingId = created.body.configured.default_binding!.id;
    const stagingStarted = deferred<void>();
    const releaseStaging = deferred<void>();
    secretStore.beforePut = async () => {
      if (secretStore.putCalls === 2) {
        stagingStarted.resolve();
        await releaseStaging.promise;
      }
    };
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const blockedRotation = rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-disable-rotation-next' },
      }),
      'disable-rotation-blocked',
      await currentEtag(),
      undefined,
      { report: (event) => reports.push(event) },
    );
    await stagingStarted.promise;

    expect(await disable('disable-rotation-emergency', null)).toMatchObject({
      kind: 'success',
      status: 200,
    });
    const [rotationIdempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, 'disable-rotation-blocked'),
        ),
      );
    const [rotationReservation] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.id, rotationIdempotency!.reservationId));
    expect(rotationReservation).toMatchObject({
      status: 'fenced',
      targetBindingId: bindingId,
      targetBindingScope: 'organization',
    });

    releaseStaging.resolve();
    await expect(blockedRotation).resolves.toEqual({ kind: 'conflict' });
    expect(reports).toEqual([]);
    const [staging] = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, rotationReservation!.id));
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
  });

  it('returns conflict without reporting when disable fences a PUT before writer claim', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-preclaim-put-create'),
      ),
      'disable-preclaim-put-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const stagingPaused = deferred<void>();
    const releaseStaging = deferred<void>();
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const blockedPut = execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody(
          'secret-disable-preclaim-put',
          'https://collector.example/v1/traces-disable-preclaim-put',
        ),
      ),
      'disable-preclaim-put',
      await currentEtag(),
      {
        beforeStagingWrite: async () => {
          stagingPaused.resolve();
          await releaseStaging.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );
    await stagingPaused.promise;

    expect(await disable('disable-preclaim-put-emergency', null)).toMatchObject({
      kind: 'success',
      status: 200,
    });
    releaseStaging.resolve();
    await expect(blockedPut).resolves.toEqual({ kind: 'conflict' });
    expect(reports).toEqual([]);
    expect(secretStore.putCalls).toBe(1);
    const reservation = await reservationForIdempotencyKey('disable-preclaim-put');
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(await stagingByReservationId(reservation!.id)).toMatchObject({
      status: 'cleanup_pending',
    });
  });

  it('returns conflict without reporting when disable fences rotation before writer claim', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-preclaim-rotation-create'),
      ),
      'disable-preclaim-rotation-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const stagingPaused = deferred<void>();
    const releaseStaging = deferred<void>();
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const blockedRotation = rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-disable-preclaim-rotation' },
      }),
      'disable-preclaim-rotation',
      await currentEtag(),
      {
        beforeStagingWrite: async () => {
          stagingPaused.resolve();
          await releaseStaging.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );
    await stagingPaused.promise;

    expect(await disable('disable-preclaim-rotation-emergency', null)).toMatchObject({
      kind: 'success',
      status: 200,
    });
    releaseStaging.resolve();
    await expect(blockedRotation).resolves.toEqual({ kind: 'conflict' });
    expect(reports).toEqual([]);
    expect(secretStore.putCalls).toBe(1);
    const reservation = await reservationForIdempotencyKey('disable-preclaim-rotation');
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(await stagingByReservationId(reservation!.id)).toMatchObject({
      status: 'cleanup_pending',
    });
  });

  it.each([
    ['initial', false],
    ['replacement', true],
  ] as const)(
    'returns conflict without reporting when disable preempts a staged %s PUT after staging',
    async (_kind, replacement) => {
      if (replacement) {
        const created = await execute(
          parseOrganizationAgentObservabilityPutRequest(
            requestBody('secret-disable-postwrite-replacement-create'),
          ),
          'disable-postwrite-replacement-create',
          null,
        );
        if (created.kind !== 'success') throw new Error('expected initial configuration');
      }
      const finalizationPaused = deferred<void>();
      const releaseFinalization = deferred<void>();
      const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
      const key = `disable-postwrite-${replacement ? 'replacement' : 'initial'}-put`;
      const blockedPut = execute(
        parseOrganizationAgentObservabilityPutRequest(
          requestBody(
            `secret-${key}`,
            `https://collector.example/v1/traces-${replacement ? 'replacement' : 'initial'}`,
          ),
        ),
        key,
        replacement ? await currentEtag() : null,
        {
          beforeFinalizationAuthorityLock: async () => {
            finalizationPaused.resolve();
            await releaseFinalization.promise;
          },
        },
        { report: (event) => reports.push(event) },
      );
      await finalizationPaused.promise;

      expect(await disable(`${key}-disable`, null)).toMatchObject({ kind: 'success', status: 200 });
      releaseFinalization.resolve();
      await expect(blockedPut).resolves.toEqual({ kind: 'conflict' });
      expect(reports).toEqual([]);

      const reservation = await reservationForIdempotencyKey(key);
      expect(reservation).toMatchObject({ status: 'fenced' });
      const staging = await stagingByReservationId(reservation!.id);
      expect(staging).toMatchObject({ status: 'cleanup_pending' });
      const state = await loadOrganizationAgentObservabilityState({ db, organizationId });
      expect(state.response).toMatchObject({
        configured: { default_binding: null },
        effective: { source: 'none', status: 'disabled', binding: null },
      });

      const stagedRef = staging!.secretRef;
      await reconcileAgentObservabilityStagingIntents(db, secretStore, {
        now: new Date(Date.now() + 21 * 60 * 1_000),
      });
      expect(await secretStore.resolve(stagedRef)).toBeNull();
    },
  );

  it('returns conflict without reporting when disable preempts a staged rotation after staging', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-postwrite-rotation-create'),
      ),
      'disable-postwrite-rotation-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const finalizationPaused = deferred<void>();
    const releaseFinalization = deferred<void>();
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const key = 'disable-postwrite-rotation';
    const blockedRotation = rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-disable-postwrite-rotation-next' },
      }),
      key,
      await currentEtag(),
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationPaused.resolve();
          await releaseFinalization.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );
    await finalizationPaused.promise;

    expect(await disable(`${key}-disable`, null)).toMatchObject({ kind: 'success', status: 200 });
    releaseFinalization.resolve();
    await expect(blockedRotation).resolves.toEqual({ kind: 'conflict' });
    expect(reports).toEqual([]);
    const reservation = await reservationForIdempotencyKey(key);
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(await stagingByReservationId(reservation!.id)).toMatchObject({
      status: 'cleanup_pending',
    });
  });

  it('retries a real serialization failure after disable preempts a staged PUT', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-serialization-put-create'),
      ),
      'disable-serialization-put-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const finalizationReady = deferred<void>();
    const releaseFinalization = deferred<void>();
    const disableLocked = deferred<void>();
    const releaseDisable = deferred<void>();
    const firstFinalizerPid = deferred<number>();
    const finalizationAttempts: number[] = [];
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    let outerFinalizationCalls = 0;
    const key = 'disable-serialization-put';
    const loser = execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody(
          'secret-disable-serialization-put-next',
          'https://collector.example/v1/traces-disable-serialization-put',
        ),
      ),
      key,
      await currentEtag(),
      {
        beforeFinalizationAuthorityLock: async () => {
          outerFinalizationCalls += 1;
          finalizationReady.resolve();
          await releaseFinalization.promise;
        },
        beforeFinalizationTransaction: async ({ attempt, tx }) => {
          finalizationAttempts.push(attempt);
          if (attempt === 1) firstFinalizerPid.resolve(await transactionBackendPid(tx));
        },
      },
      { report: (event) => reports.push(event) },
    );
    await finalizationReady.promise;

    const disabling = disable('disable-serialization-put-emergency', null, {
      afterAcquisitionAuthorityLocked: async () => {
        disableLocked.resolve();
        await releaseDisable.promise;
      },
    });
    await disableLocked.promise;
    releaseFinalization.resolve();
    await waitForAuthorityLockWait(await firstFinalizerPid.promise);
    releaseDisable.resolve();

    await expect(disabling).resolves.toMatchObject({ kind: 'success', status: 200 });
    await expect(loser).resolves.toEqual({ kind: 'conflict' });
    expect(finalizationAttempts).toEqual([1, 2]);
    expect(outerFinalizationCalls).toBe(1);
    expect(reports).toEqual([]);
    expect(secretStore.putCalls).toBe(2);
    const reservation = await reservationForIdempotencyKey(key);
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(await stagingByReservationId(reservation!.id)).toMatchObject({
      status: 'cleanup_pending',
    });
  });

  it('retries a real serialization failure after disable preempts a staged rotation', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-serialization-rotation-create'),
      ),
      'disable-serialization-rotation-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const finalizationReady = deferred<void>();
    const releaseFinalization = deferred<void>();
    const disableLocked = deferred<void>();
    const releaseDisable = deferred<void>();
    const firstFinalizerPid = deferred<number>();
    const finalizationAttempts: number[] = [];
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    let outerFinalizationCalls = 0;
    const key = 'disable-serialization-rotation';
    const loser = rotate(
      parseOrganizationAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'secret-disable-serialization-rotation-next' },
      }),
      key,
      await currentEtag(),
      {
        beforeFinalizationAuthorityLock: async () => {
          outerFinalizationCalls += 1;
          finalizationReady.resolve();
          await releaseFinalization.promise;
        },
        beforeFinalizationTransaction: async ({ attempt, tx }) => {
          finalizationAttempts.push(attempt);
          if (attempt === 1) firstFinalizerPid.resolve(await transactionBackendPid(tx));
        },
      },
      { report: (event) => reports.push(event) },
    );
    await finalizationReady.promise;

    const disabling = disable('disable-serialization-rotation-emergency', null, {
      afterAcquisitionAuthorityLocked: async () => {
        disableLocked.resolve();
        await releaseDisable.promise;
      },
    });
    await disableLocked.promise;
    releaseFinalization.resolve();
    await waitForAuthorityLockWait(await firstFinalizerPid.promise);
    releaseDisable.resolve();

    await expect(disabling).resolves.toMatchObject({ kind: 'success', status: 200 });
    await expect(loser).resolves.toEqual({ kind: 'conflict' });
    expect(finalizationAttempts).toEqual([1, 2]);
    expect(outerFinalizationCalls).toBe(1);
    expect(reports).toEqual([]);
    expect(secretStore.putCalls).toBe(2);
    const reservation = await reservationForIdempotencyKey(key);
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(await stagingByReservationId(reservation!.id)).toMatchObject({
      status: 'cleanup_pending',
    });
  });

  it.each(['PUT', 'rotation'] as const)(
    'retries an emergency disable blocked behind a %s finalizer',
    async (mutation) => {
      const created = await execute(
        parseOrganizationAgentObservabilityPutRequest(
          requestBody(`secret-disable-blocked-${mutation.toLowerCase()}-create`),
        ),
        `disable-blocked-${mutation.toLowerCase()}-create`,
        null,
      );
      if (created.kind !== 'success') throw new Error('expected initial configuration');

      const finalizationLocked = deferred<void>();
      const releaseFinalization = deferred<void>();
      const blockedDisablePid = deferred<number>();
      const disableAttempts: number[] = [];
      const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
      const finalizer =
        mutation === 'PUT'
          ? execute(
              parseOrganizationAgentObservabilityPutRequest(
                requestBody(
                  'secret-disable-blocked-put-next',
                  'https://collector.example/v1/traces-disable-blocked-put',
                ),
              ),
              'disable-blocked-put-finalizer',
              await currentEtag(),
              {
                afterFinalizationAuthorityLocked: async () => {
                  finalizationLocked.resolve();
                  await releaseFinalization.promise;
                },
              },
            )
          : rotate(
              parseOrganizationAgentObservabilityCredentialRotationRequest({
                credentials: { type: 'bearer', token: 'secret-disable-blocked-rotation-next' },
              }),
              'disable-blocked-rotation-finalizer',
              await currentEtag(),
              {
                afterFinalizationAuthorityLocked: async () => {
                  finalizationLocked.resolve();
                  await releaseFinalization.promise;
                },
              },
            );
      await finalizationLocked.promise;

      const disabling = disable(
        `disable-blocked-${mutation.toLowerCase()}-retry`,
        null,
        {
          beforeDisableTransaction: async ({ attempt, tx }) => {
            disableAttempts.push(attempt);
            if (attempt === 1) blockedDisablePid.resolve(await transactionBackendPid(tx));
          },
        },
        { report: (event) => reports.push(event) },
      );
      await waitForAuthorityLockWait(await blockedDisablePid.promise);
      releaseFinalization.resolve();

      await expect(finalizer).resolves.toMatchObject({ kind: 'success' });
      await expect(disabling).resolves.toMatchObject({
        kind: 'success',
        status: 200,
        body: {
          configured: { default_binding: null },
          effective: { source: 'none', status: 'disabled', binding: null },
        },
      });
      expect(disableAttempts).toEqual([1, 2]);
      expect(reports).toEqual([]);
      expect(secretStore.putCalls).toBe(2);
    },
  );

  it('retries a disable blocked behind another disable with a distinct idempotency key', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-blocked-by-disable-create'),
      ),
      'disable-blocked-by-disable-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');

    const firstLocked = deferred<void>();
    const releaseFirst = deferred<void>();
    const blockedDisablePid = deferred<number>();
    const secondAttempts: number[] = [];
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const first = disable(
      'disable-blocked-by-disable-first',
      null,
      {
        afterAcquisitionAuthorityLocked: async () => {
          firstLocked.resolve();
          await releaseFirst.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );
    await firstLocked.promise;

    const second = disable(
      'disable-blocked-by-disable-second',
      null,
      {
        beforeDisableTransaction: async ({ attempt, tx }) => {
          secondAttempts.push(attempt);
          if (attempt === 1) blockedDisablePid.resolve(await transactionBackendPid(tx));
        },
      },
      { report: (event) => reports.push(event) },
    );
    await waitForAuthorityLockWait(await blockedDisablePid.promise);
    releaseFirst.resolve();

    await expect(first).resolves.toMatchObject({ kind: 'success', status: 200 });
    await expect(second).resolves.toMatchObject({
      kind: 'success',
      status: 200,
      body: {
        configured: { default_binding: null },
        effective: { source: 'none', status: 'disabled', binding: null },
      },
    });
    expect(secondAttempts).toEqual([1, 2]);
    expect(reports).toEqual([]);
    expect(secretStore.putCalls).toBe(1);
  });

  it('returns conflict without reporting when disable preempts a same-target PUT before finalization', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(
        requestBody('secret-disable-postwrite-same-target-create'),
      ),
      'disable-postwrite-same-target-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const finalizationPaused = deferred<void>();
    const releaseFinalization = deferred<void>();
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const body = requestBody('ignored');
    const blockedPut = execute(
      parseOrganizationAgentObservabilityPutRequest({
        ...body,
        credentials: undefined,
        config: { ...body.config, compression: 'gzip' },
      }),
      'disable-postwrite-same-target',
      await currentEtag(),
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationPaused.resolve();
          await releaseFinalization.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );
    await finalizationPaused.promise;

    expect(await disable('disable-postwrite-same-target-disable', null)).toMatchObject({
      kind: 'success',
      status: 200,
    });
    releaseFinalization.resolve();
    await expect(blockedPut).resolves.toEqual({ kind: 'conflict' });
    expect(reports).toEqual([]);
    expect(secretStore.putCalls).toBe(1);
    const reservation = await reservationForIdempotencyKey('disable-postwrite-same-target');
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(await stagingByReservationId(reservation!.id)).toBeUndefined();
  });

  it('expires and conflicts without reporting when a staged reservation lapses before finalization', async () => {
    const finalizationPaused = deferred<void>();
    const releaseFinalization = deferred<void>();
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
    const key = 'expired-postwrite-finalization-loser';
    const blockedPut = execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-expired-postwrite-loser')),
      key,
      null,
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationPaused.resolve();
          await releaseFinalization.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );
    await finalizationPaused.promise;
    const reservation = await reservationForIdempotencyKey(key);
    await db
      .update(agentObservabilityMutationReservations)
      .set({ expiresAt: new Date(Date.now() - 1), updatedAt: new Date() })
      .where(eq(agentObservabilityMutationReservations.id, reservation!.id));

    releaseFinalization.resolve();
    await expect(blockedPut).resolves.toEqual({ kind: 'conflict' });
    expect(reports).toEqual([]);
    expect(await reservationForIdempotencyKey(key)).toMatchObject({ status: 'expired' });
    expect(await stagingByReservationId(reservation!.id)).toMatchObject({
      status: 'cleanup_pending',
    });
  });

  it.each(['PUT', 'rotation'] as const)(
    'returns conflict without reporting when a claimed %s staging writer expires during put',
    async (operation) => {
      let originalBindingId: string | undefined;
      if (operation === 'rotation') {
        const created = await execute(
          parseOrganizationAgentObservabilityPutRequest(
            requestBody('secret-expired-claimed-rotation-create'),
          ),
          'expired-claimed-rotation-create',
          null,
        );
        if (created.kind !== 'success') throw new Error('expected initial configuration');
        originalBindingId = created.body.configured.default_binding!.id;
      }

      const key = `expired-claimed-${operation.toLowerCase()}`;
      const expectedPutCalls = operation === 'PUT' ? 1 : 2;
      const stagingStarted = deferred<void>();
      const releaseStaging = deferred<void>();
      const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
      secretStore.beforePut = async () => {
        if (secretStore.putCalls !== expectedPutCalls) return;
        stagingStarted.resolve();
        await releaseStaging.promise;
      };
      const mutation =
        operation === 'PUT'
          ? execute(
              parseOrganizationAgentObservabilityPutRequest(
                requestBody('secret-expired-claimed-put'),
              ),
              key,
              null,
              undefined,
              { report: (event) => reports.push(event) },
            )
          : rotate(
              parseOrganizationAgentObservabilityCredentialRotationRequest({
                credentials: { type: 'bearer', token: 'secret-expired-claimed-rotation' },
              }),
              key,
              await currentEtag(),
              undefined,
              { report: (event) => reports.push(event) },
            );
      await stagingStarted.promise;

      const reservation = await reservationForIdempotencyKey(key);
      expect(await stagingByReservationId(reservation!.id)).toMatchObject({ status: 'writing' });
      await db
        .update(agentObservabilityMutationReservations)
        .set({ expiresAt: new Date(Date.now() - 1), updatedAt: new Date() })
        .where(eq(agentObservabilityMutationReservations.id, reservation!.id));

      releaseStaging.resolve();
      await expect(mutation).resolves.toEqual({ kind: 'conflict' });
      expect(reports).toEqual([]);
      expect(await reservationForIdempotencyKey(key)).toMatchObject({ status: 'expired' });
      const staging = await stagingByReservationId(reservation!.id);
      expect(staging).toMatchObject({ status: 'cleanup_pending' });

      const state = await loadOrganizationAgentObservabilityState({ db, organizationId });
      if (operation === 'PUT') {
        expect(state.response.configured.default_binding).toBeNull();
      } else {
        expect(state.response.configured.default_binding).toMatchObject({ id: originalBindingId });
      }

      await reconcileAgentObservabilityStagingIntents(db, secretStore, {
        now: new Date(Date.now() + 21 * 60 * 1_000),
      });
      expect(await secretStore.resolve(staging!.secretRef)).toBeNull();
    },
  );

  it.each(['missing', 'completed', 'wrong body', 'wrong target'] as const)(
    'reports fence_failure and returns unavailable for a pending reservation with a %s idempotency row',
    async (failure) => {
      const finalizationPaused = deferred<void>();
      const releaseFinalization = deferred<void>();
      const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
      const key = `invalid-idempotency-finalization-settlement-${failure.replaceAll(' ', '-')}`;
      const blockedPut = execute(
        parseOrganizationAgentObservabilityPutRequest(requestBody(`secret-${key}`)),
        key,
        null,
        {
          beforeFinalizationAuthorityLock: async () => {
            finalizationPaused.resolve();
            await releaseFinalization.promise;
          },
        },
        { report: (event) => reports.push(event) },
      );
      await finalizationPaused.promise;
      const reservation = await reservationForIdempotencyKey(key);
      const idempotencyWhere = eq(agentObservabilityIdempotencyKeys.reservationId, reservation!.id);
      if (failure === 'missing') {
        await db.delete(agentObservabilityIdempotencyKeys).where(idempotencyWhere);
      } else if (failure === 'completed') {
        await db
          .update(agentObservabilityIdempotencyKeys)
          .set({
            status: 'completed',
            responseStatus: 200,
            responseBody: {},
            completedAt: new Date(),
            updatedAt: new Date(),
          })
          .where(idempotencyWhere);
      } else if (failure === 'wrong body') {
        await db
          .update(agentObservabilityIdempotencyKeys)
          .set({
            bodyHash: agentObservabilityMutationBodyHash({ operation: 'wrong-idempotency-body' }),
            updatedAt: new Date(),
          })
          .where(idempotencyWhere);
      } else {
        await db
          .update(agentObservabilityIdempotencyKeys)
          .set({
            targetKey: agentObservabilityMutationTargetKey({
              type: 'workspace_setting',
              organizationId,
              workspaceId: 'ws_wrong_idempotency_target',
            }),
            updatedAt: new Date(),
          })
          .where(idempotencyWhere);
      }

      releaseFinalization.resolve();
      await expect(blockedPut).resolves.toEqual({ kind: 'unavailable' });
      expect(reports).toEqual([
        {
          phase: 'fence',
          code: 'fence_failure',
          requestId: `request_${key}`,
          organizationId,
          message: 'registry-service-ts failed organization agent observability mutation fence',
        },
      ]);
      expect(await reservationById(reservation!.id)).toMatchObject({ status: 'pending' });
    },
  );

  it.each(['committed', 'mismatched', 'missing', 'forced'] as const)(
    'reports fence_failure and returns unavailable for a %s finalization settlement',
    async (failure) => {
      const finalizationPaused = deferred<void>();
      const releaseFinalization = deferred<void>();
      const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];
      const key = `invalid-finalization-settlement-${failure}`;
      const blockedPut = execute(
        parseOrganizationAgentObservabilityPutRequest(requestBody(`secret-${key}`)),
        key,
        null,
        {
          beforeFinalizationAuthorityLock: async () => {
            finalizationPaused.resolve();
            await releaseFinalization.promise;
          },
          ...(failure === 'forced'
            ? {
                beforeFencing: async () => {
                  throw new Error('forced finalization settlement failure');
                },
              }
            : {}),
        },
        { report: (event) => reports.push(event) },
      );
      await finalizationPaused.promise;
      const reservation = await reservationForIdempotencyKey(key);
      if (failure === 'committed') {
        await db
          .update(agentObservabilityMutationReservations)
          .set({ status: 'committed', committedAt: new Date(), updatedAt: new Date() })
          .where(eq(agentObservabilityMutationReservations.id, reservation!.id));
      } else if (failure === 'mismatched') {
        await db
          .update(agentObservabilityMutationReservations)
          .set({ ownerPrincipal: 'different-finalization-owner', updatedAt: new Date() })
          .where(eq(agentObservabilityMutationReservations.id, reservation!.id));
      } else if (failure === 'missing') {
        await db.transaction(async (tx) => {
          await tx.execute(sql`set local session_replication_role = replica`);
          await tx
            .delete(agentObservabilityMutationReservations)
            .where(eq(agentObservabilityMutationReservations.id, reservation!.id));
        });
      } else {
        await db
          .update(agentObservabilityMutationReservations)
          .set({
            expectedStateVersion: 'forced-finalization-settlement-version',
            updatedAt: new Date(),
          })
          .where(eq(agentObservabilityMutationReservations.id, reservation!.id));
      }

      releaseFinalization.resolve();
      await expect(blockedPut).resolves.toEqual({ kind: 'unavailable' });
      expect(reports).toEqual([
        {
          phase: 'fence',
          code: 'fence_failure',
          requestId: `request_${key}`,
          organizationId,
          message: 'registry-service-ts failed organization agent observability mutation fence',
        },
      ]);
    },
  );

  it('preempts only the current setting and current organization binding', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-disable-target-boundary')),
      'disable-target-boundary-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    const currentBindingId = created.body.configured.default_binding!.id;
    const otherOrganizationId = nextOrganizationId();
    await insertOrganization(db, otherOrganizationId);
    const unrelatedOrganizationBindingId = await createReservationBinding(organizationId);
    const workspaceId = `ws_preempt_${organizationId}`;
    await db.insert(workspaces).values({
      id: workspaceId,
      organizationId,
      name: workspaceId,
      status: 'active',
      createdBy: 'integration-test',
    });
    const workspaceBindingId = await createReservationBinding(organizationId, workspaceId);
    const otherBindingId = await createReservationBinding(otherOrganizationId);

    const currentSetting = await acquirePendingReservation(
      { type: 'organization_setting', organizationId },
      'preempt-current-setting',
      true,
    );
    const currentBinding = await acquirePendingReservation(
      {
        type: 'binding',
        organizationId,
        bindingId: currentBindingId,
        bindingScope: 'organization',
      },
      'preempt-current-binding',
      true,
    );
    const unrelatedOrganizationBinding = await acquirePendingReservation(
      {
        type: 'binding',
        organizationId,
        bindingId: unrelatedOrganizationBindingId,
        bindingScope: 'organization',
      },
      'preempt-unrelated-organization-binding',
    );
    const workspaceBinding = await acquirePendingReservation(
      {
        type: 'binding',
        organizationId,
        bindingId: workspaceBindingId,
        bindingScope: 'workspace',
        workspaceId,
      },
      'preempt-workspace-binding',
    );
    const otherSetting = await acquirePendingReservation(
      { type: 'organization_setting', organizationId: otherOrganizationId },
      'preempt-other-setting',
    );
    const otherBinding = await acquirePendingReservation(
      {
        type: 'binding',
        organizationId: otherOrganizationId,
        bindingId: otherBindingId,
        bindingScope: 'organization',
      },
      'preempt-other-binding',
    );

    expect(await disable('disable-target-boundary', null)).toMatchObject({
      kind: 'success',
      status: 200,
    });
    const [
      currentSettingRow,
      currentBindingRow,
      unrelatedOrganizationBindingRow,
      workspaceBindingRow,
      otherSettingRow,
      otherBindingRow,
    ] = await Promise.all([
      reservationById(currentSetting.reservationId),
      reservationById(currentBinding.reservationId),
      reservationById(unrelatedOrganizationBinding.reservationId),
      reservationById(workspaceBinding.reservationId),
      reservationById(otherSetting.reservationId),
      reservationById(otherBinding.reservationId),
    ]);
    expect(currentSettingRow).toMatchObject({ status: 'fenced' });
    expect(currentBindingRow).toMatchObject({ status: 'fenced' });
    for (const reservation of [
      unrelatedOrganizationBindingRow,
      workspaceBindingRow,
      otherSettingRow,
      otherBindingRow,
    ]) {
      expect(reservation).toMatchObject({ status: 'pending' });
    }
    const [settingStaging, bindingStaging] = await Promise.all([
      stagingByReservationId(currentSetting.reservationId),
      stagingByReservationId(currentBinding.reservationId),
    ]);
    expect(settingStaging).toMatchObject({ status: 'cleanup_pending' });
    expect(bindingStaging).toMatchObject({ status: 'cleanup_pending' });
  });

  it('fails closed when a selected pointer has no owned binding', async () => {
    const created = await execute(
      parseOrganizationAgentObservabilityPutRequest(requestBody('secret-disable-corrupt')),
      'disable-corrupt-create',
      null,
    );
    if (created.kind !== 'success') throw new Error('expected initial configuration');
    await db.transaction(async (tx) => {
      // Test-only durable-corruption seam: the normal composite FK prevents
      // this pointer shape, but mixed-version/manual repair damage must fail
      // closed rather than make disable target an arbitrary binding.
      await tx.execute(sql`set local session_replication_role = replica`);
      await tx
        .update(agentObservabilityOrganizationSettings)
        .set({ activeDefaultBindingId: `aob_missing_${organizationId}` })
        .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId));
    });
    const reports: OrganizationAgentObservabilityMutationFailureEvent[] = [];

    const result = await disable('disable-corrupt', null, undefined, {
      report: (event) => reports.push(event),
    });
    expect(result).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      {
        phase: 'acquisition',
        code: 'invariant_violation',
        requestId: 'request_disable-corrupt',
        organizationId,
        message: 'registry-service-ts failed organization agent observability mutation acquisition',
      },
    ]);
  });

  function execute(
    request: NormalizedOrganizationAgentObservabilityPutRequest,
    idempotencyKey: string,
    ifMatch: string | null,
    hooks?: OrganizationAgentObservabilityMutationExecutionHooks,
    reporter?: OrganizationAgentObservabilityMutationReporter,
  ) {
    return executeForOrganization(
      organizationId,
      request,
      idempotencyKey,
      ifMatch,
      hooks,
      reporter,
    );
  }

  function executeForOrganization(
    targetOrganizationId: string,
    request: NormalizedOrganizationAgentObservabilityPutRequest,
    idempotencyKey: string,
    ifMatch: string | null,
    hooks?: OrganizationAgentObservabilityMutationExecutionHooks,
    reporter?: OrganizationAgentObservabilityMutationReporter,
  ) {
    return executeOrganizationAgentObservabilityPut({
      db,
      secretStore,
      organizationId: targetOrganizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: `request_${idempotencyKey}`,
      request,
      idempotencyKey,
      ifMatch,
      ...(hooks === undefined ? {} : { hooks }),
      ...(reporter === undefined ? {} : { reporter }),
    });
  }

  function rotate(
    request: ReturnType<typeof parseOrganizationAgentObservabilityCredentialRotationRequest>,
    idempotencyKey: string,
    ifMatch: string | null,
    hooks?: OrganizationAgentObservabilityMutationExecutionHooks,
    reporter?: OrganizationAgentObservabilityMutationReporter,
  ) {
    return executeOrganizationAgentObservabilityCredentialRotation({
      db,
      secretStore,
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: `request_${idempotencyKey}`,
      request,
      idempotencyKey,
      ifMatch,
      ...(hooks === undefined ? {} : { hooks }),
      ...(reporter === undefined ? {} : { reporter }),
    });
  }

  function disable(
    idempotencyKey: string,
    ifMatch: string | null,
    hooks?: OrganizationAgentObservabilityMutationExecutionHooks,
    reporter?: OrganizationAgentObservabilityMutationReporter,
  ) {
    return executeOrganizationAgentObservabilityDisable({
      db,
      organizationId,
      principal: 'service-integration-test',
      authMethod: 'admin_api_key',
      requestId: `request_${idempotencyKey}`,
      request: parseOrganizationAgentObservabilityDisableRequest({}),
      idempotencyKey,
      ifMatch,
      ...(hooks === undefined ? {} : { hooks }),
      ...(reporter === undefined ? {} : { reporter }),
    });
  }

  async function acquirePendingReservation(
    target: AgentObservabilityMutationTarget,
    key: string,
    staging = false,
  ) {
    const result = await db.transaction((tx) =>
      acquireAgentObservabilityMutationReservation(tx, {
        target,
        idempotency: {
          organizationId: target.organizationId,
          principal: 'preemption-boundary-test',
          scope: 'organization_agent_observability.test',
          key,
        },
        bodyHash: agentObservabilityMutationBodyHash({ operation: key }),
        expectedVersions: {
          stateVersion: `preemption-boundary-${key}`,
          configVersion: null,
          credentialVersion: null,
        },
        ...(staging
          ? {
              staging: {
                candidateBindingId:
                  target.type === 'binding' ? target.bindingId : `aob_pending_${key}`,
                proposedCredentialVersion: 1,
                secretRef: newAgentObservabilitySecretReference(),
              },
            }
          : {}),
      }),
    );
    if (result.kind !== 'acquired') {
      throw new Error(`expected pending reservation, got ${result.kind}`);
    }
    return result.reservation;
  }

  async function reservationById(reservationId: string) {
    return (
      await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.id, reservationId))
    )[0];
  }

  async function reservationForIdempotencyKey(key: string) {
    const [idempotency] = await db
      .select({ reservationId: agentObservabilityIdempotencyKeys.reservationId })
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, key),
        ),
      );
    if (!idempotency) throw new Error(`missing idempotency row for ${key}`);
    return reservationById(idempotency.reservationId);
  }

  async function stagingByReservationId(reservationId: string) {
    return (
      await db
        .select()
        .from(agentObservabilityCredentialStagingIntents)
        .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservationId))
    )[0];
  }

  async function createReservationBinding(organization: string, workspaceId: string | null = null) {
    const id = `aob_preemption_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const now = new Date();
    await db.transaction(async (tx) => {
      await tx.insert(agentObservabilityBindings).values({
        id,
        organizationId: organization,
        workspaceId,
        scopeType: workspaceId === null ? 'organization' : 'workspace',
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
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      });
      await tx.insert(agentObservabilityBindingVersions).values({
        bindingId: id,
        version: 1,
        adapterType: 'otlp_http',
        semanticProfile: 'otel_genai',
        protocol: 'http/protobuf',
        compression: 'none',
        timeoutMs: 5_000,
        environment: null,
        release: null,
        captureMode: 'metadata_only',
        sampleRate: '1',
        configSchemaVersion: 1,
        createdBy: 'integration-test',
        createdAt: now,
      });
    });
    return id;
  }

  async function transactionBackendPid(tx: DbTransaction): Promise<number> {
    const result = await tx.execute<{ pid: number }>(sql`select pg_backend_pid() as pid`);
    const pid = result.rows[0]?.pid;
    if (typeof pid !== 'number' || !Number.isSafeInteger(pid) || pid <= 0) {
      throw new Error('missing transaction backend pid');
    }
    return pid;
  }

  async function waitForAuthorityLockWait(pid: number): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      const result = await db.execute<{ waiting: boolean }>(sql`
        select exists(
          select 1
          from pg_locks
          where pid = ${pid}
            and not granted
        ) as waiting
      `);
      if (result.rows[0]?.waiting === true) return;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error('transaction did not block on organization authority');
  }

  async function currentEtag(): Promise<string> {
    const state = await loadOrganizationAgentObservabilityState({ db, organizationId });
    return agentObservabilityStateEtag(state.etagInput);
  }
});

async function insertOrganization(db: DbClient, organizationId: string): Promise<void> {
  await db.insert(organizations).values({
    id: organizationId,
    name: `Observability mutation service integration ${organizationId}`,
    status: 'active',
  });
}

function requestBody(password: string, endpoint = 'https://collector.example/v1/traces') {
  return {
    target: {
      adapter_type: 'otlp_http' as const,
      endpoint_kind: 'traces_endpoint' as const,
      endpoint_class: 'public' as const,
      endpoint_url: endpoint,
    },
    config: {
      semantic_profile: 'otel_genai' as const,
      protocol: 'http/protobuf' as const,
      compression: 'none' as const,
      timeout_ms: 5000,
      capture_mode: 'metadata_only' as const,
      sample_rate: 1,
    },
    capture_ceiling: 'metadata_only' as const,
    credentials: { type: 'basic' as const, username: 'project', password },
  };
}

function nextOrganizationId(): string {
  return `org_observability_service_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

function postgresError(code: string): Error & { code: string } {
  return Object.assign(new Error(`PostgreSQL ${code}`), { code });
}

class TrackingSecretStore implements SecretStore {
  readonly values = new Map<string, string>();
  putCalls = 0;
  deleteCalls = 0;
  beforePut: (() => Promise<void>) | undefined;

  async put(reference: string, value: string, options?: { signal?: AbortSignal }): Promise<void> {
    if (options?.signal?.aborted) throw options.signal.reason;
    this.putCalls += 1;
    await this.beforePut?.();
    if (options?.signal?.aborted) throw options.signal.reason;
    this.values.set(reference, value);
  }

  async resolve(reference: string): Promise<string | null> {
    return this.values.get(reference) ?? null;
  }

  async delete(reference: string): Promise<void> {
    this.deleteCalls += 1;
    this.values.delete(reference);
  }
}

class HangingSecretStore implements SecretStore {
  signal: AbortSignal | undefined;

  async put(_reference: string, _value: string, options?: { signal?: AbortSignal }): Promise<void> {
    this.signal = options?.signal;
    await new Promise<void>(() => {});
  }

  async resolve(): Promise<string | null> {
    return null;
  }

  async delete(): Promise<void> {}
}

class FailingSecretStore extends TrackingSecretStore {
  override async put(
    _reference: string,
    _value: string,
    options?: { signal?: AbortSignal },
  ): Promise<void> {
    if (options?.signal?.aborted) throw options.signal.reason;
    this.putCalls += 1;
    throw new Error('provider rejected secret staging write');
  }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}
