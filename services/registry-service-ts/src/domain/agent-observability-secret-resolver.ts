// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull, sql } from 'drizzle-orm';
import { AgentObservabilitySecretResolutionSchema } from '../contracts/internal.contract.js';
import type {
  AgentObservabilitySecretResolution,
  AgentObservabilitySessionContext,
} from '../contracts/internal.contract.js';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import {
  adminAuditEvents,
  agentObservabilityBindingCredentials,
  agentObservabilityBindings,
} from '../persistence/postgres/schema.js';
import type { SecretStore } from '../secrets/secret-provider.js';
import {
  decodeAgentObservabilitySecretBundle,
  isAgentObservabilitySecretReference,
  type AgentObservabilitySecretBundle,
} from './agent-observability-secrets.js';
import {
  AgentObservabilitySessionContextNotFoundError,
  AgentObservabilitySessionContextUnavailableError,
  loadAgentObservabilitySessionContextInTransaction,
} from './agent-observability-context-resolver.js';
import { newId } from './versioning.js';

/** Bound coherent-head attempts and fresh reauthorization transactions. */
export const AGENT_OBSERVABILITY_SECRET_RESOLUTION_MAX_ATTEMPTS = 3;

/** One release, including every retry, cannot wait longer than this. */
export const AGENT_OBSERVABILITY_SECRET_RESOLUTION_DEADLINE_MS = 5_000;

/** Path pin does not exist or does not belong to the requested workspace. */
export class AgentObservabilitySecretResolutionNotFoundError extends Error {
  override readonly name = 'AgentObservabilitySecretResolutionNotFoundError';

  constructor() {
    super('agent observability secret resolution not found');
  }
}

/** Current lifecycle or policy state denied a release before it could be sent. */
export class AgentObservabilitySecretResolutionDeniedError extends Error {
  override readonly name = 'AgentObservabilitySecretResolutionDeniedError';

  constructor() {
    super('agent observability secret resolution denied');
  }
}

/** Safe retryable storage, authority, bundle, or audit failure. */
export class AgentObservabilitySecretResolutionUnavailableError extends Error {
  override readonly name = 'AgentObservabilitySecretResolutionUnavailableError';

  constructor() {
    super('agent observability secret resolution unavailable');
  }
}

export interface ResolveAgentObservabilitySessionSecretInput {
  db: DbClient;
  /** Deliberately direct: this route never falls back through SecretProvider. */
  secretStore: SecretStore | undefined;
  workspaceId: string;
  sessionId: string;
  actor: string;
  requestId: string;
  /** Domain test seam. Production uses the fixed attempt bound for both retry scopes. */
  maxAttempts?: number;
  /** Domain test seam. It can shorten, but never extend, the production deadline. */
  deadlineMs?: number;
  /** Stops awaiting work and prevents any later secret release when aborted. */
  signal?: AbortSignal;
}

type EnabledContext = AgentObservabilitySessionContext & {
  status: 'enabled';
  binding: NonNullable<AgentObservabilitySessionContext['binding']>;
};

interface CredentialHead {
  bindingId: string;
  credentialVersion: number;
  secretRef: string;
}

interface ResolutionSnapshot {
  context: EnabledContext;
  head: CredentialHead;
}

interface ResolutionDeadline {
  expiresAt: number;
  signal: AbortSignal;
  dispose(): void;
}

type Reauthorization =
  | { kind: 'retry' }
  | { kind: 'authorized'; resolution: AgentObservabilitySecretResolution };

/**
 * Resolve one current binding credential at the exporter pre-send boundary.
 *
 * Secret bytes are fetched outside transactions. A second fresh locked
 * transaction binds those bytes to the same current head and commits the
 * success audit before the bytes leave this function.
 */
export async function resolveAgentObservabilitySessionSecret(
  input: ResolveAgentObservabilitySessionSecretInput,
): Promise<AgentObservabilitySecretResolution> {
  const maxAttempts = resolveMaxAttempts(input.maxAttempts);
  const deadline = createResolutionDeadline(input.deadlineMs, input.signal);
  try {
    if (!isNonEmptyString(input.actor) || !isNonEmptyString(input.requestId)) {
      throw unavailable();
    }

    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      // This is intentionally one deadline for the entire release, rather than
      // one fresh deadline per head or serialization retry.
      assertResolutionActive(deadline);
      const snapshot = await loadResolutionSnapshot(input, deadline, maxAttempts);
      // Classify disabled/suppressed state before treating a missing dependency
      // as unavailable. A denial must never attempt SecretStore resolution.
      if (!input.secretStore) throw unavailable();

      assertResolutionActive(deadline);
      const bundle = await resolveBundle(input.secretStore, snapshot, deadline);
      assertResolutionActive(deadline);
      const reauthorization = await reauthorizeAndAudit(
        input,
        snapshot,
        bundle,
        deadline,
        maxAttempts,
      );

      if (reauthorization.kind === 'retry') continue;
      assertResolutionActive(deadline);
      return reauthorization.resolution;
    }

    throw unavailable();
  } finally {
    deadline.dispose();
  }
}

async function loadResolutionSnapshot(
  input: ResolveAgentObservabilitySessionSecretInput,
  deadline: ResolutionDeadline,
  maxAttempts: number,
): Promise<ResolutionSnapshot> {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      const snapshot = await awaitWithinResolutionDeadline(
        input.db.transaction(
          async (tx) => {
            await configureResolutionTransactionTimeouts(tx as DbTransaction, deadline);
            assertResolutionActive(deadline);
            const context = requireEnabledContext(
              await loadAgentObservabilitySessionContextInTransaction({
                tx: tx as DbTransaction,
                workspaceId: input.workspaceId,
                sessionId: input.sessionId,
              }),
            );
            assertResolutionActive(deadline);
            const head = await loadExactCredentialHead(tx as DbTransaction, context);
            assertResolutionActive(deadline);
            return { context, head };
          },
          { isolationLevel: 'repeatable read' },
        ),
        deadline,
      );
      assertResolutionActive(deadline);
      return snapshot;
    } catch (error) {
      if (isPostgresSerializationFailure(error) && attempt + 1 < maxAttempts) continue;
      throw mapResolutionError(error);
    }
  }

  throw unavailable();
}

async function resolveBundle(
  secretStore: SecretStore,
  snapshot: ResolutionSnapshot,
  deadline: ResolutionDeadline,
): Promise<AgentObservabilitySecretBundle> {
  let encoded: string | null;
  try {
    assertResolutionActive(deadline);
    encoded = await resolveSecretStoreValue(secretStore, snapshot.head.secretRef, deadline);
    assertResolutionActive(deadline);
  } catch {
    throw unavailable();
  }
  if (encoded === null) throw unavailable();

  let bundle: AgentObservabilitySecretBundle;
  try {
    bundle = decodeAgentObservabilitySecretBundle(encoded, {
      bindingId: snapshot.head.bindingId,
      credentialVersion: snapshot.head.credentialVersion,
    });
  } catch {
    throw unavailable();
  }
  if (bundle.adapterType !== snapshot.context.binding.target.adapter_type) throw unavailable();
  assertResolutionActive(deadline);
  return bundle;
}

async function reauthorizeAndAudit(
  input: ResolveAgentObservabilitySessionSecretInput,
  snapshot: ResolutionSnapshot,
  bundle: AgentObservabilitySecretBundle,
  deadline: ResolutionDeadline,
  maxAttempts: number,
): Promise<Reauthorization> {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      assertResolutionActive(deadline);
      const reauthorization = await awaitWithinResolutionDeadline(
        input.db.transaction(
          async (tx) => {
            await configureResolutionTransactionTimeouts(tx as DbTransaction, deadline);
            assertResolutionActive(deadline);
            const context = requireEnabledContext(
              await loadAgentObservabilitySessionContextInTransaction({
                tx: tx as DbTransaction,
                workspaceId: input.workspaceId,
                sessionId: input.sessionId,
              }),
            );
            assertResolutionActive(deadline);
            assertSamePinnedRelease(snapshot.context, context);

            const head = await loadExactCredentialHead(tx as DbTransaction, context);
            assertResolutionActive(deadline);
            const headComparison = compareCredentialHeads(snapshot.head, head);
            if (headComparison === 'retry') return { kind: 'retry' } as const;

            const authorizationId = newId('obsauth');
            // Validate the exact release shape before writing a success audit.
            // A schema failure must never create an audit row for unreleased bytes.
            const resolution = validatedResolutionResponse({
              authorizationId,
              context,
              credentialVersion: head.credentialVersion,
              bundle,
            });
            assertResolutionActive(deadline);
            await tx.insert(adminAuditEvents).values({
              id: newId('audit'),
              organizationId: context.organization_id,
              workspaceId: context.workspace_id,
              actor: input.actor,
              authMethod: 'internal-service',
              action: 'agent_observability.credential_resolved',
              targetType: 'agent_observability_binding',
              targetId: context.binding.id,
              requestId: input.requestId,
              result: 'success',
              metadata: {
                authorization_id: authorizationId,
                session_id: context.session_id,
                binding_version: context.binding.version,
                credential_version: head.credentialVersion,
                selection_source: context.selection_source,
                effective_capture_mode: context.capture.effective_mode,
              },
            });
            assertResolutionActive(deadline);
            return { kind: 'authorized', resolution } as const;
          },
          { isolationLevel: 'repeatable read' },
        ),
        deadline,
      );
      assertResolutionActive(deadline);
      return reauthorization;
    } catch (error) {
      // PostgreSQL aborts this transaction. Retry only in a new RR transaction;
      // keep the already-read bundle and the caller's original total deadline.
      if (isPostgresSerializationFailure(error) && attempt + 1 < maxAttempts) continue;
      throw mapResolutionError(error);
    }
  }

  throw unavailable();
}

/** Bound every transaction's lock and statement waits to the remaining release budget. */
async function configureResolutionTransactionTimeouts(
  tx: DbTransaction,
  deadline: ResolutionDeadline,
): Promise<void> {
  const timeout = `${assertResolutionActive(deadline)}ms`;
  await tx.execute(sql`
    select
      set_config('lock_timeout', ${timeout}, true),
      set_config('statement_timeout', ${timeout}, true)
  `);
  assertResolutionActive(deadline);
}

/**
 * SecretStore implementations may ignore cancellation. Race their promise so
 * this resolver returns at its deadline, while retaining a late rejection.
 */
async function resolveSecretStoreValue(
  secretStore: SecretStore,
  secretRef: string,
  deadline: ResolutionDeadline,
): Promise<string | null> {
  assertResolutionActive(deadline);
  const operation = Promise.resolve().then(() => {
    assertResolutionActive(deadline);
    deadline.signal.throwIfAborted();
    return secretStore.resolve(secretRef, { signal: deadline.signal });
  });
  // Keep a handler on a provider operation that outlives this resolver race.
  void operation.catch(() => undefined);
  return awaitWithinResolutionDeadline(operation, deadline);
}

/**
 * The deadline wins even when a provider or pool checkout ignores AbortSignal.
 * The underlying promise remains observed; transaction callbacks repeatedly
 * check the same signal before continuing to a release or audit write.
 */
async function awaitWithinResolutionDeadline<T>(
  operation: Promise<T>,
  deadline: ResolutionDeadline,
): Promise<T> {
  assertResolutionActive(deadline);
  const interruption = resolutionInterruption(deadline);
  void operation.catch(() => undefined);
  try {
    const outcome = await Promise.race([
      operation.then(
        (value) => ({ kind: 'resolved' as const, value }),
        (error: unknown) => ({ kind: 'failed' as const, error }),
      ),
      interruption.promise,
    ]);
    if (outcome.kind === 'interrupted') throw unavailable();
    if (outcome.kind === 'failed') throw outcome.error;
    assertResolutionActive(deadline);
    return outcome.value;
  } finally {
    interruption.dispose();
  }
}

function resolutionInterruption(deadline: ResolutionDeadline): {
  promise: Promise<{ kind: 'interrupted' }>;
  dispose: () => void;
} {
  assertResolutionActive(deadline);
  let resolve!: (value: { kind: 'interrupted' }) => void;
  const onAbort = () => resolve({ kind: 'interrupted' });
  const promise = new Promise<{ kind: 'interrupted' }>((resolvePromise) => {
    resolve = resolvePromise;
    deadline.signal.addEventListener('abort', onAbort, { once: true });
    if (deadline.signal.aborted) onAbort();
  });
  return {
    promise,
    dispose: () => deadline.signal.removeEventListener('abort', onAbort),
  };
}

/**
 * Context owns all authority loading. This is intentionally the sole place
 * outside mutation persistence that selects an observability secret reference.
 */
async function loadExactCredentialHead(
  tx: DbTransaction,
  context: EnabledContext,
): Promise<CredentialHead> {
  const binding = context.binding;
  const ownership = [
    eq(agentObservabilityBindingCredentials.bindingId, binding.id),
    eq(agentObservabilityBindings.id, binding.id),
    eq(agentObservabilityBindings.organizationId, context.organization_id),
    eq(agentObservabilityBindings.scopeType, binding.scope),
    eq(agentObservabilityBindings.adapterType, binding.target.adapter_type),
  ];
  if (binding.workspace_id === null) ownership.push(isNull(agentObservabilityBindings.workspaceId));
  else ownership.push(eq(agentObservabilityBindings.workspaceId, binding.workspace_id));

  const row = (
    await tx
      .select({
        bindingId: agentObservabilityBindingCredentials.bindingId,
        credentialVersion: agentObservabilityBindingCredentials.credentialVersion,
        secretRef: agentObservabilityBindingCredentials.secretRef,
        organizationId: agentObservabilityBindings.organizationId,
        scopeType: agentObservabilityBindings.scopeType,
        workspaceId: agentObservabilityBindings.workspaceId,
        adapterType: agentObservabilityBindings.adapterType,
      })
      .from(agentObservabilityBindingCredentials)
      .innerJoin(
        agentObservabilityBindings,
        eq(agentObservabilityBindingCredentials.bindingId, agentObservabilityBindings.id),
      )
      .where(and(...ownership))
      .for('share')
      .limit(1)
  )[0];

  if (
    !row ||
    row.bindingId !== binding.id ||
    row.organizationId !== context.organization_id ||
    row.scopeType !== binding.scope ||
    row.workspaceId !== binding.workspace_id ||
    row.adapterType !== binding.target.adapter_type ||
    !isPositiveSafeInteger(row.credentialVersion) ||
    row.credentialVersion !== binding.current_credential_version ||
    !isAgentObservabilitySecretReference(row.secretRef)
  ) {
    throw unavailable();
  }
  return {
    bindingId: row.bindingId,
    credentialVersion: row.credentialVersion,
    secretRef: row.secretRef,
  };
}

function requireEnabledContext(context: AgentObservabilitySessionContext): EnabledContext {
  if (context.status === 'disabled' || context.status === 'suppressed') {
    throw new AgentObservabilitySecretResolutionDeniedError();
  }
  if (
    context.binding === null ||
    !isPositiveSafeInteger(context.binding.current_credential_version) ||
    !isPositiveSafeInteger(context.binding.version)
  ) {
    throw unavailable();
  }
  return context as EnabledContext;
}

/**
 * Selection-pointer changes are intentionally absent; the pinned release
 * identity is immutable. A same-version target/config mutation is corruption,
 * never an opportunity to release bytes against changed authority.
 */
function assertSamePinnedRelease(previous: EnabledContext, current: EnabledContext): void {
  const previousBinding = previous.binding;
  const currentBinding = current.binding;
  if (
    previous.organization_id !== current.organization_id ||
    previous.workspace_id !== current.workspace_id ||
    previous.session_id !== current.session_id ||
    previous.selection_source !== current.selection_source ||
    previous.capture.pinned_mode !== current.capture.pinned_mode ||
    previousBinding.id !== currentBinding.id ||
    previousBinding.version !== currentBinding.version ||
    previousBinding.scope !== currentBinding.scope ||
    previousBinding.workspace_id !== currentBinding.workspace_id ||
    previousBinding.target.adapter_type !== currentBinding.target.adapter_type ||
    previousBinding.target.endpoint_kind !== currentBinding.target.endpoint_kind ||
    previousBinding.target.endpoint_class !== currentBinding.target.endpoint_class ||
    previousBinding.target.endpoint_url !== currentBinding.target.endpoint_url ||
    previousBinding.target.external_project_id !== currentBinding.target.external_project_id ||
    previousBinding.config.semantic_profile !== currentBinding.config.semantic_profile ||
    previousBinding.config.protocol !== currentBinding.config.protocol ||
    previousBinding.config.compression !== currentBinding.config.compression ||
    previousBinding.config.timeout_ms !== currentBinding.config.timeout_ms ||
    previousBinding.config.environment !== currentBinding.config.environment ||
    previousBinding.config.release !== currentBinding.config.release ||
    previousBinding.config.capture_mode !== currentBinding.config.capture_mode ||
    previousBinding.config.sample_rate !== currentBinding.config.sample_rate ||
    previousBinding.config.config_schema_version !== currentBinding.config.config_schema_version
  ) {
    throw unavailable();
  }
}

/** A rotation is coherent only when both generation and opaque ref advance together. */
function compareCredentialHeads(
  previous: CredentialHead,
  current: CredentialHead,
): 'same' | 'retry' {
  if (previous.bindingId !== current.bindingId) throw unavailable();
  const versionChanged = previous.credentialVersion !== current.credentialVersion;
  const referenceChanged = previous.secretRef !== current.secretRef;
  if (!versionChanged && !referenceChanged) return 'same';
  if (
    versionChanged &&
    referenceChanged &&
    current.credentialVersion > previous.credentialVersion
  ) {
    return 'retry';
  }
  throw unavailable();
}

function resolutionResponse(input: {
  authorizationId: string;
  context: EnabledContext;
  credentialVersion: number;
  bundle: AgentObservabilitySecretBundle;
}): AgentObservabilitySecretResolution {
  const { binding } = input.context;
  return {
    schema_version: 1,
    authorization_id: input.authorizationId,
    binding_id: binding.id,
    binding_version: binding.version,
    credential_version: input.credentialVersion,
    effective_capture_mode: input.context.capture.effective_mode,
    bundle: bundleResponse(input.bundle),
  };
}

function validatedResolutionResponse(input: {
  authorizationId: string;
  context: EnabledContext;
  credentialVersion: number;
  bundle: AgentObservabilitySecretBundle;
}): AgentObservabilitySecretResolution {
  const parsed = AgentObservabilitySecretResolutionSchema.safeParse(resolutionResponse(input));
  if (!parsed.success) throw unavailable();
  return parsed.data;
}

function bundleResponse(
  bundle: AgentObservabilitySecretBundle,
): AgentObservabilitySecretResolution['bundle'] {
  if (bundle.adapterType === 'langfuse_sdk') {
    return {
      adapter_type: 'langfuse_sdk',
      public_key: bundle.publicKey,
      secret_key: bundle.secretKey,
    };
  }
  if (bundle.auth.type === 'basic') {
    return {
      adapter_type: 'otlp_http',
      auth: {
        type: 'basic',
        username: bundle.auth.username,
        password: bundle.auth.password,
      },
    };
  }
  if (bundle.auth.type === 'bearer') {
    return { adapter_type: 'otlp_http', auth: { type: 'bearer', token: bundle.auth.token } };
  }
  return {
    adapter_type: 'otlp_http',
    auth: { type: 'custom_headers', headers: { ...bundle.auth.headers } },
  };
}

function mapResolutionError(error: unknown): Error {
  if (
    error instanceof AgentObservabilitySecretResolutionNotFoundError ||
    error instanceof AgentObservabilitySecretResolutionDeniedError ||
    error instanceof AgentObservabilitySecretResolutionUnavailableError
  ) {
    return error;
  }
  if (error instanceof AgentObservabilitySessionContextNotFoundError) {
    return new AgentObservabilitySecretResolutionNotFoundError();
  }
  if (error instanceof AgentObservabilitySessionContextUnavailableError) return unavailable();
  // PostgreSQL lock, statement, and serialization failures intentionally share
  // this sanitized outcome. All other errors fail closed below as well.
  if (isResolutionDatabaseFailure(error)) return unavailable();
  return unavailable();
}

function resolveMaxAttempts(value: number | undefined): number {
  if (value === undefined) return AGENT_OBSERVABILITY_SECRET_RESOLUTION_MAX_ATTEMPTS;
  if (
    !Number.isSafeInteger(value) ||
    value <= 0 ||
    value > AGENT_OBSERVABILITY_SECRET_RESOLUTION_MAX_ATTEMPTS
  ) {
    throw unavailable();
  }
  return value;
}

function createResolutionDeadline(
  deadlineMs: number | undefined,
  parentSignal: AbortSignal | undefined,
): ResolutionDeadline {
  const timeoutMs = resolveDeadlineMs(deadlineMs);
  const controller = new AbortController();
  const abort = () => {
    if (!controller.signal.aborted) controller.abort(parentSignal?.reason);
  };
  const onParentAbort = () => abort();
  const timer = setTimeout(abort, timeoutMs);
  if (parentSignal?.aborted) abort();
  else parentSignal?.addEventListener('abort', onParentAbort, { once: true });
  return {
    expiresAt: performance.now() + timeoutMs,
    signal: controller.signal,
    dispose() {
      clearTimeout(timer);
      parentSignal?.removeEventListener('abort', onParentAbort);
    },
  };
}

function resolveDeadlineMs(value: number | undefined): number {
  if (value === undefined) return AGENT_OBSERVABILITY_SECRET_RESOLUTION_DEADLINE_MS;
  if (
    !Number.isSafeInteger(value) ||
    value <= 0 ||
    value > AGENT_OBSERVABILITY_SECRET_RESOLUTION_DEADLINE_MS
  ) {
    throw unavailable();
  }
  return value;
}

/** Returns an integer timeout PostgreSQL can safely receive in a set_config parameter. */
function assertResolutionActive(deadline: ResolutionDeadline): number {
  if (deadline.signal.aborted) throw unavailable();
  const remainingMs = deadline.expiresAt - performance.now();
  if (!Number.isFinite(remainingMs) || remainingMs <= 0) throw unavailable();
  return Math.max(1, Math.ceil(remainingMs));
}

function isPostgresSerializationFailure(error: unknown): boolean {
  return postgresSqlState(error) === '40001';
}

function isResolutionDatabaseFailure(error: unknown): boolean {
  const sqlState = postgresSqlState(error);
  return sqlState === '55P03' || sqlState === '57014' || sqlState === '40001';
}

function postgresSqlState(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null || !('code' in error)) return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === 'string' ? code : undefined;
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function unavailable(): AgentObservabilitySecretResolutionUnavailableError {
  return new AgentObservabilitySecretResolutionUnavailableError();
}
