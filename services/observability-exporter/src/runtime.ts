// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { KafkaSessionEventSource, KafkaTranscriptStore } from '@orca/transcript-store';
import type { Event } from '@orca/transcript-store-types';
import { boundedAgentEventId, eventIdentityKey } from './event-identity.js';
import {
  LitefuseOtlpHttpClient,
  OtlpHttpResponseError,
  OtlpHttpStatusError,
  OtlpHttpTransportError,
  OtlpPartialSuccessError,
} from './litefuse-client.js';
import { deliveryRetryDelayMs } from './delivery-retry.js';
import { isLangfuseHttpJsonContext } from './delivery-capabilities.js';
import { encodeLangfuseOtlpJson } from './otlp-json.js';
import { createHardenedOtlpFetch, OtlpEgressPolicyError } from './egress.js';
import { DELIVERY_LEASE_OWNER_SUFFIX, PROJECTOR_LEASE_OWNER_SUFFIX } from './lease-owner.js';
import {
  ExporterLeaseLostError,
  ExporterReplayConflictError,
  hashTranscriptEnvelope,
  ObservabilityExporterRepository,
  type ClaimedProjectionSession,
  type InboxEventIdentity,
} from './persistence.js';
import {
  CanonicalProjectionError,
  CanonicalProjectionStateError,
  reduceCanonicalEventBatch,
} from './projector.js';
import {
  RegistryObservabilityClient,
  RegistryResolverHttpError,
  RegistryResolverResponseError,
  RegistryResolverScopeError,
} from './registry-client.js';
import { TRACE_SAMPLING_VERSION } from './sampling.js';
import type { OtlpDeliveryOutcome, PinnedDeliveryContext } from './types.js';
import { PROJECTED_TRACE_SCHEMA_VERSION } from './types.js';

const LEASE_COMPLETION_MARGIN_MS = 5_000;

class UnreadableTranscriptCursorError extends Error {
  constructor() {
    super('claimed Transcript cursor was not readable');
    this.name = 'UnreadableTranscriptCursorError';
  }
}

export interface KafkaExporterRuntimeOptions {
  repository: ObservabilityExporterRepository;
  transcriptStore: Pick<KafkaTranscriptStore, 'read'>;
  eventSource: Pick<KafkaSessionEventSource, 'start' | 'stop'>;
  registryClient: Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
  workerId: string;
  projectorLeaseMs: number;
  projectorBatchSize: number;
  projectorBatchBytes: number;
  deliveryLeaseMs: number;
  registryRequestTimeoutMs: number;
  projectorPollMs: number;
  deliveryPollMs: number;
  otlpFetchImpl?: typeof fetch;
  retryRandom?: () => number;
}

/**
 * Narrow Kafka Transcript vertical slice. Kafka owns source/replay; Postgres
 * owns only exporter state. The event callback returns after inbox commit;
 * that ACK ledger does not gate authoritative Kafka replay.
 */
export class KafkaObservabilityExporterRuntime {
  private readonly repository: ObservabilityExporterRepository;
  private readonly transcriptStore: Pick<KafkaTranscriptStore, 'read'>;
  private readonly eventSource: Pick<KafkaSessionEventSource, 'start' | 'stop'>;
  private readonly registryClient: Pick<
    RegistryObservabilityClient,
    'resolveContext' | 'resolveSecret'
  >;
  private readonly workerId: string;
  private readonly projectorLeaseMs: number;
  private readonly projectorBatchSize: number;
  private readonly projectorBatchBytes: number;
  private readonly deliveryLeaseMs: number;
  private readonly registryRequestTimeoutMs: number;
  private readonly projectorPollMs: number;
  private readonly deliveryPollMs: number;
  private readonly otlpFetchImpl: typeof fetch;
  private readonly retryRandom: () => number;
  private projectorLastErrorCode: string | null = null;
  private deliveryLastErrorCode: string | null = null;

  constructor(options: KafkaExporterRuntimeOptions) {
    this.repository = options.repository;
    this.transcriptStore = options.transcriptStore;
    this.eventSource = options.eventSource;
    this.registryClient = options.registryClient;
    this.workerId = options.workerId;
    this.projectorLeaseMs = options.projectorLeaseMs;
    this.projectorBatchSize = options.projectorBatchSize;
    this.projectorBatchBytes = options.projectorBatchBytes;
    this.deliveryLeaseMs = options.deliveryLeaseMs;
    this.registryRequestTimeoutMs = options.registryRequestTimeoutMs;
    this.projectorPollMs = options.projectorPollMs;
    this.deliveryPollMs = options.deliveryPollMs;
    this.otlpFetchImpl = options.otlpFetchImpl ?? createHardenedOtlpFetch();
    this.retryRandom = options.retryRandom ?? Math.random;
  }

  async startIngestion(): Promise<void> {
    await this.eventSource.start(async (event) => {
      // KafkaSessionEventSource commits only after this handler resolves.
      await this.repository.acceptEvent(event);
    });
  }

  async stopIngestion(): Promise<void> {
    await this.eventSource.stop();
  }

  async run(signal: AbortSignal): Promise<void> {
    await Promise.all([this.runProjector(signal), this.runDelivery(signal)]);
  }

  /** Project one claimed Session from Kafka's authoritative source order. */
  async projectOnce(signal?: AbortSignal): Promise<boolean> {
    signal?.throwIfAborted();
    const claim = await this.repository.claimSession(
      `${this.workerId}${PROJECTOR_LEASE_OWNER_SUFFIX}`,
      this.projectorLeaseMs,
    );
    if (claim === null) return false;

    try {
      signal?.throwIfAborted();
      await this.projectClaimedSession(claim, signal);
      return true;
    } catch (error) {
      if (error instanceof ExporterLeaseLostError || error instanceof ExporterReplayConflictError) {
        throw error;
      }
      if (signal?.aborted) {
        await this.repository.releaseProjectionClaim(claim).catch(() => undefined);
      } else if (
        error instanceof CanonicalProjectionError ||
        error instanceof CanonicalProjectionStateError
      ) {
        await this.repository
          .quarantineProjectionClaim(claim, 'canonical_projection_error')
          .catch(() => undefined);
      } else if (isTerminalContextResolverError(error)) {
        await this.repository
          .quarantineProjectionClaim(claim, 'registry_context_rejected')
          .catch(() => undefined);
      } else if (error instanceof RegistryResolverScopeError) {
        await this.repository
          .quarantineProjectionClaim(claim, 'registry_scope_invalid')
          .catch(() => undefined);
      } else if (error instanceof UnreadableTranscriptCursorError) {
        await this.repository
          .quarantineProjectionClaim(claim, 'transcript_cursor_unreadable')
          .catch(() => undefined);
      } else {
        await this.repository.releaseProjectionClaim(claim).catch(() => undefined);
      }
      throw error;
    }
  }

  private async projectClaimedSession(
    claim: ClaimedProjectionSession,
    signal?: AbortSignal,
  ): Promise<void> {
    const heartbeatController = new AbortController();
    const claimController = new AbortController();
    const workSignal =
      signal === undefined
        ? claimController.signal
        : AbortSignal.any([signal, claimController.signal]);
    const heartbeatSignal =
      signal === undefined
        ? heartbeatController.signal
        : AbortSignal.any([signal, heartbeatController.signal]);
    let heartbeatError: unknown;
    const heartbeat = this.maintainProjectionLease(claim, heartbeatSignal).catch(
      (error: unknown) => {
        heartbeatError = error;
        claimController.abort(error);
      },
    );
    try {
      const replayedEvents: Event[] = [];
      let scannedNextSeq: string | undefined;
      for await (const event of this.transcriptStore.read(claim.workspaceId, claim.sessionId, {
        fromCursor: claim.nextSeq,
        maxEvents: this.projectorBatchSize,
        maxBytes: this.projectorBatchBytes,
        onScannedCursor: (nextCursor) => {
          scannedNextSeq = nextCursor;
        },
        subpath: '*',
        signal: workSignal,
      })) {
        throwHeartbeatError(heartbeatError);
        workSignal.throwIfAborted();
        replayedEvents.push(event);
      }
      throwHeartbeatError(heartbeatError);
      workSignal.throwIfAborted();
      const inboxIdentities = await this.repository.loadInboxEventIdentities(
        claim,
        replayedEvents.map((event) => event.id),
      );
      workSignal.throwIfAborted();
      const canonical = selectCanonicalReplayEvents(replayedEvents, inboxIdentities);
      if (canonical.conflict !== undefined) {
        await this.repository.quarantineReplayConflict(claim, canonical.conflict);
        throw new ExporterReplayConflictError();
      }
      const events = canonical.events;
      const acceptedSourceIds = await this.repository.loadAcceptedSourceIds(
        claim,
        acceptanceCandidateIds(events),
      );
      throwHeartbeatError(heartbeatError);
      workSignal.throwIfAborted();
      // Resolve the immutable Session policy before the reducer can construct observations.
      const deliveryContext = await this.projectionContext(claim, events, workSignal);
      throwHeartbeatError(heartbeatError);
      workSignal.throwIfAborted();
      const result = reduceCanonicalEventBatch(
        claim.state,
        events,
        acceptedSourceIds,
        deliveryContext === undefined
          ? undefined
          : {
              algorithmVersion: TRACE_SAMPLING_VERSION,
              bindingId: deliveryContext.bindingId,
              bindingVersion: deliveryContext.bindingVersion,
              sampleRate: deliveryContext.sampleRate,
            },
      );
      if (result.issues.length > 0) throw new CanonicalProjectionError(result.issues);

      const traces =
        deliveryContext === undefined
          ? []
          : result.completedTraces.map((trace) => ({ trace, deliveryContext }));
      throwHeartbeatError(heartbeatError);
      workSignal.throwIfAborted();
      const nextSeq = nextReplayCursor(claim, replayedEvents, scannedNextSeq);
      await this.repository.completeProjection(
        claim,
        nextSeq,
        result.state,
        traces,
        result.acceptedSourceIds,
      );
    } catch (error) {
      throwHeartbeatError(heartbeatError);
      throw error;
    } finally {
      heartbeatController.abort();
      claimController.abort();
      await heartbeat;
    }
  }

  /** Resolve current Registry authority, then deliver one claimed canonical outbox row. */
  async deliverOnce(signal?: AbortSignal): Promise<boolean> {
    return (await this.attemptDelivery(signal)) !== false;
  }

  private async attemptDelivery(
    signal?: AbortSignal,
  ): Promise<boolean | { retryErrorCode: string }> {
    signal?.throwIfAborted();
    const item = await this.repository.claimOutbox(
      `${this.workerId}${DELIVERY_LEASE_OWNER_SUFFIX}`,
      this.deliveryLeaseMs,
      signal,
    );
    if (item === null) return false;

    let refreshedAfterCredentialRejection = false;
    let resolvingSecret = false;
    try {
      // The SQL runtime deliberately remains metadata-only, including restored outbox rows.
      if (
        item.trace.schemaVersion !== PROJECTED_TRACE_SCHEMA_VERSION ||
        item.deliveryContext.captureMode !== 'metadata_only'
      ) {
        await this.repository.markOutboxSuppressed(item, 'capture_mode_mismatch');
        return true;
      }
      const payload = encodeLangfuseOtlpJson(item.trace, item.deliveryContext);
      while (true) {
        signal?.throwIfAborted();
        if (refreshedAfterCredentialRejection) {
          await this.repository.renewOutboxClaim(
            item,
            this.registryRequestTimeoutMs +
              item.deliveryContext.timeoutMs +
              LEASE_COMPLETION_MARGIN_MS,
          );
        }

        resolvingSecret = true;
        const secret = await this.registryClient.resolveSecret({
          workspaceId: item.trace.workspaceId,
          sessionId: item.trace.sessionId,
          signal: requestSignal(signal, this.registryRequestTimeoutMs),
        });
        resolvingSecret = false;
        signal?.throwIfAborted();
        if (
          secret.bindingId !== item.deliveryContext.bindingId ||
          secret.bindingVersion !== item.deliveryContext.bindingVersion
        ) {
          await this.repository.markOutboxSuppressed(item, 'binding_mismatch');
          return true;
        }
        if (secret.effectiveCaptureMode !== 'metadata_only') {
          await this.repository.markOutboxSuppressed(item, 'capture_mode_mismatch');
          return true;
        }
        if (secret.auth.type !== 'basic') {
          await this.repository.markOutboxSuppressed(item, 'unsupported_credential');
          return true;
        }

        await this.repository.renewOutboxClaim(
          item,
          item.deliveryContext.timeoutMs + LEASE_COMPLETION_MARGIN_MS,
        );
        signal?.throwIfAborted();

        const client = new LitefuseOtlpHttpClient({
          endpoint: item.deliveryContext.endpointUrl,
          publicKey: secret.auth.username,
          secretKey: secret.auth.password,
          timeoutMs: item.deliveryContext.timeoutMs,
          fetchImpl: this.otlpFetchImpl,
        });
        let outcome: OtlpDeliveryOutcome;
        try {
          outcome = await client.send(payload, signal);
        } catch (error) {
          if (error instanceof OtlpPartialSuccessError) {
            // A complete partial response is terminal even if shutdown arrives
            // at EOF. Do not let cancellation release it for whole-row replay.
            outcome = {
              kind: 'partial_rejection',
              rejectedSpans: error.rejectedSpans,
              messageBytes: error.messageBytes,
              messageSha256: error.messageSha256,
            };
          } else if (isCredentialRejectedOtlpStatus(error)) {
            if (!refreshedAfterCredentialRejection) {
              refreshedAfterCredentialRejection = true;
              continue;
            }
            await this.repository.markOutboxSuppressed(item, 'credential_rejected');
            return true;
          } else {
            throw error;
          }
        }
        await this.repository.completeOutboxDelivery(item, outcome);
        return true;
      }
    } catch (error) {
      if (error instanceof ExporterLeaseLostError) throw error;
      if (signal?.aborted) {
        await this.repository.releaseOutboxClaim(item).catch(() => undefined);
        throw error;
      }
      if (error instanceof RegistryResolverScopeError) {
        await this.repository.markOutboxSuppressed(item, 'registry_scope_invalid');
        return true;
      }
      if (error instanceof OtlpHttpResponseError) {
        await this.repository.markOutboxSuppressed(item, 'invalid_otlp_response');
        return true;
      }
      if (error instanceof OtlpEgressPolicyError) {
        await this.repository.markOutboxSuppressed(item, 'egress_denied');
        return true;
      }
      const retry = retryableDeliveryFailure(error, resolvingSecret);
      if (retry !== null) {
        const delayMs = deliveryRetryDelayMs(
          item.attemptCount,
          retry.retryAfterMs,
          this.retryRandom,
        );
        await this.repository.scheduleOutboxRetry(item, delayMs, retry.errorCode);
        // Durable scheduling is progress, but not recovery for error-log deduplication.
        return { retryErrorCode: runtimeFailureCode(error) };
      }
      if (error instanceof OtlpHttpStatusError) {
        await this.repository.markOutboxSuppressed(item, 'permanent_http_status');
        return true;
      }
      if (error instanceof RegistryResolverHttpError) {
        await this.repository.markOutboxSuppressed(item, 'registry_denied');
        return true;
      }
      await this.repository.releaseOutboxClaim(item).catch(() => undefined);
      throw error;
    }
  }

  private async maintainProjectionLease(
    claim: ClaimedProjectionSession,
    signal: AbortSignal,
  ): Promise<void> {
    const intervalMs = Math.max(1_000, Math.min(10_000, Math.floor(this.projectorLeaseMs / 3)));
    while (!signal.aborted) {
      await wait(intervalMs, signal);
      if (signal.aborted) return;
      await this.repository.renewProjectionClaim(claim, this.projectorLeaseMs);
    }
  }

  private async projectionContext(
    claim: ClaimedProjectionSession,
    events: readonly Event[],
    signal?: AbortSignal,
  ): Promise<PinnedDeliveryContext | undefined> {
    if (events.length === 0) return undefined;
    const context = await this.registryClient.resolveContext({
      workspaceId: claim.workspaceId,
      sessionId: claim.sessionId,
      signal: requestSignal(signal, this.registryRequestTimeoutMs),
    });
    if (context.status !== 'enabled') return undefined;

    const deliveryContext = context.deliveryContext;
    return isLangfuseHttpJsonContext(deliveryContext) ? deliveryContext : undefined;
  }

  private async runProjector(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      try {
        const progressed = await this.projectOnce(signal);
        if (progressed) this.projectorLastErrorCode = null;
        else await wait(this.projectorPollMs, signal);
      } catch (error) {
        if (signal.aborted) return;
        const code = runtimeFailureCode(error);
        if (this.projectorLastErrorCode !== code) {
          console.error(JSON.stringify({ component: 'observability-exporter-projector', code }));
        }
        this.projectorLastErrorCode = code;
        if (!signal.aborted) await wait(this.projectorPollMs, signal);
      }
    }
  }

  private async runDelivery(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      try {
        const outcome = await this.attemptDelivery(signal);
        if (typeof outcome === 'object') {
          if (!signal.aborted) this.logDeliveryFailure(outcome.retryErrorCode);
        } else if (outcome) this.deliveryLastErrorCode = null;
        else await wait(this.deliveryPollMs, signal);
      } catch (error) {
        if (signal.aborted) return;
        this.logDeliveryFailure(runtimeFailureCode(error));
        if (!signal.aborted) await wait(this.deliveryPollMs, signal);
      }
    }
  }

  private logDeliveryFailure(code: string): void {
    if (this.deliveryLastErrorCode !== code) {
      console.error(JSON.stringify({ component: 'observability-exporter-delivery', code }));
    }
    this.deliveryLastErrorCode = code;
  }
}

function acceptanceCandidateIds(events: readonly Event[]): string[] {
  const candidates = new Set<string>();
  for (const event of events) {
    if (
      event.subpath === '' &&
      event.producedBy === 'client' &&
      (event.kind.startsWith('user.') || event.kind === 'system.message')
    ) {
      const eventId = boundedAgentEventId(event.id);
      if (eventId !== undefined) candidates.add(eventId);
      continue;
    }
    if (event.subpath !== '' || event.kind !== 'session.user_event_processed') continue;
    try {
      const payload = JSON.parse(Buffer.from(event.payload).toString('utf8')) as unknown;
      const sourceEventId =
        payload !== null && typeof payload === 'object' && !Array.isArray(payload)
          ? (payload as Record<string, unknown>).user_event_id
          : undefined;
      const boundedSourceEventId = boundedAgentEventId(sourceEventId);
      if (boundedSourceEventId !== undefined) candidates.add(boundedSourceEventId);
    } catch {
      // The reducer records the typed invalid-marker issue.
    }
  }
  return [...candidates];
}

export function selectCanonicalReplayEvents(
  events: readonly Event[],
  persistedIdentities: readonly InboxEventIdentity[],
): {
  events: Event[];
  conflict?: { eventKey: string; existingHash: string; incomingHash: string };
} {
  const persisted = new Map(
    persistedIdentities.map((identity) => [identity.eventKey, identity] as const),
  );
  const local = new Map<string, { sourceSeq: string; sourceHash: string }>();
  const selectedIds = new Set<string>();
  const selected: Event[] = [];

  for (const event of events) {
    if (!Number.isSafeInteger(event.seq) || event.seq < 0) {
      throw new Error('Kafka Transcript returned an invalid source sequence');
    }
    const sourceSeq = BigInt(event.seq).toString();
    const sourceHash = hashTranscriptEnvelope(event);
    const eventKey = eventIdentityKey(event.id);
    const existing = persisted.get(eventKey);
    if (existing !== undefined) {
      if (existing.sourceHash !== sourceHash || BigInt(sourceSeq) < BigInt(existing.sourceSeq)) {
        return {
          events: selected,
          conflict: {
            eventKey,
            existingHash: existing.sourceHash,
            incomingHash: sourceHash,
          },
        };
      }
      if (sourceSeq === existing.sourceSeq && !selectedIds.has(eventKey)) {
        selected.push(event);
        selectedIds.add(eventKey);
      }
      continue;
    }

    const first = local.get(eventKey);
    if (first === undefined) {
      local.set(eventKey, { sourceSeq, sourceHash });
      selected.push(event);
      selectedIds.add(eventKey);
      continue;
    }
    if (first.sourceHash !== sourceHash) {
      return {
        events: selected,
        conflict: {
          eventKey,
          existingHash: first.sourceHash,
          incomingHash: sourceHash,
        },
      };
    }
  }
  return { events: selected };
}

function nextReplayCursor(
  claim: ClaimedProjectionSession,
  replayedEvents: readonly Event[],
  scannedNextSeq?: string,
): string {
  if (scannedNextSeq !== undefined) {
    if (BigInt(scannedNextSeq) <= BigInt(claim.nextSeq)) {
      throw new Error('Kafka Transcript returned an invalid scanned cursor');
    }
    return scannedNextSeq;
  }
  const last = replayedEvents.at(-1);
  if (last !== undefined) {
    if (!Number.isSafeInteger(last.seq) || last.seq < 0) {
      throw new Error('Kafka Transcript returned an invalid source sequence');
    }
    return (BigInt(last.seq) + 1n).toString();
  }
  if (BigInt(claim.firstPendingSeq) > BigInt(claim.nextSeq)) return claim.firstPendingSeq;
  throw new UnreadableTranscriptCursorError();
}

function throwHeartbeatError(error: unknown): void {
  if (error !== undefined) throw error;
}

function isRetryableOtlpStatus(status: number): boolean {
  return status === 429 || status === 502 || status === 503 || status === 504;
}

function isCredentialRejectedOtlpStatus(error: unknown): error is OtlpHttpStatusError {
  return error instanceof OtlpHttpStatusError && (error.status === 401 || error.status === 403);
}

function retryableDeliveryFailure(
  error: unknown,
  resolvingSecret: boolean,
): { errorCode: string; retryAfterMs: number | undefined } | null {
  if (error instanceof OtlpHttpTransportError) {
    return {
      errorCode: error.kind === 'timeout' ? 'otlp_timeout' : 'otlp_transport',
      retryAfterMs: undefined,
    };
  }
  if (error instanceof OtlpHttpStatusError && isRetryableOtlpStatus(error.status)) {
    return { errorCode: `otlp_http_${error.status}`, retryAfterMs: error.retryAfterMs };
  }
  if (error instanceof RegistryResolverResponseError) {
    return { errorCode: 'registry_invalid_response', retryAfterMs: undefined };
  }
  if (error instanceof RegistryResolverHttpError && isRetryableRegistryStatus(error.status)) {
    return { errorCode: 'registry_unavailable', retryAfterMs: undefined };
  }
  if (
    resolvingSecret &&
    !(error instanceof RegistryResolverHttpError) &&
    !(error instanceof RegistryResolverResponseError) &&
    !(error instanceof RegistryResolverScopeError)
  ) {
    return { errorCode: 'registry_unavailable', retryAfterMs: undefined };
  }
  return null;
}

function isRetryableRegistryStatus(status: number): boolean {
  return (
    status === 401 ||
    status === 403 ||
    status === 408 ||
    status === 425 ||
    status === 429 ||
    (status >= 500 && status <= 599)
  );
}

function isTerminalContextResolverError(error: unknown): boolean {
  return (
    error instanceof RegistryResolverHttpError &&
    error.resolver === 'context' &&
    (error.status === 400 || error.status === 404)
  );
}

function runtimeFailureCode(error: unknown): string {
  if (error instanceof ExporterLeaseLostError) return 'lease_lost';
  if (error instanceof ExporterReplayConflictError) return 'replay_conflict';
  if (error instanceof CanonicalProjectionError) return 'canonical_projection_error';
  if (error instanceof CanonicalProjectionStateError) return 'canonical_projection_state_error';
  if (error instanceof UnreadableTranscriptCursorError) return 'transcript_cursor_unreadable';
  if (error instanceof RegistryResolverHttpError) return `registry_http_${error.status}`;
  if (error instanceof RegistryResolverResponseError) return 'registry_invalid_response';
  if (error instanceof RegistryResolverScopeError) return 'registry_scope_invalid';
  if (error instanceof OtlpHttpStatusError) return `otlp_http_${error.status}`;
  if (error instanceof OtlpHttpTransportError) return `otlp_${error.kind}`;
  if (error instanceof OtlpPartialSuccessError) return 'otlp_partial_rejection';
  if (error instanceof OtlpHttpResponseError) return 'otlp_invalid_response';
  if (error instanceof OtlpEgressPolicyError) return 'egress_denied';
  return 'unexpected_error';
}

function requestSignal(signal: AbortSignal | undefined, timeoutMs: number): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs);
  return signal === undefined ? timeout : AbortSignal.any([signal, timeout]);
}

async function wait(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(done, milliseconds);
    const onAbort = (): void => done();
    function done(): void {
      clearTimeout(timer);
      signal.removeEventListener('abort', onAbort);
      resolve();
    }
    signal.addEventListener('abort', onAbort, { once: true });
  });
}
