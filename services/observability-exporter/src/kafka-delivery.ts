// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHardenedOtlpFetch, OtlpEgressPolicyError } from './egress.js';
import { parseKafkaDeliveryContext, parseKafkaProjectedTrace } from './kafka-validation.js';
import {
  LitefuseOtlpHttpClient,
  OtlpHttpResponseError,
  OtlpHttpStatusError,
  OtlpHttpTransportError,
  OtlpPartialSuccessError,
} from './litefuse-client.js';
import { encodeLangfuseOtlpJson } from './otlp-json.js';
import {
  type RegistryObservabilityClient,
  RegistryResolverHttpError,
  RegistryResolverResponseError,
  RegistryResolverScopeError,
} from './registry-client.js';
import type { PinnedDeliveryContext, ProjectedTrace } from './types.js';
import { IO_PROJECTED_TRACE_SCHEMA_VERSION } from './types.js';

export type KafkaTraceDeliveryResult =
  | { kind: 'terminal'; reason: string }
  | { kind: 'retry'; retryAfterMs?: number };

/** One Kafka delivery attempt, with at most one credential refresh. No persistence or sleeps.
 * Caller cancellation and unexpected local errors throw; complete OTLP outcomes are terminal.
 */
export async function deliverKafkaTrace(
  record: { trace: ProjectedTrace; deliveryContext: PinnedDeliveryContext },
  options: {
    registryClient: Pick<RegistryObservabilityClient, 'resolveSecret'>;
    registryRequestTimeoutMs: number;
    otlpFetchImpl?: typeof fetch;
  },
  signal?: AbortSignal,
): Promise<KafkaTraceDeliveryResult> {
  signal?.throwIfAborted();
  if (
    !Number.isSafeInteger(options.registryRequestTimeoutMs) ||
    options.registryRequestTimeoutMs <= 0 ||
    options.registryRequestTimeoutMs > 4_294_967_295
  ) {
    throw new Error('observability exporter registry request timeout is invalid');
  }
  let context: PinnedDeliveryContext;
  try {
    context = parsePinnedDeliveryContext(record.deliveryContext);
  } catch {
    return { kind: 'terminal', reason: 'invalid_delivery_context' };
  }
  let trace: ProjectedTrace;
  try {
    trace = parseKafkaProjectedTrace(record.trace);
  } catch {
    return { kind: 'terminal', reason: 'invalid_projected_trace' };
  }
  const requiresRawIo = trace.schemaVersion === IO_PROJECTED_TRACE_SCHEMA_VERSION;
  if (requiresRawIo && context.captureMode !== 'raw_io') {
    return { kind: 'terminal', reason: 'capture_mode_mismatch' };
  }
  const payload = encodeLangfuseOtlpJson(trace, context);
  const fetchImpl = options.otlpFetchImpl ?? createHardenedOtlpFetch();
  let resolvingSecret = false;
  let refreshed = false;
  try {
    while (true) {
      signal?.throwIfAborted();
      resolvingSecret = true;
      const timeout = AbortSignal.timeout(options.registryRequestTimeoutMs);
      const secret = await options.registryClient.resolveSecret({
        workspaceId: trace.workspaceId,
        sessionId: trace.sessionId,
        signal: signal === undefined ? timeout : AbortSignal.any([signal, timeout]),
      });
      resolvingSecret = false;
      signal?.throwIfAborted();
      if (
        secret.bindingId !== context.bindingId ||
        secret.bindingVersion !== context.bindingVersion
      ) {
        return { kind: 'terminal', reason: 'binding_mismatch' };
      }
      if (requiresRawIo && secret.effectiveCaptureMode !== 'raw_io') {
        return { kind: 'terminal', reason: 'capture_mode_mismatch' };
      }
      if (secret.auth.type !== 'basic') {
        return { kind: 'terminal', reason: 'unsupported_credential' };
      }
      const client = new LitefuseOtlpHttpClient({
        endpoint: context.endpointUrl,
        publicKey: secret.auth.username,
        secretKey: secret.auth.password,
        timeoutMs: context.timeoutMs,
        fetchImpl,
      });
      try {
        const outcome = await client.send(payload, signal);
        return { kind: 'terminal', reason: outcome.kind };
      } catch (error) {
        // A complete partial response must never be replayed, including cancellation at EOF.
        if (error instanceof OtlpPartialSuccessError) {
          return { kind: 'terminal', reason: 'partial_rejection' };
        }
        if (
          error instanceof OtlpHttpStatusError &&
          (error.status === 401 || error.status === 403)
        ) {
          signal?.throwIfAborted();
          if (refreshed) return { kind: 'terminal', reason: 'credential_rejected' };
          refreshed = true;
          continue;
        }
        throw error;
      }
    }
  } catch (error) {
    signal?.throwIfAborted();
    if (error instanceof RegistryResolverScopeError) {
      return { kind: 'terminal', reason: 'registry_scope_invalid' };
    }
    if (error instanceof OtlpHttpResponseError) {
      return { kind: 'terminal', reason: 'invalid_otlp_response' };
    }
    if (error instanceof OtlpEgressPolicyError) {
      return { kind: 'terminal', reason: 'egress_denied' };
    }
    if (error instanceof OtlpHttpTransportError || error instanceof RegistryResolverResponseError) {
      return { kind: 'retry' };
    }
    if (error instanceof OtlpHttpStatusError) {
      return [429, 502, 503, 504].includes(error.status)
        ? {
            kind: 'retry',
            ...(error.retryAfterMs === undefined ? {} : { retryAfterMs: error.retryAfterMs }),
          }
        : { kind: 'terminal', reason: 'permanent_http_status' };
    }
    if (error instanceof RegistryResolverHttpError) {
      return [401, 403, 408, 425, 429].includes(error.status) ||
        (error.status >= 500 && error.status <= 599)
        ? { kind: 'retry' }
        : { kind: 'terminal', reason: 'registry_denied' };
    }
    if (resolvingSecret) return { kind: 'retry' };
    throw error;
  }
}

/** Reconstruct a supported pinned context and enforce the OTLP client's timeout ceiling. */
export function parsePinnedDeliveryContext(value: unknown): PinnedDeliveryContext {
  const context = parseKafkaDeliveryContext(value);
  if (context.timeoutMs > 120_000) {
    throw new Error('observability exporter delivery context is invalid');
  }
  return context;
}
