// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { isDeepStrictEqual } from 'node:util';
import {
  IO_PROJECTED_TRACE_SCHEMA_VERSION,
  type ProjectedSpan,
  type ProjectedTrace,
  type PinnedDeliveryContext,
} from '../../src/types.js';

const LITEFUSE_TRACES_PATH = '/api/public/otel/v1/traces';

export interface LitefuseSmokeCredentials {
  endpoint: string;
  publicKey: string;
  secretKey: string;
}

export function requiredLitefuseSmokeCredentials(): LitefuseSmokeCredentials {
  return {
    endpoint: requiredEnv('LITEFUSE_OTLP_ENDPOINT'),
    publicKey: requiredEnv('LITEFUSE_PUBLIC_KEY'),
    secretKey: requiredEnv('LITEFUSE_SECRET_KEY'),
  };
}

export function assertCanonicalHttpsLitefuseEndpoint(endpoint: string): void {
  let parsed: URL;
  try {
    parsed = new URL(endpoint);
  } catch {
    throw invalidCanonicalHttpsEndpoint();
  }
  if (
    endpoint.length > 4_096 ||
    endpoint !== endpoint.trim() ||
    parsed.protocol !== 'https:' ||
    parsed.username !== '' ||
    parsed.password !== '' ||
    parsed.pathname !== LITEFUSE_TRACES_PATH ||
    parsed.search !== '' ||
    parsed.hash !== '' ||
    parsed.protocol + '//' + parsed.host + parsed.pathname !== endpoint
  ) {
    throw invalidCanonicalHttpsEndpoint();
  }
}

export async function waitForLitefuseProjectedTrace(input: {
  credentials: LitefuseSmokeCredentials;
  trace: ProjectedTrace;
  context?: PinnedDeliveryContext;
}): Promise<void> {
  const timeoutMs = litefuseSmokeTimeoutMs();
  const deadline = Date.now() + timeoutMs;
  const query = new URL(input.credentials.endpoint);
  let observationsApi: 'v1' | 'v2' = 'v2';
  const authorization =
    'Basic ' +
    Buffer.from(input.credentials.publicKey + ':' + input.credentials.secretKey).toString('base64');
  const projectedSpans = [input.trace.root, ...input.trace.spans];

  while (Date.now() < deadline) {
    configureObservationsQuery(query, observationsApi, input.trace.traceId);
    let response: Response;
    try {
      response = await fetch(query, {
        headers: { accept: 'application/json', authorization },
        redirect: 'error',
        signal: AbortSignal.timeout(Math.min(10_000, Math.max(1, deadline - Date.now()))),
      });
    } catch {
      await delay(1_000);
      continue;
    }
    if (response.status !== 200) {
      await response.body?.cancel().catch(() => undefined);
      if (response.status === 404 && observationsApi === 'v2') {
        observationsApi = 'v1';
        continue;
      }
      if (isTransientQueryStatus(response.status)) {
        await delay(1_000);
        continue;
      }
      throw new Error('Litefuse observations query returned HTTP ' + response.status);
    }
    let body: unknown;
    try {
      body = (await response.json()) as unknown;
    } catch {
      await delay(1_000);
      continue;
    }
    const observations = readObservations(body, input.trace.traceId);
    const matched = projectedSpans.map((span) =>
      observations.find(
        (observation) =>
          observation.id === span.spanId &&
          observationMatches(observation, span, input.trace) &&
          attributionMatches(observation, input.context, observationsApi),
      ),
    );
    if (matched.every((observation) => observation !== undefined)) {
      const expectedUserId =
        input.trace.userId === undefined
          ? undefined
          : input.trace.workspaceId + ':' + input.trace.userId;
      const expectedSessionId = input.trace.workspaceId + ':' + input.trace.sessionId;
      if (observationsApi === 'v2') {
        if (
          matched.every(
            (observation) =>
              observation?.sessionId === expectedSessionId &&
              (expectedUserId === undefined || observation.userId === expectedUserId),
          )
        ) {
          return;
        }
      } else {
        const identity = await fetchTraceIdentity(
          input.credentials.endpoint,
          input.trace.traceId,
          authorization,
          deadline,
        );
        if (
          identity?.sessionId === expectedSessionId &&
          (expectedUserId === undefined || identity.userId === expectedUserId) &&
          (input.context?.release === undefined || identity.release === input.context.release)
        ) {
          return;
        }
      }
    }
    await delay(1_000);
  }
  throw new Error(
    'Litefuse trace ' + input.trace.traceId + ' was not queryable within ' + timeoutMs + 'ms',
  );
}

function observationMatches(
  observation: Observation,
  span: ProjectedSpan,
  trace: ProjectedTrace,
): boolean {
  const io = trace.schemaVersion === IO_PROJECTED_TRACE_SCHEMA_VERSION ? span.io : undefined;
  const type =
    span.observationType === 'agent_turn'
      ? 'AGENT'
      : span.observationType === 'tool'
        ? 'TOOL'
        : span.observationType === 'outcome_evaluation'
          ? 'EVALUATOR'
          : 'SPAN';
  return (
    observation.name === (io?.toolName ?? span.name) &&
    observation.type === type &&
    observation.level === (span.status === 'error' ? 'ERROR' : 'DEFAULT') &&
    (span.parentSpanId === undefined
      ? observation.parentObservationId == null || observation.parentObservationId === ''
      : observation.parentObservationId === span.parentSpanId) &&
    Date.parse(observation.startTime) === Date.parse(span.startedAt) &&
    observation.endTime !== undefined &&
    observation.endTime !== null &&
    Date.parse(observation.endTime) === Date.parse(span.endedAt) &&
    metadataContains(
      observation,
      trace.schemaVersion,
      trace.anchorEventId,
      trace.workspaceId,
      trace.sessionId,
    ) &&
    ioMatches(observation.input, io?.input?.json) &&
    ioMatches(observation.output, io?.output?.json) &&
    usageMatches(observation, span) &&
    Object.entries(span.metadata)
      .filter(([key]) => key.startsWith('orca.tool.last_approval.'))
      .every(([key, value]) => readMetadata(observation)[key] === value)
  );
}

function readMetadata(observation: Observation): Record<string, unknown> {
  let metadata = observation.metadata;
  if (typeof metadata === 'string') {
    try {
      metadata = JSON.parse(metadata) as unknown;
    } catch {
      return {};
    }
  }
  return isRecord(metadata) ? metadata : {};
}

function attributionMatches(
  observation: Observation,
  context: PinnedDeliveryContext | undefined,
  api: 'v1' | 'v2',
): boolean {
  if (
    context === undefined ||
    [
      context.agentId,
      context.agentVersion,
      context.harness,
      context.harnessMode,
      context.environment,
      context.release,
    ].every((value) => value === undefined)
  )
    return true;
  const expected = {
    'orca.observability.binding_id': context.bindingId,
    'orca.observability.binding_version': context.bindingVersion,
    'orca.observability.config_schema_version': context.configSchemaVersion,
    ...(context.agentId === undefined ? {} : { 'orca.agent.id': context.agentId }),
    ...(context.agentVersion === undefined ? {} : { 'orca.agent.version': context.agentVersion }),
    ...(context.harness === undefined ? {} : { 'orca.harness.name': context.harness }),
    ...(context.harnessMode === undefined ? {} : { 'orca.harness.mode': context.harnessMode }),
    ...(context.environment === undefined
      ? {}
      : { 'orca.deployment.environment': context.environment }),
    ...(context.release === undefined ? {} : { 'orca.deployment.release': context.release }),
  };
  const metadata = readMetadata(observation);
  return (
    Object.entries(expected).every(([key, value]) => metadata[key] === value) &&
    (context.agentVersion === undefined || observation.version === String(context.agentVersion)) &&
    (context.environment === undefined || observation.environment === context.environment) &&
    (context.release === undefined || api === 'v1' || observation.release === context.release)
  );
}

function usageMatches(observation: Observation, span: ProjectedSpan): boolean {
  if (span.observationType !== 'turn_model_summary' || span.modelSummary === undefined) return true;
  const { usage, totalCostUsd } = span.modelSummary;
  const actual = isRecord(observation.usageDetails) ? observation.usageDetails : {};
  const cost = isRecord(observation.costDetails) ? observation.costDetails : {};
  return (
    (usage?.inputTokens === undefined || actual.input === usage.inputTokens) &&
    (usage?.outputTokens === undefined || actual.output === usage.outputTokens) &&
    (usage?.cacheCreationInputTokens === undefined ||
      actual.cache_creation_input_tokens === usage.cacheCreationInputTokens) &&
    (usage?.cacheReadInputTokens === undefined ||
      actual.input_cached_tokens === usage.cacheReadInputTokens) &&
    (totalCostUsd === undefined || cost.total === totalCostUsd)
  );
}

function ioMatches(actual: unknown, expectedJson: string | undefined): boolean {
  if (expectedJson === undefined) return actual == null;
  const expected: unknown = JSON.parse(expectedJson);
  if (isDeepStrictEqual(actual, expected)) return true;
  // Compatible deployments may return I/O as JSON strings instead of parsed values.
  if (typeof actual !== 'string') return false;
  try {
    return isDeepStrictEqual(JSON.parse(actual) as unknown, expected);
  } catch {
    return false;
  }
}

function configureObservationsQuery(query: URL, version: 'v1' | 'v2', traceId: string): void {
  query.pathname = version === 'v2' ? '/api/public/v2/observations' : '/api/public/observations';
  query.search = new URLSearchParams(
    version === 'v2'
      ? { fields: 'core,basic,io,metadata,usage,trace_context', traceId }
      : { limit: '100', traceId },
  ).toString();
}

async function fetchTraceIdentity(
  endpoint: string,
  traceId: string,
  authorization: string,
  deadline: number,
): Promise<
  { userId: string | null; sessionId: string | null; release?: string | null } | undefined
> {
  const url = new URL(endpoint);
  url.pathname = `/api/public/traces/${encodeURIComponent(traceId)}`;
  url.search = '';
  let response: Response;
  try {
    response = await fetch(url, {
      headers: { accept: 'application/json', authorization },
      redirect: 'error',
      signal: AbortSignal.timeout(Math.min(10_000, Math.max(1, deadline - Date.now()))),
    });
  } catch {
    return undefined;
  }
  if (response.status !== 200) {
    await response.body?.cancel().catch(() => undefined);
    if (response.status === 404 || isTransientQueryStatus(response.status)) return undefined;
    throw new Error('Litefuse trace query returned HTTP ' + response.status);
  }
  let body: unknown;
  try {
    body = (await response.json()) as unknown;
  } catch {
    return undefined;
  }
  if (
    !isRecord(body) ||
    body.id !== traceId ||
    (body.userId !== null && typeof body.userId !== 'string') ||
    (body.sessionId !== null && typeof body.sessionId !== 'string')
  ) {
    return undefined;
  }
  return {
    userId: body.userId as string | null,
    sessionId: body.sessionId as string | null,
    ...(typeof body.release === 'string' || body.release === null ? { release: body.release } : {}),
  };
}

interface Observation {
  id: string;
  traceId: string;
  type: string;
  startTime: string;
  endTime?: string | null;
  level?: string;
  usageDetails?: unknown;
  costDetails?: unknown;
  version?: string | null;
  environment?: string | null;
  release?: string | null;
  name?: string;
  parentObservationId?: string | null;
  userId?: string | null;
  sessionId?: string | null;
  metadata?: unknown;
  input?: unknown;
  output?: unknown;
}

function readObservations(value: unknown, traceId: string): Observation[] {
  if (!isRecord(value) || !Array.isArray(value.data)) return [];
  return value.data.filter((entry): entry is Observation => {
    return (
      isRecord(entry) &&
      typeof entry.id === 'string' &&
      entry.traceId === traceId &&
      typeof entry.type === 'string' &&
      typeof entry.startTime === 'string' &&
      (entry.name === undefined || typeof entry.name === 'string') &&
      (entry.parentObservationId === undefined ||
        entry.parentObservationId === null ||
        typeof entry.parentObservationId === 'string')
    );
  });
}

function metadataContains(observation: Observation, ...values: string[]): boolean {
  const encoded = JSON.stringify(observation.metadata ?? null);
  return values.every((value) => encoded.includes(value));
}

export function litefuseSmokeTimeoutMs(): number {
  const raw = process.env['LITEFUSE_SMOKE_TIMEOUT_MS'];
  if (raw === undefined) return 60_000;
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > 300_000) {
    throw new Error('LITEFUSE_SMOKE_TIMEOUT_MS must be an integer between 1 and 300000');
  }
  return parsed;
}

function isTransientQueryStatus(status: number): boolean {
  return status === 429 || status === 502 || status === 503 || status === 504;
}

function invalidCanonicalHttpsEndpoint(): Error {
  return new Error(
    'LITEFUSE_OTLP_ENDPOINT must be canonical HTTPS with exact path ' + LITEFUSE_TRACES_PATH,
  );
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(name + ' is required for Litefuse smoke');
  }
  return value;
}
