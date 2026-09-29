// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { isBoundedAgentEventId } from './event-identity.js';
import { deterministicChildSpanId, deterministicRootSpanId } from './ids.js';
import { isValidSampleRate } from './sampling.js';
import {
  MAX_CAPTURED_IO_BYTES_PER_TURN,
  parseProjectedIo,
  capturedIoBytes,
} from './captured-io.js';
import { MAX_PROJECTED_TOOLS_PER_TURN, parseCompletedToolSpan } from './tool-projection.js';
import {
  PROJECTED_TRACE_SCHEMA_VERSION,
  IO_PROJECTED_TRACE_SCHEMA_VERSION,
  MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN,
  MAX_PROJECTED_EVALUATIONS_PER_TURN,
  type PinnedDeliveryContext,
  type ProjectedMetadataValue,
  type ProjectedModelUsage,
  type ProjectedSpan,
  type ProjectedTrace,
  type ProjectedTurnModelSummary,
} from './types.js';

export function parseProjectedTrace(value: unknown): ProjectedTrace {
  if (
    !isRecord(value) ||
    (value.schemaVersion !== PROJECTED_TRACE_SCHEMA_VERSION &&
      value.schemaVersion !== IO_PROJECTED_TRACE_SCHEMA_VERSION) ||
    !isTraceId(value.traceId) ||
    !isWorkspaceId(value.workspaceId) ||
    !isSessionId(value.sessionId) ||
    !isEventId(value.anchorEventId) ||
    !isEventId(value.acceptanceEventId) ||
    !Array.isArray(value.spans) ||
    value.spans.length >
      MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN +
        MAX_PROJECTED_TOOLS_PER_TURN +
        MAX_PROJECTED_EVALUATIONS_PER_TURN
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  const userId = optionalBoundedString(value.userId, 512);
  if (value.userId !== undefined && userId === undefined) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  if (
    value.schemaVersion === IO_PROJECTED_TRACE_SCHEMA_VERSION &&
    !hasOnlyKeys(value, [
      'schemaVersion',
      'traceId',
      'workspaceId',
      'sessionId',
      'anchorEventId',
      'acceptanceEventId',
      'userId',
      'root',
      'spans',
    ])
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  // Each span owns one decode; the trace boundary checks schema, role and total bytes.
  let ioBytes = 0;
  const decodeSpan = (raw: unknown, type: ProjectedSpan['observationType']): ProjectedSpan => {
    if (
      !isRecord(raw) ||
      !isRecord(raw.metadata) ||
      raw.metadata['orca.projection.schema_version'] !== value.schemaVersion ||
      (value.schemaVersion === IO_PROJECTED_TRACE_SCHEMA_VERSION &&
        !hasOnlyKeys(raw, [
          'spanId',
          'parentSpanId',
          'sourceEventId',
          'subpath',
          'name',
          'observationType',
          'startedAt',
          'endedAt',
          'status',
          'metadata',
          'modelSummary',
          'io',
        ])) ||
      ('io' in raw &&
        (raw.io === undefined ||
          value.schemaVersion !== IO_PROJECTED_TRACE_SCHEMA_VERSION ||
          (type !== 'agent_turn' && type !== 'tool')))
    ) {
      throw new Error('observability exporter outbox trace is invalid');
    }
    const span = type === 'tool' ? parseCompletedToolSpan(raw) : parseProjectedSpan(raw, type);
    if (span.io !== undefined) {
      const io = span.io;
      if (
        (type !== 'tool' && io.toolName !== undefined) ||
        (type !== 'agent_turn' && io.outputScope !== undefined) ||
        (type === 'agent_turn' &&
          io.output?.json !== undefined &&
          io.outputScope !== 'turn_messages')
      ) {
        throw new Error('observability exporter outbox trace is invalid');
      }
      ioBytes += capturedIoBytes(io);
    }
    return span;
  };
  const root = decodeSpan(value.root, 'agent_turn');
  if (
    root.parentSpanId !== undefined ||
    root.name !== 'orca.agent.turn' ||
    root.sourceEventId !== value.anchorEventId ||
    root.subpath !== '' ||
    root.metadata['orca.workspace.id'] !== value.workspaceId ||
    root.metadata['orca.session.id'] !== value.sessionId ||
    root.metadata['orca.turn.anchor_event_id'] !== value.anchorEventId ||
    root.metadata['orca.turn.acceptance_event_id'] !== value.acceptanceEventId
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  const spans = value.spans.map((raw: unknown) =>
    decodeSpan(
      raw,
      isRecord(raw) && raw.observationType === 'tool'
        ? 'tool'
        : isRecord(raw) && raw.observationType === 'outcome_evaluation'
          ? 'outcome_evaluation'
          : 'turn_model_summary',
    ),
  );
  if (ioBytes > MAX_CAPTURED_IO_BYTES_PER_TURN) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  const traceId = value.traceId;
  if (
    spans.some((span) => span.parentSpanId !== root.spanId) ||
    spans.filter((span) => span.observationType === 'tool').length > MAX_PROJECTED_TOOLS_PER_TURN ||
    spans.filter((span) => span.observationType === 'turn_model_summary').length >
      MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN ||
    spans.filter((span) => span.observationType === 'outcome_evaluation').length >
      MAX_PROJECTED_EVALUATIONS_PER_TURN ||
    spans.some(
      (span) =>
        (span.observationType === 'tool' || span.observationType === 'outcome_evaluation') &&
        (span.spanId !==
          deterministicChildSpanId(
            traceId,
            span.observationType,
            span.subpath,
            span.sourceEventId,
          ) ||
          span.parentSpanId !== deterministicRootSpanId(traceId)),
    ) ||
    new Set(spans.map((span) => span.spanId)).size !== spans.length ||
    new Set(spans.map((span) => span.sourceEventId)).size !== spans.length
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  return {
    schemaVersion: value.schemaVersion,
    traceId: value.traceId,
    workspaceId: value.workspaceId,
    sessionId: value.sessionId,
    anchorEventId: value.anchorEventId,
    acceptanceEventId: value.acceptanceEventId,
    ...(userId === undefined ? {} : { userId }),
    root,
    spans,
  };
}

function parseProjectedSpan(
  value: unknown,
  expectedObservationType: ProjectedSpan['observationType'],
): ProjectedSpan {
  const expectedName =
    expectedObservationType === 'agent_turn'
      ? 'orca.agent.turn'
      : expectedObservationType === 'outcome_evaluation'
        ? 'orca.agent.outcome_evaluation'
        : 'orca.agent.turn_model_summary';
  if (
    !isRecord(value) ||
    (expectedObservationType === 'outcome_evaluation' &&
      ('outcomeId' in value || 'outcomeKey' in value)) ||
    !isSpanId(value.spanId) ||
    !isEventId(value.sourceEventId) ||
    value.subpath !== '' ||
    value.name !== expectedName ||
    !isTimestamp(value.startedAt) ||
    !isTimestamp(value.endedAt) ||
    value.observationType !== expectedObservationType ||
    (value.status !== 'ok' &&
      value.status !== 'error' &&
      !(expectedObservationType === 'outcome_evaluation' && value.status === 'unset')) ||
    !isRecord(value.metadata)
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  const parentSpanId = value.parentSpanId === undefined ? undefined : value.parentSpanId;
  if (value.parentSpanId !== undefined && parentSpanId === undefined) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  if (parentSpanId !== undefined && !isSpanId(parentSpanId)) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  const metadata =
    expectedObservationType === 'agent_turn'
      ? parseRootMetadata(value.metadata)
      : expectedObservationType === 'outcome_evaluation'
        ? parseEvaluationMetadata(value.metadata, value.sourceEventId)
        : parseSummaryMetadata(value.metadata, value.sourceEventId);
  const modelSummary =
    value.modelSummary === undefined ? undefined : parseProjectedModelSummary(value.modelSummary);
  if (expectedObservationType !== 'turn_model_summary' && modelSummary !== undefined) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  if (
    expectedObservationType === 'outcome_evaluation' &&
    (Date.parse(value.endedAt) < Date.parse(value.startedAt) ||
      value.status !==
        (metadata['orca.outcome.result'] === 'interrupted'
          ? 'unset'
          : metadata['orca.outcome.result'] === 'failed'
            ? 'error'
            : 'ok'))
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  return {
    spanId: value.spanId,
    ...(parentSpanId === undefined ? {} : { parentSpanId }),
    sourceEventId: value.sourceEventId,
    subpath: value.subpath,
    name: expectedName,
    observationType: expectedObservationType,
    startedAt: value.startedAt,
    endedAt: value.endedAt,
    status: value.status,
    metadata,
    ...(modelSummary === undefined ? {} : { modelSummary }),
    ...('io' in value ? { io: parseProjectedIo(value.io) } : {}),
  };
}

function parseRootMetadata(value: Record<string, unknown>): Record<string, ProjectedMetadataValue> {
  const keys = [
    'orca.projection.schema_version',
    'orca.session.id',
    'orca.thread.subpath',
    'orca.turn.acceptance_event_id',
    'orca.turn.accepted_at',
    'orca.turn.anchor_event_id',
    'orca.turn.companion_system_message_count',
    'orca.turn.continuation_count',
    'orca.turn.input_received_at',
    'orca.turn.interrupt_count',
    'orca.turn.model_summary_count',
    'orca.turn.source_event_count',
    'orca.turn.terminal_reason',
    'orca.workspace.id',
  ];
  // Additive v1 metadata: legacy persisted roots have no tool counter.
  if (Object.hasOwn(value, 'orca.turn.unmatched_tool_result_count')) {
    keys.push('orca.turn.unmatched_tool_result_count');
    if (
      !isNonNegativeInteger(value['orca.turn.unmatched_tool_result_count']) ||
      value['orca.turn.unmatched_tool_result_count'] === 0
    ) {
      throw new Error('observability exporter outbox trace is invalid');
    }
  }
  if (
    !hasExactKeys(value, keys) ||
    !isProjectionSchema(value['orca.projection.schema_version']) ||
    !isNonEmptyString(value['orca.workspace.id']) ||
    !isNonEmptyString(value['orca.session.id']) ||
    value['orca.thread.subpath'] !== '' ||
    !isNonEmptyString(value['orca.turn.anchor_event_id']) ||
    !isNonEmptyString(value['orca.turn.acceptance_event_id']) ||
    !isTimestamp(value['orca.turn.input_received_at']) ||
    !isTimestamp(value['orca.turn.accepted_at']) ||
    !isTerminalReason(value['orca.turn.terminal_reason']) ||
    !isNonNegativeInteger(value['orca.turn.source_event_count']) ||
    !isNonNegativeInteger(value['orca.turn.continuation_count']) ||
    !isNonNegativeInteger(value['orca.turn.companion_system_message_count']) ||
    !isNonNegativeInteger(value['orca.turn.interrupt_count']) ||
    !isNonNegativeInteger(value['orca.turn.model_summary_count'])
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  return Object.fromEntries(keys.map((key) => [key, value[key] as ProjectedMetadataValue]));
}

function parseSummaryMetadata(
  value: Record<string, unknown>,
  sourceEventId: string,
): Record<string, ProjectedMetadataValue> {
  const keys = [
    'observation_type',
    'orca.model.observation.kind',
    'orca.projection.schema_version',
    'orca.source.end_event_id',
    'orca.source.start_event_id',
    'orca.thread.subpath',
  ];
  if (
    !hasExactKeys(value, keys) ||
    value.observation_type !== 'turn_model_summary' ||
    value['orca.model.observation.kind'] !== 'turn_model_summary' ||
    !isProjectionSchema(value['orca.projection.schema_version']) ||
    value['orca.source.start_event_id'] !== sourceEventId ||
    !isEventId(value['orca.source.end_event_id']) ||
    value['orca.thread.subpath'] !== ''
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  return Object.fromEntries(keys.map((key) => [key, value[key] as ProjectedMetadataValue]));
}

function parseEvaluationMetadata(
  value: Record<string, unknown>,
  sourceEventId: string,
): Record<string, ProjectedMetadataValue> {
  const keys = [
    'observation_type',
    'orca.projection.schema_version',
    'orca.source.start_event_id',
    'orca.source.end_event_id',
    'orca.thread.subpath',
    'orca.outcome.iteration',
    'orca.outcome.result',
  ];
  if (
    !hasExactKeys(value, keys) ||
    value.observation_type !== 'outcome_evaluation' ||
    !isProjectionSchema(value['orca.projection.schema_version']) ||
    value['orca.source.start_event_id'] !== sourceEventId ||
    !isEventId(value['orca.source.end_event_id']) ||
    value['orca.thread.subpath'] !== '' ||
    !isNonNegativeInteger(value['orca.outcome.iteration']) ||
    !['satisfied', 'needs_revision', 'max_iterations_reached', 'failed', 'interrupted'].includes(
      value['orca.outcome.result'] as string,
    )
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  return Object.fromEntries(keys.map((key) => [key, value[key] as ProjectedMetadataValue]));
}

function parseProjectedModelSummary(value: unknown): ProjectedTurnModelSummary {
  if (!isRecord(value)) throw new Error('observability exporter outbox trace is invalid');
  const provider = optionalBoundedString(value.provider, 256);
  const requestedModel = optionalBoundedString(value.requestedModel, 256);
  const totalCostUsd = optionalNonNegativeNumber(value.totalCostUsd);
  if (
    (value.provider !== undefined && provider === undefined) ||
    (value.requestedModel !== undefined && requestedModel === undefined) ||
    (value.totalCostUsd !== undefined && totalCostUsd === undefined)
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  const usage = value.usage === undefined ? undefined : parseProjectedModelUsage(value.usage);
  return {
    ...(provider === undefined ? {} : { provider }),
    ...(requestedModel === undefined ? {} : { requestedModel }),
    ...(usage === undefined ? {} : { usage }),
    ...(totalCostUsd === undefined ? {} : { totalCostUsd }),
  };
}

function parseProjectedModelUsage(value: unknown): ProjectedModelUsage {
  if (!isRecord(value)) throw new Error('observability exporter outbox trace is invalid');
  const inputTokens = optionalNonNegativeInteger(value.inputTokens);
  const outputTokens = optionalNonNegativeInteger(value.outputTokens);
  const cacheCreationInputTokens = optionalNonNegativeInteger(value.cacheCreationInputTokens);
  const cacheReadInputTokens = optionalNonNegativeInteger(value.cacheReadInputTokens);
  if (
    (value.inputTokens !== undefined && inputTokens === undefined) ||
    (value.outputTokens !== undefined && outputTokens === undefined) ||
    (value.cacheCreationInputTokens !== undefined && cacheCreationInputTokens === undefined) ||
    (value.cacheReadInputTokens !== undefined && cacheReadInputTokens === undefined)
  ) {
    throw new Error('observability exporter outbox trace is invalid');
  }
  return {
    ...(inputTokens === undefined ? {} : { inputTokens }),
    ...(outputTokens === undefined ? {} : { outputTokens }),
    ...(cacheCreationInputTokens === undefined ? {} : { cacheCreationInputTokens }),
    ...(cacheReadInputTokens === undefined ? {} : { cacheReadInputTokens }),
  };
}

/** Attribution labels are bounded ASCII identifiers, not arbitrary free text. */
export function isSafeAttributionLabel(value: unknown): value is string {
  return typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$/u.test(value);
}

export function parsePinnedDeliveryContext(value: unknown): PinnedDeliveryContext {
  if (!isRecord(value)) throw new Error('observability exporter delivery context is invalid');
  const bindingVersion = value.bindingVersion;
  const timeoutMs = value.timeoutMs;
  const sampleRate = value.sampleRate;
  const configSchemaVersion = value.configSchemaVersion;
  const attribution: Partial<PinnedDeliveryContext> = {};
  for (const key of ['agentId', 'harness', 'harnessMode', 'environment', 'release'] as const) {
    if (value[key] === undefined) continue;
    if (
      !isSafeAttributionLabel(value[key]) ||
      (key === 'agentId' && !/^(?:agt|agent)_[A-Za-z0-9_-]+$/u.test(value[key]))
    ) {
      throw new Error('observability exporter delivery context is invalid');
    }
    attribution[key] = value[key];
  }
  if (value.agentVersion !== undefined) {
    if (
      typeof value.agentVersion !== 'number' ||
      !Number.isSafeInteger(value.agentVersion) ||
      value.agentVersion <= 0
    ) {
      throw new Error('observability exporter delivery context is invalid');
    }
    attribution.agentVersion = value.agentVersion;
  }
  if (
    !isBoundedIdentifier(value.organizationId, 512) ||
    !isBoundedIdentifier(value.bindingId, 512) ||
    value.adapterType !== 'otlp_http' ||
    value.endpointKind !== 'traces_endpoint' ||
    value.endpointClass !== 'public' ||
    !isExactLitefuseTracesEndpoint(value.endpointUrl) ||
    value.semanticProfile !== 'langfuse' ||
    value.protocol !== 'http/json' ||
    value.compression !== 'none' ||
    (value.captureMode !== 'metadata_only' &&
      value.captureMode !== 'redacted_io' &&
      value.captureMode !== 'raw_io') ||
    typeof bindingVersion !== 'number' ||
    !Number.isSafeInteger(bindingVersion) ||
    bindingVersion <= 0 ||
    typeof timeoutMs !== 'number' ||
    !Number.isSafeInteger(timeoutMs) ||
    timeoutMs <= 0 ||
    !isValidSampleRate(sampleRate) ||
    typeof configSchemaVersion !== 'number' ||
    !Number.isSafeInteger(configSchemaVersion) ||
    configSchemaVersion !== 1
  ) {
    throw new Error('observability exporter delivery context is invalid');
  }
  return {
    organizationId: value.organizationId as string,
    bindingId: value.bindingId as string,
    bindingVersion,
    adapterType: value.adapterType as string,
    endpointKind: value.endpointKind as string,
    endpointClass: value.endpointClass as string,
    endpointUrl: value.endpointUrl as string,
    semanticProfile: value.semanticProfile as string,
    protocol: value.protocol as string,
    compression: value.compression as string,
    timeoutMs,
    captureMode: value.captureMode as string,
    sampleRate,
    configSchemaVersion,
    ...attribution,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isProjectionSchema(value: unknown): boolean {
  return value === PROJECTED_TRACE_SCHEMA_VERSION || value === IO_PROJECTED_TRACE_SCHEMA_VERSION;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function optionalBoundedString(value: unknown, maximumLength: number): string | undefined {
  return typeof value === 'string' &&
    value.length > 0 &&
    value.length <= maximumLength &&
    !hasControlCharacter(value)
    ? value
    : undefined;
}

function isTraceId(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{32}$/u.test(value);
}

function isSpanId(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{16}$/u.test(value);
}

function isEventId(value: unknown): value is string {
  return isBoundedAgentEventId(value);
}

function isWorkspaceId(value: unknown): value is string {
  return typeof value === 'string' && /^[A-Za-z0-9_-]{1,128}$/u.test(value);
}

function isSessionId(value: unknown): value is string {
  return typeof value === 'string' && /^ses_[A-Za-z0-9_-]{1,508}$/u.test(value);
}

function hasControlCharacter(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) return true;
  }
  return false;
}

function isBoundedIdentifier(value: unknown, maximumLength: number): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= maximumLength &&
    !hasControlCharacter(value)
  );
}

function isExactLitefuseTracesEndpoint(value: unknown): value is string {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > 4_096 ||
    value !== value.trim() ||
    value.includes('?') ||
    value.includes('#')
  ) {
    return false;
  }
  try {
    const endpoint = new URL(value);
    return (
      endpoint.protocol === 'https:' &&
      endpoint.username === '' &&
      endpoint.password === '' &&
      endpoint.pathname === '/api/public/otel/v1/traces' &&
      `${endpoint.protocol}//${endpoint.host}${endpoint.pathname}` === value
    );
  } catch {
    return false;
  }
}

function optionalNonNegativeInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function optionalNonNegativeNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : undefined;
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isTimestamp(value: unknown): value is string {
  return typeof value === 'string' && Number.isFinite(Date.parse(value));
}

function isTerminalReason(value: unknown): value is string {
  return (
    value === 'end_turn' ||
    value === 'retries_exhausted' ||
    value === 'unknown' ||
    value === 'session.status_terminated' ||
    value === 'session.archived' ||
    value === 'session.deleted'
  );
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    actual.length === sortedExpected.length &&
    actual.every((key, index) => key === sortedExpected[index])
  );
}

function hasOnlyKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  return Reflect.ownKeys(value).every((key) => typeof key === 'string' && allowed.includes(key));
}
