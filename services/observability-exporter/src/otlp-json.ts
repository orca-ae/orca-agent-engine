// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { parseProjectedIo } from './captured-io.js';
import { parsePinnedDeliveryContext } from './canonical-validation.js';
import {
  IO_PROJECTED_TRACE_SCHEMA_VERSION,
  type ProjectedIo,
  type PinnedDeliveryContext,
  type ProjectedMetadataValue,
  type ProjectedSpan,
  type ProjectedTrace,
} from './types.js';

export interface OtlpAnyValue {
  stringValue?: string;
  boolValue?: boolean;
  intValue?: string;
  doubleValue?: number;
  arrayValue?: { values: OtlpAnyValue[] };
}

export interface OtlpAttribute {
  key: string;
  value: OtlpAnyValue;
}

export interface OtlpSpan {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  kind: number;
  startTimeUnixNano: string;
  endTimeUnixNano: string;
  attributes: OtlpAttribute[];
  status: { code: number };
}

export interface OtlpExportRequest {
  resourceSpans: Array<{
    resource: { attributes: OtlpAttribute[] };
    scopeSpans: Array<{
      scope: { name: string; version: string };
      spans: OtlpSpan[];
    }>;
  }>;
}

const EXPORTER_VERSION = 'phase3-wire-v1';

/** Encode one complete projected trace using Langfuse's OTLP semantic profile. */
export function encodeLangfuseOtlpJson(
  trace: ProjectedTrace,
  context?: PinnedDeliveryContext,
): OtlpExportRequest {
  const attribution =
    context === undefined ? [] : attributionAttributes(parsePinnedDeliveryContext(context));
  const spans = [
    rootSpan(trace, attribution),
    ...trace.spans.map((span) => childSpan(trace, span, attribution)),
  ];
  return {
    resourceSpans: [
      {
        resource: {
          attributes: [
            stringAttribute('service.name', 'orca-observability-exporter'),
            stringAttribute('service.version', EXPORTER_VERSION),
            stringAttribute('orca.workspace.id', trace.workspaceId),
            stringAttribute('telemetry.sdk.name', 'orca-transcript-projector'),
            stringAttribute('telemetry.sdk.language', 'nodejs'),
            stringAttribute('telemetry.sdk.version', EXPORTER_VERSION),
          ],
        },
        scopeSpans: [
          {
            scope: { name: '@orca/observability-exporter', version: EXPORTER_VERSION },
            spans,
          },
        ],
      },
    ],
  };
}

function rootSpan(trace: ProjectedTrace, attribution: OtlpAttribute[]): OtlpSpan {
  const io = observationIo(trace, trace.root);
  const attributes = [
    ...sharedTraceAttributes(trace),
    ...attribution,
    stringAttribute('langfuse.trace.name', trace.root.name),
    arrayAttribute('langfuse.trace.tags', ['managed-agent', 'transcript-projection']),
    stringAttribute('langfuse.observation.type', 'agent'),
    ...metadataAttributes('langfuse.observation.metadata.', trace.root.metadata),
    ...ioAttributes(io),
    ...diagnosticAttributes(io),
  ];
  return otlpSpan(trace, trace.root, attributes);
}

function childSpan(
  trace: ProjectedTrace,
  span: ProjectedSpan,
  attribution: OtlpAttribute[],
): OtlpSpan {
  const io = observationIo(trace, span);
  const attributes = [
    ...sharedTraceAttributes(trace),
    ...attribution,
    stringAttribute('langfuse.trace.name', trace.root.name),
    arrayAttribute('langfuse.trace.tags', ['managed-agent', 'transcript-projection']),
    // A current turn summary is explicitly a generic span, never generation.
    stringAttribute(
      'langfuse.observation.type',
      span.observationType === 'tool'
        ? 'tool'
        : span.observationType === 'outcome_evaluation'
          ? 'evaluator'
          : 'span',
    ),
    ...metadataAttributes('langfuse.observation.metadata.', {
      ...trace.root.metadata,
      ...span.metadata,
    }),
    ...modelSummaryAttributes(span),
    ...ioAttributes(io),
    ...(io?.toolName === undefined ? [] : [stringAttribute('gen_ai.tool.name', io.toolName)]),
  ];
  return { ...otlpSpan(trace, span, attributes), name: io?.toolName ?? span.name };
}

function observationIo(trace: ProjectedTrace, span: ProjectedSpan): ProjectedIo | undefined {
  if (
    trace.schemaVersion !== IO_PROJECTED_TRACE_SCHEMA_VERSION ||
    (span.observationType !== 'agent_turn' && span.observationType !== 'tool') ||
    span.io === undefined
  )
    return undefined;
  return parseProjectedIo(span.io);
}

function ioAttributes(io: ProjectedIo | undefined): OtlpAttribute[] {
  if (io === undefined) return [];
  const attributes: OtlpAttribute[] = [
    stringAttribute('langfuse.observation.metadata.orca.io.capture_version', io.version),
  ];
  if (io.outputScope !== undefined) {
    attributes.push(
      stringAttribute('langfuse.observation.metadata.orca.io.output_scope', io.outputScope),
    );
  }
  for (const direction of ['input', 'output'] as const) {
    const value = io[direction];
    if (value?.json !== undefined) {
      attributes.push(stringAttribute('langfuse.observation.' + direction, value.json));
    }
    if (value?.omitted !== undefined) {
      attributes.push(
        stringAttribute(
          'langfuse.observation.metadata.orca.io.' + direction + '.omitted',
          value.omitted,
        ),
      );
    }
    if (value?.truncated === true) {
      attributes.push(
        attribute('langfuse.observation.metadata.orca.io.' + direction + '.truncated', true),
      );
    }
  }
  return attributes;
}

/** Only fixed, validated last-reported facts; never flatten arbitrary JSON into metadata. */
function diagnosticAttributes(io: ProjectedIo | undefined): OtlpAttribute[] {
  const diagnostic = io?.diagnostic;
  if (diagnostic === undefined) return [];
  const attributes: OtlpAttribute[] = [];
  if (diagnostic.omitted !== undefined)
    attributes.push(
      stringAttribute('langfuse.observation.metadata.orca.last_error.omitted', diagnostic.omitted),
    );
  if (diagnostic.json === undefined) return attributes;
  // observationIo already validates the closed diagnostic schema and captured JSON.
  const facts = JSON.parse(diagnostic.json) as Record<string, ProjectedMetadataValue>;
  for (const [field, key] of [
    ['type', 'orca.last_error.type'],
    ['message', 'orca.last_error.message'],
    ['will_retry', 'orca.retry.will_retry'],
    ['next_attempt', 'orca.retry.next_attempt'],
    ['retry_delay_ms', 'orca.retry.retry_delay_ms'],
  ] as const) {
    const value = facts[field];
    if (value !== undefined)
      attributes.push(attribute('langfuse.observation.metadata.' + key, value));
  }
  return attributes;
}

function sharedTraceAttributes(trace: ProjectedTrace): OtlpAttribute[] {
  const sessionId = `${trace.workspaceId}:${trace.sessionId}`;
  return [
    stringAttribute('session.id', sessionId),
    stringAttribute('orca.workspace.id', trace.workspaceId),
    stringAttribute('orca.session.id', trace.sessionId),
    stringAttribute('orca.turn.anchor_event_id', trace.anchorEventId),
    ...(trace.userId === undefined
      ? []
      : [stringAttribute('langfuse.user.id', `${trace.workspaceId}:${trace.userId}`)]),
    ...metadataAttributes('langfuse.trace.metadata.', trace.root.metadata),
  ];
}

/** Registry-approved, persisted pin facts, never mutable state or Transcript metadata. */
function attributionAttributes(context: PinnedDeliveryContext): OtlpAttribute[] {
  // Missing optional pin facts identify legacy queued work. Do not backfill its
  // wire representation with context that was not captured when it was projected.
  if (
    [
      context.agentId,
      context.agentVersion,
      context.harness,
      context.harnessMode,
      context.environment,
      context.release,
    ].every((value) => value === undefined)
  )
    return [];
  const metadata: Record<string, ProjectedMetadataValue> = {
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
  return [
    ...metadataAttributes('langfuse.observation.metadata.', metadata),
    ...metadataAttributes('langfuse.trace.metadata.', metadata),
    ...(context.agentVersion === undefined
      ? []
      : [stringAttribute('langfuse.version', String(context.agentVersion))]),
    ...(context.environment === undefined
      ? []
      : [stringAttribute('langfuse.environment', context.environment)]),
    ...(context.release === undefined
      ? []
      : [stringAttribute('langfuse.release', context.release)]),
  ];
}

function modelSummaryAttributes(span: ProjectedSpan): OtlpAttribute[] {
  if (span.observationType !== 'turn_model_summary' || span.modelSummary === undefined) return [];
  const { provider, requestedModel, usage, totalCostUsd } = span.modelSummary;
  const usageDetails =
    usage === undefined
      ? undefined
      : JSON.stringify({
          ...(usage.inputTokens === undefined ? {} : { input: usage.inputTokens }),
          ...(usage.outputTokens === undefined ? {} : { output: usage.outputTokens }),
          ...(usage.cacheCreationInputTokens === undefined
            ? {}
            : { cache_creation_input_tokens: usage.cacheCreationInputTokens }),
          ...(usage.cacheReadInputTokens === undefined
            ? {}
            : { input_cached_tokens: usage.cacheReadInputTokens }),
        });
  return [
    ...(provider === undefined ? [] : [stringAttribute('gen_ai.provider.name', provider)]),
    ...(requestedModel === undefined
      ? []
      : [stringAttribute('gen_ai.request.model', requestedModel)]),
    ...(usage?.inputTokens === undefined
      ? []
      : [integerAttribute('gen_ai.usage.input_tokens', usage.inputTokens)]),
    ...(usage?.outputTokens === undefined
      ? []
      : [integerAttribute('gen_ai.usage.output_tokens', usage.outputTokens)]),
    ...(usageDetails === undefined
      ? []
      : [stringAttribute('langfuse.observation.usage_details', usageDetails)]),
    ...(totalCostUsd === undefined
      ? []
      : [
          stringAttribute(
            'langfuse.observation.cost_details',
            JSON.stringify({ total: totalCostUsd }),
          ),
        ]),
  ];
}

function otlpSpan(
  trace: ProjectedTrace,
  span: ProjectedSpan,
  attributes: OtlpAttribute[],
): OtlpSpan {
  return {
    traceId: trace.traceId,
    spanId: span.spanId,
    ...(span.parentSpanId === undefined ? {} : { parentSpanId: span.parentSpanId }),
    name: span.name,
    kind: 1,
    startTimeUnixNano: toUnixNano(span.startedAt),
    endTimeUnixNano: toUnixNano(span.endedAt),
    attributes,
    status: { code: span.status === 'unset' ? 0 : span.status === 'error' ? 2 : 1 },
  };
}

function metadataAttributes(
  prefix: string,
  metadata: Record<string, ProjectedMetadataValue>,
): OtlpAttribute[] {
  return Object.entries(metadata)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => attribute(`${prefix}${key}`, value));
}

function attribute(key: string, value: ProjectedMetadataValue): OtlpAttribute {
  if (typeof value === 'boolean') return { key, value: { boolValue: value } };
  if (typeof value === 'number') {
    return Number.isSafeInteger(value)
      ? integerAttribute(key, value)
      : { key, value: { doubleValue: value } };
  }
  return stringAttribute(key, value);
}

function integerAttribute(key: string, value: number): OtlpAttribute {
  return { key, value: { intValue: value.toString() } };
}

function stringAttribute(key: string, value: string): OtlpAttribute {
  return { key, value: { stringValue: value } };
}

function arrayAttribute(key: string, values: readonly string[]): OtlpAttribute {
  return {
    key,
    value: { arrayValue: { values: values.map((value) => ({ stringValue: value })) } },
  };
}

function toUnixNano(value: string): string {
  const millis = Date.parse(value);
  const safeMillis = Number.isFinite(millis) ? Math.max(0, Math.floor(millis)) : 0;
  return (BigInt(safeMillis) * 1_000_000n).toString();
}
