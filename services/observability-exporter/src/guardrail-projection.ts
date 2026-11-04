// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from '@orca/transcript-store';
import { isBoundedAgentEventId } from './event-identity.js';
import { isCanonicalSubpath } from './thread-projection.js';
import { deterministicChildSpanId, deterministicRootSpanId } from './ids.js';
import {
  GUARDRAIL_PROJECTED_TRACE_SCHEMA_VERSION,
  GUARDRAIL_IO_PROJECTED_TRACE_SCHEMA_VERSION,
  type ProjectedSpan,
  type ProjectedMetadataValue,
} from './types.js';

export const MAX_GUARDRAIL_DECISIONS_PER_TURN = 256;
export const GUARDRAIL_DECISION = 'guardrail_decision';
const outcomes = ['allow', 'ask', 'deny'];
const phases = ['request', 'tool_call', 'tool_result', 'response', 'llm_request', 'llm_response'];
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);
const text = (value: unknown, max: number): value is string =>
  typeof value === 'string' &&
  value.length > 0 &&
  value.length <= max &&
  [...value].every((char) => char.charCodeAt(0) >= 32 && char.charCodeAt(0) !== 127);

/** Explicit allowlist; tool inputs, outputs and unknown payload fields never enter a trace. */
export function guardrailMetadata(
  payload: unknown,
  sourceEventId: string,
): Record<string, ProjectedMetadataValue> | undefined {
  if (
    !record(payload) ||
    !phases.includes(String(payload.phase)) ||
    !outcomes.includes(String(payload.decision)) ||
    !Array.isArray(payload.guardrails) ||
    payload.guardrails.length === 0 ||
    payload.guardrails.length > 100
  )
    return undefined;
  const rules: Record<string, unknown>[] = [];
  for (const rule of payload.guardrails) {
    if (
      !record(rule) ||
      !text(rule.guardrail_id, 256) ||
      (rule.mode !== 'observe' && rule.mode !== 'enforce') ||
      !outcomes.includes(String(rule.verdict))
    )
      return undefined;
    if (rule.reason !== undefined && !text(rule.reason, 1024)) return undefined;
    if (
      rule.tools !== undefined &&
      (!Array.isArray(rule.tools) ||
        rule.tools.length > 128 ||
        !rule.tools.every((tool) => text(tool, 256)))
    )
      return undefined;
    for (const field of ['deciding_rules', 'errors'])
      if (
        rule[field] !== undefined &&
        (!Array.isArray(rule[field]) ||
          rule[field].length > 100 ||
          !rule[field].every((v) => text(v, field === 'errors' ? 1024 : 256)))
      )
        return undefined;
    rules.push({
      ...(rule.deciding_rules === undefined ? {} : { deciding_rules: rule.deciding_rules }),
      ...(rule.errors === undefined ? {} : { errors: rule.errors }),
      guardrail_id: rule.guardrail_id,
      mode: rule.mode,
      verdict: rule.verdict,
      ...(rule.reason === undefined ? {} : { reason: rule.reason }),
      ...(rule.tools === undefined ? {} : { tools: rule.tools }),
    });
  }
  if (payload.tool_use_id !== undefined && !text(payload.tool_use_id, 512)) return undefined;
  if (payload.enforcer !== undefined && !['harness', 'gateway'].includes(String(payload.enforcer)))
    return undefined;
  return {
    ...(payload.enforcer === undefined
      ? {}
      : { 'orca.guardrail.enforcer': String(payload.enforcer) }),
    observation_type: GUARDRAIL_DECISION,
    'orca.projection.schema_version': GUARDRAIL_PROJECTED_TRACE_SCHEMA_VERSION,
    'orca.source.start_event_id': sourceEventId,
    'orca.source.end_event_id': sourceEventId,
    'orca.guardrail.phase': payload.phase as string,
    'orca.guardrail.decision': payload.decision as string,
    'orca.guardrail.verdicts': JSON.stringify(rules),
    ...(payload.tool_use_id === undefined
      ? {}
      : { 'orca.guardrail.tool_use_id': payload.tool_use_id as string }),
  };
}

export function projectGuardrailDecision(event: Event, traceId: string): ProjectedSpan | undefined {
  if (
    !isBoundedAgentEventId(event.id) ||
    !isCanonicalSubpath(event.subpath) ||
    !Number.isFinite(Date.parse(event.producedAt))
  )
    return undefined;
  let payload: unknown;
  try {
    payload = JSON.parse(Buffer.from(event.payload).toString('utf8'));
  } catch {
    return undefined;
  }
  const metadata = guardrailMetadata(payload, event.id);
  if (!metadata) return undefined;
  return {
    spanId: deterministicChildSpanId(traceId, GUARDRAIL_DECISION, event.subpath, event.id),
    parentSpanId: deterministicRootSpanId(traceId),
    sourceEventId: event.id,
    subpath: event.subpath,
    name: 'orca.agent.guardrail_decision',
    observationType: GUARDRAIL_DECISION,
    startedAt: event.producedAt,
    endedAt: event.producedAt,
    status: 'ok',
    metadata,
  };
}

export function parseGuardrailMetadata(
  value: unknown,
  sourceEventId: string,
): Record<string, ProjectedMetadataValue> {
  if (
    !record(value) ||
    (value['orca.projection.schema_version'] !== GUARDRAIL_PROJECTED_TRACE_SCHEMA_VERSION &&
      value['orca.projection.schema_version'] !== GUARDRAIL_IO_PROJECTED_TRACE_SCHEMA_VERSION) ||
    typeof value['orca.guardrail.verdicts'] !== 'string'
  )
    throw new Error('invalid guardrail decision metadata');
  const canonical = guardrailMetadata(
    {
      ...(value['orca.guardrail.enforcer'] === undefined
        ? {}
        : { enforcer: value['orca.guardrail.enforcer'] }),
      phase: value['orca.guardrail.phase'],
      decision: value['orca.guardrail.decision'],
      guardrails: JSON.parse(value['orca.guardrail.verdicts']),
      ...(value['orca.guardrail.tool_use_id'] === undefined
        ? {}
        : { tool_use_id: value['orca.guardrail.tool_use_id'] }),
    },
    sourceEventId,
  );
  if (!canonical) throw new Error('invalid guardrail decision metadata');
  canonical['orca.projection.schema_version'] = value['orca.projection.schema_version'];
  if (
    JSON.stringify(Object.keys(value).sort()) !== JSON.stringify(Object.keys(canonical).sort()) ||
    Object.entries(canonical).some(([key, entry]) => value[key] !== entry)
  )
    throw new Error('invalid guardrail decision metadata');
  return canonical;
}

export function parseGuardrailSpan(value: unknown): ProjectedSpan {
  if (
    !record(value) ||
    !isBoundedAgentEventId(value.sourceEventId) ||
    !isCanonicalSubpath(value.subpath) ||
    value.observationType !== GUARDRAIL_DECISION ||
    value.name !== 'orca.agent.guardrail_decision' ||
    value.status !== 'ok' ||
    typeof value.startedAt !== 'string' ||
    !Number.isFinite(Date.parse(value.startedAt)) ||
    value.endedAt !== value.startedAt ||
    typeof value.spanId !== 'string' ||
    !/^[a-f0-9]{16}$/u.test(value.spanId) ||
    typeof value.parentSpanId !== 'string' ||
    !/^[a-f0-9]{16}$/u.test(value.parentSpanId) ||
    Object.keys(value).some(
      (key) =>
        ![
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
        ].includes(key),
    )
  )
    throw new Error('invalid guardrail decision span');
  return {
    spanId: value.spanId,
    parentSpanId: value.parentSpanId,
    sourceEventId: value.sourceEventId,
    subpath: value.subpath,
    name: value.name,
    observationType: GUARDRAIL_DECISION,
    startedAt: value.startedAt,
    endedAt: value.endedAt as string,
    status: 'ok',
    metadata: parseGuardrailMetadata(value.metadata, value.sourceEventId),
  };
}
