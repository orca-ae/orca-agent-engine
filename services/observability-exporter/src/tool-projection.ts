// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { boundedAgentEventId, isBoundedAgentEventId } from './event-identity.js';
import { deterministicChildSpanId, deterministicRootSpanId } from './ids.js';
import { CanonicalProjectionStateError } from './projector.js';
import {
  PROJECTED_TRACE_SCHEMA_VERSION,
  IO_PROJECTED_TRACE_SCHEMA_VERSION,
  IO_VERSION,
  type ProjectedSpan,
  type ProjectedIo,
  type CapturedValue,
} from './types.js';
import {
  captureJson,
  captureToolName,
  parseProjectedIo,
  parseCapturedValue,
} from './captured-io.js';

export const MAX_PROJECTED_TOOLS_PER_TURN = 256;
type ToolFamily = 'local' | 'mcp' | 'custom';
export interface ToolResult {
  family: ToolFamily;
  correlationKey?: string;
  outcome: 'success' | 'error';
  output?: CapturedValue;
}
interface ToolUse {
  family: ToolFamily;
  correlationKey: string;
  sourceEventId: string;
  startedAt: string;
  completed?: ProjectedSpan;
  io?: ProjectedIo;
  lastApproval?: ToolApprovalFact;
}
export interface PendingToolApproval {
  result: 'allow' | 'deny';
  correlationKeys: Record<ToolFamily, string>;
}
interface ToolApprovalFact {
  result: 'allow' | 'deny';
  sourceEventId: string;
  acceptanceEventId: string;
  acceptedAt: string;
}
const APPROVAL_PREFIX = 'orca.tool.last_approval.';
const APPROVAL_KEYS = ['result', 'source_event_id', 'acceptance_event_id', 'accepted_at'].map(
  (key) => APPROVAL_PREFIX + key,
);

export function readToolApproval(
  payload: Record<string, unknown>,
): PendingToolApproval | undefined {
  if (payload.result !== 'allow' && payload.result !== 'deny') return undefined;
  const local = correlation('local', payload.tool_use_id);
  if (local === undefined) return undefined;
  return {
    result: payload.result,
    correlationKeys: {
      local,
      mcp: correlation('mcp', payload.tool_use_id)!,
      custom: correlation('custom', payload.tool_use_id)!,
    },
  };
}
export function parsePendingToolApproval(value: unknown): PendingToolApproval {
  if (
    !record(value) ||
    Object.keys(value).length !== 2 ||
    (value.result !== 'allow' && value.result !== 'deny') ||
    !record(value.correlationKeys) ||
    Object.keys(value.correlationKeys).length !== 3 ||
    !digest(value.correlationKeys.local) ||
    !digest(value.correlationKeys.mcp) ||
    !digest(value.correlationKeys.custom)
  )
    return fail();
  return {
    result: value.result,
    correlationKeys: {
      local: value.correlationKeys.local,
      mcp: value.correlationKeys.mcp,
      custom: value.correlationKeys.custom,
    },
  };
}
function parseApprovalFact(value: unknown): ToolApprovalFact {
  if (
    !record(value) ||
    Object.keys(value).length !== 4 ||
    (value.result !== 'allow' && value.result !== 'deny') ||
    !isBoundedAgentEventId(value.sourceEventId) ||
    !isBoundedAgentEventId(value.acceptanceEventId) ||
    !timestamp(value.acceptedAt)
  )
    return fail();
  return {
    result: value.result,
    sourceEventId: value.sourceEventId,
    acceptanceEventId: value.acceptanceEventId,
    acceptedAt: value.acceptedAt,
  };
}
function approvalMetadata(fact: ToolApprovalFact): ProjectedSpan['metadata'] {
  return {
    [APPROVAL_PREFIX + 'result']: fact.result,
    [APPROVAL_PREFIX + 'source_event_id']: fact.sourceEventId,
    [APPROVAL_PREFIX + 'acceptance_event_id']: fact.acceptanceEventId,
    [APPROVAL_PREFIX + 'accepted_at']: fact.acceptedAt,
  };
}
export function scrubToolApprovals(state: ToolProjectionState): void {
  for (const use of state.uses) {
    delete use.lastApproval;
    if (use.completed !== undefined)
      for (const key of APPROVAL_KEYS) delete use.completed.metadata[key];
  }
}
export function projectToolApproval(
  state: ToolProjectionState,
  receipt: PendingToolApproval,
  sourceEventId: string,
  acceptanceEventId: string,
  acceptedAt: string,
): void {
  // A confirmation does not carry a family discriminator. Require one exact
  // match across all families; custom calls use custom_tool_result, not approval.
  const matches = state.uses.filter(
    (use) => use.correlationKey === receipt.correlationKeys[use.family],
  );
  if (matches.length !== 1 || matches[0]!.family === 'custom') return;
  const use = matches[0]!;
  const fact = parseApprovalFact({
    result: receipt.result,
    sourceEventId,
    acceptanceEventId,
    acceptedAt,
  });
  if (use.completed !== undefined) {
    Object.assign(use.completed.metadata, approvalMetadata(fact));
    use.completed.metadata['orca.projection.schema_version'] = IO_PROJECTED_TRACE_SCHEMA_VERSION;
  } else use.lastApproval = fact;
}
export interface ToolProjectionState {
  uses: ToolUse[];
  results: { sourceEventId: string; producedAt: string; result: ToolResult }[];
  unmatchedResultCount: number;
}
export const initialToolProjectionState = (): ToolProjectionState => ({
  uses: [],
  results: [],
  unmatchedResultCount: 0,
});

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function fail(): never {
  throw new CanonicalProjectionStateError();
}
function family(value: unknown): value is ToolFamily {
  return value === 'local' || value === 'mcp' || value === 'custom';
}
function timestamp(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    Number.isFinite(Date.parse(value)) &&
    new Date(value).toISOString() === value
  );
}
function digest(value: unknown): value is string {
  return typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
}
function correlation(f: ToolFamily, value: unknown): string | undefined {
  if (typeof value !== 'string' || value.length === 0) return undefined;
  // Hash correlation identity only, never tool content. Family and primary subpath
  // are domain separated, including for sandbox-native IDs that are not evt_*.
  return createHash('sha256')
    .update(JSON.stringify(['orca.observability.tool-correlation.v1', f, '', value]))
    .digest('hex');
}
export function isClientToolResult(kind: string): boolean {
  return kind === 'user.tool_result' || kind === 'user.custom_tool_result';
}
export function readToolResult(
  kind: string,
  payload: Record<string, unknown>,
  state?: ToolProjectionState,
): ToolResult {
  const f =
    kind === 'user.custom_tool_result'
      ? 'custom'
      : kind === 'agent.mcp_tool_result'
        ? 'mcp'
        : 'local';
  const key = correlation(f, payload[f === 'custom' ? 'custom_tool_use_id' : 'tool_use_id']);
  return {
    family: f,
    ...(key === undefined ? {} : { correlationKey: key }),
    outcome: payload.is_error === true ? 'error' : 'success',
    ...(state?.uses.some(
      (use) => use.family === f && use.correlationKey === key && use.completed === undefined,
    )
      ? { output: captureJson(payload.content) }
      : {}),
  };
}
export function parseToolResult(value: unknown, ioEnabled = false): ToolResult {
  if (
    !record(value) ||
    !family(value.family) ||
    (value.correlationKey !== undefined && !digest(value.correlationKey)) ||
    (value.outcome !== 'success' && value.outcome !== 'error')
  )
    return fail();
  return {
    family: value.family,
    ...(value.correlationKey === undefined
      ? {}
      : { correlationKey: value.correlationKey as string }),
    outcome: value.outcome,
    ...(ioEnabled && value.output !== undefined
      ? { output: parseCapturedValue(value.output) }
      : {}),
  };
}
export function projectToolUse(
  state: ToolProjectionState,
  kind: string,
  id: string,
  at: string,
  payload: Record<string, unknown>,
  rawSourceId = id,
  ioEnabled = false,
): boolean {
  const f: ToolFamily =
    kind === 'agent.custom_tool_use' ? 'custom' : kind === 'agent.mcp_tool_use' ? 'mcp' : 'local';
  // Custom IDs are canonical source IDs; separate/local and MCP may explicitly
  // carry an independent native tool_use_id. Never fall back across families.
  const key =
    correlation(
      f,
      f === 'custom'
        ? rawSourceId
        : Object.hasOwn(payload, 'tool_use_id')
          ? payload.tool_use_id
          : rawSourceId,
    ) ??
    createHash('sha256')
      .update(JSON.stringify(['orca.observability.invalid-tool-correlation.v1', f, '', id]))
      .digest('hex');
  if (state.results.some((r) => r.sourceEventId === id)) fail();
  const existing = state.uses.find(
    (u) => u.sourceEventId === id || (u.family === f && u.correlationKey === key),
  );
  if (existing !== undefined) {
    if (
      existing.sourceEventId !== id ||
      existing.family !== f ||
      existing.correlationKey !== key ||
      existing.startedAt !== at
    )
      fail();
    return false;
  }
  if (state.uses.length >= MAX_PROJECTED_TOOLS_PER_TURN) fail();
  const toolName = ioEnabled ? captureToolName(payload.name) : undefined;
  state.uses.push({
    family: f,
    correlationKey: key,
    sourceEventId: id,
    startedAt: at,
    ...(ioEnabled
      ? {
          io: {
            version: IO_VERSION,
            input: captureJson(payload.input),
            ...(toolName === undefined ? {} : { toolName }),
          },
        }
      : {}),
  });
  return true;
}
export function projectToolResult(
  state: ToolProjectionState,
  result: ToolResult,
  id: string,
  at: string,
  traceId: string,
): boolean {
  if (state.uses.some((u) => u.sourceEventId === id)) fail();
  // Receipts preserve correlation/status duplicate semantics, not another payload copy.
  const { output, ...receipt } = result;
  const previous = state.results.find((r) => r.sourceEventId === id);
  if (previous !== undefined) {
    if (JSON.stringify(previous.result) !== JSON.stringify(receipt) || previous.producedAt !== at)
      fail();
    return false;
  }
  if (state.results.length >= MAX_PROJECTED_TOOLS_PER_TURN) fail();
  state.results.push({ sourceEventId: id, producedAt: at, result: receipt });
  const use = state.uses.find(
    (u) => u.family === result.family && u.correlationKey === result.correlationKey,
  );
  if (use === undefined || use.completed !== undefined) {
    state.unmatchedResultCount = Math.min(Number.MAX_SAFE_INTEGER, state.unmatchedResultCount + 1);
    return true;
  }
  if (use.io !== undefined && output !== undefined) use.io.output = output;
  use.completed = toolSpan(use, traceId, id, at, result.outcome);
  delete use.io;
  delete use.lastApproval;
  return true;
}
function toolSpan(
  use: ToolUse,
  traceId: string,
  endId: string,
  at: string,
  outcome: 'success' | 'error' | 'incomplete',
): ProjectedSpan {
  return {
    spanId: deterministicChildSpanId(traceId, 'tool', '', use.sourceEventId),
    parentSpanId: deterministicRootSpanId(traceId),
    sourceEventId: use.sourceEventId,
    subpath: '',
    name: 'orca.agent.tool',
    observationType: 'tool',
    startedAt: use.startedAt,
    endedAt: at < use.startedAt ? use.startedAt : at,
    status: outcome === 'incomplete' ? 'unset' : outcome === 'error' ? 'error' : 'ok',
    ...(use.io === undefined ? {} : { io: use.io }),
    metadata: {
      observation_type: 'tool',
      'orca.projection.schema_version':
        use.io === undefined && use.lastApproval === undefined
          ? PROJECTED_TRACE_SCHEMA_VERSION
          : IO_PROJECTED_TRACE_SCHEMA_VERSION,
      'orca.tool.family': use.family,
      'orca.tool.outcome': outcome,
      'orca.source.start_event_id': use.sourceEventId,
      'orca.source.end_event_id': endId,
      'orca.thread.subpath': '',
      ...(use.lastApproval === undefined ? {} : approvalMetadata(use.lastApproval)),
    },
  };
}
export function closeTools(
  state: ToolProjectionState,
  traceId: string,
  terminalId: unknown,
  at: string,
): ProjectedSpan[] {
  if (state.uses.length === 0) return [];
  const id = boundedAgentEventId(terminalId);
  if (id === undefined) return fail();
  return state.uses.map((u) => u.completed ?? toolSpan(u, traceId, id, at, 'incomplete'));
}
/** Strict persisted child decoder, shared by reducer checkpoints and outbox. */
export function parseCompletedToolSpan(value: unknown): ProjectedSpan {
  if (!record(value) || !record(value.metadata)) return fail();
  if (record(value.io) && value.io.outputScope !== undefined) return fail();
  const m = value.metadata;
  const keys = [
    'observation_type',
    'orca.projection.schema_version',
    'orca.tool.family',
    'orca.tool.outcome',
    'orca.source.start_event_id',
    'orca.source.end_event_id',
    'orca.thread.subpath',
  ];
  const outcome = m['orca.tool.outcome'];
  if (APPROVAL_KEYS.some((key) => Object.hasOwn(m, key))) {
    if (
      m['orca.projection.schema_version'] !== IO_PROJECTED_TRACE_SCHEMA_VERSION ||
      m['orca.tool.family'] === 'custom'
    )
      return fail();
    parseApprovalFact({
      result: m[APPROVAL_PREFIX + 'result'],
      sourceEventId: m[APPROVAL_PREFIX + 'source_event_id'],
      acceptanceEventId: m[APPROVAL_PREFIX + 'acceptance_event_id'],
      acceptedAt: m[APPROVAL_PREFIX + 'accepted_at'],
    });
    keys.push(...APPROVAL_KEYS);
  }
  if (
    Object.keys(m).length !== keys.length ||
    keys.some((k) => !Object.hasOwn(m, k)) ||
    m.observation_type !== 'tool' ||
    ![PROJECTED_TRACE_SCHEMA_VERSION, IO_PROJECTED_TRACE_SCHEMA_VERSION].includes(
      m['orca.projection.schema_version'] as typeof PROJECTED_TRACE_SCHEMA_VERSION,
    ) ||
    (value.io !== undefined &&
      m['orca.projection.schema_version'] !== IO_PROJECTED_TRACE_SCHEMA_VERSION) ||
    !family(m['orca.tool.family']) ||
    !['success', 'error', 'incomplete'].includes(String(outcome)) ||
    m['orca.source.start_event_id'] !== value.sourceEventId ||
    !isBoundedAgentEventId(m['orca.source.end_event_id']) ||
    m['orca.thread.subpath'] !== '' ||
    !isBoundedAgentEventId(value.sourceEventId) ||
    value.subpath !== '' ||
    value.name !== 'orca.agent.tool' ||
    value.observationType !== 'tool' ||
    typeof value.spanId !== 'string' ||
    !/^(?!0{16}$)[a-f0-9]{16}$/u.test(value.spanId) ||
    typeof value.parentSpanId !== 'string' ||
    !/^(?!0{16}$)[a-f0-9]{16}$/u.test(value.parentSpanId) ||
    !timestamp(value.startedAt) ||
    !timestamp(value.endedAt) ||
    value.endedAt < value.startedAt ||
    value.modelSummary !== undefined ||
    value.status !== (outcome === 'incomplete' ? 'unset' : outcome === 'error' ? 'error' : 'ok')
  )
    return fail();
  return {
    spanId: value.spanId,
    parentSpanId: value.parentSpanId,
    sourceEventId: value.sourceEventId,
    subpath: '',
    name: 'orca.agent.tool',
    observationType: 'tool',
    startedAt: value.startedAt,
    endedAt: value.endedAt,
    status: value.status as ProjectedSpan['status'],
    metadata: Object.fromEntries(keys.map((k) => [k, m[k] as string])),
    ...(value.io === undefined ? {} : { io: parseProjectedIo(value.io) }),
  };
}
export function parseToolProjectionState(
  value: unknown,
  traceId: string,
  ioEnabled = false,
): ToolProjectionState {
  if (
    !record(value) ||
    !Array.isArray(value.uses) ||
    !Array.isArray(value.results) ||
    value.uses.length > MAX_PROJECTED_TOOLS_PER_TURN ||
    value.results.length > MAX_PROJECTED_TOOLS_PER_TURN ||
    !Number.isSafeInteger(value.unmatchedResultCount) ||
    (value.unmatchedResultCount as number) < 0
  )
    return fail();
  const uses: ToolUse[] = value.uses.map((u: unknown) => {
    if (
      !record(u) ||
      !family(u.family) ||
      !digest(u.correlationKey) ||
      !isBoundedAgentEventId(u.sourceEventId) ||
      !timestamp(u.startedAt)
    )
      return fail();
    const completed =
      u.completed === undefined
        ? undefined
        : parseCompletedToolSpan(
            !ioEnabled && record(u.completed) && record(u.completed.metadata)
              ? {
                  ...u.completed,
                  io: undefined,
                  metadata: Object.fromEntries(
                    Object.entries(u.completed.metadata).filter(
                      ([key]) => !APPROVAL_KEYS.includes(key),
                    ),
                  ),
                }
              : u.completed,
          );
    const lastApproval =
      ioEnabled && u.lastApproval !== undefined ? parseApprovalFact(u.lastApproval) : undefined;
    if (lastApproval !== undefined && (u.family === 'custom' || completed !== undefined))
      return fail();
    if (
      ioEnabled &&
      u.io !== undefined &&
      (completed !== undefined || (record(u.io) && u.io.outputScope !== undefined))
    )
      return fail();
    if (!ioEnabled && completed !== undefined)
      completed.metadata['orca.projection.schema_version'] = PROJECTED_TRACE_SCHEMA_VERSION;
    if (
      completed !== undefined &&
      (completed.status === 'unset' ||
        completed.sourceEventId !== u.sourceEventId ||
        completed.startedAt !== u.startedAt ||
        completed.metadata['orca.tool.family'] !== u.family ||
        completed.spanId !== deterministicChildSpanId(traceId, 'tool', '', u.sourceEventId) ||
        completed.parentSpanId !== deterministicRootSpanId(traceId))
    )
      return fail();
    return {
      family: u.family,
      correlationKey: u.correlationKey,
      sourceEventId: u.sourceEventId,
      startedAt: u.startedAt,
      ...(completed === undefined ? {} : { completed }),
      ...(ioEnabled && u.io !== undefined ? { io: parseProjectedIo(u.io) } : {}),
      ...(lastApproval === undefined ? {} : { lastApproval }),
    };
  });
  const results = value.results.map((r: unknown) => {
    if (!record(r) || !isBoundedAgentEventId(r.sourceEventId) || !timestamp(r.producedAt))
      return fail();
    return {
      sourceEventId: r.sourceEventId,
      producedAt: r.producedAt,
      result: parseToolResult(r.result),
    };
  });
  if (
    new Set(uses.map((u) => u.sourceEventId)).size !== uses.length ||
    new Set(uses.map((u) => u.correlationKey)).size !== uses.length ||
    new Set([...uses, ...results].map((r) => r.sourceEventId)).size !== uses.length + results.length
  )
    return fail();
  for (const use of uses) {
    if (use.completed === undefined) continue;
    const receipt = results.find(
      (r) => r.sourceEventId === use.completed!.metadata['orca.source.end_event_id'],
    );
    if (
      receipt === undefined ||
      receipt.result.correlationKey !== use.correlationKey ||
      receipt.result.family !== use.family ||
      receipt.result.outcome !== use.completed.metadata['orca.tool.outcome'] ||
      use.completed.endedAt !==
        (receipt.producedAt < use.startedAt ? use.startedAt : receipt.producedAt)
    )
      fail();
  }
  return { uses, results, unmatchedResultCount: value.unmatchedResultCount as number };
}
