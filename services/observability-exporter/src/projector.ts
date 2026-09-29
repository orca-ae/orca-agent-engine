// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from '@orca/transcript-store-types';
import type { SessionStopReasonType } from '@orca/agent-event-contract';
import {
  closeTools,
  initialToolProjectionState,
  isClientToolResult,
  parseToolProjectionState,
  parseToolResult,
  projectToolResult,
  projectToolUse,
  readToolResult,
  readToolApproval,
  parsePendingToolApproval,
  projectToolApproval,
  scrubToolApprovals,
  type PendingToolApproval,
  type ToolProjectionState,
  type ToolResult,
} from './tool-projection.js';
export { parseCompletedToolSpan } from './tool-projection.js';
import {
  boundedAgentEventId,
  isBoundedAgentEventId,
  outcomeIdentityKey,
  isOutcomeIdentityKey,
} from './event-identity.js';
import { deterministicChildSpanId, deterministicRootSpanId, deterministicTraceId } from './ids.js';
import { isTraceSampled, parseTraceSamplingPolicy, type TraceSamplingPolicy } from './sampling.js';
import {
  PROJECTED_TRACE_SCHEMA_VERSION,
  IO_PROJECTED_TRACE_SCHEMA_VERSION,
  IO_VERSION,
  type CaptureMode,
  type CapturedValue,
  type ProjectedIo,
  MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN,
  MAX_PROJECTED_EVALUATIONS_PER_TURN,
  type ProjectedMetadataValue,
  type ProjectedModelUsage,
  type ProjectedSpan,
  type ProjectedTrace,
  type ProjectedTurnModelSummary,
} from './types.js';
import {
  captureJson,
  captureTextContent,
  parseProjectedIo,
  parseCapturedValue,
  MAX_CAPTURED_VALUE_BYTES,
  MAX_CAPTURED_IO_BYTES_PER_TURN,
  MAX_PENDING_CAPTURED_IO_BYTES,
} from './captured-io.js';

const PRIMARY_SUBPATH = '';

function captureLastError(payload: Record<string, unknown>): CapturedValue {
  const error = isRecord(payload.error) ? payload.error : {};
  const retry = isRecord(payload.retry_status) ? payload.retry_status : {};
  const facts = {
    ...(typeof error.type === 'string' && error.type.length <= 256 ? { type: error.type } : {}),
    ...(typeof error.message === 'string' ? { message: error.message } : {}),
    ...(typeof retry.will_retry === 'boolean' ? { will_retry: retry.will_retry } : {}),
    ...(typeof retry.next_attempt === 'number' &&
    Number.isSafeInteger(retry.next_attempt) &&
    retry.next_attempt > 0
      ? { next_attempt: retry.next_attempt }
      : {}),
    ...(typeof payload.retry_delay_ms === 'number' &&
    Number.isFinite(payload.retry_delay_ms) &&
    payload.retry_delay_ms >= 0
      ? { retry_delay_ms: payload.retry_delay_ms }
      : {}),
  };
  if (Object.keys(facts).length === 0) return { omitted: 'unavailable' };
  return captureJson(facts);
}

function scrubIo(state: CanonicalProjectionState): void {
  for (const pending of state.pendingInputs) {
    delete pending.input;
    if (pending.toolApproval !== undefined) {
      delete pending.toolApproval;
      delete pending.toolTraceId;
    }
    if (pending.toolResult !== undefined) delete pending.toolResult.output;
  }
  const active = state.activeTurn;
  if (active === null) return;
  delete active.io;
  scrubToolApprovals(active.tools);
  for (const use of active.tools.uses) {
    delete use.io;
    if (use.completed !== undefined) {
      delete use.completed.io;
      use.completed.metadata['orca.projection.schema_version'] = PROJECTED_TRACE_SCHEMA_VERSION;
    }
  }
  for (const receipt of active.tools.results) delete receipt.result.output;
}

function appendTurnOutput(io: ProjectedIo, next: CapturedValue): void {
  // Only complete, allowlisted text messages contribute. The array records
  // message order, not an assertion about which message is the final answer.
  if (
    io.output?.truncated === true ||
    (io.output?.omitted !== undefined && io.output.omitted !== 'partial')
  )
    return;
  if (next.json === undefined) {
    // An oversized text message is a real gap, not permission to silently
    // resume accumulation with later messages and imply a complete sequence.
    if (next.omitted === 'too_large') {
      io.output = io.output?.json === undefined ? next : { ...io.output, truncated: true };
      io.outputScope = 'turn_messages';
    }
    return;
  }
  const messages: unknown[] = io.output?.json === undefined ? [] : JSON.parse(io.output.json);
  if (!Array.isArray(messages)) throw new CanonicalProjectionStateError();
  if (messages.length >= 1024) {
    io.output = { ...io.output!, truncated: true };
    return;
  }
  messages.push(JSON.parse(next.json));
  const json = JSON.stringify(messages);
  io.outputScope = 'turn_messages';
  if (Buffer.byteLength(json, 'utf8') > MAX_CAPTURED_VALUE_BYTES) {
    io.output =
      io.output?.json === undefined ? { omitted: 'too_large' } : { ...io.output, truncated: true };
  } else {
    io.output = {
      json,
      ...(next.truncated === true ? { truncated: true as const } : {}),
      ...(next.omitted === 'partial' || io.output?.omitted === 'partial'
        ? { omitted: 'partial' as const }
        : {}),
    };
  }
}

/** Bound all durable I/O, including pending client receipts and completed tools. */
function checkIoBudgets(state: CanonicalProjectionState, strict = false): void {
  const size = (value: unknown): number => Buffer.byteLength(JSON.stringify(value), 'utf8');
  type Entry = CapturedValue | ProjectedIo;
  const pending: Entry[] = [];
  for (const input of state.pendingInputs) {
    if (input.input !== undefined) pending.push(input.input);
    if (input.toolResult?.output !== undefined) pending.push(input.toolResult.output);
  }
  const turn: Entry[] = [];
  const active = state.activeTurn;
  if (active !== null) {
    if (active.io !== undefined) turn.push(active.io);
    for (const use of active.tools.uses) {
      const holder = use.completed ?? use;
      if (holder.io !== undefined) turn.push(holder.io);
    }
  }
  for (const [entries, maximum] of [
    [pending, MAX_PENDING_CAPTURED_IO_BYTES],
    [turn, MAX_CAPTURED_IO_BYTES_PER_TURN],
  ] as const) {
    let total = entries.reduce((sum, entry) => sum + size(entry), 0);
    if (strict && total > maximum) throw new CanonicalProjectionStateError();
    // Prefer root I/O and earlier evidence; newest tool/pending additions yield
    // first. This is deterministic source-order admission, not a keep-newest cache.
    for (const entry of [...entries].reverse()) {
      if (total <= maximum) break;
      const before = size(entry);
      if ('version' in entry) {
        if (entry.input !== undefined) entry.input = { omitted: 'budget' };
        if (entry.output !== undefined) entry.output = { omitted: 'budget' };
        if (entry.diagnostic !== undefined) entry.diagnostic = { omitted: 'budget' };
        delete entry.toolName;
      } else {
        delete entry.json;
        delete entry.truncated;
        entry.omitted = 'budget';
      }
      total += size(entry) - before;
    }
  }
}

const TURN_MODEL_SUMMARY = 'turn_model_summary';
const COMPANION_SYSTEM_EVENT_ID_FIELD = '_orca_companion_system_event_id';
const MAX_PENDING_INPUTS = 1_024;
const MAX_COMPANION_SYSTEM_EVENTS = 64;
const MAX_OPEN_MODEL_SUMMARIES = 64;
const MAX_OPEN_EVALUATIONS = 64;
const OUTCOME_EVALUATION = 'outcome_evaluation';
const IGNORED_INTERNAL_EVENT_KINDS = new Set([
  'session.user_event_completed',
  'session.deferred_user_message',
  'session.deferred_user_message_submitted',
]);

export type CanonicalProjectionIssueCode =
  | 'invalid_acceptance_marker'
  | 'unresolved_acceptance_source'
  | 'orphan_companion_system_message'
  | 'overlapping_turn_acceptance';

export interface CanonicalProjectionIssue {
  code: CanonicalProjectionIssueCode;
  workspaceId: string;
  sessionId: string;
  markerEventId: string;
  sourceEventId?: string;
}

/** Complete-slice projection failed without retaining Transcript content. */
export class CanonicalProjectionError extends Error {
  constructor(readonly issues: readonly CanonicalProjectionIssue[]) {
    super(`canonical observability projection found ${issues.length} acceptance issue(s)`);
    this.name = 'CanonicalProjectionError';
  }
}

/** Persisted state was not written by this reducer and cannot be used safely. */
export class CanonicalProjectionStateError extends Error {
  constructor() {
    super('canonical observability projection state is invalid');
    this.name = 'CanonicalProjectionStateError';
  }
}

interface PendingInput {
  eventId: string;
  kind: string;
  producedAt: string;
  userId?: string;
  companionSystemEventId?: string;
  toolResult?: ToolResult;
  toolApproval?: PendingToolApproval;
  toolTraceId?: string;
  input?: CapturedValue;
}

interface OpenModelSummary {
  sourceEventId: string;
  startedAt: string;
  provider?: string;
  requestedModel?: string;
}

interface OpenEvaluation {
  sourceEventId: string;
  startedAt: string;
  outcomeKey: string;
  iteration: number;
}

interface ActiveTurn {
  io?: ProjectedIo;
  workspaceId: string;
  sessionId: string;
  anchorEventId: string;
  acceptanceEventId: string;
  userId?: string;
  inputReceivedAt: string;
  acceptedAt: string;
  executionStartedAt: string;
  executionStarted: boolean;
  sourceEventCount: number;
  continuationCount: number;
  awaitingAction: boolean;
  companionSystemEventIds: string[];
  companionSystemMessageCount: number;
  interruptCount: number;
  sawNonRetryableError: boolean;
  openModelSummaries: OpenModelSummary[];
  completedModelSummaries: ProjectedSpan[];
  tools: ToolProjectionState;
  /** Optional for pre-evaluator v1/v2/v3 checkpoints; no payload or usage is retained. */
  openEvaluations?: OpenEvaluation[];
  completedEvaluations?: ProjectedSpan[];
}

/**
 * Allowlisted reducer state. v4 may retain bounded raw turn/tool I/O and approval facts
 * under a sticky raw_io pin; older versions contain metadata only.
 */
export interface CanonicalProjectionState {
  version: 1 | 2 | 3 | 4;
  captureMode?: CaptureMode;
  pendingInputs: PendingInput[];
  activeTurn: ActiveTurn | null;
  sampling?: {
    policy: TraceSamplingPolicy;
    suppressed: SampledOutWatermark | null;
  };
}

/** One coalesced, fixed-size completion watermark per Session, not per turn. */
interface SampledOutWatermark {
  workspaceId: string;
  sessionId: string;
  firstTraceId: string;
  lastTraceId: string;
  firstSourceSeq: string;
  lastSourceSeq: string;
  turnCount: string;
  reason: 'sampled_out';
}

export interface CanonicalProjectionResult {
  state: CanonicalProjectionState;
  completedTraces: ProjectedTrace[];
  issues: CanonicalProjectionIssue[];
  acceptedSourceIds: string[];
}

interface OrderedEvent {
  event: Event;
  index: number;
}

export function initialCanonicalProjectionState(): CanonicalProjectionState {
  return {
    version: 3,
    pendingInputs: [],
    activeTurn: null,
  };
}

/**
 * Decode a previously committed reducer state. Rebuild every field from an
 * allowlist so a row can never smuggle arbitrary Transcript-shaped data back
 * into a later outbox write.
 */
export function parseCanonicalProjectionState(value: unknown): CanonicalProjectionState {
  if (
    !isRecord(value) ||
    (value.version !== 1 && value.version !== 2 && value.version !== 3 && value.version !== 4) ||
    !Array.isArray(value.pendingInputs)
  ) {
    throw new CanonicalProjectionStateError();
  }
  if (
    value.version === 4 &&
    value.captureMode !== 'metadata_only' &&
    value.captureMode !== 'raw_io'
  )
    throw new CanonicalProjectionStateError();
  const ioEnabled = value.version === 4 && value.captureMode === 'raw_io';
  const toolVersion = value.version >= 3;
  if (value.pendingInputs.length > MAX_PENDING_INPUTS) throw new CanonicalProjectionStateError();
  const pendingInputs = value.pendingInputs.map((input) =>
    parsePendingInput(input, toolVersion, ioEnabled),
  );

  const parsed: CanonicalProjectionState = {
    version: value.version,
    ...(value.version === 4 ? { captureMode: value.captureMode as CaptureMode } : {}),
    pendingInputs,
    activeTurn:
      value.activeTurn === null
        ? null
        : parseActiveTurn(value.activeTurn, value.version >= 3, ioEnabled),
    ...(value.version === 2 || (value.version >= 3 && value.sampling !== undefined)
      ? { sampling: parseSamplingState(value.sampling) }
      : {}),
  };
  // Approval facts are never restored into a sampled-out checkpoint, even if
  // persisted input was produced before the sampling pin was attached.
  if (
    parsed.activeTurn !== null &&
    parsed.sampling !== undefined &&
    !isTraceSampled(
      parsed.sampling.policy,
      deterministicTraceId(
        parsed.activeTurn.workspaceId,
        parsed.activeTurn.sessionId,
        parsed.activeTurn.anchorEventId,
      ),
    )
  ) {
    scrubToolApprovals(parsed.activeTurn.tools);
    for (const input of parsed.pendingInputs) {
      if (input.toolApproval !== undefined) {
        delete input.toolApproval;
        delete input.toolTraceId;
      }
    }
  }
  checkIoBudgets(parsed, true);
  return parsed;
}

function parseSamplingState(value: unknown): NonNullable<CanonicalProjectionState['sampling']> {
  if (!isRecord(value)) throw new CanonicalProjectionStateError();
  let policy: TraceSamplingPolicy;
  try {
    policy = parseTraceSamplingPolicy(value.policy);
  } catch {
    throw new CanonicalProjectionStateError();
  }
  if (value.suppressed === null) return { policy, suppressed: null };
  const entry = value.suppressed;
  if (
    !isRecord(entry) ||
    !isWorkspaceId(entry.workspaceId) ||
    !isSessionId(entry.sessionId) ||
    typeof entry.firstTraceId !== 'string' ||
    !/^[a-f0-9]{32}$/u.test(entry.firstTraceId) ||
    typeof entry.lastTraceId !== 'string' ||
    !/^[a-f0-9]{32}$/u.test(entry.lastTraceId) ||
    !isUint63Decimal(entry.firstSourceSeq) ||
    !isUint63Decimal(entry.lastSourceSeq) ||
    BigInt(entry.firstSourceSeq) > BigInt(entry.lastSourceSeq) ||
    !isUint63Decimal(entry.turnCount) ||
    entry.turnCount === '0' ||
    entry.reason !== 'sampled_out'
  ) {
    throw new CanonicalProjectionStateError();
  }
  return {
    policy,
    suppressed: {
      workspaceId: entry.workspaceId,
      sessionId: entry.sessionId,
      firstTraceId: entry.firstTraceId,
      lastTraceId: entry.lastTraceId,
      firstSourceSeq: entry.firstSourceSeq,
      lastSourceSeq: entry.lastSourceSeq,
      turnCount: entry.turnCount,
      reason: 'sampled_out',
    },
  };
}

const MAX_UINT63 = (1n << 63n) - 1n;

function isUint63Decimal(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    /^(0|[1-9][0-9]{0,18})$/u.test(value) &&
    BigInt(value) <= MAX_UINT63
  );
}

function sampleActiveTurn(state: CanonicalProjectionState): boolean {
  const active = state.activeTurn;
  if (active === null || state.sampling === undefined) return true;
  const sampled = isTraceSampled(
    state.sampling.policy,
    deterministicTraceId(active.workspaceId, active.sessionId, active.anchorEventId),
  );
  if (!sampled) {
    delete active.io;
    // Also scrub an open v1 metadata-only turn when adopting its Registry pin.
    delete active.userId;
    active.openModelSummaries = [];
    active.completedModelSummaries = [];
    active.tools = initialToolProjectionState();
    for (const input of state.pendingInputs) {
      delete input.toolResult;
      delete input.toolApproval;
      delete input.toolTraceId;
    }
    delete active.openEvaluations;
    delete active.completedEvaluations;
  }
  return sampled;
}

function recordSampledOut(
  state: CanonicalProjectionState,
  active: ActiveTurn,
  terminal: Event,
): void {
  if (state.sampling === undefined || !Number.isSafeInteger(terminal.seq) || terminal.seq < 0) {
    throw new CanonicalProjectionStateError();
  }
  const previous = state.sampling.suppressed;
  if (
    previous !== null &&
    (previous.workspaceId !== active.workspaceId || previous.sessionId !== active.sessionId)
  ) {
    throw new CanonicalProjectionStateError();
  }
  const traceId = deterministicTraceId(active.workspaceId, active.sessionId, active.anchorEventId);
  const seq = String(terminal.seq);
  if (previous !== null && BigInt(seq) <= BigInt(previous.lastSourceSeq)) {
    throw new CanonicalProjectionStateError();
  }
  const count = BigInt(previous?.turnCount ?? '0');
  state.sampling.suppressed = {
    workspaceId: active.workspaceId,
    sessionId: active.sessionId,
    firstTraceId: previous?.firstTraceId ?? traceId,
    lastTraceId: traceId,
    firstSourceSeq: previous?.firstSourceSeq ?? seq,
    lastSourceSeq: seq,
    turnCount: (count < MAX_UINT63 ? count + 1n : count).toString(),
    reason: 'sampled_out',
  };
}

/**
 * Reduce one authoritative, source-ordered Session batch. Payload bytes are
 * parsed only while the event is in memory and only explicit safe fields are
 * copied into the returned state.
 */
export function reduceCanonicalEventBatch(
  state: CanonicalProjectionState,
  events: readonly Event[],
  previouslyAcceptedSourceIds: ReadonlySet<string> = new Set(),
  samplingPolicy?: TraceSamplingPolicy,
  captureMode: CaptureMode = 'metadata_only',
): CanonicalProjectionResult {
  const next = parseCanonicalProjectionState(state);
  if (captureMode !== 'metadata_only' && captureMode !== 'raw_io')
    throw new CanonicalProjectionStateError();
  // Legacy nonempty checkpoints are metadata-only; neither migration nor a later
  // authorization expansion may enrich an already established session.
  if (next.version === 4 || captureMode === 'raw_io') {
    next.captureMode ??=
      next.activeTurn !== null ||
      next.pendingInputs.length > 0 ||
      next.sampling !== undefined ||
      previouslyAcceptedSourceIds.size > 0
        ? 'metadata_only'
        : captureMode;
    if (captureMode === 'metadata_only') next.captureMode = captureMode;
    next.version = 4;
  } else {
    // Preserve the legacy metadata-only checkpoint shape, including empty/gap
    // batches. v4 is required only once I/O capability has been considered.
    next.version = 3;
    delete next.captureMode;
  }
  if (next.captureMode !== 'raw_io') scrubIo(next);
  const ioEnabled = next.captureMode === 'raw_io';
  if (samplingPolicy !== undefined) {
    const sampling = parseSamplingState({ policy: samplingPolicy, suppressed: null });
    if (
      next.sampling !== undefined &&
      JSON.stringify(next.sampling.policy) !== JSON.stringify(sampling.policy)
    ) {
      // Registry must return the immutable Session pin, never a newer binding head.
      throw new CanonicalProjectionStateError();
    }
    next.sampling ??= sampling;
  }
  let sampled = sampleActiveTurn(next);
  const completedTraces: ProjectedTrace[] = [];
  const issues: CanonicalProjectionIssue[] = [];
  const acceptedSourceIds = new Set(previouslyAcceptedSourceIds);
  const newlyAcceptedSourceIds: string[] = [];

  for (const { event } of [...events.entries()]
    .map(([index, event]) => ({ event, index }))
    .sort(compareEvents)) {
    if (event.subpath !== PRIMARY_SUBPATH) continue;

    if (isPrimaryClientInput(event)) {
      const eventId = boundedAgentEventId(event.id);
      if (eventId === undefined) throw new CanonicalProjectionStateError();
      addPendingInput(next, event, eventId, acceptedSourceIds);
      continue;
    }

    if (event.kind === 'session.user_event_processed') {
      const markerEventId = boundedAgentEventId(event.id);
      if (markerEventId === undefined) throw new CanonicalProjectionStateError();
      processAcceptanceMarker(
        next,
        { ...event, id: markerEventId },
        issues,
        acceptedSourceIds,
        newlyAcceptedSourceIds,
      );
      // Acceptance establishes trace identity; gate all observation construction here.
      sampled = sampleActiveTurn(next);
      continue;
    }
    if (event.kind.startsWith('harness.') || IGNORED_INTERNAL_EVENT_KINDS.has(event.kind)) continue;

    const active = next.activeTurn;
    if (active === null) continue;
    if (
      sampled &&
      [
        'agent.tool_use',
        'agent.mcp_tool_use',
        'agent.custom_tool_use',
        'agent.tool_result',
        'agent.mcp_tool_result',
      ].includes(event.kind)
    ) {
      const id = boundedAgentEventId(event.id);
      if (id === undefined) throw new CanonicalProjectionStateError();
      const at = atOrAfter(
        normalizeTimestamp(event.producedAt, active.executionStartedAt),
        active.executionStartedAt,
      );
      const added = event.kind.endsWith('_use')
        ? projectToolUse(active.tools, event.kind, id, at, parsePayload(event), event.id, ioEnabled)
        : projectToolResult(
            active.tools,
            readToolResult(event.kind, parsePayload(event), ioEnabled ? active.tools : undefined),
            id,
            at,
            deterministicTraceId(active.workspaceId, active.sessionId, active.anchorEventId),
          );
      if (added)
        active.sourceEventCount = Math.min(Number.MAX_SAFE_INTEGER, active.sourceEventCount + 1);
      checkIoBudgets(next);
      continue;
    }
    active.sourceEventCount = Math.min(Number.MAX_SAFE_INTEGER, active.sourceEventCount + 1);

    switch (event.kind) {
      case 'agent.message': {
        if (!sampled || !ioEnabled || active.io === undefined) break;
        const payload = parsePayload(event);
        if (payload.partial === true) break;
        appendTurnOutput(active.io, captureTextContent(payload.content));
        checkIoBudgets(next);
        break;
      }
      case 'session.status_running':
        // During a required-action phase, Harness emits running only after it
        // accepts the final pending action. Keep that resume boundary
        // event-driven so durable replay observes the same transition.
        active.awaitingAction = false;
        if (!active.executionStarted) {
          active.executionStartedAt = atOrAfter(
            normalizeTimestamp(event.producedAt, active.executionStartedAt),
            active.acceptedAt,
          );
          active.executionStarted = true;
        }
        break;
      case 'span.model_request_start':
        {
          if (!sampled) break;
          const eventId = boundedAgentEventId(event.id);
          if (eventId === undefined) throw new CanonicalProjectionStateError();
          startTurnModelSummary(active, { ...event, id: eventId });
        }
        break;
      case 'span.model_request_end':
        {
          if (!sampled) break;
          const eventId = boundedAgentEventId(event.id);
          if (eventId === undefined) throw new CanonicalProjectionStateError();
          endTurnModelSummary(active, { ...event, id: eventId });
        }
        break;
      case 'span.outcome_evaluation_start':
      case 'span.outcome_evaluation_end':
        if (sampled) projectOutcomeEvaluation(active, event);
        break;
      case 'session.error':
        active.sawNonRetryableError ||=
          readBoolean(parsePayload(event), 'retry_status', 'will_retry') === false;
        if (sampled && ioEnabled && active.io !== undefined) {
          active.io.diagnostic = captureLastError(parsePayload(event));
          checkIoBudgets(next);
        }
        break;
      case 'session.status_idle': {
        const stopReason = readStopReason(parsePayload(event));
        if (stopReason === 'requires_action') {
          active.awaitingAction = true;
          break;
        }
        if (sampled) completedTraces.push(closeTurn(active, event, stopReason ?? 'unknown'));
        else recordSampledOut(next, active, event);
        next.activeTurn = null;
        break;
      }
      case 'session.status_terminated':
      case 'session.archived':
      case 'session.deleted':
        if (sampled) completedTraces.push(closeTurn(active, event, event.kind));
        else recordSampledOut(next, active, event);
        next.activeTurn = null;
        break;
      default:
        break;
    }
  }

  return { state: next, completedTraces, issues, acceptedSourceIds: newlyAcceptedSourceIds };
}

/**
 * Compatibility helper for foundation callers. It runs the same reducer from
 * empty state for each Session and reports the aggregate acceptance issues.
 */
export function projectCanonicalTurns(events: readonly Event[]): ProjectedTrace[] {
  const groups = new Map<string, Event[]>();
  for (const event of events) {
    const key = JSON.stringify([event.workspaceId, event.sessionId]);
    const entries = groups.get(key);
    if (entries === undefined) groups.set(key, [event]);
    else entries.push(event);
  }

  const completedTraces: ProjectedTrace[] = [];
  const issues: CanonicalProjectionIssue[] = [];
  for (const entries of groups.values()) {
    const result = reduceCanonicalEventBatch(initialCanonicalProjectionState(), entries);
    completedTraces.push(...result.completedTraces);
    issues.push(...result.issues);
  }
  if (issues.length > 0) throw new CanonicalProjectionError(issues);
  return completedTraces;
}

function processAcceptanceMarker(
  state: CanonicalProjectionState,
  marker: Event,
  issues: CanonicalProjectionIssue[],
  acceptedSourceIds: Set<string>,
  newlyAcceptedSourceIds: string[],
): void {
  const sourceEventId = readEventId(parsePayload(marker), 'user_event_id');
  if (sourceEventId === undefined) {
    issues.push(projectionIssue('invalid_acceptance_marker', marker));
    return;
  }
  const sourceIndex = state.pendingInputs.findIndex((input) => input.eventId === sourceEventId);
  if (sourceIndex < 0) {
    if (acceptedSourceIds.has(sourceEventId)) return;
    issues.push(projectionIssue('unresolved_acceptance_source', marker, { sourceEventId }));
    return;
  }
  if (acceptedSourceIds.has(sourceEventId)) {
    state.pendingInputs.splice(sourceIndex, 1);
    return;
  }
  const source = state.pendingInputs[sourceIndex]!;
  const active = state.activeTurn;

  if (source.kind === 'system.message') {
    if (active === null || !active.companionSystemEventIds.includes(source.eventId)) {
      issues.push(projectionIssue('orphan_companion_system_message', marker, { sourceEventId }));
      return;
    }
    active.companionSystemEventIds = active.companionSystemEventIds.filter(
      (id) => id !== source.eventId,
    );
    active.companionSystemMessageCount += 1;
    active.sourceEventCount += 2;
    consumePendingInput(state, sourceIndex, acceptedSourceIds, newlyAcceptedSourceIds);
    return;
  }

  if (source.kind === 'user.interrupt') {
    if (active !== null) {
      active.interruptCount += 1;
      active.sourceEventCount += 2;
    }
    consumePendingInput(state, sourceIndex, acceptedSourceIds, newlyAcceptedSourceIds);
    return;
  }

  if (active === null) {
    if (isContinuationOnlyInput(source.kind)) {
      issues.push(projectionIssue('overlapping_turn_acceptance', marker, { sourceEventId }));
      return;
    }
    state.activeTurn = startTurn(source, marker);
    if (state.captureMode === 'raw_io')
      state.activeTurn.io = {
        version: IO_VERSION,
        ...(source.input === undefined ? {} : { input: source.input }),
      };
    consumePendingInput(state, sourceIndex, acceptedSourceIds, newlyAcceptedSourceIds);
    return;
  }

  if (!active.awaitingAction || !isContinuationOnlyInput(source.kind)) {
    issues.push(projectionIssue('overlapping_turn_acceptance', marker, { sourceEventId }));
    return;
  }

  active.continuationCount += 1;
  active.sourceEventCount += 2;
  if (
    source.toolApproval !== undefined &&
    state.captureMode === 'raw_io' &&
    sampleActiveTurn(state) &&
    source.toolTraceId ===
      deterministicTraceId(active.workspaceId, active.sessionId, active.anchorEventId)
  ) {
    projectToolApproval(
      active.tools,
      source.toolApproval,
      source.eventId,
      marker.id,
      atOrAfter(normalizeTimestamp(marker.producedAt, source.producedAt), source.producedAt),
    );
  }
  if (isClientToolResult(source.kind) && sampleActiveTurn(state)) {
    const traceId = deterministicTraceId(
      active.workspaceId,
      active.sessionId,
      active.anchorEventId,
    );
    if (source.toolResult === undefined) {
      // Pre-turn and legacy pending inputs have no sampled correlation receipt.
      // Acceptance is still observable, but must not reconstruct a tool result.
      active.tools.unmatchedResultCount = Math.min(
        Number.MAX_SAFE_INTEGER,
        active.tools.unmatchedResultCount + 1,
      );
    } else {
      projectToolResult(
        active.tools,
        source.toolTraceId === traceId
          ? source.toolResult
          : { family: source.toolResult.family, outcome: source.toolResult.outcome },
        source.eventId,
        source.producedAt,
        traceId,
      );
    }
  }
  if (
    source.companionSystemEventId !== undefined &&
    !active.companionSystemEventIds.includes(source.companionSystemEventId)
  ) {
    if (active.companionSystemEventIds.length >= MAX_COMPANION_SYSTEM_EVENTS) {
      throw new CanonicalProjectionStateError();
    }
    active.companionSystemEventIds.push(source.companionSystemEventId);
  }
  consumePendingInput(state, sourceIndex, acceptedSourceIds, newlyAcceptedSourceIds);
  checkIoBudgets(state);
}

function addPendingInput(
  state: CanonicalProjectionState,
  event: Event,
  eventId: string,
  acceptedSourceIds: ReadonlySet<string>,
): void {
  if (acceptedSourceIds.has(eventId)) return;
  const existing = state.pendingInputs.find((input) => input.eventId === eventId);
  if (existing !== undefined && existing.kind !== event.kind)
    throw new CanonicalProjectionStateError();
  if (
    existing !== undefined &&
    !isClientToolResult(event.kind) &&
    existing.toolApproval === undefined
  )
    return;
  // Tool results queued before acceptance retain only correlation/status, and only
  // when an already accepted turn establishes a sampled-in identity. A pre-turn
  // orphan is deliberately not enriched from its payload later.
  const toolInput = isClientToolResult(event.kind);
  // Client payloads also carry acceptance-control metadata. Read the companion
  // identity even for sampled-out turns so its exact system.message acceptance
  // remains valid. This intentional control-metadata exception does not construct
  // tool observations or retain content; agent tool payloads stay gated.
  const payload = parsePayload(event);
  const companionSystemEventId = readEventId(payload, COMPANION_SYSTEM_EVENT_ID_FIELD);
  const toolResult =
    toolInput && state.activeTurn !== null && sampleActiveTurn(state)
      ? readToolResult(
          event.kind,
          payload,
          state.captureMode === 'raw_io' ? state.activeTurn.tools : undefined,
        )
      : undefined;
  const userId = optionalBoundedString(event.userId, 512);
  const toolApproval =
    event.kind === 'user.tool_confirmation' &&
    state.captureMode === 'raw_io' &&
    state.activeTurn !== null &&
    sampleActiveTurn(state)
      ? readToolApproval(payload)
      : undefined;
  const toolTraceId =
    (toolResult === undefined && toolApproval === undefined) || state.activeTurn === null
      ? undefined
      : deterministicTraceId(
          state.activeTurn.workspaceId,
          state.activeTurn.sessionId,
          state.activeTurn.anchorEventId,
        );
  if (existing !== undefined) {
    if (
      existing.producedAt !== normalizeTimestamp(event.producedAt, EPOCH) ||
      (existing.toolApproval !== undefined &&
        toolApproval !== undefined &&
        JSON.stringify(existing.toolApproval) !== JSON.stringify(toolApproval)) ||
      (existing.toolResult !== undefined &&
        toolResult !== undefined &&
        JSON.stringify(parseToolResult(existing.toolResult)) !==
          JSON.stringify(parseToolResult(toolResult)))
    )
      throw new CanonicalProjectionStateError();
    return;
  }
  if (state.pendingInputs.length >= MAX_PENDING_INPUTS) throw new CanonicalProjectionStateError();
  const candidateSampled =
    state.sampling === undefined ||
    isTraceSampled(
      state.sampling.policy,
      deterministicTraceId(event.workspaceId, event.sessionId, eventId),
    );
  state.pendingInputs.push({
    eventId,
    kind: event.kind,
    producedAt: normalizeTimestamp(event.producedAt, EPOCH),
    ...(userId === undefined ? {} : { userId }),
    ...(companionSystemEventId === undefined ? {} : { companionSystemEventId }),
    ...(toolResult === undefined ? {} : { toolResult }),
    ...(toolApproval === undefined ? {} : { toolApproval }),
    ...(toolTraceId === undefined ? {} : { toolTraceId }),
    ...(state.captureMode === 'raw_io' && event.kind === 'user.message' && candidateSampled
      ? { input: captureTextContent(payload.content) }
      : {}),
  });
  checkIoBudgets(state);
}

function consumePendingInput(
  state: CanonicalProjectionState,
  index: number,
  acceptedSourceIds: Set<string>,
  newlyAcceptedSourceIds: string[],
): void {
  const [source] = state.pendingInputs.splice(index, 1);
  if (source === undefined) throw new CanonicalProjectionStateError();
  if (!acceptedSourceIds.has(source.eventId)) {
    acceptedSourceIds.add(source.eventId);
    newlyAcceptedSourceIds.push(source.eventId);
  }
}

function startTurn(source: PendingInput, acceptance: Event): ActiveTurn {
  const inputReceivedAt = source.producedAt;
  const acceptedAt = atOrAfter(
    normalizeTimestamp(acceptance.producedAt, inputReceivedAt),
    inputReceivedAt,
  );
  return {
    workspaceId: acceptance.workspaceId,
    sessionId: acceptance.sessionId,
    anchorEventId: source.eventId,
    acceptanceEventId: acceptance.id,
    ...(source.userId === undefined ? {} : { userId: source.userId }),
    inputReceivedAt,
    acceptedAt,
    executionStartedAt: acceptedAt,
    executionStarted: false,
    sourceEventCount: 2,
    continuationCount: 0,
    awaitingAction: false,
    companionSystemEventIds:
      source.companionSystemEventId === undefined ? [] : [source.companionSystemEventId],
    companionSystemMessageCount: 0,
    interruptCount: 0,
    sawNonRetryableError: false,
    openModelSummaries: [],
    completedModelSummaries: [],
    tools: initialToolProjectionState(),
  };
}

function startTurnModelSummary(active: ActiveTurn, event: Event): void {
  const payload = parsePayload(event);
  if (
    !hasTurnModelSummaryLabel(payload) ||
    active.openModelSummaries.some((summary) => summary.sourceEventId === event.id) ||
    active.completedModelSummaries.some((summary) => summary.sourceEventId === event.id)
  ) {
    return;
  }
  if (active.openModelSummaries.length >= MAX_OPEN_MODEL_SUMMARIES) {
    throw new CanonicalProjectionStateError();
  }

  active.executionStarted = true;
  const provider = optionalBoundedString(payload.provider, 256);
  const requestedModel = optionalBoundedString(payload.model, 256);
  active.openModelSummaries.push({
    sourceEventId: event.id,
    startedAt: atOrAfter(
      normalizeTimestamp(event.producedAt, active.executionStartedAt),
      active.executionStartedAt,
    ),
    ...(provider === undefined ? {} : { provider }),
    ...(requestedModel === undefined ? {} : { requestedModel }),
  });
}

function endTurnModelSummary(active: ActiveTurn, event: Event): void {
  const payload = parsePayload(event);
  if (!hasTurnModelSummaryLabel(payload)) return;
  const startEventId = readEventId(payload, 'model_request_start_id');
  if (startEventId === undefined) return;
  const openIndex = active.openModelSummaries.findIndex(
    (summary) => summary.sourceEventId === startEventId,
  );
  if (openIndex < 0) return;
  const [open] = active.openModelSummaries.splice(openIndex, 1);
  if (open === undefined) throw new CanonicalProjectionStateError();

  const traceId = deterministicTraceId(active.workspaceId, active.sessionId, active.anchorEventId);
  const provider = optionalBoundedString(payload.provider, 256) ?? open.provider;
  const requestedModel = optionalBoundedString(payload.model, 256) ?? open.requestedModel;
  const usage = readModelUsage(payload.model_usage);
  const totalCostUsd = readNonNegativeNumber(payload.total_cost_usd);
  const modelSummary: ProjectedTurnModelSummary = {
    ...(provider === undefined ? {} : { provider }),
    ...(requestedModel === undefined ? {} : { requestedModel }),
    ...(usage === undefined ? {} : { usage }),
    ...(totalCostUsd === undefined ? {} : { totalCostUsd }),
  };
  const endedAt = atOrAfter(normalizeTimestamp(event.producedAt, open.startedAt), open.startedAt);
  if (active.completedModelSummaries.length >= MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN) {
    throw new CanonicalProjectionStateError();
  }
  active.completedModelSummaries.push({
    spanId: deterministicChildSpanId(
      traceId,
      TURN_MODEL_SUMMARY,
      PRIMARY_SUBPATH,
      open.sourceEventId,
    ),
    parentSpanId: deterministicRootSpanId(traceId),
    sourceEventId: open.sourceEventId,
    subpath: PRIMARY_SUBPATH,
    name: 'orca.agent.turn_model_summary',
    observationType: TURN_MODEL_SUMMARY,
    startedAt: open.startedAt,
    endedAt,
    status: payload.is_error === true ? 'error' : 'ok',
    metadata: {
      observation_type: TURN_MODEL_SUMMARY,
      'orca.projection.schema_version': PROJECTED_TRACE_SCHEMA_VERSION,
      'orca.model.observation.kind': TURN_MODEL_SUMMARY,
      'orca.source.start_event_id': open.sourceEventId,
      'orca.source.end_event_id': event.id,
      'orca.thread.subpath': PRIMARY_SUBPATH,
    },
    ...(Object.keys(modelSummary).length === 0 ? {} : { modelSummary }),
  });
}

function projectOutcomeEvaluation(active: ActiveTurn, event: Event): void {
  const sourceEventId = boundedAgentEventId(event.id);
  if (sourceEventId === undefined) throw new CanonicalProjectionStateError();
  const payload = parsePayload(event);
  const outcomeKey = outcomeIdentityKey(payload.outcome_id);
  const iteration = readNonNegativeInteger(payload.iteration);
  if (outcomeKey === undefined || iteration === undefined) return;

  if (event.kind === 'span.outcome_evaluation_start') {
    // The producer supplies the same explicit start ID in envelope and payload.
    if (payload.id !== undefined && boundedAgentEventId(payload.id) !== sourceEventId) return;
    if (
      active.openEvaluations?.some((entry) => entry.sourceEventId === sourceEventId) ||
      active.completedEvaluations?.some((entry) => entry.sourceEventId === sourceEventId)
    )
      return;
    const open = (active.openEvaluations ??= []);
    if (open.length >= MAX_OPEN_EVALUATIONS) throw new CanonicalProjectionStateError();
    open.push({
      sourceEventId,
      outcomeKey,
      iteration,
      startedAt: atOrAfter(
        normalizeTimestamp(event.producedAt, active.executionStartedAt),
        active.executionStartedAt,
      ),
    });
    return;
  }

  const startId = readEventId(payload, 'outcome_evaluation_start_id');
  if (startId === undefined || !isEvaluationResult(payload.result)) return;
  const openIndex =
    active.openEvaluations?.findIndex(
      (entry) =>
        entry.sourceEventId === startId &&
        entry.outcomeKey === outcomeKey &&
        entry.iteration === iteration,
    ) ?? -1;
  if (openIndex < 0) return;
  const completed = (active.completedEvaluations ??= []);
  if (completed.length >= MAX_PROJECTED_EVALUATIONS_PER_TURN)
    throw new CanonicalProjectionStateError();
  const open = active.openEvaluations!.splice(openIndex, 1)[0]!;
  const traceId = deterministicTraceId(active.workspaceId, active.sessionId, active.anchorEventId);
  completed.push({
    spanId: deterministicChildSpanId(traceId, OUTCOME_EVALUATION, PRIMARY_SUBPATH, startId),
    parentSpanId: deterministicRootSpanId(traceId),
    sourceEventId: startId,
    subpath: PRIMARY_SUBPATH,
    name: 'orca.agent.outcome_evaluation',
    observationType: OUTCOME_EVALUATION,
    startedAt: open.startedAt,
    endedAt: atOrAfter(normalizeTimestamp(event.producedAt, open.startedAt), open.startedAt),
    status: evaluationStatus(payload.result),
    metadata: {
      observation_type: OUTCOME_EVALUATION,
      'orca.projection.schema_version': PROJECTED_TRACE_SCHEMA_VERSION,
      'orca.source.start_event_id': startId,
      'orca.source.end_event_id': sourceEventId,
      'orca.thread.subpath': PRIMARY_SUBPATH,
      'orca.outcome.iteration': iteration,
      'orca.outcome.result': payload.result,
    },
  });
}

// The shared producer contract includes interrupted; Claude currently emits the other four.
function isEvaluationResult(
  value: unknown,
): value is 'satisfied' | 'needs_revision' | 'max_iterations_reached' | 'failed' | 'interrupted' {
  return (
    value === 'satisfied' ||
    value === 'needs_revision' ||
    value === 'max_iterations_reached' ||
    value === 'failed' ||
    value === 'interrupted'
  );
}

function evaluationStatus(result: string): ProjectedSpan['status'] {
  // An unmet criterion is a verdict, not an evaluator execution failure.
  return result === 'failed' ? 'error' : result === 'interrupted' ? 'unset' : 'ok';
}

function closeTurn(active: ActiveTurn, terminal: Event, terminalReason: string): ProjectedTrace {
  const schemaVersion =
    active.io === undefined ? PROJECTED_TRACE_SCHEMA_VERSION : IO_PROJECTED_TRACE_SCHEMA_VERSION;
  const traceId = deterministicTraceId(active.workspaceId, active.sessionId, active.anchorEventId);
  const rootSpanId = deterministicRootSpanId(traceId);
  const tools = closeTools(
    active.tools,
    traceId,
    terminal.id,
    atOrAfter(
      normalizeTimestamp(terminal.producedAt, active.executionStartedAt),
      active.executionStartedAt,
    ),
  );
  const spans = [
    ...active.completedModelSummaries,
    ...tools,
    ...(active.completedEvaluations ?? []),
  ];
  for (const span of spans) span.metadata['orca.projection.schema_version'] = schemaVersion;
  const latestChildEnd = spans.reduce(
    (latest, span) => atOrAfter(latest, span.endedAt),
    active.executionStartedAt,
  );
  const endedAt = atOrAfter(
    latestChildEnd,
    atOrAfter(
      normalizeTimestamp(terminal.producedAt, active.executionStartedAt),
      active.executionStartedAt,
    ),
  );
  // Evaluator verdicts do not override the accepted agent turn's execution status.
  const childFailed = active.completedModelSummaries.some((span) => span.status === 'error');
  const terminalFailed =
    terminalReason === 'retries_exhausted' || terminalReason === 'session.status_terminated';
  const status = childFailed || terminalFailed || active.sawNonRetryableError ? 'error' : 'ok';
  const metadata: Record<string, ProjectedMetadataValue> = {
    'orca.projection.schema_version': schemaVersion,
    'orca.workspace.id': active.workspaceId,
    'orca.session.id': active.sessionId,
    'orca.turn.anchor_event_id': active.anchorEventId,
    'orca.turn.acceptance_event_id': active.acceptanceEventId,
    'orca.turn.input_received_at': active.inputReceivedAt,
    'orca.turn.accepted_at': active.acceptedAt,
    'orca.turn.terminal_reason': terminalReason,
    'orca.turn.source_event_count': active.sourceEventCount,
    'orca.turn.continuation_count': active.continuationCount,
    'orca.turn.companion_system_message_count': active.companionSystemMessageCount,
    'orca.turn.interrupt_count': active.interruptCount,
    'orca.turn.model_summary_count': active.completedModelSummaries.length,
    'orca.thread.subpath': PRIMARY_SUBPATH,
    ...(active.tools.unmatchedResultCount === 0
      ? {}
      : { 'orca.turn.unmatched_tool_result_count': active.tools.unmatchedResultCount }),
  };

  return {
    schemaVersion,
    traceId,
    workspaceId: active.workspaceId,
    sessionId: active.sessionId,
    anchorEventId: active.anchorEventId,
    acceptanceEventId: active.acceptanceEventId,
    ...(active.userId === undefined ? {} : { userId: active.userId }),
    root: {
      spanId: rootSpanId,
      sourceEventId: active.anchorEventId,
      subpath: PRIMARY_SUBPATH,
      name: 'orca.agent.turn',
      observationType: 'agent_turn',
      startedAt: active.executionStartedAt,
      endedAt,
      status,
      metadata,
      ...(active.io === undefined ? {} : { io: active.io }),
    },
    spans,
  };
}

function parsePendingInput(value: unknown, toolVersion = true, ioEnabled = false): PendingInput {
  if (
    !isRecord(value) ||
    !isBoundedAgentEventId(value.eventId) ||
    !isAcceptedInputKind(value.kind)
  ) {
    throw new CanonicalProjectionStateError();
  }
  if (!isTimestamp(value.producedAt)) throw new CanonicalProjectionStateError();
  const userId = optionalBoundedString(value.userId, 512);
  const companionSystemEventId = optionalEventId(value.companionSystemEventId);
  const toolResult =
    !toolVersion || value.toolResult === undefined
      ? undefined
      : parseToolResult(value.toolResult, ioEnabled);
  const toolApproval =
    toolVersion && ioEnabled && value.toolApproval !== undefined
      ? parsePendingToolApproval(value.toolApproval)
      : undefined;
  if (
    toolApproval !== undefined &&
    (value.kind !== 'user.tool_confirmation' || toolResult !== undefined)
  )
    throw new CanonicalProjectionStateError();
  const toolTraceId =
    toolVersion && !(value.toolApproval !== undefined && !ioEnabled)
      ? value.toolTraceId
      : undefined;
  if (
    toolResult === undefined && toolApproval === undefined
      ? toolTraceId !== undefined
      : typeof toolTraceId !== 'string' || !/^(?!0{32}$)[a-f0-9]{32}$/u.test(toolTraceId)
  )
    throw new CanonicalProjectionStateError();
  if (
    toolResult !== undefined &&
    (!isClientToolResult(value.kind) ||
      toolResult.family !== (value.kind === 'user.custom_tool_result' ? 'custom' : 'local'))
  )
    throw new CanonicalProjectionStateError();
  if (
    (value.userId !== undefined && userId === undefined) ||
    (value.companionSystemEventId !== undefined && companionSystemEventId === undefined)
  ) {
    throw new CanonicalProjectionStateError();
  }
  return {
    eventId: value.eventId,
    kind: value.kind,
    producedAt: value.producedAt,
    ...(userId === undefined ? {} : { userId }),
    ...(companionSystemEventId === undefined ? {} : { companionSystemEventId }),
    ...(toolResult === undefined ? {} : { toolResult }),
    ...(toolApproval === undefined ? {} : { toolApproval }),
    ...(toolTraceId === undefined ? {} : { toolTraceId: toolTraceId as string }),
    ...(ioEnabled && value.kind === 'user.message' && value.input !== undefined
      ? { input: parseCapturedValue(value.input) }
      : {}),
  };
}

function parseActiveTurn(value: unknown, toolVersion = true, ioEnabled = false): ActiveTurn {
  if (!isRecord(value)) throw new CanonicalProjectionStateError();
  if (ioEnabled && value.io !== undefined) {
    const io = parseProjectedIo(value.io);
    if (
      io.toolName !== undefined ||
      (io.output?.json !== undefined && io.outputScope !== 'turn_messages')
    )
      throw new CanonicalProjectionStateError();
  }
  const workspaceId = value.workspaceId;
  const sessionId = value.sessionId;
  const anchorEventId = value.anchorEventId;
  const acceptanceEventId = value.acceptanceEventId;
  const inputReceivedAt = value.inputReceivedAt;
  const acceptedAt = value.acceptedAt;
  const executionStartedAt = value.executionStartedAt;
  const sourceEventCount = value.sourceEventCount;
  const continuationCount = value.continuationCount;
  const companionSystemMessageCount = value.companionSystemMessageCount;
  const interruptCount = value.interruptCount;
  if (
    !isWorkspaceId(workspaceId) ||
    !isSessionId(sessionId) ||
    !isBoundedAgentEventId(anchorEventId) ||
    !isBoundedAgentEventId(acceptanceEventId) ||
    !isTimestamp(inputReceivedAt) ||
    !isTimestamp(acceptedAt) ||
    !isTimestamp(executionStartedAt) ||
    !isNonNegativeSafeInteger(sourceEventCount) ||
    !isNonNegativeSafeInteger(continuationCount) ||
    !isNonNegativeSafeInteger(companionSystemMessageCount) ||
    !isNonNegativeSafeInteger(interruptCount)
  ) {
    throw new CanonicalProjectionStateError();
  }
  if (
    typeof value.executionStarted !== 'boolean' ||
    typeof value.awaitingAction !== 'boolean' ||
    typeof value.sawNonRetryableError !== 'boolean' ||
    !Array.isArray(value.companionSystemEventIds) ||
    !Array.isArray(value.openModelSummaries) ||
    !Array.isArray(value.completedModelSummaries)
  ) {
    throw new CanonicalProjectionStateError();
  }
  if (
    value.companionSystemEventIds.length > MAX_COMPANION_SYSTEM_EVENTS ||
    value.openModelSummaries.length > MAX_OPEN_MODEL_SUMMARIES ||
    value.completedModelSummaries.length > MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN
  ) {
    throw new CanonicalProjectionStateError();
  }
  const userId = optionalBoundedString(value.userId, 512);
  if (value.userId !== undefined && userId === undefined) throw new CanonicalProjectionStateError();
  const openEvaluations =
    value.openEvaluations === undefined
      ? undefined
      : parseEvaluationArray(value.openEvaluations, MAX_OPEN_EVALUATIONS, parseOpenEvaluation);
  const completedEvaluations =
    value.completedEvaluations === undefined
      ? undefined
      : parseEvaluationArray(
          value.completedEvaluations,
          MAX_PROJECTED_EVALUATIONS_PER_TURN,
          parseCompletedEvaluation,
        );
  const traceId = deterministicTraceId(workspaceId, sessionId, anchorEventId);
  const rootSpanId = deterministicRootSpanId(traceId);
  const startIds = new Set<string>();
  for (const evaluation of [...(openEvaluations ?? []), ...(completedEvaluations ?? [])]) {
    if (startIds.has(evaluation.sourceEventId)) throw new CanonicalProjectionStateError();
    startIds.add(evaluation.sourceEventId);
  }
  for (const evaluation of completedEvaluations ?? []) {
    if (
      evaluation.parentSpanId !== rootSpanId ||
      evaluation.spanId !==
        deterministicChildSpanId(
          traceId,
          OUTCOME_EVALUATION,
          PRIMARY_SUBPATH,
          evaluation.sourceEventId,
        )
    )
      throw new CanonicalProjectionStateError();
  }
  return {
    workspaceId,
    sessionId,
    anchorEventId,
    acceptanceEventId,
    ...(userId === undefined ? {} : { userId }),
    inputReceivedAt,
    acceptedAt,
    executionStartedAt,
    executionStarted: value.executionStarted,
    sourceEventCount,
    continuationCount,
    awaitingAction: value.awaitingAction,
    companionSystemEventIds: value.companionSystemEventIds.map((entry) => {
      if (!isBoundedAgentEventId(entry)) throw new CanonicalProjectionStateError();
      return entry;
    }),
    companionSystemMessageCount,
    interruptCount,
    sawNonRetryableError: value.sawNonRetryableError,
    openModelSummaries: value.openModelSummaries.map(parseOpenModelSummary),
    completedModelSummaries: value.completedModelSummaries.map(parseCompletedModelSummarySpan),
    tools: toolVersion
      ? parseToolProjectionState(
          value.tools,
          deterministicTraceId(workspaceId, sessionId, anchorEventId),
          ioEnabled,
        )
      : initialToolProjectionState(),
    ...(openEvaluations === undefined ? {} : { openEvaluations }),
    ...(completedEvaluations === undefined ? {} : { completedEvaluations }),
    ...(ioEnabled && value.io !== undefined ? { io: parseProjectedIo(value.io) } : {}),
  };
}

function parseEvaluationArray<T>(value: unknown, limit: number, parse: (entry: unknown) => T): T[] {
  if (!Array.isArray(value) || value.length > limit) throw new CanonicalProjectionStateError();
  return value.map(parse);
}

function parseOpenEvaluation(value: unknown): OpenEvaluation {
  if (
    !isRecord(value) ||
    !isBoundedAgentEventId(value.sourceEventId) ||
    !isTimestamp(value.startedAt) ||
    'outcomeId' in value ||
    !isOutcomeIdentityKey(value.outcomeKey) ||
    !isNonNegativeSafeInteger(value.iteration)
  ) {
    throw new CanonicalProjectionStateError();
  }
  return {
    sourceEventId: value.sourceEventId,
    startedAt: value.startedAt,
    outcomeKey: value.outcomeKey,
    iteration: value.iteration,
  };
}

function parseCompletedEvaluation(value: unknown): ProjectedSpan {
  if (
    !isRecord(value) ||
    'outcomeId' in value ||
    'outcomeKey' in value ||
    !isSpanId(value.spanId) ||
    !isSpanId(value.parentSpanId) ||
    !isBoundedAgentEventId(value.sourceEventId) ||
    value.subpath !== PRIMARY_SUBPATH ||
    value.name !== 'orca.agent.outcome_evaluation' ||
    value.observationType !== OUTCOME_EVALUATION ||
    !isTimestamp(value.startedAt) ||
    !isTimestamp(value.endedAt) ||
    Date.parse(value.endedAt) < Date.parse(value.startedAt) ||
    !isRecord(value.metadata) ||
    value.modelSummary !== undefined
  )
    throw new CanonicalProjectionStateError();
  const metadata = value.metadata;
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
    !hasExactKeys(metadata, keys) ||
    metadata.observation_type !== OUTCOME_EVALUATION ||
    metadata['orca.projection.schema_version'] !== PROJECTED_TRACE_SCHEMA_VERSION ||
    metadata['orca.source.start_event_id'] !== value.sourceEventId ||
    !isBoundedAgentEventId(metadata['orca.source.end_event_id']) ||
    metadata['orca.thread.subpath'] !== PRIMARY_SUBPATH ||
    !isNonNegativeSafeInteger(metadata['orca.outcome.iteration']) ||
    !isEvaluationResult(metadata['orca.outcome.result']) ||
    value.status !== evaluationStatus(metadata['orca.outcome.result'])
  )
    throw new CanonicalProjectionStateError();
  return {
    spanId: value.spanId,
    parentSpanId: value.parentSpanId,
    sourceEventId: value.sourceEventId,
    subpath: PRIMARY_SUBPATH,
    name: value.name,
    observationType: OUTCOME_EVALUATION,
    startedAt: value.startedAt,
    endedAt: value.endedAt,
    status: evaluationStatus(metadata['orca.outcome.result']),
    metadata: Object.fromEntries(keys.map((key) => [key, metadata[key] as ProjectedMetadataValue])),
  };
}

function parseOpenModelSummary(value: unknown): OpenModelSummary {
  if (
    !isRecord(value) ||
    !isBoundedAgentEventId(value.sourceEventId) ||
    !isTimestamp(value.startedAt)
  ) {
    throw new CanonicalProjectionStateError();
  }
  const provider = optionalBoundedString(value.provider, 256);
  const requestedModel = optionalBoundedString(value.requestedModel, 256);
  if (
    (value.provider !== undefined && provider === undefined) ||
    (value.requestedModel !== undefined && requestedModel === undefined)
  ) {
    throw new CanonicalProjectionStateError();
  }
  return {
    sourceEventId: value.sourceEventId,
    startedAt: value.startedAt,
    ...(provider === undefined ? {} : { provider }),
    ...(requestedModel === undefined ? {} : { requestedModel }),
  };
}

function parseCompletedModelSummarySpan(value: unknown): ProjectedSpan {
  if (!isRecord(value)) throw new CanonicalProjectionStateError();
  const spanId = value.spanId;
  const sourceEventId = value.sourceEventId;
  const subpath = value.subpath;
  const name = value.name;
  const startedAt = value.startedAt;
  const endedAt = value.endedAt;
  const parentSpanId = value.parentSpanId;
  const observationType = value.observationType;
  const status = value.status;
  if (
    !isSpanId(spanId) ||
    !isBoundedAgentEventId(sourceEventId) ||
    subpath !== PRIMARY_SUBPATH ||
    name !== 'orca.agent.turn_model_summary' ||
    !isTimestamp(startedAt) ||
    !isTimestamp(endedAt) ||
    !isSpanId(parentSpanId) ||
    observationType !== TURN_MODEL_SUMMARY ||
    (status !== 'ok' && status !== 'error') ||
    !isRecord(value.metadata)
  ) {
    throw new CanonicalProjectionStateError();
  }
  const metadata = parseSummaryMetadata(value.metadata, sourceEventId);
  const modelSummary =
    value.modelSummary === undefined ? undefined : parseModelSummary(value.modelSummary);
  return {
    spanId,
    parentSpanId,
    sourceEventId,
    subpath,
    name,
    observationType,
    startedAt,
    endedAt,
    status,
    metadata,
    ...(modelSummary === undefined ? {} : { modelSummary }),
  };
}

function parseSummaryMetadata(
  value: Record<string, unknown>,
  sourceEventId: string,
): Record<string, ProjectedMetadataValue> {
  const expectedKeys = [
    'observation_type',
    'orca.model.observation.kind',
    'orca.projection.schema_version',
    'orca.source.end_event_id',
    'orca.source.start_event_id',
    'orca.thread.subpath',
  ];
  if (
    !hasExactKeys(value, expectedKeys) ||
    value.observation_type !== TURN_MODEL_SUMMARY ||
    value['orca.model.observation.kind'] !== TURN_MODEL_SUMMARY ||
    value['orca.projection.schema_version'] !== PROJECTED_TRACE_SCHEMA_VERSION ||
    value['orca.source.start_event_id'] !== sourceEventId ||
    !isBoundedAgentEventId(value['orca.source.end_event_id']) ||
    value['orca.thread.subpath'] !== PRIMARY_SUBPATH
  ) {
    throw new CanonicalProjectionStateError();
  }
  return Object.fromEntries(expectedKeys.map((key) => [key, value[key] as string]));
}

function parseModelSummary(value: unknown): ProjectedTurnModelSummary {
  if (!isRecord(value)) throw new CanonicalProjectionStateError();
  const provider = optionalBoundedString(value.provider, 256);
  const requestedModel = optionalBoundedString(value.requestedModel, 256);
  const totalCostUsd = readNonNegativeNumber(value.totalCostUsd);
  if (
    (value.provider !== undefined && provider === undefined) ||
    (value.requestedModel !== undefined && requestedModel === undefined) ||
    (value.totalCostUsd !== undefined && totalCostUsd === undefined)
  ) {
    throw new CanonicalProjectionStateError();
  }
  const usage = value.usage === undefined ? undefined : parseModelUsage(value.usage);
  return {
    ...(provider === undefined ? {} : { provider }),
    ...(requestedModel === undefined ? {} : { requestedModel }),
    ...(usage === undefined ? {} : { usage }),
    ...(totalCostUsd === undefined ? {} : { totalCostUsd }),
  };
}

function parseModelUsage(value: unknown): ProjectedModelUsage {
  if (!isRecord(value)) throw new CanonicalProjectionStateError();
  const inputTokens = readNonNegativeInteger(value.inputTokens);
  const outputTokens = readNonNegativeInteger(value.outputTokens);
  const cacheCreationInputTokens = readNonNegativeInteger(value.cacheCreationInputTokens);
  const cacheReadInputTokens = readNonNegativeInteger(value.cacheReadInputTokens);
  if (
    (value.inputTokens !== undefined && inputTokens === undefined) ||
    (value.outputTokens !== undefined && outputTokens === undefined) ||
    (value.cacheCreationInputTokens !== undefined && cacheCreationInputTokens === undefined) ||
    (value.cacheReadInputTokens !== undefined && cacheReadInputTokens === undefined)
  ) {
    throw new CanonicalProjectionStateError();
  }
  return {
    ...(inputTokens === undefined ? {} : { inputTokens }),
    ...(outputTokens === undefined ? {} : { outputTokens }),
    ...(cacheCreationInputTokens === undefined ? {} : { cacheCreationInputTokens }),
    ...(cacheReadInputTokens === undefined ? {} : { cacheReadInputTokens }),
  };
}

function compareEvents(left: OrderedEvent, right: OrderedEvent): number {
  const leftSeq = Number.isFinite(left.event.seq) ? left.event.seq : Number.MAX_SAFE_INTEGER;
  const rightSeq = Number.isFinite(right.event.seq) ? right.event.seq : Number.MAX_SAFE_INTEGER;
  return leftSeq - rightSeq || left.index - right.index;
}

function isPrimaryClientInput(event: Event): boolean {
  return (
    event.producedBy === 'client' &&
    (event.kind.startsWith('user.') || event.kind === 'system.message')
  );
}

function hasTurnModelSummaryLabel(payload: Record<string, unknown>): boolean {
  return (
    payload.observation_type === TURN_MODEL_SUMMARY ||
    payload.model_observation_kind === TURN_MODEL_SUMMARY
  );
}

function parsePayload(event: Event): Record<string, unknown> {
  try {
    const parsed = JSON.parse(Buffer.from(event.payload).toString('utf8')) as unknown;
    return isRecord(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function readRecord(value: Record<string, unknown>, key: string): Record<string, unknown> {
  const candidate = value[key];
  return isRecord(candidate) ? candidate : {};
}

function readStopReason(payload: Record<string, unknown>): SessionStopReasonType | undefined {
  const type = readRecord(payload, 'stop_reason').type;
  switch (type) {
    case 'end_turn':
    case 'requires_action':
    case 'retries_exhausted':
      return type;
    default:
      return undefined;
  }
}

function readEventId(value: Record<string, unknown>, key: string): string | undefined {
  return boundedAgentEventId(value[key]);
}

function readBoolean(
  value: Record<string, unknown>,
  parentKey: string,
  key: string,
): boolean | undefined {
  const candidate = readRecord(value, parentKey)[key];
  return typeof candidate === 'boolean' ? candidate : undefined;
}

function readModelUsage(value: unknown): ProjectedModelUsage | undefined {
  if (!isRecord(value)) return undefined;
  const inputTokens = readNonNegativeInteger(value.input_tokens);
  const outputTokens = readNonNegativeInteger(value.output_tokens);
  const cacheCreationInputTokens = readNonNegativeInteger(value.cache_creation_input_tokens);
  const cacheReadInputTokens = readNonNegativeInteger(value.cache_read_input_tokens);
  const usage: ProjectedModelUsage = {
    ...(inputTokens === undefined ? {} : { inputTokens }),
    ...(outputTokens === undefined ? {} : { outputTokens }),
    ...(cacheCreationInputTokens === undefined ? {} : { cacheCreationInputTokens }),
    ...(cacheReadInputTokens === undefined ? {} : { cacheReadInputTokens }),
  };
  return Object.keys(usage).length === 0 ? undefined : usage;
}

function readNonNegativeInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function readNonNegativeNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : undefined;
}

function normalizeTimestamp(value: string, fallback: string): string {
  const millis = Date.parse(value);
  return Number.isFinite(millis) ? new Date(millis).toISOString() : fallback;
}

function atOrAfter(value: string, minimum: string): string {
  return Date.parse(value) >= Date.parse(minimum) ? value : minimum;
}

function projectionIssue(
  code: CanonicalProjectionIssueCode,
  marker: Event,
  extra: { sourceEventId?: string } = {},
): CanonicalProjectionIssue {
  return {
    code,
    workspaceId: marker.workspaceId,
    sessionId: marker.sessionId,
    markerEventId: marker.id,
    ...(extra.sourceEventId === undefined ? {} : { sourceEventId: extra.sourceEventId }),
  };
}

function isContinuationOnlyInput(kind: string): boolean {
  return (
    kind === 'user.tool_confirmation' ||
    kind === 'user.tool_result' ||
    kind === 'user.custom_tool_result'
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isSpanId(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{16}$/u.test(value);
}

function isTimestamp(value: unknown): value is string {
  return typeof value === 'string' && Number.isFinite(Date.parse(value));
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    actual.length === sortedExpected.length &&
    actual.every((key, index) => key === sortedExpected[index])
  );
}

function optionalBoundedString(value: unknown, maximumLength: number): string | undefined {
  return typeof value === 'string' &&
    value.length > 0 &&
    value.length <= maximumLength &&
    !hasControlCharacter(value)
    ? value
    : undefined;
}

function optionalEventId(value: unknown): string | undefined {
  return isBoundedAgentEventId(value) ? value : undefined;
}

function isWorkspaceId(value: unknown): value is string {
  return typeof value === 'string' && /^[A-Za-z0-9_-]{1,128}$/u.test(value);
}

function isSessionId(value: unknown): value is string {
  return typeof value === 'string' && /^ses_[A-Za-z0-9_-]{1,508}$/u.test(value);
}

function isAcceptedInputKind(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length <= 128 &&
    (value === 'system.message' || /^user\.[a-z0-9_.-]+$/u.test(value))
  );
}

function hasControlCharacter(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) return true;
  }
  return false;
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

const EPOCH = '1970-01-01T00:00:00.000Z';
