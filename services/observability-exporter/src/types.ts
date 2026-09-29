// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Stable schema label for the transport-free projected trace model. */
export const PROJECTED_TRACE_SCHEMA_VERSION = 'orca.observability.projected-trace.v1' as const;
export const IO_PROJECTED_TRACE_SCHEMA_VERSION = 'orca.observability.projected-trace.v2' as const;
export type CaptureMode = 'metadata_only' | 'raw_io';
export const IO_VERSION = 'orca.observability.raw-io.v1' as const;
export interface CapturedValue {
  json?: string;
  truncated?: true;
  omitted?: 'unsupported' | 'too_large' | 'budget' | 'partial' | 'unavailable';
}
export interface ProjectedIo {
  version: typeof IO_VERSION;
  input?: CapturedValue;
  output?: CapturedValue;
  /** Last reported session.error facts, not the turn outcome or inferred retry counts. */
  diagnostic?: CapturedValue;
  toolName?: string;
  outputScope?: 'turn_messages';
}
export const MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN = 256;
export const MAX_PROJECTED_EVALUATIONS_PER_TURN = 256;

/** Complete OTLP response summary. Messages never cross this boundary as raw text. */
export type OtlpDeliveryOutcome =
  | { kind: 'accepted' }
  | {
      kind: 'accepted_with_warning';
      rejectedSpans: '0';
      messageBytes: number;
      messageSha256: string;
    }
  | {
      kind: 'partial_rejection';
      /** Canonical positive signed-int64 decimal, never a JavaScript number. */
      rejectedSpans: string;
      messageBytes: number;
      messageSha256: string;
    };

export type ProjectedMetadataValue = string | number | boolean;

/** Aggregate usage reported by a current Harness turn summary, never a provider-request ledger. */
export interface ProjectedModelUsage {
  inputTokens?: number;
  outputTokens?: number;
  cacheCreationInputTokens?: number;
  cacheReadInputTokens?: number;
}

/**
 * Current model pairs represent a coarse Harness turn/query summary. They are
 * intentionally not typed as a provider request or generation.
 */
export interface ProjectedTurnModelSummary {
  provider?: string;
  requestedModel?: string;
  usage?: ProjectedModelUsage;
  totalCostUsd?: number;
}

export interface ProjectedSpan {
  spanId: string;
  parentSpanId?: string;
  sourceEventId: string;
  subpath: string;
  name: string;
  observationType: 'agent_turn' | 'turn_model_summary' | 'tool' | 'outcome_evaluation';
  startedAt: string;
  endedAt: string;
  /** unset marks incomplete tools or interrupted evaluators; roots/models require ok/error. */
  status: 'ok' | 'error' | 'unset';
  metadata: Record<string, ProjectedMetadataValue>;
  modelSummary?: ProjectedTurnModelSummary;
  io?: ProjectedIo;
}

/** One accepted primary-path client turn; content is opt-in under schema v2. */
export interface ProjectedTrace {
  schemaVersion: typeof PROJECTED_TRACE_SCHEMA_VERSION | typeof IO_PROJECTED_TRACE_SCHEMA_VERSION;
  traceId: string;
  workspaceId: string;
  sessionId: string;
  /** Accepted client event selected by exact `user_event_id` join. */
  anchorEventId: string;
  acceptanceEventId: string;
  userId?: string;
  root: ProjectedSpan;
  spans: ProjectedSpan[];
}

/**
 * Non-secret, Registry-authoritative target snapshot persisted beside one
 * canonical outbox trace. It is never selected from a Transcript event.
 */
export interface PinnedDeliveryContext {
  organizationId: string;
  bindingId: string;
  bindingVersion: number;
  adapterType: string;
  endpointKind: string;
  endpointClass: string;
  endpointUrl: string;
  semanticProfile: string;
  protocol: string;
  compression: string;
  timeoutMs: number;
  captureMode: string;
  sampleRate: number;
  configSchemaVersion: number;
  /** Registry snapshot labels, never refreshed or inferred during delivery. */
  agentId?: string;
  agentVersion?: number;
  harness?: string;
  harnessMode?: string;
  environment?: string;
  release?: string;
}
