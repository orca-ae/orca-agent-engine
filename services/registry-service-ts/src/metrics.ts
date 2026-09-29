// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry, prefix: 'registry_service_' });

export const requestTotal = new Counter({
  name: 'registry_service_request_total',
  help: 'Total requests handled',
  labelNames: ['method', 'route', 'status'] as const,
  registers: [registry],
});

export const legacyApiKeyFallbackTotal = new Counter({
  name: 'registry_service_legacy_api_key_fallback_total',
  help: 'Legacy API-key fingerprint fallback outcomes.',
  labelNames: ['result'] as const,
  registers: [registry],
});

/**
 * Workspace-plane OIDC tokens refused by organization-audience resolution.
 *
 * Every refusal is the same bare 401 to the caller, so without this the faults
 * the mode can hit — an organization with no audience, an organization that
 * grew a second active workspace, a token minted for another relying party —
 * are one indistinguishable symptom. `reason` is a closed set; see
 * `WorkspaceResolutionRejectionReason` in `auth/oidc.ts`. No audience value and
 * no token material is ever a label.
 */
export const oidcWorkspaceResolutionRejectedTotal = new Counter({
  name: 'registry_service_oidc_workspace_resolution_rejected_total',
  help: 'Workspace-plane OIDC tokens refused by organization-audience resolution, by reason.',
  labelNames: ['reason'] as const,
  registers: [registry],
});

export const eventsAppendTotal = new Counter({
  name: 'registry_service_events_append_total',
  help: 'Events accepted via POST /v1/sessions/:id/events',
  labelNames: ['workspace_id', 'status'] as const,
  registers: [registry],
});

export const eventsReadTotal = new Counter({
  name: 'registry_service_events_read_total',
  help: 'Events served via GET /v1/sessions/:id/events',
  labelNames: ['workspace_id'] as const,
  registers: [registry],
});

export const unprocessedClientUserEvents = new Gauge({
  name: 'registry_service_session_events_unprocessed_client_user',
  help: 'Unprocessed public client user events that await execution, by workspace.',
  labelNames: ['workspace_id'] as const,
  registers: [registry],
});

export const oldestUnprocessedClientUserEventAgeSeconds = new Gauge({
  name: 'registry_service_session_events_oldest_unprocessed_client_user_age_seconds',
  help: 'Age in seconds of oldest unprocessed public client user event, by workspace.',
  labelNames: ['workspace_id'] as const,
  registers: [registry],
});

export const sessionLifecycleOutboxPending = new Gauge({
  name: 'registry_service_session_lifecycle_outbox_pending',
  help: 'Unpublished session lifecycle outbox rows.',
  registers: [registry],
});

export const sessionLifecycleOutboxOldestPendingAgeSeconds = new Gauge({
  name: 'registry_service_session_lifecycle_outbox_oldest_pending_age_seconds',
  help: 'Age in seconds of oldest unpublished session lifecycle outbox row.',
  registers: [registry],
});

export const sessionLifecycleOutboxPendingAttempts = new Gauge({
  name: 'registry_service_session_lifecycle_outbox_pending_attempts',
  help: 'Sum of attempt_count across unpublished session lifecycle outbox rows.',
  registers: [registry],
});

export const sessionDispatchObservabilityRefreshTotal = new Counter({
  name: 'registry_service_session_dispatch_observability_refresh_total',
  help: 'Session dispatch observability refreshes by outcome.',
  labelNames: ['result'] as const,
  registers: [registry],
});

export const sessionDispatchObservabilityLastSuccessTimestampSeconds = new Gauge({
  name: 'registry_service_session_dispatch_observability_last_success_timestamp_seconds',
  help: 'Unix timestamp of the last successful session dispatch observability refresh.',
  registers: [registry],
});

export const sseConnectionsActive = new Gauge({
  name: 'registry_service_sse_connections_active',
  help: 'Currently open SSE connections',
  registers: [registry],
});

export const sseBufferDepth = new Histogram({
  name: 'registry_service_sse_buffer_depth',
  help: 'Per-flush SSE buffer occupancy',
  buckets: [0, 1, 4, 16, 64, 128, 192, 256],
  registers: [registry],
});

export const sseDropTotal = new Counter({
  name: 'registry_service_sse_drop_total',
  help: 'SSE connections dropped',
  labelNames: ['reason'] as const,
  registers: [registry],
});

// Git credential-helper observability.

export const registryGitCredsRequestTotal = new Counter({
  name: 'registry_git_creds_request_total',
  help: 'POST /v1/git-creds invocations. Labels: result=ok|jwt_invalid|credential_mismatch|pat_unresolvable|repo_unmatched.',
  labelNames: ['result'] as const,
  registers: [registry],
});

export const triggerPlannerTotal = new Counter({
  name: 'registry_trigger_planner_total',
  help: 'Cron Trigger planner outcomes.',
  labelNames: ['result'] as const,
  registers: [registry],
});

export const triggerDispatcherTotal = new Counter({
  name: 'registry_trigger_dispatcher_total',
  help: 'Cron Trigger dispatcher outcomes.',
  labelNames: ['result'] as const,
  registers: [registry],
});

export const triggerDueBacklog = new Gauge({
  name: 'registry_trigger_due_backlog',
  help: 'Number of active cron Triggers due when the latest planner tick began.',
  registers: [registry],
});

export const triggerOldestDueAgeSeconds = new Gauge({
  name: 'registry_trigger_oldest_due_age_seconds',
  help: 'Age in seconds of the oldest cron Trigger seen by the latest planner tick.',
  registers: [registry],
});

// Owner-pod session event bridge: user turns the single-writer bridge drove to
// the runner. `source=catchup` counts turns the bridge picked up at start from
// the transcript (a pre-connect / un-driven first turn, or an un-completed turn
// re-driven on reconnect); `source=live` counts turns the from-boundary tail
// delivered while the bridge was already running. Lets ops see catch-up drives
// distinctly from steady-state ones.
export const bridgeTurnsDrivenTotal = new Counter({
  name: 'registry_service_bridge_turns_driven_total',
  help: 'User turns the owner-pod event bridge drove to the runner. Labels: source=catchup|live.',
  labelNames: ['source'] as const,
  registers: [registry],
});

// Owner-pod event bridge: tool-confirmation verdicts the bridge pushed to the
// runner's confirmation route (NOT the turn route). A `user.tool_confirmation` is
// the client's verdict for an in-flight gated tool call within the CURRENT turn —
// not a new turn — so the bridge forwards it to the runner's parked approval
// instead of driving it as a turn. `source=catchup|live` mirrors the turn metric
// (a confirmation appended before the runner connected is replayed on catch-up);
// `result=delivered|undelivered` distinguishes a 2xx-acked push from a contained
// transport failure (offline / non-2xx / mid-push drop) that self-heals because the
// verdict's durable source of truth is the transcript and recovery re-serves it.
export const bridgeConfirmationsPushedTotal = new Counter({
  name: 'registry_service_bridge_confirmations_pushed_total',
  help: 'Tool-confirmation verdicts the owner-pod event bridge pushed to the runner. Labels: source=catchup|live, result=delivered|undelivered.',
  labelNames: ['source', 'result'] as const,
  registers: [registry],
});

// Owner-pod event bridge: user.interrupt control signals the bridge pushed to the
// runner's interrupt route (NOT the turn route). A `user.interrupt` is an
// out-of-band signal that PREEMPTS the turn currently in flight — not a new turn —
// so the bridge forwards it to the runner's interrupt route concurrently with the
// turn driver (so it reaches a turn blocked on the model). `source=catchup|live`
// mirrors the confirmation metric (an interrupt appended before the runner connected
// is re-pushed on catch-up only while still un-answered); `result=delivered|undelivered`
// distinguishes a 2xx-acked push from a contained transport failure (offline /
// non-2xx / mid-push drop) that self-heals because the interrupt's durable source of
// truth is the transcript and recovery re-serves it.
export const bridgeInterruptsPushedTotal = new Counter({
  name: 'registry_service_bridge_interrupts_pushed_total',
  help: 'user.interrupt control signals the owner-pod event bridge pushed to the runner. Labels: source=catchup|live, result=delivered|undelivered.',
  labelNames: ['source', 'result'] as const,
  registers: [registry],
});

// Owner-pod reverse-lookup session recovery: transcript events the owner pushed
// to a (re)connecting runner as a resume replay. `mode=fresh` counts events pushed
// in a full replay to a runner that presented an empty cursor (rebuild-from-start);
// `mode=resume` counts events pushed in an `after={cursor}` slice to a runner
// resuming from a known cursor. Lets ops see fresh rebuilds distinctly from
// incremental resumes (and the replay volume each drives).
export const sessionRecoveryReplayedTotal = new Counter({
  name: 'registry_service_session_recovery_replayed_total',
  help: 'Transcript events the owner pod pushed to a (re)connecting runner as a resume replay. Labels: mode=fresh|resume.',
  labelNames: ['mode'] as const,
  registers: [registry],
});

// Owner-pod snapshot delivery: the credential-free agent snapshot the owner pod
// pushed to a (re)connecting runner at session start (before recovery + the
// bridge). `result=delivered` counts a 2xx-acked push; `result=undelivered`
// counts a contained transport failure (offline / non-2xx / mid-push drop) that
// self-heals on the next reconnect. A push that was skipped (no snapshot for the
// session) is not counted here.
export const snapshotDeliveredTotal = new Counter({
  name: 'registry_service_snapshot_delivered_total',
  help: 'Agent snapshots the owner pod pushed to a (re)connecting runner. Labels: result=delivered|undelivered.',
  labelNames: ['result'] as const,
  registers: [registry],
});

// Skill-bundle pushes the owner pod delivered to a (re)connecting runner BEFORE the
// snapshot. A push that was skipped (the session has no Skills) is not counted here.
export const skillsDeliveredTotal = new Counter({
  name: 'registry_service_skills_delivered_total',
  help: 'Skill bundle pushes the owner pod delivered to a (re)connecting runner. Labels: result=delivered|undelivered.',
  labelNames: ['result'] as const,
  registers: [registry],
});
