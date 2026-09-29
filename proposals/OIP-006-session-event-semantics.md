# OIP-006: Session event acceptance and completion

- *Author(s)*: @jiangpengcheng, @freeznet
- *Status*: Released
- *Proposal time*: 2026-07-15
- *Components*: registry-service-ts, harness-server, transcript-store
- *Discussion*: None (predates the public repository)
- *Implementation*: harness-server `src/harness/` (`agent-harness.ts`, `claude/`, `in-sandbox/`,
  `codex-sdk/`), `src/runner/{dispatcher,session-runner}.ts`; registry-service-ts
  `src/domain/events.ts`, `src/api/sessions.routes.ts`, `src/events/session-events-index.ts`,
  `src/streaming/sse.ts`, `src/observability/session-dispatch.ts`, `src/migrate.ts`, migrations
  `0023_processed_marker_seq.sql` and `0052_session_events_unprocessed_client_user_index.sql`;
  `packages/agent-event-contract/src/kinds.ts`, `packages/transcript-store/src/types.ts`
- *Released in*: v0.5.0

## TL;DR

A client event's `processed_at` was written only after the harness's `submit()` returned, so a
running `user.message` looked queued until its turn ended, and nothing durable said a turn had
finished. A harness now awaits a durable `session.user_event_processed` marker before applying an
event, and a `session.user_event_completed` marker written with the turn's terminal status lets a
redelivery be skipped; Registry derives `processed_at` from the earliest marker and closes a slow
SSE stream for resumption. No shape changes; it affects `processed_at` readers and upgrade order.

## Background

[`event-processing-semantics.md`](../docs/managed-agents/event-processing-semantics.md) holds the
contract and [`sse-backpressure.md`](../docs/operation/sse-backpressure.md) the SSE policy.
`produced_at` is when Registry persisted an event; `processed_at` is a nullable RFC 3339 timestamp,
equal to `produced_at` on harness and Registry events. Event sources deliver client `user.*` events
at least once, in session order ([OIP-001](OIP-001-transcript-store-backends.md)); Registry projects
the transcript into `session_events_index`; harnesses sit behind the `AgentHarness` seam
([OIP-002](OIP-002-agent-harnesses-and-execution-modes.md)); status and span events, from
`session.status_running` to `session.status_idle` or `session.error`, report execution.

## Motivation

- **One timestamp for queue state and outcome.** The marker followed `await runner.submit()`, which
  spans the Claude model query, so a `user.message` stayed null until `session.status_idle`, replies
  to required actions and interrupts until they had run, and a failed append was only logged.
- **Order-dependent projection.** Markers were unconditional updates of indexed rows. Postgres and
  Pulsar projectors can run concurrently on several Registry replicas, so a marker projected before
  its event was lost and a later one could overwrite an earlier time.
- **No durable completion.** A redelivered event whose turn had finished (after a restart, an outbox
  replay or a lost offset commit) ran again; once Kafka turn work left the consumer callback, so one
  long turn could not stall other session topics, the offset commit needed the same signal.
- **Slow SSE readers.** Unbounded buffers grow with the slowest reader; skipped events leave gaps.

## Goals

### In scope

- `processed_at` null until a harness accepts the event, then the durable acceptance time, never
  reset, with nothing applied before that append; projection converging on the earliest marker.
- Durable completion that suppresses redelivery of finished turns and gates Kafka offset commits;
  deferred messages that survive restarts and are never overtaken; a companion `system.message`
  accepted with its event; bounded SSE memory without lost events; no shape changes.

### Out of scope

- Stamping queued events at append, a completion meaning or a public `completed_at`; exactly-once
  side effects before the terminal append; distributed fencing; mid-turn interrupt preemption; SSE
  correction frames; transcript user attribution ([OIP-012](OIP-012-agent-observability.md)).
- Shared SSE subscriptions and Pulsar seek cursors for resumed tails, listed on `roadmap.md` under
  [Remaining API performance work](../docs/managed-agents/roadmap.md#remaining-api-performance-work)
  and [Deferred by design](../docs/managed-agents/roadmap.md#deferred-by-design).

## Design

### High-level design

```text
POST /v1/sessions/{id}/events  append user.* event; processed_at null while queued
event source, per session      Kafka partition | Pulsar KeyShared lane | Postgres claim
dispatcher                     completed? skip, settle | drain older deferred | submit(e, hooks)
harness                        validate, select -> await onAccepted() -> apply
  onAccepted()                 append session.user_event_processed (+ companion system.message)
SessionRunner                  terminal status + session.user_event_completed
event source                   settle: Kafka commit | Pulsar ack | Postgres claim processed
Registry projector             processed_at = produced_at of the lowest-seq marker
SSE                            immutable frames, bounded buffer, drop, resume from Last-Event-ID
```

### Detailed design

**Acceptance hook.** `AgentHarness.submit(event, hooks)` receives `SubmitHooks.onAccepted()`,
awaited once after validation and selection and before anything is applied. The dispatcher's hook
is memoized per submission attempt, appends the markers in one append and turns a failure into the
retryable `UserEventAcceptanceError`, so the source is redelivered with no work started. Each
harness owns its point
([table](../docs/managed-agents/event-processing-semantics.md#event-specific-acceptance-points)):
Claude handles all six `user.*` kinds and defers a message while a required action is pending; the
colocated bridge accepts `user.message` and a matching `user.custom_tool_result`, emitting its
synthetic `session.status_running` and `span.model_request_start` after acceptance and before its
sandbox POST; the Codex SDK and Pi SDK harness accepts messages, tool confirmations, custom tool
results and interrupts, recording a turn's sources in its Registry turn receipt. An inapplicable
event throws `UnappliedUserEventError` first, and the dispatcher appends an `unapplied_event` error.

**Marker, projection and companions.** `session.user_event_processed {user_event_id}` is the source
of truth; its `produced_at` is the event's `processed_at`. `applyUserEventProcessedMarkers` gathers
targets from markers and from newly projected client `user.*` events, so either arrival order
converges, and applies per target the marker with the lowest `(seq, projection_ordinal, event_id)`,
to the event and to thread projections naming it, only while `processed_marker_seq` is null or
higher. The legacy `session.deferred_user_message_submitted` still counts, but the earlier
acceptance marker wins; duplicates are harmless. A Claude-dialect request may end with one
`system.message` after a `user.message`, `user.tool_result` or `user.custom_tool_result`; Registry
stores its ID on that event (`_orca_companion_system_event_id`, stripped from public views), the
dispatcher reads exactly that event, one append accepts both, and a new runner rebuilds system
context from accepted ones.

**Deferred messages.** A deferred `user.message` is recorded as `session.deferred_user_message`
before it joins the in-memory queue, and its source settles with `processed_at` still null. When the
required action clears, the drain selects it by ID, runs the ordinary hook and then appends
`session.deferred_user_message_submitted`. Failed acceptance retries in place (three times, a second
apart, by default), then leaves the durable record for the next runner activation; a newer message
fails retryably (`DeferredDrainBlockedError`) rather than overtake a blocked older one.

**Completion.** The dispatcher tracks accepted turn-driving sources (`user.message`,
`user.define_outcome`) per session in FIFO order. When `SessionRunner` persists a terminal
`session.status_idle` or `session.status_terminated`, one append carries the terminal and
`session.user_event_completed` for the oldest source. A `requires_action` pause counts unless the
harness marks it `pending` (settling delivery without completing), and for sources a harness names
on the terminal (`completionPolicy`, as the Codex SDK and Pi SDK receipts do), the terminal and then
each marker are appended. Marker IDs are UUIDv5 over workspace, session and source, so retries
deduplicate. The completed-ID cache changes only after the append and is rebuilt from the
transcript before a source runs; a completed source is skipped and settled, and a failed read is
retryable. A permanent preparation failure or an interrupt with no warm runner completes
without a runner: acceptance, `session.error` (`setup_failed`) when failing, `session.status_idle`
and the completion marker, one per append, with source-derived IDs so a retry fills only the gaps
([details](../docs/managed-agents/services/harness-server.md#execution-preparation-failures)).

**Settlement, ordering and interrupts.** Kafka pauses a turn-driving delivery's partition, runs the
turn outside `eachMessage` and commits the offset once the source settles, while the same
assignment owns the partition, backing off from 250 ms to 5 s and seeking back on failure. Pulsar
nacks retryable failures, exempt from its five-redelivery poison cap, and Pulsar and Postgres rerun
a `SessionEventBarrierError` step before the session advances
([failures](../docs/managed-agents/event-processing-semantics.md#failure-and-recovery-semantics)).
The hook is not a lock; each backend serializes a session: one Kafka partition per session topic, a
Pulsar `KeyShared` subscription keyed by session ID with in-order per-session lanes, and Postgres
claims of a session's earliest unprocessed event under a renewed lease, settled only by the owner.
A stalled handler is not fenced, so side effects before the terminal append are at least once.
Because the dispatcher awaits a turn-driving `submit()`, an interrupt reaches the harness once the
turn ends or parks at `requires_action`; the colocated bridge rejects interrupts as unapplied.

**SSE delivery.** Session and thread streams tail the transcript (`src/streaming/sse.ts`); a frame's
`id:` is its transcript sequence, and a stream starts inclusively from `Last-Event-ID` or
`from_cursor`, so a resumed stream repeats the frame at its cursor and clients deduplicate by `id`.
A full buffer (`SSE_BUFFER_SIZE`) waits for the socket to drain until its oldest event is
`SSE_DROP_AGE_MS` old, then Registry counts a drop, aborts the tail and closes; `orca-beta` streams
first get `event: drop` with `{"reason":"client-too-slow","last_seq":...}` naming the last frame the
socket accepted, while default streams close without it, keeping Claude's event union. Streams get
a `:heartbeat` comment every `SSE_HEARTBEAT_MS`, and `session.deleted` drains the buffer before
closing. Frames are immutable and markers are never streamed
([views](../docs/managed-agents/event-processing-semantics.md#sse-and-history-views)).

## Changes by component

- **registry-service-ts**: null stamping and marker classification (`src/domain/events.ts`),
  companion correlation, the projector, the SSE bridge, the backlog gauges and the concurrent index.
- **harness-server**: `SubmitHooks`, `completionPolicy`, `UnappliedUserEventError`, each harness's
  acceptance point, and the dispatcher's hook, deferred queue, completed-ID cache and settlement.
- **Libraries**: `@orca/agent-event-contract` names the markers; `@orca/transcript-store` defines
  `RetryableSessionEventError` (never poison) and `SessionEventBarrierError`. **Helm chart**:
  `registry.sse.*`. The observability-exporter opens a turn at the acceptance marker (OIP-012).

## Public-facing changes

### API

No shape changes. The `POST /v1/sessions/{id}/events` response is an immutable snapshot:
`user.message`, `user.interrupt`, `user.tool_confirmation`, `user.custom_tool_result`,
`user.tool_result` and `system.message` carry `null`, other client events their append time
(`user.define_outcome` because the vendored schema requires a value), and no client-supplied value
survives; the event list shows acceptance after projector lag. The vendored schema describes
`user.message.processed_at` as when the agent finished processing it; Orca's value is earlier, and
completion is read from `session.status_idle`, `session.status_terminated` and `session.error`. An
unapplied event stays `null` and is followed by `session.error` (`error.type: unapplied_event`,
`retry_status.will_retry: false`, `user_event_kind`).

### Events and streaming

Four internal kinds never reach public views: `session.user_event_processed`,
`session.user_event_completed`, `session.deferred_user_message` and
`session.deferred_user_message_submitted`, each carrying only `user_event_id`. An applied
`user.message`'s marker precedes its `session.status_running`, and `orca-beta` streams can end with
`event: drop`. Wire protocols are unchanged: colocated acceptance is host-side.

### Storage

`0023_processed_marker_seq.sql` adds the nullable `session_events_index.processed_marker_seq` and
the partial index `session_events_index_user_event_marker_idx` over marker rows.
`0052_session_events_unprocessed_client_user_index.sql` is a checkpoint after which each migration
run builds `session_events_index_unprocessed_client_user_idx` with `CREATE INDEX CONCURRENTLY`,
replacing an invalid leftover. The column dates from `0019_session_event_processed_at.sql`. The
Postgres backend's `transcript_event_claims.processed_at` is delivery bookkeeping, never joined.

### Configuration

Read by Registry; the engine chart sets them from `registry.sse.*`.

| Variable | Default | Effect |
| --- | --- | --- |
| `SSE_BUFFER_SIZE` | `256` | events buffered per SSE connection |
| `SSE_DROP_AGE_MS` | `5000` | oldest-event age at which a full buffer drops the stream |
| `SSE_HEARTBEAT_MS` | `15000` | interval between `:heartbeat` comments |

### Metrics, logs and traces

Registry refreshes `registry_service_session_events_unprocessed_client_user` and
`registry_service_session_events_oldest_unprocessed_client_user_age_seconds` (by `workspace_id`)
from index metadata every five seconds; SSE has `registry_service_sse_connections_active`,
`registry_service_sse_buffer_depth` and `registry_service_sse_drop_total{reason}`. harness-server:
`harness_accepted_event_to_status_running_seconds`, from `produced_at` to `session.status_running`.

## Compatibility

### Upgrade

No backfill and no transcript rewrite: old completion-time markers keep their values, and existing
rows start with a null `processed_marker_seq` and reconcile when their event or marker is projected
again. `0023` builds its index with a plain `CREATE INDEX` in the migration transaction, blocking
writes to `session_events_index` for the build, so a large table wants a low-write window.

### Rollback

Roll back harness-server first, then Registry. The column and indexes are additive and ignored by
older code; an older harness goes back to completion-time markers and writes no completion markers.

### Version skew

Apply migrations and replace every Registry projector replica before harness-server. An older
projector applies markers last-write-wins, so a later legacy marker can overwrite an acceptance
time, and a Registry that does not know `session.user_event_completed` projects it as public
([rollout](../docs/managed-agents/event-processing-semantics.md#compatibility-and-rollout)).

## Security considerations

- Registry refuses client events typed as an internal marker kind or prefixed `harness.` and
  replaces client-supplied `processed_at`; markers carry only event IDs and are never streamed.
- Backlog gauges read aggregate metadata, never payloads; per-connection buffers bound SSE memory.

## Testing

- harness-server: `test/unit/dispatcher-event-source.spec.ts` (acceptance failure starts nothing,
  acceptance never suppresses redelivery, completion-based skips across restart and idle eviction,
  failed terminal appends, deferred ordering, retries and shutdown),
  `test/unit/{claude-harness-protocol,codex-sdk-harness,codex-turn-recovery}.spec.ts`,
  `test/integration/in-sandbox-bridge.spec.ts`, and on real Kafka
  `test/integration/{dispatcher-real-kafka-discovery,dispatcher-preparation-failure-kafka}.spec.ts`.
- registry-service-ts: `test/unit/session-events-index.spec.ts`, `test/integration/events.spec.ts`
  (marker before event, earliest of duplicates, a null POST snapshot beside an updated list),
  `test/unit/sse-buffer.spec.ts` (grace without loss, drop aborts the tail, Claude union,
  `session.deleted` drain), `test/unit/event-mapping.spec.ts` and the dispatch-observability specs.
- transcript-store: `test/integration/{postgres,pulsar}-store.spec.ts` (repair before the session
  advances; a lost claim cannot settle). No suite restarts harness-server mid-turn; that coverage is
  listed under [Production hardening](../docs/managed-agents/roadmap.md#production-hardening).

## Alternatives

- **Stamping at append** hides the queue. **Keeping the completion-time marker** makes running work
  look queued and repeats what status events say. **Marking on broker read or claim** would call a
  message deferred behind a required action processed. **Acceptance on the sandbox's receipt** in
  colocated mode needs a two-phase remote protocol. **A public `completed_at`** is not in the
  Anthropic schema. **An SSE correction frame** would re-send an event ID a client has already seen.
- **Suppressing redelivery on the acceptance marker** would lose an event whose process died
  mid-turn, so completion has its own marker, written with the terminal.
- **A durable-execution engine (an earlier design, withdrawn)** used Postgres execution-control rows
  with epoch compare-and-set (a claim fenced the previous owner, a commit recorded a turn-aligned
  cursor), stamped every event with its writer's epoch, drove the harness as an executor over the
  journal and committed the Kafka offset after each turn commit. It added a second ownership
  authority beside every backend's delivery state yet still re-ran whole turns on recovery, so tools
  had to be idempotent; it was withdrawn before its harness integration landed.
- **Unbounded SSE buffering or skipping events** ties Registry memory to the slowest reader or loses
  events silently; the durable transcript makes a reconnect from a cursor cheap.

## Status notes

- **Completion came later.** The acceptance design suppressed nothing; the completion marker arrived
  when Kafka turn work left the consumer callback and offsets began to commit after the terminal.
- **Not always one append.** The marker was specified to share one append with its terminal, as the
  ordinary runner terminal does. Work that never starts and harness-named sources write one event
  per append, the terminal before the marker, because a multi-event broker append can commit a
  later message after an earlier one fails; stable IDs let a retry fill only the gaps.
- **`user.define_outcome`** was designed as harness-accepted; Registry now stamps its append time,
  which the vendored schema requires, and the harness still records acceptance before applying it.
- **Ordering across replicas.** The design promised per-session order only for Kafka and for
  single-replica Postgres or Pulsar sources; the Pulsar source later moved to `KeyShared` and the
  Postgres source to earliest-unprocessed-per-session claims, so it holds across replicas, unfenced.
- **SSE.** `event: drop` was designed for every stream; default streams now close without it. The
  first bridge also discarded an event arriving while the buffer was full but younger than the drop
  age, and named the newest buffered sequence in `last_seq`; both could open a gap on resume and
  gave way to the wait-then-drop rule and the last written sequence.
- **Runner-driven sessions.** The contract covers sessions whose execution owner is harness-server.
  Sessions Registry drives through its runner event bridge
  ([OIP-011](OIP-011-self-hosted-session-runner.md)) mark a turn with `agent.turn_completed` and
  append no acceptance marker, so their client events keep `processed_at: null` and count in the
  unprocessed-event gauges; the bridge's partial-turn trade-off is listed under
  [Designed, not built](../docs/managed-agents/roadmap.md#designed-not-built).
