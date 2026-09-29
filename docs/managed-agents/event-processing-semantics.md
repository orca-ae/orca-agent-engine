# Session Event Processing Semantics

> **Status:** Implemented. This document defines the meaning and ordering of
> `processed_at` for client events.

## Decision

For a client event, `processed_at` means **the harness accepted the event from
its session queue and is about to apply it**. It does not mean that the model
turn, tool execution, or session completed successfully.

`produced_at` remains the time at which the registry accepted and persisted the
event. `processed_at: null` means that the event has not been accepted for
application: it may still be queued behind earlier work, deferred behind a
required action, or rejected by harness-side state validation. Once a durable
acceptance marker exists, later model failures and harness restarts do not reset
the value.

Anthropic's Managed Agents documentation says that a null `processed_at` means
the event is queued by the harness until preceding work finishes, and
Anthropic's published OpenAPI spec describes `user.message.processed_at` as the
"Timestamp when the agent finished processing this message". Orca uses the
acceptance boundary above instead, because it is the first durable transition
after dequeue and before execution; using turn completion made a long-running
event appear queued while it was already executing. See Anthropic's
[Session event stream](https://platform.claude.com/docs/en/managed-agents/events-and-streaming).

## Why

The previous implementation appended the processing marker after
`await runner.submit(...)`. For the Claude harness, that await includes the
model query, so a normal `user.message` remained null until after
`span.model_request_end` and `session.status_idle`. Tool confirmations and
custom-tool results were likewise marked only after the resumed query paused or
completed, and interrupts were marked after the abort.

That conflated queue state with execution outcome. The public event stream
already has explicit execution signals:

- `session.status_running` and `span.model_request_start` show work began.
- `span.model_request_end` reports model completion and error state.
- `session.status_idle`, `session.status_terminated`, and `session.error`
  describe the turn boundary and outcome.

The current model start/end pair is explicitly labeled
`model_observation_kind: turn_model_summary`. It summarizes one Harness
query/turn and must not be interpreted as one provider request, SDK retry, or
Gateway fallback attempt. Its end references the start envelope ID through
`model_request_start_id`; the start payload has no second identity.

No public `completed_at` field is introduced. Clients that need completion must
use those events.

## State model

```text
                       preceding session work
                                │
                                ▼
client event persisted ──► QUEUED/DEFERRED ──► ACCEPTED ──► EXECUTING
 produced_at = t0          processed_at = null   processed_at = t1
                                                     │
                                                     ├─ session.status_running
                                                     ├─ span.model_request_start
                                                     ├─ span.model_request_end
                                                     └─ session.status_idle/error
```

`ACCEPTED` is not a success or exactly-once state. A process can die after
acceptance and before the event-source handler completes; broker redelivery may
then execute the event again. The timestamp still correctly records that the
event first left the harness queue.

Turn completion is a separate durable boundary. A source event is eligible for
redelivery suppression only after its terminal session status and a completion
marker are both durable; `processed_at` never supplies that decision.

## Durable acceptance marker

The internal transcript event is:

```json
{
  "type": "session.user_event_processed",
  "user_event_id": "evt_…"
}
```

Its `produced_at` is the public event's derived `processed_at`. The marker is
the source of truth; `session_events_index.processed_at` is a read-model field,
not an independently written lifecycle record.

When a request ends with a `system.message`, registry writes an internal
correlation from the immediately preceding `user.message`,
`user.tool_result`, or `user.custom_tool_result` to the generated system-event
ID. The correlation is not serialized by POST, list, or stream responses.
Dispatcher follows that exact ID rather than assuming the two events have
adjacent sequence values: concurrent Postgres inserts can interleave sequence
numbers, and broker implementations may split large batches.

Acceptance of the turn-driving event appends a marker for both the user event
and its correlated `system.message` in one transcript-store batch. Both public
events therefore move from `processed_at: null` at the same acceptance
boundary. An accepted system message applies to the current turn and every
later turn. Warm runners retain it in their system context; a newly spawned
runner rebuilds accepted system context from the transcript. For a tool-result
continuation, where the Claude SDK cannot replace the system prompt of an
already-running query, the harness injects the instruction into that tool
result as a system-reminder block and also retains it for later prompts.

The projector obeys these rules:

1. The marker with the lowest transcript `seq` for a user event wins. Projector
   execution order is not authoritative because Postgres and Pulsar projectors
   can run concurrently across replicas.
2. Projection reconciles in both directions. A marker updates an existing
   target, and a client event projected later looks for an already-indexed
   marker. This makes marker-before-target projection converge correctly.
3. `session.deferred_user_message_submitted` remains a legacy fallback marker
   during rollout. New code always emits `session.user_event_processed` before
   starting a drained deferred message; the earlier transcript marker wins and
   the later queue-completion record cannot overwrite it.
4. Duplicate acceptance markers are harmless. Correctness does not depend on
   the transcript store's process-local LRU deduplication.

The read model stores the winning sequence in nullable
`session_events_index.processed_marker_seq`. Reconciliation updates
`processed_at` only when the candidate marker sequence is lower than the stored
one. This monotonic compare-and-set prevents a concurrent projector with a
stale snapshot from overwriting an earlier marker. Migration 0023 adds the
internal column; existing rows begin with a null marker sequence and reconcile
when their target or marker is projected again.

## Durable turn-completion marker

The dispatcher records accepted turn-driving source user-event IDs FIFO for
each live session. (`user.message` and `user.define_outcome` start turns;
required-action reply events resume an existing turn.) When `SessionRunner`
persists a terminal `session.status_idle` or `session.status_terminated`, it
appends an internal companion marker for the oldest source ID in that FIFO or,
when the harness names the sources a terminal completes (`CodexSdkHarness`, which
runs the Codex and Pi SDKs, does), for each named source:

```json
{
  "type": "session.user_event_completed",
  "user_event_id": "evt_…"
}
```

Without named sources, the terminal status and its companion marker are one
`TranscriptStore.append` batch. With named sources, each event is appended
separately, terminal status first, so a completion marker never becomes durable
before its terminal. The dispatcher's outcome for work that never starts (for
example a rejected execution preparation) is appended the same way, one event at
a time: acceptance marker, any error, idle status, then completion marker.
Marker event IDs and idempotency keys are deterministic from workspace,
session, and source event ID, so an append retry deduplicates the marker. The
dispatcher removes IDs from its active FIFO and updates its completed-ID cache
only after those appends succeed. A terminal append failure leaves the source
uncompleted and retryable.

Before executing any source event with an ID, dispatcher lazily rebuilds that
session's completed-ID cache by reading these markers from the transcript. A
matching marker skips `runner.submit` and lets the source delivery commit. A
transcript-read failure is retryable and never treated as completion. This
covers redelivery after a completed turn, including outbox replay and process
restart; it does not infer completion from `session.user_event_processed`.

Completion markers are internal transcript events. Registry filters them from
SSE, public event listing, and public event-index projections.

## Required ordering

For each executable `user.*` event:

1. Registry persists the client event with `processed_at: null`.
2. Reading or claiming the broker message alone does not mark it processed.
   Events deferred behind a required action remain null.
3. The selected harness validates the event and determines that it can be
   applied now.
4. The harness calls and awaits `SubmitHooks.onAccepted()`. The dispatcher
   appends `session.user_event_processed` for the source event id and, when
   present, its explicitly correlated `system.message` id.
5. Only after the append succeeds may the harness mutate agent state, resume a
   blocked promise, abort a query, emit `session.status_running`, or start
   model/tool work.
6. The source handler retains its existing completion boundary. Moving the
   public acceptance marker does not redefine Kafka commits, Pulsar ACKs, or
   Postgres claim completion.

For `mode: colocated`, acceptance is owned by the host-side orchestrator. It
awaits the marker before emitting the synthetic running/model-start events and
before POSTing the event to the sandbox harness. Defining acceptance as the
remote server's receipt would require a separate two-phase remote protocol and
is not the v1 contract. The current sandbox wire supports `user.message` and a
matching `user.custom_tool_result`; other controls are rejected as unapplied
before the hook instead of being rewritten into a message and falsely marked
processed.

### Event-specific acceptance points

| Client event              | Validate/select before the hook                                                 | Apply only after the hook                            |
| ------------------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `user.message`            | supported non-empty content; no unresolved required action; selected from queue | conversation mutation, running events, `runQuery()`  |
| `user.tool_confirmation`  | a matching pending confirmation exists                                          | remove the pending gate and resolve its promise      |
| `user.custom_tool_result` | a matching pending custom-tool call exists                                      | remove the pending gate and resolve its promise      |
| `user.tool_result`        | a matching pending self-hosted agent-tool call exists                           | remove the pending gate and resolve its promise      |
| `user.interrupt`          | target runner/thread is valid                                                   | invoke abort if a query is still active              |
| `user.define_outcome`     | rubric/description are valid and the event can start a turn                     | register the criterion and run the evaluation prompt |

A companion `system.message` is validated and accepted with the preceding
message/tool-result event; it is never independently dispatched.

Validation and application must remain separable. In particular, resolving a
confirmation/custom-result promise before the hook can let a blocked query
resume while the marker append is still in flight.

## Harness interface

```ts
interface SubmitHooks {
  // Called exactly once after validation/selection and before application.
  onAccepted(): Promise<void>;
}

interface AgentHarness {
  submit(event: UserEvent, hooks?: SubmitHooks): Promise<UserEventSubmitResult | void>;
}
```

`SessionRunner` passes the hook through unchanged. The dispatcher memoizes the
hook promise within one submission attempt so an implementation accidentally
calling it twice cannot append two markers in that attempt. Each harness owns
the exact acceptance point because only it knows when validation is complete
and whether it will return `deferred`.

## Deferred messages

A `user.message` received while the runner is waiting for a required action is
handled in two durable stages:

1. Dispatcher appends `session.deferred_user_message` and returns from the
   original event-source handler. The append must succeed before the item is
   added to the in-memory queue. The broker message may then be committed or
   ACKed, but public `processed_at` remains null.
2. When the required action clears, the deferred queue selects the message by
   its original event id. The harness calls the normal acceptance hook before
   starting its turn. After `submit()` returns, dispatcher appends
   `session.deferred_user_message_submitted` to record removal from the durable
   deferred queue.

If acceptance-marker persistence fails during drain, no turn starts. The
dispatcher retries marker persistence with a delay inside the current
serialized source delivery (three retries by default), covering transient
failures without requiring a new client event. If those retries are exhausted,
it requeues the item and returns from the source handler so a single
session-scoped failure cannot block all Pulsar deliveries on the replica
indefinitely. The durable deferred record remains eligible for reconstruction
on the next runner activation; recovery never depends on redelivery of the
already-ACKed original broker message. Before accepting a later `user.message`,
the dispatcher drains any older deferred messages first; if the older message
is still blocked after its bounded retries, the later source event remains
retryable rather than overtaking it.

## Failure and recovery semantics

| Failure point                                                                          | Public state                                                 | Recovery behavior                                                                                                                                                                                                                                |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Direct event: before acceptance marker is durable                                      | `processed_at: null`                                         | Hook failure escapes the dispatcher; Kafka does not commit, Pulsar nacks (the retryable infrastructure error bypasses its poison-message cap), and a Postgres claim remains retryable. No agent work has started.                                |
| Deferred event: before acceptance marker is durable                                    | `processed_at: null`                                         | No agent work starts; the current drain retries a bounded number of times, then returns and leaves the durable deferred record for queue reconstruction on the next runner activation.                                                           |
| Handled model/tool failure after acceptance                                            | non-null, plus `session.error` / terminal span or idle event | Dispatcher records the handled outcome and returns normally; this is not automatically redelivered.                                                                                                                                              |
| Process death or lost ownership after acceptance but before terminal completion append | non-null                                                     | The backend may redeliver. The acceptance marker does not suppress dispatch, so execution remains at-least-once.                                                                                                                                 |
| Process death after terminal status plus completion-marker append                      | non-null                                                     | Dispatcher rebuilds or updates its completed-ID cache from the marker and skips the completed source event on redelivery.                                                                                                                        |
| Terminal status / completion-marker append failure                                     | non-null                                                     | No completion cache mutation or Kafka commit occurs. Dispatcher retires the failed event pump; Kafka keeps the partition paused through bounded backoff, seeks only if the same assignment still owns it, and redelivery creates a fresh runner. |
| Duplicate or out-of-order marker projection                                            | earliest marker timestamp                                    | The projector derives from the lowest marker `seq` and converges regardless of projection order.                                                                                                                                                 |
| State-invalid confirmation/custom result                                               | null, plus `session.error{type: unapplied_event}`            | The permanent mismatch is ACKed to avoid poison-message loops; it was handled but never accepted for application.                                                                                                                                |
| Validation-invalid user event (for example, empty/unsupported `user.message`)          | null, plus `session.error{type: unapplied_event}`            | The permanent client input error is rejected before acceptance and ACKed; no turn starts.                                                                                                                                                        |

This contract suppresses redelivery only after durable terminal completion. It
does not provide exactly-once model execution or external tool side effects
before that append.

### Public `processed_at` versus delivery bookkeeping

`transcript_event_claims.processed_at` in the Postgres transcript backend is an
internal event-source ACK/handler-completion field. It is unrelated to the
public session-event `processed_at` described here. The two fields share a
historical name but have different owners and must never be joined or copied.

### Private OIDC user attribution

For an OIDC-authenticated public Session create or event append, Registry stamps
an opaque issuer-qualified identifier derived from the verified OIDC `(iss, sub)`
pair on the private Transcript `Event.userId` envelope for the client event it
creates. Domain-separated versioned hashing keeps raw issuer and subject values
out of the Transcript; Registry retains the raw standard `sub` only for existing
public Memory `user_actor` attribution. It never derives the Transcript value
from event payloads, request headers, API-key principals, or API-key IDs.
Session initial-event outbox rows preserve the envelope value through reconcile
recovery.

This metadata is not copied into payload bytes, the public POST/GET/SSE event
views, or the session-event index. API-key requests, Trigger-generated events,
Harness events, and Registry lifecycle sentinels remain unattributed.

## SSE and history views

Transcript events are immutable. The POST response and an SSE frame can show a
new client event with `processed_at: null`; the later internal marker does not
rewrite or re-emit that already-sent frame, and internal marker events are not
publicly streamed.

`GET /v1/sessions/{id}/events` reads the registry projection and therefore
shows the derived non-null value after normal projector lag. Tests compare the
marker's transcript order with running/model events and then verify the list
projection; they do not expect an SSE correction frame.

## Ordering scope

The acceptance hook records when the chosen runner accepts an event; it does
not itself create distributed per-session mutual exclusion. Each event source
orders a session's events: Kafka's topic-per-session single partition provides
ordered ownership; the Postgres source claims only the earliest unprocessed
event of each session within a consumer group; and the Pulsar source uses a
`KeyShared` subscription keyed by session ID, so the broker keeps a session's
events on one consumer. Delivery remains at-least-once: a lease expiry or an
ownership change can redeliver an event that another replica already accepted,
and `processed_at` records the first acceptance, not an exactly-once queue
position.

## Compatibility and rollout

- The public schema is unchanged: `processed_at` remains a nullable RFC-3339
  timestamp.
- Migration 0023 adds nullable `processed_marker_seq` and a marker-correlation
  lookup index to the registry read model. Both are internal and are not
  serialized on the API.
- Migration 0023 creates the marker-correlation index with ordinary
  `CREATE INDEX` inside the Drizzle migration transaction. On a large
  `session_events_index` table, schedule this migration during a maintenance or
  low-write window and validate the expected lock duration before rollout. Do
  not run it under sustained projector write load. If the lock window is not
  acceptable for a deployment, replace this rollout step with a separately
  designed non-transactional concurrent-index migration and corresponding
  migration bookkeeping; `CONCURRENTLY` cannot be added directly to the
  existing transactional migration.
- Existing transcripts are not rewritten; their old completion-timed markers
  retain their historical value.
- Roll out in this order: apply the migrations; replace **all** registry
  projector replicas; then deploy the harness. Do not mix an old projector
  with a harness that emits acceptance markers: the old projector can overwrite
  the earlier acceptance time with a later legacy deferred-submitted time.
  Roll back in the reverse order (harness first, registry second). No backfill
  is required because the upgraded projector understands both the new marker
  and the legacy deferred-submitted marker.
- Harness images should use immutable digests or versioned tags during rollout.

`user.interrupt` does not currently provide mid-turn preemption. Event-source
delivery is serialized per session and the dispatcher awaits a turn-driving
`submit()`, so an interrupt normally reaches the harness after that submit has
returned. The acceptance barrier still precedes any abort call if an active
query exists.

## Verification

The implementation is complete when the following remain covered:

1. `user.message` acceptance is durable before `session.status_running`,
   `span.model_request_start`, and model invocation.
2. A second/deferred `user.message` remains null until selected, then receives
   its acceptance marker before its own turn starts.
3. Tool confirmation, custom-tool result, interrupt, and outcome-definition
   side effects cannot occur while `onAccepted()` is pending.
4. A forced marker-append failure starts no model/remote harness work and makes
   a direct source event retryable.
5. Repeated delivery does not use an acceptance marker to suppress at-least-once
   execution, while a terminal-batched completion marker suppresses completed
   source redelivery after cache rebuild.
6. Duplicate markers and marker-before-target projection converge on the
   lowest-sequence marker.
7. API tests distinguish the immutable POST snapshot from the eventually
   updated event-list projection; SSE visibility remains an immutable-frame
   contract rather than a correction stream.
8. Deferred acceptance retries are bounded and interruptible during shutdown;
   later user messages cannot overtake an older blocked deferred message.
9. The final custom-tool-result terminal wait is bounded. A silent sandbox
   mismatch times out and fails the runner instead of wedging the source
   handler or fabricating a second turn terminal.
10. Shutdown advances the dispatcher lifecycle generation before source join
    and runner cancellation. A handler crossing that fence rejects retryably;
    no shutdown-interrupted delivery is ACKed/committed, and late persistence
    callbacks cannot recreate completion or latency caches.

## Non-goals

- Setting `processed_at = produced_at` at registry append time.
- Treating `processed_at` as a successful-turn or completion timestamp.
- Adding a public completion field or changing Anthropic-compatible shapes.
- Providing exactly-once arbitrary model/tool side effects before terminal
  completion append, or distributed session fencing.
- Providing mid-turn interrupt preemption.
