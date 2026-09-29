# `@orca/agent-event-contract`

> Library: canonical vocabulary for agent-produced transcript events. It
> provides canonical event-ID and primary/subagent-path types, persisted producer
> kinds, and constrained lifecycle, model-summary, and acceptance-marker
> payload builders. Pure TypeScript — no I/O and no runtime dependencies.
> Source: `packages/agent-event-contract/`.

## Purpose

An agent event needs producer-supplied identity, a canonical producer path, and one shared
kind vocabulary before a consumer can correlate it without inferring meaning
from a harness-native payload or transcript position. This package owns those
small, transport-free primitives. Its package export is only
`@orca/agent-event-contract`; consumers do not import source subpaths.

`harness-server` consumes its producer vocabulary and strict lifecycle/model
payload builders; the Dispatcher uses its acceptance-marker builder. Registry
consumes the shared vocabulary and marker constant while its HTTP event type
remains open. Harness-server's service-local output envelope is also strict;
only producer-boundary draft inputs may omit ID/subpath before synchronous
completion and validation.

For a canonical producer event, `AgentEvent.id` is its unique canonical
identity. Payloads do not
define a second event identity; correlation fields, including required-action
`event_ids`, `model_request_start_id`, and `user_event_id`, reference envelope
IDs. Current model-start producers keep identity only on the envelope; the
payload no longer repeats an `id` field.

`AgentEvent` requires all four producer fields:

```ts
import { AgentEventKind, type AgentEvent } from '@orca/agent-event-contract';

const event: AgentEvent<typeof AgentEventKind.message, { content: string }> = {
  id: 'evt_message_1',
  subpath: '',
  kind: AgentEventKind.message,
  payload: { content: 'done' },
};
```

This exported contract and harness-server's local `AgentEvent` output are
strict. Harness-server's three producer boundaries — the separated Claude Agent
SDK harness, the in-sandbox harness, and the Codex/Pi SDK harness (`codex_sdk`
and `pi_sdk`, including the terminal events it rebuilds from a turn receipt) —
each complete a draft input through
`withCanonicalAgentEventEnvelope`, which stamps a canonical ID and validates an
explicit path before the event is exposed. Harness-server's own `SessionRunner`
preserves that envelope as Transcript `Event.id`/`subpath` and keeps native
payload fields byte-preserving. Public event conversion never treats payload IDs
or correlation fields as envelope identity.

The other producer in this tree, the `@orca/session-runner` service, does not
import this package. Its wire line carries an `id` only when the harness had a
stable one to offer — the registry mints an identity otherwise — and carries a
`subpath` only for a subagent thread, so the registry's bridge rather than the
producer supplies the canonical envelope. The runner re-declares the handful of
kind strings and subpath constants it speaks, pinned against this package's
source by `services/session-runner/test/unit/multiagent-thread-events.spec.ts`.
See [`../services/session-runner.md`](../services/session-runner.md).

`AgentEventId` is an `evt_`-prefixed string type, and `isAgentEventId` requires
a non-empty suffix. The library deliberately has no ID generator: persistence
owners create IDs and this contract validates them.

## Producer paths

**PRIMARY_AGENT_SUBPATH** is `''`. `subagentEventSubpath(threadId)` constructs
`subagents/<threadId>` and rejects empty or wildcard IDs. `isAgentEventSubpath`
accepts only these two canonical shapes.

`*` is a transcript read selector, not a producer path. Legacy `threads/...`
paths remain outside this contract.

## Kinds and payloads

**CANONICAL_AGENT_EVENT_KINDS** is the duplicate-free persisted producer list:

- agent content, agent-thread messaging, session lifecycle, session-thread
  lifecycle, and span events;
- excludes `agent.usage`, which is a runtime signal; and
- excludes `session.user_event_processed`, which is an internal transcript
  acceptance marker.

`sessionIdlePayload` encodes the Anthropic-compatible stop reasons `end_turn`,
`requires_action`, and `retries_exhausted`. An applied interrupt returns
`end_turn`, as a completed turn does; it does not add a public stop reason. The
`requires_action` form requires a non-empty `event_ids` tuple, so callers cannot
construct an empty required-action boundary through the typed API.

Current model events describe a coarse turn summary. Both model-summary builders
set `model_observation_kind: 'turn_model_summary'`; the end payload requires
its start envelope ID, `model_usage`, and error state. Both payloads preserve
optional trusted configured `provider` and requested `model` values when known.
The contract does not present this summary as a served model, logical
provider-call, retry, or fallback graph.

`userEventProcessedPayload` validates the exact accepted user-event ID before
constructing an internal marker payload.

## Tests

```bash
COVERAGE=1 pnpm -F @orca/agent-event-contract test
```

No integration suite: the package has no I/O.

## See also

- [`../agent-harness.md`](../agent-harness.md) — current shared harness event
  vocabulary and public event stream semantics.
- [`../event-processing-semantics.md`](../event-processing-semantics.md) —
  durable acceptance-marker semantics.
- [`../roadmap.md`](../roadmap.md) — service adoption status and remaining
  observability delivery.
