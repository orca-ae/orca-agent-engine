# @orca/agent-event-contract

> Pure canonical vocabulary for agent-produced transcript events: stable event
> IDs, primary/subagent paths, persisted event kinds, and constrained lifecycle
> and model-summary payload builders. No I/O and no runtime dependencies.

Import only package root:

```ts
import {
  AgentEventKind,
  ModelObservationKind,
  subagentEventSubpath,
  turnModelSummaryEndPayload,
} from '@orca/agent-event-contract';
```

`harness-server` consumes the package kind and strict lifecycle/model payload
builders; its Dispatcher uses the acceptance-marker builder. Registry consumes
the shared kinds and marker constant while keeping its public event wire open.
Harness-server now uses a strict service-local `AgentEvent` output type. Its
producer boundaries accept only a short-lived draft input, complete every event
with a canonical ID and path, and expose only the strict result.

## Event identity and paths

`AgentEvent.id` is unique canonical identity for an event. An envelope also
requires a canonical subpath, a canonical kind, and a payload.
`isAgentEventId` validates that an ID has a non-empty `evt_` suffix; this
package deliberately does not generate IDs. Payloads do not define a second
event identity: correlation fields such as `event_ids`,
`model_request_start_id`, and `user_event_id` reference envelope IDs.

Model-start identity lives only on the event envelope. Current producers use the
package summary builders and no longer repeat that ID in the start payload.

`''` is primary-agent path. `subagentEventSubpath(threadId)` produces
`subagents/<threadId>` and rejects empty or wildcard thread IDs.
`isAgentEventSubpath` accepts only these producer paths. `*` is a transcript
read selector, and legacy `threads/...` paths are not part of this contract.

## Persisted kinds

**CANONICAL_AGENT_EVENT_KINDS** contains unique agent, agent-thread, session,
session-thread, and span producer kinds. It deliberately excludes
`agent.usage`, a runtime signal, and `session.user_event_processed`, an
internal transcript acceptance marker. Their constants remain exported for
consumers that handle those separate channels.

## Payload invariants

- `sessionIdlePayload('requires_action', eventIds)` requires a non-empty event
  ID tuple. Other stop reasons have no event-ID construction path.
- `sessionErrorPayload` carries typed `error` and discriminated `retry_status`
  objects while retaining caller-specific error context. `will_retry: true`
  requires a positive integer `next_attempt`; `false` omits it.
- `turnModelSummaryStartPayload` and `turnModelSummaryEndPayload` label current
  coarse model observations as `turn_model_summary`. The end payload carries
  `model_usage` and references start envelope ID through
  `model_request_start_id`; both payloads preserve optional trusted configured
  `provider` and requested `model` values when known. They do not claim a served
  model, logical provider-call, retry, or fallback graph.
- `userEventProcessedPayload` accepts only a non-empty canonical user-event ID.

## Tests

```bash
pnpm -F @orca/agent-event-contract test
```

No integration suite: package performs no I/O.
