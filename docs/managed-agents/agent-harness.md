# Pluggable AgentHarness — designing for Codex / Deep Agents

> The `AgentHarness` interface is intentionally narrow. Each harness brings its own session adapter on top of `transcript-store`; producer vocabulary constants come from `@orca/agent-event-contract`, while native resume payloads remain harness-specific.

## What a new harness needs

A new harness needs two things:

1. An `AgentHarness` implementation (the loop driver — start/submit/stop). Internal to `harness-server`.
2. A **session adapter** that satisfies the harness's native session contract on top of `transcript-store`. In the current TypeScript stack this is an in-process library dependency; older proto/generated-stub experiments remain as legacy/internal scaffolding only. The adapter wraps each native record in `{id, kind, payload bytes, …}` and writes it; on read, it parses bytes back to the native record. Registry never inspects the bytes.

That's it. There is no shared canonical **resume-payload** schema across harnesses. The Anthropic SDK can talk to a Claude-SDK-backed session because that adapter happens to emit Anthropic-shaped wire bytes; that's a deliberate property of _that_ adapter, not a system-wide resume canonicalization.

A session is bound to one harness for life — no cross-harness session resumption.

## Status per harness

| Harness              | Mode         | Pluggability                                                                                                                                                             | Status                     | Notes                                                                                                                                                                                                                                                                              |
| -------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Claude Agent SDK** | `separate`   | Official `SessionStore` plug point ([SDK types.py:1192-1291](https://github.com/anthropics/claude-agent-sdk-python/blob/main/src/claude_agent_sdk/types.py#L1192-L1291)) | Shipped — platform default | `ClaudeAgentSdkAdapter` wraps each SDK `SessionStoreEntry` in a `harness.claude.session_entry` envelope and writes it as `payload`, with `kind = harness.claude.session_entry`. A hand-written SessionStore suite (10 tests, `services/harness-server/test/integration/session-adapter.spec.ts`, modeled on the SDK's `runSessionStoreConformance`) runs in the CI integration job. |
| **Claude Code**      | `colocated`  | `@orca/sandbox-harness` HTTP/SSE bridge                                                                                                                                  | Shipped                    | `InSandboxHarness` drives the `@orca/sandbox-harness` server over HTTP/SSE. Events are synced to `transcript-store` by `SessionRunner.pumpEvents`. See [`harness-modes.md`](./harness-modes.md).                                                                            |
| **Codex SDK**        | `separate` (default), `colocated` | Native thread history, exported as a private checkpoint that Registry stores                                                                                  | Shipped                    | `CodexSdkHarness` runs `@orca/codex-harness` on `harness-server` for cloud `separate`, and its worker inside the shared `@orca/sandbox-harness` image for cloud `colocated`. Self-hosted Sessions require `colocated` and run in `session-runner`. See [`libraries/codex-harness.md`](./libraries/codex-harness.md). |
| **Pi SDK**           | `separate` (default), `colocated` | Native Pi session, exported as a private checkpoint that Registry stores                                                                                      | Shipped                    | `@orca/pi-harness` follows the same three execution paths and host adapter as `codex_sdk`. See [`libraries/pi-harness.md`](./libraries/pi-harness.md).                                                                                                                       |

Harnesses that are not built — the native Codex CLI on the `@orca/sandbox-harness`
bridge, the OpenAI Agents SDK, Deep Agents, and the Codex CLI shim — are listed in
[`roadmap.md`](./roadmap.md), with the plug point each exposes and what would
trigger the work. This table describes what ships.

## The AgentHarness interface (TS)

```ts
interface SubmitHooks {
  // Awaited after validation/selection and before any event side effect.
  onAccepted(): Promise<void>;
}

interface AgentHarness {
  start(input: SessionStartInput): Promise<void>;
  submit(event: UserEvent, hooks?: SubmitHooks): Promise<UserEventSubmitResult | void>;
  stop(reason: TerminationReason): Promise<void>;
  // Async event stream emitted by the harness back to the SessionRunner;
  // SessionRunner forwards them to transcript-store.
  events(): AsyncIterable<AgentEvent>;
}
```

Events in, events out. State lives in the selected `transcript-store` backend, never in the harness process. That mirrors the Anthropic engineering post's "brain / hands / session decoupled" pattern.

The package-level and harness-server-local `AgentEvent` output types are strict:
every exposed event has a canonical `evt_...` ID and primary/subagent subpath.
Producer boundaries may accept a short-lived `AgentEventInput`, but they complete
and validate it before queueing. `SessionRunner` passes the resulting ID/subpath
directly to Transcript `Event.id`/`subpath` and copies the event ID to
`idempotencyKey` metadata. It never derives envelope identity from payload
`id`, `uuid`, or correlation fields.
An invalid supplied public subpath is never coerced to primary: it fails the
event pump and poisons that local runner. It does not asynchronously tear down
the runner on its own; a later `submit()` follows the Dispatcher's existing hard
submit-failure lifecycle.
Transcript-store deduplication is by `Event.id` in one session-wide collision
domain, independent of subpath. Public child events do not broaden native SDK or
root in-sandbox replay selection.

Harness producers use the package's strict lifecycle/error builders. Their
current model observations retain the `span.model_request_start/end` wire kinds
but represent one coarse query/turn summary, labeled
`model_observation_kind: turn_model_summary`. The start event's envelope ID is
its only identity; the end has its own envelope ID and references the start via
`model_request_start_id`. Optional `provider` and `model` values come from the
trusted execution snapshot and mean configured/requested values, not a verified
served route. The default Anthropic event view removes these Orca-only summary
fields; `orca-beta` retains them.

Shared conformance coverage (`services/harness-server/test/support/model-summary.ts`)
exercises harness-server's two Claude producers — the separated Claude Agent SDK
harness and the in-sandbox harness — at this root/coarse-summary granularity:
valid unique primary-path envelopes, running/start/end/terminal ordering, summary
labeling and start correlation, required-action continuation, and proven
tool-use/result correlations. It also checks typed non-retry terminal errors where
both of those producers emit them. The current sandbox wire has no equivalent of
the separated SDK's `api_retry` frame, so the gate does not claim retry-sequence
parity. It does not infer child public subpaths or per-logical-call observations,
and does not claim full multiagent parity; those require explicit producer
evidence. The suite is imported by harness-server specs only; the
`@orca/session-runner` service runs its own conformance gate over its ten
providers and the multiagent decorator
(`services/session-runner/test/unit/harness-turn-terminal-contract.spec.ts`),
which asserts turn-terminal delivery rather than envelope and model-summary shape.

`SubmitHooks.onAccepted()` is the durable dequeue boundary for public
`processed_at`. A harness validates and selects the event first, then awaits
the hook before mutating conversation state, resolving a required-action gate,
aborting work, or starting a turn. See
[`event-processing-semantics.md`](./event-processing-semantics.md).

## Session adapter contract

For a given harness `H`, its session adapter implements `H`'s native session contract. Two operations are required:

- **append (or equivalent):** translate native record → `Event` and call `transcript-store.Append`.
- **load (or equivalent):** call `transcript-store.Read` → translate back to native record. Return `null` (or empty equivalent) if there are no events.

Optional operations (`listSessions`, `delete`, `listSubkeys`) are implemented when the harness uses them.

The adapter MUST satisfy the harness's own conformance tests in CI. Each upstream
publishes a suite worth mirroring, though none is wired in here — the
`ClaudeAgentSdkAdapter` cases in `session-adapter.spec.ts` are written by hand:

- Claude Agent SDK carries [`runSessionStoreConformance`](https://github.com/anthropics/claude-agent-sdk-typescript/blob/main/examples/session-stores/shared/conformance.ts) (13 tests) in its examples, not as an exported API.
- OpenAI Agents SDK has implicit tests in [`agents-core` memory tests](https://github.com/openai/openai-agents-js/tree/main/packages/agents-core/tests/memory).
- LangGraph ships a [conformance package](https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-conformance).

## Public event vocabulary is unified; resume payloads stay native

Two distinct concerns must not be conflated:

- **Resume / history bytes** — the harness-native transcript a cold or respawned runner replays (`harness.claude.session_entry` for the Claude SDK). This stays per-harness: we deliberately do **not** add a `canonical_payload` field mirroring Anthropic's taxonomy per event, because sessions never cross harnesses, forcing a translation table for _resume_ doubles the schema surface, and the Anthropic SDK resumes a Claude-SDK session only because that adapter writes Anthropic-shaped bytes — not because of platform canonicalization.
- **Public (client-facing) stream events** — the server→client RECEIVED taxonomy a client tails over SSE / lists via `/events`. These **are** unified across backends: both the Claude SDK and in-sandbox backends emit the identical Claude-aligned event kinds (`agent.thinking`, `agent.tool_use`/`agent.tool_result`, `agent.mcp_tool_*`, `agent.thread_context_compacted`, `session.status_running/idle/rescheduled/terminated`, `session.error`, `span.model_request_*`, `span.outcome_evaluation_*`) with a complete `stop_reason` (`end_turn` / `requires_action` / `retries_exhausted`).

`@orca/agent-event-contract` owns the shared producer vocabulary and strict lifecycle, model-summary, and internal acceptance-marker payload builders. Harness and Registry consume that contract; `services/harness-server/src/harness/event-kinds.ts`, re-exported from `agent-harness.ts`, remains a compatibility facade for existing imports. `HttpEvent.type` stays a free-form string and visibility is still decided by `transcriptEventVisibility`. This gives clients one taxonomy regardless of which harness ran the session, without a runtime translation table on the resume path.

## Multi-language adapters

Every shipped adapter is TypeScript. The `.proto` files under `services/proto/` exist for generated-type coverage and CI; public Claude-compatible clients do not call registry or harness over gRPC.

## Harness modes

Each harness runs in one of two topologies selected by `metadata.mode` on the agent record:

- **`separate`** (default): the agent loop runs outside the sandbox; only tool dispatch enters it. For cloud Sessions the loop runs in `harness-server` (`ClaudeAgentSdkHarness` for `claude_agent_sdk`, `CodexSdkHarness` for `codex_sdk` and `pi_sdk`); self-hosted Sessions run it in `session-runner`.
- **`colocated`**: the agent loop and its tools run *together*, inside the sandbox. Cloud `claude_code`, `codex_sdk`, and `pi_sdk` Sessions are driven by `harness-server` over the `@orca/sandbox-harness` HTTP/SSE bridge. Every other `colocated` Session — cloud `codex`, `cursor`, `pi`, `custom`, and `mock`, and every self-hosted Session — is driven by `session-runner`: the registry is the shared server, coordinates the session over the WS tunnel, and is the single writer of its `agent.*` events; `harness-server` is not involved.

The per-agent `metadata.harness` and `metadata.mode` keys, the harness catalog (`@orca/harness-catalog`), the transport abstraction, and the stream-parity semantics are described in full in [`harness-modes.md`](./harness-modes.md); the topology diagrams live in [`deployment-topologies.md`](./deployment-topologies.md).

## Skills

[`skills.md`](./skills.md) specifies the disclosure rubric: both harness modes receive the
same compact per-agent Skill catalog, while immutable bundles are materialized
read-only in the sandbox and read on demand. Skill bodies are not composed into
the system prompt and do not alter the Agent's tool configuration.
