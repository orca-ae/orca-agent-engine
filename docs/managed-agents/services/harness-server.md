# harness-server

> Internal-only service that hosts the agent loop. Runs Claude Agent SDK, Codex SDK and Pi SDK. Has
> no public listener and no auth chain for client traffic. State and bytes flow
> through the imported store libraries (`@orca/transcript-store`,
> `@orca/file-store`, `@orca/memory-store`, and `@orca/skill-store`); MCP
> egress flows through `ai-gateway` over plain HTTP (`/v1/mcp`, session JWT).

Depends on `@orca/transcript-store` (`workspace:*`). Calls
`TranscriptStore.append/read/tail` directly — no gRPC hop. Proto/generated stubs remain legacy/internal type coverage, not public client transport. Kafka is the default
transcript backend; Postgres and Apache Pulsar can be selected with
`TRANSCRIPT_STORE_BACKEND=postgres` or `TRANSCRIPT_STORE_BACKEND=pulsar`.

## Execution ownership

`harness-server` owns cloud `separate` Sessions and cloud `claude_code` /
`codex_sdk` / `pi_sdk` `colocated` Sessions. Self-hosted Sessions and other cloud colocated
harnesses belong to Registry and
`session-runner`. Both services use `resolveExecutionOwner` from
`@orca/harness-catalog`; Registry resolves the harness and mode from the Session-pinned
Agent version, including after the Agent or Environment is archived.

The dispatcher checks the internal execution-owner route before each user event,
including cold interrupts. Registry-owned events do not prepare execution, acquire
a sandbox, or append completion events here. Failed ownership reads are retried
without assuming ownership.

For an owned session, `HarnessProviderRegistry` selects an implementation by
harness identity and supported mode. The registered separate implementations are
`claude_agent_sdk`, `codex_sdk` and `pi_sdk`;
an unregistered identity fails setup rather than using Claude implicitly. The
HTTP/SSE adapters serve the cloud Claude, Codex, and Pi `colocated` paths, including shared resource
mounts, Skills, output capture, stateful budget enforcement and, for Claude, native subagents. See [harness modes](../harness-modes.md).

## Pi SDK execution

`metadata: {"harness":"pi_sdk"}` selects cloud `separate` execution. An explicit
`mode: "colocated"` selects the shared sandbox-harness image and HTTP/SSE bridge.
Both paths use the same host adapter as Codex for tool policies, callbacks,
guardrails, scoped credentials, durable turn receipts, usage and recovery.
The provider selector supplies `PiSdkWorker` locally or `pi-sdk` remotely.
[`@orca/pi-harness`](../libraries/pi-harness.md) owns the embedded SDK and its
versioned native history. The pinned model provider selects its official Pi protocol, with matching direct credentials or
Gateway egress; see [Pi configuration](../libraries/pi-harness.md).
Self-hosted `pi_sdk` uses session-runner instead.

## Codex SDK colocated execution

Cloud `codex_sdk` with explicit `mode=colocated` uses the same sandbox image,
entrypoint, resource limits, readiness probes, HTTP/SSE transport, mounts, output
indexer and teardown as cloud Claude. It does not launch an Environment worker.
`RemoteCodexSdkWorker` replaces the local SDK worker inside `CodexSdkHarness`;
the existing host adapter owns tools, policies, durable turn receipts and usage.
Both use the existing `SANDBOX_HARNESS_CLAUDE_CODE_IMAGE` deployment pin.
The worker receives a refreshed scoped Gateway JWT, never a provider key.
Native SDK execution tools remain disabled; sandbox tools use the same
policy-enforced handle as separate execution.

Private commands are acknowledged by the sandbox subprocess. Each acknowledgement
carries an event sequence; the host waits for the corresponding SSE events before
committing a turn. Native checkpoints remain private adapter input and never enter
the public transcript. Self-hosted Codex continues using session-runner.

## Codex SDK separate execution

`metadata: {"harness":"codex_sdk"}` selects `separate` by default, matching
`claude_agent_sdk`. Cloud Sessions execute the SDK and its native CLI on
harness-server. A private temporary working directory and `CODEX_HOME` isolate
native history and configuration. The SDK receives no ambient provider
credentials, project settings, or executable native tool surfaces. All enabled
Sandbox tools use the dispatcher's policy-wrapped Sandbox handle through a
loopback MCP relay; remote MCP tools use the Session gateway and refreshed MCP JWT.
No Codex binary or session-runner is required in the Session Sandbox.

The shared [`@orca/codex-harness`](../libraries/codex-harness.md) library owns
the pinned SDK, native thread, isolated configuration and checkpoint format.
The server adapter owns canonical Orca events, tool permission policies,
`user.tool_confirmation`, custom client tool results, interruption and cleanup.
Sandbox tools default to `always_allow`, matching the managed toolset contract;
explicit `always_ask` and `always_deny` policies still gate them. Remote MCP tools
inherit their toolset policy when no per-tool or server wildcard override exists.
Messages waiting behind a required action remain unaccepted. Unsupported message
content, outcome commands, and `system.message` companions fail explicitly.
Stateless `request`, `tool_call` and `tool_result` guardrails are enforced.
Stateful rules are supported only at `request`; other phases and subagent-scoped
rules are rejected during execution preparation. The adapter derives statefulness
and state scope from the compiled rule again before starting the worker.
A guardrail asking for approval on a custom client tool fails closed.

Custom client callback results use the shared
[Codex content converter](../libraries/codex-harness.md#client-callback-content).
Text and `is_error` are preserved. Inline PNG/JPEG/WebP/GIF images become MCP images;
text documents and search results become JSON text retaining content and metadata.
Base64 `text/*` documents require valid UTF-8. URL/File-store image or document
sources, binary documents, unsupported image MIME types, and malformed encoded
content interrupt the turn with a descriptive `session.error` and terminal
`retries_exhausted` idle status. The adapter performs no callback-source fetches.
Installed SDK fixtures verify document, search, and PNG continuation.

Each separate Codex runtime claims a private Registry ownership revision with a
caller-stable token. Every accepted SDK turn has a durable pending receipt before
submission. It records the primary source and every accepted confirmation,
custom result, or interrupt source. A `requires_action` pause settles transport
delivery so client replies can flow, while the primary source remains incomplete.

Stateful requests run in one serialized acceptance lane. Each accepted turn reads
Registry-authenticated daily counters and authoritative session totals, then
persists rule updates and `codex_sdk_usage_pending:<turn_event_id>` before starting
the SDK. The marker contains its version, turn event ID, and stable usage event ID.
Completed usage is recorded under that ID. After every response event and the
model-summary end are acknowledged by transcript storage, Registry atomically
commits native history and a ready receipt. A successful guarded commit also
checks the usage ledger and deletes only the matching pending marker. The usage
event is marked already recorded only after Registry confirms it.

The adapter publishes the receipt's stable terminal events, then source-completion
markers, in order, and awaits persistence before settling the receipt or accepting
another turn. A lost commit or terminal acknowledgement retires the local runtime.
Recovery scans transcript identities and appends only missing terminal/completion
events, without resubmitting the SDK prompt. It also runs before the completed-source
fast path and before cold interrupts, independently of execution preparation.
A broker append already issued by a retired runtime can finish after a replacement
has scanned and repaired the transcript. Such a race can produce duplicate broker
records with the same terminal event ID and timestamp; it does not repeat the SDK
submission or create a different terminal outcome.

A pending receipt recovered after a crash is explicitly abandoned with an
interrupted-turn error. Recovery retains the previous native snapshot and completes
all accepted sources without replaying model or tool work. Unknown guarded usage
remains marked and blocks later requests; controls remain available. Receipt
ownership fences stale writers, including the legacy checkpoint and marker-clear
paths. Completed usage and marker removal survive a lost commit acknowledgement.
Session cost/token caps apply between turns, not inside a running turn. Daily
budgets check recorded spend and do not reserve spend across concurrent Sessions.
Soft approval thresholds are rejected. Unpriced spend denies at this request-only
boundary unless `on_unpriced: "allow"` is explicit.

Direct LLM egress uses harness-server's `OPENAI_API_KEY` and optional
`OPENAI_BASE_URL` (the SDK default is `https://api.openai.com/v1`). Separate
Sessions inherit `LLM_EGRESS_DEFAULT` (`direct` when unset), and
`metadata.orca_llm_egress` overrides that default. With Gateway egress, the SDK
uses `LLM_GATEWAY_URL`, a scoped Session JWT refreshed before each turn, and
`X-Orca-Session-Id`. Registry gives
Codex Gateway tokens a 660-second lifetime; the harness requires at least 630 seconds
remaining before reuse, covering the ten-minute turn timeout and client tool waits.
A shorter minted credential fails before SDK execution. The URL retains
its `/v1` prefix: the SDK appends `/responses`. The Gateway must implement native
Responses and authorize `llm-responses` plus the selected model. Model provider
keys remain at the Gateway for this path.

Native checkpoints and recovery receipts share a private, versioned Registry
storage envelope. Native-only readers unwrap the checkpoint; ownership and receipts
never enter public transcript payloads or Session fields. Retiring a runtime rejects
outstanding persistence acknowledgements and ignores late worker checkpoints.
SDK usage includes the pinned model and source turn ID; cached input is counted
separately and the existing authoritative usage writer prevents double counting.

`metadata.mode: "colocated"` runs on harness-server for cloud Environments (see
[Codex SDK colocated execution](#codex-sdk-colocated-execution)) and on the
Registry/session-runner path for self-hosted Environments. Self-hosted Codex
Sessions require that explicit mode; `separate` is admitted only for cloud
Environments.

## Stack

Node 22, TypeScript strict (Kafka access only via the `@orca/transcript-store` library), `@anthropic-ai/claude-agent-sdk` (the Claude Agent SDK), E2B SDK (`@e2b/code-interpreter`), OpenTelemetry SDK.

## Sandbox runtimes

Every sandbox is acquired through the `SandboxRuntime` interface
(`src/sandbox/sandbox-runtime.ts`). Five implementations ship, selected by the
**required** `SANDBOX_RUNTIME` environment variable — `loadConfig` throws on a
missing or unrecognized value, so there is no silent default and no deployment
can accidentally run without isolation.

| Value         | Implementation           | Isolation                                                                | FUSE                                                  | Pause/resume | Use                                                                                                                        |
| ------------- | ------------------------ | ------------------------------------------------------------------------ | ----------------------------------------------------- | ------------ | -------------------------------------------------------------------------------------------------------------------------- |
| `local`       | `LocalSandboxRuntime`    | Host-OS sandbox via `srt` (`sandbox-exec` on macOS, bubblewrap on Linux) | No                                                    | No-op        | Local development, as the local stack's default. No CI stack runs it; CI's `integration` job tests it against a real `srt` |
| `e2b`         | `E2BSandboxRuntime`      | Firecracker VM                                                           | Yes, with the `orca-default` template                 | Yes          | Nightly mount-strategy CI, FUSE-dependent tests                                                                            |
| `opensandbox` | `OpenSandboxRuntime`     | gVisor Pod per sandbox, controller-managed                               | Always on; gVisor in-sandbox FUSE, probed fail-closed | Refused      | Self-hosted Kubernetes; the chart default                                                                                  |
| `agentenv`    | `AgentEnvRuntime`        | Firecracker VM managed by AgentENV                                       | No; envd Files API fallback                           | Yes          | Self-hosted AgentENV deployments                                                                                           |
| `in-memory`   | `InMemorySandboxRuntime` | **None** — a null sandbox                                                | No                                                    | No-op        | Unit tests and the `e2e-stack` backend legs                                                                                |

`capabilities.supportsFuse` is what the strategy factory reads: a runtime that
reports `false` gets `LocalMemoryStrategy` instead of `MemoryFuseStrategy`, and
output capture falls back to directory scanning. This is why the same session
definition behaves identically across runtimes without the caller choosing a
strategy.

Required companions: `e2b` needs `E2B_API_KEY`; `opensandbox` needs
`OPEN_SANDBOX_DOMAIN` and `OPEN_SANDBOX_IMAGE`; `agentenv` needs
`AGENTENV_BASE_URL`, `AGENTENV_API_KEY`, and `AGENTENV_IMAGE`; `local` needs
`srt` on `PATH` and refuses to start without it. A new runtime implements the
interface and is wired in `src/main.ts` — no other file should branch on the
runtime kind.

`AgentEnvRuntime` creates secure cold sandboxes and keeps the returned envd
access token inside the trusted adapter. Lifecycle calls use the AgentENV REST
API; Bash and filesystem operations use envd through the gateway's
`x-agentenv-sandbox-id` and `x-agentenv-target-port` routing headers. It
advertises `supportsFuse=false` and `supportsLocalMemory=true`, so memory stores
and outputs use the existing Files API materialization and filesystem indexing
paths. The required image contract is defined by
`sandbox-templates/orca-agentenv/Dockerfile`; setup runs as root, while agent
commands run under the same Bubblewrap uid/gid 1000, capability-free write
policy used by the other remote runtimes.

## Responsibilities

1. Subscribe to incoming control + user events through the backend event source. Kafka uses consumer groups on session topics; Postgres leases the earliest unprocessed event per workspace/session within each group; Pulsar uses a `KeyShared` subscription over the session-topic regex with the session ID as the message key.
2. For each session it claims, instantiate the configured `AgentHarness` (Claude Agent SDK) plus its session adapter (`ClaudeAgentSdkAdapter`). The adapter calls into the `TranscriptStore` library in-process — the SDK sees it as its native `SessionStore`.
   For coordinator agents, dispatcher expands registry's resolved
   `multiagent.agents[]` refs into fixed-version runtime snapshots and passes
   them to Claude Agent SDK as `Options.agents`. Each subagent gets its own
   system prompt/model/tool allowlist, while the SDK's `SessionStore.subpath`
   keeps its thread history separate under the parent Orca session.
   The adapter refuses to create a 26th active subpath for a session, matching
   Claude's 25 concurrent session-thread limit.
3. **Enforce guardrails.** Harness is the enforcement point: registry hands it a
   compiled, tier-ordered guardrail list plus the session's restored state in the
   prepared runtime, and harness evaluates every wired phase — `request`,
   `tool_call`, `tool_result`, `llm_request` — through `@orca/guardrails`. The
   tool-call decision is a single fold seeded by the agent's `permission_policy`,
   so guardrails and permission policies are one mechanism rather than two
   consulted in sequence; a guardrail `ask` reuses the existing tool-confirmation
   round trip and adds no new event kind. Native SDK `Agent` dispatch is routed
   through that same permission callback by its `PreToolUse` hook, so the
   proposing response's usage acknowledgment, guardrail denial, and required
   user approval all complete before the SDK sends a child model request. A
   policy allow proceeds without a user confirmation. Delegating to the
   internal primary managed-agent boundary is always denied by the hook.
   Editing a guardrail invalidates the
   prepared runtime: registry enqueues a runtime-invalidation sentinel through
   the lifecycle outbox, and the dispatcher re-prepares the runner at the next
   turn boundary — a mid-turn session finishes its turn under the old rule and
   never starts one under a stale rule. See [`../guardrails.md`](../guardrails.md).
   Before draining each user message deferred behind a required-action boundary,
   the dispatcher reloads Registry's authoritative guardrail counters and leaves
   the message queued if that refresh fails, so request-phase budgets fail closed.
4. **Rewrite MCP server URLs** for the SDK. Read the Agent record's `mcp_servers[]` from the session-start event, build a `mcpServers` map for the SDK where every `url` points at the gateway's MCP endpoint — the configured `AI_GATEWAY_URL` with `/v1/mcp` appended unless it already ends in `/v1/mcp` — and headers carry `X-Orca-Backend`, `X-Orca-Session-Id`, and a session-scoped JWT. The dispatcher retains a session-scoped JWT provider; each new Claude SDK query checks token freshness and refreshes on demand before constructing its MCP configuration. A refresh failure refuses query startup rather than reusing stale headers. The harness closes the provider before awaiting startup during stop, cancelling pending refresh work. Running queries retain their current transport. See [`../mcp-routing.md`](../mcp-routing.md).
5. **Materialize session resources** before the first turn via the appropriate `MountStrategy` per resource type :
   - `file` → `TarballPrefetchStrategy`. Harness opens the exact prepared file IDs host-side; sandbox credentials contain no file grant. See [`../mount-strategies.md`](../mount-strategies.md).
   - `memory_store` → `MemoryFuseStrategy` (FUSE-capable runtimes) or `LocalMemoryStrategy` (InMemory fallback). FUSE mounts only `{root}workspaces/{ws}/memory-stores/{store_id}/live/`; a per-session watcher registers through the scoped internal memory-version route. See [`../libraries/memory-store.md`](../libraries/memory-store.md).
   - `github_repository` → `GitCloneStrategy`. Resolve PAT via registry's git credential resolve; host-side `git clone --filter=blob:none --depth=1 [--branch <ref>]`; stream working tree + `.git/` into sandbox at `mount_path`. PAT never enters sandbox; in-sandbox `git push` / `git fetch` flow through the `orca-git-creds` credential helper. See the GitHub-repository section below.

   Emit `session.resource_mounted` events on success. Required setup failures
   emit public `session.error` (`setup_failed`, `retry_status.will_retry=false`)
   and `session.status_idle` (`retries_exhausted`) events, clean
   up partial sandbox/workdir state, and prevent the turn from running without
   the requested files, memory stores, output mount, or repositories. See
   [`../resource-mounting.md`](../resource-mounting.md).

6. **Materialize Session-pinned Skills last** before sealing the write policy
   and starting the first turn. Harness opens exact immutable bundles host-side
   through `@orca/skill-store`, verifies package and file digests, and writes
   them read-only under `/workspace/skills/<name>/`. Each agent gets only its
   own compact name/description/path catalog; the model reads `SKILL.md` and
   referenced files on demand. Before the first sandbox write, remote runtimes
   reject aliases and pre-existing mounts at or below every planned filesystem
   root. Until providers expose a sealed setup namespace, `colocated` rejects
   Environment custom images and all managed sandbox executions reject package
   installer hooks. See [`../skills.md`](../skills.md).

7. Tool dispatch:
   - **`agent_toolset`** (bash, read, write, edit, glob, grep, web_fetch — seven tools; `web_search` is accepted as a wire name but has no implementation) → `SandboxRuntime.run(tool, input)`. Accept `agent_toolset_20260401` from inbound SDK and normalize.
   - For `agent_toolset` writes targeting `/mnt/memory/<store>/`, the FUSE mount routes the write directly to the attached store's `live/` prefix. The watcher detects it asynchronously and calls the workspace/session/store-scoped Registry route. On CAS mismatch it emits `session.memory_conflict`.
   - `mcp_toolset` → SDK's MCP client opens directly to `ai-gateway` via the rewritten config. Harness does NOT proxy individual MCP calls — the gateway is on path because of the URL rewrite.
   - **In-sandbox git** — agents run `bash + git push / fetch` directly against `github_repository` mounts; the registered `orca-git-creds` credential helper round-trips through the registry's `POST /v1/git-creds` route per call. There is no `orca.git_push` skill; the credential helper is the whole mechanism. See the GitHub-repository section below.
   - Primary-agent `custom` → `TranscriptStore.append` with
     `agent.custom_tool_use`; wait for `user.custom_tool_result`. `agent_toolset`
     permission policies do not add a preceding confirmation gate: the
     custom-tool callback is already its own required-action boundary.
8. Handle session lifecycle: idle, running, rescheduling, terminated. Session
   creation stays `idle`; the dispatcher only creates a `SessionRunner` and
   acquires a sandbox after it consumes the first `user.*` event. After a turn
   completes, registry is updated back to `idle` while retaining the warm
   `sandbox_handle_id`; `SESSION_IDLE_TIMEOUT_MS` (default 60000) bounds how
   long that runner stays warm. When the timer fires, the runner stops,
   mounted resources are torn down, outputs are indexed, the sandbox is
   destroyed, and registry clears `sandbox_handle_id`. Registry accumulates
   running intervals into `session.stats.active_seconds`; idle warm-retention
   time is excluded.
   Registry archive publishes a durable `session.archived` transcript sentinel
   and permanent delete publishes `session.deleted` (through the retrying
   lifecycle outbox); consuming either sentinel stops the runner even when idle
   teardown is disabled or has not fired yet.
   After each **model call** (each assistant response, not once per SDK turn),
   harness records
   `session.usage.{input_tokens,output_tokens,cache_read_input_tokens,cache_creation}`
   through registry's mesh-internal usage route. For Claude SDK streams, one
   provider message is one model call: `message_start` supplies initial input
   and cache counts, cumulative `message_delta` values replace fields, and
   `message_stop` triggers one report with final output tokens, the actual
   model, and the managed subagent attribution. SDK assistant content blocks
   do not produce additional usage reports. The SDK forwards child assistant
   blocks without partial usage events, so subagent calls use final assistant
   entries from its eager `SessionStore` mirror. Message ids deduplicate blocks
   and mirror retries; the entry's agent type retains attribution even after
   the runtime child stops. Registry usage acknowledgments are serialized across
   the iterator and mirror callbacks so older totals cannot replace newer ones.
   Zero-usage messages also wait for preceding acknowledgments before their
   tools proceed, without creating an additional usage report.
   Other assistant-only SDK responses retain their supplied usage fallback;
   result totals are recorded only when no per-message usage was reported.
   Streamed and mirrored messages require complete, valid final input/output
   counts and valid supplied cache counts before tool permissions can proceed.
   Missing or malformed final usage fails closed; explicit zero counts remain
   valid. Nullable cumulative input/cache deltas preserve their previous counts.
   Every tool permission callback waits for its proposing message's priced
   acknowledgment before evaluating `tool_call` guardrails, including callbacks
   that arrive before the iterator consumes the tool block. Failed usage
   acknowledgments block tools and retain the usage event id for retry. An
   interrupted acknowledgment can be retried with that same id; cancellation,
   query failure, or an unmatched tool id at the end of its model message
   releases pending callbacks with a denial. A failed usage acknowledgment in
   the mirror aborts the query instead of being swallowed by SDK mirror retries. This makes final model-call spend
   visible to the same response's tools (see
   [`../guardrails.md`](../guardrails.md), [`../pricing.md`](../pricing.md)).
   Usage records are control plane updates and are not appended to the
   transcript stream.
9. Crash recovery: the selected backend redelivers unprocessed messages to a healthy replica; `adapter.load()` reads the session's events via the `TranscriptStore` library; SDK resumes with `resume: <id>` from the rebuilt context. Replicas are cattle.
   `ClaudeAgentSdkAdapter.listSubkeys()` enumerates non-empty transcript
   subpaths so SDK-managed subagent threads can be discovered and replayed
   across turns/restarts.
   `user.interrupt` invokes abort if a Claude SDK query is still active; when it
   carries a `session_thread_id`, registry has already routed it to that
   thread's subpath before the dispatcher sees the event. Current event-source
   delivery is serialized per session, so it does not promise mid-turn
   preemption; see
   [`../event-processing-semantics.md`](../event-processing-semantics.md#compatibility-and-rollout).
   Without a runner, `user.interrupt` is completed directly with
   `session.status_idle` (`end_turn`) and an idle Registry state; it does
   not prepare an execution or acquire a sandbox.
10. Record client-event acceptance. After a harness validates and selects a
    `user.*` event, it awaits the dispatcher-provided acceptance hook before
    applying it. The hook appends `session.user_event_processed`; registry
    projects that marker into the public event's `processed_at`. This is a
    dequeue/application boundary, not turn completion. After a terminal
    `session.status_idle` or `session.status_terminated`, `SessionRunner`
    appends an internal `session.user_event_completed` marker for the oldest
    accepted turn-driving source event, or one for each source the harness names
    (`CodexSdkHarness` names them). Without named sources the terminal and its
    marker share one transcript-store batch; named sources, and the outcome of
    work that never starts, are appended one event at a time, the terminal status
    before the completion marker.
    Dispatcher adds a source id to its completed cache only after those appends
    succeed, and rebuilds the cache from those markers before executing a
    redelivery after restart. An acceptance marker never suppresses delivery;
    an event without a completion marker remains at-least-once. Deferred
    messages stay null until selected for their later turn. See
    [`../event-processing-semantics.md`](../event-processing-semantics.md).
    Dispatcher advances a lifecycle generation before shutdown waits: source
    handlers crossing that fence reject retryably, and late acceptance,
    completion, and status callbacks cannot recreate retired turn state.

### Bounded server-side Read

Both server-side agent Read surfaces (the Claude Orca MCP tool and the legacy
agent toolset) share a 4096-byte default page and an 8192-byte effective maximum.
The optional limit still accepts integers from 1 through 100000 as a requested
upper bound. Each returned tool-result envelope is at most 16384 UTF-8 bytes
after JSON serialization, including text blocks, metadata, and escaping. If a
page exceeds that budget, the handler halves the effective window and rereads
at the same offset until the complete envelope fits; it never slices formatted
text or JSON. Oversized error details are replaced with a short diagnostic.

Every successful Read includes the existing text metadata trailer; the legacy
surface also returns structured metadata. The trailer reports the effective
limit, source bytes_read, total_bytes, truncation, and next_offset. Offsets are
UTF-8 byte positions, not character or line numbers. Follow next_offset to read
the next page; EOF returns zero bytes and no continuation. A requested limit
smaller than a code point still returns that complete code point (up to three
extra bytes), preserving the underlying ranged-read progress rule.

Each retry uses the existing independently authorized sandbox ranged-read
operation and returns only that attempt's content and metadata. Retries and
successive pages do not constitute a file snapshot: concurrent file changes
can affect them. The underlying decoder's replacement behavior for malformed
UTF-8 is unchanged; byte metadata describes source bytes, not re-encoded
replacement characters. Valid UTF-8 files retain byte-accurate continuation.
No host filesystem fallback is involved. The underlying ranged-read default
and maximum, and Edit's whole-file capacity, remain 100000 bytes. These budgets
do not change client-executed Read handlers or the SDK's token thresholds.

### Execution preparation failures

Registry's `invalid_runtime_binding` response is a permanent failure of the
current source event, both on a cold start and when refreshing a warm runner.
This includes a Session's pinned Agent version whose Agent has been archived;
retaining the immutable version snapshot does not keep that binding runnable.
The dispatcher stops the stale runner, emits `session.error` with
`retry_status.will_retry=false` and `session.status_idle` with
`stop_reason.type=retries_exhausted`, and clears the sandbox handle in Registry's
idle state.
It persists the processed marker before the error and idle result, then appends
the completion marker. Each event is written separately in that order, so a
partial backend failure cannot publish a terminal result ahead of acceptance or
completion ahead of the result. Redelivery reads the durable transcript and
appends only missing event IDs, preserving the original timestamps and payloads
even after a transcript client restart. Only then does it acknowledge the source
event, so redelivery does not rerun the failed turn and subsequent control events
can be consumed.

Network failures and temporary Registry errors remain retryable. A failure to
persist the failure result or update Registry state also leaves the source
unacknowledged; a rejected preparation is not considered handled until its
failure is durable.
For an unstarted outcome, the dispatcher supplies a lifecycle-fenced persistence
retry through `SessionEventBarrierError`. Pulsar retains the delivery and retries
that operation before dispatching further events in the same workspace/session
queue; unrelated sessions on the consumer keep progressing. `KeyShared`
ownership prevents other replicas from consuming later events for that session.
Pulsar defaults to 8 concurrent handler/repair calls and 100 active or queued
deliveries. At the pending limit it pauses receiving until a delivery settles;
repair backoff releases execution capacity while retaining session order.
Postgres retains and renews the earliest unprocessed event's claim while retrying
the same callback; other workers cannot claim later events in that session.
Both paths preserve the original preparation failure and prevent a queued
interrupt from closing a partially written failed turn. Pulsar uses the configured
negative-ack delay and Postgres uses its poll interval for backoff; shutdown
cancels the wait and leaves unfinished work unacknowledged.
Recovery scans must finish successfully before missing outcome IDs are appended.
Pulsar reader timeouts and other read errors reject even a partially yielded scan,
so a restart cannot mistake an unread committed outcome for an absent one.
If Registry reports that the Session is already archived, terminated, or
deleted, the dispatcher acknowledges the event without publishing a new idle
result or reopening the Session.

### Kafka discovery, readiness, and metrics

Kafka discovery uses `admin.listTopics()` every
`KAFKA_TOPIC_REDISCOVER_INTERVAL_MS` (default `30000`; Kafka configuration
requires a positive integer). Both bare Kafka and `KAFKA_TOPIC_PREFIX`/KoP
paths canonicalize that one metadata snapshot and pass its exact topic list to
`consumer.subscribe({ topics })`; the dispatcher never uses a regex
subscription. A topic created after that snapshot is absent from the current
consumer and is picked up by the next discovery tick.

When a snapshot changes, dispatcher connects and explicitly subscribes a
candidate while the current consumer remains active. It disconnects the old
consumer before calling candidate `run()`, waits for the candidate's KafkaJS
group-join event, then records the active subscribed list and assignment.
Candidate failure or join timeout disconnects the candidate, restores a fresh
consumer for the prior joined list when possible, and leaves the new snapshot
pending for the next tick; if old retirement itself fails, that still-tracked
old consumer remains active rather than running an untracked fallback beside
it. If a running candidate cannot disconnect, it also remains tracked until a
later tick retires it.

Kafka delivery runs with `autoCommit: false`. `eachMessage` pauses only the
delivered topic-partition, starts or reuses one in-process work promise keyed by
the durable event id, and returns to KafkaJS without waiting for model/tool
execution. The work promise remains cached through terminal settlement and is
removed only after manual offset commit succeeds, so replacement redelivery
does not start duplicate in-process work. Terminal settlement means the terminal
status and its `session.user_event_completed` marker(s) are durable. Any
transcript-pump persistence failure retires the runner and rejects
settlement so redelivery can build a fresh runner. A warm runner's idle-timeout
countdown does not start while its Kafka settlement is pending; terminal
durability either arms that countdown or rejects the settlement first.

Request-policy denials and usage-acknowledgment failures use the same durable
terminal path. Accepted turns append their completion marker with the idle
event before committing the source offset and resuming the partition. Requests
rejected before acceptance also settle their source delivery after the idle
append. A usage failure retains the runner and pending usage identities for
retry, while queued archive/delete sentinels remain deliverable and can release
its sandbox. A failed terminal append leaves the source retryable.

Every detached delivery captures the current assignment epoch and verifies
topic-partition ownership before commit, seek, or resume. Rebalancing, crash,
consumer retirement, and shutdown revoke that epoch; stale continuations leave
the offset for its new owner. Retryable work or commit failures keep the
partition paused during an interruptible exponential backoff from 250 ms to 5
seconds, seek to the failed offset only while the same assignment still owns
it, then resume. Assignment change or shutdown wakes the delay immediately and
suppresses the stale seek/resume.

Shutdown cancels candidate group-join waiters and gives
Kafka transition/disconnect, non-Kafka source join, and parallel runner cleanup
one shared 10-second grace. Source quiesce and runner cancellation begin
together, so a source waiting on its in-flight handler cannot deadlock before
that runner is stopped. On expiry dispatcher logs a fixed sanitized diagnostic,
detaches the handled late promise, and returns; late callbacks cannot restore
consumer pointers or turn bookkeeping while stopping. Kafka crash and rebalancing events clear
consumer readiness and assignment; group join restores them. A crash against
an otherwise unchanged snapshot causes a fresh replacement. Dispatcher is the
sole crash-recovery owner: its consumers disable KafkaJS automatic restart via
`retry.restartOnFailure`, while request retries and normal group rebalances
remain enabled. This prevents an internal restart still joining the group from
outliving retirement and taking partitions from the tracked replacement.

`/healthz` is process liveness. `/readyz` is `503` while Kafka metadata is
stale, known topics are not joined, a transition remains failed, or dispatcher
is stopping. A fresh empty Kafka snapshot is ready. For Postgres and Pulsar,
dispatcher queries the source's optional `status()` after `start()` rather than
trusting startup forever: a stopped source reports `event_source_stopped` and a
terminal run-loop failure reports `event_source_failed`, with no backend error
details. Built-in Postgres and Pulsar sources additionally expose `whenFailed()`;
harness main observes only escaped terminal failures, starts re-entrant
controlled shutdown with exit code 1, and keeps retry loops readiness-neutral.
Their terminal logs contain only safe error-name and explicitly allowlisted
stable-code fields. Legacy custom sources without `status()` retain start-based
readiness.

The harness Prometheus registry exports unlabelled Kafka gauges for discovered
topics, group-joined subscriptions, assigned topics/partitions, consumer
readiness, and latest `END_BATCH_PROCESS.offsetLag` (zero while unassigned);
discovery and transition outcome counters; runner-spawn attempt/failure
counters; and a producer-`produced_at`-to-durable-`session.status_running`
latency histogram. These metrics carry no topic, session, or prompt labels.

## Key abstractions (TS interface sketches)

```ts
interface SubmitHooks {
  onAccepted(): Promise<void>;
}

// AgentHarness — one per active session.
interface AgentHarness {
  start(input: SessionStartInput): Promise<void>;
  submit(event: UserEvent, hooks?: SubmitHooks): Promise<UserEventSubmitResult | void>;
  stop(reason: TerminationReason): Promise<void>;
  // Async event stream emitted by the harness back to the SessionRunner;
  // SessionRunner forwards them to TranscriptStore.append.
  events(): AsyncIterable<AgentEvent>;
}

// SandboxRuntime — pluggable execution environment for the built-in toolset.
interface SandboxRuntime {
  acquire(env: EnvironmentSpec): Promise<SandboxHandle>;
}

interface SandboxHandle {
  run(cmd: ToolCall): Promise<ToolResult>; // bash, read, write, edit, glob, grep, web_*
  files: FilesystemAdapter; // for write/edit/read tool primitives if not via run
  id: string; // e.g. E2B sandbox id
  pause(): Promise<void>;
  resume(): Promise<void>;
  destroy(): Promise<void>;
}
```

The `AgentHarness` interface is intentionally narrow: events in, events out. State lives in the selected transcript backend (accessed via `@orca/transcript-store`), never in the harness process. That mirrors the Anthropic engineering post's "brain / hands / session decoupled" pattern.

Current idle behavior is destroy-and-rebuild. An idle session restores from
the transcript log plus durable file, memory, output, and
repository resources; arbitrary unpersisted scratch files inside the destroyed
sandbox are not preserved across idle.

See [`../agent-harness.md`](../agent-harness.md) for the multi-harness story (Codex, Deep Agents, OpenAI Agents).

## Layout

```
services/harness-server/
  src/
    runner/
      session-runner.ts            # one logical loop per session
      dispatcher.ts                # event source, execution-owner check, harness + sandbox setup
    harness/
      agent-harness.ts             # interface
      claude/                      # ClaudeAgentSdkHarness
        index.ts
        event-mapper.ts            # SDK entry <-> Event {kind, payload bytes}
        session-adapter.ts         # SDK SessionStore over @orca/transcript-store
      codex-sdk/                   # CodexSdkHarness (codex_sdk, pi_sdk) + remote sandbox worker
      in-sandbox/                  # InSandboxHarness + DialInTransport (cloud claude_code)
    mcp/
      rewrite.ts                   # mcp_servers[].url rewrite
      session-jwt-provider.ts      # on-demand session JWT refresh
    sandbox/
      sandbox-runtime.ts           # SandboxRuntime / SandboxHandle interfaces
      e2b/
        runtime.ts                 # E2BSandboxRuntime
      local/ opensandbox/ agentenv/ in-memory/   # the other SandboxRuntime implementations
      mounts/
        tarball-prefetch.ts        # file resources (the only file strategy)
        memory-fuse.ts             # memory-store mount
        git-clone.ts               # github_repository mount
    git/                           # host-side clone + work dirs
      git-worker.ts                # simple-git wrapper (host-side clone)
      work-dir.ts                  # WorkDirManager (per-session ephemeral repos)
    config.ts                      # the env surface; scanned by docs:env-check
    main.ts
  test/
    integration/
      session-adapter.spec.ts      # hand-written SessionStore conformance, 10 cases
    unit/
      session-adapter.spec.ts      # 4 unit cases, same name, different suite
  package.json                     # workspace deps: transcript/file/memory/skill stores
```

Store libraries are imported as workspace dependencies — there are no gRPC stubs for them. MCP egress is plain HTTP via the configured `AI_GATEWAY_URL`; harness-server does not compile a gateway client.

`harness-server` does **not** speak directly to Postgres or object storage outside of what the imported store libraries do under the hood, and does not speak directly to MCP servers. MCP calls land in `ai-gateway` with a Session JWT. Claude SDK model calls follow `LLM_EGRESS_DEFAULT` for `separate` Sessions, with `metadata.orca_llm_egress` as a per-Session override; Gateway-selected calls use `LLM_GATEWAY_URL`, a fresh Registry-minted JWT at query startup, and an `X-Orca-Session-Id` header. Outcome evaluations use the same gateway URL and fetch a JWT for each request. Registry internal calls use a ServiceAccount JWT in Kubernetes or the shared internal service token elsewhere. Keeping the harness narrow makes each replica safe to throw away.

## Output capture

The harness automatically captures of agent artifacts under
`/mnt/session/outputs/` (a FUSE-backed output mount on FUSE-capable
runtimes), driven from `Dispatcher.spawnRunner`. File resources are
materialized host-side via tarball prefetch only. See
[`../mount-strategies.md`](../mount-strategies.md) and
[`../output-capture.md`](../output-capture.md) for the user-facing model.

### Boot sequence

```
mintSessionCreds        -> SessionCredsMinter.mint({ workspaceId, sessionId })
runtime.acquire         -> SandboxRuntime.acquire({}) (FUSE-capable in prod)
mountSessionOutputs     -> /mnt/session/outputs (s3fs in FUSE; mkdir InMemory)
materializeResources    -> per-resource pickStrategy(...) → activate
runner.start            -> SessionRunner pumps AgentEvents into TranscriptStore
```

For public `AgentEvent` output, `SessionRunner` preserves a valid supplied
envelope ID as Transcript `Event.id` and its canonical subpath as the event
subpath. Both current production Harness emit boundaries complete every draft
event with a unique canonical ID and explicit path before the runner sees it;
the service-local output type requires both fields. Payload `id`/`uuid` and
correlation fields never select the envelope. The canonical envelope ID also
becomes `idempotencyKey` metadata.
Transcript-store deduplicates `Event.id` in a session-wide collision domain
independent of subpath. This does not alter native Claude SDK subpath replay or
make root in-sandbox replay include child public events.

An invalid supplied public subpath is not silently routed to the primary path:
the event pump fails and poisons its local runner. That failure alone does not
asynchronously tear down the runner; a later `submit()` uses the Dispatcher's
existing hard submit-failure lifecycle.

Current model spans retain `span.model_request_start/end` for Anthropic event
compatibility but carry an Orca `turn_model_summary` marker in their canonical
payload. Start identity is envelope-only; end has its own envelope ID and
references the start. Trusted configured provider/requested model values come
from the prepared execution snapshot. Registry strips these Orca-only fields
from the default Anthropic response while retaining them for `orca-beta`.

Shared current-producer conformance is owned by
[`../agent-harness.md`](../agent-harness.md). It covers only the
primary-path/coarse-summary boundary and does not establish child public
subpaths, per-logical-call observations, or full multiagent parity.

`mintSessionCreds` runs before `runtime.acquire` so the scoped creds flow
into the output mount (and memory-store mounts). File
resources always resolve to `TarballPrefetchStrategy` — the host fetches
blobs and streams them into the sandbox FS; the creds carry no file-blob
grant. `pickStrategy` reads `runtime.capabilities.supportsFuse` only for
non-file resource kinds.

(Cloud `colocated` Sessions use the same sequence: `EnvironmentSpec` also
selects the harness image, entrypoint and port, and after resources are mounted
the harness opens its HTTP/SSE session — `InSandboxHarness.start()` for
`claude_code`, the remote SDK worker for `codex_sdk` and `pi_sdk`. Resource
mounting for Registry-owned `colocated` sessions is the registry +
`session-runner`'s job; see [`../harness-modes.md`](../harness-modes.md).)

### Stop sequence

```
harness.stop + pump drain   -> quiesce agent writes and persisted events
OutputIndexer.indexSession  -> final walk, POST scoped internal files route per changed blob
mounts.deactivate           -> per-active-mount strategy.deactivate(...)
sandbox.destroy             -> SandboxRuntime release
```

The harness and event pump stop first so no later tool-triggered scan can race
mount teardown. `OutputIndexer.indexSession` still runs **before**
`mounts.deactivate`, while the FUSE mount (or sandbox FS) remains readable.
Harness stop, event-pump drain, and output scans have bounded grace periods so
a broken harness or stuck scan cannot prevent eventual sandbox teardown;
timeouts are logged and the remaining cleanup proceeds best-effort.
Incremental scans are drained (or cancelled through an `AbortSignal`) before
the final pass, which receives its own 60-second default budget so queued live
scans cannot consume the shutdown safety scan's time allowance.

During a live turn, `SessionRunner` also queues the same indexer immediately
after each persisted `agent.tool_result` or `agent.mcp_tool_result`, without
blocking later transcript events. A runner-local relative-path + SHA-256 cache
filters unchanged files, and the serialized shutdown scan remains the fallback
for background writes.
Each runner mounts a distinct S3 staging generation, so a later runner writing
the same filename still creates a new File id instead of being de-duplicated
against an earlier runner's artifact.

### Env knobs

| Var                       | Default           | Effect                                                                                                                                                                                                                |
| ------------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `S3_BUCKET`               | _unset_           | Bucket holding workspace-isolated files, memories, and execution outputs. When unset, the dispatcher skips creds-minting + output mounting + indexing entirely.                                                       |
| `S3_KEY_PREFIX`           | `managed-agents/` | Deployment root shared by all stores. Components append `workspaces/{workspace_id}/...`; callers cannot supply an absolute object prefix.                                                                             |
| `S3_ENDPOINT`             | _unset_           | S3 data-plane endpoint used by the S3 SDK and `s3fs`'s `-o url=` flag.                                                                                                                                                |
| `S3_FORCE_PATH_STYLE`     | `true`            | Path-style bucket addressing for host-side AWS SDK clients and sandbox s3fs mounts. Set `false` for AWS S3 virtual-hosted addressing.                                                                                 |
| `S3_STS_ENDPOINT`         | _unset_           | Optional STS endpoint override for `AssumeRole`. When unset, the AWS SDK selects its default STS endpoint; `S3_ENDPOINT` is never reused implicitly.                                                                  |
| `S3_REGION`               | _unset_           | Region passed to STS / S3 clients. Defaults via SDK when unset.                                                                                                                                                       |
| `S3_STS_ROLE_ARN`         | _unset_           | IAM role ARN to `AssumeRole` for per-session creds. **Production MUST set this.** When unset, sandbox FUSE creds require the explicitly enabled static-key fallback (DEV ONLY).                                       |
| `S3_ACCESS_KEY_ID`        | _unset_           | Optional static access-key for host-side S3 and STS clients. When the pair is absent, both use AWS default credential chain (including IRSA). Used directly by the sandbox dev fallback only when explicitly enabled. |
| `S3_SECRET_ACCESS_KEY`    | _unset_           | Pair to `S3_ACCESS_KEY_ID`.                                                                                                                                                                                           |
| `E2B_TEMPLATE_ID`         | _unset_           | Custom E2B template id (from `e2b template build`). When set, `E2BSandboxRuntime` uses it; otherwise the SDK default is used.                                                                                         |
| `SESSION_IDLE_TIMEOUT_MS` | `60000`           | Warm-runner retention timeout after a completed turn. The session is already `idle`; when it fires, the dispatcher stops the runner, destroys the sandbox, and clears `sandbox_handle_id`.                            |

The custom E2B template `orca-default` bakes in `s3fs-fuse + fuse3`, removes
the upstream blanket sudo rule, and permits only the constrained
`orca-s3fs-mount` helper plus `umount`. See
[`../../services/harness-server/sandbox-templates/orca-default/README.md`](../../../services/harness-server/sandbox-templates/orca-default/README.md)
for the template build + push flow.

The Orca OpenSandbox images also bake in `s3fs + fuse3` and run only through
`RuntimeClass/gvisor`. gVisor supplies `/dev/fuse` inside sandbox kernel; Pods
have no host device, `hostPath`, privileged container, `hostUsers`, or
`procMount`. `charts/opensandbox-patches` requires operator-owned repository or
legacy exact-image trust with an exact entrypoint array, both isolation/FUSE
extensions, and adds only `SETFCAP` and `SYS_ADMIN`. See
[OpenSandbox trust configuration](../../opensandbox/README.md#repository-and-exact-image-trust)
for repository defaults, replacement policy, and exact-image-only configuration. Runtime
probe checks binaries, opens gVisor device, and mounts/unmounts before returning
a usable sandbox. See
[`../../services/harness-server/sandbox-templates/orca-opensandbox/README.md`](../../../services/harness-server/sandbox-templates/orca-opensandbox/README.md).

## Memory mounts and the version watcher

Memory mounts reuse the same FUSE plane as output capture. When a session attaches
`{ type: 'memory_store', memory_store_id, access?, instructions? }`, Registry
derives the output-only path under `/mnt/memory/`; the dispatcher mounts each
store via `MemoryFuseStrategy` (or
`LocalMemoryStrategy` for AgentENV, Local, and `InMemorySandboxRuntime`) and starts a per-session
`MemoryVersionWatcher` that polls each store's workspace-isolated live S3
prefix every ~2 s, detecting writes and registering them through the scoped
`.../workspaces/{workspace}/sessions/{session}/memory-stores/{store}/memory-versions`
internal route.

### Boot sequence (output + memory mounts)

```
generationId                -> create once for this runner generation
mintSessionCreds            -> mint({ workspaceId, sessionId, generationId, memoryStores })
runtime.acquire             -> SandboxRuntime.acquire({}) (FUSE-capable in prod)
mountSessionOutputs         -> /mnt/session/outputs (s3fs in FUSE; mkdir InMemory)
materializeResources        -> per-resource pickStrategy(...) → activate
                               (memory_store → MemoryFuseStrategy mounts /mnt/memory/{store_name}/)
memoryWatcher.start         -> per-store poll loop; emits session.memory_conflict on CAS mismatch
runner.start                -> SessionRunner pumps AgentEvents into TranscriptStore
```

`mintSessionCreds` runs before `runtime.acquire`. The credentials grant writes
only to the exact execution output generation and to the attached memory-store
`live/` prefixes, with read-only stores receiving `GetObject` only. File inputs
are tarball-prefetched by the host and receive no S3 grant. `ListBucket` is
limited to those exact prefixes.

### Stop sequence

```
memoryWatcher.stop           -> cancels the poll loop BEFORE mount teardown
harness.stop + pump drain    -> quiesces agent writes and persisted events
OutputIndexer.indexSession   -> final walk, POST workspace/session-scoped files route
mounts.deactivate            -> per-active-mount strategy.deactivate(...) (memory + file)
sandbox.destroy              -> SandboxRuntime release
```

The watcher stops before the harness and both finish before mount
deactivation, so no in-flight poll, event, or output scan runs against a
torn-down FUSE mount. `OutputIndexer.indexSession` performs the final pass
after the harness is quiescent but while the mount remains alive.

### Conflict events

When the watcher's CAS check (`previous_sha256` vs cached value) fails on
the Registry's scoped memory-version route — i.e. the registry
returns `conflict: true` after applying last-writer-wins — the watcher
appends a `session.memory_conflict` event to the transcript stream:

```jsonc
{
  "kind": "session.memory_conflict",
  "store_id": "mems_…",
  "path": "plans/2026-q2.md",
  "observed_sha256": "…", // sha the watcher saw before the conflict
  "expected_sha256": "…", // sha the watcher had cached
  "written_by_session_id": "ses_winning",
}
```

SDK consumers can surface these to user code; agents themselves don't see
conflicts mid-turn. See
[`../memory-conflict-semantics.md`](../memory-conflict-semantics.md).

### Configuration

Memory mounts use the same `S3_KEY_PREFIX` root as output capture. The watcher's
2 s tick is fixed in `MemoryVersionWatcher` and reads no environment variable;
it bounds the cross-session consistency window.

The cross-session e2e test
(`services/harness-server/test/integration/memory-cross-session.spec.ts`)
is the validation target for this lifecycle: session A writes through the
sandbox FS at `/mnt/memory/{store}/`, the watcher registers the new
version, and session B reads the bytes back through its own mount.

## GitHub repository mounts and in-sandbox git

`github_repository` resource mounts use a host-side
`git clone` plus stream-into-sandbox flow, and a custom credential helper
that lets in-sandbox `git push` / `git fetch` round-trip through the
registry without the PAT ever reaching the sandbox. There is no new
library and no new service: a `GitWorker` and `WorkDirManager` are added
inside `harness-server`, and a public `POST /v1/git-creds` route is added
on `registry-service-ts` (see [`./registry-service.md`](./registry-service.md)).

### Lifecycle

1. **Session-spawn dispatch.** The `Dispatcher` filters `github_repository`
   entries from the mesh-internal session representation's `resources[]`.
   Public session responses omit `repo_ref`; the internal representation keeps
   `{ git_credential_id, url, checkout? }` for harness-only resolution
   (`services/harness-server/src/runner/dispatcher.ts`).
2. **Mint git-creds JWT.** The dispatcher calls
   `RegistryClient.mintGitCredsJwt({ workspaceId, sessionId, repoUrls })` — the
   audience is fixed inside that method and the lifetime comes from the
   server's `SESSION_JWT_TTL_SECS` (default 300s); neither is a parameter
   to get a session-scoped token. The JWT carries `aud='git-creds'`, the
   workspace + session ids, and the `repo_urls[]` allowlist (see
   `SessionJwtMinter` in
   [`./registry-service.md`](./registry-service.md)).
3. **Inject helper env.** The dispatcher writes
   `/etc/profile.d/orca-git-creds.sh` exporting `ORCA_GIT_CREDS_URL`
   (`config.gitCredsPublicUrl`) and `ORCA_GIT_CREDS_TOKEN` (the JWT) into the
   sandbox. The image registers
   `git config --system credential.helper /usr/local/bin/orca-git-creds` at
   build time. Without the per-session env pair the helper returns no
   credentials and Git falls back to anonymous access.
4. **Resolve PAT + clone (host-side).** For each repo, the dispatcher calls
   `RegistryClient.resolveGitCredential(...)` over the mesh-internal route to
   get the cleartext PAT. The PAT NEVER enters the sandbox. `GitWorker`
   (`services/harness-server/src/git/git-worker.ts`) runs `simple-git`'s
   `clone` with `--filter=blob:none --depth=1` (and `--branch <ref>` when the
   resource pins a checkout) into
   `${HARNESS_WORK_DIR}/sessions/{ws}/{ses}/repo-{N}/`. Post-clone, the
   remote URL is reset to the bare form so the embedded `username:password@`
   never lands in `.git/config`.
5. **Stream working tree into sandbox.**
   `GitCloneStrategy.activate(sandbox, resource)`
   (`services/harness-server/src/sandbox/mounts/git-clone.ts`) walks the
   work dir + `.git/` and `sandbox.files.write`s each file at
   `mount_path/<rel>`. The sandbox layer creates parent dirs implicitly.
6. **Agent runs git from bash.** With the helper registered + envs exported,
   `bash + git status / diff / push / fetch` works directly. The helper
   POSTs `{ protocol, host, path }` to `${ORCA_GIT_CREDS_URL}` with the
   bearer JWT; the registry resolves the PAT per-call and returns
   `{ username: 'x-access-token', password: <PAT> }`. PAT bytes live only
   in the helper's transient stdout — no persistence at any layer.
7. **Stop sequence.** `SessionRunner.stop` invokes
   `WorkDirManager.releaseSession(workspaceId, sessionId)`
   (`services/harness-server/src/git/work-dir.ts`), which `rm -rf`s the
   per-session work dir on the harness host. The JWT expires on its own.

### Git envs

| Var                    | Default                 | Effect                                                                                                                                                                                                                |
| ---------------------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HARNESS_WORK_DIR`     | `/var/tmp/orca-harness` | Host-side base dir for per-session ephemeral repos. `WorkDirManager` allocates `<base>/sessions/{ws}/{ses}/repo-{N}/` per repo and `rm -rf`s the session subtree on stop.                                             |
| `GIT_CREDS_PUBLIC_URL` | _unset_                 | Public registry URL the in-sandbox helper POSTs to (e.g. `https://api.example.com/v1/git-creds`). **Required when any session attaches a `github_repository` resource** — when unset the dispatcher refuses to spawn. |

### Custom E2B template requirement

GitHub mounts extend the same `orca-default` template output capture uses.
The Dockerfile adds `git`, `jq`, and bakes
`/usr/local/bin/orca-git-creds` (the credential helper script). **Operators
MUST rebuild + push the template (`e2b template build`) before in-sandbox
git push works in production.** See
[`../../services/harness-server/sandbox-templates/orca-default/README.md`](../../../services/harness-server/sandbox-templates/orca-default/README.md)
and [`../../services/harness-server/README.md`](../../../services/harness-server/README.md)
for the operator checklist.

### Git metrics

| Metric                                          | Type      | Notes                                                                                                          |
| ----------------------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------- |
| `harness_git_clone_seconds`                     | Histogram | Wall-clock seconds for `GitCloneStrategy.activate` (clone + walk + stream). p95 SLO < 30 s for repos ≤ 100 MB. |
| `harness_git_clone_total{workspace_id, result}` | Counter   | `result=ok` on success, `result=error` on any throw inside `activate`.                                         |

The corresponding registry-side `registry_git_creds_request_total{result}`
counter (success / `jwt_invalid` / `credential_mismatch` / `pat_unresolvable` /
`repo_unmatched`) is documented in
[`./registry-service.md`](./registry-service.md).

## Gaps

Everything this service does not do yet is in [`../roadmap.md`](../roadmap.md).

Pi-only direct credentials also accept `PI_SDK_PROVIDER_CREDENTIALS`, a JSON map of
provider IDs to `apiKeyEnv` and optional `baseUrlEnv` references. `GEMINI_API_KEY`,
`GEMINI_BASE_URL`, `ZAI_API_KEY` and `ZAI_BASE_URL` select Gemini and ZAI directly.
See [Pi SDK credentials](../libraries/pi-harness.md#credentials-and-native-gateway-transport).
