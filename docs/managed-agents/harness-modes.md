# Harness Modes — running the harness inside the sandbox

This document describes the pluggable harness mode framework introduced alongside the `claude_code` `colocated` path. Read [`agent-harness.md`](./agent-harness.md) first for the base `AgentHarness` interface contract, and [`deployment-topologies.md`](./deployment-topologies.md) for the canonical topology diagrams.

## The annotation

Agents carry two optional keys in their free-form `metadata` JSONB field — Anthropic Managed Agents-compatible, no additional public request field:

```jsonc
// agent.metadata
{ "harness": "claude_code", "mode": "colocated" }
```

| Key                | Values                                                                                                                                               | Default            |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `metadata.harness` | `pi_sdk` \| `codex_sdk` \| `claude_agent_sdk` \| `claude_agent_sdk_persistent` \| `claude_code` \| `codex` \| `cursor` \| `pi` \| `custom` \| `mock` | `claude_agent_sdk` |
| `metadata.mode`    | `separate` \| `colocated`                                                                                                                            | `separate`         |

The registry validates the annotation on every agent create and update (`agents.routes.ts`) using `resolveHarnessAnnotation` from `@orca/harness-catalog`. An unsupported `(harness, mode)` combination — for example `claude_agent_sdk`/`colocated` — is rejected with a `400`. Annotations are stored unchanged in `metadata` JSONB and snapshotted via `agent_versions`.

> `colocated` is the rename of the former `in_sandbox` mode value (same annotation key, same catalog, same bridge concept). `packages/harness-catalog/src/catalog.ts` declares `HarnessMode = 'colocated' | 'separate'` and rejects anything else with `metadata.mode must be one of: colocated, separate`. `in_sandbox` remains accepted as a **deprecated alias** that normalizes to `colocated` (see the catalog's deprecated-mode alias table, resolved by `normalizeHarnessMode`): `metadata.mode` is stored verbatim in the agent's `metadata` JSONB and snapshotted into `agent_versions`, no migration rewrites those rows, and a value that round-trips through `GET`/`PUT /v1/agents/:id` must not start returning `400`. Nothing downstream sees the alias — `resolveHarnessAnnotation` returns `colocated` — and new annotations should use `colocated`, which is why the validation error advertises only the current spellings. Anything below still labeled `in-sandbox` in an identifier name (`InSandboxHarness`, `harness/in-sandbox/*`, `@orca/sandbox-harness`) is the cloud `claude_code` / `codex_sdk` / `pi_sdk` HTTP bridge; other Registry-owned harnesses use `session-runner`.

Both keys absent → defaults (`claude_agent_sdk` / `separate`, today's behavior); `harness` present but `mode` absent → the harness's first supported mode from the catalog.

The effective harness type is immutable after creation, including the default on
agents without an annotation. Registry stores it in `agents.harness_type`, and a
Postgres trigger also rejects changing it. Select another harness by creating a
new Agent. Model changes create Agent versions; existing Sessions keep their
pinned version and validate overrides against that version's harness.
Version snapshots created before harness immutability retain their original
`metadata.harness` (or the historical Claude default when absent), even when
the migrated Agent's current harness differs. New snapshots stamp `harness_type`
and must match the immutable Agent identity.

`GET /apis/runtime.runorca.ai/v1/harnesses` returns the managed SDK harnesses,
supported models, effort levels, modes, and capabilities. The catalog describes
SDK compatibility; Gateway authorization still determines which models a
deployment grants. `codex_sdk` requires an `openai` model from its pinned catalog;
`pi_sdk` admits pinned `anthropic`, `openai` and `deepseek` models;
`claude_agent_sdk` and its persistent variant require Claude models. An omitted
model provider is inferred from the selected harness. Native CLI provider model
configuration retains its existing operator-defined behavior.

## The harness catalog (`@orca/harness-catalog`)

`packages/harness-catalog/src/catalog.ts` is the single source of truth for:

- which modes each harness supports,
- the default sandbox image for `colocated` harnesses, and
- the HTTP port used by the cloud in-sandbox bridge.

`packages/harness-catalog/src/model-controls.ts` also owns the Claude fast-mode
allowlist plus per-model effort levels/defaults shared by Registry,
`harness-server`, and `sandbox-harness`. Each boundary still validates
independently; sharing the capability data keeps their fail-closed decisions
aligned when the supported model catalog changes.

```
harness                       supportedModes    defaultImage                                        port
claude_agent_sdk              [separate]        null                                                null
claude_agent_sdk_persistent   [separate]        null                                                null
claude_code                   [colocated]       ghcr.io/orca-ae/sandbox-harness-claude-code:latest  4096
pi_sdk                        [separate, colocated] ghcr.io/orca-ae/sandbox-harness-claude-code:latest  4096
codex_sdk                     [separate, colocated] ghcr.io/orca-ae/sandbox-harness-claude-code:latest  4096
codex                         [colocated]       ghcr.io/orca-ae/sandbox-harness-codex:latest        4096
cursor                        [colocated]       ghcr.io/orca-ae/sandbox-harness-cursor:latest       4096
pi                            [colocated]       ghcr.io/orca-ae/sandbox-harness-pi:latest           4096
custom                        [colocated]       ghcr.io/orca-ae/sandbox-harness-custom:latest       4096
mock                          [colocated]       null                                                null
```

`registry-service-ts`, `harness-server`, and `sandbox-harness` import `@orca/harness-catalog` via `workspace:*`. The catalog is the only place where images and ports are declared; no other code hard-codes them. The `defaultImage`/`port` columns configure the shared cloud `claude_code` / `codex_sdk` / `pi_sdk` HTTP bridge. Other CLI image entries are placeholders; their implemented providers run through `session-runner` over the WS tunnel. `codex_sdk` shares the Claude sandbox image, entrypoint and port in cloud colocated mode. Its cloud separate mode uses a host-side SDK worker in harness-server; self-hosted mode uses session-runner.

## The two modes

Two orthogonal axes govern execution: `metadata.mode` decides where the **agent loop** runs (this section); the environment `target` (`cloud` | `self_hosted`) decides where the **sandbox** runs (unchanged, orthogonal — see [`deployment-topologies.md`](./deployment-topologies.md)).

### `separate` (default)

For a cloud `claude_agent_sdk` Session, the agent loop (`ClaudeAgentSdkHarness`)
runs in `harness-server` and dispatches tool calls through `SandboxRuntime`.
Model calls use the deployment's `LLM_EGRESS_DEFAULT` (`direct` by default).
A Session's `metadata.orca_llm_egress` value overrides that default in either
direction. Gateway-selected Sessions send model calls and outcome evaluations
through `LLM_GATEWAY_URL` with a Session-scoped JWT. Self-hosted Sessions use
`session-runner`, including the two Claude SDK providers; see the ownership
table below.

For a cloud `codex_sdk` Session with `metadata.mode: "separate"`, the Codex SDK
and its executable run on the harness-server host. Every exposed tool is relayed
to the Session Sandbox or its configured MCP server. The SDK worker has a private
home and working directory; native shell, file editing, and web tools are disabled.
Managed Skills use the shared read-only Sandbox tree and per-agent catalog;
Codex reads `SKILL.md` and referenced files through Orca's permission-controlled
`read` tool. Matching `block_skills` rules remove bundles before delivery.
The separate Codex deployment accepts stateless `request`, `tool_call`, and
`tool_result` guardrails plus stateful `request` rules. Request budgets use durable
Registry usage and pending markers; they cap the next turn, not an in-flight turn.
Soft approval thresholds are rejected, and daily budgets check recorded spend
without reserving cross-Session capacity. Other stateful phases, subagent-scoped
rules, and `response`, `llm_request`, and `llm_response` are rejected before
execution. The [server adapter](services/harness-server.md#codex-sdk-separate-execution)
describes accounting failure and recovery behavior.
Managed `codex_sdk` supports durable stateful request budgets in both modes.
For cloud Sessions in either mode, harness-server evaluates policy and acknowledges
Registry writes. For self-hosted colocated Sessions, Registry evaluates policy before
SDK submission and commits SDK usage itself through the private managed snapshot.
Soft request approval thresholds, stateful tool phases and subagent budgets are refused.
SDK usage remains authoritative in both modes when the Gateway usage sink is enabled.

Self-hosted Codex Sessions require the colocated mode.
Registry rejects incompatible Session creation and Environment target changes,
and rechecks the pinned deployment during execution preparation and runner
snapshot resolution. Updating a Session cannot replace its Agent identity or
Environment, and Session metadata does not override the Agent's harness mode.

Both Claude modes map primary/subagent `model.effort` to typed SDK options and
map uniform `model.speed` to session-wide `settings.fastMode`. Mixed coordinator
speeds are rejected by Agent/Session mutation and rechecked during execution
preparation. `fast` is accepted only for `claude-opus-5-5`, `claude-opus-5`,
and `claude-opus-4-8`; unknown or older models fail closed. Fast turns must report
`fast_mode_state: "on"` and each
provider response must report `usage.speed: "fast"`; streaming responses are
checked at `message_start` before any content delta is forwarded. This separates
SDK opt-in from provider-observed activation and rejects silent gateway/model
downgrades.

Registry resolves known model effort defaults (`medium` for Opus 5.5, `high` for
the other known models) in stored snapshots and
public Agent/Session responses. Explicit effort is validated before storage and
again before runtime startup:

| Models                                                         | Supported effort levels                 |
| -------------------------------------------------------------- | --------------------------------------- |
| Claude Opus 5.5, Opus 5, Opus 4.8, Opus 4.7, Sonnet 5, Fable 5 | `low`, `medium`, `high`, `xhigh`, `max` |
| Claude Opus 4.6, Sonnet 4.6                                    | `low`, `medium`, `high`, `max`          |
| Claude Opus 4.5                                                | `low`, `medium`, `high`                 |

SDK models and explicit effort controls are validated against their harness catalog before storage and before runtime startup.

### `colocated`

The agent loop and its tools run together in the sandbox. Cloud `claude_code`
uses `harness-server` and the `@orca/sandbox-harness` HTTP bridge, which provides
resource mounts, Skills, native SDK subagents, output capture and stateful budgets.
Cloud `codex_sdk` uses the same image, HTTP/SSE transport, resource mounts and
sandbox lifecycle. Its host adapter retains native checkpoint, tool policy and
usage handling; only the SDK worker executes inside the sandbox. All self-hosted
Sessions and other cloud colocated harnesses use Registry and `session-runner`
over the outbound WS tunnel.
Each Session has exactly one execution owner; see "Dispatcher routing" below.

## The harness catalog and Environment — image resolution

Registry-owned cloud Sessions run in the common **Orca Environment image**
(`services/environment-image/`, containing environment-worker and session-runner).
The configured environment launcher provisions this image and starts its worker.
Self-hosted workers run on the operator's host.

Cloud `claude_code` uses `sandbox/environment-spec.ts: buildEnvironmentSpec`
to select its catalog image, entrypoint and HTTP port. Cloud separate Sessions
use the configured tool sandbox. The fields passed to `SandboxRuntime.acquire()` are:

| Field                 | Purpose                                                                                          |
| --------------------- | ------------------------------------------------------------------------------------------------ |
| `image`               | Container image to boot (for `colocated`: the harness image).                                    |
| `entrypoint`          | Per-image harness server command; overrides the OpenSandbox default image entrypoint.            |
| `exposePorts`         | Ports the sandbox must expose so `endpoint(port)` can resolve them.                              |
| `harnessEnv`          | Sandbox process environment assembled by the dispatcher, including Gateway credentials.          |
| `fileUploadOwnership` | OpenSandbox account names resolving to agent UID/GID 1000 (`node` colocated, `ubuntu` separate). |
| `target`              | Environment target (`cloud` or `self_hosted`); execution ownership is defined below.             |

The Environment record retains optional `image` and `target` fields, but
`colocated` currently rejects an Environment-supplied image before sandbox
acquisition. Remote providers expose no sealed setup namespace, so a
custom image could otherwise leave a process racing trusted resource and Skill
materialization. Operators change the Environment image at deployment time
instead. Any operator-trusted Environment image must preserve the corresponding
account-to-1000 mapping because execd rejects numeric owner strings.
`separate` mode does not use the Environment image. Any execution that actually
acquires a managed sandbox also rejects Environment package installers, because
an installer hook could likewise leave a process racing setup.

## The cloud Claude Code in-sandbox bridge

> `InSandboxHarness` and `@orca/sandbox-harness` serve cloud `claude_code`
> Sessions. Registry-owned Sessions use the outbound runner tunnel. Image and
> port declarations alone do not establish a working provider in an image.

The in-sandbox path skips only the separate-mode Claude Agent SDK MCP rewrite
and host-side `agent_toolset` construction. It uses the same session-resource
mount machinery as `separate`: file, memory-store, and GitHub resources are
prepared in the harness-image sandbox before `InSandboxHarness` opens its HTTP
session, and `SessionRunner.stop()` owns the same watcher, mount, work-dir, and
sandbox cleanup ordering. Output capture remains a sandbox-local
`/mnt/session/outputs/` directory because the harness image owns the agent
process; `OutputIndexer` registers artifacts after tool completion and performs
a final safety scan when the runner stops. Before the image starts, the
dispatcher injects a fail-closed write policy covering the output directory,
the read-only Skill tree, and the attached resource paths; the in-image Claude
SDK refuses to start unless it can enforce that boundary. The bridge forwards
the agent system prompt plus the compact per-agent Skill catalog and maps the
managed `agent_toolset` allowlist and permission
policies to the Claude SDK's built-in `tools` / `allowedTools`, so declared
`always_allow` filesystem tools run headlessly without granting access to
undeclared built-ins. The in-sandbox bridge does not implement the
Managed Agents tool-confirmation round trip, so `always_ask` tools are omitted
from the SDK tool list (as are `always_deny` tools) instead of falling through
to an SDK permission mode that could auto-approve them.

The harness-image entrypoint waits for the dispatcher to finish mounting and
probing session resources before it creates the long-running bubblewrap mount
namespace. This keeps later FUSE mounts visible to the agent while preserving a
read-only root outside the policy's writable output, memory, and repository
paths.

At spawn time, the dispatcher's trust check (`sandbox/environment-trust.ts`) and
`buildEnvironmentSpec` resolve a `colocated` sandbox as:

```
require environment.image == null
image       = SANDBOX_HARNESS_CLAUDE_CODE_IMAGE ?? HARNESS_CATALOG[harness].defaultImage
entrypoint  = HARNESS_CATALOG[harness].entrypoint   (when not null)
exposePorts = [ HARNESS_CATALOG[harness].port ]
target      = environment.target                    (when set)
```

The `EnvironmentSpec` fields that reach `SandboxRuntime.acquire()`:

| Field                 | Purpose                                                                                                                                                                                          |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `image`               | Container image to boot (for `colocated`: the harness image).                                                                                                                                    |
| `entrypoint`          | Per-image harness server command; overrides the OpenSandbox default image entrypoint.                                                                                                            |
| `exposePorts`         | Ports the sandbox must expose so `endpoint(port)` can resolve them.                                                                                                                              |
| `harnessEnv`          | Extra env injected into the sandbox: gateway LLM base URL and per-session JWT (`LITELLM_API_BASE` / `LITELLM_API_KEY`), the write policy, the output-capture dir, and the git-creds URL + token. |
| `fileUploadOwnership` | OpenSandbox account names resolving to agent UID/GID 1000 (`node` in-sandbox, `ubuntu` separate).                                                                                                |
| `target`              | Environment target (`cloud` or `self_hosted`); execution ownership is defined below.                                                                                                             |

The Environment record retains optional `image` and `target` fields, but
`colocated` currently rejects an Environment-supplied image before sandbox
acquisition. Remote providers expose no sealed setup namespace, so a
custom image could otherwise leave a process racing trusted resource and Skill
materialization. Operators change the catalog image at deployment time instead.
Any operator-trusted catalog image must preserve the corresponding account-to-1000
mapping because execd rejects numeric owner strings.
`separate` mode does not use the Environment image. Any execution that actually
acquires a managed sandbox also rejects Environment package installers, because
an installer hook could likewise leave a process racing setup.

`SandboxHandle.endpoint(port)` resolves a reachable URL for a port the sandbox exposes. It is optional on the interface; only runtimes that actually expose ports implement it. `InSandboxHarness` calls it at `start()` to obtain the URL before opening the `DialInTransport`.

```
client ─HTTP→ registry ─append(user.*, producedBy=client)→ transcript-store ← source of truth
                                                                  │
harness-server (Dispatcher → SessionRunner)                       │
  ├ mode=separate   → ClaudeAgentSdkHarness  (unchanged)         │
  └ mode=colocated  → InSandboxHarness ── SYNC ──────────────────┘
      HarnessChannel (transport-agnostic):
        open(opts) / events(): AsyncIterable<RawSandboxEvent> / submit() / stop()
      events() → mapSandboxEvent → AgentEvent → pumpEvents.append()
```

`SessionRunner.pumpEvents` is the sync mechanism. For each `AgentEvent` emitted by
`InSandboxHarness.events()`, the production mapper/push boundary has already
completed a canonical ID and explicit primary path. The strict service-local
output becomes Transcript `Event.id`/`subpath` directly, and its event ID also
becomes `idempotencyKey` metadata. Payload IDs never select the envelope.
`transcript-store` remains the source of truth and deduplicates `Event.id` in a
session-wide collision domain independent of subpath; the sandbox harness is
ephemeral (in-memory only).

An invalid supplied public subpath is not coerced to the primary path: it fails
the event pump and poisons the local runner. It does not independently trigger
asynchronous teardown; a later `submit()` takes the Dispatcher's existing hard
submit-failure path.

### `@orca/sandbox-harness`

`services/sandbox-harness/` is a TypeScript HTTP server that runs inside the harness image. It is:

- **Ephemeral** — all session state is in-memory; durability is the bridge's job.
- **Built into per-harness Docker images** — the catalog's `defaultImage` references these.
- **Provider-dispatched** — `GET /v1/harnesses` lists available providers; `POST /v1/sessions` selects one by `agent` name.

Guardrails resolve at the same point as the tool mapping above and share its
limitation ([`guardrails.md`](./guardrails.md) is the reference for guardrail
enforcement). Because the bridge decides tool exposure once, up front, rather
than gating each call, name-keyed stateless guardrails run there: one resolving
to `ask` or `deny` removes the tool from the list, exactly as a permission
policy does. Argument-dependent stateless guardrails cannot resolve before a
tool input exists; exposure-time evaluation uses an unavailable-input sentinel,
and a matching rule that reads arguments resolves to `ask`, removing the tool
because no confirmation round trip exists there. The host evaluates the PII
screen's `request` leg once per turn. The sandbox protocol has no
model-endpoint interceptor for its `llm_request` leg, so runtime preparation
rejects any `in_sandbox` configuration that declares that phase rather than
starting with an unenforced rule. Of the stateful guardrails, only budgets
declaring the `request` phase are enforced, and `request` has no confirmation
vehicle for their ask steps: the unpriced check follows its `on_unpriced`
parameter (the default `ask` degrading to deny after the first unpriced turn),
soft thresholds do not fire, and hard caps fire unchanged. Stateful guardrails
that never declare `request` are **inert** under `colocated`, and runtime
preparation emits a session warning event naming each configured rule it cannot
enforce under the requested mode. The two topologies therefore differ: under
`separate` a budget denies the individual tool call that exceeds it; under
`colocated` it denies the next turn.

Wire API consumed by `DialInTransport`:

| Method   | Path                             | Notes                                                                                                                                    |
| -------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `POST`   | `/v1/sessions`                   | Body: `{ agent, model?, modelSpeed?, modelEffort?, agents?, systemPrompt?, tools?, allowedTools?, replay? }` → `201 { id, status, ... }` |
| `POST`   | `/v1/sessions/:id/events`        | Body supports `user.message` and `user.custom_tool_result`                                                                               |
| `GET`    | `/v1/sessions/:id/events/stream` | SSE; replays full history on every (re)connect                                                                                           |
| `DELETE` | `/v1/sessions/:id`               | Teardown                                                                                                                                 |

The `claude` provider (in `services/sandbox-harness/src/providers/claude.ts`) runs `@anthropic-ai/claude-agent-sdk`'s `query()` in-process. Managed `model.effort` maps to the SDK's typed primary/subagent `effort` options. Managed `model.speed` maps to inline `settings.fastMode`; every fast turn verifies both SDK `fast_mode_state` and provider `usage.speed`, failing before streamed output when either shows a standard-speed fallback. Claude fast mode is session-wide, so coordinator rosters with mixed primary/subagent speeds are rejected before execution; uniform rosters map exactly. On a warm sandbox, its runtime captures the SDK-generated top-level `session_id` from the first stream and passes it as `resume` on each later turn. `persistSession: true` keeps native Claude transcript, tool state, and compaction context in sandbox-local SDK configuration. The native id is runtime-local and never stored in Orca durable state; Orca's `sess_*` wire id is never supplied as an SDK `sessionId`. After a cold subprocess or sandbox rebuild, the sandbox-local SDK state is unavailable, so the existing provider-agnostic Orca transcript replay remains the first-turn preamble mechanism. LLM endpoint and key come from the process environment only (`ANTHROPIC_BASE_URL` / `ANTHROPIC_API_KEY`; an optional `LITELLM_API_BASE` / `LITELLM_API_KEY` can redirect to the gateway when `ANTHROPIC_BASE_URL` is not already set — and `harness-server` sets both for a `colocated` sandbox when Registry mints the Session's gateway JWT (`dispatcher.ts` via `harness/in-sandbox/llm-env.ts`); a failed mint is logged and the sandbox starts without them).

The SDK is pinned to `0.3.283`; its bundled Claude Code runtime recognizes
Claude Opus 5.5 and the other supported fast-mode models.

Both Claude runtimes set typed SDK options `promptSuggestions: false` and a
fixed `title`. Managed Agents exposes no prompt-suggestion event or SDK-local
title surface; those features otherwise issue separate structured-output
requests without inheriting Messages API speed. The separate runtime also
assigns each runner an ephemeral `CLAUDE_CONFIG_DIR`: SDK `0.3.x` mirrors
`SessionStore` transcripts through local files, and isolating/cleaning that
directory prevents stale local session ids from colliding after runner restart.

Anthropic fast mode requires the `fast-mode-2026-02-01` beta header plus the
top-level Messages API `speed: "fast"` field. A custom LLM gateway must preserve
both request controls and return `usage.speed` unchanged. The managed runtime
does not use undocumented first-party URL overrides: incompatible gateways fail
explicitly instead of being presented as fast mode.

`services/sandbox-harness/test/claude-sdk-fast-mode-wire.spec.ts` executes the
pinned SDK against a hermetic custom `ANTHROPIC_BASE_URL` without either
`CLAUDE_CODE_SKIP_FAST_MODE_ORG_CHECK` or
`_CLAUDE_CODE_ASSUME_FIRST_PARTY_BASE_URL`. It asserts `fast_mode_state: "on"`,
the beta header, request `speed: "fast"`, and result `usage.speed: "fast"` on
the real SDK wire path. A delegated-Agent case also captures the child request
and proves its model-specific effort plus session-wide fast controls; a standard
case proves the fast beta token and request field stay absent.

`services/harness-server/test/unit/claude-sdk-fast-mode-wire.spec.ts` exercises
the separate runtime through the real bundled binary, including a
`SessionStore`-backed cold resume whose second provider request contains the
first turn, and an HTTP 400 regression proving provider errors are not replaced
by speed-validation errors.

### Stream-parity semantics

Both harness backends emit the **Claude RECEIVED taxonomy**; `@orca/agent-event-contract` owns the shared event-kind vocabulary, and `harness/event-kinds.ts` remains a compatibility facade re-exported from `harness/agent-harness.ts`. `mapSandboxEvent` (`harness/in-sandbox/event-mapper.ts`) translates the sandbox-native wire into that vocabulary, returning **zero or more** `AgentEvent`s per sandbox event:

| Sandbox event          | Mapped to                                                                                         | Reason                                                                       |
| ---------------------- | ------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `user.message`         | **dropped** (`[]`)                                                                                | Registry already appended the client's user message before the turn started  |
| `agent.message`        | `agent.message`                                                                                   | Forwarded with `id` preserved                                                |
| `agent.thinking`       | `agent.thinking`                                                                                  | Extended-thinking surfaced as its own event, not folded into `agent.message` |
| `agent.tool_use`       | `agent.tool_use`                                                                                  | Forwarded with `id` preserved                                                |
| `agent.tool_result`    | `agent.tool_result`                                                                               | Forwarded with `id` preserved                                                |
| `agent.usage`          | `agent.usage` (internal)                                                                          | Per-message provider usage, retaining model and managed-subagent attribution |
| `session.status_idle`  | `agent.usage` (internal) + `span.model_request_end` + `session.status_idle{stop_reason:end_turn}` | Usage → internal sink + public span; terminal turn boundary                  |
| `session.status_error` | `session.error{will_retry:false}` + `session.status_idle{stop_reason:retries_exhausted}`          | Typed error + terminal boundary; the session stays idle (not killed)         |
| unknown types          | **dropped** (`[]`)                                                                                | Future-proof; never throws                                                   |

The bridge (`InSandboxHarness`) also emits one `session.status_running` + one `span.model_request_start` at the start of every `user.message` turn. Each mapped event carries the sandbox's original canonical `id` where applicable; fan-out siblings that share one sandbox frame carry ids derived from the raw frame id (`<frameId>:model_end` on `span.model_request_end`, `<frameId>:idle` on `session.status_idle`, while the raw id rides on the diverted `agent.usage`). The mapper/push boundary validates or generates every envelope and sets the explicit primary subpath before exposing it. Transcript preserves that as `Event.id`/`subpath`, and copies the same event ID to `idempotencyKey` metadata. The store deduplicates `Event.id`, not `idempotencyKey`; DialInTransport's seen-ID set separately suppresses raw full-buffer reconnect frames before mapping. The current raw sandbox wire supplies no child subpath, so these bridge events remain primary.

The model pair keeps those existing span kinds but is explicitly a coarse
`turn_model_summary`, not a provider-call ledger. Start identity exists only on
the event envelope. The end has a distinct envelope ID and references the start
through `model_request_start_id`; both carry trusted configured `provider` and
requested `model` when the execution snapshot supplies them. A raw
`session.status_error` has no terminal usage/model fact, so it still maps only
to `session.error` + idle and does not fabricate a failed model end. The default
Anthropic event projection removes the summary marker/provider/model/cost fields;
`orca-beta` retains them. A `requires_action` idle is only a phase boundary: it
keeps the original summary start correlation, and the final custom-tool result
resumes with `session.status_running` before the eventual correlated end.

Both current Claude producers run the shared primary-path/coarse-summary
conformance gate described in [`agent-harness.md`](./agent-harness.md). It does
not establish child public subpaths, per-logical-call observations, or full
multiagent parity.

The sandbox harness emits streamed Claude usage once at `message_stop`, after
merging the initial counters with the final cumulative `message_delta` usage.
Early assistant content snapshots do not double-count the same message.
Assistant-only frames retain their usage fallback; terminal aggregate usage is
accounted only when no per-message usage has been emitted. Terminal Claude
usage remains available for the public turn-summary span.
The shared `services/harness-server/src/usage-normalization.ts` helper preserves
the SDK's nested 5m/1h `cache_creation` counters for both harness modes and the
public model-request span. The current `orca-ai-gateway` streaming path preserves
that nested object through generic usage metadata. The helper also accepts legacy
or third-party Anthropic-compatible frames that expose only the flat
`cache_creation_input_tokens` total, assigning it to the 5m bucket so
cache-creation usage is not dropped. The public span exposes the normalized
5m/1h sum as `cache_creation_input_tokens`.

The current in-sandbox ingress does not implement `user.interrupt`,
`user.tool_confirmation`, or `user.define_outcome`. The host bridge rejects
those events as unapplied before recording acceptance; it never rewrites them
to an empty `user.message`. Custom-tool results are accepted only when the
bridge has observed the matching `agent.custom_tool_use` id. That raw tool-use
frame must carry a canonical `evt_...` ID; mapper validation failures propagate
through `SessionRunner` and poison the runner rather than masquerading as a
successful end of the event stream.

The `separate` (Claude SDK) backend produces the same vocabulary directly: it decomposes each SDK `assistant` frame into `agent.thinking` + a text-only `agent.message` + `agent.tool_use`/`agent.mcp_tool_use`, surfaces tool results as `agent.tool_result`/`agent.mcp_tool_result`, maps `compact_boundary` → `agent.thread_context_compacted`, and opens each accepted turn with `session.status_running` + `span.model_request_start`. `session.status_idle{requires_action}` pauses that turn without closing its model summary; accepting the final outstanding tool confirmation/result emits another `session.status_running` phase, and the eventual result emits `span.model_request_end` before terminal `session.status_idle{end_turn|retries_exhausted}`. Each SDK `api_retry` frame becomes `session.error{will_retry:true}` + `session.status_rescheduled`. The model pair uses the same `turn_model_summary` payload and trusted configured provider/model semantics as the in-sandbox path; SDK retries remain lifecycle facts inside that aggregate rather than separate inferred model calls. Setup/mount failures surface as `session.error{type:setup_failed}` + `session.status_idle{retries_exhausted}` (recoverable on the next event) rather than the old non-Claude `session.setup_failed`.

### Transport abstraction

`InSandboxHarness` is transport-agnostic. It depends only on `HarnessChannel` / `HarnessTransport` (`harness/in-sandbox/transport.ts`):

```ts
interface HarnessChannel {
  events(): AsyncIterable<RawSandboxEvent>; // reconnect-safe; deduped by event id
  submit(event: UserEvent): Promise<void>;
  stop(): Promise<void>;
}
interface HarnessTransport {
  open(opts: OpenSessionOptions): Promise<HarnessChannel>;
}
```

**`DialInTransport`** (implemented, `harness/in-sandbox/dial-in.ts`): calls `SandboxHandle.endpoint(port)` to get the reachable URL, then opens an HTTP/SSE connection to the in-sandbox server. Used for cloud sandboxes (E2B / OpenSandbox) and for local/in-memory integration tests where `endpoint()` points at an in-process fake server. Maintains a `Set<string>` of seen event ids to deduplicate the sandbox's full-buffer SSE replay on reconnect.

**Self-hosted execution** uses `session-runner` and Registry's outbound runner
tunnel. It does not use `InSandboxHarness` or require a
`SelfHostedTransport` adapter. The runner advertises its registered providers
and receives Session requests over that tunnel.

The **outbound channel is a WebSocket**: the runner dials the public `WS /v1/tunnels/runners/:runnerId` endpoint and the registry pushes framed HTTP requests back down the tunnel. The endpoint uses binding-token auth, is loopback-only by default, fails closed on owner resolution, and registers each runner under its resolved owner. See [`services/registry-service.md`](./services/registry-service.md) ("Runner tunnel (public, token-authed)"). The outbound runner binary is `services/session-runner/`, and session→runner assignment is the distributor's claim-based dispatch.

**Runner-side turn contract.** The framed HTTP request the owner-pod `SessionEventBridge` pushes down the tunnel for each turn is a streaming `POST` to the path `RUNNER_TURN_PATH` (`/v1/runner/turn`), carrying the `user.*` event JSON as the body, `Content-Type: application/json`, and the `RUNNER_SESSION_HEADER` (`X-Orca-Session-Id`) scoping the turn — both constants are exported from `services/registry-service-ts/src/tunnel/session-event-bridge.ts`, the single source of truth. The runner answers `200` and streams the turn's agent events back as the response body in **newline-delimited JSON** (one event object per line, each with a string `type`; an optional `id` and `subpath`). The bridge persists each line to the transcript as `producedBy=harness`, awaited and in order (persist-before-forward), so a non-2xx status or a malformed line is dropped, not persisted. The contract is served by `services/session-runner/` (`register-handlers.ts` registers `POST /v1/runner/turn`) and exercised end-to-end by `packages/e2e-tests/test/self-hosted-e2e.spec.ts` (`pnpm e2e:self-hosted`, against the local stack); `FakeRunner` ws-peers remain the bridge-side test doubles in the bridge's unit and Kafka/Postgres integration specs. The bridge itself is complete and the contract is frozen.

### Reconnect deduplication

The `@orca/sandbox-harness` SSE stream replays its entire in-memory event buffer on every (re)connect. `DialInTransport` guards against double-delivery with a `Set<string>` of seen event ids: any event whose `id` has already been yielded is silently skipped. This is the single most important correctness property of the transport.

## Dispatcher routing

Registry and harness-server use the same `resolveExecutionOwner(target, selection)`
rule from `@orca/harness-catalog`:

| Environment target | Session-pinned harness and mode                                                    | Execution owner                  |
| ------------------ | ---------------------------------------------------------------------------------- | -------------------------------- |
| `cloud`            | Claude SDK, `separate`                                                             | harness-server                   |
| `cloud`            | `codex_sdk` or `pi_sdk`, `separate` or `colocated`                                 | harness-server                   |
| `cloud`            | `claude_code`, `colocated`                                                         | harness-server / sandbox-harness |
| `cloud`            | other `colocated` harnesses                                                        | Registry / session-runner        |
| `self_hosted`      | supported harness/mode combinations (`codex_sdk` and `pi_sdk` require `colocated`) | Registry / session-runner        |

Registry loads the mode and provider from the Session's fixed Agent version,
validating new snapshots against the immutable Agent identity and preserving
legacy version selections. The internal execution-owner
route uses the same binding loader as distribution. Archive does not change
routing, missing workers do not transfer ownership, and invalid persisted
bindings are rejected.

The harness-server dispatcher reads that route before each user event, including
interrupts. Registry-owned events are acknowledged only to its event source;
it does not mark them processed or produce terminal events. It does not cache
ownership across Session updates.

For sessions it owns, `harness/registry.ts: selectHarness` looks up a registered
factory by `selection.harness`. Duplicate registrations and unknown providers
are errors. `claude_agent_sdk_persistent` has a session-runner implementation;
a cloud `separate` selection fails harness-server setup explicitly.

`HARNESS_CAPABILITIES` declares the default mode's model policy, managed-feature
admission, model controls, cloud execution ownership, host tool/MCP setup, and
guardrail policy. `resolveHarnessCapabilities(selection)` applies the pinned
mode's overrides, and execution ownership and runtime setup consume that resolved
policy. `validateHarnessGuardrails` rejects unsupported phases, stateful rules,
and subagent scopes when the harness declares a guardrail policy. SDK discovery
and feature validation consume the same capabilities. Legacy provider model,
feature, and guardrail policies remain provider-defined.

The `InSandboxHarness` HTTP/SSE adapter serves cloud `claude_code` and is
exercised by the OpenSandbox real-agent suite. Registry-owned Sessions do not
select it. Provider IDs come from `HARNESS_CATALOG`, including distinct
`claude-code`, `claude-sdk-persistent`, `codex`, and `codex-sdk` identities.

### `codex`, `cursor`, `pi`, and `custom` — native-CLI providers on the runner

`codex`, `cursor`, and `pi` are catalogued as `colocated` (image + port) for uniformity, but their production path is the **session-runner native-CLI provider**, not an in-sandbox `@orca/sandbox-harness` image. The registry's snapshot resolver derives `provider` from this same catalog (`resolveAgentProvider` → `harnessToProvider`), stamping `provider: 'codex'` / `provider: 'cursor'` / `provider: 'pi'` on the credential-free snapshot; the runner's `ProviderRegistry` resolves that key to `CodexCliHarness` / `CursorCliHarness` / `PiCliHarness`, which boot the real CLI as a long-lived child **inside the session sandbox** via `SandboxHandle.spawn` and drive it over the CLI's own stdio protocol (codex: JSON-RPC `app-server`; cursor: `cursor-agent --print --output-format stream-json`; pi: newline-delimited-JSON `--mode rpc`). The native-CLI tool-bridge is wired as the CLI's tool surface (codex + cursor: an `orca` MCP server; pi: a pi **extension** loaded via `--extension`), so the model's orca tools resolve inside the sandbox. The runner advertises `codex` / `cursor` / `pi` in its tunnel hello (`ProviderRegistry.providerNames()`), and the distribution capability-match validates the snapshot's provider against that advertisement. A fifth native-CLI provider, `claude-code` — the headless `claude` binary driven over stream-json stdio — is registered, advertised, and reached the same way: `harnessToProvider('claude_code')` resolves to it, so a self-hosted agent annotated `metadata.harness: claude_code` runs the real CLI. Its cloud path uses the sandbox-harness SDK bridge described above. The SDK path keeps its own annotations (`claude_agent_sdk`, `claude_agent_sdk_persistent`), so the two are chosen per agent and never substituted for one another; a turn driven by the SDK and one driven by the CLI look nearly identical on the wire, which is why the distinction lives in the snapshot's `provider` rather than in the event stream. See [`architecture.md`](./architecture.md) (native-CLI providers) and `services/session-runner/src/harness/{codex,cursor,pi}/`.

`custom` is the **generic** native-CLI provider — the escape hatch that boots ANY operator-declared CLI without a bespoke provider. It is catalogued identically (`colocated`, `ghcr.io/orca-ae/sandbox-harness-custom:latest`, port `4096`) and resolves the same way: `harnessToProvider('custom')` → `'custom'`, stamped on the snapshot, resolved by the runner's `ProviderRegistry` to `CustomCliHarness` (registered + advertised as `custom` alongside the other native-CLI providers). Instead of a hard-coded stdio protocol, the harness reads a **declarative** spec that rides the snapshot as `custom_spec`: `command` / `argv` (with `{...}` placeholders the launcher substitutes) / `env` / `cwd` / a `stdin` template / a `stdout`→`AgentEvent` mapping (`text` or `jsonLine` mode) / an optional `approvals` opt-in that routes a CLI-raised approval to the uniform transcript gate. The spec is a **structured object** (JSON, not YAML — operator-facing serialization is out of the runner's scope; the runner receives it already parsed and validates it fail-fast at session start via `parseCustomAgentSpec`). The native-CLI tool-bridge is wired as the CLI's `orca` MCP server, exactly as for `codex`/`cursor`. See `services/session-runner/src/harness/custom/`.

Pi specifics that track the real `pi` CLI (v0.58) contract:

- **`--extension` (singular)** loads the orca bridge extension (the plural `--extensions` does not exist and is silently dropped); `--no-extensions` precedes it so the session ignores the operator's host `settings.json` extensions.
- **`--no-builtin-tools`** disables pi's default in-process tools (`read`/`bash`/`edit`/`write`) so they never run un-gated or shadow the sandbox-scoped, approval-gated orca tools; extension-registered tools stay enabled.
- **Pre-execution approval** uses pi's extension `tool_call` hook (fires before a tool runs, can block): the orca extension asks the client via `ctx.ui.select(...)` (an `extension_ui_request`), and the harness routes it to the uniform transcript approval and replies `Allow`/`Block` (an `extension_ui_response`). A `Block` blocks the tool **before** it executes. Pi's RPC mode has no per-tool server→client approval request, so this hook is the correct gate.
- **RPC framing** splits stdout on `\n` only (stripping a trailing `\r`); the shared native-CLI launcher does not use Node `readline` (which also splits on U+2028/U+2029, valid inside JSON strings).

The dispatcher's `colocated` setup (`runner/dispatcher.ts`) runs after harness selection:

1. Read the Environment from Registry's prepared execution snapshot.
2. Call `buildEnvironmentSpec(environment, selection)` → `EnvironmentSpec` with
   the operator catalog `image`, `entrypoint`, `exposePorts`, and `target`, and
   reject an Environment custom image.
3. For GitHub resources, mint the `git-creds` JWT and place the helper URL/token
   in `harnessEnv` before sandbox acquisition; raw PAT bytes remain host-side.
4. `sandboxRuntime.acquire(spec)` → `SandboxHandle`.
5. Before the first sandbox write, preflight the output, resource, and optional
   Skill roots; reject aliases and pre-existing mounts at or below them.
6. Create the local output path and materialize every attached file,
   memory-store, and GitHub resource through the existing `MountStrategy`
   implementations.
7. Build a `SessionRunner` with the selected harness (`InSandboxHarness` for
   `claude_code`, `CodexSdkHarness` for `codex_sdk` and `pi_sdk`), mounts,
   memory watcher, output indexer, and acquired sandbox; call `runner.start()`.
8. Mark the session `running` and return the runner.

The host-side MCP rewrite and vault-bound SDK `mcpServers` map are shared by
the Claude `separate` adapter and the Codex and Pi SDK adapters in both modes. An unreadable harness annotation is a setup failure, not a
default: the dispatcher emits `session.error{type:setup_failed, phase:
harness_selection}` + `session.status_idle{retries_exhausted}`, marks the session
idle, and aborts the spawn. Selecting and building the harness runs inside that
same guard, so a harness this deployment cannot execute — a `colocated` harness
the catalog gives no in-sandbox image or port, such as `mock` — surfaces the same
way instead of escaping the spawn uncaught.

## Security

The sandbox holds only what it needs to reach the LLM and MCP servers:

- A per-session ai-gateway JWT (minted by `auth/session-jwt.ts`) as the LLM API key.
- The gateway base URL as `ANTHROPIC_BASE_URL` (or `LITELLM_API_BASE`).
- For GitHub resources, a per-session `git-creds` JWT plus the registry helper
  URL. Repository PATs are resolved host-side and never enter the sandbox.

No Kafka credentials, no provider API keys, no vault bindings enter the sandbox. The ai-gateway enforces vault resolution and credential injection on the host side, just as it does for the `separate` path.

The OpenSandbox adapter assigns uploaded resources to the harness image's
unprivileged `node` user as it creates each file and parent directory. The
entrypoint therefore performs only a constant-time writable-root handoff rather
than recursively changing ownership on the cold-start path. Before Node starts,
`setpriv` enables `no_new_privs` and clears the capability bounding set; the
image contains no sudo or post-start privilege escalation path. It does contain
`s3fs` + `fuse3` so harness-server can create memory/output mounts while the
entrypoint is still waiting as root. The ready marker is released only after
those mounts exist; the agent then runs in a fresh `/dev`, with no capabilities
and no access to the parent `/dev/fuse` device.

## What is implemented

> Both engines are active. `session-runner` serves Registry-owned Sessions;
> the DialIn bridge serves cloud `claude_code`, `codex_sdk`, and `pi_sdk`
> `colocated` Sessions. The ownership table above is
> authoritative. Deferred parity work — the sandbox-harness `codex` provider and
> cloud `SandboxHandle.endpoint()` coverage among it — is in
> [`roadmap.md`](./roadmap.md).

### `colocated` engine (`session-runner`)

- WS tunnel transport (`@orca/harness-tunnel`) and the registry-side coordinator (`SessionEventBridge` single-writer/persist-before-forward, `SessionDistributor`, `TunnelRegistry`, recovery, snapshot delivery).
- `session-runner` itself: serves `/v1/runner/*` via a socket-free dispatcher; registers ten providers (`claude` / `claude-sdk-persistent` / `claude-code` / `codex-sdk` / `pi-sdk` / `codex` / `cursor` / `pi` / `custom` / `mock`) plus the wrapping `multiagent` coordinator harness.
- Self-hosted host + affinity routing (`environment-worker`, `sessions.runner_id`, `environment_claims`).
- Registry provisions configured cloud Environment images, mints Environment Tokens,
  waits for workers to connect, and persists runner events before forwarding them.
  The cloud `claude_code`, `codex_sdk`, and `pi_sdk` `colocated` paths remain with
  harness-server.

### Cloud Claude Code DialIn engine

- `@orca/harness-catalog` — catalog, types, `resolveHarnessAnnotation`.
- Registry annotation validation on agent create/update (`agents.routes.ts`).
- Environment `image` + `target` fields in contract and the scoped prepared-execution snapshot.
- `harness/registry.ts: selectHarness` + dispatcher routing.
- `EnvironmentSpec` fields `image`, `exposePorts`, `harnessEnv`, `target`; `SandboxHandle.endpoint(port)` (interface).
- `buildEnvironmentSpec` mapping (`sandbox/environment-spec.ts`).
- `services/sandbox-harness` — HTTP server with in-memory store, `claude` provider (`@anthropic-ai/claude-agent-sdk` in-process), gateway env hook.
- Warm `claude` turns use runtime-private SDK `resume` plus sandbox-local SDK
  session persistence; native ids never enter Orca durable state. Cold subprocess
  and sandbox rebuilds read the Orca transcript, pass it as `replay`, and inject
  it as a first-turn preamble.
- `InSandboxHarness`, `HarnessChannel`/`HarnessTransport`, `DialInTransport`, `mapSandboxEvent`, `harnessToProvider`.
- Valid `AgentEvent.id` → transcript `Event.id` preservation, with raw
  `idempotencyKey` metadata and optional canonical-subpath preservation.
- Dispatcher `colocated` routing (boots the harness-image sandbox, mounts
  session resources through the shared lifecycle, and skips separate-mode
  Claude SDK MCP/tool construction).
- Hermetic e2e test (`test/integration/dispatcher-in-sandbox.spec.ts`): fake registry + fake `SandboxRuntime` + in-test wire server → annotation → routing → sandbox acquire → dial-in → mapped events → transcript-store, with user-echo dropped, terminal marker emitted, and preserved `Event.id` plus `idempotencyKey` metadata verified.
- Opt-in real-stack e2e (`packages/e2e-tests/test/sandbox-harness-agent.spec.ts`):
  OpenSandbox + the harness image verify file and GitHub reads from the live
  sandbox filesystem plus memory-store writeback through the public API.

### Idle lifecycle

**Idle teardown is the same in both topologies: the sandbox is destroyed.** When the idle timer
fires, `Dispatcher.idleOutRunner` calls `SessionRunner.stop('idle.timeout')`, and `stop()` calls
`SandboxHandle.destroy()`. Neither branches on harness mode. The next user message cold-starts a
new sandbox and replays prior turns as a transcript preamble.

`SandboxHandle.pause()` / `resume()` exist on the interface and every runtime implements them, but
no production code path invokes them. `sandbox/write-policy.ts` forwards them onto the
policy-enforced handle wrapper it builds; that wiring is never exercised, because nothing calls
pause. A warm pause path for either topology is in [`roadmap.md`](./roadmap.md) with the latency
condition that would justify building it.

The consequence is that anything an agent wrote outside `/mnt/session/outputs/` or a memory-store
mount is gone after an idle gap — those two are backed by object storage, so they survive. A
**repository mount does not**: `GitCloneStrategy` streams the working tree and `.git/` into the
sandbox filesystem, `deactivate` is a no-op with no write-back, and the host work dir is removed on
stop. The next turn re-clones from origin at the pinned ref, so an edit or a local commit that was
never pushed is lost.

## Managed Codex SDK

```json
{
  "name": "OpenAI agent",
  "metadata": { "harness": "codex_sdk" },
  "model": { "provider": "openai", "id": "gpt-5.4", "effort": "high" },
  "system": "Help the user complete tasks."
}
```

`codex_sdk` defaults to `separate`, matching `claude_agent_sdk`: for a cloud
Environment, the SDK runs on harness-server while its tools execute in the Session
Sandbox. Set `"mode": "colocated"` explicitly to run the SDK in the shared
`sandbox-harness` image. Self-hosted Codex Sessions require that explicit mode
and use the `codex-sdk` session-runner provider.
Both modes use `@openai/codex-sdk` 0.154.0, its bundled executable, and native
thread history. The existing `codex` entry selects the distinct `codex app-server`
adapter. See [session-runner](services/session-runner.md#codex-sdk) for self-hosted
deployments and [harness-server](services/harness-server.md) for cloud deployments.

Cloud Codex recovery in both modes uses one private owner-fenced turn receipt alongside native
history. Response transcript persistence precedes the native checkpoint commit;
terminal and source-completion acknowledgements precede the next SDK submission.
A ready receipt repairs missing terminal events without another model request. A
pending receipt is explicitly abandoned on replacement, retaining previous native
history and unknown usage. Required-action pauses keep the primary turn incomplete;
recovery completes its accepted callback, confirmation and interrupt sources together.
See [harness-server](services/harness-server.md) for the persistence boundaries.

## Pi SDK

`pi_sdk` follows the same three execution paths as `codex_sdk`: cloud separate,
cloud colocated through the shared sandbox-harness image, and self-hosted colocated
through session-runner. It embeds Pi 0.87.1 with only managed tools and exports
private native checkpoints. The existing `pi` CLI provider remains a distinct
harness. See [Pi SDK](libraries/pi-harness.md) for selection, credentials,
accounting and recovery.
