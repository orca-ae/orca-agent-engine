# OIP-002: Agent harnesses and execution modes

- *Author(s)*: @jiangpengcheng, @sijie
- *Status*: Released
- *Proposal time*: 2026-05-02
- *Components*: harness-server, sandbox-harness, packages/harness-catalog, packages/sdk-harness,
  packages/codex-harness, packages/pi-harness, packages/harness-tunnel, Helm chart
- *Discussion*: None (predates the public repository)
- *Implementation*: `packages/harness-catalog/src/`, `packages/sdk-harness/src/`,
  `packages/codex-harness/src/`, `packages/pi-harness/src/`; harness-server `src/harness/`,
  `src/runner/dispatcher.ts`, `src/sandbox/environment-*.ts`; `services/sandbox-harness/src/`;
  registry-service-ts `src/api/agents.routes.ts`, `src/api/discovery.routes.ts`,
  `src/domain/prepare-execution.ts`, `src/domain/harness-state.ts`; migrations `0061` and `0062`
- *Released in*: v0.5.0

## TL;DR

The engine began with one agent runtime: the Claude Agent SDK looping inside harness-server and
dispatching tools into a sandbox. It now runs the Claude Agent SDK, Claude Code, the Codex SDK, the
Pi SDK and native coding CLIs behind one `AgentHarness` seam. An Agent selects its harness and
topology with two keys in Anthropic-compatible `metadata`, and one pure catalog decides which pairs
are legal, what each harness can enforce and which service executes the session: `separate` runs
the loop outside the sandbox, `colocated` runs loop and tools inside it with no provider key in
reach. Agent authors, operators who pin images and credentials, and harness authors are affected.

## Background

A session's only durable state is its transcript; a harness takes user events in and emits agent
events out ([`agent-harness.md`](../docs/managed-agents/agent-harness.md)). `metadata.mode` places
the loop and the Environment `target` the sandbox
([`deployment-topologies.md`](../docs/managed-agents/deployment-topologies.md)). Owning documents:
[`harness-modes.md`](../docs/managed-agents/harness-modes.md), the harness library docs under
[`libraries/`](../docs/managed-agents/libraries/), and the service docs for
[`harness-server`](../docs/managed-agents/services/harness-server.md) and
[`sandbox-harness`](../docs/managed-agents/services/sandbox-harness.md). Related records are
[OIP-011](OIP-011-self-hosted-session-runner.md), [OIP-004](OIP-004-sandbox-runtimes.md),
[OIP-003](OIP-003-egress-boundary.md), [OIP-006](OIP-006-session-event-semantics.md) and
[OIP-010](OIP-010-guardrails-pricing-and-spend.md).

## Motivation

Teams pick an agent runtime for its native behaviour: Claude Code's built-in tools and subagents,
Codex's Responses-native loop and rollout history, Pi's multi-provider adapters. Rebuilding those
loops on one SDK loses the reason to choose them, and coding agents want to run *inside* the
workspace rather than proxy every tool call from a service host. Three constraints shaped the design:

- **The API cannot fork.** An Anthropic Managed Agents client keeps working unchanged, and a client
  that knows nothing about harnesses gets the original behaviour.
- **Validation and execution cannot disagree.** Registry accepts an Agent; harness-server or a
  runner executes it later. Separate tables of harnesses, modes, images and features let a pair be
  accepted on write yet unrunnable, or let a runtime fall back to Claude silently.
- **A sandbox is untrusted.** Moving the loop inside must not move provider keys, vault bindings or
  repository tokens with it, and must not let two services drive one session.

## Goals

### In scope

- Per-Agent harness selection through `metadata.harness` and `metadata.mode`, validated on every
  write, defaulting to the original behaviour, immutable once the Agent exists.
- One pure catalog, shared by Registry, harness-server, sandbox-harness and session-runner, for
  modes, images, ports, runner provider ids, capabilities, model policy and execution ownership.
- Exactly one execution owner per session, from the pinned Agent version and the Environment target.
- A `colocated` topology that keeps the transcript the source of truth and provider keys outside.
- Native Codex and Pi SDK harnesses on one worker protocol over in-process, HTTP/SSE and stdio
  transports, with private native history and crash-safe turn recovery; discovery of both.

### Out of scope

- Cross-harness resume, a canonical resume payload, or changing an existing Agent's harness.
- The tunnels, the runner's providers and self-hosted admission (OIP-011); sandbox runtimes
  (OIP-004); guardrail semantics (OIP-010).
- Listed on [`roadmap.md`](../docs/managed-agents/roadmap.md): one colocated execution path that
  retires the in-sandbox bridge, an ACP harness provider, Deep Agents, persistent Claude on cloud
  `separate`, warm pause on idle, cloud Claude Code runner parity, and further Codex and Pi scope.

## Design

### High-level design

```text
 client ── POST /v1/agents {metadata: {harness, mode}} ──► registry ◄── @orca/harness-catalog
                                                             │ validate on write, pin the version
                                  resolveExecutionOwner(environment target, pinned selection)
            ┌────────────────────────────────────────────────┴─────────────────────┐
  harness-server (cloud owner)                                   registry + session-runner
    separate:  SDK loop on the host ── tools ──► sandbox          over the runner tunnel
    colocated: host adapter ── HTTP/SSE ──► sandbox-harness       (OIP-011)
                                            (loop + tools in the sandbox)
  every writer appends to the transcript store, the only durable state
```

| Environment target | Session-pinned harness and mode | Execution owner |
| --- | --- | --- |
| `cloud` | `claude_agent_sdk` `separate`; `claude_code` `colocated`; `codex_sdk` or `pi_sdk` in either mode | harness-server |
| `cloud` | `codex`, `cursor`, `pi`, `custom`, `mock` (`colocated`) | Registry and session-runner |
| `self_hosted` | every supported pair; `codex_sdk` and `pi_sdk` only `colocated` | Registry and session-runner |

### Detailed design

#### The `AgentHarness` seam

harness-server's `AgentHarness` (`src/harness/agent-harness.ts:278-296`) is `start`, `submit`,
`stop` and `events()`, plus optional probes for a parked required action and unacknowledged usage.
`submit` validates an event, then awaits `SubmitHooks.onAccepted()`, the durable dequeue boundary
behind `processed_at` (OIP-006), before changing anything; an inapplicable event throws
`UnappliedUserEventError` first. Every `AgentEvent` carries a canonical `evt_` id and subpath that
`SessionRunner` stores as transcript `Event.id` and subpath; a malformed subpath poisons the runner.

Public events share one Claude-aligned vocabulary from `@orca/agent-event-contract`; resume state
stays native. `ClaudeAgentSdkAdapter` is the Claude SDK's `SessionStore` over the transcript,
writing `harness.claude.session_entry` records, a subpath per subagent thread (25 at most); Codex
and Pi checkpoints live in Registry's private `session_harness_states`. No canonical resume schema
exists because a session keeps one harness for life. session-runner's own interface
(`services/session-runner/src/harness/agent-harness.ts:323-345`) adds `interrupt()`.

harness-server selects through a provider registry (`src/harness/registry.ts`) that rejects
duplicate ids; `selectHarness` throws for an unregistered harness or unsupported mode, so nothing
falls back to Claude. The dispatcher registers `claude_agent_sdk`, and `codex_sdk` and `pi_sdk` in
both modes, and sends `claude_code`, `codex`, `cursor`, `pi` and `custom` to `InSandboxHarness`
behind a guard requiring a catalog image and port (`src/runner/dispatcher.ts:691-741`). A failure
appends `session.error` (`setup_failed`, phase `harness_selection`) and an idle `retries_exhausted`.

#### The catalog and the annotation

`HARNESS_CATALOG` (`packages/harness-catalog/src/catalog.ts:133-266`) is pure data; images live
under `ghcr.io/orca-ae/`, and `harnessToProvider` is the only harness-to-provider mapping.

| Harness | Modes (first is the default) | Colocated image, port | Runner provider |
| --- | --- | --- | --- |
| `claude_agent_sdk` | `separate` | none | `claude` |
| `claude_agent_sdk_persistent` | `separate` | none | `claude-sdk-persistent` |
| `claude_code` | `colocated` | `sandbox-harness-claude-code`, 4096 | `claude-code` |
| `codex_sdk`, `pi_sdk` | `separate`, `colocated` | the same image, 4096 | `codex-sdk`, `pi-sdk` |
| `codex`, `cursor`, `pi`, `custom` | `colocated` | `sandbox-harness-<harness>`, 4096 | the harness name |
| `mock` | `colocated` | none | `mock` |

`resolveHarnessAnnotation` (`catalog.ts:327-368`) returns `claude_agent_sdk`/`separate` when both
keys are absent, the harness's first mode when only `harness` is set, and an error for an unknown
harness, invalid mode or unsupported pair. It never mutates `metadata`, which is free-form on
Anthropic's Agent, so the annotation round-trips as written and no request field is added. The
harness is part of an Agent's identity: migration `0061` stores it in `agents.harness_type` behind a
trigger that rejects changes, the update route refuses one first (`validateHarnessUpdate`), and
versions older than the column keep theirs (`bindStoredHarness`, `harness-models.ts:183-204`).

Model policy is per harness (`harness-models.ts:63-120`): Claude models from the price seed for
the Claude SDK harnesses, the OpenAI catalog pinned with `@openai/codex-sdk` 0.154.0 for
`codex_sdk`, Pi 0.87.0's per-provider catalog plus those ids for `pi_sdk`, operator-defined models
for CLI harnesses. `model-controls.ts` holds the Claude fast-mode allowlist (`claude-opus-5`,
`claude-opus-4-8`) and per-model effort levels, enforced by Registry and both harness-server Claude
runtimes; a fast turn must report `fast_mode_state: "on"` and `usage.speed: "fast"`.

#### Capabilities, admission and execution ownership

`resolveHarnessCapabilities(selection)` merges a default-mode entry with per-mode overrides
(`capabilities.ts:36-161`), so consumers never switch on harness names.

| Harness (mode) | Targets | Cloud owner | Skills / multiagent | `nativeResume` | Guardrail policy |
| --- | --- | --- | --- | --- | --- |
| Claude SDK harnesses | both | harness-server | yes / yes | no | provider-defined |
| `claude_code` | both | harness-server | provider-defined | no | provider-defined |
| `codex_sdk`, `pi_sdk` (`separate`) | `cloud` | harness-server | yes / no | yes | `request`, `tool_call`, `tool_result`; stateful at `request` only |
| `codex_sdk`, `pi_sdk` (`colocated`) | both | harness-server | yes / no | yes | `request`, stateful allowed |
| CLI harnesses, `mock` | both | Registry | provider-defined | no | provider-defined |

Admission reads the pinned version. Agent writes check the annotation, model controls,
`validateHarnessModel`, `validateHarnessFeatures` and the roster (`agents.routes.ts:96-147`,
`:422-448`); `validateHarnessDeployment` runs at session create, on an Environment target change
against every live bound session (`environments.routes.ts:440-466`), at preparation and at runner
snapshot resolution; `validateHarnessGuardrails` refuses rules a declared policy cannot enforce.
`resolveExecutionOwner` (`execution-owner.ts:10-16`) gives `self_hosted` sessions to Registry and
`cloud` sessions to their declared owner, whatever workers are connected. The distributor applies
it; harness-server asks Registry's internal `execution-owner` route before every user event, cold
interrupts included, uncached, settling unowned events without side effects (`dispatcher.ts:2156`).

#### One sandbox lifecycle, two topologies

harness-server acquires one sandbox per runner in both modes (`dispatcher.ts:4254`), so mounts,
Skills, output capture, the write policy and teardown are shared. For `colocated` the sandbox *is*
the harness image: `buildEnvironmentSpec` (`environment-spec.ts:25-67`) takes
`SANDBOX_HARNESS_CLAUDE_CODE_IMAGE` for `claude_code`, `codex_sdk` and `pi_sdk`, else the catalog
default, with its entrypoint and port; `assertSandboxEnvironmentTrust` rejects an Environment
`image` for `colocated` and package installers for any managed sandbox. The sandbox receives an
output directory, the gateway LLM URL with a session JWT and `X-Orca-Session-Id` header, the write
policy and, for repositories, a git-credential helper URL and JWT (`dispatcher.ts:4335-4409`); the
image starts its server only after the dispatcher marks mounts ready.

- **`claude_agent_sdk`** (`separate`): Claude Agent SDK 0.3.220 in-process; sandbox tools through an
  in-process `orca` MCP server; remote MCP servers rewritten to the gateway with a refreshed JWT;
  model egress from `LLM_EGRESS_DEFAULT` or the session's `metadata.orca_llm_egress`.
- **`claude_code`** (`colocated`): `InSandboxHarness` drives the sandbox-harness `claude` provider
  (alias `claude-code`) on the same SDK. The toolset maps to built-ins `Bash`, `Write`, `Edit`,
  `Glob`, `Grep`; `read` stays on an in-process `orca` MCP server for bounded pages; web tools are
  omitted. Custom tools round-trip as `agent.custom_tool_use`, native subagents run in the sandbox,
  and only `user.message` and `user.custom_tool_result` apply (`in-sandbox/index.ts:194-217`).
- **`codex_sdk`, `pi_sdk`**: one host adapter, `CodexSdkHarness`, serves both modes
  (`dispatcher.ts:806-878`); `separate` runs the worker in-process in a private temporary directory,
  and `colocated` substitutes `RemoteCodexSdkWorker` and always uses gateway egress (`:880-889`).

#### The in-sandbox bridge

`@orca/sandbox-harness` is an image payload, not a cluster service: an HTTP server on port 4096 in
a read-only-root bubblewrap namespace (`no_new_privs`, no capabilities, writes only where the
validated `ORCA_SANDBOX_WRITE_POLICY` allows). It serves `GET /v1/harnesses`, `POST /v1/sessions`,
SSE `.../events/stream`, `GET` and `POST .../events`, `POST .../sdk-command`, and `GET` and
`DELETE /v1/sessions/:id` (`server.ts:170-177`). Each session is one child process speaking the
Claude Agent SDK's NDJSON stream-json control protocol over stdio; the providers are a static array
(`providers/registry.ts:48`): `claude`, `codex-sdk`, `pi-sdk`.

Events live in memory and every SSE connect replays the whole buffer, so durability is the host's:
`DialInTransport` drops replayed ids (a bounded set of 10,000), `mapSandboxEvent` translates frames,
and `SessionRunner.pumpEvents` appends with the frame id as `Event.id` and `idempotencyKey`. Frame
shapes are a two-sided contract between `protocol.ts` and `harness/in-sandbox/event-mapper.ts`, with
no shared type. The server checks no credential of its own; it is reached only through
`SandboxHandle.endpoint(port)` and what the sandbox runtime enforces there.

#### The SDK worker protocol

`@orca/sdk-harness` (`src/wire.ts`) is the contract both SDK workers implement. Commands are `start`
(root, session id, model, provider, effort, instructions, API key, base or gateway URL, MCP tool
definitions, optional checkpoint), `submit`, `tool_result`, `interrupt` and `stop`; `refreshOptions`
replaces credentials between turns. Events are `ready`, normalized thread, turn and item `event`s,
`tool_call`, `checkpoint`, `done` and `failure`. It runs in-process on harness-server; over HTTP/SSE
via `sdk-command` and `harness.sdk_event` frames, where each acknowledgement carries the latest
event sequence, the host waits for SSE to catch up before committing, a sequence gap fails the
worker, and acknowledged frames are pruned; and over stdio, where session-runner spawns the worker
in the sandbox through `env -i` with private directories.

Executable tools exist only as `tool_call`/`tool_result`, so the adapter owns permission,
confirmation, guardrails, sandbox execution, remote MCP and client callbacks.
`assertSdkTerminalUsage` requires complete non-negative safe-integer counters, cached input no larger
than input; malformed usage is fatal and exports no checkpoint. `checkpoint` precedes `done`, and
`validateSdkCheckpoint` binds its format to the pinned harness (`pi-checkpoint.ts:39-47`).

#### Codex SDK and Pi SDK

The **Codex worker** (`packages/codex-harness/src/worker.ts`) gets a private `CODEX_HOME` and an
allowlisted environment, approval policy `never`, a read-only native sandbox, web search off, the
SDK's shell, patch, image, multi-agent, plugin, hook and remote-model surfaces disabled, and one
bearer-authenticated MCP relay on `127.0.0.1` in place of the whole MCP table. Developer
instructions are fixed after the first native turn; checkpoints are rollout files (at most 32, 16
MiB decoded). The **Pi worker** (`packages/pi-harness/src/worker.ts`) embeds Pi 0.87.0 with
in-memory managers, nothing discovered, built-in tools off and only supplied tools, SDK retries and
auto-compaction off. Pi's official adapters encode each request, redirected to the gateway's
`/v1/proxy/{provider}/{upstream-path}` under route `llm-pi-{provider}-{api}`; Registry mints a Pi
token only for a route in `SESSION_JWT_LLM_ROUTES`. Its checkpoint is one `session.json`.

**Cloud recovery.** The adapter records turns through Registry's internal `harness-turn` route
(`claim`, `begin`, `accept_source`, `commit`, `abandon`, `settle`), fenced by an ownership revision
and owner token and stored with the checkpoint in one private, versioned envelope. A pending receipt
precedes submission, responses are persisted before the native commit, a ready receipt repairs
missing terminal events without a model call, and a pending one found after a crash is abandoned
with an error, keeping the prior checkpoint and any unknown-usage budget marker. Gateway tokens for
both SDKs live 660 seconds and are reused only with 630 left, covering the ten-minute turn bound.

#### Tool confirmation, by runtime

No capability declares whether a harness can run the `always_ask` round trip, and Registry never
checks `permission_policy` against the harness: the contract accepts `always_allow` and `always_ask`
(`agents.contract.ts:12`), and `validateAgentConfiguration` checks shapes only
(`agent-version-configuration.ts:97`).

- **`claude_agent_sdk`**: every managed tool passes `canUseTool` under `permissionMode: 'default'`
  with no auto-approved `allowedTools` (`claude/index.ts:734`, `:2391`); `always_ask` becomes
  `user.tool_confirmation`.
- **`codex_sdk`, `pi_sdk` on harness-server, both modes**: the adapter folds the policy with
  `tool_call` guardrails and parks `ask` on `user.tool_confirmation` (`codex-sdk/index.ts:800-817`);
  a guardrail `ask` on a custom client tool fails closed.
- **`claude_code` on the cloud bridge**: no confirmation round trip exists. A tool whose policy,
  after the stateless guardrail pass, is not `always_allow` is left out of the SDK tool list, and an
  unclassified tool counts as `always_ask` (`in-sandbox/index.ts:656-691`, `:781-795`).
- **session-runner**: the loop always binds its transcript-backed gate (`session-loop.ts:700-707`);
  in-process providers park `always_ask`, native CLIs reach the gate through their own approval
  requests, and `custom` only when its spec declares the approvals opt-in.

#### Idle lifecycle and cold start

After a durable terminal a warm runner waits `SESSION_IDLE_TIMEOUT_MS` (default 60000; `0`
disables), not while a required action is pending or usage is unacknowledged
(`dispatcher.ts:3799-3880`). Idle-out indexes outputs, releases mounts and calls
`SandboxHandle.destroy()` with no mode branch; archive and delete sentinels stop a runner anyway.
Next, the Claude SDK reloads history through its `SessionStore`, Codex and Pi restore their
checkpoint, and `claude_code` replays prior turns as a preamble (warm turns resume a sandbox-local
SDK `session_id`). Files outside `/mnt/session/outputs/` and memory mounts do not survive idle.

## Changes by component

- **registry-service-ts**: Agent annotation, model, feature and immutability checks; deployment and
  guardrail admission; internal `execution-owner`, `harness-turn` and `harness-state` routes;
  harness discovery; LLM token lifetimes and Pi route claims; migrations `0061` and `0062`.
- **harness-server**: the provider registry and owner check, `InSandboxHarness`, `DialInTransport`,
  the event mapper, `CodexSdkHarness` with local and remote workers, colocated specs and idle-out.
- **New**: the sandbox-harness image and the `harness-catalog`, `sdk-harness`, `codex-harness` and
  `pi-harness` libraries (`@orca/harness-tunnel` is OIP-011's); Helm values under Configuration.

## Public-facing changes

### API

- **Agents.** `metadata.harness` and `metadata.mode` are ordinary metadata keys. An unknown harness,
  invalid mode, unsupported pair or changed harness is a `400`.
- **Models.** `model` keeps Anthropic's string or `{id, speed, effort}` form; `speed: "fast"` only
  for the allowlisted Claude models, `effort` per model, with OpenAI's `ultra` for Codex and Pi. A
  non-Anthropic `provider` rides the model object as an Orca extension, defaulting from the harness;
  default responses omit it, and `orca-beta` responses return `{provider, id}` (`model-wire.ts:245`).
- **Sessions and Environments.** Session create is a `400` when the pinned harness cannot run on the
  target or with the requested Skills, roster or model; `metadata.orca_llm_egress` (`direct` or
  `gateway`) picks `separate` egress. A target change that would strand a live session is a `400`.
- **Discovery.** `GET /apis/runtime.runorca.ai/v1/harnesses` lists managed SDK harnesses with
  provider, modes, `managed_skills`, `multiagent`, `native_resume` and model efforts; the register
  keeps it as `extension-runtime-group` ([OIP-005](OIP-005-conformance-decision-register.md)).

### Events and streaming

No event kind is added. The bridge drops the user echo, opens a turn with `session.status_running`
and `span.model_request_start`, maps an idle frame to internal usage, `span.model_request_end` and
`session.status_idle` (`end_turn`), and a sandbox error to `session.error` (`will_retry: false`)
plus `session.status_idle` (`retries_exhausted`), deriving sibling ids from the frame id. SDK
frames, checkpoints and receipts never reach the public transcript.

### Wire protocols

The in-sandbox HTTP/SSE surface and its stream-json child protocol, the SDK worker protocol, the
Codex relay, the gateway's Pi proxy path, and Registry's internal routes above, scoped by path.

### Storage

Migration `0061_codex_sdk_state.sql` adds `agents.harness_type` (backfilled from `metadata.harness`)
with the `agents_harness_type_immutable` trigger, and `sessions.harness_state`, which
`0062_separate_session_harness_states.sql` moves into `session_harness_states` (cascading with its
session). The transcript gains `harness.claude.session_entry` records.

### Configuration

| Setting | Default | Read by | Effect |
| --- | --- | --- | --- |
| `LLM_EGRESS_DEFAULT` | `direct` | harness-server | model egress for `separate`; session metadata overrides |
| `LLM_GATEWAY_URL` | `http://localhost:8090/v1` | harness-server | gateway base for gateway egress and every colocated sandbox |
| `SANDBOX_HARNESS_CLAUDE_CODE_IMAGE` | catalog default | harness-server | sandbox image for `claude_code`, `codex_sdk`, `pi_sdk` |
| `SESSION_IDLE_TIMEOUT_MS` | `60000` | harness-server | warm retention after a turn; `0` disables |
| `ANTHROPIC_*`, `OPENAI_*`, `DEEPSEEK_*`, `GEMINI_*`, `ZAI_*`, `PI_SDK_PROVIDER_CREDENTIALS` | unset | harness-server | direct keys and base URLs for `separate`; Pi's map names variables |
| `SESSION_JWT_LLM_ROUTES`, `SESSION_JWT_LLM_MODELS` | empty | Registry | LLM route and model claims; Pi needs its own route |
| `SESSION_JWT_TTL_SECS` | `300` | Registry | lifetime of the colocated `claude_code` gateway token |

Helm: `images.sandboxHarness.claudeCode` (by digest when set) renders the image setting;
`harness.llmEgressDefault`, `harness.sessionIdleTimeoutMs` and `registry.aiGatewayLlmUrl` feed the
harness; `aiGateway.openai`, `.anthropic`, `.deepseek` and `.piProviders` configure gateway
upstreams, and `sessionJwt.llmRoutes` and `.llmModels` the token claims.

### Metrics, logs and traces

No metric carries a harness or mode label and no trace span is added; harness-server logs each tool
it omits from a colocated session for lack of an approval path.

## Compatibility

### Upgrade

Agents without an annotation stay `claude_agent_sdk`/`separate`. The earlier mode spelling
`in_sandbox` normalizes to `colocated` (`DEPRECATED_MODE_ALIASES`, `catalog.ts:33-35`); stored rows
are not rewritten and round-trip without a `400`, while errors advertise only current spellings.
`0061` backfills `harness_type`, older version snapshots keep their harness, and `0062` moves state
written under `0061`. Registry, harness-server and the sandbox image ship together.

### Rollback

v0.5.0 is the first public release containing this design, so there is no earlier public release
to return to. Migrations `0061` and `0062` are forward-only, and the immutability trigger lives in
the database, so older code still cannot change an Agent's harness.

### Version skew

Registry decides ownership, so a harness-server built against an older catalog fails setup with
`harness_selection` for a harness it cannot build rather than running another. The in-sandbox
frames share no type across the wire; releases pin the image by digest (the release workflow
refuses a floating tag), and an image lacking a requested provider fails the session.

## Security considerations

- **No provider key in a colocated sandbox.** Model traffic goes to the gateway with a
  Registry-minted session JWT, and colocated Pi refuses to start without its gateway URL
  (`sandbox-harness/src/providers/codex-sdk.ts:68-69`). The AI gateway, available as a public image
  under Apache-2.0 (`ghcr.io/orca-ae/orca-ai-gateway`), injects upstream keys. Repository PATs stay
  on the host; in-sandbox git uses a session-scoped git-credential JWT.
- **Sandbox hardening.** Read-only root, `no_new_privs`, no capabilities, policy-derived writable
  paths and a mandatory nested bubblewrap; Environment images are refused for `colocated`, and
  package installers everywhere, until providers expose a sealed setup namespace.
- **Host-side workers.** Private working directories and `CODEX_HOME`; Codex's execution surfaces
  off behind a bearer-token loopback relay; Pi with no discovery and static API keys only (no OAuth
  or ambient cloud credentials). Direct keys live only in the host environment.
- **Private history.** Checkpoints are validated for paths, count, size, encoding, instruction
  digest and harness binding before persistence and restore, and never become public events.

## Testing

- **Unit** (required `test` job): the four harness libraries; harness-server `harness-registry`,
  `environment-spec`, `in-sandbox-event-mapper`, `in-sandbox-replay`, `codex-sdk-native` and
  `codex-sdk-remote-worker` (the installed SDK against scripted Responses endpoints),
  `codex-turn-recovery`, `claude-sdk-fast-mode-wire`; sandbox-harness `protocol`, `routes`,
  `codex-sdk-provider`, `image-hardening`, `write-policy`.
- **Integration**: harness-server `dispatcher-in-sandbox` (annotation to transcript through a fake
  runtime and wire server), `in-sandbox-bridge`, `session-adapter` (ten hand-written cases); Registry
  on Postgres `execution-owner`, `codex-deployment-admission`, `session-harness-state-migration`.
- **End to end**: `ORCA_E2E_AGENT_HARNESS` selects `claude_agent_sdk`, `codex_sdk` or `pi_sdk` for
  `pnpm e2e:agent` and `pnpm e2e:agent:sandbox`; the OpenSandbox matrix in `e2e-stack.yml` runs
  Claude on Kafka (`separate` and `claude_code`), Pi SDK with `claude-sonnet-4-6` on Postgres and
  Codex SDK with `gpt-5.4` on Pulsar, each in both modes, after merge or with the `run-e2e` label.

## Alternatives

- **A dedicated harness field on the Agent.** Rejected: it changes Anthropic's shape, while
  metadata keys are invisible to clients that do not use them.
- **A table per service.** The catalog began inside Registry and moved to a shared pure package when
  harness-server needed it; a parallel harness-to-provider switch there drifted and was replaced by
  `harnessToProvider`. A canonical resume payload was rejected too: sessions never cross harnesses.
- **Earlier designs, reversed.** An Environment image overriding the harness image (it could race
  trusted materialization); a lean colocated branch without mounts (one lifecycle now serves both);
  Orca-specific turn markers from the bridge (Claude-aligned stop reasons replaced them); and cloud
  Codex colocated on a Registry-launched worker and runner (it moved to harness-server and the
  shared image, reusing its mounts, output capture, probes, teardown and one host adapter).
- **Retire the bridge for session-runner** (an earlier plan). Not done: the runner lacks parity for
  FUSE mounts, native subagents, output capture and stateful budgets (`capabilities.ts:70-72`), so
  cloud `claude_code` keeps the bridge; the unified path is listed on the roadmap.
- **A pluggable provider seam** with generated capability tables, a conformance kit, a
  tool-confirmation capability and an in-process LangGraph Deep Agents harness (an unmerged
  design); the registry and capability table came with Codex and Pi, and Deep Agents is listed on
  the roadmap.
- **CLI or SDK.** Both exist as distinct identities: `codex` and `pi` drive native CLIs on the
  runner; `codex_sdk` and `pi_sdk` embed the SDKs for managed tools and private checkpoints.

## Status notes

- **Tool confirmation is not validated.** An `always_ask` tool on a cloud `claude_code` Agent is
  accepted on write and absent at run time, signalled only by a server log line; the bridge also
  rejects `user.interrupt`, `user.tool_confirmation` and `user.define_outcome` as unapplied.
- **Cloud `claude_code` has no remote MCP.** The host MCP rewrite runs only for harnesses declaring
  `needsMcpRewrite`, and the sandbox provider starts with `strictMcpConfig` and only its in-process
  `orca` server (`sandbox-harness/src/providers/claude.ts:359-371`).
- **The colocated `claude_code` gateway token is not refreshed.** It is minted once at acquisition
  with the default `SESSION_JWT_TTL_SECS` and not renewed while the sandbox stays warm; a failed
  mint is logged and the sandbox starts without it (`dispatcher.ts:4345-4358`).
- **Cold `claude_code` replay is text only.** `buildReplayTurns` keeps `user.message` and
  `agent.message` text (`in-sandbox/replay.ts:34-49`); tool history is not replayed.
- **Pause is unused.** An earlier decision kept pause and resume for `separate`; both topologies
  destroy on idle and no path reaches `pause()`. Warm pause is listed on the roadmap.
- **Model controls stop at harness-server.** The runner's snapshot model has no `speed`
  (`services/session-runner/src/snapshot.ts:27-32`); its Claude providers apply neither control.
- **Catalog entries outrun builds.** Releases build only `sandbox-harness-claude-code`; the
  `codex`, `cursor`, `pi` and `custom` tags are placeholders for runner-owned harnesses. Cloud
  `claude_agent_sdk_persistent` fails setup, since harness-server registers no provider for it.
