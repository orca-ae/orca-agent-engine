# OIP-011: Self-hosted session runner and environments

- *Author(s)*: @sijie
- *Status*: Released
- *Proposal time*: 2026-08-05
- *Components*: session-runner, environment-worker, registry-service-ts (tunnels, environments),
  packages/oeadm, packages/harness-tunnel, services/environment-image
- *Discussion*: None (predates the public repository)
- *Implementation*: `services/session-runner/src/`, `services/environment-worker/src/`,
  `services/registry-service-ts/src/api/runner-tunnel.routes.ts`,
  `services/registry-service-ts/src/api/worker-tunnel.routes.ts`,
  `services/registry-service-ts/src/auth/tunnel-auth.ts`, `services/registry-service-ts/src/tunnel/`,
  `services/registry-service-ts/src/environment/`, `packages/harness-tunnel/src/`,
  `packages/oeadm/src/`, `services/environment-image/`
- *Released in*: v0.5.0

## TL;DR

A session's agent loop and tools can run on a machine the operator controls and the control plane
cannot reach: a workstation, a private VM, or a sandbox the registry launches in a provider account.
A long-lived `environment-worker` per Environment and a `session-runner` per session both dial the
registry outbound over WebSocket; the registry pushes launches, turns and control signals down those
tunnels and remains the only writer of the session's events. This affects operators who run
execution themselves and anyone adding a harness to the runner.

## Background

Two axes decide where a session executes: the Agent's harness mode (`separate` or `colocated`, in
`metadata.mode`) and the Environment's target (`cloud` or `self_hosted`), as
[`deployment-topologies.md`](../docs/managed-agents/deployment-topologies.md) draws them.
`resolveExecutionOwner` (`packages/harness-catalog/src/execution-owner.ts`) gives every
`self_hosted` session to the registry and every `cloud` session to the owner its harness declares;
the distributor, the event bridge and harness-server (through the registry's internal
`execution-owner` route) all apply it. Component docs:
[`session-runner.md`](../docs/managed-agents/services/session-runner.md),
[`environment-worker.md`](../docs/managed-agents/services/environment-worker.md),
[`harness-tunnel.md`](../docs/managed-agents/libraries/harness-tunnel.md),
[`registry-service.md`](../docs/managed-agents/services/registry-service.md) ("Runner tunnel") and
[`session-runner-scope.md`](../docs/managed-agents/session-runner-scope.md).

## Motivation

Operators want agents to work where their code, data and tools already are: a workstation with a
checked-out repository, a VM on a private network, a sandbox account they already pay for.
harness-server drives sessions from inside the cluster and reaches into sandboxes it provisions,
but a machine behind NAT or an egress-only firewall accepts no inbound connection, cannot reach the
mesh-only `/internal/*` listener, and should never hold database, broker, object-store or provider
credentials. Whatever runs there must dial out, talk only to the registry, and receive
configuration, Skill and resource bytes, history and turns over that one connection, and it must
absorb unreliable networks: laptops sleep, ingresses recycle idle sockets, registries restart.

## Goals

### In scope

- Run a session's loop and tools on compute that makes only outbound connections, while durable
  state and upstream credentials stay on the control-plane side.
- Keep one writer per session, so every reconnect is a replay and no event is persisted twice.
- Recover from tunnel drops, ingress recycles and registry restarts without operator action.
- Serve Environments the registry provisions itself (`target=cloud`) with the same processes.
- Attach a machine with one command; keep Orca-only API surface `orca-beta`-gated or classified.

### Out of scope

- Splitting a session across the tunnel: self-hosted sessions run their loop on the host, and Claude
  SDK Agents annotated `separate` run in the runner there too.
- Anthropic's pull-based work queue (`/v1/environments/{id}/work*`) and Tunnels resource, listed on
  [`roadmap.md`](../docs/managed-agents/roadmap.md#anthropic-surface-not-implemented); several live
  workers per Environment; worker autoscaling; the runner exclusions in
  [`session-runner-scope.md`](../docs/managed-agents/session-runner-scope.md#out-of-scope).

## Design

### High-level design

```text
 client --- /v1 API + SSE ---> registry replicas (public listener) ---> transcript store
                                 ^ worker tunnel                 ^ runner tunnel
                                 | /v1/tunnels/environments/{id} | /v1/tunnels/runners/{id}
 operator host or launched box   |                               |
          environment-worker ----+   -- spawns -->   session-runner --> tools in its sandbox
          (one per Environment)                      (one per session)
```

1. `oeadm env create` makes a `self_hosted` Environment and prints its Env Key once; the worker that
   `oeadm worker` starts dials the worker tunnel with it, and the accepting replica records a claim.
2. A session is created `pending`; with the worker connected to this replica, the registry mints a
   binding token and sends `worker.launch_runner`. The spawned runner dials in and says hello.
3. The registry marks the session `assigned`, pushes managed resources, Skill bytes, the
   credential-free snapshot and a transcript replay, then starts the session's event bridge.
4. The bridge drives each turn as a streaming `POST /v1/runner/turn` and persists every streamed
   event before reading the next; confirmations, client tool results and interrupts use own routes.

### Detailed design

#### The worker and the runner

The **environment-worker** (`services/environment-worker/`) has no listener; one runs per
Environment. On `worker.launch_runner` it spawns `RUNNER_LAUNCH_COMMAND` in
`<WORKSPACE_DIR>/<runner id>`, or in an existing absolute path the frame names (the mount path of the
session's first GitHub repository). Launches are idempotent per runner id. An exit the worker did not
request is reported as `worker.runner_exited` with the exit status and the last 15 lines (4 KiB at
most) of output: the only diagnostic for a runner that dies before connecting. The worker runs as
the operator, so a runner inherits an allowlist (process essentials, locale, TLS trust stores, the
runner's knobs, names in `ORCA_RUNNER_ENV_PASSTHROUGH`) plus four wiring variables whose names are
single-sourced in `packages/harness-tunnel/src/identity.ts`; an inherited binding token is stripped.

The **session-runner** (`services/session-runner/`) has no listener; one runs per session. It serves
the ten routes in `src/protocol.ts` and drives the provider the snapshot names, so every runner
starts generic. `defaultRegisterProviders` registers `claude`, `claude-sdk-persistent`,
`claude-code`, `codex-sdk`, `pi-sdk`, `codex`, `cursor`, `pi`, `custom` and `mock`, and the hello
advertises that set. Tools run through `@orca/sandbox-runtime`'s `SandboxHandle`, native CLIs reach
them through one `orca` MCP bridge, and every gated call parks on one transcript-backed approval gate
that fails closed. The runner exits after `ORCA_RUNNER_IDLE_TIMEOUT_S` (default 3600) without work.

#### Two tunnels over WebSocket

Both are framed JSON over one outbound WebSocket to the public listener under `/v1/tunnels/*`,
which the API-key/OIDC pre-handler skips, so each handler authenticates its own peer. Both clients
send `Origin: orca://internal`, which the cross-site WebSocket guard accepts and no browser can
produce; both require `frame_protocol_version` 1 (strict major), ping every 30 s and close a peer
silent for three intervals. Close codes: `4001` no hello, `4002` major mismatch, `4003` ping timeout,
`4004` auth or binding refusal, `4403` bad `Origin`. `@orca/harness-tunnel` holds both schemas,
header names, the runner-id derivation and the wiring variable names.

- **Runner tunnel**, `WS /v1/tunnels/runners/{runner_id}`: HTTP over WebSocket in eleven frame kinds
  (`hello`, `request`, `response.head`/`body`/`end`, `request.cancel`, `ping`, `pong`, and
  `ws.open`/`frame`/`close` channels). A message is capped at 100 MiB and a buffered response at
  32 MiB per request; overflow cancels only that request. Control signals avoid `/v1/runner/turn`
  because turns are serial and a signal sent as a turn would queue behind the turn it must unblock.
- **Worker tunnel**, `WS /v1/tunnels/environments/{environment_id}`: control RPC in sixteen frame
  kinds: `worker.hello` (version, name, live runner ids, configured-harness map), launch and stop
  with results, one-way `worker.runner_exited`, and `stat`, `list_dir`, `create_dir`,
  `create_worktree`, `remove_worktree` with results. The worker drops frame kinds it does not know.

Both reconnect with capped exponential backoff (0.5 s initial, 10 s cap, jittered), promptly after a
recycle (`1001`, `1012`) or abrupt drop. The runner exits on a `403` upgrade or a `4001`, `4002`,
`4004` or `4500` close; the worker exits with `EnvironmentConnectError` on a 4xx upgrade except 408
and 429.

#### Session distribution and runner affinity

`SessionDistributor` (`src/tunnel/session-distributor.ts`) serves both targets, needs a connected
worker, and never provisions anything.

- **Create.** Without a worker on this replica the session stays `pending` with no runner, counted
  by `work_stats.depth`; a connecting worker later dispatches every such session of its Environment.
  A harness-server-owned session is never dispatched, and a worker whose configured-harness map
  lacks the session's provider fails it before anything is spawned.
- **Launch.** A 256-bit binding token is minted and the runner id derived as `runner_token_` plus
  the first 32 hex digits of SHA-256 over `orca-runner:<token>`. The distributor pre-registers a
  connect waiter, persists `runner_id` and `host_environment_id`, sends token, workspace and
  provider, and awaits the result for 10 s in the background: a refusal fails the session, a timeout
  does not, and a failed send rolls the binding back.
- **Connect.** The session fails if the runner's hello lacks its provider, else becomes `assigned`.
- **Failure.** `worker.runner_exited`, or a runner tunnel closing with no newer tunnel for that id,
  fails the session; a later reconnect of the same runner restores `assigned`.

A session maps to one runner through `sessions.runner_id`; a runner serves one session, learned from
`X-Orca-Session-Id` on each pushed request. The replica holding the worker tunnel sends launches.

#### The event bridge: one writer, persist before forward

When a runner connects, the replica terminating its tunnel prepares it in order (managed resources,
Skill bundle bytes, agent snapshot, replay) and starts a `SessionEventBridge`
(`src/tunnel/session-event-bridge.ts`); a newer connection of the same runner id retires the older
one. The bridge is the session's only writer of agent events. It reads the transcript from the start,
drives every user turn with no linked output, then follows from an explicit cursor one past the head
it read, so a message posted before the runner connected is still driven and nothing falls into the
gap. Each streamed event is appended (`producedBy=harness`), awaited and in order, before the next is
read; clients see only persisted events because SSE tails the transcript from any replica. Outputs
carry `source_event_id` and `agent.turn_completed` carries `turn_event_id`, so an answered turn is
never driven again. Read-only followers push `user.tool_confirmation`, `user.custom_tool_result` and
`user.interrupt` to their runner routes, where each push is idempotent.

#### Recovery and resume cursors

Runner state is disposable; the transcript is not. Before the bridge starts, `SessionRecovery`
reads the session's public events to the head and pushes them as `POST /v1/runner/replay` in
acknowledged pages of 500, each naming its starting cursor in `X-Orca-Resume-Cursor`. The runner
deduplicates by transcript id (`SessionLoop.applyReplay`) and advances its cursor only to the last id
it applied, never from the header. Its next hello presents `resume_cursors` (session id to last
applied id), so the registry replays only what follows; a fresh runner receives everything. Recovery
never appends or drives a turn, so a reconnect restores state without re-running work.

The turn request and its event stream are one streaming POST. A drop mid-turn loses the unstreamed
remainder: a turn with no persisted event is driven again on reconnect, while a turn that dropped
after partial output keeps its prefix and is not re-run; the triggers that would change this are
under "Self-hosted runner transport" in [`roadmap.md`](../docs/managed-agents/roadmap.md).

#### Credential-free snapshots and egress modes

`AgentSnapshotResolver` (`src/domain/agent-snapshot-resolver.ts`) composes one snapshot per session:
model, provider, system prompt, tool and MCP allowlists, Skills, guardrails, managed resources,
custom tools, tool permissions, the multiagent roster and an `egress` block. Before delivery,
`assertSnapshotCredentialFree` checks it against an exact key allowlist and requires every
credential-shaped value to be JWT-shaped, so a new field carrying a secret fails by its structure.
The Environment's `egress_mode` picks the egress strategy; unset means `gateway`.

- `gateway`: MCP servers are rewritten to `AI_GATEWAY_MCP_URL` with a scoped session JWT and vault-id
  routing headers; `AI_GATEWAY_LLM_URL` adds an LLM base URL and a separate `llm-proxy` JWT (120 s by
  default). The AI gateway, available as a public image under Apache-2.0
  (`ghcr.io/orca-ae/orca-ai-gateway`), swaps the tokens for real credentials. Without
  `AI_GATEWAY_MCP_URL`, a gateway session fails snapshot resolution with a configuration error.
- `sidecar`: only per-host bindings (host, auth scheme, vault reference, env names to inject) for a
  credential proxy beside the runner. This repository does not include that proxy, and providers
  take no model credential from the block.

To call a provider directly, an operator forwards the key through `ORCA_RUNNER_ENV_PASSTHROUGH`; the
Claude providers use `ANTHROPIC_API_KEY` only when the snapshot has no scoped LLM token. Skills and
managed resources arrive as bytes, and managed Git checkouts use the registry's read-only Git proxy
with a short-lived capability; the runner holds no object-store credential or upstream Git token.

#### Environment credentials and durable claims

A worker presents exactly one credential; each is stored on the `environments` row as a SHA-256
digest with an expiry, compared in constant time, and never persisted raw.

- **Env Key** (`sk-` and 32 random bytes, 7-day lifetime) for operator-run workers, in
  `X-Orca-Environment-Key`. Every `POST /v1/environments` arms one and echoes it once to `orca-beta`
  callers; `rotate-key` replaces it atomically; `revoke-key` clears it.
- **Environment Token** (`et-` and 32 random bytes, same lifetime) for registry-launched workers, in
  `X-Orca-Environment-Token`. It is minted before the worker starts, overwritten by a relaunch, and
  revoked by a failed launch or an archived or deleted Environment. A presented token must resolve;
  it never falls through to the Env Key check.

Verification fails identically for an unknown id, a wrong or expired credential and an archived
Environment, so the route is no existence oracle; the engine's generic launch-token header is inert
and closes the connection with `4004`. `environment_claims` records which replica terminates each
worker tunnel: one row per Environment, taken newest-wins, heartbeated on each ping, released only by
its own connection, and swept after `ENVIRONMENT_CLAIM_TTL_MS` (90 s) without a heartbeat.
`work_stats.worker_connected` reads it.

#### Registry-launched Environments

With `ORCA_ENVIRONMENT_LAUNCHER_BACKEND` set, creating a session on a `cloud` Environment for a
registry-owned harness while no worker is connected starts `EnvironmentLaunchLifecycle`
(`src/environment/launch/`) in the background: provision a box, mint an Environment Token, start the
worker there with seven dial-back variables (tunnel URL, Environment id, token, workspace, runner
command, worker name and id), and wait up to 120 s for it to connect. A failure after provisioning
terminates the box and revokes the token. Launches are single-flighted per Environment within a
replica, and the ordinary worker-connect path dispatches the session. When a `cloud` worker
disconnects while sessions still wait, a relaunch follows a 3 s grace period, at most three times in
a row. `local` spawns the worker as a registry child process; `e2b` and `opensandbox` share
`CloudEnvironmentLauncher`, which creates a sandbox through `@orca/cloud-sandbox` and starts the
worker detached through its exec channel. Those boxes use the Orca Environment image
(`services/environment-image/`), which bakes both binaries under `/opt/orca/` and starts neither.

#### The `oeadm` client and terminal attach

`oeadm` (`packages/oeadm`) is a pure client of public routes. `oeadm env create --name <n>` creates
a `self_hosted` Environment with `orca-beta` and prints the Env Key once, failing if none came back.
`oeadm worker` maps flags onto the worker's variables and runs its built entry as a child,
propagating the exit code; it needs no API key and teaches `ENVIRONMENT_KEY` over `--env-key`, which
`ps` would show. `oeadm run --agent <id>` creates a session and chats, and
`oeadm attach --session <id>` co-drives one; both read the `orca-beta` stream for its
`agent.turn_completed` boundary, and approvals allow only `y` or `yes` (a closed stdin denies).
`ork`, the general-purpose client CLI, has no counterpart to `oeadm worker` or
`oeadm attach --terminal`. The latter uses `WS /v1/sessions/{id}/terminals/{terminal_id}/attach`:
API authentication, a workspace-scoped lookup, and a bridge over the online runner's tunnel
(`/v1/runner/terminal/attach/<terminal_id>`), served when its sandbox has a tmux terminal host.

## Changes by component

- **registry-service-ts**: tunnel routes and auth, `src/tunnel/`, `src/environment/`, snapshot
  resolver and egress checks, key, token and claim stores, terminal attach, `work_stats`.
- **New**: session-runner, environment-worker, `@orca/harness-tunnel`, `@orca/oeadm` (a registered
  exception to the library-only rule for `packages/`) and `services/environment-image`;
  `@orca/harness-catalog` gains the `colocated` mode, snapshot composition and the owner rule.
- **Helm chart**: no new values; `/v1/*` in `registry.istio.authorizationPolicy.allowedPaths` admits
  the tunnels.

## Public-facing changes

### API

- `POST /v1/environments` accepts Anthropic's `config: {type: "self_hosted"}` or legacy `target`
  (default `cloud`), plus `egress_mode` and `llm`; `scope` only for `self_hosted`, `config.packages`
  only for `cloud`. The default response is Anthropic's `BetaEnvironment` exactly; `orca-beta`
  callers also get `env_key` once on create, and `env_key_set` and `env_key_expires_at` on reads
  ([`orca-extensions.md`](../docs/managed-agents/orca-extensions.md#environment-key-lifecycle)).
- `POST /v1/environments/{id}/rotate-key` (returns `{env_key, env_key_expires_at}`) and
  `.../revoke-key` are `keep` extensions; Anthropic's Environment has no key concept.
- `GET /v1/environments/{id}/work_stats` returns `{depth, in_flight, worker_connected}`, recorded
  `fix-later`: Anthropic publishes it at `.../work/stats` with another shape
  ([`roadmap.md`](../docs/managed-agents/roadmap.md#environment-work-stats-path-and-shape)).
- `orca-beta` session reads carry `runner_id`, `host_environment_id` and `distribution_state`;
  `WS /v1/sessions/{id}/terminals/{terminal_id}/attach` is authenticated like the rest of `/v1`.
- The tunnel WebSockets are absent from the published OpenAPI document and claim only the `runners`
  and `environments` sub-segments of `/v1/tunnels`. Agents opt in with `metadata.mode: colocated`;
  the older `in_sandbox` is normalized.

### Events and streaming

Runner events reach clients only through the transcript, after they are persisted. The default SSE
stream keeps the Claude vocabulary, `orca-beta` streams add `agent.turn_completed`, and the private
`orca.harness_checkpoint` and `orca.resource_checkpoint` records never reach the public transcript.

### Wire protocols

Both tunnels speak `frame_protocol_version` 1 with snake_case JSON keys. Pushed requests carry
`X-Orca-Session-Id`, replays add `X-Orca-Resume-Cursor`, and bodies are NDJSON. Handshake headers:
`X-Orca-Runner-Tunnel-Token`, `X-Orca-Environment-Key`, `X-Orca-Environment-Token`. The runner's
copies of route and header literals are checked against the registry source by a pin spec.

### Storage

Migration `0053_open_maginty.sql` adds `environment_claims`; `env_key_*`, `environment_token_*`,
`egress_mode` (checked) and `llm` on `environments`; and `runner_id`, `host_environment_id` and
`distribution_state` on `sessions` ([`data-model.md`](../docs/managed-agents/data-model.md)).
Migration `0062` moves native SDK checkpoints to `session_harness_states`. No raw secret is stored.

### Configuration

- **Registry**: `RUNNER_TUNNEL_TOKENS` (empty; a static allow-list admitting remote runners under
  stable ids), `RUNNER_TUNNEL_ALLOWED_ORIGINS` (empty), `RUNNER_TUNNEL_LOCAL_MODE` (`true`),
  `ENVIRONMENT_CLAIM_TTL_MS` (`90000`), `AI_GATEWAY_MCP_URL`, `AI_GATEWAY_LLM_URL`,
  `AI_GATEWAY_LLM_JWT_TTL_SECS` (`120`), `SESSION_JWT_TTL_SECS` (`300`),
  `ORCA_ENVIRONMENT_LAUNCHER_BACKEND` (unset disables launching; `local`, `e2b`, `opensandbox`),
  `ORCA_REGISTRY_TUNNEL_URL` (`http://localhost:<httpPort>`), and the per-backend
  `LOCAL_ENVIRONMENT_*`, `E2B_API_KEY`, `E2B_ENVIRONMENT_*`, `OPEN_SANDBOX_*` and
  `OPEN_SANDBOX_ENVIRONMENT_*` variables (`src/environment/launcher/launcher-factory.ts`).
- **environment-worker**: `ENVIRONMENT_ID`, `ENVIRONMENT_KEY`, `REGISTRY_TUNNEL_BASE_URL`,
  `WORKSPACE_DIR`, `RUNNER_LAUNCH_COMMAND`, `REGISTRY_RUNNER_URL`, `ENVIRONMENT_WORKER_NAME`,
  `ORCA_ENVIRONMENT_TOKEN`, `ENVIRONMENT_WORKER_ID`, `ORCA_RUNNER_ENV_PASSTHROUGH`
  ([`AGENTS.md`](../services/environment-worker/AGENTS.md)).
- **session-runner** ([`AGENTS.md`](../services/session-runner/AGENTS.md)):
  `ORCA_RUNNER_IDLE_TIMEOUT_S` (`3600`), `ANTHROPIC_ALLOWED_MODELS`, `ANTHROPIC_MODEL_DEFAULT`,
  `SANDBOX_RUNTIME`; fallbacks `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `PI_SDK_PROVIDER_CREDENTIALS`.
- **oeadm**: `ORCA_BASE_URL` (`http://localhost:8080`), `ORCA_API_KEY`.
- **Helm**: `registry.aiGatewayMcpUrl`, `registry.aiGatewayLlmUrl`, `sessionJwt.llmRoutes` and
  `sessionJwt.llmModels` feed snapshot egress; everything else goes in `registry.extraEnv`.

### Metrics, logs and traces

Registry counters, all prefixed `registry_service_`: `bridge_turns_driven_total{source}`,
`bridge_confirmations_pushed_total{source,result}`, `bridge_interrupts_pushed_total{source,result}`,
`session_recovery_replayed_total{mode}`, `snapshot_delivered_total{result}`,
`skills_delivered_total{result}`. The runner's boot line (runner id, tunnel URL, session id,
providers, idle timeout) and the worker's dial log carry no credential. No trace spans.

## Compatibility

### Upgrade

Additive schema. Environments created before migration `0053` have no Env Key until `rotate-key`
issues one; `separate` and harness-server-owned `cloud` sessions route as before.

### Rollback

v0.5.0 is the first public release with this feature, so rolling back means running without it. A
registry without the tunnel routes rejects the upgrade: the worker exits with
`EnvironmentConnectError`, the runner keeps retrying, and `self_hosted` sessions, which have no other
driver, stay unserved. Code that does not read the added table and nullable columns ignores them.

### Version skew

Upgrade the registry first; it serves both tunnels. A peer on another protocol major is refused at
hello (`4002`). Optional hello fields default and fail open: a runner listing no providers, a worker
sending `configured_harnesses: null` and a launch with `harness: null` skip their capability checks.
Unknown worker frames are dropped and unknown runner routes answer `404`. In a rolling upgrade,
workers and runners reconnect to any replica; the claim moves newest-wins and recovery replays.

## Security considerations

- **Outbound only.** Neither process listens; both reach only the registry and hold no database,
  broker or object-store credential. The snapshot's only tokens are scoped JWTs. Terminal attach,
  the one way into a running runner, is authenticated, workspace-scoped and rides its own tunnel.
- **Runner admission.** The 256-bit binding token is seeded only through the runner's wiring
  variable; the worker strips any inherited copy before it launches a runner. Without an allow-list
  the path id must be the token's derived id, and a peer without a resolved owner is refused. The
  shipped wiring (`buildRunnerTunnelAuth`) resolves owners for loopback peers only, unless
  `RUNNER_TUNNEL_TOKENS` admits exactly those tokens from anywhere.
- **Worker admission and runner environment.** Worker credentials are digest-only, echoed once,
  rotatable and revocable; a credential reaches a runner only through `ORCA_RUNNER_ENV_PASSTHROUGH`.
- **Tool isolation.** The runner's default sandbox runtime is in-memory (a temporary directory and
  plain child processes). `SANDBOX_RUNTIME=local` selects the `srt`-wrapped runtime, degrading to
  in-memory with a warning without `srt`; providers with managed resources require Linux, `srt` and
  bubblewrap and never fall back. The worker forwards `SANDBOX_RUNTIME` only when passed through.
- **Logs.** No token or JWT is logged. A `worker.runner_exited` tail can contain agent output; the
  registry logs it and returns it on the mesh-only runner status read only to the runner's owner.

## Testing

- **Unit**, in the required `test` job: the runner (fake registry tunnel, fake CLIs, a turn-terminal
  matrix over every catalog provider, `protocol-registry-pin.spec.ts`), the worker
  (`runner-env.spec.ts` guards the allowlist), `harness-tunnel`, `oeadm`, and registry specs for both
  routes, `tunnel-auth` (a remote peer is refused by default), distributor, bridge, recovery,
  deliveries, credential-free egress, claims, tokens, launch lifecycle, launchers and `work_stats`.
- **Integration**, in the `integration` job on Postgres: `sessions-tunnel-three-layer.spec.ts` (real
  tunnels over loopback) and `cloud-local-launch-e2e.spec.ts` (the `local` launcher spawns the real
  binaries and a `mock` turn reaches SSE); an E2B launcher spec runs only with credentials.
- **End to end**: after `make self-hosted-up` (Postgres and RustFS; `oeadm worker` attaches this
  machine), `pnpm e2e:self-hosted` asserts a `mock` run, `pending` without a worker and single-writer
  ordering; `spend-runner.spec.ts` runs the production worker and runner in `e2e-stack.yml`. A suite
  against real vendor CLIs is listed on [`roadmap.md`](../docs/managed-agents/roadmap.md).

## Alternatives

- **Dial into the host**, as harness-server does with sandboxes it provisions. A machine behind NAT
  has no address, and opening ports or running a VPN is the burden this design removes.
- **Pull a work queue**, as Anthropic's API models self-hosted execution. Pushing over a connection
  the host already holds reaches a turn blocked on an approval or the model at once, and keeps the
  transcript the only durable queue. The published family stays `not-implemented`.
- **gRPC streaming**: WebSocket crosses plain HTTP ingress, and HTTP over it keeps routes simple.
- **Store access for the runner** would put credentials on customer machines and add a writer.
- **Runner without a worker.** Something must exist on the host before any session to spawn runners,
  filter their environments and report crashes before they connect; the worker holds the long-lived
  credential and runners stay disposable with per-launch tokens.
- **An input queue and ready handshake for the first turn.** The transcript is already an ordered,
  durable buffer; reading it from the start and following an explicit cursor closes the gap. A
  separate submit and resumable stream per turn is deferred, with its trigger on the roadmap.
- **Per-connection host rows** with owner-conflict checks and a re-own override. The Environment row
  plus newest-wins, connection-scoped claims give one durable owner with less state.
- **Split transport by target.** An earlier design drove self-hosted runners over tunnels but cloud
  `colocated` sessions from harness-server over HTTP/SSE into the sandbox. One tunnel and one writer
  for every registry-owned runner replaced it; cloud `claude_code` still uses that bridge.
- **Inherit the worker's environment**: simpler, but it hands the operator's secrets to every runner.

## Status notes

- **Remote runner admission is narrower than the goal.** The binding token proves which runner id a
  peer may claim, but the route does not check it against the tokens the distributor minted, so it
  admits no non-loopback peer on its own, and `RUNNER_TUNNEL_TOKENS` admits only static tokens. The
  flows exercising worker-launched runners (`make self-hosted-up`, the self-hosted and spend suites,
  the `local` launcher) dial over loopback; validating dial-back from provider sandboxes is listed on
  [`roadmap.md`](../docs/managed-agents/roadmap.md#cloud-claude-code-runner-parity).
- **Refused worker credentials retry.** The registry refuses a worker credential with a `4004` close
  after the upgrade, and the worker treats only HTTP upgrade rejections as permanent, so a revoked or
  expired Env Key shows as a reconnect loop capped at 10 s rather than an exit.
- **Pre-spawn capability check.** The shipped worker sends no configured-harness map, so that check
  fails open; the runner's providers at connect and the snapshot route (`422`) still gate sessions.
- **A runner that exits is not replaced.** An idle timeout or crash fails the session's distribution
  and no replacement is dispatched, so later messages are persisted but not driven. The runner
  receives the worker's pid but does not watch it.
- **Unused worker operations; bridge placement.** The registry sends only `worker.launch_runner`;
  stop, stat, list, create-directory and worktree frames have no registry producer, and the runner
  ids in `worker.hello` are not reconciled. The bridge runs on the replica terminating the runner's
  tunnel, which can differ from the claim holder; the binding is resolved from the database.
- **Credential refresh.** A fresh snapshot, with fresh JWTs, arrives on every connect, before every
  turn for `codex-sdk` and `pi-sdk`, and when guardrails change; a renewal path for other runner
  credentials is listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#production-hardening).
- **Cloud execution owners.** On `cloud`, only `codex`, `cursor`, `pi`, `custom` and `mock` Agents
  run on registry-launched runners; the Claude SDK harnesses, `claude_code`, `codex_sdk` and `pi_sdk`
  stay with harness-server. Moving cloud `claude_code` to the runner is listed in the same entry.
- **Recovery acknowledgement.** Only sessions with managed resources require a fully acknowledged
  replay; for others the bridge starts after a failed replay and the next reconnect retries.
- **Work-queue depth.** Dispatch marks every session on a `cloud` Environment `pending`, including
  harness-server-owned ones it never launches, so `work_stats.depth` counts those sessions too.
