# session-runner (`@orca/session-runner`)

> The `colocated` engine. One long-running **client** process per session: it
> dials the registry's runner tunnel outbound, then serves the requests the
> owning registry pod pushes back down it, running the agent loop and its tools
> together inside the sandbox. Source: `services/session-runner/`; developer
> guidance for working in it is
> [`services/session-runner/AGENTS.md`](../../../services/session-runner/AGENTS.md).
> Scope — what the runner owns, what it reuses from Orca, and what is out of
> scope — is [`../session-runner-scope.md`](../session-runner-scope.md). Topology:
> [`../architecture.md`](../architecture.md),
> [`../harness-modes.md`](../harness-modes.md),
> [`../deployment-topologies.md`](../deployment-topologies.md).

## A client, not a listener

The runner holds **no listener**. It dials `WS /v1/tunnels/runners/:runnerId` on
the registry's public listener with its binding token, says hello, and then
behaves like a local app reached over that tunnel: the registry frames HTTP
requests down it and the runner responds. That inversion is the whole design — a
runner sitting behind NAT on customer-owned compute reaches the registry and
nothing else, and therefore needs **no database, no broker, and no object-store
credentials**. The same transport carries every `colocated` session regardless of
where the sandbox runs; there is no per-`target` transport split. The server half
of the handshake — auth gates, owner resolution, close codes — is
[`registry-service.md`](./registry-service.md) ("Runner tunnel").

The runner is spawned once per session by the environment-worker, which points a
launch command at `node dist/main.js` and seeds four wiring variables (binding
token, workspace root, parent pid, registry tunnel URL). Those four names are
single-sourced in `@orca/harness-tunnel` and imported by both sides, so neither
spells a name the other owns.

`src/tunnel/serve.ts` is the serve loop. Disconnects retry forever with capped
backoff plus jitter, so starting the runner before the registry is reachable is
valid. A routine ingress recycle (close `1001`/`1012`, or an abrupt drop)
reconnects promptly at the base delay rather than escalating, because escalating
would leave the runner unregistered and turns undeliverable for seconds on every
recycle. A binding or frame-protocol refusal — a `403` upgrade rejection, or a
`4001`/`4002`/`4004`/`4500` close — is **fatal**: retrying can never succeed, so
the loop exits with a rejection the caller surfaces. A `401` refreshes the
binding token through the optional token factory and retries; a bounded run of
consecutive `401`s means the binding is permanently wrong and is fatal too.

## The wire it serves

`src/protocol.ts` declares the ten routes the owner pod pushes, the two request
headers, and the NDJSON content type.

| Route                                  | Pushed when                           | Runner behavior                                                                                                                                             |
| -------------------------------------- | ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POST /v1/runner/resources`            | before Skills and snapshot            | Stages bounded resource manifest/chunks and commits the verified tool sandbox.                                                                              |
| `POST /v1/runner/resource-changes`     | after a tool or on reconnect          | Returns the pending immutable checkpoint and its file chunks.                                                                                               |
| `POST /v1/runner/resource-changes/ack` | after Registry persistence            | Releases the matching checkpoint barrier independently of the parked turn.                                                                                  |
| `POST /v1/runner/skills`               | before the snapshot                   | Materializes the pushed Skill bundle bytes to a native plugin directory and records it. `400` malformed, `500` on a write failure so the owner pod retries. |
| `POST /v1/runner/snapshot`             | before the first turn                 | Configures the provider from the credential-free agent snapshot. A snapshot naming an unregistered provider acks a capability mismatch (`422`).             |
| `POST /v1/runner/replay`               | on every (re)connect                  | Applies the resume replay, deduping by stable event id.                                                                                                     |
| `POST /v1/runner/turn`                 | per user turn                         | Drives the harness for one turn and streams the agent events back as the NDJSON response body.                                                              |
| `POST /v1/runner/confirmation`         | a turn is parked on a gated tool      | Resolves the parked verdict by `tool_use_id` so `canUseTool` proceeds or returns a clean denial.                                                            |
| `POST /v1/runner/custom-tool-result`   | a turn is parked on a client callback | Validates the public result body and resolves the matching `custom_tool_use_id` in the configured Session.                                                  |
| `POST /v1/runner/interrupt`            | a turn is in flight                   | Aborts the in-flight turn without tearing down the harness.                                                                                                 |

Confirmation, custom-tool result, and interrupt are separate routes rather than turn bodies for a
structural reason: turns are **serial** — the owner pod drives one at a time and
the in-flight turn blocks its turn loop inside the streaming POST — so a control
signal delivered as a turn would queue behind the very turn it is meant to
unblock. All three are idempotent, so a re-push after a flapping reconnect is safe.

A turn body checks its captured harness and configuration generation when the
transport begins consuming it, then occupies the turn slot before yielding
control. A snapshot or resource delivery between response creation and consumption
invalidates that body; it cannot submit to a stale harness. Configuration remains
blocked until the consumed turn body closes. Closing the response early interrupts
the provider and drains its remaining turn events before releasing that slot. A
cancelled or closed response receives no synthetic turn-completed event.

The two headers are `X-Orca-Session-Id` on every pushed request and
`X-Orca-Resume-Cursor` on a replay push. The runner **does not consume** the
cursor header; see "Recovery" below.

Every one of those literals exists twice — once here and once in the registry's
`src/tunnel/*` — because the runner is a separate process that does not link the
registry. `test/unit/protocol-registry-pin.spec.ts` closes that gap by asserting
each runner constant against the registry **source file, read as text**; an
assertion against a second hand-copied literal would pass just as happily when
the registry side drifts. The pin skips itself only when the registry source is
absent from the checkout.

Tunneled WebSocket channels (`ws.open` / `ws.frame` / `ws.close`) ride the same
socket alongside the framed requests, dispatched to per-channel handlers. The
one registered channel is remote terminal attach — see "Terminals" below.

## Providers

`src/harness/provider.ts` is the dispatch seam: a registry of factories keyed by
the **snapshot's** `provider` field. The launch frame deliberately omits the
provider, so the worker spawns a generic runner shell and the credential-free
snapshot selects the harness. `defaultRegisterProviders` in `src/main.ts` wires
all ten, and the hello advertises exactly that set, which is what the
registry's capability-match validates a session's snapshot against.

| Provider                | Shape                         | Notes                                                                    |
| ----------------------- | ----------------------------- | ------------------------------------------------------------------------ |
| `claude`                | in-process Claude Agent SDK   | lean — one `query()` per turn                                            |
| `claude-sdk-persistent` | in-process Claude Agent SDK   | long-lived session; the only provider honoring a per-turn model override |
| `claude-code`           | native CLI over stream-json   | headless `claude` as a long-lived child                                  |
| `pi-sdk`                | native Pi SDK subprocess      | `@earendil-works/pi-coding-agent` 0.87.1; private native session history |
| `codex-sdk`             | native Codex SDK subprocess   | `@openai/codex-sdk` 0.154.0; native persisted thread history             |
| `codex`                 | native CLI over JSON-RPC      | `codex app-server`                                                       |
| `cursor`                | native CLI over stream-json   | `cursor-agent`                                                           |
| `pi`                    | native CLI over NDJSON RPC    | `pi --mode rpc` plus the bridge extension                                |
| `custom`                | native CLI, operator-declared | driven entirely by the snapshot's `custom_spec` — no runner change       |
| `mock`                  | none                          | tests and CI e2e; no LLM egress, no sandbox                              |

The two in-process Claude providers share the runner's one transcript store as
their conversation-history substrate. The five native-CLI providers need none:
their history lives in the child process.

The session loop never branches on the provider name. Adding a provider is a
`register*Provider` call and nothing else.

`multiagent` is not an eleventh provider. `CoordinatorHarness`
(`src/harness/multiagent/coordinator-harness.ts`) is a **decorator**: it wraps
the coordinator's own base harness — built by the ordinary registry from the
coordinator snapshot, so single-agent providers are untouched — and injects a
`delegate` seam the provider feature-detects to expose the delegation tool. On a
delegation it resolves the roster agent, enforces the concurrent-thread cap and
one-level delegation, builds a fresh subagent harness sharing the coordinator's
sandbox and confirm gate, and re-emits that subagent's events under a
`subagents/<session_thread_id>` subpath on one merged `events()` stream. The
subpath is a top-level field on the NDJSON line, so the registry persists each
event on the right thread; a single-agent session emits none and its wire lines
are unchanged.

## Tools and the sandbox

`src/sandbox/seam.ts` is this package's **only** import point for
`@orca/sandbox-runtime` and the place a concrete runtime is constructed at boot.
Runtime selection mirrors harness-server's `SANDBOX_RUNTIME` with one deliberate
difference: on `local` without `srt` on the host, the runner **degrades to the
in-memory runtime with a visible warning** rather than failing at boot, because a
self-hosted runner must not crash on an operator's box that has not installed it
yet. `SandboxHandle.spawn` is optional on the interface, so a native-CLI provider
feature-detects it and fails loudly rather than quietly running unsandboxed.

The trusted local SDK/CLI worker permits the explicit provider API hosts
`api.anthropic.com` and `api.openai.com`, plus hosts configured through
`AI_GATEWAY_URL` and `S3_ENDPOINT`. This supports direct provider credentials
without broad network grants.

Managed model tools use the seam's separate `createManagedToolSandboxRuntime`
factory. It requires Linux, SRT and bubblewrap and never falls back to the
in-memory runtime. It enables the Local runtime's mapped tool filesystem and
honors the Environment network mode. `limited` grants only the supplied model
hosts plus the Registry Git proxy host for Git attachments; ambient provider,
gateway and object-store origins do not expand the list. Omitted networking or
`unrestricted` preserves the Environment's network access without the SRT domain
filter. Both modes retain the mapped filesystem, user/PID namespaces, dropped
capabilities and private worker-state boundary. A policy-enforced handle is
returned only after the actual nested isolation probe passes. Initial Git
cloning runs in the Registry. Under `limited` networking the proxy also requires
the upstream Git hostname in the Environment's `allowed_hosts`, including for the
initial clone; a Registry host grant alone does not authorize upstream Git traffic. The
native SDK worker uses a separate trusted handle, with its home, temporary
files, native history and checkpoints outside the model tool root.

The Claude Agent SDK's own built-ins would otherwise execute on the runner
**host** — `process.cwd()`, the runner's network. The in-process providers
therefore bind every file and exec tool to the session's `SandboxHandle` through
an in-process MCP server named `orca` (`src/harness/claude/mcp-tools.ts`) and
anchor the SDK `cwd` at the sandbox root, so both the bound tools and anything
the SDK runs itself land inside the sandbox. Native-CLI providers reach the same
tools through one shared bridge (`src/mcp/native-cli-bridge.ts`), exposed to each
CLI as its `orca` MCP server.

There is exactly one approval path. A provider that gates a tool call — the SDK's
`canUseTool` on the in-process side, the CLI's own permission request on the
native side — routes it to the transcript-backed gate in
`src/tool-confirmation.ts`, which parks the call in `src/pending-approvals.ts`
keyed by `tool_use_id` until the pushed verdict resolves it. The gate is
fail-**closed**: only an explicit allow proceeds; a deny, a missing decision, or
a foreign one all deny. It
classifies the tri-state (`allow` / `deny` / `ambiguous`) alongside that boolean
purely so an operator can tell a malformed producer apart from a deliberate
refusal. The verdict's durable source of truth is the transcript; the pushed
confirmation is only the live delivery that unparks the call.

### Terminals

The `orca` built-ins all run to completion, which is the wrong shape for a REPL,
a pager, or an installer prompt. The `sys_terminal_*` toolset
(`src/tools/sys-terminal.ts`) covers that: launch a program in a fresh tmux pane,
send literal text and key chords, read the rendered pane plus scrollback, list
the live panes, close one. They are backed by `TerminalHost`, a capability a
`SandboxHandle` may expose — the tmux-backed handle does, a cloud-only handle
need not, so callers feature-detect it.

`src/tunnel/terminal-attach.ts` bridges a live pane's pty to a tunneled WS
channel opened on `/v1/runner/terminal/attach/<terminalId>`: raw pane bytes out
as binary frames (the live pty stream, escape sequences and all, not a rendered
grid), input bytes in, and a `{"type":"resize","cols":…,"rows":…}` text control.
Detaching never kills the terminal; an unknown id closes the channel with `1011`.
That path is not in `src/protocol.ts` and needs no pin: it is single-sourced in
`@orca/harness-tunnel`, which both sides import, and a shared symbol cannot
drift.

## Skills

The runner holds no object-store credentials and no `@orca/skill-store`, so it
cannot pull Skill bundles itself. The owner pod pushes the bytes to the skills
route and `src/skills-materialize.ts` writes them into the layout the `claude`
CLI's `--plugin-dir` expects — a `.claude-plugin/plugin.json` manifest plus
`skills/<skill-name>/…` — which is a different layout from the raw tree
harness-server writes for the in-process Agent SDK. The pure validators and the
read-only chmod plan come from `@orca/sandbox-runtime`'s shared core, which the
registry push side also uses. harness-server's writer keeps its own copy of the
validators (`services/harness-server/src/sandbox/skills/materialize.ts`), so the
two write paths agree only while both copies change together. See
[`../skills.md`](../skills.md).

For managed resources, the Skills manifest also carries exact descriptors and
bundle file metadata. The runner validates each file's bytes and matches the full
binding multiset, including identities, metadata and duplicate counts, to
`snapshot.skills` before deduplicating shared physical trees. An empty delivery
clears removed bindings. The runner replaces `/workspace/skills` in the model-tool root while
idle. It composes the shared progressive-disclosure catalog with pinned package
digests, and omits the Claude plugin field. Stateless top-level `block_skills`
rules filter both the visible tree and catalog; refresh/removal clears stale files.

## Recovery and the resume cursor

`RunnerResources` stages resource bytes in a private directory, verifies their
declared size and digest, and exposes a separate tool handle only after its write
policy passes the sandbox probe. Repeating a committed binding revision retains
live Memory changes. Staging and frozen checkpoints stay outside the tool root.

Managed Git origins point to the Registry's read-only Git proxy. Each repository's
config includes `/.orca/git/<resource_id>.config`, a read-only policy root containing
only a short-lived capability for that bound session and repository. The Registry
mints runner-facing capabilities after resource-byte transfer and output
reconciliation, then refreshes again before applying snapshots and after turn
preparation (Skills, snapshot and accounting), immediately before dispatch; this updates auth files without replacing local Git changes. Missing or
expired capabilities prevent tool execution. Starting a new turn requires more
than the full ten-minute turn window remaining; in-flight tools can continue
while their grant is still live. Within a delivery generation, Registry
queries the runner's active resource revision and reuses its own previous Git
snapshot descriptors when bindings still match. The runner validates retained
Git bindings against its committed revision, so Memory refreshes require no
Git clone or retransmission and preserve local working-tree edits. A new runner,
changed binding revision, or new Registry delivery generation prepares fresh Git
snapshots. Upstream Git PATs remain in the Registry.

The helper serializes tool operations and freezes changed outputs and writable
Memory files before awaiting Registry acknowledgement. Failed tools also flush
any bytes they wrote. A missing acknowledgement retains the frozen copy and blocks
the next operation; exact acknowledgement retries are idempotent. Read-only mounts
and Git checkouts are not exported as Memory changes. Checkpoint scans reject links,
devices, aliased descriptors, and changing files, and stop after 100,000 directory
entries in addition to the shared file/byte limits.

The runner's conversation history lives in the tunnel-fed
`src/transcript/in-memory-transcript-store.ts`. The registry pushes the recovery
replay down; the runner streams new agent events up and the registry is the
single writer of the durable transcript.

The runner derives its resume position **solely** from the stable ids of events
it actually applied: `applyReplay` advances the cursor to the last id in the
replay body, and the next connect's hello re-advertises that id. On a caught-up
frame the registry sends a zero-byte body and carries the position only in
`X-Orca-Resume-Cursor`; the runner advances nothing for that frame and
re-presents its last applied id instead, which the registry re-serves
idempotently. So body-id derivation is exactly-once on its own and the header is
informational — reading it would couple the runner to a value it must derive from
applied state.

## Snapshots are credential-free

`src/snapshot.ts` is the runner-side mirror of the registry's `AgentSnapshot`
shape plus its body parser. The runner does not re-derive anything the registry
already resolved — no skills or tool composition here — it reads the resolved
fields and configures the provider. The snapshot is credential-free by
construction and the registry asserts that before it leaves the wire: the egress
block carries opaque scoped JWTs and vault-id references, never an upstream
secret. The runner reaches credentialed upstreams through that egress config,
never by holding a secret. See [`../auth-and-vaults.md`](../auth-and-vaults.md).

## Lifecycle and environment

An inactivity watchdog (`src/idle.ts`) requests a graceful shutdown after
`ORCA_RUNNER_IDLE_TIMEOUT_S` with no work and no turn in flight; every work frame
on the tunnel refreshes the stamp, so a standalone cancel between turns defers
the window too. `0` disables it, and a malformed value fails at startup rather
than silently changing the lifecycle.

The runner's own operator-facing variables — the idle window, the per-turn model
allow-list, and the opportunistic session/workspace ids — are tabulated in
[`services/session-runner/AGENTS.md`](../../../services/session-runner/AGENTS.md)
and checked by `pnpm docs:env-check`, which scans `src/config.ts`. There is no
`KAFKA_*` or database wiring at all.

The boot line carries the derived runner id, the tunnel URL, the pinned session
id, and the advertised providers — never the binding token and never an egress
JWT.

## Layout

```
services/session-runner/
  src/
    main.ts                   Entry point: config → providers → loop → tunnel → watchdog
    config.ts                 Runner-wiring config read from the worker-seeded env
    protocol.ts               The ten pushed routes, two headers, NDJSON type
    runner.ts                 SessionRunner — dial + serve + the dispatch seam
    session-loop.ts           Consume snapshot, construct harness, drive turns, apply replay
    register-handlers.ts      Wires the loop's handlers onto the dispatcher
    snapshot.ts               Credential-free agent-snapshot shape + parser
    tool-confirmation.ts      The one transcript-backed approval gate
    interrupt.ts              `user.interrupt` body validation
    pending-approvals.ts      Parked verdicts, keyed by tool_use_id
    idle.ts                   Inactivity watchdog
    skills-materialize.ts     Skill bundle bytes → native `--plugin-dir` plugin
    tunnel/                   ws-client, serve loop, request dispatch, terminal attach
    harness/                  agent-harness seam, provider registry, one dir per provider
    mcp/native-cli-bridge.ts  The `orca` MCP server every native CLI reaches
    sandbox/                  seam.ts (the only @orca/sandbox-runtime import), tmux, CLI launcher
    tools/sys-terminal.ts     The sys_terminal_* interactive toolset
    transcript/               The in-memory, tunnel-fed transcript store
  test/unit/                  The package's whole coverage provenance
```

## Tests

The turn-terminal conformance matrix checks the real registered provider set
against `HARNESS_CATALOG`. Every catalog provider must have a registration and a
matrix row; each fault case either executes through `SessionLoop` or records an
explicit reason it does not apply. This uses the runner's own lifecycle contract,
including interruption and its turn-completion boundary. harness-server's
acceptance-hook contract is a separate interface.

```bash
pnpm -F @orca/session-runner test
pnpm -F @orca/session-runner lint
```

There is no separate integration suite. Most specs use a fake registry tunnel
(`test/unit/support/fake-registry-runner-tunnel.ts`) and fake CLIs. The Codex SDK
worker specs also run the installed native executable against a loopback mock
Responses endpoint. The unit run is this package's whole coverage provenance.
`protocol-registry-pin.spec.ts` is a guard rather than a behavior test: it pins
the wire literals against the registry source.

## Codex SDK

This provider serves self-hosted `metadata.mode: "colocated"` Sessions.
Cloud Codex Sessions run through harness-server in both modes; colocated uses the
shared sandbox-harness image. Self-hosted Codex selects `colocated` explicitly. All
adapters import the pinned SDK worker from `@orca/codex-harness`.

The `pi-sdk` provider uses the same runner adapter and sandbox process boundary,
with the worker entry point importing [`@orca/pi-harness`](../libraries/pi-harness.md).
It opts into managed resources, client callbacks, Registry-owned request budgets,
and native checkpoint recovery. Its private checkpoint event carries `provider: "pi-sdk"`;
Registry binds the versioned history format to the immutable `pi_sdk` Agent selection.
Pi selects its official Pi model protocol from
the pinned model provider. Gateway tokens override provider-specific direct credentials;
see [Pi credentials and models](../libraries/pi-harness.md).

The `codex-sdk` provider launches a trusted worker through `SandboxHandle.spawn`
in a private worker sandbox. Its explicit `/usr/bin/env -i` environment contains
only PATH, configured TLS certificate paths, and private HOME/TMPDIR locations.
The working directory is private and separate from model-tool resources. Runner
binding tokens, store credentials, Git credentials, and ambient runtime hooks do
not enter the worker. The scoped LLM token travels through the control pipe. That worker calls `Codex.startThread`, `runStreamed`, and
`resumeThread`. The runner image includes the pinned SDK and its platform
executable. The SDK owns inference, compaction, and native history; Orca owns
session scheduling, tenant isolation, tool approvals, sandbox execution, events,
and persistence.

Only the Orca MCP relay exposes executable tools. It supplies the configured
sandbox tools, approved remote MCP toolsets, and declared client callback tools. A snapshot carrying
`managed_resources: { version: 1, revision }` must match the committed controller
revision and a provider declaring managed-resource support; missing or mismatched
configuration fails before provider start. The Codex bridge binds only the
policy-enforced tool handle, while the raw handle launches the trusted worker. Sandbox and remote MCP calls use the existing
transcript-backed confirmation gate and permission policies. Registry snapshots carry
explicit per-tool and toolset policies; exact overrides take precedence over
server defaults, and `always_deny` cannot be overridden by client approval.
Malformed policies are rejected at both snapshot boundaries. Native shell,
patch, web search, image, browser, and subagent execution are disabled; the
pinned model catalog removes native shell/patch declarations as well. The
worker uses a fresh private `CODEX_HOME` and a forced provider/MCP configuration,
so project configuration cannot add an execution server or redirect credentials.

Declared `custom_tools` travel with their descriptions and full input schemas from
Registry through the snapshot parser to the provider. Registry forwards every declared
custom tool — Skills never narrow the tool set; malformed definitions, duplicate names,
and names colliding with reserved tools fail configuration. Provider registration explicitly
opts into callback support; other runner providers reject nonempty callback declarations.
Custom callbacks bypass the
`agent_toolset` confirmation policy: one `agent.custom_tool_use` event supplies the
canonical `evt_` ID, tool name, and arguments. There is no extra generic tool-use/result
pair or confirmation request. The runner parks the callback before publishing it, then
emits `session.status_idle` with `stop_reason: { type: "requires_action", event_ids }`.
A matching `user.custom_tool_result` resumes the worker and emits running status after
the last pending callback resolves. Final idle status uses `end_turn`.

The independent result route checks the configured Session and validates the public
text, image, document, and search-result block shapes. The shared
[Codex content converter](../libraries/codex-harness.md#client-callback-content)
preserves text and `is_error`, maps inline PNG/JPEG/WebP/GIF images to MCP images,
and encodes text documents and search results as JSON text retaining their content
and metadata. Base64 `text/*` documents are decoded as strict UTF-8. URL/File-store
sources, binary documents, unsupported image MIME types, and malformed encoded
content interrupt the turn with an explicit `agent.error`. Native SDK fixture tests
verify document, search, and PNG continuation. The first result wins, and unknown,
duplicate, or late IDs are harmless. Waits expire after two minutes;
there are at most 256 outstanding callback waits. Interrupt, worker exit, snapshot
replacement, and shutdown release pending waits. The transcript follower delivers
results concurrently with the parked turn and catches up from an explicit cursor;
unrelated agent output does not suppress a result.

A private `orca.harness_checkpoint` control record carries bounded native
rollout files to Registry before turn completion. Registry validates its paths
and size, checks runner ownership, and saves it to `session_harness_states.state`.
It never enters the public transcript or SSE. Reconnection restores those files
and resumes the same native thread. A same-Session/provider/resource-revision
refresh retains both the worker filesystem and the live model-tool root. A new
revision uses its newly committed tool root; stopping closes both roots.
Checkpoint persistence failure leaves the turn without a completion marker and
retires the bridge, stopping turns and controls for that runner generation. A
reconnected bridge restores the durable snapshot before accepting another turn.
The private state table keeps native history out of ordinary Session queries.

A callback whose turn has no durable `agent.turn_completed` when the runner reconnects
is abandoned, including when its client result already reached the transcript. Replay
rebuilds the transcript, but cannot restore the suspended SDK callback. The bridge
records `session.error` with `custom_tool_callback_abandoned`, terminal idle, and the
turn-completed marker under the current ownership fence. The bridge stamps the source
user-event ID on callback records, so later queued messages retain their own turn. It does not re-run that call
or bind an old result to a new call. A new message can start an unguarded turn; existing
uncertain-usage markers continue to block guarded requests.

Checkpoints include a SHA-256 fingerprint of the effective Orca developer
instructions, including the managed Skill catalog. Both Codex adapters and the
shared worker check it before native resume. For self-hosted managed resources,
a `block_skills` refresh transitions the pinned SDK rollout's managed developer
block to the new catalog while preserving its thread, conversation and tool
history. The old and new catalogs must both be exact subsets of the same verified
Skill pins with unchanged Agent instructions; an instruction or Skill-version
change still fails closed. The original durable checkpoint is retained until the
next turn emits its replacement through the normal persistence barrier.
Credentials and sandbox working-directory changes do not affect the fingerprint. Legacy checkpoints
without a fingerprint resume only when a single pinned SDK `0.154.0` rollout
records the exact nonempty text in its initial developer message. Unverifiable
legacy history fails closed, and a successful legacy resume writes a fingerprint
with its next checkpoint.

Remote Codex MCP bindings publish `agent.mcp_tool_use` and
`agent.mcp_tool_result`, retaining the upstream tool name, `mcp_server_name`, and
matching `mcp_tool_use_id` even on permission denial or upstream tool failure.
The encoded tool name remains the permission lookup key.

Registry refreshes the Codex snapshot before each turn, rebuilding MCP clients with
fresh credentials. Both MCP and LLM JWTs cover the ten-minute turn limit with
a lifetime of at least eleven minutes.
The Gateway JWT uses the configured audience, an exact model allowlist and the
`llm-responses` route; the token has no MCP or vault scopes. The worker sends
`X-Orca-Session-Id` and uses native HTTP Responses streaming. Usage maps cached
input separately from uncached input. For managed Codex snapshots, Registry owns
request guardrail enforcement and durable usage accounting. Failure to persist
usage or completion accounting retires the bridge generation before another turn
can start; pending usage markers remain for ownership-fenced reconnect recovery.
The private
`request_guardrails_owner: "registry"` field requires a managed, single-agent
`codex-sdk` snapshot; the generic runner loop does not evaluate those request
rules again. For stateful policies, Registry persists request decisions before SDK submission, then
acknowledges priced usage, native history and pending-marker deletion before
forwarding completion. Missing usage or interrupted turns retain the marker and
block subsequent guarded requests while control followers remain available. Unguarded
interrupts do not leave a budget marker. Public Agent/Session APIs retain Orca's
Managed Agents event format.

Gateway deployments configure:

- `AI_GATEWAY_MCP_URL`: the gateway MCP endpoint.
- `AI_GATEWAY_LLM_URL`: the Responses base URL, e.g. `http://ai-gateway:8090/v1`.
- `SESSION_JWT_LLM_ROUTES`: includes `llm-responses`.
- `SESSION_JWT_LLM_MODELS`: the deployment's allowed model IDs or patterns.

The Gateway must serve native `/v1/responses`, preserving Responses input,
reasoning items, tools, and SSE. A Chat Completions translation is insufficient.
The OpenAI provider key is injected into the Gateway through its vault resolver.
For an operator-managed direct-provider runner, `OPENAI_API_KEY` is the explicit
fallback when no Gateway LLM credential is supplied; a worker must include it
in its explicit `ORCA_RUNNER_ENV_PASSTHROUGH` to pass it to a child runner.

The Helm chart's `aiGateway.openai.enabled` configures a native Responses route,
OpenAI destination and env vault. `aiGateway.openai.apiKeyEnv` defaults to
`OPENAI_API_KEY`; `aiGateway.extraEnv` can bind it to a Secret key. Set
`sessionJwt.llmRoutes` and `sessionJwt.llmModels` to authorize use. Override
`registry.aiGatewayLlmUrl` for an external Gateway. Use a Gateway image that
implements the native Responses route; the image tag alone is not capability
validation.

This SDK integration accepts text input, sandbox tools, remote MCP tools, client
callback tools, and managed Skills. Managed colocated Sessions receive File, Memory, and Git mounts
through bounded resource delivery. After each sandbox or remote MCP tool operation the runner
publishes its public result, freezes changed outputs/writable Memory, and emits a
private `orca.resource_checkpoint` record. The SDK receives the tool result only
after the Registry persists the checkpoint and sends the matching ACK. Checkpoint
failure ends the turn and prevents native continuation. Reconnects retrieve pending
changes before applying a snapshot. After acknowledging pending writes, Registry
rebuilds Memory descriptors and the runner refreshes shared Memory updates and
deletions between turns, preserving Git, Skills, outputs and unacknowledged writes.
On a Memory compare-and-swap conflict, Registry persists one logical
`session.memory_conflict` event with the expected and observed hashes before the
last-writer-wins overwrite and checkpoint ACK. If that overwrite fails and a
concurrent writer changes the observed hash, a retry rejects the stale conflict
receipt without overwriting the new Memory or acknowledging the checkpoint.
Failed Skill replacements retain
the previous tree and retry on snapshot delivery without clearing checkpoint failures.
The runner holds no object-store credentials.
Both modes reject multiagent rosters, including use as a roster member.
Other input content fails explicitly.
The discovery response reports these capability boundaries.

The installed SDK tests use a loopback mock OpenAI endpoint and real Codex
executables. Setting `CODEX_TEST_GATEWAY_BINARY` to a locally built
`orca-gateway` also exercises the native Gateway HTTP route, JWT authorization,
and vault key replacement in `codex-sdk-worker.spec.ts`; no paid provider call
is made by those tests.

Pi-only direct credentials also accept `PI_SDK_PROVIDER_CREDENTIALS`, a JSON map of
provider IDs to `apiKeyEnv` and optional `baseUrlEnv` references. `GEMINI_API_KEY`,
`GEMINI_BASE_URL`, `ZAI_API_KEY` and `ZAI_BASE_URL` select Gemini and ZAI directly.
See [Pi SDK credentials](../libraries/pi-harness.md#credentials-and-native-gateway-transport).
