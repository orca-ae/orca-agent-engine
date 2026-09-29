# session-runner scope

The session-runner is the per-session process of a self-hosted environment: it dials the registry's
runner tunnel outbound, serves the requests the owning registry pod pushes down it, and drives one
agent harness for one session. This file records the runner's boundary — what the runner itself
owns, what it takes from the rest of Orca instead of reimplementing, and what is deliberately out of
scope — so a new responsibility can be placed against it. The component description (routes, wire,
configuration) is [`services/session-runner.md`](./services/session-runner.md).

The boundary follows from one constraint: a self-hosted runner sits behind a NAT and reaches nothing
but the registry. Anything that needs a database, a broker, an object store or a credential therefore
stays on the Orca side of the tunnel, and the runner keeps only the glue between the tunnel and a
harness.

## What the runner owns

| Responsibility           | Where                                                  | What it does                                                                                                                                                                                         |
| ------------------------ | ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tunnel client            | `src/tunnel/serve.ts`                                  | dials `/v1/tunnels/runners/:runnerId`, serves framed request/response over it, answers keepalives, reconnects with backoff                                                                           |
| Entrypoint and lifecycle | `src/main.ts`, `src/idle.ts`                           | reads the binding token, registry tunnel URL and workspace root; connects; shuts down gracefully after the idle timeout                                                                              |
| Turn loop                | `src/session-loop.ts`                                  | receives a user turn, runs the provider, streams its agent events up the tunnel                                                                                                                      |
| Session lifecycle        | `src/session-loop.ts`, `src/harness/provider.ts`       | builds and starts the provider the snapshot names; stops and cleans it up                                                                                                                            |
| Snapshot and recovery    | `src/snapshot.ts`, `src/session-loop.ts`               | configures the provider from the credential-free snapshot; applies the pushed `after={cursor}` replay slice with dedup (`SessionLoop.applyReplay`) and reports `resumeCursors()` on the tunnel hello |
| Approval parking         | `src/pending-approvals.ts`, `src/tool-confirmation.ts` | parks the verdict for a gated tool call and resolves it when the registry delivers the transcript's `user.tool_confirmation`                                                                         |

## What it reuses from Orca

- **Sandbox.** `@orca/sandbox-runtime` — the same `SandboxRuntime` / `SandboxHandle` boundary
  harness-server uses. The `agent_toolset` tools (bash, read, write, edit, list, delete, glob, grep)
  execute through `SandboxHandle`: its `files` API and `run`.
- **Web fetch.** The runner adds no fetch tool of its own: the Claude providers keep the Agent SDK's
  built-in `WebFetch` alongside the `orca` tools.
- **MCP and LLM egress.** The ai-gateway. The snapshot carries the gateway-rewritten MCP configuration
  and a scoped session JWT; the gateway swaps the JWT for the real credential, so the runner never
  holds one.
- **Workspace, resources and skills.** The environment-worker hands the runner its workspace root.
  Managed resources and skill bundles arrive as bytes on the tunnel's resources and skills routes and
  the runner materializes them locally, so it never holds object-store credentials.
- **Policy.** Orca's permission model and the transcript approval: approval parking is the client half
  of it. Guardrails ride the same snapshot. The runner evaluates stateless top-level `request`
  guardrails, delegates managed Codex request budgets to the Registry's durable preflight, and refuses
  a snapshot carrying unsupported phases, other stateful rules or subagent-scoped rules — see
  [`guardrails.md`](./guardrails.md) ("Self-hosted runner sessions").
- **Session routing.** The registry's session → runner affinity (`sessions.runner_id`, the
  `TunnelRegistry`, claim-based distribution) decides which runner serves a session; the runner does
  no routing of its own.
- **Transcript.** The `@orca/transcript-store-types` contract, implemented by an in-memory store the
  tunnel feeds. Durable history stays in the registry, which pushes the recovery replay down.

## Native-CLI providers

Besides the in-process Claude SDK providers, the runner drives native coding CLIs — Claude Code,
Codex, Cursor, Pi, and an operator-declared `custom` CLI described by the snapshot's `custom_spec` —
as long-lived children. They share one foundation:

- **`SandboxHandle.spawn`**, the streaming spawn primitive. It is optional on the interface, because
  cloud-only runtimes may omit it, so a provider feature-detects it and fails loudly rather than run
  unsandboxed.
- **`TmuxSandboxHandle`**, a runner-side tmux backend: stdout is the pane, stdin is `send-keys`, kill
  is `kill-session`. An operator on the runner host can `tmux attach` to a running CLI.
- **The launch framework** (`src/sandbox/native-cli-launcher.ts`), one launch path that each provider
  feeds its own CLI arguments.
- **The tool bridge** (`src/mcp/native-cli-bridge.ts`), which exposes Orca's tools to the CLI as its
  `orca` MCP server. The CLI's own approval request routes to the one transcript-backed gate in
  `src/tool-confirmation.ts`.
- **`sys_terminal_*` tools** (`launch`, `send`, `read`, `list`, `close`) for driving a REPL, pager or
  installer inside the sandbox — additional Orca tools, a deliberate superset of Anthropic's toolset.
- **Remote terminal attach.** A client opens `/v1/sessions/:sessionId/terminals/:terminalId/attach`
  on the registry; the registry opens a WebSocket channel down the runner tunnel, and the runner
  bridges it to the tmux pane.
- **Capability match.** The runner advertises the providers it registered on its tunnel hello, and
  the registry fails a session bound to a runner that does not advertise its provider. The worker
  hello's `configured_harnesses` map feeds a pre-spawn check that refuses a session whose provider
  the worker cannot serve (`capability_mismatch`); the environment-worker sends
  `configured_harnesses: null`, which the registry treats as not advertised, so that check does not
  run.

The `codex-sdk` and `pi-sdk` providers run the Codex and Pi SDKs in a child process started through
the same launch framework, with Orca's tools served by the same bridge.

Agent events from a native CLI are durable through the transcript; the tmux pane is a view onto the
process, not the record.

## Multiagent

Multiagent follows Anthropic's thread model: one session, several threads, one shared sandbox. The
registry serves the `session_threads` API (list, get, per-thread events and stream, interrupt,
archive) and the `multiagent` agent field. On the runner, `CoordinatorHarness` wraps the
coordinator's provider: a delegation runs the roster agent in its own thread, in the same process and
sandbox, and the coordinator emits the primary-thread events (`session.thread_created`,
`session.thread_status_*`, `agent.thread_message_received` / `agent.thread_message_sent`). It allows
at most 25 concurrent threads and one level of delegation. Distribution is unchanged: one session is
served by one runner, with no child sessions.

## Claude SDK providers

Two in-process providers use the Claude Agent SDK. Both emit Orca-native `AgentEvent`s, take the LLM
base URL and scoped JWT from the snapshot's egress block, route tool calls through `canUseTool` to
the transcript approval, and map thinking blocks and tool-use/result pairs through the shared
`SdkMessageMapper`. They differ in turn model and in how much lifecycle they carry:

- **`claude` (A, lean)** — a stateless one-shot `query()` per turn that reloads history from the
  transcript through `sessionStore`. Nothing lives between turns, so recovery is a transcript reload.
  A `user.interrupt` aborts the in-flight turn and keeps the harness for the next one, and the query
  is force-closed on interrupt, stop or fault so a torn-down turn never leaks the CLI subprocess.
- **`claude-sdk-persistent` (B, persistent)** — one live streaming-input `query()` kept across turns.
  A per-turn model override on the user message is validated, optionally constrained to an operator
  allow-list, and applied with `setModel`. An interrupt closes the live session and the next turn
  rebuilds structured history from the transcript; a stream fault marks the session crashed and the
  next turn self-heals the same way. `api_retry` frames surface as `agent.status` diagnostics, and an
  auth or endpoint failure ends the turn. The live handle is force-closed on interrupt, stop or fault.

Both are selectable per agent. `claude` keeps the stateless model on purpose: a persistent client's
lifecycle works against reconnect and recovery, which the one-shot model gets from the transcript.
Neither provider wraps the SDK process in a sandbox — the `orca` tools run through `SandboxHandle`,
and the SDK's built-ins are anchored at the sandbox root.

## The `oeadm` client

`@orca/oeadm` is the operator CLI and a pure client on the registry's HTTP API: `oeadm run` and
`oeadm attach --session` drive a session through the public events API (`user.message`, the SSE
stream, `user.tool_confirmation`), `oeadm attach --terminal` proxies the terminal-attach WebSocket,
`oeadm env create` creates an environment and prints its key, and `oeadm worker` runs an
environment-worker on the local machine. The runner itself has no REPL and no terminal UI.

## Out of scope

These are deliberately not part of the runner, and Orca's API exposes none of them:

- **Cost control.** No cost advisor that picks models and no cost judge. Model selection is explicit:
  the agent's `model`, plus a per-turn override where the provider supports one.
- **A catalog-function tool.** Vendor and platform tools are reached as MCP servers through the
  gateway.
- **Comment tools** (listing and updating comments). Orca has no comments API.
- **Web-UI terminal HTTP endpoints.** Terminal access is the remote terminal-attach WebSocket route
  and a local `tmux attach`.
- **A built-in REPL or terminal client.** `oeadm` is the client.
