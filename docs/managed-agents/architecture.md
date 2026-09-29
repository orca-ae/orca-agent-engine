# High-Level Architecture

This document describes how the components fit together at runtime. Read [`overview.md`](./overview.md) first for context and decisions; this doc focuses on data flow and inter-component contracts.

## Component model

The platform has **three** in-repo deployable services, one external gateway image, and a small set of pluggable **store libraries**:

| Kind           | Name                     | Language                          | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------- | ------------------------ | --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Service        | `registry-server`        | TS (Node 22)                      | Public Anthropic-compatible REST API, Orca Core extensions, control plane, and Postgres-coordinated cron Trigger worker. **The shared server**: also coordinates Registry-owned Sessions over the WS tunnel, including self-hosted `codex_sdk` and `pi_sdk` Sessions.                                                                                                                                                                                    |
| Service        | `harness-server`         | TS (Node 22)                      | Hosts cloud `separate` agent loops (Claude Agent SDK, Codex SDK, Pi SDK); dispatches tool calls to a sandbox. Also drives cloud `claude_code`, `codex_sdk`, and `pi_sdk` `colocated` Sessions through the in-sandbox HTTP/SSE bridge. Skips Registry-owned Sessions using the shared execution-owner rule.                                                                                                                                       |
| Service        | `session-runner`         | TS (Node 22)                      | The `colocated` engine: the agent loop and its tools run together inside the sandbox; serves `/v1/runner/*` over the WS tunnel back to the registry. Registers ten providers (`claude` / `claude-sdk-persistent` / `claude-code` / `codex-sdk` / `pi-sdk` / `codex` / `cursor` / `pi` / `custom` / `mock`) plus the wrapping `multiagent` coordinator harness. Executes Registry-owned Sessions; cloud `claude_code`, `codex_sdk`, and `pi_sdk` `colocated` Sessions use the sandbox-harness bridge instead. |
| Package        | `@orca/sandbox-harness`  | TS (Node 22)                      | Managed-agents HTTP server built into per-harness sandbox images; runs the harness process inside the sandbox. The cloud `claude_code`, `codex_sdk`, and `pi_sdk` `colocated` engine — see [`services/sandbox-harness.md`](./services/sandbox-harness.md).                                                                                                                                                                                     |
| External image | `ai-gateway`             | `ghcr.io/orca-ae/orca-ai-gateway` | MCP egress (and model egress when routed through it) + vault resolution + audit.                                                                                                                                                                                                                                                                                                                                                              |
| Library        | `@orca/transcript-store` | TS                                | `TranscriptStore` interface; default backend = Kafka (Apache Kafka or any Kafka-compatible backend, retained for `separate`); Postgres and Apache Pulsar backends for self-hosted deployments. The `colocated` path appends in-process from the registry and is broker-free when deployed on the Postgres backend (the recommended `colocated` deployment; the bridge is backend-agnostic — Kafka remains the stack default).                                                  |
| Library        | `@orca/file-store`       | TS                                | `FileStore` interface; backend = `LocalFileStore` (S3-compatible blobs + Postgres metadata, SHA-256 dedup).                                                                                                                                                                                                                                                                                                                                    |
| Library        | `@orca/memory-store`     | TS                                | `MemoryStore` interface; backend = `LocalMemoryStore` (S3-FUSE live state + sha-keyed version archive + Postgres metadata).                                                                                                                                                                                                                                                                                                                    |
| Library        | `@orca/skill-store`      | TS                                | Immutable digest-addressed Skill bundles in S3/MinIO; Registry writes and Harness reads exact Session pins.                                                                                                                                                                                                                                                                                                                                    |
| Library        | `@orca/harness-catalog`  | TS                                | Single source of truth for harness types, supported modes, default sandbox images, and ports.                                                                                                                                                                                                                                                                                                                                                  |
| Library        | `@orca/harness-tunnel`   | TS                                | The WS tunnel engine. `session-runner` dials out to the registry over it from any sandbox location (local, cloud, self-hosted) — one transport for Registry-owned Sessions.                                                                                                                                                                                                                                                                    |
| Library        | `@orca/guardrails`       | TS                                | Guardrail rule model, builtin catalog, composition, and evaluation engine — pure, no I/O.                                                                                                                                                                                                                                                                                                            |

Stores are **not** services. They are TypeScript interfaces with one or more
backend implementations. `registry-server` and `harness-server` import them as
workspace dependencies (`workspace:*`) and call them in-process. Kafka
(Apache Kafka or any Kafka-compatible backend) is the default transcript substrate;
Postgres and Apache Pulsar can be selected for transcript events with
`TRANSCRIPT_STORE_BACKEND=postgres` or `TRANSCRIPT_STORE_BACKEND=pulsar`.

This component model is honoured throughout `services/` and `packages/`, with
one deliberate exception: `services/sandbox-harness/` is an **image payload**,
not a cluster service. It lives under `services/` because it builds and ships
like one — its own `package.json`, `tsup` build, and Dockerfile — but nothing
deploys it standalone; it is baked into the per-harness sandbox images that
`colocated` agents run.

## Diagram

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/agent-engine-architecture-dark.svg">
  <img alt="Clients (SDK, CLI, UI) call the registry, which coordinates a harness server that runs the agent loop; session code executes in a sandbox; the AI Gateway governs MCP tool calls from both, and model calls from colocated harnesses; transcripts and audit logs stream out to the data-streaming layer." src="../images/agent-engine-architecture-light.svg">
</picture>

<sub>The model path shown is `colocated`. On `separate`, harness-server calls the model
provider directly (for the Claude Agent SDK, `api.anthropic.com` or the configured
`ANTHROPIC_BASE_URL`) unless the Session's LLM egress is `gateway` (`LLM_EGRESS_DEFAULT`, or
`metadata.orca_llm_egress`), in which case model calls go through the gateway — MCP egress
goes through it in both topologies.</sub>

## System diagram

The ASCII topology below carries the wire-level detail the diagram above
elides — headers, ports, and which calls are in-process library calls.

```
                ┌──────────────────────────────────────────────────────────┐
                │ Anthropic SDK client (or first-party Orca client)        │
                └──────────────────────────────────────────────────────────┘
                                          │ HTTPS  (x-api-key | OIDC/JWT)
                                          │ + orca-beta: managed-agents-<ver>
                                          │   (anthropic-beta is ignored if sent)
                                          ▼
                                 ┌────────────────────────────────────┐
                                 │ registry-server  (Node 22)         │
                                 │ Public API: /v1/{agents,           │
                                 │   environments, sessions, files,   │
                                 │   memory_stores, vaults, skills,   │
                                 │   triggers}                        │
                                 │ Imports (workspace:*):             │
                                 │   • @orca/transcript-store         │
                                 │   • @orca/file-store               │
                                 │   • @orca/memory-store             │
                                 │   • @orca/skill-store              │
                                 │ Owns: Postgres metadata            │
                                 └────────┬───────────────────────────┘
                                          │ in-process library calls
                ┌─────────────────────────┼────────────────────────────┐
                │                         │                            │
                ▼                         ▼                            ▼
       ┌────────────────┐        ┌────────────────┐         ┌────────────────┐
       │ Kafka          │        │ Object store + │         │ Object store + │
       │ topic-per-     │        │ Postgres       │         │ Postgres       │
       │ session        │        │ (file metadata)│         │ (memory live + │
       │                │        │                │         │  versions)     │
       └─────┬──────────┘        └────────────────┘         └────────────────┘
             │ Kafka consumer group
             │ (control + user events)
             ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│ harness-server (Node 22)            INTERNAL ONLY — no public listener          │
│  SessionRunner   AgentHarness (Claude Agent SDK v1)                             │
│  Imports @orca/transcript-store (workspace:*); calls .append/.read/.tail        │
│  ClaudeAgentSdkAdapter implements SDK's SessionStore over the library           │
│  SandboxRuntime: selected by SANDBOX_RUNTIME; lazy-provisioned                  │
│  Tool routing:                                                                  │
│    • agent_toolset    → SandboxRuntime (bash/read/write/edit/glob/grep/web_*)   │
│      mounts: file-store reads materialized; memory-store mounted via shim       │
│    • mcp_toolset      → ai-gateway (/v1/mcp, session JWT) with X-Orca-Credential-Id│
│    • custom           → emit event; wait for client response                    │
└──────┬──────────────────────────────────┬──────────────────────────────────────┘
       │                                  │
       │ HTTPS (Claude Agent SDK)          │ HTTP POST /v1/mcp (session JWT)
       │  → api.anthropic.com              │  + X-Orca-Credential-Id, X-Orca-Session-Id
       ▼                                  ▼
                                 ┌──────────────────────────────┐
                                 │ ai-gateway (external image)│
                                 │ Resolves credential_id via   │
                                 │   registry /internal/...     │
                                 │ Forwards MCP JSON-RPC        │
                                 │ Audit → Kafka topic          │
                                 └──────────┬───────────────────┘
                                            ▼
                                   Backend MCP servers
```

This diagram shows two deployed in-repo services — `registry-server` and `harness-server` — plus the external `ai-gateway` image and pluggable store libraries (`@orca/transcript-store`, `@orca/file-store`, `@orca/memory-store`, `@orca/skill-store`) imported in-process. `session-runner` belongs to the `colocated` path in the next diagram. The standalone `observability-exporter` runtime consumes Kafka Transcript and Registry's scoped resolver pair outside both request-path diagrams. One deployment serves many logical workspaces: public API keys resolve to one server-side workspace context, and public resources do not carry `workspace_id`. `ai-gateway` exposes data traffic on `/v1/mcp` and admin health/metrics on its admin listener.

This diagram traces the cloud `separate` path; cloud `claude_code`, `codex_sdk`, and `pi_sdk` `colocated` Sessions follow it too, with the agent loop inside the sandbox behind the sandbox-harness HTTP/SSE bridge. For Registry-owned Sessions (coordinated by the registry as shared server over the WS tunnel, for both `target=cloud` and `target=self_hosted`), see [`deployment-topologies.md`](./deployment-topologies.md) and the section below.

### `colocated` topology — the registry as shared server

For a Registry-owned `colocated` Session — every self-hosted one, and cloud `codex`, `cursor`, `pi`, `custom`, and `mock` — the agent loop and its tools run _together_ inside the sandbox, driven by `session-runner`. **The registry is the shared server: the single coordinator for these sessions, over the WS tunnel, for every sandbox location** (local dev, Orca-managed cloud, self-hosted). `harness-server` is not involved:

```
client ─HTTP→ registry ─append(user.*, producedBy=client)→ transcript-store ← source of truth
                 │
                 │  registry · shared server (SessionEventBridge — single writer, persist-before-forward)
                 ▼
        streaming POST /v1/runner/turn  ── over the WS tunnel ──►  session-runner (dials the tunnel)
                 ▲                                                        │  loop + tools, in the sandbox
                 └──────────────── agent.* (NDJSON) ─────────────────────┘
                 │
                 ▼  append (producedBy=harness, before the next event is consumed)
            transcript-store ← source of truth
```

Registry persists each streamed agent event before consuming the next one. This
single-writer path serves all self-hosted Sessions and Registry-owned cloud
harnesses. Cloud `claude_code`, `codex_sdk` and `pi_sdk` share the sandbox-harness
HTTP/SSE bridge, with harness-server as their transcript writer. Both services
consult the same pinned-harness ownership rule; a connected worker never changes
that ownership. See [`harness-modes.md`](./harness-modes.md) for the dispatch table
and [`deployment-topologies.md`](./deployment-topologies.md) for the topology diagrams.

`github_repository` mounts use a host-side `GitWorker` +
`WorkDirManager` inside `harness-server` (no new library, no new service)
plus a public `POST /v1/git-creds` route on `registry-server` that the
in-sandbox `orca-git-creds` credential helper round-trips through. The
PAT lives only in the registry's per-call resolution path; sandboxes only
ever hold a session-scoped `aud='git-creds'` JWT. See
[`services/harness-server.md`](./services/harness-server.md) and
[`services/registry-service.md`](./services/registry-service.md) `/v1/git-creds`.

## Interaction model: registry vs harness

Runtime events flow through the selected transcript backend. The services only
call each other directly for narrow mesh-internal control-plane operations.
Harness prepares an immutable, workspace-validated execution snapshot before
creating a sandbox, then uses workspace/session-scoped routes for JWT minting,
dynamic memory access, artifact registration, and lifecycle state updates.

```
Client ─────────────► registry-service (public)
                         │
                         │ TranscriptStore.append(...)  (user events)
                         │ TranscriptStore.tail(...)    (SSE relay)
                         │ Postgres read model          (list history)
                         ▼
                   Transcript backend
                   (Kafka default, Postgres, or Pulsar)
                         ▲                         │
                         │ consume control + user  │ append (agent events)
                         │ via backend event source│
                         │                         │
                   harness-server (internal-only)  │
                   • Consumer group on             │
                     control events                │
                   • Runs Claude Agent SDK loop ───┘
                   • Calls the model provider (LLM), or ai-gateway
                     when the Session's LLM egress is gateway
                   • Calls ai-gateway   (MCP tools via /v1/mcp)
                   • Calls SandboxRuntime (built-in tools)
```

### Why this works

1. **Single public base URL preserved.** Anthropic SDK (or any first-party client) talks to one host. Registry owns all `/v1/*`. Harness has no public listener.
2. **No registry→harness RPC.** Registry doesn't know which replica is running a given session; it doesn't need to. The selected transcript backend distributes work across harness replicas with at-least-once delivery: Kafka consumer groups for Kafka, Postgres lease claims for Postgres, and `KeyShared` subscriptions for Pulsar.
3. **No harness→registry RPC for events.** Harness publishes via `TranscriptStore.append`. Registry consumes live streams via `TranscriptStore.tail` and keeps a Postgres `session_events_index` read model for list history. Both call the selected backend through the `@orca/transcript-store` library. Harness does call registry's internal listener for a prepared execution snapshot and workspace/session-scoped runtime operations.
4. **SSE termination at registry.** Registry holds the long-lived client connection and reads from a `TranscriptStore.tail` stream. Reconnect with `Last-Event-ID` resumes the backend cursor. Registry replicas are stateless beyond the open socket.
5. **Failure isolation.** Harness can crash, restart, scale up/down — the selected backend redelivers unprocessed control/user events to a healthy replica. Registry can crash — clients reconnect, cursor is preserved. Kafka handles high-scale durable streaming; Postgres handles local/test/smaller self-hosted use through durable rows and leases; Pulsar handles Pulsar-native streaming with `KeyShared` subscriptions.
6. **Harness stays internal.** It has no public listener and holds no workspace
   API key. The event's `(workspace_id, session_id)` is cross-checked against
   the Registry-prepared snapshot before side effects. Harness authenticates to
   Registry's separate internal listener with a projected ServiceAccount JWT
   in Kubernetes or the shared internal service token elsewhere. Store access
   (`@orca/transcript-store`, `@orca/file-store`, `@orca/memory-store`,
   `@orca/skill-store`) is in-process and every tenant key or query includes
   workspace scope.
7. **Cron scheduling stays with Registry metadata.** Every Registry replica may
   run the Trigger planner and dispatcher. Short PostgreSQL transactions,
   `SKIP LOCKED`, a unique fire slot, and generation fencing coordinate them;
   dispatch commits an ordinary Session plus its initial-event outbox before
   any transcript I/O. This is a logical worker boundary, not a third service.

### Mesh-internal control plane

The hot event path stays backend-mediated. Synchronous service calls are limited
to mesh-internal control-plane work:

- **Session lifecycle.** `POST /v1/sessions` creates metadata in `idle`.
  `POST /v1/sessions/{id}/events` appends `user.*` events and marks the row
  `running`; the transcript backend delivers that event to a harness replica.
  After the turn, harness calls `PATCH /internal/v1/workspaces/{workspace_id}/sessions/{id}/state` to mark
  the row `idle` while keeping the warm sandbox handle. When the idle timeout
  fires, harness destroys the sandbox and clears `sandbox_handle_id`.
  Session archive commits a `session.archived` lifecycle event and permanent
  delete commits `session.deleted` to a Registry outbox in the same metadata
  transaction. Registry immediately attempts publication and periodically
  retries pending rows, so a successful API response cannot permanently strand
  a warm runner when the transcript backend is temporarily unavailable. Public
  event streams expose only `session.deleted`, which terminates every active
  session/thread stream.
- **Harness boot dependencies.** Harness calls
  `POST /internal/v1/workspaces/{workspace_id}/sessions/{id}/executions:prepare`.
  Registry atomically validates the pinned agent graph, session-pinned
  SkillVersions, environment, vault metadata, and active resources. Harness
  then opens the exact immutable Skill bundles through the in-process
  `@orca/skill-store` library and materializes them read-only in the sandbox
  before runner startup. Later internal calls retain the same workspace/session
  path scope. These calls are not used for event delivery.
- **Operational queries** (e.g., admin debug "which replica is running session X?"). Harness exposes internal health/diagnostic endpoints as needed. This is for ops, not the hot path.

The rule is: **events go through the transcript backend; metadata/control
queries use mesh-internal HTTP.**

### What this rules out

- We are NOT proxying SSE through registry to harness. Architecturally simpler to terminate at registry.
- We are NOT routing public traffic to harness via a path-based Ingress. Harness has no public listener.
- We are NOT building `transcript-store` as a standalone gRPC service. It is a library (`@orca/transcript-store`) that registry-server and harness-server both import; non-managed-agents consumers (eval pipelines, analytics, debugging UIs) either import the library directly or read the selected backend directly.

### The `colocated` event bridge: the registry as shared server, over the tunnel

Sessions that `harness-server` owns distribute turns through the transcript
backend's consumer group — any `harness-server` replica can pick up a `user.*`
event; that is unchanged by any of this section. Registry-owned sessions work
differently: the agent runs in a `session-runner` reachable only over a
**runner tunnel** (a
WebSocket the runner dials _out_ to the registry) — whether that runner was
spawned by the customer's own `environment-worker` (`target=self_hosted`) or by
the Orca-managed cloud host (`target=cloud`; see
[`deployment-topologies.md`](./deployment-topologies.md)). The replica that
holds that environment's worker-tunnel claim — the **owner pod** — is the only
replica that can reach the runner, so it takes a dedicated role for that
session.

This is the shared-server model: the registry, not `harness-server`,
coordinates Registry-owned execution. Exactly one driver per session, chosen by
the shared execution-owner rule (`resolveExecutionOwner` over the Environment
`target` and the pinned harness and mode) — `harness-server`'s `Dispatcher`
drives cloud `separate` sessions and cloud `claude_code`, `codex_sdk`, and
`pi_sdk` `colocated` sessions (the latter through the in-sandbox DialIn
bridge); the registry's owner-pod bridge below drives every other session, on
either `target`. `harness-server` **skips** Registry-owned sessions after
reading Registry's internal execution-owner route (see
[`harness-modes.md`](./harness-modes.md), "Dispatcher routing", and
[`deployment-topologies.md`](./deployment-topologies.md)).
Whichever path a given session is on, exactly one
component drives it, never both: a session driven by two components at once —
the Dispatcher's sandbox runner _and_ the owner-pod bridge's tunnel runner —
would have its agent events written by two writers, breaking the single-writer
invariant this section establishes.

The owner pod runs one `SessionEventBridge` per connected runner (managed by
`SessionEventBridgeManager`, wired to the runner-tunnel connect/disconnect hooks).
The bridge is the **single writer** of that session's agent events and the
turn-forwarder of its user events:

```
client ─POST /v1/sessions/:id/events─► registry (ANY replica) ─append(user.*, client)─► transcript
                                                                          │
owner pod (holds the env's worker-tunnel claim)                          │ follow
  └ SessionEventBridge ── catch-up + live tail ────────────────────────────┘
        │  for each un-driven user.* turn:
        ▼
   streaming POST /v1/runner/turn (over the runner tunnel) ── runner streams ──► NDJSON agent events
        │                                                                            │
        └ persist each agent event (producedBy=harness) BEFORE the next ◄────────────┘
                 │  (persist-before-forward = invariant I1, single writer)
                 ▼
            transcript  ──► the existing SSE path (any replica) tails it to clients
```

Key properties:

- **Persist-before-forward (I1).** Each agent event the runner streams is appended
  to the transcript (awaited, in order) before the next is consumed. Clients only
  ever observe persisted agent events — the SSE relay tails the same transcript,
  so there is **no cross-replica HTTP forwarding**; the transcript is the cross-pod
  bus, exactly as in the cloud path.
- **Single writer by construction.** Only the owner pod holds the runner tunnel +
  claim, so every agent event for the session is appended by exactly one bridge
  instance, in runner-stream order. A newest-wins runner reconnect starts a fresh
  bridge that replaces the old one.
- **First-turn catch-up + gapless boundary.** A client can POST the first
  `user.message` before the runner is online (the public append path does not wait
  for distribution). At start the bridge therefore (1) reads the transcript from
  the beginning to find the **un-driven** `user.*` turns — those with no agent
  event after them — and drives them in order, then (2) follows live from an
  **explicit** cursor `head + 1` (a bounded resume, not a from-now subscribe). The
  explicit cursor is gapless across every backend (Kafka seek, Postgres, Pulsar):
  an event that lands during the join window has `seq > head` and is delivered by
  the resume. An already-answered turn (its agent events sit after it) is skipped,
  so a reconnect never re-drives an answered prompt. No separate
  per-session input queue buffers turns across a reconnect — the transcript itself
  is that durable buffer.
- **Lossy-on-drop tradeoff.** The turn request and its agent-event stream are one
  tunneled streaming POST, so a mid-turn tunnel drop loses the un-streamed
  remainder (only persisted-before-drop events survive). A turn that produced _no_
  agent event is self-healing — the next bridge start re-drives it via catch-up —
  but a turn that dropped after partial output is treated as answered (re-driving
  would duplicate the persisted prefix) and is not auto-resumed. See
  [`roadmap.md`](./roadmap.md#designed-not-built).
- **Reverse-lookup recovery on (re)connect.** Before the bridge starts, the same
  owner pod serves a **resume replay** (`SessionRecovery`): it reverse-looks-up the
  session's persisted events and **pushes them forward** over the tunnel
  (`POST /v1/runner/replay`) so the runner rebuilds its state, **deduping by event
  id**. Recovery only _serves_ (read + push, in bounded chunks, never appends);
  the **bridge alone drives turns**, so the two never both drive a turn — a
  partial-then-dropped turn is replayed as state but not re-executed, preserving
  exactly-once. A delivery failure is contained (the bridge still starts; the next
  reconnect retries). See [`services/registry-service.md`](./services/registry-service.md)
  ("Owner-pod reverse-lookup recovery").

The self-hosted distribution state machine that places a session on a worker and
binds the runner is documented in
[`services/registry-service.md`](./services/registry-service.md); this section
covers the event bridge that rides the bound tunnel **and** the reverse-lookup
recovery served alongside it on (re)connect.

## Cross-service auth

Internal cluster traffic is plain HTTP/JSON for current managed-agents service
boundaries; legacy proto/generated stubs remain for CI/type coverage only.
Registry's internal listener always requires application bearer auth:
projected ServiceAccount JWTs verified with Kubernetes TokenReview in
Kubernetes, or a shared service token outside Kubernetes. Istio `STRICT` mTLS
remains recommended for transport encryption and network-level identity;
application code does not manage certificates. Details in
[`auth-and-vaults.md`](./auth-and-vaults.md) and
[`docs/operation/internal-traffic-auth.md`](../operation/internal-traffic-auth.md).

## Kafka topology

- **Per-session topic** for events: `orca.{workspace_id}.sessions.{session_id}.events` in raw mode or `.events-avro` in Avro mode. Subagents differentiated by a Kafka message header `subpath: "subagents/<id>"` (no separate topic — keeps the topic count bounded). Encoding selects one set for all sessions; reads do not merge sets. Changing it requires a [coordinated incompatible cutover](./libraries/transcript-store.md#coordinated-incompatible-cutover-and-rollback), not a live rolling update or history migration.
- **Audit topic** for `ai-gateway`: `orca.{workspace_id}.audit.ai-gateway` — one record per `/v1/mcp` call (workspace, session, backend, method, latency, status).
- Tiered storage and retention are the operator's broker configuration. When the broker tiers cold segments to object storage, `KafkaTranscriptStore` reads transparently across tiers through standard consumer semantics — no application-level archive step. The `@orca/transcript-store` package consumes the Kafka-compatible API as a black box.

## Postgres transcript topology

- `transcript_events` stores append-only events. `seq` is a generated cursor.
- `transcript_event_claims` stores harness delivery state per consumer group.
  Claims have a lease; failed handlers are retried after lease expiry.
- `PostgresTranscriptStore.read/tail` query rows directly; `tail` polls until
  canceled. This backend is intended for local, test, and smaller self-hosted
  deployments; it is not the default backend.

## Pulsar transcript topology

- **Per-session persistent topic**:
  `persistent://{tenant}/{namespace}/orca.{workspace_id}.sessions.{session_id}.events`.
- `PulsarTranscriptStore` uses the official Apache `pulsar-client` Node library.
  Event metadata is stored in message properties and payload bytes stay opaque
  in the message body.
- `PulsarSessionEventSource` consumes client user events with a `KeyShared`
  subscription over a session-topic regex and acknowledges after the runner
  accepts the event.
- Read/tail cursors are numeric values derived from the Pulsar message ID
  (ledger × 10⁹ + entry × 10⁴ + batch index); a message ID that cannot be
  converted falls back to the message's position in the scan. Resume from a non-empty cursor
  starts at the topic beginning and skips messages below the cursor; this keeps
  the existing `Event.seq` shape but is not optimized for very large Pulsar
  topics.

## Per-component responsibilities at a glance

| Component                | Kind                     | Public?                        | Persists                                                     | Calls out to                                                                                                                                                                                                                                                                     |
| ------------------------ | ------------------------ | ------------------------------ | ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `registry-server`        | Service                  | Yes (HTTPS)                    | Postgres metadata + idempotency keys + vault secret refs     | `@orca/transcript-store`, `@orca/file-store`, `@orca/memory-store`, `@orca/skill-store` (in-process); SecretProvider backend                                                                                                                                                     |
| `harness-server`         | Service                  | No                             | Nothing (cattle)                                             | `@orca/transcript-store`, `@orca/file-store`, `@orca/memory-store`, `@orca/skill-store` (in-process); `ai-gateway`, model provider APIs, the configured sandbox runtime (cloud `separate`, and cloud `claude_code`, `codex_sdk`, and `pi_sdk` `colocated`; skips Registry-owned sessions — see "The `colocated` event bridge" above) |
| `session-runner`         | Service (in the sandbox) | No (dials the registry tunnel) | Nothing (ephemeral)                                          | Registry `/v1/runner/*` over the WS tunnel; MCP egress via `ai-gateway`; LLM egress via `ai-gateway` or direct runner credentials                                                                                                                                              |
| `ai-gateway`             | External image           | No                             | None (audit to Kafka)                                        | Registry's workspace/session-scoped vault-credential resolver (ServiceAccount JWT or service token), upstream MCP servers, Kafka audit                                                                                                                                           |
| `@orca/transcript-store` | Library                  | n/a                            | Kafka (default), Postgres, or Pulsar                         | Kafka, Postgres, or Pulsar                                                                                                                                                                                                                                                       |
| `@orca/file-store`       | Library                  | n/a                            | Postgres metadata + object-store bytes                       | Postgres + object store                                                                                                                                                                                                                                                          |
| `@orca/memory-store`     | Library                  | n/a                            | Object store (live + sha-keyed versions) + Postgres metadata | Object store + Postgres                                                                                                                                                                                                                                                          |
| `@orca/skill-store`      | Library                  | n/a                            | Immutable digest-addressed bundle objects                    | Object store                                                                                                                                                                                                                                                                     |
| `@orca/harness-catalog`  | Library                  | n/a                            | None                                                         | None (pure data)                                                                                                                                                                                                                                                                 |
| `@orca/guardrails`       | Library                  | n/a                            | None                                                         | None (pure; state reaches it through consumer-supplied interfaces)                                                                                                                                                                                                               |

See per-service detail in [`services/`](./services/) and per-library detail in [`libraries/`](./libraries/).
