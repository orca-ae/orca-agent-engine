# Self-Hosted Managed Agent Provider — Design Docs

These documents describe the design of Orca's self-hosted alternative to Anthropic's Managed Agents API — the single source of truth for how the pieces fit together and why.

**Every doc here describes what the code does today.** Anything not built — gaps, deferrals, and the conditions that would justify building them — lives in [`roadmap.md`](./roadmap.md) and nowhere else. The reasoning behind a design — the problem it solved, the alternatives it rejected — lives in its Orca Improvement Proposal under [`proposals/`](../../proposals/README.md).

## How to read these docs

If you are new to this work, read in order:

1. [`overview.md`](./overview.md) — the why, the goals, the confirmed decisions, and the engineering principles we've adopted from Anthropic's published guidance.
2. [`architecture.md`](./architecture.md) — high-level architecture diagram, component model, and the interaction model. Read this before any of the per-component docs.
3. [`deployment-topologies.md`](./deployment-topologies.md) — the canonical reference for the two orthogonal axes (`mode` × environment `target`): the topology flows, the responsibility split, and the implemented capabilities. Read alongside `architecture.md`.
4. The per-component docs:
   - Services in [`services/`](./services/) — `registry-service`, `harness-server`, [`session-runner`](./services/session-runner.md), `sandbox-harness`, and the external `ai-gateway` dependency contract. The source tree also contains the Kafka-first Phase 3 [`observability-exporter` runtime](../../services/observability-exporter/README.md). The `session-runner` doc describes the component; the separate [`session-runner-scope.md`](./session-runner-scope.md) records what the runner owns, what it reuses from Orca, and what is out of scope. [`environment-worker`](./services/environment-worker.md) is the process that spawns those runners on a self-hosted Environment.
   - Libraries in [`libraries/`](./libraries/) — `agent-event-contract`, `transcript-store`, `file-store`, `memory-store`, `skill-store`, `guardrails`, `harness-catalog`, `codex-harness`, `pi-harness`, `sdk-harness`, `sandbox-runtime`, `cloud-sandbox`, `harness-tunnel`.
5. The cross-cutting docs:
   - [`mcp-routing.md`](./mcp-routing.md) — how MCP tool calls flow from harness through the gateway.
   - [`resource-mounting.md`](./resource-mounting.md) — how Files, MemoryStores, and GitHub repos are surfaced inside the sandbox.
   - [`mount-strategies.md`](./mount-strategies.md) — the `MountStrategy` interface and the per-resource strategies that implement it.
   - [`output-write-policy.md`](./output-write-policy.md) — sandbox write boundary: deliverable outputs, writable resource exceptions, and enforcement requirements.
   - [`output-capture.md`](./output-capture.md) — how session outputs leave the sandbox and become registered Files.
   - [`memory-conflict-semantics.md`](./memory-conflict-semantics.md) — the CAS window, `session.memory_conflict`, and the customer-facing summary.
   - [`skills.md`](./skills.md) — immutable Skill bundles, session pinning, and progressive disclosure.
   - [`auth-and-vaults.md`](./auth-and-vaults.md) — auth model + vault credential resolution sequence.
   - [`multi-workspace-isolation.md`](./multi-workspace-isolation.md) — workspace/session invariants, runtime snapshot contract, storage namespace, and internal listener boundary.
   - [`workspace-administration.md`](./workspace-administration.md) — Platform/organization admin credentials, organization/workspace lifecycle, API-key provisioning, observability configuration/credential rotation, and bootstrap.
   - [`api-groups-and-extensions.md`](./api-groups-and-extensions.md) — the URL model (`/v1` canonical, `/api/v1` alias, `/api` + `/apis` discovery, `/apis/<group>/<version>` extension groups), how the `orca-extension` tag is computed and pinned, and the deprecation path for `/v1/registry/*`.
   - [`guardrails.md`](./guardrails.md) — declarative allow/ask/deny rules over agent actions: the verdict model, four authority tiers, state, and the `policy.runorca.ai/v1` group.
   - [`pricing.md`](./pricing.md) — per-model price sourcing, resolution, and the `pricing.runorca.ai/v1` group.
   - [`orca-extensions.md`](./orca-extensions.md) — Orca-only headers/routes/internal APIs kept outside Claude compatibility.
   - [`conformance.md`](./conformance.md) — how conformance is measured: the spec-diff pipeline and the decision register that pins every difference.
   - [`conformance-matrix.md`](./conformance-matrix.md) — **generated.** core/missing/extension classification against Anthropic's published OpenAPI spec, with a decision and reference on every difference.
   - [`cron-triggers.md`](./cron-triggers.md) — Orca Core extension for cron-only Agent Triggers, including durable scheduling and Session handoff semantics.
   - [`event-processing-semantics.md`](./event-processing-semantics.md) — the `produced_at` / `processed_at` contract, durable acceptance marker, recovery behavior, and verification plan.
   - [`pagination-and-filters.md`](./pagination-and-filters.md) — current list envelopes, query params, and known limitations.
   - [`data-model.md`](./data-model.md) — Postgres schemas per service.
   - [`agent-harness.md`](./agent-harness.md) — the pluggable `AgentHarness` interface.
   - [`harness-modes.md`](./harness-modes.md) — per-agent `metadata.harness`/`metadata.mode` annotation, the harness catalog, `separate` vs. `colocated` topologies, the bridge/sync, transport abstraction, and implementation status.
   - [`local-stack.md`](./local-stack.md) — operator's reference for the hybrid local stack (`make stack-up`, env vars, troubleshooting, known limitations).
   - [`kubernetes.md`](./kubernetes.md) — Helm chart deployment with external Postgres / Kafka / Pulsar / MinIO / OpenSandbox.
   - [`../compatibility.md`](../compatibility.md) — the AI gateway, `ork` CLI, and TypeScript SDK versions tested with this release.
6. [`roadmap.md`](./roadmap.md) — everything not built yet, with the trigger for each.
   Test coverage is documented with the suites: [`packages/e2e-tests/README.md`](../../packages/e2e-tests/README.md).

## Layout

```
docs/managed-agents/
  README.md                  ← you are here
  overview.md                Context, decisions table, engineering principles
  architecture.md            High-level architecture + interaction model
  deployment-topologies.md   Canonical mode x target topology reference (flows + implemented capabilities)
  mcp-routing.md             How MCP traffic redirects through the gateway
  resource-mounting.md       Per-resource (file/memory/repo) mounting details
  mount-strategies.md        The MountStrategy interface + strategy selection
  output-write-policy.md     Sandbox write boundary + deliverable outputs
  output-capture.md          Session outputs → OutputIndexer → registered Files
  memory-conflict-semantics.md CAS window + session.memory_conflict contract
  skills.md                  Immutable Skill bundles + progressive disclosure
  auth-and-vaults.md         Auth model + vault sequence diagram
  multi-workspace-isolation.md Workspace/session isolation and runtime authorization
  workspace-administration.md Platform provisioning plus organization-scoped workspace/API-key/observability management
  api-groups-and-extensions.md URL model, discovery, computed `orca-extension` tag
  guardrails.md              Allow/ask/deny rules over agent actions + `policy.runorca.ai/v1`
  pricing.md                 Per-model prices + `pricing.runorca.ai/v1`
  orca-extensions.md         Orca-only headers/routes/internal APIs
  conformance.md             How conformance is measured + the decision register
  conformance-matrix.md      Generated: classification + decision per difference
  cron-triggers.md           Cron-only Trigger API + planner/dispatcher semantics
  event-processing-semantics.md `produced_at` / `processed_at` semantics + recovery design
  pagination-and-filters.md  Current pagination/filter behavior inventory
  data-model.md              Per-service Postgres schemas
  agent-harness.md           Pluggable AgentHarness interface
  harness-modes.md           Harness annotation, catalog, separate vs. colocated topologies + bridge
  session-runner-scope.md    What `session-runner` owns, what it reuses from Orca, and what is out of scope
  local-stack.md             Operator's reference for the hybrid local stack
  kubernetes.md              Helm chart deployment (external Postgres/Kafka/Pulsar/MinIO/OpenSandbox)
  roadmap.md                 The only forward-looking doc: gaps + triggers
  services/
    registry-service.md      Public Anthropic-compatible API; control plane
    harness-server.md        Internal; hosts the agent loop and owns sandboxes
    session-runner.md        The `colocated` engine: dials the runner tunnel, runs the loop in the sandbox
    sandbox-harness.md       The in-sandbox HTTP/SSE server for `colocated` harnesses
    environment-worker.md    Dials the registry's worker tunnel from a self-hosted Environment; launches session runners
    ai-gateway.md           External orca-ai-gateway image contract for vault-aware MCP egress
  libraries/
    agent-event-contract.md  `@orca/agent-event-contract` — canonical agent-event IDs, paths, kinds, and payloads (no I/O)
    transcript-store.md      `@orca/transcript-store` — append-only event log over Kafka (library)
    file-store.md            `@orca/file-store` — content-addressed file storage (S3 + Postgres)
    memory-store.md          `@orca/memory-store` — path-addressed memory + sha-keyed version archive (S3 + Postgres)
    skill-store.md           `@orca/skill-store` — immutable digest-addressed Skill bundles (S3)
    guardrails.md            `@orca/guardrails` — guardrail model, catalog, and evaluation engine (no I/O)
    harness-catalog.md       `@orca/harness-catalog` — harness modes, model controls, and pricing (no I/O)
    pi-harness.md            `@orca/pi-harness` — native Pi worker across all three execution paths
    sdk-harness.md           `@orca/sdk-harness` — shared worker wire, usage and callback conversion
    codex-harness.md         `@orca/codex-harness` — shared SDK worker, private tool relay, native history
    sandbox-runtime.md       `@orca/sandbox-runtime` — SandboxRuntime interface + InMemory/Local impls + write policy
    cloud-sandbox.md         `@orca/cloud-sandbox` — E2B + OpenSandbox runtimes
    harness-tunnel.md        `@orca/harness-tunnel` — self-hosted runner/worker WebSocket tunnels
```

Service source lives under `services/` and store-library source under `packages/` at the repo root (see `AGENTS.md` → "Repo map"). Internal cluster traffic between services is plain HTTP/JSON where a service boundary exists. Registry internal calls use ServiceAccount JWT + TokenReview in Kubernetes or a shared service token elsewhere; mesh mTLS remains recommended for encryption. Store libraries are imported in-process via `workspace:*`. Kafka cluster operation (retention, tiered storage) is the operator's broker configuration — application code does not configure it.

## Out of scope (linked but not described here)

- The Anthropic Managed Agents API itself. We mirror its surface; we don't redesign it. See [`overview.md`](./overview.md) for a pointer to Anthropic's docs.
