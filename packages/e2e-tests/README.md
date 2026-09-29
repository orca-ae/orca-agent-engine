# @orca/e2e-tests

End-to-end tests for the Orca Managed Agents local stack. Drives the registry
over raw HTTP against `http://localhost:8080`; the official Anthropic SDK is
pinned and constructed, but assertions go through a `fetch` wrapper — see
"SDK and raw HTTP coverage" below.

Coverage groups:

- **Layer A — wire-protocol conformance** (`test/wire-conformance.spec.ts`).
  Always-on. No `ANTHROPIC_API_KEY` required. Covers the agent, environment,
  session, file, memory and skill routes, asserting response shapes, status
  codes, and SSE handshake semantics — not every public route.
- **Layer A — multi-workspace isolation**
  (`test/multi-workspace-isolation.spec.ts`). Always-on. Provisions two
  workspaces through the admin listener, verifies credential-plane separation,
  cross-workspace resource/binding isolation, file/memory namespace isolation,
  and terminal workspace archive behavior.
- **Layer A — cron Trigger wire contract** (`test/trigger-wire.spec.ts`).
  Always-on. Covers the complete nested request/response shape, unsupported
  future discriminators, CRUD lifecycle, HTTP idempotency, Session history,
  archived filtering, and keyset pagination through the live Registry.
- **Layer A — pricing and budget contracts** (`test/model-prices-wire.spec.ts`,
  `test/budget-wire.spec.ts`). Live public/admin HTTP with isolated organizations:
  price resolution and authority, budget schemas, invalid writes, unpriced modes,
  and daily authoring scopes with visible Agent/Session references.
- **Deterministic spend** (`test/spend-control.spec.ts`,
  `test/spend-subagents.spec.ts`, `test/spend-runner.spec.ts`). Owns Registry,
  harness, worker/runner processes, four Postgres databases and an S3 bucket.
  The real Claude SDK consumes scripted Messages responses to test exact costs,
  tier composition, approvals, restart, ACK loss, and native subagent dispatch.
  The real self-hosted runner rejects a stateful request budget, then resumes
  under a stateless rule in the same Session. No paid provider or OS sandbox is
  involved; file assertions cover temporary directories and the real Files API.
- **Layer A.5 — ai-gateway MCP forwarding** (`test/ai-gateway-mcp.spec.ts`).
  Always-on. No `ANTHROPIC_API_KEY` required. Calls the real ai-gateway
  container's `/v1/mcp` path with a registry-minted session JWT and a fake MCP
  upstream, then asserts vault credential injection, JWT ACL failures, and
  Kafka audit emission.
- **Layer B — real-agent-loop** (`test/real-agent-loop.spec.ts`).
  Drives full agent loops with `claude_agent_sdk` (default, `ANTHROPIC_API_KEY`)
  or `codex_sdk` (`OPENAI_API_KEY`), selected by `ORCA_E2E_AGENT_HARNESS`. Includes sandbox
  MCP tools plus a real-agent remote MCP scenario that verifies harness MCP
  rewrite → harness remote MCP client → ai-gateway → fake upstream → Kafka
  audit. It also runs two workspace-scoped file-reading sessions concurrently
  and verifies archive-driven warm-runner teardown. The real-model custom-tool
  callback scenario is enabled for all SDKs by default; set
  `ORCA_E2E_REAL_CUSTOM_TOOL=0` to omit it during a targeted local run.
  The stack E2E workflow runs this layer with `SANDBOX_RUNTIME=opensandbox`:
  Claude SDK on Kafka, Pi SDK with `claude-sonnet-4-6` on PostgreSQL, and
  Codex SDK with `gpt-5.4` on Pulsar, each in separate and colocated modes.
  Pi uses `ANTHROPIC_API_KEY`; Codex uses `OPENAI_API_KEY`. Separate SDKs
  use direct provider egress; colocated Pi uses Gateway Messages and Codex
  uses Gateway Responses. Both use gateway MCP egress and exercise pinned
  Skill disclosure: read `SKILL.md`, then a referenced file whose unique
  marker is absent from the prompt and catalog. Multiagent and session-thread
  scenarios run only with `claude_agent_sdk`.
  No real agent runs against the local runtime.
- **Layer B — cron Trigger agent loop** (`test/trigger-agent-loop.spec.ts`).
  Creates a cron Trigger through `/v1/triggers`, lets the live Registry
  planner/dispatcher and Session lifecycle outbox perform the handoff, and
  waits for the real Harness/model response through the public Session event
  API. It shares the Layer B provider/runtime requirements and backend matrix.
- **Layer B — budget request smoke** (`test/guardrails-budget-agent.spec.ts`).
  Uses real priced output from the selected model to cross a tiny request-phase cap, then verifies
  the next public request is denied without new model work, assistant output,
  token growth or output files. Separate mode is always registered; colocated
  mode is added with `ORCA_E2E_SANDBOX_HARNESS=1`. The selected provider key
  is required. `ORCA_E2E_BUDGET_MODEL` defaults to `gpt-5.4` for Codex and
  `claude-sonnet-4-6` for Claude. When Codex has no effective price, this suite
  creates synthetic operator rates through the admin API for the E2E organization
  and deletes only that fixture after both topology cases. Existing prices stay
  unchanged; these fixture rates do not represent OpenAI retail pricing.
- **Layer B.1 — sandbox harness mode** (`test/sandbox-harness-agent.spec.ts`).
  Opt-in via `ORCA_E2E_SANDBOX_HARNESS=1`. Creates an agent with
  `metadata.harness=claude_code` or `codex_sdk`, with `mode=colocated`.
  The same single-agent cases verify text, usage and request budgets, progressive
  Skill disclosure, read-only File mounts, authenticated Git reads and writable
  checkouts, Memory writeback, custom callbacks, and immediate output capture.
  Custom callback coverage checks the persisted call ID and payload exactly, and
  requires the model reply to include a callback-only random marker and ticket
  status without requiring a particular Markdown layout. Local assertion regressions
  run with `pnpm -F @orca/e2e-tests exec vitest run test/custom-tool-result-assertions.spec.ts`
  and are included in `pnpm e2e:agent:sandbox`.
  Both assert a real sandbox handle with no Environment worker assignment.
  CI runs both through the same harness-server → sandbox-harness HTTP bridge,
  OpenSandbox image, 250m CPU / 1 GiB reservation, mounts and gVisor write policy.

## Invariants under test

`pnpm e2e:agent` reports individual scenarios and stops after the first failed
scenario exhausts its configured retries. This surfaces its assertion before
the CI job timeout; a successful run still executes every selected scenario.

What the suites exist to hold. A change that breaks one of these should fail
here, not in production.

**Isolation**

- A sandbox cannot reach another workspace's files, memory stores, or sessions.
  Equal file SHA-256 and equal memory-store path names stay workspace-local.
- Organization-admin and workspace data-plane credentials are mutually rejected
  — neither is accepted on the other's listener. (The registry also has a
  platform credential; no spec in this package mints or presents one.)
- Archiving a workspace invalidates only that workspace and tears down its warm
  runners; a sibling session stays readable.

**Secrets**

- A vault credential's bearer never appears in a public response or in the audit
  topic. (Harness logs and the transcript topic are _not_ asserted anywhere —
  no spec reads either.)
- A GitHub PAT never appears in a session response or in an SSE frame. Postgres
  rows, `.git/config`, the sandbox environment, and what rotation does to the
  runtime credential are _not_ inspected by any spec here.
- `/v1/mcp` rejects a missing JWT, a wrong audience, and a forbidden backend. An
  expired JWT is covered only against the registry's `/v1/git-creds`, not here.

**Durability and replay**

- A memory write is registered as a version whose `content_sha256` matches what
  the session wrote. (Attribution fields and multi-entry version chains are _not_
  asserted here; the specs compare path and digest only.)

**Wire compatibility**

- Response shapes, status codes, and the error envelope match the published
  contract on every core operation. (Layer A asserts these through a raw `fetch`
  wrapper, not through the SDK; the SDK object is constructed but not called.
  The SDK is exercised in `registry-service-ts` instead:
  `sdk-roundtrip.spec.ts` covers agent create + retrieve, and
  `sdk-conformance.spec.ts` drives its whole driven-operation list against a live
  listener, plain and with `orca-beta`.)
- Event streams stay structurally equivalent to Anthropic's, modulo ids and
  timestamps.

Coverage outside this package: the `ClaudeAgentSdkAdapter` SessionStore
conformance block is a hand-written suite in
`services/harness-server/test/integration/session-adapter.spec.ts`; the SDK
round-trip is
`services/registry-service-ts/test/integration/sdk-roundtrip.spec.ts`; sandbox
write confinement is
`services/harness-server/test/integration/local-sandbox-runtime.spec.ts`; the
split-chart Kind Helm deployment is `e2e-kind-helm`.

Not covered anywhere: no suite kills and restarts `harness-server` mid-turn, and
none forces a tiered-storage offload before re-reading. `crash-recovery.spec.ts`
appends the event before any dispatcher exists and then starts one, so it covers
cold-start catch-up rather than redelivery after a stop — there is no second
dispatcher, no rebalance, and no uncommitted offset. See
[`roadmap.md`](../../docs/managed-agents/roadmap.md).

## Prerequisites

The tests run against a real running stack. Bring it up first:

```bash
make stack-up        # boots Postgres + Kafka + RustFS + the three app services
make stack-status    # confirms each service /healthz is green
```

Default registry URL is `http://localhost:8080`. Override with `ORCA_BASE_URL`.

`pnpm e2e:spend` starts its own services and requires built workspace packages
(`pnpm -r build`), Postgres with `CREATEDB`, and S3 bucket create/list/delete and
object read/write access. It uses `ORCA_SPEND_POSTGRES_ADMIN_URL` (default
`postgres://orca:orca@127.0.0.1:5432/postgres`), `ORCA_SPEND_S3_ENDPOINT`
(default `http://127.0.0.1:9000`), and `ORCA_SPEND_S3_ACCESS_KEY_ID` /
`ORCA_SPEND_S3_SECRET_ACCESS_KEY` (both default `minioadmin`). The test process
can load the usual `.env`; owned processes inherit only explicit allowlisted
values and never its provider keys or the user's home credentials. Unique
resources are removed on success and failure. Logs, captured Messages/usage
exchanges, and a cleanup manifest remain under the system temporary directory
as `orca-spend-logs-*` (`/tmp` on CI).

The existing stack workflow runs deterministic spend only in the Postgres
control-plane matrix leg and uploads those diagnostics on failure. Paid request-budget
smoke runs on all three real-agent legs, in both modes. Job triggers and the E2E
aggregator are unchanged.

## Environment variables

| Var                            | Default                                        | Purpose                                                                                                                                                                                                                                                   |
| ------------------------------ | ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ORCA_BASE_URL`                | `http://localhost:8080`                        | Registry HTTP endpoint the SDK + raw-fetch path target. Override to point at a remote stack.                                                                                                                                                              |
| `ORCA_ADMIN_BASE_URL`          | `http://localhost:8082`                        | Registry organization-admin endpoint used by multi-workspace e2e coverage.                                                                                                                                                                                |
| `ORCA_HARNESS_BASE_URL`        | `http://localhost:9094`                        | Harness health/metrics endpoint used to verify archived workspaces tear down warm runners.                                                                                                                                                                |
| `DATABASE_URL`                 | `postgres://orca:orca@localhost:5432/registry` | Direct Postgres connection used by `seedWorkspaceApiKey()` to provision the e2e workspace + api-key row. Override if your dev Postgres uses non-default creds, port, or DB name.                                                                          |
| `KAFKA_BROKERS`                | `localhost:9092`                               | Kafka brokers used by Layer A.5 to verify the ai-gateway audit event.                                                                                                                                                                                     |
| `AI_GATEWAY_URL`               | `http://localhost:8090`                        | ai-gateway base URL used by Layer A.5 to build the MCP endpoint.                                                                                                                                                                                          |
| `AI_GATEWAY_ADMIN_PORT`        | `9099`                                         | ai-gateway admin port used by Layer A.5's `/healthz` preflight.                                                                                                                                                                                           |
| `AI_GATEWAY_E2E_UPSTREAM_HOST` | `host.docker.internal`                         | Hostname written into Registry MCP destinations for Layer A.5's fake upstream. Set `localhost` when running ai-gateway natively instead of in Docker.                                                                                                     |
| `ORCA_API_KEY`                 | _(unset)_                                      | Plaintext api-key passed to the legacy `buildClient()` helper. Layer A specs do NOT need it — they call `seedWorkspaceApiKey()` + `buildClientFromConfig()` to get a fresh key per run. Set only if you write a spec that uses `buildClient()` directly.  |
| `ANTHROPIC_API_KEY`            | _(unset)_                                      | Required by Claude Layer B — the harness uses it to call Anthropic. Unused by Layer A.                                                                                                                                                                    |
| `OPENAI_API_KEY`               | _(unset)_                                      | Required by Codex SDK Layer B in both the test process and harness-server.                                                                                                                                                                                |
| `ORCA_E2E_AGENT_HARNESS`       | `claude_agent_sdk`                             | Selects `claude_agent_sdk` or `codex_sdk` for Layer B and its colocated B.1 counterpart. Codex uses `gpt-5.4` with low effort; both omit metadata.mode to verify the catalog default.                                                                     |
| `GITHUB_TOKEN`                 | _(unset)_                                      | Required by the Layer B.1 session-resource case. Used as a write-only session attachment token to clone this repository and verify in-sandbox `git ls-remote` through the scoped credential helper.                                                       |
| `ORCA_E2E_REAL_CUSTOM_TOOL`    | `1`                                            | Both SDKs run the real-agent custom tool callback in `pnpm e2e:agent`; set to `0` to omit it locally.                                                                                                                                                     |
| `ORCA_E2E_SANDBOX_HARNESS`     | _(unset)_                                      | Set to `1` to include the sandbox harness e2e in `pnpm e2e:agent`, or use `pnpm e2e:agent:sandbox` to run the sandbox specs and budget smoke. The stack must use OpenSandbox/E2B; `in-memory` and local runtimes do not expose the harness HTTP endpoint. |

## Run

```bash
# Layer A only (fast feedback, no provider key required):
pnpm e2e:wire

# Deterministic spend (built packages + Postgres + S3, no provider key):
pnpm e2e:spend

# Layer A.5 only (ai-gateway data-plane + audit, no Anthropic key required):
pnpm e2e:gateway

# Claude Layer B (needs ANTHROPIC_API_KEY in the stack):
pnpm e2e:agent

# Codex SDK Layer B (needs OPENAI_API_KEY in the stack; CI uses Pulsar/OpenSandbox):
ORCA_E2E_AGENT_HARNESS=codex_sdk pnpm e2e:agent

# Targeted Layer B run without the custom tool callback:
ORCA_E2E_REAL_CUSTOM_TOOL=0 pnpm e2e:agent

# Claude colocated specs + budget smoke (needs ANTHROPIC_API_KEY and OpenSandbox/E2B):
pnpm e2e:agent:sandbox

# Codex colocated specs + budget smoke (needs OPENAI_API_KEY and the shared sandbox-harness image):
ORCA_E2E_AGENT_HARNESS=codex_sdk pnpm e2e:agent:sandbox

# Layer B + Layer B.1 in one run:
ORCA_E2E_SANDBOX_HARNESS=1 pnpm e2e:agent

# All suites (includes paid cases):
pnpm -F @orca/e2e-tests test
```

When you are done:

```bash
make stack-down
```

## How auth works

The first call to `seedWorkspaceApiKey()` provisions a workspace + API key
directly in the registry's Postgres database (idempotent — re-running rotates
the key on the same workspace row). The api-key middleware
(`services/registry-service-ts/src/auth/api-key.ts`) accepts `x-api-key:
orca_…` headers; the SDK's `apiKey` config maps to that header.

Multi-workspace specs seed only the organization-admin credential, modeling
the output of the offline bootstrap command. They create workspaces and issue
workspace API keys through the live `:8082` Admin API; plaintext workspace
keys are never inserted directly by those scenarios.

## SDK and raw HTTP coverage

Layer A pins official Anthropic SDK 0.113.0 and constructs the client, but
every assertion goes through a raw `fetch` wrapper — exact response keys, error
envelopes and Orca extensions are easier to check on the wire. The SDK-driven
coverage lives in `registry-service-ts`: `sdk-roundtrip.spec.ts` and
`sdk-conformance.spec.ts`.
