# OIP-010: Guardrails, model pricing and spend limits

- *Author(s)*: @sijie
- *Status*: Released
- *Proposal time*: 2026-08-01
- *Components*: registry-service-ts, harness-server, session-runner, `packages/guardrails`,
  `packages/harness-catalog` (model pricing), Helm chart
- *Discussion*: None (predates the public repository)
- *Implementation*: `packages/guardrails/src/`; `packages/harness-catalog/src/` (`pricing.ts`,
  `catalog-wire.ts`, `capabilities.ts`); registry-service-ts `src/api/`, `src/domain/`, `src/pricing/`;
  harness-server `src/harness/`, `src/runner/dispatcher.ts`; session-runner `src/guardrails.ts`;
  migrations `0050` and `0051`
- *Released in*: v0.5.0

## TL;DR

A tool's `permission_policy` answers one static question on the agent author's authority, so spend
caps, argument checks and organization-wide rules had nowhere to live. Guardrails make that policy
the seed of one fold over `allow < ask < deny`: rules from four authority tiers can only tighten it,
may keep durable counters, and include token and USD budgets. Registry prices every model call from
a seed, upstream and operator price catalog, so budgets meter exact spend and treat an unknown price
as unknown, never as free. Agent authors, administrators and operators who pay for usage are affected.

## Background

Agent tools carry the Anthropic-compatible `permission_policy` (`always_allow`, `always_ask`,
`always_deny`); `always_ask` uses the managed-agents tool-confirmation round trip. Orca-only APIs live
in named groups under `/apis/<group>/<version>` ([OIP-009](OIP-009-core-and-extension-api-groups.md)).
Before each turn harness-server asks Registry to *prepare* the session's execution, which now carries
the composed guardrails and their restored state. Sessions run `separate`, `colocated` or on the
self-hosted runner ([OIP-002](OIP-002-agent-harnesses-and-execution-modes.md),
[OIP-011](OIP-011-self-hosted-session-runner.md)). The owning documents are
[`guardrails.md`](../docs/managed-agents/guardrails.md), [`pricing.md`](../docs/managed-agents/pricing.md),
[`libraries/guardrails.md`](../docs/managed-agents/libraries/guardrails.md) and
[`harness-modes.md`](../docs/managed-agents/harness-modes.md).

## Motivation

The permission policy is stateless, keyed on tool name, and written by whoever writes the agent.
Three kinds of control had no home: **accumulation** ("stop at $25", "at most 200 tool calls"),
**argument inspection** ("ask before any `rm -rf`", "never push to `main`"), and **authority above
the agent author** — a rule a workspace or organization imposes and an agent cannot relax, which is
the difference between configuration and governance. Spend adds a fourth. Tokens become money only
through per-model prices, and prices move: vendors reprice, organizations negotiate rates,
self-hosted deployments run models no catalog lists, and an air-gapped deployment fetches nothing.
A cap that measures an unpriced model at $0 is worse than no cap, because it looks enforced.

Each gap could have become its own check in each provider, with its own verdict vocabulary, its own
failure behavior, and rules that hold in one topology and not another. Instead every action goes
through one decision computed by one library, which Registry uses to validate a rule when it is
written and every runtime uses to enforce it.

## Goals

### In scope

- One decision per action, seeded by the permission policy, over four authority tiers — including an
  organization tier a workspace can read but not change; composition can only tighten.
- Stateful rules a restart cannot reset; 22 authorable builtins and CEL expressions, validated at
  write time; token and USD budgets per session, per principal per UTC day, per subagent identity.
- A price catalog that works offline, honours per-organization overrides and never reads unknown as
  zero, metering each model call's final usage exactly.
- No silently inert rule (each is enforced, warned about or refused); no `/v1` change beyond
  additive fields.

### Out of scope

- Model-judged guardrails, risk scoring, rules that rewrite sandbox configuration
  ([`guardrails.md`](../docs/managed-agents/guardrails.md#not-in-scope)), approval at any phase but
  `tool_call`, invoicing, and USD on public session objects.
- Listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#designed-not-built): the `llm_request`
  interception point, request-phase budget approval, guardrail `tool_call`, `tool_result` and durable
  state on `session-runner`, deterministic spend on Kafka and Pulsar, the AI gateway guardrail rollout.

## Design

### High-level design

```
 write time (Registry)                     run time (harness-server, session-runner)
 compile + validate rule ──► guardrails    prepare execution: compose tiers, restore state
 organization prices ──────► model_prices        │
                                                 ▼
                                           evaluateGuardrails() at every wired phase
                                           state deltas ──► Registry, acknowledged before release
                                           final usage  ──► Registry prices it, returns totals
```

`@orca/guardrails` is pure — no database, network or agent SDK; its only dependency is
`@bufbuild/cel` — so the rule Registry accepts is evaluated by the code that enforces it. Registry is
the control plane: CRUD, compilation, tier composition, durable state, prices and the pricing of
usage. Runtimes enforce where the action happens and never hold price rows.

### Detailed design

**The decision.** `lattice.ts` orders `allow < ask < deny` and composes by maximum; `allow` is the
identity, so a rule with no opinion abstains. The permission policy maps onto the lattice
(`always_allow`→`allow`, `always_ask`→`ask`, `always_deny` or disabled→`deny`) and seeds the fold,
`verdict = fold(guardrails, seed = permissionPolicy(tool), max)`. In `engine.ts` a `deny` seed
returns before any rule runs, so nothing is read or advanced for a call that cannot execute, and
`ask` on a phase without an approval exchange resolves to `deny`. `require_approval_for_tools` and
`block_tools` are the predicates a permission policy expresses; at workspace or organization scope
they become rules the agent author cannot override. The separate Claude harness folds guardrails from
`allow` and then takes the maximum with the seed, so a client-executed tool can tell a policy `ask`
from a guardrail `ask` (`harness/claude/index.ts`).

**Tiers and composition.**

| Tier | Authority | Declared by |
| --- | --- | --- |
| session | end user | `guardrail_ids` inside `agent_with_overrides` at session create |
| agent | agent author | `guardrail_ids` on the coordinator Agent and on each subagent's Agent |
| workspace | workspace admin | `scope: "workspace"` — the default when a create omits `scope` |
| organization | organization admin | `scope: "organization"`, written only on the admin listener |

`scope: "explicit"` rules apply only where referenced. Registry composes the tiers when it prepares
a session (`guardrail-composition.ts`): session, coordinator, each subagent's own rules (bound to
that subagent identity), workspace, organization. A rule reached twice is composed once; a stored
rule that no longer compiles fails preparation. The engine runs the stateless partition, then the
stateful one, each in tier order; a deny short-circuits, so later counters do not advance, and order
changes which reason surfaces first, never the verdict. A coordinator's rules apply to its
subagents' actions, so delegation cannot launder work past the invoked agent's rules; a subagent's
own rules apply only to events attributed to that identity — the persistent Agent ID in the managed
Claude harness.

**Rules.** A rule is a parameterized builtin or a CEL expression.

- `catalog.ts` holds 23 builtins (22 authorable; `tool_permission_policy` is the internal seed) with
  phases, state scope, verdicts, allowed scopes and a closed JSON Schema, served verbatim as
  `guardrailtypes`. `compile.ts` rejects unknown parameters, non-finite numbers, `max_cost_usd: 0`,
  a budget with neither cap nor threshold, and `user_daily_cost_budget` at `explicit` scope.
- Expressions read one root, `event` (`phase`, `tool`, `result`, `session`, `usage`, `model`,
  `state`), with `on_false: ask | deny`. `cel.ts` compiles them at write time within fixed bounds
  (4,096 characters, parse depth 128, no nested comprehensions) and marks them stateful when they
  read `event.state`; the evaluator cannot be preempted, so its 50 ms budget detects overruns.
- `resolveGuardrailAuthoring` (`contracts/guardrails.contract.ts`, shared by both listeners) also
  rejects a phase outside the builtin's catalog, an explicitly requested phase no runtime fires, and
  `on_false: ask` where the phase cannot ask. A catalog default is stored verbatim even when it names
  an unfired phase, so `deny_pii_in_llm_request` keeps declaring `llm_request`.

**Phases and failure modes** (`types.ts`). `ask` resolves only at `tool_call`, through the
tool-confirmation round trip; elsewhere it resolves to `deny`.

| Phase | Fired by a runtime | Evaluator error |
| --- | --- | --- |
| `request`, `tool_call` | yes | deny (fail closed) |
| `tool_result` | yes | abstain (fail open) |
| `llm_request` | no — listed on the roadmap | deny (fail closed) |
| `response`, `llm_response` | no — authoring rejects them | abstain (fail open) |

Gating phases fail closed because an unevaluable rule is not a rule that passed; observational
phases fail open because suppressing output over an internal error costs more than it protects. The
engine returns each error on `decision.errors` with the `failedClosed` flag it was handled under.

**State.** Rules return deltas (`set`, `increment`, `delete`, `append`); the engine never writes.
`turn` state lives in memory and resets each turn; `session` state is `guardrail_state`, one row per
key; `subject_window` state is `guardrail_counters`, per principal and UTC date. The engine prefixes
a rule's own keys with `g:<guardrail id>[@<subagent id>]:`, so two caps hold two counters and one
rule's approval cannot satisfy another's; keys the runtime writes stay bare and shared
(`SHARED_USAGE_KEYS` in `state.ts`: `session_cost_usd`, `total_tokens`, `daily_cost_usd`,
`subagent_cost_<id>` and the unpriced markers).

| Outcome | Deltas persisted |
| --- | --- |
| allow | all |
| ask | none until approved; approval applies the withheld deltas, so a refusal asks again |
| deny at `request` or `tool_call` | none — the action did not happen |
| deny at `tool_result` | those of rules that allowed before it — the tool already ran |
| read-only evaluation | none; intended deltas are still returned |

harness-server writes session deltas through Registry on one FIFO lane with a 5 s deadline and
releases the guarded action only after the acknowledgment (`dispatcher.ts`); a timeout denies and is
not retried, because an additive delta is not idempotent on its own. Registry applies each delta as
an additive upsert, never a read-modify-write, and restores session state at every preparation; a
failed restore fails preparation, so forcing a restart cannot reset a cap. The harness never writes
`subject_window` counters; Registry derives them from priced usage. The principal is stamped where
the credential was validated: accepting a user event records `user:<id>`, else `api-key:<id>`, else
`principal:<name>` on its event-index row (`auth/guardrail-subject.ts`); a cron trigger records its
creator's subject.

**Budgets** (`builtins/cost.ts`) fire at `request` and `tool_call`: `token_budget` (needs no
prices), `cost_budget` (session USD), `user_daily_cost_budget` (per principal per UTC day; workspace
or organization scope only) and `subagent_cost_budget` (per acting subagent identity). Order matters:

1. **Unpriced first.** Consumed tokens with an absent, non-finite or negative cost, or a sticky
   unpriced marker from Registry, are unmeasured. `on_unpriced: deny` denies, `allow` continues, and
   the default `ask` asks at `tool_call`; at `request` it allows once, records a pending marker, and
   denies the next unapproved request.
2. **Hard cap.** At or above `max_cost_usd` it denies — unless `expensive_models` is set and the
   model matches none of its substrings: the cap is then a downgrade gate. A missing model id is blocked.
3. **Soft thresholds** (`ask_thresholds_usd`) ask once each, at `tool_call` only; approval records a
   high-water mark.

`cost_budget` and `token_budget` read the larger of the event's figure and the accumulated counter,
so a malformed `0` after a breach cannot buy another call. Session update accepts `agent.model`
while the session is idle — the escape route the downgrade gate points to.

**Enforcement by runtime.**

| Runtime | Enforced | Otherwise |
| --- | --- | --- |
| Claude `separate` (`harness/claude/index.ts`) | `request` before the model sees the turn; `tool_call` in the SDK permission callback, which native `Agent` dispatch also passes through (deny = tool error, ask = tool confirmation); `tool_result` (deny = suppression notice) | partly unfired rule: `session.warning`; wholly unfired: start refused |
| Claude `colocated` (`harness/in-sandbox/index.ts`) | tool exposure by the stateless pass — `ask` or `deny` removes the tool, and a rule that reads tool input meets an unavailable-input sentinel and removes it too; `request` rules, budgets included, on the host each turn | stateful rule without `request`: warning, inert; any `llm_request` rule: start refused |
| Codex SDK, Pi SDK (`harness/codex-sdk/index.ts`) | `separate`: stateless `request`, `tool_call`, `tool_result` and stateful `request`; `colocated`: `request` | soft thresholds, subagent rules, other phases: rejected before execution (also `validateHarnessGuardrails`) |
| `session-runner` (`src/guardrails.ts`) | stateless top-level `request`; `block_skills` on managed resources; Registry evaluates managed Codex and Pi request policies (`domain/codex-runner-accounting.ts`) | anything else: snapshot refused |

`block_skills` is enforced where Skills are materialized, in every topology (`skillIsBlocked`,
`managedSkillIsBlocked`): a blocked Skill is never staged. Edits apply from the next turn: the
dispatcher re-prepares before each `user.message`, and guardrails are part of the runtime
configuration key, so a changed rule restarts the runner; an in-flight turn finishes unchanged.

**Pricing.** A model identity is `{provider, model_id}`, the provider defaulting to `anthropic`.
Cost adds five buckets — input, output, cache read, 5-minute and 1-hour cache creation — with cached
tokens counted only in their own bucket (`pricing.ts`). Absent cache rates derive from the input rate
(read ×0.1, 5-minute write ×1.25, 1-hour write ×2); a published 5-minute rate derives the 1-hour
rate at ×1.6. An explicit zero cache rate is honoured; base rates must be finite, strictly positive,
and at most three decimal places per million tokens. Resolution tries the exact id, then a dated
snapshot (`-YYYYMMDD` stripped), then the family (last hyphen segment dropped). Specificity outranks
source: each rung is tried across all sources, and only within a rung does `operator > upstream >
seed` decide; candidates at the winning precedence that disagree on any rate resolve to unpriced.
Operator rows apply only to the organization that wrote them. Unknown is never zero: resolution
returns `null`, a zero-rate entry is dropped as unpriced, and the public routes 404 or omit it.

- **Seed**: `packages/harness-catalog/src/model-prices.seed.json` (with a provenance header), loaded
  on every Registry boot, so an offline deployment prices correctly; its model ids also populate the
  Claude SDK model allow-list (`harness-models.ts`).
- **Upstream**: with `PRICE_REFRESH_URL` set, `pricing/refresher.ts` fetches at start and every
  interval. A failed fetch or malformed payload changes nothing; an entry with a non-positive rate,
  or one over 10× from the current upstream (else seed) row, is skipped; the accepted set atomically
  replaces that provider's upstream rows.
- **Operator**: organization rows from the admin listener; deleting one falls back to upstream or seed.

**Spend accounting.** Runtimes report each model call's *final* usage — for Claude `separate`,
merged at `message_stop`, native child calls from the SDK SessionStore mirror, terminal-result usage
only as a fallback (`harness/claude/usage-tracker.ts`) — with its model, provider, turn event id and
usage event id. Registry prices the delta then, in integer nano-USD, and in one transaction adds it
to the session and thread rows (`usage_cost_nano_usd`, NULL until the first priced delta;
`usage_has_unpriced`), the shared guardrail keys and the turn subject's `daily_cost_usd` (or sets
`daily_cost_unpriced`), then acknowledges with the totals (`domain/usage-accounting.ts`).
`session_usage_events` makes a replay a no-op, so delivery is at-least-once and accounting
exactly-once; recorded cost never changes, and a mid-session model change is priced per portion.
Budgets evaluate against acknowledged totals. An unacknowledged report blocks the tool its call
proposed and the turn's completion; exhausted, it fails the turn with `guardrail_usage_unavailable`.
Each turn refreshes the principal's daily counters before `request` evaluation. Each session has one
usage writer: harness-server by default, the AI gateway for non-Codex `colocated` sessions when
`AI_GATEWAY_REGISTRY_USAGE_ENABLED=true`, and Registry itself for self-hosted `colocated` Codex SDK
and Pi SDK sessions (`domain/usage-authority.ts`).

## Changes by component

- **registry-service-ts**: both groups; organization routes on the admin listener; reference
  validation; composition and state restore; internal usage, state, subject-window and bundle
  routes; the price store, seed load, refresher and pricing of usage; subject stamping.
- **harness-server** and **session-runner**: the enforcement above; environment-worker and
  observability-exporter are unchanged.
- **Libraries**: new `@orca/guardrails`; `@orca/harness-catalog` gains pricing, the upstream parser,
  the seed file and per-harness guardrail capabilities. **Helm chart**: the two gateway blocks below.

## Public-facing changes

### API

| Route (public listener, credential required) | Behavior |
| --- | --- |
| `GET`, `POST /apis/policy.runorca.ai/v1/guardrails` | list (`limit` ≤ 100, `page`, `include_archived`); create answers 201 |
| `GET`, `POST …/guardrails/{id}`, `POST …/{id}/archive`, `DELETE …/{id}` | an organization rule is readable and every mutation of one is 403; delete is 409 while an agent names the rule |
| `GET /apis/policy.runorca.ai/v1/guardrailtypes` | the public builtin catalog, verbatim |
| `GET /apis/pricing.runorca.ai/v1/modelprices[/{model_id}]` | effective price (`type: model_price`), `?provider=`; unpriced is 404; no write routes exist |
| `GET /apis/{policy,pricing}.runorca.ai/v1` | `APIResourceList`, from the table behind `GET /apis` |

Group routes answer with the Claude error envelope. The admin listener serves
`/v1/organizations/guardrails` (create, list, get, `PATCH`, delete; `scope` is `organization`) and
`/v1/organizations/modelprices` (create, list across sources with `?source=`, get, `PATCH`, delete
of the operator row; `type: model_price_entry`) to `org:admin` or a matching `guardrails:*` or
`model_prices:*` scope. Core `/v1` shapes gain additive fields only: `guardrail_ids` (≤ 64, checked
against visible, non-archived rules) on Agent create and update and in `agent_with_overrides`, and
`agent.model` on session update while idle. Agent responses carry `guardrail_ids` under `orca-beta`;
`permission_policy` is unchanged. The conformance register holds one scoped decision per group
(`extension-policy-group`, `extension-pricing-group`) ([OIP-005](OIP-005-conformance-decision-register.md)).
Group names follow one rule: engine-defined groups use `runorca.ai`; a third party uses its own domain.

### Events and streaming

- `session.warning`, an Orca-only kind: `{warning: {type: "guardrail_not_enforced", message},
  guardrail_id, guardrail_name}` when a runtime cannot enforce part of a configured rule.
- A `request` denial appends `session.error` (`error.type: "policy_denied"`, `reasons`) and
  `session.status_idle` with `end_turn`; the model never sees the message.
- An exhausted usage acknowledgment appends `session.error` (`guardrail_usage_unavailable`) and
  `session.status_idle` with `retries_exhausted`.
- `ask` reuses the tool-confirmation events: no new client protocol or event type.

### Wire protocols

- Prepared executions carry `guardrails[]` (`id`, `name`, `tier`, `phases`, `rule`, `stateful`,
  `state_scope`, `subagent_id`) and `guardrail_state`, additive with no `schema_version` bump;
  runner snapshots add `request_guardrails_owner` for Registry-evaluated Codex and Pi requests.
- Internal routes: `POST …/usage` and `POST …/guardrail-state` (Harness or AI gateway) and
  `POST …/guardrail-subject-window` (Harness), scoped by workspace and session in the path; and
  `GET /internal/v1/guardrails/effective` (AI gateway), a session bundle for the scope the gateway
  projects from a verified session JWT, checked against the runtime revision and valid for an hour.
  Gateway-audience session JWTs carry `agent_id`, `guardrail_ids` and `runtime_config_revision`.

### Storage

Migration `0050_guardrails_and_model_prices` adds `guardrails` (a check ties `organization` scope to
a null `workspace_id`), `guardrail_state`, `guardrail_counters`, `model_prices` (key `provider,
model_id, source, organization_id`, with `''` as the global scope), `agents.guardrail_ids`, and cost
columns on `sessions` and `session_threads`. Migration `0051_guardrail_subject_and_usage_events` adds
`session_usage_events` and `guardrail_subject` on `session_events_index` and `agent_triggers`
(backfilled `trigger:<id>`). Soft deletion (`deleted_at`) came with `0058`.

### Configuration

| Setting | Default | Read by | Effect |
| --- | --- | --- | --- |
| `PRICE_REFRESH_URL` | unset | Registry | upstream catalog; unset disables the refresher |
| `PRICE_REFRESH_INTERVAL_MS` | `21600000`, minimum `60000` | Registry | refresh cadence |
| `PRICE_REFRESH_PROVIDER` | `anthropic` | Registry | provider the feed prices; scopes the replace |
| `AI_GATEWAY_REGISTRY_USAGE_ENABLED` | `false` | Registry | gateway writes usage for non-Codex `colocated` sessions |
| `aiGateway.registryUsage.*` | `enabled: false` | chart | gateway usage sink; sets the Registry switch above |
| `aiGateway.registryGuardrails.*` | `enabled: false`; `on*` failure settings `deny`; `cacheTtlSecs: 300` | chart | gateway Registry guardrail source; needs `agent_id` in `aiGateway.scopeDims` |

The chart has no dedicated pricing values; `PRICE_REFRESH_*` go through `registry.extraEnv`.

### Metrics, logs and traces

`registry_service_model_price_refresh_last_success_timestamp_seconds` (0 until the first success)
and `registry_service_model_price_refresh_total{result}` (`succeeded`, `failed`, `skipped`).
Organization guardrail and price writes commit an audit record (`guardrail.*`, `model_price.*`) in
the same transaction.

## Compatibility

### Upgrade

The migrations are additive: existing agents get `guardrail_ids: []`, existing triggers a stable
`trigger:<id>` subject (recreating one attributes it to its creator), and seed prices load at every
boot. With no rules the fold reduces to the permission policy — `tool-permission-baseline.spec.ts`
was written against the permission code before the engine took over and passes unchanged after.

### Rollback

Migrations are forward-only. Code without this change ignores the new tables and columns: a rollback
keeps rules, counters and prices but enforces none of them; rolling forward resumes from them.

### Version skew

Upgrade Registry and harness-server together. The prepared-execution fields are additive, so a
harness-server that predates them ignores the rules and a skewed window is unenforced. The session
runner refuses a snapshot it cannot enforce; the gateway's Registry source is opt-in.

## Security considerations

- **Write paths stay off the public listener.** Organization rules and all prices are written only
  on the admin listener, each write committing its audit record; the public pricing group has no
  write routes. The organization comes from the workspace row, never the request; the
  workspace-selector guard covers `/apis/` as well as `/v1/` (`server.ts`); discovery is not on the
  unauthenticated allowlist (`auth/auth.ts`).
- **Zero prices are not an attack surface.** A workspace able to set prices could zero them and walk
  through every budget, so base rates are strictly positive in the API, the resolver and the upstream
  parser, and an override prices only its own organization. CEL evaluates supplied data only.
- **Fail closed where it gates, fail open where it observes.** Gating-phase errors, an unregistered
  builtin, an unreadable model under a gated cap, a timed-out state write, a failed restore, a rule
  that no longer compiles and an unacknowledged usage report deny or refuse; unpriced usage asks, then
  denies. `tool_result` errors abstain; a failed upstream refresh keeps the last rows.
- **Daily-budget subjects.** The usage route resolves the principal from Registry's own accepted-event
  record, and each internal route admits only the workloads it names (`auth/internal-auth.ts`). The
  guardrail-state route accepts `subject` and `window` for `subject_window` deltas from its caller;
  harness-server never sends them, so there the property rests on the caller.

## Testing

- **Libraries**: `packages/guardrails/test/unit/` (lattice laws, composition, write timing, failure
  modes, catalog invariants, CEL bounds, every builtin); `packages/harness-catalog/test/` (resolution,
  derivations, partition-independent nano-USD, parser skips and delta bound, a 180-day seed-age test).
- **Registry**: unit suites for composition, subject stamping, the state and usage routes, prices,
  discovery, route-contract parity and the auth allowlist; Postgres integration suites `guardrails`,
  `model-prices`, `internal-usage-pricing` and `internal-guardrail-state`.
- **Runtimes**: `tool-permission-baseline.spec.ts`; `guardrail-topology-matrix.spec.ts` (one rule
  through `separate` and `colocated`, asserting the difference); `claude-usage-tracker.spec.ts`,
  `claude-sdk-usage-wire.spec.ts`; the runner's `guardrails.spec.ts`.
- **End to end**: `guardrails-wire`, `budget-wire`, `model-prices-wire` in `pnpm e2e:wire`;
  `spend-control`, `spend-subagents`, `spend-runner` in `pnpm e2e:spend` (production services, the
  Claude SDK against scripted Messages responses, on the Postgres leg of `e2e-stack.yml`); the
  provider-backed `guardrails-agent` and `guardrails-budget-agent`.

## Alternatives

- **A second permission check beside the policy, or the policy moved into the library.** Two
  consultations need a precedence rule; the policy stays where its runtime inputs live — remote
  toolsets, subagent maps, client execution — and seeds the fold instead.
- **Unauthenticated discovery** (an earlier draft), so a client could find groups before holding a
  key. Rejected: the answer describes the deployment, so it needs a credential.
- **Hand-written conformance rows.** The matrix is generated; each group enters as one scoped
  decision, not a shared `/apis/*` glob that would cover any group a distribution mounts later.
- **Price writes answered on the public listener with a 403.** That registered routes no contract
  declared, which route-contract parity caught; a `/apis/:group/:version` wildcard would likewise
  have answered 200 for invented groups.
- **Guardrail state as a JSON blob on the session.** One row per key makes an increment an additive
  upsert, so a respawned runner's late flush cannot lose an update.
- **Intermediate library revisions** dropped family price matching and let `llm_request` fail open;
  the shipped code keeps family matching behind the ambiguity guard and fails `llm_request` closed.
- **Deny keeping earlier rules' writes** (the first library), **or withholding all writes on any
  deny.** Chosen: withhold where the deny blocks the action, keep where the tool already ran.
- **Refusing a `separate` session with any `llm_request` rule** would trade the PII screen's working
  `request` leg for no screen, so `separate` warns unless no phase is enforceable.
- **`ask` at `request`** needs a new client protocol; request-phase approval is on the roadmap.
- **Pricing in the harness, or at read time.** Registry prices each delta when reported, so price
  rows never cross the internal seam, overrides apply everywhere, and recorded cost is immutable.
- **Advisory runner state on the turn's response stream** cannot be acknowledged before an action is
  released, so the runner refuses stateful rules; durable runner state is on the roadmap.

## Status notes

Corrections made after the first implementation landed, each verified in current code:

- `llm_request` was documented as intercepted in `separate`; neither topology had the point. Now
  `separate` warns or refuses, `colocated` refuses, and authoring rejects explicit unfired phases.
- `daily_cost_unpriced` was read but never written, so the daily cap failed open after unmeasured
  spend; Registry now sets it to `1` with the unpriced usage, and `flagged()` reads either form.
- `block_skills` keyed on a `Skill` tool no runtime exposed; it is enforced at materialization.
- Agent `guardrail_ids` were never persisted (a rename dropped them from the version snapshot);
  create and update now store them (`agents.routes.ts`).
- The runner first logged unwired `tool_call` and `tool_result` rules; it now refuses the snapshot.
- Budgets once read usage before the SDK's final output counts, and native `Agent` dispatch bypassed
  the permission callback; per-call final usage with acknowledgment gating replaced both.

Where the shipped code diverges from the original design (the owning documents,
[`guardrails.md`](../docs/managed-agents/guardrails.md), [`pricing.md`](../docs/managed-agents/pricing.md)
and [`data-model.md`](../docs/managed-agents/data-model.md), describe the code):

- **References.** Archive checks no references; delete refuses only while an agent's current
  `guardrail_ids` names the rule; composition skips an archived or missing id silently.
- **`orca-beta`** gates only the Agent response projection; request-side fields need no opt-in.
- **Daily counters** are keyed by `workspace_id, subject, window, key`, so an organization-scope
  `user_daily_cost_budget` caps a principal per workspace. State deltas carry no guardrail id, and the
  state route takes no idempotency key.
- **Edit propagation** is the per-turn re-preparation above; there is no guardrail edit outbox.
- **Daily-budget bookkeeping.** Its approval and pending-unpriced markers are `subject_window`
  deltas, which the harness writer refuses, so those paths fail the action; its hard cap and
  `on_unpriced: deny | allow` work. `colocated` emits no soft-threshold warnings.
- **Evaluation errors.** No runtime logs or emits `decision.errors`; a failing `tool_result` rule is silent.
- **Upstream refresh.** A skipped entry's prior upstream row is withdrawn by the replace; the 10×
  factor has no setting; failures and skips are not logged (`main.ts` wires no failure callback),
  and skips are not counted; no scheduled seed-refresh job exists.
- **Price shape.** One `cache_write_per_million_tokens` (the 5-minute rate) is stored; the 1-hour
  rate is always derived. Admin `GET` and `PATCH` answer 404, not 400, for an unreadable `provider`.
- **Admin scopes.** `registry:create-admin-key` cannot mint `guardrails:*` or `model_prices:*`
  (`auth/admin-api-key.ts`), so these routes take `org:admin` in practice.
