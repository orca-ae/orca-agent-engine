# Guardrails

A **Guardrail** is a declarative rule that evaluates an agent action in context
and returns one of three verdicts: **allow**, **ask**, or **deny**. Guardrails
compose across four scopes, may carry state across a session, and can only ever
tighten what an agent is permitted to do.

This document defines the model, the `policy.runorca.ai/v1` API group that
manages them, and where they are enforced. Pricing for the cost guardrails is
defined separately in [`pricing.md`](./pricing.md); the evaluation library is
described in [`libraries/guardrails.md`](./libraries/guardrails.md).

## Problem

An agent's `permission_policy` answers one question, statically: _may this tool
be called at all?_ It is declared per tool on the agent, it is stateless, and it
is authored by whoever writes the agent.

That leaves three classes of control unexpressible.

- **Nothing accumulates.** "Stop once this session has spent $25" or "cap this
  session at 200 tool calls" cannot be written, because a permission policy sees
  one tool name and no history.
- **Nothing inspects arguments.** "Ask before any `rm -rf`" and "never push to
  `main`" are decisions about a tool's _input_, not its name.
- **Nothing outranks the agent author.** A permission policy lives on the agent,
  so the person who writes the agent can always relax it. There is no way for a
  workspace or an organization to impose a rule the agent cannot override — which
  is the difference between configuration and governance.

## The model

### Permission policies are guardrails

A `permission_policy` is a guardrail that happens to be stateless, agent-scoped,
and keyed on tool name. The correspondence is exact:

| `permission_policy`                     | Equivalent guardrail                               |
| --------------------------------------- | -------------------------------------------------- |
| `always_allow`                          | no guardrail at all — allow is the default verdict |
| `always_ask` on tool X                  | `require_approval_for_tools {tools: [X]}`          |
| `always_deny` on X, or `enabled: false` | `block_tools {tools: [X]}`                         |

Guardrails generalize it along three axes: **scope** (agent-only becomes four
tiers), **state** (none becomes counters and budgets), and **predicate** (tool
name becomes tool input, result, usage, model, or an expression).

There is therefore **one decision per action**, produced by one fold over the
verdict lattice `allow < ask < deny`, seeded by the permission policy:

```
verdict = fold(guardrails, seed = permissionPolicy(tool), max)
```

Taking the maximum at every step is what makes composition **monotonically
restrictive**: a guardrail can tighten a verdict, never loosen one. A workspace
rule cannot be relaxed by an agent, and an agent's rule cannot be relaxed by a
session.

`permission_policy` keeps its exact wire shape on `agents.tools[]` — this
introduces no change to it. The two are one mechanism underneath, with the
permission policy seeding the fold.

### Verdicts

| Verdict | Effect                                                                              |
| ------- | ----------------------------------------------------------------------------------- |
| `allow` | the action proceeds                                                                 |
| `ask`   | the action pauses for client approval; approved becomes allow, refused becomes deny |
| `deny`  | the action is blocked and the agent is told why                                     |

A guardrail that has no opinion abstains, which is equivalent to `allow`.

### Phases

A guardrail declares the phases it fires on.

| Phase          | Fires                                   | Failure mode | Verdicts         |
| -------------- | --------------------------------------- | ------------ | ---------------- |
| `request`      | before a user message reaches the model | closed       | allow, deny      |
| `tool_call`    | before a tool executes                  | closed       | allow, ask, deny |
| `tool_result`  | after a tool returns                    | open         | allow, deny      |
| `response`     | after the model produces a response     | open         | allow, deny      |
| `llm_request`  | before a model request                  | closed       | allow, deny      |
| `llm_response` | after a model request returns           | open         | allow, deny      |

**Failure mode** is what happens when evaluation itself errors. The three
phases that gate an action before it happens — `request`, `tool_call`, and
`llm_request` — fail **closed**, so an evaluation error denies; for
`llm_request`, the bytes have not reached the model yet.
Every phase that observes after the fact fails **open**, because suppressing
output on an internal error is worse than the risk it mitigates. Either way the
engine records the error on the decision's `errors`, with the `failedClosed`
flag it was handled under; none of the runtimes reads that field or emits it.

**`ask` is available at `tool_call` only.** That is a deliberate boundary, not a
limitation of the model: `tool_call` reuses the tool-confirmation round trip that
already exists between server and client, so guardrails add no new client
protocol. `tool_result` cannot meaningfully ask — the tool has already run — and
the other phases have no approval anchor. Guardrails that need to ask at
`request` are out of scope until that protocol exists.

`llm_request` **evaluation** receives the full serialized outbound request —
system prompt, messages, and tool results included — because a screen over a
preview is not a screen: `deny_pii_in_llm_request` must scan everything that
will reach the model, including PII introduced mid-turn by a tool result. What
is **persisted** about an `llm_request` evaluation is metadata only — model,
message and tool counts, a system-prompt preview, and the last user message —
so the full prompt never enters the transcript or audit stream through this
path.

**No topology evaluates `llm_request` today.** The phase is modelled, and
`deny_pii_in_llm_request` declares it, but neither harness has the interception
point it needs, so a guardrail is never handed the outbound bytes. Both
topologies say so rather than degrading in silence:

- `in_sandbox` **refuses to start** a session configured with an `llm_request`
  guardrail. It cannot offer even a partial screen, so there is nothing to
  degrade to.
- `separate` emits a `guardrail_not_enforced` session warning naming the
  guardrail and the phases it does evaluate — for `deny_pii_in_llm_request`,
  the `request` leg still enforces, and dropping the session would trade a
  partial screen for none. A guardrail whose phases are _all_ unenforceable
  has nothing left to run, so `separate` refuses to start too.

Authoring is screened ahead of both: `POST /guardrails` rejects an explicitly
requested phase no enforcement point fires, so a rule cannot be written that is
inert from the moment it is stored. A builtin's catalog default is kept
verbatim — the catalog describes the rule, not this tree's wiring — so a
stored `deny_pii_in_llm_request` row keeps declaring `llm_request`. The design
for the interception point is in
[`roadmap.md`](./roadmap.md#designed-not-built).

### Stateless and stateful guardrails

Every guardrail type declares whether it is stateful. This is not bookkeeping —
it decides **where the guardrail can run**.

|                 | Stateless                                                        | Stateful                        |
| --------------- | ---------------------------------------------------------------- | ------------------------------- |
| Input           | the event                                                        | the event and accumulated state |
| Evaluation      | pure function, no I/O                                            | requires a state store          |
| Emits           | a verdict                                                        | a verdict and state updates     |
| Can run         | anywhere: tool-list construction, sandboxed enforcement, dry-run | only where state is reachable   |
| Short-circuited | no consequence                                                   | its counter does not advance    |

The engine evaluates in two passes: the stateless partition first — cheap, no
I/O, and able to short-circuit before any state is touched — then the stateful
partition. A caller that cannot reach state runs the stateless pass alone. That
is a typed capability rather than a silent gap, and it is what makes enforcement
possible at points that decide tool exposure up front.

For an expression guardrail, statefulness is **derived at authoring time** by
inspecting the compiled expression for state references. An expression that never
reads state is eligible for the stateless pass automatically.

## Scopes and composition

Four tiers, distinguished by who has authority over them.

| Tier         | Authority          | How it is declared                                          |
| ------------ | ------------------ | ----------------------------------------------------------- |
| session      | end user           | `guardrail_ids` in `agent_with_overrides` at session create |
| agent        | agent developer    | `guardrail_ids` on the Agent                                |
| workspace    | workspace admin    | `scope: "workspace"` on the Guardrail                       |
| organization | organization admin | `scope: "organization"`, admin listener only                |

The two reference tiers name guardrails by ID, validated when the referencing
agent or session is written. The two scoped tiers apply to every session in their
scope without being referenced at all.

Deletion checks references in the other direction, but only against agents:
**deleting a guardrail that an agent's current `guardrail_ids` still names is
rejected with 409**. Session-tier references are not checked, and archiving
checks none. When a runtime is prepared, a referenced guardrail that is archived
or deleted is skipped without an error — the same outcome as `enabled: false`,
the explicit dial for turning a rule off in place.

The reference-tier fields — `guardrail_ids` on the Agent and inside
`agent_with_overrides` at session create — are Orca-only wire extensions to
Claude-compatible core shapes, as is `model` on session update (the budget
escape hatch below). The request side accepts all three with or without the
`orca-beta` header; the header decides only whether an Agent response carries
`guardrail_ids`. The conformance register records them as decided divergences.
See [`orca-extensions.md`](./orca-extensions.md).

**The organization tier is what makes this governance.** It is the only tier a
workspace administrator cannot delete. Organization-scoped guardrails are
readable from the workspace API — an operator must be able to see what applies to
them — but every mutation from that listener is rejected with 403. Writes require
an organization credential on the admin listener.

Guardrails are evaluated **session → agent → workspace → organization**. A deny
short-circuits the rest. An ask accumulates and is never cleared by a later
allow. Because the fold is monotonic, order affects only which reason surfaces
first, never the verdict.

The two evaluation passes nest **outside** the tiers: pass one evaluates every
stateless guardrail in tier order, pass two every stateful guardrail in tier
order, and a deny in either pass short-circuits the remainder of the whole
evaluation. An organization-tier stateless deny therefore preempts a
session-tier stateful counter — whose non-advance is the advisory-counters rule
below, not a lost update.

One consequence is worth stating plainly: guardrails after a deny do not run, so
their counters do not advance. Counters are advisory under short-circuit.

## Multi-agent

Subagents run inside a single session rather than as separate sessions, and the
runtime knows which subagent is acting on every tool call. Guardrails resolve
through that same identity.

The tiers that apply to a subagent's tool call are:

```
session → coordinator agent → acting subagent's agent → workspace → organization
```

The coordinator's own guardrails apply to actions its subagents take. Without
that, an agent that blocks a tool would not stop a subagent it dispatched from
using it, and **delegation would launder work past the guardrails of the agent
the user actually invoked**. Composition remains monotonic, so including the
coordinator's tier can only tighten.

A guardrail resolved from a subagent's own agent record binds to that subagent:
it evaluates only for events the runtime attributes to that Agent identity. The
reverse does not hold — coordinator, session, workspace, and organization rules
carry no binding and apply to every actor, which is what keeps delegation from
laundering work past them.

Session-scoped state is shared across subagents automatically, because they share
a session: an approval for an unbound session-wide rule is not re-asked inside a
subagent. Subagent-bound rule state is namespaced by the acting subagent identity
as well as the guardrail's identity. The managed Claude harness uses the persistent
Agent ID within the Session, so repeated dispatches of the same Agent definition
share cost, threshold approvals, and unpriced-usage state. Different Agent
definitions have independent state, even when they reference the same guardrail.

## State

Stateful guardrails emit updates rather than writing directly:

```jsonc
{ "key": "tool_calls", "action": "increment", "value": 1 }
```

Actions are `set`, `increment`, `delete`, and `append`. Updates are applied in
order.

A rule reads and writes bare names, but what the store persists is namespaced by
the guardrail's identity — and, for a rule resolved from a subagent,
by the runtime-provided acting subagent identity. In the managed Claude harness
that identity is the persistent Agent ID, shared by its dispatches in the Session.
Independent rules in the same scope therefore cannot interfere:
two call caps hold two counters, and a `token_budget` approval cannot satisfy a
`cost_budget` threshold. The keys the runtime itself writes stay bare and are
visible to every rule; they are the shared usage vocabulary (`daily_cost_usd`,
`session_cost_usd`, `total_tokens`, `subagent_cost_<id>`) — measured facts many
rules legitimately read, distinct from any one rule's bookkeeping. Because the
runtime accumulates these independent of any verdict, a `cost_budget` reads the
larger of the event's reported cost and `session_cost_usd`: a single malformed
event reporting `$0` after a breach cannot fall below the spend already recorded
and buy a call past the cap. `token_budget` does the same with `total_tokens`.

### Scopes

| Scope            | Storage                                                                                                  | Lifetime                      |
| ---------------- | -------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `turn`           | in memory, never persisted                                                                               | cleared at each turn boundary |
| `session`        | `guardrail_state`                                                                                        | the session                   |
| `subject_window` | `guardrail_counters`, keyed by workspace, principal, and window (see [`data-model.md`](./data-model.md)) | the window                    |

`subject_window` exists for budgets that span sessions — a per-principal daily
cap. The subject is the authenticated user when one is present and the API key
otherwise, so a key-only deployment still gets a coherent cap rather than none.
The window is a UTC date string, zero-padded so lexicographic ordering drives
range queries.

The subject is **stamped by Registry, never supplied by the harness**. When
Registry accepts the user event that starts a turn, it persists the
authenticated principal on the accepted-event record — a server-authenticated
field written at the trust boundary where the credential was actually
validated. When the internal usage route prices a delta, Registry resolves the
turn's subject from its own record (the delta references the turn it belongs
to) before incrementing the `subject_window` counter. A session that receives
turns from different principals attributes each turn's spend to the principal
who sent it, and a compromised harness cannot shift spend onto another
principal's counter or dodge a cap by inventing a subject, because nothing it
sends carries one.

Cron triggers authenticate when Registry accepts the trigger definition rather
than when a scheduled fire runs. Registry stamps that creator subject on the
trigger and resolves it through the Registry-owned fire/event association, so
autonomous sessions keep the same trusted attribution without a credential in
the scheduler or harness. Triggers created before subject stamping was added
receive a stable per-trigger migration subject; recreating one replaces that
legacy isolation with creator-principal attribution.

`turn` scope is deliberately never persisted. A respawn mid-turn restarts the
turn, so there is nothing to recover.

### Write timing

Write timing is part of the contract, because the guardrails that ask depend on
it.

| Verdict | State writes                                                            |
| ------- | ----------------------------------------------------------------------- |
| allow   | applied                                                                 |
| deny    | at a gating phase, **all writes withheld** — the action does not happen |
| ask     | **all writes withheld** until the client approves                       |
| dry-run | nothing persisted; intended updates are still returned                  |

At the phases that gate an action — `request`, `tool_call` and `llm_request` —
state persists only when the action is allowed. Withholding on `deny` keeps a
counter an earlier guardrail advanced from standing for an action that was
blocked — a tool call the budget denies must not consume the tool-call cap.
Withholding on `ask` is what makes a refused approval re-ask rather than silently
arm itself: the threshold that triggered it was never recorded as approved. In
both cases the intended updates are still returned, so a caller can see what a
rule _would_ have written.

At `tool_result` and later the tool has already run, so a deny only suppresses
output: the denying rule's writes are withheld, while the writes of guardrails
that allowed before it stand — a record of a confidential read is still there on
the next turn.

Three composite cases make the commit set explicit:

- **Approved ask** — the withheld writes apply atomically, the approval
  high-water mark among them.
- **Refused ask** — the action is denied and the withheld writes are discarded
  in full. Discarding them whole is what makes the same threshold re-ask.
- **Ask, then a later guardrail denies** — the deny short-circuits, and nothing
  persists: not the asker's withheld writes, and not the writes of guardrails
  that allowed before it. The action did not happen, so no counter advances for
  it. The intended updates are still returned, so the caller can see what each
  rule would have written.

### Durability

State is written through to Postgres before a guarded action is released. Each
session uses one FIFO write lane, and every Registry request has a bounded
deadline. The in-memory mirror advances only after Registry acknowledges the
write. A timeout or write error therefore denies that action instead of letting
the durable counter lag behind it; there is no background queue to retry or
drain during stop, and an unclean harness crash cannot lose an acknowledged
increment.

Session state is restored whenever a session's runtime is prepared, so **a cap
cannot be reset by forcing a restart**. If the restore read fails, preparation
fails — the runtime never starts from empty state, because an inducible restore
failure would be exactly the cap reset this guarantee excludes.

That bounded loss is acceptable for the counters at risk — call counts and
approval high-water marks. **Spend is never priced by the harness.** Registry
prices each usage delta as it arrives on the internal usage route (see
[`pricing.md`](./pricing.md)), updates the session and acting-subagent cost
accumulators and — for `user_daily_cost_budget` — the per-principal
`subject_window` counter in the same transaction, and returns the updated
totals in the acknowledgment — each **marked when unpriced usage is present**,
per the unpriced-vs-`$0.00` invariant, since the harness holds no price rows
and the ack is its only way to learn a model went unpriced mid-run. The
harness caches those acknowledged totals and evaluates every cost budget
against them, so price rows never cross the internal seam and an
organization's price overrides apply wherever its sessions run.

That acknowledgment is also the freshness contract for the one cross-session
scope: a `subject_window` read is as fresh as the last acknowledged usage
report, so two parallel sessions of one principal converge on the shared daily
total at usage-report cadence instead of diverging until respawn. A cached
total alone would still let a parallel session _start_ a turn one full turn
stale, so at each turn boundary — before `request`-phase evaluation — the
harness refreshes `subject_window` counters from Registry; the cache is a
within-turn optimization only. The residual bound is then in-flight concurrent
turns at reporting cadence — the same bound the flush path has (a report
unacknowledged past the retry window surfaces and denies stateful-guarded
actions) — so N parallel sessions cannot dilute a daily cap by more than the
documented reporting window.

Every state update is applied as an atomic delta. An increment is a conflicting
upsert that adds, never a read-modify-write, so concurrent writers cannot lose
each other's updates. The harness does not retry an ambiguous timed-out write:
additive upserts are not idempotent by themselves, so retrying without a durable
receipt could double-apply an increment.

State reaches Postgres through Registry, not directly: the harness sends
ordered non-cost deltas to the internal guardrail-state route (see the internal
API list in [`services/registry-service.md`](./services/registry-service.md)),
which applies them as the additive upserts described above, while cost enters
through the usage route as just described. Registry applies a flush only to a
session that belongs to the workspace on the route, and writes every row —
session state and `subject_window` counters alike — under that workspace. The
harness has no Postgres client; the control-plane/enforcement split holds for
state exactly as it does for rules.

## The API group

```
/apis/policy.runorca.ai/v1/guardrails         Guardrail CRUD
/apis/policy.runorca.ai/v1/guardrailtypes     the type catalog, read-only
/apis/policy.runorca.ai/v1                    APIResourceList discovery
```

This is an engine-owned extension group: it ships in this repository and is
available on every deployment, unlike extension groups a distribution serves
under its own domain. See
[`api-groups-and-extensions.md`](./api-groups-and-extensions.md) for the URL
model and how a client discovers which groups a server supports.

Organization-scoped guardrails are managed on the admin listener under
`/v1/organizations/guardrails`, alongside workspaces and API keys, and require
`org:admin`. The routes also accept `guardrails:read` and `guardrails:write`,
but admin API keys cannot be issued with those scopes and admin OIDC tokens must
carry `org:admin`.

### Guardrail

```jsonc
{
  "id": "grd_01K…",
  "type": "guardrail",
  "name": "block-force-push",
  "description": "",
  "enabled": true,
  "phases": ["tool_call"],
  "scope": "organization" | "workspace" | "explicit",
  "rule": {
    "kind": "builtin",
    "builtin": "blast_radius",
    "params": { "gate_pushes": true, "risky_action": "ask" }
  },
  "metadata": {},
  "archived_at": null,
  "created_at": "2026-07-31T00:00:00Z",
  "updated_at": "2026-07-31T00:00:00Z"
}
```

`scope: "explicit"` means the guardrail applies only where an agent or session
names it in `guardrail_ids`. The other two apply to every session in their scope.

### Rules

A rule is either a parameterized builtin or an expression.

```jsonc
{ "kind": "builtin", "builtin": "token_budget", "params": { "max_total_tokens": 2000000 } }

{ "kind": "expression",
  "expression": "!(event.tool.name == 'Bash' && event.tool.input.command.contains('rm -rf /'))",
  "on_false": "deny",
  "reason": "Destructive shell command blocked." }
```

Expressions are written in [CEL](https://cel.dev), the same language Kubernetes
uses for admission policies. CEL is a good fit here for the reason it is a good
fit there: it is declarative, total, and evaluates against supplied data with no
ability to reach the host. A tenant can express arbitrary conditions without the
server executing tenant code.

Expressions are **compiled when the guardrail is written**, not when it fires.
An expression that does not parse, references an unknown field, or exceeds the
size limit is rejected with a 400 at authoring time. Evaluation carries an
explicit timeout.

The activation exposes:

```
event.phase                                    the phase being evaluated
event.tool.name  event.tool.input              tool_call
event.result                                   tool_result
event.session.id  .agent_id  .turn_index
event.usage.*                                  accumulated tokens and cost
event.model.id
event.state.*                                  guardrail state, if any
```

### Guardrail types

`GET /apis/policy.runorca.ai/v1/guardrailtypes` returns the catalog: for each
builtin, the phases it supports, whether it is stateful, its verdicts, and a JSON
Schema for its parameters. It is the same catalog the server validates against,
so a client that renders a form from it cannot produce a rejected guardrail.

## Builtin catalog

### Tool and argument predicates

| Builtin                           | Phases               | Stateful | Purpose                                          |
| --------------------------------- | -------------------- | -------- | ------------------------------------------------ |
| `tool_permission_policy`          | tool_call            | no       | the fold seed; not authored directly             |
| `require_approval_for_tools`      | tool_call            | no       | escalate named tools to `ask`                    |
| `block_tools`                     | tool_call            | no       | deny named tools                                 |
| `ask_on_os_tools`                 | tool_call            | no       | ask before any filesystem or shell tool          |
| `read_only_os`                    | tool_call            | no       | deny file-mutating tools                         |
| `block_skills`                    | tool_call            | no       | prevent named Skills from loading                |
| `headless_subagent_purpose_guard` | tool_call            | no       | require a declared purpose on dispatch           |
| `deny_pii_in_llm_request`         | request, llm_request | no       | pattern-scan for PII before it reaches the model |
| `max_tool_calls_per_session`      | tool_call            | session  | cap total tool calls                             |
| `token_budget`                    | request, tool_call   | session  | cap total tokens; needs no price data            |
| `spawn_bounds`                    | tool_call            | turn     | cap subagent dispatches per turn                 |
| `detect_loop`                     | tool_call            | session  | detect a repeating tool-and-argument cycle       |
| `detect_thrashing`                | tool_result          | session  | detect a run of failing results                  |

`require_approval_for_tools` and `block_tools` are deliberately the same
predicate a permission policy expresses. That is the point: the identical rule
at workspace or organization scope is one the agent author cannot override. Same
evaluator, different authority.

### Shell-command predicates

| Builtin                     | Phases    | Stateful | Purpose                                                 |
| --------------------------- | --------- | -------- | ------------------------------------------------------- |
| `blast_radius`              | tool_call | no       | classify shell commands as safe, risky, or catastrophic |
| `block_working_dir_changes` | tool_call | no       | block `cd`, `pushd`, `git -C`, worktree moves           |
| `worktree_guard`            | tool_call | no       | block writes outside an allowed root                    |

These parse the command rather than matching it as a string, unwrapping `sudo`,
`env`, `bash -c`, and `eval`, and splitting chained commands. A gate that only
matched literal text would be bypassed by nesting.

They are heuristic readers, not sandboxes, and fail toward denial: a command
whose effect cannot be resolved (a shell nested past the reader's depth bound, a
write whose destination is not in argv) is denied rather than passed. Two limits
follow from being a reader. `worktree_guard` recognises writes by a curated set
of interpreters running inline code (`python -c`, `perl -e`, `node -e`, `awk`
redirecting to a file, and their common siblings); an interpreter outside that
set that writes from inside its own code is not seen. And a remote script a shell
runs from a source the reader cannot inspect — a fetched-and-decoded payload, an
exotic redirection into a sourcer — may slip an individual variant even though the
common `curl … | sh` / `sh <(curl …)` / `eval "$(curl …)"` forms are denied. For
airtight confinement, pair these with a tool-level rule (`read_only_os`, or
`block_tools`/`require_approval_for_tools` on the shell tool) rather than relying
on the parser to enumerate every evasion.

### Integration predicates

| Builtin            | Phases                 | Stateful | Purpose                                                   |
| ------------------ | ---------------------- | -------- | --------------------------------------------------------- |
| `github_policy`    | tool_call              | no       | restrict repository reads and writes                      |
| `gdrive_policy`    | tool_call, tool_result | session  | restrict Drive access, with confidential-file containment |
| `gmail_policy`     | tool_call              | no       | restrict mail access; no send by default                  |
| `gcalendar_policy` | tool_call              | no       | restrict calendar access; read-only by default            |

`github_policy` covers both the MCP tools and `git`/`gh` invocations in a shell.
Covering only one would produce a guardrail that looks enforced and is not.

`gdrive_policy` tracks files the session created so it can allow writes to them
while denying writes to confidential files read earlier in the same session —
containment, not just an access list. That is why it also fires on `tool_result`.

### Cost budgets

| Builtin                  | Phases             | Stateful                       | Purpose                                                                                                         |
| ------------------------ | ------------------ | ------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `cost_budget`            | request, tool_call | session                        | cap session spend in USD                                                                                        |
| `user_daily_cost_budget` | request, tool_call | subject_window                 | cap a principal's spend per UTC day                                                                             |
| `subagent_cost_budget`   | request, tool_call | session, per subagent identity | cap spend attributed to one acting subagent identity; managed Claude dispatches of the same Agent share the cap |

`user_daily_cost_budget` is valid at **workspace and organization scope only** —
a cross-session per-principal cap is an authority-tier control. The catalog
declares the restriction, so authoring it as `scope: "explicit"` is rejected at
write time. `guardrail_counters` keys the daily spend by workspace, principal,
and UTC day, so the cap counts a principal's spend within one workspace: an
organization-scoped rule applies its limit in each workspace separately.
Agent and Session `guardrail_ids` may reference a visible daily rule; references
preserve the stored workspace or organization authority scope and its counter
namespace. They do not turn the rule into an explicit-scoped budget.

All three evaluate in the same order, and the order matters:

1. Phases other than `request` and `tool_call` abstain.
2. **Unpriced check first.** No actual usage means there is nothing to price,
   so it does not trigger an acknowledgment. Once tokens have been consumed
   (including cache buckets), an absent, non-finite, or negative cost is
   unpriced. Registry retains a known-cost subtotal and a sticky unpriced flag;
   the runtime presents the affected budget's cost as absent instead of treating
   that subtotal as the complete spend.

   `on_unpriced: "deny"` denies and `"allow"` continues without an unpriced
   acknowledgment. The default `"ask"` asks at `tool_call`; only approval records
   consent to continue unmetered. At `request`, which has no confirmation
   exchange, the first evaluation with unpriced usage records a pending
   acknowledgment and allows the turn to reach a tool gate. Another request
   without approval denies. Thus an initially empty text-only session can record
   unknown usage on its first turn, mark it pending on the next request, and
   deny a further unapproved request. A tool approval clears that barrier for
   the rule's state namespace.

   Producers preserve unknown cost as absent rather than laundering it into a
   measured zero. A known subtotal never removes the sticky unpriced flag.

3. **Hard cap.** At or above the limit, deny — but see below. Over the cap on a
   permitted model, allow and return; soft thresholds do not run above the cap.
4. **Soft thresholds.** Ask the first time spend crosses each threshold, once.

Thresholds fire once because approval records a high-water mark, and that mark is
written only when the client approves. A refused approval leaves it unset, so the
same threshold asks again rather than passing silently.

Because `ask` is available at `tool_call` only, a soft threshold crossed during a
text-only turn surfaces at the next tool call rather than immediately. For a
_priced_ session the hard cap still applies at `request`, so an over-budget
session cannot keep running by never calling a tool. The request phase has no
approval protocol; its current behavior is the pending-then-deny sequence above.

**The hard cap is a downgrade gate, not always a hard stop.** With an explicit
`expensive_models` list, reaching the cap denies only while the session is on a
matching model, and allows again once it moves to a cheaper one — the session
degrades instead of dying. With no list configured, every model is blocked and
the cap is a true stop. An unknown model is treated as blocked, so a budget
cannot be evaded by making the model unreadable.

That gate needs an escape hatch to be honest, so `model` is accepted on session
update — `orca-beta`-gated, like every Orca-only field on a core shape. Changing
it re-prepares the session's runtime.

## Enforcement

Guardrails are **managed** by Registry and **enforced** where the action happens.

| Component | Role                                                                             |
| --------- | -------------------------------------------------------------------------------- |
| Registry  | control plane: CRUD, validation, catalog, composition, durable state, price data |
| Harness   | enforcement: evaluates every wired phase and applies the verdict                 |

Registry resolves and orders all four tiers when a session's runtime is prepared,
and hands the harness a compiled, ordered list plus the session's restored state
(the prepared-runtime contract in
[`multi-workspace-isolation.md`](./multi-workspace-isolation.md) carries both).
An edit sends no invalidation message. Before each `user.message`, the
dispatcher prepares the session's runtime again and compares a runtime
configuration key that includes the resolved guardrails; when the key has
changed, it stops the warm runner and starts a fresh one from the new
preparation. An edit therefore takes effect at the next **turn boundary** —
never mid-turn, which event-processing semantics rule out. A session mid-turn
finishes that turn under the old rule; its next turn runs under the new one. An
organization-scoped edit reaches each session the same way, at that session's
next user message. A runner parked on a required action (a pending tool
confirmation or custom tool result) is kept, so the matching result can still
complete.

A `deny` at `tool_call` becomes a tool error the agent reads. An `ask` becomes
the existing tool-confirmation round trip. A `deny` at `request` rejects the
message; at `tool_result` it replaces the output with a suppression notice.

**`block_skills` is enforced at materialization, in both topologies.** Skills
are staged into the sandbox as files rather than loaded through a tool, so a
rule keyed on a tool call has no call to key on. The list of Skills a session
gets is built from the prepared runtime, which carries the guardrails too, so
the stateless pass runs there and a blocked Skill is never staged. That is
strictly stronger than denying a load: the bytes never reach the sandbox.

### Sandboxed sessions

The cloud Claude sandbox-harness provider decides its tool exposure up
front rather than gating each call. Enforcement there splits by what a rule
reads, and the buckets below cover the whole builtin catalog:

- **Name-keyed stateless rules** — `block_tools`,
  `require_approval_for_tools`, `read_only_os`, `ask_on_os_tools`,
  and the permission-policy seed — read tool identity alone, so they resolve at
  tool-list construction: a rule resolving to `ask` or `deny` removes the tool
  from the list. (`block_skills` is not in this bucket — it is enforced at
  materialization for every topology, above.)
- **Argument-dependent stateless rules** — `blast_radius`,
  `block_working_dir_changes`, `worktree_guard`, `github_policy`'s shell
  coverage, `gmail_policy`, `gcalendar_policy`,
  `headless_subagent_purpose_guard`, and any expression over
  `event.tool.input` — cannot resolve before a tool input exists. Until the
  sandbox protocol carries a per-call policy hook, exposure-time evaluation
  supplies an unavailable-input sentinel. A matching rule that reads input is
  therefore treated as `ask`, which removes the tool because this topology has
  no confirmation round trip. This is conservative: even an input that would
  eventually pass cannot be admitted without a real per-call enforcement point.
- **Stateless `request` rules** run once per turn in the host. The current
  sandbox protocol has no per-model-call interceptor, so an `in_sandbox`
  runtime rejects every configured guardrail that declares `llm_request`
  (including `deny_pii_in_llm_request`) before opening the sandbox session.
  This fail-closed startup gate remains until the protocol can evaluate the
  exact bytes on **every** model call.
- **Stateful budgets that declare `request`** — `token_budget`, `cost_budget`,
  `user_daily_cost_budget`, `subagent_cost_budget` — are enforced at `request`,
  which the host-side dispatcher still sees per turn. Their ask steps have no
  confirmation vehicle in this topology: the unpriced check follows its
  `on_unpriced` parameter, with the default `ask` first marking an unpriced
  request pending and denying another unapproved request (see above); soft
  thresholds do not fire, because they ask only at `tool_call`; hard caps
  enforce unchanged.
- **Stateful rules that never fire on `request`** —
  `max_tool_calls_per_session`, `spawn_bounds`, `detect_loop`,
  `detect_thrashing`, `gdrive_policy` — have no enforcement point under
  `colocated`: their phases run in-sandbox without a state store, so their
  counters never advance. They are **inert in this topology**.

A configured rule that would be inert is surfaced, not ignored: runtime
preparation emits a session warning event naming each guardrail it cannot
enforce under the requested mode, so an operator sees the gap — and can gate
the mode — instead of discovering it after an incident. This is a real
difference in behavior between the two topologies and is documented rather than
hidden; see [`harness-modes.md`](./harness-modes.md).

Cloud Codex uses `CodexSdkHarness` in both modes for policy evaluation and durable
request budgets. Moving its SDK into sandbox-harness does not move policy or
accounting ownership into Registry. The catalog retains the colocated request-only
guardrail admission contract.

### Self-hosted runner sessions

A session driven by `session-runner` is a third case. It evaluates stateless,
top-level `request` rules. Managed-resource Sessions also enforce stateless,
top-level `block_skills` rules before installing Skill files and composing the catalog.
Self-hosted Codex SDK and Pi SDK colocated Sessions delegate request policies to
Registry, including durable stateful request budgets.
Anything outside that boundary prevents the policy snapshot from being accepted;
there is no configured-but-inert mode.

The runner is a client: it dials the registry outbound and holds no database,
no broker, and no durable state store. Guardrails and their restored state ride the
same credential-free snapshot that already carries the permission policy, which
is where they belong — the permission policy is the seed of the guardrail fold,
not a separate mechanism.

|                                                                             | Behavior                                                                                                                                      |
| --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| stateless, top-level `request`                                              | evaluated before `harness.submit`; deny produces a durable `agent.error` plus the turn-completed marker, and the model never sees the message |
| stateful `request` rules                                                    | Registry enforces them for managed Codex SDK and Pi SDK; other runners refuse the snapshot                                                    |
| subagent-scoped rules                                                       | **the snapshot is refused** until the coordinator supplies the dispatched subagent identity at an enforcement point                           |
| stateless, top-level `block_skills` on managed resources                    | blocked Skills are absent from both `/workspace/skills` and the advertised catalog                                                            |
| other `tool_call`, `tool_result`, `llm_request`, `llm_response`, `response` | **the snapshot is refused**                                                                                                                   |

Registry is the producer for this wire contract. It composes the same session →
agent → subagent → workspace → organization fold as the separate harness path,
restores the session state, and includes both in the initial snapshot. Before
every user turn, the owner-pod bridge resolves the fold again. An unchanged fold
does not reconfigure the persistent harness; a changed fold is re-delivered
before the turn. If resolution or delivery fails, the bridge refuses the turn
instead of running it under stale policy. A runner reconnect re-delivers the
full snapshot and catch-up retries an unanswered turn.

Managed Codex SDK and Pi SDK snapshots carry the private
`request_guardrails_owner: "registry"` field only with verified managed
resources. The runner skips local request evaluation;
Registry compiles and evaluates the refreshed policy before sending the accepted
message to the SDK. It reloads durable Session state and the authenticated actor's
UTC daily counters on every request. Client-supplied subject fields have no authority.
Request-only cost policies treat `on_unpriced: "ask"` as denial; explicit `allow`
remains supported. Soft approval thresholds and subagent budgets are refused.

For stateful request policies, Registry acknowledges request state updates and a private pending-usage marker before
SDK submission. Complete SDK usage is priced against the Session's pinned model
(an OpenAI model for Codex SDK) and committed once per accepted turn, together
with Session statistics, guardrail totals and authenticated daily counters. The bridge acknowledges usage,
then native history, then marker deletion before forwarding completion. Missing usage,
interrupts, execution failures and unacknowledged writes retain the marker and deny
later guarded requests, including after reconnect. Unguarded Sessions still record
SDK usage and remain usable after interruptions. A lost deletion acknowledgement is reconciled
against durable state. Confirmation and interrupt followers remain independent of a
budget-denied request. Native checkpoints and accounting markers never enter the
public transcript. SDK usage remains authoritative when Gateway's usage sink is enabled;
internal usage writes, including shared-token calls, are refused for these Sessions.

### Egress enforcement

Registry also serves a session's composed guardrails to the AI Gateway:
`GET /internal/v1/guardrails/effective` returns a session-scoped bundle holding
the same organization, workspace, Agent and Session fold, and the restored
state, that a prepared runtime carries. The Gateway is a separate binary that
cannot import the evaluation library, so it consumes this bundle rather than
the implementation. The chart switch that enables the
Gateway's Registry guardrail source is described in
[`services/ai-gateway.md`](./services/ai-gateway.md); its rollout status is in
[`roadmap.md`](./roadmap.md#designed-not-built).

## Not in scope

Model-judged guardrails, risk scoring and sandbox configuration enforcement are
not built; each is listed, with what it requires, in
[`roadmap.md`](./roadmap.md#deferred-by-design).

## Verification

- **Unit, evaluation library** — the verdict lattice over every pair; composition
  ordering, short-circuit, and that no tier loosens a higher one, including the
  pass nesting: an organization-tier stateless deny preempting a session-tier
  stateful guardrail, whose counter does not advance; write timing per verdict
  and the three composite commit sets — an approved ask applying its withheld
  writes atomically, a refused ask discarded whole, and an ask preempted by a
  later deny persisting nothing at all; the stateless
  pass running with no state store; expression
  compilation rejecting bad input at authoring time and timing out on
  pathological input; **failure modes as specified: an evaluator that throws
  (and one that times out) yields deny on a fail-closed phase and allow on a
  fail-open one, and an unregistered builtin denies on a fail-closed phase;
  either way the error is recorded on the decision's `errors` with the
  `failedClosed` flag it was handled under**; each builtin against its
  documented behavior; every builtin with a fixed tool set asserted against the
  real runtime tool names.
- **Unit, service** — group routes on the public surface only; discovery
  advertising the group; the error envelope applied to group paths.
- **Integration** — CRUD with pagination, workspace isolation, and idempotency;
  reference validation on agents and sessions; deletion of a guardrail an agent
  still names rejected, at the workspace and the organization tier; an
  organization-scoped guardrail applying to a session that never referenced it,
  readable but not writable from the workspace API; composition order in the
  prepared runtime; authoring `user_daily_cost_budget` with `scope: "explicit"`
  rejected at write time, with the rendered schema declaring the restriction;
  visible Agent and Session references accepted without changing the stored
  authority scope; concurrent state
  increments both landing; two same-kind rules in one session, and two
  runtime-provided acting subagent identities, each holding independent
  namespaced state (one rule's approval never arming or suppressing the
  other's); the usage route resolving the `subject_window` subject from the
  turn's accepted-event record and ignoring any caller-supplied subject;
  preparation refusing to start when guardrail state cannot be restored.
- **Harness** — a permission-policy regression suite that passes unchanged before
  and after the engine takes over the decision; counters surviving a respawn;
  turn counters resetting; an `ask` producing the existing confirmation round trip
  and no new event type; a coordinator guardrail blocking a tool for a subagent it
  dispatched; a warm runner replaced before the next user turn when the
  session's prepared runtime changes (exercised with a session tool override);
  a flush failure surfacing and denying stateful-guarded actions past
  the staleness window; the unpriced `ask` first recording a pending
  acknowledgment and denying a later unapproved `request`, and `on_unpriced: allow`
  accepting unmeasured spend explicitly; **colocated:** a tool a stateless
  guardrail resolves to `ask` or `deny` omitted from the constructed tool list,
  a stateful budget denying at the next `request` rather than mid-turn, the
  inert-guardrail warning event emitted at preparation, and runtime preparation
  rejecting `deny_pii_in_llm_request` before the sandbox opens, because no
  per-model-call interceptor exists.
- **End to end** — guardrails at each tier against a live session, observing both
  a denial and an approval in the transcript; a cost budget edited between two
  turns denying the next turn before any model call.

The HTTP budget authoring suite (`budget-wire.spec.ts`, 20 cases) covers the
four budget catalog entries, parameter rejection and preservation on rejected
updates, unpriced modes, daily authoring scope, visible references, and admin
write permissions. The deterministic spend suites cover 26 core scenarios,
eight native subagent scenarios, and a production self-hosted runner refusal
with a stateless positive control. See [`pricing.md`](./pricing.md#verification)
for the accounting evidence and [`local-stack.md`](./local-stack.md#running-e2e-tests)
for the commands. The real-Claude budget smoke checks the next request boundary
in separate mode and, when enabled, colocated mode; it requires provider access.
