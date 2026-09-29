# `@orca/guardrails`

`@orca/guardrails` is the evaluation library for Guardrails: the rule model, the
builtin catalog, the composition engine, and the expression compiler. It is a
TypeScript library imported in-process by Registry, Harness, and the in-sandbox
harness (which runs the stateless pass alone), not a deployable service.

See [`../guardrails.md`](../guardrails.md) for the model this implements.

## Contract

This package defines the consumer contract below. Registry consumes it from the
`policy.runorca.ai/v1` routes and Harness from its enforcement points; the
library itself reaches neither a database nor a network.

- Registry integration calls `compileGuardrailRule(rule, scope)` when a Guardrail is written.
  Compilation validates parameters against the catalog's JSON Schema, parses and
  type-checks an expression, and reports whether the rule is stateful and which
  state keys it reads. A rule that cannot compile is rejected at authoring time,
  so a rule can never fail for the first time while an agent is running.
- Registry integration serves the catalog verbatim as `guardrailtypes`, so the schema a
  client validates against is the schema the server enforces.
- Harness integration calls `evaluateGuardrails(guardrails, event, opts)` and receives a
  verdict, the reasons that produced it, the state updates to apply, and any
  evaluation errors the engine folded into the verdict. The engine never writes
  state itself. Tier order — session → agent → workspace → organization — is
  applied inside evaluation, so the caller passes the prepared guardrails without
  ordering them first.
- `evaluate` runs the stateless partition first and the stateful partition
  second. A caller that passes no store gets the stateless pass alone, which is
  how enforcement works at points that decide tool exposure before any tool runs.
- A `deny` seed is terminal: the permission policy already refused the action, so
  `evaluate` returns before any rule runs — no state is read, and no update is
  emitted for a call that can never execute.
- A guardrail resolved from a dispatched subagent applies only to that subagent's
  events. Rules with no subagent binding apply to every actor in the session.
- Each rule's state is namespaced by its guardrail identity (and dispatch
  identity, when bound), applied by the engine on both the read view and the
  emitted updates — evaluators handle bare keys and independent rules cannot
  read or overwrite each other's bookkeeping. Keys the runtime writes stay bare
  and are shared: the usage vocabulary in `SHARED_USAGE_KEYS`.
- `readOnly` evaluation returns the updates a run *would* have applied without
  reporting them for persistence, which is how a rule is tested against live
  state without arming it.

## No I/O

The package depends on no HTTP server, database client, object store, or agent
SDK. State and model-judgement reach it through interfaces its consumers
implement:

- `GuardrailStateStore` — read a scope, apply updates. `InMemoryGuardrailStateStore`
  supplies deterministic in-process tests; Harness supplies an implementation
  backed by Registry through the internal guardrail-state route (see
  [`../services/registry-service.md`](../services/registry-service.md)).

This constraint is the design, not an accident of layering. A package that cannot
reach a database mid-evaluation forces every state access to be explicit, keeps
the whole suite runnable without infrastructure, and makes it structurally
impossible for a rule Registry accepted to mean something different when Harness
enforces it.

## Verdicts compose as a lattice

`allow < ask < deny`, combined by maximum. Every composition in the library —
across guardrails, across tiers, and between a guardrail and the seeding
permission policy — is that one operation. Monotonicity is therefore a property
of the type, not a rule each call site must remember: nothing in the library can
express loosening a verdict.

## Related

Pricing types and cost arithmetic live in `@orca/harness-catalog` alongside the
model catalog they belong to, not here — see [`../pricing.md`](../pricing.md).
This package does not depend on it: the cost guardrails read an already-computed
`total_cost_usd` off the event, so pricing is the caller's to resolve, upstream
of evaluation.
