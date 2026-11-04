# Dogwood guardrails

Dogwood is the temporal policy rule kind. It composes with builtin and CEL guardrails
and the permission-policy seed. An enforce rule can tighten a decision;
an observe rule records its raw verdict without changing the enforcement fold.

## Authoring and schema

`GUARDRAILS_DOGWOOD_ENABLED=false` is the default. When enabled, guardrail create/update
accepts this rule shape:

```json
{
  "kind": "dogwood",
  "policy": "@id(\"baseline\") permit(principal,action,resource);",
  "applies_to": "all_tools",
  "history": "session"
}
```

`applies_to` defaults to `all_tools`; `history` defaults to `session`. MCP rules carry
`mcp_manifests`, an array of `{server,url,tools}` objects from MCP `tools/list`. Registry
stores the camelCase equivalents. Dogwood fires at `tool_call` and records outcomes at
`tool_result`; explicit phases outside those two are rejected. Authoring uses the actual
pinned core parser, macro expansion, schema validation, annotation checks and limits.
Responses include core warnings. Empty/forbid-only policy sets retain default deny.

`GET /apis/policy.runorca.ai/v1/policyschemas/dogwood`, with `guardrails:read` or workspace
full access, serves the same base schema, session/principal event schemas, core/language
versions and limits used by validation. Reads and lists preserve opaque future rule kinds;
composition refuses unsupported kinds.

The implementation pins `@runorca/dogwood` / `orca-dogwood` 0.1.0, actual upstream
`dogwood-language` 1.1.0 and Cedar 4.11.0. The Node WASM package and matching source archive
are owned by `vendor/dogwood/`; [its provenance](../../vendor/dogwood/provenance.json)
records the exact source commit and hashes. Consumers build without a sibling checkout or
an unpublished remote package. The source archive includes the native clock helper.
CI and the Harness image build verify both archive hashes, the source manifest, package
pins, WASM and packaged helpers against that committed provenance. The Linux Rust builder
emits its helper checksum; installation checks it and the deployed package pin, records a
source-bound consumer build attestation, and checks a native clock reading. This attestation
records input and transfer integrity, without claiming a reproducible binary build.

## One session enforcer

| Execution                              | Dogwood enforcer | Accepted coverage                                                    |
| -------------------------------------- | ---------------- | -------------------------------------------------------------------- |
| Cloud Claude Agent SDK, separate       | Harness          | Published canonical tools, MCP tools, pinned acting-agent identities |
| Colocated Claude; Codex, Pi or DSH SDK | Gateway          | Unbound `mcp_tools` rules                                            |
| Self-hosted, Gateway egress            | Gateway          | Unbound `mcp_tools` rules                                            |
| Self-hosted, sidecar egress            | None             | Preparation refuses Dogwood                                          |

`AI_GATEWAY_DOGWOOD_ENABLED=false` is the default. Registry chooses Gateway only when
this deployment qualification gate is enabled. The chart renders these gates from
`guardrails.dogwood.enabled` and `aiGateway.registryGuardrails.dogwood`. Consumers declare
`dogwood.0.1.0`; an incompatible reader is refused before ownership changes. Rules reach
only their selected enforcer. Gateway-enforced execution snapshots omit Dogwood while the
Gateway effective-bundle route returns the complete active prepared policy, including
builtin rules, under the policy log's identical `bundle_id`.

A turn binds its policy, principal, Agent roster, original MCP endpoints and authenticated
budget subject. The first Vault is the principal; Vault metadata and Agent attributes are
Registry-owned. A no-Vault session uses an unshared Unattributed identity. MCP action
names come from URL-matched manifests; a local server alias cannot rename them. Endpoint
hostnames exclude ports. Ambiguous server/URL manifest identities are refused.

The published vocabulary contains nine logical effect tools plus manifest-backed MCP actions.
Unknown native callbacks, including `Agent`/`Task` orchestration, fail closed. They do not
alias an effect tool or silently bypass `all_tools`. The roadmap records this workflow limit.

The Harness compares live MCP catalogs through its existing credential-free Gateway
route, with bounded reads, and emits `session.warning` on drift or an unreadable catalog.
It never obtains Vault credentials. Missing fields remain evaluation errors and enforce
rules deny. Gateway sends managed drift evidence to the internal policy-warning route;
Registry creates the safe warning text and a deterministic transcript event identity.

## Registry authorization transaction

`GET` and `POST /internal/v1/workspaces/:workspaceId/sessions/:id/policy-events` are
workload-authenticated, enforcer-bound and `Cache-Control: no-store`. The contract is
[`policy-events.contract.ts`](../../services/registry-service-ts/src/contracts/policy-events.contract.ts).
GET returns fresh Registry time, active epoch/bundle, complete retained history, server
identities, original endpoint bindings, log heads, day/month windows and a `state_revision`.
Its `guardrail_state` is a complete authoritative scoped snapshot, including read-only
usage facts. Pending writes are restricted to private keys of prepared rules. The longest
matching prepared `g:<id>:` namespace owns a private key, including IDs containing colons.
Builtin counter periods come from declared catalog metadata; shared usage counters use
exact known keys. Reads, private writes and accepted usage share the same period selector.
Canonical policy and bundle digests use locale-independent code-unit key ordering.

Malformed sequence cursors return 400. A missing or terminated session returns 404;
a different enforcer or unauthenticated turn returns 403; unavailable/incompatible policy
bindings return 409. Preparation refuses a backwards Registry clock with 503 before
changing ownership. These error responses are part of the internal contracts.

In one PostgreSQL transaction, a decision append:

1. Locks the session, workspace state revision and applicable principal/session heads.
2. Replays an identical committed batch before new epoch/time validation. Changed payloads
   under the same batch ID conflict.
3. Checks epoch, pinned bundle, log heads and the state revision read for the builtin fold.
4. Re-stamps the candidate's identities, re-evaluates governing Dogwood rules in bounded
   workers with fresh Registry time, and verifies per-rule verdicts and permitted events.
5. Validates a confirmation against the current pending call and a Registry-authenticated
   `user.tool_confirmation`. Only its actual asking rules receive approvals.
6. Applies private guardrail updates, samples append time again and commits events, call
   status, immutable receipt and state updates together. Expiry or a backward clock step
   rolls the entire transaction back.

Every state/usage mutation advances the workspace state revision. This also fences shared
budget subjects used by sessions with different Vault principals. A conflicting reader
refreshes the complete snapshot and folds again. The shared core merges bounded manifest
projections into one ordinary event, preserves distinct approvals, orders approvals before
the consuming request and rejects conflicting projections.

Receipts carry `batch_id`, `tool_use_id`, epoch, bundle, `committed_at` and an explicit
`valid_before` timestamp or `null`. The exclusive bound is the minimum finite bound of
enforced Dogwood results; observed results impose no dispatch bound. Completion appends
record an existing authorization after expiry without authorizing it again. A new owner or
turn closes uncompleted requests as interrupted, preserving their count/amount reservation.
Known completed calls receive no extra interruption.

Native forward migrations 0067/0068 record their provenance. History includes the exact
24-hour lower boundary. A bounded timer prunes only older events; heads and receipts remain
for replay. Event capacity accounts for completion reservations, so a full log prevents new
authorizations without preventing an already authorized result from being recorded.

## Harness release and recovery

The Harness refreshes history/time before each tentative decision, evaluates the actual
WASM core in bounded disposable workers, and serializes authorizations per session. Parallel
siblings see earlier request reservations. A later model message waits a bounded interval
for earlier results. Results and failures enter the log before their local completion barrier
opens. Turn-scoped updates remain local; durable updates commit with the log.

Only an original `created` receipt for a still-active attempt can release `canUseTool` once.
The final synchronous check immediately before returning/resolving allow covers cancellation,
local ownership, receipt identity, disposition and the finite deadline. Confirmations use the
same release path. A changed ask is presented again with a fresh public confirmation identity.

For finite receipts, `DOGWOOD_DISPATCH_CLOCK_ERROR_BOUND_MS` is an explicit deployment bound
for the whole dispatch interval; absence means unknown and finite release fails closed. It
is independent of a bundle lease bound. The native helper uses Linux BOOTTIME or Darwin
MONOTONIC plus OS boot and process identities. Missing/corrupt helpers, clock reset and an
unusable bound fail closed. There is no JavaScript wall-clock/monotonic fallback.

The conservative deadline is the original pre-POST native sample plus
`valid_before - committed_at - error_bound`. Equality is expired. A null bound adds no time
cap, while cancellation, ownership and recovery checks still apply. This release boundary
is the permission callback resolution, not physical tool start inside the SDK.

After an uncertain exchange, the attempt retires before bounded identical retries. A
replayed receipt is historical acknowledgment and never releases a tool. Reconciliation
returns a recorded outcome or closes only an uncompleted request as interrupted. A late
created response cannot revive an abandoned attempt. There is no automatic resend or claim
of exactly-once external side effects. The permission callback does not provide an execution lease.

## Evidence

Each participating tool call emits `session.guardrail_decision` before any requires-action
notification. It includes the enforcer, final fold, Dogwood rule IDs/errors and observed
verdicts. The exporter keeps bounded allowlisted metadata in the projected trace, without
tool inputs, outputs or policy text. The model sees a deciding description or a generic
message. Registry exposes bounded-label evaluation latency/count, append-result and prune
metrics. Qualification evidence and platform validation boundaries live in
[the roadmap](roadmap.md).
