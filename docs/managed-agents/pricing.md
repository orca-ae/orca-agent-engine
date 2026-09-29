# Model Pricing

Sessions accumulate token usage. Turning that into money requires per-model
prices, and prices are neither static nor universal: vendors reprice, operators
negotiate rates, and a self-hosted deployment may serve models no public catalog
has ever listed.

This document defines where prices come from, how they are resolved, and the
`pricing.runorca.ai/v1` API group operators use to manage them. The cost
guardrails that consume this are defined in [`guardrails.md`](./guardrails.md).

## What is priced

Four token classes are priced independently, matching how model providers bill:

| Class          | Notes                                                      |
| -------------- | ---------------------------------------------------------- |
| input          | the non-cached portion of the prompt                       |
| output         | includes reasoning tokens, which are not billed separately |
| cache read     | a cache hit, typically ~10% of the input rate              |
| cache creation | writing to the cache; rate varies by cache lifetime        |

Cost is the sum of each bucket at its own rate. The buckets are **additive**:
cached tokens are counted in their own class and excluded from `input`, never
subtracted from a total.

```
cost = input×inputRate
     + output×outputRate
     + cacheRead×cacheReadRate
     + cacheCreation5m×cacheWrite5mRate
     + cacheCreation1h×cacheWrite1hRate
```

Sessions track two cache-creation buckets with different lifetimes, and providers
price them differently — a five-minute cache write costs less than a one-hour
one. Catalogs generally publish a single cache-write rate: it applies to the
short-lived bucket, and the long-lived rate derives from it as
**1h = 1.6 × 5m**, the ratio between the standard input multiples below.

When a catalog entry omits cache rates entirely, they derive from the input rate
at fixed ratios rather than being dropped: **cache read = 0.1 × input,
five-minute write = 1.25 × input, one-hour write = 2 × input.** Dropping them
would under-report a cache-heavy session; billing them at the full input rate
would over-report one by roughly ten times. An **explicit zero cache rate** is a
real rate and is honored — absence, never zero, is what triggers cache-rate
derivation. Input and output rates remain strictly positive: a zero base rate
would present unmeasured usage as `$0` and disable every cost budget. Negative,
over-precise, or non-numeric rates are rejected everywhere.

## Priced and unpriced are different from zero

A session whose model has no price data is **unpriced**, and that is not the same
as costing nothing. The distinction is load-bearing:

- Cost budgets follow `on_unpriced`. With the default `ask`, no actual usage
  triggers no acknowledgment; a tool call asks for approval once usage is
  unknown. At a request boundary the first unpriced evaluation records a
  pending acknowledgment and allows, while another unapproved request denies.
  See [`guardrails.md`](./guardrails.md#cost-budgets).
- Registry retains the subtotal of priced usage alongside a sticky unpriced
  flag. Later known usage increases that subtotal without clearing the flag.
  Budget evaluation receives absent cost for an affected scope, so it cannot
  mistake the partial subtotal for complete spend.
- The public Session `usage` contains token counters, not Registry-priced USD
  totals. A `span.model_request_end` event can carry the SDK's `total_cost_usd`
  estimate, which can differ from Registry's organization-specific prices.
  Registry usage acknowledgments are the accounting authority.

An absent cost, a known subtotal with unknown usage, and a measured zero carry
different meanings; consumers preserve that distinction.

## Resolution

A model identity — the `{ provider, id }` pair, since the same id served
through two providers may carry different rates — resolves against the catalog
in a fixed order, always within its provider:

1. **Exact match** on the model id.
2. **Dated-snapshot match** — a trailing date suffix is stripped, so a pinned
   snapshot inherits its base model's price.
3. **Family match** — the final hyphen-delimited segment is dropped and the
   remaining prefix is matched, so a newly released point revision inherits from
   its family.
4. **Unpriced** if nothing matches.

Family matching has an ambiguity guard: if several candidates at the winning
source precedence disagree on any rate, the result is unpriced rather than one
of them. Guessing a price from an ambiguous family would silently mis-bill;
refusing to guess surfaces it.

A rate that is zero, negative, or non-finite is not a price at all — it would
compute a `$0`/`NaN` cost that reads as measured and silently disables the
budget — so such an entry is dropped and the model resolves unpriced.

The ladder composes with the sources in one fixed way: **specificity outranks
source**. Each rung is tried across every source before the next rung is
consulted — an exact seed match for the queried model beats an operator row
that would only match its family, because a fuzzier match from a higher
authority is still a guess about a different model. Within one rung, sources
break the tie by precedence:

```
operator (the resolving organization's own rows)  >  upstream  >  seed
```

Operator rows are organization-scoped and the other two sources are
deployment-global, so one organization's override never changes what another
organization is charged. A workspace resolves through its owning organization.

## Sources

### Seed

The seed is a price file checked in at
`packages/harness-catalog/src/model-prices.seed.json` and loaded at startup (see
[`libraries/harness-catalog.md`](./libraries/harness-catalog.md)). It exists so
that a deployment with no network egress prices correctly out of the box, and it
doubles as the worked example an operator copies when adding an entry.

The file carries a provenance header naming its upstream and that upstream's
license.

### Upstream

When a refresh URL is configured, a background refresher periodically fetches the
upstream catalog and atomically replaces the deployment-global `upstream` snapshot.
A failed fetch changes nothing: the last successful rows remain authoritative.
Models omitted from a successful snapshot are removed, so a vendor withdrawal
cannot leave a stale price active forever. Leaving the URL unset is a supported
configuration — the deployment runs on seed and operator entries and never
reaches the network.

A fetch that _succeeds_ with bad content is not applied blindly. A malformed
payload — not a JSON object, a missing or unsupported `schema_version`, or no
`models` — is rejected **whole**, treated like a failed fetch, and so is a
payload in which every entry is skipped. An entry whose rates cannot be read is
skipped. A well-formed payload is validated **per entry**: an entry with
a non-positive rate or an out-of-bounds delta is skipped (its prior row stays
authoritative) while the rest of the catalog applies, so one bad or
legitimately free upstream model cannot pin every other price to stale rows.
**Out-of-bounds is a pinned rule, not a judgment call**: a rate more than
**10× above or below** the baseline is skipped, where the baseline is the
currently persisted `upstream` row for that entry, falling back to the `seed`
row when no upstream row exists; an entry with no baseline at all (first
sighting of a new model) has no delta bound and is accepted on the other
checks alone. The factor is fixed at 10.
The strictly-positive floor is the zero-price-attack guard: upstream outranks
seed, and the writes section below names the attack ("set them to zero and
walk through every budget") as the reason price writes are
organization-gated — a buggy or compromised upstream catalog must not achieve
the same thing through the refresher. A failed fetch or rejected payload is
counted in `registry_service_model_price_refresh_total` with `result="failed"`,
so a failing refresh is visible before the staleness metric ages.

Prices are stored in Postgres rather than cached per process, so every replica
resolves identically and a restart keeps the last successful rows. When a URL
is configured, each replica's refresher fetches once at startup and then every
`PRICE_REFRESH_INTERVAL_MS` (six hours by default).

### Operator

Operators add and update entries through the API group below. Operator entries
outrank both other sources — for the organization that wrote them alone. An
operator row carries the `organization_id` of the admin credential that created
it, so a multi-organization deployment cannot have one organization repricing
every other's budgets; seed and upstream rows stay deployment-global.

Recorded cost is **immutable**. A price change applies to usage reported after
it, never retroactively — accumulated spend is a record of what was charged, not
a derived value.

## The API group

```
/apis/pricing.runorca.ai/v1/modelprices     read
/apis/pricing.runorca.ai/v1                 APIResourceList discovery
```

Model prices are a separate group from guardrails because they evolve on a
different cadence and for different reasons — vendor repricing rather than policy
design — and because they are useful for cost reporting on a deployment that runs
no guardrails at all. See
[`api-groups-and-extensions.md`](./api-groups-and-extensions.md) for the rule
that groups split by who owns an API's evolution.

### Reads and writes are split

Reads are served on the workspace API: an operator must be able to see the rates
their spend is measured against.

**Writes are organization-scoped and served only on the admin listener**, under
`/v1/organizations/modelprices`, and take an `org:admin` credential: the routes
also accept `model_prices:write`, which neither admin-key command mints (see
[`workspace-administration.md`](./workspace-administration.md#authentication)).
A workspace that could set its own prices could set them to zero and walk
through every budget that applies to it. Price authority sits with the same tier
that owns organization guardrails, for the same reason.

### ModelPrice

```jsonc
{
  "type": "model_price_entry",
  "provider": "anthropic",
  "model_id": "claude-opus-4-8",
  "input_per_million_tokens": 15.0,
  "output_per_million_tokens": 75.0,
  "cache_read_per_million_tokens": 1.5,
  "cache_write_per_million_tokens": 18.75,
  "source": "operator",
  "fetched_at": null,
  "created_at": "2026-07-31T00:00:00Z",
  "updated_at": "2026-07-31T00:00:00Z",
}
```

Rates are quoted **per million tokens**, matching how vendors publish them. An
entry stores one cache-write rate, `cache_write_per_million_tokens`, which is the
5-minute rate; the 1-hour rate is never stored and derives at 1.6 × that rate
per the ratios above. A `null` cache rate derives from the input rate. The
workspace read returns the resolved price as a `model_price` with the same
identity and rate fields, no `source` or timestamps, and derived cache rates
filled in.

`source` is read-only; entries created through the API are always `operator`.
Deleting one falls back to whatever `upstream` or `seed` provides — or, when
neither has an entry, the model becomes unpriced and live sessions follow
their budgets' `on_unpriced` behavior at the next evaluation. An override is
reversible without re-entering the original numbers.

There is **no surrogate id**. `model_prices` is keyed on
`(provider, model_id, source, organization_id)` (see
[`data-model.md`](./data-model.md); `organization_id` is the empty scope for the
deployment-global seed and upstream rows), and that composite key is what an
entry is — unlike the `vlt_`/`vcrd_`-style resources, which carry a stored
primary key.

Item routes therefore address an entry by its model id in the path and its
provider in an optional `?provider=` query, which defaults to `anthropic`. A
deployment pricing one vendor never spells the provider out, and the model id
stays a single addressable path segment. An unreadable `provider` never falls
back to the default, because quietly resolving it would patch or delete a
different entry than the caller named: the workspace read and the admin DELETE
answer `400`, and the admin GET and PATCH answer `404`. List cursors carry the
provider for the same reason — `provider/model_id` on the workspace list, and
`provider model_id source` on the admin list — since the same id under two
providers is two prices, and paging on the id alone would collapse them into one.

## Keeping the seed current

A checked-in price file that goes stale silently is worse than no file, so two
mechanisms keep it honest:

1. **A staleness test** fails once the seed file's `generated_at` date is more
   than 180 days old. The threshold is deliberately generous: it catches a seed
   nobody has updated for two quarters without failing unrelated pull requests
   over a routine repricing.
2. **A staleness metric**,
   `registry_service_model_price_refresh_last_success_timestamp_seconds`, exports
   the time of the last successful refresh (0 until one succeeds), so an operator
   can alert on a deployment that has lost egress and is quietly running on seed
   prices.

## Cost accounting

Usage is reported as it is produced and **priced by Registry** at the internal
usage route, at that moment, with the model identity that produced it — the
harness never holds price rows, so the catalog (including an organization's
operator overrides) never crosses the internal seam. Cost accumulates as
integer nano-USD and can be converted to micro-USD only at a presentation
boundary, avoiding the drift of repeatedly summing floating-point currency.

The fixed-point contract makes that accumulation deterministic and independent
of how usage is partitioned into deltas: rates are validated to at most
**three decimal places per million tokens**, which makes every per-token cost
exact in integer **nano-USD** (a rate of $1.50/M is exactly 1,500 nano-USD per
token). A derived cache rate is converted to nano-USD once before any token
count is applied. Each delta's cost is then accumulated with exact integer
addition, with no per-delta currency rounding. A presentation layer may convert
the total to micro-USD by truncating only after accumulation. Reporting one call
as three deltas therefore yields bit-identical totals to reporting it as one.

Pricing per delta rather than at read time is what makes a session that changes
model mid-run accurate: each portion of the usage is priced at the rate that
applied when it was incurred. It is also what makes recorded cost immutable.

Spend is attributed to the session and the runtime-provided acting subagent
identity. Managed Claude uses the persistent Agent ID: repeated dispatches of
that Agent within the Session share its spend counter. A new dispatch thread
does not create a fresh subagent budget. Transcript thread IDs still identify
individual dispatches.

Budget enforcement is only as reliable as delivery of these deltas, so the
delivery contract is explicit: the harness retries a usage report until the
registry acknowledges it, and each delta carries an idempotency key so a replay
cannot double-price an immutable record — at-least-once delivery, exactly-once
accounting. The acknowledgment returns updated session, subagent-identity and
per-principal `subject_window` totals, including unpriced flags. The harness uses
these Registry snapshots for the next cost-budget evaluation.

The separate Claude adapter merges cumulative usage frames for each assistant
message and reports that model call's final usage once at `message_stop`, with
its provider model identity. Native child calls use the final assistant message
from the SDK SessionStore mirror. SDK terminal-result usage is a fallback only
when no per-message usage was reported; it is not added to already reported
calls. Usage acknowledgments gate both tool execution and successful turn
completion: a model call that crosses the cap can block the tool it
proposes, and a pure-text result cannot report success while its final usage
remains unacknowledged. An exhausted write-through attempt fails closed with
`guardrail_usage_unavailable`; a retry of an already committed delta cannot
charge twice. `token_budget` also tracks tokens locally, independent of prices.

Public `span.model_request_start`/`end` events represent an SDK query, which can
contain multiple provider requests. Deterministic per-call assertions therefore
compare captured Messages requests and Registry usage acknowledgments, rather
than counting these spans as provider calls.

## Verification

- **Unit, catalog** — resolution order: exact, dated-snapshot, and family
  matches within the model's provider (the same id under two providers
  resolving independently), and unpriced when nothing matches; specificity
  outranking source, with precedence `operator > upstream > seed` breaking
  ties within a rung only, and an operator row resolving only for its own
  organization; the family-ambiguity guard (two candidates disagreeing on any
  rate resolve to unpriced, never to either); cost arithmetic across all five
  buckets, including distinct 5m/1h write rates, the fixed derivation
  ratios (read 0.1×, 5m write 1.25×, 1h write 2× input; 1h = 1.6 × a published
  5m rate) and an explicit zero cache rate honored as a real rate; nano-USD accumulation
  partition-independent — one call reported as three deltas totaling
  bit-identically to one delta — and correct across a mid-session model
  change; the
  unpriced-vs-`$0.00` invariant — no layer defaults an absent price to zero,
  and a partially-priced session retains its known subtotal and sticky
  unpriced flag without presenting the subtotal as complete spend.
- **Unit, refresher** — a failed fetch changing nothing, neither the rows nor
  the last-success timestamp; a malformed payload rejected whole; a well-formed
  payload with one non-positive or out-of-bounds entry applying the rest while
  that entry is skipped and counted, its prior row still authoritative.
- **Integration** — reads on the workspace API; writes rejected there and
  accepted on the admin listener only; deleting an operator entry falling back
  to the upstream or seed row, and to unpriced when neither exists; replayed
  usage deltas priced once; a usage delta retried until acknowledged, and exhausted
  write-through failing closed with `guardrail_usage_unavailable`; the
  acknowledgment returning session, subagent-identity and subject-window totals —
  an unpriced delta's acknowledgment marking the model unpriced, with the
  session's next cost-budget evaluation following
  `on_unpriced`; usage reported per model call — a cost budget crossed by the
  model call that proposed a tool call denying that same tool call, not merely
  the next `request` (a fixture that fails under once-per-turn reporting).
- **Seed** — the 180-day staleness test (Keeping the seed current above) runs in
  the unit suite of the package that owns the seed file.

The executable end-to-end accounting checks are:

| Suite                       | Cases | Evidence                                                                                                       |
| --------------------------- | ----: | -------------------------------------------------------------------------------------------------------------- |
| `model-prices-wire.spec.ts` |    11 | live public/admin pricing HTTP, authority and organization isolation                                           |
| `budget-wire.spec.ts`       |    20 | catalog, authoring validation, unpriced modes and authority scopes                                             |
| `spend-control.spec.ts`     |    26 | real SDK with scripted Messages, exact Registry costs, all authority tiers, approvals, restart and ACK failure |
| `spend-subagents.spec.ts`   |     8 | native SDK Agent dispatch, public approvals, child identity, model-specific usage and file effects             |
| `spend-runner.spec.ts`      |     1 | real worker/runner rejects stateful budgets with 422, then accepts a stateless rule in the same Session        |

`pnpm e2e:spend` runs the last three suites serially on owned Postgres databases
and an S3 bucket. File assertions inspect real temporary directories and the
public S3-backed Files API. The in-memory development runtime does not establish
OS sandbox confinement. `guardrails-budget-agent.spec.ts` is the paid Claude
request-boundary smoke registered in `e2e:agent`; colocated coverage is enabled
with `ORCA_E2E_SANDBOX_HARNESS=1`. It checks real text output and positive public
token usage, then a fresh policy denial with no new model query, assistant output,
token growth or output files. These suites have different provider and runtime
requirements; registering the paid smoke is not evidence of a live provider run.
