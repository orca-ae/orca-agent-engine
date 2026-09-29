# OIP-005: Conformance as a decision register

- *Author(s)*: @freeznet, @sijie
- *Status*: Released
- *Proposal time*: 2026-06-25
- *Components*: registry-service-ts (conformance tooling, vendored spec, decision register,
  generated matrix), CI
- *Discussion*: None (predates the public repository)
- *Implementation*: under `services/registry-service-ts/`: `scripts/sync-anthropic-spec.mjs`,
  `scripts/{conformance-core,generate-conformance,orca-beta-status-scan}.mjs`, `scripts/lib/`,
  `vendor/anthropic/`, `conformance-decisions.yaml`, `anthropic-prose-invariants.yaml`;
  `docs/managed-agents/conformance-matrix.md`;
  `.github/workflows/{test-ts,release-images,nightly-anthropic-spec}.yml`
- *Released in*: v0.5.0

## TL;DR

The engine promises that Anthropic's SDKs work against it with only the base URL changed, so "how
does this API differ from Anthropic's Claude Managed Agents API?" needs an answer that cannot be
wrong by omission. The engine vendors Anthropic's OpenAPI description under a digest it computes,
diffs its own generated document against it, and fails the build unless every difference matches
exactly one rule in a decision register, with a decision and a resolvable reference, and every rule
still matches something. Contributors keep the register true; client authors read the matrix.

## Background

Public routes are ts-rest contracts, rendered by `pnpm openapi:gen` to
`openapi/managed-agents.yaml`; placement on `/v1` or in API groups, discovery and the
`orca-extension` tag are [OIP-009](OIP-009-core-and-extension-api-groups.md). Rules rest on the
[confirmed decisions](../docs/managed-agents/overview.md#confirmed-decisions): mirror the
managed-agents surface but not the inference API, and version by `orca-beta`. The reference is
[`conformance.md`](../docs/managed-agents/conformance.md), the output
[`conformance-matrix.md`](../docs/managed-agents/conformance-matrix.md).

## Motivation

- **Read from prose, the answer was wrong.** The first matrix was a hand-written table marking each
  Claude endpoint `implemented`, `partial`, `absent` or `extension`. A classification kept by hand
  labelled six Orca-only operations, `DELETE /v1/agents/{id}` among them, as Anthropic core, and one
  Anthropic operation, `GET /v1/skills/{id}/versions/{version}/content`, as Orca-only; the error
  reached a design document and a generated OpenAPI document. Anthropic publishes a complete
  machine-readable description, so presence can be computed.
- **A diff alone is noise, and a register decays into false coverage.** The matrix holds 352
  differences; without a decision on each, a new divergence looks like an accepted one. With
  decisions, a rule matching nothing reads as coverage, a family absorbs a newcomer nobody looked
  at, a rationale for "we accept more" outlives its delta, and a reference outlives its document.
- **Some differences are invisible to a diff.** Anthropic's spec declares `anthropic-beta` as an
  optional free-form string on its managed-agents operations, as Orca's does, while its
  documentation requires particular values.
- **Silence must mean something.** An absent row can mean no difference, never compared, or
  swallowed by a glob, unless the comparing code also states its reach.

## Goals

### In scope

- A computed, byte-stable answer to how the public API differs from Anthropic's published one,
  regenerated whenever either side moves, that states which axes it compares and which it does not.
- A decision and a resolvable reference on every difference, and a build that fails in both
  directions and whenever a rule's scope or approved delta changes under it.
- A pinned, verified copy of the upstream spec, refreshed through a reviewable pull request.
- Documented but unencoded requirements, the one input a diff cannot derive, kept apart and cited.

### Out of scope

- Closing differences; the unserved Anthropic surface is listed on
  [`roadmap.md`](../docs/managed-agents/roadmap.md#anthropic-surface-not-implemented).
- Error-response shapes, descriptions, security schemes, servers and webhooks; the matrix's
  [coverage table](../docs/managed-agents/conformance-matrix.md#what-is-compared) says why.
- The admin and internal listeners: the vendored spec has no such paths, and `openapi:gen` throws
  on any `/internal/` path.
- Validating requests or responses at runtime against either document.

## Design

Paths in this section are relative to `services/registry-service-ts/`, except those under
`.github/`.

### High-level design

```text
 .stats.yml (Anthropic's TypeScript SDK) ── openapi_spec_url ──► spec bytes
 pnpm anthropic:sync ──► vendor/anthropic/openapi.json + PINNED.json (sha256 computed here)
 src/contracts/*.contract.ts ── pnpm openapi:gen ──► openapi/managed-agents.yaml
 pnpm conformance:gen
   1. re-hash the vendored bytes against the pin; validate register, references, invariants
   2. classify both documents on (method, normalized path): core | missing | extension
        core: 2xx codes, 2xx media types, required parameters, error codes
        oasdiff, Anthropic as base: request body, 2xx body, header and query parameters
        anthropic-prose-invariants.yaml: one row per invariant
   3. every difference → exactly one rule; every rule → at least one difference;
      covers and deltaFingerprint unchanged               any failure: exit 1, nothing written
   4. write docs/managed-agents/conformance-matrix.md
 CI (test job; release validate job): openapi:gen → conformance:gen → git diff --exit-code
 nightly: anthropic:sync → both generators → "chore: sync anthropic spec" pull request, or fail
```

### Detailed design

**Vendoring** ([details](../docs/managed-agents/conformance.md#vendoring)).
`scripts/sync-anthropic-spec.mjs` reads `openapi_spec_url` from `.stats.yml` on the `main` branch of
`anthropics/anthropic-sdk-typescript`, as URL discovery only: the spec, not the SDK, defines the
surface. It writes the bytes verbatim, and `PINNED.json` records their provenance and a sha256
computed locally, since `.stats.yml`'s `openapi_spec_hash` is not a digest of them. With no
timestamp, an unchanged upstream is a no-op. Pull-request CI never re-vendors, so
`generate-conformance.mjs` re-hashes the file and refuses bytes that do not match their pin.

**One definition of an operation** ([details](../docs/managed-agents/conformance.md#normalization)).
`scripts/lib/normalize-operation.mjs`, shared by both generators, strips the `?beta=true` suffix on
Anthropic's path keys and reduces path parameters to positions, so operations compare on
`(method, normalized path)`; a GA and a beta variant collapse, beta winning, and any other collision
throws. If nothing matches at all, `classify` throws rather than report the whole surface as both
missing and an extension, as an early attempt did.

**Classification** ([details](../docs/managed-agents/conformance.md#classification)).
`scripts/conformance-core.mjs` is pure, so tests drive it directly:

| Class | Compares | Rule |
| --- | --- | --- |
| `missing`, `extension` | presence | one row per operation only one side publishes |
| `success-codes`, `response-media` | 2xx statuses; 2xx media types | strict; direction-sensitive |
| `required-params` | required query and header parameters, top-level body properties | informational; a union contributes what every branch requires; path parameters excluded; direction-sensitive |
| `error-codes` | declared 4xx and 5xx statuses | informational |
| `request-schema`, `success-schema`, `header-parameters`, `query-parameters` | wire schemas | oasdiff; content-fingerprinted |
| `prose-invariant` | documented rules the spec does not encode | hand-transcribed, cited |

`scripts/lib/schema-diff.mjs` runs oasdiff (`@oasdiff-js/oasdiff-js`, locked at 1.0.0) with `allOf`
flattened and prose and vendor extensions excluded, and wraps what running it showed: its output
order varies between runs, so it is canonicalized and the rows rendered here; it pairs responses by
literal status and media type, so a lone `201` and a single-media success body are aligned onto
Anthropic's before diffing, while `success-codes` and `response-media` report those divergences
from the untouched documents; and its thousands of leaves ("atoms") aggregate to one row per
operation and axis. `COVERAGE` in `conformance-core.mjs` lists what is and is not compared, and
the matrix renders both lists from it.

**Prose invariants**
([details](../docs/managed-agents/conformance.md#prose-invariants--the-one-thing-not-derived)).
`anthropic-prose-invariants.yaml` is the one hand-written input: each entry names a family, the
paths it governs, what Anthropic requires, what Orca does, and the https source it was transcribed
from, and two families of one kind may not claim overlapping paths. Its seven entries are six
beta-header rules and the threads list's ordering, which Anthropic states only in a description.
The rows render in their own section marked as not derived, and the tag computation ignores them.

**The register** ([details](../docs/managed-agents/conformance.md#the-decision-register)).
`conformance-decisions.yaml` holds match rules, not rows: 46 rules for 352 differences today. A rule
matches one class and either an exact `operation` or a `path` glob, and carries a `decision`
(`keep`, `not-implemented`, `accepted-deviation`, or `fix-later` for a divergence the project
considers wrong), a `reference` and a `rationale`.

- **Precedence.** An exact operation beats a glob and the longest glob wins, so one operation can be
  carved out of a family; a tie is an error. `*` matches any run of characters, slashes included,
  which is why discovery and the probes have exact rules and each API group a glob of its own.
- **Pins.** A glob declares `covers: <n>` and fails when its match count moves, so a newcomer to a
  family is looked at. Rules on the oasdiff classes declare `deltaFingerprint`, a SHA-256 of the
  matched rows' keys and atoms, so a changed atom fails while the count holds. A `success-codes`,
  `response-media` or `required-params` rule states the delta it approves and stops matching when
  that delta changes, unless it declares `anyDelta: true`. A `prose-invariant` rule names one
  invariant; a glob there would approve requirements nobody has read.
- **References resolve.** Each is a repository-relative document with an optional anchor, checked
  on every run against its `<a id>` anchors and heading slugs; `roadmap.md` marks such headings.

**The gate and the matrix.** `generate-conformance.mjs` exits non-zero and writes nothing when the
pin, register, a reference or the invariant file is invalid; when a difference has no rule, a rule
matches nothing, rules tie, or a count or fingerprint moved; or when per-class counts stop summing
to the total. A fix fails the build until its rule is deleted or narrowed in the same change. The
matrix states what was compared, summarizes by class and decision, and gives every row's decision.

**Drift gate.** The `test` job in `test-ts.yml`, after `pnpm -r build`, runs `pnpm openapi:gen`,
`pnpm conformance:gen` and `git diff --exit-code` over `openapi/` and the matrix, in that order, so
the matrix derives from the document the same run produced; the `validate` job in
`release-images.yml` repeats it at tag time. `changed-areas.sh` treats Markdown outside `services/`
and `packages/` as inert, the matrix included, so a hand edit of the matrix, or a renamed heading a
reference names, fails the next run of the gate rather than its own docs-only pull request.

**Nightly refresh.** `nightly-anthropic-spec.yml` runs the sync daily and on dispatch, then both
generators with `continue-on-error` so one run names every problem; if anything moved, it opens a
`chore: sync anthropic spec` pull request with the vendored pair, `managed-agents.yaml` (the tag is
computed from the vendored spec) and the matrix. An undecided difference or a tripped tagging
tripwire fails the run instead: a pull request that needs a human decision is worse than none.

## Changes by component

- **registry-service-ts**: the scripts, vendored spec, register, invariant file and matrix above,
  the tests below, and the `anthropic:sync`, `openapi:gen` and `conformance:gen` scripts (also at
  the repository root); devDependencies `@oasdiff-js/oasdiff-js`, `@apidevtools/swagger-parser`,
  `ajv`, and `@anthropic-ai/sdk` 0.113.0 as a test client.
- **CI**: the drift gate in `test-ts.yml` and `release-images.yml`, the nightly workflow, and the
  matrix's classification in `.github/scripts/changed-areas.sh`. Everything else: none.

## Public-facing changes

### API

None: no path, shape, header or status changes; changes made on the register's findings are in
Status notes. What readers can rely on:

- **The matrix** is the engine's answer to how its API differs from Anthropic's pinned spec,
  regenerated and never edited. `fix-later` rows are divergences the project considers wrong.
- **The register** is a contract for contributors: a change that adds a difference adds or extends
  a rule, updating `covers` or `deltaFingerprint` on purpose, and one that removes a difference
  deletes or narrows its rule. Prose that disagrees with the computed classification is wrong
  ([documentation rule](../docs/managed-agents/orca-extensions.md#documentation-rule)).

Events and streaming, wire protocols, storage, configuration, and metrics: none.

## Compatibility

Nothing is persisted and nothing runs in a deployment, so upgrade and rollback are unaffected; a
revert takes its artifacts and rules with it, and the gate fails if they disagree. There is no skew
between components. The matrix describes Anthropic's spec as pinned, not as served today: every row
is true of the digest the matrix cites, and upstream changes reach it only through a refresh.

## Security considerations

- **No runtime surface.** The vendored spec, register and generators are used only at build and
  test time, never by a service (`vendor/anthropic/README.md`); every added tool is a devDependency.
- **Verified inputs.** The generator and `conformance-matrix.spec.ts` re-hash the vendored file
  against its pin, so an altered copy is refused, not diffed. The nightly workflow's token is
  read-only except in its `sync` job, which pushes a branch and opens a pull request.
- **No internal routes in the public document.** `generate-openapi.mjs` refuses `/internal/` paths.

## Testing

Each check compares two independently sourced things, never the differ against itself.

- **Unit** (`services/registry-service-ts/test/unit/`): `conformance-core.spec.ts` (normalization,
  precedence and ties, both halves of the gate, union intersection, delta pins, `covers`,
  fingerprints, invariant citations and overlaps, reference resolution); `schema-diff.spec.ts`;
  `conformance-matrix.spec.ts` (the real comparison over the committed artifacts, against
  expectations written by hand from Anthropic's spec); `conformance.spec.ts` (tags read from the
  committed document against a hand-written audit; all 131 raw Anthropic operation keys accounted
  for; success codes aligned, with `orca-beta-status-scan.mjs` reading `reply.code(...)` from the
  route sources); `route-contract-parity.spec.ts` (served routes against the contract, both ways);
  `openapi-instances.spec.ts` (every published schema compiles and accepts or rejects real values).
- **Integration**: `test/integration/sdk-conformance.spec.ts` drives `@anthropic-ai/sdk` against a
  live listener, with and without `orca-beta`, over 44 of the 72 core operations, and validates
  default responses against Anthropic's vendored schemas, directionally.
- **CI**: the drift gate above. On the committed tree the differ reports 352 differences and 46
  rules, none unmatched, unused, tied, miscounted or misfingerprinted.

## Alternatives

- **A hand-written matrix** (the first design) listed Orca-only operations as Claude endpoints and
  could not notice an upstream change. **One entry per row** rots within a release.
- **A one-way gate** lets dead entries accumulate and read as coverage; a register nobody can trust
  is worse than none, because it is believed. **Globs without counts, or rules without deltas**, let
  a rule approve a newcomer or the reverse of the delta its rationale justified.
- **An in-house schema comparator.** Its normalization list matched oasdiff's flags, and review had
  found two bugs in its traversal. **Strict error-code comparison** buries every other row.
- **Registering Fastify routes from the contracts** would make drift impossible but meant rewriting
  every route file, a behavior risk on each endpoint; the parity test detects drift instead.
- **Deriving the published API from Anthropic's spec** (a spike, withdrawn). The pinned document was
  the base; an overlay could remove operations and patch fields but never add, and Orca-only
  operations came only from extension files, so the boundary held by construction. Divergences no
  patch could express went to an `unencoded:` section naming their test. AJV checked bodies against
  the document as the integration suite ran, but zod still decided: giving the document the deciding
  vote broke six integration tests (unpublished semantic rules, legacy shapes, exact 400 wording).
  It was closed as an exploration, with no recorded reason for keeping the register. As they stand:
  - For the spike: the boundary cannot be mislabelled, every divergence is caused rather than
    observed, and its document compiled under AJV where the diffed one of the time failed 26 of 258
    schemas (the register's document now compiles every schema).
  - Against it: the published contract is Anthropic's document repaired (62 `nullable` without a
    `type`, 50 bare `const`, 859 unreachable components pruned), not a rendering of the code that
    serves; agreement with the server was sampled from test traffic, with route and auth parity
    unbuilt and the overlay covering only Agents; and each refresh changes what the engine
    publishes, where the register moves only the classification, the tag and the matrix.
  - The register's cost is that conformance is inferred, so its reach is what the differ compares:
    hence the coverage ledger, the oasdiff wrappers, the pins, and the tagging tripwire.

## Status notes

The proposal date is that of the first, hand-written matrix. Divergences from the register's first
design, each verified in current code:

- The differ first compared presence, status codes and required-parameter names only. The oasdiff
  axes, `response-media` (once the SSE routes declared `text/event-stream`), delta pins, union
  intersection, `covers`, the coverage ledger and `deltaFingerprint` came later in the same series,
  each after a check was shown to read as coverage without covering.
- Reference resolution came after retiring a design document left four rules, 30 operations,
  pointing at a missing file, and nothing failed.
- Six creates answering `201` (`fix-later`) and three `orca-beta`-dependent statuses (accepted
  deviations) now answer Anthropic's `200`, so `success-codes` has no rows and no rule. Six
  Orca-only operations in the first register, such as Files archive and a `PATCH` on memories, were
  retired with their rules; `route-contract-parity.spec.ts` pins that none returns.
- Only `extension-environment-work-stats` among `fix-later` rules has its alignment listed on
  [`roadmap.md`](../docs/managed-agents/roadmap.md#environment-work-stats-path-and-shape).
- As the tree stands, the nightly job cannot complete: Anthropic's `.stats.yml` no longer carries
  `openapi_spec_url`, so the sync stops at its first check; the job runs `openapi:gen` without
  `pnpm -r build`, though the contracts use workspace packages from `dist/`; and its report blames
  any `openapi:gen` failure on the tripwire. Until discovery changes, the pin does not move.
