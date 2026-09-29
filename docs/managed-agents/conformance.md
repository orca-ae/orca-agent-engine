# How conformance is measured

[`conformance-matrix.md`](./conformance-matrix.md) answers one question: **how
does our API differ from Anthropic's Managed Agents API?** This page explains how
that answer is produced, and why it is produced mechanically.

## Why mechanically

The question used to be answered by a human reading prose documentation, and it
was answered wrong: six Orca-only operations were published as Anthropic core
operations, and the mistake propagated into a design doc and a generated spec
before anyone noticed. Anthropic publishes a complete
machine-readable OpenAPI spec. Diffing against it makes that class of error
impossible — a route either appears in their spec or it does not.

## The pipeline

| Step                    | Command                | Produces                                                                   |
| ----------------------- | ---------------------- | -------------------------------------------------------------------------- |
| Vendor Anthropic's spec | `pnpm anthropic:sync`  | `services/registry-service-ts/vendor/anthropic/{openapi.json,PINNED.json}` |
| Publish ours            | `pnpm openapi:gen`     | `services/registry-service-ts/openapi/managed-agents.yaml`                 |
| Diff and register       | `pnpm conformance:gen` | `docs/managed-agents/conformance-matrix.md`                                |

The last two run in that order in CI, followed by `git diff --exit-code`. The
matrix must be generated from the spec that run just produced, or it describes a
surface that no longer exists.

### Vendoring

The spec URL is discovered from `openapi_spec_url` in `.stats.yml` in
`anthropic-sdk-typescript`. **Reading that file is URL discovery only.** It is
not an adoption of an SDK version, and nothing downstream may treat the SDK as
defining the API surface — the spec does.

`.stats.yml` also carries an `openapi_spec_hash`, which is **not** a checksum of
the served bytes: the Python SDK pins a different URL under the same value and
both serve byte-identical content. `PINNED.json` therefore records a sha256 we
compute ourselves, under a field name that says so.

`PINNED.json` carries no fetch timestamp. A timestamp would dirty the tree on
every run and defeat the drift gate; running the sync twice with no upstream
change is a byte-identical no-op. A nightly workflow re-runs it; when upstream
has moved and both generators below succeed, it opens a
`chore: sync anthropic spec` PR, so a spec change arrives as a reviewable diff
rather than silently.

That PR carries **both** generated artifacts, not just the matrix. Once the
`orca-extension` tag became computed from the vendored spec, `managed-agents.yaml`
joined the matrix downstream of the file the sync re-vendors: an upstream
reclassification moves the tag, so a PR that committed only the matrix arrived
already failing its own drift gate. The nightly runs the same two generators in
the same order as CI, and commits the same two paths.

Neither generator is allowed to half-succeed there. `openapi:gen` can fail on an
upstream change alone — the extension tripwire trips if Anthropic starts
publishing an operation we pinned as Orca-only — and `conformance:gen` fails when
a difference carries no decision. Both run with `continue-on-error` so one run
reports every problem, and no PR is opened when either fails: an automatic PR
that needs a human decision is worse than none.

### Normalization

Two facts about Anthropic's spec decide whether the diff means anything:

- Its path _keys_ carry a `?beta=true` suffix — a Stainless convention, not part
  of the request path. Every managed-agents operation is beta-only, so failing to
  strip the suffix reports the **entire** surface as absent.
- Path parameters are named for their resource (`{agent_id}`) where ours are
  often `{id}`. Names are cosmetic; every parameter is reduced to a positional
  placeholder and operations are compared on `(method, normalized path)`.

Where Anthropic publishes the same operation in both GA and beta form, the beta
variant wins: the managed-agents surface is beta, so that is the shape a client
of this API would be built against.

If **nothing** matches between the two specs, normalization is broken and the
generator throws rather than publishing a matrix that claims the whole surface is
simultaneously missing and an extension.

## Classification

| Class       | Meaning                                   |
| ----------- | ----------------------------------------- |
| `core`      | the operation exists in both specs        |
| `missing`   | Anthropic has the operation, we do not    |
| `extension` | we have the operation, Anthropic does not |

For `core` operations, five comparison groups run:

- **Success codes** are compared strictly. A create that answers `201` where
  Anthropic answers `200` is a client-visible divergence.
- **Success response media types** — the media types a `2xx` body is offered
  under — are compared as a set, without the status code, so a success-code
  difference is not reported a second time.
- **Required parameters** — required query/header parameters and required
  top-level request-body properties — are reported informationally. Path
  parameters are excluded: their names are cosmetic and their positions are
  already part of the operation key.

- **Wire schemas** — request bodies, success response bodies, header parameters,
  and query parameters as separate axes — are compared by
  [oasdiff](https://github.com/oasdiff/oasdiff), with `allOf`
  flattened and descriptions, examples and vendor extensions excluded, so what
  is reported is what a client can observe rather than how either document is
  written. Three things are ours rather than the tool's, and each exists because
  running it showed it was needed:
  - its output ordering is unstable between runs, so nothing consumes its bytes
    — we parse, canonicalize and render our own rows;
  - it matches responses by literal status code, so a success-code difference
    would report a code change and never compare the bodies. Success codes are
    aligned before diffing; any status difference is still reported separately
    under success codes;
  - it reports every leaf, thousands of them. Rows are aggregated to
    `(operation, axis)` and the leaves become the row's detail.

  Error _response shapes_ are out of scope: Anthropic enumerates the whole error
  taxonomy on nearly every operation, and that convention is already recorded
  once rather than per operation.

  A union body (`oneOf` / `anyOf`) contributes the **intersection** of what its
  branches require, not nothing. A property required on one branch is not
  required of the request, but one required on every branch is, and treating the
  whole union as unanalysable loses that. `POST /v1/sessions/{id}/resources` is
  the case that proves it: Anthropic publishes a single `file` branch requiring
  `file_id`, we publish three whose shared requirement is only `type`, and while
  unions were skipped the matrix reported no difference at all.

- **Error codes** are reported informationally. Anthropic enumerates fifteen
  error statuses (plus `200`) on Create Agent alone; comparing them strictly
  would drown every other signal.

### Error-code enumeration

Anthropic's spec lists the platform's whole error taxonomy on nearly every
operation, or a single `4XX` wildcard on the older ones. This service enumerates
only the statuses a given route actually returns. Neither convention is wrong and
neither is a wire difference a client can observe on a successful call, so these
rows are recorded once as an accepted deviation rather than chased per route.

### Orca-only operations

An `extension` row is an operation that exists here and not in Anthropic's spec.
That is not automatically a defect — several are deliberate, documented
surfaces — but it must never be published as though Anthropic defined it. See
[`orca-extensions.md`](./orca-extensions.md) for the surface-labelling rule.

Every `extension` row is also carried into the published spec as an
`orca-extension` tag on the operation, so a consumer generating a client can
filter Orca-only operations out without reading this matrix.
`scripts/orca-extension-tagging.mjs` computes it from the same classification and
pins it with a tripwire written independently of the differ; see
[`api-groups-and-extensions.md`](./api-groups-and-extensions.md#extension-tagging).

### Prose invariants — the one thing not derived

`services/registry-service-ts/anthropic-prose-invariants.yaml` is the single
hand-written input to this pipeline, and it is kept visibly separate for that
reason.

It exists because Anthropic documents requirements their spec does not encode.
Their OpenAPI declares `anthropic-beta` as an optional free-form string on every
operation — byte-for-byte what we declare — while their documentation requires
`managed-agents-2026-04-01`, requires `agent-memory-2026-07-22` on memory calls
instead, and rejects both together. The official client also supplies the
endpoint-specific Files and Skills values, and the managed-agents value on Vault
calls. We accept every value and ignore it. A pure spec diff therefore reports
conformance on behaviour that plainly differs, and silence there is not
neutrality: it is the matrix asserting something untrue.

The trade is deliberate and it has a real cost. A derived row re-checks itself
every time the vendored spec is synced; a transcribed one cannot. So every entry
must cite the page it came from, the citation is the authority rather than the
table, and the matrix renders these rows in their own section saying so. If a
requirement ever becomes expressible in Anthropic's spec, delete it here and let
the differ find it — a hand-written row that duplicates a derived one is worse
than either alone.

## The decision register

A matrix that only classifies decays into wallpaper. Every difference must carry
an explicit decision and a reference to where the reasoning lives.
`services/registry-service-ts/conformance-decisions.yaml` holds them.

Entries are keyed by **match rules**, not one per row — there are over a hundred
differences and per-row entries would rot. A rule matches either an exact
operation or a path glob within one difference class:

```yaml
- id: missing-deployments
  match: { class: missing, path: '/v1/deployment*' }
  covers: 10
  decision: accepted-deviation
  reference: docs/managed-agents/cron-triggers.md#decision
  rationale: >-
    Orca exposes cron scheduling through the Orca-owned /v1/triggers Core
    extension. It does not claim wire compatibility with Anthropic
    Deployments or publish a public DeploymentRun resource.
```

Exact `operation` matches beat globs, and among globs the longest pattern wins,
so one operation can be carved out of a family rule without reordering the file.
Two rules that tie are an error rather than a coin flip.

### Globs pin how much they cover

A family rule matches an open-ended set, which is how a register quietly stops
describing anything: a new difference lands in an existing family, the rule is
neither unmatched nor unused, and the both-ways gate stays green while nobody
has looked at the newcomer.

So every `path` glob declares `covers: <n>`, and the gate fails when the real
count moves:

```
conformance gate failed.

1 glob rule(s) no longer cover the number of differences they claim:
  - error-code-enumeration-convention: declares covers 72, matched 73
```

Updating the number is deliberately a manual step: it is the moment to confirm
the recorded rationale still applies to the difference that just joined. Exact
`operation` rules take no `covers` — they already name their single target.

### Schema rules pin what they cover

A stable row count cannot detect an existing schema row changing from one
incompatibility to another. Rules for request schemas, success schemas, header
parameters, and query parameters therefore also declare a
`deltaFingerprint`. It is the SHA-256 of the matched rows' normalized operation
keys plus their sorted `onlyAnthropic` and `onlyOrca` atoms.

The gate recomputes that aggregate on every run. If an atom changes while
`covers` stays constant, generation fails with both digests; updating the
fingerprint is the explicit review point for the new directional delta.

### Direction-sensitive classes

For `success-codes`, `required-params`, and `response-media`, naming the operation is not enough:
the same operation can differ in opposite directions at different times, and the
rationales are not interchangeable. "We answer `201` where they answer `200`"
does not justify the reverse, and "we accept a field they require" does not
justify rejecting input they consider valid.

Rules on those classes must therefore state the delta they approve, and stop
matching the moment it changes:

```yaml
- id: success-code-create-agent
  match:
    class: success-codes
    operation: 'POST /v1/agents'
    onlyAnthropic: ['200']
    onlyOrca: ['201']
```

Reverse that difference and the rule no longer applies; the difference lands in
`unmatched` and the build asks for a decision about what is true now. A rule that
genuinely covers any direction — an enumeration convention rather than a
judgement about one delta — says `anyDelta: true`, so breadth is a claim someone
made rather than something nobody wrote down.

### Vocabulary

| Decision             | Meaning                                         |
| -------------------- | ----------------------------------------------- |
| `keep`               | deliberate Orca behaviour, staying              |
| `not-implemented`    | Anthropic has it, we do not                     |
| `accepted-deviation` | documented, permanent, not a bug                |
| `fix-later`          | genuine divergence to correct in a follow-up PR |

### The gate cuts both ways

- A difference with **no matching decision** fails the build. An unrecorded
  divergence is the thing this whole apparatus exists to prevent.
- A decision matching **no difference** fails the build. Without that half, the
  file quietly accumulates dead entries that read as coverage — and a register
  nobody can trust is worse than no register, because it is believed.

So when a follow-up PR fixes a divergence, the build fails until its decision is
deleted. Fixing and de-registering are the same action.
