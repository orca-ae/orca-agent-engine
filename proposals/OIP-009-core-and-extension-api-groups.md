# OIP-009: Core and extension API groups

- *Author(s)*: @sijie
- *Status*: Released
- *Proposal time*: 2026-07-31
- *Components*: registry-service-ts (routing, discovery, OpenAPI documents)
- *Discussion*: None (predates the public repository)
- *Implementation*: under `services/registry-service-ts/`: `src/middleware/api-v1-alias.ts`,
  `src/api/discovery.routes.ts`, `src/contracts/{discovery,health}.contract.ts`,
  `src/contracts/index.ts` (`publicContract`), `src/auth/auth.ts`, `src/middleware/claude-edge.ts`,
  `src/server.ts`, `scripts/{generate-openapi,generate-observability-openapi,orca-extension-tagging}.mjs`,
  `conformance-decisions.yaml`, `openapi/{managed-agents,observability-admin}.yaml`;
  `.github/workflows/test-ts.yml`
- *Released in*: v0.5.0

## TL;DR

An Anthropic client works against the engine with only the host changed, but only while `/v1` keeps
Anthropic's paths and shapes and nothing Orca adds passes for Anthropic's API. The engine borrows the
Kubernetes URL model: core stays canonical at `/v1` (an `/api/v1` alias is rewritten before routing),
optional or independently evolving capabilities live in DNS-named, separately versioned groups under
`/apis/<group>/<version>`, authenticated `/api` and `/apis` routes say what a deployment serves, and
engine-owned OpenAPI documents tag every operation Anthropic does not publish, gated by CI against
the code. Client authors, distributions and operators exposing the public listener are affected.

## Background

Registry runs public, internal and admin listeners with separate credentials
([OIP-007](OIP-007-tenancy.md)). Routes are ts-rest contracts in `src/contracts/*.contract.ts`;
`publicContract` (`src/contracts/index.ts`) composes the public listener's. Whether an operation is
Anthropic's is computed against Anthropic's vendored spec, with a decision for every difference
([OIP-005](OIP-005-conformance-decision-register.md), [`conformance.md`](../docs/managed-agents/conformance.md)).
The reference is [`api-groups-and-extensions.md`](../docs/managed-agents/api-groups-and-extensions.md),
with [`orca-extensions.md`](../docs/managed-agents/orca-extensions.md) for each Orca-only surface.

## Motivation

The engine mirrors Anthropic's Managed Agents API so that the official SDKs, and a curl example
copied from Anthropic's documentation, work with only the host swapped. Four pressures act on that:

- **The engine serves what Anthropic does not**: guardrails, model prices, a harness catalog, cron
  triggers, Session output files, probes. Unlabelled on `/v1`, each reads as Anthropic's API to a
  generated client. It happened: six Orca-only operations were once published as Anthropic core
  operations, because "is this Anthropic's?" was answered by reading prose.
- **Capabilities evolve on different clocks.** Prices move when vendors reprice, policy when its
  model changes, core when Anthropic's API does. One version for all couples unrelated breakages.
- **Others build on the engine.** A distribution or third party adds its own surfaces; without
  namespaces, names collide and a reader cannot tell who owns a path.
- **A deployment's surface is undiscoverable.** Nothing in a URL, an SDK method or a docs page says
  which versions and groups a deployment serves, and a 404 cannot separate "not served here" from
  "wrong base URL". Clients generated from a contract the engine does not own address a
  `/v1/registry/...` prefix the engine has never served, and fail on every call.

## Goals

### In scope

- `/v1` unchanged for Anthropic clients, with Claude's error envelope on every client-facing failure.
- A home for everything else: namespaced by owner, versioned per group, never mistaken for core.
- One route tree however a path is spelled, so no security check learns a second spelling.
- Discovery that lets a client test availability without widening the unauthenticated surface.
- Engine-owned OpenAPI documents that label every Orca-only operation, computed rather than
  maintained, and gated against both the committed artifacts and the routes actually served.
- A rule for promoting an extension into core.

### Out of scope

- Operation-level discovery; discovery on the admin or internal listener; Kubernetes CRDs, whose
  group names are a separate namespace.
- Serving `/v1/registry/*` or any second path dialect. Moving clients off it, and the
  [Anthropic operations not implemented](../docs/managed-agents/roadmap.md#anthropic-surface-not-implemented),
  are listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#first-party-clients-on-v1).
- Each group's resources: [OIP-010](OIP-010-guardrails-pricing-and-spend.md) for policy and pricing,
  [`registry-service.md`](../docs/managed-agents/services/registry-service.md) for the harness catalog.

## Design

### High-level design

```text
 rewriteUrl (every listener): /api/v1 and /api/v1/... → /v1...; nothing else is rewritten
 one route tree on the public listener:
   /v1/*                       core: Anthropic operations plus tagged Orca Core extensions
   /api, /apis                 discovery                        ┐ driven by one table,
   /apis/<group>/<version>     APIResourceList per group        ┘ GROUP_VERSIONS
   /apis/<group>/<version>/*   group resources
   /healthz, /readyz           probes, open without a credential
 preHandler: authenticator → workspace-selector guard → idempotency (route-template scope)
 onSend:     any failure under /v1, /api or /apis → Claude error envelope
 CI: contracts ─► openapi:gen (managed-agents.yaml, tagged against vendor/anthropic; observability-
     admin.yaml) ─► conformance:gen (conformance-matrix.md) ─► git diff --exit-code
```

### Detailed design

**Placement.** Where a capability lives follows from who owns it and how it evolves:

- An operation Anthropic publishes is core: `/v1`, in Claude's shape.
- An engine-owned capability that is stable and served by every distribution may also live on `/v1`
  as an *Orca Core extension*, tagged `orca-extension`. `/v1/triggers` is the first such family: a
  cron-only Trigger control plane, not an alias for Anthropic's `/v1/deployments`, which stays listed
  as not implemented. Single Orca-only operations on `/v1`, such as Session output files and
  `DELETE /v1/agents/{id}`, are tagged the same way.
- Anything optional, deployment-specific or evolving on its own cadence lives in a named group.

**Core at `/v1`, alias by rewrite.** Kubernetes keeps its core at `/api/v1` for a historical reason;
ours is pinned by Anthropic serving `/v1/agents` and `/v1/sessions`. `/api/v1/*` exists so tooling
built around the Kubernetes shape can pair it with `/apis/<group>/<version>`. `rewriteApiV1Path`
(`src/middleware/api-v1-alias.ts`) rewrites exactly `/api/v1` and `/api/v1/...`, keeping the query
string; `/api` is a route, not a prefix to strip, and `/api/v2/...` or `/api/v1beta/...` 404 as
themselves. It is Fastify's `rewriteUrl`, the only hook that runs before routing, on every listener
`src/server.ts` builds, admin included, so "`/api/v1` means `/v1`" is one fact about a deployment. A
second route tree would have had to teach four consumers of `req.url` or the matched route about the
alias, and missing one is a security or correctness bug: `rejectExplicitWorkspaceSelector`
(`src/server.ts`), `isClaudeEnvelopePath` (`src/middleware/claude-edge.ts`), the idempotency scope
`METHOD route-template` (`src/middleware/idempotency.ts`, where a second template would let a
replayed `idempotency-key` create a second resource) and the authenticator's path checks
(`src/auth/auth.ts`). One tree also keeps each operation once in the spec, the matrix and the request
metrics, which label by matched route template (`src/observability/api-performance.ts`).

**Groups and naming.** A group name is DNS-scoped, and its domain says who ships the group, not who
hosts it: the engine's groups use `runorca.ai`, and a distribution or third party uses a domain it
controls, such as `/apis/widgets.example.com/v1/...`. Groups are versioned independently of core and
of each other, and split by who owns an API's evolution: guardrails change with the policy model,
prices when vendors reprice, and a deployment may use one without the other. The engine ships three,
present on every deployment (`GROUP_VERSIONS`, `src/api/discovery.routes.ts`):

| Group version | Resources (`namespaced`) | What it serves |
| --- | --- | --- |
| `runtime.runorca.ai/v1` | `harnesses` (false) | managed SDK harnesses whose `@orca/harness-catalog` entry declares a model policy, with provider, modes, capabilities and models |
| `policy.runorca.ai/v1` | `guardrails` (true), `guardrailtypes` (false) | guardrail CRUD and archive; the read-only type catalog |
| `pricing.runorca.ai/v1` | `modelprices` (false) | effective price reads; no write method is registered, so writes live on the admin listener |

**Discovery.** Kubernetes shapes, because tooling already reads them, with `snake_case` multi-word
keys like every other response. `GET /api` answers `{"kind": "APIVersions", "versions": ["v1"],
"preferred_version": "v1"}`, listing only versions actually routed (`CORE_VERSIONS`). `GET /apis`
answers an `APIGroupList` with each group's versions and preferred version, and
`GET /apis/<group>/<version>` an `APIResourceList`, whose `namespaced` states ownership, not URL
shape: the workspace comes from the credential and never appears in a path. One table drives the
group list and every resource list, so they cannot disagree, and resource lists are registered per
group version, not behind `/apis/:group/:version`, which would answer 200 for any invented name. A
distribution appends its groups to the same table beside their routes. The group list is the only
supported availability test, never a version string or the deployment kind; the CLI hides command
trees for groups `/apis` does not list ([`kubernetes.md`](../docs/managed-agents/kubernetes.md)).
Discovery lives on the public listener only; internal callers ship with Registry and have no boundary
to probe.

**Authentication.** Discovery and every group route go through the public listener's authenticator
(`buildAuth`, `src/auth/auth.ts`): a present `x-api-key` decides the request, and an OIDC bearer
token is consulted only when no key was presented. `UNAUTHENTICATED_PATHS`, pinned by a test, holds
`/healthz`, `/readyz`, and `/metrics`, which only the internal listener serves. A few routes, such as
the Git credential helper and the tunnel upgrades, skip this authenticator because their handlers
authenticate the caller themselves; no discovery or group route does. Membership is by exact path, so
a new `/apis/` route is authenticated because everything else is. Discovery describes the deployment,
so a caller without a key has no claim on it, while a probe's caller is a kubelet or an ingress that
cannot hold one. Kubernetes draws the same line: `system:public-info-viewer` (`/healthz`, `/livez`,
`/readyz`, `/version`) is bound to unauthenticated callers, `system:discovery` (`/api`, `/apis` and
below) to authenticated ones. A keyless caller still learns from the 401 that its base URL is right.
Group resources take their workspace from the credential: `rejectExplicitWorkspaceSelector` answers
400 to a top-level `workspace_id` or `workspaceId` under `/v1/` and `/apis/`, since stopping at `/v1/`
would let `POST /apis/policy.runorca.ai/v1/guardrails` name a workspace nobody authorized.

**Error envelope.** `registerClaudePublicEdge` (`src/middleware/claude-edge.ts`) turns any status of
400 or above under `/v1`, `/api` or `/apis`, matched on whole segments, into Claude's
`{type: "error", error: {type, message}, request_id}`, mapping the status to Claude's type (401
`authentication_error`, 404 `not_found_error`, 429 `rate_limit_error`, 503 `overloaded_error`, ...).
It keeps an envelope a handler already built (group handlers use `buildClaudeErrorResponse`) and
hides the exception message of an uncaught 5xx; it covers what handlers cannot, such as a failed
authentication or a route miss. `/api/v1/*` needs no entry because the rewrite ran first. The probes
are outside the list: an orchestrator reads the status code, and an `error.type` would dress a
liveness check up as an API call.

**The extension tag.** Every operation Anthropic does not publish carries `orca-extension` in
`openapi/managed-agents.yaml`, so a portable client can be generated without them. The tag is
computed: `services/registry-service-ts/scripts/orca-extension-tagging.mjs` runs the conformance differ against
`vendor/anthropic/openapi.json` and tags whatever classifies as `extension`. An independently written
tripwire pins it: `MUST_BE_EXTENSIONS` holds six operations (`DELETE /v1/agents/{id}`,
`GET /v1/sessions/{id}/outcome` and the four Session file operations), and `MUST_BE_CORE` holds
`GET /v1/skills/{id}/versions/{version}/content`, the Anthropic operation once wrongly published as
Orca-only. `pnpm openapi:gen` fails before writing when a pinned operation comes back wrongly tagged
or is no longer published (a deleted route satisfies "not tagged" for free), or when tagging moved
the operation set. The register gives `GET /api` and `GET /apis` exact-operation `keep` entries and
each engine group one rule scoped to its path with a pinned `covers` (policy 8, runtime 2, pricing
3), never a shared `/apis/*` that would cover groups nobody decided about.

**Engine-owned OpenAPI documents.** The engine publishes its own contract rather than implementing
one generated elsewhere.

- `openapi/managed-agents.yaml` renders `publicContract`, discovery and probes included, so it
  describes the whole public listener. Its default security is `apiKey` or `oidcBearer`, cleared with
  `security: []` only on the probes, and `anthropic-beta` is declared on every operation as an
  accepted, ignored string. `services/registry-service-ts/scripts/generate-openapi.mjs` throws rather than publish a `/internal/`
  path, `:param` syntax, templates differing only in parameter names, two operations that normalize
  to one, a schema that constrains nothing, or an invalid document; paths and schemas are sorted.
- `openapi/observability-admin.yaml` covers the ten admin-listener observability operations with
  separate organization and platform security schemes, outside `publicContract` and conformance
  because they are neither public nor Anthropic-shaped. It is stamped with its contracts' SHA-256
  digests, not a commit or timestamp. `pnpm openapi:gen` regenerates both documents.

**Drift gates.** The `test` job in `.github/workflows/test-ts.yml`, mirrored in `release-images.yml`,
runs `pnpm openapi:gen`, then `pnpm conformance:gen`, then `git diff --exit-code` over
`services/registry-service-ts/openapi` and the matrix: the artifacts match the contracts.
`test/unit/route-contract-parity.spec.ts` shows the contracts match the server, comparing the routes
the public app registered (`recordRouteTable`, read through `registeredRoutes` in `src/server.ts`)
with `publicContract` in both directions, apart from an exact list of transport routes with no
describable body. `nightly-anthropic-spec.yml` re-vendors Anthropic's spec, reruns both generators,
and opens a pull request carrying both artifacts when upstream moved, or none when either fails.

**Promotion.** A capability may start as an extension and become core, most often because Claude's
API adds it. For a group, core implements the Claude-compatible endpoint under `/v1`, the group path
remains as a deprecated alias for at least one release, and its discovery entry is marked deprecated
before removal. For an Orca-only operation already on `/v1` it is mechanical: once Anthropic publishes
it, the next spec refresh drops its tag (or trips the tripwire, if pinned), and its `extension` decision
matches nothing and fails the register until removed. The tier is a named group rather than an `x-`
or `-ext` marker because such markers outlive the experimental status they describe, which is why
RFC 6648 deprecated the `X-` convention.

## Changes by component

- **registry-service-ts**: the alias, discovery routes and contract, the runtime group route, the
  `/apis/` reach of the selector guard and the envelope, the allowlist, both OpenAPI generators, the
  tagging script, the discovery and group decisions, and the parity test. The policy and pricing
  routes are [OIP-010](OIP-010-guardrails-pricing-and-spend.md)'s.
- **Helm chart** (exposure only): `registry.ingress` forwards the whole public listener (path `/`,
  `Prefix`); `registry.istio.authorizationPolicy.allowedPaths` defaults to `/v1/*`, `/api`,
  `/api/v1/*`, `/apis` and `/apis/*`, and the gateway denies every other path.
- Other services and libraries: none.

## Public-facing changes

### API

| Route (public listener, credential required) | Behavior |
| --- | --- |
| `/api/v1/*` | served by the core route at `/v1/*`; responses and errors are identical |
| `GET /api` | `APIVersions` |
| `GET /apis` | `APIGroupList`: the three engine groups, plus any a distribution adds |
| `GET /apis/{group}/{version}` | `APIResourceList` for a served group version; anything else 404 |
| `GET /apis/runtime.runorca.ai/v1/harnesses` | the managed harness and model catalog, `{data: [...]}` |
| `/apis/{policy,pricing}.runorca.ai/v1/*` | the resources of [OIP-010](OIP-010-guardrails-pricing-and-spend.md) |

Failures on all of them use Claude's error envelope. Every operation here except the alias is
published, tagged `orca-extension` and has a `keep` decision; the alias adds no operation. No `/v1`
path, shape or status changes. `GET /healthz` and `GET /readyz` are published with `security: []`.

Events and streaming, wire protocols, storage, and metrics: none. An aliased request is recorded
under its canonical `/v1` route template.

### Configuration

No environment variable; the Helm defaults are those under *Changes by component*.

## Compatibility

### Upgrade

Additive for `/v1` clients. An operator who replaced the chart defaults with a `/v1`-only Ingress
path or gateway allowlist must widen it, or `/api`, `/apis` and the alias stop at the ingress and
never reach the rewrite. Clients built around `/v1/registry/*` move to `/v1` and `/apis` in a direct
cutover, listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#first-party-clients-on-v1), so no
deployment serves two path dialects.

### Rollback

This design persists nothing. A group's routes and its `/apis` entry ship in one build, so a release
without the group has neither; data a group wrote stays in the database
([OIP-010](OIP-010-guardrails-pricing-and-spend.md)).

### Version skew

Only Registry changes. Each replica answers `/apis` from its own build, so during a rolling upgrade
that adds a group, a replica not yet upgraded 404s a group another one advertises.

## Security considerations

- **Discovery needs a credential**, and so does any route a distribution adds under `/apis/`: the
  allowlist is exact-path membership, so authentication is the default, not something to remember.
- **One path per check**: the rewrite leaves one canonical URL for the authenticator, the selector
  guard (which covers `/apis/` as well as `/v1/`), the envelope and idempotency.
- **No invented surface**: no wildcard answers for an unserved group, and the parity test fails on
  any route no contract declares.
- **Listener separation**: discovery is absent from the admin and internal listeners, and price and
  organization-tier writes stay on the admin listener.

## Testing

- **Unit** (`services/registry-service-ts/test/unit/`): `api-v1-alias.spec.ts` (rewrite before
  routing, `/api` served, `/api/v2` not downgraded, one canonical URL for later hooks);
  `discovery-routes.spec.ts` (exact payloads, resource lists, invented groups 404, a Claude-shaped
  401, open probes, absent from the internal listener); `auth-allowlist.spec.ts` (the exact set);
  `claude-edge.spec.ts` (whole segments; `/api/v1` failures at 401, 404, 400, 403 and 500 through a
  real `rewriteUrl` instance; a 2xx left byte-identical); `server-surfaces.spec.ts` (the alias on
  every listener serving `/v1`, no discovery on admin); `route-contract-parity.spec.ts`;
  `openapi-document.spec.ts` (validity, the exact security overrides); `observability-openapi.spec.ts`.
- **Integration**: `test/integration/auth.spec.ts` serves discovery to a real key and refuses it
  without one. **End to end**: `guardrails-wire` and `model-prices-wire` (`packages/e2e-tests`) read
  `/apis` and a group's resource list on a running stack; the first checks that an invented group 404s.
- **Chart**: the render suite (`pnpm test:chart:render`, in the `test` job) asserts that the Ingress
  forwards the whole listener and pins the gateway's exact `notPaths`.

## Alternatives

- **Unauthenticated discovery** (an earlier revision), so a client could probe before holding a key.
  Reversed: the answer describes the deployment, and Kubernetes authenticates discovery too.
- **Core canonical at `/api/v1`.** Breaks the host swap; the alias gives Kubernetes-shaped tooling
  its symmetry anyway. **A second route tree** for the alias: four consumers to teach, and every
  operation twice in the spec and matrix.
- **A `/apis/:group/:version` wildcard** answers 200 for groups nobody serves.
- **`x-` or `-ext` markers** outlive the experimental status they describe (RFC 6648).
- **A single extension group** (the first design's start). Groups split by who owns an API's
  evolution; guardrails and prices are separate for that reason, not for being different resources.
- **One domain for every group.** The first design named its groups under one distribution's domain;
  a group's domain now tells a reader which project owns its evolution.
- **An engine answering `/apis` with an empty list** (the first design), leaving every Orca-only
  capability to distributions; superseded once guardrails, prices and the harness catalog shipped.
- **Serving `/v1/registry/*`, or keeping it one release as a deprecated union.** Either makes the
  engine implement a contract it does not own.
- **A hand-maintained extension list** goes stale silently the day Anthropic publishes an operation
  the engine already serves; **operation-level discovery** is a second copy of the spec.
- **An `/api*` or `/apis/*` glob in the register.** `*` is not a segment wildcard, so `/api*` also
  matched `/apis/<group>/<version>/*`; the first group would have inherited discovery's decision.
- **Admin operations in the public document** would put admin-only, differently authenticated
  operations into Anthropic conformance.
- **A prefix list at the Ingress** is a second hand-maintained route table; a `/v1`-only list hid
  discovery, groups and the alias until the chart forwarded the whole listener.

## Status notes

Divergences from the first design, each verified in current code:

- The engine ships groups of its own, and `/v1` admits Orca Core extensions. The first design
  answered `/apis` with an empty list and reserved `/v1` for Anthropic's operations; guardrails,
  prices and the harness catalog brought the three groups, and `/v1/triggers` the tagged-core rule.
- Discovery became authenticated in review, and its decisions became exact-operation entries.
- The deprecated `/v1/registry/*` union was dropped; the engine has never served that prefix.
- The alias reached the admin listener; discovery and the probes joined the contract, so the parity
  test exempts neither; a second document covers the admin observability operations.
- The chart's Ingress forwarded only `/v1`, and its gateway policy allowed only `/v1/*`, hiding
  discovery, groups and the alias until both were widened.

No group has been promoted, so that path is unexercised; `ApiGroupList` has no deprecation field.
