# API groups and extensions

How this API is addressed, how a client discovers which **API versions and
extension groups** a deployment serves, and how an operation Anthropic does not
publish is labelled so a generated client can filter it.

Discovery here is group/version discovery, deliberately. `/api` names the core
API versions; `/apis` names the extension groups and their versions. Neither
enumerates operations — for that, read the published spec, where every Orca-only
operation carries the `orca-extension` tag.

## The problem this solves

A client generated from a contract the engine does not own addresses paths the
engine does not serve: a client built around a `/v1/registry/...` prefix 404s on
every call against this service, which serves `/v1/...`. The cause is contract
ownership, not a path typo, and the answer is that this repository owns its
contract — see [Deprecating `/v1/registry/*`](#deprecating-v1registry).

The second symptom is that the boundary is undocumentable. Nothing in a URL, an
SDK method name, or a docs page tells you which API versions and extension groups
a self-hosted deployment serves. A client finds out by 404, and a 404 cannot
distinguish "this deployment does not serve that" from "you have the base URL
wrong".

## The URL model

Kubernetes solved this exact shape — one core group plus a versioned tree of
deployment-specific groups — so the model is borrowed rather than invented:

| Path                        | What it is                                                                                            |
| --------------------------- | ----------------------------------------------------------------------------------------------------- |
| `/v1/*`                     | **Core API.** Canonical; Anthropic-compatible operations plus explicitly tagged Orca Core extensions. |
| `/api/v1/*`                 | Alias of the core API, rewritten before routing.                                                      |
| `/api`                      | Discovery: the core API versions this deployment serves.                                              |
| `/apis`                     | Discovery: the extension groups this deployment serves.                                               |
| `/apis/<group>/<version>`   | Discovery: the resources in one group.                                                                |
| `/apis/<group>/<version>/*` | Extension groups — shipped by the engine, or by the distribution built on it.                         |

### Why core is canonical at `/v1`, not `/api/v1`

Kubernetes' core group lives at `/api/v1` for a legacy reason it cannot escape.
Ours is pinned by the contract it mirrors: Anthropic serves `/v1/messages`,
`/v1/agents`, `/v1/sessions`. Canonical `/v1` is what makes a Claude curl example
work against Orca with only the host swapped, and that property is the entire
point of the compatibility work.

`/v1` is not an Anthropic-owned allowlist. An Orca capability that is stable,
available in every distribution, and owned by this engine may live there as an
Orca Core extension. Such an operation is mechanically tagged
`orca-extension`; `/v1/triggers` is the first resource family using this rule.
Deployment-specific or optional capabilities still belong under
`/apis/<group>/<version>`.

The `/api/v1` alias exists so tooling built around the Kubernetes shape can pair
`/api/v1` with `/apis/<group>/<version>` symmetrically. It is an alias, not a
second surface: there is one route tree, and the alias is resolved by
`rewriteUrl` — the only Fastify hook that runs _before_ routing — in
`src/middleware/api-v1-alias.ts`.

Rewriting rather than mounting a second route tree is the load-bearing choice,
because four things downstream key off `req.url` or the matched route:

| Consumer                                                    | What a second mount would have done                                                                                            |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `rejectExplicitWorkspaceSelector` (`src/server.ts`)         | Only inspects paths under `/v1/`. `POST /api/v1/agents` could then carry a `workspace_id` the server never authorized.         |
| `isClaudeEnvelopePath` (`src/middleware/claude-edge.ts`)    | The alias would answer failures in a non-Claude shape.                                                                         |
| The idempotency scope key (`src/middleware/idempotency.ts`) | One logical write split across two scopes, so the same `idempotency-key` replayed against the alias creates a second resource. |
| The unauthenticated allowlist (`src/auth/auth.ts`)          | Two path lists to keep in step.                                                                                                |

A second mount would have to teach all four about the alias, and forgetting any
one of them is a security or correctness bug rather than a cosmetic gap. A
rewrite leaves exactly one canonical path and touches none of them — asserted in
`test/unit/api-v1-alias.spec.ts`. It also avoids doubling the surface a reader
must keep consistent, and avoids each operation appearing twice in the
conformance matrix.

Every listener that serves `/v1` carries the same rewrite, including the admin
control plane: "`/api/v1` means `/v1`" is one fact about the deployment, not a
per-listener quirk a caller has to memorise. Discovery, by contrast, is
public-listener only — the admin listener is a control plane, not the surface a
client probes for API versions and groups.

Two behaviours the alias deliberately does **not** have:

- `/api` is a discovery route, not a prefix to strip. Rewriting it would answer
  the core-version probe with whatever `/` routes to.
- `/api/v2/...` and `/api/v1beta/...` are not downgraded to `/v1`. A version this
  deployment does not serve must 404 as itself.

## Group naming

Groups are DNS-scoped and versioned independently of core and of each other, so
a breaking change in one extension never forces a version bump on another or on
the core API.

**The domain identifies who ships the group, not who hosts it.** Groups shipped
by the open-source engine use `runorca.ai`; a distribution or third party built
on the engine ships its groups under a domain it controls. Both satisfy the
Kubernetes requirement that a group name use a domain the project controls, and
the split means a reader can tell from a group name which project owns its
evolution.

Reserved groups:

| Group | Ships in | Resources |
| ----- | -------- | --------- |
| `runtime.runorca.ai` | this repository | `harnesses` |
| `policy.runorca.ai` | this repository | `guardrails`, `guardrailtypes` |
| `pricing.runorca.ai` | this repository | `modelprices` |

A distribution's or third party's extensions use a domain it controls — for
example `/apis/cloud.example.com/v1/...` or `/apis/connectors.example.com/v1/...`.

Groups split **by who owns an API's evolution**. Guardrails and model prices are
separate groups for that reason and not merely because they are different
resources: prices change when vendors reprice, guardrails change when the policy
model changes, and a deployment may care about one without the other. The same
test applies to any further split.

HTTP API group names and Kubernetes CRD group names are deliberately separate
namespaces, not two spellings of one thing.

## Discovery

Both routes **require a credential**, like every other route on this listener.
They are not on the unauthenticated allowlist in `src/auth/auth.ts`, which holds
the two health probes and `/metrics` — and the public listener does not serve
`/metrics` (metrics are served on the internal listener).

The tempting argument is that discovery has to be askable before you hold a key.
It does not survive contact with what the routes actually answer: what this
deployment serves. That is a statement about the deployment, and a caller who
cannot present a key has no claim on it. The probes differ _in kind_, not in
degree — their callers are a kubelet, a compose healthcheck, an ingress, none of
which can hold a credential, and "this process is up" says nothing about the API
surface.

Kubernetes, whose URL model this borrows, draws the line in exactly that place:
`system:public-info-viewer` (`/healthz`, `/livez`, `/readyz`, `/version`) is
bound to `system:unauthenticated`, while `system:discovery` (`/api`, `/api/*`,
`/apis`, `/apis/*`) is bound to `system:authenticated` only. Borrowing the shape
without the boundary would have been half the model.

Nothing the routes exist for is lost. The ambiguity they resolve — a 404 that
cannot separate "this deployment does not serve that" from "you have the base URL
wrong" — belongs to a client that is about to call the API, which is to say a
client that holds a key. And for one that does not, a 401 is itself informative
in a way a 404 is not: the base URL is right, the credential is missing. The 401
arrives in the Claude envelope, because `/api` and `/apis` are on its prefix list.

The default now falls the safe way. With discovery off the allowlist, the
unauthenticated surface is exactly the two probes, so a deployment adding
`/apis/<group>/<version>/*` gets an authenticated group because everything is
authenticated — not because whoever added it remembered to say so.

### `GET /api`

```json
{
  "kind": "APIVersions",
  "versions": ["v1"],
  "preferred_version": "v1"
}
```

The core API versions this deployment serves. Each `<version>` is served at
`/<version>` — canonical — with `/api/<version>` accepted as an alias.

### `GET /apis`

The extension groups this deployment serves. The engine ships three —
`runtime.runorca.ai` (the managed harness catalog, below), and
`policy.runorca.ai` and `pricing.runorca.ai`, defined by
[`guardrails.md`](./guardrails.md) and [`pricing.md`](./pricing.md) — so every
deployment answers:

```json
{
  "kind": "APIGroupList",
  "groups": [
    {
      "name": "runtime.runorca.ai",
      "versions": [{ "group_version": "runtime.runorca.ai/v1", "version": "v1" }],
      "preferred_version": { "group_version": "runtime.runorca.ai/v1", "version": "v1" }
    },
    {
      "name": "policy.runorca.ai",
      "versions": [{ "group_version": "policy.runorca.ai/v1", "version": "v1" }],
      "preferred_version": { "group_version": "policy.runorca.ai/v1", "version": "v1" }
    },
    {
      "name": "pricing.runorca.ai",
      "versions": [{ "group_version": "pricing.runorca.ai/v1", "version": "v1" }],
      "preferred_version": { "group_version": "pricing.runorca.ai/v1", "version": "v1" }
    }
  ]
}
```

A distribution built on the engine answers the same route with its own groups
appended. **The list, not a 404, is the point of the route.** It is the
difference between "this deployment does not serve that group" and "this client
cannot tell", and it lets a first-party client degrade honestly instead of
404ing its way to a conclusion. A client written against one answer works
against the others unchanged, which is why the group list — not a version
string, not the deployment kind — is the only supported way to test whether a
group is available.

`GET /apis/<group>/<version>` then lists that group's resources as an
`APIResourceList`, so a client can enumerate a group it just discovered. One
table in `src/api/discovery.routes.ts` drives both routes, so the group list and
the resource lists cannot disagree, and a group missing from it 404s:

```json
{
  "kind": "APIResourceList",
  "group_version": "policy.runorca.ai/v1",
  "resources": [
    { "name": "guardrails", "namespaced": true, "kind": "Guardrail" },
    { "name": "guardrailtypes", "namespaced": false, "kind": "GuardrailType" }
  ]
}
```

The structure follows Kubernetes (`APIVersions` / `APIGroupList`) because that is
the shape tooling already understands; multi-word keys are `snake_case` to match
every other response this API serves. `namespaced` is `true` only on
`guardrails`, whose rows belong to a workspace or its organization; workspace
scoping rides the credential, and no route carries a namespace path segment.
These are Orca extensions — Anthropic
publishes no discovery surface — and are tagged as such in the generated spec.

### <a id="probes"></a>The health probes

`GET /healthz` and `GET /readyz` are published in
`openapi/managed-agents.yaml` for the same reason: they are served on the public
listener, reachable without credentials, and already depended on by the compose
healthchecks and the Helm charts. A route a client can call and a document that
omits it is exactly the undocumented boundary this work exists to remove. They
classify as Orca extensions. Each operation declares `security: []` to clear the
document's default API-key/OIDC requirement, matching the public auth allowlist.

They are the one public-listener surface **not** wrapped in the Claude error
envelope — see below.

## The Claude error envelope past `/v1`

`src/middleware/claude-edge.ts` turns a failure into Claude's nested envelope
(`type`, `error.type`, `error.message`, `request_id`) rather than a bare
`{"error": ...}`. Its prefix list covers the core API and the API-group tree:

```
/v1    /api    /apis
```

Matched on whole path segments, so `/apis` is the group tree rather than a suffix
of `/api`, and `/v1x` is neither.

`/api/v1/*` needs no entry of its own — `rewriteUrl` has rewritten it to `/v1/*`
before any hook runs. That is a claim about framework ordering, and this repo has
been bitten by assuming those, so `test/unit/claude-edge.spec.ts` asserts it
against a real `Fastify({ rewriteUrl: rewriteApiV1Alias })` instance across the
failure classes a client can hit: 401, 404, 400, 403 and 500, plus a negative
control that a 2xx discovery response is left byte-identical.

`/healthz` and `/readyz` are deliberately excluded. An orchestrator reads the
status code and nothing else; giving a liveness check an `error.type` and a
`request_id` would dress it up as an API call.

## <a id="extension-tagging"></a>Extension tagging

Every operation Anthropic does not publish carries the `orca-extension` tag in
`openapi/managed-agents.yaml`:

```yaml
/v1/sessions/{id}/outcome:
  get:
    tags:
      - outcomes
      - orca-extension
```

A consumer generating a client that must stay portable across Anthropic and Orca
can exclude that tag at generation time. It is the machine-readable form of the
boundary that [`conformance-matrix.md`](./conformance-matrix.md) states in prose.

**The tag is computed, never hand-maintained.** `scripts/orca-extension-tagging.mjs`
runs the same differ that produces the conformance matrix
(`scripts/conformance-core.mjs`) against the vendored Anthropic spec, and tags
whatever comes back classified `extension`. A hand-maintained list would go stale
the moment Anthropic published an operation we already serve — and it would go
stale _silently_, because nothing about a stale list looks different from a
correct one.

What can still go wrong is the computation: a normalization change, a malformed
sync, a differ regression. So the computed tag is pinned by a tripwire written
independently of it — hardcoded literals from the audit against
`vendor/anthropic/openapi.json`, not derived from the diff:

| Constant             | Contents                                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------------------- |
| `MUST_BE_EXTENSIONS` | `DELETE /v1/agents/{id}`, `GET /v1/sessions/{id}/outcome`, and the four `/v1/sessions/{id}/files*` operations |
| `MUST_BE_CORE`       | `GET /v1/skills/{id}/versions/{version}/content`                                                              |

The check fails `pnpm openapi:gen` in three directions, before anything is
written:

- an operation in `MUST_BE_EXTENSIONS` that came back untagged;
- an operation in `MUST_BE_CORE` that came back tagged;
- an operation named in either list that this API no longer publishes at all —
  the quiet one, because a deleted route satisfies "is not tagged" for free and
  would let `MUST_BE_CORE` pass by vacuum.

Two differently-sourced sides is what makes the check real. A tag derived from
the diff and then checked against the diff proves nothing.

Tagging is a **labelling** pass: paths, methods, parameters, bodies and responses
are untouched, and the generator refuses to write if the published operation set
moved.

### The framing that must not return

An earlier draft described seven operations as "core-parity gaps".
**Six of those seven are Orca extensions Anthropic does not have.**
Exactly one — `GET /v1/skills/{id}/versions/{version}/content` — is genuinely
Anthropic's, and publishing it as an Orca extension is the specific error that
propagated into a generated spec before anyone caught it.
That error is why the conformance framework exists, and why `MUST_BE_CORE` pins
that one operation in the opposite direction from everything else.

## Promotion path

A capability may start as an extension and later become core — most often
because Claude's API adds it. No extension group has been promoted to core, and
discovery has no deprecation marker: each `GET /apis` entry carries only the
group's name, versions and preferred version. The designed promotion procedure
is in [`roadmap.md`](./roadmap.md#designed-not-built).

Extension paths are therefore not a permanent statement about a capability. This
is the reason the tier is expressed as a *named group* rather than an `-ext` or
`x-` marker: RFC 6648 deprecated the `X-` header convention precisely because
such markers outlive the experimental status they describe.

**Cron is the live case — and it has since landed.** Claude's cron scheduling
*is* core API — `POST /v1/deployments` with
`schedule: {type: "cron", expression, timezone}`, plus `/v1/deployment_runs`
for run history. This model said a cron schedule belongs in the open-source
engine, and the engine now ships it: `/v1/triggers` is the cron-only durable
Trigger control plane, tagged as an Orca Core extension — the first resource
family to use the Orca-Core-on-`/v1` rule above. Kafka and Pulsar trigger
sources are not part of the engine, as the test predicts: they depend on
workspace `connections`, which the engine does not serve.

## Deprecating `/v1/registry/*`

`/v1/registry/*` is a path prefix this engine does not serve. Serving it would
make the engine implement a contract generated somewhere else rather than own
its own.

**This repo owns the contract.** `openapi/managed-agents.yaml` is generated from
`src/contracts/*.contract.ts` and gated in CI; it is the artifact first-party
clients are generated from. Core operations live under `/v1/*`. Anything a
distribution serves that the engine does not belongs in an extension group under
`/apis/<group>/<version>/*` — which is what the group tree is for, and what makes
the difference visible in a URL rather than only in a 404. Such a group never
appears in this engine's spec, so it never appears in the conformance matrix.

A client pointed at a deployment detects what it serves rather than guessing:
`GET /apis` names the groups that deployment serves, and `GET /api` names the
core version and its path. A group the deployment does not serve simply does
not appear. Moving clients still built around `/v1/registry/*` onto `/v1` is
tracked in [`roadmap.md`](./roadmap.md#first-party-clients-on-v1).

## Where this is implemented

| Concern                        | File                                                                                                      |
| ------------------------------ | --------------------------------------------------------------------------------------------------------- |
| `/api/v1` alias                | `services/registry-service-ts/src/middleware/api-v1-alias.ts`                                             |
| Discovery routes               | `services/registry-service-ts/src/api/discovery.routes.ts`                                                |
| Discovery contract             | `services/registry-service-ts/src/contracts/discovery.contract.ts`                                        |
| `policy.runorca.ai/v1` routes  | `services/registry-service-ts/src/api/guardrails.routes.ts`                                               |
| `pricing.runorca.ai/v1` routes | `services/registry-service-ts/src/api/model-prices.routes.ts`                                             |
| Probe contract                 | `services/registry-service-ts/src/contracts/health.contract.ts`                                           |
| Unauthenticated allowlist      | `services/registry-service-ts/src/auth/auth.ts`                                                           |
| Claude error envelope          | `services/registry-service-ts/src/middleware/claude-edge.ts`                                              |
| Extension tagging + tripwire   | `services/registry-service-ts/scripts/orca-extension-tagging.mjs`                                         |
| Contract/route drift test      | `services/registry-service-ts/test/unit/route-contract-parity.spec.ts`                                    |

A contract and its routes must move together. The drift test builds the real
public app, collects the route table through the `recordRouteTable` hook and
`registeredRoutes` reader in `src/server.ts`, and asserts it equals the
operation set of `publicContract` apart from a pinned list of transport routes
that carry no contract surface — so a contract entry with no route, or a route
with no contract entry, fails the build. It found one on its first run: an undeclared PATCH alias had been served
for months without appearing in any spec or any conformance row.
`discovery-routes.spec.ts` checks the discovery corner of the same comparison.


`runtime.runorca.ai/v1` serves `GET /apis/runtime.runorca.ai/v1/harnesses`, the
managed SDK harness/model/capability catalog, including `claude_agent_sdk`,
`claude_agent_sdk_persistent`, and `codex_sdk`. Entries come from the shared
capability catalog's managed model policies. Clients use the returned IDs in
Agent metadata and model selection; this endpoint does not grant provider
credentials or model access.
