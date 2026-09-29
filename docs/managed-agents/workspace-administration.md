# Workspace administration

This document defines two management-plane roles: a deployment-wide Platform
Admin that provisions organizations and their initial workspaces, and an
organization-scoped admin that manages workspaces and workspace API keys. Both
are distinct from the workspace-scoped Managed Agents data plane and from the
mesh-internal runtime API.

## Trust hierarchy

```text
one-time installation bootstrap
  -> platform admin identity or API key
    -> organization
      -> organization admin identity or API key
        -> workspace API key
```

Platform Admin is a separate, deliberately small authority. Its principal has
no `organization_id` and can call only the explicit `/v1/platform/*`
provisioning routes. An organization admin always has exactly one
`organization_id`; it cannot select another organization in a request path,
query, or body. Platform credentials cannot call organization-admin or
workspace data routes, and organization credentials cannot call Platform API
routes.

The three Registry surfaces are deliberately separate:

```text
public   :8080  /v1/agents, /v1/sessions, ...     workspace auth
internal :8081  /internal/v1/...                  service workload auth
admin    :8082  /v1/organizations/...             organization admin auth
admin    :8082  /v1/platform/...                  platform admin auth
```

An admin credential is rejected by the public data-plane authenticator. A
workspace credential is rejected by both admin authenticators. Harness and AI
Gateway never receive a platform/admin credential or a workspace API key.

## Authentication

Organization routes on the admin listener accept either:

- an `orca_admin_...` API key stored in `admin_api_keys`; or
- an OIDC bearer token from the separately configured admin issuer/audience,
  with `org:admin` scope and an `organization_id`/`orca_organization` claim.

OIDC authorization is intentionally all-or-nothing: only `org:admin` is
accepted. Use admin API keys for narrower `observability:read` /
`observability:write` / `observability:rotate`, `workspaces:*` or `api_keys:*`
delegation; scoped OIDC administrator roles are not implemented. Guardrail and
model-price administration has no narrower delegation and takes `org:admin`
(see below).

Admin keys use a fingerprint domain distinct from workspace keys and retain
only a SHA-256 lookup fingerprint, an Argon2id verifier and a partial display
hint. Multiple admin keys may coexist for rotation. Admin keys cannot call the
workspace data plane, mint other admin keys or change OIDC/RBAC configuration.

The first organization, workspace and admin key are created with the offline
`registry:bootstrap-admin` command. Database access is the bootstrap authority;
there is no permanent bootstrap secret in the Registry process. The command is
one-shot and refuses to run after an organization exists. It also creates a
separate `orca_platform_...` Platform API key. The Platform key is not stored
in `admin_api_keys` and has no organization binding.
`ORCA_BOOTSTRAP_ORGANIZATION_AUDIENCE` optionally gives that first organization
an OIDC audience, which is the only way it can acquire one: an organization's
audience is set at creation and no API creates the bootstrap organization. See
[Organization-audience workspace resolution](#organization-audience-workspace-resolution).

Admin-key rotation uses the separate offline `registry:create-admin-key`
command with `ORCA_ADMIN_ORGANIZATION_ID`. It can add a new key to an existing
active organization and, when `ORCA_ARCHIVE_ADMIN_KEY_ID` is supplied,
atomically archive the old key after inserting its replacement.
`ORCA_ADMIN_KEY_SCOPES` optionally accepts a comma-separated subset of
`org:admin`, `observability:read`, `observability:write`, `observability:rotate`,
`workspaces:read`, `workspaces:write`, `api_keys:read`, and `api_keys:write`; any
other scope fails the command. When it is unset, the new key receives
`org:admin`. The organization-scoped guardrail and model-price routes described
in `guardrails.md` and `pricing.md` check `guardrails:*` and `model_prices:*`
scopes that neither admin-key command mints, so they take an `org:admin` key or
OIDC token. The one-time bootstrap key always receives `org:admin`.

Platform routes accept either:

- an `orca_platform_...` API key stored in `platform_api_keys`, with the sole
  `platform:admin` scope; or
- an OIDC bearer token from the independently configured
  `PLATFORM_OIDC_ALLOWED_ISSUERS` / `PLATFORM_OIDC_AUDIENCE`, with
  `platform:admin` scope. No organization claim is required or copied into the
  resulting principal.

Platform-key rotation uses the offline `registry:create-platform-key` command.
It can atomically create a replacement and archive the key named by
`ORCA_ARCHIVE_PLATFORM_KEY_ID`. Platform and organization key prefixes,
fingerprint domains, tables, authenticators, and OIDC audiences are distinct.
An installation upgrading from a schema that predates Platform Admin runs this
command once without an archive ID to establish its first Platform key.

### OIDC claim sources

Identity claims are read from the top level of the verified token first. Some
issuers cannot mint arbitrary top-level claims and instead nest the values
supplied at provisioning time under a single top-level `metadata` object whose
values are strings. When — and only when — the top-level claim is absent, each
plane falls back to `metadata`.

The `metadata` fallback is disabled by default and is enabled per plane with
`OIDC_METADATA_CLAIMS` / `ADMIN_OIDC_METADATA_CLAIMS` /
`PLATFORM_OIDC_METADATA_CLAIMS`. Enable it only where the token's `metadata`
object is controlled by the issuer itself. Where `metadata` is supplied by
whoever requests the credential, enabling this plane's fallback makes the
ability to create a credential equivalent to the ability to mint any identity
that plane accepts — including `platform:admin` on the platform plane.

A plane that enables the fallback must name exactly one issuer in its
`*_OIDC_ALLOWED_ISSUERS`; otherwise the registry refuses to start and prints the
variable that has to change. The opt-in is one setting for the whole plane, so
with several issuers it does not say "trust this issuer's `metadata`" — it
trusts the `metadata` of every issuer the plane allows, including any that fills
it in from whoever requested the credential. Note what this does not cover:
allowing several issuers on one plane is a plane-wide trust decision to begin
with, and any allowed issuer can already claim any workspace, organization or
scope through the top-level claims; this check only closes the `metadata` route.
A per-issuer opt-in is listed in
[`roadmap.md`](./roadmap.md#known-limitations).

| Plane     | Top-level claims                            | `metadata` fallback          |
| --------- | ------------------------------------------- | ---------------------------- |
| Workspace | `workspace_id`, then `orca_workspace`       | `metadata.orca_workspace`    |
| Admin     | `organization_id`, then `orca_organization` | `metadata.orca_organization` |
| All       | `scopes` (array), then `scope`              | `metadata.orca_scopes`       |

`scopes` is an array of strings. `scope` is either a space-separated string or
an array of strings, since issuers differ on which shape they mint.
`metadata.orca_scopes` is a space-separated string, e.g. `org:admin` or
`workspaces:read api_keys:write`.

Each claim is read in that order and the first one that grants a scope wins
outright — a token that already carries scopes keeps exactly those scopes, so
`metadata` can never widen a grant. A claim that is present but says nothing
(absent, `null`, an empty array, or a blank `scope` string) falls through to the
next source. This matters for issuers that always emit an empty `scope` array,
which carries no scope and so falls through to `metadata`.

A claim that is present but malformed grants nothing and blocks the metadata
fallback: the token ends up with no scopes at all, and `metadata.orca_scopes` is
not consulted. Malformed means a shape carrying no readable scope — a number,
boolean or object in either claim, a plain string in the array-only `scopes`
claim, or a populated array without a single non-empty string in it (`[42]`,
`[""]`). An array holding a mix grants the strings it does contain. Each
distinct malformed shape is logged once per process; the claim value itself is
never logged.

### OIDC token revocation

`OIDC_DENIED_JTI_FILE` optionally names a JSON file of revoked token
identifiers. When set, a token whose `jti` claim appears in the file is
rejected on all three OIDC planes, after its signature, issuer and audience
have been verified. Leaving the variable unset disables the check.

The file is a bare JSON array in either of two shapes:

```json
["<jti>", "<jti>"]
```

```json
[{ "key": "<jti>", "exp": 1764000000 }]
```

`null` is read as an empty list, since a producer marshalling an empty list may
emit `null` rather than `[]`. `exp` is ignored: the file is the authority on
what is revoked, and entries are never aged out against the Registry's own
clock. Tokens carrying no `jti` cannot be named by the list and are therefore
not deniable; they remain subject to every other check.

The file is re-read at most every few seconds, so revocations take effect
without a restart. If it is missing, unreadable or malformed, the Registry
keeps serving the last list that loaded successfully and logs a warning on
every retry; if no load has ever succeeded it denies nothing and logs loudly.
This fails open deliberately — a typo in the path would otherwise reject every
OIDC token on the workspace, admin and platform planes at once, leaving no
authenticated route to repair the configuration. Alerting should page on these
warnings, because while they persist newly revoked tokens keep working.

A document the Registry reads in full is authoritative about the whole list and
replaces it wholesale, so both additions and removals take effect on the very
next reload. A document that parses but contains some entries in neither shape
above is authoritative only about what it adds: its readable entries are added
to the list already being enforced, the unrecognised ones are skipped, and a
warning names how many were dropped (never their values). Removals are not
applied from such a document — an entry the Registry could not read is never
evidence that a revocation was withdrawn — so an entry deleted from the file
keeps being denied until the file parses cleanly again, at which point the
clean document replaces the list wholesale and every held removal lands at
once. Revocations therefore always take effect on the next reload, even while
the file is dirty; only un-revocations wait. This behavior assumes the producer
never reuses a `jti`.

### Organization-audience workspace resolution

The workspace plane normally expects one credential per workspace: the token
names its workspace in a claim, and `OIDC_AUDIENCE` is one static value every
token on the plane carries. Some issuers cannot mint a per-workspace credential
at all — what they can scope is the audience. `OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE`
turns on a second mode for that case, on the workspace plane only. It is off by
default and the default is byte-identical to the behaviour described above.

With it on, an organization may be created with an `audience` (see the Platform
API below). A credential is then issued per organization: the issuer sets `aud`
to that organization's audience and needs to know nothing about workspace IDs.
Registry resolves the workspace itself:

1. If the token carries a workspace claim, it is read exactly as always —
   `workspace_id`, then `orca_workspace`, then `metadata.orca_workspace` where
   that plane has opted into the `metadata` fallback.
2. Otherwise the organization whose configured audience the token names is
   looked up, and its single active workspace is the principal's.

**The binding rule.** However the workspace was reached, the organization that
owns it must have an audience configured and the token's `aud` must contain it.
This one rule is what keeps one organization's credential out of another's
workspace: an issuer can put any workspace ID it likes in a claim, but a token
carrying the wrong `aud` resolves a foreign workspace and is then refused. That
holds only as far as `aud` is beyond the requester's reach, which is an
operator's responsibility rather than a check Registry can make — see
**Operator requirements** below. It follows that an organization created without
an audience authenticates nothing in this mode, including through a claim that
names its workspace directly.

Audience validation moves rather than disappears. `jwtVerify` runs with issuer
and JWKS but without the static `OIDC_AUDIENCE` check, because there is no
longer one value every token carries; the binding rule above replaces it.

**`OIDC_AUDIENCE` and the flag are one setting with two states.** Registry
refuses to start on either mismatch, and the chart refuses to render it:

| `OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE` | `OIDC_AUDIENCE` | Result                 |
| ------------------------------------ | --------------- | ---------------------- |
| `true`                               | empty           | resolution mode        |
| `true`                               | set             | refuses to start       |
| `false`, with issuers configured     | set             | static-audience mode   |
| `false`, with issuers configured     | empty           | refuses to start       |
| `false`, no issuers configured       | either          | plane verifies nothing |

The first refusal exists so that no key is silently ignored: with the flag on,
`OIDC_AUDIENCE` is never read, and a value left behind would read as an audience
rule the plane does not have. Turning the flag on therefore means clearing
`OIDC_AUDIENCE` in the same change, not before it.

The second refusal is the load-bearing one. An empty `OIDC_AUDIENCE` is **not**
"no audience check": jose treats an empty audience option as presence-only, so
`jwtVerify` requires the token to carry an `aud` claim and then compares nothing
at all. A plane in that state accepts every token an allowed issuer minted for
any relying party while its configuration reads as if an audience were enforced.
A Kubernetes `secretKeyRef` with `optional: false` does not prevent this — it
guarantees the key exists, never that its value is non-empty — which is why the
check is a runtime assertion and not only a deployment-time one.

The two checks are not comparable, and the replacement is not a narrowing. The
binding rule is narrower in the **organization** dimension — it ties a token to
one organization rather than to the deployment as a whole — and wider in the
**deployment** dimension. With the flag off, a token had to carry this
deployment's `OIDC_AUDIENCE` _and_ a resolvable workspace claim. With it on, all
that is required is a valid signature from an allowed issuer, a non-empty `sub`,
and an organization's audience among the token's `aud` values; no workspace
claim is needed at all. A token an allowed issuer minted for an entirely
different relying party is rejected in the default mode and accepted in this
one.

**Operator requirements.** An organization's audience is therefore the only
thing standing between a token of an allowed issuer and that organization's
workspace, so before enabling this mode confirm both of the following. Neither
is something Registry can check for you.

- **`aud` must be issuer-controlled, not requester-selectable.** GitHub Actions
  lets the caller choose the audience outright: with such an issuer allowed, any
  repository that can request a token could ask for an organization's audience
  and be authenticated into its workspace.
- **Know what else accepts an organization's audience, because Registry does not
  discriminate between credential purposes.** An organization's audience is a
  value the issuer emits, and nothing here checks that it was emitted _for
  Registry_. Where the issuer emits a general-purpose audience, every credential
  carrying it authenticates on this plane — so recipient discrimination has to
  come from wherever those credentials are issued. Two shapes:
  - _Registry-dedicated audience._ The issuer emits the value for this Registry
    and nothing else — for example
    `https://registry.example.com/organizations/org_7f3c9a`. The audience is
    then the recipient check by itself. Prefer this where the issuer allows it.
  - _Namespace-derived audience._ An issuer that derives `aud` from a tenant
    namespace (for example `urn:example:<ns>`) stamps it on every credential
    issued in that namespace, not on Registry credentials specifically. Which
    credential may act for the organization is then governed by the issuer's
    own access control at issuance time, not by Registry — a credential that
    should not reach Registry must not be granted in the first place. The
    trade-off is deliberate, and its consequence is that an over-broad grant at
    the issuer is a Registry-visible fault: treat the access control that mints
    these credentials as Registry authorization. A Registry-dedicated audience
    is optional hardening, not a prerequisite.

  A value the issuer emits by default is the case to avoid outright: Keycloak
  puts `"account"` in `aud` for every token in a realm, so an organization
  configured with that audience would accept every principal of that realm.

Revocation is unaffected: the `OIDC_DENIED_JTI_FILE` check still runs
immediately after the signature verifies and before anything is looked up.

Every case that is not "exactly one organization, exactly one active workspace,
audience bound" is refused. The refusals fall into two groups, because the two
rules answer for different tokens. Resolution runs only for a token that carries
no workspace claim:

| Case, for a token carrying no workspace claim              | Result |
| ---------------------------------------------------------- | ------ |
| No organization is configured with the token's audience    | reject |
| Several `aud` values name several organizations            | reject |
| The organization has no active workspace, or more than one | reject |

The binding rule then applies to every token, however its workspace was reached:

| Case, always                                               | Result |
| ---------------------------------------------------------- | ------ |
| The token carries no `aud` at all                          | reject |
| The owning organization has no `audience` configured       | reject |
| The token's `aud` omits the owning organization's audience | reject |
| The organization or the workspace is archived              | reject |

Carrying several `aud` values is therefore not refused as such: it is fatal only
where it leaves "the organization" without a referent, which is the resolution
branch. A token that names its workspace in a claim authenticates whenever the
owning organization's audience is among the values it carries.

Scopes are not required on this plane and this mode does not change that: a
workspace-plane token authenticates with whatever scopes it carries, including
none, exactly as a per-workspace token does. The mode is about which workspace a
token speaks for, not about what it may do there.

**Known limitation: one active workspace per organization.** Registry does not
enforce this, and deliberately does not: `workspaces` allows many, the Platform
and Admin APIs still create them, and refusing a second workspace would break
every multi-workspace deployment for the sake of this one mode. The rule is
resolution-side only — an organization with a
second active workspace stops resolving by audience, and its tokens then have to
name a workspace in a claim, which the binding rule still checks. A deployment
using this mode must keep any organization that carries an audience to a single
active workspace. The `multiple_active_workspaces` rejection below is the
diagnostic for it: it is what distinguishes "this organization grew a second
workspace" from every other reason a credential stopped working.

**Diagnosing a rejection.** Every refusal in this mode is the same bare `401`;
the reason is never in the response, because it is information about this
deployment's organizations and the request has not authenticated. It goes to the
Registry log instead, at `warn`, as
`{"reason": "...", "organizationId": "...", "workspaceId": "..."}` — the two ids
appear only once resolution has established them — and to the
`registry_service_oidc_workspace_resolution_rejected_total{reason}` counter. The
token and the token's `aud` values are never logged. The reasons are
`no_audience_claim`, `workspace_not_found`, `audience_matches_no_organization`,
`audience_matches_several_organizations`, `no_active_workspace`,
`multiple_active_workspaces`, `organization_has_no_audience` and
`audience_not_bound`, one per row of the two tables above.

**Audience is set at creation only.** There is no update route
and no backfill; an organization created before audiences existed has a null
`audience` and authenticates nothing in this mode. That makes enabling the flag a
migration, not a toggle. Before turning it on:

1. **Inventory.** List every organization that must keep authenticating on the
   workspace plane. Each one needs an audience, and each needs exactly one
   active workspace.
2. **Give each one an audience.** Create it with
   `POST /v1/platform/organizations` and an `audience` field. An organization
   that already exists without one cannot acquire it through a supported API;
   in a test environment, recreate the Registry database.
3. **Seed the bootstrap organization.** The one organization every installation
   starts with is created offline by `registry:bootstrap-admin`, so set
   `ORCA_BOOTSTRAP_ORGANIZATION_AUDIENCE` on that run — alongside the existing
   `ORCA_BOOTSTRAP_ORGANIZATION_ID` / `ORCA_BOOTSTRAP_WORKSPACE_ID` — for a
   fresh install that will run in this mode. A blank value is refused rather
   than treated as unset.
4. **Flip the flag and clear the audience together.** `OIDC_AUDIENCE` must be
   emptied in the same change that sets `OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE`;
   neither Registry nor the chart accepts one without the other.
5. **Watch the rejection counter.** A rollout that missed an organization shows
   up as `audience_matches_no_organization` or `organization_has_no_audience`,
   not as a silent outage.

## Name uniqueness

Organization names are unique across the deployment. Workspace names are unique
within their organization; different organizations can each have a workspace
named `default`. Both rules include archived records, so archiving does not
release a name.

The Platform and organization Admin APIs trim leading and trailing whitespace
before storing names. Comparison is case-sensitive: `Production` and `production`
are distinct names. Creating a duplicate organization returns `409` with
`{ "error": "organization name already exists" }`. Creating or renaming a
workspace to a name already used in that organization returns `409` with
`{ "error": "workspace name already exists in this organization" }`. An active
workspace can keep its own name during an update. Replaying a successful Platform
request with the same `Idempotency-Key` and body returns the original response.

Postgres unique indexes enforce both rules across all writers, including
concurrent requests and offline commands. Migration `0057_name_uniqueness` adds
these indexes without renaming or deleting records. Existing duplicates cause
the migration to fail; operators must resolve them before retrying. These queries
identify conflicts, including archived records:

```sql
SELECT name, array_agg(id ORDER BY id) AS organization_ids
FROM organizations
GROUP BY name HAVING count(*) > 1;

SELECT organization_id, name, array_agg(id ORDER BY id) AS workspace_ids
FROM workspaces
GROUP BY organization_id, name HAVING count(*) > 1;
```

## Platform API

The Platform API is an Orca deployment extension, not an Anthropic Admin API
route:

```http
POST /v1/platform/organizations
POST /v1/platform/organizations/{organization_id}/workspaces
```

The workspace body contains only `{ "name": "..." }`. The organization body
accepts `{ "name": "...", "audience": "...", "capture_ceiling": "raw_io" }`.
Both `audience` and `capture_ceiling` are optional. `audience` is
set only at creation — there is no update route and no route that reads it
back, and an organization created without one takes part in no audience-based
resolution. The bootstrap organization, which no API creates, takes one from
`ORCA_BOOTSTRAP_ORGANIZATION_AUDIENCE`. An audience is
unique across organizations; reusing one returns `409`. Any other field in either body
is a `400`. IDs are generated by Registry; an organization or workspace ID is
never accepted from the request body. Workspace creation locks and verifies the
path organization is active before inserting the workspace.

The optional creation-time `capture_ceiling` uses the Registry capture-mode schema
and defaults to `metadata_only` when omitted; a platform that wants `raw_io` for new
organizations sends it explicitly. The organization, initial ceiling, platform audit
and idempotency result commit together. The mixed-version trigger's seed can be
replaced only for the organization just inserted in that same transaction: this
does not update an existing organization, create a default binding, or change
Session pins. An original create replay never reapplies the initial ceiling over
a subsequent administrator restriction. Existing callers that omit the field
retain their original request and default semantics.

Both writes accept an optional `Idempotency-Key`. Successful responses are
cached for 24 hours by `(platform principal, route scope, key)`; reusing a key
with a different body returns `409`. Authentication runs before idempotency.

Creating an organization does not mint an organization credential. Deployments
using OIDC grant an organization-scoped identity separately. API-key-only
deployments use the offline `registry:create-admin-key` command with the new
organization ID before handing organization administration to its owner.

### Platform observability policy

The admin listener serves the deployment-wide singleton policy through:

```http
GET /v1/platform/agent_observability
PUT /v1/platform/agent_observability
```

Both require the independent Platform API key or Platform OIDC identity with
`platform:admin`, not an organization admin or workspace key. The existing
`/api/v1` alias applies. These routes are not exposed on the public or internal
listeners and are not part of the Anthropic-compatible public OpenAPI.

GET returns `type: "agent_observability_platform_policy"`, the three policy fields
below, the server-owned `capture_restriction_epoch`, and `created_at`/`updated_at`.
GET and successful PUT return a strong opaque `ETag`. All responses, including
authentication and malformed-JSON errors, carry `Cache-Control: private, no-store`.

PUT fully replaces only these three fields; start from a fresh GET and retain
the current allowlists unless intentionally changing them:

```json
{
  "allowed_adapters": ["otlp_http"],
  "allowed_endpoint_classes": ["public"],
  "max_capture_mode": "raw_io"
}
```

Allowed adapters are `otlp_http` and `langfuse_sdk`; endpoint classes are `public`
and `private`. Both arrays must be nonempty and duplicate-free, and are returned
in sorted order. These are permission ceilings, not a declaration that an
exporter supports every allowed adapter or endpoint class. Capture modes are
`metadata_only`, reserved `redacted_io`, and `raw_io`. Unknown fields, including
caller-supplied epochs, timestamps, credentials, or organization IDs, are rejected.

Every PUT requires `Idempotency-Key` and the exact GET `ETag` in `If-Match`:

- Body validation runs first: an invalid body returns `400` even if `If-Match`
  is absent. For a valid body, missing `If-Match` returns `428`; stale state returns `412`. Weak, wildcard,
  multi-tag or malformed preconditions and invalid/missing idempotency keys return `400`.
- Successful requests are cached for 24 hours by platform principal, route and
  key. The normalized body and precondition both belong to the request identity;
  changing either with the same key returns `409` with
  `{"error":"idempotency-key reused with different request"}`. An exact retry replays its
  original response and ETag even after later policy changes; GET reads current state.
- The writer exclusively locks the same singleton row that Session pinning,
  context resolution and tenant configuration mutations lock for shared access.
  Policy, epoch, non-secret before/after platform audit and replay record commit
  atomically. Missing or corrupt policy state returns a sanitized `503`, not a
  newly seeded policy. A no-op preserves the ETag/epoch and creates no change audit.

Every capture-ceiling reduction increments the restriction epoch once; expansion
does not reset it. An existing Session pinned before a reduction stays
`metadata_only` even if the ceiling later rises. Allowlist changes use live
eligibility checks and do not advance the capture-restriction epoch.

Raising the platform ceiling is global: it can authorize new Sessions in any
organization/workspace whose own ceilings and binding request already permit it.
It does not change those scoped settings or upgrade existing pins. Enabling
`raw_io` requires explicit authorization for unredacted content and the retention
boundary described in the [exporter README](../../services/observability-exporter/README.md).
The platform API does not change organization settings. Organization administrators
can change only their ceiling through the scoped endpoint described below, without
creating or replacing a default binding.

## Admin API

The workspace routes follow Anthropic's organization Admin API paths and
pagination envelope:

```http
GET  /v1/organizations/me

POST /v1/organizations/workspaces
GET  /v1/organizations/workspaces
GET  /v1/organizations/workspaces/{workspace_id}
POST /v1/organizations/workspaces/{workspace_id}
POST /v1/organizations/workspaces/{workspace_id}/archive

GET  /v1/organizations/agent_observability
PUT  /v1/organizations/agent_observability
PUT  /v1/organizations/agent_observability/capture_ceiling
POST /v1/organizations/agent_observability:disable
POST /v1/organizations/agent_observability:rotate_credentials
GET  /v1/organizations/workspaces/{workspace_id}/agent_observability
PUT  /v1/organizations/workspaces/{workspace_id}/agent_observability
POST /v1/organizations/workspaces/{workspace_id}/agent_observability:rotate_credentials

GET  /v1/organizations/api_keys
GET  /v1/organizations/api_keys/{api_key_id}
POST /v1/organizations/api_keys/{api_key_id}
```

Workspace and API-key lists accept `after_id` or `before_id` (but not both)
plus `limit` from 1 to 1000. Workspace lists hide archived rows unless
`include_archived=true`; API-key lists additionally accept `workspace_id` and
`status`. Responses use `{ data, first_id, last_id, has_more }`.

The self-hosted workspace object implements the fields that have runtime
meaning here: `id`, `type`, `name`, `created_at`, and `archived_at`.
Anthropic-hosted concerns such as Console display color, geographic data
residency, CMEK compartments/external keys, and tags have no corresponding
enforcement in this deployment: the create and update routes read only `name`
and ignore any other field. The route and core lifecycle shape stay compatible
without pretending those policies were applied.

Because this self-hosted service has no Console, it also exposes one explicit
Orca extension:

```http
POST /v1/organizations/workspaces/{workspace_id}/api_keys
```

The create response contains the plaintext workspace key exactly once and is
sent with `Cache-Control: no-store`. Subsequent reads expose only
`partial_key_hint`. The create route does not accept an idempotency key because
persisting a replayable successful response would persist plaintext key
material; a client that loses the response creates a replacement and archives
the orphan by ID.

Admin API keys carry `org:admin` or any of `observability:read`,
`observability:write`, `observability:rotate`, `workspaces:read`,
`workspaces:write`, `api_keys:read`, and `api_keys:write`. `org:admin`, on a key
or an OIDC token, satisfies every admin scope check. All lookups constrain both
`organization_id` and the requested object ID; a cross-organization reference is
indistinguishable from a missing object.

The observability GET routes are current-state views with common
`type: "agent_observability"` and `scope` discriminators. Organization state
returns `default_binding` and its capture ceiling; workspace state returns
`mode`, `binding`, and its capture ceiling. Both return the effective binding
decision with non-secret target/config/credential metadata. A workspace custom
binding does not fall back to the organization default. Missing, archived, and
cross-organization workspaces all return `404`; corrupt observability state
returns a sanitized `503`. Every successful response has an opaque strong
`ETag` plus `Cache-Control: private, no-store`; `If-Match` is not processed on
these reads. Credential references and credential bytes are never returned.
Disabled effective state always reports `capture_mode: "metadata_only"`.
Capture permissions are ordered `metadata_only` < `redacted_io` < `raw_io`.
`raw_io` explicitly permits original input/output without automatic redaction;
the legacy `redacted_io` value is not an alias and does not authorize raw content.
The effective mode is bounded by the pinned request and every applicable ceiling.

`PUT /v1/organizations/agent_observability/capture_ceiling` changes only the
authenticated organization's ceiling, including when no default binding exists.
It requires `org:admin` (not merely `observability:write`), a strong organization
GET ETag in `If-Match`, and `Idempotency-Key`. The strict body is:

```json
{ "capture_ceiling": "raw_io" }
```

The `/api/v1` alias has the same authorization and precondition requirements.
Missing If-Match returns 428, malformed fields/headers 400, stale state 412,
and conflicting idempotency or a pending target mutation 409. Missing organization
returns 404; unavailable/corrupt state returns a sanitized 503. A successful PUT
returns the organization state with configured.default_binding unchanged; use GET
for the current ETag, as with the other scoped mutations. Exact retries replay
the original body; changing either capture_ceiling or If-Match under the same key
conflicts. Authority locks, the setting update, sticky epoch, audit, and replay
completion share one repeatable-read transaction using the existing mutation kernel.
Serialization failures have the existing bounded transaction retry. No SecretStore
call, credential rotation, binding config version, or Session-pin write occurs.
Lowering the ceiling permanently restricts older pins; re-expansion permits only
eligible new pins to capture raw I/O.

`PUT /v1/organizations/agent_observability` fully replaces the authenticated
organization's default. It requires `observability:write` (or `org:admin`) and
`Idempotency-Key`; its strict body contains only `target`, `config`,
`capture_ceiling`, and optional write-only `credentials`. V1 accepts
`otlp_http` with `otel_genai` or `langfuse` semantics. First configuration and
target identity changes include credentials and return `201`/`200` respectively;
same-active-target policy changes omit credentials and create a new immutable
config version. Existing configuration requires an exact strong GET `ETag` in
`If-Match` (`428` absent, `412` stale, malformed preconditions `400`). The
fenced idempotency/staging lifecycle writes raw credentials only to SecretStore;
every mutation response, including errors, uses `Cache-Control: no-store`, and
responses, audit metadata, and cached response bodies contain no credential
bytes or references.

`PUT /v1/organizations/workspaces/{workspace_id}/agent_observability` fully
replaces that active, authenticated-organization workspace's configuration. It
requires `observability:write` (or `org:admin`), a normalized
`Idempotency-Key`, and an exact strong workspace GET `ETag` in `If-Match`
(`428` absent, `412` stale, malformed headers `400`). Its strict
mode-discriminated body is either `{ mode: "inherit" | "disabled",
capture_ceiling }` or `{ mode: "custom", target, config, capture_ceiling,
credentials? }`; inherit and disabled reject target, config, and credentials.
The route is admin-listener-only and returns the authoritative workspace state
with `201` when it creates a new workspace custom binding and `200` otherwise.

Custom configuration creates a workspace-owned `otlp_http` binding when no
custom binding is selected or target identity changes, and those calls require
write-only credentials. A same-active-target update omits credentials and
adds an immutable config version while preserving its credential head. A target
replacement or transition from custom to inherit/disabled moves only an active
old workspace binding to `draining`; it does not alter organization defaults or
existing Session pins. Mode/binding selection changes advance the workspace
selection epoch, any capture-ceiling reduction (`raw_io` to either lower mode,
or `redacted_io` to `metadata_only`) advances its
capture-restriction epoch, and entering explicit disabled advances its
workspace revocation epoch. Explicit disabled fences only live reservations on
that exact workspace setting and current workspace binding before finalizing;
it never fences organization or sibling-workspace work. Completed workspace
PUT retries replay their original authoritative result after active workspace
authority validation but before `If-Match` or transition evaluation; live
same-key work returns `409`. Staged credentials use the same
fenced SecretStore lifecycle as organization PUT, while inherit/disabled make
no SecretStore call. Every canonical and `/api/v1` alias response, including
auth, validation, conflict, not-found, and availability failures, has
`Cache-Control: no-store`.

If archive commits after an ordinary workspace PUT stages credentials and before
its finalizer reacquires workspace authority, the PUT settles that exact staged
attempt and returns `404`; it does not activate a candidate binding. Existing
Session pin rows remain historical selection snapshots; this administration
surface does not itself make an exporter-delivery claim.

`POST /v1/organizations/workspaces/{workspace_id}/agent_observability:rotate_credentials`
rotates only the current active workspace-owned `otlp_http` custom binding's
credential head. It requires `observability:rotate` (or `org:admin`), an
`Idempotency-Key`, and an exact strong workspace `If-Match`; missing, stale, or
malformed preconditions return `428`, `412`, or `400`. Its strict body contains
only write-only OTLP `credentials`; target, binding, config, adapter, version,
and secret-reference fields are rejected. Inherit, disabled, non-active,
non-OTLP, and missing-head custom states return `409`; malformed ownership,
version, epoch, or ref state returns a sanitized `503`.

The route derives its binding only from locked active workspace authority. It
stages the next bundle, CAS-swaps only the exact current credential head from
`N` to `N + 1`, and queues the superseded ref for asynchronous workspace-binding
cleanup after commit. Target/config/selection/revocation/capture state and
Session pins remain unchanged. A completed same-key retry validates and returns
its historic authoritative `200` before current `If-Match` or eligibility,
including after later custom-target replacement, inherit, or disabled changes;
a live same-key attempt and a same-key different body are `409`. Explicit
workspace disabled preempts only the exact pending workspace binding rotation;
an archive that wins before finalization returns `404` after the staged cleanup
handoff. Every success and error response, including `/api/v1` alias responses,
uses `Cache-Control: no-store`; no response, audit, cache, or fixed workspace
operator event exposes credential bytes or refs.

`POST /v1/organizations/agent_observability:rotate_credentials` rotates only
the authenticated organization's current active organization-owned `otlp_http`
default credential head. It requires `observability:rotate` (or `org:admin`),
`Idempotency-Key`, and an exact strong state `If-Match`; missing/stale/malformed
preconditions return `428`/`412`/`400`. Its strict body is only write-only OTLP
`credentials`; target, config, adapter, and secret-reference fields are
rejected. A rotation leaves target/config/selection/revocation/capture state
and Session pins unchanged, advances only credential generation, and returns
the authoritative organization state with `200`. Every response uses
`Cache-Control: no-store`.
Successful replay uses durable route-scoped idempotency and remains valid after
a later PUT replaces the default. The superseded ref is queued for asynchronous
maintenance cleanup after the credential-head CAS, never deleted inline.

`POST /v1/organizations/agent_observability:disable` disables the authenticated
organization's default with `observability:write` (or `org:admin`) and an
`Idempotency-Key`. Its strict body is `{}` and accepts no target, binding,
archive, or policy field. `If-Match` is optional: a supplied exact current GET
ETag must match (`412` when stale; malformed forms `400`), while no header
allows emergency disable. A completed same-key retry returns its original
authoritative disabled `200` before evaluating current state or `If-Match`;
live same-key pending work returns `409`.

Disable serializes under organization observability authority locks. It fences
only pending PUT/disable work on the organization setting and pending rotation
work on its current organization binding, then clears the default pointer,
increments selection and default-revocation epochs, and moves only an active
binding to `draining`. A selected `draining`, `disabled`, or `archived` binding,
or one without a credential head, remains safely removable and otherwise
unchanged. Organization-wide/capture-restriction and binding revocation epochs,
credentials, targets, configs, SecretStore refs, and workspace custom settings
remain unchanged. It retains the selected credential head; a preempted staged PUT
or rotation follows normal durable staging cleanup. A no-default call writes
audit/idempotency state but changes no epoch or binding. Re-enable uses PUT and
creates a new binding. Every response,
including `/api/v1` alias and error responses, uses `Cache-Control: no-store`;
no response, audit, idempotency cache, or log carries a credential or ref.

Workspace API key scopes remain data-plane-only. A workspace key cannot create
or rotate itself or sibling keys.

## Lifecycle

Workspace archive is terminal and replaces hard delete. The archive
transaction:

1. locks the active workspace and its exact `(organization_id, workspace_id)`
   observability setting row plus its archive-revocation marker;
2. inserts the exact archive marker, CASes that setting's `revocation_epoch` to
   the marker's post-increment epoch, then marks the workspace archived;
3. archives all workspace API keys;
4. archives active sessions so new prepared executions fail; migration 0054's
   Session lifecycle trigger tombstones each active or disabled observability
   pin in that same transaction;
5. records an immutable admin audit event.

The setting increment is a fenced compare-and-set of its locked current epoch.
The durable marker records the Workspace identity, first archive timestamp, and
post-increment epoch. Missing, invalid, overflowing, marker-conflicting, or
CAS-lost state rolls back every row above. An archive of an already archived
workspace is a no-op: it leaves the setting epoch and marker unchanged. The
archive changes no other setting field, binding row, or credential head.

Migration 0056 backfills all Workspaces archived before this archive path
existed. Its temporary Workspace archive trigger covers direct old writers
during a rolling update: an old writer inserts the marker and advances the
exact setting itself, while a matching current marker is recognized without a
second increment. Missing, malformed, or overflowing setting or marker state
aborts that direct archive transaction.

The same transaction inserts one durable lifecycle-outbox row per affected
session. Registry immediately attempts to publish their `session.archived`
sentinels and its periodic reconciler retries failures, so a shared Harness
eventually stops every warm runner. Delivery failure does not re-enable the
workspace: public authentication and execution preparation both consult the
authoritative workspace row and fail closed.

The temporary mixed-version lifecycle trigger is also the database guard for
this bulk update: a missing or malformed Session pin raises and rolls back the
entire workspace archive transaction rather than leaving an archived workspace
with a live or unrevoked Session pin.

The v1 archive transaction updates every active session and inserts its outbox
row before commit, so its lock duration and work are linear in active-session
count. It is intentionally not presented as suitable for unbounded workspace
cardinality; schedule unusually large archives as maintenance operations.
[`roadmap.md`](./roadmap.md) records the durable, resumable batched fan-out needed before that
limitation can be removed without weakening archive correctness.

API key status is `active`, `inactive`, `archived` or computed `expired`.
`inactive` is reversible; `archived` and `expired` are terminal. Expiration is
set only at creation. Rotation is create-new, deploy-new, then archive-old.

Every successful organization-admin mutation records the actor, auth method,
organization, optional workspace, action, target, request ID, result and
non-secret metadata. Platform mutations and offline Platform-key rotation use
the separate deployment-wide `platform_audit_events` log, with optional target
organization/workspace IDs. Plaintext keys, hashes and fingerprints never
enter audit rows or logs.

## Data ownership

`workspaces` is the Registry authority. Registry-owned tenant tables reference
it with `ON DELETE RESTRICT`. File and Memory metadata live in separate
databases and cannot use a cross-database foreign key; their calls remain
reachable only after Registry has authenticated an active workspace. Archived
workspace bytes remain for retention/forensics; archiving deletes none of them,
and no SQL cascade reaches them.
