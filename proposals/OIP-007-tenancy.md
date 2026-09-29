# OIP-007: Tenancy: workspace isolation, admin planes and OIDC audiences

- *Author(s)*: @jiangpengcheng, @freeznet
- *Status*: Released
- *Proposal time*: 2026-07-17
- *Components*: registry-service-ts, harness-server, `packages/file-store`, `packages/memory-store`,
  `packages/transcript-store`, Helm chart
- *Discussion*: None (predates the public repository)
- *Implementation*: registry-service-ts `src/auth/`, `src/server.ts`,
  `src/api/{admin,platform}.routes.ts`, `src/api/platform-mutations.ts`,
  `src/domain/prepare-execution.ts`, `src/{bootstrap-admin,create-admin-key,create-platform-key}.ts`,
  migrations `0024`–`0026`, `0028`, `0043`, `0045`, `0057`; harness-server `src/runner/dispatcher.ts`,
  `src/auth/sts-creds.ts`; `packages/{file-store,memory-store}/src/blob/`, memory-store migration
  `0002`, `packages/transcript-store/src/route.ts`; chart `service-registry-*.yaml`, `validation.yaml`
- *Released in*: v0.5.0

## TL;DR

One deployment serves many organizations, but the Anthropic-compatible `/v1` surface has no field
that names a workspace. Registry derives exactly one workspace from the credential and carries it
into every row key, internal route, object key and sandbox credential. Three credential families that
no other plane accepts separate workspace, organization and deployment authority; each OIDC plane
verifies its own issuers and audience, with an opt-in `metadata` fallback, a revocation list and a
mode that resolves the workspace from an organization's audience. Operators, organization
administrators and OIDC clients are affected.

## Background

Harness runs Sessions from snapshots Registry prepares ([OIP-002](OIP-002-agent-harnesses-and-execution-modes.md));
runners and workers carry their own tunnel credentials ([OIP-011](OIP-011-self-hosted-session-runner.md));
`/apis` groups are [OIP-009](OIP-009-core-and-extension-api-groups.md). Owning documents:
[`multi-workspace-isolation.md`](../docs/managed-agents/multi-workspace-isolation.md),
[`workspace-administration.md`](../docs/managed-agents/workspace-administration.md),
[`auth-and-vaults.md`](../docs/managed-agents/auth-and-vaults.md),
[`internal-traffic-auth.md`](../docs/operation/internal-traffic-auth.md).

## Motivation

**The credential is the only selector the contract has.** An Anthropic SDK client sends `x-api-key`
and nothing that names a workspace. A `workspace_id` parameter would diverge from that contract and
move the tenant boundary into request data, where any key holder could aim it elsewhere.

**Below the API, an earlier design shared nearly everything.** Harness called Registry with a
workspace API key (`HARNESS_REGISTRY_API_KEY`); version rows hung off global IDs; file blobs were
content-addressed deployment-wide, and a dormant FUSE strategy could mount that whole prefix into a
sandbox; sandbox credentials covered every memory store of a workspace.

**Authority above a workspace must be split** between organizations, which own workspaces and keys as
in Anthropic's organization Admin API, and a provisioner of organizations that is not itself one.
**Identity providers differ**: some cannot mint top-level custom claims, some can scope an audience
per relying party but not a credential per workspace, and any token may need revoking early.

## Goals

### In scope

- Exactly one workspace per public request, from the credential, carried into every tenant key,
  internal Session path, object key and sandbox credential and checked on every Session binding
  before any sandbox, Git, S3 or secret side effect; another workspace's resource reads as missing.
- Workspace, organization-admin and platform-admin credentials that no other plane accepts; internal
  workload identity that never carries a tenant credential; offline bootstrap; audited writes.
- Per-plane OIDC validated at startup, with claim fallbacks, revocation and organization-audience
  resolution; fail closed wherever a check cannot decide.

### Out of scope

- Listed on [`roadmap.md`](../docs/managed-agents/roadmap.md): per-operation
  RBAC, per-workspace OIDC issuers, distributed per-workspace rate limits, resumable workspace-archive
  fan-out and a lazy file mount; its [known limitations](../docs/managed-agents/roadmap.md#known-limitations)
  include scoped OIDC administrator roles, per-workspace buckets or KMS keys and `/v1/api_keys`.
- Organization lifecycle routes, hard deletion of workspaces or their bytes, Anthropic-hosted
  workspace attributes (display color, data residency, CMEK).

## Design

### High-level design

```
 client     x-api-key orca_…          | Bearer, workspace OIDC ─► public   :8080 ─► one workspace
 org admin  x-api-key orca_admin_…    | Bearer, admin OIDC     ─► admin    :8082 ─► one organization
 operator   x-api-key orca_platform_… | Bearer, platform OIDC  ─► admin    :8082 ─► no organization
 Harness, AI gateway, exporter:   Bearer workload token        ─► internal :8081 ─► caller + routes
 installer: DATABASE_URL ─► registry:bootstrap-admin, registry:create-{admin,platform}-key
 workspace ─► rows keyed (workspace_id, id); /internal/v1/workspaces/{ws}/sessions/{session}/…;
              {S3_KEY_PREFIX}workspaces/{ws}/…; runner (ws, session); one STS policy per execution
```

No listener accepts another's credential: the platform owns organizations, an organization its
workspaces and keys, a workspace everything a `/v1` caller can name.

### Detailed design

**Organizations and workspaces.** An organization (`org_…`) has a deployment-unique name, an
optional `audience` unique when set, and status `active | archived`; a workspace (`wrkspc_…`) has a
name unique in its organization (both rules include archived rows) and is archived terminally, never
deleted. Routes never accept IDs; only the offline bootstrap can set them. A workspace ID must match
`^[A-Za-z0-9_-]{1,128}$` to authenticate. Tenant tables reference `workspaces` with `ON DELETE
RESTRICT`; File and Memory metadata live in their own databases, used only for a checked workspace.

**A public request's workspace** (`auth/auth.ts`, `auth/api-key.ts`, `server.ts`). Only `/healthz`,
`/readyz` and `/metrics` skip authentication; `/v1/git-creds`, two read-only git-proxy operations and
`/v1/tunnels/*` verify their own session JWT, binding token or environment key. A present
`x-api-key` is authoritative: another family's prefix is rejected without a query; otherwise a
SHA-256 fingerprint (domain `orca-api-key`) selects one active, unexpired, unrevoked key of an active
workspace and Argon2id verifies it. The outcome is `authenticated`, `absent` or `rejected`; only
`absent` lets an OIDC Bearer try, and a rate-limited legacy lookup is `rejected` with a `429`. Each
request re-reads the workspace row and admits only `active`; `rejectExplicitWorkspaceSelector`
answers `400` to a top-level `workspace_id` or `workspaceId` in a `/v1/` or `/apis/` query or body.
Queries include the workspace even for globally unique IDs; idempotency keys on
`(workspace_id, scope, key)`. Only trigger routes inspect `scopes` (`workspace.agentTriggers.*`, or
`workspace.full_access`, which Admin-API-minted keys carry).

**Isolation in storage.** Migration `0024` gives `agent_versions`, `skill_versions` and
`session_resources` a `workspace_id`, adds `(workspace_id, id)` keys and makes each in-database
relationship a workspace-aware composite foreign key (version to parent, Session to pinned Agent
version and Environment, thread and resource to Session, credential to Vault, Git credential to
resource); memory-store `0002` does the same for `memories` and `memory_versions`. Key builders
reject unsafe segments and non-canonical digests; file deduplication never crosses a workspace:

```text
{root}workspaces/{ws}/files/blobs/{aa}/{bb}/{sha256}/content
{root}workspaces/{ws}/memory-stores/{store}/live/{relative_path}
{root}workspaces/{ws}/memory-stores/{store}/versions/{sha256}
{root}workspaces/{ws}/sessions/{session}/executions/{generation}/outputs/{relative_path}
```

**Runtime preparation and sandbox credentials.** Harness prepares a runner with one call,
`POST /internal/v1/workspaces/{ws}/sessions/{session}/executions:prepare`. A path mismatch reads as
a missing Session (`404`); a binding that no longer resolves in the workspace (a deleted or foreign
resource, an archived Vault, an archived or terminated Session) is `409 invalid_runtime_binding`,
never a partial snapshot; an Environment or Skill version archived after pinning stays resolvable.
The snapshot holds IDs and non-secret metadata only. Harness checks its `workspace_id` fields
(`validatePreparedExecution`) and keys runners by `{ws}/{session}`; the transcript store refuses an
event routed to another workspace or Session (`assertEventsMatchRoute`). Each execution generation
gets one STS session (`sts-creds.ts`): read-write-list on its output prefix, read or read-write-delete
on each attached memory store's `live/`, `s3:ListBucket` limited to those prefixes, nothing else.
Files arrive only by host-side `tarball_prefetch`. Static keys reach a sandbox only with
`ALLOW_INSECURE_STATIC_S3_CREDS=true` in development or test; otherwise Harness refuses them.

**Planes and listeners.**

| Plane | Listener | Credential | Principal |
| --- | --- | --- | --- |
| Workspace | public, `HTTP_PORT` (8080) | `orca_…` key or workspace-plane OIDC | one workspace |
| Organization | admin, `ADMIN_HTTP_PORT` (8082), `/v1/organizations/*` | `orca_admin_…` key or admin OIDC with `org:admin` and an organization claim | one active organization |
| Platform | admin, `/v1/platform/*` | `orca_platform_…` key (scope `platform:admin`) or platform OIDC with `platform:admin` | none; OIDC as `platform-oidc:<issuer>:<sub>` |
| Workload | internal, `INTERNAL_HTTP_PORT` (8081) | static token, or projected ServiceAccount JWT via TokenReview | `harness`, `ai-gateway`, `observability-exporter` or `shared` |

Key families differ in prefix, fingerprint domain, table, authenticator and default audience; rows
keep a fingerprint, an Argon2id verifier and a `...last4` hint; the admin authenticators follow the
tri-state rule; no listener registers another's routes. An organization principal's organization
comes only from its credential and must be active, and lookups pair organization and object ID, so
a foreign workspace or key is `404`. Admin keys carry `org:admin` or a subset of `observability:*`,
`workspaces:{read,write}`, `api_keys:{read,write}`; admin OIDC requires `org:admin`. Only the admin
listener reads `x-orca-registry-authorization: Bearer <token>`, a proxy-safe carrier moved into
`Authorization` first; beside it, repeated or non-Bearer, it is `401`.

The internal listener requires a Bearer except on probes and `/metrics`. `INTERNAL_AUTH_MODE` defaults
to `kubernetes_service_account` where `KUBERNETES_SERVICE_HOST` is set, else `static_token` (one token
of 32 or more characters, compared in constant time, yielding `shared` on every route). TokenReview
checks audience `orca-registry-internal` and three distinct subjects, caching successes for 30 s; a
route outside the caller's capability family (`internal-auth.ts`) is `403`. The env-docs gate fails
if anything reads `HARNESS_REGISTRY_API_KEY`; Session JWTs take `org_id` from the workspace row.

**Bootstrap and key rotation.** `registry:bootstrap-admin` runs offline; `DATABASE_URL` is the only
bootstrap authority. One serializable transaction under an advisory lock refuses if any organization
exists, then creates an organization (with `ORCA_BOOTSTRAP_ORGANIZATION_AUDIENCE`, never blank, when
set), a workspace, an `org:admin` key and a platform key. `registry:create-admin-key` and
`registry:create-platform-key` add a key and can archive the old one in the same audited transaction.

**Provisioning and administration.** The Platform API creates organizations (`name`, optional
`audience` of at most 200 characters, optional `capture_ceiling`) and workspaces under an active
organization (`name` only); another field is `400`, a taken name or audience `409`. An optional
`Idempotency-Key` replays for 24 hours per platform principal, route and key (another body is
`409`); each write commits a `platform_audit_events` row and mints no credential. The Admin API
follows Anthropic's organization paths and pagination. Creating a workspace key is an Orca extension
(there is no Console): the plaintext is returned once, `no-store`, without an idempotency key, since
a replay would store it; keys get `workspace.full_access` and an optional `expires_at`. Organization
mutations commit `admin_audit_events` rows, which never hold keys, hashes or fingerprints.

**Workspace archive** ([lifecycle](../docs/managed-agents/workspace-administration.md#lifecycle)) is
one transaction: lock the workspace and its observability setting, advance its revocation epoch
(OIP-012), archive the workspace, revoke its keys, archive its active Sessions with one outbox row
each (publishing `session.archived` to stop warm runners), and audit.

**OIDC planes** (`auth/oidc.ts`). Each plane has its own issuers, audience and `metadata` opt-in; a
token is checked against each allowed issuer's `/.well-known/jwks.json`. Startup refuses issuers with
an empty audience (jose would only check that `aud` exists), a `metadata` opt-in with more than one
issuer, and the resolution flag outside the workspace plane; the chart checks the audience rules too.

| Plane | Identity claim | `metadata` fallback |
| --- | --- | --- |
| Workspace | `workspace_id`, then `orca_workspace` | `metadata.orca_workspace` |
| Admin | `organization_id`, then `orca_organization` | `metadata.orca_organization` |
| All (scopes) | `scopes` (array), then `scope` (string or array) | `metadata.orca_scopes` |

The first scope claim that grants wins, an empty one falls through, and a malformed one grants
nothing and blocks the fallback, so `metadata` never widens a grant. The principal is `sub`, else a
legacy `principal` claim that is never attributed; a verified `sub` and its issuer feed Memory
`user_actor` and OIP-012's transcript attribution. The workspace plane needs no scope, the admin
plane `org:admin` and an active organization, the platform plane `platform:admin`.

**Revocation.** `OIDC_DENIED_JTI_FILE` lists `jti` strings or `{key}` records, checked on every plane
after signature, issuer and audience, reread at most every 5 seconds (1 MiB, 1-second deadline). A
clean document replaces the list and a partly unreadable one only adds to it; a missing or unreadable
file keeps the last good list (none: deny nothing) and warns. A token without `jti` cannot be denied
([revocation](../docs/managed-agents/workspace-administration.md#oidc-token-revocation)).

**Organization-audience resolution.** With `OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE=true` and an empty
`OIDC_AUDIENCE`, the workspace plane checks no static audience. A workspace claim is kept; without
one, the token resolves to the single active organization configured with one of its `aud` values,
then to its single active workspace. Either way the owning organization must have an audience that
the token's `aud` contains. Each refusal (`no_audience_claim`, `workspace_not_found`,
`audience_matches_no_organization`, `audience_matches_several_organizations`, `no_active_workspace`,
`multiple_active_workspaces`, `organization_has_no_audience`, `audience_not_bound`) is a bare `401`
plus a `warn` log and `registry_service_oidc_workspace_resolution_rejected_total{reason}` that omit
the token and its `aud`. The requester must not be able to choose `aud`
([operator requirements](../docs/managed-agents/workspace-administration.md#organization-audience-workspace-resolution)).

**Deployment cells.** A boundary stronger than a workspace is a deployment: stacks ("cells") can
share one Postgres instance with one logical database per service per cell, using the chart's
`external.databases.{existingSecret,tls}` and `databasePools.*`; store migrations serialize on an
advisory lock ([External Postgres](../docs/managed-agents/kubernetes.md#external-postgres)).

## Changes by component

- **registry-service-ts**: authenticators, listeners, guards, Platform and Admin routes, offline
  commands, workspace-scoped queries and preparation.
- **harness-server**: workload credential, snapshot check, runner keys, STS, tarball-only files.
- **Libraries**: store key layouts and migrations; the transcript route assertion.
- **Helm chart**: three Services, TokenReview RBAC, per-plane OIDC values and checks, cell values.

## Public-facing changes

### API

`/v1` and `/apis` shapes are unchanged; a top-level `workspace_id` or `workspaceId` is `400`. Admin
routes answer `{error}` bodies, accept `/api/v1` and are absent from the public OpenAPI document:

| Route | Authority | Behavior |
| --- | --- | --- |
| `GET /v1/organizations/me` | any organization principal | the caller's organization |
| `POST`, `GET /v1/organizations/workspaces`; `GET`, `POST /v1/organizations/workspaces/{id}` | `workspaces:write` / `workspaces:read` | create (`200`), list (`include_archived`), get, rename; archived is `409` |
| `POST /v1/organizations/workspaces/{id}/archive` | `workspaces:write` | terminal; a repeat returns the archived workspace |
| `POST /v1/organizations/workspaces/{id}/api_keys` | `api_keys:write` | Orca extension; `201`, plaintext once |
| `GET /v1/organizations/api_keys[/{id}]`; `POST /v1/organizations/api_keys/{id}` | `api_keys:read` / `api_keys:write` | list (`workspace_id`, `status`), get; update `name` or `status` |
| `POST /v1/platform/organizations` | `platform:admin` | `201`; `name`, `audience?`, `capture_ceiling?` |
| `POST /v1/platform/organizations/{id}/workspaces` | `platform:admin` | `201`; `name`; missing organization `404`, inactive `409` |

`org:admin` satisfies every organization scope; lists take `after_id` or `before_id` and `limit`
1–1000; a workspace is `id`, `type`, `name`, `created_at`, `archived_at`. Observability, guardrail
and price routes on this listener belong to OIP-010 and OIP-012.

### Events and streaming

No new event kind; workspace archive publishes the existing `session.archived` sentinel per Session.

### Wire protocols

Internal Session routes carry workspace and Session in the path; Session JWTs carry `org_id`.

### Storage

Registry migrations `0024`–`0026`, `0028`, `0043`, `0045`, `0057`, memory-store `0002`, and the
object layout, all described above.

### Configuration

| Setting | Default | Read by | Effect |
| --- | --- | --- | --- |
| `OIDC_ALLOWED_ISSUERS`, `ADMIN_OIDC_ALLOWED_ISSUERS`, `PLATFORM_OIDC_ALLOWED_ISSUERS` | empty | Registry | issuers per plane |
| `OIDC_AUDIENCE`, `ADMIN_OIDC_AUDIENCE`, `PLATFORM_OIDC_AUDIENCE` | `orca-managed-agents`, `orca-managed-agents-admin`, `orca-managed-agents-platform` | Registry | audience per plane |
| `OIDC_METADATA_CLAIMS`, `ADMIN_OIDC_METADATA_CLAIMS`, `PLATFORM_OIDC_METADATA_CLAIMS` | `false` | Registry | `metadata` fallback; one issuer only |
| `OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE` | `false` | Registry | audience resolution; needs an empty `OIDC_AUDIENCE` |
| `OIDC_DENIED_JTI_FILE` | unset | Registry | revocation list for every plane |
| `INTERNAL_AUTH_MODE`; `INTERNAL_SERVICE_TOKEN[_FILE]` | by environment; unset | Registry, Harness | workload auth; one token source in static mode |
| `INTERNAL_AUTH_AUDIENCE`, `INTERNAL_AUTH_{HARNESS,AI_GATEWAY,OBSERVABILITY_EXPORTER}_SUBJECT` | `orca-registry-internal`; required in Kubernetes mode | Registry | TokenReview audience, three distinct subjects |
| `S3_KEY_PREFIX`; `S3_STS_ROLE_ARN`, `S3_STS_ENDPOINT` | empty; unset | Registry, Harness; Harness | object root; execution-scoped sandbox credentials |
| `ALLOW_INSECURE_STATIC_S3_CREDS` | `false` | Harness | static sandbox keys; development and test only |

Ports are `HTTP_PORT`, `INTERNAL_HTTP_PORT`, `ADMIN_HTTP_PORT`; pool ceilings (`10`) are
`DATABASE_POOL_MAX`, `TRANSCRIPT_STORE_POOL_MAX`, `FILESTORE_POOL_MAX`, `MEMORYSTORE_POOL_MAX`. The
chart renders these from `registry.*` (`trustedProxyCidrs` is required with Ingress or Istio),
`internalAuth.*`, `objectStorage.*` and `databasePools.*`; it sets `ALLOW_INSECURE_STATIC_S3_CREDS`
only when `objectStorage.stsRoleArn` is empty and static keys are supplied, defaulting Harness
`NODE_ENV` to `development`. `OIDC_DENIED_JTI_FILE` has no chart value.

### Metrics, logs and traces

`registry_service_oidc_workspace_resolution_rejected_total{reason}`,
`registry_service_legacy_api_key_fallback_total{result}` and Harness `harness_sts_mint_total{result}`
(`dev_fallback` marks non-isolating credentials). Denied-JTI reload failures and malformed scope
claims log warnings without values; offline key commands audit as `offline-database`.

## Compatibility

### Upgrade

Migrations upgrade a populated database: `0024` copies each child row's workspace from its parent,
drops Session threads whose Session is gone and gives each API key a `legacy:<id>` fingerprint;
`0025` puts every workspace ID found in tenant tables under a new organization `org_legacy_default`;
memory-store `0002` backfills and rebuilds tombstones for versions whose memory was deleted. A legacy
key upgrades on first use through a bounded fallback (8 candidates; per replica one scan at a time
and 8 a minute; 2 a minute per source; else `429`); rotate legacy keys rather than rely on it.
Bootstrap refuses once any organization exists, so an upgraded installation mints its first keys
with `registry:create-admin-key` and `registry:create-platform-key`. Object keys are not rewritten
and no earlier layout is read. `0057` fails on existing name collisions
([detection queries](../docs/managed-agents/workspace-administration.md#name-uniqueness)). Enabling
audience resolution is a migration, not a toggle. An `x-api-key` that fails (empty, whitespace,
unknown, another family) is `401` even beside a valid Bearer.

### Rollback

Migrations are forward-only. Turning audience resolution off means restoring a non-empty
`OIDC_AUDIENCE` in the same change.

### Version skew

Upgrade Registry and Harness together: Harness sends a workload token, never a tenant key, and
addresses Sessions by workspace. The AI gateway reads its bearer from `bearer_token_file`.

## Security considerations

- **Fail closed.** A rejected key never falls through; the workspace row, and for administrators the
  organization, is checked on every request; wrong-family keys, malformed scope claims, resolution
  ambiguities and binding misses deny; inconsistent audience, `metadata` or subject configuration
  stops startup. The one exception, an unreadable denied-JTI file, keeps the last good list, because
  failing closed would lock every plane, including the one that repairs it.
- **Listener separation.** Admin and internal Services are `ClusterIP`; only the public listener has
  an Ingress or Istio Gateway, trusting forwarded addresses only from `registry.trustedProxyCidrs`.
- **Trust.** An allowed issuer can assert any workspace, organization or scope its plane reads, so
  each plane has its own issuers and audience (the defaults differ), `metadata` is opt-in and
  single-issuer, and audience resolution needs an issuer-controlled `aud`. `static_token` makes every
  holder one shared principal; per-workload capabilities need Kubernetes mode.
- **Credential material.** Keys are shown once and stored as a fingerprint plus Argon2id; no creation
  response is cached; the legacy fallback is bounded; refusal reasons never reach the caller.

## Testing

- **Registry**: unit `oidc-auth`, `oidc-metadata-claims`, `oidc-denied-jti`, `internal-auth`,
  `internal-route-family-parity`, `auth-allowlist`, `server-surfaces`; Postgres `auth.spec.ts` (real
  JWKS issuers, no fall-through on any listener, legacy limits, the admin carrier), `admin-workspaces`,
  `platform-admin`, `internal-prepare-execution`, `name-uniqueness*`; `internal-auth-kubernetes.e2e`
  against TokenReview on kind.
- **Harness and libraries**: `sts-creds`, `dispatcher-event-source`; file-store `workspace-isolation`
  and `s3-workspace-isolation`; memory-store `migration-upgrade`; transcript-store
  `producer-workspace-isolation`.
- **End to end and chart**: `multi-workspace-isolation.spec.ts` (wrong-listener credentials,
  selectors, lookups, relationships, raw object keys and bytes, archive), the concurrent
  two-workspace case in `real-agent-loop.spec.ts`, `internal-auth.spec.ts`; `render.test.mjs`.

## Alternatives

- **A request-supplied workspace selector** diverges from the Anthropic contract and makes every key
  a key for whatever it names; the guard refuses one so no caller believes it reached a workspace.
- **Workspace selection for audience tokens, plus cross-workspace keys** (withdrawn). A request
  header would let a claim-free organization-audience token pick among its organization's
  workspaces; a `workspace | cell` key scope, minted only on the platform plane, would let one key act
  in any active workspace; workspaces could take caller-chosen IDs. Not adopted generically: the
  header makes an organization the credential's boundary, a cell key is a deployment-wide data-plane
  credential issued by the plane designed to reach no tenant data, and the need was one downstream
  control plane's onboarding model, where that design continued.
- **Enforcing one active workspace per audience-carrying organization** would break multi-workspace
  deployments for one mode; resolution refuses instead, with its own reason.
- **A tenant key in Harness** (the earlier design) puts one tenant's credential in a service shared by
  all of them; **mesh policy alone** would leave a mesh-less internal listener open.
- **Scoping the FUSE blob strategy** still mounts a prefix; prefetch copies exactly the attached
  files, and a manifest-aware lazy mount is listed as deferred by design.
- **One administrator credential, or admin routes on the public listener.** Separate listeners and
  families keep administration off the ingress path and make a leaked key useful on one plane only.
- **An unconditional `metadata` fallback** (an earlier revision) makes issuing a credential equal to
  minting any identity wherever the requester fills `metadata`.
- **Other revocation semantics.** Failing closed on an unreadable file locks out every plane;
  replacing the list from a partly unreadable one un-revokes tokens; keeping the last good list on
  any unreadable entry freezes new revocations. **Idempotent key creation** would persist keys.

## Status notes

Order of landing: data-plane isolation; the admin listener and internal workload authentication; the
platform plane; OIDC fallbacks, revocation and audience resolution; name uniqueness. Corrections:

- **Authentication no longer falls open past a bad key.** The public, organization and platform
  authenticators treated an `x-api-key` that failed like an absent one, so another credential on the
  request could decide, as could a rate-limited legacy lookup. Each now returns `authenticated`,
  `absent` or `rejected` and tries OIDC only on `absent`.
- Pre-fingerprint keys were unreachable until the bounded fallback; the FUSE blob strategy was
  removed; platform workspace creation now locks its organization `FOR NO KEY UPDATE`.
