# Multi-workspace isolation

This document defines the tenant boundary for a shared Registry, Harness and
storage deployment. A workspace is a hard authorization boundary, not an
optional resource attribute. Session/execution scope is a second,
least-privilege boundary inside a workspace.

Registry's database migrations carry existing rows into this layout, for
example by backfilling a workspace row for every workspace ID already in use.
The public Anthropic-compatible `/v1/*` wire shape remains unchanged.

## Invariants

1. Public requests derive exactly one workspace from the authenticated API key
   or OIDC principal. Request bodies and query strings cannot select another
   workspace.
2. Internal runtime requests identify both workspace and session in their
   path. There is no tenant-resource lookup by bare resource ID.
3. Every relationship used to start a session is checked in the session's
   workspace before any sandbox, Git, S3 or secret side effect occurs.
4. Harness never owns or receives a workspace API key. In particular,
   `HARNESS_REGISTRY_API_KEY` does not exist.
5. The dispatcher keys runners by `(workspace_id, session_id)` and rejects an
   event whose workspace disagrees with Registry's prepared snapshot.
6. Tenant child rows carry `workspace_id`; database relationships use
   workspace-aware unique keys and foreign keys wherever they are within one
   database.
7. Every object-storage key contains a validated workspace segment. Sandbox
   credentials authorize only the current execution output prefix and the
   session's attached memory-store live prefixes.
8. Vault and Git secret material never appears in a runtime snapshot. The
   snapshot contains identifiers and non-secret binding metadata only.
9. The public listener does not register `/internal/*`. The internal listener
   has a separate ClusterIP/port and requires authentication on its
   `/internal/*` routes: in Kubernetes, a ServiceAccount token verified by
   TokenReview and admitted per route family; elsewhere, one shared service
   token.

## Runtime preparation

Harness prepares a runner with one Registry call:

```http
POST /internal/v1/workspaces/{workspace_id}/sessions/{session_id}/executions:prepare
```

Registry first resolves the Session by the composite path key. It then builds
one `PreparedExecutionV2` containing:

- the Session and its full-replacement Agent tool/MCP overrides;
- the pinned primary AgentVersion, rather than the Agent's latest version;
- pinned coordinator subagents;
- each agent's session-pinned custom SkillVersions in declaration order,
  represented by immutable bundle metadata rather than inline contents;
- the active Environment;
- active Vault credential metadata, excluding every secret/ref field;
- active File, MemoryStore and Git resources with resolved non-secret
  metadata;
- the compiled, tier-ordered guardrail list (session → agent → workspace →
  organization), the session's restored guardrail state, and the restored cost
  accumulators (session, per-thread, per-principal window) — composed as data
  by Registry, evaluated only by Harness (see
  [`guardrails.md`](./guardrails.md)).

The operation is fail-closed. A missing or cross-workspace binding, or an
archived Agent, Vault, File, MemoryStore or Git credential, returns
`409 invalid_runtime_binding`; archived Environments and SkillVersions stay
resolvable for the Sessions already bound to them. A workspace/session path
mismatch is indistinguishable from a missing session (`404`). Registry never
returns a partial snapshot.

Harness keeps runtime-specific behavior—Skill bundle materialization,
progressive-disclosure catalog composition, guardrail *evaluation* (the
permission-policy decision included, since a permission policy is the seed of
the guardrail fold), and sandbox selection—outside the Registry snapshot
builder. Registry compiles and orders guardrails as data in the snapshot;
applying a verdict is Harness's alone. Skill bodies are never returned by this
endpoint or concatenated into prompts.

Dynamic memory reads and commits use paths scoped by the same
`workspace/session/memory-store` tuple. Registry verifies that the store is an
active Session resource before calling `@orca/memory-store`; a caller cannot
submit a different `workspace_id` in the body.

## Control-plane data

Top-level tenant resources and their version/resource children carry a
workspace key:

```text
agents(workspace_id, id)
  └─ agent_versions(workspace_id, agent_id, version)

skills(workspace_id, id)
  └─ skill_versions(workspace_id, skill_id, version)

sessions(workspace_id, id)
  ├─ session_resources(workspace_id, session_id, id)
  └─ session_threads(workspace_id, session_id, id)

vaults(workspace_id, id)
  └─ vault_credentials(workspace_id, vault_id, id)
```

All repository queries include workspace even where generated IDs are globally
unique. Session creation validates AgentVersion, Environment, Vault, File,
MemoryStore, Git credential, SkillVersion and coordinator roster bindings in
that workspace. Public cross-workspace references use the same not-found
semantics as nonexistent references to avoid resource enumeration.

## Object-storage namespace

`S3_KEY_PREFIX` is a deployment root. Components append one canonical layout:

```text
{root}workspaces/{ws}/files/blobs/{aa}/{bb}/{sha256}/content
{root}workspaces/{ws}/memory-stores/{store}/live/{relative_path}
{root}workspaces/{ws}/memory-stores/{store}/versions/{sha256}
{root}workspaces/{ws}/sessions/{session}/executions/{generation}/outputs/{relative_path}
```

File content deduplication is workspace-local. Memory `live/` is the only
prefix mounted into a sandbox; `versions/` is never visible through FUSE.
Read-only memory resources use both a read-only IAM policy and a read-only
mount option.

File resources are materialized through host-side tarball prefetch. A sandbox
does not receive bucket credentials merely to read files and does not mount a
workspace-wide blob tree.

## Execution-scoped storage credentials

For a FUSE-capable runner, the Harness mints one short-lived STS session after
it has the prepared resources and a fresh generation ID. Its inline policy
contains only:

- read/write/list for the exact execution output prefix;
- read/list for each attached `read_only` memory-store `live/` prefix;
- read/write/delete/list for each attached `read_write` memory-store `live/`
  prefix.

There is no file grant, bucket-wide wildcard or workspace-wide memory grant.
Production requires STS; static credentials remain an explicit development
fallback and must be called out as non-isolating.

`S3_ENDPOINT` configures only S3 data access. `S3_STS_ENDPOINT` is an optional,
independent override for custom STS deployments; when it is absent, the AWS SDK
selects its default STS endpoint.

## Vaults and service identity

Prepared snapshots expose credential IDs, Vault IDs, auth type and routing
metadata, never secret values or secret-store references. Registry derives the
session JWT credential allowlist from persisted Session/Vault bindings; the
Harness cannot submit arbitrary credential IDs.

The gateway resolves secret material only through
`POST /internal/v1/workspaces/{workspace}/sessions/{session}/vault-credentials/{credential}/resolve`.
Workspace and Session come from verified JWT scope. Registry checks that the
Session is active and unarchived in that workspace, the request
`{ credential_id, vault_id, force_refresh? }` is valid, and both body IDs agree
with the path credential ID. The body `vault_id` is a legacy gateway alias for
`credential_id`, not a Registry `vlt_*` resource. Registry uses the credential
row's persisted Vault ownership instead and requires that Vault in the
Session's persisted `vault_ids`. Every ownership or binding miss returns
`404`. For an authorized `mcp_oauth` credential, `force_refresh: true` performs
the OAuth refresh and compare-and-swap token rotation before returning secret
material. Registry routes this and public `mcp_oauth_validate` through one
short-lived Postgres lease on that credential, so concurrent replicas return
the committed winner instead of consuming the same one-time refresh token
without holding a transaction over outbound HTTP. The holder renews the lease
through staged persistence, and lease owner fences the pointer CAS; scope checks
still complete before any token-endpoint request.

Gateway destination selection uses the same composite scope:
`POST /internal/v1/workspaces/{workspace}/sessions/{session}/mcp-destination/resolve`
with strict body `{ "backend": "<logical-name>" }`. Registry does not trust a
gateway-provided URL or credential id. In one `REPEATABLE READ` transaction it
loads active workspace/Session state, pinned AgentVersion plus Session override,
and eligible Session-Vault credentials. Credential URL identity follows
Anthropic normalization while the declared forwarding URL is preserved.
Unknown, cross-workspace, archived, or terminated scope returns `404`;
malformed persisted MCP/Vault bindings return the standard
`409 invalid_runtime_binding` shape. Response is secret-free:
`{ url, credential_id, revision }`, where `revision` is a stable fingerprint of
the exact URL-plus-credential binding rather than the broad Session runtime revision.
Kubernetes workload auth permits Gateway and shared-token callers and rejects
Harness identity on this route.

The public and internal Registry surfaces run on separate listeners:

```text
public   :8080  /v1/*, probes
internal :8081  /internal/v1/*, internal probes
```

Public Ingress targets only `:8080`. The internal ClusterIP targets `:8081`;
in Kubernetes, Registry's internal workload authorization limits Harness routes
and gateway credential resolution to their respective ServiceAccount identities
(see [`auth-and-vaults.md`](./auth-and-vaults.md#internal-mesh-auth)). Local development may use the private
loopback network, but must never reintroduce a tenant API key as service
identity.

## Required verification

- Two workspaces can run sessions concurrently through one Harness and each
  receives its own pinned Agent, Environment, Skill, Vault metadata and
  resources.
- A path workspace mismatch creates no sandbox and returns `404`.
- Every cross-workspace Session binding fails before runner creation.
- The same file SHA and memory store/path in two workspaces produce different
  object keys and bytes.
- STS credentials for workspace A cannot list, read or write workspace B,
  another Session generation, or an unattached memory store.
- Read-only memory mounts reject writes at both IAM and mount layers.
- Public Registry requests to `/internal/*` return `404`.
- Repository and architecture tests reject new unscoped internal tenant
  routes and any reintroduction of `HARNESS_REGISTRY_API_KEY`.

The merge-gated black-box coverage lives in
`packages/e2e-tests/test/multi-workspace-isolation.spec.ts` (Admin API,
resource/storage isolation, cross-workspace binding rejection, archive
fail-closed) and the concurrent workspace scenario in
`packages/e2e-tests/test/real-agent-loop.spec.ts` (Registry → transcript
backend → Harness → sandbox file mount, plus warm-runner teardown on archive).
