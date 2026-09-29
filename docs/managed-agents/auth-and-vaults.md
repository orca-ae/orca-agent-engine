# Auth and Vault Flow

> Inbound auth for the public API, workload auth for internal traffic, and the end-to-end vault sequence that keeps credentials out of the sandbox.

## Inbound auth (`registry-service` only)

`registry-service` accepts both `x-api-key` (Anthropic-SDK compat) and OIDC/JWT (first-party Orca clients). The two paths share a single authorization model behind them — keys map to a workspace/principal, JWTs carry the same identity claims.

| Header                             | Path                                                                        | Notes                                                                                |
| ---------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `x-api-key: orca_…`                | SHA-256 fingerprint lookup + Argon2id verification                          | Used by Anthropic SDK clients pointing `base_url` at us.                             |
| `Authorization: Bearer <oidc-jwt>` | OIDC validator (issuers and audience configured per plane, deployment-wide) | First-party Orca clients.                                                            |
| `orca-beta: managed-agents-<ver>`  | Optional. Opts into our beta features.                                      | The Anthropic SDK's `anthropic-beta: managed-agents-2026-04-01` is silently ignored. |

Failure modes: missing both → 401. Unknown api-key → 401. JWT `aud` mismatch → 401. JWT `iss` not in the plane's allowed-issuers list (`OIDC_ALLOWED_ISSUERS` on the workspace plane) → 401. With `OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE` on, the workspace plane checks `aud` against the audience of the organization that owns the token's workspace instead of the static `OIDC_AUDIENCE`; see [`workspace-administration.md`](./workspace-administration.md#organization-audience-workspace-resolution).

For normally fingerprinted workspace API keys, each request still reads the active key row,
expiry, revocation, current scopes and workspace status. The Argon2id result alone is reused
for up to 60 seconds, keyed by the **presented credential fingerprint plus current stored hash**.
This positive-only, per-authenticator LRU holds at most 1,024 proofs and 1,024 in-flight entries;
simultaneous requests for the same proof share one verification. Failed verification is not
cached, and a hash rotation or different presented credential cannot reuse the old proof.
No plaintext key, principal or authorization decision is retained in the cache.
Setting `REGISTRY_API_KEY_PROOF_CACHE_ENABLED` to `false` disables proof reuse; its default is `true`.
`lastUsedAt` remains a synchronous write on every successful authentication. OIDC and
admin/platform/internal authenticators do not use this proof cache. Cache counters and public
auth-stage timings are described in [Registry performance metrics](services/registry-service.md#performance-metrics).

API-key rows created before fingerprint lookup was introduced are backfilled
with a `legacy:<api-key-id>` placeholder. When direct fingerprint lookup misses,
Registry queries only active legacy rows, verifies their Argon2id hashes, and
replaces the matched placeholder with the real SHA-256 fingerprint. Later
requests use the normal indexed lookup. This fallback is a bounded migration
bridge: Registry refuses to scan more than 8 candidates, permits one scan at a
time per replica, and admits at most 8 scans per minute globally plus 2 per
source address. Limiter rejection returns HTTP 429 with Claude-compatible
`rate_limit_error` and `Retry-After`; already-fingerprinted keys and valid OIDC
continue normally. The source quota uses Fastify's verified `request.ip`.
Reverse-proxy deployments must configure `TRUST_PROXY_CIDRS` with explicit
proxy IPs/CIDRs; the Helm chart requires `registry.trustedProxyCidrs` whenever
chart-managed Ingress or Istio Gateway exposure is enabled and rejects
trust-all `/0` ranges. If the candidate cap is exceeded, legacy fallback fails
closed while already-fingerprinted keys and OIDC continue normally. Operators
must rotate or revoke legacy keys until the candidate set is within the cap,
then rotate unused legacy keys instead of retaining fallback indefinitely.
Registry exposes
`registry_service_legacy_api_key_fallback_total{result}` with `upgraded`,
`rate_limited`, and `candidate_cap_exceeded` outcomes; alert on the latter two.

Workspace credentials are data-plane credentials only. Organization workspace
and API-key lifecycle operations use the separate admin listener and
organization-scoped credentials described in
[`workspace-administration.md`](./workspace-administration.md). In particular,
an `orca_admin_...` key cannot call workspace data routes and an `orca_...`
workspace key cannot call `/v1/organizations/*`.

Deployment-wide provisioning is a third credential class:
`orca_platform_...` keys and `platform:admin` OIDC tokens carry no
organization claim and are accepted only on `/v1/platform/*`. Platform,
organization-admin, workspace, and internal workload authenticators are
fail-closed and mutually non-interchangeable.

## Internal-mesh auth

Registry's separate internal listener requires bearer auth in every environment.

- **Kubernetes:** Harness and AI Gateway use independent, short-lived projected
  ServiceAccount JWTs. Registry submits them to Kubernetes `TokenReview` and
  checks the configured audience and exact ServiceAccount subject. Registry can
  authenticate an audience-bound ServiceAccount JWT for a third configured
  observability-exporter subject through the same TokenReview path. Harness is
  authorized for general runtime routes; AI Gateway is authorized only for MCP
  destination and vault-credential resolution; the observability-exporter
  principal is authorized only for the two Session-scoped observability resolver
  routes.
- **Non-Kubernetes:** Registry, Harness, AI Gateway, and any observability
  exporter share a high-entropy internal token supplied directly or through a
  file. Every trusted holder of that shared token may call both observability
  resolver routes; `static_token` provides no workload isolation. File
  consumers reread it per request so rotation does not require a process
  restart.

Workspace and admin keys are rejected by the internal listener. Istio mTLS,
`NetworkPolicy`, and `AuthorizationPolicy` remain recommended defense-in-depth
for encryption and reachability, but are not the only authentication boundary.

The application layer retains responsibility for:

- **Public-ingress auth** at `registry-service-ts` (`x-api-key` and `Authorization: Bearer <jwt>`).
- **Outbound auth** from `ai-gateway` to backend MCP servers (vault-resolved bearer tokens).
- **Session-scoped JWT** issued by `registry-service-ts` and verified by `ai-gateway` (this is application-level because the JWT carries authorization claims, not just identity).

See [`docs/operation/internal-traffic-auth.md`](../operation/internal-traffic-auth.md).

## Session JWT minting

The session-scoped JWT used for `ai-gateway` calls is minted by
`registry-service` via
`POST /internal/v1/workspaces/{workspace_id}/sessions/{id}/mint-jwt`:

- Caller: `harness-server`, through Registry's authenticated internal listener.
- Workspace: derived from the prepared execution context and repeated in the
  internal path for ownership cross-checking. This is not a public API field.
- Signing: registry holds a long-lived RS256 private key, loaded from
  `SESSION_JWT_PRIVATE_KEY_PEM` (the PEM itself or a path to a PEM file). The
  matching public key is shipped to `ai-gateway`'s config so it can verify.
- TTL: `SESSION_JWT_TTL_SECS` (default 300 seconds); `codex_sdk` and `pi_sdk`
  Sessions receive 660-second tokens when Registry carries an LLM policy
  (`SESSION_JWT_LLM_ROUTES` / `SESSION_JWT_LLM_MODELS`). Harness refreshes on
  demand after its cache refresh point: the token's expiry minus the smaller of
  30 seconds and one fifth of the lifetime remaining when it was cached.
- Claims: `iss=orca-registry`, `aud=ai-gateway`, `sub=session-id`,
  `org_id`, `workspace_id`, `session_id`, `mcp_server_names`, `vault_ids`,
  `credential_ids`, `agent_id`, `guardrail_ids`, `runtime_config_revision`,
  optional `llm_routes`, optional `llm_models`, `iat`, and
  `exp`. Registry derives `org_id` from the persisted
  `workspaces.organization_id`; it never reads organization identity from the
  mint request. Registry derives `llm_routes` from its trusted deployment
  policy and derives `llm_models` by intersecting that policy with the
  session's concrete Agent models. The latter follows the session's pinned
  primary Agent version, applies its model override, and expands every pinned
  coordinator subagent reference. The Harness caller cannot choose or expand
  either allowlist.
  Allowlist claims fail closed per dimension: a missing or empty list
  authorizes no resource in that dimension. Consequently, omitting both LLM
  claims preserves MCP-only token behavior but grants no LLM route or model
  access. `vault_ids` is the
  session authorization root. `credential_ids` is derived from persisted
  Registry state instead of being trusted from the mint caller. It is a startup
  snapshot used for caller-supplied header policy and exact static Gateway
  destinations; trusted dynamic Registry resolution re-checks current
  Session/Vault state so
  replacements propagate without restarting the runner.

## Vault flow (sequence)

```
SDK client                    registry-svc           harness-server      ai-gateway      registry-svc (internal)        backend MCP
  │ POST /sessions {agent, env, vault_ids} │             │                  │                     │                          │
  │ x-api-key ─────────────────────────►   │             │                  │                     │                          │
  │                                        │ INSERT session                 │                     │                          │
  │                                        │ Kafka: ensure topic            │                     │                          │
  │ ◄── 200 Session                        │             │                  │                     │                          │
  │                                        │             │                  │                     │                          │
  │ POST /sessions/{sid}/events            │             │                  │                     │                          │
  │ x-api-key ─────────────────────────►   │ produce(user evts)              │                     │                          │
  │ ◄── 202                                │             │                  │                     │                          │
  │                                        │             │ consume(user evts)                     │                          │
  │                                        │             │ harness step                            │                         │
  │                                        │             │ tool=mcp_toolset, credential_id=vcrd_42         │                          │
  │                                        │             │ POST /v1/mcp (session JWT) ►       │                          │
  │                                        │             │ X-Orca-Credential-Id: vcrd_42                  │                          │
  │                                        │             │                  │ /internal/v1/workspaces/ws_1/sessions/ses_1/                      │
  │                                        │             │                  │ vault-credentials/vcrd_42/resolve (service bearer)                │
  │                                        │             │                  │ ◄── {creds, ttl}    │ via SecretStore       │
  │                                        │             │                  │ inject Authorization: Bearer ...               │
  │                                        │             │                  │ ────────── outbound MCP call ───────────────►  │
  │                                        │             │                  │ ◄────────── tool result ──────────────────────│
  │                                        │             │ ◄── tool result  │                     │                          │
  │                                        │             │ produce(agent.tool_result) ──┐                                    │
  │                                        │             │                              │                                    │
  │ GET /sessions/{sid}/events/stream      │             │                              │                                    │
  │ x-api-key ─────────────────────────►   │ Reader: tail topic ────────────────────── │                                    │
  │ ◄═════ SSE: agent.tool_result ════════│             │                                                                   │
```

The credential resolves at `ai-gateway`, never at the harness, never in the
sandbox. Registry's application auth permits only the AI Gateway
ServiceAccount to call the scoped credential resolver in Kubernetes. Network
policy and mesh policy further restrict reachability. Registry additionally
checks the active Session's workspace and persisted `vault_ids` before
releasing secret material.

## Vault credential storage

`registry-service` stores Vault metadata and credential **references** in Postgres; actual secret material lives in the configured secret store.

- `vaults` contains metadata only: `display_name`, `metadata`, lifecycle fields. It never contains target URLs or secret refs.
- `vault_credentials.access_secret_ref`, `refresh_secret_ref`, and `client_secret_ref` are SecretStore keys for `/v1/vaults/{vault_id}/credentials`.
- Provider credentials reuse `access_secret_ref` for opaque API-key,
  service-account, or signing material. Their Postgres row carries only the
  canonical `provider`, `scheme`, URL-path-safe `llm:*` logical ID, and non-secret
  `resolution_version`; no `mcp_server_url` is required. Because this
  credential discriminator is Orca-only, public provider CRUD requires a
  non-empty `orca-beta` header; non-opted list/get calls cannot expose provider
  records to Anthropic SDK response unions.
- Postgres never stores secret material.
- `ai-gateway`'s in-memory cache holds resolved credentials with the TTL the registry returns; cache is purged on 401 from the upstream backend.
- After purging a credential on upstream 401, `ai-gateway` resolves it again
  with `force_refresh: true`. For `mcp_oauth`, Registry performs the same
  guarded refresh + staged-secret/CAS persistence used by
  `mcp_oauth_validate`, then returns the newly persisted access token. Both
  entry points claim the same short-lived Postgres lease per credential before
  the one-time refresh-token exchange. Concurrent callers that loaded the same
  refs wait, then return the committed access-token winner without replaying
  the consumed refresh token. The holder heartbeats the lease through secret
  resolution and staged SecretStore writes; the UUID owner also fences the
  pointer CAS. An abandoned lease expires automatically. Static bearer and
  environment-variable credentials remain non-refreshable.
- Credential create and secret-rotation updates require a write-capable `SecretStore` with delete support; if none is configured the registry returns `503 { error: 'secret store unavailable' }` rather than falling back to inline secrets. Display/metadata-only updates do not need secret-store access.
- The dev stack may wire `ORCA_SECRET_STORE_MODE=local`, an in-process `LocalSecretStore` suitable only for local tests. Registry accepts it only with `NODE_ENV=development|test` and rejects it whenever Kubernetes service discovery is present.
- Kubernetes deployments use `ORCA_SECRET_STORE_MODE=kubernetes`. `KubernetesSecretStore` hashes each opaque SecretStore reference into a data key and stores values in one dedicated Kubernetes Secret using JSON Merge Patch. A dedicated registry ServiceAccount receives only `get` and `patch` on that named Secret; harness and gateway identities receive no access.
- Moving from `LocalSecretStore` cannot migrate existing values automatically: those bytes exist only in the old process heap. Operators first verify source credentials are available, perform the acknowledged store cutover, then recreate or rotate credentials into the persistent store; Postgres secret-reference rows alone are insufficient.

Organization-default and workspace-custom agent observability credentials use
the same write-capable `SecretStore`, but are not Vault credentials and are
never returned by the Session, Harness, or AI Gateway resolver route families.
Only `kubernetes_service_account` mode provides exporter-only isolation:
Registry maps the configured observability-exporter ServiceAccount subject to
two strict-`{}` Session-scoped resolver capabilities. In `static_token` mode,
every trusted holder of the shared internal token may call both routes; it
provides no workload isolation:

- `POST /internal/v1/workspaces/{workspace}/sessions/{session}/agent-observability/context/resolve`
  returns only the Session-pinned binding/version/target/configuration,
  delivery classification, capture ceiling, and epoch snapshot. It calls
  neither `SecretStore` nor an external delivery process, and its response
  excludes credential refs, secret values, and authorization headers. The
  resolver compares current platform, organization, and workspace
  capture-restriction epochs with the pin's observed epochs. Any advance gives
  that existing Session a sticky `metadata_only` effective mode even after
  ceilings expand; `current_ceilings` still reports the actual values. A new
  Session observes current epochs and is evaluated from its own pin. The clamp
  changes neither delivery classification nor reason.
- `POST /internal/v1/workspaces/{workspace}/sessions/{session}/agent-observability/secret/resolve`
  is the pre-send release boundary. It rereads the pinned authority and exact
  credential head in fresh locked snapshots around one direct `SecretStore`
  lookup. A changed complete `(credential_version, secret_ref)` head retries
  from the new generation; a partial mismatch, malformed bundle, unavailable
  store, or audit failure returns a sanitized `503`. A current deny, archive,
  revocation, policy restriction, or absent head returns `409` before the
  store is called. The successful second snapshot inserts an
  `agent_observability.credential_resolved` audit event before returning the
  provider-specific bundle. That audit contains the opaque authorization ID,
  binding/config and credential versions, Session ID, source, and effective
  capture mode only — never a ref, header, endpoint, bundle, or secret value.

Both resolver paths use `Cache-Control: private, no-store` before parsing or
workload authentication, including wrong-listener failures. An authorization
ID correlates one release audit event; it is not a bearer credential. Cached
clients or credential bytes do not authorize a later send. If a caller loses a
response after its audit commit, retrying creates a new release audit event:
this is an at-least-once authorization boundary, not exactly-once secret
delivery. The
admin-only observability PUT and credential-rotation routes first commit a
fenced staging intent, write one canonical opaque bundle outside their database
transaction, then atomically activate a binding head. Organization rotation
accepts credentials only for the current active organization-owned `otlp_http`
default; workspace rotation accepts them only for the current active
workspace-owned `otlp_http` custom binding. Each CAS-swaps that same binding's
head from generation `N` to `N + 1`; target/config/selection/revocation state
and Session pins do not change. The exact superseded ref is enqueued only after
the authoritative head swap and is deleted by durable reconciliation outside a
database transaction. Raw Basic/bearer/custom-header values and refs are absent
from Registry responses, audit metadata, and idempotency responses. Raw
credential values never enter Postgres. Opaque SecretStore refs persist only in
their dedicated binding-credential, staging-intent, and cleanup-outbox lifecycle
columns; they are never returned, audited, or cached in idempotency responses.

Organization-default disable does not call `SecretStore`. It clears selection,
advances only organization selection/default-revocation epochs, and marks an
active selected binding `draining`; a selected non-active binding keeps its
status, and a missing credential head does not block clearing it. Its credential
ref and head remain intact for retention and existing pin references. It enqueues
no cleanup for the selected credential head. A pending PUT or rotation fenced by
disable instead leaves its staged intent on the normal durable cleanup handoff.
Disable cannot expose a ref in state, audit, idempotency, or operator output.
Workspace-custom bindings and pins are outside this default-disable scope; a
later re-enable creates a new organization binding through PUT.

Credential responses are sanitized. `token`, `access_token`, `refresh_token`,
`client_secret`, provider `secret_value`, and the Postgres secret-reference
columns are write-only and never returned from public routes. Provider
responses expose an opaque version that changes whenever secret material or
the logical binding changes.

### Credential validation (`mcp_oauth_validate`)

`POST /v1/vaults/{vault_id}/credentials/{credential_id}/mcp_oauth_validate` is an on-demand
diagnostic for `mcp_oauth` credentials (bodyless POST, 400 for other credential types). It runs
refresh-then-probe:

1. **Refresh** — if a `refresh_secret_ref` is stored, the registry performs a real OAuth
   `refresh_token` grant against the credential's `token_endpoint` (client auth per
   `token_endpoint_auth_type`: `client_secret_basic`, `client_secret_post`, or client-id-only).
   On success the rotated `access_token` (and `refresh_token`, when the server rotates it) is
   **persisted** through the staged secret-rotation path before the probe runs. Persistence is
   mandatory: many OAuth servers invalidate the old refresh token on use, so exercising the grant
   without storing the result would brick the credential.
   Standard top-level OAuth token fields are preferred. For Slack user OAuth
   compatibility, Registry also accepts `authed_user.access_token`,
   `authed_user.refresh_token`, and `authed_user.expires_in` when top-level
   token fields are absent. Slack-style HTTP 200 `{ "ok": false, "error": ... }`
   responses are classified by error code: known auth/grant failures such as
   `invalid_refresh_token` are definitive, while transient or unknown
   application errors such as `internal_error` remain `connect_error`.
   Successful rotations update `auth_config.expires_at` from `expires_in`.
2. **Probe** — a JSON-RPC `initialize` request is sent to `mcp_server_url` with the freshest
   bearer token available.

The route always returns `200` with a `vault_credential_validation` object; HTTP errors are
reserved for genuine request/resource errors (unknown vault/credential, wrong credential type,
missing secret store). `refresh.status` uses the Anthropic-compatible enum `succeeded |
connect_error | failed | no_refresh_token`: a successful exchange is `succeeded`; a definitive
4xx or Slack auth/grant rejection is `failed`; a transient/network refresh failure
(429/5xx/network/timeout, transient or unknown Slack `ok:false`, or a 2xx response body that is
missing `access_token`) is `connect_error`. Top-level `status`
mapping: refresh `failed` → `invalid` (re-authorize); refresh `connect_error` → `unknown`
**even when the old access token still probes 2xx** — transient refresh breakage must surface
immediately rather than stay hidden until that token eventually expires; otherwise probe 2xx →
`valid`; probe 401/403 → `invalid`; anything else (5xx/429/network/timeout on the probe) →
`unknown` (retry later). Captured upstream bodies are truncated at 2048 chars; a successful (2xx)
token-endpoint response body carries fresh secrets and is replaced with `[redacted]` instead of
being echoed.

Two safeguards around the live calls:

- **Egress guard** — `mcp_server_url` and `token_endpoint` are user-controlled, and the registry
  dials them directly (unlike session traffic, which resolves at `ai-gateway`). The default fetch
  is wrapped in an SSRF guard (`egress-guard.ts`) that rejects loopback, link-local (incl. cloud
  metadata), RFC1918/CGNAT, and ULA destinations and disables redirect following; blocked calls
  surface as network failures (`refresh.status: connect_error` for `token_endpoint`, top-level
  `status: unknown` for `mcp_server_url`), never as echoed internal responses.
- **Refresh lease + rotation CAS** — every rotating refresh entry point first claims the same
  credential lease. The holder renews it until the staged-secret pointer swap is fenced; waiters
  return the committed winner without calling the token endpoint again. Persistence remains a
  compare-and-swap on the original secret refs plus lease owner. While that lease is live, a public
  secret update that changes only the access token or client secret returns `409` so the holder can
  persist any one-time rotated refresh token first. An update that replaces or clears the refresh
  token may supersede the lease; the holder then returns `409` (`credential was rotated
concurrently`, safe to retry) instead of last-writer-wins orphaning secret refs.

## Vault binding (Anthropic-aligned)

Following Anthropic Managed Agents semantics ([`platform.claude.com/docs/en/managed-agents/vaults`](https://platform.claude.com/docs/en/managed-agents/vaults)):

- Vaults are **session-scoped containers** (Session.vault_ids[]) — a session lists which workspace vaults it is authorized to use.
- Credentials under those vaults bind to upstream MCP servers through `Credential.auth.mcp_server_url`.
- Registry's destination resolver selects active credentials under Session
  Vaults after lowercasing scheme/host and stripping default ports and trailing
  slashes. Order is Session Vault order followed by credential `created_at` and
  id; first match wins and no match is explicit `credential_id: null`. The
  declared destination URL remains unchanged for forwarding.
- Credential creation enforces one active normalized MCP URL per Vault while
  holding the Vault row lock. Resolver rejects pre-existing normalized conflicts
  as `invalid_runtime_binding` instead of choosing one credential silently.
- Harness may rewrite matching MCP servers with `X-Orca-Credential-Id: vcrd_*`.
  Header is optional startup metadata. Wildcard Gateway dispatch ignores a
  stale valid value and always uses Registry's authoritative live destination
  response. A null authoritative credential forwards unauthenticated.
- The session JWT minted at the workspace/session-scoped internal `mint-jwt`
  route carries `vault_ids` and Registry-derived startup `credential_ids`.
  Gateway authorizers may use those IDs for a supplied header, but the header
  cannot select a dynamic credential and the live Registry result is not
  constrained to the startup list.
- `ai-gateway` resolves credentials via `POST /internal/v1/workspaces/{workspace_id}/sessions/{session_id}/vault-credentials/{credential_id}/resolve`, with workspace/session rendered from verified JWT scope, and injects outbound auth. Concrete `vcrd_*` requests retain exact MCP lookup. Namespaced `llm:*` aliases must select exactly one active provider credential across that Session's attached Vaults; zero or ambiguous matches fail closed. The response carries the selected concrete ID, canonical scheme, secret, TTL, and opaque version, while Registry persists a non-secret alias-selection audit event.
- Before credential resolution, `ai-gateway` resolves untrusted
  `X-Orca-Backend` via `POST /internal/v1/workspaces/{workspace_id}/sessions/{session_id}/mcp-destination/resolve`.
  Response contains only URL, nullable credential id, and a stable binding-tuple
  revision—never secret bytes or SecretStore references.

The agent's `mcp_servers[]` entries do NOT carry vault or credential references. Binding is determined by matching `agent.mcp_servers[].url` to `vault_credentials.auth.mcp_server_url`.

## GitHub PAT flow (a sibling pattern)

GitHub PATs and GitHub App installation tokens for `github_repository`
resources do not use Vault flat fields. Claude-compatible session requests
send the raw token in write-only `authorization_token`. Registry first records
a durable staging intent containing only generated ids + opaque secret ref,
then materializes bytes into `SecretStore`; the resource transaction atomically
inserts `git_credentials.secret_ref` metadata and consumes the intent.
Expired intents are reconciled after crashes. `git_cred://<id>` is retained
only as an Orca extension for pre-provisioned credentials.

Resolution path:

1. Public create/attach/update responses omit both raw token and internal
   credential reference. Idempotency storage contains only request hash and
   sanitized response.
2. Harness resolves the session's internal `github_repository.repo_ref`
   credential through registry over mesh-internal HTTP.
3. Harness uses the PAT to clone server-side.
4. PAT is held in harness-process memory only for the duration of the clone
   and re-resolved by `/v1/git-creds` on in-sandbox git operations.
5. PAT never enters the sandbox FS or env.

Session archive is terminal: registry disables helper resolution, archives
resource-owned Git credential metadata, and purges its SecretStore value.
Pre-provisioned `git_cred://` credentials remain workspace-owned and are not
deleted with the session.

See [`resource-mounting.md`](./resource-mounting.md) for the full git-mount sequence.

## Idempotency on auth

The `Idempotency-Key` middleware (registry-side) keys on `(workspace_id, scope, key)`. Auth happens before idempotency: an unauthenticated request returning 401 is not cached. Caching only kicks in for authorized requests so a stolen-key replay can't poison the cache from outside.

## Transport security

- Registry internal routes use static service-token auth
  outside Kubernetes and projected ServiceAccount JWT + TokenReview auth in
  Kubernetes. The application still does not manage mTLS certificates. See
  [`roadmap.md`](./roadmap.md) and
  [`docs/operation/internal-traffic-auth.md`](../operation/internal-traffic-auth.md).
