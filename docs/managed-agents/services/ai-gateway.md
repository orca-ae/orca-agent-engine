# ai-gateway

> External MCP egress gateway used by this repo, available as a public image (`ghcr.io/orca-ae/orca-ai-gateway`) and OCI Helm chart (`oci://ghcr.io/orca-ae/charts/orca-ai-gateway`) under Apache-2.0; this repo configures the image for managed-agents. Versions tested with this release are listed in [`../../compatibility.md`](../../compatibility.md).

## Dependency contract

Managed-agents depends on the `orca-ai-gateway` MCP forwarding path, not on gateway source code in this repository.

- The managed Helm chart defaults to
  `ghcr.io/orca-ae/orca-ai-gateway:v0.4.3-rc.3`. Managed-agents requires
  JWT-derived LLM route/model scope authorization, HTTP resolver
  `bearer_token_file`, wildcard MCP `destination_resolver`, and Registry usage
  sink support. Kind and OpenSandbox CI exercise that released Gateway image.
  The local Compose stack defaults to the same `v0.4.3-rc.3` image.
- `prepare-ai-gateway-image.sh` positively probes both required capabilities:
  missing HTTP `bearer_token_file` must fail with its field diagnostic, and a
  wildcard resolver with zero DNS timeout must fail with typed MCP egress
  validation. Full config and optional boot checks are additional only.
- Data listener: `POST /v1/mcp` on port `8090` in the local stack.
- LLM listener: `POST /v1/messages` on port `8090` for Anthropic Messages-compatible in-sandbox harness egress.
- Fast-mode LLM compatibility requires preserving the incoming
  `anthropic-beta` token `fast-mode-2026-02-01`, the JSON body field
  `speed: "fast"`, `output_config.effort`, and response `usage.speed`. A
  gateway that reconstructs Anthropic request headers without forwarding
  `anthropic-beta`, or rebuilds the request body without `speed` and
  `output_config`, is not fast-mode/effort compatible: retained response usage
  metadata cannot compensate for missing request controls, and managed fast
  sessions fail closed against it.
- Admin listener: `/healthz`, `/readyz`, and metrics on port `9099`.
- Routing header: `X-Orca-Backend` is the logical MCP server name from the Agent record.
- Optional credential header: `X-Orca-Credential-Id` remains rewrite metadata;
  Registry's destination response is authoritative for selected credential.
- Session header: `X-Orca-Session-Id` mirrors the session id for audit and policy context.
- Auth header: `Authorization: Bearer <session-jwt>` with `aud=ai-gateway`.

The harness rewrites each `agent.mcp_servers[]` URL to the gateway's MCP endpoint — the configured `AI_GATEWAY_URL` with `/v1/mcp` appended unless it already ends in `/v1/mcp` — and keeps the original backend identity in headers. The gateway resolves the backend URL and any vault credential, forwards the JSON-RPC envelope to the upstream MCP server, returns the upstream response, and emits one audit event per call.

## Responsibilities this repo relies on

1. Validate registry-minted session JWTs using issuer `orca-registry` and audience `ai-gateway`.
2. Extract scope dimensions from JWT claims: `org_id`, `workspace_id`,
   `session_id`, `agent_id`, `mcp_server_names`, `vault_ids`, `credential_ids`,
   `llm_routes`, and `llm_models`. Registry derives `org_id` from persisted
   `workspaces.organization_id` and never reads it from the mint request, so
   Gateway may use this verified scope value for authorization, usage, and
   metrics attribution. Allowlist dimensions fail closed: a missing or empty
   list authorizes no resource in that dimension. For example, a token without
   `llm_routes` or `llm_models` may still use its authorized MCP servers, but
   it cannot authorize an LLM route or model.
3. Authorize each call so `X-Orca-Backend` is in `mcp_server_names`.
4. Resolve destination through Registry's scoped `POST /internal/v1/workspaces/{workspace_id}/sessions/{session_id}/mcp-destination/resolve`, sending strict `{ backend }`. Treat returned `{ url, credential_id, revision }` as authoritative; do not accept a caller URL or use YAML destination data.
5. When `credential_id` is non-null, resolve it through Registry's scoped `POST /internal/v1/workspaces/{workspace_id}/sessions/{session_id}/vault-credentials/{credential_id}/resolve` endpoint. The trusted destination response and credential route both re-check current workspace/session/Vault state, so wildcard dispatch does not constrain the live result to JWT `credential_ids` captured when the runner started. The gateway renders workspace/session from verified JWT scope, authenticates both HTTP resolvers with a bearer token reread from `bearer_token_file`, sends `{ credential_id, vault_id, force_refresh? }` (`vault_id` is a legacy alias carrying the same `vcrd_*` value), and injects the returned credential as the upstream `Authorization` header. Kubernetes mounts a short-lived projected AI Gateway ServiceAccount JWT at that file; the non-Kubernetes stack mounts the shared internal service token.
6. Apply gateway egress admission to the Registry-resolved HTTP(S) URL, then forward Streamable HTTP MCP JSON-RPC. Resolver output is routing metadata, not an SSRF-policy bypass. A null credential means unauthenticated forwarding.
7. Cache idempotent responses keyed by `Idempotency-Key` per destination/session so SDK retries do not duplicate upstream side effects. Partition dynamic cache identity by backend, Registry `revision`, and authoritative credential binding (including explicit null).
8. Emit audit records to `orca.{workspace_id}.audit.ai-gateway` with workspace, session, backend, method, latency, status, request hash, and credential id. Secret material must never be logged.
9. For Anthropic fast-mode LLM calls, preserve the `fast-mode-2026-02-01`
   `anthropic-beta` token when merging gateway-owned beta tokens, preserve the
   top-level request `speed` and `output_config`, and pass response `usage.speed`
   through unchanged.
   Managed-agents verifies both SDK `fast_mode_state` and provider
   `usage.speed`; omission or downgrade must surface as a turn failure, never a
   standard-speed success.
10. When Registry-backed spend plugins are configured, authenticate with the
    projected Registry-internal token and write LLM deltas to
    `/internal/v1/workspaces/{workspace_id}/sessions/{session_id}/usage` and
    coalesced policy state to the sibling `/guardrail-state` route. Registry's
    Kubernetes workload authorization accepts the AI Gateway identity on these
    two routes; it remains denied from Harness-only internal capabilities.
    Harness remains authoritative until Registry is started with
    `AI_GATEWAY_REGISTRY_USAGE_ENABLED=true`. With that switch and the Gateway
    Registry usage sink both enabled, AI Gateway is the sole writer for
    `colocated` Sessions; `separate` Sessions remain Harness-owned.

The managed Helm chart coordinates both sides with
`aiGateway.registryUsage.enabled`. It defaults to `false`; when enabled, the
chart sets Registry's `AI_GATEWAY_REGISTRY_USAGE_ENABLED=true` and, when it
also owns the Gateway workload, renders the Registry usage sink. The chart's
default Gateway image, `v0.4.3-rc.3`, supports that sink. A separately installed
Gateway must configure the matching sink itself before enabling the switch.

The chart also exposes `aiGateway.registryGuardrails.enabled`. When enabled it
renders a Registry guardrail source and policy config, and extracts `agent_id`
from the signed Session JWT alongside the existing scope dimensions. Registry
returns a Session-scoped bundle from the current organization, workspace, Agent
and Session guardrails. The Gateway fetches it using its projected internal
token and refreshes it after `cacheTtlSecs` (300 seconds by chart default), so
policy edits do not require a Gateway restart. The switch defaults to `false`;
the chart's pinned Gateway image must be upgraded to a release with Registry
guardrail-source support before enabling it. Stateful budget rules also need a
shared `registryGuardrails.state` backend when Gateway has multiple replicas.

The OpenSandbox sandbox-harness E2E configures both the Registry and Kafka
usage sinks. Its real LLM case requires the Registry Session aggregate and the
Kafka schema-v2 UsageEvent to report the same input/output token counts. The
Kafka broker's Docker listener is exposed to the in-cluster Gateway through a
selectorless `opensandbox-system/kafka` Service; the test process consumes the
same topic through the host listener without committing offsets.

## Local-stack configuration

The checked-in local template is `services/dev/ai-gateway-config.yaml`. It configures:

- `server.listen: 0.0.0.0:8090`
- `server.admin_listen: 0.0.0.0:9099`
- JWT issuer `orca-registry`, audience `ai-gateway`, and public key mount `/etc/orca-gateway/session-jwt-pub.pem`
- LLM route accepts registry-minted ai-gateway JWTs for in-sandbox harness calls to `/v1/messages`
- scope extraction from the session JWT claims listed above
- registry credential resolution at `http://registry:8081/internal/v1/workspaces/{scope.workspace_id}/sessions/{scope.session_id}/vault-credentials/{credential_id}/resolve`
- registry destination resolution at `http://registry:8081/internal/v1/workspaces/{scope.workspace_id}/sessions/{scope.session_id}/mcp-destination/resolve`
- exact static MCP destinations, when configured, before the `"*"` dynamic
  destination; the wildcard route target is the default
- dynamic egress admission with only `host.docker.internal` allowed as a
  private and plaintext-HTTP hostname for the local fake-MCP E2E fixture
- YAML ACL authorization for backend and vault allowlists
- Kafka audit sink topic `orca.{workspace_id}.audit.ai-gateway` (the local
  gateway config uses the gateway template syntax
  `orca.{scope.workspace_id}.audit.ai-gateway` to render that topic)

`services/dev/docker-compose.yml` runs the gateway as service `ai-gateway` and
bind-mounts that config, the generated JWT public key, and the generated
internal service token. The image must support wildcard `destination_resolver`;
`prepare-ai-gateway-image.sh` probes for it before the stack starts.

The registry/harness `KAFKA_CONNECTION_MODE` / SASL / TLS env vars configure only the managed-agents transcript KafkaJS clients. They do not configure the ai-gateway audit sink; gateway Kafka audit auth/TLS remains an external-image configuration concern.

For dynamic destinations, the client credential header may be omitted. When
present, it is advisory startup metadata and never selects or constrains
Registry's current `credential_id`; this permits credential archive/delete and
replacement to propagate to running sessions. The wildcard name is reserved
(`X-Orca-Backend: *` is invalid). Resolver scope placeholders are rendered only
from verified JWT-derived principal attributes, never request headers. Egress
admission rejects loopback, link-local/metadata, private, and special-use
addresses unless the configured hostname is explicitly allowed. HTTPS is the
default; the same explicit hostname allowlist is required for plaintext HTTP
and private/special resolution. The pinned client disables environment proxies,
pins connect-time DNS results, and does not follow redirects. Dynamic upstream
`3xx` responses are rejected as `502` rather than relayed to a caller that might
follow them outside gateway admission. Buffered dynamic responses are capped at
8 MiB; SSE stays streamed.

Gateway-to-Registry destination and vault resolver clients also ignore ambient
proxy variables and reject redirects, so projected workload tokens never move
to an endpoint other than the configured Registry URL.

Gateway unit tests own DNS rebinding/pinning, proxy-environment bypass,
HTTPS-by-default admission, and redirect socket behavior. Cross-service E2E
owns Registry-backed resolution, live credential replacement with a stale
runner JWT/header, representative loopback/metadata/private-host denials, and
one allowed-host redirect proving no second outbound request. The redirect call
itself returns `502`.

## In-repo integration points

- `services/harness-server/src/config.ts` reads `AI_GATEWAY_URL` and defaults to `http://localhost:8090`.
- `services/harness-server/src/config.ts` reads `LLM_GATEWAY_URL` and defaults to `http://localhost:8090/v1` for in-sandbox Anthropic Messages traffic.
- `services/registry-service-ts/src/config.ts` reads `SESSION_JWT_LLM_ROUTES` and `SESSION_JWT_LLM_MODELS`. Registry places configured routes into session JWTs and narrows configured model patterns to the concrete primary and coordinator-subagent models pinned by that session. Both variables must be configured together, and leaving both unset omits the LLM claims.
- `services/harness-server/src/mcp/rewrite.ts` emits the gateway URL plus `X-Orca-*` headers.
- `services/registry-service-ts/src/auth/session-jwt.ts` mints and verifies the `ai-gateway` audience used on the MCP path.
- `services/registry-service-ts/src/api/internal.routes.ts` exposes destination resolution, vault resolution, and session-JWT minting endpoints the gateway/harness call.

## Explicitly out of this repo

- Gateway implementation source, toolchain setup, and gateway unit tests.
- Standalone gateway deployment docs.
- Gateway release engineering and image publication.

The gateway ships as its public image and OCI chart. This repository only consumes a configured image and documents the managed-agents contract.
