# OIP-003: The egress boundary: AI gateway, MCP routing and LLM egress

- *Author(s)*: @freeznet, @tuteng, @sijie
- *Status*: Released
- *Proposal time*: 2026-05-03
- *Components*: registry-service-ts, harness-server, sandbox-harness, Helm chart
- *Discussion*: None (predates the public repository)
- *Implementation*: harness-server `src/mcp/`, `src/harness/in-sandbox/llm-env.ts`,
  `src/runner/dispatcher.ts`, `src/config.ts`; registry-service-ts `src/auth/session-jwt.ts`,
  `src/auth/internal-auth.ts`, `src/api/internal.routes.ts`, `src/domain/mcp-destination.ts`,
  `src/domain/llm-policy.ts`, `src/domain/session-llm-egress.ts`, `src/domain/credential-egress.ts`;
  sandbox-harness `src/providers/claude.ts`; the chart's `templates/configmap-ai-gateway.yaml`
- *Released in*: v0.5.0

## TL;DR

MCP servers need vault credentials and model providers need API keys; neither may reach a sandbox
running agent-written code, and vault secrets may not reach the harness process either. The engine
delegates that egress to one separate component, the AI gateway, and owns only the contract:
Registry-minted session JWTs, MCP servers rewritten to the gateway, destinations and credentials
Registry resolves on every call, and model calls routed through the gateway from every sandbox and,
for `separate` sessions, by a deployment default each session can override. Operators who deploy
the gateway and anyone binding vault credentials to MCP servers are affected.

## Background

In Anthropic's hosted Managed Agents an Agent lists `mcp_servers[]`, a Session lists `vault_ids[]`,
each vault credential binds to one `auth.mcp_server_url`, and Anthropic's egress injects the token
when the sandbox dials that server. Orca runs the loop itself: `separate` sessions in
harness-server, cloud `colocated` `claude_code`, `codex_sdk` and `pi_sdk` sessions in a sandbox that
harness-server drives over HTTP/SSE, and Registry-owned sessions in `session-runner`
([OIP-002](OIP-002-agent-harnesses-and-execution-modes.md), [OIP-011](OIP-011-self-hosted-session-runner.md)).
The gateway is not built from this repository; it is a public image under Apache-2.0
(`ghcr.io/orca-ae/orca-ai-gateway`) with an OCI Helm chart (`oci://ghcr.io/orca-ae/charts/orca-ai-gateway`),
tested at the version [`compatibility.md`](../docs/compatibility.md) lists. The owning documents are
[`mcp-routing.md`](../docs/managed-agents/mcp-routing.md), [`services/ai-gateway.md`](../docs/managed-agents/services/ai-gateway.md)
and [`auth-and-vaults.md`](../docs/managed-agents/auth-and-vaults.md).

## Motivation

A session has two credentialed exits, MCP tool calls and model calls. The sandbox executes code the
model writes, so anything in its environment or filesystem can leave with the agent's output.
harness-server replicas serve every workspace in a deployment and run third-party SDK processes on
untrusted tool results; keeping vault secrets out of them keeps a harness fault from exposing them.
The Claude Agent SDK has no seam for intercepting its traffic: an MCP config is a URL and headers,
there is no `fetch` hook, `HTTP(S)_PROXY` is not honored reliably, DNS rewrites break TLS SNI, and a
patched `globalThis.fetch` breaks across SDK upgrades and catches model traffic too
([`mcp-routing.md`](../docs/managed-agents/mcp-routing.md)); the SDK does accept a rewritten URL.
Credentials also move while sessions run (operators archive, replace and rotate them, and OAuth
access tokens expire), so binding a call to a credential must read current state, and audit, SSRF
admission and credential refresh belong in one place that sees every call.

## Goals

### In scope

- No vault credential in harness-server or a sandbox, and no provider key in a sandbox.
- MCP routing without an SDK fork, invisible to agents: server names, `mcp_toolset` references and
  `mcp__<server>__<tool>` names are unchanged; no change to Anthropic's Agent or Vault shapes.
- Registry as the sole authority for a call's destination and credential, read per call from
  persisted state, so archive, replacement and rotation reach running sessions.
- Short-lived, session-scoped, audience-bound tokens with Registry-derived claims; the signing key
  never leaves Registry. Model egress through the same boundary, selectable for `separate`.

### Out of scope

- Gateway behavior beyond the contract: forwarding, egress admission, idempotency cache, audit sink
  and credential cache ([`services/ai-gateway.md`](../docs/managed-agents/services/ai-gateway.md)).
- Listed on [`roadmap.md`](../docs/managed-agents/roadmap.md): tool federation, token renewal in a
  running query, `web_fetch` egress (it runs from the harness host), `llm_request` interception.
- Git credentials, the runner's `sidecar` mode ([OIP-011](OIP-011-self-hosted-session-runner.md)),
  gateway guardrails and usage writing ([OIP-010](OIP-010-guardrails-pricing-and-spend.md)).

## Design

### High-level design

```text
 harness-server ── POST …/sessions/{id}/mint-jwt ──────► Registry: signs RS256, derives claims
   │ rewrites mcp_servers[] to <gateway>/v1/mcp            (runner snapshots mint in-process)
   │ sandbox env: LITELLM_API_BASE, LITELLM_API_KEY           ▲
   ▼                                                           │ POST …/mcp-destination/resolve
 SDK MCP client · SDK model calls · sandbox · runner           │ POST …/vault-credentials/…/resolve
   │ Authorization: Bearer <session JWT>, X-Orca-Backend,      │ (gateway workload identity only)
   │ X-Orca-Session-Id[, X-Orca-Credential-Id]                 │
   ▼                                                           │
 AI gateway: verify the JWT, scope from claims, ACL ───────────┘
   ├─ /v1/mcp ──────────────────────► upstream MCP server, credential injected, audit record
   └─ /v1/messages, /v1/responses, /v1/chat/completions, /v1/proxy/… ─► provider, gateway key
```

harness-server prepares a session's execution (credential metadata, never secrets), has Registry
mint a session JWT, points every MCP server at the gateway, and decides each tool call against the
permission policy and guardrails before it leaves. The gateway verifies the token, authorizes the
backend, asks Registry for destination and credential, fetches the secret, forwards and audits.

### Detailed design

#### Session JWTs

`SessionJwtMinter` (`src/auth/session-jwt.ts`) signs RS256 with `SESSION_JWT_PRIVATE_KEY_PEM` (a PEM
or a file path); the gateway holds the public half. The Harness-only
`POST /internal/v1/workspaces/{workspace_id}/sessions/{id}/mint-jwt` answers `{token, expires_at}`:

| Claim | Derived from |
| --- | --- |
| `iss`, `aud`, `sub`, `iat`, `exp` | `SESSION_JWT_ISSUER`; `SESSION_JWT_AUDIENCE` unless the caller names another audience; the session id; mint time plus the lifetime below |
| `org_id` | the persisted `workspaces.organization_id`, never the request |
| `workspace_id`, `session_id` | the path, once the session loads as not archived, terminated or deleted in an active workspace |
| `mcp_server_names` | requested names present among the session's effective MCP servers: its full-replacement override, else the pinned AgentVersion |
| `vault_ids`, `credential_ids` | the persisted `sessions.vault_ids` (the request's `vault_ids` is not read) and the active credentials under them at mint time |
| `agent_id`, `guardrail_ids`, `runtime_config_revision` | audience `ai-gateway` only, for the gateway's guardrail source |
| `llm_routes` | audience `ai-gateway` with an LLM policy: `SESSION_JWT_LLM_ROUTES`, narrowed for `pi_sdk` to its model's `llm-pi-<provider>-<api>` route (403 if the policy lacks it) |
| `llm_models` | `SESSION_JWT_LLM_MODELS` patterns (exact or simple `*` globs, the gateway ACL's semantics) intersected with the concrete model ids of the pinned primary version, its model override and every pinned coordinator subagent; ids containing `*` never cross |

Beyond choosing the audience and supplying `repo_urls` (for Git credential tokens), a caller can
only narrow `mcp_server_names`. Tokens live `SESSION_JWT_TTL_SECS` (300 s); `codex_sdk` and `pi_sdk`
sessions get 660 s when an LLM policy is configured, chosen from the pinned harness so one bearer
covers a ten-minute turn. The gateway treats a missing or empty allowlist as authorizing nothing in
that dimension, so without an LLM policy no token opens a model route.

#### MCP routing

`rewriteMcpServers` (harness-server `src/mcp/rewrite.ts`) maps each `mcp_servers[]` entry to an SDK
`http` config keyed by its own name. The URL is `AI_GATEWAY_URL` plus `/v1/mcp` unless already
present; the headers are `Authorization: Bearer <session JWT>`, `X-Orca-Backend: <name>`,
`X-Orca-Session-Id`, and `X-Orca-Credential-Id` when a prepared credential's normalized
`mcp_server_url` matches (first in Session Vault order, then `created_at`, then id). The dispatcher
rewrites for harnesses whose catalog capability sets `needsMcpRewrite` (`claude_agent_sdk`,
`codex_sdk`, `pi_sdk`; `packages/harness-catalog/src/capabilities.ts`), mints only when the agent
has an MCP server, and after a mint or rewrite failure logs it and runs the session without remote
MCP servers. The Claude SDK gets the map with `strictMcpConfig` and no setting sources, so no user
or project MCP configuration loads; the Codex and Pi adapters run their MCP clients on the host. A
server no `mcp_toolset` enables exposes no callable tool; an enabled one defaults to `always_ask`.

#### Destinations and credentials, resolved per call

The gateway treats `X-Orca-Backend` as an untrusted logical name. After checking it against
`mcp_server_names`, it renders the workspace and session from verified claims only and calls
`POST /internal/v1/workspaces/{workspace_id}/sessions/{session_id}/mcp-destination/resolve` with a
strict `{"backend": …}`. Registry answers `{url, credential_id, revision}`
(`src/domain/mcp-destination.ts`) from one `REPEATABLE READ` transaction:

- **Scope.** An active workspace and a session not archived, terminated or deleted (else 404), and
  the pinned AgentVersion of a non-archived Agent (else 409 `invalid_runtime_binding`).
- **Destination.** The session's full-replacement MCP override, else the version's servers; the
  last duplicate name wins, as in the rewrite. Entries must be `http` or `https` URLs without
  userinfo or fragment (else 409); an unknown backend is 404.
- **Credential.** Every Session Vault must be active (else 409). Active credentials whose
  `mcp_server_url` normalizes to the destination's (scheme and host lowercased, default port
  dropped, trailing slashes stripped, query kept) are ordered by Session Vault, `created_at` and
  id; none yields `credential_id: null`, forwarded unauthenticated. Two matches in one Vault are a
  409, never a guess; creation rejects normalized duplicates under the Vault row lock.
- **Revision.** A positive 53-bit fingerprint of the exact URL and nullable credential id, which
  partitions the gateway's idempotency cache by binding, unaffected by other session changes.

For a non-null id the gateway calls `POST …/vault-credentials/{id}/resolve` with a strict
`{credential_id, vault_id, force_refresh?}`; `vault_id` is a legacy alias that must equal the id,
and a mismatch is a 404, so the route is no existence oracle. Registry re-checks the session and
its vaults, reads the SecretStore and returns `{credential_id, vault_id, version, scheme: "bearer",
secret_value, ttl_seconds: 300}` for an MCP credential; `force_refresh` on `mcp_oauth` runs the
leased refresh and re-checks scope before releasing the new token, and an `llm:*` alias selects
exactly one provider credential ([`auth-and-vaults.md`](../docs/managed-agents/auth-and-vaults.md#vault-binding-anthropic-aligned)).
Registry's answer is authoritative: `X-Orca-Credential-Id` and the startup `credential_ids` can
still constrain a caller-supplied header or an exact static destination but never pick the dynamic
destination's credential, so archive and replacement reach running sessions without a new token,
and harness-server leaves MCP credentials out of its runner configuration key, so a warm runner
survives a credential change.

#### LLM egress

| Path | Selection | Endpoint and credential | Provider key |
| --- | --- | --- | --- |
| `separate` Claude Agent SDK | `metadata.orca_llm_egress`, else `LLM_EGRESS_DEFAULT` (`direct` when unset) | `gateway`: `LLM_GATEWAY_URL` minus `/v1` as `ANTHROPIC_BASE_URL`, a JWT fetched per query as `ANTHROPIC_AUTH_TOKEN`, `X-Orca-Session-Id`; outcome evaluation too | `direct`: harness-server; `gateway`: gateway |
| `separate` Codex SDK, Pi SDK | same | `gateway`: `LLM_GATEWAY_URL` with `/v1` (the SDK appends `/responses`) or Pi's `/v1/proxy/<provider>/…`; a JWT per turn with at least 630 s left | same |
| cloud `colocated` sandbox | always `gateway`; metadata is validated but cannot select `direct` | `LITELLM_API_BASE` = `LLM_GATEWAY_URL`, `LITELLM_API_KEY` = JWT, `ANTHROPIC_CUSTOM_HEADERS` with `X-Orca-Session-Id`, `LITELLM_DEFAULT_MODEL` when pinned | gateway |
| Registry-owned runner | the Environment's `egress_mode`, `gateway` by default | snapshot `llm_base_url` = `AI_GATEWAY_LLM_URL` and `llm_jwt` (below) | gateway, or an operator passthrough |

harness-server mints model tokens for audience `ai-gateway` with no MCP server names, so they open
no MCP backend. sandbox-harness maps `LITELLM_API_BASE` to `ANTHROPIC_BASE_URL` (dropping `/v1`)
and the token to `ANTHROPIC_API_KEY` and `ANTHROPIC_AUTH_TOKEN` unless those are set. Registry
validates `orca_llm_egress` on session create, update and Trigger session templates; a change needs
an idle session and bumps the runtime revision, which rebuilds the runner. The gateway must pass
`anthropic-beta: fast-mode-2026-02-01`, `speed`, `output_config` and `usage.speed` through;
managed fast sessions fail closed against one that does not.

#### Token refresh

`SessionJwtProvider` (harness-server `src/mcp/session-jwt-provider.ts`) refreshes on demand with no
timers: it reuses a token until `exp − min(30 s, remaining ÷ 5)`, shares one mint among concurrent
callers, lets an abort cancel only its caller's wait, bounds each mint, token acquisition included,
with one 10 s deadline, rejects a token shorter than the caller's minimum validity, and never falls
back to an expired cache. Before each Claude SDK `query()` the harness replaces only
`Authorization` in a fresh MCP map, keeping routing headers, SDK settings and the built-in `orca`
server; a failed refresh refuses the query, and a running query keeps its header. The Codex and Pi
adapters set `Authorization` per MCP request; a cloud `colocated` sandbox gets one model token when
harness-server acquires it.

#### Registry-owned runners

`AgentSnapshotResolver` (registry `src/domain/agent-snapshot-resolver.ts`) mints in-process and
embeds a credential-free `egress` block built by `src/domain/credential-egress.ts`
([OIP-011](OIP-011-self-hosted-session-runner.md)). Unlike the harness-server path it uses
`AI_GATEWAY_MCP_URL` as given, sends `X-Orca-Vault-Id` when a vault's single active credential URL
equals a server URL exactly, and mints the MCP token with empty `credential_ids`. With
`AI_GATEWAY_LLM_URL` set, the `codex-sdk` and `pi-sdk` model token carries exactly the native route
and pinned model for at least 660 s, and resolution fails unless the policy admits both; other
providers get `aud: llm-proxy` for `AI_GATEWAY_LLM_JWT_TTL_SECS` (120 s) with no route or model
claims. A `gateway` session without `AI_GATEWAY_MCP_URL` fails with a configuration error.

## Changes by component

- **registry-service-ts**: minter and `mint-jwt`, both resolvers, LLM model policy,
  `orca_llm_egress` validation, per-route workload authorization, runner snapshot egress.
- **harness-server**: MCP rewrite, token provider, egress selection, sandbox gateway env.
- **sandbox-harness**: maps the gateway variables onto the Claude SDK's environment.
- **Helm chart**: renders the gateway and its configuration (validator, scope dimensions, resolvers,
  exact destinations ahead of `"*"`, ACL, audit sink); wires URLs, egress default and JWT keypair.

## Public-facing changes

### API

- Agent, Vault and credential shapes are unchanged: `mcp_servers[]` carries no credential
  reference, a credential binds through `auth.mcp_server_url`, and, as on Anthropic's platform, MCP
  calls reach upstreams with the credential injected outside the sandbox.
- Session `metadata.orca_llm_egress` accepts `"direct"` or `"gateway"` on create, update and Trigger
  session templates; another value is 400, a change on a non-idle session is 409, and the response
  shape is unchanged ([`orca-extensions.md`](../docs/managed-agents/orca-extensions.md)).
- The Environment `egress_mode` field ([OIP-011](OIP-011-self-hosted-session-runner.md)).

### Events and streaming

None.

### Wire protocols

- harness-server to Registry: `POST …/sessions/{id}/mint-jwt` with `mcp_server_names`, `vault_ids`
  (not read) and optional `audience` and `repo_urls`, returning `{token, expires_at}`.
- Callers to the gateway: MCP Streamable HTTP on `POST /v1/mcp` with the headers above, and the
  Messages, Responses, Chat and per-provider Pi routes with the session JWT in place of a provider
  key and `X-Orca-Session-Id`.
- Gateway to Registry, on the internal listener: both resolvers, plus the usage, guardrail-state
  and guardrail-bundle routes ([OIP-010](OIP-010-guardrails-pricing-and-spend.md)).

### Storage

None added. The boundary reads `sessions.vault_ids`, `sessions.mcp_servers`, `sessions.metadata`,
`vault_credentials.mcp_server_url` and `workspaces.organization_id`.

### Configuration

| Setting | Default | Read by | Effect |
| --- | --- | --- | --- |
| `SESSION_JWT_PRIVATE_KEY_PEM` | none | Registry | signing key; the chart requires both halves when it creates its Secret |
| `SESSION_JWT_ISSUER`, `_AUDIENCE`, `_TTL_SECS` | `orca-registry`, `ai-gateway`, `300` | Registry | token identity and lifetime |
| `SESSION_JWT_LLM_ROUTES`, `SESSION_JWT_LLM_MODELS` | unset: no LLM claims | Registry | comma lists, set together; an empty entry fails startup |
| `AI_GATEWAY_URL`, `LLM_GATEWAY_URL` | `http://localhost:8090`, `http://localhost:8090/v1` | harness-server | MCP base (`/v1/mcp` appended) and model base |
| `LLM_EGRESS_DEFAULT` | `direct` | harness-server | `separate` default; any other value fails startup |
| `AI_GATEWAY_MCP_URL`, `AI_GATEWAY_LLM_URL`, `AI_GATEWAY_LLM_JWT_TTL_SECS` | unset, unset, `120` | Registry | runner snapshot egress |
| `INTERNAL_AUTH_AI_GATEWAY_SUBJECT` | none | Registry | the gateway ServiceAccount allowed to call the resolvers |

Helm ([`kubernetes.md`](../docs/managed-agents/kubernetes.md)): `sessionJwt.*`; `harness.aiGatewayUrl`
(the in-chart Service by default); `harness.llmEgressDefault` (`direct`; `gateway` requires
`registry.aiGatewayLlmUrl` or an enabled gateway LLM provider); `registry.aiGatewayMcpUrl` and
`aiGatewayLlmUrl`; `aiGateway.enabled` (`false` requires `harness.aiGatewayUrl` and keeps the
gateway ServiceAccount authorized); `images.aiGateway.tag` (`v0.4.2-rc.1`); `aiGateway.scopeDims`
(six required dimensions, plus `agent_id` with `registryGuardrails`); `aiGateway.destinations`
(`"*"` reserved); `aiGateway.destinationResolver.*`; provider switches and `aiGateway.piProviders`
(keys via `aiGateway.extraEnv`); `aiGateway.audit.kafka.*` (`required: true`, `failureMode: deny`).
`LLM_GATEWAY_URL` and `AI_GATEWAY_LLM_URL` render only when `registry.aiGatewayLlmUrl` is set or a
gateway LLM provider is enabled; Registry's `AI_GATEWAY_MCP_URL`, only when a gateway URL is set or
a provider is enabled.

### Metrics, logs and traces

The engine adds no metric. The gateway writes one audit record per MCP call to
`orca.{workspace_id}.audit.ai-gateway` and serves metrics on its admin listener (port 9099 in the
local stack); Registry records a non-secret audit event for each `llm:*` credential resolution.

## Compatibility

Without a reachable gateway, harness-server has no direct MCP path, so MCP calls fail in the SDK's
client, and a failed mint or rewrite starts the session without remote MCP servers. `separate`
model calls default to `direct` and need no gateway. Sandboxes never receive a provider key; if
their token cannot be minted they start without gateway variables and their model calls fail. A
Registry-owned `gateway` session needs `AI_GATEWAY_MCP_URL`; `sidecar` needs no gateway.

### Upgrade

v0.5.0 is the first public release with this boundary; it adds no migration. A deployment needs the
session-JWT keypair and a gateway build with the wildcard `destination_resolver`, the HTTP resolver
`bearer_token_file` and JWT-derived route and model scope; the local stack's
`prepare-ai-gateway-image.sh` probes the first two. Existing Agents and credentials need no change.

### Rollback

Rolling back to a build without the boundary means running without it; the only persisted artifact
is the optional `orca_llm_egress` metadata key, which such code ignores. harness-server reads
`LLM_EGRESS_DEFAULT` at startup, so a changed default applies as replicas restart.

### Version skew

Upgrade Registry before harness-server and before the gateway configuration that points `"*"` at
its resolver; exact static destinations keep routing meanwhile. Claims are additive, and a gateway
that lists a dimension Registry does not mint denies that dimension rather than widening it. A
signing-key rotation invalidates outstanding tokens once the gateway stops trusting the old key;
tokens live at most 660 s, and `aiGateway.extraVolumes` can mount a second trust anchor meanwhile.

## Security considerations

- **Secrets leave Registry through one route.** In Kubernetes, TokenReview-verified ServiceAccount
  tokens let only the gateway call the two resolvers, and the Harness identity is refused
  (`src/auth/internal-auth.ts`); elsewhere one shared token authenticates every internal caller with
  no workload isolation ([`internal-traffic-auth.md`](../docs/operation/internal-traffic-auth.md)).
  Registry re-checks the session and its vaults before releasing a secret, and no reply carries a
  SecretStore reference.
- **Tokens, not keys, cross the boundary.** Registry keeps the signing key; harness-server and
  sandboxes hold tokens that expire in minutes, and a sandbox's model token names no MCP server.
  `org_id`, `vault_ids`, `credential_ids` and the LLM allowlists are never read from the request,
  and the gateway verifies `aud` (a `git-creds` token is refused with 401).
- **Destinations are data, not a bypass.** Registry resolves URLs only from persisted Agent and
  Session state and trusts the gateway to render resolver paths from verified claims. The gateway
  admits every resolved URL itself: loopback, link-local, metadata and private addresses are denied
  unless allowed, HTTPS is the default, and redirects are not followed (an upstream 3xx becomes
  502). Registry's own calls to user-supplied URLs (OAuth refresh, `mcp_oauth_validate`) go through
  `createGuardedFetch`.
- **Audit and reach.** The end-to-end suite checks that the per-call audit record carries neither
  the secret nor the upstream URL. `direct` egress keeps the provider key in harness-server, and
  `web_fetch` runs from the harness host, outside the boundary.

## Testing

- **Unit** (the required `test` job): harness-server `mcp-rewrite`, `session-jwt-provider`,
  `claude-harness-mcp-auth`, `dispatcher-session-llm-egress`, `dispatcher-mcp-toolsets`, `llm-env`;
  registry `session-jwt`, `mcp-destination`, `llm-policy`, `internal-auth`, `session-llm-egress`,
  `credential-egress`, `egress-credential-free`, `vault-credential-resolution` and both internal
  contract specs; the chart's `render.test.mjs` (scope dimensions, reserved `"*"`, egress default).
- **Integration**: registry `session-jwt` (database-derived `org_id`, caller values ignored) and
  `internal-mcp-destination`; harness-server `claude-sdk-mcp-auth-rotation`, a pinned-SDK probe.
- **End to end**: in `e2e-stack.yml`, `pnpm e2e:gateway` (`ai-gateway-mcp.spec.ts`) drives the real
  gateway image through credential injection, `mcp-session-id` propagation, idempotent replay, audit
  content, an ignored stale hint, replacement under an old token, missing auth, wrong audience, a
  forbidden backend, a literal `"*"`, blocked loopback, metadata and private destinations, a 502 for
  a redirect with no second request, and an unpinned model; `pnpm e2e:agent:sandbox` sends colocated
  model calls through an in-cluster gateway. `pnpm e2e:kind-helm` (`e2e-kind-helm.yml`) installs the
  gateway from its own chart and rotates the signing key and trust anchor.

## Alternatives

- **The earlier in-repo gateway.** An earlier design built the gateway in this repository with
  pluggable JWT validation, vault resolution and audit sinks, destinations in its YAML
  configuration, and a vault-level resolver. It was replaced by the external gateway image; the
  engine delegates egress to that separate component and keeps the contract here.
- **Proxy or environment interception** (`HTTP(S)_PROXY`, DNS rewrites, a patched `fetch`), rejected
  for the reasons under Motivation; config-time URL rewriting is the mechanism the SDK supports.
- **The first design's harness-side signing and gRPC resolver.** It had the harness sign with
  Registry's key; Registry signs in-process instead, so the key has one holder. It specified gRPC
  for credential resolution, which added nothing for one low-frequency call; HTTP/JSON stayed.
- **Destinations in gateway YAML.** Destination authority sat in static gateway configuration,
  apart from the Agent and Session records that declare each server; Registry now resolves every
  backend from those records, and exact static names remain operator overrides.
- **The startup header as credential authority** would pin an archived or replaced credential until
  restart; Registry's live answer wins instead. **Vault-level binding** (one vault, one target URL,
  `X-Orca-Vault-Id`) gave way to Anthropic's model: each credential binds by `auth.mcp_server_url`.
- **Tool federation** into one server with `server__tool` names; one client per backend matches
  Anthropic and keeps tool names ([`roadmap.md`](../docs/managed-agents/roadmap.md#deferred-by-design)).
- **Rotating `Authorization` inside a running query.** A pinned-SDK probe shows it reinitializes the
  remote MCP session and loses server-side context, so tokens renew before each query; in-query
  renewal is listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#production-hardening).
- **A separate LLM proxy for sandboxes.** The early local stack ran one; it was consolidated into
  the gateway, so one component and one token format carry both kinds of traffic.

## Status notes

How the design evolved, each step verified in current code:

- **LLM egress.** `separate` sessions first called the provider directly with harness-server's key
  while only sandboxes used the gateway; a per-session opt-in (`metadata.orca_llm_egress`) came
  next, then the deployment default `LLM_EGRESS_DEFAULT` (chart `harness.llmEgressDefault`), which
  metadata overrides in either direction and which stays `direct` when unset.
- **Claims, refresh, authority.** `org_id`, the LLM allowlists and the guardrail claims were added
  to the minter later. The harness first fixed one MCP token at runner start; tokens now refresh on
  demand before each query. `AI_GATEWAY_URL` became a base URL (an endpoint value still works).
  Destinations moved from gateway YAML to Registry, and the credential hint became advisory. The
  sandbox variables keep their `LITELLM_*` names from the separate-proxy design.
- **Gaps.** The runner's `aud: llm-proxy` token has no verifier in this repository's gateway
  configurations, which accept only the session-JWT audience. Cloud `claude_code` does not set
  `needsMcpRewrite`, so its sandbox gets no remote MCP servers, and its model token, minted once
  per sandbox, is not renewed while that sandbox lives.
