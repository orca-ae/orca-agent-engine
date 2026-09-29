# MCP Request Routing

> How MCP tool calls flow from the harness through `ai-gateway` to upstream MCP servers — without forking the Claude Agent SDK.

## How Anthropic does it (and why we need a different mechanism)

In Anthropic's hosted Managed Agents, `mcp_servers[]` lives on the Agent record (max 20, `type: "url"` only, Streamable HTTP transport). Vault credentials are **bound to `mcp_server_url`** — so when the platform's harness dials a server, it can match a credential and inject the bearer token. Per Anthropic's production cookbook: _"the agent calls tools on that server directly from inside the sandbox, with no round-trip through your application; Anthropic proxies the calls."_ The dial is sandbox-attributed but routed through Anthropic's egress where the token gets injected.

For our self-hosted version, we run the Claude Agent SDK ourselves in `harness-server`. The SDK's MCP types ([`claude-agent-sdk-python types.py:548-583`](https://github.com/anthropics/claude-agent-sdk-python/blob/main/src/claude_agent_sdk/types.py#L548-L583)) are:

```
McpHttpServerConfig { type:"http", url, headers }
McpSseServerConfig  { type:"sse",  url, headers }
McpStdioServerConfig { type:"stdio", command, args, env }
McpSdkServerConfig  { type:"sdk",  name, instance }
```

There is **no `fetch` / `dispatcher` / `httpAgent` hook**. `HTTP_PROXY` / `HTTPS_PROXY` are unreliable (Node's native `fetch` ignores them by default; [SDK issue #169](https://github.com/anthropics/claude-agent-sdk-typescript/issues/169) shows MCP SSE GET handshakes skip proxy config even when set). DNS rewrites break TLS SNI. Monkey-patching `globalThis.fetch` is fragile across SDK upgrades and contaminates outbound traffic to `api.anthropic.com`.

The only contract-supported mechanism — and the one used by every production MCP gateway in the wild (IBM ContextForge, Microsoft's MCP Gateway, Envoy AI Gateway, LangChain `MultiServerMCPClient`) — is **URL rewriting at config time**.

## Our mechanism

```
Agent record (in registry-service Postgres)
  mcp_servers: [
    { name: "github",   url: "https://api.githubcopilot.com/mcp/" },
    { name: "internal-pulsar", url: "https://pulsar-mcp.internal/mcp" }
  ]
  tools: [
    { type: "agent_toolset" },
    { type: "mcp_toolset", mcp_server_name: "github",
      default_config: { permission_policy: { type: "always_ask" } } }
  ]

                     │ harness-server reads, rewrites, hands to SDK
                     ▼
SDK config (passed via `mcpServers` to Claude Agent SDK query options):
  github: {
    type: "http",
    url: "https://ai-gateway.internal/v1/mcp",
    headers: {
      "X-Orca-Backend":   "github",
      "X-Orca-Session-Id": "ses_abc",
      "Authorization":    "Bearer <session-jwt>"
    }
  }
SDK tool policy:
  tools: []                  # remove Claude Code host built-ins
  disallowedTools: ["Bash", "Read", "Write", ...]
  settingSources: []         # ignore user/project settings allow rules
  strictMcpConfig: true      # ignore project/user MCP config
  canUseTool: managed-agent permission gate for mcp__<server>__<tool>
  internal-pulsar: { url: "https://ai-gateway.internal/v1/mcp",
                     headers: { "X-Orca-Backend": "internal-pulsar", ... } }

                     │ SDK opens MCP client; every JSON-RPC over Streamable HTTP
                     ▼
ai-gateway (external orca-ai-gateway image)
  • Validates session JWT (issued by registry; carries workspace, session_id,
    mcp_servers allowlist, vault_ids, credential_ids)
  • Resolves X-Orca-Backend through Registry's trusted, session-scoped
      POST .../mcp-destination/resolve endpoint
  • Resolves credential_id → real backend credential via
      Registry POST /internal/v1/workspaces/{workspace}/sessions/{session}/
        vault-credentials/{id}/resolve (HTTP/JSON; mesh-auth + binding check)
  • Forwards JSON-RPC envelope to upstream with Authorization header injected
  • Audits to Kafka (one record per call: workspace, session, backend, method, latency)

                     │
                     ▼
Real upstream MCP server (api.githubcopilot.com / pulsar-mcp.internal / …)
```

Three properties this gives us:

1. **Sandbox sees only the gateway URL.** Real backend URLs resolve from the pinned AgentVersion plus Session override in Registry; the sandbox doesn't know them. Agents can't accidentally leak an internal MCP server URL or admin endpoint.
2. **Vault credentials never reach the sandbox or the harness process.** They resolve at the gateway only.
3. **Policy-gated egress path.** The harness applies managed-agent tool permission policies via the SDK `canUseTool` callback before execution; every MCP call that is allowed then lands in the gateway for JWT allowlist validation, vault binding, and audit logging.

## Where the rewrite happens, exactly

`services/harness-server/src/mcp/rewrite.ts`. At session start (after the harness consumes the `session.start_request` event), it:

1. Calls the workspace/session-scoped `executions:prepare` endpoint and uses its immutable, Registry-validated Agent snapshot.
2. Uses the prepared snapshot's active credential metadata. Builds an initial normalized `mcp_server_url → credential_id` hint without resolving secret bytes in Harness.
3. For each `mcp_servers[]` entry, looks up an initial credential after lowercasing scheme/host and stripping default ports/trailing slashes. Builds an `McpHttpServerConfig` whose `url` is the gateway's MCP endpoint (`AI_GATEWAY_URL` with `/v1/mcp` appended unless it already ends in `/v1/mcp`), the JWT in `Authorization`, `X-Orca-Backend = mcp_servers[].name`, and `X-Orca-Credential-Id = matched_credential.id` only when a match exists. This header supports exact static Gateway overrides and policy metadata; wildcard Registry-backed dispatch never treats it as credential authority. Archive/delete/replacement changes therefore propagate without rebuilding the running MCP client.
4. Calls Registry's workspace/session-scoped internal `mint-jwt` endpoint (Registry signs with its static key; ~5 min TTL). Registry derives `org_id` from persisted `workspaces.organization_id` and `credential_ids` from persisted session vault bindings; neither value is read from or trusted from the mint request. The JWT carries derived `org_id`, `workspace_id`, `session_id`, `mcp_server_names`, `vault_ids`, derived `credential_ids`, `aud: ai-gateway`, and standard claims.
5. Passes the rewritten map and a session-scoped `SessionJwtProvider` to the harness. Before each new SDK `query()`, the harness calls `getValidToken()` and replaces only Authorization in a fresh map, preserving routing headers, SDK settings, and the built-in `orca` instance.

The provider refreshes on demand, not on a background timer. Its cache refresh point is
the JWT expiry minus the smaller of 30 seconds and one fifth of its remaining lifetime
when cached. Concurrent refresh callers share one mint; cancelling a caller cancels only
that caller's wait. The provider owns the single 10-second refresh deadline, including
workload-token acquisition and HTTP response reading. The Registry client observes that
same cancellation signal without starting another timeout. A failed refresh prevents a new query from starting with stale
headers. Stopping the harness closes the provider before joining pending query startup.
An accepted user interrupt during that wait ends the turn with an idle event, without
starting the SDK or reporting a refresh error; the shared mint remains usable by the next query.
An already-running SDK query keeps its existing MCP transport and Authorization header.

The agent's `mcp_toolset` `mcp_server_name` field continues to refer to the original logical name (`"github"`, `"internal-pulsar"`); the SDK's MCP client matches on `mcpServers` keys, which we kept identical. The rewrite is invisible to the agent code.

## What ai-gateway provides

The gateway treats `X-Orca-Backend` as an untrusted logical name. It renders
workspace and Session from verified JWT scope, then calls:

```http
POST /internal/v1/workspaces/{workspace_id}/sessions/{session_id}/mcp-destination/resolve
Authorization: Bearer <gateway-workload-token>
Content-Type: application/json

{"backend":"github"}

200 {"url":"https://api.githubcopilot.com/mcp/","credential_id":"vcrd_...","revision":4}
```

Registry resolves from one `REPEATABLE READ` snapshot: active workspace and
Session, pinned AgentVersion, full-replacement Session MCP override, active
Session Vaults, and active credentials. Duplicate backend names retain Harness
rewrite semantics (last entry wins). Credential binding normalizes scheme/host
case, strips default ports and trailing slashes, then applies deterministic
Session `vault_ids[]` order, credential `created_at`, and id; no match returns
`credential_id: null`. The declared destination URL is preserved for
forwarding. `revision` is a stable positive fingerprint of the exact forwarding
URL plus authoritative nullable credential id, so unrelated Session
metadata/runtime revisions do not invalidate Gateway idempotency replay.
Credential creation serializes normalized duplicate checks under the Vault row
lock. Pre-existing normalized conflicts within one Vault fail resolution with
`409 invalid_runtime_binding` rather than selecting an arbitrary credential.
Unknown/cross-scope or inactive workspace/Session returns
`404`; malformed persisted bindings return `409 invalid_runtime_binding`. Only
`http` and `https` destinations without userinfo or fragments are valid.

This removes destination URL authority from gateway YAML. JWT validation,
scope extraction, policy, secret resolution, idempotency, and audit remain
gateway responsibilities. See [`services/ai-gateway.md`](./services/ai-gateway.md).

Gateway config may retain exact-name static destinations as operator overrides;
routes list those first, then the reserved `"*"` dynamic destination. Exact
names win. Literal backend `*` is not routable. The dynamic resolver URL's
`{scope.workspace_id}` and `{scope.session_id}` placeholders come only from
verified JWT claims mapped into principal attributes, not caller headers.

`X-Orca-Credential-Id` is optional compatibility metadata. If omitted,
Registry's returned credential is still injected. A syntactically valid stale
value is ignored by wildcard dispatch; it never selects or constrains the
Registry result. Gateway authorizers may still validate caller-supplied header
values against the JWT's startup `credential_ids[]`, while the trusted
workspace/session-scoped Registry response authorizes the live dynamic binding.

Resolved URLs pass gateway SSRF admission. Default policy denies loopback,
link-local/cloud metadata, private, and special-use addresses. Operators may
allow named private MCP hosts explicitly. HTTPS is required by default;
plaintext HTTP needs the same explicit hostname allowlist used for
private/special resolution in local/service-mesh deployments. Forwarding
disables environment proxies, pins admitted DNS
addresses at connect time, and never follows redirects. Dynamic upstream `3xx`
responses become `502` rather than being relayed to callers. DNS
rebinding/pinning and proxy isolation are deterministic gateway unit tests;
managed-agents E2E covers Registry state, live credential replacement,
representative blocked destinations, and allowed-host redirect behavior without
inventing flaky DNS infrastructure.

For Streamable HTTP MCP, `ai-gateway` is responsible for upstream
`Accept: application/json, text/event-stream`, `mcp-session-id` request/response
propagation, and idempotency-cache replay of upstream response headers. The
managed-agents e2e suite treats those as gateway contract requirements even
though the implementation ships in the `ghcr.io/orca-ae/orca-ai-gateway` image.

## One MCP client per backend

For v1, we keep one logical SDK MCP client per backend (one `mcp_servers` entry → one gateway-rewritten entry). This matches Anthropic's behavior and keeps tool names un-namespaced. Collapsing N backends into a single federated MCP server with `__`-prefixed tool names (`github__create_issue`, `pulsar_admin__create_topic`) is recorded in [`roadmap.md`](./roadmap.md), driven by tool-list size in the model context.

Collapsing multiple backends into one federated MCP server is recorded in [`roadmap.md`](./roadmap.md) with the condition that would justify it.
