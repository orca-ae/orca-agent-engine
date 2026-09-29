# Internal Traffic Auth

**Status:** Resolved for v1; updated for deployments without a service mesh.

## Decision

Registry's internal listener always requires application-layer bearer
authentication. Public workspace keys and organization admin keys are never
accepted on this listener and are never forwarded by Harness.

Two deployment modes are supported:

| Mode                         | Intended deployment                    | Caller credential                                                     | Registry verification                                                                                                     |
| ---------------------------- | -------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `static_token`               | Docker Compose, bare VM, local process | One high-entropy service token shared by trusted internal callers     | Constant-time comparison with `INTERNAL_SERVICE_TOKEN` or a token reread from `INTERNAL_SERVICE_TOKEN_FILE`               |
| `kubernetes_service_account` | Kubernetes                             | Short-lived, audience-bound projected ServiceAccount JWT per workload | Kubernetes `TokenReview`, exact audience, and exact Harness, AI Gateway, or observability-exporter ServiceAccount subject |

Kubernetes is the chart default. Harness, AI Gateway, and the optional
observability-exporter identity have independent subjects. Harness, AI Gateway,
and the enabled exporter receive projected tokens with audience
`orca-registry-internal`; the kubelet rotates them and each caller rereads its
token file per request. Registry's ServiceAccount is bound to the built-in
`system:auth-delegator` ClusterRole so it can submit TokenReviews. Registry
authorizes each identity against explicit HTTP method-and-path capabilities.
Harness receives its execution, session, memory, credential, environment, and
runner capabilities. AI Gateway receives the vault-credential and MCP-destination
resolver capabilities. Both identities may authenticate to the session-usage
and guardrail-state routes. The usage handler then admits exactly one identity
from the immutable Session mode: Harness for `separate` and AI Gateway for
`colocated`. The observability exporter receives exactly the two Session-pinned
observability resolver capabilities:

- `POST /internal/v1/workspaces/{workspace}/sessions/{session}/agent-observability/context/resolve`
  returns non-secret context and never calls `SecretStore`.
- `POST /internal/v1/workspaces/{workspace}/sessions/{session}/agent-observability/secret/resolve`
  performs audited pre-send secret release. It reauthorizes current pinned
  authority and credential head before committing a success audit and returning
  one provider bundle; its opaque authorization ID is audit correlation, not a
  credential.

In `kubernetes_service_account` mode, an authenticated caller using a method and
path outside its explicit capability set receives `403`. Both paths set
`Cache-Control: private, no-store`
before authentication and body parsing. Registry caches only successful
TokenReview results, keyed by a SHA-256 token digest, for 30 seconds with a
32-entry bound. Rejections are never cached, so token rotation is not delayed;
the short positive TTL limits API-server load and transient outage amplification
while keeping revocation lag bounded.

The non-Kubernetes stack creates `services/dev/run/internal-service-token`
inside a host-only `0700` runtime directory when no operator-managed value is
supplied. The token file is `0644` so AI Gateway's non-root container user can
read the direct bind mount; other host users cannot traverse the private parent
directory. Registry, Harness, AI Gateway, and any observability exporter
consume that file. `static_token` identifies every authenticated caller only as
a shared trusted service: every trusted holder may call both observability
resolver routes, and this mode provides no workload isolation. Use Kubernetes
mode when individual workload identity or per-route capability isolation is required.

`/healthz`, `/readyz`, and `/metrics` remain unauthenticated for probes and
scraping. All other paths on the internal listener fail closed. A missing or
unreadable token file or an unavailable TokenReview API produces `503`; an
invalid credential produces `401`.

## Transport security and network policy

Application authentication does not encrypt the plain HTTP/JSON connection.
In production, Istio `STRICT` mTLS (or equivalent transport security) remains
recommended to prevent token disclosure on the network. Kubernetes
`NetworkPolicy` and Istio `AuthorizationPolicy` remain defense-in-depth and
should limit reachability to the expected ServiceAccounts. The Helm chart
configures application credentials and TokenReview RBAC; cluster-wide Istio
and network policies are platform concerns and are not installed by the chart.

## Other application credentials

- The public Registry listener authenticates `x-api-key` workspace keys and
  workspace OIDC tokens.
- The admin listener authenticates organization-scoped `orca_admin_...` keys
  and admin OIDC tokens on `/v1/organizations/*`. Its `/v1/platform/*` routes
  use independent `orca_platform_...` keys or `platform:admin` OIDC tokens
  with no organization scope.
- Registry mints session-scoped JWTs for Harness-to-AI-Gateway data-plane calls.
- AI Gateway resolves and injects vault credentials for outbound MCP calls.
- AI Gateway can write authoritative LLM usage and coalesced guardrail-state
  deltas to Registry when those Gateway plugins are configured.

These credentials are distinct trust domains. None substitutes for the
internal service credential.

## Configuration

Registry:

- `INTERNAL_AUTH_MODE=static_token|kubernetes_service_account`
- `INTERNAL_SERVICE_TOKEN` or `INTERNAL_SERVICE_TOKEN_FILE` (exactly one in
  static mode)
- `INTERNAL_AUTH_AUDIENCE`
- `INTERNAL_AUTH_HARNESS_SUBJECT`
- `INTERNAL_AUTH_AI_GATEWAY_SUBJECT`
- `INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT`
- `AI_GATEWAY_REGISTRY_USAGE_ENABLED` (default `false`; set `true` only when
  Gateway's Registry usage sink is configured)

Harness reads exactly one of `INTERNAL_SERVICE_TOKEN` or
`INTERNAL_SERVICE_TOKEN_FILE`. AI Gateway's Registry HTTP vault config uses
`bearer_token_file`, which is reread for every resolve request.

## What we do not do

- We do not pass through workspace API keys to internal calls.
- We do not use an organization admin key as a service credential.
- We do not mint ad-hoc JWTs from transcript events.
- Application processes do not load client certificates or implement mTLS.
