# Orca Extensions

This page separates Orca-only surfaces from the Claude Managed Agents-compatible
public API. These routes and headers are intentional compatibility-adjacent
extensions, not Claude API endpoints.

**Which operations are extensions is not decided here.** It is computed by
diffing our published spec against Anthropic's vendored one, and every operation
Anthropic does not publish carries the `orca-extension` tag in
`openapi/managed-agents.yaml`. See
[`api-groups-and-extensions.md`](./api-groups-and-extensions.md#extension-tagging)
for how the tag is produced and how it is pinned, and
[`conformance-matrix.md`](./conformance-matrix.md) for the current list with a
decision on every row. This page carries the _reasoning_ for the surfaces below;
it is not the register, and a surface described here without a matching
conformance row is a bug in one of the two.

[`api-groups-and-extensions.md`](./api-groups-and-extensions.md) is likewise the
authority on the URL model these classifications live in — core `/v1`, the
`/api/v1` alias, and named extension groups under `/apis/<group>/<version>`.
This page classifies individual surfaces within that model.

## Header behavior

| Header                                      | Classification        | Behavior                                                                                                                                                                                                                                                                                                              |
| ------------------------------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `anthropic-beta: managed-agents-2026-04-01` | Compatibility header  | Accepted and ignored. Declared on every public operation in `openapi/managed-agents.yaml` as a free-form `string`, never as Anthropic's value enum — publishing the enum would advertise that we recognise those values, and no value of this header changes request handling. It must not enable Orca-only behavior. |
| `orca-beta: managed-agents-<ver>`           | Orca extension header | Enables Orca-native response aliases and feature flags, such as returning `agent_toolset` instead of the dated SDK literal.                                                                                                                                                                                           |
| `Authorization: Bearer <oidc-jwt>`          | Orca deployment auth  | First-party/OIDC auth path for Orca deployments. Claude public examples use `x-api-key`; OIDC is not a Claude compatibility requirement.                                                                                                                                                                              |

## Route boundaries

| Surface                         | Classification          | Why                                                                                                                                                                                                                                                                                                |
| ------------------------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/v1/*`                         | Core route tree         | Canonical. Anthropic-published operations are compatibility core; Orca-owned operations available in every distribution are explicitly tagged `orca-extension`.                                                                                                                                    |
| `/api/v1/*`                     | Alias of the core API   | Rewritten to `/v1/*` before routing, so Kubernetes-symmetric tooling can pair it with `/apis/<group>/<version>`. One route tree, not two.                                                                                                                                                          |
| `/api`, `/apis`                 | Orca extension route    | Group/version discovery — core API versions and extension groups, not an operation list. Authenticated like the rest of the listener: the answer describes the deployment, so a caller with no key does not get it. The group list is the only supported way to test whether a group is available. |
| `/apis/*.runorca.ai/v1/*`       | Engine extension group  | Ships in this engine, so available on every deployment. `runtime.runorca.ai` serves the managed SDK harness catalog; `policy.runorca.ai` serves Guardrails; `pricing.runorca.ai` serves model prices.                                                                                                |
| `/apis/cloud.example.com/v1/*`  | Distribution extension group | Deployment-specific surfaces a downstream distribution serves under its own domain, not this engine.                                                                                                                                                                                     |
| Guardrail fields on core shapes | Orca extension behavior | `guardrail_ids` on the Agent and inside `agent_with_overrides` at session create, and `model` on session update, are accepted without `orca-beta`. Agent responses include `guardrail_ids` only when the request carries `orca-beta`. See [`guardrails.md`](./guardrails.md).                          |
| `/healthz`, `/readyz`           | Orca extension route    | Liveness/readiness probes. Published because they are served on the public listener; the one public surface not wrapped in the Claude error envelope.                                                                                                                                              |
| `/v1/platform/*`                | Orca extension route    | Deployment-wide organization/workspace provisioning on the admin listener. It requires a Platform principal with no organization scope.                                                                                                                                                            |
| `/v1/git-creds`                 | Orca extension route    | Session-JWT credential helper for in-sandbox Git operations. It is not reachable with normal workspace API keys.                                                                                                                                                                                   |
| `/v1/triggers*`                 | Orca Core extension     | Cron-only durable Trigger control plane implemented by Registry/Postgres in every distribution. It is not an alias for Anthropic Deployments.                                                                                                                                                      |
| `git_cred://<id>` input         | Orca extension syntax   | Binds a pre-provisioned workspace Git credential. Claude-compatible clients should send a raw write-only `authorization_token`.                                                                                                                                                                    |
| `/internal/*`                   | Internal workload API   | Harness, gateway, and output-indexer control-plane calls. Dedicated service workload auth gates access; public API credentials are rejected.                                                                                                                                                       |
| Sandbox harness HTTP routes     | Sandbox/internal API    | The `@orca/sandbox-harness` bridge runs inside or behind a sandbox boundary and is driven by `harness-server`; clients should use registry `/v1/sessions/*`.                                                                                                                                       |
| `orca-beta` feature aliases     | Orca extension behavior | Keeps first-party Orca names clean without changing SDK-compatible default wire shapes.                                                                                                                                                                                                            |
| Vault `auth.type=provider`      | Orca extension behavior | Requires `orca-beta` for public CRUD. Without opt-in, create rejects it, list filters it before pagination, and item routes return not found.                                                                                                                                                      |

## Git read proxy

`GET /v1/git-proxy/{resourceId}/info/refs?service=git-upload-pack` and
`POST /v1/git-proxy/{resourceId}/git-upload-pack` provide bounded read-only Git smart
HTTP to managed runners. A 900-second `git-proxy` JWT binds one active Session
resource, repository and credential revision. Registry resolves upstream PATs and
retains them inside its HTTPS connector; the capability cannot resolve raw PATs
through `/v1/git-creds`. The endpoints support protocol v2 `ls-refs` and reject
receive-pack, redirects and private-network destinations. See
[Registry Git read proxy](services/registry-service.md#git-read-proxy-public).

## Git credential helper

Claude-compatible `github_repository.authorization_token` accepts raw GitHub
PAT/App tokens and never returns them. Orca additionally accepts
`git_cred://<id>` on session create, attach, and token rotation. This syntax is
input-only and does not change the public resource response shape.

`POST /v1/git-creds` supports the custom `orca-git-creds` helper baked into
sandbox images. The helper sends a session-scoped JWT minted for
`aud=git-creds`; registry resolves the repository credential only for that
session/resource binding. PATs remain out of sandbox files and environment
variables.

This endpoint is public-listener reachable because Git invokes it from inside
the sandbox, but it is not a Claude Managed Agents endpoint and must be treated
as an Orca extension in conformance reports.

## Platform provisioning

`POST /v1/platform/organizations` and
`POST /v1/platform/organizations/{organization_id}/workspaces` are Orca
deployment-management extensions on the isolated admin listener. They use
independent Platform credentials and are not enabled by either Claude beta
headers or organization-admin authority. See
[`workspace-administration.md`](./workspace-administration.md).

## Cron Triggers

`/v1/triggers` is an Orca-owned Core extension. Registry evaluates a validated
five-field cron expression, creates a durable internal fire row, and atomically
hands the occurrence to the normal Session initial-event outbox. It introduces
no public run resource and no separate scheduler service. See
[`cron-triggers.md`](./cron-triggers.md).

## Session metadata filtering

### LLM egress selection

For `separate` Sessions that harness-server runs (`claude_agent_sdk`,
`codex_sdk`, and `pi_sdk`), Session metadata key `orca_llm_egress` accepts
`"direct"` or `"gateway"`. `"direct"` uses direct provider model egress, and
omitting the key applies harness-server's `LLM_EGRESS_DEFAULT` (`direct` when
unset). `"gateway"` uses the configured `LLM_GATEWAY_URL` and a Registry-minted
Session JWT for model requests, including Claude outcome evaluation.
Harness-server's `colocated` Sessions always use the Gateway. Create the Session with
`"metadata": { "orca_llm_egress": "gateway" }` to opt in. A metadata update can
change or remove the key only while the Session is idle; invalid values return
400 and a change to a running Session returns 409. This selection does not
change MCP routing or the Session response shape.
Registry must also have `SESSION_JWT_LLM_ROUTES` and `SESSION_JWT_LLM_MODELS`
configured with the Gateway route and permitted models; without these claims,
the Gateway's LLM allowlist denies the request.

`GET /v1/sessions?metadata_<key>=<value>` filters any Session metadata key by
exact, case-sensitive string equality. Multiple parameters use AND, for example
`metadata_AGENT_TRIGGER=local-trigger&metadata_team=ops`. Keys and values use
normal URL encoding; punctuation in a key is literal, not a nested JSON path.
A missing key does not match an empty value.

A request accepts at most 16 metadata filters. Keys contain 1–64 characters and
values contain at most 512 characters, matching the Session metadata limits.
Empty keys, repeated parameters, and oversized keys or values return 400.
Filtering is workspace-scoped, composes with the normal Session filters, and
applies before pagination and cursor validation. A cursor whose Session does
not satisfy all filters is invalid. The query parameters enable this extension
in either response dialect without `orca-beta` or a different response envelope.
The generated OpenAPI operation description documents the dynamic parameter
names; OpenAPI does not express a parameter-name wildcard.

An external control plane that keeps its own Trigger records puts the resource
name in `metadata.AGENT_TRIGGER` when invoking Orca. They query `/v1/sessions?metadata_AGENT_TRIGGER=<resource-name>` after
authorizing the local Trigger. Orca's `/v1/triggers/{id}/sessions` remains the
history of Orca-owned Trigger fire records. Metadata is editable Session data,
not proof of execution origin.

## Session files

Orca exposes a Session-nested view of the output files produced by that
Session:

- `GET /v1/sessions/{id}/files`
- `GET /v1/sessions/{id}/files/{file_id}`
- `GET /v1/sessions/{id}/files/{file_id}/content`
- `DELETE /v1/sessions/{id}/files/{file_id}`

These routes only expose active records whose `purpose` is `agent_output` and
whose `scope_id` is the addressed Session. A file from another Session or
workspace, and an ordinary user upload even if it carries the same `scope_id`,
is treated as not found. Content download additionally enforces the File's
`downloadable` flag. The list uses the top-level Files API's `limit`,
`after_id`, and `before_id` pagination shape; callers cannot override the scope.

Anthropic publishes the equivalent output lookup through
`GET /v1/files?scope_id={session_id}` rather than nested routes, so all four
operations are Orca extensions.

## <a id="environment-key-lifecycle"></a>Environment key lifecycle

An Environment carries a worker/host tunnel credential — the **Env Key**. Two
Orca-only routes manage its lifetime:

- `POST /v1/environments/{id}/rotate-key` issues a fresh key, returns it once,
  and atomically revokes the previous one (the new digest replaces the old).
- `POST /v1/environments/{id}/revoke-key` clears the armed key while keeping the
  Environment row, and is idempotent.

Anthropic's `BetaEnvironment` has no key concept, so neither route shadows a
published operation. Their nearest published analogue is the Tunnel resource's
`POST /v1/tunnels/{tunnel_id}/rotate_token` and `.../reveal_token` — the same
credential-on-a-resource shape, applied to a resource Orca does not serve.

The key's own fields are `orca-beta`-gated, for the same reason. The default
Environment response is Anthropic's `BetaEnvironment` projection exactly — `id`,
`type`, `name`, `description`, `metadata`, `config`, `scope` (only when set),
`archived_at`, `created_at`, `updated_at` — and carries no Orca field. An
`orca-beta` caller additionally receives `env_key_set` and `env_key_expires_at`
on every read, the raw `env_key` once on create, and the legacy flat
`packages`, `networking`, `image`, `target` plus `egress_mode` and `llm`.
`rotate-key` returns the raw key to any caller: its body is `{ env_key,
env_key_expires_at }`, an Orca shape with no Anthropic projection to stay
byte-compatible with. `revoke-key` answers with the environment instead, and so
is projected by the same rule as every other environment read — a caller who
wants to see `env_key_set` flip to `false` sends `orca-beta` there too.

Storage and verification are described in
[`services/registry-service.md`](./services/registry-service.md#environment-worker-tunnel-auth--durable-claims).

## <a id="thread-interrupt"></a>Thread interrupt

`POST /v1/sessions/{id}/threads/{thread_id}/interrupt` appends a `user.interrupt`
to the addressed thread. Anthropic publishes five thread operations — list, get,
`POST /archive`, `GET /events`, `GET /stream` — and no interrupt route, so this
one is machine-labelled an Orca extension.

It is a REST alias, not a capability. Anthropic models interrupt as a **message
type**: `user.interrupt` on `POST /v1/sessions/{id}/events`, which is also the
primary mechanism here. That event is what this route appends, what the harness
acts on, and what a Claude-compatible client sends without knowing the route
exists. The route shadows no Anthropic path and adds nothing a caller cannot
already express, so it is kept as a convenience over the published event — the
event, not the route, is the contract.

## <a id="environment-work-stats"></a>Environment work stats

`GET /v1/environments/{id}/work_stats` reports an Environment's `self_hosted`
distribution backlog as `{ depth, in_flight, worker_connected }`. What each
field counts is in
[`services/registry-service.md`](./services/registry-service.md#environment-worker-tunnel-auth--durable-claims).

Unlike every other surface on this page, it is **not** a settled extension.
Anthropic already publishes this capability, at
`GET /v1/environments/{environment_id}/work/stats`, returning
`BetaSelfHostedWorkQueueStats`: `depth`, `oldest_queued_at`, `pending`, `type`
and `workers_polling`, all five required. Orca's path spells the last segment
`work_stats` rather than `work/stats`, so the differ sees a distinct operation
rather than a divergent one. Of the fields, only `depth` overlaps by name, and
even it is counted differently — Anthropic's is consumer-group lag on a Redis
stream, ours is a `COUNT(*)` of pending sessions with no runner minted. Two of
Anthropic's required fields have no Orca counterpart, and two Orca fields have
no Anthropic counterpart.

Its conformance row is therefore recorded `fix-later`, not `keep`: a route that
shadows a published operation under a different name is a divergence, not an
extension. Renaming it would move the row from `extension` to `core` and change
which decision accounts for the rest of that path family, which is a product
call — see [`roadmap.md`](./roadmap.md#environment-work-stats-path-and-shape).

## Internal registry routes

`/internal/*` routes support harness and gateway operations such as credential
resolution, session state transitions, usage accounting, output-file upload,
memory version registration, and session JWT minting. They are not part of the
Claude-compatible public API and should not appear in public SDK examples.

## Documentation rule

Docs and tests should label every surface as one of:

- **Claude-compatible public API** — `/v1/*` routes intended to mirror Claude
  Managed Agents. Available on every deployment.
- **Orca extension** — public-listener route or header that exists only for
  Orca runtime behavior, and ships in this engine. Available on every
  deployment.
- **Engine extension group** — `/apis/<group>.runorca.ai/<version>/*` route
  shipped by this engine, so available on every deployment. Distinct from an
  Orca extension only in living under a named, independently versioned group
  rather than on the core path.
- **Cloud extension** — `/apis/<group>/<version>/*` route served by a
  distribution built on this engine, not by the engine itself. Availability is
  deployment-specific.
- **Internal workload API** — `/internal/*` control-plane route.
- **Sandbox implementation API** — sandbox harness bridge and runtime-local
  routes.

Availability of **any** group — engine or cloud — is discovered via `/apis`.
Docs and clients must not infer it from the deployment kind.

For an operation, the label is not a judgement call: it is whether the operation
appears in Anthropic's vendored spec, computed by
`scripts/orca-extension-tagging.mjs` and readable from the `orca-extension` tag
in `openapi/managed-agents.yaml`. Prose that disagrees with the tag is wrong.

Docs must not present a cloud extension as something a self-hosted reader can
call. See [`api-groups-and-extensions.md`](./api-groups-and-extensions.md) for
the tier definitions and the promotion path when an extension becomes core.
