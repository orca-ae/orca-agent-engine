# registry-service-ts

Anthropic-compatible REST control plane. Public-facing service. Owns Postgres metadata.

See `docs/managed-agents/services/registry-service.md` for the full design.

## Run locally

```bash
make dev-up                                                            # Postgres + Kafka + RustFS
pnpm -F @orca/registry-service-ts db:generate                          # generate migration if schema changed
DATABASE_URL=postgres://orca:orca@localhost:5432/registry \
  pnpm -F @orca/registry-service-ts registry:bootstrap-admin           # one-time org/workspace/admin key
# Later admin-key rotation: set ORCA_ADMIN_ORGANIZATION_ID and run registry:create-admin-key.
# Optional ORCA_ADMIN_KEY_SCOPES is a comma-separated delegated scope list;
# it defaults to org:admin when unset.
INTERNAL_AUTH_MODE=static_token \
  INTERNAL_SERVICE_TOKEN=development-internal-service-token-32chars \
  DATABASE_URL=postgres://orca:orca@localhost:5432/registry \
  pnpm -F @orca/registry-service-ts dev                                # public :8080, internal :8081, admin :8082
DATABASE_URL=postgres://orca:orca@localhost:5432/registry \
  pnpm -F @orca/registry-service-ts test:integration                   # full integration suite
make dev-down
```

## Endpoints

| Path                                                                                         | Purpose                                                                                                                                                                                                                                                                                                     |
| -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POST /v1/agents` (+ CRUD)                                                                   | Versioned. Skill selectors are retained; Sessions resolve them to exact immutable versions.                                                                                                                                                                                                                 |
| `POST /v1/environments` (+ CRUD)                                                             | Mutable.                                                                                                                                                                                                                                                                                                    |
| `GET /v1/environments/{id}/work_stats`                                                       | `self_hosted` distribution backlog: `depth` (pending, no runner) + `in_flight` (launch-in-flight + running) + `worker_connected`. Cloud envs report zeros. `worker_connected` is durable-claim heartbeat liveness ("a worker claim is live somewhere"), not per-replica launch-readiness.                   |
| `POST /v1/sessions` (+ CRUD)                                                                 | Metadata/status lifecycle; events, `/events/stream`, resources, usage, and stats are implemented.                                                                                                                                                                                                           |
| `GET /v1/sessions/{id}/threads` (+ get/events/stream, `POST .../{thread_id}/archive`)        | Thread projection over the session event log; each thread has its own event list and SSE stream. There is no thread-create route — a thread appears when an event targets a new subpath.                                                                                                                    |
| `POST /v1/skills` (+ CRUD)                                                                   | Immutable multipart bundles; metadata in Postgres and exact bytes in `@orca/skill-store`.                                                                                                                                                                                                                   |
| `POST /v1/files` (+ upload/download/list/delete)                                             | Content-addressed uploads via `@orca/file-store`; `purpose`, `scope_id`, `downloadable`.                                                                                                                                                                                                                    |
| `POST /v1/memory_stores` (+ nested memories and memory_versions)                             | Path-addressed memories with an immutable version chain via `@orca/memory-store`.                                                                                                                                                                                                                           |
| `POST /v1/vaults` (+ CRUD, nested credentials)                                               | Stores vault metadata only; runtime secrets live in vault credentials / git credentials.                                                                                                                                                                                                                    |
| `GET /v1/sessions/{id}/outcome`                                                              | Returns the Session's most recent outcome evaluation, folded from `span.outcome_evaluation_end` events, or `null` when none exists.                                                                                                                                                                         |
| `POST /internal/v1/workspaces/{ws}/sessions/{ses}/executions:prepare`                        | harness-server only. Resolves one immutable, workspace-validated execution snapshot.                                                                                                                                                                                                                        |
| `POST /internal/v1/workspaces/{workspace}/sessions/{session}/vault-credentials/{id}/resolve` | ai-gateway only. Resolves a credential bound to that active Session. Gated by the internal-listener bearer token — a TokenReview-checked ServiceAccount JWT under `kubernetes_service_account`, or a shared secret under `static_token`, which authenticates the cluster rather than the individual caller. |
| `POST /internal/v1/workspaces/{workspace}/sessions/{session}/usage`                          | Applies an idempotent, additive LLM usage delta and returns updated usage/guardrail state. Harness is authoritative by default; when `AI_GATEWAY_REGISTRY_USAGE_ENABLED=true`, ai-gateway owns `colocated` Sessions and Harness still owns `separate` Sessions.                                             |
| `POST /internal/v1/workspaces/{workspace}/sessions/{session}/guardrail-state`                | Harness and ai-gateway. Applies an ordered batch of durable guardrail-state deltas.                                                                                                                                                                                                                         |
| `POST /internal/v1/workspaces/{ws}/sessions/{ses}/git-credentials/{id}/resolve`              | harness-server only. Resolves a session-attached Git credential.                                                                                                                                                                                                                                            |
| `POST /v1/git-creds`                                                                         | Orca extension. Session-JWT Git credential helper; not Claude-compatible public API.                                                                                                                                                                                                                        |
| `GET/POST /v1/organizations/*` (admin listener only)                                         | Organization workspaces and workspace API keys; see workspace-administration.md.                                                                                                                                                                                                                            |
| `POST /v1/platform/organizations[/{organization_id}/workspaces]` (admin listener only)       | Platform Admin provisioning; creates organizations or workspaces under an active organization.                                                                                                                                                                                                              |

### Admin observability state

The admin listener alone serves these organization-scoped routes:

```http
GET /v1/organizations/agent_observability
PUT /v1/organizations/agent_observability
POST /v1/organizations/agent_observability:disable
POST /v1/organizations/agent_observability:rotate_credentials
GET /v1/organizations/workspaces/{workspace_id}/agent_observability
PUT /v1/organizations/workspaces/{workspace_id}/agent_observability
POST /v1/organizations/workspaces/{workspace_id}/agent_observability:rotate_credentials
```

The GET routes require `observability:read` or `org:admin`, return opaque strong
`ETag` headers with `Cache-Control: private, no-store`, and never call
SecretStore or expose its references or material. PUT requires
`observability:write` or `org:admin` and a trimmed `Idempotency-Key`. A fresh
workspace PUT requires an exact strong workspace `If-Match`; completed same-key
replay follows durable replay semantics before current precondition evaluation.
Its strict
mode-discriminated body permits only `{ mode, capture_ceiling }` for `inherit`
or `disabled`, while `custom` supplies a workspace-owned target/config and
credentials only when creating or replacing that target. Custom creation or
target replacement stages write-only credentials in SecretStore and returns
`201`/`200`; same-target policy replacement omits credentials and creates a new
immutable config version. Inherit and disabled make no SecretStore call.
Every mutation response uses `Cache-Control: no-store`, and successful
responses contain no secret bytes or refs.

Credential rotation requires `observability:rotate` or `org:admin`, an
`Idempotency-Key`, and an exact strong `If-Match`. Its strict body contains only
write-only OTLP `credentials`; it cannot select a target or submit config,
adapter, version, binding, or `secret_ref`. The organization route applies only
to the current active organization-owned `otlp_http` default; the workspace
route applies only to the current active workspace-owned `otlp_http` custom
binding. Both advance only that binding's credential generation and return
authoritative `200` state with `Cache-Control: no-store`.
They leave target/config/selection/revocation/capture state and Session pins
unchanged. Completed workspace retries validate and replay their historic state
after later custom-target replacement, inherit, or disabled updates; all
workspace paths are first constrained to the active authenticated organization.
The old ref enters durable maintenance cleanup only after the head CAS, never
inline. Explicit workspace disabled can preempt only its exact pending binding
rotation; an archive winner returns `404` after staging cleanup handoff.

Disable requires `observability:write` or `org:admin`, a trimmed
`Idempotency-Key`, and strict `{}`. `If-Match` is optional: a supplied exact
strong ETag is checked after completed replay and before emergency fencing;
without it, disable clears the current organization default. It fences only
pending mutations on that organization setting and selected organization binding.
It clears the pointer and advances selection/default-revocation epochs; only an
active selected binding becomes `draining`. Degraded selected defaults remain
removable when their credential head is missing or their binding is already
`draining`, `disabled`, or `archived`. Disable never calls SecretStore, retains
the selected credential head, and leaves a preempted staged PUT or rotation to
normal durable staging cleanup. It does not change workspace-custom settings,
Session pins, target/configuration, or binding revocation state. Successful and
error responses use `Cache-Control: no-store` and never expose secret material
or references.

This standalone admin contract lives in
`src/contracts/agent-observability.contract.ts`; it is intentionally outside
`publicContract`. [`openapi/managed-agents.yaml`](openapi/managed-agents.yaml)
is generated from `publicContract` by `pnpm openapi:gen`.
`pnpm conformance:gen` diffs it against Anthropic's vendored spec into
[`docs/managed-agents/conformance-matrix.md`](../../docs/managed-agents/conformance-matrix.md),
requiring an explicit decision for every difference. Both artifacts are checked
in and CI fails if regenerating them produces a diff, so edit the contracts —
never the artifacts.

### Internal observability resolution

The canonical internal listener alone mounts these Session-scoped routes:

- `POST /internal/v1/workspaces/{workspace}/sessions/{session}/agent-observability/context/resolve`
- `POST /internal/v1/workspaces/{workspace}/sessions/{session}/agent-observability/secret/resolve`

Under `kubernetes_service_account`, the configured `observability-exporter`
identity can call both routes and no Harness or AI Gateway route. The Helm chart
renders its dormant ServiceAccount identity for Registry subject verification.
It does not render an exporter Pod or projected token mount.

The context resolver loads the exact pinned binding version under
repeatable-read locks, classifies current lifecycle/revocation/platform policy,
clamps capture mode against current ceilings, and returns no secret material or
references. The secret resolver is the audited pre-send credential release
boundary. Every outcome, including authentication, parsing, not-found, and
availability failures, uses `Cache-Control: private, no-store`.

## Authentication

- `x-api-key: orca_…` (Anthropic-SDK compat) — argon2id-hashed, stored in `api_keys`.
- `Authorization: Bearer <jwt>` (first-party clients) — OIDC verified against per-workspace allowed-issuers.
- Admin listener: `x-api-key: orca_admin_…`, or a separately configured OIDC token with `org:admin`.
- `orca-beta: managed-agents-<ver>` — opts into Orca extensions (e.g., `agent_toolset` cleaner tool name).
- `anthropic-beta: managed-agents-2026-04-01` — accepted/ignored for SDK compatibility unless route docs say otherwise.

See [`docs/managed-agents/orca-extensions.md`](../../docs/managed-agents/orca-extensions.md) for the boundary between Claude-compatible public API, Orca extensions, mesh-internal routes, and sandbox implementation APIs.

## Idempotency

Every write endpoint accepts `Idempotency-Key`. Cached for 24h. Replays return the cached body bit-for-bit. Conflict (same key, different body) → 409.

## Listeners, JWTs, and admin auth

| Var                                 | Default                     | Meaning                                                                                                                             |
| ----------------------------------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `HTTP_PORT`                         | `8080`                      | Public listener — the Anthropic-compatible `/v1/*` surface                                                                          |
| `INTERNAL_HTTP_PORT`                | `8081`                      | Internal listener — `/internal/v1/workspaces/{ws}/…` only                                                                           |
| `ADMIN_HTTP_PORT`                   | `8082`                      | Admin listener — organization and platform provisioning                                                                             |
| `GRPC_PORT`                         | `50054`                     | Parsed into config and never used — no gRPC server is constructed, and only the three Fastify listeners above bind a port           |
| `ADMIN_OIDC_ALLOWED_ISSUERS`        | (unset)                     | Comma-separated issuers accepted on the admin listener                                                                              |
| `ADMIN_OIDC_AUDIENCE`               | `orca-managed-agents-admin` | Required `aud` for admin OIDC tokens                                                                                                |
| `SESSION_JWT_ISSUER`                | `orca-registry`             | `iss` on session-scoped JWTs minted for ai-gateway and git-creds                                                                    |
| `SESSION_JWT_AUDIENCE`              | `ai-gateway`                | Default `aud` on session-scoped JWTs                                                                                                |
| `SESSION_JWT_TTL_SECS`              | `300`                       | Session-JWT lifetime. Shorter limits blast radius; too short breaks long tool calls                                                 |
| `TRIGGER_SCHEDULER_ENABLED`         | `true`                      | Runs the in-process cron planner and dispatcher. Set `false` for API-only replicas that should serve requests but not fire Triggers |
| `TRIGGER_RECONCILE_INTERVAL_MS`     | `5000`                      | How often the planner looks for due Trigger slots                                                                                   |
| `TRIGGER_RECONCILE_BATCH_SIZE`      | `100`                       | Maximum Trigger rows one reconcile pass claims with `FOR UPDATE SKIP LOCKED`                                                        |
| `SESSION_JWT_PRIVATE_KEY_PEM`       | (required to mint)          | RSA private key used to sign session JWTs. Never log or echo it                                                                     |
| `OIDC_AUDIENCE`                     | `orca-managed-agents`       | Required `aud` for public-listener OIDC tokens                                                                                      |
| `INTERNAL_AUTH_MODE`                | (inferred)                  | `static_token` or `kubernetes_service_account` for the internal listener                                                            |
| `KUBERNETES_SERVICE_HOST`           | (set by Kubernetes)         | Presence is how the registry detects it is running in-cluster                                                                       |
| `NODE_ENV`                          | (unset)                     | `ORCA_SECRET_STORE_MODE=local` is refused unless this is `development` or `test`                                                    |
| `ORCA_SECRET_STORE_K8S_NAMESPACE`   | (required for `kubernetes`) | Namespace holding the secret-store Secret                                                                                           |
| `ORCA_SECRET_STORE_K8S_SECRET_NAME` | `registry-secret-store`     | Name of that Secret                                                                                                                 |
| `DATABASE_POOL_MAX`                 | `10`                        | Postgres pool size for registry metadata                                                                                            |
| `FILESTORE_POOL_MAX`                | `10`                        | Postgres pool size for file metadata                                                                                                |
| `MEMORYSTORE_POOL_MAX`              | `10`                        | Postgres pool size for memory metadata                                                                                              |
| `TRANSCRIPT_STORE_POOL_MAX`         | `10`                        | Postgres pool size for the transcript backend                                                                                       |
| `KAFKA_SASL_MECHANISM`              | (unset)                     | SASL mechanism for `KAFKA_CONNECTION_MODE=custom`                                                                                   |
| `PRICE_REFRESH_URL`                 | (unset)                     | `http(s)` catalog the model-price refresher pulls from. Unset disables the refresher; seed and operator rows still apply            |
| `PRICE_REFRESH_INTERVAL_MS`         | `21600000`                  | How often that pull runs. Rejected below `60000`, so a misconfiguration cannot hammer the upstream catalog                          |
| `PRICE_REFRESH_PROVIDER`            | `anthropic`                 | Which provider `PRICE_REFRESH_URL` serves. One feed prices one vendor, and a refresh replaces only that vendor's `upstream` rows    |

`kubernetes_service_account` requires distinct
`INTERNAL_AUTH_HARNESS_SUBJECT`, `INTERNAL_AUTH_AI_GATEWAY_SUBJECT`, and
`INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT` values.

`OIDC_ALLOWED_ISSUERS` and `PLATFORM_OIDC_ALLOWED_ISSUERS` are the public- and
platform-listener equivalents of `ADMIN_OIDC_ALLOWED_ISSUERS`. All three are
deployment-global lists, not per-workspace.

## Kafka transcript configuration

When `TRANSCRIPT_STORE_BACKEND=kafka`, the registry constructs a KafkaJS client from the same env surface as harness-server:

| Var                            | Default                   | Meaning                                                                              |
| ------------------------------ | ------------------------- | ------------------------------------------------------------------------------------ |
| `KAFKA_BROKERS`                | `localhost:9092`          | Comma-separated bootstrap brokers                                                    |
| `KAFKA_CLIENT_ID`              | `registry-service-ts`     | Kafka client identifier                                                              |
| `KAFKA_CONNECTION_MODE`        | `plaintext`               | `plaintext`, `sasl-plain-token-tls`, `sasl-plain-tls`, or `custom`                   |
| `KAFKA_AUTH_TOKEN`             | (unset)                   | Token/JWT for the `sasl-plain-token-tls` and `sasl-plain-tls` modes                  |
| `KAFKA_SASL_*` / `KAFKA_SSL_*` | (unset)                   | Custom plain SASL/TLS knobs; see `services/dev/.env.example`                         |
| `KAFKA_TOPIC_PREFIX`           | (unset = bare names)      | Topic prefix as dot-terminated segments (e.g. `public.default.` for Kafka-on-Pulsar) |
| `PULSAR_SERVICE_URL`           | `pulsar://localhost:6650` | Broker URL when `TRANSCRIPT_STORE_BACKEND=pulsar`                                    |

These settings only affect registry/harness transcript clients. The external ai-gateway image owns its own Kafka audit sink configuration.

### Kafka transcript Avro and Schema Registry

Kafka defaults to raw payload bytes in `.events` topics. `KAFKA_TRANSCRIPT_ENCODING=avro`
selects `.events-avro` for all session reads and writes; a Registry URL alone leaves
raw topic selection unchanged, even though the codec can decode Avro frames.
Registry, harness and exporter must use the same encoding and undergo a full
coordinated restart after existing turns are quiesced. This incompatible cutover
does not migrate old history or preserve existing session replay/cursors; create
fresh sessions. These settings are host-service configuration, never worker,
session-runner, or sandbox credentials.

| Variable                                   | Default                           | Behavior                                                                                  |
| ------------------------------------------ | --------------------------------- | ----------------------------------------------------------------------------------------- |
| `KAFKA_TRANSCRIPT_ENCODING`                | `raw`                             | Selects the raw `.events` or Avro `.events-avro` topic set; Avro requires a Registry URL. |
| `KAFKA_SCHEMA_REGISTRY_URL`                | unset                             | Confluent-compatible endpoint, including an optional path prefix.                         |
| `KAFKA_SCHEMA_REGISTRY_SUBJECT`            | `orca.transcript.TranscriptEvent` | Shared deployment-level subject across session topics.                                    |
| `KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER`      | `true`                            | Avro writers register the shipped schema; false performs exact-schema lookup.             |
| `KAFKA_SCHEMA_REGISTRY_AUTH_MODE`          | `none`                            | `none` or `basic`; credentials require HTTPS.                                             |
| `KAFKA_SCHEMA_REGISTRY_USERNAME`           | unset                             | Basic username, supplied from a Secret.                                                   |
| `KAFKA_SCHEMA_REGISTRY_PASSWORD`           | unset                             | Basic password, independent of broker SASL credentials.                                   |
| `KAFKA_SCHEMA_REGISTRY_CA_FILE`            | unset                             | Custom CA PEM path in this service container.                                             |
| `KAFKA_SCHEMA_REGISTRY_CERT_FILE`          | unset                             | mTLS certificate PEM; requires the key file.                                              |
| `KAFKA_SCHEMA_REGISTRY_KEY_FILE`           | unset                             | mTLS private-key PEM; requires the certificate file.                                      |
| `KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS` | `5000`                            | Positive per-request timeout in milliseconds.                                             |

Registry companion settings without a URL, Avro without a URL, invalid enums,
and enabled Kafka settings with a non-Kafka backend fail configuration validation.
TLS certificate validation is always enabled. Local unauthenticated HTTP is allowed;
URL userinfo, query strings and fragments are rejected. Basic requires both credentials;
certificates and keys are paired. File paths require explicit read-only mounts via the
service's Helm `extraVolumes` / `extraVolumeMounts`; paths do not create mounts.

See [Kafka transcript encoding, rollout and external consumers](../../docs/managed-agents/libraries/transcript-store.md#optional-kafka-avro-envelope).

## Offline bootstrap and key rotation

Three entrypoints run against the database directly, never through the HTTP
surface: possession of Registry database credentials _is_ the one-time
installation bootstrap authority, so the server itself never reads these.

`pnpm -F @orca/registry-service-ts registry:bootstrap-admin` (`src/bootstrap-admin.ts`) creates the first organization,
workspace, admin key, and platform key:

| Var                                | Default                  | Meaning                                                                                                                          |
| ---------------------------------- | ------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                     | _(required)_             | Postgres connection string. The command throws without it.                                                                       |
| `ORCA_BOOTSTRAP_ORGANIZATION_NAME` | `Default organization`   | Display name for the organization row it creates.                                                                                |
| `ORCA_BOOTSTRAP_ORGANIZATION_ID`   | a fresh `org_…`          | Use an explicit id instead of minting one — for reproducible installs.                                                           |
| `ORCA_BOOTSTRAP_WORKSPACE_NAME`    | `Default workspace`      | Display name for the workspace row.                                                                                              |
| `ORCA_BOOTSTRAP_WORKSPACE_ID`      | a fresh `wrkspc_…`       | Explicit workspace id, same rationale.                                                                                           |
| `ORCA_BOOTSTRAP_ADMIN_KEY_NAME`    | `Bootstrap admin key`    | Label stored against the admin key.                                                                                              |
| `ORCA_BOOTSTRAP_ADMIN_API_KEY`     | generated                | Supply the plaintext admin key rather than generating one. Must match `orca_admin_` + ≥32 URL-safe chars, or the command throws. |
| `ORCA_BOOTSTRAP_PLATFORM_KEY_NAME` | `Bootstrap platform key` | Label stored against the platform key.                                                                                           |
| `ORCA_BOOTSTRAP_PLATFORM_API_KEY`  | generated                | Supply the plaintext platform key. Must match `orca_platform_` + ≥32 URL-safe chars.                                             |

`pnpm -F @orca/registry-service-ts registry:create-admin-key` (`src/create-admin-key.ts`) mints a replacement admin key:

| Var                          | Default             | Meaning                                                                       |
| ---------------------------- | ------------------- | ----------------------------------------------------------------------------- |
| `ORCA_ADMIN_ORGANIZATION_ID` | _(required)_        | Organization the new key belongs to. The command throws without it.           |
| `ORCA_ADMIN_KEY_NAME`        | `Rotated admin key` | Label for the new key.                                                        |
| `ORCA_ADMIN_KEY_SCOPES`      | `org:admin`         | Comma-separated scopes, parsed by `parseAdminApiKeyScopes`.                   |
| `ORCA_ARCHIVE_ADMIN_KEY_ID`  | _(unset)_           | Archive this key id in the same transaction, so rotation is atomic.           |
| `ORCA_NEW_ADMIN_API_KEY`     | generated           | Supply the plaintext instead of generating it; same prefix rule as bootstrap. |

`pnpm -F @orca/registry-service-ts registry:create-platform-key` (`src/create-platform-key.ts`) is the platform-key equivalent:

| Var                            | Default                | Meaning                                        |
| ------------------------------ | ---------------------- | ---------------------------------------------- |
| `ORCA_PLATFORM_KEY_NAME`       | `Rotated platform key` | Label for the new key.                         |
| `ORCA_ARCHIVE_PLATFORM_KEY_ID` | _(unset)_              | Archive this key id in the same transaction.   |
| `ORCA_NEW_PLATFORM_API_KEY`    | generated              | Supply the plaintext instead of generating it. |

## Skills progressive disclosure

See [`docs/managed-agents/skills.md`](../../docs/managed-agents/skills.md). At
Session creation, Registry resolves the primary agent and its direct
coordinator roster and persists exact SkillVersion bindings. Nested
coordinators are rejected because the Harness runtime supports one roster
level. Harness then verifies and materializes those immutable bundles under
`/workspace/skills/<name>/`.

The runtime prompt receives only each Skill's name, description, and
`SKILL.md` path. The model opens the entrypoint and any referenced resources
with its normal sandbox tools when relevant. Skill instructions are never
concatenated into the system prompt, and Skills neither grant nor restrict
tools.

Deleting the final public SkillVersion is allowed and leaves
`latest_version: null`. Session-bound versions remain as execution-only
tombstones. Unbound bundle deletion is tracked transactionally in Postgres and
retried against SkillStore at startup and on a fixed interval.

## Limitations and deferrals

- API-key lookup selects a row by a domain-separated SHA-256 fingerprint (`fingerprintApiKey`); argon2 remains the verifier. Keys issued before fingerprinting still fall back to a scan over non-revoked rows.
- Session update accepts `title`, `metadata`, and `agent.tools` / `agent.mcp_servers` / `agent.model`; `vault_ids` is parsed for Claude compatibility but every attempt to set it is rejected.
- Vault DELETE doesn't check session references; archive is the recommended lifecycle path.
- The Dockerfile is the original scaffold and has not been hardened for production.
- Request-body validation remains partial: the ts-rest contracts in `src/contracts/` define Zod schemas, but many Fastify handlers still validate manually or fall through to default/flat errors. Anthropic-style error envelopes are not complete.
