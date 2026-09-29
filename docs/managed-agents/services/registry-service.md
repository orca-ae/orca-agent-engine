# registry-service

> Public Anthropic-compatible API + control plane. The only service exposed to
> the internet. Owns Postgres metadata. Translates REST operations into
> in-process calls against `@orca/transcript-store`, `@orca/file-store`,
> `@orca/memory-store`, and `@orca/skill-store`.

Depends on `@orca/transcript-store` (`workspace:*`). Calls
`TranscriptStore.append/read/tail` directly — no gRPC hop. Kafka is the default
transcript backend; Postgres and Apache Pulsar can be selected with
`TRANSCRIPT_STORE_BACKEND=postgres` or `TRANSCRIPT_STORE_BACKEND=pulsar`.

## Resource deletion

Resource DELETE operations retain database rows and set a nullable `deleted_at`
(timestamp with time zone). This applies to Agents and their versions, Environments,
Sessions and their resources/threads, Triggers, Vaults and credentials, Skills and
versions, Guardrails, model-price entries, Files, and MemoryStores and their children.
`deleted_at` is internal metadata; public deletion responses keep their existing shape.
Get, list (including `include_archived=true`), update, runtime lookup, and new binding
queries exclude deleted objects. Repeating a public DELETE returns `404` unless an
idempotency key replays the original response. Archive remains a separate lifecycle.

Deletion retains immutable histories, Session events, usage, Skill bindings and bundle
bytes. Environment keys are revoked; credential secrets are purged as before. Session
deletion still publishes `session.deleted` through its durable outbox and revokes its
observability pin. Session thread subpaths retain their unique identity after deletion;
the event projector uses that identity to update the same thread. Active-only name and
digest indexes exclude deleted rows. Resource-owned Git credential rotation retains the
old row while its unique resource binding applies only to undeleted credentials.
Model-price PUT can reactivate the same natural key; ordinary object updates cannot restore rows.
Ephemeral claims, reservations, expired caches, and failed-upload cleanup still use
physical deletion. There is no public restore or purge operation.

## The registry as shared server

The registry is the **shared server** — the single coordinator for every
`colocated` session (agent loop + tools running together in the sandbox, driven
by `session-runner`), over the WS tunnel, for every sandbox location (local,
Orca-managed cloud, self-hosted — see [`../deployment-topologies.md`](../deployment-topologies.md)
for the topology diagrams and responsibility split). For a `colocated` session
the registry is the **single writer** of that session's `agent.*` events:
persist-before-forward, appended **in-process** to the transcript store. That
path is backend-agnostic and broker-free when deployed on the Postgres
transcript backend (the recommended `colocated` deployment); Kafka stays the
default backend and remains fully available for
`separate` — see [`../architecture.md`](../architecture.md) ("The `colocated`
event bridge"). **Target:** `harness-server` handles `separate` only and is
not part of the `colocated` path at all — the registry drives every
`colocated` session (both `target=self_hosted` and `target=cloud`, any harness
mode; see [`../deployment-topologies.md`](../deployment-topologies.md)).

## Stack

Node 22.21+, TypeScript strict, [Fastify](https://fastify.dev/),
[ts-rest](https://ts-rest.com/) or Zod-first contracts, in-process store
libraries (`@orca/transcript-store`, `@orca/file-store`,
`@orca/memory-store`, `@orca/skill-store`), HTTP/JSON for internal service
boundaries, `pg` + Drizzle ORM, OpenTelemetry SDK.

## API read performance and admission

Agents list projects the selected rows directly. Session list and Trigger Session history
bulk-reload the selected Session IDs and hydrate resources, pinned Agent versions, skill bindings,
and outcomes in separate batches of at most 100 keys. Thread list/detail and Session detail
share the same request-owned loaders. A repeated Agent/version or missing Agent is read once
per request; fallback Agent rows are scoped to the authenticated workspace. Session overrides
and skill bindings remain keyed by Session, Agent and version, including coordinator children.
There is no cross-request Session response cache. All items in a batch use one timestamp for
time-dependent statistics. Resource detachment, skill ordinal/checksum validation, outcome
fold ordering, filters and existing cursors retain their normal semantics.
Sessions deleted before the bulk reload are omitted, while cursor boundaries still use the
original selected page. The reload preserves the legacy reader's freshness boundary; the
page and its associations are not a transactionally consistent snapshot.

`REGISTRY_BATCH_READS_ENABLED` defaults to `true`; `false` restores the per-item readers for
Agents, Sessions, Threads and Trigger history. It does not change writes or stored data.
Environment `work_stats` uses one conditional aggregate for its three Session counts, then
reads the durable worker claim. Memory lists fetch version histories only for page memory IDs;
version lists with `memory_id` narrow both history and active-memory reads to that ID.
Namespace sorting/prefix grouping and version-operation derivation retain their existing
behavior. Memory/version pages read at most four items concurrently in either view. On failure,
they stop starting new items and wait for already-started reads to settle before propagating
the first error, retaining admission capacity throughout that work.

Batch loaders stop starting further chunks after a read client disconnects. A loader that fails
also rejects new keys with its first failure instead of starting new queries. Memory full-view
streams also receive the disconnect signal. SQL already submitted to `pg` is allowed to settle;
this is not server-side SQL cancellation or an end-to-end request deadline. Write completion,
outbox persistence and synchronous API-key `lastUsedAt` updates are unchanged. Idempotency
captures the original parsed request-body hash once and reuses it in the response hook, including
when a handler normalizes the body.

Response caching runs in an asynchronous `onSend` hook before the response is written. Async
write handlers return `reply.send(...)` on success, keeping Fastify's handler completion
tied to the pending send rather than allowing a second, empty response while the cache write
is in flight. Cache-write failures are logged and preserve the original successful response.

### Optional heavy-read admission

Admission runs **after authentication** using the resolved workspace, FIFO within each workspace
and round-robin across workspaces. It applies to GET Session list, Thread list, Trigger
Session history, Memory list and Memory-version list, including their generated HEAD routes
and `/api/v1` aliases. Light reads, writes, internal/admin traffic, downloads, SSE and tunnel
upgrades do not acquire these slots.

| Variable                                       | Default | Meaning                                                |
| ---------------------------------------------- | ------- | ------------------------------------------------------ |
| `REGISTRY_HEAVY_READ_ADMISSION_ENABLED`        | `false` | Enables the per-process heavy-read gate.               |
| `REGISTRY_HEAVY_READ_MAX_CONCURRENT`           | `4`     | Active heavy reads per Registry process when enabled.  |
| `REGISTRY_HEAVY_READ_MAX_PER_WORKSPACE`        | `2`     | Active heavy reads per authenticated workspace.        |
| `REGISTRY_HEAVY_READ_MAX_QUEUED`               | `32`    | Total queued heavy reads.                              |
| `REGISTRY_HEAVY_READ_MAX_QUEUED_PER_WORKSPACE` | `8`     | Queued reads from one workspace.                       |
| `REGISTRY_HEAVY_READ_QUEUE_TIMEOUT_MS`         | `5000`  | Maximum wait before admission, not a handler deadline. |

All numeric environment settings are positive integers. Capacity rejection returns 429
`rate_limit_error`; queue timeout or shutdown returns 503 `overloaded_error`, with the normal
error envelope and `Retry-After: 1`. Disconnected queued requests are removed without starting
their handler. Slots release once the response finishes or disconnects and any running handler
settles, so disconnecting does not admit a replacement while the old read is still in flight.
Shutdown drains queued requests.
The gate is not a distributed rate limiter and does not reserve database connections or bound
authentication work before admission. Operators enable and tune it against their measured
pool budget and mixed workload; disabling it restores ungated reads.

### Performance metrics

The internal `/metrics` endpoint exposes metrics from all three listeners and all enabled pools.
HTTP labels use the route template (or `<unmatched>`), bounded method, status and listener surface;
no workspace/session IDs, credentials, SQL text, parameters or response bodies are labels.

| Metric                                          | Meaning                                                                                                                                                 |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `registry_service_request_total`                | Completed or aborted HTTP requests, by method/route/status.                                                                                             |
| `registry_service_http_duration_seconds`        | Non-SSE request lifetime through response finish/close; aborts have status `aborted`.                                                                   |
| `registry_service_http_stream_duration_seconds` | SSE connection lifetime, separate from ordinary HTTP latency.                                                                                           |
| `registry_service_http_payload_bytes`           | Completed non-SSE response Content-Length, when present; excludes framing.                                                                              |
| `registry_service_http_db_queries`              | Database query calls started in each request's context before its response closes.                                                                      |
| `registry_service_db_acquire_seconds`           | Pool acquisition, including connection establishment for a new client.                                                                                  |
| `registry_service_db_round_trip_seconds`        | Client query call to completion: includes network, server execution and client queueing, excludes pool acquisition.                                     |
| `registry_service_db_pool_connections`          | `total`, `idle`, `waiting` for `metadata`, `file`, `memory` and enabled `transcript` pools; sampled every second.                                       |
| `registry_service_auth_stage_seconds`           | Direct API-key lookup, proof verification, last-used update and public workspace lookup; stage result is `ok` or `error`, not an authorization verdict. |
| `registry_service_api_key_proof_cache_total`    | Cryptographic proof `hit`, `miss`, `coalesced`; see [auth](../auth-and-vaults.md).                                                                      |
| `registry_service_heavy_reads`                  | Active and queued heavy reads when admission is enabled.                                                                                                |
| `registry_service_heavy_read_queue_seconds`     | Queue time of admitted heavy reads.                                                                                                                     |
| `registry_service_heavy_read_rejected_total`    | Capacity, timeout, disconnect or shutdown removals.                                                                                                     |

Pool metrics include background work; HTTP query counts follow the acquiring request even when
`pg-pool` fulfills its callback from another request's connection release. Neither HTTP duration
nor database round-trip is TTFB or server-only SQL execution time. PostgreSQL execution plans and
the separate acquire/round-trip distributions distinguish network and pool effects.

## Responsibilities (control plane only)

The internal `GET /internal/v1/guardrails/effective` route serves the AI Gateway's
registry guardrail source. The Gateway authenticates as its internal workload and
supplies the session scope from verified JWT claims. Registry checks the requested
organization, workspace, Session, pinned primary or roster Agent, and runtime revision against its
database, then loads only the Session's pinned guardrail composition and returns a
one-hour bundle scoped to that Session. The bundle includes the prepared rules and
the durable Session guardrail state seed. Gateway caches it according to its
configured registry-source TTL; a policy edit takes effect after that cache expires
unless the Session runtime revision changes sooner.

This route uses a fixed path because the Gateway Registry source calls a fixed endpoint. It is an
exception to the usual path-scoped internal route shape. Only the authenticated Gateway workload
may call it; the Registry checks the query's principal ID against the Session ID, resolves the
Session within the requested workspace, verifies its organization and runtime revision, and accepts
an Agent ID only when it belongs to the Session's pinned primary or delegated roster. The query
fields alone do not authorize access to another Session.

1. Serve the documented Anthropic-compatible REST operations for **Agents, Environments, Sessions, Files, MemoryStores (with nested memories + memory_versions), Vaults, Skills**, plus the polymorphic `/v1/sessions/{id}/resources` endpoint and the **Session Threads API** (`/v1/sessions/{id}/threads[...]`, the Anthropic thread-model multiagent — read routes per the Anthropic spec plus the mutating `/archive`, and an Orca-extension `/interrupt`; see below). Serve cron-only **Triggers** at `/v1/triggers` as an explicitly tagged Orca Core extension. The Agent contract additionally carries an optional `multiagent` coordinator field (roster of delegate agents, pinned to an immutable snapshot at create; surfaced as the top-level `multiagent` field on read and stored as a dedicated `agents.multiagent` column plus a top-level key in the version snapshot). The Outcomes field on Session returns null for SDK compatibility — no execution behind it in v1.
2. Authenticate inbound (`x-api-key` table-lookup OR OIDC/JWT validator).
3. Serve the engine extension groups: `policy.runorca.ai/v1` (Guardrails and the
   guardrail type catalog) and `pricing.runorca.ai/v1` (model prices), plus the
   `/api` and `/apis` discovery routes that advertise them. Registry is the
   control plane for guardrails: it validates and compiles rules at write time,
   composes the four authority tiers when a session's runtime is prepared, and
   owns the durable guardrail state and price data that `harness-server`
   evaluates against. Registry also enforces request policies for managed
   colocated Codex Sessions before forwarding a message to the runner. Organization-scoped guardrails
   and all price writes are served on the admin listener only. See
   [`../guardrails.md`](../guardrails.md) and [`../pricing.md`](../pricing.md).
4. Persist all relational metadata in Postgres. **Persists no event payloads.** Agents and Skills are versioned (new version row on update).
   Agent snapshots may include Claude-compatible `multiagent` coordinator
   config. Registry resolves string/self/unversioned roster entries at
   create/update time into fixed `{ type: "agent", id, version }` refs, rejects
   archived or nested coordinator agents, and stores the resolved roster in the
   agent version snapshot.
5. Manage vault credentials via the `SecretProvider` abstraction (one TS implementation per backend: `AzureKeyVaultSecretProvider`, `AwsSecretsManagerSecretProvider`, `GcpSecretManagerSecretProvider`, `KubernetesSecretProvider`, `EnvSecretProvider`). `DefaultSecretProvider` dispatches by reference scheme.
   Manage GitHub repository credentials separately: raw write-only session
   resource tokens are stored in `SecretStore`; `git_credentials` and
   `session_resources.repo_ref` contain only internal references.
6. **Translate session/event public endpoints to in-process store calls:**
   - `POST /v1/sessions` → requires `environment_id`, verifies the environment
     is active and owned by the caller's workspace, then creates metadata with
     `status='idle'` plus a primary `session_threads` row for the empty
     transcript subpath; no sandbox is created at session creation time. Its
     400/404 validation failures use the Claude Beta error envelope with
     `invalid_request_error` / `not_found_error` and `request_id`.
   - Environment target updates lock the Environment row while validating existing
     Session harnesses and writing the target. Session creation shares that lock
     and rechecks deployment compatibility inside its transaction, so concurrent
     target changes cannot admit an unsupported Session.
   - `DELETE /v1/environments/{id}` sets `deleted_at` only when no undeleted Session row
     references the environment; otherwise it returns `409` with the Claude
     `conflict_error` envelope. Archiving prevents new sessions while preserving
     environment resolution for existing ones.
   - `POST /v1/sessions/{id}/events` → `TranscriptStore.append`.
     Executable `user.*` events mark the session `running`; harness later
     marks it `idle` again through the internal lifecycle route after idle
     timeout.
     OIDC-authenticated public Session creates and event appends stamp an opaque
     issuer-qualified identifier derived from verified `(iss, sub)` only on the
     private Transcript envelope for their client events. API-key, Trigger,
     Harness, and lifecycle events remain unattributed; the value is neither
     payload data nor public event output.
   - Session responses include Anthropic-compatible
     `stats: { active_seconds, duration_seconds }`. `active_seconds` is
     accumulated from `running` intervals and excludes idle time;
     `duration_seconds` is elapsed time since creation, frozen when status is
     `terminated`.
   - Session responses include Anthropic-compatible
     `usage: { cache_creation, cache_read_input_tokens, input_tokens, output_tokens }`.
     Harness records per-model-call usage through the internal usage route and
     registry accumulates it on the session row.
   - `GET /v1/sessions` uses opaque bidirectional cursors bound to the original
     `asc`/`desc` order. A follow-up request may omit `order`; an explicit
     mismatch is rejected with 400.
     Optional Orca query parameters `metadata_<key>=<value>` match arbitrary
     Session metadata strings exactly and case-sensitively within the
     authenticated workspace. Multiple filters use AND and compose with
     `agent_id` and `include_archived` before pagination and cursor validation.
     At most 16 filters are accepted, with 1–64 character keys and values up to
     512 characters; repeated parameters return 400. Keys and values use normal
     URL encoding, and a missing key does not match an empty value.
     `metadata_AGENT_TRIGGER=<resource-name>` supports externally managed
     triggers whose runners create ordinary Sessions without an Orca Trigger
     or fire record. These query parameters opt into filtering in either
     response dialect without `orca-beta` or changing Session response fields.
   - `POST /v1/sessions/{id}` allows title and metadata edits while a session
     is running, but tool/MCP/model overrides and LLM egress changes require
     `status='idle'`. Only these execution changes advance the runtime revision;
     cosmetic edits preserve active checkpoint fences. Archived and
     terminated sessions reject every update with 409. The final write repeats
     the lifecycle/status predicate so a concurrent state transition cannot
     race the preflight check.
   - Session archive writes a durable `session.archived` outbox row, while
     permanent delete writes `session.deleted`, in the same transaction as the
     metadata mutation. Registry attempts to append the sentinel immediately
     and a startup + periodic reconciler retries it until publication succeeds;
     soft deletion retains the outbox row without a session foreign
     key. Only `session.deleted` is public and terminates all session/thread
     streams.
   - Session creation stores `initial_events` in the same durable outbox before
     committing Session metadata. Registry publishes the ordered batch only
     after commit, so Harness consumers cannot observe a turn before
     `executions:prepare` can see its Session. Stable event IDs make immediate
     publication and periodic crash recovery idempotent. The private OIDC user
     attribution on an initial client event survives this outbox/reconcile path.
   - `GET /v1/sessions/{id}/events/stream` → `TranscriptStore.tail` (SSE-bridged with periodic heartbeats); internal `harness.*` replay-state and dispatch markers (`session.user_event_processed`, `session.user_event_completed`, and deferred-message markers) are filtered before writing SSE frames or public event-index rows.
   - `GET /v1/sessions/{id}/events` → reads public events from the
     registry-owned `session_events_index` Postgres read model. The transcript
     backend remains the source of truth; registry projectors consume
     transcript events asynchronously, so history listing is eventually
     consistent with normal projector lag. The endpoint does not perform
     request-time catch-up from the transcript backend.
   - **Session dispatch backlog metrics:** the internal `/metrics` endpoint
     refreshes these metadata-only gauges at startup and every five seconds.
     The event gauges aggregate only public, client-produced `user.*` index
     rows with `processed_at IS NULL`; they never read event payloads, tokens,
     credential references, or MCP bodies. Only the harness-server dispatcher
     writes the acceptance markers that set `processed_at`, so the client events
     of runner-driven sessions (every `self_hosted` session, and cloud sessions of
     the runner harnesses) never leave these gauges; see
     [`../roadmap.md`](../roadmap.md#known-limitations). A successful refresh replaces all
     workspace label series, so a drained workspace does not retain a stale
     value. `registry_service_events_append_total{status="ok"}` remains the
     accepted-event counter, separate from processing state. Migration `0052`
     records the schema checkpoint transactionally, then `db:migrate` ensures
     its partial index with `CREATE INDEX CONCURRENTLY IF NOT EXISTS` after the
     checkpoint commits and while its advisory migration lock remains held.
     The concurrent ensure runs on every invocation, removing an invalid or
     missing index after an interrupted migration without a table write lock
     beyond PostgreSQL's normal concurrent-index phases.

     | Metric                                                                                     | Meaning                                                                               |
     | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
     | `registry_service_session_events_unprocessed_client_user{workspace_id}`                    | Unprocessed executable client `user.*` event count by workspace.                      |
     | `registry_service_session_events_oldest_unprocessed_client_user_age_seconds{workspace_id}` | Age of the oldest such event by workspace.                                            |
     | `registry_service_session_lifecycle_outbox_pending`                                        | Unpublished `session_lifecycle_outbox` row count.                                     |
     | `registry_service_session_lifecycle_outbox_oldest_pending_age_seconds`                     | Age of the oldest unpublished lifecycle row.                                          |
     | `registry_service_session_lifecycle_outbox_pending_attempts`                               | Sum of `attempt_count` over unpublished lifecycle rows.                               |
     | `registry_service_session_dispatch_observability_refresh_total{result}`                    | Refresh outcomes: `success`, `error`, or overlap skipped by the in-process guard.     |
     | `registry_service_session_dispatch_observability_last_success_timestamp_seconds`           | Unix timestamp of last snapshot/gauge update; unchanged on refresh errors or overlap. |

   - **Session Threads** (`/v1/sessions/{id}/threads[...]`) → read/act on the
     `session_threads` read model. A thread's per-thread events / SSE stream are
     the session events / stream scoped to that thread's transcript subpath (the
     primary thread = the whole session at the empty subpath; a child thread =
     its own `subagents/<id>` subpath). `POST /interrupt` appends a
     `session.thread_status_terminated` on the target thread's own stream (the
     durable signal a live runner notices there; the event-index projection also
     surfaces a copy on the primary) then flips the read-model row. See the Session Threads
     API subsection below and [`../session-runner-scope.md`](../session-runner-scope.md).
   - **Cross-thread reply routing** on `POST /v1/sessions/{id}/events`: a
     `user.tool_confirmation` / `user.custom_tool_result` may carry a
     `session_thread_id` naming the originating subagent thread (the request was
     cross-posted onto the primary stream, so the client replies against the
     session). The route resolves the session's primary thread id once and
     rewrites each such reply's transcript subpath to the target thread's stream,
     so the runner driving that thread — not the primary — receives it. A reply
     with no `session_thread_id` (and every single-agent session) stays on the
     primary stream.
   - The Session Threads surface is `GET /v1/sessions/{id}/threads`,
     `GET /v1/sessions/{id}/threads/{thread_id}/events`,
     `GET /v1/sessions/{id}/threads/{thread_id}/stream`, and
     `POST /v1/sessions/{id}/threads/{thread_id}/archive`; the primary thread
     maps to the empty subpath, exists for every session, and reports the
     parent session's lifecycle, usage, and timing stats. Creating a new
     thread subpath is capped at 25 concurrent non-archived/non-terminated
     threads per session; existing thread-targeted control events
     (`user.interrupt`, `user.tool_confirmation`, `user.custom_tool_result`)
     are routed to the recorded subpath and do not consume another thread
     slot. Registry's event index also projects subthread activity into the
     primary thread: lifecycle events keep their `session.thread_*` type,
     subthread `user.message` becomes `agent.thread_message_sent`, and
     subthread `agent.message`/Claude SDK assistant entries become
     `agent.thread_message_received`.
   - `POST /v1/files` → `FileStore.createFile` (streaming upload).
   - `GET /v1/files/{id}/content` → `FileStore.openContent` (streaming download).
   - All Memory APIs → `MemoryStore.*` calls.

   **SSE backpressure:** the `GET /v1/sessions/{id}/events/stream` endpoint drops slow consumers and signals them to reconnect. See [`../../operation/sse-backpressure.md`](../../operation/sse-backpressure.md).

7. Expose `POST /internal/v1/workspaces/{workspace}/sessions/{session}/vault-credentials/{credential}/resolve` (`ai-gateway` internal caller; access enforced by Registry's internal authenticator and, in Kubernetes ServiceAccount mode, its workload route-family check; optionally also by service-mesh policy) returning gateway-compatible credential material. Registry requires an active, unarchived Session in that workspace, validates the gateway body `{ credential_id, vault_id, force_refresh? }`, and requires both body IDs to equal the path credential ID byte-for-byte. The request `vault_id` is a legacy gateway alias for `credential_id`, not a Registry `vlt_*` resource. A concrete `vcrd_*` request preserves the exact MCP lookup. An `llm:*` request selects active provider rows only from the Session's persisted `vault_ids`; exactly one match is required, while zero or multiple matches return the same `404`. This endpoint is **not** routed through the public listener.
   Provider responses return the selected concrete `vcrd_*` ID, persisted
   `vlt_*` ID, canonical scheme, opaque resolution version, secret material,
   and TTL. Logical selection writes an immutable organization audit event with
   workspace, session, requested alias, selected concrete ID/version on
   success, and a sanitized denial reason on failure. Audit metadata never
   includes secret bytes, secret-store references, authorization headers, or
   ambiguous candidate IDs. A successful selection is not released if its
   audit event cannot be persisted.
   For `mcp_oauth`, `force_refresh: true` performs the guarded OAuth refresh
   grant before returning. It and public `mcp_oauth_validate` share one
   short-lived Postgres credential lease, serializing one-time refresh-token
   consumption across Registry replicas without holding a database transaction
   during outbound HTTP. Waiters that loaded the same secret refs return the
   committed access-token winner without another token-endpoint call. The
   holder heartbeats through secret resolution and staged SecretStore writes;
   lease owner fences the final pointer CAS. Public access/client-secret-only
   rotations return `409` while the lease is live; replacing or clearing the
   refresh token may supersede it safely. Rotated access/refresh tokens update
   `auth_config.expires_at` from `expires_in`. Slack-compatible token
   payloads under `authed_user` are accepted when top-level token fields are absent.
   HTTP 200 `ok: false` responses map known permanent auth/grant errors to
   rejection and keep transient or unknown errors retryable.
   Only `kubernetes_service_account` mode provides exporter-only isolation:
   its configured `observability-exporter` ServiceAccount may call the two
   observability resolver routes below. In `static_token` mode, every trusted
   holder of the shared internal token may call both routes; it provides no
   workload isolation.
   - `POST /internal/v1/workspaces/{workspace}/sessions/{session}/agent-observability/context/resolve`
     — strict `{}` resolver for the Session's
     immutable observability pin. It reads the exact pinned binding version
     under repeatable-read locked authority, returns only non-secret binding,
     target, configuration, capture-clamp, epoch, and classification metadata.
     A platform, organization, or workspace capture-restriction epoch advance
     creates a sticky `metadata_only` clamp for that pre-existing pin. A later
     ceiling expansion remains visible in `current_ceilings` but cannot widen
     `effective_mode`; a newly created Session observes the expanded epoch and
     is evaluated from its own pin. The route marks every response
     `Cache-Control: private, no-store`. It neither
     selects a SecretStore reference nor calls `SecretStore`; credential-head
     presence is represented only as non-secret metadata.
   - `POST /internal/v1/workspaces/{workspace}/sessions/{session}/agent-observability/secret/resolve`
     — strict `{}` pre-send release resolver. It
     loads the same Session-pinned authority in two fresh repeatable-read,
     row-locked snapshots around one direct `SecretStore.resolve` call. It
     returns a strict provider-specific current credential bundle only after
     the second snapshot proves the same pin and complete credential head and
     commits an `agent_observability.credential_resolved` audit event. A whole
     head rotation retries with the new generation; denial returns `409`, and
     storage, bundle, audit, or churn failures return sanitized `503`. The
     response never includes a SecretStore reference; its `authorization_id`
     is opaque audit correlation, not a bearer credential. Every response,
     including parsing, auth, and wrong-listener errors, is
     `Cache-Control: private, no-store`.
   - `POST /internal/v1/workspaces/{workspace}/sessions/{session}/mcp-destination/resolve`
     — AI Gateway-only resolver for strict `{ backend }`. It returns the pinned
     Session destination, authoritative nullable credential id, and a stable
     URL-plus-credential binding revision for Gateway idempotency. Credential matching
     follows Anthropic URL normalization (scheme/host case, default ports, and
     trailing slashes). Kubernetes Harness identity is rejected; shared-token
     mode is retained for non-Kubernetes deployments.
   - `POST /internal/v1/workspaces/{workspace}/sessions/{id}/executions:prepare`
     — validates and returns the immutable runtime snapshot. This is the only
     general runtime resource lookup; bare tenant-resource lookup routes do not exist.
     Archiving an Agent invalidates its pinned versions for existing Sessions
     as well as new bindings: preparation returns `409 invalid_runtime_binding`
     with `resource_type=agent_version`, even while the version snapshot remains
     stored. Harness treats this response as a failed turn and ends source-event
     retry; see [execution preparation failures](harness-server.md#execution-preparation-failures).
   - `POST /internal/v1/workspaces/{workspace}/sessions/{id}/mint-jwt` — mints
     a session-scoped JWT for `harness-server` and derives credential IDs from
     persisted vault bindings. When the Registry LLM policy
     (`SESSION_JWT_LLM_ROUTES` / `SESSION_JWT_LLM_MODELS`) is configured, Codex SDK
     and Pi SDK tokens for the `ai-gateway` audience last 660 seconds to cover the
     bounded model turn; Registry selects this lifetime from the pinned harness,
     and ignores caller-provided TTL fields. Every other token retains the
     configured Session JWT lifetime.
   - `PATCH /internal/v1/workspaces/{workspace}/sessions/{id}/state` — internal-only lifecycle update used
     by harness to publish `running`/`idle` transitions and the current
     `sandbox_handle_id`. Registry uses these transitions to accumulate
     session `stats.active_seconds`.
   - `POST /internal/v1/workspaces/{workspace}/sessions/{id}/usage` — internal-only additive token-usage
     update after each model call (each assistant response, not once per SDK
     turn — see `services/harness-server.md`). Harness is the safe default
     writer. `AI_GATEWAY_REGISTRY_USAGE_ENABLED=true` switches `colocated` Sessions
     other than Codex SDK and Pi SDK to AI Gateway only when its Registry usage
     sink is deployed; `separate` Sessions always remain Harness-owned. Registry includes the
     decision as `session.usage_writer` in every prepared execution. Under workload-aware Kubernetes
     authentication Registry rejects the other identity, and Harness suppresses
     its usage callback whenever Registry selects Gateway, so shared-token
     development deployments also avoid duplicate accounting. Codex SDK and Pi SDK
     turn usage is authoritative in both modes: `usage_writer: "harness"` denotes
     SDK-origin usage, which Harness reports for cloud Sessions. For self-hosted
     `colocated` Codex SDK and Pi SDK Sessions, the Registry bridge commits it
     directly, and this endpoint rejects every external producer for them, including
     AI Gateway and shared-token callers, to prevent duplicate accounting. Harness
     preserves the Claude Agent SDK's `usage.cache_creation` 5m/1h TTL breakdown. The current `orca-ai-gateway`
     streaming path preserves that nested usage object. For
     compatibility with legacy or third-party Anthropic-compatible terminal
     frames that expose only the flat `cache_creation_input_tokens` total, the
     harness maps that value to
     `usage.cache_creation.ephemeral_5m_input_tokens`. Existing terminated or
     archived rows still accept a final additive flush that raced with lifecycle
     teardown; deleted rows return `404`.
   - `POST /internal/v1/workspaces/{workspace}/sessions/{id}/guardrail-state` —
     internal-only ordered guardrail-state deltas from Harness or AI Gateway.
     Registry applies
     each delta as the additive conflict-upsert `guardrails.md` specifies, into
     `guardrail_state` / `guardrail_counters`; harness never opens a Postgres
     connection for guardrail state, mirroring how the usage route above carries
     token deltas. Harness uses synchronous write-through and does not retry an
     ambiguous timeout, because additive deltas are not independently idempotent.
     The usage route additionally prices each delta against the session
     organization's resolved catalog and returns the updated session,
     per-thread, and subject-window totals — each marked when unpriced usage
     is present — in its acknowledgment (see [`../pricing.md`](../pricing.md));
     the harness evaluates cost budgets against those acknowledged totals and
     never holds price rows. The `subject_window` subject is resolved
     server-side from the turn's accepted-event record (the delta references
     its turn); a caller-supplied subject is ignored, so a compromised harness
     cannot re-attribute spend.
   - `POST /internal/v1/workspaces/{workspace}/sessions/{session}/files` —
     internal-only file upload used by the output indexer. Registry derives
     ownership, `purpose`, `scope_id`, and downloadability from the path.
     Caps each upload at 500 MB to match the public route. See
     [`../output-capture.md`](../output-capture.md).
8. Translate `agent_toolset_20260401` (Anthropic-SDK literal) ↔ internal `agent_toolset` at the API boundary so first-party Orca clients see the cleaner name.
9. Apply the **idempotency middleware** on every write endpoint from day one: keyed on `(workspace_id, scope, Idempotency-Key)` with a 24 h TTL, returning the cached response body bit-for-bit on retry.
10. Serve the isolated management plane on `:8082`: organization-scoped
    `/v1/organizations/*` routes derive organization ownership from an
    `orca_admin_...` key or `org:admin` OIDC token, while deployment-wide
    `/v1/platform/*` provisioning and observability-policy routes require an independent
    `orca_platform_...` key or `platform:admin` OIDC token. Platform principals
    carry no organization ID. Platform writes use a 24-hour idempotency cache
    keyed by platform principal and write immutable platform audit events.
    Organization creation accepts an optional capture_ceiling, validated by the
    observability contract schema. It defaults to metadata_only; a caller that wants
    raw_io requests it explicitly. The initial setting commits with the new organization, audit
    and idempotency result, without a second policy mutation or any existing-row
    policy overwrite on replay.

## API contract

Source of truth is the split ts-rest contracts in `services/registry-service-ts/src/contracts/*.contract.ts`, composed into the public surface by `src/contracts/index.ts`. `pnpm openapi:gen` renders them into `services/registry-service-ts/openapi/managed-agents.yaml`, and `pnpm conformance:gen` diffs that against Anthropic's vendored spec into [`../conformance-matrix.md`](../conformance-matrix.md), where every difference carries an explicit decision from `conformance-decisions.yaml`. Both artifacts are committed and CI fails if regenerating them produces a diff.

The Fastify route handlers in `src/api/*.routes.ts` are a separate list of the same surface — nothing generates one from the other — so `test/unit/route-contract-parity.spec.ts` fails when the served route table and the contract disagree.

The wire shape mirrors Anthropic's beta surface for SDK compatibility where implemented, but Orca-owned extensions are documented separately in [`../orca-extensions.md`](../orca-extensions.md):

- Clients use **`orca-beta: managed-agents-<ver>`** to opt into Orca-only beta features.
- The Anthropic SDK's auto-injected `anthropic-beta: managed-agents-2026-04-01` header is accepted and currently ignored unless a route doc says otherwise — it must not enable Orca-only behavior.
- Where we diverge from Anthropic's wire shape (e.g. `model: { provider, id }` instead of a string, the `agent_toolset` rename), Orca additions are header-gated by `orca-beta` so SDK-only clients stay on the conservative shape. Pagination/filter behavior is inventoried in [`../pagination-and-filters.md`](../pagination-and-filters.md).

### Agent observability state (admin listener)

The organization admin listener serves current-state routes, workspace replacement, and workspace credential rotation:

```http
GET /v1/organizations/agent_observability
GET /v1/organizations/workspaces/{workspace_id}/agent_observability
PUT /v1/organizations/workspaces/{workspace_id}/agent_observability
POST /v1/organizations/workspaces/{workspace_id}/agent_observability:rotate_credentials
```

They are declared in the standalone
`src/contracts/agent-observability.contract.ts`, not in `publicContract`; public
OpenAPI and Anthropic conformance therefore do not advertise them. The caller
needs `observability:read` or `org:admin`. The workspace route constrains both
the organization from the authenticated principal and the workspace path; a
missing, archived, or foreign workspace is the same `404`.

`pnpm openapi:observability:gen` in `services/registry-service-ts` generates the separate
`openapi/observability-admin.yaml` from this contract and `platform-agent-observability.contract.ts`.
The regular `pnpm openapi:gen` also refreshes it. The standalone document declares distinct
organization/platform security schemes, preserves literal action colons, and includes required
mutation bodies and response ETags. It does not alter the public contract or conformance surface.

Each response has `type: "agent_observability"` and an explicit `scope`.
Organization configuration has `default_binding` plus its capture ceiling;
workspace configuration has `mode`, `binding`, and its capture ceiling. Both
include resolved effective source, status, disabled reason, capture mode, and a
selected non-secret binding view. A binding view contains its target identity,
canonical endpoint kind/class/URL, lifecycle status, immutable policy version,
and credential configured/version/hint/rotation metadata only. It never
contains a credential reference, credential bytes, or validation fields.
Disabled effective state always reports `capture_mode: "metadata_only"`.
Capture capabilities are ordered `metadata_only` < `redacted_io` < `raw_io`;
effective capture is the minimum of the request or immutable Session pin and
all applicable platform, organization, and workspace ceilings. `raw_io` explicitly
permits input/output bodies without automatic redaction or sensitive-field/string
replacement. `redacted_io` remains a legacy/reserved mode, not an alias for raw
capture: neither existing configuration nor existing pins are upgraded. The
platform maximum still defaults to `metadata_only`; accepting `raw_io` through
the existing admin contracts does not enable it globally or bypass those ceilings.
Platform administrators read and replace that singleton through
`GET/PUT /v1/platform/agent_observability` on the admin listener. Its standalone
`platform-agent-observability.contract.ts` accepts only `allowed_adapters`,
`allowed_endpoint_classes` and `max_capture_mode`. PUT requires a strong
`If-Match` and an `Idempotency-Key`; canonical allowlists, epoch and timestamps
form the response and opaque ETag. The writer takes an exclusive lock on the
same singleton authority used by shared-lock readers, increments the sticky
epoch on capture reduction, and atomically commits policy, platform audit and
24-hour replay. Successful retries return their original representation/ETag;
reusing a key with a different body or precondition conflicts. All responses are
private/no-store, and missing or corrupt policy fails closed with a sanitized
`503`. See [platform policy administration](../workspace-administration.md#platform-observability-policy)
for request fields, global rollout scope and status codes. This writer does not
alter scoped settings, immutable binding versions or existing Session pins.
Migration `0060_agent_observability_raw_io` installs the Session-pin capture CHECK
as `NOT VALID`, which enforces new writes without scanning history under the DDL
transaction's exclusive lock. The migration runner validates it after Drizzle
commits; a rerun finishes interrupted validation. The initial DDL still takes a
brief exclusive lock, and validation is not a lock-free operation.
The Registry resolves one repeatable Postgres snapshot and never selects a
binding from request metadata. A selected custom workspace binding never falls
back to an organization binding when it is unavailable.

Every successful read carries an opaque strong `ETag` derived from non-secret
authoritative state and `Cache-Control: private, no-store`; a shared cache must
not reuse the tenant-neutral organization URL across credentials. The routes do
not process `If-Match`. Missing or malformed control-plane state returns a
sanitized `503`; these reads do not call `SecretStore` or an external endpoint.

The same admin listener serves organization-default replacement, disable, and credential rotation:

```http
PUT /v1/organizations/agent_observability
POST /v1/organizations/agent_observability:disable
POST /v1/organizations/agent_observability:rotate_credentials
```

`PUT /v1/organizations/agent_observability/capture_ceiling` is the separate
ceiling-only operation. It requires org:admin, a strong organization GET ETag,
and Idempotency-Key; its strict body contains only capture_ceiling. It works
without a default binding and preserves any existing target, config version and
credential head. The existing organization authority locks and mutation kernel
commit the ceiling, sticky epoch, audit and replay atomically in one repeatable-read
transaction, with bounded serialization retry and no SecretStore calls. Both the
body and original If-Match participate in idempotency identity. It returns the
organization state, not a workspace binding response; a following GET supplies
the current ETag. The /api/v1 alias has identical authorization. See
[workspace administration](../workspace-administration.md#admin-api) for errors
and raw-content authorization requirements.

The authenticated organization is the only organization either route addresses;
the request has no organization selector. PUT requires `observability:write`
or `org:admin`, plus a trimmed 1–255-character `Idempotency-Key`. Its standalone
contract accepts a strict full replacement with `target`, `config`,
`capture_ceiling`, and write-only `credentials`. V1 accepts only the
`otlp_http` adapter, `otel_genai` or `langfuse` semantic profiles,
`http/protobuf` or `http/json`, and `none` or `gzip` compression. Endpoint input
is canonicalized to one absolute HTTP(S) identity before persistence.

The first default creates a new organization binding and returns `201`; a later
replacement returns `200`. A current default requires an exact strong
`If-Match` from the read representation (`428` when absent, `412` when stale;
weak/list/wildcard/malformed forms are `400`). First creation and a changed
target identity require credentials; a same-active-target replacement forbids
them and creates a new immutable policy version while retaining the credential
head. A Langfuse profile requires a non-null external project identity. OTLP
Basic, bearer, and custom-header credentials remain independent from semantic
profile selection. A changed target creates a new binding, selects it atomically,
and moves the prior active binding to `draining`, leaving existing Session pins
untouched.

The organization capture ceiling is stored independently from the current
platform ceiling; effective state continues to use the minimum. A
ceiling reduction (`raw_io` → `redacted_io`/`metadata_only`, or
`redacted_io` → `metadata_only`) advances the organization
capture-restriction epoch. Context resolution compares that epoch, and its
platform/workspace counterparts, with the immutable pin snapshot: any advance
clamps that existing Session to `metadata_only` permanently, even if all current
ceilings later expand, including to `raw_io`. The response still reports those actual
current ceilings; a new Session pins the current epochs and can use the expanded
policy.

Credential bundles are staged durably before `SecretStore.put`, written outside
the database transaction, and activated only by the fenced final transaction.
Failed writes or finalization conflicts fence the attempt and leave cleanup to
the durable reconciler; Registry does not issue inline deletes. Every PUT
response, including authentication, scope, validation, precondition, conflict,
not-found, and availability errors, uses `Cache-Control: no-store`. Successful
responses and idempotency cache bodies are same-transaction authoritative state
views and contain no credential bytes or references. Registry runs mutation
retention continuously and, when a
write-capable `SecretStore` exists, staging and superseded-secret cleanup on a
non-overlapping bounded interval.

The standalone admin route only authenticates, parses, and maps a typed result.
Its application executor owns authority locking, idempotency replay, staging,
fencing, and finalization. It reports fixed phase labels for unexpected
acquisition, staging-write, finalization, or fencing failures through one safe
structured operator event: `{ phase, code, requestId, organizationId, message }`.
Its codes and messages are fixed; it never logs a provider error, bundle,
reference, or request value.

#### Workspace observability replacement

`PUT /v1/organizations/workspaces/{workspace_id}/agent_observability` requires
`observability:write` or `org:admin`, a normalized 1–255-character
`Idempotency-Key`, and an exact strong workspace-state `If-Match` (`428` when
absent, `412` when stale, weak/list/wildcard/malformed forms `400`). The route
is standalone/admin-only. It constrains the path workspace to the authenticated
organization and active workspace; missing, archived, and foreign paths all
return `404`.

Its strict body is discriminated by `mode`. `inherit` and `disabled` contain
only `mode` and `capture_ceiling`; they cannot select a target, policy, or
credentials. `custom` requires `target`, `config`, and `capture_ceiling`, with
write-only credentials required when it creates a workspace binding or changes
target identity. A same-active-target custom request rejects credentials,
appends an immutable config version, and retains the existing credential head.
V1 accepts the same `otlp_http` target/config vocabulary and semantic validation
as organization PUT. A workspace custom binding is always workspace-owned and
never reuses an organization binding.

Custom creation returns `201`; all mode, same-target policy, and target
replacement updates return `200`, each with same-transaction authoritative
workspace state. Mode or selected-binding changes advance the workspace
selection epoch. Any capture ceiling reduction advances only its
capture-restriction epoch. A custom-to-inherit/disabled or custom target
replacement moves the active old workspace binding to `draining`; selection
changes never mutate existing Session pins. Entering explicit disabled also
advances the workspace revocation epoch, while an already-disabled PUT is an
audited/idempotent no-op for selection and revocation state.

Ordinary custom PUT uses the fenced reservation/staged SecretStore lifecycle;
authority/finalization transactions never call `SecretStore`. Inherit and
disabled writes use no SecretStore operation. Workspace idempotency is scoped
by the exact workspace setting, so a completed same-key retry replays its
historical authoritative response after active workspace authority validation
but before `If-Match` or transition evaluation, including after later
mode/target changes; a same-key live pending attempt returns `409`.
Explicit disabled retains workspace authority locks through its precondition,
fences only pending reservations on that exact workspace setting and current
workspace binding, then finalizes its own response. It cannot fence an
organization default, another binding, or a sibling workspace. Every canonical
and `/api/v1` alias response, including auth, parse, scope, precondition,
conflict, not-found, and availability failures, has `Cache-Control: no-store`;
responses, audit/cache records, and operator events contain no credential bytes
or refs.

If workspace archive wins after an ordinary PUT has staged credentials but
before finalization reacquires workspace authority, the PUT settles its exact
reservation and staging intent, returns `404`, and never activates its candidate
binding. This is a workspace resource outcome, not a SecretStore or exporter
delivery signal.

#### Workspace credential rotation

`POST /v1/organizations/workspaces/{workspace_id}/agent_observability:rotate_credentials`
requires `observability:rotate` or `org:admin`, a normalized `Idempotency-Key`,
and one exact strong workspace-state `If-Match` (`428` absent, `412` stale,
weak/list/wildcard/malformed forms `400`). Its strict body is only
`{ credentials }`, reusing the bounded OTLP Basic, bearer, and custom-header
credential schema. Callers cannot provide a binding, target, config, adapter,
version, or SecretStore reference. The path is constrained to an active
workspace owned by the authenticated organization; missing, archived, and
foreign workspaces are all `404` before replay or precondition processing.

Rotation admits only the current configured `custom`, workspace-owned, active
`otlp_http` binding with a valid positive config version and configured current
credential head. `inherit`, `disabled`, non-active/non-OTLP, and no-head states
are ordinary `409` outcomes; contradictory ownership, epoch, version, or ref
state is a sanitized `503`. The executor locks platform policy, organization,
organization setting, workspace, workspace setting, selected workspace binding,
current config version, and credential head in Session/PUT/archive-compatible
order. It derives the target only from that locked authority and stages the
next credential bundle at `N + 1` outside the database transaction. Acquisition
retries a fresh repeatable-read transaction only for PostgreSQL `40001` before
staging; it never repeats a SecretStore write or finalization hook.

The durable route scope includes the path workspace. A completed exact retry
restores and validates its linked workspace-binding target and historical `200`
state before current `If-Match` or eligibility evaluation, including after a
later custom-target replacement, inherit, or disabled update; a live exact
pending retry is `409`. Same-key different bodies are `409`; expired attempts
rebind through the mutation kernel. Cached state must retain one exact active
workspace binding with unchanged config version, credential version `N + 1`,
and coherent configured/effective capture state, or replay fails closed with
`503`. An enabled historical response has a semantically valid immutable
config; `binding_configuration_invalid` has an invalid one. Platform
adapter/endpoint policy-disabled responses remain replayable for an otherwise
active binding because their historical policy snapshot is not rotation metadata.

Finalization re-locks the exact workspace authority and old credential head in
a fresh repeatable-read transaction, verifies the old state/ref/version tuple,
then CAS-swaps only that head. Target/config/selection/revocation/capture state
and Session pins do not change. The old ref enters the workspace binding's
durable superseded-secret cleanup outbox only after that CAS; Registry never
deletes it inline. Explicit workspace disabled can preempt the exact pending
workspace-binding rotation before claim, during staging, or after a staged
write; the loser settles its staging tombstone and returns `409` without an
operator failure signal. If archive wins before finalization, the loser settles
the exact staging state and returns `404`. Every canonical and `/api/v1` alias
response, including auth, parsing, scope, precondition, conflict, not-found,
and availability failures, uses `Cache-Control: no-store`; responses, audit,
idempotency cache, and fixed workspace reporter events contain no credentials
or refs.

`POST /v1/organizations/agent_observability:disable` requires
`observability:write` or `org:admin`, a trimmed 1–255-character
`Idempotency-Key`, and strict `{}` body. It accepts no binding, target, archive,
or policy selector. `If-Match` is optional: a supplied exact strong state ETag
is checked after completed replay but before emergency fencing; a missing header
permits emergency disable. Weak/list/wildcard/malformed forms return `400`; a
stale supplied value returns `412` without fencing a pending PUT or rotation.

Durable route-scoped idempotency restores the organization-setting target from
the linked reservation. A completed retry replays its original `200` before
current state, `If-Match`, or fencing, including after later PUT re-enables a
default. A same-key live pending attempt returns `409`; terminal or expired
pending attempts can rebind. The caller never chooses a target.

Each disable attempt holds platform policy, organization, setting, current
binding, and credential-head authority locks in one repeatable-read transaction
through precondition, exact preemption, reservation, finalization, audit, and
authoritative response caching. Disable has no `SecretStore` operation, so a
PostgreSQL `40001` retries that entire DB-only attempt from a fresh snapshot,
bounded to three attempts. Preemption sorts/deduplicates canonical target keys
and fences only pending reservations for this organization setting (PUT/disable)
and current organization binding (rotation), using normal staging cleanup
handoff. It never fences a workspace, another organization, or another binding.

With a configured default, disable clears `configured.default_binding` and
advances organization selection and default-revocation epochs once. An active
selected binding changes to `draining`; a `draining`, `disabled`, or `archived`
selected binding keeps its status, and a selected binding with no credential
head is still safely removable. Disable leaves organization-wide/
capture-restriction and binding revocation epochs, target/configuration,
credential head, and SecretStore refs unchanged; it writes no SecretStore
operation, cleanup for the selected credential head, Session lifecycle outbox, or
Transcript record. A staged PUT or rotation preempted by disable follows normal
durable staging cleanup. Existing inherited Session pin rows remain unchanged
and retain the default-revocation epoch they observed at creation;
workspace-custom settings and pins also remain unchanged. A missing default is
an idempotent `200` no-op with no epoch or binding mutation but still writes
audit/idempotency finalization.
Re-enabling follows PUT and creates a new binding; an old draining binding is
not selected again.

Every disable response, including auth, scope, parse, precondition, conflict,
not-found, and availability errors and `/api/v1` alias responses, uses
`Cache-Control: no-store`. Response, audit metadata, idempotency cache, and
operator event contain no credential bytes or refs.

`POST /v1/organizations/agent_observability:rotate_credentials` requires
`observability:rotate` or `org:admin`, an `Idempotency-Key`, and an exact strong
current `If-Match` (`428` when absent, `412` when stale, malformed forms
`400`). Its strict body is only `{ credentials }`, using the same Basic,
bearer, or bounded custom-header credential validation as PUT; it accepts no
target, config, adapter, or SecretStore reference. A completed retry replays
its original authoritative `200` state before evaluating `If-Match`, including
after a later PUT selects a replacement binding.

Rotation admits only the currently configured, organization-owned, active
`otlp_http` default with a valid positive credential head. It stages a bundle
for that same binding at credential version `N + 1`, repeatably revalidates the
setting, binding, config, and exact old head, then CAS-swaps only the head.
An unconfigured or ineligible default returns `409`; a missing row for a
claimed head, or a contradictory reference, generation, or ownership claim, is
an invariant failure reported through the fixed safe operator event and returned
as a sanitized `503`.
Target/config versions, selection and revocation/capture epochs, and Session
pins remain unchanged. The old opaque ref enters the durable superseded-secret
cleanup outbox only after the authoritative swap; Registry never deletes it
inline. Every rotation response uses `Cache-Control: no-store`; success is
`200`; responses, audit, and idempotency bodies contain no secret bytes or refs.

### Agent observability mutation persistence kernel

`src/domain/agent-observability-mutations.ts` provides a DB-only persistence
kernel. It has no HTTP route, server SecretStore injection, or timer
registration. Its reservation/finalization path has no SecretStore call. A
caller owns the authority-row transaction and lock, hashes its request body
before calling the kernel, and acquires one fenced reservation for an
organization setting, workspace setting, or existing binding. Finalization
requires and verifies a repeatable-read or serializable outer transaction so
its multi-query authoritative state projection cannot mix concurrent policy or
binding generations. Idempotency is
partitioned by `(organization_id, principal, scope, key)`, records its exact
target key, and has a TTL. Before TTL expiry, changed body or target conflicts;
after expiry, the row atomically rebinds to a new target/body attempt unless its
linked reservation remains live, in which case it reports in progress. A bounded
retention operation first CAS-expires an abandoned pending reservation when its
lease elapsed, then removes terminal-safe expired idempotency rows. Reservations
remain append-only so pruning cannot reset a target's generation fence; staging
rows remain reconciler-owned.

Emergency disable uses a kernel authority-only preemption helper while it
already holds corresponding authority rows. It sorts/deduplicates canonical
target keys, locks matching pending reservations with `FOR UPDATE`, performs a
pending-status CAS without a caller owner token, and gives staged rows the
existing cleanup handoff. Organization disable admits only its organization
setting/current organization binding; workspace explicit disable admits only
its exact workspace setting/current workspace binding. It does not grant a
generic cross-target fencing capability.

`src/domain/agent-observability-secrets.ts` serializes one strict canonical
versioned bundle for `otlp_http` Basic, bearer, or bounded custom-header
credentials and for schema-compatible `langfuse_sdk` public/secret keys. The
Registry mints opaque `local:agent_observability/obssec_*` references. Bundles
contain no endpoint or `Authorization` header; custom headers reject server-owned
protocol/compression names (`content-type`, `content-encoding`), transport and
trace-context names, CR/LF, duplicates, and size/count violations. Every bundle
carries binding ID plus credential version; decode requires that exact expected
identity and rejects cross-binding or stale-generation bytes.

For a credential-bearing attempt, the acquisition transaction inserts one
staging intent before any caller writes the encoded bundle to `SecretStore`.
The intent records candidate binding ID, proposed credential version, opaque ref,
and durable writer/cleanup tokens plus leases. A short writer claim moves
`pending -> writing`; bundle write occurs outside a transaction; token-CAS
completion moves only a live attempt to `written`. Heartbeats extend only a live
writer. Each staging writer receives the Registry-minted input reference as its
authoritative location plus a cancellation signal; it never returns a replacement
reference. The kernel bounds its staging `SecretStore` wait to 45 seconds by
default. Deadline expiry or heartbeat loss aborts its wait and converts the
writer into a durable cleanup tombstone. Local storage observes the signal
directly. The Kubernetes wrapper uses a private client-node API instance with a
30-second default raw HTTP transport deadline (configurable per store), so its
put/delete promise is bounded independently of that wait signal. The deadline
does not prove an already-issued request was cancelled server-side; a late put
therefore remains possible and is handled by the permanent tombstone cleanup
path. Each successful delete retains and reschedules that tombstone indefinitely.
An expired writer token cannot heartbeat, mark an intent written, or consume a
tombstone.

Finalization re-locks the exact owner/generation/version tuple and only a
`written` intent, then passes its DB-only callback one typed staged activation
(`candidateBindingId`, `credentialVersion`, `secretRef`). An existing binding
requires an exact `expectedCredentialVersion + 1` rotation and its locked old
head/ref; a setting candidate must be a new binding at version 1. The kernel
then proves post-callback binding/ref/version ownership. Callback, proof, audit,
or lifecycle-CAS failure rolls back the savepoint.

After apply and proof, finalization calls the transaction-scoped authoritative
organization/workspace state loader and caches that exact state projection; it
accepts a 2xx status but no caller-provided response body. A binding target must
appear in its configured or effective authoritative state. A staged organization
candidate must be the configured default; a staged workspace candidate must be
the configured custom binding. Audit metadata remains fixed version/lifecycle
data. Neither cache nor audit receives secret bytes, bundles, header values,
candidate IDs, or refs.

The explicit staging writer/cleanup and superseded-secret outbox entry points use
short claims only. Every `SecretStore.put` or `delete` occurs outside a database
transaction. Each reconciliation delete receives a cancellation signal and has a
45-second default wait, shorter than its cleanup lease. Timeout or cancellation
uses the normal failed completion path: staging tombstones reschedule and
superseded-secret work records back off, so a claimed row does not retain an
active lease. A provider may still complete an already-issued Kubernetes patch
late; deletes are idempotent and durable retry work remains authoritative.
Kubernetes retries for one opaque reference share its bounded outstanding delete
patch until it settles or rejects; after that transport result, a later retry
issues a new patch.

Registry periodically runs retention and, when a `SecretStore` is configured,
staging and superseded-secret cleanup. Each maintenance pass has an abortable
deadline. A deadline-bound stage yields first position to following maintenance
stage on next pass, so repeated slow cleanup cannot starve later work. Shutdown
marks maintenance stopped, clears its timer, aborts its active pass, and waits
only its bounded shutdown grace before dependent resources close. A deadline
gives cooperative cleanup that same bounded grace to persist claimed-row failure
completion before a non-cooperative operation detaches. Any late detached
operation has settlement handlers; shutdown can stop waiting but cannot prove an
already-issued provider request stopped.

### Session observability pins

Every public Session create and every cron Trigger dispatch uses the same
transactional domain path. Before taking Agent, Environment, Vault, or Skill
locks, Registry locks observability authority rows, selects either an active
binding or a disabled outcome, then writes one `session_observability_bindings`
row with selection source/status, all selection/revocation/capture epochs,
pin-time effective capture mode, `session_revocation_epoch=0`, Agent id/version,
and resolved Harness/mode from that locked Agent-version snapshot. The row
contains no endpoint, credential reference, or secret material.

Selection availability or authority-corruption errors abort the entire Session
graph transaction. Public create returns a stable Claude `503 overloaded_error`
(`agent observability unavailable`); an archived authoritative parent returns
the normal non-leaking `404 workspace not found`. Trigger dispatch treats these
selection failures as transient fire retries and leaves the Trigger active.

Migration 0049 closes the rolling-upgrade gap for Sessions written after 0048
but before every Registry writer used this path. It backfills every missing pin
from the owning Workspace as a disabled metadata-only row, and an `AFTER INSERT`
fallback on `sessions` does the same for a direct legacy insert. Current
writers insert their authoritative pin before the Session row; the fallback uses
`ON CONFLICT DO NOTHING`, so it cannot replace an active or custom pin. Neither
path writes endpoint, credential-reference, or secret fields. The fallback is
intended only for that mixed-version period, but remains installed and active
until its explicit follow-up removal migration runs.

Session archive and soft deletion lock their exact pin in the normal application
lifecycle transaction. Archive changes an active or disabled pin to `archived`,
records the first `archived_at`, and advances its `session_revocation_epoch`
once; re-archive leaves that pin tombstone and epoch unchanged. Soft deletion
changes an active, disabled, or archived pin to `deleted`, advances the epoch,
preserves an existing archive tombstone, and retains the pin alongside the retained
Session row.

Workspace archive locks its active Workspace, then that Workspace's exact
`(organization_id, workspace_id)` observability setting row and its archive
marker, before locking Sessions. Its first successful archive inserts one
durable marker with the exact archive timestamp and post-increment
`revocation_epoch`, CASes only that setting epoch, then changes Workspace
status. Re-archive leaves the epoch and marker unchanged. Mode, binding,
selection/capture epochs, binding rows, and credential heads remain unchanged.
A missing, invalid, overflowing, or lost-CAS setting row aborts the whole
Workspace archive transaction.

Migration 0056 backfills every prior archived Workspace and temporarily
enforces the same fence for direct old writes through a `workspaces` trigger.
The trigger locks and validates the exact setting and marker. Its marker
inserter advances an unaccounted first archive; an existing marker must match
the Workspace archive identity and current setting epoch, so it recognizes a
current application's pre-status write without a second increment. A missing,
malformed, or overflowing setting or marker aborts the direct archive statement
and its enclosing transaction. Re-archive does not advance the epoch.

Migration 0054 adds a temporary database lifecycle trigger as mixed-version
enforcement for older writers that directly change `sessions.archived_at` or
delete a Session. Migration 0058 also applies this enforcement to `deleted_at`
updates. On a first archive it turns the exact active or disabled pin
into an archived tombstone using the Session timestamp; on delete it creates a
deleted tombstone with one statement-stable timestamp. It recognizes the valid
archived or deleted tombstone already written by the current application path
and does not advance the epoch twice. A missing or malformed pin aborts the
Session statement and its enclosing transaction. Workspace archive performs
its batch Session archive through this same trigger, so its workspace, API-key,
Session, pin, outbox, and audit changes all roll back together on a bad pin.

### Cron Triggers API

`/v1/triggers` is an Orca-owned Core extension rather than an Anthropic
Deployment alias. It exposes create/list/get/update/delete, pause/unpause, and
Trigger Session history. Each five-field cron slot creates a new ordinary
Session with one initial `user.message`; the Trigger pins the Agent version and
stores no raw secret or repository token.

Registry replicas run an in-process planner and dispatcher. The planner
materializes unique fire rows with `FOR UPDATE SKIP LOCKED`; the dispatcher
atomically creates the Session graph, initial-event outbox, and fire link. No
network call occurs in that transaction. PostgreSQL locks, generation fencing,
and the unique slot key coordinate replicas, so an independent scheduler
service is not required. Set `TRIGGER_SCHEDULER_ENABLED=false` for API-only
Registry replicas. Full semantics are in [`../cron-triggers.md`](../cron-triggers.md).

### Files API

The `File` shape mirrors Anthropic's contract:

```jsonc
{
  "id": "file_…",
  "filename": "report.pdf",
  "mime_type": "application/pdf",
  "size_bytes": 12345,
  "sha256": "…",
  "metadata": { "k": "v" },
  "purpose": "agent" | "agent_output",
  "scope_id": "ses_…" | null,
  "downloadable": true | false,
  "archived_at": null,
  "created_at": "…",
  "updated_at": "…"
}
```

- `POST /v1/files` accepts `purpose` / `scope_id` as multipart text fields;
  `downloadable` is **always derived server-side** from `purpose` (the route
  ignores client-supplied `downloadable` to prevent privilege escalation —
  user uploads must not become directly retrievable).
- `GET /v1/files?scope_id=ses_…` filters to a single session's files.
  `?scope_id=` (empty) is a no-op (returns all rows). Unknown ids return an
  empty page.
- `GET /v1/files/:id/content` enforces a **403 gate** on
  `record.downloadable === false` — `purpose='agent'` user uploads are not
  retrievable per Anthropic's contract; only `agent_output` files (which
  default to `downloadable=true`) pass.
- Orca's Session-nested extension exposes the same output records through
  `GET /v1/sessions/:id/files`, `GET /v1/sessions/:id/files/:file_id`,
  `GET /v1/sessions/:id/files/:file_id/content`, and
  `DELETE /v1/sessions/:id/files/:file_id`. These routes validate the Session
  in the caller's workspace and require both `purpose='agent_output'` and
  `scope_id` equal to the addressed Session; callers cannot select a different
  scope.

See [`../libraries/file-store.md`](../libraries/file-store.md) for the
`FileRecord` shape and dedup semantics, and
[`../output-capture.md`](../output-capture.md) for the agent-output flow.

### Per-resource `mount_strategy`

`session_resources` carries an optional `mount_strategy` field, valid only
on file resources. The only accepted value is `tarball_prefetch` — the API
rejects anything else. File resources are always materialized host-side via
tarball prefetch; the sandbox never mounts the file-blob namespace and its
execution credentials carry no file-blob grant — see
[`../mount-strategies.md`](../mount-strategies.md).

### MemoryStores API

The `MemoryStore`, `Memory`, and `MemoryVersion` shapes mirror Anthropic's
contract. IDs use the prefixes `mems_…`, `mem_…`, and `memver_…`. The full
ts-rest contract lives at
`services/registry-service-ts/src/contracts/memory-stores.contract.ts`.

| Route                                                      | Method | Notes                                                                                                |
| ---------------------------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------- |
| `/v1/memory_stores`                                        | POST   | Create a workspace-scoped store; `name` is unique per workspace                                      |
| `/v1/memory_stores`                                        | GET    | List with pagination                                                                                 |
| `/v1/memory_stores/:id`                                    | GET    | Fetch a store record                                                                                 |
| `/v1/memory_stores/:id/archive`                            | POST   | Soft-archive (`archived_at` set; bytes retained in S3)                                               |
| `/v1/memory_stores/:id`                                    | DELETE | Soft-delete metadata with `deleted_at`; retain live and version blobs                                |
| `/v1/memory_stores/:id/memories`                           | POST   | Create from required UTF-8 `path` + `content`; Registry derives content size and SHA-256             |
| `/v1/memory_stores/:id/memories`                           | GET    | List all memory paths in a store                                                                     |
| `/v1/memory_stores/:id/memories/:memory_id`                | GET    | Fetch a memory record                                                                                |
| `/v1/memory_stores/:id/memories/:memory_id`                | POST   | Claude-compatible update of `content` and optional `path`; `precondition.content_sha256` enables CAS |
| `/v1/memory_stores/:id/memories/:memory_id`                | DELETE | Remove a memory; the prior version chain stays accessible via `/memory_versions`                     |
| `/v1/memory_stores/:id/memory_versions`                    | GET    | List the version chain (optionally scoped to a single `memory_id`)                                   |
| `/v1/memory_stores/:id/memory_versions/:version_id/redact` | POST   | Mark a version `redacted_at` and delete the version blob (audit row stays)                           |

CRUD writes accept `Idempotency-Key`. Conflict semantics on `writeMemory`
match `MemoryConflictError` — the public route returns 409, the watcher's
internal route falls back to last-writer-wins (see below).

### Memory mount resources

`session_resources` accepts
`{ type: 'memory_store', memory_store_id, access?, instructions? }`.
`mount_path` is output-only: Registry derives and persists
`/mnt/memory/{store_name}/`, and rejects caller-supplied memory paths. The
mount mechanism is picked by the harness (`memory_fuse` on FUSE-capable
runtimes, `local_memory` on runtimes with the Files API fallback) and is not client-settable — the API
rejects `mount_strategy` on non-file resources. See
[`../libraries/memory-store.md`](../libraries/memory-store.md) for the
library shape and [`../mount-strategies.md`](../mount-strategies.md) for
the strategy decision matrix.

### Session Threads API (Anthropic thread-model multiagent)

A coordinator session (an agent created with a `multiagent` roster) runs ONE
session across MULTIPLE threads: the **primary** thread IS the session-level
event stream (`parent_thread_id = null`, empty transcript subpath), and each
roster subagent the coordinator delegates to runs in its own child thread whose
events live under the `subagents/<thread_id>` subpath. The Threads API is the
read model over `session_threads` (projected from the primary stream's
`session.thread_*` events — see [`../data-model.md`](../data-model.md) and
[`../session-runner-scope.md`](../session-runner-scope.md)). Every session gets
a primary `session_threads` row inside the create transaction (empty subpath),
so a single-agent session lists exactly one thread; child threads appear only
when a coordinator spawns roster subagents, keeping the single-agent path
otherwise unchanged (purely additive).

The `SessionThread` wire shape (`SessionThread` in
`contracts/sessions.contract.ts`; `agent_name` and `stop_reason` exist only as
`session_threads` columns, not on the wire):

```
SessionThread {
  type               "session_thread"
  id                 sth_…
  session_id         ses_…
  parent_thread_id   sth_… | null      # null = the PRIMARY thread
  agent              { … }             # roster agent descriptor for this thread
  status             running|idle|rescheduling|terminated
  stats              thread stats | null
  usage              token usage | null
  archived_at        timestamp | null
  created_at         timestamp
  updated_at         timestamp
}
```

Endpoints (all workspace-scoped; a session-not-found and a thread-not-found are
distinct 404s):

- `GET  /v1/sessions/{id}/threads` — list a session's threads, newest-first
  (`desc(created_at, id)` — the primary row, created with the session, sorts
  last); `page`/`limit` only, and archived threads are not filtered out.
  (Anthropic's spec orders primary-first and does not serve an
  `include_archived` toggle either — the ordering divergence is **registered
  and decided**: prose-invariant `threads-list-ordering`, decision `fix-later`
  in `conformance-decisions.yaml` (`prose-threads-list-ordering`), rendered in
  [`../conformance-matrix.md`](../conformance-matrix.md).)
- `GET  /v1/sessions/{id}/threads/{thread_id}` — one thread.
- `POST /v1/sessions/{id}/threads/{thread_id}/interrupt` — Orca extension (not
  in Anthropic's thread surface): record the client's interrupt intent — append
  a `session.thread_status_terminated` on the target thread's own stream (the
  durable signal a live runner notices there; the event-index projection also
  surfaces it on the primary) then flip the read-model row to `terminated` so an
  immediate GET reflects it. A transcript-append failure returns **502**
  (`upstream-unavailable`) and does not touch the read model.
- `POST /v1/sessions/{id}/threads/{thread_id}/archive` — archive a thread
  (idempotent in status and response code; re-archiving overwrites
  `archived_at` with the newer timestamp); the row keeps `archived_at` but
  still appears in the list (no server-side archived filter).
- `GET  /v1/sessions/{id}/threads/{thread_id}/events` — the session events read
  model scoped to the thread's subpath (paged; same cursor/limit contract as
  `GET /v1/sessions/{id}/events`).
- `GET  /v1/sessions/{id}/threads/{thread_id}/stream` — the session SSE stream
  scoped to the thread's subpath (the primary thread streams the whole session at
  the empty subpath; a child streams only its `subagents/<id>` events).

Client replies to a subagent's gated / custom-tool request are posted against the
**session** (not the child thread) carrying the child's `session_thread_id`; the
`POST /v1/sessions/{id}/events` route rewrites their subpath to the originating
thread — see responsibility 5 above.

### `/internal/memory_versions` (mesh-only)

The harness's `MemoryVersionWatcher` polls each session's mounted memory
store every ~2 s and registers detected writes via
`POST /internal/v1/workspaces/{workspace}/sessions/{session}/memory-stores/{store}/memory-versions`.
The route shape is documented in
`services/registry-service-ts/src/contracts/internal.contract.ts`:

```jsonc
// Request
{
  "path": "plans/2026-q2.md",
  "content_base64": "…",
  "content_sha256": "…",
  "previous_sha256": "…" | null,    // watcher's cached sha for CAS
  "written_by_event_id": "evt_…"    // optional
}

// Response 201
{
  "memory":  { /* MemoryRecord */ },
  "version": { /* MemoryVersionRecord */ },
  "conflict": false                  // true → last-writer-wins applied; emit session.memory_conflict
}
```

Registry first verifies that the store is an active resource on the path
session and is attached `read_write`; workspace, store, and writer session
cannot be selected in the body. Unlike the public PATCH (which 409s on CAS
mismatch), this internal route falls
back to **last-writer-wins** on `MemoryConflictError`. The
watcher emits a `session.memory_conflict` event on the transcript stream
when `conflict: true`. See
[`../memory-conflict-semantics.md`](../memory-conflict-semantics.md).

The route exists only on the internal listener. Kubernetes TokenReview auth
constrains callers to the configured Harness ServiceAccount; network and mesh
policies may further restrict reachability.

### Environment worker-tunnel auth + durable claims

An environment carries a **worker/host tunnel credential** (the env key) and a
**durable claim** so a worker launched into the environment can open its tunnel
back to the registry replica that owns it. The registry runs multiple replicas,
so neither the credential nor the owner can live in process memory: the env-key
digest sits on the `environments` row and the claim sits in the
`environment_claims` table (see [`../data-model.md`](../data-model.md)). The pure
decision logic + thin Drizzle wrappers live in
`services/registry-service-ts/src/domain/environment-key-state.ts` and
`.../environment-claims.ts`; the routes are hand-mounted in
`src/api/internal.routes.ts` and declared in
`src/contracts/internal.contract.ts`.

**Env key.** `POST /v1/environments` arms a key on create; `…/rotate-key`
rotates it (the new digest atomically revokes the prior key) and `…/revoke-key`
clears it. The raw `sk-…` key is echoed exactly once at create/rotate and never
persisted or returned again — only the digest + expiry are stored, so a DB leak
cannot reconstruct it. Reads (`GET`, list) expose only `env_key_set` +
`env_key_expires_at`.

The key is an Orca concept — Anthropic's `BetaEnvironment` has none — so the
whole of it is `orca-beta`-gated on the Anthropic-shaped routes: create echoes
`env_key` and reads expose `env_key_set` / `env_key_expires_at` only when the
caller sends `orca-beta`. A default caller still gets a key armed on their
environment; they recover it through `…/rotate-key`, which is an extension route
and answers any caller. See
[`../orca-extensions.md`](../orca-extensions.md#environment-key-lifecycle).

**Worker auth.** `POST /internal/environments/{id}/verify-key` authenticates a
worker's presented raw key against the stored digest + expiry, rejecting
archived environments. It **fails closed**: an unknown id, a wrong/expired/
revoked key, or an archived environment all return `{ "valid": false }` with no
workspace leaked and no existence oracle. On success it returns
`{ "valid": true, "workspace_id": "ws_…" }`.

**Managed auth (Environment Token).** A registry-_launched_ worker (a
server-managed sandbox with no operator to provision an Env Key ahead of time)
authenticates with a per-launch **Environment Token** instead: `environments`
carries a second digest+expiry pair — `environment_token_digest` /
`environment_token_expires_at`, same shape and same row as the Env Key pair
above. `mintEnvironmentToken` / `resolveEnvironmentToken`
(`EnvironmentTokenStore` in `src/domain/environment-token-store.ts`, over the
pure primitives in `environment-token.ts` / `environment-token-state.ts`) mint
and verify it; the environment-launch lifecycle calls
`mintEnvironmentToken` when it launches a managed worker — arming the token
before the worker's first dial, with the Env Key's 7-day TTL (the worker resends
the same token on every reconnect, so reconnects within that window keep
resolving), and revoking it on launch failure or environment teardown. On the worker tunnel
(`src/api/worker-tunnel.routes.ts`), a presented `X-Orca-Environment-Token`
header is resolved against this pair and **MUST** succeed or the connection is
refused (`4004`) — it never falls through to the Env-Key check below it. Absent
that header, the Env-Key path is completely unaffected. This is a _route-level_
fork, layered ABOVE the reusable `@orca/harness-tunnel` engine and independent
of the engine's own (separate, and on this route still deliberately inert)
`X-Orca-Host-Token` / `resolveLaunchToken` seam described below — the two
headers, two credential stores, and two auth decisions do not interact.

**Durable claims** — the multi-replica equivalent of an in-memory tunnel/host
registry. One exclusive claim per `environment_id` (enforced by the table's
primary key), **newest-wins** on reconnect, a `last_ping` heartbeat with a
staleness TTL, and connection-scoped release:

| Route                                        | Method | Notes                                                                                                                                                                                                                                                                                                       |
| -------------------------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/internal/environments/:id/claim`           | PUT    | Newest-wins: a fresh claim unconditionally replaces any existing one, so a reconnecting/relocated worker takes over. Body `{ owner_pod, worker_conn_id }`.                                                                                                                                                  |
| `/internal/environments/:id/claim/heartbeat` | POST   | Advance `last_ping`. **Connection-scoped**: a ping from a connection already taken over (newest-wins) is a no-op (`refreshed:false`) and never resurrects the stale owner.                                                                                                                                  |
| `/internal/environments/:id/claim/release`   | POST   | **Connection-scoped** release (worker teardown): drops the row only when the releasing `worker_conn_id` still owns it. A superseded worker's release is a no-op (`released:false`), so it can never delete the live owner's claim and unclaim an environment that still has a live owner.                   |
| `/internal/environments/:id/claim`           | GET    | Current owner, or `{ "claim": null }` when unclaimed.                                                                                                                                                                                                                                                       |
| `/internal/environments/claims/reap`         | POST   | Background sweeper: bulk-delete every claim with `last_ping < now - ttl`. This is what gives the TTL effect on its own — without it a dead owner's row would linger until a _new_ worker claims the same environment. The registry also runs this on an interval (`ENVIRONMENT_CLAIM_TTL_MS`, default 90s). |

Staleness uses a strict boundary: a claim is reapable once `now - last_ping >
ttl` (a claim exactly on the TTL is still live), and the bulk reaper's cutoff is
derived from the same arithmetic so the predicate and the sweep never drift.
`worker_conn_id` is the connection identity the worker asserts; release and
heartbeat are scoped to it, while claim (newest-wins) is not. An
operator/cascade escape hatch (`releaseUnconditional`, plus the FK
`ON DELETE CASCADE`) drops a claim irrespective of owner — never used on the
worker path.

**Work-queue stats (public).** `GET /v1/environments/:id/work_stats` is the
public, workspace-scoped read of an environment's `self_hosted` distribution
backlog — the self-hosted equivalent of a managed work-queue's depth/lease view.
It is declared in `src/contracts/environments.contract.ts` (`workStats`) and
mounted in `src/api/environments.routes.ts`; the pure partition + liveness math
lives in `src/domain/work-stats.ts`. A missing / cross-workspace id 404s before
any counting (the row lookup is the tenancy boundary). It returns three fields:

| Field              | Type    | Meaning                                                                                                                                               |
| ------------------ | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `depth`            | integer | Sessions PENDING with no runner minted yet (`distribution_state='pending'` AND `runner_id IS NULL`) — the queue depth nobody has picked up.           |
| `in_flight`        | integer | Sessions a worker is already handling but that are not complete: launch-in-flight (`pending` with a `runner_id`) plus connected/running (`assigned`). |
| `worker_connected` | boolean | Whether a worker claim for the environment is currently live (durable `environment_claims` heartbeat within the TTL).                                 |

`depth` + `in_flight` are a disjoint partition of the non-terminal distributed
sessions; FAILED (terminal) sessions and sessions with a null
`distribution_state` match no filter and are excluded by the counting query.
`POST /v1/sessions` dispatches every session it creates on a `self_hosted` or
`cloud` environment; a Trigger-created session is not dispatched and keeps a
null `distribution_state`. A dispatched session the distributor does not launch
— no worker is connected yet, or it is a `cloud` session harness-server runs,
such as `cloud`+`separate`, which is never launched — is persisted `pending`
with no runner and counts toward `depth`. The endpoint serves **both targets** —
the same distributor, claim, and dispatch machinery runs for `self_hosted`
sessions and the `cloud` `colocated` sessions the registry runs — so a cloud
environment's dispatched sessions count toward `depth` from creation, before
its provisioned worker comes online and launches begin; the route is always
available.

`worker_connected` is deliberately the **durable-claim** signal, not the
distributor's dispatchability. It reuses the same `environment_claims` heartbeat
and staleness TTL the claim reaper sweeps with (`ENVIRONMENT_CLAIM_TTL_MS`), so it
is the correct **cross-replica** answer to "is a worker claim live _somewhere_"
for a stats endpoint that any replica can serve. It can legitimately diverge from
the distributor's per-replica in-memory `WorkerRegistry` dispatchability
(`isDispatchable` in `src/tunnel/session-distributor.ts`), which answers the
narrower "will a launch on _this_ pod succeed right now". Read
`worker_connected: true` as "a worker claim is live somewhere", **not** as a
guarantee that a launch will be dispatched immediately.

**Worker WS endpoint + claim-store mapping.** The
worker dials a public WebSocket — `WS /v1/tunnels/environments/:environmentId`,
the worker-tunnel sibling of the runner tunnel below — because the worker runs
outside the mesh (a laptop, a customer VM) and cannot reach the `/internal/*`
listener; the claim HTTP routes above are the mesh-only durable backing the WS
drives. That WS is wired in `src/api/worker-tunnel.routes.ts` on the reusable
`WorkerTunnelServer` engine (`@orca/harness-tunnel`), and the engine's persistent
"worker store" seam is deliberately mapped onto the `environment_claims` store
rather than a per-connection host-row model. Three consequences are intentional:

- **No host-row upsert.** The connect seam does not upsert a host row guarded by
  an owner-conflict check. It _claims_ the environment (`PUT …/claim`,
  newest-wins) and _releases_ it on
  teardown (connection-scoped); the env-key auth gate (below) is the credential,
  and the `environments` row — not a per-connection host row — is the durable
  identity. There is no separate host table on this path.
- **No single-user re-own override.** Because claims are unconditionally
  newest-wins, no escape hatch lets a host re-own an id already held by a
  different owner: the engine's re-own flag is left at its `false` default and
  never overridden. A reconnect
  takes over via newest-wins, not via an owner-conflict bypass.
- **Hello readiness is not persisted.** The worker's `worker.hello` advertises its
  per-harness readiness; the engine validates the hello and registers it in the
  in-memory `WorkerRegistry` (so a live caller can read it), but this path does not
  write that readiness to any durable row — there is no host row to carry it.

The engine's managed-launch-token credential path (`resolveLaunchToken` →
worker-id + owner, gated on a dedicated token header) is likewise inert on this
route: the env key is the only credential, verified in the route before the
engine runs (see **Worker auth** above), so the route's token resolver always
returns `null`. The engine treats a present-but-unresolvable managed token as an
auth refusal (it never falls through to the route's resolved-owner provider), so
this path is not just inert but **fail-closed** — a peer that presents the token
header is closed with `4004` even when it also presents a valid env key, and a
stray token header therefore cannot ride a good env key into an authenticated
tunnel. The token resolve / mismatch / unknown-fail-closed semantics still ship
and are covered at the engine layer (`packages/harness-tunnel`), and the route
spec (`test/unit/api-worker-tunnel.routes.spec.ts`) pins the route's own choice —
a presented token header fails closed; only the env key authenticates.

The **control routes** above (`verify-key`, the durable-claim routes, claims
reap) are mesh-only — `req.url` starts with `/internal/` so the auth
pre-handler (`src/auth/auth.ts`) skips the api-key/OIDC check, and the internal
listener's own pre-handler (`src/auth/internal-auth.ts`) admits only the Harness
workload identity or the shared internal service token to them — the same
internal-listener boundary as the vault-resolve and session lifecycle routes. The **worker WS
tunnel itself** (`WS /v1/tunnels/environments/:environmentId`) is the
exception: it lives on the public listener under the `/v1/tunnels/` auth
bypass and self-authenticates with the raw env key (see the Auth section
below).

### Runner tunnel (public, token-authed) — `WS /v1/tunnels/runners/:runnerId`

> The self-hosted runner transport: a WebSocket channel; binding-token +
> loopback-only + owner-fail-closed auth; owner / registration model. See
> [`../harness-modes.md`](../harness-modes.md) ("Transport abstraction").

Every `colocated` runner (`mode: colocated`) — cloud or self-hosted — dials this
same outbound tunnel; it is the one transport for `colocated`, by design (no
per-`target` transport split). The need is sharpest for `target: self_hosted`:
that runner runs on compute the control plane cannot reach back into — a
developer laptop behind NAT, an outbound-only network. It cannot accept an
inbound dial and it cannot reach the mesh-only `/internal/*` listener. So — like
the worker/host tunnel above, which dials the same public listener presenting
its raw env key — the runner opens an **outbound WebSocket** to the registry and
the registry pushes framed HTTP requests back down it. Both tunnels therefore
live on the **public, internet-facing** listener, where authentication is each
handler's own job, not the mesh's: the worker self-authenticates by env key, the
runner by its per-runner binding token.

The WS handler is in `src/api/runner-tunnel.routes.ts`; its auth posture is
assembled from config in `src/auth/tunnel-auth.ts`. Because the endpoint sits on
the public listener, the global app-auth pre-handler is **bypassed** for
`/v1/tunnels/*` (see the Auth section below and `src/auth/auth.ts`): the runner
presents no `x-api-key` and no OIDC token — only its tunnel binding token in the
`X-Orca-Runner-Tunnel-Token` handshake header — so the handler self-authenticates
the dial. The endpoint is **safe-by-default even though the listener binds
`0.0.0.0`**; the handshake is refused with a WS close frame (never registered)
unless every one of these gates passes:

- **CSWSH origin guard.** A browser cross-site-WebSocket-hijack attempt — a
  forbidden `Origin` — is closed with `4403` before any protocol I/O. In
  local mode (the default, see `RUNNER_TUNNEL_LOCAL_MODE`) only a loopback
  `Origin`, the internal sentinel, or an explicitly allow-listed origin
  (`RUNNER_TUNNEL_ALLOWED_ORIGINS`) is accepted; a missing `Origin` (non-browser
  clients never send one) is allowed. In non-local mode the connection is
  cookie/proxy-authenticated, so any `Origin` passes unless an allow-list is
  configured (then it is deny-by-default).
- **Binding-token correlation.** When the runner presents a tunnel token, the
  path `runnerId` must equal the id cryptographically bound to that token (or, in
  an allow-list deployment, the token must be in `RUNNER_TUNNEL_TOKENS` and the
  runner may use a stable id). A loopback peer may omit the token (the single-user
  local-runner flow) and bypasses the allow-list. Mismatch / empty /
  unauthorized → `4004`.
- **Owner resolution, fail-closed.** The owner is resolved from the handshake and
  an unauthenticated **non-loopback** peer is refused with `4004` rather than
  registered owner-less. This matters because the token gate above only proves the
  peer knows _a_ token; in the no-allow-list default any attacker-chosen non-empty
  token derives a valid id and clears that gate. Registering owner-less would
  bypass the owner-scoped listing filter and binding-ownership check (both skip
  enforcement when the owner is unset), making the runner visible to — and bindable
  by — every tenant. The fail-closed gate is what stops that.

**Safe default (no operator config).** With neither `RUNNER_TUNNEL_TOKENS` nor an
external identity provider set, the route wires a **loopback-only auth provider**:
a local runner dialing over loopback registers as the reserved local owner
(`'local'`), and every remote peer resolves to no identity and is refused by the
fail-closed gate. Admitting remote runners is therefore an **explicit opt-in**
(provision `RUNNER_TUNNEL_TOKENS`), never the silent default of a `0.0.0.0`
listener.

The two companion HTTP reads — `GET /internal/runners` (list the caller's online
runners) and `GET /internal/runners/:runnerId/status` (online + host-reported exit
cause) — are control-plane queries the registry / harness issue, so they stay
**mesh-only** under `/internal/*`. Only the WS tunnel itself moved to the public
namespace; the old mesh-only tunnel path is gone (`api-runner-tunnel.routes.spec.ts`
pins both the public mount and the 404 on the retired `/internal/...` tunnel path).

| Env var                         | Purpose                                                                                                                                                                                                                                                 | Default                                    |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `RUNNER_TUNNEL_TOKENS`          | Comma-separated binding-token allow-list. Non-empty admits **remote** runners whose `X-Orca-Runner-Tunnel-Token` is one of these exact tokens (each registers under a stable, not token-derived, id). The opt-in that opens the tunnel beyond loopback. | empty (loopback-only; remote fails closed) |
| `RUNNER_TUNNEL_ALLOWED_ORIGINS` | Comma-separated extra permitted WS `Origin` values for the CSWSH guard, beyond the internal sentinel + loopback hosts.                                                                                                                                  | empty                                      |
| `RUNNER_TUNNEL_LOCAL_MODE`      | CSWSH local-mode flag. In local mode an `Origin` is allowed only when its hostname is loopback (the single-user posture, no cookie/proxy auth).                                                                                                         | `true`                                     |

#### AI-gateway egress + environment-launcher configuration

For `separate` Sessions, create metadata
`{"orca_llm_egress":"gateway"}` opts model requests into the Harness's
`LLM_GATEWAY_URL`. The Registry validates `direct` and `gateway` values on
Session create and update; a change to this key requires an idle Session.
Omitting it applies the Harness's `LLM_EGRESS_DEFAULT` (`direct` when unset).
Registry mints the scoped `ai-gateway` JWT on the internal harness path.

| Env var                             | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Default                       |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `AI_GATEWAY_MCP_URL`                | Public ai-gateway MCP base URL. Gateway egress (`egress_mode` `gateway`, the default) rewrites the agent's MCP servers to this URL and carries a scoped session JWT in the agent snapshot delivered to a runner at session start. Unset ⇒ a gateway-egress session's snapshot fails to resolve; `sidecar` sessions resolve without it.                                                                                                                                                                                                                               | empty                         |
| `AI_GATEWAY_LLM_URL`                | Public ai-gateway LLM-proxy base URL. When set alongside `AI_GATEWAY_MCP_URL`, gateway egress carries an LLM-proxy URL and a scoped JWT so the runner reaches the model through the gateway without holding a provider key. The JWT is `aud='llm-proxy'`, except for `codex_sdk` and `pi_sdk`: theirs carries the Session JWT audience (`SESSION_JWT_AUDIENCE`) with `llm_routes` / `llm_models` claims for the harness's one native route and its model, and their snapshot fails to resolve unless `SESSION_JWT_LLM_ROUTES` / `SESSION_JWT_LLM_MODELS` admit both. | empty                         |
| `AI_GATEWAY_LLM_JWT_TTL_SECS`       | Lifetime (seconds) of the `aud='llm-proxy'` JWT — an independently-scoped, short-lived credential minted deliberately shorter than the session/MCP JWT's `SESSION_JWT_TTL_SECS` so a leaked LLM token expires fast. A non-positive value falls back to the default. `codex_sdk` and `pi_sdk` tokens use this value or 660 seconds, whichever is longer, to cover the bounded model turn.                                                                                                                                                                             | `120`                         |
| `ORCA_ENVIRONMENT_LAUNCHER_BACKEND` | Selects the environment-provisioning backend the registry launches `colocated` workers with (`local`, `e2b`, `opensandbox`), passed to the launcher factory. Unset ⇒ no environment-launch lifecycle is wired.                                                                                                                                                                                                                                                                                                                                                       | unset                         |
| `ORCA_REGISTRY_TUNNEL_URL`          | Registry origin used by launched workers for the WS tunnel and by managed Git tools for the read-only Git proxy. Defaults to this process's loopback origin for local workers; cloud and remote self-hosted runners set an origin reachable from their sandbox. `ws`/`wss` is converted to `http`/`https` for Git.                                                                                                                                                                                                                                                   | `http://localhost:<httpPort>` |

#### Owner-pod event bridge (single writer over the bound tunnel)

`REGISTRY_LOG_LEVEL` enables the Registry's structured logger, including event
bridge diagnostics. A turn refused because its guardrail snapshot cannot be
refreshed is logged at `error` with `sessionId`, `runnerId`, and `userEventId`.
This identifies the refused message separately from initial snapshot delivery;
the bridge does not publish a `session.error` for that dispatch failure.

The runner tunnel is the transport; the **`SessionEventBridge`**
(`src/tunnel/session-event-bridge.ts`, managed by `SessionEventBridgeManager`) is
what rides it to give every `colocated` session — cloud or self-hosted — the
same single-writer, persist-before-forward behavior. The replica
that holds the environment's durable claim — the **owner pod** — is the only one
that can reach the runner, so on each runner-tunnel **connect** it starts a bridge
for that runner's bound session and on **disconnect** it stops it (wired through
the same `onRunnerConnect` / `onRunnerDisconnect` hooks the distributor uses;
newest-wins reconnect replaces the bridge). The bridge resolves the runner's bound
`(workspace, session)` from the `sessions` row the distributor wrote
(`runner_id`), so it is a no-op for a runner with no session distributed here —
runners for both targets (`self_hosted` and `cloud`) ride the same bridge once
dispatched.

The bridge is the session's **single writer** of agent events and the
turn-forwarder of its user events, persisting before it forwards:

- **User turns → runner.** It follows the transcript for parent-agent `user.*`
  events and sends each as a streaming `POST /v1/runner/turn` over the tunnel
  (`X-Orca-Session-Id` scopes the turn). The runner answers by streaming agent
  events back as newline-delimited JSON in the response body.
- **Agent output → transcript (I1).** Each streamed agent event is appended to the
  transcript (`producedBy=harness`), awaited and in order, **before** the next is
  consumed — the persist-before-forward invariant. The public SSE path tails the
  same transcript, so clients observe only persisted agent events; the bridge never
  writes to a client socket. This is the transcript-as-cross-pod-bus model, with no
  cross-replica HTTP forwarding.
- **First-turn catch-up + gapless boundary.** Because a client can append the first
  `user.message` before the runner connects, the bridge does **not** subscribe
  from-now. At start it reads the transcript from the beginning, drives every
  un-driven `user.*` turn (one with no output linked to its event ID) in order, then follows
  live from an **explicit** cursor `head + 1` — gapless across Kafka / Postgres /
  Pulsar. Registry stamps every parent output with `source_event_id`, and completion
  markers also carry `turn_event_id`. Output from an earlier turn does not answer
  a later queued prompt, even when it appears after that prompt in the transcript.
  Historical output without either ID retains the legacy sequence boundary.
  An already-answered turn is skipped, so a
  reconnect never re-drives an answered prompt. No separate per-session input
  queue buffers turns; the transcript is the durable buffer.
- **Tool-confirmation verdicts are pushed, not driven as turns.** A
  `user.tool_confirmation` is a `user.*` client event but it is **not a turn** — it
  is the verdict for an in-flight gated tool call within the _current_ turn (the
  runner's harness has parked its `canUseTool` on it). The bridge runs a **second,
  concurrent follower** alongside the turn loop that forwards each
  `user.tool_confirmation` to the runner's `POST /v1/runner/confirmation` route
  (keyed by `tool_use_id`), so the runner resolves the parked approval (allow → the
  tool proceeds; deny → a clean denial back to the model). The two loops **must** be
  concurrent: a turn parked on a gated tool blocks the turn loop inside its streaming
  POST, so the verdict can only be delivered by a separate loop. The confirmation
  follower is **read-only** on the transcript (it never appends — the gated tool's
  output flows back through the turn's own stream, persisted by the single-writer
  turn loop), and re-pushes (a flapping reconnect's catch-up) are idempotent on the
  runner (first verdict wins). The transcript `user.tool_confirmation` is the verdict's
  durable source of truth, so a contained push failure self-heals on the next
  reconnect. Observability: `registry_service_bridge_confirmations_pushed_total{source,result}`.

See [`../architecture.md`](../architecture.md) ("The `colocated` event bridge:
the registry as shared server, over the tunnel") for the data-flow diagram and
the lossy-on-drop tradeoff, and [`../roadmap.md`](../roadmap.md#designed-not-built)
for the from-now → catch-up decision.

#### Owner-pod reverse-lookup recovery (resume replay on runner (re)connect)

The bridge above keeps a _connected_ runner's session moving. **`SessionRecovery`**
(`src/tunnel/session-recovery.ts`) is its companion for the (re)connect moment: it
puts a runner that just dialed back where it left off. On each runner-tunnel
**connect**, the `SessionEventBridgeManager` (the same owner pod, the same
connect hook) first serves a resume replay, **then** starts the bridge:

1. **Reverse-lookup + replay-forward.** The owner pod reads the session's persisted
   parent-agent events from the transcript (the cross-pod bus — whatever _any_ pod
   persisted) and **pushes** them down the runner tunnel as a streaming
   `POST /v1/runner/replay` (`X-Orca-Session-Id` scopes it; `X-Orca-Resume-Cursor`
   names the slice). The runner applies each event and **dedups by the stable
   transcript `id`**, so a re-push (a flapping reconnect, or an overlapping chunk)
   is idempotent. The slice is keyed on the runner's **last-consumed cursor**,
   which it presents per session in its tunnel **hello** (`resume_cursors`:
   `sessionId → eventId`, the per-session catch-up cursor
   the runner tracks, sourced from the runner side). The connect hook
   (`onRunnerConnect`) threads that
   map through; the `SessionEventBridgeManager` selects the cursor for the resolved
   bound session and calls `recover(cursor)`: a **present** cursor serves an
   incremental `after={cursor}` slice (only the events the runner has not yet
   consumed — the resume win on a reconnect), and an **absent/empty** one (a fresh
   runner, or one whose in-memory state is gone) serves a **fresh full replay**
   from the start.
2. **Bounded pagination.** One bounded-to-head read drains the transcript at its
   captured high-watermark. Public events are pushed in acknowledged pages
   (`replayChunkSize`, default 500, additionally bounded by `maxReplayEvents`,
   default 10k). Only one page is buffered; private events advance the scan without
   consuming replay space. There is no total history cap. Recovery scans to the
   head even when a non-empty cursor occurs beyond 10,000 events, and computes the
   pending turn across the complete history. A read failure aborts recovery; after
   a failed page ACK, later pages are not delivered. Either failure prevents bridge
   startup for a self-hosted `codex_sdk` or `pi_sdk` session with `mode: colocated`;
   for any other session it is logged and the bridge still starts.
3. **Recovery serves; the bridge drives (exactly-once).** Recovery **never appends
   and never drives a turn**. Turn execution stays with the bridge — the single
   writer — whose catch-up is the **sole** turn driver. So there is exactly one
   driver with exactly one definition of "answered" (the bridge's: a user turn with
   any agent event **linked to that turn** is already answered and is **not** re-driven, keeping
   a partial-then-dropped turn's persisted prefix). `recover()` still returns a
   `pendingUserTurn` (keyed on the `agent.turn_completed` marker) but it is a
   **serving-side observation** for diagnostics / an explicit-resume seam — never
   fed into a second driver — so the bridge's and recovery's answered-rules cannot
   conflict at the system level. A recovery delivery failure (runner offline /
   non-2xx / mid-push drop) is logged and, for those self-hosted colocated
   `codex_sdk` / `pi_sdk` sessions, prevents bridge startup; the next reconnect
   re-attempts the replay.

**Runner-side contract (built in `session-runner`).** Two halves live in the
outbound runner binary, **symmetric** with the bridge's `/v1/runner/turn`
partner, and both are implemented: (a) the `/v1/runner/replay` **handler** —
`SessionLoop.applyReplay` applies each pushed event with dedup-by-id and cursor
advancement, acking 2xx (an unexpected apply fault returns 500 so the owner pod
records the replay undelivered); (b) the runner **populates** the hello's
`resume_cursors` via `resumeCursors()` with the last-consumed event id per
in-memory session, so the owner serves the incremental `after={cursor}` slice
rather than a full replay. The server side matches: the runner-tunnel route
reads `resume_cursors` off the hello and threads it through `onRunnerConnect`,
and recovery serves `after={cursor}` when a cursor is present (a runner that
presents none still gets the correct full replay, so the contract degrades
safely). The route + replay path are single-sourced as constants the owner pod
owns (`RUNNER_REPLAY_PATH`, `RUNNER_TURN_PATH`) and exercised in-repo against a
fake runner. The conditions that would reopen this design are in
[`../roadmap.md`](../roadmap.md#designed-not-built).

**Design note: recovery covers every harness mode.** Catch-up does not skip
native-harness sessions. A transcript mirrored from a native CLI could end in a
user item that is a real failed native turn rather than an unanswered task, but
the Orca transcript is **authored** by the owner-pod bridge with an explicit
`agent.turn_completed` marker (it is not a mirror), so that false positive does
not structurally arise — and in any case recovery only _serves_ (the bridge
drives), so even a surfaced pending turn is never auto-executed. Observability: `registry_service_session_recovery_replayed_total{mode=fresh|resume}`.

**Design note: pending detection.** Recovery tracks pending user messages by
event ID, not by inspecting the last history item, and removes a message when
its matching `agent.turn_completed` appears. Legacy
completion markers without source identity retain the sequence-based boundary.
An interrupted turn without a completion marker remains pending; completion of
an earlier turn does not hide later queued messages. Recovery only serves replay,
while the bridge drives turns and skips those with linked output, including
partial output. `pendingUserTurn` is diagnostic-only and cannot execute a turn.

### `POST /v1/git-creds` (public)

Public route consumed by the in-sandbox `orca-git-creds` credential helper.
Auth is JWT-only (no `x-api-key`, no OIDC); `buildAuth`
(`src/auth/auth.ts`) returns early for `/v1/git-creds` so the helper can call us
with just `Authorization: Bearer <session-jwt>`, and the route authenticates the
session JWT itself. The JWT MUST
have `aud='git-creds'` and a populated `repo_urls[]` claim — the public
`ai-gateway` JWT cannot be replayed against this route.

Request body (handler in
`services/registry-service-ts/src/api/git-creds.routes.ts`):

```jsonc
{
  "protocol": "https",
  "host": "github.com",
  "path": "/org/repo", // optional; some git callers omit it
}
```

Success response (`200`):

```jsonc
{ "username": "x-access-token", "password": "<resolved-PAT>" }
```

`x-access-token` is GitHub's PAT-as-password convention; the same shape
also works for fine-grained PATs.

Error matrix:

| Status | When                                                                                                                                                                                                  | Counter label                                                 |
| ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| 400    | Body missing `protocol` or `host`, or `protocol` is not `https`.                                                                                                                                      | `jwt_invalid` / `repo_unmatched`                              |
| 401    | Authorization header missing/malformed; JWT signature, issuer, or expiry invalid; `aud` claim is not `'git-creds'`; required claims (`workspace_id`, `session_id`) missing.                           | `jwt_invalid`                                                 |
| 404    | Requested URL is not in the JWT's `repo_urls` allowlist; no matching `github_repository` row in the session; repo credential not found/archived; PAT cannot be resolved by the secret provider/store. | `repo_unmatched` / `credential_mismatch` / `pat_unresolvable` |
| 403    | Repo credential URL does not match the requested repo URL (URL re-binding post-spawn).                                                                                                                | `credential_mismatch`                                         |
| 409    | Multiple active session resources match the requested canonical repository URL (legacy/inconsistent data); no PAT is returned.                                                                        | `credential_mismatch`                                         |

The PAT is resolved per-call through the repo credential secret reference
— **no caching**. The route is invoked roughly once per in-sandbox git
operation. [`../roadmap.md`](../roadmap.md) records the rate that would justify caching, if
`registry_git_creds_request_total{result='ok'}` rate per session > 30.

### Git read proxy (public)

`GET /v1/git-proxy/{resourceId}/info/refs?service=git-upload-pack` and
`POST /v1/git-proxy/{resourceId}/git-upload-pack` serve read-only Git smart HTTP,
including protocol v2 `ls-refs`. No receive-pack or repository write route exists.
The POST performs a read and bypasses the JSON idempotency cache; every request
revalidates authority before reading Git data.

The runner receives a 900-second `aud=git-proxy` JWT bound to its organization,
workspace, Session, resource ID, exact repository URL, credential ID and credential
revision. The revision hashes the credential's secret reference and update timestamp.
Every request reloads the active workspace, unarchived/nonterminated Session, attached
resource and unarchived credential, including credential ownership. Detach, archive,
repository rebinding and credential rotation revoke existing capabilities. The
`git-proxy` audience cannot call the raw-PAT `/v1/git-creds` endpoint.

The attached Environment's networking policy is also reloaded on every mint and
request. `limited` networking requires the repository hostname to appear explicitly
in `allowed_hosts`; attaching a repository does not grant upstream host access.
Removing that host immediately invalidates existing grants. Unrestricted or omitted
networking permits the repository host. Protocol validation alone is not an egress
boundary: even valid Git fields can carry data to a hostile repository.

Only Registry resolves the PAT and adds outbound Basic `x-access-token:PAT`
authentication. The proxy forwards a validated `Git-Protocol` header and the Git
content headers; it never forwards client authorization, cookies or arbitrary
headers. Upstream HTTPS connections resolve and validate all addresses inside the
connector lookup, rejecting private, loopback and metadata addresses without a
second DNS resolution. Redirects are rejected. Native Git's gzip request bodies
are decoded before forwarding; compressed and inflated bytes are each bounded to
1 MiB. Malformed gzip and unsupported content encodings are rejected. After
inflation, pkt-line lengths, framing, and read-command arguments are validated.
Supported requests are v0/v1 upload-pack negotiation and v2 `ls-refs`/`fetch`;
arbitrary payloads, writes, and unknown extensions are rejected before reading
credentials or contacting upstream. Responses
are bounded to 64 MiB, and upstream calls to 60 seconds with a 15-second idle timeout.
A 65-second request deadline starts before body parsing and closes slow clients.
Client disconnect aborts the upstream call. Unexpected statuses, encodings, content
types and upstream failures produce a fixed error without upstream bytes or headers.
Successful responses carry the Git binary content type and `Cache-Control: no-store`.

### Runner resource checkpoint protocol

For self-hosted `codex_sdk` or `pi_sdk` with `mode: colocated`, the Registry prepares the session's File,
Memory and Git bindings and delivers them before Skills, the agent snapshot, recovery
and the event bridge. File mounts are read-only; Memory mounts retain their configured
access; Git snapshots include the checkout and sanitized local Git metadata. Initial
checkouts and later Git reads use the same scoped proxy; only its HTTPS transport
receives raw Git PATs. Registry preparation addresses its own public listener over
loopback, while repository origins use the configured sandbox-reachable Registry URL.
Tool network access uses exact host
allowlists. Resource binding and network changes produce a new revision.

`SessionResourcesDelivery` stages credential-free resource manifests and bounded
1 MiB chunks over the authenticated runner tunnel. It verifies source digests before
commit and resolves opaque resource binding IDs to Registry-owned Memory stores.
Read-only bindings cannot receive checkpoint writes. Frozen checkpoints are persisted
before the runner receives an acknowledgement and continues the tool call. Managed
preparation and transcript persistence failures prevent subsequent execution. Each bridge
pins its tunnel generation, so a delayed reconnect cannot configure or drive its replacement.
Managed Skill admission includes stateless top-level `block_skills`: the runner
excludes denied Skills from both the catalog and the tool-visible tree before execution.
This does not admit arbitrary tool-phase policies.
Before each turn the Registry refreshes resources, verified Skills and the snapshot;
the runner preserves writable data when the resource binding revision is unchanged.
File `session.resource_mounted` events appear after all preparation acknowledgements,
with stable event IDs per binding revision and idempotent refresh/reconnect delivery.

For Codex and Pi SDK, the bridge also owns request-budget preflight and SDK usage accounting.
It reloads policy, Session counters and the authenticated request actor's current UTC
daily counters before each message. A transaction verifies the assigned runner and
active Session, persists stateful decision updates and a private pending marker for guarded requests, and acknowledges
before SDK submission. The same Session lock validates the effective model from the
pinned AgentVersion plus the persisted Session model override against the selected harness provider
and model allowlist. SDK usage is validated as complete nonnegative safe-integer counters,
priced against that persisted effective provider/model and deduplicated by accepted turn. The shared
usage transaction updates Session statistics and both budget scopes atomically.
Usage, native checkpoint and marker deletion are acknowledged in order before
completion. An interrupt received during resources, Skills, snapshot or budget preparation
prevents the turn POST and emits an interruption error, idle status and turn boundary.
Interrupts are compared with the preparing/running source message's transcript
sequence; a delayed interrupt that precedes that message cannot cancel it or alter
its accounting during reconnect catch-up.
It retains any guarded pending marker while keeping confirmation and interrupt followers
alive; the next unguarded request can proceed. Uncertain accounting fails closed across
reconnect; private checkpoints and markers never enter public events. Internal usage producers cannot write for
Registry-owned Codex, including through the shared internal token. Other harnesses
retain their configured usage authority.

Each output occurrence has a deterministic File ID derived from the workspace, session,
checkpoint and path. The File row is the durable receipt for downloadable output bytes;
`session.output_indexed` uses a stable event ID. Reconnect reconciles these receipts with
the transcript to repair a lost append acknowledgement without creating another logical
output. This does not require physical broker exactly-once delivery. Memory writes reuse
version IDs across retries and retain the existing CAS-then-last-writer-wins conflict
policy. Before deleting Memory paths, an internal `harness.resource_deletions` transcript
record pins each original Memory ID or records that the path is already absent. The
deletion also uses a digest precondition and a durable version receipt. Replaying an old
checkpoint cannot reinterpret an absent or deleted target as a recreated path.

### GitHub session-resource token lifecycle

- `POST /v1/sessions` and `POST /v1/sessions/{id}/resources` accept raw
  `github_repository.authorization_token` values. Raw token is written to
  `SecretStore` only after registry commits a durable
  `git_credential_staging_intents` row. One DB transaction then inserts
  resource-owned `git_credentials` metadata + internal `repo_ref` and consumes
  the staging intent atomically.
- `POST /v1/sessions/{id}/resources/{resource_id}` rotates token under a
  resource-row lock: stage new secret, swap internal credential reference,
  commit, then purge old secret.
- Public GitHub resource responses contain only `id`, `type`, `url`,
  `mount_path`, optional `checkout`, `created_at`, and `updated_at`.
- `git_cred://<id>` remains an input-only Orca extension for existing
  pre-provisioned workspace credentials.
- Failed/rejected DB writes claim the staging intent before purging staged
  secret refs. A startup + periodic reconciler retries stale `cleaning` rows
  and purges expired `pending` refs left by process crashes. Delete/detach
  purges resource-owned credentials; pre-provisioned credentials are retained.
- Session archive archives resource-owned Git credential rows and purges their
  SecretStore values. HTTPS repository URLs without owner/repository path
  segments are rejected to prevent host-wide credential bindings.
- Session create/attach rejects a second active `github_repository` with the
  same canonical URL. The credential-helper protocol supplies only the remote
  URL, so allowing separate resource-owned tokens would make token selection
  ambiguous.

#### `SessionJwtMinter`

`SessionJwtMinter.mint(input, options?)`
(`services/registry-service-ts/src/auth/session-jwt.ts`) accepts an optional
`options.audience` to override the default `aud` claim, plus an optional
`options.repoUrls` custom claim. The ai-gateway minter and the git-creds
minter share this code path — the ai-gateway callers
use the configured default audience (`'ai-gateway'`); the harness's
git-creds caller passes `audience: 'git-creds'` and `repoUrls`.
`verify(token, { expectedAudience })` rejects tokens whose `aud` claim
does not match the caller's expectation.

#### Git-credential metrics

| Metric                                     | Type    | Notes                                                                                                               |
| ------------------------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------- |
| `registry_git_creds_request_total{result}` | Counter | `result` ∈ `ok / jwt_invalid / credential_mismatch / pat_unresolvable / repo_unmatched`. One increment per request. |

The corresponding `harness_git_clone_seconds` + `harness_git_clone_total`
metrics are documented in [`./harness-server.md`](./harness-server.md).

### Vault credentials API

`/v1/vaults` is the Anthropic-style metadata container API: `display_name`,
`metadata`, and lifecycle fields only. Vault IDs use the `vlt_` prefix. List routes
return `{ data, next_page }` and accept `limit`, `page`, and `include_archived`.
`metadata` updates are patches: string values upsert, `null` deletes, and omitted
keys are preserved. Runtime auth lives in nested credentials.

Vault archive and DELETE revoke credentials even when an undeleted Session references
the Vault. DELETE retains Vault and credential metadata, but purges their stored secret
payloads; subsequent execution preparation and MCP credential resolution reject those
credentials. Retained Session bindings do not pin Vault credentials or preserve their
runtime availability.

Registry exposes the credential lifecycle:

| Route                                                                | Method | Notes                                                                                                                                       |
| -------------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `/v1/vaults/:vault_id/credentials`                                   | GET    | Lists credentials newest-first with `limit` / `page` pagination and `include_archived`                                                      |
| `/v1/vaults/:vault_id/credentials`                                   | POST   | Creates `static_bearer`, `mcp_oauth`, `environment_variable`, or `provider`; writes secret material through `SecretStore`                   |
| `/v1/vaults/:vault_id/credentials/:credential_id`                    | GET    | Fetches one workspace-owned credential                                                                                                      |
| `/v1/vaults/:vault_id/credentials/:credential_id`                    | POST   | Patches display/metadata, rotates mutable secret fields, and may rebind a provider `logical_id`; MCP URLs and provider/scheme are immutable |
| `/v1/vaults/:vault_id/credentials/:credential_id/archive`            | POST   | Sets `archived_at` and purges stored secret payloads after DB update succeeds                                                               |
| `/v1/vaults/:vault_id/credentials/:credential_id/mcp_oauth_validate` | POST   | Live `mcp_oauth` diagnostic: OAuth refresh attempt (rotated tokens persisted) + MCP `initialize` probe; always 200 with tri-state `status`  |
| `/v1/vaults/:vault_id/credentials/:credential_id`                    | DELETE | Sets `deleted_at` and purges stored secret payloads after the metadata transaction succeeds                                                 |

`vault_credentials` stores only secret references. Public responses omit
`token`, `access_token`, `refresh_token`, `client_secret`, and all secret-ref
columns. Create and secret-rotation updates return 503 if no write-capable
`SecretStore` is configured, instead of storing raw secret material in
Postgres; display/metadata-only updates do not need secret-store access.
`LocalSecretStore` is development/test-only. Kubernetes deployments use the
write-capable `KubernetesSecretStore`, backed by one dedicated Secret and a
registry-only `get`/`patch` Role.

The `provider` variant stores an LLM-provider binding without an
`mcp_server_url`. It is an Orca-only wire extension and requires a non-empty
`orca-beta` header for public create, list, get, update, archive, and delete
operations. Without opt-in, create rejects `auth.type: "provider"`, list omits
provider records at query time, and item routes treat provider records as not
found. Existing credential variants remain visible and mutable without the
header, and internal resolver routes do not depend on this public API opt-in.

```jsonc
{
  "auth": {
    "type": "provider",
    "provider": "anthropic",
    "scheme": "api_key",
    "logical_id": "llm:anthropic",
    "secret_value": "write-only",
  },
}
```

Logical IDs must use the LLM alias namespace and match
`^llm:[A-Za-z0-9][A-Za-z0-9._:-]{0,123}$`. This keeps logical aliases
disjoint from concrete `vcrd_*` credential IDs. Supported
provider/scheme pairs are Anthropic `api_key`, OpenAI and OpenAI-compatible
`bearer`, Azure OpenAI `api_key` or `bearer`, Vertex
`gcp-service-account`, and Bedrock `aws-sig-v4`. Public responses replace
`secret_value` with the non-secret `version`; that opaque version changes on
secret rotation or logical-ID rebinding. The internal resolver preserves exact
`vcrd_*` MCP behavior and additionally resolves `llm:*` aliases through only
the active Session's attached Vaults. Provider rows return their stored
canonical scheme rather than being coerced to MCP `bearer`; an ambiguous alias
fails closed.

## Layout

```
services/registry-service-ts/
  src/
    api/                          # Fastify route registration
      agents.routes.ts
      environments.routes.ts
      sessions.routes.ts          # incl. /resources sub-paths and SSE
      triggers.routes.ts          # Orca cron-only /v1/triggers extension
      files.routes.ts
      memory-stores.routes.ts
      vaults.routes.ts            # + vault-credentials.routes.ts
      skills.routes.ts
      outcomes.routes.ts          # stub: returns null for SDK compat
      internal.routes.ts          # workspace/session-scoped internal runtime routes
      admin.routes.ts             # organization-scoped workspace/key administration
      platform.routes.ts          # deployment-wide organization/workspace provisioning
      platform-agent-observability.routes.ts # deployment-wide observability policy
      platform-mutations.ts       # shared platform transaction/idempotency/audit
      discovery.routes.ts         # /api + /apis version and extension-group discovery
      git-creds.routes.ts         # /v1/git-creds (JWT-only auth)
    contracts/                    # ts-rest / zod contracts; toolset-name aliasing
    domain/                       # state transitions; cross-resource invariants
      session-creation.ts         # shared HTTP/Trigger transactional Session graph
      trigger-cron.ts             # five-field cron + IANA-zone validation
      trigger-reconciler.ts       # Postgres planner and dispatcher
    events/
      session-events-index.ts     # Postgres read model for list history
    persistence/
      postgres/                   # Drizzle client, schema, migrations
    secrets/                      # SecretProvider ports + SecretStore backends
    auth/
      api-key.ts, oidc.ts         # public listener
      platform-api-key.ts, platform-auth.ts  # platform listener: deployment-wide provisioning
      internal-auth.ts            # static-token or Kubernetes TokenReview workload auth
      admin-auth.ts, admin-api-key.ts
      session-jwt.ts              # session-scoped JWTs for gateway + git-creds
    middleware/
      idempotency.ts              # Idempotency-Key handling
      claude-edge.ts              # Claude-compat request/response edge handling
    streaming/
      sse.ts                      # TranscriptStore.tail → SSE bridge
    server.ts                     # app assembly; main.ts binds the three listeners
    config.ts
  test/
  package.json                    # workspace deps: transcript/file/memory/skill stores
  tsconfig.json
```

The ts-rest contracts under `src/contracts/` are the route source of truth.
`pnpm openapi:gen` generates `openapi/managed-agents.yaml` from them, and
`pnpm conformance:gen` diffs that against Anthropic's vendored spec into
[`../conformance-matrix.md`](../conformance-matrix.md). Both artifacts are
checked in and CI fails if regenerating them produces a diff — edit the
contracts, never the artifacts.

## Secret providers and authorization scope

- The `SecretProvider` family (Azure KV, AWS SM, GCP SM, K8s, Env) lives in `src/secrets/`.
- Authorization is workspace-level: a credential resolves to one workspace and may do anything
  within it, except on the Trigger routes. `AuthenticatedPrincipal.scopes` is populated and a
  workspace key carries a single `workspace.full_access` scope. Only the Trigger handlers inspect
  scopes (`requireWorkspaceScopes` in `src/auth/workspace-scope.ts`): each accepts
  `workspace.full_access` or its `workspace.agentTriggers.*` operation scope, and Trigger Session
  history also needs `workspace.sessions.describe`. Per-operation permissions for every other
  route are in [`../roadmap.md`](../roadmap.md).

## Auth

See [`../auth-and-vaults.md`](../auth-and-vaults.md). Public API-key/OIDC auth produces a server-side workspace context; public resources do not expose or accept `workspace_id`. Organization admins similarly derive one organization from their credential. Platform Admin is a separate principal without organization scope and is accepted only by explicit `/v1/platform/*` routes on the admin listener. Internal traffic uses a separate listener and either a shared static token or distinct Kubernetes workload credentials. Harness paths repeat workspace/session for ownership cross-checking but Harness holds no tenant API key. Store libraries are imported in-process and scope every tenant operation explicitly.

The admin listener also reads `x-orca-registry-authorization: Bearer <token>`, a proxy-safe alternative to `Authorization` for its Bearer credential. Before the Organization or Platform authenticator runs, the listener moves the value into `Authorization` and removes the header, so route handlers never see it. A request that also sends `Authorization`, repeats the header, or carries anything other than one `Bearer <token>` value gets `401`. The public and internal listeners do not read it.

The global app-auth pre-handler (`src/auth/auth.ts`) runs the `x-api-key` / OIDC check on every request **except** these callers, which carry their own credential model and so bypass the api-key/OIDC layer (each then enforces its own auth, so the bypass skips the app layer, it does not skip authentication):

- `/internal/*` — mesh-only control-plane routes (vault-resolve, session lifecycle, memory-versions, environment `verify-key` + durable-claim control (claims, heartbeat, release, reap)). They are registered only on the separate internal listener, whose own pre-handler (`src/auth/internal-auth.ts`) requires a Bearer workload credential on every route except `/healthz`, `/readyz` and `/metrics` — a Kubernetes ServiceAccount JWT checked by TokenReview, or the shared internal service token — and admits each Kubernetes workload identity only to its own route families. Network and mesh policy may further restrict who reaches the listener.
- `/v1/git-creds` — public, but Bearer-JWT-only (`aud='git-creds'`); the in-sandbox helper holds only the session JWT, so the route's handler verifies the JWT itself. Bypassing the app layer keeps the api-key check from masking the JWT-specific error.
- `/v1/tunnels/*` — the **public** tunnel WebSockets. The runner tunnel (`WS /v1/tunnels/runners/:runnerId`, documented above): a self-hosted runner dials in from outside the mesh with no api-key / OIDC token, only its `X-Orca-Runner-Tunnel-Token`; the WS handler self-authenticates by binding token (loopback-only default, allow-list opt-in, owner fail-closed, CSWSH guard). The worker/host tunnel (`WS /v1/tunnels/environments/:environmentId`) rides the same bypass, self-authenticating with its raw env key; the `verify-key` and durable-claim control routes stay on the mesh-only `/internal/*` listener (previous bullet).
- `/healthz`, `/readyz`, `/metrics` — unauthenticated cluster probes.

## Data model

See [`../data-model.md`](../data-model.md) for the full Postgres schema owned by this service.

## Skills

See [`../skills.md`](../skills.md). Registry stores immutable
SkillVersion metadata, writes bundle bytes through `@orca/skill-store`, and
pins the primary agent and direct coordinator roster when a Session is
created. Nested coordinators are rejected at Agent validation and again at the
Session binding boundary because the Harness supports one roster level.
Prepared execution returns metadata catalogs only; Harness reads exact pinned
bundles directly from the store. Deleting a version removes it from the public
API even when it is the final version (`latest_version` becomes `null`). Every
Skill and version deletion is soft: it sets `deleted_at` and keeps the metadata
and the SkillStore bundle, bound or not, so existing Session bindings keep
resolving. The transactional bundle deletion outbox, reconciled at startup and
periodically, handles only an uploaded bundle whose SkillVersion metadata did not
commit. Parent Skill deletion returns `400` while any public version remains and
otherwise succeeds.

### SDK harness binding and recovery

Agent `metadata.harness` selects an immutable runtime type; `agents.harness_type`
persists the effective choice, including the implicit Claude default. Agent
model versions and Session overrides are validated against that harness's
model catalog. Snapshot delivery reads the Session's pinned Agent version.
`GET /apis/runtime.runorca.ai/v1/harnesses` exposes SDK model and capability
choices. Native Codex recovery data resides in the private
`session_harness_states` table, outside ordinary Session queries and public
Agent/Session responses and transcript events. See [harness modes](../harness-modes.md).

For cloud `codex_sdk` or `pi_sdk` with `mode: separate`, execution preparation includes private
`session.harness_state`, `session.harness_state_revision`, and
`session.harness_ownership_revision` fields. The private
`POST /internal/v1/workspaces/{workspaceId}/sessions/{sessionId}/harness-turn`
route provides bounded inspect, claim, begin, accept-source, commit, abandon, and
settle transitions. Registry stores one receipt and an ownership token/revision in
a versioned envelope alongside native state in `session_harness_states.state`.
Legacy bare checkpoints read as ownership revision zero. Preparation and colocated
snapshot readers expose only normalized native state.

Mutation transitions lock the Session and validate workspace, lifecycle, runtime
revision, and pinned cloud/separate Codex binding. Inspection validates an existing
owned envelope against the current lifecycle and binding. A claim uses a caller-stable owner token
and expected ownership revision; retries of the same claim are idempotent, while
successor claims fence every previous owner. Claims and transitions return
Registry's authoritative session guardrail state. Inspection and recovery do not
require execution preparation, so an archived Agent does not prevent cold interruption.
Inspection returns no recoverable receipt for an archived or terminated Session or
an inactive workspace; delayed source deliveries follow the normal lifecycle drop
path. Mutation requests retain their lifecycle and workspace errors.

A pending receipt records the primary turn, accepted control sources, stable usage
identity, terminal identities, and timestamp before SDK submission. After the
transcript response fence is acknowledged, commit atomically replaces native history
and marks the receipt ready, even when the native digest is unchanged. A successful
guarded commit checks the matching usage ledger entry and pending marker before
deleting that marker in the same transaction. An abandoned receipt preserves previous
native history and unknown usage. Settling retains the latest receipt until the next
turn, making lost acknowledgements retryable. Cold recovery re-appends only missing
terminal/completion events and never re-executes the recorded SDK turn.

The legacy `/harness-state` route retains native-checkpoint compare-and-swap behavior
only before ownership is claimed. Legacy separate/runner checkpoint writes and generic
pending-usage deletion are rejected after a receipt owner exists. Native state retains
the worker's 16 MiB decoded limit (32 files); receipts have at most 256 source IDs.
These routes accept only the Harness workload under workload-aware authentication,
are absent from the public listener, and do not write transcript events themselves.
Corrupt stored checkpoints or receipts fail inspection, ownership transitions, and
preparation with `409 invalid_runtime_binding`, preserving native history. Harness
completes the affected source as a permanent setup failure. Transient Registry and
transcript recovery failures retain source delivery for retry, including beyond
Pulsar’s poison-message limit, without submitting the recorded turn again.

Preparation also validates Codex guardrail capabilities. Separate Codex admits stateless
request, tool-call, and tool-result policies and supported stateful request budgets.
Colocated Codex admits request-only stateful budgets through the Registry bridge.
Response, model-call, stateful tool and subagent policies fail preparation before execution. Self-hosted separate Codex is
rejected by the deployment capability check.

The internal `GET /internal/v1/workspaces/{workspaceId}/sessions/{sessionId}/execution-owner`
route reads the Session-pinned Agent version and immutable harness identity,
without preparing execution or resolving runtime dependencies. It shares the
binding loader and `resolveExecutionOwner` rule with Session distribution:
self-hosted Sessions and cloud colocated harnesses other than `claude_code`
belong to Registry; cloud separate and cloud `claude_code` Sessions belong to
harness-server. `resolveHarnessCapabilities` selects the declared execution owner for the
pinned harness and deployment mode. Agent/Environment archive does not change
ownership. An inconsistent stored binding returns 409, never a default owner.
The harness server checks this route before every user event, including cold
interrupts, and does not generate completion events for Registry-owned Sessions.
The Registry bridge checks the same ownership loader before connection setup,
each turn, tool confirmation and interrupt. An unavailable ownership read
forwards no request to the runner. Event followers retain the current event and
retry failed reads with bounded backoff; stopping the bridge cancels that wait.

### Pi SDK state and accounting

`pi_sdk` uses the same execution ownership, protocol-specific gateway credentials, durable
turn receipts, usage ledger and request-budget enforcement as `codex_sdk` in both
cloud modes and self-hosted colocated mode. Native history is stored only in
`session_harness_states`. The checkpoint format (`pi_sdk`, SDK 0.87.0 or 0.87.1) is checked
against the Session-pinned Agent selection; Codex and Pi histories cannot be exchanged.
The private envelope and pending-usage key retain their historical Codex names for
compatibility. See [Pi SDK runtime](../libraries/pi-harness.md).
