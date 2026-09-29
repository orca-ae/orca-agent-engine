# Data Model

Tables are scoped to the service that owns them. Cross-service references are by ID; no cross-service joins.

Resource tables expose a nullable `deleted_at` timestamp internally. DELETE writes
this timestamp while retaining rows, historical versions, and Session bindings;
normal application reads and writes exclude deleted rows. File and MemoryStore
metadata follow the same rule in their own databases. Archive and deletion remain
separate states. The Registry deletion contract is described in
[Resource deletion](services/registry-service.md#resource-deletion).

## registry-service (Postgres)

```
organizations         (id, name, status, created_at, updated_at)

platform_api_keys     (id, name, hashed_key, key_fingerprint,
                       partial_key_hint, scopes, status, expires_at,
                       last_used_at, revoked_at, created_by,
                       created_at, updated_at)
                       -- Deployment-wide provisioning authority. It has no
                       -- organization_id and uses a fingerprint domain
                       -- distinct from every other key class.

platform_audit_events (id, organization_id, workspace_id, actor, auth_method,
                       action, target_type, target_id, request_id, result,
                       metadata, created_at)
                       -- Immutable deployment-wide provisioning/key audit.
                       -- Target organization/workspace are nullable.

platform_idempotency_keys
                      (principal, scope, key, response_status, response_body,
                       body_hash, expires_at, created_at,
                       PRIMARY KEY (principal, scope, key))
                       -- 24-hour cache for authenticated /v1/platform writes.

workspaces            (id, organization_id, name, status, created_by,
                       archived_at, created_at, updated_at)
                       -- Organization-scoped admin-plane authority. Tenant
                       -- tables reference workspaces with ON DELETE RESTRICT.

agent_observability_platform_policy
                      (id = 'default', allowed_adapters, allowed_endpoint_classes, max_capture_mode,
                       capture_restriction_epoch, created_at, updated_at)
                      -- One constrained singleton platform policy row. It
                      -- starts with only otlp_http enabled and a metadata-only
                      -- capture ceiling. Schema validation also recognizes the
                      -- separately release-gated langfuse_sdk identifier.

agent_observability_bindings
                      (id, organization_id, workspace_id NULL, scope_type,
                       adapter_type, endpoint_kind, endpoint_class, endpoint,
                       external_project_id, current_version, status,
                       revocation_epoch, created_by, updated_by,
                       archived_at, created_at, updated_at)
                      -- A fixed external target identity. Organization
                      -- bindings have NULL workspace_id; workspace bindings
                      -- use the (organization_id, workspace_id) ownership FK.
                      -- status is active|draining|disabled|archived;
                      -- current_version has a deferred FK to an immutable
                      -- binding-version row created in the same transaction.

agent_observability_binding_versions
                      (binding_id, version, adapter_type, semantic_profile,
                       protocol, compression, timeout_ms, environment, release,
                       capture_mode, sample_rate, config_schema_version,
                       created_by, created_at)
                      -- Immutable non-secret encoding/policy versions. PK
                      -- (binding_id, version); adapter/profile/protocol,
                      -- capture, sample-rate, and version bounds are checked.

agent_observability_binding_credentials
                      (binding_id PRIMARY KEY, secret_ref, credential_version,
                       key_hint, rotated_at, updated_by, created_at, updated_at)
                      -- Mutable credential head. secret_ref is an opaque
                      -- SecretStore reference; no credential bytes, tokens,
                      -- passwords, authorization headers, or header values
                      -- are stored in Postgres.

agent_observability_organization_settings
                      (organization_id PRIMARY KEY, active_default_binding_id NULL,
                       active_default_binding_scope NULL, selection_epoch,
                       default_revocation_epoch, organization_revocation_epoch,
                       capture_ceiling, capture_restriction_epoch,
                       created_at, updated_at)
                      -- Stable organization setting/lock row. A selected
                      -- default is constrained to an organization-owned
                      -- binding in that organization.

agent_observability_workspace_settings
                      (workspace_id PRIMARY KEY, organization_id, mode,
                       binding_id NULL, selection_epoch, revocation_epoch,
                       capture_ceiling, capture_restriction_epoch,
                       created_at, updated_at)
                      -- mode is inherit|disabled|custom; custom requires a
                      -- same-workspace binding, while inherit/disabled require
                      -- no binding. The first successful Workspace archive
                      -- advances only revocation_epoch. Its immutable archive
                      -- marker below, not updated_at, proves that increment.
                      -- Missing, invalid, overflowing, or CAS-lost setting
                      -- state aborts the whole archive transaction.

agent_observability_workspace_archive_revocations
                      (workspace_id PRIMARY KEY, organization_id, archived_at,
                       revocation_epoch, created_at)
                      -- Durable exactly-once Workspace archive marker. It has
                      -- exact Workspace/setting ownership, the first archive
                      -- timestamp, and the post-increment revocation epoch.
                      -- Migration 0056 backfills legacy archives; its temporary
                      -- Workspace trigger writes the same marker for old writers.

agent_observability_mutation_reservations
                      (id, organization_id, workspace_id NULL, target_type,
                       target_key, target_binding_id NULL,
                       target_binding_scope NULL, owner_principal, body_hash,
                       expected_state_version, expected_config_version NULL,
                       expected_credential_version NULL, generation, status,
                       expires_at, fenced_at, expired_at, committed_at,
                       created_at, updated_at)
                      -- Append-only fenced admin-mutation attempt. Target is
                      -- one stable organization setting, workspace setting, or
                      -- owned binding. A partial unique index permits only one
                      -- pending target. The status machine is
                      -- pending -> committed|fenced|expired; generation is
                      -- monotonic per target. Version values are non-secret
                      -- caller CAS inputs, not mutable state copies.

agent_observability_credential_staging_intents
                      (reservation_id PRIMARY KEY, generation,
                       candidate_binding_id, proposed_credential_version,
                       secret_ref, status, writer_token NULL,
                       writer_lease_expires_at NULL, put_completed_at NULL,
                       cleanup_token NULL, cleanup_lease_expires_at NULL,
                       next_cleanup_at, created_at, updated_at)
                      -- One durable SecretStore bundle intent per reservation
                      -- attempt. `(reservation_id, generation)` references its
                      -- exact fence. candidate_binding_id and
                      -- proposed_credential_version bind the opaque bundle to
                      -- one credential head but have no binding FK because a
                      -- target replacement creates it in finalization.
                      -- status is pending -> writing -> written, or terminal
                      -- cleanup_pending -> cleaning. Writer and cleanup tokens
                      -- use separate leased CAS claims. A terminal live writer
                      -- is never cleaned; an expired writer lease becomes a
                      -- tombstone. Successful deletes retain that tombstone
                      -- and reschedule its next cleanup indefinitely. Only
                      -- finalization consumes a written intent; a terminal
                      -- tombstone remains because SecretStore.put has no
                      -- proven cancellation or quiescence signal.

agent_observability_secret_cleanup_outbox
                      (id, organization_id, binding_id, binding_scope,
                       secret_ref, status, claim_token NULL,
                       lease_expires_at NULL, attempt_count, next_attempt_at,
                       created_at, updated_at)
                      -- Durable deletion of an explicitly superseded,
                      -- same-binding opaque ref. Binding ownership FK prevents
                      -- cross-organization cleanup. pending -> deleting is a
                      -- leased token claim; success removes the row and failure
                      -- restores pending with bounded backoff. A stale token
                      -- cannot complete a newer claim.

agent_observability_idempotency_keys
                      (organization_id, principal, scope, key, target_key,
                       body_hash, status, reservation_id,
                       response_status NULL, response_body NULL,
                       completed_at NULL, expires_at,
                       created_at, updated_at,
                       PRIMARY KEY (organization_id, principal, scope, key))
                      -- Organization-admin idempotency partition. pending is
                      -- linked to one reservation. Before expires_at a key is
                      -- target/body-bound; after expiry it atomically rebinds
                      -- to a new target/body attempt unless the linked
                      -- reservation remains live. A completed response is an
                      -- authoritative organization/workspace state projection
                      -- replayed only before expires_at; no generic JSON cache
                      -- or caller-provided response body exists.

session_observability_bindings
                      (workspace_id, session_id, organization_id,
                       binding_id NULL, binding_version NULL, binding_scope NULL,
                       binding_workspace_id NULL, selection_source, status,
                       selection/revocation/capture epochs,
                       effective_capture_mode, session_revocation_epoch,
                       agent_id, agent_version, harness, harness_mode,
                       archived_at, deleted_at, created_at, updated_at,
                       PRIMARY KEY (workspace_id, session_id))
                      -- Immutable binding pin or non-exporting tombstone.
                      -- No Session FK: it survives a hard Session delete.
                      -- Binding ownership, source, status, capture mode,
                      -- version, lifecycle, and non-negative epoch shapes are
                      -- database-constrained.
                      -- First archive changes active/disabled to archived and
                      -- advances session_revocation_epoch; re-archive preserves
                      -- its first archived_at and epoch. Soft deletion changes an
                      -- active/disabled/archived pin to deleted, advances the
                      -- epoch again, and preserves archived_at when present.
                      -- Migration 0054 temporarily enforces those transitions
                      -- in a Session DB trigger for mixed-version writers;
                      -- workspace archive reaches each active Session through
                      -- the same trigger and rolls back on a missing/bad pin.

                      -- Organization/workspace production provisioning inserts
                      -- its corresponding setting in the same transaction.
                      -- Migration 0048 seeds policy, backfills all existing
                      -- setting rows, and creates a disabled metadata-only pin
                      -- for every Session present at that migration. Migration
                      -- 0049 backfills any pin missing from the post-0048 /
                      -- pre-pinning writer window and temporarily adds an
                      -- AFTER INSERT Session fallback that writes the same safe
                      -- disabled pin for a legacy direct insert. Parent-row
                      -- AFTER INSERT triggers also provision settings for
                      -- older Registry replicas during a mixed-version rollout.
                      -- Migration 0054 also backfills legacy archive/delete
                      -- lifecycle mismatches before installing its temporary
                      -- Session update/delete trigger. Both mixed-version
                      -- Session fallback triggers are currently installed.

admin_api_keys        (id, organization_id, name, hashed_key, key_fingerprint,
                       partial_key_hint, scopes, status, expires_at,
                       last_used_at, revoked_at, created_by,
                       created_at, updated_at)
                       -- Admin-key fingerprints use a separate domain from
                       -- workspace API keys; plaintext is never persisted.

admin_audit_events    (id, organization_id, workspace_id, actor, auth_method,
                       action, target_type, target_id, request_id, result,
                       metadata, created_at)
                       -- Immutable non-secret audit record for admin mutations.

agents               (id, workspace_id, name, version, latest_version_id,
                      model_provider, model_id, system, tools, mcp_servers,
                      skills, metadata, multiagent,
                      archived_at, created_at, updated_at)
agent_versions       (id, workspace_id, agent_id, version, snapshot_jsonb, created_at)
                     -- UNIQUE (workspace_id, agent_id, version)

environments         (id, workspace_id, name, packages_jsonb, networking_jsonb,
                      image, target, egress_mode, llm_jsonb,
                      env_key_digest, env_key_expires_at,
                      environment_token_digest, environment_token_expires_at,
                      archived_at, created_at, updated_at)
                     -- packages_jsonb stores canonical `config.packages` object:
                     -- `{ apt?, cargo?, gem?, go?, npm?, pip? }`, string arrays only.
                     -- Empty managers are omitted. Legacy top-level
                     -- `packages: string[]` input maps to `packages_jsonb.apt`.
                     -- Cloud API responses project stored packages to beta
                     -- shape with all managers present plus `type: "packages"`.
                     -- Cloud `target` + typed `networking_jsonb` project to
                     -- `config.type` + `config.networking`; `self_hosted`
                     -- projects to `{ type: "self_hosted" }`; untyped legacy
                     -- networking remains flat-only.
                     -- env_key: a worker/host tunnel credential. Only the
                     -- SHA-256 digest + expiry are persisted; the raw `sk-…`
                     -- key is returned once at create/rotate and never stored,
                     -- so a DB leak can't reconstruct it. NULL/NULL = revoked.
                     -- The worker presents the raw key at
                     -- POST /internal/environments/{id}/verify-key (below).
                     -- environment_token: the registry-minted Environment Token
                     -- for a provisioned cloud environment (digest + expiry
                     -- only, same once-only-echo model as env_key); the cloud
                     -- worker presents it via X-Orca-Environment-Token.
                     -- egress_mode in ('gateway','sidecar') | NULL (deploy default).

environment_claims   (environment_id PRIMARY KEY -> environments(id) ON DELETE CASCADE,
                      owner_pod, worker_conn_id,
                      claimed_at, last_ping)
                     -- Durable, multi-replica equivalent of an in-memory
                     -- tunnel/host registry: an environment is claimed by
                     -- exactly one registry pod at a time so a worker's tunnel
                     -- terminates on the replica that owns its environment.
                     -- The PRIMARY KEY on environment_id IS the one-exclusive-
                     -- claim lock (one row per environment). Newest-wins on
                     -- reconnect (upsert overwrites the row); last_ping is the
                     -- heartbeat watermark, staleness = now - last_ping > ttl,
                     -- reaped in bulk by a background sweeper. See
                     -- services/registry-service.md and
                     -- src/domain/environment-claims.ts.
                     -- Also the affinity anchor for `colocated` sessions: the
                     -- owning pod is the only one that can reach that
                     -- environment's runner over the tunnel (see the
                     -- sessions.runner_id note below). The same claim + tunnel
                     -- mechanism serves BOTH targets: for target=self_hosted
                     -- the environment-worker dials in; for target=cloud the
                     -- environment-launch lifecycle provisions the sandbox,
                     -- mints its Environment Token, and starts a worker that
                     -- dials the same tunnel (see deployment-topologies.md's
                     -- build-status table).

agent_triggers       (id, workspace_id, name, agent_id, agent_version,
                      environment_id, title_template, metadata, vault_ids, payload,
                      cron_expression, timezone, status, generation,
                      next_fire_at, last_fired_at, last_error,
                      archived_at, created_at, updated_at)
                     -- Orca-owned /v1/triggers configuration. Agent version is
                     -- pinned at create time. The public session_mode/source/
                     -- session/replicas envelope is normalized into these
                     -- cron-only columns; v1 accepts SESSION_PER_EVENT, cron,
                     -- and one replica. Active rows are polled through the
                     -- partial next_fire_at index.

agent_trigger_fires  (id, workspace_id, trigger_id, generation, scheduled_for,
                      status, planned_session_id, session_id, event_id,
                      attempt_count, last_error, next_attempt_at, enqueued_at,
                      created_at, updated_at)
                     -- Internal occurrence ledger, not a public run resource.
                     -- UNIQUE (workspace_id, trigger_id, generation, scheduled_for)
                     -- planned_session_id and event_id are stable before dispatch.
                     -- Session creation and the enqueued link commit atomically.
                     -- session_id deliberately has no Session FK so the
                     -- Session deletion API can remove Trigger Sessions;
                     -- retained fire history then has an unresolved text id.
                     -- Pending retries are selected through next_attempt_at;
                     -- terminal fire retention is an open policy decision (roadmap.md).

sessions             (id, workspace_id, agent_id, agent_version, environment_id,
                      runner_id, distribution_state, host_environment_id,
                      vault_ids text[], status, runtime_revision, last_event_seq bigint,
                      sandbox_handle_id, started_at, last_active_at,
                      guardrail_ids text[],          -- stored in agent_overrides JSONB
                      usage_cost_nano_usd bigint, usage_has_unpriced boolean,
                      archived_at, created_at, updated_at)
                     -- runner_id + distribution_state: `colocated`-session
                     -- affinity — which session-runner a colocated session is
                     -- bound to, and its distribution status (pending /
                     -- assigned / failed; see services/registry-service.md's
                     -- "Work-queue stats" for the full state semantics).
                     -- host_environment_id records which claim-holding
                     -- environment's worker was actually sent the launch. Set
                     -- by the distributor for a colocated session on EITHER
                     -- environment target (cloud or self_hosted) — the same
                     -- affinity mechanism serves both, per the shared-server
                     -- model. NULL for `separate` sessions and for `colocated`
                     -- sessions not yet distributed.

session_lifecycle_outbox
                     (id, workspace_id, session_id, kind, events, created_at,
                      published_at, attempt_count, last_attempt_at)
                     -- Durable initial-event and lifecycle publication.
                     -- Deliberately has no session FK so deletion retains
                     -- its sentinel.

session_resources    (id, workspace_id, session_id, type,         -- 'file'|'memory_store'|'github_repository'
                      file_id, memory_store_id, repo_ref,         -- one of these set
                      mount_path, access, mount_strategy, instructions,
                      attached_at, updated_at, detached_at)
                     -- INDEX (workspace_id, session_id, detached_at)

git_credentials      (id, workspace_id, provider, repo_url,
                      secret_ref, session_resource_id, metadata,
                      archived_at, created_at, updated_at)
                     -- Raw github_repository.authorization_token bytes live
                     -- only in SecretStore. secret_ref is opaque.
                     -- session_resource_id != NULL means resource-owned token;
                     -- UNIQUE (session_resource_id) for those rows.
                     -- Pre-provisioned Orca credentials keep NULL ownership;
                     -- UNIQUE (workspace_id, repo_url) WHERE active + unowned.

git_credential_staging_intents
                     (git_credential_id, workspace_id, session_resource_id,
                      secret_ref, status, cleanup_after,
                      created_at, updated_at)
                     -- Durable intent committed before raw token bytes are
                     -- written to SecretStore. The resource transaction
                     -- atomically deletes the pending intent while inserting
                     -- git_credentials metadata. A reconciler claims expired
                     -- pending/cleaning rows and purges unreferenced secrets.
                     -- No raw token bytes are stored here.

session_threads      (id,                                         -- 'sth_…'
                      workspace_id, session_id, subpath,
                      agent_id, agent_version,
                      agent_name,                                 -- roster agent (name) in this thread
                      parent_thread_id,                           -- NULL = PRIMARY (session-level) thread
                      status,                                     -- 'running'|'idle'|'rescheduling'|'terminated' (default 'idle');
                                                                  -- archive also writes 'archived' to the COLUMN — the wire
                                                                  -- enum stays 4-value (threadStatusToApi folds it to 'terminated')
                      stop_reason,                                -- last thread_status_idle reason; NULL until idle
                      usage_cost_nano_usd bigint, usage_has_unpriced boolean,
                      archived_at, created_at, updated_at)
                     -- UNIQUE (workspace_id, session_id, subpath)
                     -- Maps Claude-compatible thread ids to transcript subpaths
                     -- for list/events/stream/archive APIs. Read model for the
                     -- Anthropic thread-model multiagent: ONE session runs
                     -- MULTIPLE threads. The PRIMARY thread IS the session-level
                     -- event stream (empty subpath); each roster subagent runs
                     -- in its own child thread whose events live under
                     -- `subagents/<id>`, with parent_thread_id pointing at the
                     -- primary thread (delegation is one level only). Derived
                     -- state — the transcript stays source of truth;
                     -- primary-thread `session.thread_*` events (created/
                     -- running/idle/terminated) are projected here so the
                     -- Threads API can list threads and interrupt/archive can
                     -- flip a single thread's row. The create-session
                     -- transaction inserts the primary row itself (subpath ''),
                     -- so every session — single-agent included — has exactly
                     -- one row until a coordinator's thread events add children
                     -- (purely additive). Projector:
                     -- upsertSessionThreadsFromEvents in
                     -- src/events/session-events-index.ts.

vaults               (id, workspace_id, display_name, metadata,
                      archived_at, created_at, updated_at)
                     -- IDs use vlt_ prefix. Metadata container only. No target
                     -- URLs, principals, secret refs, or secret material live here.
                     -- display_name is not unique; it is human-readable only.

vault_credentials    (id, workspace_id, vault_id, display_name,
                      auth_type, provider, scheme, logical_id,
                      resolution_version, mcp_server_url, secret_name,
                      networking, auth_config,
                      access_secret_ref, refresh_secret_ref,
                      token_endpoint, client_id, token_endpoint_auth_type,
                      client_secret_ref,
                      oauth_refresh_lease_owner, oauth_refresh_lease_expires_at,
                      metadata,
                      archived_at, created_at, updated_at)
                     -- Runtime credential unit under a vault. Secret columns
                     -- are SecretStore references only; raw token/access/
                     -- refresh/client-secret bytes never live in Postgres.
                     -- oauth_refresh_lease_* is internal, short-lived
                     -- cross-replica serialization state for one-time refresh
                     -- token consumption across validation and runtime entry
                     -- points. Holders heartbeat until owner-fenced pointer
                     -- CAS; abandoned leases expire.
                     -- vault_id references vaults(id) with ON DELETE RESTRICT
                     -- provider rows require provider, canonical scheme,
                     -- URL-path-safe llm:* logical_id, and opaque
                     -- resolution_version; they do not use mcp_server_url.
                     -- Provider secret material reuses access_secret_ref.
                     -- resolution_version changes when the provider secret
                     -- or logical binding changes. Legacy MCP rows keep all
                     -- four provider fields NULL and require no backfill.
                     -- UNIQUE (workspace_id, vault_id, mcp_server_url)
                     --   WHERE archived_at IS NULL
                     -- UNIQUE (workspace_id, vault_id, logical_id)
                     --   WHERE archived_at IS NULL AND logical_id IS NOT NULL
                     -- INDEX (workspace_id, vault_id, archived_at, mcp_server_url)
                     -- Application invariant: one active normalized MCP URL
                     -- per vault. Creation checks under the vault row lock;
                     -- runtime resolution rejects legacy conflicts.
                     -- Gateway resolves through the workspace/session-scoped
                     -- `/internal/v1/workspaces/{workspace}/sessions/{session}/vault-credentials/{id}/resolve`
                     -- using the authoritative live destination binding.
                     -- `X-Orca-Credential-Id` remains optional startup metadata
                     -- and cannot select a dynamic credential.

skills               (id, workspace_id, type, name, slug,
                      version, latest_version_id,
                      description, display_title,
                      archived_at, deleted_at, created_at, updated_at)
                     -- `slug` mirrors the uploaded version directory and is
                     -- not a resource identifier or uniqueness boundary.
                     -- UNIQUE (workspace_id, display_title)
                     --   WHERE type = 'custom'
                     --     AND display_title IS NOT NULL
                     --     AND archived_at IS NULL
                     --     AND deleted_at IS NULL

skill_versions       (id, workspace_id, skill_id, version,
                      version_identifier, name, description, directory,
                      entrypoint, package_sha256, package_size_bytes,
                      package_manifest, archived_at, deleted_at, created_at)
                     -- UNIQUE (workspace_id, skill_id, version)
                     -- Bundle bytes live in private @orca/skill-store objects.
                     -- The manifest contains metadata only, never file contents.
                     -- Deleting a version or Skill sets deleted_at; a retained
                     -- version, deleted or not, keeps its bundle.

skill_bundle_deletion_outbox
                     (workspace_id, skill_version_id, package_sha256,
                      created_at, attempt_count, last_attempt_at)
                     -- PK (workspace_id, skill_version_id, package_sha256)
                     -- Cleanup for bundles whose SkillVersion metadata never
                     -- committed. No SkillVersion FK, because no committed
                     -- version exists for such a row.

session_skill_bindings
                     (workspace_id, session_id, agent_id, agent_version,
                      ordinal, skill_version_id, bundle_sha256)
                     -- PK (workspace_id, session_id, agent_id,
                     --     agent_version, ordinal)
                     -- Concrete session-time resolution of AgentVersion refs,
                     -- including refs whose requested version was `latest`.
                     -- The primary and direct coordinator roster are pinned
                     -- atomically; nested coordinators are rejected.

api_keys             (id, workspace_id, hashed_key, key_fingerprint, name,
                      partial_key_hint, principal, scopes, status, expires_at,
                      last_used_at, revoked_at, created_by,
                      created_at, updated_at)
                     -- UNIQUE (key_fingerprint); exact lookup first, then
                     -- Argon2id verification of hashed_key.

idempotency_keys     (workspace_id, scope text, key text,
                      response_status int, response_body text, body_hash text,
                      expires_at timestamptz, created_at timestamptz,
                      PRIMARY KEY (workspace_id, scope, key))
                     -- 24h TTL via partial index on expires_at; pruned by background job.
                     -- 'scope' = endpoint identifier ('POST /v1/sessions/{id}/events',
                     -- 'POST /v1/files', etc.) so keys are namespaced per-endpoint.
                     -- body_hash is a sha256 of the request body for replay-only-with-same-body
                     -- semantics: same key + same body → cached response; same key + different body → 409.
```

No `events_archive` table in the registry schema. Transcript events live in the
selected `@orca/transcript-store` backend: Kafka by default, or the
Postgres transcript tables below when `TRANSCRIPT_STORE_BACKEND=postgres`, or
Apache Pulsar topics when `TRANSCRIPT_STORE_BACKEND=pulsar`.

The registry owns a Postgres read model for event listing:

```
session_events_index (workspace_id, session_id, seq, projection_ordinal,
                      projection_version, event_id, subpath, processed_at,
                      processed_marker_seq,
                      produced_at, produced_by, kind, visibility, payload, indexed_at,
                      PRIMARY KEY (workspace_id, session_id, event_id))
                     -- visibility = 'public' | 'internal'
                     -- INDEX (workspace_id, session_id, visibility, subpath,
                     --        seq, projection_ordinal, event_id)
                     -- INDEX (workspace_id, session_id, seq)
```

`session_events_index` is derived state. The transcript backend remains the
source of truth; registry projectors asynchronously upsert transcript events
into this table. The list-events endpoint reads this index directly and is
eventually consistent with normal projector lag; it does not perform
request-time catch-up from the transcript backend. For a harness-driving client
event, `processed_at` is derived from the earliest matching
`session.user_event_processed` transcript marker. Projectors reconcile
marker-first and target-first arrival; they do not use projection execution
order as lifecycle order. `processed_marker_seq` is an internal monotonic
compare-and-set guard: only a lower marker sequence may replace the derived
timestamp. See
[`event-processing-semantics.md`](./event-processing-semantics.md).
`projection_ordinal` preserves deterministic order when one transcript event
expands into multiple public events at the same `seq`; `projection_version`
marks rows whose legacy embedded tool blocks have been backfilled.
`session_threads` contains one primary row created with each session for the
empty transcript subpath. Additional thread rows are derived from transcript
activity: lifecycle events such as `session.thread_created` update status
directly; otherwise the projector creates a stable thread row for any non-empty
event subpath.
Index registry metadata by `(workspace_id, archived_at IS NULL)` for list queries.

### Guardrails and pricing

```
guardrails           (id, organization_id, workspace_id NULL, name, description,
                      enabled, phases, scope, rule, metadata,
                      archived_at, created_at, updated_at)
guardrail_state      (workspace_id, session_id, key,
                      value_num, value_json, updated_at)
                     -- PRIMARY KEY (workspace_id, session_id, key). A key a
                     -- rule writes is prefixed with the emitting guardrail id
                     -- and, for a rule resolved from a subagent, the runtime
                     -- subagent-dispatch identity, which makes it
                     -- collision-free across stacked same-kind rules and
                     -- concurrent dispatches.
guardrail_counters   (workspace_id, subject, window, key, value_num, updated_at)
                     -- PRIMARY KEY (workspace_id, subject, window, key)
model_prices         (provider, organization_id, model_id, source,
                      input_per_million, output_per_million,
                      cache_read_per_million, cache_write_per_million,
                      fetched_at, created_at, updated_at)
```

`guardrails.workspace_id` is nullable: an organization-scoped guardrail belongs
to an organization and applies across every workspace in it, so it has no owning
workspace. Both lookup paths are indexed. Every other tenant table keeps
`workspace_id` non-null.

`guardrail_state` is keyed per state key rather than holding a JSON blob per
session, so an increment is a conflicting upsert that adds. Guardrail state is
never read-modify-written, which is what allows two writers to increment the same
counter without losing an update. A rule-written key includes the emitting
guardrail id and, for a rule resolved from a subagent, the dispatch identity
(see `guardrails.md` State), so two rules of the same kind — or two concurrent
dispatches of one subagent type — never share a row.

The accepted user event that starts a turn carries a **server-authenticated
subject** (the principal Registry validated at the trust boundary), persisted
on the registry-owned event index row; the internal usage route joins a
delta's turn reference to it to pick the `subject_window` counter, so the
subject is never taken from the reporting harness. `guardrail_counters` is the same shape for
state that outlives a session, keyed by the session's workspace, the principal
and the window. Because the workspace is part of the key, a daily cap counts a
principal's spend within one workspace: an organization-tier cap applies its
limit in each workspace separately, the same as a workspace-tier cap.

For a cron-created turn, the authenticated subject is captured when Registry
accepts the trigger definition and is resolved through the durable
trigger-fire/event association when usage arrives; the scheduler and harness
still cannot provide or override it. A pre-existing trigger whose creator
cannot be recovered receives a stable per-trigger migration subject.

`model_prices` is keyed on `(provider, model_id, source, organization_id)`: the
same model id served through two providers can carry different rates. The three
sources coexist per model and resolution picks per the
specificity-then-precedence rule in `pricing.md`. The empty organization id is
a reserved database sentinel for deployment-global `seed` and `upstream` rows;
`operator` rows must carry a real organization id, enforced by a check
constraint, so an organization's override prices that organization alone. See
[`guardrails.md`](./guardrails.md) and [`pricing.md`](./pricing.md).

`sessions` additionally carries exact accumulated `usage_cost_nano_usd` and a
`usage_has_unpriced` marker; `session_threads` carries the same pair so a
dispatched subagent's spend is attributable to it. Cost is **NULL until the
first priced delta** — never defaulted to zero. A non-empty delta with no price
sets `usage_has_unpriced` without discarding already-known spend, so a
partially-priced session can report "at least $X plus unpriced usage" rather
than presenting partial cost as a total, per `pricing.md`'s
unpriced-vs-`$0.00` invariant.

The two reference tiers persist where their authors write: agent-tier
`guardrail_ids` live in the agent version snapshot
(`agent_versions.snapshot_jsonb`, like every other agent field, so a session
pins the guardrail references its pinned version declared), and session-tier
`guardrail_ids` are stored in the session's `agent_overrides`, set from
`agent_with_overrides` at create.

## file-store (Postgres + object storage)

```
files                (id, workspace_id, filename, mime_type, size_bytes,
                      sha256, blob_uri, metadata_jsonb,
                      archived_at, created_at, updated_at)
                     -- UNIQUE (workspace_id, sha256) for content dedup
                     -- blob_uri is the object-storage URI; bytes never in Postgres
```

## memory-store (Postgres `memorystore` + object storage)

The `memorystore` Postgres database is separate from `registry` and `filestore`
(mirroring the file-store split). Live memory bytes and sha-keyed version blobs
live in S3-compatible object storage at
`{bucket}/{root}workspaces/{workspace_id}/memory-stores/{store_id}/live/{path}` and
`{bucket}/{root}workspaces/{workspace_id}/memory-stores/{store_id}/versions/{sha256}`
respectively — Postgres holds only metadata.

```
memory_stores        (id, workspace_id, name, description,
                      archived_at, created_at, updated_at)
                     -- INDEX (workspace_id)
                     -- UNIQUE (workspace_id, name)

memories             (id, workspace_id, store_id, path, current_sha256, size_bytes,
                      updated_at, updated_by_session_id, updated_by_event_id,
                      deleted_at)
                     -- UNIQUE (workspace_id, store_id, path)
                     -- FK (workspace_id, store_id) -> memory_stores
                     -- bytes live under the workspace/store `live/` prefix

memory_versions      (id, workspace_id, store_id, memory_id, path, sha256, size_bytes,
                      written_by_session_id, written_by_event_id,
                      written_at, redacted_at)
                     -- INDEX (workspace_id, store_id, written_at)
                     -- FK (workspace_id, store_id, memory_id) -> memories
                     -- bytes live under the workspace/store `versions/` prefix
                     -- redact: set redacted_at; row stays for audit trail.
                     --   Shared versions/{sha256} bytes require reference-aware
                     --   cleanup before physical deletion is safe.
```

## transcript-store

Kafka backend: topic-per-session is the durable store. Topic name carries the
workspace and session ID; tenant isolation is enforced at the Kafka cluster +
auth layer, not in a metadata table.

Pulsar backend: persistent topic-per-session is the durable store. Topic name:
`persistent://{tenant}/{namespace}/orca.{workspace_id}.sessions.{session_id}.events`.
Event metadata is stored in Pulsar message properties.

Postgres backend:

```
transcript_events    (seq bigserial primary key,
                      workspace_id, session_id, event_id,
                      subpath, produced_at, produced_by, kind,
                      payload bytea, idempotency_key, user_id NULL, inserted_at)
                     -- UNIQUE (workspace_id, session_id, event_id)
                     -- INDEX (workspace_id, session_id, seq)
                     -- partial INDEX (seq) WHERE produced_by = 'client'
                     --   AND kind LIKE 'user.%'
                     -- partial INDEX (seq) WHERE the same client user events
                     --   OR kind IN ('session.archived', 'session.deleted')

transcript_event_claims
                     (group_id, event_seq, claimed_by, lease_until,
                      processed_at, updated_at)
                     -- PRIMARY KEY (group_id, event_seq)
                     -- event_seq references transcript_events(seq)
                     -- processed_at is internal handler/ACK bookkeeping;
                     -- it is not session_events_index.processed_at.
```

## ai-gateway

External gateway image; no persistent state in this repo. Audit events go to a Kafka topic (`orca.{workspace_id}.audit.ai-gateway`). Vault credentials live in registry's secret store and are cached in-memory only with TTL.

## Kafka topic naming

| Topic                                                   | Owner                | Schema                                                                                                                                             |
| ------------------------------------------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `orca.{workspace_id}.sessions.{session_id}.events`      | transcript-store     | Raw mode: opaque payload bytes with Event metadata in headers. Subagents differentiated by `subpath`.                                              |
| `orca.{workspace_id}.sessions.{session_id}.events-avro` | transcript-store     | Avro mode: full Event envelope with opaque payload bytes. Selected instead of the raw set for all sessions; no automatic history/cursor migration. |
| `orca.{workspace_id}.audit.ai-gateway`                  | ai-gateway           | One audit record per `/v1/mcp` call: workspace, session, backend, method, latency, status, request hash. Credential id present; secret never.      |
| `orca.{workspace_id}.audit.file-store`                  | file-store (later)   | Per-CreateFile/Open audit; v2.                                                                                                                     |
| `orca.{workspace_id}.audit.memory-store`                | memory-store (later) | Per-UpdateMemory audit; v2.                                                                                                                        |
