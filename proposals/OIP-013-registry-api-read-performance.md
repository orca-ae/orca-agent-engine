# OIP-013: Registry API read performance

- *Author(s)*: @jiangpengcheng
- *Status*: Released
- *Proposal time*: 2026-09-12
- *Components*: registry-service-ts (persistence, list/read paths, API-key authentication,
  metrics); `packages/memory-store`; Helm chart unchanged (pools keep the `databasePools` values)
- *Discussion*: None (predates the public repository)
- *Implementation*: under `services/registry-service-ts/src/`: `domain/request-batch.ts`,
  `domain/session-read-context.ts`, `domain/read-concurrency.ts`,
  `api/{sessions,agents,triggers,environments,memory-stores}.routes.ts`,
  `auth/{api-key-proof-cache,api-key}.ts`, `middleware/{read-admission,idempotency}.ts`,
  `observability/api-performance.ts`, `config.ts`, `main.ts`, `server.ts`;
  `packages/memory-store/src/{store.ts,metadata/}`
- *Released in*: v0.5.0

## TL;DR

Registry list endpoints rendered each row with the single-item loader, so a page of N Sessions cost
about 1 + 5N queries, all queued on the small pool that also authenticates every other request.
Pages now hydrate through request-owned loaders that issue one query per 100 keys, not several per
row; an Argon2id proof is reused for 60 seconds while authorization is still read per request; an
opt-in gate bounds the heaviest reads per workspace; new metrics separate pool wait from round trip.
Shapes, data and pool defaults are unchanged; Registries on a remote or shared Postgres gain most.

## Background

Current behavior is documented in [API read performance and admission](../docs/managed-agents/services/registry-service.md#api-read-performance-and-admission),
[`auth-and-vaults.md`](../docs/managed-agents/auth-and-vaults.md) and
[`libraries/memory-store.md`](../docs/managed-agents/libraries/memory-store.md). Each database
Registry uses has its own `pg` pool per process (metadata, file, memory, and transcript under the
Postgres backend of [OIP-001](OIP-001-transcript-store-backends.md)), 10 connections by default; a
checkout that finds every connection busy waits in a FIFO queue. A Session embeds its resolved Agent
(pinned snapshot or Agent-row fallback, overrides, bound Skills, coordinator roster), resources and
folded outcomes, in default and `orca-beta` dialects. Session list pages default to the 100 maximum.

## Motivation

List latency grew with the number of Sessions through read amplification, not slow SQL: each
statement was an index lookup whose server time was a small fraction of its round trip.

- **Per-row hydration.** Session list ran `loadSession` per row under `Promise.all`: the Session
  again, then its resources, pinned Agent version, skill bindings and outcome events, 1 + 5N queries
  plus fallbacks and a repeat per roster entry. Trigger Session history did the same, Thread list
  read each thread's Agent and bindings (about 2N), and Agent list re-read every row it selected.
- **One shared queue.** N loaders do not get N connections. With P connections and round trip r, a
  100-Session page holds the pool for about 500·r/P, while every API-key request needs three
  metadata queries (key lookup, `lastUsedAt` update, workspace read) from that pool, so light
  requests, discovery included, wait behind it. Clients polling the full list multiply the effect.
- **Smaller amplifiers.** A memory-hard Argon2id verification per request (64 MiB at the `argon2`
  defaults); Memory lists reading every version in the store; full-view Memory pages opening up to
  20 objects at once; three `work_stats` counts; idempotency hashing the body twice.
- **No evidence path.** `registry_service_request_total` was declared but never incremented, and
  nothing measured queries per request, pool acquisition or authentication stages.

## Goals

### In scope

- Queries per 100 keys rather than per row for Session list and detail, Thread list and detail,
  Trigger Session history and Agent list, with identical responses in both dialects.
- Workspace scoping on every batched query; less Argon2id work without caching authorization; a
  workspace-fair gate for the heaviest reads, off until capacity is measured; metrics for pool,
  round-trip, query-count and auth time; no migration; a switch per default-on change.

### Out of scope

API shape changes (such as summary or watch endpoints for pollers), cross-request response caching,
pool defaults, database placement, replica count and server-side SQL cancellation. Remaining work is
listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#remaining-api-performance-work).

## Design

### High-level design

```text
GET /v1/sessions?limit=100    auth (3 queries) and the page query (1) are the same on both paths
  per-item (batch reads off)  100 × loadSession, 5+ queries each, all queued on one pool
  batched (default)           1 bulk reload of the page IDs, then one query per loader per wave:
                              resources | Agent versions | fallback Agents | bindings | outcomes
```

### Detailed design

**Batch loader.** `createRequestBatch` (`domain/request-batch.ts`) coalesces the keys requested in
one microtask turn into queries of at most 100 keys, memoizes each key's promise (missing results
and failures included) for the loader's life, and runs one query at a time, draining later keys as
a new wave; a coordinator roster adds a wave and its bindings can add chunks. After a failure it
rejects pending and new keys without querying; after a disconnect it starts no further chunks. The
renderers in `api/sessions.routes.ts` (`loadResolvedAgentSnapshot`, `loadBoundSessionSkillRefs`,
`loadCanonicalOutcomeEvaluations`, `threadToApi`) ask the loader instead of the database, so nothing
duplicates their traversal to decide what to preload.

**Session read context.** `createSessionReadContext` (`domain/session-read-context.ts`), built per
HTTP read and never shared across requests or with a mutation, has loaders, all filtered on the
authenticated workspace, for attached resources and the dialect's outcome events (in fold order) by
Session, pinned snapshots by Agent and version, Agent rows when a snapshot is missing or invalid,
and Skill bindings matching the bound checksum by Session, Agent and version. Separate collections
avoid a resources × skills × outcomes fan-out, bindings and overrides stay keyed by Session, and
`now` is captured once. Session list reloads the page's IDs in one query (`loadSessionRows`),
dropping Sessions deleted meanwhile but keeping cursors from the original page; Trigger history
reuses it, Session detail and Thread reads use a context, and Agent list projects its rows with
`agentDataFromRow`. Neither path reads a transactionally consistent snapshot of a page.

**Smaller amplifiers.** `countEnvironmentWork` computes `work_stats` in one `count(*) FILTER`
aggregate. `listMemories` and `listAllVersions` accept `MemoryIdsFilter` (an empty list returns no
rows), so Memory list fetches histories only for page memories and a `memory_id` version list
narrows both reads; histories stay complete, so this is not version pagination. `mapReadItems`
(`domain/read-concurrency.ts`) caps Memory and Memory-version pages at four concurrent item reads
and waits for started reads before rethrowing. Idempotency hashes the body once and reuses the hash.

**API-key proof reuse.** `createApiKeyProofCache` (`auth/api-key-proof-cache.ts`) keys a proof by
the presented credential's SHA-256 fingerprint plus the current stored hash and keeps only an
expiry 60 seconds out, never renewed: at most 1,024 proofs (LRU) and 1,024 in-flight verifications,
one per proof however many requests wait on it. Failures are never cached. Key status, revocation,
expiry, scopes and workspace are still read per request, and `lastUsedAt` stays synchronous
(`auth/api-key.ts`). The legacy-fingerprint fallback, OIDC and other authenticators do not use it.

**Heavy-read admission.** `registerReadAdmission` (`middleware/read-admission.ts`) gates `GET` and
`HEAD` on Session list, Thread list, Trigger Session history, Memory list and Memory-version list
after authentication, FIFO per authenticated workspace and round-robin across workspaces, under
global and per-workspace limits on active and queued reads. A full queue answers 429 and a timeout
or shutdown 503, with `Retry-After: 1`, rendered as `rate_limit_error` and `overloaded_error` by
`middleware/claude-edge.ts`. A queued request whose client leaves never runs; a lease is released
only once the response has closed and its handler settled. The gate is per process.

**Metrics.** `registerApiPerformance` (`observability/api-performance.ts`) records every request on
every listener by route template (or `<unmatched>`), method, status and listener, counting its
queries in an `AsyncLocalStorage` context. `observePostgresPool` wraps each pool's `connect` and
each client's `query` in all three `pg` call forms, restoring the acquiring request's context when
`pg-pool` hands it a connection another request released.

**Pool sizing.** Batching cuts round trips and placing Registry near its database cuts their cost; a
larger pool does neither, so defaults stay at 10. Per Postgres instance, keep replicas × enabled
pool maxima, plus migrations and rolling-update surge, under `max_connections`, and raise the
metadata pool only while acquisition time falls and database load does not rise. Transaction
pooling stays unsupported: metadata, file and memory migrations hold a session-level advisory lock.

## Changes by component

- **registry-service-ts**: everything above, wired through `config.ts`, `main.ts` and `server.ts`.
- **Libraries**: `@orca/memory-store` adds the optional `MemoryIdsFilter` (`src/store.ts`) to its
  PostgreSQL and in-memory backends; harness-server links it but calls neither method.
- **Helm chart** and other services: none; the new switches go through `registry.extraEnv`.

## Public-facing changes

### API

No shape changes. With admission enabled the five gated routes can answer 429 `rate_limit_error` or
503 `overloaded_error` in the normal error envelope. A Session deleted between page selection and
hydration is omitted, as on the per-item path. Events, streaming and wire protocols: none.

### Storage

None: no migration and no new index; the batched predicates use existing indexes such as
`session_resources_session_idx` and `session_events_index_session_seq_idx`
(`src/persistence/postgres/schema.ts`).

### Configuration

Read by Registry (`config.ts`); an invalid value fails startup. Pool settings are unchanged.

| Variable | Default | Effect |
| --- | --- | --- |
| `REGISTRY_BATCH_READS_ENABLED` | `true` | `false` restores the per-item readers |
| `REGISTRY_API_KEY_PROOF_CACHE_ENABLED` | `true` | `false` verifies Argon2id on every request |
| `REGISTRY_HEAVY_READ_ADMISSION_ENABLED` | `false` | enables the per-process heavy-read gate |
| `REGISTRY_HEAVY_READ_MAX_CONCURRENT` | `4` | active heavy reads per process |
| `REGISTRY_HEAVY_READ_MAX_PER_WORKSPACE` | `2` | active heavy reads per workspace |
| `REGISTRY_HEAVY_READ_MAX_QUEUED` | `32` | queued heavy reads per process |
| `REGISTRY_HEAVY_READ_MAX_QUEUED_PER_WORKSPACE` | `8` | queued heavy reads per workspace |
| `REGISTRY_HEAVY_READ_QUEUE_TIMEOUT_MS` | `5000` | longest queue wait; not a handler deadline |

### Metrics, logs and traces

On the internal listener's `/metrics`, with no ID, credential, SQL or body labels, all prefixed
`registry_service_`: `request_total` (now incremented); `http_duration_seconds`,
`http_stream_duration_seconds`, `http_payload_bytes`, `http_db_queries`; per pool,
`db_acquire_seconds`, `db_round_trip_seconds`, `db_pool_connections`; `auth_stage_seconds`,
`api_key_proof_cache_total`; `heavy_reads`, `heavy_read_queue_seconds`, `heavy_read_rejected_total`.

## Compatibility

### Upgrade

No migration or operator action; the two default-on changes return the same responses.

### Rollback

Safe; nothing new is persisted. In place, `REGISTRY_BATCH_READS_ENABLED=false` and
`REGISTRY_API_KEY_PROOF_CACHE_ENABLED=false` restore per-item reads and per-request verification.

### Version skew

Only Registry changes; mixed-version replicas return identical responses.

## Security considerations

- The proof cache holds no plaintext key, principal or authorization decision. Reuse needs the same
  fingerprint and stored hash within 60 seconds; revocation, expiry, workspace archival and scope
  changes apply on the next request because the lookup always runs.
- Batched queries filter on the authenticated workspace; `loadSessionRows` and the read context
  reject mixed-workspace input; admission keys on the authenticated workspace, never a header;
  metric labels are bounded, and `/metrics` is not served on the public listener.

## Testing

- `services/registry-service-ts/test/integration/api-performance.spec.ts` (real Postgres) runs a
  per-item and a batched app over 100 Sessions, Agents and Threads with coordinator children, a
  missing roster Agent, an invalid snapshot, overrides, differing bindings, detached resources,
  outcomes and a second workspace, requiring equal status and JSON in both dialects for limits 1, 20
  and 100, cursors, filters, detail, Threads and Trigger history. Including three auth queries it
  asserts at most 14 queries per Session page, exactly 4 per Agent page and at most 8 per Thread
  page; at 100 items it measured Sessions 874 → 13, Threads 205 → 7, Agents 104 → 4 (fixture query
  counts, not latency). It also covers foreign-workspace keys, concurrent deletion, and revocation,
  expiry, scope change and hash rotation after a warmed proof.
- `test/integration/api-performance-metrics.spec.ts` (real `pg`) covers every call form and
  per-request counts on a one-connection pool. Unit specs cover the loader, bounded reads,
  admission, proof cache and SSE metrics, and memory-store's `filtered-metadata` spec the ID filter.

## Alternatives

- **A bigger pool** removes no round trips and multiplies connections across replicas on a shared
  instance. **New indexes**: the hot statements already used indexes; their count was the cost.
- **One CTE returning every collection as JSON**: fewest round trips and fast in a prototype, but it
  moves JSON aggregation into Postgres and is harder to keep in parity through fallbacks or rosters.
- **A list-only preloader (an earlier design)** read a Session page's associations up front, so it
  mirrored the renderer's roster traversal and missed detail, Threads and Trigger history.
- **Caching responses, principals or failed verifications, or cheaper Argon2 parameters**: state
  changes continuously, revoked keys would keep working, failures are unbounded, hashes weaken.
- **Coalescing `lastUsedAt`** changes a timestamp that may back audit or cleanup; the freshness
  contract it needs is on the roadmap. **Read replicas** add lag that revocation cannot accept.
  **Admission on every request, or on by default**, needs measured capacity and risks starving light
  reads, writes, internal usage and heartbeats.

## Status notes

Divergences from the pre-implementation design: loaders were injected into the existing renderers
instead of a separate bundle loader and pure projection, keeping the per-item path behind one switch
rather than one per route family (the Environment, Memory and idempotency fixes have none). The
optional read-only transaction is not used; admission for every JSON request became an opt-in gate
on five routes; `lastUsedAt` coalescing and the provisional latency targets were not adopted.

Deployment validation, end-to-end cancellation and caller polling, Memory logical pagination and
version summaries, an outcome read model, revision-aware pricing reuse, shared SSE subscriptions,
outbox leases, archive packaging and cross-replica launch coordination are listed under
[Remaining API performance work](../docs/managed-agents/roadmap.md#remaining-api-performance-work).
