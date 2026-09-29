# memory-store (library)

> An _interface_ (TS module) for path-addressed text-memory storage with an
> immutable version chain, backed by S3-compatible blob storage (live state +
> sha-keyed version archive) plus Postgres metadata. v1 ships
> `LocalMemoryStore` composing `S3MemoryBlobStore` and
> `PostgresMemoryMetadataStore`. `memory-store` is **not** a deployable
> service — consumers import the `@orca/memory-store` package and call S3 +
> Postgres in-process.

The first principles below explain why path-addressed live state in S3 (the
FUSE-mountable namespace) plus a sha-keyed version archive is the right shape
for cross-session memory; everything else is an implementation detail of the
S3 + Postgres backend.

## What it is

The Anthropic Managed Agents API exposes top-level **MemoryStores**
(`/v1/memory_stores`) — workspace-scoped collections of path-addressed text
documents that get mounted as a navigable directory inside the sandbox at
`/mnt/memory/{store_name}/`. Agents read and write via the standard file
tools (`bash`, `read`, `write`, `edit`); each write produces an immutable
version. We mirror that exactly. `@orca/memory-store` is a TypeScript library
that holds the bytes (in object storage, both live state and the version
archive) and the metadata (in Postgres). Consumers (`registry-server`,
`harness-server`) import it as a workspace dependency and call into S3 +
Postgres in-process — no gRPC hop.

## Architecture

```
s3://{bucket}/{root}workspaces/{workspace_id}/memory-stores/{store_id}/live/{path}
s3://{bucket}/{root}workspaces/{workspace_id}/memory-stores/{store_id}/versions/{sha256}

memorystore (Postgres):
  memory_stores      → workspace-scoped collection metadata
  memories           → current sha + size + last writer per (workspace_id, store_id, path)
  memory_versions    → append-only chain; one row per write; redact = mark redacted_at
```

- **Live state path-addressed.** The FUSE mount **is** the live namespace:
  agent writes via `s3fs` land directly in S3 at
  the workspace/store `live/` prefix. No write-through interceptor
  in the harness path.
- **Version chain sha-keyed.** A per-session `MemoryVersionWatcher` polls the
  live prefix every ~2 s, computes `sha256` for each changed path, copies the
  bytes to `versions/{sha256}`, and registers a `memver_…` row in
  `memory_versions`. Versions are immutable; redact marks `redacted_at`
  rather than deleting the row.
- **Cross-session persistence inherent.** Bytes live in S3, persisted across
  sandbox lifecycle. A second session attaching the same `memory_store` sees
  the live S3 state.
- **Async write detection.** Writes are observed by a polling watcher rather than synchronous libfuse
  write-through; [`roadmap.md`](../roadmap.md) records the condition that would
  change that. See
  [`../mount-strategies.md`](../mount-strategies.md) and
  [`../memory-conflict-semantics.md`](../memory-conflict-semantics.md).

## Conflict semantics — last-writer-wins + `session.memory_conflict` events

Two sessions writing to the same path race in S3 (s3fs has no conditional
PUT). The watcher detects the race after-the-fact: when the registered
`previous_sha256` does not match its cached value for that path, the
Registry's workspace/session/store-scoped memory-version route applies last-writer-wins on
`memories.current_sha256` and returns `conflict: true`. The watcher then
appends a `session.memory_conflict` event to the transcript stream so SDK
consumers can reconcile.

The full conflict-event shape, the eventual-consistency window, and the
upgrade path to synchronous write-through are documented in
[`../memory-conflict-semantics.md`](../memory-conflict-semantics.md).

## Public interface

The library exports a `MemoryStore` interface and one v1 implementation,
`LocalMemoryStore`. The shape mirrors what was originally drafted as a
`.proto` service definition from before memory-store became a library
(mirroring the file-store demotion):

```ts
// Logical shape (the package exports a TS interface; consumers call methods
// in-process, no RPC.)
interface MemoryStore {
  createStore(input: CreateStoreInput): Promise<MemoryStoreRecord>;
  listStores(workspaceId: string, opts?: ListStoresOptions): Promise<{ items: MemoryStoreRecord[]; nextCursor: string | null }>;
  getStore(workspaceId: string, storeId: string): Promise<MemoryStoreRecord | null>;
  archiveStore(workspaceId: string, storeId: string): Promise<void>;
  deleteStore(workspaceId: string, storeId: string): Promise<void>;

  // Memories within a store:
  getMemory(workspaceId: string, storeId: string, memoryId: string): Promise<MemoryRecord | null>;
  getMemoryByPath(workspaceId: string, storeId: string, path: string): Promise<MemoryRecord | null>;
  listMemories(workspaceId: string, storeId: string): Promise<MemoryRecord[]>;
  openMemory(workspaceId: string, storeId: string, memoryId: string): Promise<OpenMemory | null>;
  writeMemory(input: WriteMemoryInput): Promise<{ memory: MemoryRecord; version: MemoryVersionRecord }>;

  // Versions:
  listVersions(workspaceId: string, storeId: string, memoryId: string): Promise<MemoryVersionRecord[]>;
  redactVersion(workspaceId: string, storeId: string, versionId: string): Promise<MemoryVersionRecord>;

  close(): Promise<void>;
}

class MemoryConflictError extends Error {
  constructor(public readonly observedSha: string, public readonly expectedSha: string);
}
```

`writeMemory` accepts an optional `previousSha256` for optimistic concurrency
on the public route; the in-process call surfaces `MemoryConflictError` on
CAS mismatch. The mesh-internal scoped memory-version path used by the watcher
derives workspace/store/writer session from the URL, uses last-writer-wins,
and returns `conflict: true` so the
caller can emit a transcript event.

`MemoryStoreRecord.id` uses the `mems_…` prefix; `MemoryRecord.id` uses
`mem_…`; `MemoryVersionRecord.id` uses `memver_…` — matching Anthropic's
published id conventions.

`listMemories` and `listAllVersions` accept an optional third argument,
`{ memoryIds: readonly string[] }`. PostgreSQL and in-memory metadata backends apply this
filter together with workspace/store scope; an empty list returns no rows. Ordering and
historical deletion/redaction behavior are unchanged. `LocalMemoryStore` preserves its store
ownership check and forwards the filter. Registry uses it to read only page memories' histories
and to narrow memory-specific version queries. These methods still return complete histories
for the selected memories; the filter is not version pagination.

## Layout

```
packages/memory-store/
  src/
    index.ts                 # public re-exports (MemoryStore, LocalMemoryStore, S3MemoryBlobStore, …)
    store.ts                 # MemoryStore interface + MemoryConflictError
    local-memory-store.ts    # LocalMemoryStore — v1 implementation
    types.ts                 # MemoryStoreRecord, MemoryRecord, MemoryVersionRecord, errors
    ids.ts                   # mems_/mem_/memver_ id minting
    blob/
      blob-store.ts          # MemoryBlobStore interface: live + version namespaces
      s3.ts                  # S3MemoryBlobStore (AWS S3 / MinIO / R2 / GCS-via-interop)
      in-memory.ts           # InMemoryMemoryBlobStore (test double)
    metadata/
      store.ts               # MemoryMetadataStore interface
      postgres.ts            # PostgresMemoryMetadataStore + applyMigrations + Drizzle schema
      in-memory.ts           # InMemoryMemoryMetadataStore (test double)
      migrations/            # drizzle-generated SQL migrations
  test/
    unit/                    # interface-level tests (in-memory blob + metadata)
    integration/             # against real RustFS + Postgres
  package.json               # name: "@orca/memory-store"
  drizzle.config.ts
  vitest.integration.config.ts
```

The package has no runtime entry point of its own — it exports types and
classes for in-process use by `services/registry-service-ts/` and
`services/harness-server/`. There is no Dockerfile, no `/healthz` server,
and no gRPC stubs.

Pure library. No replicas, no coordination — the consuming service's
lifecycle owns the Postgres `Pool` and S3 client.

## Quick start

```ts
import { Pool } from 'pg';
import { S3Client } from '@aws-sdk/client-s3';
import {
  LocalMemoryStore,
  S3MemoryBlobStore,
  PostgresMemoryMetadataStore,
  applyMigrations,
} from '@orca/memory-store';

const pool = new Pool({ connectionString: process.env.MEMORYSTORE_DATABASE_URL });
await applyMigrations(pool);

const s3 = new S3Client({
  endpoint: 'http://localhost:9000',
  region: 'us-east-1',
  credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' },
  forcePathStyle: true,
});

const memoryStore = new LocalMemoryStore({
  blob: new S3MemoryBlobStore({ client: s3, bucket: 'orca-files', keyPrefix: 'memory/' }),
  metadata: new PostgresMemoryMetadataStore({ pool }),
});

const store = await memoryStore.createStore({ workspaceId: 'ws_abc', name: 'notes' });
const { memory, version } = await memoryStore.writeMemory({
  storeId: store.id,
  path: 'plans/2026-q2.md',
  content: Readable.from(Buffer.from('quarterly plan')),
  sizeBytes: 14,
  sha256: '…',
  writtenBySessionId: 'ses_xyz',
});
```

## Backends

Backend object store is pluggable. The live namespace is path-addressed (one
S3 key per memory path); the version archive is sha-keyed (one S3 key per
unique sha256). The same blob can back many `memver_…` rows when the bytes
repeat.

| Implementation            | Use case                                        |
| ------------------------- | ----------------------------------------------- |
| `S3MemoryBlobStore`       | Production (AWS S3, MinIO, R2, GCS-via-interop) |
| `InMemoryMemoryBlobStore` | Tests + development                             |

Metadata backend is also pluggable. Production uses
`PostgresMemoryMetadataStore`; tests use `InMemoryMemoryMetadataStore`.

## Sandbox materialization

When a session attaches a `memory_store` resource, the harness mounts each
store at `/mnt/memory/{store_name}/` via `MemoryFuseStrategy` (FUSE-capable
runtimes) or `LocalMemoryStrategy` (test InMemory runtime). The FUSE mount
**is** the live S3 namespace — the agent's `read`/`write`/`edit` operations
through the FUSE layer translate 1:1 to S3 GET/PUT/DELETE under the same
scoped STS credentials minted by `SessionCredsMinter`.

A per-session `MemoryVersionWatcher` polls each attached store's
`{root}workspaces/{workspace_id}/memory-stores/{store_id}/live/` prefix
every 2 s. The interval is fixed in `MemoryVersionWatcher` and is not
operator-tunable. On each tick it
lists live keys, computes `sha256` for any path whose `LastModified`
advanced since the previous tick, and calls the scoped internal
`memory-versions` route to register the new version. The
registry-side route applies last-writer-wins on `memories.current_sha256`
and appends to `memory_versions`; on CAS mismatch it returns
`conflict: true` so the watcher can emit a `session.memory_conflict` event
on the transcript stream.

See [`../resource-mounting.md`](../resource-mounting.md) and
[`../mount-strategies.md`](../mount-strategies.md) for the full
`MountStrategy` story, and
[`../memory-conflict-semantics.md`](../memory-conflict-semantics.md) for
the consistency model.

## Idempotency

- `LocalMemoryStore.writeMemory` accepts an optional `previousSha256` for
  optimistic CAS — concurrent writes through the public route surface a
  `MemoryConflictError`. The mesh-internal route used by the watcher
  short-circuits to last-writer-wins (returns `conflict: true`) so the
  watcher can keep up with S3 even when two sessions race.
- `LocalMemoryStore.redactVersion` is idempotent on `(version_id)` —
  re-marking a version as redacted is a no-op.
- All other write methods (`archiveStore`, `deleteStore`) are idempotent at
  the registry layer via the `Idempotency-Key` header — registry's
  `idempotency_keys` table caches the response for 24 h before the
  in-process call lands here.

## Public exports

| Export                        | Kind      | Notes                                                       |
| ----------------------------- | --------- | ----------------------------------------------------------- |
| `MemoryStore`                 | Interface | Public type — what consumers depend on                      |
| `LocalMemoryStore`            | Class     | v1 implementation composing blob + metadata                 |
| `MemoryBlobStore`             | Interface | Live (path) + version (sha) namespaces                      |
| `S3MemoryBlobStore`           | Class     | Production backend (AWS S3, MinIO, R2, GCS-via-interop)     |
| `InMemoryMemoryBlobStore`     | Class     | Test double                                                 |
| `MemoryMetadataStore`         | Interface | Postgres or in-memory                                       |
| `PostgresMemoryMetadataStore` | Class     | Production metadata backend (Drizzle + Postgres)            |
| `InMemoryMemoryMetadataStore` | Class     | Test double                                                 |
| `MemoryConflictError`         | Class     | Thrown on CAS mismatch in `writeMemory` (public route only) |
| `MemoryStoreRecord`           | Type      | `id` prefix `mems_`                                         |
| `MemoryRecord`                | Type      | `id` prefix `mem_`                                          |
| `MemoryVersionRecord`         | Type      | `id` prefix `memver_`                                       |
| `applyMigrations`             | Function  | Apply Drizzle migrations against the consumer's `Pool`      |

## Tables (in the `memorystore` Postgres database)

```
memory_stores       (id, workspace_id, name, description,
                     archived_at, created_at, updated_at)
                    -- INDEX (workspace_id)
                    -- UNIQUE (workspace_id, name)

memories            (id, workspace_id, store_id, path, current_sha256, size_bytes,
                     updated_at, updated_by_session_id, updated_by_event_id,
                     deleted_at)
                    -- UNIQUE (workspace_id, store_id, path)
                    -- FK (workspace_id, store_id) -> memory_stores

memory_versions     (id, workspace_id, store_id, memory_id, path, sha256, size_bytes,
                     written_by_session_id, written_by_event_id,
                     written_at, redacted_at)
                    -- INDEX (workspace_id, store_id, written_at)
                    -- FK (workspace_id, store_id, memory_id) -> memories
```

`memorystore` is a separate Postgres database from `registry` and `filestore`
(the same split pattern file-store uses). Apply migrations via `applyMigrations(pool)` at
service startup. See [`../data-model.md`](../data-model.md).

## Metrics

The library exposes a Prometheus registry consumers can merge into their own
`/metrics` exposition. The harness-server's per-session
`MemoryVersionWatcher` registers complementary metrics in its own registry
(`harness_*`) for the consistency window:

| Metric                                   | Type      | Owner   | Labels                                                 | Meaning                                                                                               |
| ---------------------------------------- | --------- | ------- | ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `memory_store_create_total`              | Counter   | library | `result` (`ok` / `conflict` / `error`)                 | `createStore()` / `writeMemory()` outcomes                                                            |
| `memory_store_version_total`             | Counter   | library | `result` (`ok` / `redacted` / `conflict` / `error`)    | Version-chain mutations                                                                               |
| `memory_store_blob_bytes_total`          | Counter   | library | `op` (`putLive` / `putVersion` / `openLive` / …)       | Bytes flowing through the BlobStore                                                                   |
| `harness_memory_write_lag_seconds`       | Histogram | harness | —                                                      | Wall-clock seconds from S3 `LastModified` to `recordMemoryVersion` return. **SLO target: p95 < 3 s.** |
| `harness_memory_versions_recorded_total` | Counter   | harness | `workspace_id`, `result` (`ok` / `conflict` / `error`) | Versions recorded by the per-session watcher                                                          |
| `harness_memory_watcher_poll_total`      | Counter   | harness | `result` (`ok` / `error`)                              | Watcher poll cycles                                                                                   |

## Tests

```bash
pnpm -F @orca/memory-store test                # unit (no infra)

# Integration (against real RustFS + Postgres):
make dev-up
MEMORYSTORE_DATABASE_URL=postgres://orca:orca@localhost:5432/memorystore \
S3_ENDPOINT=http://localhost:9000 \
S3_BUCKET=orca-files \
S3_ACCESS_KEY=minioadmin \
S3_SECRET_KEY=minioadmin \
pnpm -F @orca/memory-store test:integration
make dev-down
```

## Resource lifecycle

`LocalMemoryStore.archiveStore(workspaceId, storeId)` marks the metadata row
but intentionally does NOT delete the underlying S3 prefix — operators can
restore a soft-archived store by clearing `archived_at`. `deleteStore`
sets `deleted_at` on the store, its memories, and its versions in one transaction.
The metadata graph and both live and immutable version blobs remain stored. Reads
exclude deleted rows, including when archived stores are requested; subsequent
writes cannot revive a deleted store. `deleteMemory` marks the
current memory row deleted, removes its live blob, and retains version rows.

Internal filesystem checkpoint deletions can pass a deterministic `versionId`,
`previousSha256`, and `writtenByEventId`. The metadata transaction checks the digest
and records one deletion receipt; retries cannot delete a new memory at the same path.
Checkpoint deletions remove the live blob before succeeding, so a fresh FUSE mount or
version watcher cannot rediscover the deleted bytes. If blob cleanup fails after the
tombstone commits, the call fails; a retry with the same receipt retries cleanup without
adding another version. Cleanup checks active path ownership under the store write lock
and preserves a recreated Memory, including one with the same bytes. Public deletion
keeps its existing best-effort live-blob cleanup.
Replaying an older write repairs its immutable version bytes, but only repairs live
bytes when that same active Memory occurrence still owns the path and digest.
Store writes and deletions hold `MemoryMetadataStore.withStoreWriteLock` across
metadata and blob operations. PostgreSQL uses a session advisory lock scoped to the
workspace and store, and runs the callback's metadata queries on that same connection;
the in-memory backend queues writers. This serializes a delayed live-byte repair with
another writer's delete, rename or replacement. Deletion preconditions also pin the
original path, so a renamed Memory is not removed by an older path deletion.

`LocalMemoryStore.redactVersion(workspaceId, storeId, versionId)` marks
`memory_versions.redacted_at`. The version row stays visible in `listVersions`
so audit trails remain intact, while subsequent reads return 410 / null.
Version blobs are SHA-addressed and may be shared by a non-redacted Version, so
the bytes are retained until reference-aware deletion or per-Version blob keys
are implemented.

## Consistency model

Per [`../mount-strategies.md`](../mount-strategies.md), the library ships:

- `MemoryFuseStrategy` (S3-FUSE backed live mount) as the default for
  FUSE-capable runtimes. `LocalMemoryStrategy` is the runtime Files API
  fallback for AgentENV, Local, and `InMemorySandboxRuntime`.
- An async polling `MemoryVersionWatcher` (~2 s) for version registration.
  Synchronous libfuse write-through is recorded in [`roadmap.md`](../roadmap.md) with the condition that would justify it — if the
  `harness_memory_write_lag_seconds` p95 SLO is violated.

The customer-visible caveat (eventual consistency window across sessions,
last-writer-wins on concurrent writes, `session.memory_conflict` events)
is documented in
[`../memory-conflict-semantics.md`](../memory-conflict-semantics.md).
