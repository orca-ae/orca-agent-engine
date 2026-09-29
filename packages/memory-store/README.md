# @orca/memory-store

> Library: `MemoryStore` interface + `LocalMemoryStore` v1 implementation backed
> by S3-compatible blob storage + Postgres metadata. Not a deployable service —
> consuming services (`registry-server`, `harness-server`) import the package as
> a workspace dependency and call into S3 + Postgres in-process.

## Quick start

```ts
import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { Pool } from 'pg';
import { S3Client } from '@aws-sdk/client-s3';
import {
  LocalMemoryStore,
  PostgresMemoryMetadataStore,
  S3MemoryBlobStore,
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
  blobStore: new S3MemoryBlobStore({ client: s3, bucket: 'orca-memory', keyPrefix: '' }),
  metadataStore: new PostgresMemoryMetadataStore(pool),
});

const store = await memoryStore.createStore({ workspaceId: 'ws_abc', name: 'notes' });

const bytes = Buffer.from('# hello');
const { version } = await memoryStore.writeMemory({
  workspaceId: 'ws_abc',
  storeId: store.id,
  path: 'notes/a.md',
  content: Readable.from(bytes),
  sizeBytes: bytes.byteLength,
  sha256: createHash('sha256').update(bytes).digest('hex'),
});
console.log(version.sha256);
```

## Architecture

```
{root}workspaces/{ws}/memory-stores/{store}/
  live/{path}              ← current bytes; the sandbox FUSE-mounts this prefix
  versions/{sha256}        ← immutable content-addressed archive
```

```
LocalMemoryStore.writeMemory(input)
  ├─ metadata CAS on previousSha256 → MemoryConflictError on mismatch
  ├─ blobStore.putVersion(sha256, bytes)   (immutable archive)
  └─ blobStore.putLive(path, bytes)        (live mount)
```

Metadata commits **before** blob writes, so a CAS failure leaves no orphan blobs
behind.

- **Path-addressed, not vector.** Memories are text documents keyed by a
  workspace/store-scoped relative path (`normalizeMemoryRelativePath` enforces
  the shape; `MAX_MEMORY_PATH_LENGTH` caps it).
- **Immutable version chain.** Every write appends a `versions/{sha256}` object
  attributed to the originating session and event. Deleting a memory removes the
  live bytes; historical versions stay addressable.
- **Postgres metadata** lives in a separate `memorystore` database
  (`memory_stores`, `memories`, `memory_versions`). Apply migrations via
  `applyMigrations(pool)` at service startup.

## Backends

| Implementation                | Use case                                        |
| ----------------------------- | ----------------------------------------------- |
| `S3MemoryBlobStore`           | Production (AWS S3, MinIO, R2, GCS-via-interop) |
| `InMemoryMemoryBlobStore`     | Tests + development                             |
| `PostgresMemoryMetadataStore` | Production metadata                             |
| `InMemoryMemoryMetadataStore` | Tests + development                             |

## How the sandbox sees it

`harness-server` mounts only each attached store's `live/` prefix via
`MemoryFuseStrategy` (`services/harness-server/src/sandbox/mounts/memory-fuse.ts`)
with a read-only or read-write policy per resource. A per-session
`MemoryVersionWatcher`
(`services/harness-server/src/sandbox/memory/version-watcher.ts`) polls that
prefix and registers new versions through a workspace/session/store-scoped
internal route, emitting `session.memory_conflict` on CAS mismatch.

That polling watcher is the deliberate v1 design choice over synchronous
libfuse write-through — see
[`memory-conflict-semantics.md`](../../docs/managed-agents/memory-conflict-semantics.md)
for the eventual-consistency window and the conflict-event shape.

## Tests

```bash
pnpm -F @orca/memory-store test                # unit (no infra)

# Integration (against real RustFS + Postgres):
make dev-up
MEMORYSTORE_DATABASE_URL=postgres://orca:orca@localhost:5432/memorystore \
S3_ENDPOINT=http://localhost:9000 \
pnpm -F @orca/memory-store test:integration
make dev-down
```

## Design docs

- [`libraries/memory-store.md`](../../docs/managed-agents/libraries/memory-store.md)
  — the library contract.
- [`mount-strategies.md`](../../docs/managed-agents/mount-strategies.md) — how
  `MemoryFuseStrategy` fits the wider `MountStrategy` interface.
- [`memory-conflict-semantics.md`](../../docs/managed-agents/memory-conflict-semantics.md)
  — conflict detection, the consistency window, and the customer-facing summary.
