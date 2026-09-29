# @orca/file-store

> Library: `FileStore` interface + `LocalFileStore` v1 implementation backed by
> S3-compatible blob storage + Postgres metadata. Not a deployable service —
> consuming services (`registry-server`, `harness-server`) import the package
> as a workspace dependency and call into S3 + Postgres in-process.

## Quick start

```ts
import { Pool } from 'pg';
import { S3Client } from '@aws-sdk/client-s3';
import { LocalFileStore, S3BlobStore, applyMigrations } from '@orca/file-store';

const pool = new Pool({ connectionString: process.env.FILESTORE_DATABASE_URL });
await applyMigrations(pool);

const s3 = new S3Client({
  endpoint: 'http://localhost:9000',
  region: 'us-east-1',
  credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' },
  forcePathStyle: true,
});

const fileStore = new LocalFileStore({
  pool,
  blobStore: new S3BlobStore({ client: s3, bucket: 'orca-files' }),
});

const file = await fileStore.create({
  workspaceId: 'ws_abc',
  filename: 'hello.txt',
  mimeType: 'text/plain',
  content: Readable.from(Buffer.from('hello world')),
});

const opened = await fileStore.open('ws_abc', file.id);
console.log(await readAll(opened.stream));
```

## Architecture

```
LocalFileStore.create(input)
  ├─ stream content → tmp file (compute sha256 + size)
  ├─ findBySha(workspaceId, sha256) → if an active user upload exists, return it (DEDUP)
  ├─ BlobStore.put(sha256, fs.createReadStream(tmp), size)
  └─ INSERT files (workspace_id, sha256, blob_uri, …)
```

- **Content addressing**: blobs are keyed by their SHA-256 hex digest. The
  same content uploaded twice in one workspace returns the same active
  `file_…` id; archiving that record releases the digest for a fresh upload.
- **Postgres metadata** lives in a separate `filestore` database. Apply
  migrations via `applyMigrations(pool)` at service startup.
- **Object storage** is pluggable: `S3BlobStore` (works with AWS S3, MinIO,
  Cloudflare R2, GCS via interop), `InMemoryBlobStore` (test double).

## Backends

| Implementation      | Use case                                        |
| ------------------- | ----------------------------------------------- |
| `S3BlobStore`       | Production (AWS S3, MinIO, R2, GCS-via-interop) |
| `InMemoryBlobStore` | Tests + development                             |

## Metrics

The library exposes a Prometheus registry (`fileStoreMetricsRegistry`) consumers
can merge into their own `/metrics` exposition:

| Metric                        | Type    | Labels                              | Meaning                                                 |
| ----------------------------- | ------- | ----------------------------------- | ------------------------------------------------------- |
| `file_store_create_total`     | Counter | `status` (`ok` / `dedup` / `error`) | `create()` call outcomes                                |
| `file_store_dedup_hits_total` | Counter | —                                   | Calls that hit an existing `(workspace_id, sha256)` row |
| `file_store_blob_bytes_total` | Counter | `op` (`put` / `open`)               | Bytes flowing through the BlobStore                     |

## Tests

```bash
pnpm -F @orca/file-store test                 # unit (no infra)

# Integration (against real RustFS + Postgres):
make dev-up
KAFKA_BROKERS=localhost:9092 \
FILESTORE_DATABASE_URL=postgres://orca:orca@localhost:5432/filestore \
S3_ENDPOINT=http://localhost:9000 \
pnpm -F @orca/file-store test:integration
make dev-down
```

## Resource lifecycle

`LocalFileStore.archive(workspaceId, fileId)` marks the metadata row but
intentionally does NOT delete the underlying blob — other workspaces / future
re-uploads may share the same SHA-256. There is no sweeper for orphaned
content.

## How files reach the sandbox

Per [`docs/managed-agents/mount-strategies.md`](../../docs/managed-agents/mount-strategies.md),
`TarballPrefetchStrategy` is the only strategy for materializing file
resources into the sandbox: the harness host fetches the blobs and streams
them in at session start. Files up to ~50 MB pre-fetch in sub-second.

Sandbox execution credentials never include the file-blob namespace, so a
sandbox-side S3/FUSE mount of the blob prefix is not an option. If large
files (>50 MB median, or per-session boot p50 > 5 s) become routine, any
lazy-read replacement must be manifest-aware and authorize exact files. See
[`roadmap.md`](../../docs/managed-agents/roadmap.md).
