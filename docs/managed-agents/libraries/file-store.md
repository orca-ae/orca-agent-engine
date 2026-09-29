# file-store (library)

> An _interface_ (TS module) for content-addressed file storage, with
> pluggable blob backends. v1 ships `LocalFileStore` composing `S3BlobStore`
> and `PostgresFileMetadataStore`. `file-store` is **not** a deployable
> service — consumers import the `@orca/file-store` package and call S3 +
> Postgres in-process.

The first principles below explain why content-addressed (SHA-256) storage
with workspace-scoped dedup is the right shape; everything else is
implementation detail of the S3 + Postgres backend.

## What it is

The Anthropic Managed Agents API exposes top-level **Files** (`/v1/files`) — content-addressed blobs that get attached to a session via the `resources[]` array as a read-only mount at `mount_path`. We mirror that. `@orca/file-store` is a TypeScript library that holds the bytes (in object storage) and the metadata (in Postgres). Consumers (`registry-server`, `harness-server`) import it as a workspace dependency and call into S3 + Postgres in-process — no gRPC hop.

## Public interface

The library exports a `FileStore` interface and one v1 implementation, `LocalFileStore`. The shape mirrors what was originally drafted as a `.proto` service definition:

```ts
// Logical shape (the package exports a TS interface; consumers call methods
// in-process, no RPC.)
interface FileStore {
  create(input: CreateFileInput): Promise<FileRecord>;
  get(workspaceId: string, fileId: string): Promise<FileRecord>;
  list(workspaceId: string, opts?: ListOptions): Promise<FileRecord[]>;
  open(workspaceId: string, fileId: string): Promise<OpenStream>; // streaming download
  archive(workspaceId: string, fileId: string): Promise<void>;
  delete(workspaceId: string, fileId: string): Promise<void>;
}

interface FileRecord {
  id: string; // file_…
  workspaceId: string;
  filename: string;
  mimeType: string;
  sizeBytes: number;
  sha256: string; // content-addressed; dedup (purpose-aware, see below)
  createdAt: string;
  archivedAt: string | null;
  metadata: Record<string, string>;

  // Anthropic-compatible Files API fields.
  purpose: 'agent' | 'agent_output'; // user upload vs harness-registered output
  scopeId: string | null; // session id (`ses_…`) for `agent_output`; null for `agent`
  downloadable: boolean; // gates GET /v1/files/:id/content
}
```

### Purpose / scope_id / downloadable semantics

| Field          | `purpose='agent'` (user upload)                                               | `purpose='agent_output'` (indexer-registered)                          |
| -------------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `scope_id`     | `null` (workspace-scoped)                                                     | `ses_…` (originating session)                                          |
| `downloadable` | `false` (never retrievable via getContent)                                    | `true`                                                                 |
| Dedup          | Yes — among active `(workspace_id, sha256)` rows; archive releases the digest | No content dedup; explicit output identities replay their existing row |

`defaultDownloadable(purpose)` derives the bit from the purpose by default:
`agent_output → true`, everything else → `false`. The Harness route derives
`workspaceId`, `purpose='agent_output'`, `scopeId=sessionId`, and
`downloadable=true` from its scoped path; multipart fields cannot override
those ownership values.

### Dedup is purpose-aware

`LocalFileStore.create` only consults `findBySha` when `purpose='agent'`. For
`agent_output` we insert a distinct row per output operation, replaying only an
explicit matching output identity. Equal content never merges distinct outputs.
Two reasons:

1. **Correctness.** Output rows differ on `scope_id`, so two sessions
   producing the same bytes need two distinct `file_…` ids — the SDK's
   `files.list({ scope_id })` would otherwise miss one of them.
2. **Schema.** The Postgres unique index is partial:

   ```sql
   CREATE UNIQUE INDEX files_ws_sha256_agent_idx
     ON files (workspace_id, sha256)
     WHERE purpose = 'agent' AND archived_at IS NULL;
   ```

   Non-`agent` rows are excluded from the constraint, so `INSERT` is free
   to land duplicates by sha. The store's `findBySha` mirrors this — it
   only matches active `purpose='agent'` rows, so a previously-registered
   output blob does NOT short-circuit a future user upload of the same bytes,
   and re-uploading bytes from an archived user file creates a fresh active
   file record.

Uploads happen via HTTP `POST /v1/files` (multipart) terminating in `registry-server`, which streams bytes through to `LocalFileStore.create` directly (in-process). Direct uploads from clients go through registry only — the `FileStore` interface has no public listener.

## Layout

```
packages/file-store/
  src/
    index.ts                 # public re-exports (FileStore, LocalFileStore, S3BlobStore, …)
    store.ts                 # FileStore interface
    file-store.ts            # LocalFileStore — v1 implementation
    types.ts                 # FileRecord, CreateFileInput, errors
    metrics.ts               # prom-client counters
    blob/
      blob-store.ts          # BlobStore interface: put / open / delete
      s3.ts                  # S3BlobStore (AWS S3 / MinIO / R2 / GCS-via-interop)
      in-memory.ts           # InMemoryBlobStore (test double)
    metadata/
      postgres.ts            # PostgresFileMetadataStore + applyMigrations + files table
      migrations/            # drizzle-generated SQL migrations
  test/
    unit/                    # interface-level tests (in-memory blob)
    integration/             # against real RustFS + Postgres
  package.json               # name: "@orca/file-store"
  drizzle.config.ts
  vitest.integration.config.ts
```

The package has no runtime entry point of its own — it exports types and classes for in-process use by `services/registry-service-ts/` and `services/harness-server/`. There is no Dockerfile, no `/healthz` server, and no gRPC stubs.

Pure library. No replicas, no coordination — the consuming service's lifecycle owns the Postgres `Pool` and S3 client.

## Backends

Backend object store is pluggable. Content-addressed by SHA-256 — uploads dedupe automatically (idempotent on hash) inside a workspace. Physical keys use `{root}workspaces/{workspace_id}/files/blobs/{aa}/{bb}/{sha256}/content`; identical bytes in two workspaces never share an object.

| Implementation      | Use case                                        |
| ------------------- | ----------------------------------------------- |
| `S3BlobStore`       | Production (AWS S3, MinIO, R2, GCS-via-interop) |
| `InMemoryBlobStore` | Tests + development                             |

## Sandbox materialization

When a session has files in its `resources[]`, the `harness-server` resolves each `file_id` to bytes via `LocalFileStore.open` at mount time (in-process call) and writes them into the sandbox at `mount_path` (read-only directory marker). For E2B v1 this is a copy via `sandbox.files.write`; for K8s sandboxes later, a CSI volume could fetch lazily. The mount path layout is identical to Anthropic's, so SDK code paths that read mounted files keep working.

See [`../resource-mounting.md`](../resource-mounting.md) and [`../mount-strategies.md`](../mount-strategies.md) for the full `MountStrategy` story.

## Idempotency

- `LocalFileStore.create` deduplicates active user uploads on `(workspace_id, sha256)`; archived uploads and agent outputs do not participate.
- All other write methods (`archive`, `delete`) are idempotent at the registry layer via the `Idempotency-Key` header — registry's `idempotency_keys` table caches the response for 24 h before the in-process call lands here.

## Data model

File deletion sets `deleted_at` and retains both the metadata row and blob. Reads,
lists, and deduplication exclude deleted rows. The active digest is released so a
new upload of the same bytes receives a new File ID. Archive remains separate.

See [`../data-model.md`](../data-model.md) — `files` table.

## Why every file resource is prefetched

`TarballPrefetchStrategy` handles all file resources. Execution STS credentials
carry no file grant, so a sandbox-side FUSE mount cannot expose a workspace blob
prefix — a lazy reader would have to be manifest-aware and authorize exact file
ids. See [`../mount-strategies.md`](../mount-strategies.md), and
[`../roadmap.md`](../roadmap.md) for the size threshold that would justify one.

### Replaying session output writes

Internal writers may supply `CreateFileInput.id` for a session-scoped
`agent_output`. The ID identifies one output operation, independent of its
content digest. Retrying after a process restart returns the existing File only
when its workspace, session, bytes, filename, MIME type, metadata, and download
policy match. Concurrent retries use the existing File primary key; different
output identities still produce distinct Files even when their bytes match.
Archived or deleted outputs are never resurrected by a retry. Public upload
requests do not accept this internal replay parameter.
