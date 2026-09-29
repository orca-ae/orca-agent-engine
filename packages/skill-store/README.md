# @orca/skill-store

> Library: `SkillStore` interface + `S3SkillStore` implementation for immutable,
> digest-addressed Skill bundles in object storage. Not a deployable service —
> `registry-service-ts` writes bundles and `harness-server` reads the exact versions
> a Session pinned, both in-process. The registry also reads pinned bundles to push
> them to session runners.

## Quick start

```ts
import { S3Client } from '@aws-sdk/client-s3';
import { S3SkillStore } from '@orca/skill-store';

const store = new S3SkillStore({
  client: new S3Client({
    endpoint: 'http://localhost:9000',
    region: 'us-east-1',
    credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' },
    forcePathStyle: true,
  }),
  bucket: 'orca-skills',
  keyPrefix: 'managed-agents/',
});

const record = await store.put('ws_abc', 'skv_123', [
  { path: 'SKILL.md', content: Buffer.from('# My skill\n') },
  { path: 'scripts/run.sh', content: Buffer.from('#!/bin/sh\n'), mode: 0o755 },
]);

// `record.sha256` is the bundle digest — the only handle that can reopen it.
const bundle = await store.open('ws_abc', 'skv_123', record.sha256);
```

## Architecture

```
{root}workspaces/{workspaceId}/skill-versions/{versionId}/bundles/{sha256}/bundle.json
```

A bundle is a single `orca.skill-bundle.v1` envelope holding every file plus a
manifest. The whole envelope is content-addressed by SHA-256: `open()` re-hashes
on read and throws `SkillBundleIntegrityError` on mismatch, so a tampered or
truncated object can never be materialized into a sandbox.

- **Immutable.** There is no update path. A new bundle is a new digest;
  `delete()` is idempotent so re-deleting an absent object succeeds.
- **Pinned per session.** Registry records the exact `(versionId, sha256)` a
  Session bound to; the harness opens that digest, not "latest".
- **Validated on write.** `codec.ts` rejects absolute, Windows-style,
  traversing, or over-long paths, caps the bundle at 64 MiB, and enforces
  unique paths under **Unicode full case folding**
  (`unicode-case-fold.ts`) — JavaScript's own casing primitives are
  locale-sensitive and would let `ẞ` and `ß` collide differently across hosts.

## Backends

| Implementation       | Use case                                        |
| -------------------- | ----------------------------------------------- |
| `S3SkillStore`       | Production (AWS S3, MinIO, R2, GCS-via-interop) |
| `InMemorySkillStore` | Tests + development                             |

## Errors

| Error                        | Raised when                                                      |
| ---------------------------- | ---------------------------------------------------------------- |
| `SkillBundleValidationError` | A path, mode, or size violates the envelope rules on `put()`     |
| `SkillBundleIntegrityError`  | The stored bytes do not hash to the requested digest on `open()` |
| `SkillBundleNotFoundError`   | No object exists at `(workspaceId, versionId, sha256)`           |

## Tests

```bash
pnpm -F @orca/skill-store test                # unit (no infra)

# Integration (against real RustFS):
make dev-up
S3_ENDPOINT=http://localhost:9000 pnpm -F @orca/skill-store test:integration
make dev-down
```

The Unicode fold table is generated, not hand-maintained:
`pnpm -F @orca/skill-store generate:unicode-case-fold`.

## Design docs

- [`libraries/skill-store.md`](../../docs/managed-agents/libraries/skill-store.md)
  — the library contract.
- [`skills.md`](../../docs/managed-agents/skills.md) — session pinning,
  read-only materialization, and progressive disclosure of the Skill catalog.
