# `@orca/skill-store`

`@orca/skill-store` is the private byte store for immutable SkillVersion
bundles. It is a TypeScript library imported in-process by Registry and Harness,
not a deployable service.

## Contract

- Registry calls `put(workspaceId, skillVersionId, files)` after validating an
  uploaded bundle. Input paths are relative to the bundle root.
- `put` serializes files deterministically, computes SHA-256 over the exact
  stored bytes, and returns package size plus a content-free manifest.
- Harness calls `open(workspaceId, skillVersionId, sha256)` using values from a
  persisted Session binding. `open` verifies the package digest, paths,
  manifest, and per-file digests before returning bytes.
- Objects are namespaced by workspace, SkillVersion, and digest. The sandbox
  never receives object-store credentials.
- `delete` cleans up uncommitted uploads after relational checks prove no
  SkillVersion owns the bundle. Registry records the target in
  `skill_bundle_deletion_outbox` and removes that entry after `delete` succeeds.
  Public Skill and SkillVersion deletion retains rows with `deleted_at` and
  retains their bundles, including after all referencing Sessions are deleted.
  Object deletion is idempotent, so startup and periodic reconciliation safely
  close process-crash and transient S3 failure windows.

The production backend uses the same S3-compatible client, bucket, and root
prefix as the other object-backed libraries, under a disjoint Skill namespace.
The configured backend must honor `If-None-Match: *` for `PutObject` and provide
strong read-after-write consistency for a single object key. CI verifies these
requirements against the pinned RustFS release with concurrent writes of one
bundle and a write over pre-existing bytes.

`InMemorySkillStore` supplies deterministic in-process tests.

See [`../skills.md`](../skills.md) for Session pinning, materialization, and
progressive-disclosure semantics.
