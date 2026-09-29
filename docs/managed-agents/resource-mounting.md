# Resource Mounting

> Git credential helper routes and `git_cred://` references are Orca extensions; see [`orca-extensions.md`](./orca-extensions.md).
> How session `resources[]` (Files, MemoryStores, GitHub repos) are surfaced inside the agent sandbox at `mount_path` so the standard file tools (bash, read, write, edit, glob, grep) work transparently.

## Strategy interface

The harness owns mount lifecycle. We define a `MountStrategy` interface inspired by OpenAI Agents SDK's [`MountStrategyBase`](https://github.com/openai/openai-agents-python/blob/main/src/agents/extensions/sandbox/e2b/mounts.py):

```ts
interface MountStrategy {
  activate(sandbox: SandboxHandle, resource: SessionResource): Promise<MountHandle>;
  deactivate(handle: MountHandle): Promise<void>;
  // Pause/resume hooks for sandbox snapshotting
  teardown_for_snapshot(handle: MountHandle): Promise<TornDownState>;
  restore_after_snapshot(sandbox: SandboxHandle, torn: TornDownState): Promise<MountHandle>;
}
```

v1 runtime selection uses three `MountStrategy` implementations:

- `TarballPrefetchStrategy` — all `file` resources; exact files are fetched host-side and receive no sandbox S3 grant.
- `MemoryFuseStrategy` (+ `LocalMemoryStrategy` test fallback) for `memory_store` resources.
- `GitCloneStrategy` for `github_repository` resources (host-side `git clone --filter=blob:none --depth=1` + stream working tree + `.git/` into the sandbox).

`MountStrategy` choice is per resource type; see [`mount-strategies.md`](./mount-strategies.md).

## Polymorphic session-resources field

A session's `resources[]` is a polymorphic attachment array with its own ID per entry (`sesrsc_…`) so it can be revoked individually:

```
session.resources[*] = oneof {
  { type: "file",            file_id,         mount_path,  access: "read_only" }
  { type: "memory_store",    memory_store_id, access, instructions }
  { type: "github_repository", url, authorization_token, mount_path, checkout }
}
```

For `memory_store`, `mount_path` exists only on responses. Registry derives it
under `/mnt/memory/` from the attached store and rejects caller-supplied paths,
matching Anthropic's `BetaManagedAgentsMemoryStoreResourceParam` contract.

Upgrade migration `0036_migrate_memory_mount_paths` rewrites active legacy
attachments that persisted caller-selected paths outside this root. Each gets a
stable `/mnt/memory/legacy-{session_resource_id}[-N]/` path; `-N` resolves an
existing active mount collision, and the owning Session's `runtime_revision` is
advanced once so its next runner uses the migrated snapshot. If another active
resource owns `/`, `/mnt`, or `/mnt/memory`, no non-overlapping destination
exists; migration fails with an explicit detach-or-move gate instead of starting
a partially mounted session. Detached historical attachments are unchanged.

Constraints enforced in `registry-service`:

- Max 100 files per session (matches Anthropic; protects sandbox boot time).
- Max 8 memory stores per session (matches Anthropic).
- `/workspace/skills` is reserved for verified Skill bundles. No Session
  resource may mount at that path, below it, or at one of its ancestors
  (including `/workspace` and `/`); create, attach, and update all reject the
  overlap after requiring an absolute path and applying POSIX normalization.
- Memory-store mount paths are Registry-derived beneath `/mnt/memory/`; clients
  cannot relocate them.
- Files can be attached/detached on a running session (`POST/DELETE /v1/sessions/{id}/resources`); memory_stores can only be attached at session creation (matches Anthropic; simpler invariant for the mount lifecycle).
- Workspace-scoping is strict: only resources in the same workspace as the session can be attached.

`harness-server` reads the `resources[]` of the session at start time,
materializes each into the sandbox via the right `MountStrategy`, and emits a
`session.resource_mounted` event for observability. Requested resource setup is
mandatory: if file, memory, output, or repository setup fails, the harness does
not start the turn against a partially prepared sandbox. It emits a public
`session.error` event (`setup_failed`, `retry_status.will_retry=false`) followed
by `session.status_idle` (`retries_exhausted`), cleans up partial sandbox/workdir
state, and leaves the session idle with no sandbox handle.

Harness repeats the reserved-root overlap check on the prepared execution
snapshot before any resource materialization. Immediately before every
resource write or mount it first requires every planned root to resolve to its
exact lexical path and rejects any pre-existing mount point at or below that
root. It also compares the sandbox-canonical target and Skill-root paths,
rejecting aliases through image-provided symlinks. After all resources are
active, Harness removes any image-preinstalled
`/workspace/skills` tree and writes the exact Session-pinned bundles as the
last trusted filesystem materialization, so sandbox image or resource contents
cannot impersonate a managed Skill. Production Linux runtimes then compare
device/inode identities for writable roots and both Skill-root ancestor chains;
this catches bind-mount and hard-link aliases that canonical path comparison
cannot reveal. They also reject nested mount points below every readable or
writable session root. Because current remote providers do not expose a sealed
filesystem namespace for setup, `colocated` accepts only the operator catalog
image; Environment-supplied custom images fail closed. All managed sandbox
executions reject Environment package installers until the same sealing
primitive is available.

Agent-facing file reads are restricted to the exact resource, output, and
Skill roots in the same sandbox policy. Resolved targets outside those roots,
including `/proc`, `/sys`, devices, and image filesystem paths, are denied.

This lifecycle applies to both harness topologies. In `separate`, the sandbox
is the tool executor; in `colocated`, the same sandbox also contains the
harness process. All resources are mounted before the in-sandbox HTTP session
is opened, and both modes use `SessionRunner.stop()` for watcher shutdown,
mount deactivation, work-dir cleanup, and sandbox destruction.

## Files (`{type: "file", file_id, mount_path, access: "read_only"}`)

**v1: pre-fetch + tar-extract.** At session start, harness calls `file-store.OpenContent` for each attached file, accumulates a tarball, ships it via E2B `sandbox.files.write` to the requested `mount_path`. Cold-start cost for typical file sizes (KB–MB) is sub-second per file. Read-only — no write-back path.

If/when we hit large blobs (>50 MB) routinely, any lazy-read replacement must be manifest-aware and authorize exact file IDs; bucket-prefix FUSE against `file-store`'s object backend is not an acceptable fallback, since it requires an object-store grant broad enough to traverse the workspace blob tree. See [`mount-strategies.md`](./mount-strategies.md).

## MemoryStore (`{type: "memory_store", memory_store_id, access, instructions}`)

**Live S3-FUSE mount, watched asynchronously.** `MemoryFuseStrategy` s3fs-mounts
the store's `live/` prefix straight into `/mnt/memory/<store_name>/` with
`use_cache=` — no disk cache, so reads go to object storage and a session sees
whatever is there now, not a snapshot taken at start. There is no pre-fetch step
on this path; `dispatcher.ts` seeds content by hand only for the non-S3 backends.

Writes are not intercepted. The agent writes through the mount, the bytes land in
S3, and `MemoryVersionWatcher` notices on its next poll (2 s by default) and
registers the version through the internal `memory-versions` route. That route is
last-writer-wins: on a conflicting write it still records the version and returns
`conflict: true`, and the watcher emits a `session.memory_conflict` transcript
event. No 409 reaches the agent and nothing retries on its behalf.

A synchronous libfuse daemon would close the poll-interval window; see
[`roadmap.md`](./roadmap.md) for the condition that would justify building one.

## GitHub repository (`{type: "github_repository", url, authorization_token, mount_path, checkout}`)

**PAT bytes never persist in the sandbox.** Repo materialization runs server-side in `harness-server`. In-sandbox `git push`, `git fetch`, and other auth-required HTTPS git operations work via a credential helper that round-trips to the registry per call.

**Spec match with Anthropic.** Fields per [`anthropics/skills` managed-agents-environments.md`](https://github.com/anthropics/skills/blob/main/skills/claude-api/shared/managed-agents-environments.md):

```
{
  type: "github_repository",
  url: "https://github.com/owner/repo",
  authorization_token: "ghp_…",             // write-only PAT or GitHub App token
  mount_path: "/workspace/repo",              // default: /workspace/<repo-name>
  checkout: { type: "branch", name: "main" }  // OR { type: "commit", sha: "<40-hex>" }
}
```

The Claude-compatible path accepts the raw token. Registry immediately writes
it to `SecretStore`, creates a resource-owned `git_credentials` row containing
only an opaque secret reference, and persists only the credential id in the
internal `session_resources.repo_ref`. Public create/get/list/update responses
never contain either `authorization_token` or `repo_ref`.

`authorization_token: "git_cred://<id>"` remains an Orca extension for binding
a pre-provisioned workspace credential. It is accepted for backward
compatibility but is never returned to clients. `access`, `clone_depth`, and
`sparse_paths` are not in Anthropic's shape.

**URL-binding rule.** Every internal repo credential is bound to
`resource.url`. A `git_cred://<id>` reference MUST already have the same
`repo_url`, else session create/attach/update returns `400`. Raw tokens create
a new credential already bound to that URL. The credential helper repeats the
host+protocol+path check at runtime. Repository URLs must use HTTPS and
include at least owner + repository path segments; host-wide or
organization-wide bindings are rejected. A session may have only one active
resource for a canonical repository URL (`.git` and trailing-slash variants
are equivalent), because the standard Git credential-helper request identifies
the remote URL but not the sandbox mount or session-resource id.

**v1 mount sequence:**

1. **Validate + materialize.** Registry accepts a non-empty raw token or the
   Orca `git_cred://<id>` extension. Before raw token bytes are staged in
   `SecretStore`, registry commits a durable staging intent containing only the
   generated credential id + opaque secret reference. The resource transaction
   atomically inserts credential metadata and consumes that intent. Failed DB
   writes claim + purge the staged secret; a periodic reconciler handles
   expired intents left by process crashes.
2. **Mint JWT.** At session-spawn, harness mints a session-scoped JWT (aud=`git-creds`, ttl 1h, re-mintable) carrying the session id + workspace id.
3. **Server-side clone.** Harness runs `git clone --filter=blob:none --depth=1 [--sparse=cone-paths]` against the upstream URL with the PAT in `Authorization: bearer …`. Clone happens in a per-session ephemeral working dir at `${HARNESS_WORK_DIR}/sessions/{ws}/{ses}/{repoIdx}/`.
4. **Stream into sandbox.** Harness streams the working tree + `.git/` into the sandbox at `mount_path` via `sandbox.files.write`.
5. **Inject helper env.** Harness injects `ORCA_GIT_CREDS_URL` (the registry's public `POST /v1/git-creds` endpoint) and `ORCA_GIT_CREDS_TOKEN` (the JWT) into the sandbox env. The image already registers `git config --system credential.helper /usr/local/bin/orca-git-creds` plus `credential.useHttpPath=true`; the latter is required so Git includes owner/repository path in the helper request and the registry can enforce repository-scoped credential binding. Without session env, the helper returns no credentials and Git remains anonymous.
6. **Agent runs git.** When the agent invokes `bash + git push` (or any git operation needing auth), the helper POSTs to `/v1/git-creds` with the JWT in `Authorization: Bearer`; the registry validates the JWT, looks up the matching `github_repository` `session_resources` row by host+protocol+path, resolves the repo credential PAT per-call via the configured secret provider/store, and returns `{username: 'x-access-token', password: <PAT>}` for the one operation. The PAT lives only in the helper's stdout for that single `git credential` exchange.
7. **Teardown.** Session ends → harness `rm -rf`s the work dir. The JWT expires; the PAT is never persisted at any layer of the sandbox.

There is no Orca-specific git tooling — agents trained on `bash + git push` work without server-side primitives. See [`mount-strategies.md`](./mount-strategies.md) for the credential-helper details.

**SSH-vs-HTTPS limitation.** The credential helper only catches HTTPS git operations. SSH remotes (`git@github.com:...`) bypass the helper and will fail to authenticate — no SSH agent is forwarded into the sandbox, and the PAT cannot be exchanged for an SSH key. **Customers must use HTTPS URLs** for the `github_repository` resource and any in-sandbox `git remote add`. Document this in customer-facing docs.

**Observability.** The host-side clone path emits `harness_git_clone_seconds` (Histogram, p95 SLO < 30 s for repos ≤ 100 MB) and `harness_git_clone_total{workspace_id, result}`; the registry-side credential-helper route emits `registry_git_creds_request_total{result}` (`ok` / `jwt_invalid` / `credential_mismatch` / `pat_unresolvable` / `repo_unmatched`). See [`services/harness-server.md`](./services/harness-server.md) and [`services/registry-service.md`](./services/registry-service.md) `/v1/git-creds`.

**Update semantics.** `POST /v1/sessions/{session_id}/resources/{resource_id}`
accepts a new write-only `authorization_token` for `github_repository`
resources. Registry stages the new secret, swaps the resource credential under
a row lock, then purges the replaced secret. The helper and future host-side
operations resolve the new token immediately; responses never echo it. There
is no automatic refresh. On session resume after pause, the working tree is
preserved and the helper continues to resolve credentials per operation.
Archiving a session archives its resource-owned credential metadata and purges
the token from SecretStore; deletion sets `deleted_at` on the metadata row and purges the secret.

**Why no separate `git-proxy` service for v1.** A standalone git-proxy (analogous to `ai-gateway` for MCP) was an option. We rejected it because: (a) the credential helper is a few hundred lines of stdin/stdout against an existing registry route — much smaller than a new service; (b) all server-side outbound git operations originate in `harness-server` already; (c) caching credentials on a sidecar would add a window where a PAT lives in the cluster outside the registry ↔ vault path. If a future use case demands inspecting/auditing arbitrary in-sandbox git smart-protocol traffic, a git-proxy becomes the right answer.

See [`mount-strategies.md`](./mount-strategies.md).

## Mount-time concurrency model (memory store specifically)

Two sessions in the same workspace attaching the same `memory_store` with `read_write`:

- Both read the same live prefix through s3fs, so each sees the other's writes
  as soon as they land — the poll interval governs when a _version is recorded_,
  not when the bytes become readable.
- Both can write; the bytes land in S3 directly and the watcher registers each
  version afterwards.
- Last writer wins per path. The loser's write is still recorded as a version and
  its session gets a `session.memory_conflict` event; no error is raised to the
  agent and nothing retries automatically.
- Every registered version is retained in `memory_versions`; nothing is lost.

What the poll interval does _not_ affect is readability: s3fs is mounted with
`use_cache=` so reads go to object storage, and a concurrent session's write is
visible as soon as it lands. The interval governs only how long a write waits to
be _registered_ as a version — so `memory_versions` and the transcript lag the
filesystem, not the other way round.

## Strategy decisions

Per-resource-type strategy choices, and the conditions under which each would
change, live in [`mount-strategies.md`](./mount-strategies.md).
