# Mount Strategies

> How each resource type is materialized into the sandbox: files, memory stores,
> and GitHub repositories, plus the snapshot lifecycle every strategy honors.
> Git credential helper routes and headers are Orca extensions; see [`orca-extensions.md`](./orca-extensions.md).
> See [`memory-conflict-semantics.md`](./memory-conflict-semantics.md) for the memory
> consistency model. The sandbox-wide write boundary, including the read-write MemoryStore and
> repository exceptions, is specified in
> [`output-write-policy.md`](./output-write-policy.md). Strategies that do not exist yet, and the
> conditions that would justify building them, are in [`roadmap.md`](./roadmap.md).

## File mounts

Every `file` resource uses
`TarballPrefetchStrategy`. Harness opens only the exact files authorized by the
prepared execution snapshot and materializes them host-side. Execution STS
credentials contain no file-object permission, so a sandbox cannot list or
guess other files in the workspace.

The earlier `S3FuseStrategy` prototype required an object-store grant broad
enough to traverse a content-addressed blob tree and is therefore not selected
by the dispatcher. A future lazy reader must be manifest-aware and authorize
exact file IDs; bucket-prefix FUSE is not an acceptable fallback.

## Override knob (per-resource `mount_strategy`)

`session_resources.mount_strategy` accepts only `tarball_prefetch`; omitting it
selects the same default. SDK clients may state it explicitly:

```ts
await client.beta.sessions.create({
  ...,
  resources: [
    {
      type: 'file',
      file_id: 'file_…',
      mount_path: '/mnt/data/big.parquet',
      mount_strategy: 'tarball_prefetch',
    },
  ],
});
```

The `MountStrategy` interface boundary remains so a future exact-file lazy
reader can be introduced without changing session resource lifecycle.

## Metrics

- `harness_fuse_mount_total{strategy,result}` (Counter) — mount attempts, with
  `strategy` the resolved strategy name and `result` ∈ {`ok`, `error`}. This is
  the only mount metric; there is no per-file size histogram and no
  lifecycle-phase counter.
- `harness_fuse_mount_total{strategy,result}` (Counter) — file-resource
  materialization attempts; `strategy` = `tarball_prefetch` (the resolved
  strategy name), `result` ∈ {`ok`, `error`}. The name predates the removal
  of the file FUSE surface and is kept for dashboard continuity.

## Snapshot-aware lifecycle

The `MountStrategy` interface defines the ordering any sandbox snapshot must
honor: `teardownForSnapshot(handle)` on every active mount before
`SandboxRuntime.pause()`, and `restoreAfterSnapshot(state)` before the next tool
dispatch on resume.

**Nothing invokes it today.** Sessions are torn down on idle rather than paused
(see [`harness-modes.md`](./harness-modes.md)), so the only exercise of this
ordering is the `sandbox-snapshot.spec.ts` integration test. The contract is
specified and enforced ahead of a warm-pause path that
[`roadmap.md`](./roadmap.md) records as not built.

For `TarballPrefetchStrategy`, both hooks are no-ops — the bytes live in the
sandbox FS, which E2B persists across pause/resume. The hooks exist so a future
stateful strategy (a custom write-through cache, a libfuse daemon needing
graceful shutdown) has a contract for cleanup.

## Lifecycle ordering

```
activate              -> on session start
{ tool calls }
teardownForSnapshot   -> before sandbox.pause()
sandbox.pause()
sandbox.resume()
restoreAfterSnapshot  -> before next tool dispatch
{ tool calls }
deactivate            -> on session stop / resource detach
```

## Integration test invariant

`sandbox-snapshot.spec.ts` is an integration test that:

1. Activates a mount.
2. Calls `teardownForSnapshot` on each handle.
3. Calls `sandbox.pause()`.
4. Calls `sandbox.resume()`.
5. Calls `restoreAfterSnapshot` on each torn-down handle.
6. Asserts the mounted file is readable both pre-pause and post-resume.
7. Asserts each strategy method was invoked the expected number of times.

Future MountStrategy implementations MUST keep this test green.

## Memory-store mounts

`MemoryFuseStrategy` is the default for FUSE-capable runtimes;
`LocalMemoryStrategy` is the Files API fallback for AgentENV, Local, and
`InMemorySandboxRuntime`.
Writes are picked up by an async polling watcher rather than synchronous
write-through — see [`roadmap.md`](./roadmap.md) for the trigger that would
change that. See
[`libraries/memory-store.md`](./libraries/memory-store.md) for the v1
library design (S3-FUSE live state + sha-keyed version archive) and
[`memory-conflict-semantics.md`](./memory-conflict-semantics.md) for the
consistency model and the upgrade path.

### `MemoryFuseStrategy` (v1 default for FUSE-capable runtimes)

Mounts each attached `memory_store` at Registry's output-only
`/mnt/memory/{store_name}/` path via `s3fs-fuse` read-write (or read-only for a
`read_only` attachment). Clients cannot supply another memory mount root, and
the privileged strategy rejects prepared paths outside `/mnt/memory/`. Bytes live at
`s3://{bucket}/{root}workspaces/{workspace_id}/memory-stores/{store_id}/live/{path}`;
past versions live at
`s3://{bucket}/{root}workspaces/{workspace_id}/memory-stores/{store_id}/versions/{sha256}`
by the version watcher (see below). The FUSE mount **is** the live state —
agent reads and writes hit S3 directly through the same scoped STS
credentials minted by `SessionCredsMinter` for that exact store. File inputs
receive no S3 grant. No separate
write-through interceptor sits in the harness path.

Mount startup creates a mode-0600 temporary AWS profile at
`/root/.aws/credentials`, starts `s3fs` under `env -i`, then truncates and
deletes the profile. Session credentials therefore remain absent from the
long-lived daemon argv and `/proc/<pid>/environ`.

### `LocalMemoryStrategy` (non-FUSE fallback)

Backs the mount with the runtime filesystem at `/mnt/memory/{store_name}/`.
The watcher walks `sandbox.files.list` instead of `ListObjectsV2` to detect
changes. AgentENV enforces the normal Bubblewrap write policy around this path;
Local enforces its host-OS sandbox policy. `InMemorySandboxRuntime` has no
isolation boundary and remains test-only.

### Async version watcher

A per-session `MemoryVersionWatcher` polls each attached store's S3 prefix
every 2 s. The interval is fixed in `MemoryVersionWatcher` and is not
operator-tunable. On
each tick it lists live keys, computes the SHA-256 of any path whose
`LastModified` advanced since the previous tick, and calls
the workspace/session/store-scoped internal `memory-versions` route to register the new version. The
registry-side route applies the optimistic `previousSha256` precondition
and updates `memories.current_sha256` + appends to `memory_versions`.

Concurrent writes to the same path from two sessions follow last-writer-wins
in S3. When the watcher observes a `current_sha256` advance whose
`previous_sha256` does not match its cached value for that path, it appends
a `session.memory_conflict` event to the transcript stream so SDK
consumers can reconcile. The conflict event shape is documented in
[`memory-conflict-semantics.md`](./memory-conflict-semantics.md).

### Customer-visible caveat

Agents may briefly read stale state if a concurrent session wrote in the
same path within the last poll cycle. The window is bounded by
the 2 s poll interval plus registration latency on top.
Customer-facing docs document this verbatim:

> Memory writes are eventually consistent across sessions (~2 s).
> Concurrent writes to the same path from two sessions follow
> last-writer-wins; the API exposes `session.memory_conflict` events so
> callers can detect and reconcile.

See [`memory-conflict-semantics.md`](./memory-conflict-semantics.md) for
the SDK-side handling pattern and the upgrade path to synchronous
write-through.

### Snapshot-lifecycle compatibility

Both memory strategies' `teardownForSnapshot` and `restoreAfterSnapshot`
hooks are no-ops in v1:

- `MemoryFuseStrategy` — E2B preserves its in-sandbox `s3fs` process across
  pause/resume. OpenSandbox snapshot resume recreates the Pod and cannot
  preserve mount namespaces, so `OpenSandboxHandle.pause()` fails closed while
  FUSE is enabled until explicit teardown/remount lifecycle exists. The watcher
  poll loop therefore never resumes against an silently-unmounted local path.
- `LocalMemoryStrategy` — the bytes live in the sandbox tmpdir, which
  persists for the lifetime of the sandbox.

The hooks remain in the `MountStrategy` interface so future stateful
strategies — a custom libfuse daemon that needs graceful shutdown of
in-flight writes, for example — have a contract for cleanup.

### Metrics

- `harness_memory_write_lag_seconds` (Histogram, no labels) — observed lag
  from S3 `LastModified` to `recordMemoryVersion` return. Tracks the
  consistency window. SLO target: **p95 < 3 s** (allows 1 s of registration
  latency on top of the 2 s poll interval).
- `harness_memory_watcher_poll_total{result}` (Counter) — poll cycles, `result`
  ∈ {`ok`, `error`}.
- `harness_memory_versions_recorded_total{workspace_id,result}` (Counter) —
  versions the watcher registered, `result` ∈ {`ok`, `conflict`, `error`}. The
  `conflict` label is what counts a `session.memory_conflict`; there is no
  separate conflict counter.

## GitHub repository mounts

A `TarballPrefetchStrategy`-shaped mount driven by
server-side `git clone --filter=blob:none --depth=1 [--sparse]` into a
per-session host work dir, then stream the working tree (and `.git/`) into
the sandbox FS via `sandbox.files.write`. No S3-FUSE for repos in v1.

Cold-start cost is linear in working-tree size; fine for repos up to a few
hundred MB. The bytes live in the sandbox FS, which E2B persists across
pause/resume — so the lifecycle hooks (`teardownForSnapshot`,
`restoreAfterSnapshot`) are no-ops, identical to the file-mount
`TarballPrefetchStrategy` shape.

A partial-clone-on-read FUSE backend would trade that cold-start cost for
per-read latency; [`roadmap.md`](./roadmap.md) records the conditions under
which it becomes worth building.

### Cache (v1)

An ephemeral host-side work dir at
`${HARNESS_WORK_DIR}/sessions/{ws}/{ses}/repo-{repoIdx}/`, removed on session stop.
There is no durable `(repo_url, sha)` cache, so repeated session-spawns of the
same repo re-clone.

### Override knob

There is no override knob for a repository today. `mount_strategy` is rejected
on any resource whose `type` is not `file`, before the allowed-value check runs,
so `tarball_prefetch` and `git_fuse` are both refused on a `github_repository` —
the field is only meaningful for `file` resources.

### Metrics

- `harness_git_clone_seconds` (Histogram, no labels) — wall-clock cost of the
  whole `GitCloneStrategy.activate()` flow (clone + walk + sandbox stream). SLO
  target in the metric's own help text: **p95 < 30 s** for repos ≤ 100 MB.
- `harness_git_clone_total{workspace_id,result}` (Counter) — `result` ∈
  {`ok`, `error`}.

## In-sandbox `git push`

In-sandbox `bash + git push` works via a
custom credential helper baked into the E2B template. The original v1
resolution ("disable in-sandbox push, expose only the `orca.git_push`
skill") is **dropped from v1 scope** to match Anthropic's published
contract — agents trained on `bash + git push` work without an Orca-specific
skill.

### Mechanism

A binary at `/usr/local/bin/orca-git-creds` is baked into the
`orca-default` E2B template. At session-spawn the harness:

1. Mints a session-scoped JWT (`aud=git-creds`, re-mintable). There is no per-audience TTL: every mint uses `SESSION_JWT_TTL_SECS`, which defaults to 300 seconds and ships as `300` in the chart. The
   JWT carries the session id + workspace id; the registry signs it with
   the same key it uses for other internal JWTs.
2. Injects two env vars into the sandbox: `ORCA_GIT_CREDS_URL` (the
   registry's public `POST /v1/git-creds` endpoint) and
   `ORCA_GIT_CREDS_TOKEN` (the JWT).
3. The sandbox image has already registered
   `git config --system credential.helper /usr/local/bin/orca-git-creds` and
   `credential.useHttpPath=true` at build time. Without the injected env pair,
   the helper returns no credentials and Git falls back to anonymous access.

When `git` invokes the helper for any HTTPS git operation, the helper:

1. Reads the host+protocol+path from stdin (standard
   `git credential` protocol).
2. POSTs to `ORCA_GIT_CREDS_URL` with `Authorization: Bearer
${ORCA_GIT_CREDS_TOKEN}` and the repo URL bytes.
3. Registry validates the JWT, looks up the matching `github_repository`
   `session_resources` row by host+protocol+path, resolves the
   resource-bound PAT via the configured `SecretProvider`/`SecretStore`, and returns
   `{username: 'x-access-token', password: <PAT>}` for that one operation.
4. Helper writes the credentials to stdout for the single
   `git credential` exchange and exits.

### Customer-visible behavior

Agents trained on `bash + git push`, `git fetch`, `git pull`, etc. work
unchanged. There are no Orca-specific git skills; the credential helper is the
whole mechanism.

### PAT-handling invariant

The PAT bytes are **never persisted in the sandbox** — no file, no env
var, no shell history. Only the short-lived JWT lives in the sandbox env.
The credential helper holds the PAT in stdout for the single
`git credential` exchange and exits. Each `git` operation that needs auth
re-roundtrips to the registry; the registry re-resolves the PAT from
the repo credential per call.

### URL-binding rule

`git_credentials.repo_url` MUST match the `resource.url` on the
`github_repository` resource at session-create, attach, and token-rotation
time, else the request fails with 400. Raw tokens create a resource-owned
credential with that URL; `git_cred://<id>` references are checked against the
pre-provisioned row. The helper looks up the resource by
host+protocol+path, so a mismatched repo credential would resolve no
PAT (or the wrong one) at credential-exchange time.

### SSH-vs-HTTPS limitation

The credential helper only catches HTTPS git operations. SSH remotes
(`git@github.com:...`) bypass the helper and will fail to authenticate —
no SSH agent is forwarded into the sandbox, and the PAT cannot be
exchanged for an SSH key. **Customers must use HTTPS URLs** for the
`github_repository` resource and any in-sandbox `git remote add`. This is
documented in the customer-facing Resource Mounting page.

### Caching trigger

The credential helper does not cache PATs — every git operation that needs auth
round-trips to the registry. The only counter for this path is the registry-side
`registry_git_creds_request_total{result}`; there is no harness-side metric and no
latency histogram.

### Metrics

- `registry_git_creds_request_total{result}` (Counter, registry-side) — `result`
  ∈ {`ok`, `jwt_invalid`, `credential_mismatch`, `pat_unresolvable`,
  `repo_unmatched`}. One increment per request. There is no harness-side
  counter and no latency histogram for this path.
