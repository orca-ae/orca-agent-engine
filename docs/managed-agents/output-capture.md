# Output Capture

> How agents publish artifacts back to the SDK without an explicit
> upload tool. Companion to [`mount-strategies.md`](./mount-strategies.md) and
> [`resource-mounting.md`](./resource-mounting.md). The enforcement
> boundary is specified in [`output-write-policy.md`](./output-write-policy.md).

## What

Every output-capture-enabled session has a writable directory at
`/mnt/session/outputs/`. Agents are instructed to place user-deliverable files
there; anything written there is auto-registered as a `File` row in the
registry with:

- `purpose = "agent_output"`
- `scope_id = ses_…` (the originating session id)
- `downloadable = true`

The SDK retrieves these through the standard Files API:

```ts
const files = await client.beta.files.list({ scope_id: session.id });
const blob = await client.beta.files.download(files[0].id);
```

Orca also exposes the same records through the Session-nested extension
`GET /v1/sessions/{id}/files` and its metadata, content, and delete subroutes.
The nested form derives the scope from the path and rejects files not owned by
that Session.

The shape mirrors Anthropic's contract: the `scope_id` filter on
`files.list` and the `downloadable` gate on `files.download` are the same
fields that appear in their Files API today.

## Write-policy status

The capture path indexes only `/mnt/session/outputs/`, and the implemented
[`output-write-policy.md`](./output-write-policy.md) makes that path the only
writable non-resource location. `separate` mode retains explicitly read-write
MemoryStore and Git repository mounts; the current `colocated` branch does not
mount session resources. The prompt remains a usability hint, while runtime
policy installation is the security boundary and fails closed.

## Lifecycle

1. **Session boot.** `Dispatcher.spawnRunner` selects the output path by
   harness topology:
   - **`separate`:** mint scoped per-session S3 credentials via
     `SessionCredsMinter` (STS `AssumeRole` in production; static-key dev
     fallback when `S3_STS_ROLE_ARN` is unset), then mount the session output
     prefix:
     - FUSE-capable runtime (E2B with `orca-default`, or OpenSandbox with
       gVisor-provided `/dev/fuse`) →
       `s3fs-fuse` mounts a fresh
       `s3://<bucket>/<outputs_prefix><workspace>/<session>/<runner_generation>/`
       staging prefix read-write at `/mnt/session/outputs/`.
       Mount startup uses a transient mode-0600 AWS profile and a scrubbed
       daemon environment; STS credentials do not persist in process argv or
       `/proc/<pid>/environ`.
     - Non-FUSE runtime (AgentENV, Local, or `InMemorySandboxRuntime`) → a
       sandbox-FS `mkdir -p /mnt/session/outputs/` through its Files API.
   - **`colocated`:** use the same directory, which the sandbox-harness image
     creates at build time and assigns to its non-root runtime user. On a
     FUSE-capable OpenSandbox deployment, harness-server mounts that directory
     before releasing the image's ready marker. Non-FUSE test runtimes index it
     locally after tool completion and on shutdown. The image also
     defaults Claude Code to `acceptEdits` so its non-interactive `Write`/`Edit`
     tools can create requested artifacts within its `/mnt/session` working
     directory without granting blanket command execution; operators can
     override `SANDBOX_HARNESS_DEFAULT_PERMISSION_MODE` and
     `SANDBOX_HARNESS_DEFAULT_CWD`.
2. **Agent runs.** The agent's `write` and `bash` tools land bytes at
   `/mnt/session/outputs/<filename>` (and arbitrary nested subdirectories).
   The FUSE mount keeps a single source of truth in S3; a non-FUSE runtime
   keeps it in the sandbox filesystem until the indexer uploads it.
3. **Runner-scoped staging.** Every runner assignment gets its own output
   generation. Repeated scans within that runner share a relative-path +
   SHA-256 cache, but a later runner starts from an independent staging
   prefix. File identity remains the generated File `id`: if the later runner
   writes the same filename (even with the same bytes), it is registered as a
   new File.
4. **Immediate registration.** After every `agent.tool_result` or
   `agent.mcp_tool_result` has been appended to the transcript, the runner
   asynchronously scans the output directory and registers new or changed
   files. Scans are serialized but do not block later `agent.message` or
   `session.status_idle` events. They are
   de-duplicated by relative output path and content SHA-256, so an unchanged
   artifact is not registered again after a later tool call or at shutdown.
5. **Session stop.** `SessionRunner.stop()` first stops the harness and drains
   its event pump, then invokes the
   `OutputIndexer.indexSession(...)` closure built by `Dispatcher.spawnRunner`
   one final time **before** deactivating mounts. The FUSE mount stays alive
   while the indexer paginates `ListObjectsV2` over the session prefix; the
   local path (InMemory and `colocated`) walks
   `sandbox.files.list('/mnt/session/outputs')` recursively.
6. **Per-blob registration.** For each new or changed artifact, the indexer
   reads the
   bytes back and posts them to Registry's
   `/internal/v1/workspaces/{workspace}/sessions/{session}/files` route.
   `LocalFileStore.create` writes a `files` row with
   `purpose='agent_output'`, `scope_id=ses_…`, `downloadable=true`. The 500
   MB per-object cap (matching the public `/v1/files` contract) is enforced
   here; oversized blobs are skipped and logged. One
   `session.output_indexed` event is appended to the transcript store per
   registered file.
   `session.output_indexed` is the readiness signal that the returned file id
   can be downloaded; a tool-result event only signals that a scan was
   scheduled. If the File row succeeds but the readiness event append fails,
   the runner retains that exact event id and retries it without creating a
   second File.
7. **SDK retrieval.** `client.beta.files.list({ scope_id: session.id })`
   returns the registered `agent_output` rows; `client.beta.files.download`
   passes the `downloadable=true` gate and streams bytes through the
   registry's `GET /v1/files/:id/content` endpoint.

## Retention semantics

`agent_output` files share the file-store lifecycle: they are kept until an
explicit `DELETE /v1/files/:id` (or workspace archival). Per-workspace
retention overrides are not implemented; see [`roadmap.md`](./roadmap.md).

The `scope_id` link to the originating session is permanent — even after the
session is archived, the files stay scoped to it for SDK retrieval. There is
no "cascade delete on session archive" path; SDK consumers expect a session
id that returned outputs yesterday to keep returning them today.

## `scope_id` contract

Only `ses_…` ids are meaningful. The public route does not validate the scope
at all; the internal-only route derives `scope_id` from its validated session
path, which is the one place the prefix is guaranteed.
Other scope prefixes (`mst_…` for memory-store attachments, `agnt_…` for
agent-pinned reference files) are reserved in the id space but **not enforced**:
the `idString('ses')` constraint lives in the ts-rest contract, which does not
register the Fastify routes. `GET /v1/files?scope_id=mst_…` returns `200` with an
empty page rather than a validation error, and `POST /v1/files` never reads
`scope_id` at all — it hard-codes `null`. See [`roadmap.md`](./roadmap.md).

## Indexer SLO

- Metric: `harness_output_index_lag_seconds` (Histogram,
  `trigger=tool_result|shutdown`).
- Target: **p95 < 5 s** from a tool-result or shutdown trigger to indexer
  return. Alert on `trigger=tool_result` for the live-capture SLO; use
  `trigger=shutdown` to track the final safety-net pass.
- Tool-result scans trade one prefix listing per local tool completion for
  immediate visibility; unchanged files are filtered before registry upload.
  [`roadmap.md`](./roadmap.md) records the S3-event-notification alternative,
  which swaps in behind the same `OutputIndexer` interface, and the SLO
  condition that would justify it.

## Local-indexing caveat

`AgentEnvRuntime`, `InMemorySandboxRuntime`, and `LocalSandboxRuntime` have no
FUSE. Their indexer walks `sandbox.files.list('/mnt/session/outputs')`
recursively and posts each file via the scoped internal files route.

Continuous S3 write-through is used by both harness topologies when the
acquired runtime advertises verified FUSE support.

## Limitations / out of scope

- **Writes that outlive a tool result.** A background process can continue
  writing after its `bash` tool call returns; its final bytes are caught by the
  shutdown scan. Agents should write user-facing artifacts synchronously when
  immediate download visibility matters.
- **Exactly-once across ambiguous failures.** Within a live runner, successful
  registrations are cached so later scans do not duplicate them. If the
  registry commits a File row but its response is lost, a retry can still
  create another row because internal file registration is not an atomic
  idempotency operation. Closing that narrow window requires a registry-side
  idempotency key.
- **Staging-prefix garbage collection.** Runner-generation S3 prefixes are
  staging storage, separate from the registry-managed File blob. A generation's
  prefix is not deleted after a successful final scan.

Alternatives considered and not built — the `mountpoint-s3` FUSE driver and
bucket event notifications — are in [`roadmap.md`](./roadmap.md) with the
conditions that would justify them.
