# Memory Conflict Semantics

> How concurrent memory writes are detected and surfaced. Companion to
> [`mount-strategies.md`](./mount-strategies.md).

## What this doc covers

`memory_store` writes are eventually consistent across sessions under v1's
async-watcher design — there is a ~2 s window between the moment one
session writes a path through its FUSE mount and the moment a different
session can observe the new bytes (or the registry has registered the new
version). This document explains the consistency model, the
`session.memory_conflict` event shape that surfaces concurrent writes to
SDK consumers, and the upgrade path to synchronous write-through if the
window proves too wide for production traffic.

## Why async, not sync

1. Synchronous write-through requires a custom libfuse driver (the open
   source FUSE primitives let us hook every write, but operating one
   reliably — graceful shutdown, error mapping, signal handling — is
   substantially more operational work than running `s3fs-fuse`).
2. The design reuses the same S3-FUSE infrastructure as output capture
   (`s3fs-fuse` + the `runPrivileged` helper) and bolts on a polling
   watcher — minimal new infrastructure, all of it contained in the
   harness process.
3. The trade-off is a ~2 s window where two concurrent sessions can race
   in S3. We accept last-writer-wins at the S3 layer and surface the race
   to SDK consumers as a `session.memory_conflict` event.
   [`roadmap.md`](./roadmap.md) records the conditions under which this would
   become a synchronous design.

## The consistency window

- The watcher polls every 2 s. The interval is fixed in
  `MemoryVersionWatcher` and is not operator-tunable.
- The window is bounded by the polling interval. p95 SLO target:
  **< 3 s** — the 2 s poll plus 1 s of registration latency
  (scoped internal `memory-versions` call → Postgres write).
- Tracked by `harness_memory_write_lag_seconds` (Histogram, no labels).
  Start = S3 `LastModified` header on the freshly observed object.
  End = `recordMemoryVersion` return.
- Within a single harness process, sessions sharing the same FUSE mount
  see writes immediately (`s3fs-fuse` caches with a TTL of ~5 s by
  default, but that is the s3fs layer, not our watcher). Cross-session
  visibility — when two sessions on different harness processes write the
  same store — is the watcher's job and follows the 2 s cadence.

## Conflict event shape

When the watcher observes a `current_sha256` advance whose
`previous_sha256` does not match its cached value for the path, it appends
a `session.memory_conflict` event to the transcript stream:

```json
{
  "id": "evt_…",
  "type": "session.memory_conflict",
  "produced_by": "harness",
  "produced_at": "2026-05-04T23:30:12.345Z",
  "content": {
    "store_id": "mems_…",
    "path": "user/preferences.json",
    "observed_sha256": "<the sha the watcher just saw>",
    "expected_sha256": "<the previous sha the watcher had cached>",
    "written_by_session_id": "ses_…"
  }
}
```

Notes:

- `expected_sha256` may be `null` if this is the first time the watcher
  has seen the path on this session (no cached value to compare against).
- `written_by_session_id` is the session whose write the watcher is
  registering — it identifies the **winner** of the race. The losing
  session's earlier write is now overwritten in S3; its prior version
  remains accessible under `.versions/{previous_sha256}`.
- The event is appended to the affected session's transcript stream, not
  the writer's. SDK consumers filter on the session id they care about.

## What the SDK should do

Filter the events stream for `type === 'session.memory_conflict'` and
surface the entries to client code. The client decides whether to:

- Re-read the path through `client.beta.memoryStores.get(...).download(...)`
  to fetch the winning version, then re-apply its intended mutation with
  the new `precondition_sha256`, or
- Surface the conflict to the end user (e.g. "another session updated
  this file") and let them resolve it.

The agent itself does NOT see conflict events mid-turn. Interrupting an
in-flight tool call to re-read memory would require the harness to inject
the conflict into the agent's context window mid-execution, which is out
of scope for v1. The tool-call-atomicity caveat: the agent saw a snapshot
of memory when its last `view_memory` (or equivalent) ran, and works from
that snapshot until the tool call returns.

## Customer-facing language

Verbatim two-line summary for customer-facing docs:

> Memory writes are eventually consistent across sessions (~2 s).
> Concurrent writes to the same path from two sessions follow
> last-writer-wins; the API exposes `session.memory_conflict` events so
> callers can detect and reconcile.
