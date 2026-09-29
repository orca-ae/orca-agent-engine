# OIP-014: Exporter state and startup recovery

- *Author(s)*: @freeznet
- *Status*: Released
- *Proposal time*: 2026-09-14
- *Components*: observability-exporter, Helm chart
- *Discussion*: None (predates the public repository)
- *Implementation*: `services/observability-exporter/src/` (`kafka-state-v2.ts`, `kafka-blob.ts`,
  `kafka-state-io.ts`, `kafka-disk-index.ts`, `kafka-shared-state.ts`, `startup-admission.ts`,
  `kafka-runtime.ts`, `broker-main.ts`, `config.ts`), `Dockerfile`; chart `validation.yaml`,
  `deployment-observability-exporter.yaml` and `configmap-observability-exporter.yaml`
- *Released in*: v0.5.0

## TL;DR

The first Kafka state kept each Session's reducer and full replay ledger in one checkpoint value
capped at 512 KiB, rescanned the shared checkpoint log per assignment and joined consumers serially,
so long Sessions stopped committing and recovery grew with history and topics. Version-2 state keeps
a small head, a separate-key ledger, chunked large values, one scratch index per process and bounded
concurrency; v1 checkpoints import atomically. It affects exporter operators, not the `/v1` API.

## Background

[OIP-012](OIP-012-agent-observability.md) describes the exporter, which commits reducer state,
sampled delivery records and a single-partition Session topic's offset in one Kafka transaction over
`orca.observability.v1.checkpoints` (one partition, compacted) and `orca.observability.v1.delivery`.
Replay is exact: a repeated event identity with the same envelope hash is skipped, with another hash
it fails the instance, and no source is accepted twice. Owning documents: the exporter
[README](../services/observability-exporter/README.md#state-v1-to-v2-upgrade-and-recovery) and [`kubernetes.md`](../docs/managed-agents/kubernetes.md).

## Motivation

- **State grew with history.** v1 kept one value per Session, `{route, nextOffset, reducer,
  identities[], acceptedSourceIds[], deliveryContext}`, capped at 512 KiB and rewritten whole by
  every batch. Each unique event's identity digest, hash and first offset (about 165 bytes) is kept
  forever, even under a terminal `null` pin, so a Session of a few thousand events hit a commit it
  could not make: the instance failed closed, and a restart replayed into the same limit. Delivery
  records shared the cap, below a legal trace at the projector's limits (about 1.8 MB).
- **Every restore rescanned the log**: a fresh consumer read the whole shared checkpoint topic up to
  the assignment's fencing barrier, refusing past 64 MiB or 100,000 records.
- **Joins were serial**, at startup and per discovery pass, so readiness grew linearly with topics.

## Goals

### In scope

- A history-independent head and batch-sized ledger work, legal large values in 512 KiB records,
  one log scan per process, barrier-authenticated restores, bounded admission, unchanged readiness.
- Automatic, atomic import of every persisted v1 checkpoint at its offset, with no new topic or ACL.

### Out of scope

- Bounded total storage (exact replay keeps state per unique event; evicting it would project
  replays twice), and any change to consumer topology, readiness or the legacy Postgres backend.
- Listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#agent-observability): evidence at 1,000
  topics and one million identities, multi-topic groups, warm index reuse, scratch reclamation,
  cleanup of acknowledged delivery and chunks, and larger v1 imports.

## Design

### High-level design

```text
joins (8) ─► one consumer and group per Session ─► first batch of an assignment: restore (2)
  stable transactional ID ─ barrier:<key> + nonce ─► checkpoints (1 partition, compact)
  one shared reader per process ─ read_committed ─► scratch SQLite index ─► snapshot at barrier
every batch: projection (2) ─ point-read membership ─► pure reducer ─► one Kafka transaction:
  delivery ◄─ inline record or manifest   checkpoints ◄─ I/ A/ rows, R/ D/ chunks, head last
  source group ◄─ offset = head.nextOffset
```

### Detailed design

**State model** (`kafkaStateKeys`, `kafka-state-v2.ts`). Kafka stays the only authority. `S` is the
SHA-256 of the Session's checkpoint key (its source group ID); ledger keys digest the event ID.

| Key | Value | Lifetime |
|---|---|---|
| checkpoint key | head: `version: 2`, route, `nextOffset`, pin or `null`, `identityCount`, `acceptedCount`, reducer blob, optional `importBaseline` | rewritten last in each commit; at most 512 KiB |
| `I/S/<digest>` | identity digest, envelope hash, first offset, `visibleOffset`, `imported` | once per unique event |
| `A/S/<digest>` | bounded accepted-source ID, `visibleOffset`, `imported` | once per accepted source |
| `R/S/state/:<n>` | reducer chunks, when too large to inline in the head | slots overwritten; a shrunk tail is unreferenced |
| `R/S/import/:<n>` | the imported v1 value, verbatim, when too large to inline | once |
| `D/S/<sha256>:<n>` | delivery chunks, addressed by content digest | immutable |

**Membership prefetch.** `projectKafkaState` point-reads only a batch's candidates (identity keys
and bounded IDs, each acceptance marker's `user_event_id`, pending inputs), 256 per read, each
checking the owner's head in the same SQLite transaction, and hands them to the unchanged pure
projector as its ledger. Identity rows are visible from the offset after their event, acceptance
rows from their commit's `nextOffset`; a row visible past the head's is a mixed view and fails.
History lives only in ledger rows; the reducer, bounded by per-turn limits, is rewritten per commit.

**Blob codec** (`kafka-blob.ts`). Reducer state, import evidence and large deliveries share one
codec: up to 64 KiB inline, else a manifest `{kind: chunks, keyPrefix, count, bytes, sha256}` plus
canonical-base64 chunks of at most 48,000 raw bytes, each record within 512 KiB with its key and
framing, at most 65,536 chunks and 32 MiB decoded by default. Decoding bounds sizes before
allocating and checks the SHA-256 before parsing. Delivery chunks are content-addressed, so a queued
manifest always resolves to the bytes it was written with; reducer slots are overwritten.

**Commit** (`commitProjection`, `kafka-runtime.ts`). One transaction sends delivery records (inline
up to 64 KiB, else a `version: 2` manifest), then ledger rows, reducer and delivery chunks and last
the head, then the source offset, in Produce requests of at most 512 KiB and 500 records under a
64 MiB budget, and commits once. A budget failure before the transaction halves the batch; one
transition over budget, a fencing error or an ambiguous commit fails the instance, never retried.
The owner then waits for the shared reader to apply the head and reloads its state from the index.

**Shared index** (`kafka-shared-state.ts`, `kafka-disk-index.ts`). The first restore starts one
`read_committed` reader per process (fresh `orca-exporter-restore-<UUID>` group, no auto-commit)
that applies the checkpoint topic from the beginning to a `better-sqlite3` database on a worker
thread in a new `mkdtemp` directory, never adopting a cursor or earlier file. Upserts are
offset-fenced and per-scope `I/` and `A/` counts change in the same SQLite transaction, so
cardinality is one lookup. The 1 GiB quota keeps about a third for pages; running out fails the
instance, and the fix is more disk, never deleting ledger keys.

**Restore order.** The first batch of a new assignment generation, holding a restore permit:

1. commits `barrier:<checkpoint key>` with a random nonce under the Session's stable transactional
   ID, which fences any previous owner, and waits for the reader to snapshot the scope at exactly
   that record; no other nonce, offset hole or watermark counts;
2. reads the head: absent requires no ledger rows; `version: 1` builds the import; `version: 2`
   loads the reducer and checks ledger cardinality against the head (no digest grows with history)
   and the stored v1 evidence against `importBaseline`, row by row (`validateKafkaState`);
3. requires source low watermark ≤ `nextOffset` ≤ high watermark, and any group offset ≤ it;
4. commits the import, or for a new Session an initial head with the Registry pin or `null`, and
   only then publishes the owned state. Delivery partitions restore their progress the same way.

**Manifests that overtake chunks.** A delivery consumer can read a committed manifest before the
reader applies its chunks; the delivery owner then commits its own barrier and re-reads. Chunks
still missing, or a digest, trace, route or pin mismatch, fail the instance with no HTTP send or
offset progress (`decodeKafkaDelivery`). Manifest deliveries share one process-wide permit.

**Admission and shutdown** (`startup-admission.ts`, `broker-main.ts`). FIFO pools admit joins
(connect, subscribe, run) 8 at a time, restores 2 at a time across sources and delivery partitions,
and projections 2 at a time; the first failure stops admission and started work drains. Queued
restores keep heartbeating, re-check ownership when admitted and hold no transaction; SIGTERM
cancels queued joins at once. Readiness is unchanged (first discovery pass joined, runtime running,
Admin listing succeeds) and does not wait for restores. Topic listing retries metadata-availability
errors with 1 to 30 s backoff for up to 15 minutes, staying NotReady until the first discovery.

**Compaction and failure.** Heads, reducer slots and barriers compact to their latest value, all a
restore reads; ledger rows and delivery chunks have unique keys. Assignment loss releases the
producer and rejoins; any other failure stops the instance, with no per-Session quarantine.

## Changes by component

- **observability-exporter**: the modules above replace the per-restore `kafka-checkpoint-log.ts`;
  the image creates the scratch parent for UID 1000.
- **Helm chart**: Kafka mode uses `Recreate` and a disk-backed `emptyDir` at
  `/var/run/orca/exporter-state`, sized by `observabilityExporter.kafkaStateSizeLimit` (`2Gi`); the
  ConfigMap sets that directory and a 1 GiB quota, and validation reserves the volume and path.

## Public-facing changes

### Storage

No API, event or wire change. The checkpoint topic gains the key families above beside unchanged
barrier and delivery-progress keys; the delivery topic can carry `version: 2` manifests beside v1
inline records. Topics, policies, groups, transactional IDs and ACLs are unchanged.

### Configuration

Read by the exporter in Kafka mode only (`config.ts`); record and inline limits are fixed.

| Variable | Default | Effect |
|---|---|---|
| `OBSERVABILITY_KAFKA_STATE_DIRECTORY` | OS temporary directory; image and chart `/var/run/orca/exporter-state` | absolute scratch parent |
| `OBSERVABILITY_KAFKA_STATE_MAX_BYTES` | 1 GiB (128 KiB–1 TiB) | index database plus journal quota |
| `OBSERVABILITY_KAFKA_STATE_CATCHUP_TIMEOUT_MS` | `120000` (at most `600000`) | barrier and catch-up deadline |
| `OBSERVABILITY_KAFKA_MAX_ASSEMBLY_BYTES` | 32 MiB (at most 64 MiB) | decoded bytes per blob |
| `OBSERVABILITY_KAFKA_MAX_TRANSACTION_BYTES` | 64 MiB (at most 128 MiB) | serialized bytes per transaction |
| `OBSERVABILITY_KAFKA_STARTUP_CONCURRENCY` | `8` (1–32) | concurrent joins |
| `OBSERVABILITY_KAFKA_RESTORE_CONCURRENCY` | `2` (1–32) | concurrent restores |
| `OBSERVABILITY_KAFKA_PROJECTOR_CONCURRENCY` | `2` (1–32) | concurrent projections |

### Metrics, logs and traces

No Prometheus endpoint. A fixed-schema JSON summary at start, stop and every 60 seconds reports each
admission pool, the reader's scanned bytes and index size against its quota, consumer and producer
counts, transaction and assembly peaks, and shutdown drain; budget errors name only kind and bytes.

## Compatibility

### Upgrade

The import is automatic: v1 capped every checkpoint write at 512 KiB with no override, so every
persisted v1 checkpoint fits (`migrateKafkaCheckpoint`). One transaction writes the ledger rows
(first offsets kept, visible from the v1 `nextOffset`, as v1 kept no acceptance position), the v1
value as evidence, the reducer and the v2 head over the same key, and re-commits the same source
offset, with no context resolution or delivery; pin, suppression and delivery backlog are untouched.
An abort leaves v1 visible; after an uncertain commit the instance fails and the next owner reads
whichever version its barrier shows. Run one generation per topic prefix; the chart uses `Recreate`.

### Rollback

Safe until a v2 head commits; after that a v1-only release fails closed, as its parsers reject
`version: 2` heads and manifests. Recover forward with a v2-capable image and the same prefix,
encoding, groups and transactional IDs; resetting groups or deleting topics is not recovery.

## Security considerations

The index holds derived state, including captured `raw_io` content, on pod-local disk in a
per-process directory deleted on close and never adopted after an abrupt exit. A manifest must name
a checkpoint key in the runtime's namespace, chunks under that scope and digest, and a trace that
matches the Session's route and pin, so no record delivers one Session's content under another pin.
No credential, network path or ACL is added.

## Testing

- **Unit suites**: `kafka-state-v2.spec.ts` (equality with the v1 projector at every batch split,
  import authentication, future and conflicting rows) and the `kafka-blob`, `kafka-disk-index`,
  `kafka-shared-state`, `kafka-state-io`, `startup-admission`, `kafka-runtime`, `broker-main` specs.
- **Real broker**, in the required `integration` job: `kafka-v2.spec.ts` imports a suppressed v1
  checkpoint just under 512 KiB at its offset and projects past where v1 would have failed, and
  covers cold rebuild, reload after both groups are deleted, an aborted transaction, a manifest
  ahead of its chunks and damaged checkpoints. The chart's render suite checks volume and strategy.
- **Ablation.** Before implementation, each simplification of a fuller design ran alone on synthetic
  projector fixtures, SQLite models and a local broker; a negative control then broke its guard.

## Alternatives

- **A general membership store.** Prefetched candidates matched a full 100,000-ID set in 155
  differential cases; dropping acceptance-reference prefetch or ID normalization broke it.
- **An LRU with a negative cache.** On 100,000 IDs and 10,000 lookups both hit alike (p95 per 100
  lookups 0.09 against 0.35 ms, worst 0.54 against 0.37 ms); the negative cache hid a later insert.
- **Warm index reuse with a persisted cursor.** A replay model with eight crash points rebuilt to
  the same state; a cursor ahead of applied data lost the latest value. The cost: a scan per start.
- **An MVCC view manager.** A bounded read checking the head and returning a detached copy matched
  or retried safely in every modeled interleaving; without first-offset filtering, future rows leak.
- **A new state topic with an activation pointer.** A real broker gave the same logical state as
  keys in the existing topic, whose `version: 2` head is the guard v1 readers refuse.
- **Paging per collection.** One codec passed the same fixtures (350 assertions), but chunking
  stays: legal state reaches 1,140,465 bytes with 1,024 pending inputs and 10,150,835 with escaped
  content at every limit, and raising every broker, fetch and admission limit was not validated.
- **A multi-transaction import with epochs and a seal.** One transaction passed the same eight
  failure checks on seven fixtures; the largest import, 53,260 records and 9,685,160 bytes, took 109
  Produce requests without raising the 1,048,588-byte record-batch limit.
- **Multi-topic groups with a custom assignor.** Bounded joins alone cut join time for 24 empty
  topics from 130.8 to 24.6 ms and for 48 from 203.6 to 55.2 ms on a local broker.
- **Admitting only joins.** A model sent 24 first batches into restore at once; a join slot held
  through an empty topic's first restore stalled discovery after eight joins.

## Status notes

Checked against current code, the implementation differs from the ablated design in four ways: it
ships 8 joins and 2 restores where the design left admission open, adding projection admission and
one permit for manifest deliveries; it keeps the verbatim v1 value as import evidence, so each
restore re-authenticates imported rows; disk exhaustion stops the instance instead of backpressure;
and listing retry landed later. Open evidence is on the roadmap: 1,000 topics and one million
identities in 1 CPU and 1 GiB, kill and network faults at commit, forced compaction, Linux amd64.
