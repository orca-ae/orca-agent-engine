# transcript-store (library)

> An _interface_ (TS module) for an append-only session/event log, with
> pluggable backends. `KafkaTranscriptStore` is the default (Apache Kafka or any
> Kafka-compatible backend); `PostgresTranscriptStore` supports local,
> test, and smaller self-hosted deployments; `PulsarTranscriptStore` supports
> Apache Pulsar clusters. `transcript-store` is **not** a deployable service —
> consumers, including the Kafka-only observability exporter, import the package and call the
> selected backend in-process.

The first principles below explain why a per-session topic with opaque-payload
events is the right shape; everything else is implementation detail of the
Kafka backend.

## First principles

A session is harness-specific. We do not build cross-harness session interop. The store is the simplest thing that works:

- **One ordered stream per session.** Kafka stores this as one topic per session
  (`orca.{workspace_id}.sessions.{session_id}.events` in raw mode, `.events-avro`
  in Avro mode). Pulsar stores the same
  logical stream as a persistent topic under `{tenant}/{namespace}`. Postgres
  stores it in `transcript_events`, keyed by `(workspace_id, session_id, seq)`.
  Subagents are differentiated by the `subpath` event field.
- **One selected source of truth.** A deployment chooses Kafka, Postgres, or Pulsar with
  `TRANSCRIPT_STORE_BACKEND`. There is no dual-write mirror in v1.
- **Each event = minimal common metadata + opaque payload bytes.** The harness owns payload semantics; everyone else passes bytes through.
- **Library, not service.** A TypeScript interface plus one or more backend implementations. Consumers import the package as a workspace dependency (`workspace:*`); registry-server, harness-server, and observability-exporter call it in-process. The exporter accepts only Kafka and uses the Kafka implementation for authoritative replay. Other Orca components can import the same library or read the selected backend directly.

## Public interface

The library exports a `TranscriptStore` interface plus Kafka, Postgres, and
Pulsar implementations. The shape mirrors what was originally drafted as a
`.proto` service definition; the same logical `Event` schema is used by every
backend:

```proto
// Logical shape (the package exports a TS interface; the Event schema is
// stored using the selected backend's wire format, not as a protobuf message).
service TranscriptStore {
  rpc Append (AppendRequest) returns (AppendResponse);
  rpc Read   (ReadRequest)   returns (stream Event);    // bounded
  rpc Tail   (TailRequest)   returns (stream Event);    // unbounded; live
  rpc Archive (ArchiveRequest) returns (Empty);          // session lifecycle
}

message Event {
  string  id            = 1;  // event identity/dedup key; producer controls stability
  string  workspace_id  = 2;
  string  session_id    = 3;
  string  subpath       = 4;  // optional; subagents
  int64   seq           = 5;  // backend cursor (Kafka offset, Postgres seq, or Pulsar message-ID-derived value)
  string  produced_at   = 6;  // RFC3339
  string  produced_by   = 7;  // "client" | "harness"
  string  kind          = 8;  // harness-defined; opaque to this service
  bytes   payload       = 9;  // verbatim wire bytes
  string  idempotency_key = 10;
  optional string user_id = 11;
}
```

**Subagent events:** `Event.subpath` differentiates
parent and subagent traces on the same topic. Empty string = parent. `Tail`
and `Read` accept `subpath="*"` to include all subagents, `subpath=""` for
parent-only, or an exact match to filter to one subagent path. The
`transcript_store_subagent_message_rate` counter increments on every event
with a non-empty subpath, labeled by `workspace_id` and `produced_by`.

`kind` is a free-form string. `payload` is bytes; the library does not parse,
validate, or transform them. Backend cursor round-trips as `seq` so callers can
resume from a cursor. `Append` dedupes within a session by `Event.id`; a retry
dedupes only if its producer preserves that same ID. The collision domain is
session-wide and does not include `subpath`, so one ID cannot be reused for a
root and child event.

Kafka and Pulsar use bounded in-process LRU dedup caches keyed by `Event.id`
(per Kafka workspace/session and per Pulsar topic), so their dedup window is
not durable across eviction or process restart. Postgres enforces the durable
unique key `(workspace_id, session_id, event_id)`. `idempotencyKey` is opaque
stored metadata in all backends, not the store dedup key. Compatibility events
with omitted or noncanonical envelope IDs can receive a new UUIDv7 on each
emission, so they have no replay-dedup guarantee.

`user_id` is optional client-user attribution metadata for Registry-derived
identity. Backends preserve a non-empty caller-supplied value but do not derive
or stamp it. Missing or empty transport metadata reads as absent.

## Layout

```
packages/transcript-store-types/
  src/
    store.ts                     # TranscriptStore interface (append / read / tail / archive)
    types.ts                     # Event shape
    index.ts                     # types-only barrel (zero runtime deps)

packages/transcript-store/
  src/
    index.ts                     # public re-exports (TranscriptStore, backend classes, Event types)
    store.ts                     # re-exports the interface from @orca/transcript-store-types
    types.ts                     # Event re-export + retryable / barrier handler errors
    route.ts                     # workspace/session route checks for appended and read events
    source-failure.ts            # safe error name/code allowlist for event-source failure logs
    kafka-store.ts               # KafkaTranscriptStore — Kafka-backed implementation
    kafka-event-source.ts        # KafkaSessionEventSource (topic discovery + consumer groups)
    postgres-store.ts            # PostgresTranscriptStore + PostgresSessionEventSource
    pulsar-store.ts              # PulsarTranscriptStore + PulsarSessionEventSource
    metrics.ts                   # prom-client counters/histograms (incl. subagent rate)
    kafka/
      client.ts                  # singleton Kafka client
      producer.ts                # idempotent producer + per-session LRU dedup
      consumer.ts                # cursor-based consumer (powers read/tail)
      topic.ts                   # topic name builder + cursor parser
      serialize.ts               # raw encoding: Event ↔ Kafka message
      codec.ts                   # raw/Avro codec selection, encode + decode
      codec-error.ts             # sanitized codec error codes
      codec-metrics.ts           # schema cache, Registry latency and decode-failure metrics
      config.ts                  # Kafka transcript encoding + Schema Registry env parsing
      schema-registry.ts         # Schema Registry client (lookup, registration, bounded cache)
      schemas/transcript-event.ts # inline Avro record orca.transcript.TranscriptEvent
      with-heartbeat.ts          # consumer heartbeat during bounded Registry I/O
      dedup.ts                   # LRU cache
  test/
    unit/                        # topic, serialize, dedup unit tests
    integration/                 # append-read, tail, cursor, dedup, archive, metrics specs
  package.json                   # name: "@orca/transcript-store"
  tsconfig.json
```

The package has no runtime entry point of its own — it exports types and classes for in-process use by `services/registry-service-ts/`, `services/harness-server/`, and `services/observability-exporter/`. There is no Dockerfile, no `/healthz` server, and no gRPC stubs.

The interface and `Event` shape live in the separate `@orca/transcript-store-types` package so that types-only consumers (e.g. the session-runner, whose self-hosted boot must not drag in a broker client — the rationale stated in the package's own barrel) never pull a Kafka/Postgres/Pulsar client into their runtime closure; `@orca/transcript-store` re-exports them, so existing import paths and the wire `Event` shape are unchanged.

Pure library. No replicas. The consuming service's lifecycle owns the Kafka
client, Postgres pool, or Pulsar client.

## Kafka topology

- **Per-session topic** (single partition). Topic name: `orca.{workspace_id}.sessions.{session_id}.events` in raw mode, or `orca.{workspace_id}.sessions.{session_id}.events-avro` in Avro mode, optionally prefixed (see _Topic prefix_ below). Encoding selects one topic set for all sessions, not a per-session format.
- **Topic creation.** Auto-created on first produce in dev (Apache Kafka KRaft). Elsewhere, topic creation follows the broker's configuration.
- **Read / Tail.** Each call uses a kafkajs consumer with a fresh throwaway `groupId` per call (no group commit). `seek()` to `from_cursor` then poll. `Tail` stays open until canceled or a terminal error occurs.
  When an unbounded tail's subscription encounters `UNKNOWN_TOPIC_OR_PARTITION`, it retries three
  times after 250, 500, and 1000 ms to tolerate the topic-creation window. These waits are cancelable
  and retain the original start offset, including offset 0 for a missing topic; they do not resample
  the head and skip records written during startup. Exhaustion fails the tail instead of returning
  normal EOF. Bounded reads still end normally for unknown topics; other subscription errors are
  not retried by this startup loop.
  `KafkaTranscriptStore.read` additionally accepts Kafka-only `signal` and `maxBytes` controls. The byte
  cap uses KafkaJS `KafkaMessage.size` when present and otherwise sums record-batch key/value/header
  bytes. It admits one first raw record when that record alone exceeds the cap and reports the next
  scanned cursor even for poison input. Shared `TranscriptStore`, Postgres, and Pulsar read semantics
  do not change.
- **Consumer groups.** `harness-server` uses one Kafka **consumer group** so multiple replicas distribute lifecycle sentinels plus client-produced `user.*` events with at-least-once delivery and automatic redelivery on replica death. It periodically lists session topics and makes one explicit canonical topic-list subscription; a replacement records active topics only after KafkaJS group join, and never uses regex subscription expansion. `registry-service` SSE handlers use the throwaway-`groupId` + `seek()` pattern (Kafka Reader pattern).
- **Source status.** `KafkaSessionEventSource.status()` reports `ready` after its admin connection is available, `running` during recoverable discovery retries, `failed` only when the outer run loop escapes, and `stopped` after shutdown. It exposes no backend error details.
  A per-topic consumer crash removes that consumer so discovery recreates it; KafkaJS internal restart
  is disabled so discovery remains the single lifecycle owner. A callback that observes shutdown fails
  instead of returning success, so Kafka cannot acknowledge an event that never reached the handler.
  Discovery awaits every consumer startup before propagating a startup failure. `stop()` waits for
  pending startups and their connection cleanup, including when another topic's startup has failed.
- **Tiered storage** is the broker operator's configuration. Applications consume via standard `subscribe + seek + poll` semantics, so when the broker offloads cold segments to object storage, `Read` and `Tail` callers don't see the boundary.
- **Audit topics** (for other services) follow the same naming convention: `orca.{workspace_id}.audit.<service>`.

### Topic prefix (Kafka-on-Pulsar)

`KafkaTranscriptStore`, `KafkaSessionEventSource`, and the harness dispatcher accept an optional `topicPrefix` (env: `KAFKA_TOPIC_PREFIX`, default `''` = bare names). When set it must be dot-terminated segments matching `([A-Za-z0-9_-]+\.)+`, e.g. `public.default.`. This exists for Kafka-on-Pulsar (KoP) endpoints, which interpret dotted Kafka topic names as `<tenant>.<namespace>.<local-topic>` — producing to the bare `orca.{ws}...` name fails because no `orca` tenant exists, while `public.default.orca.{ws}...` works. One asymmetry to be aware of: for the default tenant/namespace, KoP's `admin.listTopics()` returns the BARE local name (the `public.default.` prefix is stripped from listings), so dispatcher discovery matches both bare and prefixed listings via `matchSessionTopic(name, prefix)` and always subscribes to the canonical prefixed name. Both prefixed and unprefixed dispatcher paths use that one admin snapshot as an explicit topic list; harness-server's Kafka configuration requires a positive `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS` (default `30000`) so topics created after startup are discovered. The observability exporter reads the same variable for its own discovery interval (default `1000`, at most `60000`). Registry's read-model `KafkaSessionEventSource` does not read it and rediscovers topics every second.

Topic builders, parsers and discovery match only the selected encoding. An admin
listing containing both topic sets yields only the selected set for subscription,
never a merged stream. The mode comes from the codec's `encoding`, configured by
`KAFKA_TRANSCRIPT_ENCODING` in services; there is no configurable topic suffix.
`topicPrefix` remains before `orca`, for example
`public.default.orca.{workspace_id}.sessions.{session_id}.events-avro`.

### Kafka client security knobs

`registry-service-ts` and `harness-server` construct their own KafkaJS clients before passing them into `KafkaTranscriptStore` / `KafkaSessionEventSource`. Both services accept the same env surface:

- `KAFKA_CONNECTION_MODE=plaintext` (default) keeps local-stack and CI on unauthenticated Kafka.
- `KAFKA_CONNECTION_MODE=sasl-plain-token-tls` uses `ssl: true` and SASL/PLAIN with `username=KAFKA_SASL_USERNAME || 'public'` and `password='token:' + KAFKA_AUTH_TOKEN` for endpoints requiring token-prefixed passwords, such as Kafka-on-Pulsar (KoP).
- `KAFKA_CONNECTION_MODE=sasl-plain-tls` uses `ssl: true` and SASL/PLAIN with `KAFKA_SASL_USERNAME` plus `KAFKA_AUTH_TOKEN` for any Kafka-compatible backend that takes SASL/PLAIN over TLS.
- `KAFKA_CONNECTION_MODE=custom` exposes KafkaJS plain SASL/TLS knobs: `KAFKA_SSL`, `KAFKA_SASL_MECHANISM=plain`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SSL_REJECT_UNAUTHORIZED`, `KAFKA_SSL_CA_FILE`, `KAFKA_SSL_CERT_FILE`, and `KAFKA_SSL_KEY_FILE`.

The observability exporter accepts the same authentication modes. Its broker-native
runtime separately reads `KAFKA_TOPIC_LISTING_MODE=canonical|bare-alias` (default
`canonical`). Enable `bare-alias` only when the broker lists canonical prefixed topics
under bare local names, as KoP does; authentication never enables aliases implicitly.
This setting does not change registry/harness discovery or the legacy Postgres exporter.

The old `kop-token` connection mode is rejected without an alias. Existing deployments
use `sasl-plain-token-tls` instead and explicitly set `KAFKA_TOPIC_LISTING_MODE=bare-alias`
in the broker-native exporter when relying on KoP listings. See the
[exporter migration example](../../../services/observability-exporter/README.md#kafka-connection-configuration-migration).

### Optional Kafka Avro envelope

Raw Kafka values remain the default: payload bytes in the value, Event metadata in
headers, and the UTF-8 event ID as key. Avro mode encodes the **full Event envelope**
using the inline record `orca.transcript.TranscriptEvent`:

- String fields: `id`, `workspace_id`, `session_id`, `subpath`, `produced_at`,
  `produced_by`, `kind`, `idempotency_key`. Empty subpath and idempotency key
  have empty-string defaults; the timestamp keeps its original RFC3339 string.
- `payload` is Avro `bytes`, preserved without JSON parsing or reserialization,
  including binary/non-UTF-8 bytes and empty archive payloads.
- `user_id` is nullable string (default null); absent/empty attribution normalizes
  to null. `seq` is not encoded: the Kafka offset remains the cursor authority.

The value is Confluent payload-prefix framing: zero magic byte, four-byte big-endian
schema ID, then Avro binary record. It is not an Avro container file. The raw key and
existing headers remain, plus `orca_transcript_encoding=avro-v1`. Orca readers dispatch
by this header, not by guessing from the first value byte: unmarked records are raw;
unknown/duplicate markers or invalid Avro fail rather than falling back to raw.
Kafka relays must preserve headers. Readers validate duplicated key/header/envelope
metadata against the topic route before dispatch, projection or archive side effects.
Schema lookup/decode failures do not advance the failed record's cursor or commit it;
the existing poison-route skip policy remains distinct from schema failures.
`KafkaSessionEventSource` applies a per-topic cooldown after decode failures: 1, 2, 4,
8, 16, then 30 seconds, capped at 30 seconds for subsequent failures. These are
minimum delays; the next normal discovery scan after expiry recreates the consumer.
Only successful decode clears the topic's retry history, not connection or group join.
Readiness remains false while any topic has an unresolved decode failure, but other
and newly discovered topics continue consuming. Heartbeat and ownership failures do
not incur codec cooldown. Shutdown cancels discovery immediately; cooldowns create
no per-topic timers.

Registry, harness dispatcher and both exporter state backends share this decoding
contract. Kafka-native exporter resolves schemas before entering its Kafka transaction;
exhausted Registry retries stop the instance and fail health, requiring supervisor
restart from its durable checkpoint after the cause is fixed. Checkpoint/delivery
records and the external ai-gateway audit sink retain their own formats.

#### Configuration and security

All three services accept the same [environment table](../../../services/registry-service-ts/README.md#kafka-transcript-avro-and-schema-registry).
`KAFKA_TRANSCRIPT_ENCODING=raw` plus `KAFKA_SCHEMA_REGISTRY_URL` enables Avro frame
decoding but still reads and writes only the raw `.events` topic set. `avro` requires
that URL and selects only `.events-avro` for both reads and writes. Legacy frame
decoding is codec compatibility, not cross-topic history access or normal mixed-format
routing. Without a URL, omit all explicit
Registry companion settings, even their default values. Postgres/Pulsar transcript
backends reject enabled Kafka encoding settings.

The default subject is the fixed record name `orca.transcript.TranscriptEvent`,
reused across session topics (RecordNameStrategy semantics), independent of topic
names, their encoding suffix and framed schema IDs. A deployment-level override
separates environments; switching topic sets does not change the subject.
Avro writers prepare before readiness: auto-register registers the exact shipped schema;
with auto-register false, exact-schema lookup requires preregistration. Neither uses
latest as the writer schema or changes Registry compatibility settings. Raw writers
and read-only exporters do not register; readers fetch writer schemas by wire ID and
resolve them against the inline reader envelope. Schema lookup is cached and bounded.

Platform operators manage subject compatibility (recommended `FULL_TRANSITIVE`) and
retain schema IDs for the entire transcript retention/replay window. Changing Registry
URL requires preserving historical ID mappings or a data migration. Broker value
schema-ID validation must match the subject strategy; the raw key is not an Avro key,
and enforced Avro-only value validation is incompatible with raw writers.

Registry authentication never reuses broker SASL implicitly. Basic credentials and
mTLS require HTTPS; TLS certificate validation is always enabled, with no bypass knob.
Local unauthenticated HTTP is allowed. URL userinfo, query strings and fragments are
rejected. Registry PEM paths are service-container paths, mounted explicitly and read-only
using Helm extra volumes, not forwarded to workers, session-runners or sandboxes.
A Schema Registry fronting Kafka-on-Pulsar (KoP) takes Basic authentication with a raw
JWT password, not broker SASL's `token:<jwt>`; a nonempty username such as `public`
accompanies it. Endpoints and credentials come from the broker operator. These examples
are configuration guidance, not a runtime compatibility certification.

#### Coordinated incompatible cutover and rollback

Switching encoding is an explicitly incompatible, deployment-wide cutover for all
sessions. There is no per-session format selection, automatic history migration,
or continuity guarantee for existing numeric cursors. The API/SSE envelope is
unchanged, but offsets belong to distinct topic streams.

1. Configure Registry access, schema retention and subject compatibility independently
   of broker credentials. Check isolated Avro canaries; raw startup, even with a
   Registry URL, does not register a schema or prove Registry connectivity.
2. Stop new work and quiesce existing turns before switching. Coordinate registry,
   harness and exporter workers, including pending delivery and stored session state.
   Set the same `KAFKA_TRANSCRIPT_ENCODING` on all three services and perform a full
   coordinated restart before resuming traffic. There is no hot reload; a reader-first
   rolling deployment is not a safe live cutover.
3. Avro mode selects `.events-avro` for every session. Unmigrated history remains in
   `.events` and is not read. Existing session state, SDK replay, public projections
   and stored numeric cursors cannot be assumed correct against the new stream.
   Create fresh sessions and replay only the new stream. The switch does not
   automatically reset the central registry database.
4. The broker-native exporter selects `.checkpoints-avro` and `.delivery-avro`
   internal state topics and an Avro-suffixed consumer-group namespace. This isolates
   old checkpoints and pending delivery; their JSON record formats are unchanged.
   The SQL-state exporter also selects the Avro transcript set, but its Postgres
   progress is session-keyed: use a fresh dedicated exporter-state database or an
   operator-verified reset before restarting. There is no automatic SQL migration
   or truncation; do not reset the shared registry database as a substitute.
5. Selecting `raw` again returns to the old topic/state set, not a seamless rollback.
   New Avro history is not read; old pending work can become active again. Quiesce
   and coordinate workers and stored state before reverting, and retain Registry
   access and historical schema IDs for Avro replay. External consumers subscribed
   to `.events-avro` do not follow raw writes.

#### External Kafka consumers

External consumers decode the schema-ID prefix and Avro envelope before interpreting
`payload`. Configure the consumer's subject selection to match Orca's
RecordNameStrategy subject (default `orca.transcript.TranscriptEvent`, or the
configured override), not the topic-name default `<topic>-value`. Resolve each
record's writer schema by its framed schema ID; do not assume the latest registered
schema is the schema that encoded every record.

The decoded `payload` remains opaque bytes. Applications can explicitly decode valid
UTF-8 and then parse JSON for event kinds whose business contract is JSON. Binary,
invalid UTF-8, invalid JSON and empty archive payloads are not universally convertible
to JSON. Preserve the original bytes and report conversion failures rather than
silently discarding events or treating a null conversion as lossless decoding.
Avro `bytes` does not imply a universal SQL column type or cast. SQL-engine-specific
connectors, type mappings and error handling are outside this interoperability contract.

Avro-only external consumers subscribe only to `.events-avro`, excluding the old
raw `.events` topics. Orca does not merge the sets or convert their history. Mixed
raw/Avro codec unit fixtures prove frame-decoding compatibility, not normal routing.
Starting at latest or filtering decoded records does not convert historical raw
values or establish complete replay. Downstream failure, retry and offset-commit policies are consumer-specific;
Orca's no-commit guarantee does not automatically apply to external consumers.

Preserve byte-identical payloads, event IDs, Kafka offsets, keys and routing headers.
Consumers requiring Orca's identity guarantees must validate topic route, key,
headers and envelope together. A subject or workspace column is not an authorization
boundary; broker and Registry ACLs remain independently configured. Topic subscription
and cross-session aggregation are consumer-owned, not implied by the Avro envelope.

## Postgres topology

- **Tables.** `transcript_events` stores append-only event rows. The generated
  `seq` is the cursor. `transcript_event_claims` stores harness consumption
  state per `(group_id, event_seq)`.
- **Read / Tail.** `Read` drains rows up to the high watermark captured at call
  time. `Tail` polls for rows with `seq >= cursor` until canceled. Both order by
  the underlying bigint sequence, including across digit and page boundaries.
- **Harness consumption.** `PostgresSessionEventSource` claims
  client `user.*` events and `session.archived` / `session.deleted` sentinels
  using a lease. Within each consumer group it claims only the earliest
  unprocessed eligible row per workspace/session, so a later interrupt cannot
  overtake an unfinished outcome on another worker. `includeAllEvents` applies
  the same ordering to all event kinds for read-model consumers.
  Active claims are renewed during handling and persistence repair. Ownership
  is checked before dispatch, each repair attempt, and completion; expired or
  reassigned batch entries are skipped. `SessionEventBarrierError` retries its
  captured callback before the batch advances, with the poll interval as its
  abortable backoff. Poll and repair waits remove their abort listener when the
  timer expires or cancellation occurs. Shutdown or loss of the claim stops
  further retries and leaves unfinished work unprocessed. Successful handler completion marks the
  currently owned claim processed. Other failures leave it available for retry
  after lease expiry; later rows in that session stay blocked. This claim-level
  `processed_at` is delivery bookkeeping and is unrelated to the public session
  event `processed_at`, which comes from the durable harness acceptance marker.
  Its `status()` is `running` across ordinary claim retries, `stopped` after an
  intentional stop, and `failed` only after an escaped run-loop rejection; the
  status contains no database error text. `whenFailed()` resolves only for that
  escaped terminal rejection, never for intentional stop; its terminal log
  carries only a safe error name and explicitly allowlisted stable code.
- **Scope.** Postgres is intended for local, test, and smaller self-hosted
  deployments. Kafka remains the default for high-throughput streaming and long
  retention.

## Pulsar topology

`PulsarTranscriptStore` owns one native client per store, including when the
first reads or appends arrive concurrently. The client stays referenced until
the store closes its producers and client.

The pinned `pulsar-client@1.17.0` has a pnpm 9 patch for native Reader error
handling: a failed read rejects without taking ownership of its output pointer.
Local installs use pnpm 9, matching CI and Docker: later pnpm majors use an
incompatible patch hash and cannot consume this frozen lockfile.
Installation builds the patched binding from source; it does not download the
upstream Node prebuilt, which contains the invalid-free bug. The build uses
SHA-512-pinned Apache C++ client 4.1.0 archives and static linking, so runtime
images need no separate `libpulsar` installation. Archive downloads have a five-minute
deadline per attempt (including the response body) and retry network failures, HTTP
408/429 and 5xx responses up to three attempts, with one- and two-second backoffs.
Other HTTP errors and checksum mismatches fail the install without retrying or
falling back to the unsafe upstream Node prebuilt. Archive extraction commands retain
a five-minute timeout; native compilation has no subprocess timeout. Release images
build on native amd64 and arm64 runners without QEMU. CI job timeouts bound the overall
build (90 minutes for release images); local builds rely on the caller for cancellation.
Supported build targets are Linux (glibc or musl) and macOS, on x64 or arm64. Local installs require a C++17
toolchain, Python 3 and make, plus `ar`/`tar`/`xz` on glibc Linux, `tar` on musl
Linux, or `unzip` and Xcode Command Line Tools on macOS. Service Docker build
stages install these tools; runtime stages do not. Unit tests compile the actual
installed Reader worker against a fault double, and Pulsar integration tests
exercise the loaded native binding's timeout and closed-reader paths in a child
process.

- **Per-session persistent topic.** Topic name:
  `persistent://{tenant}/{namespace}/orca.{workspace_id}.sessions.{session_id}.events`.
  `PULSAR_TENANT`, `PULSAR_NAMESPACE`, and `PULSAR_TOPIC_PREFIX` select the
  namespace and logical prefix.
- **Message metadata.** Event metadata is stored in Pulsar message properties;
  payload bytes stay opaque in the message body. The Node client is the official
  `pulsar-client` package.
- **Append visibility.** The producer queues every new event from one `Append`
  call into a single flushed Pulsar batch. Pulsar stores that batch as one unit
  and consumers expand it back into ordered per-event messages, so a consumer
  cannot observe the first event in a successful public event batch before its
  immediately following companion event. Events use the session ID as their
  partition key so the guarantee also holds for partitioned topics. The
  configured 1,000-message / 2 MiB batch limits cover the public API's
  100-event / 1 MiB request limits; registry hashes each batch `request_id`
  once before copying its bounded digest into each event's Pulsar properties
  to prevent per-message amplification. This is visibility atomicity, not a
  Pulsar transaction or an all-or-nothing write guarantee after producer
  failures.
- **Auth.** `PULSAR_AUTH_TYPE=token` uses `PULSAR_AUTH_TOKEN`.
  `PULSAR_AUTH_TYPE=oauth2` uses the official client's OAuth2 auth with
  `PULSAR_OAUTH2_ISSUER_URL`, optional `PULSAR_OAUTH2_CLIENT_ID`,
  `PULSAR_OAUTH2_CLIENT_SECRET`, `PULSAR_OAUTH2_PRIVATE_KEY`,
  `PULSAR_OAUTH2_AUDIENCE`, `PULSAR_OAUTH2_SCOPE`, and
  `PULSAR_OAUTH2_TYPE` (default `client_credentials`). Registry and harness
  both read the same variables because both connect to Pulsar directly.
- **Read / Tail.** `Read` uses a Pulsar reader from earliest and derives numeric
  `Event.seq` values from message IDs. Uncapped reads drain the available backlog
  until the broker-backed `hasNext()` check reports no further messages, instead
  of using publish timestamps, so broker/app clock skew cannot truncate reads.
  Once backlog is confirmed, each native read has a 5-second default deadline
  (`readTimeoutMs` overrides it); the idle end-of-transcript check does not wait
  for that deadline. A timeout or other reader error rejects the scan, including after a yielded
  prefix; recovery retries without treating unseen event IDs as absent.
  `Tail` uses a temporary subscription: from-now
  streams start at `Latest`; resumed streams start at `Earliest` and skip to the
  numeric cursor. Resuming a large topic scans earlier messages.
- **Temporary subscription cleanup.** `Tail` unsubscribes its temporary
  subscription on normal shutdown. If the process or broker dies first, Pulsar
  can retain that server-side subscription. Operators should configure inactive
  subscription expiry for the namespace (Apache Pulsar documents
  `subscriptionExpirationTimeMinutes` / namespace subscription-expiration
  settings) or run an equivalent periodic cleanup for `orca-tail-*`
  subscriptions.
- **Harness consumption.** `PulsarSessionEventSource` uses a `KeyShared`
  subscription over a topic regex, with `AutoSplit` and out-of-order delivery
  disabled. The producer uses the session ID as `partitionKey` and key-based
  batching. Broker ownership keeps the session's events on one consumer,
  including while another replica joins, so the local persistence barrier also
  prevents cross-replica overtaking. Messages are acknowledged after the runner
  accepts them or the unstarted outcome is durable.
  Most failed handler calls are negatively acknowledged for broker redelivery.
  `SessionEventBarrierError` instead retains the current delivery and retries its
  captured persistence callback within a serial workspace/session queue.
  Later deliveries for that session stay unacknowledged in the queue while
  unrelated sessions on the same consumer use the available dispatch capacity.
  By default the source runs at most 8 handler/repair calls concurrently and
  retains at most 100 active or queued deliveries. The constructor options
  `maxConcurrentHandlers` and `maxPendingDeliveries` override these positive
  integer limits. At the pending-delivery limit, the source pauses `receive()`
  until a delivery settles; further backlog stays in Pulsar. Repair backoff
  retains its pending-delivery slot but releases handler capacity for other
  sessions.
  This prevents later events from overtaking a partially committed outcome.
  The retry wait uses `nAckRedeliverTimeoutMs`; shutdown cancels it without
  acknowledging unfinished work. Shutdown also wakes capacity waiters, skips
  queued dispatches and waits for active handlers before closing the consumer; the broker can then
  redeliver unfinished work. Idle subscription refresh waits for all local
  queues to drain so it cannot transfer ownership during a repair.
  Permanent handler bugs are dropped after the poison-message cap, while
  `RetryableSessionEventError` bypasses that cap so acceptance/deferral storage
  outages cannot permanently discard a client event. Its `status()` remains
  `running` while subscribe retries continue, is `stopped` after an intentional
  stop, and becomes `failed` after an escaped run-loop rejection; it never
  exposes a Pulsar error message. `whenFailed()` resolves only for that
  terminal rejection, never for intentional stop; its terminal log carries
  only a safe error name and explicitly allowlisted stable code.

## Per-harness session adapter (lives in `harness-server`)

Each `AgentHarness` brings an adapter (a thin wrapper around the `TranscriptStore` interface) that satisfies its SDK's native session contract. v1 ships only `ClaudeAgentSdkAdapter`:

- Implements Claude SDK's `SessionStore` (`append`, `load`, optional `listSessions` / `delete` / `listSubkeys`).
- `append(SessionKey, SessionStoreEntry[])` — for each entry build an internal `Event` with `id ← entry.uuid` (or UUIDv7 fallback), `kind ← 'harness.claude.session_entry'`, `payload ← { type: 'harness.claude.session_entry', sdk_entry: entry }`, `subpath ← SessionKey.subpath`. Call `TranscriptStore.append`. A retry dedups only when it retains the same `entry.uuid`/`Event.id`.
- `load(SessionKey)` — `TranscriptStore.read`; ignore non-internal public transcript events, unwrap `sdk_entry` from internal entries, and return `null` if empty (SDK contract).
- `listSubkeys(SessionKey)` — `TranscriptStore.read(subpath="*")`; return the
  sorted set of non-empty event subpaths so Claude SDK multi-agent session
  threads can be discovered and replayed.
- Public harness output events such as `agent.message` and `session.resource_mounted` are appended separately without the SDK replay envelope, so they can be streamed/listed for clients without polluting SDK replay state.
- Pin to `projectKeyForDirectory()` from the SDK; point `CLAUDE_CONFIG_DIR` at tmpfs in the sandbox (local-disk JSONL is staging-only).
- A hand-written SessionStore suite (10 tests, `services/harness-server/test/integration/session-adapter.spec.ts`, modeled on the SDK's `runSessionStoreConformance`) runs against this adapter in the CI integration job.

A non-Claude harness ships its own adapter against the same `TranscriptStore` interface.

## Idempotency

`Append` dedupes by `(workspace_id, session_id, Event.id)`, independent of
`subpath`. The Claude SDK retries failed batches; the same `entry.uuid` becomes
the same `Event.id`, and re-appends are no-ops only while the backend retains
that ID: durably through Postgres's unique constraint, or in Kafka/Pulsar only
while their process-local LRU has not evicted it or restarted. `idempotencyKey`
is opaque event metadata, not an additional store dedup mechanism.
Application-level idempotency lives at Registry's
`POST /v1/sessions/{id}/events` endpoint, where a client `Idempotency-Key` is
honored before the event is composed and appended.

## What this library does not own

Kafka cluster operation, retention policy, and tiered storage are the broker
operator's configuration, on Apache Kafka or any Kafka-compatible backend — this
library consumes the Kafka protocol as a black box. Postgres retention and table
maintenance are a deployment/runbook concern; v1 does not implement automatic
pruning.
