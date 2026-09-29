# OIP-001: Pluggable transcript store backends

- *Author(s)*: @sijie, @freeznet, @jiangpengcheng
- *Status*: Released
- *Proposal time*: 2026-05-01
- *Components*: `packages/transcript-store`, `packages/transcript-store-types`, registry-service-ts,
  harness-server, observability-exporter (reads Kafka), session-runner (contract only), Helm chart
- *Discussion*: None (predates the public repository)
- *Implementation*: `packages/transcript-store-types/src/`; `packages/transcript-store/src/`
  (`kafka-store.ts`, `kafka-event-source.ts`, `kafka/`, `postgres-store.ts`, `pulsar-store.ts`,
  `route.ts`); `src/main.ts`, `src/config.ts` and `src/kafka-transcript-bootstrap.ts` in
  registry-service-ts and harness-server, harness-server `src/runner/dispatcher.ts`,
  observability-exporter `src/config.ts`; `services/proto/transcript_store.proto`; the chart's
  `transcriptStore` values and `templates/{_kafka-transcript.tpl,validation-kafka-transcript.yaml}`
- *Released in*: v0.5.0

## TL;DR

Registry, harness-server and the exporter share no event RPC, so they need one durable, ordered,
append-only log per session that any replica can append to, read, follow and resume from a cursor.
`@orca/transcript-store` provides it in-process: one `TranscriptStore` interface with Kafka-protocol
(the default, Kafka-on-Pulsar (KoP) included), Postgres and Pulsar backends and an optional Schema
Registry Avro envelope on Kafka. Operators select one with `TRANSCRIPT_STORE_BACKEND`; external
readers of session topics are affected, the `/v1` API is not.

## Background

The owning documents are [`libraries/transcript-store.md`](../docs/managed-agents/libraries/transcript-store.md),
[`architecture.md`](../docs/managed-agents/architecture.md#interaction-model-registry-vs-harness), [`data-model.md`](../docs/managed-agents/data-model.md#transcript-store),
the [Avro Helm guide](../docs/operation/kafka-transcript-avro.md) and [Transcript Backend](../docs/managed-agents/kubernetes.md#transcript-backend).
SDK adapters are in [OIP-002](OIP-002-agent-harnesses-and-execution-modes.md), the `colocated`
bridge that makes Registry the single writer of runner events in [OIP-011](OIP-011-self-hosted-session-runner.md),
and user attribution and the exporter in [OIP-012](OIP-012-agent-observability.md) and [OIP-014](OIP-014-exporter-state-and-startup.md).

## Motivation

A session's event history is its only durable state, and several parties use it at once. Registry
appends client `user.*` events from any replica, streams them over SSE and resumes a dropped stream
from `Last-Event-ID`; harness-server replicas share turns without Registry knowing who runs a
session, need redelivery when one dies, and append agent events; SDK adapters keep their state in
the log, subagent threads share the session's order, and the exporter replays whole sessions.
Payloads belong to each harness, so the store must not parse them. Deployments differ: Apache Kafka
or a Kafka-compatible service, Pulsar, or no broker at all on a laptop, in CI or in a small install.
Endpoints differ in TLS and SASL, KoP maps dotted topic names onto tenants and namespaces, and
consumers that decode records by schema do not read Kafka headers.

## Goals

### In scope

- One ordered stream per session, subagent `subpath`s, opaque payloads, dedup by `Event.id`.
- The same `append`, `read`, `tail` and `archive` semantics on Kafka, Postgres and Pulsar, with one
  backend per deployment as the only source of truth, and workspace scope in every name and key.
- An in-process library, with a types-only contract for components that must not load a broker.
- Kafka over TLS with SASL/PLAIN, KoP naming, Pulsar token and OAuth2 authentication, and an opt-in
  Avro envelope registered in a Schema Registry, with raw as the default.

### Out of scope

- Broker and database operation: retention, tiered storage, replication, topic creation, Postgres
  pruning. Multi-region is listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#deferred-by-design).
- Cross-harness interop, payload schemas (typed per-kind schemas are listed on
  [`roadmap.md`](../docs/managed-agents/roadmap.md#kafka-transcript-schema-registry)) and SQL-engine
  connectors; moving history or cursors between backends or encodings (encoding migration, Pulsar
  seek cursors and Pulsar support in the exporter are listed on `roadmap.md`).

## Design

### High-level design

```text
 client ─ POST events ─► Registry ── append user.*, lifecycle outbox ──► ┌──────────────────┐
 client ◄──── SSE ────── Registry ◄─ tail (Last-Event-ID = Event.seq) ── │ selected backend │
                         Registry ◄─ event source → session_events_index │  Kafka (default) │
                   harness-server ◄─ at-least-once user.* and lifecycle ─ │  Postgres        │
                   harness-server ── append agent events ──────────────► │  Pulsar          │
     observability-exporter (Kafka only) ◄─ read, event source ───────── └──────────────────┘
     session-runner: the contract only; an in-memory store fed over the runner tunnel
```

Every arrow is an in-process library call. One variable selects the backend for every service and
nothing dual-writes, so it is the single source of truth for a session's events;
`session_events_index` is a projection of it.

### Detailed design

#### A library, not a service

The package has no listener, Dockerfile or entry point; each service builds the backend from its own
configuration and owns the client, pool and codec lifecycle. The broker already provides durability,
order, fan-out and redelivery; a service in front of it would add a hop to every append and SSE
frame and a deployable with its own auth and failure mode. `services/proto/transcript_store.proto`
keeps the logical service, checked by `buf lint`, with no gRPC server. The contract lives in
`@orca/transcript-store-types`, which has no runtime code, so session-runner depends on it alone.

#### The contract

```ts
interface TranscriptStore {                       // packages/transcript-store-types/src/store.ts
  append(ws, session, events: Event[]): Promise<string[]>;         // one id per input position
  read(ws, session, { fromCursor, maxEvents, subpath }): AsyncIterable<Event>;      // bounded
  tail(ws, session, { fromCursor, subpath, onReady?, signal? }): AsyncIterable<Event>;   // live
  archive(ws, session): Promise<void>; /* session.archived sentinel */ close(): Promise<void>;
}
```

`Event` holds `id`, `workspaceId`, `sessionId`, `subpath` (`""` parent, `subagents/<id>` child,
`"*"` in a filter for all), `seq` (the backend cursor), `producedAt`, `producedBy`, `kind`, opaque
`payload` bytes, `idempotencyKey` (stored, not a dedup key) and optional `userId`. `read` from `""`
starts at the beginning and ends at the head; `tail` from `""` starts at the head and follows it.

#### Kafka backend (default)

- **Topics.** `{prefix}orca.{workspace_id}.sessions.{session_id}.events` (`.events-avro` in Avro
  mode), IDs matching `[A-Za-z0-9_-]+`, one partition each because cursors are partition 0's
  offsets; the broker or its operator creates them. KoP reads a dotted name as
  `<tenant>.<namespace>.<topic>`, so `KAFKA_TOPIC_PREFIX` (such as `public.default.`) prefixes every
  name, and discovery accepts KoP's bare listings and subscribes the prefixed name.
- **Appends, reads, tails.** One idempotent KafkaJS producer (`acks: -1`) writes the event ID as
  key, the payload as value and metadata as headers, in order per session; a per-session LRU of
  1,024 IDs in process memory answers a retry with its original `seq`. Reads and tails use a
  throwaway consumer group that seeks and never commits, skip records whose headers name another
  workspace or session than the topic, and fail on a decode error rather than skip the record.
- **Delivery.** harness-server's `Dispatcher`, one member of `HARNESS_CONSUMER_GROUP`, subscribes
  the listed session topics as one explicit list every `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS` and acts
  on client `user.*` events, `session.archived` and `session.deleted`. `KafkaSessionEventSource`
  (Registry's read model, the SQL-state exporter's inbox) runs one consumer per topic in a group
  derived from a base group and a topic digest, committing only after its handler resolves.
- **Connections**, identical in all three services: `plaintext` (default); `kop-token`, TLS with
  SASL/PLAIN as `KAFKA_SASL_USERNAME` (default `public`) and password `token:<KAFKA_AUTH_TOKEN>`;
  `sasl-plain-tls`, TLS with SASL/PLAIN as a required `KAFKA_SASL_USERNAME` and `KAFKA_AUTH_TOKEN`;
  `custom`, optional TLS (`KAFKA_SSL_*`) and optional SASL/PLAIN with `KAFKA_SASL_PASSWORD`.

#### Optional Avro envelope

- `KAFKA_TRANSCRIPT_ENCODING=avro` reads and writes only `.events-avro` for all sessions. Each value
  is the whole `Event` as the inline record `orca.transcript.TranscriptEvent` (payload as untouched
  `bytes`, nullable `user_id`, no `seq`) behind a zero byte and a four-byte schema ID; the key and
  headers stay, plus `orca_transcript_encoding=avro-v1`, on which readers dispatch. An unknown
  marker, or an envelope or key that disagrees with the headers, fails rather than falling back to
  raw; `raw` with a Schema Registry URL decodes frames but routes only `.events`.
- One subject, `orca.transcript.TranscriptEvent` by default, serves every topic. Registry and
  harness-server prepare the writer before serving (registering the exact shipped schema, or looking
  it up when auto-registration is off); nothing writes with the latest version or changes
  compatibility, which operators own (`FULL_TRANSITIVE` recommended) along with schema-ID retention.
- The Schema Registry client takes `http` or `https` without userinfo, query or fragment, needs
  HTTPS for Basic credentials or TLS files, always validates certificates, caps responses at 1 MiB
  and makes up to three attempts in 15 seconds on network errors, 429 and 5xx; writer schemas are
  cached by ID. `KafkaSessionEventSource` pauses an undecodable topic for 1, 2, 4, 8, 16, then 30 s.

#### Postgres backend

`applyPostgresTranscriptMigrations` creates `transcript_events` (a `BIGSERIAL` `seq` cursor,
`UNIQUE (workspace_id, session_id, event_id)`, a nullable `user_id`) and `transcript_event_claims`
at boot under a transaction-scoped advisory lock; `append` inserts with `ON CONFLICT DO NOTHING`, so
dedup is durable. `read` pages up to the head captured at the call and `tail` polls every 500 ms;
without `LISTEN`/`NOTIFY` or session locks, the DSN may point at a transaction pooler.
`PostgresSessionEventSource` claims rows per consumer group under a renewed lease, and only the
earliest unprocessed eligible row of a session is claimable, so a later event cannot overtake an
unfinished one; `SessionEventBarrierError` retries its persistence step before the session advances.

#### Pulsar backend

- One persistent topic per session, `persistent://{tenant}/{namespace}/{prefix}.{workspace_id}.sessions.{session_id}.events`
  (`public`, `default` and `orca` by default), metadata in message properties, on `pulsar-client`
  1.17.0, whose native binding a pnpm patch builds from source against SHA-512-pinned Apache C++
  client 4.1.0 archives to fix a Reader error path. `append` flushes a whole call as one key-based
  batch (up to 1,000 messages or 2 MiB), so no consumer sees an event before its companion.
- `read` scans from the earliest message while `hasNext()` reports more and rejects on a 5-second
  read timeout instead of treating it as the end; `seq` is computed from the message ID's ledger,
  entry and batch index. Resumed reads and tails scan from the start and skip to the cursor; a
  from-now tail starts at `Latest` on a temporary `orca-tail-*` subscription.
- `PulsarSessionEventSource` subscribes the session-topic pattern as `KeyShared` (auto-split,
  in-order), so one consumer owns a session; handlers run in per-session lanes (8 at once, 100
  pending); a failure other than `RetryableSessionEventError` is dropped after five redeliveries.

## Changes by component

- **Libraries**: `@orca/transcript-store-types` (the contract) and `@orca/transcript-store`
  (backends, event sources, the Kafka codec, topic helpers, metrics).
- **registry-service-ts**: selects the backend, prepares the Avro writer before serving, appends
  client events, publishes initial events and lifecycle sentinels from its outbox, bridges `tail` to
  SSE (`src/streaming/sse.ts`) and projects every event into `session_events_index`.
- **harness-server**: Kafka through `Dispatcher`, Postgres and Pulsar through the library's event
  sources; `ClaudeAgentSdkAdapter` maps the SDK `SessionStore` onto the interface.
- **session-runner** keeps `InMemoryTranscriptStore`, fed by its own appends and Registry's tunnel
  replay; **observability-exporter** is Kafka only and suffixes its state in Avro mode.
- **Helm chart**: `transcriptStore.*` feeds the three ConfigMaps; two validation templates reject
  inconsistent Schema Registry settings and a Postgres backend without a harness DSN.

## Public-facing changes

### API

No request or response shape changes. SSE frames carry `Event.seq` as `id:`, and a stream resumes
from `Last-Event-ID` or `from_cursor`. The cursor is opaque and backend-specific (a Kafka offset, a
Postgres sequence value or a number derived from a Pulsar message ID), so it does not survive a
change of backend or encoding. `GET /v1/sessions/{id}/events` reads Registry's projection.

### Events and streaming

Order is per session, delivery to harness replicas is at least once on every backend, consumers
deduplicate by `Event.id`, and subagent threads share the session's stream, told apart by `subpath`.

### Wire protocols

None beyond the proto's optional `user_id = 11`; runner and worker tunnels are unchanged.

### Storage

Kafka raw records carry the event ID as key, the payload as value and headers `id`, `workspace_id`,
`session_id`, `subpath`, `produced_at`, `produced_by`, `kind`, `idempotency_key` and `user_id`
(empty ones omitted); Pulsar uses the same property names. Avro adds `.events-avro` topics and moves
broker-native exporter state to `orca.observability.v1.checkpoints-avro` and `.delivery-avro`.
Postgres adds `transcript_events` and `transcript_event_claims`.

### Configuration

| Setting | Default | Read by | Effect |
| --- | --- | --- | --- |
| `TRANSCRIPT_STORE_BACKEND` | `kafka` | Registry, harness; the exporter accepts only `kafka` | selects the backend; an unknown value fails startup |
| `KAFKA_BROKERS`, `KAFKA_CLIENT_ID`, `KAFKA_CONNECTION_MODE`, `KAFKA_TOPIC_PREFIX` | `localhost:9092`, the service name, `plaintext`, empty | Registry, harness, exporter | brokers, client ID, connection mode, dot-terminated KoP prefix |
| `KAFKA_AUTH_TOKEN`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SASL_MECHANISM` | unset | same | per-mode credentials; `custom` accepts only `plain` |
| `KAFKA_SSL`, `KAFKA_SSL_REJECT_UNAUTHORIZED`, `KAFKA_SSL_CA_FILE`, `KAFKA_SSL_CERT_FILE`, `KAFKA_SSL_KEY_FILE` | unset | same | TLS in `custom` mode; certificates are validated unless disabled |
| `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS` | `30000` (harness), `1000` (exporter) | harness, exporter | topic discovery interval; must be positive |
| `KAFKA_TRANSCRIPT_ENCODING`, `KAFKA_SCHEMA_REGISTRY_URL` and its `_SUBJECT`, `_AUTO_REGISTER`, `_AUTH_MODE`, `_USERNAME`, `_PASSWORD`, `_CA_FILE`, `_CERT_FILE`, `_KEY_FILE`, `_REQUEST_TIMEOUT_MS` companions | `raw`; unset; `orca.transcript.TranscriptEvent`, `true`, `none` and `5000` where defaulted | Registry, harness, exporter | `raw` or `avro` (which needs the URL) and Schema Registry access; a companion without a URL fails startup |
| `TRANSCRIPT_STORE_DATABASE_URL`, `TRANSCRIPT_STORE_POOL_MAX` | `DATABASE_URL`, `10` | Registry, harness | Postgres DSN and pool size |
| `POSTGRES_EVENT_POLL_INTERVAL_MS`, `POSTGRES_EVENT_LEASE_MS`, `HARNESS_CONSUMER_GROUP` | `500`, `30000`, `harness-server` | harness | claim polling and lease; the Kafka group, Postgres claim group and Pulsar subscription |
| `PULSAR_SERVICE_URL`, `PULSAR_TENANT`, `PULSAR_NAMESPACE`, `PULSAR_TOPIC_PREFIX` | `pulsar://localhost:6650`, `public`, `default`, `orca` | Registry, harness | Pulsar endpoint and topic naming |
| `PULSAR_AUTH_TYPE`, `PULSAR_AUTH_TOKEN`, the `PULSAR_OAUTH2_` settings | none | Registry, harness | `token` or `oauth2` (`client_credentials` by default) |
| `PULSAR_RECEIVE_TIMEOUT_MS`, `PULSAR_ACK_TIMEOUT_MS` | `500`, unset | harness | event-source receive timeout and optional ack timeout |

Helm values ([`kubernetes.md`](../docs/managed-agents/kubernetes.md#transcript-backend)):
`transcriptStore.backend`, `.kafka` (including `encoding` and `schemaRegistry.*`), `.postgres` and
`.pulsar`; credentials in `secrets.values` or each component's `secretKeyRefs`;
`external.databases.transcriptStoreUrl`, falling back to the Registry database URL;
`databasePools.transcriptStoreMax`; `harness.consumerGroup`. Encoding and Schema Registry values
require `backend: kafka`, and an empty `schemaRegistry.url` renders no Schema Registry variable.

### Metrics, logs and traces

The library's registry holds `transcript_store_{append,read,tail,archive}_total{status}`,
`transcript_store_append_latency_seconds`, `transcript_store_read_first_byte_latency_seconds`,
`transcript_store_dedup_hits_total`, `transcript_store_subagent_message_rate{workspace_id,produced_by}`
and codec metrics (`transcript_store_schema_cache_total`, `transcript_store_schema_operation_seconds`,
`transcript_store_decode_failures_total`); Registry serves it on its internal listener and
harness-server on its health port, next to `harness_kafka_*` discovery, assignment and lag metrics.

## Compatibility

### Upgrade

v0.5.0 is the first public release with these backends. Postgres migrations are idempotent,
including the `user_id` column, and run at every Registry and harness boot; Kafka and Pulsar need
none, and records without `user_id` read as unattributed. Pulsar harness consumers moved from a
`Shared` to a `KeyShared` subscription, which Pulsar allows only once no consumer is attached: pause
input, drain turns, stop every consumer on the harness subscription and start the new replicas under
the same name, never mixing the two types.

### Rollback

- **Changing the backend is not a migration.** Services read only the selected backend; history,
  cursors and pending work stay in the old one and reappear only if it is selected again.
- **The Avro cutover is not a rolling change and has no seamless rollback.** Switching encoding
  requires quiesced turns and a full coordinated restart of Registry, harness and exporter with the
  same value; history and numeric cursors do not carry over, so sessions start fresh, and the
  SQL-state exporter needs a fresh state database or a verified reset. Returning to `raw` reselects
  the old topics, where old pending work can resume and newer Avro history is not read
  ([cutover and rollback](../docs/managed-agents/libraries/transcript-store.md#coordinated-incompatible-cutover-and-rollback)).

### Version skew

Registry, harness and exporter must agree on backend, topic prefix, encoding and Pulsar naming; the
chart renders all three from one `transcriptStore` block, and a reader-first rolling update is not a
safe encoding change. Readers that predate attribution ignore `user_id`.

## Security considerations

- **Workspace scope everywhere.** Topic names are built from validated IDs, Postgres reads and dedup
  are keyed by workspace and session, and the Kafka dedup cache includes the workspace. `append`
  rejects events routed to another session (`src/route.ts`), readers skip records whose metadata
  disagrees with their topic before any handler or Schema Registry call, and harness-server checks
  each event against its Registry-prepared snapshot.
- **The backend is trusted storage.** Consumers act on any well-formed record on a session topic or
  in `transcript_events`; broker ACLs and database roles decide who may write there, and the library
  does not configure them.
- **Credentials** for brokers, Pulsar and the Schema Registry come from Secrets; Schema Registry
  authentication never reuses broker SASL, and none of these settings reach workers, runners or
  sandboxes. Codec errors carry only a code, event-source status no backend error text, and
  Registry keeps the library's metrics off its public listener.

## Testing

- **Unit** (the required `test` job): `packages/transcript-store/test/unit/` covers topic naming and
  KoP matching, serialization, route and workspace isolation, the Kafka consumer and event source
  (byte caps, poison skips, group joins, cooldowns), the codec, the Postgres and Pulsar stores and
  the native Pulsar Reader against a fault double; `render.test.mjs` covers chart validation.
- **Integration** (live Kafka, Postgres): `library.{append-read,tail,cursor,dedup,archive,metrics}`
  and `postgres-store` specs, `pulsar-store.spec.ts` when `PULSAR_SERVICE_URL` is set, and
  harness-server's 10 Kafka-backed `SessionStore` cases (`session-adapter.spec.ts`).
- **Avro** (`test-transcript-avro.yml`): `avro-schema-registry.mjs` runs against digest-pinned
  Apache Kafka 3.7.1 and Confluent Schema Registry 7.7.1: byte-exact binary, empty and non-JSON
  payloads read by an independent Avro reader, preregistered IDs, schema evolution, the record-name
  subject, a raw session gaining `.events-avro` at offset 0, HTTPS authentication and 401 and 503
  recovery. Removing the cooldown, key, metrics or raw-header guard was shown to fail a suite.
- **End to end**: `e2e-stack.yml` runs the stack suite once per backend and real-agent sandbox legs
  as Claude on Kafka, the Pi SDK on Postgres and the Codex SDK on Pulsar.

## Alternatives

- **A standalone gRPC transcript service.** The first design made the store a deployable with gRPC
  over mTLS so other components could read transcripts without Registry. Its only callers already
  reach the broker, so it became a library before either called it, saving a hop and a deployable.
- **Pulsar as the only substrate.** The first design used a Pulsar topic per session with a `Shared`
  subscription; it moved to the Kafka protocol before any Pulsar code existed, since a Kafka client
  runs on Kafka-compatible brokers, KoP included, and offsets are cursors. Pulsar later returned.
- **A Postgres mirror or a dual write.** A second copy must be reconciled with the first, so there
  is none; Postgres became a selectable backend, and `session_events_index` is only a projection.
- **One topic per subagent.** `subpath` on the parent topic keeps the topic count flat as fan-out
  grows and one order per session; `transcript_store_subagent_message_rate` measures that traffic.
- **Application-managed retention or topic deletion.** Retention and tiering are the broker
  operator's; `archive` appends a sentinel instead of a slow, asynchronous topic deletion.
- **KafkaJS regex or Pulsar `Shared` subscriptions.** KafkaJS resolves a regex once and misses later
  topics, so the dispatcher, which first used one, subscribes explicit lists; `Shared` let a
  session's later event reach another replica before an earlier one finished, hence `KeyShared`.
- **Avro records in the raw topics.** The envelope was first written into `.events` and told apart
  per record; a separate set keeps raw records from Avro-only consumers and Avro-validating brokers.
- **Other Avro choices.** A topic-name subject (`<topic>-value`) would mean one subject per session
  topic; writing with the latest version, or letting the codec change compatibility, would let a
  registry edit change what Orca writes; reusing broker SASL would couple two unrelated credentials.

## Status notes

- **Evolution.** After the move to a Kafka-protocol library came the Postgres and Pulsar backends,
  the `kop-token`, `sasl-plain-tls` and `custom` modes and `KAFKA_TOPIC_PREFIX`; route checks and
  workspace-scoped dedup with multi-workspace isolation; the types-only package; `Event.userId`;
  and last the Avro envelope, whose ablation pass removed unused adapters, and Pulsar `KeyShared`.
- **Divergences.** Registry publishes `session.archived` and `session.deleted` from its lifecycle
  outbox via `append`, retried until the backend accepts them, instead of calling `archive()`, which
  every backend still implements. Kafka's `maxEvents` bounds the offset window, so a filtered `read`
  can return fewer events than elsewhere, and its subagent counter includes deduplicated retries.
- **Not built.** Pulsar seek cursors and Pulsar exporter support are listed on
  [`roadmap.md`](../docs/managed-agents/roadmap.md#deferred-by-design); encoding migration, typed payload schemas, Schema Registry OAuth
  and cross-session aggregation under [Kafka transcript Schema Registry](../docs/managed-agents/roadmap.md#kafka-transcript-schema-registry).
