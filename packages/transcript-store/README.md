# @orca/transcript-store

> Library: `TranscriptStore` interface with Kafka, Postgres, and Apache Pulsar implementations.
> Not a deployable service — consuming services (`registry-server`, `harness-server`,
> `observability-exporter`)
> import the package as a workspace dependency and call the selected backend in-process.

## Quick start

```ts
import { Kafka } from 'kafkajs';
import { KafkaTranscriptStore } from '@orca/transcript-store';

const kafka = new Kafka({ brokers: ['localhost:9092'], clientId: 'my-app' });
const store = new KafkaTranscriptStore({ kafka });

await store.append('ws_x', 'ses_y', [
  /* events */
]);

for await (const e of store.tail('ws_x', 'ses_y', { fromCursor: '', subpath: '' })) {
  console.log(e);
}

await store.close();
```

Remote TLS/SASL endpoints are configured by the consuming service when it constructs KafkaJS. For example, a Kafka-on-Pulsar (KoP) endpoint with token auth uses `ssl: true` plus SASL/PLAIN with `password: 'token:' + jwt`, while a Kafka-compatible backend taking SASL/PLAIN over TLS uses `ssl: true` plus SASL/PLAIN with `password: jwt`:

```ts
const kafka = new Kafka({
  brokers: process.env.KAFKA_BROKERS!.split(',').map((broker) => broker.trim()),
  clientId: 'my-app',
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_SASL_USERNAME ?? 'public',
    password: `token:${process.env.KAFKA_AUTH_TOKEN}`,
  },
});
```

Kafka supports optional Confluent-compatible Schema Registry Avro encoding of the
full Event envelope. Payloads remain opaque bytes inside that envelope; raw is the
default encoding. The codec's encoding selects `.events` (raw) or `.events-avro`
(Avro) for all sessions and both reads and writes. A Registry URL alone enables
Avro frame decoding but does not select or read the Avro topic set. Switching
encoding requires quiesced turns and a full coordinated registry/harness/exporter
restart; history and existing cursors are not migrated. Registry credentials and
TLS are independent of broker SASL/TLS. See the [wire format, incompatible cutover and
external consumers](../../docs/managed-agents/libraries/transcript-store.md#optional-kafka-avro-envelope).

Postgres backend:

```ts
import { Pool } from 'pg';
import { PostgresTranscriptStore, applyPostgresTranscriptMigrations } from '@orca/transcript-store';

const pool = new Pool({ connectionString: process.env.TRANSCRIPT_STORE_DATABASE_URL });
await applyPostgresTranscriptMigrations(pool);
const store = new PostgresTranscriptStore({ pool });
```

Pulsar backend:

```ts
import { PulsarTranscriptStore } from '@orca/transcript-store';

const store = new PulsarTranscriptStore({
  serviceUrl: process.env.PULSAR_SERVICE_URL ?? 'pulsar://localhost:6650',
  tenant: 'public',
  namespace: 'default',
});
```

## Topology

- **Topic naming:** Kafka uses `orca.{workspace_id}.sessions.{session_id}.events` in raw
  mode or `orca.{workspace_id}.sessions.{session_id}.events-avro` in Avro mode,
  optionally prefixed via the `topicPrefix` option (see below).
  Pulsar uses `persistent://{tenant}/{namespace}/orca.{workspace_id}.sessions.{session_id}.events`.
  Kafka helpers `sessionTopicName(ws, ses, prefix?, encoding?)`,
  `parseSessionTopic(topic, encoding?)`, `matchSessionTopic(topic, prefix?, encoding?)`,
  and `sessionTopicPattern(encoding?)` select exactly one mode (default raw);
  `SESSION_TOPIC_PATTERN` remains the raw regex. Discovery lists containing both
  sets yield only the chosen set. Pulsar exports `pulsarTopicName`.
- **Topic prefix (Kafka-on-Pulsar):** `KafkaTranscriptStore` and
  `KafkaSessionEventSource` accept an optional `topicPrefix` (default `''`;
  services parse it from `KAFKA_TOPIC_PREFIX`). When non-empty it must be
  dot-terminated segments (e.g. `public.default.`). A Kafka-on-Pulsar (KoP)
  endpoint maps dotted Kafka topic names to
  `<tenant>.<namespace>.<local-topic>`, so the bare `orca.{ws}...` name fails
  (no `orca` tenant) while `public.default.orca.{ws}...` works. Note the
  listing asymmetry: for the default tenant/namespace, KoP's
  `admin.listTopics()` returns the BARE local name (prefix stripped), so
  discovery uses `matchSessionTopic(name, prefix)` to tolerate both bare and
  prefixed listings and always subscribes to the canonical (prefixed) name.
  Harness dispatcher paths for both bare and prefixed Kafka use each
  `admin.listTopics()` result as an explicit canonical topic list. Kafka
  configuration requires a positive `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS`
  (default `30000`) so session topics created after startup are discovered.
- **Per-event metadata** flows through backend message metadata (`id`, `workspace_id`,
  `session_id`, `subpath`, `produced_at`, `produced_by`, `kind`, `idempotency_key`, `user_id`);
  the `payload` bytes are opaque to this library.
- **Optional user attribution.** `user_id` preserves a non-empty caller-supplied Registry-derived
  client user ID. The backends do not derive or stamp it; missing or empty metadata reads as absent.
- **Subagent traces** share the parent topic, distinguished by the `subpath`
  field. Empty string = parent; `"*"` matches all in `read`/`tail`.

## Metrics

The library registers Prometheus metrics on its own `Registry`. To expose them
via your service's `/metrics` endpoint, import `transcriptStoreMetricsRegistry`
and concatenate its output:

```ts
import { transcriptStoreMetricsRegistry } from '@orca/transcript-store';
import { registry } from './my-app-metrics.js';

app.get('/metrics', async (_req, reply) => {
  reply.header('content-type', registry.contentType);
  return [await registry.metrics(), await transcriptStoreMetricsRegistry.metrics()].join('\n');
});
```

Notable metrics:

- `transcript_store_append_total{status}` / `transcript_store_archive_total{status}` / `transcript_store_read_total{status}` / `transcript_store_tail_total{status}`
- `transcript_store_append_latency_seconds` (Histogram)
- `transcript_store_dedup_hits_total`
- `transcript_store_subagent_message_rate{workspace_id, produced_by}`

## Tests

```bash
pnpm -F @orca/transcript-store test               # unit
KAFKA_BROKERS=localhost:9092 \
pnpm -F @orca/transcript-store test:integration   # against real Kafka
```

## Backends

`TranscriptStore` is an interface. `KafkaTranscriptStore` is the default
backend. `PostgresTranscriptStore` is available for local, test, and smaller
self-hosted deployments; harness-server consumes it through
`PostgresSessionEventSource`, which uses a lease table for at-least-once delivery.
`PulsarTranscriptStore` uses the official Apache `pulsar-client` Node library;
harness-server consumes it through `PulsarSessionEventSource` with a `KeyShared`
subscription. All three Session event sources expose a small `status()` result:
normal retry loops remain `running`, `stop()` reports `stopped`, and an escaped
run-loop failure reports `failed` without exposing backend error details. The
Postgres and Pulsar sources additionally expose `whenFailed()`, which resolves
only for an escaped terminal run-loop failure (never intentional stop); terminal
diagnostics carry only a safe error name and explicitly allowlisted stable code.

Registry and harness services select the backend with `TRANSCRIPT_STORE_BACKEND=kafka|postgres|pulsar`.
When `kafka` is selected, registry-service-ts and harness-server accept `KAFKA_CONNECTION_MODE=plaintext|sasl-plain-token-tls|sasl-plain-tls|custom` plus the matching `KAFKA_AUTH_TOKEN`, `KAFKA_SASL_*`, and `KAFKA_SSL_*` env vars, and an optional `KAFKA_TOPIC_PREFIX` (e.g. `public.default.` for Kafka-on-Pulsar (KoP) endpoints). Harness-server requires a positive `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS` for all Kafka deployments (default `30000`). When `postgres` is selected, set `TRANSCRIPT_STORE_DATABASE_URL` or let it fall back to `DATABASE_URL`. When `pulsar` is selected, set `PULSAR_SERVICE_URL`; `PULSAR_TENANT`, `PULSAR_NAMESPACE`, and `PULSAR_TOPIC_PREFIX` default to `public`, `default`, and `orca`.
The observability exporter imports only the Kafka implementation and rejects non-Kafka Transcript
backends; its Postgres connection stores exporter inbox/cursor/outbox state, not Transcript events.
