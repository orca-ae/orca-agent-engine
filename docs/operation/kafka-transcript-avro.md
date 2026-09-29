# Kafka transcript Avro in Helm

The chart's optional Avro envelope applies to registry, harness and observability
exporter only. See the [wire format, rollout and external consumer contract](../managed-agents/libraries/transcript-store.md#optional-kafka-avro-envelope)
and the [complete environment table](../../services/registry-service-ts/README.md#kafka-transcript-avro-and-schema-registry).

## Registry access configuration

This overlay keeps all reads and writes on raw `.events` topics while making the
codecs Avro-capable. A Registry URL alone does not select `.events-avro` or merge
the two topic sets. This is access configuration, not a safe reader-first live cutover.
The schema-auth Secret holds independent Registry Basic credentials; no broker
password or token is implicitly reused. Use HTTPS for Basic or mTLS authentication.

```yaml
transcriptStore:
  backend: kafka
  kafka:
    encoding: raw
    schemaRegistry:
      url: https://schemas.example.internal
      subject: orca.transcript.TranscriptEvent
      autoRegister: true
      authMode: basic
      requestTimeoutMs: 5000
registry:
  secretKeyRefs:
    KAFKA_SCHEMA_REGISTRY_USERNAME: { name: schema-auth, key: username }
    KAFKA_SCHEMA_REGISTRY_PASSWORD: { name: schema-auth, key: password }
harness:
  secretKeyRefs:
    KAFKA_SCHEMA_REGISTRY_USERNAME: { name: schema-auth, key: username }
    KAFKA_SCHEMA_REGISTRY_PASSWORD: { name: schema-auth, key: password }
observabilityExporter:
  enabled: true
  secretKeyRefs:
    KAFKA_SCHEMA_REGISTRY_USERNAME: { name: schema-auth, key: username }
    KAFKA_SCHEMA_REGISTRY_PASSWORD: { name: schema-auth, key: password }
```

An alternative is a shared Secret selected through `secrets.existingSecret`, with
canonical keys `KAFKA_SCHEMA_REGISTRY_USERNAME` and `KAFKA_SCHEMA_REGISTRY_PASSWORD`.
Each service's explicit `secretKeyRefs` wins over the shared Secret, including the
harness's fixed-key injection list. Exporter receives individual Registry credential
refs rather than all shared Secret contents. Development chart-created Secrets use
`secrets.values.kafkaSchemaRegistryUsername` and
`secrets.values.kafkaSchemaRegistryPassword`; never commit credentials in values files.
Opaque `extraEnvFrom` bundles remain operator-owned: inspect their resolved contents
because Helm cannot validate external Secret data. The runtime rejects credentials
without the corresponding Registry URL/auth mode.

Leave `schemaRegistry.url` empty to omit all explicit Registry settings from the
ConfigMaps. The default raw configuration needs no Registry. Auto-register false
requires preregistering the exact shipped schema; read-only exporters never register
schemas. The shared subject `orca.transcript.TranscriptEvent` is independent of topic
name and framed schema ID; changing encoding does not change it.

## Incompatible cutover

After checking isolated Avro canaries, stop new work and quiesce existing turns.
Set `encoding: avro` consistently for registry, harness and exporter, then perform a
full coordinated restart before admitting traffic. Do not rely on an ordinary Helm
rolling update or hot reload to coordinate this change. All sessions then use only
`.events-avro`, with any `topicPrefix` still before `orca` (for example
`public.default.orca.{workspace_id}.sessions.{session_id}.events-avro`).

Old history remains in `.events`; there is no automatic migration, per-session
format selection or continuity for existing numeric cursors. Existing session state
and replay are not assumed correct after the switch: create fresh sessions and
replay only the new stream. The central registry database is not automatically reset.

Broker-native exporter state uses `.checkpoints-avro`, `.delivery-avro` and an
Avro-suffixed consumer-group namespace to avoid reusing old checkpoints and pending
delivery; JSON record formats stay unchanged. For a SQL-state exporter, use a fresh
dedicated exporter-state database or an operator-verified reset: its Postgres
progress is session-keyed and is not automatically migrated or truncated.

Reverting to `raw` reselects the old set and does not read new Avro history. This is
not seamless rollback: coordinate workers and stored state because old pending work
can resume. Retain Registry access and historical schema IDs for Avro replay.
External Avro consumers subscribe only to `.events-avro`, excluding old raw history;
they do not receive new raw writes after rollback.

## Explicit TLS mounts

File settings name paths inside each consuming container; the chart does not turn
arbitrary paths into volumes. Merge the following into the overlay, and repeat the
same extra volume/mount entries under `harness` and `observabilityExporter`. Use a
restricted Secret for private keys and read-only mounts outside service workspaces.

```yaml
transcriptStore:
  kafka:
    schemaRegistry:
      caFile: /etc/orca-schema-tls/ca.pem
      certFile: /etc/orca-schema-tls/tls.crt
      keyFile: /etc/orca-schema-tls/tls.key
registry:
  extraVolumes:
    - name: schema-registry-tls
      secret:
        secretName: schema-registry-tls
  extraVolumeMounts:
    - name: schema-registry-tls
      mountPath: /etc/orca-schema-tls
      readOnly: true
```

Certificate and key are paired; CA-only TLS is also supported. Server certificate
validation is always enabled and independent of the broker's TLS flags. These are
service mounts, not sandbox resource mounts. Do not copy the Registry environment or
Secret into environment-worker, session-runner, toolset or sandbox environments.
