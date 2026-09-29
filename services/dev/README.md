# Dev Stack

## Isolated Kafka / Schema Registry Avro fixture

This opt-in fixture is separate from the default dev stack. It starts only the
Compose project `orca-transcript-avro`; the regular Kafka on port 9092 and other
containers are untouched. No cloud endpoint, user secret, or Registry is required
by the normal raw-transcript dev stack. The fixture's Registry port overlaps the
normal dev stack's Pulsar admin port; do not run both fixtures on that port together.

From the repository root (Node 22, pnpm, Docker Compose v2, and OpenSSL on PATH):

```bash
pnpm install --frozen-lockfile
pnpm --filter @orca/transcript-store... build
bash services/dev/scripts/test-transcript-avro.sh
# Explicit cleanup, including after KEEP_TRANSCRIPT_AVRO=1 debugging:
bash services/dev/scripts/test-transcript-avro.sh down
```

[`docker-compose.transcript-avro.yml`](docker-compose.transcript-avro.yml) runs
Apache Kafka 3.7.1 and a separate Confluent Schema Registry 7.7.1 with the actual
Schema Registry REST API. Local endpoints are `127.0.0.1:19092` (Kafka) and
`http://127.0.0.1:18081` (Registry). The authentication cases use a temporary
HTTPS proxy on localhost port 18085 with fixed test-only Basic credentials and a
generated trusted CA. The proxy forwards requests to the real Registry rather than
synthesizing successful schema responses; its TLS key/certificate are removed on exit.
These credentials and certificates are not production configuration.

The runner imports the built production `createKafkaTranscriptCodec` and
`KafkaTranscriptStore`. The integration fixture
[`avro-schema-registry.mjs`](../../packages/transcript-store/test/integration/avro-schema-registry.mjs)
uses an independent standard `avsc` decoder, with authentication forwarding in
[`schema-registry-proxy.mjs`](../../packages/transcript-store/test/integration/schema-registry-proxy.mjs).
Its assertions cover:

- RecordNameStrategy subject registration, preregistration/exact lookup, schema-ID
  framing and lookup through the real Registry REST API.
- Byte-identical binary, empty archive and Chinese JSON payloads; event IDs, nullable
  user attribution, route metadata, headers, keys and Kafka offsets.
- Application-level UTF-8/JSON decoding after the Avro envelope, without converting
  arbitrary opaque payload bytes into text or JSON.
- Compatible schema evolution, exact writer-schema selection rather than latest,
  and rejection of incompatible schema changes.
- Registry HTTP/authentication failures and unknown schema IDs failing Orca decoding
  rather than falling back to raw.
- Coexisting raw `.events` and Avro `.events-avro` topics for the same session,
  with cold-cache bounded reads and explicit-cursor tails confined to the selected
  set, preserving byte identity and that stream's offsets. A raw codec with a
  Registry URL still selects only raw topics; discovery excludes the other set.
- Independent `avsc` decoding of records from `.events-avro`, without consuming old
  raw history. Read-only codecs fetch schemas by ID without preparing or registering
  a writer. Mixed-frame unit fixtures cover codec compatibility, not mixed routing.

This is a Kafka/Schema Registry interoperability fixture, not SQL-engine validation.
Avro-only external consumers select only `.events-avro`; see the
[external consumer contract](../../docs/managed-agents/libraries/transcript-store.md#optional-kafka-avro-envelope)
for migration and rollback constraints.

The dedicated `test-transcript-avro.yml` CI workflow runs the `avro-schema-registry`
job with the existing fail-open `ts` classifier, not trigger-level path filters.
Local assertions, container logs and image listings are saved in the gitignored
`services/dev/logs/transcript-avro/` directory and uploaded in CI. By default the
runner removes only its own project containers/network/volumes even on failure.
Setting `KEEP_TRANSCRIPT_AVRO` to `1` retains infrastructure for diagnosis; run `down`
before another run because registration assertions require a fresh Registry.

This directory holds the docker-compose definition, the bring-up scripts, the
RSA keypair init helper, and (when running) the per-service logs / pids the
local stack uses. For the design rationale and operator walkthrough see
[`docs/managed-agents/local-stack.md`](../../docs/managed-agents/local-stack.md);
this file is the per-target reference for the Makefile.

Two layers, both driven from the repo-root Makefile:

| Target              | What it brings up                                             |
| ------------------- | ------------------------------------------------------------- |
| `make stack-up`     | infra **plus** registry, harness, and ai-gateway (full stack) |
| `make stack-down`   | reverses `stack-up`                                           |
| `make dev-up`       | infra only for `TRANSCRIPT_STORE_BACKEND`                     |
| `make dev-down`     | tears down infra only                                         |
| `make stack-status` | port table + service pids                                     |

Hybrid architecture: infra and the external `ai-gateway` image run in
containers (compose), while `registry-service-ts` and `harness-server` run
NATIVELY on the host so the harness can use the `srt` sandbox without
container-in-container headaches. See `docs/managed-agents/architecture.md`.

## stack-up (full stack)

```bash
cp services/dev/.env.example services/dev/.env  # edit ANTHROPIC_API_KEY
make stack-up
```

What it does:

1. `make dev-up` starts Postgres, RustFS, and only the broker required by
   `TRANSCRIPT_STORE_BACKEND` (`kafka` by default, `pulsar` for Pulsar mode,
   no broker for Postgres mode).
2. `services/dev/scripts/init-secrets.sh` — generates an RSA-2048 keypair at
   `services/dev/secrets/{session-jwt,session-jwt-pub}.pem`. Idempotent:
   re-running is a no-op once the files exist. The ai-gateway config is the
   checked-in `services/dev/ai-gateway-config.yaml` template.
3. `services/dev/scripts/start-services.sh` — sources `services/dev/.env` (or
   the inline defaults that mirror the compose stack), waits for infra TCP
   readiness, runs `pnpm -r build` + applies file-store / memory-store /
   registry migrations, then backgrounds registry/harness and starts
   ai-gateway via compose. Native logs land in
   `services/dev/logs/{registry,harness}.log` and pid files in
   `services/dev/run/`; ai-gateway logs come from `docker compose logs
ai-gateway`. The script blocks until all three `/healthz` endpoints return
   200, then prints the URL table.

Required prerequisites on PATH: `pnpm`, `node` (>=22), `docker`, `openssl`,
`curl`. The script bails with a clear error if any are missing.

`AI_GATEWAY_IMAGE` defaults to the published GHCR image. The selected image must
support the managed-agents dynamic MCP contract (`X-Orca-Credential-Id`,
`credential_ids[]`, workspace/session-scoped destination and vault-credential
resolvers, reserved wildcard destination, and HTTP resolver
`bearer_token_file`). The e2e workflow builds immutable Gateway commit
`ad891848f95bfe8cf62b8af85ec5b38cc81bb26e`, which contains the resolver
contracts and JWT-derived LLM route/model authorization, until a released image
supersedes that source pin.
Override it in `services/dev/.env` when validating an unreleased gateway build or
another released tag. `stack-up` pulls and smoke-tests that image before startup;
its capability probes positively verify both contracts with independent invalid
configs: a missing `bearer_token_file`, then a wildcard resolver with
`dns_timeout_ms: 0` that must trigger typed MCP egress validation. Full config
and boot checks remain additional smoke tests; resolver acceptance never depends
on `timeout`/`gtimeout` availability or unknown-field-tolerant parsing.
If the published runtime is incompatible with the shipped binary (for example a
glibc mismatch), it builds a local compatibility wrapper image from the same
external binary. If the image is older than the dynamic MCP contract, the smoke
test fails fast with an instruction to choose a newer image/tag. The local
stack generates `services/dev/run/internal-service-token` and mounts it into
the gateway; Registry and Harness read the same file.

`SANDBOX_RUNTIME=local` (the default in `.env.example`) requires the `srt`
binary on PATH — `npm install -g @anthropic-ai/sandbox-runtime`. Use
`SANDBOX_RUNTIME=in-memory` for tests that don't care about real isolation.

`make stack-down` runs `stop-services.sh` (SIGTERM with 10s grace, then
SIGKILL) followed by `docker compose down`.

`make stack-status` prints the port table plus native pid status. Tail logs
with `tail -f services/dev/logs/registry.log` (or `harness.log`); use
`docker compose -f services/dev/docker-compose.yml logs -f ai-gateway` for the
gateway container.

After `stack-up` is healthy:

```bash
pnpm e2e:wire    # Layer A — wire-protocol conformance, no Anthropic key
pnpm e2e:agent   # Layer B — real-agent-loop, requires ANTHROPIC_API_KEY
```

See [`packages/e2e-tests/README.md`](../../packages/e2e-tests/README.md) for
how the e2e suite drives the registry over HTTP and what it asserts.

## dev-up (infra only)

`make dev-up` reads `TRANSCRIPT_STORE_BACKEND` from the caller environment or
`services/dev/.env` and starts only the infra needed for that mode:

```bash
make dev-up
TRANSCRIPT_STORE_BACKEND=postgres make dev-up
TRANSCRIPT_STORE_BACKEND=pulsar make dev-up
```

`make dev-down` tears the compose stack down.

| Service        | Host port   | Notes                                                          |
| -------------- | ----------- | -------------------------------------------------------------- |
| Kafka          | 9092, 29092 | 29092 exposes the Docker listener to the OpenSandbox Kind E2E  |
| Pulsar         | 6650        | only for `TRANSCRIPT_STORE_BACKEND=pulsar`; admin API on 18081 |
| Postgres       | 5432        | user/pass: `orca`/`orca`; includes `transcriptstore` DB        |
| RustFS API     | 9000        | user/pass: `minioadmin`/`minioadmin`; bucket: `orca-files`     |
| RustFS console | 9001        | UI at `/rustfs/console/`                                       |

The checked-in local defaults set both `S3_ENDPOINT` and the optional
`S3_STS_ENDPOINT` override to the RustFS API. In production, leave
`S3_STS_ENDPOINT` unset unless the STS API is intentionally hosted at a custom
endpoint; `S3_ENDPOINT` never changes where the AWS STS client connects.

Kafka quick-check after `TRANSCRIPT_STORE_BACKEND=kafka make dev-up`:

```bash
docker compose -f services/dev/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose -f services/dev/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test <<< 'hello'
docker compose -f services/dev/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning --max-messages 1 --timeout-ms 5000
```

The dev compose broker stays plaintext. To point the native registry and harness at a remote TLS Kafka-compatible endpoint instead, set `TRANSCRIPT_STORE_BACKEND=kafka`, `KAFKA_BROKERS=<bootstrap>:9093`, and one of the supported modes in `services/dev/.env`:

```env
# Token-prefixed SASL/PLAIN over TLS, e.g. Kafka-on-Pulsar (KoP)
KAFKA_CONNECTION_MODE=sasl-plain-token-tls
KAFKA_AUTH_TOKEN=<jwt>
KAFKA_SASL_USERNAME=public

# Any Kafka-compatible backend with SASL/PLAIN over TLS
KAFKA_CONNECTION_MODE=sasl-plain-tls
KAFKA_SASL_USERNAME=<userName>
KAFKA_AUTH_TOKEN=<jwt>
```

`KAFKA_CONNECTION_MODE=custom` exposes KafkaJS plain SASL/TLS knobs (`KAFKA_SSL`, `KAFKA_SASL_MECHANISM=plain`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, and optional `KAFKA_SSL_*_FILE` paths). These envs only affect registry/harness transcript clients; the external ai-gateway Kafka audit sink has separate image-owned configuration.

When running the broker-native observability exporter against KoP, also set
`KAFKA_TOPIC_LISTING_MODE=bare-alias` in its environment. Its default `canonical` listing
mode is independent of authentication; registry and harness ignore this variable.
The old `kop-token` connection mode is rejected rather than accepted as an alias.

## Clean start

`make dev-down` removes the containers but leaves the bind-mounted data on disk under `services/dev/data/`. If you need a fresh state (e.g. Postgres rejected a re-run because data already exists, or broker log data is inconsistent), wipe the directory:

```bash
rm -rf services/dev/data
make dev-up
```

When `stack-up` services misbehave, also wipe their state:

```bash
make stack-down
rm -rf services/dev/{data,secrets,logs,run,.env}
make stack-up
```

## Launcher-to-monitor handoff

`start-services.sh` supervises each service with a small inline Node monitor. It
passes three values to it as environment prefix assignments on a `node -e`
invocation (`start-services.sh:397-399`) rather than as arguments, because the
monitor's own program is itself one of those values.

| Var                     | Meaning                                                                                      |
| ----------------------- | -------------------------------------------------------------------------------------------- |
| `ORCA_DEV_SERVICE_NAME` | Which service this monitor supervises; used in its log prefix.                               |
| `ORCA_DEV_LOGFILE`      | The file the monitor appends that service's combined stdout and stderr to.                   |
| `ORCA_DEV_MONITOR_SRC`  | The monitor's JavaScript source text, read at `:406` and handed to `spawn(node, ["-e", …])`. |

`ORCA_DEV_MONITOR_SRC` is a program, not a path — the shell builds it as a
single-quoted string at `:360` and the launcher passes it through the
environment because a `node -e` script cannot easily carry another `node -e`
script on its command line.

None of the three reaches the service: the launcher deletes all three from the
environment it hands the child (`:368-370`), so a service cannot observe them
and setting them by hand does nothing. The monitor spawns its child once and
exits when the child exits (`:373`, `:387-393`); there is no restart logic.
