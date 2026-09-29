# harness-server

Internal-only TypeScript service that hosts the agent loop, owns sandboxes and
resource mounts, and routes tool calls. Tools, MCP, sandboxes, memory mounts,
GitHub repos, and both harness topologies all ship today.

Quick orientation: [`AGENTS.md`](AGENTS.md). Full design:
[`docs/managed-agents/services/harness-server.md`](../../docs/managed-agents/services/harness-server.md).

## Architecture

```
client → registry-server (POST /v1/sessions/:id/events)
       → KafkaTranscriptStore.append → Kafka (orca.{ws}.sessions.{ses}.events)
       → harness-server.Dispatcher (consumer-group regex)
       → SessionRunner → AgentHarness
       → transcript-store.append (agent.*)
       ← registry-server (GET /v1/sessions/:id/events/stream — SSE bridge)
```

## Harness topologies

`Dispatcher` resolves the agent's `metadata.harness` + `metadata.mode`
annotation against `@orca/harness-catalog` and picks one of two topologies:

- **`separate`** (`claude_agent_sdk`, the platform default) — the Claude Agent
  SDK runs in this process (`src/harness/claude/`). `ClaudeAgentSdkAdapter`
  implements the SDK's `SessionStore` over `@orca/transcript-store`, so the SDK
  reads and writes session history through the transcript log directly. Tools
  dispatch to the sandbox; LLM calls go straight to `api.anthropic.com`.
- **`colocated`** (`claude_code`, `codex`) — the harness process runs inside
  the sandbox image and this service drives it over HTTP/SSE
  (`src/harness/in-sandbox/`). `InSandboxHarness.events()` tails the sandbox
  stream and `SessionRunner.pumpEvents` appends each mapped event to
  transcript-store. The production mapper/push boundary completes every event
  with a canonical ID and explicit primary path; the service-local `AgentEvent`
  type requires both fields, and `SessionRunner` passes them directly as
  Transcript `Event.id`/`subpath`. Payload `id`/`uuid` fields never select the
  event envelope. The canonical event ID is also retained as `idempotencyKey`
  metadata. Transcript-store dedups
  by `Event.id` across the session regardless of subpath; DialInTransport's
  raw-SSE reconnect set is separate transport-level protection. LLM egress goes through ai-gateway via
  `LITELLM_API_BASE` / `LITELLM_API_KEY`.

An invalid supplied public subpath fails the event pump rather than silently
becoming primary. It poisons that local runner; a later `submit()` follows the
existing Dispatcher hard submit-failure lifecycle, without an immediate
asynchronous teardown solely from the pump failure.

Both Harness paths keep `span.model_request_start/end` as public kinds while
labeling their current one-pair-per-query observation as `turn_model_summary`.
The start envelope ID is the only start identity; the end has its own ID and
references the start. Provider/model values are trusted configured/requested
snapshot values, not verified served routing facts.

Transcript-store is the sole source of truth in both topologies; the sandbox is
ephemeral. See [`docs/managed-agents/harness-modes.md`](../../docs/managed-agents/harness-modes.md).

## Sandbox runtimes

`SANDBOX_RUNTIME` is **required** — `loadConfig` throws on a missing or
unrecognized value, so there is no silent default.

| Value         | Runtime                  | Notes                                                                                  |
| ------------- | ------------------------ | -------------------------------------------------------------------------------------- |
| `local`       | `LocalSandboxRuntime`    | Host shell via `srt`; see [`src/sandbox/local/README.md`](src/sandbox/local/README.md) |
| `e2b`         | `E2BSandboxRuntime`      | Requires `E2B_API_KEY`; FUSE-capable with the `orca-default` template                  |
| `opensandbox` | `OpenSandboxRuntime`     | Requires one OpenSandbox endpoint + image; always uses gVisor in-sandbox FUSE          |
| `agentenv`    | `AgentEnvRuntime`        | Requires an AgentENV gateway, API key, and operator-built OCI image                    |
| `in-memory`   | `InMemorySandboxRuntime` | Tests and dev only                                                                     |

## Run locally

```bash
make dev-up                 # Postgres + Kafka + RustFS
KAFKA_BROKERS=localhost:9092 \
ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
pnpm -F @orca/harness-server dev
```

## Endpoints

| Port | Protocol | Purpose                           |
| ---- | -------- | --------------------------------- |
| 9094 | HTTP     | `/healthz`, `/readyz`, `/metrics` |

(No public RPC. Session events arrive over whichever transcript backend is
configured — a Kafka consumer-group subscription, `PostgresSessionEventSource`,
or `PulsarSessionEventSource`; `main.ts` builds one and hands it to the
dispatcher.)

## Tests

```bash
# Unit (no infra)
pnpm -F @orca/harness-server test

# Integration (Kafka + Postgres + registry-server in-process)
KAFKA_BROKERS=localhost:9092 \
DATABASE_URL=postgres://orca:orca@localhost:5432/registry \
pnpm -F @orca/harness-server test:integration
```

Integration tests cover:

- AgentENV command/file routing, Bubblewrap isolation, and pause/resume when
  `AGENTENV_BASE_URL`, `AGENTENV_API_KEY`, and `AGENTENV_IMAGE` are set.

- 10 hand-written `SessionStore` conformance cases against `ClaudeAgentSdkAdapter`
  (`session-adapter.spec.ts`). The SDK ships no conformance helper; these assert
  its contract by hand.
- Chat round-trip end-to-end (registry POST → Kafka → harness → SSE) with a
  `FakeHarness` substitute (no live LLM call).
- Cold-start catch-up (`crash-recovery.spec.ts`): a durable `user.message` is
  appended before any dispatcher exists, then one is constructed and processes
  it. Nothing is killed mid-flight — redelivery after a stop would need a second
  dispatcher, a rebalance and an uncommitted offset, and is not covered. See
  [`roadmap.md`](../../docs/managed-agents/roadmap.md).

## Configuration (env)

| Var                                    | Default                      | Meaning                                                                                                                                                                                                                                                                                                                                  |
| -------------------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HTTP_PORT`                            | 9094                         | Healthz HTTP port                                                                                                                                                                                                                                                                                                                        |
| `KAFKA_BROKERS`                        | `localhost:9092`             | Comma-separated Kafka bootstrap brokers                                                                                                                                                                                                                                                                                                  |
| `KAFKA_CLIENT_ID`                      | `harness-server`             | Kafka client identifier                                                                                                                                                                                                                                                                                                                  |
| `KAFKA_CONNECTION_MODE`                | `plaintext`                  | Kafka auth/TLS mode: `plaintext`, `sasl-plain-token-tls`, `sasl-plain-tls`, or `custom`                                                                                                                                                                                                                                                  |
| `KAFKA_AUTH_TOKEN`                     | (unset)                      | Token/JWT for the `sasl-plain-token-tls` and `sasl-plain-tls` modes                                                                                                                                                                                                                                                                      |
| `KAFKA_SASL_*` / `KAFKA_SSL_*`         | (unset)                      | Custom plain SASL/TLS knobs; see `services/dev/.env.example`                                                                                                                                                                                                                                                                             |
| `KAFKA_TOPIC_PREFIX`                   | (unset = bare names)         | Topic prefix as dot-terminated segments (e.g. `public.default.` for Kafka-on-Pulsar)                                                                                                                                                                                                                                                     |
| `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS`   | `30000`                      | Kafka topic discovery interval; must be a positive integer                                                                                                                                                                                                                                                                               |
| `HARNESS_CONSUMER_GROUP`               | `harness-server`             | Consumer-group ID; persistent across restarts                                                                                                                                                                                                                                                                                            |
| `ANTHROPIC_API_KEY`                    | (required for live LLM)      | Forwarded to the SDK                                                                                                                                                                                                                                                                                                                     |
| `ANTHROPIC_BASE_URL`                   | (SDK default)                | Override for proxies/staging                                                                                                                                                                                                                                                                                                             |
| `ANTHROPIC_MODEL_DEFAULT`              | `claude-sonnet-4-5-20250929` | Used when agentSnapshot doesn't pin a model                                                                                                                                                                                                                                                                                              |
| `NODE_ENV`                             | `production`                 | Gates the dev-only static-S3-credential fallback below. This service has no secret store; `ORCA_SECRET_STORE_MODE` is the registry's                                                                                                                                                                                                     |
| `KAFKA_SASL_MECHANISM`                 | (unset)                      | SASL mechanism for `KAFKA_CONNECTION_MODE=custom`                                                                                                                                                                                                                                                                                        |
| `TRANSCRIPT_STORE_POOL_MAX`            | `10`                         | Postgres pool size for the transcript backend                                                                                                                                                                                                                                                                                            |
| `FILESTORE_POOL_MAX`                   | `10`                         | Postgres pool size for file metadata                                                                                                                                                                                                                                                                                                     |
| `SANDBOX_HARNESS_CLAUDE_CODE_IMAGE`    | (catalog default)            | Overrides the shared `claude_code` / `codex_sdk` colocated sandbox image from `@orca/harness-catalog`                                                                                                                                                                                                                                    |
| `OPEN_SANDBOX_API_KEY`                 | (unset)                      | Credential for the OpenSandbox server, when it requires one                                                                                                                                                                                                                                                                              |
| `OPEN_SANDBOX_PROTOCOL`                | `http`                       | Transport the OpenSandbox client speaks                                                                                                                                                                                                                                                                                                  |
| `OPEN_SANDBOX_ENTRYPOINT`              | (image default)              | Comma-separated entrypoint override for the sandbox image                                                                                                                                                                                                                                                                                |
| `OPEN_SANDBOX_TIMEOUT_SECONDS`         | `1800`                       | Sandbox lifetime ceiling                                                                                                                                                                                                                                                                                                                 |
| `OPEN_SANDBOX_REQUEST_TIMEOUT_SECONDS` | `30`                         | Per-request timeout against the OpenSandbox server                                                                                                                                                                                                                                                                                       |
| `ORCA_LOCAL_FILE_OPERATION`            | (set per call)               | Set by `LocalSandboxRuntime` on the helper subprocess it spawns for each file operation: `write` or a read. Never set by an operator.                                                                                                                                                                                                    |
| `ORCA_LOCAL_FILE_ROOT`                 | (set per call)               | The canonical sandbox root the helper resolves paths under — the confinement boundary for that operation.                                                                                                                                                                                                                                |
| `ORCA_LOCAL_FILE_PATH`                 | (set per call)               | The canonical target path, already resolved under `ORCA_LOCAL_FILE_ROOT`.                                                                                                                                                                                                                                                                |
| `ENABLE_TOOL_SEARCH`                   | (unset)                      | Read only to detect an operator override. When a session selects remote MCP toolsets and this is unset, harness forces `ENABLE_TOOL_SEARCH=false` into the SDK subprocess so the model sees concrete `mcp__<server>__<tool>` names on the first turn instead of deferred tool search. Set it to any value to keep the SDK's own default. |
| `PULSAR_SERVICE_URL`                   | `pulsar://localhost:6650`    | Broker URL when `TRANSCRIPT_STORE_BACKEND=pulsar`                                                                                                                                                                                                                                                                                        |
| `PULSAR_RECEIVE_TIMEOUT_MS`            | `500`                        | Consumer receive timeout for the Pulsar backend                                                                                                                                                                                                                                                                                          |
| `PULSAR_ACK_TIMEOUT_MS`                | (unset = disabled)           | Redelivery timeout for un-acked Pulsar messages                                                                                                                                                                                                                                                                                          |
| `POSTGRES_EVENT_POLL_INTERVAL_MS`      | `500`                        | Claim-poll interval when `TRANSCRIPT_STORE_BACKEND=postgres`                                                                                                                                                                                                                                                                             |
| `POSTGRES_EVENT_LEASE_MS`              | `30000`                      | Claim lease duration; a failed handler is retried after it expires                                                                                                                                                                                                                                                                       |
| `OPEN_SANDBOX_RESOURCE_CPU`            | (runtime default)            | CPU request for `SANDBOX_RUNTIME=opensandbox` pods                                                                                                                                                                                                                                                                                       |
| `OPEN_SANDBOX_RESOURCE_MEMORY`         | (runtime default)            | Memory request for `SANDBOX_RUNTIME=opensandbox` pods                                                                                                                                                                                                                                                                                    |
| `AGENTENV_BASE_URL`                    | (unset)                      | AgentENV gateway URL; required for `SANDBOX_RUNTIME=agentenv`                                                                                                                                                                                                                                                                            |
| `AGENTENV_API_KEY`                     | (unset)                      | AgentENV lifecycle API key; retained by the trusted harness                                                                                                                                                                                                                                                                              |
| `AGENTENV_IMAGE`                       | (unset)                      | Default cold-sandbox OCI image; use the `orca-agentenv` template                                                                                                                                                                                                                                                                         |
| `AGENTENV_TIMEOUT_SECONDS`             | `1800`                       | Sandbox lifetime and resume TTL                                                                                                                                                                                                                                                                                                          |
| `AGENTENV_REQUEST_TIMEOUT_SECONDS`     | `180`                        | AgentENV lifecycle, envd, and file-transfer request timeout                                                                                                                                                                                                                                                                              |
| `AGENTENV_CPU_COUNT`                   | (runtime default)            | Optional Firecracker vCPU count                                                                                                                                                                                                                                                                                                          |
| `AGENTENV_MEMORY_MB`                   | (runtime default)            | Optional Firecracker memory in MiB                                                                                                                                                                                                                                                                                                       |
| `AGENTENV_DISK_SIZE_MB`                | (image size)                 | Optional OverlayBD disk size; smaller values fail when AgentENV shrinking is disabled                                                                                                                                                                                                                                                    |
| `ALLOW_INSECURE_STATIC_S3_CREDS`       | `false`                      | Permits the dev-only static-credential fallback when `S3_STS_ROLE_ARN` is unset. Never set this in production — it hands the sandbox long-lived keys instead of per-session STS credentials.                                                                                                                                             |

For a `separate` Session, `LLM_EGRESS_DEFAULT` selects the deployment default.
A Session's `metadata.orca_llm_egress` value overrides it. Gateway-selected
Sessions use these model-egress environment values:

| Var                        | Default                         | Meaning                                                                                                                    |
| -------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `LLM_EGRESS_DEFAULT`       | `direct`                        | Default for separate Sessions with no metadata override. Must be `direct` or `gateway`.                                    |
| `LLM_GATEWAY_URL`          | `http://localhost:8090/v1`      | Gateway LLM base URL. The Harness removes a final `/v1` before the Claude SDK appends `/v1/messages`.                      |
| `ANTHROPIC_AUTH_TOKEN`     | (set per gateway Session query) | Registry-minted Session JWT passed only to that Claude SDK subprocess. Do not configure it as a shared Harness credential. |
| `ANTHROPIC_CUSTOM_HEADERS` | (set per gateway Session query) | Supplies `X-Orca-Session-Id` to the Claude SDK subprocess. Do not configure it as a shared Harness header.                 |

### Kafka transcript Avro and Schema Registry

Kafka defaults to raw payload bytes in `.events` topics. `KAFKA_TRANSCRIPT_ENCODING=avro`
selects `.events-avro` for all session reads and writes; a Registry URL alone leaves
raw topic selection unchanged, even though the codec can decode Avro frames.
Registry, harness and exporter must use the same encoding and undergo a full
coordinated restart after existing turns are quiesced. This incompatible cutover
does not migrate old history or preserve existing session replay/cursors; create
fresh sessions. These settings are host-service configuration, never worker,
session-runner, or sandbox credentials.

| Variable                                   | Default                           | Behavior                                                                                  |
| ------------------------------------------ | --------------------------------- | ----------------------------------------------------------------------------------------- |
| `KAFKA_TRANSCRIPT_ENCODING`                | `raw`                             | Selects the raw `.events` or Avro `.events-avro` topic set; Avro requires a Registry URL. |
| `KAFKA_SCHEMA_REGISTRY_URL`                | unset                             | Confluent-compatible endpoint, including an optional path prefix.                         |
| `KAFKA_SCHEMA_REGISTRY_SUBJECT`            | `orca.transcript.TranscriptEvent` | Shared deployment-level subject across session topics.                                    |
| `KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER`      | `true`                            | Avro writers register the shipped schema; false performs exact-schema lookup.             |
| `KAFKA_SCHEMA_REGISTRY_AUTH_MODE`          | `none`                            | `none` or `basic`; credentials require HTTPS.                                             |
| `KAFKA_SCHEMA_REGISTRY_USERNAME`           | unset                             | Basic username, supplied from a Secret.                                                   |
| `KAFKA_SCHEMA_REGISTRY_PASSWORD`           | unset                             | Basic password, independent of broker SASL credentials.                                   |
| `KAFKA_SCHEMA_REGISTRY_CA_FILE`            | unset                             | Custom CA PEM path in this service container.                                             |
| `KAFKA_SCHEMA_REGISTRY_CERT_FILE`          | unset                             | mTLS certificate PEM; requires the key file.                                              |
| `KAFKA_SCHEMA_REGISTRY_KEY_FILE`           | unset                             | mTLS private-key PEM; requires the certificate file.                                      |
| `KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS` | `5000`                            | Positive per-request timeout in milliseconds.                                             |

Registry companion settings without a URL, Avro without a URL, invalid enums,
and enabled Kafka settings with a non-Kafka backend fail configuration validation.
TLS certificate validation is always enabled. Local unauthenticated HTTP is allowed;
URL userinfo, query strings and fragments are rejected. Basic requires both credentials;
certificates and keys are paired. File paths require explicit read-only mounts via the
service's Helm `extraVolumes` / `extraVolumeMounts`; paths do not create mounts.

See [Kafka transcript encoding, rollout and external consumers](../../docs/managed-agents/libraries/transcript-store.md#optional-kafka-avro-envelope).

### The local runtime's sandbox environment

`SANDBOX_RUNTIME=local` does not hand the agent the host environment. It builds
a fresh one (`sandbox/local/runtime.ts:379`) containing exactly four assignments
and an allowlist copied from the host:

| Set by the runtime      | Value                                             |
| ----------------------- | ------------------------------------------------- |
| `HOME`                  | the session's sandbox working directory           |
| `TMPDIR`, `TMP`, `TEMP` | the session's temp directory, all three spellings |

Forwarded from the host only when the host has them set: `PATH`, `TZ`, `TERM`,
`COLORTERM`, `LANG`, `LANGUAGE`, and the `LC_*` family — `LC_ALL`, `LC_COLLATE`,
`LC_CTYPE`, `LC_MESSAGES`, `LC_MONETARY`, `LC_NUMERIC`, `LC_TIME`.

Anything else the host exports — credentials, cloud tokens, `NODE_*`, the
operator's own shell state — is absent by construction. The allowlist is the
whole interface: a variable an agent needs at runtime has to be added there
explicitly, not exported before launching harness-server.

## Known limitations

- **Kafka topic discovery is polling-based.** Both bare and prefixed paths
  pass each `admin.listTopics()` snapshot as an explicit canonical topic list;
  no Kafka regex subscription is used. `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS`
  defaults to `30000` and Kafka startup rejects non-positive values.
- **Dispatcher shutdown is bounded.** Kafka transition/disconnect,
  Postgres/Pulsar source join, and parallel runner cleanup share one 10-second
  grace; source quiesce and runner cancellation start together. Expiry logs a
  fixed sanitized diagnostic and detaches late cleanup. Postgres and Pulsar
  `status()` is consulted by `/readyz` after startup; terminal failures also
  trigger re-entrant controlled shutdown with exit code 1, while `/healthz`
  stays liveness-only during shutdown.
- **No persistent consumer-group offsets in this image.** A fresh deployment
  resets to `fromBeginning: true` and replays all historical events. The Kafka
  cluster's offset retention applies; for production, set a stable `groupId` so
  the cluster persists committed offsets across replicas.
- **API key from env** on the `separate` path. The `colocated` path routes
  through ai-gateway with a per-session JWT instead.
- **No outcomes** — `Session.outcome` always returns `null`; it exists for SDK
  compatibility only.

## MCP routing

When a session starts, harness-server prepares one workspace-scoped execution
snapshot through the registry's internal API, mints a session-scoped JWT, and
rewrites every `mcp_servers[].url` to the MCP endpoint derived from
`AI_GATEWAY_URL` (`${AI_GATEWAY_URL}/v1/mcp`, with legacy endpoint inputs
preserved). The shared harness authenticates as an internal workload; it never
uses a public workspace API key or Registry API-key environment variable. The
workspace comes from the consumed event and is cross-checked by
workspace/session-scoped registry calls before a sandbox starts.

| Var                          | Default                 | Meaning                                           |
| ---------------------------- | ----------------------- | ------------------------------------------------- |
| `REGISTRY_INTERNAL_BASE_URL` | `http://localhost:8081` | Mesh-internal registry endpoint for `/internal/*` |
| `AI_GATEWAY_URL`             | `http://localhost:8090` | Gateway base URL used to build the MCP endpoint   |

## File resources and `agent_toolset`

When a session attaches `resources: [{type: 'file', file_id, mount_path, access}]`:

The public API accepts only `mount_strategy: 'tarball_prefetch'` for file
resources. File bytes are fetched through the workspace-bound file store and
never exposed to the sandbox through an S3 policy.

1. Dispatcher fetches `session.resources[]` from registry.
2. Acquires a sandbox via the `SandboxRuntime` selected by `SANDBOX_RUNTIME`
   (see [Sandbox runtimes](#sandbox-runtimes) above).
3. For each file resource, calls `TarballPrefetchStrategy.activate(sandbox, resource)`:
   - Streams bytes from `@orca/file-store` (workspace-scoped).
   - Writes them at `mount_path` via `SandboxHandle.files.write`.
4. Emits `session.resource_mounted` events through transcript-store.
5. Builds the agent_toolset (`bash`, `read`, `write`, `edit`, `glob`, `grep`, `web_fetch`)
   bound to the sandbox handle and registers them as Claude Agent SDK custom tools.

### Snapshot lifecycle

`MountStrategy` declares `teardownForSnapshot` and `restoreAfterSnapshot`, and
all four strategies implement them, but no production path calls either — the
only callers are tests. When a caller is added it must run `teardownForSnapshot`
before `SandboxHandle.pause()` and `restoreAfterSnapshot` before the next tool
dispatch on resume. For `TarballPrefetchStrategy` both are no-ops (E2B persists
the sandbox FS across pause/resume).

### Configuration (env vars)

| Var                               | Default                                         | Purpose                                                   |
| --------------------------------- | ----------------------------------------------- | --------------------------------------------------------- |
| `FILESTORE_DATABASE_URL`          | `postgres://orca:orca@localhost:5432/filestore` | Postgres for file metadata                                |
| `S3_ENDPOINT`                     | `http://localhost:9000`                         | S3-compatible blob backend                                |
| `S3_BUCKET`                       | `orca-files`                                    | Bucket name                                               |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin` / `minioadmin`                     | Credentials                                               |
| `S3_REGION`                       | `us-east-1`                                     | AWS region label (any string for MinIO)                   |
| `SANDBOX_RUNTIME`                 | (required — no default)                         | `local`, `e2b`, `opensandbox`, `agentenv`, or `in-memory` |
| `E2B_API_KEY`                     | (required when `SANDBOX_RUNTIME=e2b`)           | E2B credentials                                           |

### Limitations (v1)

- `agent_toolset.web_fetch` runs from the harness host, not inside the sandbox,
  and does not pass through ai-gateway.

## Session output capture

S3 FUSE is an internal runtime capability for writable memory mounts and
session-output capture; it is not a public file mount strategy. Public file
resources always use `tarball_prefetch`. On FUSE-capable runtimes
(`E2BSandboxRuntime` with `orca-default`, or `OpenSandboxRuntime` with
gVisor-provided `/dev/fuse`), bytes
written to `/mnt/session/outputs/` land in the current execution prefix and are
auto-registered as `purpose='agent_output'` File rows scoped to the session.
Local, InMemory, and AgentENV runtimes use a local `/mnt/session/outputs/` directory that the
indexer walks after each local or MCP tool result and once more at session end.

All object-store users share one operator-configured root, `S3_KEY_PREFIX`
(`{root}` below). Workspace ownership is always encoded immediately below it:

```text
{root}workspaces/{workspace_id}/files/blobs/{aa}/{bb}/{sha256}/content
{root}workspaces/{workspace_id}/memory-stores/{store_id}/live/{path}
{root}workspaces/{workspace_id}/memory-stores/{store_id}/versions/{sha256}
{root}workspaces/{workspace_id}/sessions/{session_id}/executions/{generation_id}/outputs/{path}
```

Session STS policies grant only the current execution-output prefix and the
live prefixes of attached memory stores. They do not grant access to file
blobs.

See [`docs/managed-agents/output-capture.md`](../../docs/managed-agents/output-capture.md)
for the full data flow + indexer SLO target.

### Output-capture envs

| Var                                         | Default                | Meaning                                                                                                                                                                      |
| ------------------------------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `S3_BUCKET`                                 | (unset → FUSE skipped) | When set, dispatcher mints per-session creds + mounts outputs + indexes after tool results and on stop                                                                       |
| `S3_KEY_PREFIX`                             | `managed-agents/`      | Single deployment root for workspace-owned files, memory stores, and execution outputs                                                                                       |
| `S3_ENDPOINT`                               | (unset)                | S3 / MinIO data-plane endpoint used by the S3 SDK and `s3fs -o url=`                                                                                                         |
| `S3_FORCE_PATH_STYLE`                       | `true`                 | Path-style bucket addressing for host-side AWS SDK clients and sandbox s3fs mounts; set `false` for AWS S3                                                                   |
| `S3_STS_ENDPOINT`                           | (AWS SDK default)      | Optional STS endpoint override; independent from `S3_ENDPOINT`                                                                                                               |
| `S3_REGION`                                 | (SDK default)          | Region passed to STS / S3 client                                                                                                                                             |
| `S3_STS_ROLE_ARN`                           | (unset)                | When set, AssumeRole mints per-session creds; static fallback requires explicit development/test opt-in                                                                      |
| `S3_ACCESS_KEY_ID` (or `S3_ACCESS_KEY`)     | (unset)                | Optional static access key for host-side S3 and STS clients. When pair is absent, both use AWS default credential chain (including IRSA); dev fallback needs explicit opt-in |
| `S3_SECRET_ACCESS_KEY` (or `S3_SECRET_KEY`) | (unset)                | Static S3 secret key                                                                                                                                                         |
| `E2B_TEMPLATE_ID`                           | (E2B SDK default)      | Custom template id from `e2b template build`; bakes in `s3fs-fuse` + `fuse3`                                                                                                 |

### Local dev quick-start

`make dev-up` provisions RustFS with bucket `orca-files` and root credentials
`minioadmin/minioadmin`. The harness-server's `SessionCredsMinter` falls back
to those static creds when `S3_STS_ROLE_ARN` is unset — that's dev-only;
production uses real STS AssumeRole. The local stack explicitly sets
`S3_STS_ENDPOINT` to the same RustFS URL as `S3_ENDPOINT`; production should
normally leave it unset so the AWS SDK uses the regional STS endpoint.

```bash
make dev-up
S3_BUCKET=orca-files \
S3_ENDPOINT=http://localhost:9000 \
S3_KEY_PREFIX=managed-agents/ \
S3_REGION=us-east-1 \
S3_ACCESS_KEY_ID=minioadmin \
S3_SECRET_ACCESS_KEY=minioadmin \
KAFKA_BROKERS=localhost:9092 \
ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
pnpm -F @orca/harness-server dev
```

The end-to-end smoke is POST file → POST session → POST `user.message`
("write to /mnt/session/outputs/x.txt") → `GET /v1/files?scope_id=$SES`. See
[`docs/managed-agents/output-capture.md`](../../docs/managed-agents/output-capture.md)
for the data flow and the indexer SLO target.

## Memory-store mounts and the version watcher

Memory mounts reuse the same custom E2B template (`s3fs` + libfuse +
constrained mount-helper sudoers) as output capture. When a session attaches a
`memory_store`,
the dispatcher mounts each store at `/mnt/memory/{store_name}/` via
`MemoryFuseStrategy` (FUSE-capable runtimes) or `LocalMemoryStrategy`
(InMemory test fallback). A per-session `MemoryVersionWatcher` polls each
store's S3 prefix every ~2 s, registers detected writes via
the workspace/session-scoped memory-version endpoint, and emits `session.memory_conflict`
events on CAS mismatch.

The watcher stops **before** mount deactivation in the stop sequence so an
in-flight poll never runs against a torn-down FUSE mount. See
[`docs/managed-agents/services/harness-server.md`](../../docs/managed-agents/services/harness-server.md)
and [`docs/managed-agents/libraries/memory-store.md`](../../docs/managed-agents/libraries/memory-store.md).

### Memory-mount configuration

Memory stores use the same `S3_KEY_PREFIX` root documented above.

The watcher's 2 s tick is fixed in `MemoryVersionWatcher` and reads no
environment variable; it bounds the cross-session consistency window.

### Validation target

The cross-session e2e test
(`test/integration/memory-cross-session.spec.ts`) is the canonical
validation: session A writes through the sandbox FS at
`/mnt/memory/{store}/`, the watcher registers the new version through
the scoped memory-version endpoint, and session B reads the bytes back through
its own mount.

## GitHub repository mounts and in-sandbox git

GitHub mounts reuse the same custom E2B template (`orca-default`). The template
adds three things for them:
`git`, `jq`, and the `/usr/local/bin/orca-git-creds` credential helper
script. **Operators must rebuild and push the template (`e2b template build`, then
update `E2B_TEMPLATE_ID`) after any Dockerfile change** before in-sandbox
`git push` works in production. See
[`./sandbox-templates/orca-default/README.md`](./sandbox-templates/orca-default/README.md)
for the rebuild + spike checklist.

The dev / `InMemorySandboxRuntime` test path does NOT need the template:
`GitCloneStrategy`'s integration tests inject a fake `GitWorker` and the
credential helper isn't exercised there.

When a session attaches `resources: [{type: 'github_repository', url, authorization_token: 'git_cred://<id>', mount_path?, checkout?}]`:

1. Dispatcher mints a session-scoped `aud='git-creds'` JWT carrying
   `repo_urls[]` (the URLs the dispatcher saw on the session).
2. Writes `/etc/profile.d/orca-git-creds.sh` (exporting `ORCA_GIT_CREDS_URL`
   and `ORCA_GIT_CREDS_TOKEN`) into the sandbox. The image already registers
   `/usr/local/bin/orca-git-creds` as the system-wide helper; without these
   per-session env vars it emits no credentials and Git remains anonymous.
3. Resolves the git credential PAT through the workspace/session-scoped internal route,
   runs `git clone --filter=blob:none --depth=1` into
   `${HARNESS_WORK_DIR}/sessions/{ws}/{ses}/repo-{N}/`, resets the remote
   URL to the bare form, then streams the working tree + `.git/` into the
   sandbox at `mount_path`.
4. Agent runs `bash + git status / diff / push / fetch` directly. The
   helper round-trips through registry's public `POST /v1/git-creds` per
   call. The PAT never lands in the sandbox.
5. `SessionRunner.stop` calls `WorkDirManager.releaseSession` to `rm -rf`
   the per-session host work dir.

See [`../../docs/managed-agents/services/harness-server.md`](../../docs/managed-agents/services/harness-server.md)
for the full lifecycle, and
[`../../docs/managed-agents/services/registry-service.md`](../../docs/managed-agents/services/registry-service.md)
`/v1/git-creds` for the registry-side route.

### Git envs

| Var                    | Default                      | Meaning                                                                                                                                                                                                                            |
| ---------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HARNESS_WORK_DIR`     | `/var/tmp/orca-harness`      | Host-side base dir for per-session ephemeral repos. `WorkDirManager` allocates `<base>/sessions/{ws}/{ses}/repo-{N}/` per repo and `rm -rf`s the session subtree on stop.                                                          |
| `GIT_CREDS_PUBLIC_URL` | (unset → required when used) | Public registry URL the in-sandbox helper POSTs to (e.g. `https://api.example.com/v1/git-creds`). **Required when any session attaches a `github_repository` resource** — when unset the dispatcher refuses to spawn that session. |
