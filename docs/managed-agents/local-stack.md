# Local Stack

The local-stack release of orca-managed-agents brings up the full hybrid
topology — compose-managed Postgres, RustFS, the selected transcript broker
when one is needed, and the external `ai-gateway` image; native
`registry-service-ts` and `harness-server` — with one command.
This doc is the operator's reference: when to use it, what to set, how to
bring it up, how to run the e2e suite, and what to do when something goes
wrong.

For the design rationale and the runtime contracts the stack implements, read
[`overview.md`](./overview.md) and [`architecture.md`](./architecture.md)
first — this doc focuses on the local mechanics.

## Why this exists / when to use it

- **You are developing or debugging the platform itself** — running the agent
  loop end-to-end, exercising the registry → transcript store → harness →
  Anthropic → ai-gateway chain locally without paying for a cloud E2B
  template.
- **You are running the e2e suite** (`pnpm e2e:wire`, `pnpm e2e:agent`) which
  drives the registry over raw HTTP and asserts
  cross-service contracts that unit + per-package integration suites can't
  prove on their own.
- **You are reproducing an issue someone reported against `make stack-up`**
  (the same path CI runs under
  [`.github/workflows/e2e-stack.yml`](../../.github/workflows/e2e-stack.yml)).

If you only need infra, run `make dev-up` instead — it boots Postgres + RustFS
plus the broker required by `TRANSCRIPT_STORE_BACKEND` and stops there. See
[`services/dev/README.md`](../../services/dev/README.md) for the per-target
breakdown.

## Hybrid topology

```
docker-compose (services/dev/docker-compose.yml)
  ├── postgres:16          (registry, transcriptstore, filestore, memorystore DBs)
  ├── kafka  (KRaft 3.7.1) (default transcript backend; per-session topics)
  ├── pulsar               (optional transcript backend)
  ├── rustfs               (bucket: orca-files; keys: minioadmin/minioadmin)
  └── ai-gateway           http://localhost:8090/v1/mcp (admin :9099)

native (services/dev/scripts/start-services.sh — backgrounded with pid files)
  ├── registry-service-ts  http://localhost:8080  (public REST + SSE)
  ├── harness-server       http://localhost:9094  (internal /healthz only)
```

Why hybrid: `LocalSandboxRuntime` (in `harness-server`) wraps the agent's
`bash` invocations with `sandbox-exec` (macOS) or `bubblewrap` (Linux). Both
need direct host kernel access, so running the harness inside a container
would force container-in-container — a nightmare for both performance and
permissions. The compromise is to keep the heavyweight infra
(Postgres / RustFS / optional broker / ai-gateway) in containers and put the two
in-repo app services on the host. See
[`services/harness-server/src/sandbox/local/README.md`](../../services/harness-server/src/sandbox/local/README.md)
for the sandbox runtime details.

## Prerequisites

| Tool                                    | Required for                     | Install (macOS)                                | Install (Debian/Ubuntu)                        |
| --------------------------------------- | -------------------------------- | ---------------------------------------------- | ---------------------------------------------- |
| Docker (compose plugin)                 | infra                            | Docker Desktop                                 | `apt install docker.io docker-compose-plugin`  |
| Node.js >= 22                           | TS services + e2e                | `brew install node`                            | NodeSource (`nodesource.com`) or `nvm`         |
| pnpm 9                                  | workspace mgr                    | `npm install -g pnpm@9.15.9`                   | `npm install -g pnpm@9.15.9`                   |
| `srt` (`@anthropic-ai/sandbox-runtime`) | LocalSandboxRuntime              | `npm install -g @anthropic-ai/sandbox-runtime` | `npm install -g @anthropic-ai/sandbox-runtime` |
| `bubblewrap`                            | LocalSandboxRuntime (Linux only) | n/a (use sandbox-exec)                         | `apt install bubblewrap`                       |
| `lsof`                                  | start-services preflight         | shipped                                        | `apt install lsof`                             |

`pnpm install` builds the [patched Pulsar native binding](libraries/transcript-store.md#pulsar-topology),
including when the selected transcript backend is Kafka or Postgres. Install
Xcode Command Line Tools (`xcode-select --install`) and Python 3 on macOS; on
Debian/Ubuntu install `python3 make g++ binutils xz-utils`. Builds download
checksum-pinned Apache C++ client archives and Node headers.

The script `services/dev/scripts/start-services.sh` checks for `pnpm`, `node`,
`docker`, `openssl`, `curl`, and `lsof` on PATH and bails with a
clear error message if anything is missing.

`SANDBOX_RUNTIME=local` (the default) requires `srt`. If you use
`SANDBOX_RUNTIME=in-memory` instead, you can skip the `srt` install — but the
harness then has no privilege boundary, which is fine for unit-style tests
and not safe for anything that touches the real filesystem.

## Environment variables

`services/dev/.env.example` documents every var with a sane default; copy it
to `services/dev/.env` and tweak. The vars that change behavior most often:

| Variable                                                 | Default                                               | What it does                                                                                                                                                                                                                                                                     |
| -------------------------------------------------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ANTHROPIC_API_KEY`                                      | _(unset)_                                             | Required by Claude Layer B / Claude sessions with direct egress. Without it the harness boots but the first session fails with an unauthenticated Anthropic error. Get one from https://console.anthropic.com/.                                                                  |
| `OPENAI_API_KEY`                                         | _(unset)_                                             | Provider key for `codex_sdk` direct egress in harness-server. Gateway egress uses a scoped Session JWT instead.                                                                                                                                                                  |
| `OPENAI_BASE_URL`                                        | _(unset)_                                             | Optional Responses-compatible base URL for `codex_sdk` direct egress; the SDK default is `https://api.openai.com/v1`.                                                                                                                                                            |
| `SANDBOX_RUNTIME`                                        | `local`                                               | One of `local` (srt-managed sandbox-exec/bubblewrap), `e2b` (cloud sandboxes; also requires `E2B_API_KEY` + `E2B_TEMPLATE_ID`), `opensandbox` (remote OpenSandbox server), `agentenv` (remote AgentENV gateway), or `in-memory` (test-only, no isolation).                       |
| `TRANSCRIPT_STORE_BACKEND`                               | `kafka`                                               | One of `kafka`, `postgres`, or `pulsar`. `make dev-up` starts Kafka only for Kafka mode, Pulsar only for Pulsar mode, and no broker for Postgres mode.                                                                                                                           |
| `TRANSCRIPT_STORE_DATABASE_URL`                          | `postgres://orca:orca@localhost:5432/transcriptstore` | Postgres transcript event DB used when `TRANSCRIPT_STORE_BACKEND=postgres`.                                                                                                                                                                                                      |
| `PULSAR_AUTH_TYPE`                                       | _(unset)_                                             | Pulsar-only auth mode. Leave unset for unauthenticated dev Pulsar; use `token` with `PULSAR_AUTH_TOKEN` or `oauth2` with the `PULSAR_OAUTH2_*` variables.                                                                                                                        |
| `S3_ENDPOINT`                                            | `http://localhost:9000`                               | S3 data-plane endpoint (the local RustFS by default) used by the S3 clients and sandbox mounts.                                                                                                                                                                                  |
| `S3_STS_ENDPOINT`                                        | `http://localhost:9000`                               | Local-only explicit STS override. Production normally leaves this unset so the AWS SDK selects its default STS endpoint; it is never inferred from `S3_ENDPOINT`.                                                                                                                |
| `HARNESS_WORK_DIR`                                       | `/var/tmp/orca-harness`                               | Per-session work-dirs land at `{HARNESS_WORK_DIR}/sessions/{workspaceId}/{sessionId}/`, one `repo-{n}/` subdirectory per attached repository. The local sandbox restricts writes to this tree.                                                                                   |
| `OPEN_SANDBOX_DOMAIN`                                    | _(unset)_                                             | Required when `SANDBOX_RUNTIME=opensandbox`. For local-to-GKE debugging, run `kubectl -n opensandbox-system port-forward svc/opensandbox-server 18080:80` and set this to `localhost:18080`.                                                                                     |
| `OPEN_SANDBOX_IMAGE`                                     | _(unset)_                                             | Required when `SANDBOX_RUNTIME=opensandbox`. Use the Orca image described in `services/harness-server/sandbox-templates/orca-opensandbox/README.md`; it adds `bubblewrap`, `s3fs`, and `fuse3`.                                                                                  |
| `OPEN_SANDBOX_USE_SERVER_PROXY`                          | `true`                                                | Routes command/files traffic through `opensandbox-server`, which is required when the local harness cannot reach sandbox pod IPs directly.                                                                                                                                       |
| `AGENTENV_BASE_URL`                                      | _(unset)_                                             | Required when `SANDBOX_RUNTIME=agentenv`; points at the AgentENV gateway, for example a local port-forward at `http://127.0.0.1:18080`.                                                                                                                                          |
| `AGENTENV_API_KEY`                                       | _(unset)_                                             | Required AgentENV lifecycle credential. The harness retains it and creates each sandbox with a separate envd access token.                                                                                                                                                       |
| `AGENTENV_IMAGE`                                         | _(unset)_                                             | Required OCI image. Use the image contract under `services/harness-server/sandbox-templates/orca-agentenv/`.                                                                                                                                                                     |
| `SESSION_IDLE_TIMEOUT_MS`                                | `60000`                                               | Milliseconds to keep a runner/sandbox warm after a completed turn. The session is already `idle`; timeout destroys the sandbox and clears `sandbox_handle_id`.                                                                                                                   |
| `ORCA_E2E_PLAINTEXT_KEY`                                 | random `orca_e2e_<nano>` per run                      | When set, `seedWorkspaceApiKey()` upserts the e2e API client's workspace key with deterministic plaintext. It is never exposed to the Harness.                                                                                                                                   |
| `KAFKA_CONNECTION_MODE`                                  | `plaintext`                                           | Kafka transcript client security mode. Use `sasl-plain-token-tls` (SASL/PLAIN over TLS with a token-prefixed password), `sasl-plain-tls` (SASL/PLAIN over TLS), or `custom` with the auth/TLS vars below for remote endpoints.                                                                                                               |
| `KAFKA_AUTH_TOKEN`                                       | _(unset)_                                             | JWT/token used by `sasl-plain-token-tls` (`password=token:<jwt>`) and `sasl-plain-tls` (`password=<jwt>`).                                                                                                                                                                    |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD`            | _(unset)_                                             | SASL username/password. `sasl-plain-token-tls` mode defaults username to `public`; custom mode requires both when `KAFKA_SASL_MECHANISM=plain`.                                                                                                                                     |
| `KAFKA_SSL_*`                                            | _(unset)_                                             | Custom-mode TLS knobs: `KAFKA_SSL`, `KAFKA_SSL_REJECT_UNAUTHORIZED`, `KAFKA_SSL_CA_FILE`, `KAFKA_SSL_CERT_FILE`, `KAFKA_SSL_KEY_FILE`.                                                                                                                                           |
| `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS`                     | `5000` for Kafka, `0` otherwise                       | Kafka-only: harness discovers canonical session topics every N ms and re-subscribes its one consumer-group member from that explicit list. Kafka config rejects non-positive values; production defaults to `30000`.                                                             |
| `REGISTRY_HTTP_PORT`                                     | `8080`                                                | Override only if `8080` is in use locally.                                                                                                                                                                                                                                       |
| `REGISTRY_INTERNAL_HTTP_PORT`                            | `8081`                                                | The internal listener harness-server calls. Override alongside `REGISTRY_HTTP_PORT` when running two stacks.                                                                                                                                                                     |
| `REGISTRY_ADMIN_HTTP_PORT`                               | `8082`                                                | The admin listener. Same reason to override.                                                                                                                                                                                                                                     |
| `REGISTRY_BASE_URL`                                      | `http://localhost:${REGISTRY_HTTP_PORT}`              | Derived from the port above; set it directly only to point the stack at a registry it did not start.                                                                                                                                                                             |
| `REGISTRY_ADMIN_BASE_URL`                                | `http://localhost:${REGISTRY_ADMIN_HTTP_PORT}`        | The admin equivalent, derived the same way.                                                                                                                                                                                                                                      |
| `AI_GATEWAY_DATA_PORT`                                   | derived from `AI_GATEWAY_URL` (`8090`)                | Not a knob: the launcher parses the port out of `AI_GATEWAY_URL` (`url_port` in `start-services.sh`) to health-check the data plane. Change `AI_GATEWAY_URL`, not this.                                                                                                                    |
| `AI_GATEWAY_COMPAT_IMAGE`                                | `orca-ai-gateway-compat:dev`                          | Tag for the locally-built compatibility image (`prepare-ai-gateway-image.sh:15`).                                                                                                                                                                                                |
| `HARNESS_HTTP_PORT`                                      | `9094`                                                | Override only if `9094` is in use locally.                                                                                                                                                                                                                                       |
| `AI_GATEWAY_URL`                                         | `http://localhost:8090`                               | Gateway base URL used to build the MCP endpoint for SDK calls.                                                                                                                                                                                                                   |
| `LLM_GATEWAY_URL`                                        | `http://localhost:8090/v1`                            | Anthropic-compatible ai-gateway base URL injected into in-sandbox harnesses. Provider API keys stay in ai-gateway; sandbox harnesses call `/v1/messages`.                                                                                                                        |
| `SESSION_JWT_LLM_ROUTES` / `SESSION_JWT_LLM_MODELS`      | `llm-messages` / `claude-sonnet-4-6`                  | Registry-owned, comma-separated LLM policy. Model patterns are intersected with the session's concrete primary and coordinator-subagent models. Configure both together; when both are unset, Registry omits the LLM claims. Gateway ACLs require these claims for LLM requests. |
| `AI_GATEWAY_ADMIN_PORT`                                  | `9099`                                                | Admin health port used by `make stack-up` readiness checks.                                                                                                                                                                                                                      |
| `AI_GATEWAY_IMAGE`                                       | `ghcr.io/orca-ae/orca-ai-gateway:v0.4.3-rc.3`         | Gateway image with JWT-derived LLM route/model authorization, HTTP `bearer_token_file`, and wildcard MCP `destination_resolver` support. `start-services.sh` probes the required contracts.                                                                                      |
| `INTERNAL_SERVICE_TOKEN` / `INTERNAL_SERVICE_TOKEN_FILE` | generated runtime file                                | Optional non-Kubernetes service-token source. Set exactly one to supply an operator-managed value; otherwise `stack-up` creates `services/dev/run/internal-service-token`.                                                                                                       |

The full list (S3 keys, Postgres URLs, JWT settings, ai-gateway URL/image) lives
in `services/dev/.env.example`.

## The self-hosted (colocated) stack

`make stack-up` runs the **`separate`** path: registry + harness-server + ai-gateway, on
whichever broker `TRANSCRIPT_STORE_BACKEND` names. `make self-hosted-up` is a different
stack, not a variant of it — it runs the **`colocated`** path, where the registry itself
coordinates the session over the worker tunnel. It therefore needs no harness-server, no
ai-gateway and no broker, and its compose file
(`services/dev/docker-compose.self-hosted.yml`) is Postgres + RustFS only.

It also does something `stack-up` does not: it creates a `target=self_hosted` Environment
over the public API and attaches **this machine** to it by running `oeadm worker`. A
session for a `colocated` agent then runs its loop and its tools here, on your hardware.

```bash
make self-hosted-up            # infra + registry + this machine as an environment
pnpm e2e:self-hosted           # the LLM-free `mock` provider, end to end
make self-hosted-down
```

The registry and the worker stay running behind pid files in `services/dev/run` after the
command returns — the same posture as `stack-up`. The created environment's id and its
one-time Env Key are written to `services/dev/run/self-hosted.env` (mode 600); the e2e
reads the id from there.

Re-running is safe and mints a **new** Environment each time: an Env Key is echoed exactly
once at create and never stored, so a previous environment's key cannot be recovered and
its row cannot be reused.

| Variable                          | Default                    | What it does                                                                                                                                                                                                                                                                                                        |
| --------------------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FRESH`                           | _(unset)_                  | `FRESH=1` discards the Postgres + RustFS volumes before starting. Everything this stack persists is derived, so the only thing a stale volume carries is a schema from an older checkout — which surfaces as a confusing migration failure. `make self-hosted-up FRESH=1`.                                          |
| `SELF_HOSTED_POSTGRES_PORT`       | `5432`                     | Host port the compose publishes Postgres on. The four database URLs derive from it, so this one variable moves them all.                                                                                                                                                                                            |
| `SELF_HOSTED_RUSTFS_PORT`         | `9000`                     | Host port for RustFS's S3 API; `S3_ENDPOINT` derives from it.                                                                                                                                                                                                                                                       |
| `SELF_HOSTED_RUSTFS_CONSOLE_PORT` | `9001`                     | Host port for the RustFS console. Published only so the two stacks can coexist.                                                                                                                                                                                                                                     |
| `SELF_HOSTED_ENVIRONMENT_NAME`    | `self-hosted-<random hex>` | Name of the Environment the script creates. Random per run because the registry enforces name uniqueness per workspace and every run creates a new one; pin it only if you want a recognizable name and are not re-running.                                                                                         |
| `SELF_HOSTED_SKIP_WORKER_WAIT`    | _(unset)_                  | Skip the post-launch poll on `work_stats.worker_connected`. The wait is what turns "the worker never attached" into an error at bring-up rather than a turn timeout later, so skip it only when you intend to attach a worker yourself.                                                                             |
| `ORCA_RUNNER_ENV_PASSTHROUGH`     | see below                  | Comma-separated env names the worker forwards worker→runner, ON TOP of its allowlist. The runner inherits an allowlist, not the worker's environment, so a model credential reaches a provider only if it is named here. Defaults to `ANTHROPIC_API_KEY,OPENAI_API_KEY,ANTHROPIC_BASE_URL,ANTHROPIC_MODEL_DEFAULT`. |
| `MEMORY_KEY_PREFIX`               | `memory/`                  | Object-store key prefix for memory-store blobs.                                                                                                                                                                                                                                                                     |
| `OUTPUTS_KEY_PREFIX`              | `outputs/`                 | Object-store key prefix for session output files.                                                                                                                                                                                                                                                                   |

`REGISTRY_HTTP_PORT`, `REGISTRY_INTERNAL_HTTP_PORT` and `REGISTRY_ADMIN_HTTP_PORT` work
here exactly as they do for `stack-up`. All three are pre-flighted before anything starts:
the registry binds three listeners, and a clash on any of them used to surface as a
120-second health-check timeout that named no port.

## Bring-up walkthrough

```bash
# 1. one-time
pnpm install --frozen-lockfile
cp services/dev/.env.example services/dev/.env
$EDITOR services/dev/.env                    # set ANTHROPIC_API_KEY

# 2. each session
make stack-up                                # ~60s on a warm machine
make stack-status                            # confirm /healthz on all three

# 3. run e2e
pnpm e2e:wire                                # Layer A — fast (~30s)
pnpm e2e:agent                               # Layer B — paid (~6min observed)

# 4. tear down
make stack-down                              # SIGTERM with 10s grace, then compose down
```

Internally, `make stack-up` chains:

1. `make dev-up` → `docker compose up -d` for Postgres + RustFS + the selected
   transcript broker, if any.
2. `make secrets` → `services/dev/scripts/init-secrets.sh` — generates an
   RSA-2048 keypair at `services/dev/secrets/{session-jwt,session-jwt-pub}.pem`
   and does not generate gateway config; `services/dev/ai-gateway-config.yaml` is checked in.
   Idempotent; re-running is a no-op.
3. `make services-up` → `services/dev/scripts/start-services.sh` — sources
   `.env`, probes prereqs and ports, waits for infra TCP readiness, runs
   prepares the ai-gateway image (pull + runtime smoke test; compatibility
   wrapper if needed), runs `pnpm -r build`, applies file-store / memory-store / registry migrations,
   backgrounds registry/harness with logs in `services/dev/logs/{registry,harness}.log`,
   starts the `ai-gateway` compose service, and blocks until registry, harness,
   and `http://localhost:${AI_GATEWAY_ADMIN_PORT}/healthz` return 200.
   If the pinned gateway image is unavailable, the script fails fast; no local
   source build fallback is used.

## Running E2E tests

```bash
pnpm e2e:wire        # Layer A (wire, pricing and budget authoring contracts)
pnpm e2e:spend       # deterministic spend, no paid provider
pnpm e2e:agent       # Layer B (Claude by default, paid)
ORCA_E2E_AGENT_HARNESS=codex_sdk pnpm e2e:agent  # requires OPENAI_API_KEY in the stack
pnpm e2e:agent:sandbox  # Layer B.1 (sandbox harness mode, paid; OpenSandbox/E2B runtime)
pnpm -F @orca/e2e-tests test    # all suites, including paid cases
```

- **Layer A** drives `/v1/agents`, `/v1/sessions`, `/v1/files`,
  `/v1/memory_stores` etc. against the registry only — fast (~30s) and the
  cheap blast radius for catching shape regressions before paying the
  Anthropic cost in Layer B. No `ANTHROPIC_API_KEY` required.
- **Deterministic spend** starts isolated Registry, harness and self-hosted
  worker/runner processes and runs the real Claude SDK against scripted Messages
  responses. It needs `pnpm -r build`, Postgres with `CREATEDB` and S3 bucket
  create/list/delete plus object read/write access, but no paid provider. It
  owns four databases, one bucket, temporary work directories and process groups;
  cleanup runs after success or failure. Provider keys from `.env` are excluded
  from owned process environments. Configure infrastructure with
  `ORCA_SPEND_POSTGRES_ADMIN_URL`, `ORCA_SPEND_S3_ENDPOINT`,
  `ORCA_SPEND_S3_ACCESS_KEY_ID` and `ORCA_SPEND_S3_SECRET_ACCESS_KEY` (defaults:
  local Postgres `orca:orca`, RustFS `minioadmin`). Diagnostics remain under
  `orca-spend-logs-*` in the system temporary directory. This suite uses Postgres
  transcripts and the in-memory development runtime; real file/API assertions
  do not establish OS sandbox confinement.
- **Layer B** drives the full agent loop (registry → transcript store →
  harness → native model SDK → Sandbox/MCP tools → output assertions).
  `ORCA_E2E_AGENT_HARNESS` selects `claude_agent_sdk` (default,
  `ANTHROPIC_API_KEY`) or `codex_sdk` (`OPENAI_API_KEY`, model `gpt-5.4`, low
  reasoning effort). Both use their default `separate` mode. The selected key
  must be present in the harness and test environments; missing keys hard-fail.
  The stack E2E OpenSandbox matrix uses Codex SDK for **Pulsar** and Claude for
  **Kafka/Postgres**. Separate Codex calls OpenAI directly; its colocated mode
  uses native Responses through ai-gateway. Remote MCP uses ai-gateway in both.
  CI builds Gateway `main` for Pulsar and records the resolved commit in the
  image label and build log; Claude retains the released Gateway image.
  The shared scenarios cover files, output capture, memory, custom tool callbacks,
  remote MCP, workspace isolation, cron Triggers, managed Skill disclosure, and
  stateless tool guardrails. Codex runs the custom callback by default.
  Both SDKs run stateful request-budget scenarios. Multiagent and session-thread
  scenarios remain Claude-only.
  The control-plane backend matrix continues using `in-memory`; its Postgres leg
  also runs deterministic spend with scripted provider responses.
  Budget smoke uses `claude-sonnet-4-6` or `gpt-5.4` (`ORCA_E2E_BUDGET_MODEL`
  overrides it) and checks request denial without additional token growth.
- **Layer B.1** runs the same single-agent scenarios with `mode=colocated`:
  Claude and Codex use the same `harness-server` → `@orca/sandbox-harness`
  HTTP/SSE bridge, image and entrypoint. Cases cover text, usage and request
  budgets, Skills, File/Git mounts, Memory writeback, custom callbacks and
  immediate output upload. Both use OpenSandbox with the same 250m CPU / 1 GiB
  reservation and gVisor/write-policy setup. The matrix runs on
  `ubuntu-latest-8-cores`; Codex does not provision an additional Environment worker.
  Run `pnpm e2e:agent:sandbox`, selecting Codex with
  `ORCA_E2E_AGENT_HARNESS=codex_sdk`. To include B.1 after the regular specs,
  use `ORCA_E2E_SANDBOX_HARNESS=1 pnpm e2e:agent`.
  CI leaves per-case cleanup enabled so completed sandbox sessions do not
  retain CPU reservations until idle/TTL expiry on the single-node kind cluster.
  For local debugging, `ORCA_E2E_SANDBOX_HARNESS_KEEP_RESOURCES=1` skips cleanup
  in these specs; retained sandboxes continue to consume cluster capacity.

See [`packages/e2e-tests/README.md`](../../packages/e2e-tests/README.md) for
how the auth / seeding flow works (a direct Postgres insert provisions the
workspace + api-key row. There is no `/v1/api_keys`
route).

## Troubleshooting

### Port already in use

`start-services.sh` runs an `lsof` pre-flight on the registry, harness, ai-gateway data, and ai-gateway admin ports before
spawning. If something else owns the port (a stale prior run, another dev
stack, a system service), the script bails with the holder shown:

```
start-services: TCP port 8080 (intended for registry) is already in use.
  another process is listening — stop it first or override the port via
  REGISTRY_HTTP_PORT / HARNESS_HTTP_PORT / AI_GATEWAY_URL / AI_GATEWAY_ADMIN_PORT.
  current holder:
    node ... LISTEN
```

Fix: kill the holder, or set the override env var in `services/dev/.env`.

### `srt: command not found` (or harness logs "sandbox manager init failed")

`SANDBOX_RUNTIME=local` requires `srt` on PATH. Re-install:

```bash
npm install -g @anthropic-ai/sandbox-runtime
which srt        # confirm it's on PATH
```

On Linux you also need `bubblewrap` (`apt install bubblewrap`). If you don't
need the sandbox boundary right now, switch to `SANDBOX_RUNTIME=in-memory` in
`services/dev/.env` and re-run `make stack-up`.

### Sandbox violations during a session

Symptoms: `bash` calls inside a session emit `Operation not permitted`,
`Permission denied`, or the agent's `read`/`write` tools fail with
`path '...' escapes the sandbox work-dir`.

The `LocalSandboxRuntime` allow-list (see
[`services/harness-server/src/sandbox/local/README.md`](../../services/harness-server/src/sandbox/local/README.md)):

- Writes are restricted to the sandbox's own `mkdtemp` directory under `{HARNESS_WORK_DIR}/sessions/` and its
  canonical `tmp/` subdir. The shared write paths that SRT normally adds
  (`/tmp/claude`, `/private/tmp/claude`, `~/.npm/_logs`, and
  `~/.claude/debug`) are explicitly denied; `HOME`, `TMPDIR`, `TMP`, and
  `TEMP` are reset inside the sandbox for every command.
- Reads default-deny the host root, then re-open only the session directory,
  minimal OS runtime paths (`/bin`, `/usr`, libraries and specific TLS/name
  service files), and operator-configured `extraReadPaths`.
- `~/.ssh`, `~/.aws`, `~/.config/gcloud` remain explicitly denied.
- Network is restricted to the operator-provided list (default:
  `api.anthropic.com`, the host of `AI_GATEWAY_URL`, the host of
  `S3_ENDPOINT`).

If your test legitimately needs another path or domain, override
`extraReadPaths` / `extraDenyReadPaths` / `allowedNetworkHosts` in
`LocalSandboxRuntimeOptions` (currently a code-level change). Or run with
`SANDBOX_RUNTIME=in-memory` to bypass the sandbox entirely.

### Harness logs

```bash
tail -f services/dev/logs/harness.log
tail -f services/dev/logs/registry.log
docker compose -f services/dev/docker-compose.yml logs -f ai-gateway
```

The CI workflow dumps registry/harness logs and ai-gateway compose logs on failure
([`e2e-stack.yml`](../../.github/workflows/e2e-stack.yml) "Dump host-service
logs on failure" step).

### Stale state, "fresh start"

```bash
make stack-down
rm -rf services/dev/{data,secrets,logs,run,.env}
make stack-up
```

`services/dev/data/` holds the bind-mounted Postgres + broker + RustFS state;
removing it gives a wholly clean infra. The `.env` file you regenerated
points back at the same defaults via `cp .env.example .env`.

## Architecture: how the harness resolves workspace-scoped sessions

The Harness dispatcher consumes workspace/session-keyed events from the
configured transcript backend. Before creating a sandbox it asks Registry to
prepare one authoritative execution snapshot through a path containing both
identities:

`POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/executions:prepare`

Registry rejects a path/session mismatch and resolves the pinned Agent,
subagents, Skills, Environment, Vault metadata and attached resources inside
the same workspace. The implementation lives in
[`services/registry-service-ts/src/api/internal.routes.ts`](../../services/registry-service-ts/src/api/internal.routes.ts).
The Harness never holds a tenant API key. The local stack generates one
high-entropy internal service token under `services/dev/run/` and supplies it
to Registry, Harness, and AI Gateway. Kubernetes deployments instead use
per-workload projected ServiceAccount JWTs verified through TokenReview.

## CI integration

The same `make stack-up` path is exercised by
[`.github/workflows/e2e-stack.yml`](../../.github/workflows/e2e-stack.yml) on
every push to `main`, on a 03:00 UTC daily schedule, and on a pull request that
carries the `run-e2e` label. It is not part of the merge gate: label a PR
`run-e2e` to run it before merging.

The workflow:

1. Installs Node, pnpm and build tools; the agent matrix provisions Kind/OpenSandbox.
2. Materialises `services/dev/.env` with the deterministic `ORCA_E2E_PLAINTEXT_KEY`.
   Real-agent jobs require `OPENAI_API_KEY` for Pulsar and `ANTHROPIC_API_KEY`
   for Kafka/Postgres; the control-plane matrix needs neither provider key.
3. Runs `make stack-up`.
4. Runs `pnpm e2e:wire` and `pnpm e2e:gateway` in the control-plane backend
   matrix, plus `pnpm e2e:spend` in its Postgres leg. The OpenSandbox agent
   matrix runs `pnpm e2e:agent` and `pnpm e2e:agent:sandbox`: the Claude Agent
   SDK on Kafka, the Pi SDK on Postgres, and the Codex SDK on Pulsar. All three
   include request-budget smoke.
5. Dumps service logs on failure and uploads owned spend diagnostics from
   `/tmp/orca-spend-logs-*/` when its Postgres leg fails.
6. Runs `make stack-down` in `if: always()`.
7. Posts to Slack when the run fails on `main` or on the schedule.

A regression in `srt`, `make stack-up`, Registry-backed dynamic MCP resolution,
gateway egress admission, dispatcher topic re-discovery, in-process MCP server
wiring, or native SDK behavior surfaces here — on the merged commit or on the
next daily tick, and in Slack, rather than in production. Local
gateway config allows private egress only to `host.docker.internal` for the
fake-MCP fixture; other private/control-plane destinations stay denied.
