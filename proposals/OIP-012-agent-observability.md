# OIP-012: Agent observability

- *Author(s)*: @freeznet
- *Status*: Released
- *Proposal time*: 2026-08-26
- *Components*: observability-exporter, registry-service-ts (observability control plane and policy
  API), `packages/agent-event-contract`, Helm chart
- *Discussion*: None (predates the public repository)
- *Implementation*: `packages/agent-event-contract/src/`; harness-server
  `src/harness/agent-harness.ts`; registry-service-ts `src/domain/agent-observability-*.ts`,
  `src/domain/session-creation.ts`, `src/domain/session-observability-lifecycle.ts`,
  `src/api/{admin,platform-agent-observability,internal}.routes.ts`, `src/auth/internal-auth.ts`,
  `openapi/observability-admin.yaml`, migrations `0048`, `0049`, `0054`–`0056` and `0060`;
  `services/observability-exporter/src/`; chart templates `*-observability-exporter.yaml`
- *Released in*: v0.5.0

## TL;DR

Organizations sharing one deployment want each agent turn traced into their own observability
project — what the agent received, which tools ran, what failed — without provider keys in the agent
path or one tenant's traces reaching another's project. Registry pins a target per Session from
organization and workspace settings, keeps credentials write-only in SecretStore and authorizes every
send; an optional exporter turns each accepted turn in the Kafka Transcript into one deterministic
trace and delivers Langfuse-compatible OTLP to targets such as Litefuse. Capture is metadata-only
unless every scope authorizes raw I/O. Administrators at every scope, and harness authors, are affected.

## Background

- A Session's history is an ordered Transcript of client and harness events
  ([OIP-001](OIP-001-transcript-store-backends.md), [OIP-002](OIP-002-agent-harnesses-and-execution-modes.md));
  a client event runs once the `session.user_event_processed` marker names it
  ([OIP-006](OIP-006-session-event-semantics.md)). Registry's admin listener carries organization and
  platform administration, its internal listener workload resolvers ([OIP-007](OIP-007-tenancy.md)).
- Owning documents: [`libraries/agent-event-contract.md`](../docs/managed-agents/libraries/agent-event-contract.md),
  [Registry](../docs/managed-agents/services/registry-service.md#session-observability-pins),
  [`workspace-administration.md`](../docs/managed-agents/workspace-administration.md),
  [`internal-traffic-auth.md`](../docs/operation/internal-traffic-auth.md),
  [`data-model.md`](../docs/managed-agents/data-model.md), [`kubernetes.md`](../docs/managed-agents/kubernetes.md)
  and the [exporter README](../services/observability-exporter/README.md); exporter state and startup
  recovery are in [OIP-014](OIP-014-exporter-state-and-startup.md).

## Motivation

**Tenancy.** One deployment serves many organizations and workspaces, each wanting its own project,
retention and access control. A per-deployment exporter sends every workspace to one project; an
exporter per tenant with keys in Helm values makes each customer a chart change and puts secrets in
rendered configuration. No request, event or header may pick the destination: the `/v1` caller does
not own it.

**Availability.** Observability is a derived view. A slow or broken provider must not reject a client
event, delay a model call or tool, or break Session streaming, so the agent path may never call it.

**Identity.** Some harness paths derived event identity from payload fields or transcript position,
and `span.model_request_*` pairs looked like provider calls while summarizing a whole query. A trace
built on that changes on replay and overstates what is known.

**Content.** Prompts, completions and tool arguments are the most useful and most sensitive data. The
default must be useful without them, content must need authority at every scope it affects, and a
later restriction must stop content already queued.

## Goals

### In scope

- A harness-independent producer contract: required envelope identity, honest model granularity.
- A Registry control plane: organization default, workspace `inherit | disabled | custom`, immutable
  bindings and policy versions, write-only credentials, Session pins, revocation and restriction
  epochs, audit.
- An optional exporter: one deterministic trace per accepted turn, no synchronous dependency from the
  agent path, authorization of every send, deterministic sampling, Langfuse-compatible OTLP output.
- Metadata-only by default; unredacted I/O only under explicit authority; `/v1` unchanged.

### Out of scope

- Several targets per workspace, per-Agent or per-Session targets, fan-out, caller-chosen bindings;
  observability credentials in Vaults; tenant clients inside Harness or sandboxes.
- Backfill when a target is enabled; deleting accepted traces; exactly-once delivery; automatic
  redaction; exporting thinking; exporter support for the Postgres Transcript.
- Listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#agent-observability): per-call model
  observations, child-thread traces, session-runner contract adoption, other OTLP profiles, the
  `langfuse_sdk` adapter, binding-wide lifecycle, finite retention, durable retry and dead-lettering,
  fair scheduling, exporter operations; Pulsar is [deferred by design](../docs/managed-agents/roadmap.md#deferred-by-design).

## Design

### High-level design

```
 platform, org admins ─► admin listener ─► Postgres: policy, settings, bindings, versions,
                               │                    credential heads, pins, audit
                               └─────────► SecretStore: write-only credential bundles
 Session create, trigger fire ─► pin chosen inside the Session transaction (no network, no secret)
 Registry, Harness ─ canonical events ─► Kafka Transcript: one single-partition topic per Session
                                             │ direct, ordered, read_committed
 exporter reducer ─ one Kafka transaction ─► checkpoints (compacted), delivery (eight partitions)
 exporter delivery ─► secret/resolve on the internal listener (audited, every attempt)
                   └► hardened HTTPS ─► /api/public/otel/v1/traces (Litefuse, Langfuse-compatible)
```

Registry alone decides who receives what; the exporter holds no tenant configuration of its own.

### Detailed design

**Canonical event contract.** `@orca/agent-event-contract` is pure TypeScript without I/O or an ID
generator. `AgentEvent` requires `id` (`evt_`-prefixed, the sole identity), `subpath` (`''` for the
primary agent, `subagents/<thread>` for a child; `*` only selects reads), `kind` and `payload`;
correlation fields such as `user_event_id` and `model_request_start_id` reference envelope IDs.
`CANONICAL_AGENT_EVENT_KINDS` excludes runtime-only `agent.usage` and the internal acceptance marker.
`span.model_request_start/end` carry `model_observation_kind: "turn_model_summary"` — one pair per
harness query or turn, the end requiring `model_request_start_id`, `model_usage` and `is_error` — and
never claim a served model or single provider request. Harness-server's `claude`, `in-sandbox` and
`codex-sdk` paths finish every draft in `withCanonicalAgentEventEnvelope`, which mints a missing ID,
defaults the subpath to primary and rejects malformed values; shared `model-summary.ts` assertions
gate the Claude producers. Registry hides the summary fields from the default public view but keeps
them for `orca-beta` (`toPublicHttpEvent`), and stamps OIDC client events with an opaque
`oidc_user_<sha256>` of issuer and subject.

**Trace identity.** One accepted turn is one trace, anchored on the client event named by the exact
`user_event_id` of its acceptance marker, never the nearest queued input. IDs are SHA-256 digests
under a fixed domain separator (`ids.ts`): trace over workspace, Session and anchor; root over trace
and role; child over trace, observation type, subpath and source event, so replay reproduces them. A
source event or trace that reappears with a different hash fails the exporter instance rather than
being overwritten, and native tool and outcome IDs are kept only as digests.

**Control-plane model.** Migration `0048` adds a singleton platform policy (allowed adapters and
endpoint classes, maximum capture mode; seeded `otlp_http`, `public`, `metadata_only`); bindings, one
per fixed target (scope, adapter, endpoint, `external_project_id`, status `active | draining |
disabled | archived`); immutable versions (profile, protocol, compression, timeout, environment,
release, capture mode, sample rate); one credential head per binding (an opaque `secret_ref`, never
bytes); organization and workspace settings with epochs and capture ceilings; and the pins
([`data-model.md`](../docs/managed-agents/data-model.md)). A new target is a new binding, a policy
change a new version, a rotation a new head. A Langfuse-profile binding must name a non-secret
`external_project_id`, such as the project's public key; changing it means a new binding. Setting
rows are created with their parent and backfilled; a missing row fails closed, never read as a default.

**Selection and pins.** An archived organization or workspace, or workspace `disabled`, is disabled;
`custom` uses the workspace binding and never falls back; `inherit` uses the organization's active
default, one external project where `session.id` and `orca.workspace.id` correlate but only `custom`
gives provider-side access control. `createSessionInTransaction` runs its observability selector
before any Agent, Environment, Vault or Skill lock, taking shared locks in the order mutations use,
and writes one pin in the Session's transaction: source, binding and version, the nine epochs
observed, `session_revocation_epoch = 0`, Agent ID and version, harness and mode, and
`effective_capture_mode`, the lowest of the requested mode and three ceilings. It holds no endpoint or
secret and contacts no provider. A selection failure rolls the Session back (`503 overloaded_error`);
triggers share the path. Setting changes reach only new Sessions.

**Revocation, restriction and tombstones.** Selection epochs fence only Session creation, so moving a
workspace to another target never cuts off a pinned Session. Delivery compares:

| Epoch | Advanced by | Effect on existing pins |
|---|---|---|
| organization default revocation | organization `:disable` | suppresses pins sourced from the default |
| workspace revocation | workspace `disabled`; workspace archive | suppresses the workspace's pins, custom included |
| session revocation | Session archive or deletion | suppresses that Session |
| organization and binding revocation | nothing yet (Status notes) | suppress the organization's or the binding's pins |
| platform, organization, workspace capture restriction | lowering that ceiling | older pins become `metadata_only` for good |

Archive and deletion lock the exact pin, mark it `archived` or `deleted` and advance its epoch once
(`session-observability-lifecycle.ts`). Pins have no foreign key to `sessions`, so the tombstone
outlives the Session; workspace archive writes a durable marker and CASes its epoch (`0056`).

**Mutations and credentials.** Writes are strict full replacements needing `Idempotency-Key`
(24-hour replay) and, over existing state, the exact strong `If-Match` (`428` absent, `412` stale).
Migration `0055` adds a fenced kernel: one pending reservation per target (15-minute TTL); a staging
intent committed before bytes reach SecretStore; SecretStore I/O outside every transaction; a final
transaction that re-verifies and CASes the credential head from `N` to `N + 1` with audit and
response cache; and a cleanup outbox. A 60-second maintenance loop reconciles abandoned staging and
the outbox; staging tombstones stay, since no SecretStore guarantees a late `put` cannot land. A
target change moves the old binding to `draining`, unselectable but resolvable for existing pins;
`:disable` takes an empty body and optional `If-Match`. Lowering a ceiling advances its restriction
epoch; raising one never resets it.

**Resolvers.** Two exporter-only internal routes take a strict `{}` body with workspace and Session in
the path. `…/agent-observability/context/resolve` returns a `schema_version: 1` context — pin, exact
pinned binding version, lifecycle, pinned and current epochs, pinned and effective capture mode,
current ceilings — and `enabled`, `disabled` or `suppressed` with a reason from a fixed-precedence
classifier (`classifySessionObservabilityContext`), without reading `secret_ref`. `…/secret/resolve`
reads that authority in two repeatable-read snapshots around one `SecretStore.resolve`,
re-classifies, and commits `agent_observability.credential_resolved` before any bytes leave, within
one 5-second deadline; denial is `409`, other failures a sanitized `503`, and the response holds the
bundle and an opaque `authorization_id`, never a reference. Kubernetes mode requires a distinct
`INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT`; a route-family allowlist grants that caller only these
routes, and a route matching no single family fails closed (`internal-auth.ts`).

**Exporter runtime.** Kafka state is the default (`broker-main.ts`, `kafka-runtime.ts`). The exporter
discovers Session topics, requires one partition each and reads it directly with `read_committed`
and auto-commit off — no inbox, no second Transcript read. On first owning a Session it resolves
context once and pins a supported delivery context or a terminal `deliveryContext: null` (disabled,
suppressed or unsendable). Each batch commits checkpoint, identity and acceptance ledgers, sampled
delivery records and the source offset in one transaction over `orca.observability.v1.checkpoints`
(compacted) and `orca.observability.v1.delivery` (eight partitions, unlimited retention).
Conflicts, invalid state and exceeded bounds fail the instance closed
([OIP-014](OIP-014-exporter-state-and-startup.md)). `OBSERVABILITY_EXPORTER_STATE_BACKEND=postgres`
keeps the earlier inbox, replay and SQL-outbox runtime.

**Projection and sampling.** Only primary-subpath events project (`projector.ts`,
`tool-projection.ts`). The root `orca.agent.turn` opens on exact acceptance, stays open across
`requires_action`, and closes on an `end_turn` or `retries_exhausted` idle, termination, archive or
deletion. Children are `orca.agent.turn_model_summary` spans, `orca.agent.tool` for local, MCP and
custom tools (a client result counts only after its exact acceptance) and metadata-only outcome
evaluations; a tool open at close is `incomplete`. A turn keeps at most 256 each of tools, results,
summaries and evaluations and 1,024 pending inputs; overflow fails closed. Sampling
(`orca.observability.trace-sampling.v1`) compares the first 64 bits of SHA-256 over the label, a NUL
byte and JSON `[bindingId, bindingVersion, traceId]` with `ceil(rate × 2^64)` before any child or
captured value exists; a sampled-out turn only advances a content-free watermark.

**Delivery and egress.** A delivery record is one OTLP request (`kafka-delivery.ts`). Every attempt
first calls the secret resolver — a fresh, audited authorization — and requires the pinned binding
and version; a content trace also needs `raw_io` pinned and fresh. Only Basic credentials (project
public and secret key) are used; other bundles end as `unsupported_credential`. A valid HTTP 200
`ExportTraceServiceResponse` is accepted, accepted with warning, or a terminal partial rejection kept
as counts, byte length and SHA-256; `401`/`403` earns one fresh resolution; `429`, `502`–`504` and
transport errors retry; anything else is terminal. A retry pauses its partition (1 s to 5 minutes,
jitter, `Retry-After` up to an hour) with counters in memory; eight partitions deliver concurrently,
and a terminal outcome commits the offset with compacted per-partition progress. HTTP cannot join the
transaction, so delivery is at-least-once and a backend may store a resent trace twice. `egress.ts`
admits only HTTPS whose every resolved address is public, pins the socket to one, follows no redirect
and ignores proxies; `litefuse-client.ts` requires `/api/public/otel/v1/traces`, caps responses at
64 KiB and timeouts at 120 s, and sends `x-langfuse-ingestion-version: 4`.

**Destinations.** The Langfuse profile (`otlp-json.ts`) types the root `agent`, summaries `span`
(never `generation`), tools `tool` and evaluations `evaluator`. Observations carry
`session.id=<workspace>:<session>`, `orca.workspace.id`, the turn anchor, `langfuse.user.id` for a
user-attributed event, and agent, harness, binding and deployment attribution snapshotted with the
pin; summaries carry only the provider, model, usage and cost the producer reported.
[Litefuse](https://litefuse.ai), which advertises Langfuse API compatibility, uses this profile.

**Authorized raw I/O.** Modes are ordered `metadata_only < redacted_io < raw_io`. `redacted_io` is
reserved: no exporter implements it and it never authorizes content. `raw_io` means unredacted
content and needs a binding version requesting it plus platform, organization and workspace ceilings
allowing it at pin time; the platform seed stays `metadata_only` until the platform policy API
changes it. Content traces (`orca.observability.projected-trace.v2`, capture format
`orca.observability.raw-io.v1`) add root input — the exactly accepted primary `user.message` text,
held as a sampled pending candidate until acceptance; root output — the turn's non-partial assistant
text messages (`orca.io.output_scope=turn_messages`), without thinking, signatures, deltas or system
instructions; tool names, arguments and results; the last reported `session.error`; and accepted
`user.tool_confirmation` decisions. Nothing is masked: `password` keys and token-like text pass
unchanged. Checks are structural (`captured-io.ts`): JSON only, getters and `toJSON` never run, 8,192
bytes per value, 262,144 per turn and for pending input, omissions marked. A raw pin re-resolves
context per bounded projection chunk (30-second deadline, 60-second maximum) and again at send; a
restriction scrubs pending capture, survives restart and suppresses queued content whole. The legacy
Postgres backend refuses `raw_io`.

## Changes by component

- **registry-service-ts**: migrations, pinning, pin tombstones, admin, platform and internal routes,
  the mutation kernel, the exporter caller and route allowlist, the admin OpenAPI document.
- **harness-server**: canonical envelopes on every producer path, summary labels, conformance tests.
- **observability-exporter**: new service with Kafka and legacy Postgres runtimes and a Node 22 image.
- **Libraries**: new `@orca/agent-event-contract`; `@orca/transcript-store-types` gains `Event.userId`.
- **Helm chart**: a dedicated exporter ServiceAccount plus a default-off Deployment and ConfigMap.

## Public-facing changes

### API

The `/v1` surface, its OpenAPI document and the conformance matrix are unchanged. These
admin-listener routes are generated into `services/registry-service-ts/openapi/observability-admin.yaml`
(ten operations, `pnpm -F @orca/registry-service-ts openapi:observability:gen`):

| Route | Authority | Behavior |
|---|---|---|
| `GET /v1/organizations/agent_observability` | `observability:read` | default, ceiling and effective decision; strong `ETag` |
| `PUT /v1/organizations/agent_observability` | `observability:write` | replace the default: `target`, `config`, `capture_ceiling`, write-only `credentials`; `201` on first configuration |
| `PUT /v1/organizations/agent_observability/capture_ceiling` | `org:admin` | change only the organization ceiling |
| `POST /v1/organizations/agent_observability:disable` | `observability:write` | emergency disable; body `{}`, `If-Match` optional |
| `POST /v1/organizations/agent_observability:rotate_credentials` | `observability:rotate` | new credential head for the active default |
| `GET /v1/organizations/workspaces/{id}/agent_observability` | `observability:read` | mode, binding, ceiling and effective decision |
| `PUT /v1/organizations/workspaces/{id}/agent_observability` | `observability:write` | `{mode: inherit \| disabled, capture_ceiling}` or `{mode: custom, target, config, capture_ceiling, credentials?}` |
| `POST /v1/organizations/workspaces/{id}/agent_observability:rotate_credentials` | `observability:rotate` | new credential head for the active custom binding |
| `GET`, `PUT /v1/platform/agent_observability` | `platform:admin` | allowlists and maximum capture mode; server-owned epoch; 24-hour replay |

`org:admin` satisfies every organization scope. `target.adapter_type` accepts only `otlp_http`,
`config.protocol` `http/json` or `http/protobuf`, `semantic_profile` `langfuse` or `otel_genai`, and
`timeout_ms` 1–120,000. `credentials` is `basic`, `bearer` or bounded `custom_headers`, never returned
or cached. Responses are `no-store`, and `POST /v1/platform/organizations` accepts a `capture_ceiling`.

### Events and streaming

No new event kind. Without `orca-beta`, `span.model_request_end` omits `model_observation_kind`,
`provider`, `model` and `total_cost_usd`. Harness-server events always carry canonical envelopes.

### Wire protocols

Transcript `Event` gains optional `userId` (`transcript_store.proto`, `user_id = 11`). The resolver
routes use schema version 1 and, in Kubernetes mode, admit only the exporter. The exporter emits
OTLP/HTTP JSON from `orca.observability.projected-trace.v1` (metadata) and `.v2` (content) traces.

### Storage

Registry: `0048` (tables, seeds, backfilled settings and disabled pins, temporary provisioning
triggers), `0049` (pin repair, temporary fallback for legacy Session inserts), `0054` (temporary
Session lifecycle trigger), `0055` (reservations, staging intents, idempotency, cleanup outbox),
`0056` (workspace archive markers, temporary archive trigger) and `0060` (`raw_io` in every capture
check; the pin check is added `NOT VALID` and validated by `src/migrate.ts` after commit). Kafka: the
two internal topics, `-avro` suffixed for an Avro Transcript, and group and transactional-ID
namespaces derived from the topic prefix
([README](../services/observability-exporter/README.md#stable-namespaces-and-permissions)).

### Configuration

| Setting | Default | Read by | Effect |
|---|---|---|---|
| `INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT` | required in Kubernetes mode | Registry | exporter ServiceAccount subject |
| `OBSERVABILITY_EXPORTER_STATE_BACKEND` | `kafka` | exporter | `postgres` selects the legacy runtime; a DSN without it fails startup |
| `REGISTRY_INTERNAL_BASE_URL`, `INTERNAL_SERVICE_TOKEN[_FILE]` | required | exporter | resolver origin and workload credential |
| `OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS` | `10000` | exporter | resolver timeout, at most 15,000 in Kafka mode |
| `TRANSCRIPT_STORE_BACKEND`, `KAFKA_*`, `OBSERVABILITY_KAFKA_*` | see README | exporter | Kafka only; Transcript settings match Registry and Harness; state budgets |
| `observabilityExporter.enabled` | `false` | chart | Deployment and ConfigMap; no Service, Ingress or NetworkPolicy |
| `observabilityExporter.stateBackend` | `kafka` | chart | `Recreate` strategy; exporter DSN references rejected |
| `observabilityExporter.kafkaStateSizeLimit` | `2Gi` | chart | `emptyDir` at `/var/run/orca/exporter-state` |
| `images.observabilityExporter.*` | `ghcr.io/orca-ae/orca-observability-exporter` | chart | image; tag defaults to `appVersion` |

Tenant targets and credentials are Registry data, never Helm values, and need no chart change or
restart; the [README](../services/observability-exporter/README.md#runtime-configuration) lists all.

### Metrics, logs and traces

The exporter has no Prometheus endpoint: it serves `GET /healthz` and `GET /readyz` on port 8080 and
logs fixed-schema JSON diagnostics every 60 seconds, never with I/O, credentials or provider messages.
Registry audits every credential release, platform policy change and scoped mutation.

## Compatibility

### Upgrade

Registry changes are additive and off by default: settings start disabled or `inherit` with
`metadata_only` ceilings and existing Sessions get disabled pins, so enabling a target exports only
new Sessions. Temporary triggers keep a rolling update safe while older replicas write directly;
their removal is listed on the roadmap. Kubernetes deployments outside the chart must set a distinct
`INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT` or Registry refuses to start. Apply `0060` before
requesting `raw_io`. Enabling the exporter on a release that predates its workload needs
`helm upgrade --reset-values` with a complete values file.

### Rollback

Disabling a setting or scaling the exporter to zero is the rollback; the agent path does not depend
on it, and broker state and offsets remain, so never reset offsets or delete the internal topics.
Registry migrations are forward-only, and a Registry older than `raw_io` support reads a `raw_io`
ceiling as corrupt authority and fails Session creation closed, so lower those first. Older exporter
binaries cannot read committed v2 checkpoints or reducer state that recorded raw capture.

### Version skew

Upgrade Registry first; the exporter needs both resolvers and its identity. Run one exporter
generation per topic prefix (the Kafka Deployment uses `Recreate`). Registry, Harness and exporter
must agree on Transcript encoding. A Session whose pin the exporter cannot send stays suppressed.

## Security considerations

- **Only Registry picks the destination.** Targets come from pins whose ownership composite foreign
  keys enforce; bodies, Session metadata, Vault IDs, headers and Transcript payloads cannot select a
  binding or credential.
- **Credentials** are write-only and live only in SecretStore, bound to one binding and generation —
  never in Postgres values, responses, audit, caches, logs, Transcript, JWTs, sandboxes or Helm
  values; the exporter holds them for one audited send. Rotation assumes the same project.
- **Workload identity.** In Kubernetes mode the exporter has its own subject and exactly two routes,
  which Harness and AI gateway tokens cannot call; static-token mode shares one principal.
- **Revocation** stops every send whose resolution runs after it commits; a request in flight may
  complete, and nothing recalls data a provider accepted.
- **Raw I/O is an explicit disclosure.** Nothing is masked, so secrets or personal data in selected
  I/O leave as they are. The platform ceiling is global: raising it can authorize new pins in any
  organization whose own settings allow raw capture. Content persists in delivery records,
  checkpoint chunks and the scratch index under unlimited retention; a restriction stops delivery
  but deletes nothing, locally or at the provider.
- **Egress and blast radius.** Admission precedes credentials, and DNS pinning, no redirects and no
  proxy keep a hostile target or resolver from steering them. The exporter is a trusted multi-tenant
  workload: compromising it exposes every credential it resolves.

## Testing

- **Contract and producers**: `packages/agent-event-contract/test/unit/`; harness-server
  `agent-event-envelope.spec.ts`, `tool-producer-conformance.spec.ts` and the `model-summary.ts`
  assertions in the Claude and in-sandbox suites.
- **Registry**: unit suites for the classifier, selection, lifecycle, mutation kernel, raw I/O,
  contract parity and `internal-route-family-parity.spec.ts`; fresh-Postgres integration for
  migrations, pinning, organization and workspace routes, ceilings, the platform policy and
  `agent-observability-secret-resolver.spec.ts` (rotation, revocation and archive races observed
  through `pg_locks`). `internal-auth-kubernetes.e2e.spec.ts` runs on a kind leg of `e2e-stack.yml`
  against real TokenReview and proves route-family isolation both ways.
- **Exporter**: unit suites for projection, sampling, encoding, response parsing, egress, retry and
  Kafka state, plus `registry-contract-parity.spec.ts` (fixtures parsed by Registry's own contract);
  `test/integration/durable/kafka-*.spec.ts` on a real broker (raw restart, approval, attribution,
  retry, revoked delivery) and legacy Postgres suites in the required `integration` job. Opt-in
  `test:smoke:litefuse*` scripts round-trip synthetic turns through a real Litefuse project.
- **Chart**: `test/render.test.mjs` covers rendering, the reset-values guard and DSN rejection. No
  stack e2e suite runs the exporter end to end.
- **Ablation before raw I/O landed.** Six simplifications of the capture path each ran against a
  frozen baseline's unit suite plus differential and adversarial cases. Adopted: decoding each span's
  I/O once (`canonical-validation.ts`): half the parse calls, trace decoding about 18–20% faster on
  synthetic traces (the whole in-process pipeline under 5%), byte-identical output.
  Rejected: dropping the defensive copy (hidden `toJSON` and getters would run on unscanned content),
  serializing during validation (no measured gain), removing the per-event budget (state outgrew its
  bound and failed to restore) and removing the send-time capture check (metadata-only and
  `redacted_io` authority each let a raw send through). Regression tests in
  `captured-io-core.spec.ts` and `captured-io.spec.ts` pin those boundaries.

## Alternatives

- **An earlier design: deployment-configured bindings.** Each Helm binding rendered its own exporter
  with one target, credentials from environment or Secret references and a Postgres outbox, and
  consumed every workspace; tenants could not choose or isolate projects without a chart change. Its
  deterministic projection, metadata-only default and subpath-aware tool correlation carried over.
- **AI gateway telemetry.** The gateway, available as a public image under Apache-2.0, sees model and
  MCP requests, not the turn: acceptance, local tools, confirmations and turn boundaries never cross
  it ([OIP-003](OIP-003-egress-boundary.md)). Its telemetry stays operator-facing.
- **Vault credentials or clients in Harness.** Vaults are attached by Session callers and owned by
  workspaces, so callers would steer the destination and no organization default could exist;
  clients in Harness or sandboxes would put provider keys in the agent path.
- **A shared-process `langfuse_sdk` adapter.** SDK processors fan out unless every filter is right,
  queue in memory and open network paths outside the hardened egress. The schema recognizes it; the
  mutation API refuses it and the exporter treats its bundles as unsupported.
- **Exporter-owned Postgres by default.** The first exporter slice used it; it duplicated broker
  offsets, transactions and ownership and needed a second Transcript read. It remains a legacy option.
- **Automatic redaction (`redacted_io`).** Allowlists, key denylists and token masking cannot be
  shown complete over arbitrary content; an explicit, authorized disclosure replaced it.
- **Re-encoding restricted content to a lower mode** needs render-manifest and send-intent machinery;
  unauthorized content is suppressed whole instead.
- **An organization-enforced policy tier.** Organization administrators already own workspace
  configuration, and ceilings cover capture.
- **Batching many Sessions into one request.** Each record is authorized and sent alone, so no
  Session borrows another's allow decision.

## Status notes

The contract and producers landed first, then the control plane, then the exporter (on its own
Postgres, later on Kafka state), and last raw I/O and the platform policy API. Divergences:

- **One delivery profile.** The design defaulted to HTTP/protobuf and allowed gzip, base endpoints,
  OTel GenAI semantics and bearer or header credentials. The exporter sends only public HTTPS,
  exact-path, uncompressed HTTP/JSON with the Langfuse profile and Basic credentials
  (`delivery-capabilities.ts`); Registry accepts more, so such a Session is pinned, then suppressed.
- **Restriction is total.** Any restriction-epoch advance makes an older pin `metadata_only`, not the
  new lower ceiling (`effectiveSessionObservabilityContextCaptureMode`).
- **Unwired checks.** Organization-wide and binding revocation epochs are enforced but nothing
  advances them, bindings only ever become `draining`, and the `:validate` routes were not built.
  Rotation does not compare the new Basic username with `external_project_id`, so rotating to another
  project's keys would move pinned Sessions there.
- **Leaner operations.** No dead-letter topic, restart-safe retry state, private-host allowlist,
  Prometheus metrics, alerts or exporter NetworkPolicy exist; terminal outcomes only commit progress.
- **Narrower traces.** Child-thread events are omitted rather than attached by payload-derived
  hierarchy; the organization ID is not exported; OIDC user attribution is not behind a policy
  switch. Sessions are soft-deleted, not hard-deleted as designed: the pin turns `deleted` beside the
  retained row and suppresses as `session_deleted`.
