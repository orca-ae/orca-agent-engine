# Cron Triggers

## Decision

Orca exposes cron-only Agent Triggers as an Orca-owned Core extension under
`/v1/triggers`. A Trigger is available in every Registry deployment and is not
backed by a Kubernetes CRD, Kubernetes CronJob, or external scheduler.

This surface is intentionally not wire-compatible with Anthropic Deployments.
Orca does not publish `/v1/deployments` or `/v1/deployment_runs`; the vendored
Anthropic contract remains unchanged and conformance records that difference as
an accepted deviation. Every Trigger operation is tagged `orca-extension` in
the generated OpenAPI document.

## V1 public surface

| Method   | Path                         | Meaning                                                      |
| -------- | ---------------------------- | ------------------------------------------------------------ |
| `POST`   | `/v1/triggers`               | Create an active or initially paused cron Trigger.           |
| `GET`    | `/v1/triggers`               | List workspace-owned Triggers with keyset pagination.        |
| `GET`    | `/v1/triggers/{id}`          | Get one Trigger.                                             |
| `POST`   | `/v1/triggers/{id}`          | Update mutable schedule/session fields.                      |
| `DELETE` | `/v1/triggers/{id}`          | Soft-delete the Trigger while retaining its Session history. |
| `POST`   | `/v1/triggers/{id}/pause`    | Stop materializing future fires.                             |
| `POST`   | `/v1/triggers/{id}/unpause`  | Resume from the first future cron slot.                      |
| `GET`    | `/v1/triggers/{id}/sessions` | List Sessions created by the Trigger.                        |

Trigger create pins the selected Agent version. Each fire creates a new
Session (`SESSION_PER_EVENT` semantics) with one initial `user.message` whose
text is the Trigger payload. V1 does not implement shared Sessions, non-cron
sources, manual runs, restart, seconds/year fields, cron macros, or public run
resources.

The public configuration deliberately keeps the same extension seams that
future source and routing modes need:

```json
{
  "name": "daily-report",
  "agent": { "type": "agent", "id": "agt_...", "version": 1 },
  "session_mode": "SESSION_PER_EVENT",
  "source": {
    "type": "cron",
    "schedule": "0 9 * * *",
    "timezone": "Asia/Shanghai",
    "payload": "Create the report"
  },
  "session": {
    "environment_id": "env_...",
    "title_template": "Daily report ${payload}",
    "metadata": {},
    "vault_ids": []
  },
  "replicas": 1
}
```

`session_mode` is required and v1 accepts only `SESSION_PER_EVENT`.
`source.type` is required and v1 accepts only `cron`. `replicas` defaults to
`1` and no other value is accepted. These discriminators remain explicit so a
future version can add shared/topic/key routing, Kafka/Pulsar sources, and
different worker topology without moving fields or breaking the wire shape.

The Trigger stores only non-secret Session creation inputs:

- pinned Agent id/version;
- active Environment id;
- Vault ids;
- title template, string metadata, and payload;
- a five-field cron expression and IANA time zone.

Trigger create and update validate the Session metadata extension
`orca_llm_egress` with the same `direct` and `gateway` values as ordinary
Session creation. Invalid values return 400 before a fire can create a Session.

For cron fires, `session.title_template` expands `${trigger.name}` and
`${payload}` (and their `{{...}}` forms) when the Session is created. Unknown
placeholders remain unchanged so later source types can define their own
context without changing stored templates.

Session resources are not accepted in v1. In particular, a long-lived Trigger
must not persist a raw `github_repository.authorization_token`. Resource
support requires a durable, non-secret pre-provisioned resource-reference
model and is a separate design.

Write routes use the existing HTTP `Idempotency-Key` middleware. That cache is
only a client-retry convenience; scheduler correctness comes from the durable
fire identity described below.

## Scheduling semantics

- Expressions contain exactly five fields: minute, hour, day of month, month,
  and day of week. The minimum interval is one minute.
- `timezone` is an IANA time-zone name and defaults to `Etc/UTC`. Both the
  expression and time zone are validated on create/update with `cron-parser`.
- `next_fire_at` is stored as an absolute UTC instant. Cron evaluation retains
  the expression's time zone, including daylight-saving transitions.
- DST follows the pinned `cron-parser` semantics. A nonexistent local time is
  shifted forward by the size of the spring-forward gap; a repeated local time
  fires once at its earlier UTC instant. These cases are covered by fixed-date
  tests so a dependency upgrade cannot silently change scheduling behavior.
- Fire identity is the original cron slot, not planner wall-clock time:
  `(workspace_id, trigger_id, generation, scheduled_for)` is unique.
- V1 uses bounded misfire handling. A slot no more than five minutes late is
  materialized, with multiple overdue slots coalesced to that one oldest slot.
  An older slot is skipped, and the schedule advances directly to the first
  future slot. A restart therefore cannot create an unbounded catch-up burst.
- Delivery and agent execution are at-least-once. Orca guarantees at most one
  durable fire row per slot and an atomic fire-to-Session handoff, but does not
  claim exactly-once model turns or external tool side effects.

## Persistence

Registry owns two workspace-scoped tables:

```text
agent_triggers
  id, workspace_id, name, agent_id, agent_version, environment_id,
  title_template, metadata, vault_ids, payload, cron_expression, timezone,
  status, generation, next_fire_at, last_fired_at, last_error,
  archived_at, created_at, updated_at

agent_trigger_fires
  id, workspace_id, trigger_id, generation, scheduled_for,
  status, planned_session_id, session_id, event_id, attempt_count, last_error,
  next_attempt_at, created_at, updated_at, enqueued_at
```

`agent_trigger_fires` is an internal occurrence ledger, not a public
DeploymentRun replacement. It provides cross-replica de-duplication and the
Trigger-to-Session history relation. Fire rows are retained when a Trigger is
archived. The fire's preallocated Session id and event id remain stable across
dispatcher retries. V1 does not automatically prune terminal fire rows; the
retention and public-history policy is in [`roadmap.md`](./roadmap.md#production-hardening).

Every update, pause, unpause, or delete increments `generation`. Updates fence
all already-materialized fires because even fields such as name and metadata
affect the Session that a pending fire would create. A dispatcher re-checks the
active Trigger and generation in the same transaction that creates a Session,
so a committed lifecycle change fences older pending fires. A Session already
committed before pause/delete is allowed to continue; existing Sessions are
never archived as a side effect of Trigger lifecycle changes.

`GET /v1/triggers/{id}/sessions` is a relation over Sessions that still exist.
Archiving a Session hides it unless `include_archived=true`; permanently
deleting a Session removes it from this API even though the internal fire row
retains the successful handoff and its now-unresolved Session id.
The endpoint returns the ordinary full Session representation with a maximum
page size of 100. It currently reuses the per-Session loader; batched hydration is in
[`roadmap.md`](./roadmap.md#production-hardening), with the traffic level that would justify it.

## Planner and dispatcher

No independent service is introduced. Every Registry replica runs two small,
logically separate reconcilers when `TRIGGER_SCHEDULER_ENABLED=true`:

1. The **planner** claims due Trigger rows with
   `FOR UPDATE SKIP LOCKED`, inserts the unique fire row, and advances
   `next_fire_at` in a short transaction.
2. The **dispatcher** selects pending fires, locks the corresponding active
   Trigger and fire, then atomically creates the ordinary Session, primary
   thread, Skill pins, initial-event outbox row, and fire-to-Session link.

The dispatcher performs no TranscriptStore or other network call while holding
database locks. After commit, the existing Session lifecycle outbox publishes
the stable initial event and retries failures. Harness consumes the resulting
ordinary client-produced `user.message`; no Registry-to-Harness RPC or
TranscriptStore interface change is needed.

All replicas may run both loops. PostgreSQL row locks and unique constraints,
not process-local timers or leader election, provide ownership. A process-local
overlap guard only prevents the same replica from starting a second tick while
its previous tick is still running.

`TRIGGER_SCHEDULER_ENABLED=false` supports API-only Registry replicas alongside
ordinary API-plus-worker replicas. A future worker-only entrypoint can reuse
the same loops without changing the API or persistence model; v1 does not need
or ship that additional process role.

## Lifecycle and failures

- Create/update validates the workspace-owned Agent version, Environment, and
  Vaults. Dispatcher repeats those checks because dependencies may be archived
  after configuration.
- Structural dispatch failures pause the Trigger, set `last_error`, and mark
  the fire failed. Database/transient failures are isolated to one fire,
  increment `attempt_count`, and retry with exponential backoff. After five
  failed attempts the fire becomes terminal `failed` while the Trigger remains
  active, so one poison occurrence cannot block newer fires.
- Paused and archived Triggers have `next_fire_at = null`.
- Unpause computes the first future slot; it never catches up slots from the
  paused interval.
- Workspace archive is enforced by planner/dispatcher joining an active
  workspace. Archived workspaces cannot materialize or dispatch fires.

## Authorization and observability

The normal public authenticator derives `workspace_id`; Trigger bodies and
queries cannot select it. Trigger routes accept `workspace.full_access` or the
operation-specific `workspace.agentTriggers.{create,alter,delete,describe}`
scope. Listing Trigger Sessions additionally requires
`workspace.sessions.describe` unless the caller has full access.

Metrics remain low-cardinality:

- planner results (`created`, `deduplicated`, `misfired`, `paused`, `error`);
- dispatcher results (`enqueued`, `retry`, `canceled`, `failed`, `error`);
- due backlog and oldest due age;

Trigger, fire, Session, workspace, and event identifiers belong in structured
logs, not Prometheus labels.

## Topic sources

This repository owns `/v1/triggers`, and v1 accepts only cron plus
`SESSION_PER_EVENT`; the `source` union has no Kafka or Pulsar member. What
widening it requires is recorded in
[`roadmap.md`](./roadmap.md#production-hardening).

## End-to-end coverage

`packages/e2e-tests/test/trigger-wire.spec.ts` is the always-on Layer A
black-box suite for the public contract. It covers the complete nested shape,
unsupported future discriminators, create replay idempotency, update,
pause/unpause/archive, empty Session history, archived filtering, and keyset
pagination against the live Registry.

`packages/e2e-tests/test/trigger-agent-loop.spec.ts` runs in the Layer B stack
against the configured real TranscriptStore backend, Registry background
loops, Harness, sandbox runtime, and Claude provider. The test creates the
Agent, Environment, and Trigger exclusively through public APIs, then advances
the Trigger's persisted `next_fire_at` to the database clock so it does not
sleep until the next wall-clock minute. It does not call either reconciler
directly. The assertions wait for the public Trigger Session history, the
initial `user.message`, and the Harness-produced `agent.message` containing a
unique marker. This proves the complete durable handoff and Agent turn while
keeping the test deterministic across Kafka, Postgres, and Pulsar CI runs.
