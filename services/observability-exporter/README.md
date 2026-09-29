# Observability Exporter

`@orca/observability-exporter` is a private service package that exports Session transcripts from
Kafka as OTLP traces. `pnpm start` runs the service entrypoint. The package export remains the
established wire/projector API; persistence, Registry integration, and runtime orchestration are
service-internal modules rather than a reusable library contract.

## Default Kafka broker-native runtime

`OBSERVABILITY_EXPORTER_STATE_BACKEND=kafka` is the default. The process uses
`broker-main.ts` and `kafka-runtime.ts` without opening a Postgres connection, running SQL
migrations, or constructing a SQL repository. Registry remains the authority for binding policy
and credentials; its own database is independent of exporter persistence.

- Each single-partition Session topic is consumed directly in broker order. The same consumed
  records feed the acceptance-aware reducer: there is no inbox or second Transcript replay read.
  Session topics are discovered at startup and periodically through Kafka Admin metadata.
- Internal `session.user_event_completed` markers are skipped before projection identity checks
  and membership collection, as they do not contribute to traces. Their source offsets advance
  with the normal checkpoint transaction. Existing completion-marker identities remain unchanged;
  replaying these markers with different timestamps does not require a checkpoint migration or
  offset reset. Other event kinds retain envelope identity conflict checks.
- A Kafka transaction writes the per-Session v2 head and auxiliary state, sampled canonical delivery records,
  and source consumer-group offset together. Consumers use `read_committed`. Checkpoints contain
  the reducer, next source offset, event identity digests, accepted-source identities, and pinned
  non-secret delivery context; delivery records contain canonical traces and context. Metadata-only
  is the default. Explicitly authorized `raw_io` adds bounded unmodified turn/tool content to
  reducer state and delivery records. The exporter does not serialize full Transcript envelopes or
  its Registry credential bundle into those records; secrets already present in selected I/O are
  copied unchanged.
- Context is resolved when initializing a Session checkpoint. Disabled/suppressed results and valid
  Registry configurations outside the supported Langfuse HTTP/JSON capabilities (including
  `protocol: http/protobuf` or `compression: gzip`) persist a terminal `deliveryContext: null`
  suppression decision for that Session. Suppressed Sessions advance checkpoints and source offsets
  transactionally without emitting delivery records or resolving secrets; supported Sessions continue
  normally. Restarts retain suppression without re-resolving context. Malformed persisted checkpoints
  or delivery contexts still fail closed rather than being converted into suppression.
  Raw-I/O pins also resolve fresh context for each projection chunk; routing and sampling remain
  immutable. Capture restriction scrubs pending/active I/O and remains sticky after restart. Metadata
  and terminal-null pins never expand. Delivery resolves current audited credentials before every
  send, checks binding/version and requires `raw_io` authority for content-bearing v2 traces.
  Disallowed queued content is terminally suppressed, not rewritten or resent as a different payload.
- Assignment restore writes a transactional barrier and waits for the shared checkpoint reader through that
  barrier before validating the scoped state. Restore and state growth are bounded; exceeding limits, identity conflicts, invalid
  state, or fatal consumer/producer failures fail closed for the **instance**, not a durable
  per-Session quarantine. Do not treat a restart or offset reset as a repair for corrupt state.
- Delivery is at-least-once relative to external HTTP: acceptance and Kafka offset commit cannot be
  atomic. A crash or lost assignment after acceptance can send the same deterministic trace again.
  Terminal results transactionally advance the delivery offset and one compacted progress value per
  delivery partition. This progress restores expired/deleted group offsets without resending completed
  history. Missing or inconsistent progress with an advanced group offset fails closed. Kafka mode
  does not persist SQL-style outcome rows or one record per HTTP attempt.
- Delivery processes up to eight partitions concurrently per instance, preserving record order
  within each partition. Each partition has its own fenced transactional producer, progress, and
  retry state. Slow HTTP occupies one concurrency slot rather than serializing the whole consumer;
  this is bounded partition concurrency, not per-binding fairness.
  Manifest-backed delivery additionally holds a single shared permit through assembly and HTTP send;
  inline delivery retains the partition concurrency above. Source projection has two permits by default.
- Retryable transport/Registry failures and OTLP HTTP 429/502/503/504 pause the affected delivery
  partition with exponential backoff, bounded jitter, and bounded `Retry-After` (up to one hour).
  Attempt counts and timers are in memory and reset on restart; the uncommitted delivery remains
  in Kafka. A paused partition blocks other records on that partition, not a binding-wide schedule.
  OTLP 401/403 performs one fresh audited secret resolution and resend; a second rejection is terminal.
- OTLP egress accepts only HTTPS public endpoints, rejects private/link-local/metadata addresses,
  pins the approved DNS address into a direct socket, follows no redirects, and uses no ambient proxy.
  SIGTERM/SIGINT stops new work and cancels incomplete Registry/HTTP requests. A fully classified
  terminal response can still complete its Kafka progress transaction while assignment remains valid.

### Internal topics, identities, and ACLs

All topic names below include `KAFKA_TOPIC_PREFIX` (empty by default). Startup creates missing
internal topics and validates existing ones; it does not repair incompatible policies.
Broker bootstrap uses `KAFKA_TOPIC_LISTING_MODE` independently of `KAFKA_CONNECTION_MODE`.
The default, `canonical`, requires exact canonical names: with a non-empty prefix,
bare internal topics do not prevent creation of prefixed topics, and bare Session topics are
ignored. Explicit `bare-alias` mode accepts bare local names as aliases for prefixed internal
and Session topics, while always using canonical prefixed names for runtime access. Use it
for endpoints such as Kafka-on-Pulsar (KoP) that omit the namespace prefix in topic listings;
do not enable it on brokers where bare and prefixed names identify distinct topics.
TLS, SASL, token authentication, and the prefix alone do not enable alias matching. An empty prefix keeps
ordinary unprefixed discovery and provisioning unchanged.
This setting controls the broker-native exporter only; registry/harness discovery and the
legacy Postgres exporter are unchanged.
Runtime startup rejects missing, duplicate, or unexpected topic metadata and requires at least
one delivery partition and exactly one partition for every source/checkpoint topic.

Bootstrap and periodic discovery retry failed `admin.listTopics()` calls with cancelable
exponential backoff (1 second, doubling to 30 seconds), with a fresh 15-minute budget for each
listing. Only allowlisted metadata availability errors, request timeouts, and transient network
errors qualify; see `metadataRetryType` in [broker-main.ts](src/broker-main.ts). KafkaJS
retry-exhaustion wrappers are classified by their cause, not a generic `retriable` flag.
Clean connection closes qualify even when KafkaJS supplies no network error code. KafkaJS 2.2.4
can discard broker errors on its warm-cache path and throw a null-metadata `TypeError` instead.
For that exact error originating in KafkaJS's admin listing, one isolated admin with an empty
cache probes again and is disconnected afterward. Only the probe's actual error is classified
for retry; authorization failures and unrelated `TypeError`s are not treated as availability
errors. The probe shares the listing budget and does not replace any runtime client. If the
probe also loses its error, it fails closed rather than recursively probing or retrying a
`TypeError`.
Authentication, configuration, state, and other errors propagate without this outer retry.
Connect, topic creation, and runtime startup retain their existing KafkaJS behavior; a partially
started runtime is never started again. Structured JSON warnings identify the component and
retry code, phase, allowlisted error type, and delay, without raw error messages or broker details.

During initial listing retry the process is Live but NotReady. Failed discovery leaves existing
Session routes running and does not submit an empty replacement. Readiness retains its direct
broker check without the outer retry loop. Runtime failure is checked before each discovery
retry (at most one 30-second backoff later when no KafkaJS call is in flight). SIGTERM/SIGINT
or caller cancellation interrupts backoff and triggers cleanup. The budget includes time spent
inside KafkaJS calls and prevents starting another attempt at expiry; it is not a hard call or
shutdown timeout. In-flight calls finish under KafkaJS's own timeout/retry settings and can
overrun the budget; a successful call is accepted, while a failed call at expiry propagates
its original error, including any KafkaJS wrapper.

| Topic suffix                        | Creation defaults / required policy                                                                         |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `orca.observability.v1.checkpoints` | One partition; `cleanup.policy=compact` exactly, not `compact,delete`.                                      |
| `orca.observability.v1.delivery`    | Eight partitions on creation; `cleanup.policy=delete`, `retention.ms=-1` and `retention.bytes=-1` required. |

These are the raw-mode names. Avro mode uses `orca.observability.v1.checkpoints-avro`
and `orca.observability.v1.delivery-avro`, with the same policies. Internal records remain JSON,
including the v2 state/manifest formats. Transcript discovery selects only `.events-avro` in Avro mode or
`.events` in raw mode, even when both exist.

Replication factor is not explicitly set by the bootstrap; broker defaults apply. Unlimited delivery
retention is a safety constraint, including for already delivered records, not a storage cleanup policy.
Identity ledgers are retained without eviction. The original checkpoint topic and per-Session key
remain authoritative: the key now stores a version-2 head with route, next offset, pinned context,
ledger counts and reducer reference. Auxiliary keys in that same compacted topic are scoped by
SHA-256 of the checkpoint key: `I/<scope>/` for event identities, `A/<scope>/` for accepted-source
identities, `R/<scope>/state/` for reducer chunks, `R/<scope>/import/` for bounded v1 import
evidence, and `D/<scope>/` for content-addressed delivery chunks. Ledger lookups are bounded point
reads; restore checks maintained I/A cardinalities and authenticates the bounded import evidence,
not a full digest of native v2 history.

Small reducer/delivery values stay inline (approximately 64 KiB); larger values use a manifest
with byte count, chunk count and SHA-256 plus base64 chunks of up to 48,000 raw bytes. Assembly
validates scope, counts, canonical encoding and digest before decoding. Default decoded JSON assembly
is bounded at 32 MiB per blob and the source transaction budget is 64 MiB across serialized state
and delivery records, including keys and a conservative 128-byte framing allowance per record.
Produce requests are split into batches of at most 512 KiB and 500 records, without splitting
the Kafka transaction. Base64 and framing consume transaction budget in addition to decoded bytes.
A pre-transaction budget failure can halve the source batch; a single transition exceeding the
budget fails closed. This does not retry an ambiguous commit. These limits do not remove the
reducer collection bounds described below.

One shared `read_committed` checkpoint reader per runtime starts from the beginning with a fresh
temporary group and automatic commits disabled. It builds a fresh `mkdtemp` SQLite index on a
worker thread, shared by all Sessions and delivery partitions in that runtime. Kafka is the source
of truth: no local cursor or previous SQLite file is trusted on restart. The index is removed on
normal close; abrupt termination can leave scratch directories, which are never reused. Its default
1 GiB quota reserves database and rollback-journal space: usable database pages are approximately
one third of that budget, specifically `floor((maxBytes - 65536) / (3 * 4096))` pages. It uses
DELETE journaling, not WAL, and UTF-16 storage plus index overhead also consumes space. This is
not a promise of 1 GiB of live Kafka values. Quota/native-worker failures fail the instance closed;
worker errors expose fixed safe messages, not SQL, paths, keys or native exception payloads.
Unexpected worker death notifies the runtime immediately, including when no state query is active;
diagnostic sampling is not the failure detector. The shared reader uses a 60-second session timeout
and maintains its own single-flight heartbeats during index I/O and barrier snapshots. Stale or
failed snapshots never authenticate a waiting owner.

Shutdown drains underlying inline and manifest delivery operations, not only their heartbeat races.
Expected cancellation exits normally, but a genuine failure discovered during the drain retains
failed status and produces an error exit. Cleanup still disconnects Admin if shared-state close
fails. Index close allows one second for close IPC before best-effort worker termination; this does
not guarantee preemption of stuck native/kernel I/O, and live database files are not removed before
termination completes.

Startup admission defaults to eight concurrent source connects/joins and two lazy assignment
restores, with cancellation of queued work. Shared-reader barrier/catch-up waits default to 120 seconds;
the old per-Session 64 MiB/100,000-record checkpoint-log scan limits do not apply. The scratch quota
and catch-up deadline still bound recovery. Source retention must preserve unread Transcript data;
deleting topics or resetting groups is not a supported recovery procedure.

The runtime emits fixed-schema JSON diagnostics at start/stop and every 60 seconds. Sampling is
single-flight and logging failures do not change readiness or processing. Admission summaries
contain active/queued work and lifetime maximum wait/run times; shared-reader summaries contain
scanned bytes/records, barrier waiters, allocated SQLite database bytes, the usable database limit
and scratch quota. Consumer counts cover owned source/delivery handles, not the one shared reader.
Producer count, attempted transaction/assembly byte peaks, shutdown drain time and diagnostic-error
count complete the summary. Stop duration includes cleanup; drain time measures outstanding work.
Budget errors include safe record kind, actual bytes and limit, never payloads, keys or credentials.
These diagnostics are logs, not a Prometheus endpoint, per-Session metric series or a startup SLO.

### State v1 to v2 upgrade and recovery

After fencing the source owner and restoring through its barrier, the runtime automatically imports
an already-persisted, valid v1 checkpoint of at most 512 KiB. One Kafka transaction writes its v2
head, I/A ledger, reducer/import evidence and unchanged source offset. Migration preserves the
Registry pin, terminal `deliveryContext: null`, source progress and pending inline delivery backlog;
it does not re-resolve context, backfill traces or change raw/Avro namespaces. The 512 KiB limit is
on the old checkpoint, not on the whole migration transaction.

Quiesce all old v1 writers before starting v2; use the chart Recreate rollout and stop any exporter
outside that Deployment sharing the namespace. Do not mix original v1 and v2 binaries. Once v2
state is committed, do not roll back to an original v1-only image. Retain a known
v2-capable recovery image and recover forward with the same topic prefix, encoding, groups and
transactional IDs. Do not delete checkpoint/delivery topics or reset progress to bypass validation.
Do not restore a scratch SQLite file as durable state. If scratch quota is exhausted, stop the
runtime and provision sufficient writable disk and quota before rebuilding from Kafka; do not
delete Kafka ledger keys.

### Stable namespaces and permissions

Let `N = observability-exporter-kafka-v1-<first 16 hex characters of SHA-256(topic prefix)>`
in raw mode; Avro mode appends `-avro` to `N`. Let
`H = SHA-256(canonical Session topic name)` (full lowercase hex):

- source group: `N-source-H`;
- delivery group: `N-delivery-v1`;
- source transactional ID: `N-source-H-partition-0-v1`;
- delivery transactional ID / compacted progress key:
  `delivery:N:<SHA-256(canonical delivery topic)>:<partition>:v1`;
- temporary restore group: `orca-exporter-restore-<UUID>`.

Replicas for the same topic prefix share this stable namespace; do not substitute Pod IDs or change
groups to bypass stored progress. The Kafka principal needs:

- metadata discovery / Describe and Read on authorized Session topics;
- Describe, Read and Write on both internal topics, plus DescribeConfigs for policy validation;
- Create for missing internal topics (or pre-provision both with the exact policies above);
- Read/Describe on the source and delivery groups above, and prefixed Read/Describe on
  `orca-exporter-restore-` for the shared checkpoint reader;
- Write/Describe on both source and delivery transactional IDs above for transactional production
  and offset commits. Grant the broker's idempotent/transactional-producer permissions as applicable
  (including cluster IdempotentWrite where required by its authorization implementation).

Kafka transaction and read-committed support is required, including when using a Kafka-compatible
endpoint. V2 reuses these topics and the existing temporary group prefix; it needs no new topic ACL.
These permissions differ from the legacy inbox/replay group prefixes below.
Remaining work and supported-backend scope are tracked in
[`docs/managed-agents/roadmap.md`](../../docs/managed-agents/roadmap.md).

### Explicit legacy Postgres state backend

`OBSERVABILITY_EXPORTER_STATE_BACKEND=postgres` retains the SQL runtime and requires an
exporter-owned `OBSERVABILITY_EXPORTER_DATABASE_URL`. It still reads Kafka Transcript, not
Postgres Transcript. Supplying a DSN without an explicit backend fails startup to prevent silent
migration. Selecting Kafka does not import SQL progress; keep existing installations explicitly on
Postgres unless performing a controlled cutover.

Only this legacy path uses `KafkaSessionEventSource` with a content-free durable inbox ACK, followed
by authoritative `KafkaTranscriptStore.read(..., { subpath: '*' })` replay. Its inbox is an
ACK/work-notification/identity ledger, not a replay watermark. Replay may run ahead of inbox ACK;
late inbox rows below the committed projection cursor are marked processed. Replay is bounded by
event count and raw broker-message bytes, admitting one oversized first record to retain its cursor.
Exporter-owned SQL stores identity digests, accepted-source identities, reducer state, canonical
outbox rows, sampling watermarks, conflicts, fenced leases, retry attempts and terminal outcomes.

Legacy claims serialize per binding and persist exponential-backoff scheduling and binding cooldowns;
claim-race reselection is bounded to eight attempts before yielding. SQL connection/query waits are
bounded. Shutdown cancels replay/Registry/OTLP work, stops lease renewal and attempts fenced release
of unfinished claims; complete accepted/partial responses attempt terminal completion instead.
Lease expiry is the fallback if completion or release cannot win. None of these SQL lease/scheduling
contracts describes the default Kafka runtime.

An accepted turn closed by `session.status_idle` preserves `end_turn` or
`retries_exhausted` in `orca.turn.terminal_reason`. An applied interrupt uses
`end_turn`; its acceptance increments `orca.turn.interrupt_count` on an active
turn. `requires_action` keeps the turn open. An interrupt alone does not mark
the turn as an execution error; unrecognized stop reasons remain `unknown`.

## Outcome evaluator observations

An explicit primary-path `span.outcome_evaluation_start` inside an accepted turn opens an
`evaluator` child. Its canonical envelope ID, not payload `id`, is the start identity.
`span.outcome_evaluation_end` correlates only by `outcome_evaluation_start_id`, with matching
`outcome_id` and zero-based `iteration`; there is no latest-open fallback. Ongoing heartbeats are
ignored, without reading their payload or creating observations. Child IDs include the trace ID,
canonical `outcome_evaluation` observation family, primary subpath, and start event ID. The accepted turn
root remains authoritative: evaluator completion neither closes the turn nor changes root status.

Evaluator observations are metadata-only: start/end IDs, iteration, and the canonical
result enum. Outcome descriptions, rubrics, explanations, prompts, transcript content, and the
producer's placeholder zero usage are not exported. The separated Claude Harness emits these
events before terminal idle. It maps judge verdicts to `satisfied`, `needs_revision`, or
`max_iterations_reached`, and a thrown evaluator exception to `failed`; the shared result contract
also accepts `interrupted`. A negative judge verdict does not prove a model invocation failed, and
the default judge can represent transport errors as negative verdicts. Only `failed` proves an
error status; `interrupted` carries unset status (OTLP code `0`, not success or failure). No evaluator
model/generation span, provider, usage, or cost is inferred. Langfuse
OTLP uses `langfuse.observation.type=evaluator` and bounded Orca observation metadata without
inventing GenAI model-call attributes.

Producer outcome IDs can contain description-derived slugs. The reducer converts each nonempty
outcome ID of at most 512 UTF-16 code units into a domain-separated SHA-256 correlation key before
retaining it. Only that opaque key enters open checkpoint state; neither the raw outcome ID nor
the key enters completed spans, outbox metadata, or OTLP. Start event IDs provide external
observation identity. This digest is a correlation mechanism, not encryption or a secrecy guarantee
against guessing low-entropy descriptions. Missing/oversized IDs, malformed results/correlation,
unmatched ends, and child-subpath evaluations are ignored. A mismatched end leaves the matching
start available. The first admitted start and first
valid matching end win; duplicates never create another observation. An unclosed start is omitted
at turn closure, not converted into an invented terminal evaluation.

Reducer v3 checkpoints and legacy v1/v2 checkpoints support optional `openEvaluations` (maximum 64) and
`completedEvaluations` (maximum 256) arrays. Exceeding either bound fails projection closed rather
than evicting correlation state. Restart reconstruction allowlists metadata and verifies child/root
IDs, unique starts, and disjoint open/completed sets. The outbox permits at most 256 model summaries,
256 tools, and 256 evaluators, independently bounded, with a total limit of 768 children. No SQL
migration is needed; the canonical trace schema
label remains v1. Legacy pre-evaluator checkpoints and traces remain readable and do not gain
invented evaluator observations. Evaluator checkpoint reconstruction requires opaque correlation
keys; completed checkpoints and the outbox reject evaluator `outcomeId`/`outcomeKey` fields and
outcome identity metadata, including description slugs.
Older binaries do not support evaluator outbox rows and can discard evaluator
checkpoint fields: use evaluator-capable workers exclusively once these observations are enabled.

## Deterministic sampling

The Session's immutable Registry binding/config version pins `sample_rate` in `[0, 1]`: zero
selects no turns and one selects every eligible turn. The reducer decides once trace identity is
known at acceptance, before constructing model summaries, tools, evaluators, or canonical traces. The whole trace,
including its root and children, shares that decision. It does not read a sampled-out turn's
model-summary, agent tool, or evaluator observation payloads. Client input payloads, including tool
results, are still decoded for the exact companion system-message ID needed by acceptance. Only
that lifecycle linkage is retained for sampled-out tool results, not tool correlation or outcome.
Metadata-only reduction constructs no captured input/output. In raw mode, a user-message
candidate uses the same deterministic trace ID and sampling policy before retaining unmodified
pending input. Only its exact acceptance attaches that input to a trace. Sampled-out candidates
and turns retain no captured I/O.

The algorithm is `orca.observability.trace-sampling.v1`. Its SHA-256 input is the UTF-8 algorithm
label, one NUL byte, and the compact JSON tuple `[bindingId, bindingVersion, traceId]`. The first
eight digest bytes are an unsigned big-endian integer `h`. Selection is the strict comparison
`h < sampleRate * 2^64`, using the exact binary64 rate returned by the Registry client. Integer
arithmetic computes the equivalent exclusive cutoff `ceil(sampleRate * 2^64)` without rounding
the hash to a JavaScript Number; equality is excluded, including exact fractional boundaries.

Reducer state versions 2, 3 and 4 store the algorithm version and pinned binding ID/config version/rate.
A changed Registry pin for that Session fails closed instead of resampling it. Legacy SQL
quarantines the Session; Kafka projection errors stop the instance. A sampled-out completion updates one content-free watermark with workspace/Session
identity, first/latest trace IDs and completion source sequences, a turn counter saturating at
`2^63 - 1`, and reason `sampled_out`. It coalesces completions, not a list of individual turns.
The existing accepted-source digest ledger still handles duplicate acceptance markers.

Kafka commits sampling state within its checkpoint transaction alongside source progress and sampled
delivery records. In legacy mode, the same fenced Postgres transaction commits the watermark, reducer
state, accepted-source identities, source checkpoint, sampled canonical rows, and inbox completion markers. Sampled-out
turns create no canonical outbox row, secret resolution, provider client, or OTLP send. The
legacy repository also rejects sampled-out canonical rows at insertion and claim boundaries.

Version 1 metadata-only reducer states adopt sampling when a supported Registry policy is first
resolved, including open turns. The combined reducer reads versions 1–4. Ordinary metadata-only
reduction continues writing version 3; adoption of raw capture writes version 4 with sticky
capture mode. A restricted v4 state stays v4/metadata-only and does not re-expand. Only v4-capable
workers can operate on v4 state. This is not a rewrite of historical traces. Existing canonical
`orca.observability.projected-trace.v1` rows remain readable and unchanged; tool and evaluator children are additive
allowlisted variant. Do not rewind checkpoints or backfill historical turns under their existing trace
IDs: adding children would conflict with the already committed canonical payload hash.

## Primary-path tool observations

The projector recognizes `agent.tool_use` / `agent.tool_result` (local),
`agent.mcp_tool_use` / `agent.mcp_tool_result` (MCP), and `agent.custom_tool_use` /
accepted `user.custom_tool_result` (custom). Accepted `user.tool_result` completes a local tool.
There is no `agent.custom_tool_result` completion kind. Only canonical primary subpath `''` events
participate; no hierarchy is guessed from tool payloads or native IDs.

A use's explicit `tool_use_id` is its correlation key when present; otherwise its canonical event ID
is the key (custom uses always use their event ID). This supports the separate producer's
`{id,name,input}` payload, whose result points to the canonical use event ID, and the in-sandbox
producer's `{name,input,tool_use_id}` payload, whose result points to the independent native ID.
Keys are family-separated fixed-width domain-separated digests in reducer state, not exported raw
native IDs. Public source identities use the existing bounded canonical event-ID representation.
Client results use only `tool_use_id` for local tools or `custom_tool_use_id` for custom tools.
They complete a tool only after the exact `session.user_event_processed.user_event_id` acceptance;
pending client inputs retain safe correlation metadata across batches. Raw mode also retains
bounded unmodified result content for a matching sampled active tool; metadata-only retains none.
The receipt is pinned to the sampled active trace at arrival. Pre-turn or legacy pending results
without that receipt remain unmatched on acceptance; their payloads are not reread or associated
with a later turn's reused native ID.

Sampled raw-I/O turns also preserve explicitly accepted `user.tool_confirmation` facts. A
confirmation is tied to its active trace at arrival and attaches only after the exact acceptance
marker, with a unique matching local/MCP tool. Its last accepted decision is exported as
`orca.tool.last_approval.result` (`allow`/`deny`), `.source_event_id`, `.acceptance_event_id`, and
`.accepted_at` under observation metadata. Unaccepted, ambiguous, orphan, custom-tool or cross-turn
confirmations do not create approval facts. `deny_message` and raw native tool IDs are not exported.
Approval is permission, not execution: allow does not complete a tool, deny does not rewrite its
result/status, and the facts are removed when capture is restricted.

Each child has fixed name `orca.agent.tool`, observation type `tool`, the turn root as parent, and
ID `deterministicChildSpanId(traceId, 'tool', '', useEventId)`. Exported tool metadata contains
fixed family/outcome enums, projection version, primary subpath, bounded source IDs and, in
raw mode, the accepted approval facts described above; timestamps
come from the source events. Metadata-only does not export tool names, arguments, results, MCP
server names, titles or error text. In raw mode, original args/results use dedicated I/O
fields, and the bounded tool name becomes the OTLP display name and `gen_ai.tool.name`.
The canonical fixed name and correlation identity remain unchanged; no raw content hashes are exported.

## Trusted version and configuration attribution

The exporter snapshots Registry-authoritative agent ID/version, harness name/mode and configured
environment/release alongside the pinned delivery context. These optional flat fields survive
checkpoint/outbox restore. Delivery uses the persisted snapshot, not the latest agent/binding
configuration. Old queued contexts without attribution remain un-enriched. Labels are bounded to
128 characters and checked for their structural label format; unavailable or invalid labels are omitted.
Attribution values are not automatically masked for secret-like text.

Every observation in a newly attributed trace receives these applicable filterable fields:

- `orca.agent.id`, `orca.agent.version`
- `orca.harness.name`, `orca.harness.mode`
- `orca.observability.binding_id`, `.binding_version`, `.config_schema_version`
- `orca.deployment.environment`, `orca.deployment.release`

They use both `langfuse.observation.metadata.*` and `langfuse.trace.metadata.*`. Agent version also
maps to `langfuse.version`; configured environment/release map to `langfuse.environment` and
`langfuse.release`. Harness name/mode do not claim a harness binary version. Exporter resource
`service.version` remains separate from the agent revision and configured application release.
No tenant settings or environment variables are added for these correlations.

## Raw turn and tool I/O

Kafka mode accepts Registry-authorized `capture_mode: raw_io` with the existing Langfuse
HTTP/JSON target. Legacy exporter Postgres remains metadata-only and rejects raw-I/O contexts
and v2 content rows. No new target, content topic, policy service or eval worker is involved.

The `orca.observability.raw-io.v1` capture format records accepted primary user text and
tool names/arguments/results. Root output is an ordered array of non-partial assistant text messages,
labelled `orca.io.output_scope=turn_messages`; it is not an assertion that the last text block is a
complete final answer. The root text extractor excludes standalone thinking/signature, partial
delta, system-instruction and non-text message blocks; it does not inspect or filter similarly
named fields inside tool arguments/results. Mixed supported/unsupported content is marked partial. Subagent
events remain outside the primary-path projection. Model summaries remain SPAN observations, not
provider generations, with only the usage/cost actually reported by the existing producer.
Raw-I/O roots also carry the last explicitly reported `session.error` facts: original type/message,
`will_retry`, a valid positive `next_attempt`, and a valid reported `retry_delay_ms`. These map to
fixed `orca.last_error.*` / `orca.retry.*` observation metadata. They do not infer retry counts or
override the turn's final status, and metadata-only mode contains no diagnostic text.

Content traces use `orca.observability.projected-trace.v2`; metadata-only traces retain v1. Root/tool
I/O is encoded with `langfuse.observation.input` and `langfuse.observation.output` as JSON strings.
Capture format version, output scope and omission/truncation markers use
`langfuse.observation.metadata.orca.io.*`. No legacy trace-level I/O or synthetic generation is added.

There is no automatic redaction, sensitive-field filtering, PII detection or string replacement.
Admitted JSON values retain their keys and values, including fields named `password`, `api_key`,
`headers` or `environment` and token-like text. Raw capture can therefore disclose secrets or
personal data present in the selected I/O; explicit tenant/scope authorization is required.
The exporter does not copy its own Registry/Litefuse credentials into trace bodies.

Structural validation and capacity limits still apply: each serialized I/O value is at most 8192
bytes; pending I/O and active-turn I/O each have a 262144-byte budget. Oversized values are omitted
whole and accumulated root output is marked truncated when its budget is exhausted. Unsupported
non-JSON values are not executed or coerced. Tool names are bounded strings; payload text is not
masked. Reasons include `unsupported`, `too_large`, `budget`, `partial` and `unavailable`.
Restore validates format and bounds without modifying or enriching the stored content.
Logs and metrics do not include I/O bodies.

The pre-existing `redacted_io` control-plane value remains reserved/unsupported by this exporter.
It is not an alias for `raw_io`, and existing redacted settings or pins are not converted to raw.
Raw capture requires the explicit new mode and raw-compatible platform/org/workspace ceilings.
Registry migration `0060_agent_observability_raw_io.sql` adds the allowed database value without
rewriting existing settings or pins; apply it before requesting raw capture.
Its Session-pin CHECK is installed `NOT VALID` to avoid a historical-row scan under the migration
transaction's exclusive lock. The Registry migration runner validates it after that transaction
commits, while retaining the advisory migration lock. New writes are checked immediately; an
interruption before validation is healed by rerunning the migration runner. The DDL still needs
a brief exclusive lock; this is not a lock-free rollout.
The mode ordering is `metadata_only < redacted_io < raw_io`; lowering either content ceiling
advances the existing restriction epoch. The platform default is unchanged.

Raw-I/O projection checks fresh context once per bounded chunk. `captureBatchTimeoutMs` is an
internal runtime option (30000 ms default, 60000 ms maximum), not a tenant policy or new environment
setting. An expired uncommitted candidate is discarded and reauthorized. After two expirations in
one callback, projection releases its admission slot without advancing the remaining offsets;
Kafka redelivers them. Projection-time expiry also reduces the source chunk size for that redelivery.
This bounds authorization age at transaction admission, not the duration of an already admitted Kafka transaction or HTTP
request. Existing Registry sticky restriction epochs and Kafka ownership fencing remain in force.

Registry platform/org/workspace ceilings and the immutable Session pin still determine the effective
mode. The migration-seeded platform maximum remains `metadata_only`; this exporter does not change
it. Registry's admin listener exposes Platform-admin-only `GET/PUT /v1/platform/agent_observability`
for explicit policy changes with ETag, idempotency and atomic restriction-epoch/audit updates; see
[platform policy administration](../../docs/managed-agents/workspace-administration.md#platform-observability-policy).
A binding request alone cannot bypass that ceiling, and old
Sessions do not acquire content permission by changing current settings.
The platform row is global: raising its maximum can authorize new pins in other organizations or
workspaces whose requested mode and ceilings already permit raw capture. It is not a
workspace-local switch, and a one-time inventory does not prevent concurrent configuration changes.

An isolated test Registry/database, dedicated org/workspace/project and synthetic data provide the
narrowest enablement boundary. A shared deployment requires explicit platform-change authority and
an inventory of every potentially affected new pin. Existing admin organization/workspace GET/PUT
operations use their scoped credentials, fresh ETags and idempotency keys; a new Session verifies
the resulting pin. Workspace `mode: disabled` stops that workspace, including custom bindings;
disabling only the organization default does not disable workspace custom bindings. A platform
ceiling reduction advances its restriction epoch; restoring an earlier numeric epoch is not a
rollback. This exporter neither performs these operational mutations nor silently deploys itself.

Internal delivery retention is still unlimited. Raw content can reside in Kafka records/chunks
and the scratch index; capture restriction prevents subsequent content delivery but is not a physical
deletion guarantee or a deletion request to Litefuse. The smoke fixtures use synthetic data. Real
content use requires authorization with this retention boundary understood. Retention and cleanup
work is tracked only in the roadmap.

Matched results use boolean `is_error` to distinguish success from error; a tool error does not
by itself fail the turn. `user.tool_confirmation` records permission, not completion, and an interrupt
does not prove tool cancellation. `requires_action` keeps the turn open. At the terminal boundary,
an unmatched use becomes an `incomplete` observation with unset span status (OTLP code `0`), not an
invented execution result. Its end source ID/time identify the observation cutoff.
Langfuse receives tool children as `tool`; coarse turn model summaries remain generic `span`.

Each turn retains at most 256 tool uses (open and completed combined) and 256 result receipts;
the pending client input collection is separately bounded at 1,024 entries. Result receipts retain
bounded source IDs, timestamps, family/outcome enums, and optional correlation digests, including
for unmatched results. Unmatched results also increment a counter saturating at
`Number.MAX_SAFE_INTEGER`, retained at completion as `orca.turn.unmatched_tool_result_count` when
positive. A single orphan result does not itself quarantine a Session, but exceeding either tool
collection limit raises `CanonicalProjectionStateError`: Kafka fails the instance closed, while the
legacy SQL runtime quarantines the Session's projection. Duplicate source identities are idempotent and conflicting identities are not silently
overwritten. Acceptance-marker-first unresolved behavior remains unchanged.

## Delivery outcomes

The client accepts only bounded HTTP 200 `application/json` responses matching the OTLP/HTTP JSON
`ExportTraceServiceResponse` schema. It interprets only the lowerCamelCase `partialSuccess`,
`rejectedSpans`, and `errorMessage` fields, ignores unknown fields, treats field `null` as unset,
rejects malformed UTF-8 or unpaired surrogates in `errorMessage`, and preserves rejected span counts as
canonical non-negative int64 decimal. `send()` returns `accepted` or `accepted_with_warning` outcomes;
partial rejection retains the `OtlpPartialSuccessError` contract. Warning and partial summaries contain
only the rejected count, UTF-8 message byte length, and SHA-256 digest, never raw provider messages.

Kafka treats these outcomes as terminal and commits the delivery offset, without persisting the
outcome details. HTTP acceptance is not atomic with that commit, including shutdown at response EOF.

### Legacy SQL outcome persistence

Only the Postgres runtime atomically records these outcomes with terminal status and lease release:

| Delivery outcome        | Outbox status | Rejected spans | Message metadata                       |
| ----------------------- | ------------- | -------------- | -------------------------------------- |
| `accepted`              | `delivered`   | `0`            | null                                   |
| `accepted_with_warning` | `delivered`   | `0`            | positive byte length and SHA-256       |
| `partial_rejection`     | `suppressed`  | positive int64 | byte length and SHA-256, even if empty |

The four nullable, no-default columns are `delivery_outcome`, `delivery_rejected_spans` (BIGINT),
`delivery_message_bytes`, and `delivery_message_sha256`. Existing rows remain null on migration:
legacy delivery/suppression is not reclassified or backfilled. Rejected counts remain canonical decimal
strings through delivery and persistence, including `9223372036854775807`. Terminal writes require the
current lease owner, generation, and unexpired lease; stale workers cannot attach outcome metadata.

Legacy SQL projector and delivery loops log only bounded error codes. Delivery logs include durably scheduled
retries and deduplicate consecutive identical codes across retries and idle polls. A completed
delivery or terminal suppression resets that deduplication; scheduling a retry does not. Logs contain
only the component and safe code, never upstream error details, response bodies, headers, or secrets.
Context resolver HTTP 400/404 responses quarantine that Session projection; authentication,
unavailability, and transport failures remain pending for retry.
Retryable delivery failures are not immediately claimable again. Partial OTLP rejection remains
terminal for the whole row with `suppression_reason=partial_rejection`; it does not retry the whole
request. Zero-rejected warnings are full acceptance, not retryable failures. Other non-200 OTLP
responses, except retryable 429/502/503/504, are terminal after the one-time 401/403 credential refresh path.

## Runtime configuration

### Optional Helm workload and image

`services/observability-exporter/Dockerfile` builds a Node 22 non-root runtime from the repository
root. Worker-thread SQLite uses the native `better-sqlite3` dependency, not experimental `node:sqlite`.
The build stage includes Python, make and g++ for native installation; the final image runs the
deployed production dependency under Node 22 without those build tools.
The release workflow builds `orca-observability-exporter` alongside Registry and Harness and
publishes it to `ghcr.io/orca-ae` for a publishing release; the packaged chart points at that
repository. The chart tag defaults to its release `appVersion`. Use a packaged release
containing this workload, or explicitly set `images.observabilityExporter.repository` and `tag`
to a published image; checked-in development defaults do not establish that an image exists.
The existing `release` workflow accepts `workflow_dispatch` inputs `version` (SemVer) and
`publish: true`, or a `v*.*.*` tag push. A tag push always publishes; a manual run publishes only
with `publish: true` and is otherwise a dry run. Images are pushed with the workflow's
`GITHUB_TOKEN`. An optional job mirrors each release to Docker Hub when the `DOCKERHUB_NAMESPACE`
repository variable and the `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets are set. Dispatch
produces a chart artifact; a publishing tag push also attaches the chart to the GitHub Release.

The chart defaults `observabilityExporter.enabled` to `false`. Enabling it creates a Deployment
and ConfigMap, not a Service or Ingress. It uses the dedicated exporter ServiceAccount already
authorized by Registry, with automatic API-token mounting disabled and a read-only audience-bound
projected token at `internalAuth.tokenMountPath`. Tokens are reread on each Registry request.
The exporter receives neither the chart-wide Secret nor Registry database credentials.
Kafka mode uses Recreate and a writable disk-backed `emptyDir` at `/var/run/orca/exporter-state`,
with `observabilityExporter.kafkaStateSizeLimit` defaulting to `2Gi` and runtime quota defaulting
to 1 GiB. Keep volume capacity and ephemeral-storage resources consistent with the quota and
journal budget. Standalone configuration may omit the directory to use the OS temporary directory;
the container image itself sets the same `/var/run/orca/exporter-state` parent as the chart.

Minimal values overlay that enables the exporter (in addition to the normal Registry/Harness values):

```yaml
transcriptStore:
  backend: kafka
  kafka:
    brokers: kafka.example:9092
observabilityExporter:
  enabled: true
  replicaCount: 1
  stateBackend: kafka
  secretKeyRefs: {} # Add Kafka credentials here when required by connectionMode.
```

When enabling the exporter on a release installed before the exporter workload was added,
use `helm upgrade ... --reset-values -f <complete-operator-values.yaml>`, not `--reuse-values`.
Retain every existing operator override in that file, then add the exporter overlay above.
The old values contain only the exporter ServiceAccount, not its image, replica or probe defaults;
the chart rejects enabling that incomplete schema with migration guidance, even if an image is
supplied separately. Resetting loads the new defaults, including all three health probes.
Keeping the exporter disabled does not require this migration.

Kafka mode needs no exporter database or database Secret; the chart rejects an old
`OBSERVABILITY_EXPORTER_DATABASE_URL` secret reference, nonempty `extraEnv` literal, or
`extraEnv` `valueFrom` with `stateBackend: kafka`. Empty literals are allowed. Helm cannot inspect
Secret/ConfigMap contents referenced by `extraEnvFrom`; operators must remove legacy DSNs from
those sources during controlled cutover. Explicit Kafka selection ignores a DSN at runtime,
so successful rendering or startup does not prove SQL progress was retained. For an existing
SQL installation, set `observabilityExporter.stateBackend: postgres` and retain that Secret reference.
Only legacy mode requires a separate exporter-owned Postgres database and applies schema migrations
at startup; its role needs DDL and read/write privileges on that database. Sharing a Postgres server is supported, sharing the Registry database is not.
Database TLS is configured through the exporter DSN (for example `sslmode=verify-full` and
`sslrootcert` pointing at a mounted CA); `extraVolumes` / `extraVolumeMounts` supply PEM files.
The chart does not copy the Registry DB TLS settings.

Kafka brokers, mode, topic prefix, TLS and discovery settings inherit `transcriptStore.kafka`.
Supply the matching credentials explicitly through exporter `secretKeyRefs`; TLS paths require
matching exporter mounts. Default Kafka mode requires the internal-topic, group, restore and
transactional ACLs listed above. Only legacy Postgres mode uses Read access to these group **prefixes**,
in addition to Session-topic discovery and reads:

- `observability-exporter-inbox-`: event ingestion uses one group per topic, suffixed by a topic hash.
- `transcript-store-`: authoritative projection replay creates groups with random/timestamp suffixes.

Use prefixed group ACLs, not an ACL for the literal `observability-exporter-inbox` group.
Granting only the inbox prefix permits legacy ingestion but leaves replay unauthorized. Legacy
replicas share one exporter DB; do not run independent databases against the same inbox group prefix.
The Registry origin defaults to this release's internal listener and can be overridden with
`observabilityExporter.registryInternalBaseUrl`. `extraEnv` / `extraEnvFrom` expose the runtime
configuration below, including pool/timeout tuning. They are operator overrides, not tenant config;
do not override the projected identity or supply Langfuse keys there. Referenced Secret changes
require a pod restart for environment values; the projected Registry token rotates without one.

The process exposes only `GET /healthz` and `GET /readyz` on port 8080. Liveness reports event-loop
responsiveness and rejects shutdown or a failed runtime. Kafka readiness requires initial discovery,
a ready runtime, and a successful Kafka Admin topic-list request; it does not query Postgres. Legacy
readiness requires completed migrations, a ready Kafka event source, and a bounded `SELECT 1`. Concurrent readiness requests share one check. In legacy mode, the default
25-second readiness timeout covers the default 10-second pool acquisition plus 11-second client query timeout;
adjust `readinessProbe.timeoutSeconds` if increasing the DB timeout. Readiness is not proof of
completion of every assignment restore, source/backlog catch-up, Registry authorization, tenant
binding validity, delivery progress, or remote ingest availability. The startup probe uses `/healthz`,
not a recovery-completion check; do not mask restore failures by merely extending probe timeouts.
There is no Prometheus metrics endpoint.
The workload has a 60-second shutdown grace period and configurable probes/resources/scheduling.
`observabilityExporter.podLabels` accepts a map of additional labels. The chart rejects
`app.kubernetes.io/name`, `app.kubernetes.io/instance`, and `app.kubernetes.io/component`
because these labels must match the Deployment selector.

An operator's control plane configures bindings separately on the Registry admin listener: GET organization
or workspace `agent_observability` for its `ETag`, then PUT with `Idempotency-Key` and applicable
`If-Match`, using `observability:write` / `org:admin` authority (reads need `observability:read`).
Use target `adapter_type: otlp_http`, `endpoint_kind: traces_endpoint`, `endpoint_class: public`,
and an HTTPS `endpoint_url` ending exactly in `/api/public/otel/v1/traces`. Config must select
`semantic_profile: langfuse`, `protocol: http/json`, `compression: none`,
`capture_mode: metadata_only` (default) or Kafka's explicitly authorized `raw_io`, a bounded
`timeout_ms`, and `sample_rate` in `[0,1]`. All applicable ceilings and the Session pin must allow
raw capture; the platform seed remains metadata-only. First configuration/target replacement includes write-only
Basic credentials (username = project public key, password = project secret key). Workspace
custom configuration uses `mode: custom`; inherited mode uses the organization default. These
keys belong in Registry SecretStore, never Helm values. Selection is pinned at Session creation;
create a new Session to exercise changed selection. See
[`workspace-administration.md`](../../docs/managed-agents/workspace-administration.md) for
replacement, precondition and rotation contracts. Network access is needed to Kafka, Registry internal HTTP, DNS and the public HTTPS target
(and exporter Postgres only in legacy mode); Registry needs TokenReview
access. No tenant keys or provider project provisioning are performed by this chart.

### Standalone process

Start with a Kafka Transcript broker and Registry internal resolver; no exporter Postgres is required:

```bash
TRANSCRIPT_STORE_BACKEND=kafka \
REGISTRY_INTERNAL_BASE_URL='http://localhost:8081' \
INTERNAL_SERVICE_TOKEN_FILE='/run/secrets/internal-service-token' \
KAFKA_BROKERS='localhost:9092' \
pnpm --filter @orca/observability-exporter start
```

Settings marked **Legacy-only** below are read and validated only with
`OBSERVABILITY_EXPORTER_STATE_BACKEND=postgres`; Kafka mode ignores their environment values,
including invalid or stale values retained after cutover. Shared Kafka, Registry, discovery, and
projector batch-size settings remain validated for the selected backend. A database URL without
an explicit state backend still rejects startup to protect existing SQL progress. The defaults
for Legacy-only settings apply to Postgres, not to broker-native scheduling or delivery.
The `OBSERVABILITY_KAFKA_*` state/admission settings below are Kafka-only: explicit Postgres
mode ignores them, including invalid values. The directory is a writable scratch parent, not a
durable database location.

| Variable                                                           | Default                      | Behavior                                                                                                                                                 |
| ------------------------------------------------------------------ | ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TRANSCRIPT_STORE_BACKEND`                                         | `kafka`                      | Must be `kafka`; any other Transcript backend fails startup.                                                                                             |
| `OBSERVABILITY_EXPORTER_STATE_BACKEND`                             | `kafka`                      | `kafka` uses broker-native state; explicit `postgres` retains legacy SQL progress.                                                                       |
| `OBSERVABILITY_KAFKA_STATE_DIRECTORY`                              | OS temporary directory       | Kafka-only absolute scratch parent; optional. Image/chart set `/var/run/orca/exporter-state`. Each runtime creates a fresh subdirectory.                 |
| `OBSERVABILITY_KAFKA_STATE_MAX_BYTES`                              | `1073741824` (1 GiB)         | Kafka-only database plus rollback-journal budget, 131072–1099511627776 bytes; usable DB pages are approximately one third.                               |
| `OBSERVABILITY_KAFKA_STATE_CATCHUP_TIMEOUT_MS`                     | `120000`                     | Kafka-only shared-reader barrier/catch-up deadline, 1–600000 ms.                                                                                         |
| `OBSERVABILITY_KAFKA_MAX_ASSEMBLY_BYTES`                           | `33554432` (32 MiB)          | Kafka-only decoded JSON bytes per reducer/delivery blob, 1–67108864; not total process memory.                                                           |
| `OBSERVABILITY_KAFKA_MAX_TRANSACTION_BYTES`                        | `67108864` (64 MiB)          | Kafka-only combined serialized state/delivery budget including key/framing allowance, 1–134217728 bytes. Produce batches remain at most 512 KiB.         |
| `OBSERVABILITY_KAFKA_STARTUP_CONCURRENCY`                          | `8`                          | Kafka-only source connect/join admission, 1–32.                                                                                                          |
| `OBSERVABILITY_KAFKA_RESTORE_CONCURRENCY`                          | `2`                          | Kafka-only lazy assignment restore admission, 1–32.                                                                                                      |
| `OBSERVABILITY_KAFKA_PROJECTOR_CONCURRENCY`                        | `2`                          | Kafka-only source projection admission, 1–32; independent of the fixed single manifest delivery permit.                                                  |
| `OBSERVABILITY_EXPORTER_DATABASE_URL`                              | required only for `postgres` | Legacy exporter-owned Postgres database; a DSN without explicit backend rejects startup. It is not a Registry or Transcript database connection.         |
| `OBSERVABILITY_EXPORTER_DATABASE_POOL_MAX`                         | `5`                          | Legacy-only Postgres pool limit, 1–50.                                                                                                                   |
| `OBSERVABILITY_EXPORTER_DATABASE_TIMEOUT_MS`                       | `10000`                      | Legacy-only Postgres connection and query timeout, 1–120000 ms.                                                                                          |
| `REGISTRY_INTERNAL_BASE_URL`                                       | required                     | Root HTTP(S) origin for scoped Registry resolvers. No target endpoint belongs here.                                                                      |
| `INTERNAL_SERVICE_TOKEN` / `INTERNAL_SERVICE_TOKEN_FILE`           | exactly one required         | Registry workload credential; token content is 32–16384 non-whitespace characters and files are reread per resolver request.                             |
| `OBSERVABILITY_EXPORTER_WORKER_ID`                                 | process-unique               | Legacy-only fenced lease owner prefix, 1–246 single-line characters; runtime suffixes keep the final owner within 256.                                   |
| `OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS`                        | `10000`                      | Resolver timeout: 1–15000 ms in Kafka mode; 1–120000 ms in legacy mode.                                                                                  |
| `OBSERVABILITY_PROJECTOR_POLL_MS`                                  | `250`                        | Legacy-only idle projector poll interval, 1–60000 ms.                                                                                                    |
| `OBSERVABILITY_PROJECTOR_LEASE_MS`                                 | `30000`                      | Legacy-only projector lease, 30000–300000 ms. Runtime renews it during Kafka replay and again before Registry context resolution.                        |
| `OBSERVABILITY_PROJECTOR_BATCH_SIZE`                               | `100` Kafka / `1000` legacy  | Kafka transaction batch size, 1–1000 records; legacy replay batch size, 1–10000 events.                                                                  |
| `OBSERVABILITY_PROJECTOR_BATCH_BYTES`                              | `8388608` (8 MiB)            | Legacy-only cumulative Kafka broker-message replay budget, 1–67108864 bytes. One oversized first raw record is admitted and its scanned cursor retained. |
| `OBSERVABILITY_DELIVERY_POLL_MS`                                   | `250`                        | Legacy-only idle one-row delivery poll interval, 1–60000 ms.                                                                                             |
| `OBSERVABILITY_DELIVERY_LEASE_MS`                                  | `180000`                     | Legacy-only initial delivery lease, greater than Registry timeout plus 5000 ms; renewed immediately before OTLP send using target timeout plus margin.   |
| `KAFKA_BROKERS`                                                    | `localhost:9092`             | Comma-separated Kafka bootstrap brokers.                                                                                                                 |
| `KAFKA_CLIENT_ID`                                                  | `observability-exporter`     | KafkaJS client ID.                                                                                                                                       |
| `KAFKA_TOPIC_PREFIX`                                               | empty                        | Optional dot-terminated Kafka topic prefix, for example `public.default.`.                                                                               |
| `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS`                               | `1000`                       | Session-topic discovery interval, 1–60000 ms.                                                                                                            |
| `KAFKA_TOPIC_LISTING_MODE`                                         | `canonical`                  | Broker-native exporter only: `canonical` or explicit `bare-alias` for namespace-stripped listings, independent of authentication.                        |
| `KAFKA_CONNECTION_MODE`                                            | `plaintext`                  | Kafka protocol mode: `plaintext`, `sasl-plain-token-tls`, `sasl-plain-tls`, or `custom`. This does not enable a Pulsar Transcript backend.               |
| `KAFKA_AUTH_TOKEN`                                                 | required by token modes      | Kafka protocol credential; `sasl-plain-token-tls` uses `token:<value>`, while `sasl-plain-tls` uses the value directly.                                  |
| `KAFKA_SASL_USERNAME`                                              | `public` in token mode       | Required for `sasl-plain-tls` and custom SASL/PLAIN; optional for `sasl-plain-token-tls`.                                                                |
| `KAFKA_SASL_MECHANISM`                                             | unset                        | Custom mode supports only `plain`.                                                                                                                       |
| `KAFKA_SASL_PASSWORD`                                              | required with custom SASL    | Custom Kafka SASL/PLAIN password.                                                                                                                        |
| `KAFKA_SSL`                                                        | `false`                      | Enable TLS in custom Kafka mode.                                                                                                                         |
| `KAFKA_SSL_REJECT_UNAUTHORIZED`                                    | TLS default                  | Custom TLS server-certificate verification.                                                                                                              |
| `KAFKA_SSL_CA_FILE` / `KAFKA_SSL_CERT_FILE` / `KAFKA_SSL_KEY_FILE` | unset                        | Optional custom Kafka TLS PEM file paths.                                                                                                                |

### Kafka connection configuration migration

`KAFKA_CONNECTION_MODE=kop-token` is no longer accepted. Set
`KAFKA_CONNECTION_MODE=sasl-plain-token-tls` in registry, harness, and exporter to retain
SASL/PLAIN over TLS with `password=token:<KAFKA_AUTH_TOKEN>` and the default username `public`.
For KoP topic listings, also set `KAFKA_TOPIC_LISTING_MODE=bare-alias` explicitly in the
broker-native exporter. Changing only the authentication mode does not enable alias matching.
These settings change neither topic names nor stored state.

With the Helm chart, set `transcriptStore.kafka.connectionMode: sasl-plain-token-tls` and
configure the exporter separately:

```yaml
observabilityExporter:
  extraEnv:
    - name: KAFKA_TOPIC_LISTING_MODE
      value: bare-alias
```

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

The exporter is read-only: it never registers a writer schema, even when the shared
encoding is Avro. Both Kafka-state and legacy Postgres-state runtimes select only
the configured transcript topic set. Kafka-state topic and consumer-group namespaces
isolate old checkpoints and pending delivery. The SQL-state runtime's Postgres
progress is session-keyed: before cutover, use a fresh dedicated exporter-state
database or an operator-verified reset. There is no automatic SQL migration or
truncation, and this is not a reset of the central registry database. Reverting to
raw reselects the old set, excluding new Avro history; coordinate workers and stored
state because old pending work can resume.

## Tests

```bash
pnpm --filter @orca/observability-exporter test
pnpm --filter @orca/observability-exporter test:integration
```

The unit suite builds fresh ESM artifacts and imports both runtime entrypoints in a native Node
subprocess, without starting external connections. This checks CommonJS dependency interop outside
Vitest's module loader, including KafkaJS's default-export access to `ConfigResourceTypes`.

The broker-native suite `kafka-only.spec.ts` needs Kafka, not exporter Postgres. It runs in the
ordinary integration command and CI. Run it independently without any SQL service:

```bash
pnpm --filter @orca/observability-exporter test:integration:kafka
```

The suite creates its fixture topics explicitly, then waits for broker offset queries to succeed
before starting consumers. This readiness check retries transient metadata and leader errors for
up to 30 seconds; authorization and other setup errors fail immediately. The fixture retains
KafkaJS's default retry budget for coordinator initialization on a cold broker.

Legacy SQL vertical integration creates an isolated exporter-state Postgres database and
uses the local Kafka broker (`KAFKA_BROKERS`, default `localhost:9092`). It verifies Kafka inbox acknowledgment/replay,
Registry resolver authorization, and a local mock Langfuse-compatible OTLP collector.
`OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL` overrides the Postgres admin connection used by
the legacy SQL suites and the legacy durable Litefuse smoke.
The Postgres sampling suite uses an in-memory Transcript reader and contract-shaped Registry/OTLP
stubs with the real repository/runtime. It covers rates 0/1/fractions, exact hash boundaries,
restart/replay, config-version isolation, suppression/checkpoint rollback, stale-worker fencing,
and absence of sampled-out outbox and provider work. It runs without Kafka:

```bash
pnpm --filter @orca/observability-exporter exec vitest run --config vitest.integration.config.ts test/integration/durable/sampling-postgres.spec.ts
```

The mixed-observation unit suite exercises model/tool/evaluator state together across serialized
batch boundaries, legacy checkpoint adoption, sampling, strict outbox reconstruction, and combined
child limits. `test/integration/durable/mixed-observations-postgres.spec.ts` uses the real Postgres
repository and runtime to verify restart, cursor advancement, sampled-out suppression, and delivery
without Kafka.

## Optional Litefuse smoke tests

All smoke commands require `LITEFUSE_OTLP_ENDPOINT`, `LITEFUSE_PUBLIC_KEY`, and
`LITEFUSE_SECRET_KEY`. An unset or empty value fails the selected smoke immediately. They are
developer-run opt-in checks: default unit/integration commands and CI do not run them, and no repo
secret is configured for them. The endpoint must be a canonical HTTPS URL with exact path
`/api/public/otel/v1/traces`; no endpoint expansion occurs.

### Lightweight direct smoke

`test:smoke:litefuse` keeps the lightweight projector → OTLP client check. It needs no Kafka or
Postgres: the spec projects run-scoped synthetic metadata-only and raw-I/O turns in process, sends
them directly, then polls Litefuse's Observations API for root/summary/tool identity, parentage and
the expected presence or absence of I/O, error levels, timing and reported usage/cost. The raw-I/O
fixture includes an error-level tool and a successful turn to distinguish their execution outcomes.

```bash
LITEFUSE_OTLP_ENDPOINT='https://litefuse.example/api/public/otel/v1/traces' \
LITEFUSE_PUBLIC_KEY='pk-lf-...' \
LITEFUSE_SECRET_KEY='sk-lf-...' \
pnpm --filter @orca/observability-exporter test:smoke:litefuse
```

`LITEFUSE_SMOKE_RUN_ID` may pin the otherwise random direct-smoke namespace. IDs remain
deterministic for that namespace.

### Kafka raw-I/O smoke

`pnpm --filter @orca/observability-exporter test:smoke:litefuse-kafka` exercises the real Kafka
runtime and production hardened OTLP transport, then reads root/tool I/O back from Litefuse. It
uses the same three credentials above and `KAFKA_BROKERS` (default `localhost:9092`), with permission
to create/delete isolated topics and consumer groups. No Postgres is required. Registry authority
responses are contract-shaped test fixtures parsed by the real Registry client, not a live Registry
or a platform-policy change. The fixture contains only synthetic text and a fake secret canary.
Both raw-I/O smoke paths include accepted approval and pinned agent/configuration attribution.
Read-back verifies these facts on the corresponding observations, not just the presence of I/O.
It checks native observation version/environment and release (v2 observation trace context or
the v1 trace response), as well as their filterable metadata copies.
The test removes its Kafka resources and scratch directory; its synthetic Litefuse trace remains
in the test project for inspection. Missing credentials fail before infrastructure is created.

### Legacy Postgres durable vertical smoke

Additional prerequisites:

- local Kafka reachable through `KAFKA_BROKERS` (default `localhost:9092`), with topic creation and
  deletion enabled;
- local Postgres reachable through `OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL` (default
  `postgres://orca:orca@localhost:5432/postgres`), using a role allowed to create and drop databases.
- optional `LITEFUSE_SMOKE_RESOLVED_IP` when the host uses fake-IP DNS. Set it to one trusted public
  A/AAAA answer for the Litefuse hostname; hardened address admission, TLS hostname verification,
  direct sockets, and no-redirect/no-proxy behavior remain active. Production runtime never reads it.

```bash
LITEFUSE_OTLP_ENDPOINT='https://litefuse.example/api/public/otel/v1/traces' \
LITEFUSE_PUBLIC_KEY='pk-lf-...' \
LITEFUSE_SECRET_KEY='sk-lf-...' \
pnpm --filter @orca/observability-exporter test:smoke:litefuse-vertical
```

This command creates a random exporter database and Kafka Session topic, then exercises real Kafka
append → `KafkaSessionEventSource` durable inbox → Kafka replay/projector → Registry client's exact
context/secret response contracts → Postgres outbox → runtime's default public-only, DNS-pinned HTTPS
egress → Litefuse. It queries the v2 Observations API when available, falls back to the v1
Observations plus trace APIs for older compatible deployments, and verifies root/summary IDs and
parentage, metadata-only omission of input/output, plus workspace-scoped Session and user namespaces.
On a successful run, cleanup must delete the consumer group, observe topic deletion, and drop the
isolated database; a cleanup failure makes the smoke fail with bounded, non-secret error codes.
The durable path additionally enforces the runtime's public-address DNS admission.

`LITEFUSE_SMOKE_TIMEOUT_MS` controls the bounded remote delivery/query waits (default 60000,
maximum 300000). Credentials and Authorization headers are never logged.
All commands intentionally leave their synthetic traces in Litefuse because no compatible delete
contract is assumed. Run them against a disposable validation project or apply that project's normal
retention policy; the successful command logs the trace ID for cleanup and inspection.
