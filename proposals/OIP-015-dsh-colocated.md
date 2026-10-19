# OIP-015: DeepSeek Harness as a colocated SDK runtime

- _Author(s)_: Rui Fu (@freeznet)
- _Status_: Proposed
- _Proposal time_: 2026-10-19
- _Components_: harness-catalog, sdk-harness, dsh-harness, sandbox-runtime, harness-server, sandbox-harness, registry-service-ts, observability-exporter, Helm charts
- _Discussion_: Design review on this proposal pull request
- _Released in_: None

## Problem and scope

Run DeepSeek Harness (DSH) through the existing managed Session API while preserving Orca tool
permissions, credential isolation, accounting and durable completion. The integration uses the
cloud colocated SDK path described in [OIP-002](OIP-002-agent-harnesses-and-execution-modes.md).
It does not introduce another scheduler, public API or sandbox lifecycle.

The initial scope pins DSH `0.2.1-alpha.2`, provider `deepseek` and model
`deepseek-v4-flash`. It supports multiple turns, managed file/shell tools, confirmations, native
in-process spawn/fork with maximum depth 2, thread projection, targeted interruption and native
conversation-tree checkpoints. Children share one Session, sandbox, model and fixed Agent grants.

Separate and self-hosted execution, other providers, tenant plugins, online plugin installation,
external child runtimes, detached work across turns, arbitrary child models and workspace/process
backup are outside this delivery. Unsupported capabilities fail admission rather than being ignored.

## Design

### Runtime and tools

The Registry resolves `metadata.harness=dsh_sdk` and `metadata.mode=colocated` through the
shared harness catalog. The existing SDK host in harness-server communicates with a DSH provider
inside sandbox-harness over the existing command and HTTP/SSE transport. A private headless
`orca-managed` profile loads the pinned runtime and Orca bridge.

Only Orca-managed tools are exposed. File, shell and MCP calls reuse the existing permission,
confirmation, output-write-policy and sandbox boundaries. Native child orchestration is gated
before dispatch; concurrent dispatches share cumulative per-turn bounds. Children cannot enlarge
their tool or model grants. Monotonic archive fences prevent late queued work from reactivating
archived threads. Per-call cancellation propagates to OpenSandbox without canceling sibling calls.

### Credentials and accounting

DSH sends a scoped bearer JWT and Orca Session identity to the Gateway native Messages route.
The Gateway validates route/model grants and injects its vault-held DeepSeek key upstream. No
provider credential enters the sandbox. The request uses `/v1/proxy/deepseek/v1/messages`; its
canonical authorization route is `llm-pi-deepseek-anthropic-messages`.

The harness remains the usage authority for DSH, including native children. Gateway native
destinations disable duplicate usage emission. Existing durable turn receipts and completion
fences retain their ordering; native completion alone is not a successful persisted turn.

### Threads, recovery and observations

Native run identity and lineage project onto existing Session thread IDs and events. The
integration does not add public event shapes or a separate persistent child scheduler.

The private checkpoint profile `orca-managed-tree-v2` contains bounded native conversation and
lineage state at a quiescent turn boundary. Interrupted or failed uncommitted trees retire and
subsequent explicit input rebuilds from the authoritative checkpoint. Retirement waits for bounded
in-flight interrupt acknowledgements before closing their transport. No automatic replay claims
to undo file writes or remote effects.

The exporter emits flat child-aware observations under the accepted-turn root, correlates
child-addressed client inputs before filtering and supports 128 open summaries (root plus 127
children). It preserves thread identity across replay and does not count forked history twice.

## Operator configuration and release artifacts

| Component | Configuration | Behavior |
|---|---|---|
| Harness | `SANDBOX_HARNESS_DSH_IMAGE` / `images.sandboxHarness.dsh` | Select the DSH image; Helm prefers digest, explicit tag, then appVersion |
| Harness | `HARNESS_TURN_TIMEOUT_MS` / `harness.turnTimeoutMs` | Whole SDK turn deadline, default 600000 ms |
| Harness | `HARNESS_TURN_TIMEOUT_OVERRIDES_MS` / `harness.turnTimeoutOverridesMs` | Optional per-harness positive integer milliseconds; matching override wins |
| Registry | `AI_GATEWAY_LLM_JWT_TTL_SECS` | SDK LLM token lifetime is at least 660 seconds; raise for longer turns |
| Registry | `sessionJwt.llmRoutes` / `sessionJwt.llmModels` | Grant the native Messages route and selected model |
| Gateway | `aiGateway.piProviders` | Configure `deepseek`, `anthropic-messages`, upstream `https://api.deepseek.com/anthropic` and a Gateway-only Secret reference |

Timeouts are bounded by the Node timer limit of 2147483647 ms. Invalid overrides fail closed.
Confirmation waits, tools and children consume the same deadline. Before a turn, its JWT must
have more than the deadline plus 30 seconds remaining. Refresh occurs between turns, not during
a turn. Increasing the harness deadline requires a longer Registry LLM token lifetime with
transport margin and compatible Gateway request deadlines; `SESSION_JWT_TTL_SECS` is unrelated.

Release automation builds `sandbox-harness-dsh` for amd64 and arm64, verifies the merged manifest
and pins its digest in the packaged engine chart. OpenSandbox trust configuration must admit the
selected image and `/usr/local/bin/orca-sandbox-harness` entrypoint. Independently deployed
Gateways require configuration through their own deployment owner.

## Compatibility and security

No database migration, public endpoint, public event shape or runner tunnel change is required.
Existing harness selections and default timeouts remain unchanged. Private SDK worker commands
gain DSH native-tree/control support, so deploy matching host, bridge and worker versions.

Upgrade Registry, harness, DSH image and child-aware exporter before admitting DSH Sessions.
Older root-only DSH checkpoints are rejected rather than silently migrated. Drain DSH Sessions
before rollback to an incompatible checkpoint reader. Exporters limited to 64 summaries cannot
restore larger open-summary checkpoints; drain incompatible pending work before downgrade.

Only operator-built, pinned images supply executable profile code. Session inputs cannot install
plugins or select arbitrary upstream endpoints. Existing sandbox isolation, resource scope,
confirmation and output-write boundaries apply to every native child. Recovery restores context,
not shell processes, workspace files or external business effects.

## Validation and remaining acceptance

Implementation validation covers catalog admission, policy gates, scoped usage authority,
checkpoint validation, real DSH runtime execution against deterministic local model fixtures,
interrupt acknowledgement races, thread lineage, exporter replay and bounded observations.
Chart tests cover image precedence, invalid digests, native Messages bindings and timeout values.
The release workflow verifies multi-architecture manifests and packaged chart image references.

Before enabling a deployment, exercise live DeepSeek with Registry-issued credentials, multiple
turns, File API outputs, spend limits, targeted child interruption, cold sandbox replacement and
long-turn credential/deadline behavior. Fixture or chart success does not establish these results.

Nested cold follow-up requires a live direct parent activation in the pinned DSH API. Physical
termination of already-dispatched shell commands and the complete public HTTP thread/control
round trip across cold restore remain deployment acceptance work. Primary-thread archive retires
the tree instead of claiming a native root archive acknowledgement. Implementation documentation
records deferred work and its triggers in the roadmap.

## Alternatives

- A generic CLI stdout adapter cannot preserve managed tool, usage and native recovery semantics.
- Native DSH tools would create a second permission/resource path, so the profile uses Orca tools.
- A new scheduler or durable child-ID registry duplicates DSH lineage and existing thread projection.
- A workspace or plugin-state backup service exceeds conversation recovery requirements.
- Waiting for a unified colocated execution path unnecessarily couples this runtime to a separate migration.

## Acceptance

Status remains Proposed until maintainer review under the repository OIP process. The implementation
PR depends on acceptance; this proposal does not claim release or deployment validation.
