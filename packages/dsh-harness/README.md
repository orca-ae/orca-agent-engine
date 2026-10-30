# @orca/dsh-harness

Fixed DeepSeek Harness `0.2.1-alpha.2` worker for Orca cloud colocated Sessions.
Implements `SdkWorker`; the shared host adapter owns tools, approvals, usage and turn receipts.

`DshSdkWorker` starts a private named profile with the published DSH CLI. The profile loads
`@orca/dsh-harness/plugin`, not the default DSH application or native executable tools.
Gateway Bearer credentials and the Orca Session header are refreshed through the worker API.
The optional second constructor argument supplies a controlled `launch` callback and
private writable `homeRoot`. The callback receives the executable, arguments, cwd and
restricted environment, and returns Node stdio streams, an exit promise and a kill function.
Launch failures propagate without a host-process fallback. Omitting the argument keeps
the existing cloud Node process launch. `inspectDshInstallation` checks packaged runtime
dependency pins and returns read-only installation paths for sandbox setup; this is
dependency validation, not confinement or self-hosted execution qualification.

Agent model.provider/id select from all chat models bundled with the official
DSH llm-pi-ai plugin 0.2.1-alpha.2 and its pinned pi-ai 1.1.0 dependency. The
legacy deepseek/deepseek-v4-flash adapter and route remain unchanged. Other selections
use the official PiAiAdapter with managed transport. The catalog includes model
identity and effort support; it does not grant access or prove protocol integration.
Managed transport and raw usage support anthropic-messages, openai-completions,
openai-responses and google-generative-ai. Other catalog protocols fail before inference.
Provider credentials stay in Gateway; no native login or ambient auth is enabled.

Host usage input includes cache reads but excludes cache writes, which are reported in their
own bucket. OpenAI-style totals are reduced by cache writes before host conversion;
Anthropic input and cache reads are combined without adding its separate cache-write count.
Native session headers correlate usage locally and are removed before requests reach Gateway.

Each submit requires the accepted root `sourceEventId` (`evt_…`); optional `threadId` addresses
a native child UUID. The reviewed profile provides native in-process `spawn`, `fork`,
`send_message` and `interrupt_agent`. Fresh child scopes explicitly receive the same pinned provider/model
and Orca tool wrappers. Model/cwd/grant overrides and external backends are rejected. Native
`tools/pre-execute` sends `orca.authorize` for each orchestration operation before native side
effects. The worker forwards `orchestration_request` and parks until the host returns
`orchestration_result`; the host owns policy and confirmations. Cancellation denies pending
authorization, and late replies cannot execute a cancelled call. Arguments and live targets are
revalidated after approval. The request uses the native projection's exact tool-use origin,
without another canonical tool event.
Native tree-wait includes child execution and result delivery before one aggregate `turn.completed`.
Canonical messages, orchestration, thread facts and model summaries retain native thread
attribution; managed tool calls carry canonical origin identities for the host producer.
Child catalog labels map to `agent_name`; structured native turn endings retain their
`stop_reason` and `native_reason` without permanently terminating continuable conversations.
Runtime settlement notices become parent-side `agent.thread_message_received` events with
`message_origin: runtime_settlement`, child/parent addresses and native notice identity. They
contain a bounded generic notice, not a second copy of child output or an inferred run/end link.

Native `surfaceOp: replace` events update native history only; they do not generate new tool
results or consume live call correlations. A matched successful compaction start/summary/end
emits the existing `agent.thread_context_compacted` with structural attribution only. Failed or
unfinished compactions emit no success fact. This does not enable a compaction plugin or a
second usage source.
Canonical payloads include the accepted `turn_event_id`; thinking blocks use public
`{ type: 'thinking', thinking: string }` content. Native delegation has maximum depth 2.
Cold startup publishes private `thread_tree` lineage in native catalog pre-order before `ready`,
without replaying public child creation. Targeted interruption stops only the selected native
subtree and acknowledges its teardown without waiting for siblings. Other branches finish
naturally. Targeted interrupt RPC waits up to 15 seconds and does not reap siblings on timeout.
Native acknowledgement is not proof of physical shell termination; the host owns managed-tool
cleanup and its result. Unavailable terminal usage prevents a tree checkpoint. Nested child follow-up requires
a live direct parent: the published native continuation API rejects a dormant parent rather than
materializing an unowned ancestor or delivering an invented prompt.
The trusted host supplies `StartCommand.archivedThreadSubpaths` from its current execution
snapshot, independently of checkpoint history. Thread-targeted `interrupt.archived: true` fences the target and
its descendants before native drain, including when idle. Model request preflight, native and
managed tool execution, and explicit follow-up reject archived sessions or ancestors, so queued
native messages cannot reactivate an archived thread. Checkpoints carry no archive authority.
Private `archive_fence { threadId }` validates a known owned native UUID, including cold lineage,
and acknowledges only a monotonic target/subtree fence. It bypasses the active submit and does
not drain native work, wait for tree idle, or claim termination of already dispatched model/tool
requests. The host updates its deny set, awaits this fence ACK, cleans up matching managed tools,
then sends targeted `interrupt { archived: true }`. A cleanup failure does not remove the fence.
Whole-turn failure or interruption emits a fatal failure after safe tree cleanup and retires the
advanced worker. It cannot accept another input or export partial failed history. The host settles
the failed turn and rebuilds from the authoritative committed checkpoint for the next explicit
input, without replaying the failed input. Restore includes a runtime notice that uncommitted
side effects may exist: history rollback does not undo files or external services. A child failure
handled by a successful parent can still commit when the tree is quiescent and all observed model
requests have validated terminal usage.

Native tree checkpoints preserve reachable conversation history up to 16 MiB decoded and
129 files including the compatibility manifest. The codec validates native catalogs, own fork
suffixes and lineage; unrelated sessions and missing descendants are rejected. The legacy DeepSeek profile is orca-managed-tree-v2; official-adapter checkpoints use
a profile tied to the pinned pi-ai version. Both bind provider/model and reject cross-model
restore. Legacy DeepSeek manifests without provider restore only as the original fixed model.
Earlier root-only checkpoints are rejected without migration. They do not
back up the workspace or background processes. `assertDshCheckpointInstructions` validates the
runtime/profile identity and fixed Agent instructions before restore.

Build before running the small named-profile fixture, which uses a local HTTP model endpoint:

```sh
pnpm --filter @orca/dsh-harness... build
pnpm -F @orca/dsh-harness test
node packages/dsh-harness/scripts/generate-catalog.mjs --check
```

The production image retains `dist/`, `profile/` and runtime dependencies. No package installation
occurs at Session startup. [Deployment configuration](../../docs/managed-agents/harness-modes.md#dsh-sdk-colocated-execution).
