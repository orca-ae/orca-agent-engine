# `@orca/codex-harness`

The shared Codex SDK runtime is an in-process library used by `harness-server`,
the cloud `sandbox-harness` provider and the self-hosted `session-runner` worker entry point. It owns the SDK's native thread,
its isolated configuration and history directory, and the private MCP transport
between the bundled Codex CLI and the calling adapter. It has no executable
entry point, deployable service, database client, or sandbox dependency.

## Execution boundary

`CodexSdkWorker` accepts a working directory, pinned model and reasoning effort,
developer instructions, explicit API credentials, and an allowlisted set of MCP
tools. Each worker creates a private `CODEX_HOME` and passes an allowlisted
environment to the CLI. Native shell, patch, image, web, multiagent, plugin, and
other execution surfaces are disabled. The MCP server table is replaced as a
whole so project configuration cannot add another executable tool path.

The SDK's MCP client reaches an ephemeral, bearer-authenticated loopback HTTP
relay. The relay exposes only the supplied tool definitions, emits `tool_call`
events, and waits for the adapter's `tool_result` command. The calling adapter
owns permissions, sandbox execution, transcript events, and remote MCP routing.
Closing the worker interrupts the turn, cancels outstanding tool requests,
closes the relay, and removes its private configuration and history directory.

`harness-server` supplies a separate private working directory for its SDK
process; session files remain accessible only through Orca's sandbox tools.
Cloud colocated execution uses the same worker through sandbox-harness private
HTTP/SSE commands; the provider creates a private working directory inside the
sandbox. Self-hosted session-runner launches the worker through its sandbox
process interface.

## Client callback content

Both Codex adapters call `customToolResultToMcp` from [`@orca/sdk-harness`](sdk-harness.md) (re-exported here) before returning public client
callback content to the native MCP consumer. Public Managed Agents blocks and
MCP blocks have different shapes. The converter preserves `is_error` as MCP
`isError` and applies these mappings:

| Public result                          | Native MCP content                                                                                     |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Text                                   | Text unchanged.                                                                                        |
| Document with a text source            | JSON text containing the entire document, including title, context, and source metadata.               |
| Document with a base64 `text/*` source | Strict UTF-8 decoding, then JSON text retaining metadata and identifying the original base64 encoding. |
| Search result                          | JSON text containing the entire block, including content and metadata.                                 |
| Base64 PNG, JPEG, WebP, or GIF image   | MCP image with `data` and `mimeType`; additional block metadata is retained in a following text block. |

URL and File-store image/document sources, binary documents such as PDF,
unsupported image MIME types, malformed base64, and invalid UTF-8 text documents
raise `CustomToolResultConversionError`. Both adapters interrupt the native turn
and publish a terminal error containing that reason; they do not submit an
invalid MCP result and then report a successful continuation. The converter
performs no network fetches, File-store lookups, or credential access.

## Turn lifecycle and recovery

The exported command/event types describe startup, submitted text, tool results,
interrupts, native SDK events, and completion. The worker checkpoints native
rollout files before emitting `done`. Checkpoints carry a thread ID and base64
history files; only matching native rollout paths are accepted, with limits of
32 files and 16 MiB decoded history. Restoration validates the complete payload
before replacing existing history. The calling adapter persists these private
checkpoints before acknowledging the turn.

Before forwarding terminal SDK usage or exporting its checkpoint, the worker validates
complete input, cached-input and output counters as nonnegative safe integers, with
cached input no greater than total input. Optional cache-write counters and the combined
token count must also be valid safe integers. Malformed usage emits a fatal failure,
exports no checkpoint, and leaves guarded accounting pending in the calling adapter.
Both adapters validate raw counters before converting total input to uncached input.

Checkpoint export failure emits a fatal failure and disables further turns and
credential refreshes in that worker. Both adapters reject subsequent turns so
execution cannot continue from native history that has not been made durable.

`refreshOptions` replaces the API key and optional endpoint between turns. It
resumes the same native thread and retains the private relay and history.
Developer instructions can be changed before the first native turn; after that
they remain pinned to the native thread. A changed instruction is rejected
because the SDK's resume operation retains the original instruction. Refreshing
during a running turn or after closing also fails, so a scoped gateway token
cannot change underneath an active request.

`transitionCodexCheckpointInstructions` supports a managed Skill policy change
before restoring a checkpoint. The caller verifies that both catalogs contain
only subsets of the same immutable Skill pins and retain the Agent's base
instructions. For the pinned CLI rollout layout, the helper replaces the managed
developer block, including one retained by compaction, and updates its digest.
It preserves the native thread, conversation and tool history, and leaves the
source checkpoint unchanged. Unverifiable or unsupported layouts are rejected.

The worker, checkpoint helpers, protocol types, native `ThreadEvent` type, and
`CodexFactory` test seam are explicit exports from `src/index.ts`. The model
catalog, its Apache license, and its attribution notice ship with this library.
The SDK version and both Orca model catalogs are pinned together.

## Validation

```bash
pnpm -F @orca/codex-harness test
pnpm -F @orca/session-runner test
```

Library tests cover lifecycle, refreshed credentials, pinned instructions, and
checkpoint validation. The session-runner suite also drives the installed SDK
and its bundled CLI against a local Responses fixture, including native thread
recovery, Skill blocking and unblocking across resume, mediated tool calls, and
the built worker executable. Both adapter suites
verify native callback continuation with text, documents, search results, and an
inline PNG, plus visible terminal failure for unsupported sources.
