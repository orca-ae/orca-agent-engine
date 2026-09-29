# `@orca/sdk-harness`

This library defines the private worker protocol shared by Codex SDK and Pi SDK.
It contains no model client, native process, listener or persistence dependency.

`SdkWorker` accepts startup, submit, interrupt, stop and tool-result commands.
Workers emit readiness, normalized model events, managed tool calls, private
checkpoints, failures and turn completion. `refreshOptions` updates explicit
credentials and endpoints between turns. The same contract serves an in-process
worker, sandbox-harness HTTP/SSE transport and session-runner's stdio child.

`SdkCheckpoint` retains the original Codex wire fields. Pi adds an explicit
format and SDK version, and each native library validates its own history.
The harness catalog checks format compatibility at Registry boundaries.

`assertSdkTerminalUsage` validates complete raw terminal counters before adapters
convert total input into uncached input. `customToolResultToMcp` maps public
callback blocks to the common tool-result shape. Its mappings and failure
contract are documented in [callback content](codex-harness.md#client-callback-content).
Codex re-exports the protocol and converter for existing consumers.
