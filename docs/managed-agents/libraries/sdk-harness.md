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

Private submit commands accept optional `executionBudgetMs`, the host's remaining
turn budget in integer milliseconds from 1 through 2147483647. Omission preserves
the legacy 600000ms execution budget. DSH forwards it to native submit and child
follow-up; the shared HTTP and sandbox ACK paths add bounded drain headroom.
Control waits and the post-ACK event-delivery fence are independent of this field.
Host budgets and watchdogs use monotonic elapsed time, independent of wall clock
corrections. Positive remaining budgets round up to integer milliseconds without
exceeding the configured limit. Watchdogs split durations above Node's single-timer
limit rather than overflowing.

The DSH host attaches this field; other harnesses retain their existing submit
timeouts. Deploy matching Harness and DSH sandbox images: older sandbox schemas
silently discard the field and keep their legacy watchdogs. The independent
post-ACK event-delivery fence remains 660000ms.

`assertSdkTerminalUsage` validates complete raw terminal counters before adapters
convert total input into uncached input. `customToolResultToMcp` maps public
callback blocks to the common tool-result shape. Its mappings and failure
contract are documented in [callback content](codex-harness.md#client-callback-content).
Codex re-exports the protocol and converter for existing consumers.
