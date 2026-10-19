# OpenSandbox per-call cancellation

`run(call, options?)` and `runWithWritePolicy(call, policy, options?)` accept
`options.signal`. The write-policy wrapper forwards the same options without changing
filesystem, FUSE or credential isolation. The runtime advertises
`capabilities.supportsPerCallCancellation=true`; other backends reject signal-bearing calls.

Both this adapter and `@orca/cloud-sandbox` use the shared execd kill-and-wait helper in
`@orca/sandbox-runtime`. It retains pending command starts until `init.text` supplies the
execution ID, interrupts that ID and polls its status for confirmed exit. Cleanup has a
10-second deadline; failures report unknown termination rather than successful cancellation.

See the [shared contract](../../../../../packages/sandbox-runtime/README.md#interface).
File APIs have pre-dispatch fencing only, not in-flight cancellation or rollback. Confirmation
depends on execd's process-group termination implementation and command status; descendants
escaping the process group are outside this guarantee.
