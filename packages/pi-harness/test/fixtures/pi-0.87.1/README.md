# Pi 0.87.1 checkpoints

These checkpoints were captured using `PiSdkWorker` with the installed Pi 0.87.1
packages and `test/support/provider-fixture.ts`. Each completed turn contains a user message,
a managed `write` call, the `Legacy write approved` tool result and an assistant reply.
The workspace is `/workspace`; all model responses and tool results are local fixtures.
No provider credentials or transport URLs are stored in the history.

The fixtures retain the actual checkpoint envelope, SDK version, session identity,
instruction digest and native entries. Tests load the files without rewriting their
SDK version, then continue the conversation and capture a new checkpoint. This
checks cross-version history compatibility independently of the current writer.

The [Pi 1.0.4 Anthropic fixture](../pi-1.0.4/anthropic.json) was captured with
`PiSdkWorker` using the Pi 1.0.4 SDK and the same local provider fixture and managed
tool round trip.
