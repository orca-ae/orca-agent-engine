# sandbox-harness — agent guidelines

`@orca/sandbox-harness` is an **image payload, not a cluster service**. It is baked into the
per-harness sandbox images and runs _inside_ the sandbox for `colocated` agents. It lives under
`services/` because it builds and ships like one, but nothing deploys it standalone.

See [`README.md`](README.md) for the image build commands,
[`services/sandbox-harness.md`](../../docs/managed-agents/services/sandbox-harness.md) for the
design, and [`harness-modes.md`](../../docs/managed-agents/harness-modes.md) for the bridge
semantics. Those describe current behavior only; anything not built belongs in
[`roadmap.md`](../../docs/managed-agents/roadmap.md).

## The wire is load-bearing

`src/protocol.ts` implements the Claude Agent SDK stream-json control protocol — the exact NDJSON
language the official `claude` CLI speaks under `--input-format stream-json --output-format
stream-json`. Frame shapes there are the contract with the session-manager's `translateFrame` and
must match byte-for-byte.

**Do not reorder or rename frame fields** without updating the consumer in the same change.
`harness-server`'s `src/harness/in-sandbox/event-mapper.ts` is the other end of that contract.

## Providers

`src/providers/registry.ts` holds a **static array**, deliberately — not filesystem
auto-discovery, which is hostile to the bundled single-file `dist`. Adding a provider is an import
plus a push onto `PROVIDERS`.

The registry contains `claude`, `codex-sdk` and `pi-sdk`. They share the image and HTTP/SSE
server. Codex private command/event frames are consumed by the host SDK adapter;
its credentials and checkpoints must never become public transcript events.
The distinct native CLI provider `codex` remains a session-runner provider.

## LLM egress

The provider routes through the gateway when **both** `LITELLM_API_BASE` and `LITELLM_API_KEY` are
set (`src/providers/claude.ts`); harness-server always sets both for `colocated` sessions. No
provider API key is ever baked into the image or placed in its environment.

## Sandbox hardening — do not weaken

The server starts inside a read-only-root bubblewrap namespace with `no_new_privs` set and an empty
capability bounding set. Only scoped session JWTs are injected; repository PATs and provider
credentials never enter the image.

FUSE **is** present and load-bearing — the Dockerfile installs and build-verifies `fuse3` and `s3fs`,
enables `user_allow_other`, and ships root-only `/usr/local/bin/orca-s3fs-mount`, because
`OpenSandboxRuntime` defaults `supportsFuse: true` and its acquire probe fails closed without them.
Removing any of it breaks `colocated` mounting; it is not dead weight to trim.

Writable paths come from data, not convention: the entrypoint re-binds everything read-only and then
re-opens only what the validated `ORCA_SANDBOX_WRITE_POLICY` payload lists (`src/write-policy.ts`).

[`docs/managed-agents/services/sandbox-harness.md`](../../docs/managed-agents/services/sandbox-harness.md#hardening)
is the authority on all of this — correct it there first, and keep this section a pointer rather than
a second copy that can drift out of step.

## Tests

```bash
pnpm -F @orca/sandbox-harness test
```
