# sandbox-harness (`@orca/sandbox-harness`)

> The HTTP/SSE server baked into per-harness sandbox images. It runs the agent
> harness process **inside** the sandbox for cloud `claude_code`, `codex_sdk`,
> and `pi_sdk` `colocated` agents, and `harness-server` drives it over the wire.
> Source: `services/sandbox-harness/`.
> Topology and bridge semantics: [`../harness-modes.md`](../harness-modes.md).

## Not a cluster service

This is an **image payload**. It lives under `services/` because it builds and
ships like a service — its own `package.json`, `tsup` build, and Dockerfile —
but nothing deploys it standalone, it has no cluster identity, and it holds no
credential of its own. Its lifetime is one sandbox.

One shared image contains the Claude, Codex SDK and Pi SDK providers:

```bash
docker build -f services/sandbox-harness/Dockerfile \
  --build-arg DEFAULT_AGENT=claude-code \
  -t ghcr.io/orca-ae/sandbox-harness-claude-code:latest .
```

The image tags and the port the server listens on are declared in
`@orca/harness-catalog`, not here — see
[`../libraries/harness-catalog.md`](../libraries/harness-catalog.md).

## The wire

`src/protocol.ts` implements the Claude Agent SDK **stream-json control
protocol** — the same NDJSON language the official `claude` CLI speaks under
`--input-format stream-json --output-format stream-json`. The server reads NDJSON
from stdin, demultiplexes on `type`, correlates `control_request` →
`control_response`, and streams a turn's frames to stdout as they arrive rather
than buffering the turn.

**Frame shapes are a two-sided contract.** `harness-server`'s
`src/harness/in-sandbox/event-mapper.ts` parses exactly what this module emits.
Field sets and JSON key spellings are load-bearing: renaming or reordering one
without changing the mapper in the same commit breaks every session on this
bridge — cloud `claude_code`, `codex_sdk` and `pi_sdk` `colocated` — and no type
system spans the gap. Other `colocated` sessions run through `session-runner`
and do not use this wire.

```
harness-server                          sandbox
  InSandboxHarness                        @orca/sandbox-harness
    DialInTransport  ──HTTP/SSE──►          server.ts
    events()         ◄──frames───           session-manager.ts
    mapSandboxEvent                           └─ provider runtime (claude / codex-sdk / pi-sdk)
      │
      ▼
    SessionRunner.pumpEvents ──► transcript-store
```

Each frame carries an `id`. When that mapped `AgentEvent.id` is canonical,
`pumpEvents` preserves it as Transcript `Event.id` and also retains the raw
non-empty value as `idempotencyKey` metadata. Transcript-store deduplicates
`Event.id` across the session independently of subpath; it does not deduplicate
by `idempotencyKey`. Current raw frames carry no subpath, so the runner uses the
primary path. DialInTransport's seen-ID set separately suppresses full-buffer
SSE reconnect frames before they reach the mapper.

The Claude provider exposes client callback requests as `agent.custom_tool_use`
and accepts `user.custom_tool_result`. Internal SDK MCP echoes for these callbacks
are omitted from ordinary `agent.tool_use` and `agent.tool_result` events; unrelated
tool events and provider usage remain visible.

Codex uses `POST /v1/sessions/:id/sdk-command` on the same HTTP server and
`harness.sdk_event` envelopes on the same SSE stream. Commands are not stored
in event history. The subprocess control response acknowledges completion and
carries the latest event sequence so HTTP completion cannot outrun SSE delivery.
The next command acknowledges received SDK event sequences; the ephemeral store
prunes those private frames so successive native checkpoints do not accumulate.
Unread frames remain available for SSE reconnects. These frames stay internal;
`CodexSdkHarness` produces public transcript events.

## Providers

`src/providers/registry.ts` holds a **static array**, deliberately: scanning a
providers directory at startup does not survive bundling to a single-file `dist`
and defeats static analysis of dynamic imports. Adding a provider is an `import`
plus a `push`.

| Provider    | Status                                                                                                                  |
| ----------- | ----------------------------------------------------------------------------------------------------------------------- |
| `claude`    | Ships. Runs `@anthropic-ai/claude-agent-sdk`'s `query()` in-process.                                                    |
| `pi-sdk`    | Ships. Runs the embedded `@orca/pi-harness` through the same private SDK command and checkpoint transport as Codex.     |
| `codex-sdk` | Ships. Runs `@orca/codex-harness` with private SDK commands; the host adapter handles policy, usage and native history. |

The `list_harnesses` control request returns the public catalog built from this
array, so a client can discover what an image actually serves rather than
inferring it from the tag.

## Model access

The Claude provider routes through ai-gateway when **both** `LITELLM_API_BASE` and
`LITELLM_API_KEY` are set; `harness-server` sets both when it starts a
`colocated` sandbox and Registry mints the session's gateway JWT
(`src/harness/in-sandbox/llm-env.ts`). A failed mint is logged and the sandbox
starts without them. The value of `LITELLM_API_KEY` is
a per-session JWT minted by the registry, not a provider key — **no provider
credential is baked into the image or placed in its environment.**

Warm turns capture the SDK-generated `session_id` from the first stream and pass
it back as `resume`, keeping native transcript and tool context in sandbox-local
SDK configuration. That state is runtime-local and never enters Orca's durable
store; after a cold rebuild the harness replays prior turns from the transcript
as a preamble instead.

For streamed Claude messages, usage accounting combines `message_start` with
the cumulative `message_delta` counters and emits one internal `agent.usage`
at `message_stop`. Initial assistant content blocks can carry an output count
of zero; those snapshots do not replace the final streamed count or charge the
same message again. Null or omitted delta fields preserve the initial input
and cache counters, including the 5m/1h cache-creation split. Message identity,
model, and parent tool-call identity stay attached to that accounting.
Assistant-only messages retain their usage fallback. The terminal result
supplies aggregate usage only when no per-message usage has been emitted;
an interrupted stream emits its observed counters before the error event.

## Hardening

The image is deliberately smaller than the `separate`-mode sandbox template:

- **FUSE is present and used.** The image installs `fuse3` and `s3fs`, verifies
  both at build time, enables `user_allow_other` in `/etc/fuse.conf`, and ships
  the root-only mount helper `/usr/local/bin/orca-s3fs-mount`. This is
  load-bearing, not vestigial: `OpenSandboxRuntime` always reports
  `supportsFuse: true` and its acquire-time probe fails closed unless `s3fs`,
  `fusermount3`, gVisor-provided `/dev/fuse`, and mount permission are present.
- **Read-only root.** The server starts inside a bubblewrap namespace with
  `no_new_privs` set and an empty capability bounding set.
- **gVisor-compatible nested sandbox.** Claude's nested filesystem/PID sandbox
  remains mandatory. All Bubblewrap calls pass through the image-baked
  `orca-gvisor-bwrap` shim; it is a transparent exec on ordinary kernels and
  creates user/network namespaces first on gVisor so SDK proxy allowlists and
  PID/filesystem isolation remain active. The image builds a checksum-pinned
  Bubblewrap 0.12 release with its safe `openat(2)` path-resolution fallback
  retained and installs it as a versioned Debian package so SBOM and
  vulnerability scanners continue to inventory it. On gVisor the shim selects
  that fallback because gVisor does not implement the `openat2` syscall used by
  Bubblewrap's native fast path.
- **Writable paths come from data, not convention.** The entrypoint re-binds the
  whole filesystem read-only and then re-binds read-write only what the
  validated `ORCA_SANDBOX_WRITE_POLICY` payload lists (`src/write-policy.ts`).
  That seeds exactly one path — `/mnt/session/outputs` — plus the mount path of
  each `read_write` memory-store or repository resource. So `/mnt/session` is
  read-only apart from `outputs/`, and `/mnt/memory` and `/workspace` are
  read-only unless such a resource mounts beneath them; `/workspace/skills` is
  read-only always. See [`../output-write-policy.md`](../output-write-policy.md).
- **Only scoped session JWTs are injected.** Repository PATs and provider
  credentials never enter the image.

The entrypoint waits for the dispatcher to finish mounting and probing session
resources before starting the HTTP server, so an agent cannot observe a
half-materialized workspace.

## Runtime dependencies

`git`, `curl`, `jq`, `bubblewrap`, and the `orca-git-creds` credential helper —
the last of these is what makes in-sandbox `git push` work without the PAT ever
touching the sandbox. See [`../mount-strategies.md`](../mount-strategies.md).

## Tests

```bash
pnpm -F @orca/sandbox-harness test
```

End-to-end coverage against a real sandbox is Layer B.1 in
[`packages/e2e-tests/README.md`](../../../packages/e2e-tests/README.md), opt-in
via `ORCA_E2E_SANDBOX_HARNESS=1`. It needs a stack on an endpoint-capable
runtime (`SANDBOX_RUNTIME=opensandbox` or `e2b`); `local` and `in-memory` do not
expose the harness HTTP endpoint.

Codex receives its scoped Gateway JWT and Responses URL through the private command channel; credentials refresh between turns.
