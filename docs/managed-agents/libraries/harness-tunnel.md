# `@orca/harness-tunnel`

`@orca/harness-tunnel` implements the self-hosted transport channel:
framed-JSON WebSocket protocols connecting self-hosted compute _outbound_ to the
control plane, so runners behind NAT need no inbound reachability. It is a
TypeScript library with zero runtime dependencies; `registry-service-ts` mounts
its engines in `src/api/runner-tunnel.routes.ts` and
`src/api/worker-tunnel.routes.ts`.

## The two tunnels

- **Runner tunnel** (`frames.ts`, `transport.ts`) — one WebSocket per runner
  carrying HTTP-over-WS request/response proxying (streamed bodies, request
  cancel) plus tunneled WS channels (e.g. a browser terminal attach). Eleven
  frame kinds, strict-major `frame_protocol_version`, membership-validated
  fields (an unknown `encoding` or status fails loudly at the frame boundary).
  Response buffering is byte-bounded per request (`pushResponseBody`; overflow
  aborts that one request and cancels it on the runner instead of growing the
  registry heap), callers can abort an in-flight request via
  `TunnelRequest.signal` (sends `request.cancel`, fails the head wait), and an
  early consumer exit from the body stream cancels the request on the runner
  before the correlation slot closes.
- **Worker tunnel** (`worker-frames.ts`, `worker-tunnel.ts`) — one WebSocket
  per self-hosted _worker_ daemon: `worker.hello` registration (runner
  reconciliation + per-harness readiness), launch/stop runner control RPC, a
  one-way runner-exited report (runner ids are worker-supplied, so every
  delivery also carries the tunnel's authenticated `WorkerRunnerExitContext`:
  the worker id and owner the handshake resolved, which the engine passes to the
  sink and hook so they can check the runner's assignment before recording the
  cause or failing sessions), and the stat / list-dir /
  worktree / create-dir filesystem ops. Of the request frames, the registry
  sends only `worker.launch_runner`. The engine owns a
  guarded teardown: newest-wins reconnects cannot be clobbered by a zombie
  connection, in-flight waiters are rejected (`WorkerTunnelClosedError`)
  instead of hanging, and a store blip never tears down a healthy tunnel.

**Auth + registration** (`identity.ts`): a runner presents its
registry-minted binding token on `X-Orca-Runner-Tunnel-Token`, and its runner
id is derived from that token, so the registry derives the same id. The
worker-tunnel engine resolves a worker's owner from a launch token on
`X-Orca-Host-Token` (scoped to exactly one worker id) or from an injected auth
provider, and refuses the handshake when the launch-token lookup fails. The
registry authenticates a worker before the engine runs — an Env Key on
`X-Orca-Environment-Key` or an Environment Token on
`X-Orca-Environment-Token`, both defined in `registry-service-ts` — and
accepts no engine launch token. `ORCA_RUNNER_WORKSPACE` carries the runner's
workspace root.

## Route + conformance posture

The engines are transport-agnostic; the registry mounts both in the public
`/v1/tunnels/*` namespace — `/v1/tunnels/runners/:runnerId` for the runner
tunnel and `/v1/tunnels/environments/:environmentId` for the worker tunnel.
Neither WebSocket route is in the published OpenAPI document.
`/v1/runner/terminal/attach` is NOT a registry route: it is the wire path of a
WS channel the registry opens _toward the runner over the runner tunnel_, and
the runner serves the pty bridge there (a sandbox/internal surface). Anthropic's beta API reserves `/v1/tunnels` +
`/v1/tunnels/{tunnel_id}` for its Tunnels resource
([`../conformance-matrix.md`](../conformance-matrix.md), currently
`not-implemented`); the tunnel routes claim only the `runners` and
`environments` sub-segments.

**Terminal attach scope** (an explicit carve-out from the "sandboxes are
sealed" principle in [`../overview.md`](../overview.md)): a terminal attach
targets a **customer-owned self-hosted runner**, is initiated only by that
runner's owner through the registry (owner-scoped auth on the attach route),
and rides the runner's own outbound tunnel. Managed cloud sandboxes accept no
attach.

## Layout

```
packages/harness-tunnel/
  src/
    frames.ts          # runner-tunnel frame schema + encode/decode
    transport.ts       # runner-tunnel registry/connection engine
    worker-frames.ts   # worker-tunnel frame schema + encode/decode
    worker-tunnel.ts   # worker-tunnel server engine (auth, loops, teardown)
    identity.ts        # env/derivation contract for worker + runner identity
    index.ts           # public re-exports
  test/unit/
```
