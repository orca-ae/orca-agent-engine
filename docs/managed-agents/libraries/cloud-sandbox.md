# `@orca/cloud-sandbox`

`@orca/cloud-sandbox` packages the two cloud `SandboxRuntime` implementations —
E2B and OpenSandbox — against the interface exported by
[`@orca/sandbox-runtime`](./sandbox-runtime.md). It is a TypeScript library
imported in-process; it depends one-way on `@orca/sandbox-runtime` and on
nothing in `services/`.

## Contract

- `E2BSandboxRuntime` — wraps the `@e2b/code-interpreter` SDK behind a minimal
  structural type. Advertises `supportsFuse: true` (the `orca-default` template
  ships libfuse) and implements `runPrivileged` via a sudoers-scoped
  `--preserve-env` allow-list (only the `ORCA_S3_*` transport names ever cross
  the sudo boundary). `pause`/`resume`/`endpoint`/`destroy` throw loudly when
  the SDK build lacks the method — a silent no-op would report a suspended (or
  destroyed) sandbox that keeps executing and billing.
- `OpenSandboxRuntime` — drives an OpenSandbox deployment over its HTTP API +
  execd command stream. Acquire is fail-closed: when the runtime enables FUSE
  (`enableFuse`, default on) and the acquire requests it (`requiresFuse`), FUSE
  prerequisite probes run before any session state enters the sandbox, and a
  failed probe destroys the sandbox. A severed command stream is marked
  (`[orca: command stream truncated before completion]`) so an infrastructure
  fault is never mistaken for the command failing with exit 1.
  Every acquisition uses a fresh management API credential and verifies that the
  local execd listener returns 401 without it. Model tool processes do not inherit
  this credential; an execd build without authentication support is rejected.
- OpenSandbox advertises `supportsEnvironmentPackages`. It sends `apt`, `cargo`,
  `gem`, `go`, `npm` and `pip` declarations through the versioned
  `orca.environment.packages.v1` provider extension. Acquire checks the read-only
  artifact mount and declaration revision before staging the workload filesystem;
  an older provider that ignores the extension fails closed and is destroyed.
  Package preparation requires unrestricted Environment networking. Its readiness
  budget is 15 minutes, added to the requested sandbox lifetime. The provider
  bounds the artifact volume to 8 GiB and supplies a 16 GiB ephemeral-storage limit
  when the operator has not configured one. Package failures prevent worker start.
- E2B rejects nonempty package declarations before calling the provider.
- Both runtimes take the branded `ValidatedSandboxWritePolicy` on their
  enforcement methods and route tool calls through the shared `parseToolCall`.
  `destroy()` latches only after the remote kill/delete succeeds, so a retry
  after a network blip actually retries.

## Layout

```
packages/cloud-sandbox/
  src/
    e2b/runtime.ts           # E2BSandboxRuntime + privileged-command builder
    opensandbox/runtime.ts   # OpenSandboxRuntime + execd stream consumer
    index.ts                 # public re-exports
  test/unit/
```

Registry imports this package for its cloud Environment launchers.
`services/harness-server` does not import it: its own E2B and OpenSandbox
runtimes live under `src/sandbox/e2b/` and `src/sandbox/opensandbox/`.
