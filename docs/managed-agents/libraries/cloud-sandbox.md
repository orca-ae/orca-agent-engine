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
