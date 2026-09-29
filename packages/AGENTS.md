# packages/ — agent guidelines

Everything under `packages/` is a **library, never a service** — with the two registered exceptions
at the foot of the table, and no unregistered ones. There is no `transcript-store`, `file-store`,
`memory-store`, or `skill-store` deployable. `registry-service-ts` and `harness-server` import these
via `workspace:*` and call Kafka / Postgres / S3 **in-process**.

If you find yourself adding an HTTP listener, a `main.ts`, or a Dockerfile to a package here, that is
the signal you are in the wrong directory. `codex-harness` has one transport-specific exception:
its SDK worker embeds a token-authenticated loopback MCP relay for the child CLI.
`pi-harness` has the equivalent private transport exception for Pi 0.87's Google adapter,
which rejects custom fetch: its token-authenticated loopback bridge forwards model
traffic through managed transport and validates raw usage. The relay is
private to one worker, has no service endpoint, and closes with the worker; the library has no
executable entry point or deployable listener.

| Package                 | Contract                                                                              |
| ----------------------- | ------------------------------------------------------------------------------------- |
| `agent-event-contract/` | Pure canonical agent-event IDs, subpaths, kinds, and payload builders                 |
| `transcript-store/`     | Append/read/tail of opaque-payload events; Kafka (default), Postgres, Pulsar backends |
| `file-store/`           | Content-addressed blobs (object store) + Postgres metadata, SHA-256 dedup             |
| `memory-store/`         | Path-addressed memories: `live/` prefix + immutable `versions/{sha256}`               |
| `skill-store/`          | Immutable, digest-addressed Skill bundles in object storage                           |
| `harness-catalog/`      | Pure data: harness types, supported modes, default images, ports                      |
| `sdk-harness/`          | Shared native-worker protocol, callback conversion and terminal usage validation      |
| `pi-harness/`           | Embedded Pi SDK worker with managed tools and private native checkpoints              |
| `codex-harness/`        | Shared Codex SDK worker, private MCP tool relay, and native history checkpoints       |
| `e2e-tests/`            | Layer A (wire-protocol) + Layer B (real-agent-loop) suites, **not a library**         |
| `oeadm/`                | The `oeadm` binary — a client on the registry HTTP API. **Not a library** (see below) |

`oeadm` breaks two of the rules above on purpose, so the exception is written down rather than
discovered. It ships a `bin`, and it depends **upward** on `@orca/environment-worker` under
`services/`. That upward edge is the point of `oeadm worker`: a self-hosted operator should run one
command, not clone a service and wire eight environment variables by hand, so the CLI maps its flags
onto the worker's env-var contract and spawns the worker's built entry as a child. Read it as a
licence for a **launcher**, not for services under `packages/` generally — it still has no listener,
no Dockerfile, and no state of its own. Its guidance is
[`oeadm/AGENTS.md`](oeadm/AGENTS.md).

Per-library design docs live in
[`docs/managed-agents/libraries/`](../docs/managed-agents/libraries/). They describe what the
library does today — anything not built goes in
[`roadmap.md`](../docs/managed-agents/roadmap.md), never in a design doc.

## Rules

1. **Interface first, backends behind it.** Each store exposes one interface (`store.ts`) with
   pluggable implementations. Consumers depend on the interface; adding a backend must not change a
   consumer.
2. **Every tenant key and query carries workspace scope.** Blob keys, table predicates, and cursors
   are workspace-scoped without exception — this is the isolation boundary, not a convention.
3. **Ship an in-memory backend.** `file-store`, `memory-store` and `skill-store` each have one for
   tests and development; keep it behaviorally equivalent to the real backend, including error
   cases. `transcript-store` does not — its three backends are kafka, postgres and pulsar, and its
   tests use the dev stack.
4. **Re-export deliberately.** `src/index.ts` is the public API. Anything not exported there is
   internal and may change freely.
5. **`test/unit/` mirrors `src/`.** Integration tests go in `test/integration/` and need the dev
   compose stack (`make dev-up`).
6. **Rebuild after touching shared types:** `pnpm -r build`, so cross-package consumers stay green.

## Commands

```bash
pnpm -F @orca/<pkg> test              # unit, no infra
pnpm -F @orca/<pkg> test:integration  # needs the dev compose stack
pnpm -F @orca/<pkg> lint
pnpm -r build                         # after changing exported types
```

`harness-catalog` is pure data with no I/O — it has no integration suite, and it is the single source
of truth for the harness table. Do not duplicate that table in `registry-service-ts` or
`harness-server`.
