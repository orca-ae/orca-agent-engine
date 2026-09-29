# session-runner — agent guidelines

A long-running **client** process, spawned once per session by the environment-worker. It holds no
listener: it dials the registry's runner tunnel outbound and then _serves_ the requests the owning
registry pod pushes down it. That inversion is the whole design — a self-hosted runner sits behind a
NAT, reaches nothing but the registry, and therefore has no database, no broker, and no object-store
credentials.

[`docs/managed-agents/services/session-runner.md`](../../docs/managed-agents/services/session-runner.md)
is the component description — what this service is and how it behaves, for a reader of the design
docs. This file is the other half: how to work _in_ it. Read
[`session-runner-scope.md`](../../docs/managed-agents/session-runner-scope.md) before adding a
responsibility here — it records what the runner itself owns, what it reuses from Orca's existing
substrate, and what is deliberately out of scope. All three describe current behavior only; anything
not built belongs in [`roadmap.md`](../../docs/managed-agents/roadmap.md).

## The wire is load-bearing

`src/protocol.ts` holds the ten routes the owner pod pushes (turn, snapshot, skills, replay,
confirmation, custom-tool result, interrupt, resources, resource changes, resource acknowledgement),
the two request headers, and the NDJSON content type. Every one of those
literals is declared **twice** — once here and once in the registry's `src/tunnel/*` — because the
runner is a separate process that does not link the registry.

Two hand-written copies are not a contract. `test/unit/protocol-registry-pin.spec.ts` asserts the
runner's value against the registry **source file**, read as text: an assertion against a second
hand-copied literal would pass just as happily when the registry side drifts, which is the failure
worth catching. The pin skips itself while the registry half is absent from the branch and
re-activates on its own once it lands.

**Adding a constant to `src/protocol.ts` means adding a row to that spec's pin table** — a
completeness check fails the build otherwise, because a hand-maintained inclusion list cannot fail
loudly on its own.

The same mechanism, shared through `test/unit/support/registry-source-pin.ts`, pins the multiagent
thread-event vocabulary in `test/unit/multiagent-thread-events.spec.ts`.

## Providers

`src/harness/provider.ts` is the dispatch seam: a registry of factories keyed by the **snapshot's**
`provider` field. The launch frame deliberately omits the provider — the worker spawns a generic
runner shell and the credential-free snapshot selects the harness.

`defaultRegisterProviders` in `src/main.ts` wires them all:

| Provider                | Shape                         | Notes                                                                    |
| ----------------------- | ----------------------------- | ------------------------------------------------------------------------ |
| `claude`                | in-process Claude Agent SDK   | lean, one `query()` per turn                                             |
| `claude-sdk-persistent` | in-process Claude Agent SDK   | long-lived session; the only provider honoring a per-turn model override |
| `claude-code`           | native CLI over stream-json   | headless `claude` as a long-lived child                                  |
| `pi-sdk`                | native Pi SDK subprocess      | `@earendil-works/pi-coding-agent` 0.87.0; private native session history |
| `codex-sdk`             | native Codex SDK subprocess   | `@openai/codex-sdk` 0.154.0; native persisted thread history             |
| `codex`                 | native CLI over JSON-RPC      | `codex app-server`                                                       |
| `cursor`                | native CLI over stream-json   | `cursor-agent`                                                           |
| `pi`                    | native CLI over NDJSON RPC    | `pi --mode rpc` plus the bridge extension                                |
| `custom`                | native CLI, operator-declared | driven entirely by the snapshot's `custom_spec` — no runner change       |
| `mock`                  | none                          | tests; no LLM egress, no sandbox                                         |

Adding a provider is a `register*Provider` call, nothing more. **The session loop never branches on
the provider name** — if a change needs it to, the seam is in the wrong place.

Every native-CLI provider reaches Orca's real tools through one bridge
(`src/mcp/native-cli-bridge.ts`), exposed to the CLI as its `orca` MCP server, and routes the CLI's
own approval request to the one transcript-backed gate in `src/tool-confirmation.ts`. Do not add a
second approval path.

## The sandbox seam

`src/sandbox/seam.ts` is this package's **only** import point for `@orca/sandbox-runtime`, and the
place a concrete runtime is constructed at boot. It re-exports the boundary types so runner code has
one path to import them from — the same shim pattern harness-server uses.

Runtime selection mirrors harness-server's `SANDBOX_RUNTIME`, with one deliberate difference:

| Value               | Result                                                                          |
| ------------------- | ------------------------------------------------------------------------------- |
| unset / `in-memory` | in-memory runtime — a tmpdir plus `child_process.spawn`, no external dependency |
| `local`             | the `srt`-wrapped local runtime **when this host has `srt`**                    |
| `local`, no `srt`   | in-memory, with a visible warning — **degrade, never boot-fail**                |

harness-server hard-fails at boot on a missing `srt`; a self-hosted runner must not crash on an
operator's box that has not installed it yet. The fallback is explained, never silent.

`SandboxHandle.spawn` is optional on the interface — cloud-only runtimes may omit it — so a
native-CLI provider must feature-detect it and fail loudly rather than quietly running unsandboxed.

## Environment

The four wiring variables the launcher injects (binding token, workspace root, parent pid, registry
tunnel URL) are **not spelled here or in `src/config.ts`**: their names are single-sourced in
`@orca/harness-tunnel`'s `identity.ts` and imported by both the worker that sets them and the runner
that reads them. A name either side spells for itself is a hand-copied contract.

What follows is the runner's own operator-facing surface — the variables nothing else owns:

| Variable                     | Default         | Effect                                                                                                                                                                                                                                                                          |
| ---------------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ORCA_RUNNER_IDLE_TIMEOUT_S` | `3600` (1 hour) | Graceful shutdown after this many seconds with no work and no turn in flight. `0` disables the watchdog. A malformed value fails at startup rather than silently changing the lifecycle.                                                                                        |
| `ANTHROPIC_ALLOWED_MODELS`   | _(unset)_       | Comma-separated allow-list a per-turn model override may select on `claude-sdk-persistent`. Unset admits any well-formed id — the gateway stays the policy point and this is defense in depth. An override outside the list falls back to the snapshot model with a diagnostic. |
| `ORCA_RUNNER_SESSION_ID`     | _(unset)_       | Pins the session id up front. The worker does not seed it today — the id arrives over the tunnel in the launch frame — so this is read opportunistically, for a manual `pnpm dev` launch.                                                                                       |
| `ORCA_RUNNER_WORKSPACE_ID`   | `''`            | Pins the owning workspace id. Also unseeded today: the runner emits agent events up the tunnel and the owner pod stamps the workspace scope on persist, so the runner does not need the real id to emit.                                                                        |

`ANTHROPIC_MODEL_DEFAULT` and `ANTHROPIC_API_KEY` are read here with harness-server's meaning: the
default model when a snapshot pinned none, and a fallback LLM credential used **only** when the
snapshot's egress carries no scoped JWT. Gateway egress always takes precedence; the fallback never
overrides it.

`src/config.ts` is scanned by `pnpm docs:env-check`, so a new variable read there fails the build
until this table describes it.

## What this process must never acquire

- **No transcript backend.** History lives in the tunnel-fed in-memory store in
  `src/transcript/in-memory-transcript-store.ts`; the registry pushes the recovery replay down and
  the runner streams new events up. Wiring a broker or a database here would break the
  outbound-only posture.
- **No object-store credentials and no `@orca/skill-store`.** Skill bundles are pushed to the skills
  route as bytes and materialized to a native plugin directory under the runner's workspace.
- **No secrets in logs.** The boot line carries the derived runner id, the tunnel URL, the pinned
  session id, and the advertised providers — never the binding token or an egress JWT.
- **No second source of the resume cursor.** The runner derives it solely from the ids of events it
  actually applied. The resume-cursor header the registry sends is informational; reading it would
  couple the runner to a value it must derive from applied state.

## Tests

```bash
pnpm -F @orca/session-runner test
pnpm -F @orca/session-runner lint
```

There is no integration suite, so the unit run is this package's whole coverage provenance. No spec
needs a registry: the tunnel specs dial an in-process fake
(`test/unit/support/fake-registry-runner-tunnel.ts`), and the native-CLI provider specs spawn the
fake CLIs in `test/unit/support/`. Some specs do run real processes: the tmux-backed specs drive a
real tmux and skip when it is not installed, and the Codex SDK and Pi SDK worker specs run the
installed SDKs (Codex's bundled executable included) through the built worker entries in `dist/`
against local model fixtures, so build the package before running them.
