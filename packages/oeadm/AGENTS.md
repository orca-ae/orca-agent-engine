# oeadm (`@orca/oeadm`) — agent guidelines

One binary, `oeadm`, with four subcommands: `run`, `attach`, `env`, `worker`. It is a **pure client**
on the registry's existing Anthropic-compatible HTTP API — every method in `src/client.ts` maps to a
route the registry already serves. The CLI never creates or binds a runner, never reaches a
database, and holds no server-side state.

## Why `oeadm` and not `ork`

There are two CLIs. `ork` is the **client** — a Go binary published as the image
`ghcr.io/orca-ae/orca-cli` and through `brew install orca-ae/tap/ork`, covering the registry's
resources and the managed-agents API (`agent sessions create`, `agent sessions events stream`, …)
against any deployment. This one is the **engine operator** CLI, and the split mirrors `kubectl` /
`kubeadm`: `ork` drives resources the way `kubectl` does, and `oeadm worker` joins a machine to an
engine much as `kubeadm join` joins a node to a cluster.

Two of the four subcommands exist nowhere else — `worker`, and `attach --terminal` (the
terminal-attach WebSocket proxy). `run`, `env`, and `attach --session` overlap the Go client
deliberately: they are what makes this binary usable on its own during development, without a second
tool on PATH.

## It lives under `packages/`, and it is not a library

[`packages/AGENTS.md`](../AGENTS.md) says everything here is "a library, never a service", and this
package is the second registered exception alongside `e2e-tests`. It ships a `bin`, and it depends
_upward_ on `@orca/environment-worker` under `services/`.

That dependency is deliberate and is the whole point of `oeadm worker`: a self-hosted operator should
run one command, not clone a service and wire eight environment variables by hand. The command maps
its flags onto the worker's env-var contract and spawns the worker's built entry as a child. Read
that as a rule rather than a licence — the exception is a **launcher** for a service, and adding a
listener or a Dockerfile here still means you are in the wrong directory.

## Environment

| Variable        | Default                 | Effect                                                                                                    |
| --------------- | ----------------------- | --------------------------------------------------------------------------------------------------------- |
| `ORCA_BASE_URL` | `http://localhost:8080` | Registry base URL — the `make stack-up` port by default. A trailing slash is trimmed.                     |
| `ORCA_API_KEY`  | _(none)_                | Workspace api key, sent as the Anthropic-canonical `x-api-key` header. Required for `run`/`attach`/`env`. |

`worker` needs neither: the client config is resolved lazily _per subcommand_, so `oeadm worker` runs
without an api key. The worker's own variables are
[`services/environment-worker/AGENTS.md`](../../services/environment-worker/AGENTS.md)'s to define;
`src/commands/worker.ts` only maps flags onto them and is scanned by `pnpm docs:env-check` against
that table, so a flag mapping that drifts from the worker's contract fails the build.

Prefer `ENVIRONMENT_KEY` in the environment over the `--env-key` flag. Both work, but a value on the
command line is visible in `ps` to every user on the host and is written into shell history, and an
Env Key authenticates a worker to the registry. The usage text teaches the environment variable.

## Failure is an exit code

Every subcommand reports failure by **throwing**; `run()` in `src/main.ts` is the one place that
becomes a non-zero status, printing `error: <message>` on stderr. `oeadm worker` is the exception —
it proxies a long-running child, so it propagates that child's exit code instead.

This matters more than it looks, because the CLI's failure modes are mostly quiet ones. Three of
them shipped as exit 0:

- a terminal attach the registry **refused** (`attachTerminal` resolves a
  `TerminalAttachResult`, and `attachCommand` throws on any close code but `1000`, surfacing the
  registry's own close `reason`);
- a transcript tail that **died mid-turn** (`drainTurn` returns `'turn_completed' | 'stream_ended'`;
  a dead tail is announced, resumed once from the last `seq`, and throws if the resume also dies —
  it never re-prompts, because a further question would be committed server-side with nothing shown
  back);
- an `env create` that returned **no `env_key`**, which is the command's entire purpose and is
  unrecoverable afterwards.

If you add a subcommand, decide which of the two shapes it is and pin it with a test. The
`error → exit 1` mapping and the worker code propagation are each one statement, and both survived
being deleted with the whole suite green.

## Seams, not mocks

Everything that touches the outside world is injected, so every spec runs in-process:

| Seam                             | Real                  | In tests                                               |
| -------------------------------- | --------------------- | ------------------------------------------------------ |
| `OrcaFetch` (`src/client.ts`)    | global `fetch`        | `test/fakes/registry.ts` — a `METHOD path` route table |
| `ChatIo` (`src/session-chat.ts`) | `src/terminal-io.ts`  | a scripted IO handing back canned lines                |
| `WebSocketCtor`                  | the Node 22 global    | a fake socket a test drives event by event             |
| `SpawnWorker` / `resolveEntry`   | `child_process.spawn` | a fake child whose exit the test triggers              |

**Test the producer, not only the consumer.** `session-chat.spec.ts` asserted both tool-approval
verdicts against a scripted `ChatIo` for a while, which proved the loop routes a verdict and nothing
about how a typed answer becomes one — `isAffirmative` could be replaced with `return true` and the
suite stayed green. `terminal-io.spec.ts` drives the real terminal IO over in-memory streams. When a
seam guards something security-relevant, both halves need their own spec.

## The gate is deny-by-default

`io.confirm` in `src/terminal-io.ts` is the operator's tool-authorization control: only `y`/`yes`
(case- and whitespace-insensitive) allow, and a **closed stdin denies** — a piped or backgrounded
`oeadm run` must not silently authorize every tool it is asked about. Do not widen that predicate to
a prefix test; `yep` and `yolo` both start with `y`.

## Node built-ins over dependencies

The only runtime dependency is `@orca/environment-worker`. Line editing is `node:readline`, the
terminal attach uses the Node 22 global `WebSocket`, and SSE framing is parsed by hand in
`src/sse.ts`. Keep it that way — this binary is the first thing an operator installs, and its
install should be a copy.

Use `fileURLToPath`, never `new URL(...).pathname`, when turning a resolved specifier into a path:
`.pathname` keeps percent-encoding, so a checkout under `~/My Projects/` reported a correctly built
worker as missing.

## Tests

```bash
pnpm -F @orca/oeadm test
pnpm -F @orca/oeadm lint
pnpm -F @orca/oeadm typecheck
```

No integration suite: every spec runs against the in-process fakes above, so the unit run is this
package's whole coverage provenance. The package is listed in `vitest.shared.mjs`'s covered-packages
list and in `coverage-thresholds.json`; the `vitest.config.ts` beside this file exists for coverage,
not discovery, and the three are one mechanism.
