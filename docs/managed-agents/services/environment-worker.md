# environment-worker (`@orca/environment-worker`)

> One long-running **client** process per self-hosted Environment. It dials the
> registry's worker tunnel outbound, then serves the control frames the registry
> pushes back down it: the registry sends `worker.launch_runner` to launch a
> session runner. The worker also serves frames that stop a runner, inspect the
> filesystem and stage a git worktree, which the registry does not send.
> Source: `services/environment-worker/`; developer
> guidance for working in it is
> [`services/environment-worker/AGENTS.md`](../../../services/environment-worker/AGENTS.md).
> The runner it spawns is [`session-runner.md`](./session-runner.md). Topology:
> [`../architecture.md`](../architecture.md),
> [`../deployment-topologies.md`](../deployment-topologies.md).

## A client, not a listener

The worker holds **no listener**. It dials
`WS /v1/tunnels/environments/:environmentId` on the registry with its Env Key on
the `X-Orca-Environment-Key` header, announces itself with a `worker.hello`, and
then answers whatever the registry sends down that socket.

That inversion is what makes a self-hosted Environment possible at all. An
operator's laptop or private VM has no public address and no inbound port the
registry could reach; because the worker dials out, the registry can drive
compute it cannot address. The worker consequently needs no database, no broker,
and no object-store credentials — the only thing it can reach is the registry.

It answers the registry's keepalive pings with a pong on the same socket. A
disconnect reconnects with backoff. The registry accepts the upgrade before it
checks the credential and refuses a missing, wrong or expired Env Key — or an
unknown or archived Environment — by closing the socket with `4004`; the worker
treats that close like any other disconnect, logging the close code and reason
on each retry. A **permanent** upgrade rejection — a `4xx` other than `408` or
`429`, such as the `404` of a registry that predates the
`/v1/tunnels/environments` route — is not retried: it surfaces as
`EnvironmentConnectError`, which the entry point prints verbatim before exiting
non-zero.

## What it does

The worker serves these request frames; the registry sends only
`worker.launch_runner`.

| Frame                     | Worker behavior                                                                                                                                                     |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `worker.launch_runner`    | Spawns one session runner with the launch command from its configuration, in a workspace under the configured workspace directory, and watches the child.           |
| `worker.stop_runner`      | Terminates a named runner, escalating to a kill. Whether the signal landed is reported, not assumed.                                                                |
| `worker.stat`             | Stats a path on the worker's own filesystem, expanding `~` — only the worker knows its own home directory.                                                          |
| `worker.list_dir`         | Lists a directory.                                                                                                                                                 |
| `worker.create_dir`       | Creates a directory for a new workspace.                                                                                                                           |
| `worker.create_worktree`  | Runs `git` (argv arrays, never a shell) to add a session worktree. Branch names are validated against git ref-format rules before reaching argv.                    |
| `worker.remove_worktree`  | Removes one.                                                                                                                                                       |

It also reports **proactively**: a runner that dies unexpectedly produces a
`worker.runner_exited` frame carrying the exit code and a bounded tail of the
child's stdout and stderr. That tail is the whole diagnostic for the worst case —
a runner that died before its own tunnel ever connected leaves its cause nowhere
else.

An inbound frame the worker has no handler for is **dropped, not errored**, so a
newer registry can send a frame kind an older worker does not know without
breaking the connection.

### Git does not block the tunnel

`git fetch` on a large repo takes seconds. The worker runs it on a one-shot
`node:worker_threads` Worker _and_ dispatches the request off its serve loop.
Both halves are required: a free event loop does nothing for a serve loop parked
awaiting the promise, because it never calls `receive()` again and the registry's
keepalive ping simply queues until the connection is declared dead.

## Configuration

Everything comes from the process environment; there are no flags. The full table
— every variable, its default, and what it does — is in
[`services/environment-worker/AGENTS.md`](../../../services/environment-worker/AGENTS.md),
which `pnpm docs:env-check` scans `src/config.ts` against.

A minimal self-hosted deployment sets five variables: the environment id, the Env
Key, the registry tunnel URL, a workspace directory, and the runner launch
command. `oeadm env create` prints the Env Key exactly once;
[`oeadm worker`](../../../packages/oeadm/AGENTS.md) maps flags onto these names
and spawns the worker, so an operator runs one command.

One variable changes the shape of the rest. When an Environment Token is present,
the worker authenticates with it on the `X-Orca-Environment-Token` header instead
of an Env Key, and its identity — worker id and name — is required from the
environment rather than falling back to the OS hostname, because a
registry-launched sandbox's hostname is meaningless. Exactly one credential
header is sent, never both.

## What a runner inherits

The worker runs **as the operator**, so its environment can hold the operator's
personal secrets. A spawned runner therefore inherits an **allowlist**, not the
environment: process essentials, the locale family, TLS trust stores, the
session-runner's own operator knobs, and the four runner-wiring variables layered
on last.

The line the allowlist draws is **secrets versus configuration**, not worker
variables versus runner ones. That distinction is load-bearing: a self-hosted
operator runs one command, so the worker's environment is the only surface they
have, and a runner knob that does not cross this boundary cannot be set at all.
A credential crosses only when the operator names it explicitly in the
passthrough variable.

The runner's per-launch binding token is a control-plane secret seeded **only**
through its wiring variable. Any inherited copy is stripped before this launch's
token is layered on, so a stale token in the base environment cannot ride
through.

The four wiring names are single-sourced in
[`@orca/harness-tunnel`](../libraries/harness-tunnel.md) and imported by both the
worker that sets them and the runner that reads them — neither side spells a name
the other owns.
