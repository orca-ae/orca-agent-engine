# environment-worker — agent guidelines

A long-running **client** process, one per self-hosted Environment. It holds no listener: it dials
the registry's worker tunnel outbound and then _serves_ the control frames pushed down it — the
registry sends `worker.launch_runner`; the worker also serves stop-runner, stat, directory and
git-worktree frames, which the registry does not send. That inversion is the whole design; a worker
on an operator's laptop or private VM reaches the registry and nothing else, so it needs no
database, no broker, and no inbound port.

[`docs/managed-agents/services/environment-worker.md`](../../docs/managed-agents/services/environment-worker.md)
is the component description — what this service is and how it behaves, for a reader of the design
docs. This file is the other half: how to work _in_ it. Both describe current behavior only;
anything not built belongs in [`roadmap.md`](../../docs/managed-agents/roadmap.md).

## The two things it owns

**Runners.** `worker.launch_runner` spawns one session runner per session, with an environment built
by `src/runner-env.ts` (below). The worker watches each child and reports an unexpected death
proactively as `worker.runner_exited`, carrying a bounded tail of the child's stdout+stderr so the
cause of a runner that died before its tunnel ever connected is not lost.

**The workspace.** The worker serves `worker.stat` / `worker.list_dir` / `worker.create_dir`
(`src/fileops.ts`) and `worker.create_worktree` / `worker.remove_worktree` (`src/git-worktree.ts`)
to inspect directories and stage a repo; the registry sends none of them — `worker.launch_runner`
is the only request frame it sends. The worker owns `~` expansion, because only the worker knows its
own `HOME`.

Git is blocking. `src/worktree-offload.ts` runs it on a one-shot `node:worker_threads` Worker _and_
the worker dispatches the request off the serve loop — both halves are needed, because a free event
loop does nothing for a serve loop that is parked awaiting the promise and therefore never calls
`receive()` to read the registry's keepalive ping.

## Environment

Configuration comes **entirely** from the process environment (`src/config.ts`); there are no flags.
`oeadm worker` in `@orca/oeadm` is a thin front-end that maps its own flags onto exactly these
names and then spawns this process.

A minimal self-hosted deployment sets the five required variables and takes the defaults for the
rest.

| Variable                   | Required     | Effect                                                                                                                                                                                                            |
| -------------------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ENVIRONMENT_ID`           | always       | Identity of this Environment, presented to the registry. It is the path segment of the tunnel the worker dials, `/v1/tunnels/environments/<id>`.                                                                  |
| `ENVIRONMENT_KEY`          | self-hosted  | The Env Key that authenticates the tunnel dial, sent on the `X-Orca-Environment-Key` handshake header. `oeadm env create` prints it exactly once. Not required when `ORCA_ENVIRONMENT_TOKEN` is set.              |
| `REGISTRY_TUNNEL_BASE_URL` | always       | Base URL of the registry worker tunnel. An `http(s)://` origin is upgraded to `ws(s)://`.                                                                                                                         |
| `WORKSPACE_DIR`            | always       | Host directory under which per-session runner workspaces are created.                                                                                                                                             |
| `RUNNER_LAUNCH_COMMAND`    | always       | argv used to launch a session runner; element `[0]` is the executable. Split on whitespace with **no shell quoting** — an argument containing a space needs a launcher script.                                    |
| `REGISTRY_RUNNER_URL`      | _(defaults)_ | Base URL of the registry _runner_ tunnel, handed to each spawned runner. Defaults to `REGISTRY_TUNNEL_BASE_URL`, which is right whenever one registry serves both tunnels.                                        |
| `ENVIRONMENT_WORKER_NAME`  | managed only | Human-readable name announced in the `worker.hello`. Defaults to the OS hostname — **except** on the managed path, where it is server-injected and required, because a managed sandbox's hostname is meaningless. |
| `ORCA_ENVIRONMENT_TOKEN`   | managed only | Per-launch Environment Token a registry-launched worker presents on `X-Orca-Environment-Token` instead of an Env Key. Setting it selects the managed path wholesale (see below).                                  |
| `ENVIRONMENT_WORKER_ID`    | managed only | Server-assigned per-launch worker identity, injected alongside `ORCA_ENVIRONMENT_TOKEN`. Optional and unused on the self-hosted path.                                                                             |

`src/config.ts` is scanned by `pnpm docs:env-check`, so a new variable read there fails the build
until this table describes it.

### One fork, taken by one variable

`ORCA_ENVIRONMENT_TOKEN` is the only switch. Set, the worker is a registry-launched sandbox with no
operator to provision an Env Key: `ENVIRONMENT_KEY` stops being required, and identity stops falling
back to the hostname — `ENVIRONMENT_WORKER_ID` and `ENVIRONMENT_WORKER_NAME` become required,
because the registry assigns a managed sandbox's identity rather than reading it off the box. Unset,
every variable and default resolves as it always did; the self-hosted path is untouched.

Exactly one credential header is sent, never both.

### What a runner inherits, and what it does not

`src/runner-env.ts` builds the spawned runner's environment. The worker runs **as the operator**, so
its environment can hold the operator's personal secrets and a runner has no business inheriting
them. So a runner gets an **allowlist**, not the environment:

```
process essentials   PATH HOME USER LOGNAME SHELL TMPDIR TZ TERM TERMINFO TERMINFO_DIRS LANG
                     and the LC_ locale family, by prefix
TLS trust stores     SSL_CERT_FILE SSL_CERT_DIR REQUESTS_CA_BUNDLE CURL_CA_BUNDLE NODE_EXTRA_CA_CERTS
```

plus the session-runner's own operator knobs — `ANTHROPIC_ALLOWED_MODELS`, `ANTHROPIC_MODEL_DEFAULT`
and `ORCA_RUNNER_IDLE_TIMEOUT_S`, whose meanings are
[`services/session-runner/AGENTS.md`](../session-runner/AGENTS.md)'s to define — and the four
runner-wiring variables, layered on last.

The allowlist separates **secrets from configuration**, not worker variables from runner ones. That
distinction is load-bearing: a self-hosted operator runs one command, `oeadm worker`, so the worker's
environment is the only surface they have. A runner knob that does not cross this boundary cannot be
set at all — and `ANTHROPIC_ALLOWED_MODELS` is a deny-by-default control whose unset case admits any
well-formed model id, so leaving it behind made a security control fail **open**.

| Variable                      | Effect                                                                                                                                                                                                                                |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ORCA_RUNNER_ENV_PASSTHROUGH` | Comma-separated **extra** variable names to forward worker→runner beyond the allowlist. This is the opt-in for a credential: `ANTHROPIC_API_KEY`, a custom gateway token or URL, a provider env ref. Everything unnamed stays behind. |

A credential is never allowlisted. `ANTHROPIC_API_KEY` reaches a runner only when an operator names
it here — the session-runner uses it as a fallback LLM credential when the snapshot's egress carries
no scoped JWT, and forwarding it is a decision the operator makes explicitly.

The four wiring variables the runner reads — the registry runner-tunnel URL, the per-launch binding
token, the workspace root and the worker's own pid — are **not spelled in this package**, and are
deliberately not spelled here either. Their names are single-sourced in `@orca/harness-tunnel`'s
`identity.ts` and imported by both the worker that sets them and the runner that reads them, because
the two packages never import each other and a name either side spells for itself is a hand-copied
contract. A doc that repeats them is a third copy with the same failure mode.
`runner-env.ts` re-declared one of them once; `test/unit/runner-env.spec.ts` now reads this module's
own source text to make that impossible to reintroduce, since a value comparison passes right up
until the moment the two diverge.

The runner's binding token is a control-plane secret seeded **only** through its wiring variable.
`stripRunnerAuthSecrets` removes any inherited copy before this launch's token is layered on, so a
stale token in the base environment can never ride through.

## Connecting

`src/ws-client.ts` is the network seam and `src/process-spawner.ts` the process seam, so the whole
loop is unit-testable against a fake registry socket and a stub runner command with no infra.

A disconnect reconnects with backoff. The registry accepts the upgrade and then refuses a missing,
wrong or expired Env Key or Environment Token — or an unknown or archived Environment — by closing
the socket with `4004`; that close is a `TunnelClosedError` like any other disconnect, so the worker
retries it with backoff and logs the close code and reason each time. A **permanent** upgrade
rejection — a `4xx` other than `408`/`429`, such as the `404` of a registry that predates the
`/v1/tunnels/environments` route — raises `EnvironmentConnectError`, which the reconnect loop
re-raises rather than backing off, so `src/main.ts` prints the actionable cause and exits non-zero.

An inbound frame with no handler (a result frame, a future request kind) is **dropped, not errored**.

## Tests

```bash
pnpm -F @orca/environment-worker test
pnpm -F @orca/environment-worker lint
```

There is no integration suite: every spec runs in-process against a fake registry tunnel and stub
child processes, so the unit run is this package's whole coverage provenance. The package is listed
in `vitest.shared.mjs`'s covered-packages list and in `coverage-thresholds.json`; the
`vitest.config.ts` beside this file exists for coverage, not discovery, and the three are one
mechanism — remove any one and `COVERAGE=1` becomes a silent no-op for this source.
