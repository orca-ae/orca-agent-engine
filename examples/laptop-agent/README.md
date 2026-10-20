# laptop-agent — run an agent on your own machine

An agent that runs **on your laptop**, driven by an ordinary application over the Orca
Agent Engine's public HTTP API.

It runs in two modes, and the point of shipping both is what stays the same between them:

> **The client is the same in both modes — same API, same events, same code. All that
> changes is which URL it points at, and who started the process behind it.**

`src/client.ts`, `src/render.ts` and `src/chat.ts` are byte-identical across the two.
`src/local.ts` and `src/shared.ts` differ only in where the engine is.

## The two modes

**Local engine.** The engine runs on this laptop; you started it. Nothing is shared, and
nothing leaves the machine.

**Shared engine.** The engine runs somewhere else and several people use it. Your laptop
joins it as an _environment_, so the agent still executes here — your files, your `claude`
install, your model credentials — while the engine coordinates the session and keeps the
transcript. The worker **dials out**, so nothing listens on your laptop and this works from
behind NAT.

Neither mode is "the real one". The second is what you deploy; the first is what you
develop against.

## Run it — local

```bash
make self-hosted-up                                   # from the repo root
pnpm --filter @orca/example-laptop-agent local -- --harness mock --prompt "hello" --once
```

Start with `--harness mock`. It is deterministic and LLM-free — it answers `mock: <your
text>` — so if it works, the whole chain works: client → registry → worker →
session-runner → single-writer bridge → SSE. If it does not, the problem is the stack,
not a credential.

Then the real thing:

```bash
pnpm --filter @orca/example-laptop-agent local -- --harness claude-code
```

`make self-hosted-up` created a `target=self_hosted` Environment and attached this machine
to it with `oeadm worker`, recording both in `services/dev/run/self-hosted.env`. This
example reads the environment id and api key from there. It does **not** create its own
environment, and that is not laziness: a session must target the environment a worker has
_claimed_. Point one at a fresh environment and it waits for a runner that will never
arrive — with no error, because "waiting for a worker" is a legitimate state.

## Run it — shared

```bash
ORCA_BASE_URL=https://engine.example ORCA_API_KEY=orca_… \
  pnpm --filter @orca/example-laptop-agent shared
```

It creates an Environment on that engine and prints the exact `oeadm worker …` command to
run in a second terminal. It then waits for `work_stats.worker_connected` and runs the same
chat loop.

The Env Key is echoed **exactly once**, at create, and is never recoverable — so that
command is printed then or not at all. It is passed through the environment rather than
`--env-key` because a credential in argv is visible in `ps` to every user on the host.

Two things this mode shows that the local one cannot: the transcript survives killing the
app mid-turn (re-attach with `oeadm attach --session <id>`), and the agent runs on your
hardware while none of your code or keys reach the engine.

## Options

```
--harness <claude-code|claude-sdk|mock>   default: claude-code
--prompt <text>                            send this as the first turn
--once                                     run one turn and exit (implies --prompt)
--environment <env_...>                    target an environment explicitly
--allow-tools                              skip the approval prompt (always_allow)
```

Tools default to `always_ask`. When a gate fires, the agent asks, the terminal prompts, and the
answer goes back as `user.tool_confirmation`; `--allow-tools` turns that off for a quieter demo.

Both Claude harnesses expose native built-in tools alongside the orca MCP tools. The runner
resolves their permission requests against the snapshot's per-tool and default policies; an
unclassified tool requires confirmation. The SDK's working directory is the session root.
That working directory alone provides no filesystem isolation.

## What `--harness claude-code` needs

It runs the **real `claude` binary**, headless, as a child of the session runner — you can
see it in `ps` during a turn, with `--input-format stream-json` and an `--mcp-config`
pointing at the orca tool bridge.

- `claude` on the **runner's** `PATH`.
- A model credential — and here is the part that is easy to get backwards. The runner passes its
  own `ANTHROPIC_API_KEY` down to the CLI, and **that key overrides the CLI's own credentials**.
  So an INVALID key is worse than no key at all: with none set, `claude` falls back to whatever
  it is already authenticated with and simply works; with a bad one, authentication fails.
  If you have a working Claude Code install and no separate API key, start the stack with
  `ANTHROPIC_API_KEY` unset. If you do want to pass a key, it must be named in
  `ORCA_RUNNER_ENV_PASSTHROUGH` (which `make self-hosted-up` does by default), because the worker
  forwards an allowlist rather than its whole environment.

`HOME` is on that allowlist, which means the CLI also picks up your personal `~/.claude`
configuration — plugins, subagents, settings. Convenient, and worth knowing: this demo is
not hermetic, and your local config can change how the agent behaves.

## If nothing comes back

This example prints a hint after 60 seconds without a reply. Check the transcript's
`agent.error` events and the runner logs for provider or authentication failures.

When that happens, check the key the _worker_ holds — not the one in your shell:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" -H 'anthropic-version: 2023-06-01' \
  -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet-4-5","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}'
```

A `401` there is the whole explanation.

## Reading the code

`src/client.ts` is the file to copy. It has no dependencies — `fetch` and the standard
library, no SDK, nothing from `@orca/*` — and it is the entire API surface this example
uses. Two details in it are worth reading before you write your own:

- **`orca-beta` is not optional.** The default stream is filtered to the documented
  Anthropic session-event vocabulary, and `agent.turn_completed` and
  `agent.requires_action` are not in it. Without the header a turn never appears to
  finish and an approval never arrives — the client hangs rather than erroring.
- **The SSE resume cursor is the `id:` line, not a field in the body.** The default
  projection strips `seq`, so a client that reads the cursor out of the payload gets
  `undefined` and silently restarts from zero on every reconnect.

If you would rather not write a client at all, the published `orca` CLI already covers this
surface (`agent sessions create`, `agent sessions events stream`, …) against any deployment.
Read this example to learn the API; reach for that when you only want to drive a session.

## What this is not

The runner's default sandbox spawns the CLI as a real child with **no privilege boundary**.
This is a development demo, not an isolation story. Tunnel auth is also loopback-only by
default, which is why the shared mode joins your laptop as an _environment_ rather than
exposing a runner.
