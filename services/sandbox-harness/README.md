# Sandbox Harness Image

This package builds the HTTP harness that runs inside a sandbox and exposes the
bridge used by `colocated` agents. The server listens on `PORT` (default `4096`)
and starts with `node dist/index.js`.

Build the shared Claude / Codex SDK harness image:

```bash
docker build \
  -f services/sandbox-harness/Dockerfile \
  --build-arg DEFAULT_AGENT=claude-code \
  -t ghcr.io/orca-ae/sandbox-harness-claude-code:latest \
  .
```

The image registers `claude` (including the `claude-code` alias) and `codex-sdk`.
The cloud catalog selects the same image, entrypoint and port for both. Codex
uses private SDK commands over HTTP and native events over SSE; the host adapter
owns tool policy, usage and durable native history. The distinct `codex` CLI
provider runs in session-runner.

## Session-resource runtime dependencies

The runtime image includes `git`, `curl`, `jq`, `bubblewrap`, `s3fs`, `fuse3`,
and the `orca-git-creds` helper. On a FUSE-enabled OpenSandbox deployment,
harness-server mounts memory/output prefixes while the entrypoint is still
waiting as root. The image contains no `sudo` or post-start privilege
escalation path: after the ready marker, bubblewrap supplies a fresh `/dev` and
`setpriv` drops every capability before Node starts.
The entrypoint re-binds the whole filesystem read-only and then re-opens only
what the validated `ORCA_SANDBOX_WRITE_POLICY` lists, which seeds exactly
`/mnt/session/outputs` plus the mount path of each `read_write` resource. So
`/mnt/memory`, `/mnt/session` and `/workspace` are read-only unless such a
resource mounts beneath them. The dispatcher injects only scoped session
JWTs before sandbox startup; repository PATs and provider credentials are not
baked into the image or placed in its environment.
S3 mount startup also uses a transient root-only AWS profile and starts the
long-lived `s3fs` daemon under a scrubbed environment.

The image entrypoint waits for the dispatcher to finish mounting and probing
session resources before it starts the HTTP server inside its read-only-root
bubblewrap namespace. Writable and read-only resource paths come from the
validated `ORCA_SANDBOX_WRITE_POLICY` environment payload. OpenSandbox upload
metadata assigns materialized files and directories directly to the runtime
user, avoiding a recursive ownership scan on the cold-start path. The final
process has `no_new_privs` set and an empty capability bounding set.

Claude's per-command filesystem/PID sandbox remains mandatory inside that
outer namespace. The image routes Bubblewrap through `orca-gvisor-bwrap`, a
transparent shim on ordinary kernels. On gVisor it creates nested user/network
namespaces with a usable loopback before Bubblewrap applies PID, mount,
filesystem, and SDK proxy isolation. The network allowlist from
`ORCA_SANDBOX_WRITE_POLICY` remains active.

## Environment

Read by the in-image server and the SDK subprocess it launches. An operator does
not set these directly — harness-server injects them into the sandbox — but they
are the contract between the two, so a change on either side shows up here.

| Var                                          | Default                         | Meaning                                                                                                                                           |
| -------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PORT`                                       | `4096`                          | Port the bridge HTTP server listens on.                                                                                                           |
| `LITELLM_API_BASE` / `LITELLM_API_KEY`       | _(unset)_                       | Gateway LLM endpoint and per-session JWT. The provider routes through the gateway only when **both** are set.                                     |
| `ANTHROPIC_BASE_URL`                         | derived from `LITELLM_API_BASE` | Derived by stripping a trailing slash run and a trailing `/v1`, because the SDK appends `/v1/messages` itself.                                    |
| `ANTHROPIC_API_KEY` / `ANTHROPIC_AUTH_TOKEN` | the gateway key                 | Set from the gateway key only when not already truthy, so an explicit value is never clobbered. A blank pre-set falls through to the gateway key. |
| `LITELLM_DEFAULT_MODEL`                      | `claude-sonnet-4-6`             | Initial model when the request names none.                                                                                                        |
| `SANDBOX_HARNESS_DEFAULT_AGENT`              | `claude`                        | Agent used when neither the CLI flag nor this variable is set.                                                                                    |
| `SANDBOX_HARNESS_DEFAULT_CWD`                | `process.cwd()`                 | Working directory for managed sessions. Read through a name constant, so the identifier never appears at the read site.                           |
| `SANDBOX_HARNESS_DEFAULT_PERMISSION_MODE`    | `default`                       | Permission mode for managed sessions, same indirection.                                                                                           |
| `ORCA_OUTPUT_CAPTURE_DIRECTORY`              | _(unset)_                       | When set, the directory the agent is instructed to write captured outputs into.                                                                   |

### Per-session launch protocol

A second, narrower set carries one session's parameters across the process
boundary from `createManagedSession` (`session-manager.ts`) to the SDK
subprocess (`subprocess-entry.ts`). Nothing else in the tree sets or reads them.

They are listed because both halves must agree: the name strings live in
`session-manager.ts` and are imported by the decoder rather than re-typed, so
the identifier never appears literally at the read site.

**A hand-set value is not always overwritten.** The child environment is
`{ ...process.env, ...replayEnv }` (`session-manager.ts:510`), and `replayEnv`
is not a fixed ten-key map — each key is written only when the session request
supplies the matching option (`session-manager.ts:216-289`). On a request that
omits one, the key is absent from the spread and whatever the sandbox
environment already held is what the subprocess reads. Baking
`SANDBOX_HARNESS_SYSTEM_PROMPT` into an image therefore sets the system prompt
for every session that does not specify one.

Payloads are base64 because the values are free-form conversation text and tool
schemas — newlines, quotes, unicode — that must survive transport as a single
environment-variable string, and only the canonical padded form decodes
(`base64.ts`). Most carry JSON; `SANDBOX_HARNESS_SYSTEM_PROMPT` carries plain
UTF-8 text.

For eight of the ten, an unset or whitespace-only value means "not supplied",
while a malformed one is a hard error at startup rather than a silent skip — a
"resumed" session starting with no context is worse than crashing loudly. Two
diverge:

- `SANDBOX_HARNESS_SYSTEM_PROMPT` is decoded by a helper with no whitespace
  guard (`subprocess-entry.ts:168`), so only an _absent_ variable means "not
  supplied". Set to whitespace it exits 2 (whitespace is not canonical base64);
  set to the empty string it succeeds and contributes nothing.
- `SANDBOX_HARNESS_FORWARD_SUBAGENT_TEXT` never errors — any non-empty value
  that is not `1` or `true` silently means off, so a typo disables the feature.

| Var                                     | Payload      | Meaning                                                                                                                                                                                                                                                 |
| --------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SANDBOX_HARNESS_REPLAY`                | base64 JSON  | Prior conversation history, flattened into a single preamble user message.                                                                                                                                                                              |
| `SANDBOX_HARNESS_AGENTS`                | base64 JSON  | Subagent definitions, as a JSON object keyed by agent name.                                                                                                                                                                                             |
| `SANDBOX_HARNESS_CUSTOM_TOOLS`          | base64 JSON  | Custom tool definitions, as a JSON array.                                                                                                                                                                                                               |
| `SANDBOX_HARNESS_SYSTEM_PROMPT`         | base64 text  | Replacement system prompt, as UTF-8 rather than JSON.                                                                                                                                                                                                   |
| `SANDBOX_HARNESS_TOOLS`                 | base64 JSON  | The tool allowlist offered to the model, as a JSON string array.                                                                                                                                                                                        |
| `SANDBOX_HARNESS_ALLOWED_TOOLS`         | base64 JSON  | Tools permitted to run without a permission prompt.                                                                                                                                                                                                     |
| `SANDBOX_HARNESS_RUNTIME_TOOLS`         | base64 JSON  | The union of tool-handler names the primary agent and its subagents require. Only `read` is acted on: its presence swaps the SDK `Read` built-in for the sandbox-scoped Orca MCP read tool (`providers/claude.ts:215`). Every other name is inert here. |
| `SANDBOX_HARNESS_FORWARD_SUBAGENT_TEXT` | `1` / `true` | Forward subagent text to the parent stream. Unset means the provider's own default, which is **on** (`providers/claude.ts:376`), and it applies only when subagents are configured.                                                                     |
| `SANDBOX_HARNESS_MODEL_SPEED`           | enum         | `standard` or `fast`. An unrecognised value is rejected, not ignored.                                                                                                                                                                                   |
| `SANDBOX_HARNESS_MODEL_EFFORT`          | enum         | `low`, `medium`, `high`, `xhigh`, or `max`. Also rejected when unrecognised.                                                                                                                                                                            |
