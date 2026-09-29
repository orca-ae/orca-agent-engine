# harness-server — agent guidelines

Internal-only service that hosts the agent loop, owns sandboxes and resource mounts, and routes tool
calls. **No public listener. No workspace API key.** Read
[`docs/managed-agents/services/harness-server.md`](../../docs/managed-agents/services/harness-server.md)
and [`harness-modes.md`](../../docs/managed-agents/harness-modes.md) before changing routing.

Those docs describe current behavior only; anything not built belongs in
[`roadmap.md`](../../docs/managed-agents/roadmap.md).

## Sandbox runtimes

`SANDBOX_RUNTIME` is **required — there is no silent default**. `src/config.ts` throws on a missing
or unrecognized value.

| Value         | Implementation             | Use                                                        |
| ------------- | -------------------------- | ---------------------------------------------------------- |
| `local`       | `src/sandbox/local/`       | Host shell wrapped by `srt` (`sandbox-exec` / bubblewrap)  |
| `e2b`         | `src/sandbox/e2b/`         | E2B cloud sandboxes; needs `E2B_API_KEY`                   |
| `opensandbox` | `src/sandbox/opensandbox/` | OpenSandbox server; needs `OPEN_SANDBOX_DOMAIN` + `_IMAGE` |
| `agentenv`    | `src/sandbox/agentenv/`    | AgentENV Firecracker VMs; needs gateway, API key, + image  |
| `in-memory`   | `src/sandbox/in-memory/`   | Tests only                                                 |

All five implement `src/sandbox/sandbox-runtime.ts`. New runtimes implement that interface and get
wired in `src/main.ts` — do not special-case a runtime anywhere else.

## Harness modes

An agent's `metadata.harness` + `metadata.mode` select the topology.
`@orca/harness-catalog` is the single source of truth for which combinations are legal; do not
duplicate that table here or in the registry.

- **`separate`** (`claude_agent_sdk`, `codex_sdk`, `pi_sdk`) — the SDK runs _in this process_
  (`src/harness/claude/`, `src/harness/codex-sdk/`). Tools dispatch to the sandbox. LLM calls
  follow `LLM_EGRESS_DEFAULT` (`direct` when unset), with `metadata.orca_llm_egress` as a
  per-Session override. Direct calls use the configured provider endpoint; Gateway-selected calls
  use `LLM_GATEWAY_URL` with a Session-scoped JWT.
- **`colocated`** (cloud `claude_code`, `codex_sdk`, `pi_sdk`) — the harness process runs inside
  the sandbox image and this service orchestrates it over HTTP/SSE (`src/harness/in-sandbox/`,
  and `src/harness/codex-sdk/remote-worker.ts` for the two SDKs). LLM egress goes through
  ai-gateway — `LITELLM_API_BASE` / `LITELLM_API_KEY` (`llm-env.ts`) for Claude Code, a
  Session-scoped Gateway JWT for the SDK workers — and **no provider keys enter the sandbox**.
  Other `colocated` harnesses (`codex`, `cursor`, `pi`, `custom`, `mock`) and every self-hosted
  Session run in `session-runner`, not here.

`src/runner/dispatcher.ts` picks the harness; `session-runner.ts` is the event-loop wrapper around
whichever `AgentHarness` it produced. Transcript-store is the sole source of truth — the sandbox is
ephemeral, and `pumpEvents` dedups replay by the sandbox event's `id`.

## Resource mounts

Everything the agent sees on disk arrives through a `MountStrategy`
(`src/sandbox/mounts/mount-strategy.ts`), selected by `strategy-factory.ts`:

| Strategy              | Resource            | Mechanism                                                                        |
| --------------------- | ------------------- | -------------------------------------------------------------------------------- |
| `tarball-prefetch.ts` | `file`              | Host-side fetch + stream in; sandbox never sees the blob namespace               |
| `memory-fuse.ts`      | `memory_store`      | S3-FUSE mount of the store's `live/` prefix, RO or RW                            |
| `git-clone.ts`        | `github_repository` | Host-side `git clone --filter=blob:none --depth=1`                               |
| `local-memory.ts`     | `memory_store`      | Runtime Files API fallback, auto-picked for `agentenv`, `local`, and `in-memory` |

[`docs/managed-agents/mount-strategies.md`](../../docs/managed-agents/mount-strategies.md) is the
authority; correct it there first and keep this table a summary rather than a second copy.

Write boundaries are enforced by `src/sandbox/write-policy.ts` — see
[`output-write-policy.md`](../../docs/managed-agents/output-write-policy.md). Session outputs are
captured from `/mnt/session/outputs/` by `src/sandbox/outputs/`.

## Security invariants

- The harness holds **no workspace API key**. It calls
  `POST /internal/v1/workspaces/{ws}/sessions/{id}/executions:prepare` for an immutable,
  workspace-validated execution snapshot, and cross-checks every event's workspace and session ids
  against it before any side effect.
- Vault credentials never enter this process or the sandbox. MCP traffic is rewritten
  (`src/mcp/rewrite.ts`) to the gateway with `X-Orca-Credential-Id`; the gateway resolves it.
- GitHub PATs live in SecretStore behind resource-owned references. In-sandbox git uses the
  session-JWT credential helper, never a raw token.

## Tests

```bash
pnpm -F @orca/harness-server test              # unit
pnpm -F @orca/harness-server test:integration  # needs the dev compose stack
```

E2B-gated specs (`test/integration/*-e2b.spec.ts`) self-skip without `E2B_API_KEY`,
`E2B_TEMPLATE_ID`, and `S3_PUBLIC_*`.
