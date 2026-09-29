# @orca/sandbox-runtime

> Library: the `SandboxRuntime` / `SandboxHandle` interface plus the two
> reusable runtime implementations (`InMemorySandboxRuntime`,
> `LocalSandboxRuntime`). Not a deployable service — consuming services
> (`harness-server`) import the package and drive the sandbox in-process.

## What it is

`SandboxRuntime` is the abstraction over the per-session execution sandbox.
Everything above this boundary (mount strategies, the agent toolset, the
dispatcher) is sandbox-agnostic and composes against `SandboxHandle` rather
than any concrete runtime.

Two runtimes ship here because they are host-side and reusable:

- **`InMemorySandboxRuntime`** — tmpdir-backed, runs `bash -c` via
  `child_process.spawn`. Test double + dev. No isolation; not for production.
- **`LocalSandboxRuntime`** — the production-equivalent of InMemory for the
  local-stack release. Filesystem ops go through a per-session host work-dir
  and every command is wrapped by `@anthropic-ai/sandbox-runtime`'s
  `SandboxManager` so the OS-level filesystem + network allow-lists are
  enforced (`sandbox-exec` on macOS / `bwrap` on Linux).

Cloud-only runtimes (E2B, OpenSandbox) live in `@orca/cloud-sandbox`; they
import this interface from the package.

Beyond the interface and the two runtimes, the package also owns the **sandbox
write-policy** construction and enforcement specified in
`docs/managed-agents/output-write-policy.md` (builder, validation brand,
bubblewrap/probe commands, the policy-enforced handle wrapper) and the
**Skill-bundle validation** from `docs/managed-agents/skills.md`.

## Interface

```ts
import type {
  SandboxRuntime,
  SandboxHandle,
  SandboxFiles,
  SpawnHandle,
  ToolCall,
  ToolResult,
  EnvironmentSpec,
  SandboxCapabilities,
} from '@orca/sandbox-runtime';
```

`SandboxHandle` exposes:

- `run(call)` — run a tool call (`bash` / `glob` / `grep`) to completion.
- `spawn?(cmd, opts?)` — spawn a **long-lived** process, returning live
  `{ stdout, stdin, kill, exited }` (`exited` resolves with the exit
  code/signal and rejects on spawn failure). Use this when a transport needs
  to keep a process attached (write to stdin, consume streamed stdout)
  instead of buffering to completion. InMemory spawns on the host; Local
  wraps the command with the `srt` sandbox profile first.
- `files` — `write` / `read` / `list` / `delete`.
- `runPrivileged?` — `sudo`-elevated command. **Optional**: absence is the
  feature-detection signal, and both runtimes in this package omit it (no
  privilege boundary). Only FUSE-capable cloud runtimes implement it —
  feature-detect, don't try/catch.
- `pause` / `resume` / `destroy`, optional `endpoint(port)`.

## Runtimes

```ts
import { InMemorySandboxRuntime, LocalSandboxRuntime } from '@orca/sandbox-runtime';

const rt = new InMemorySandboxRuntime();
const sandbox = await rt.acquire({});
const proc = await sandbox.spawn('cat');
proc.stdin.write('hello\n');
proc.stdout.on('data', (b) => process.stdout.write(b));
proc.kill();
await sandbox.destroy();
```

## Layout

```
src/
  sandbox-runtime.ts    # interface types + parseToolCall + write-policy brand/guard
  write-policy.ts       # buildSandboxWritePolicy / validateSandboxWritePolicy,
                        #   bubblewrap + alias/mount probe commands,
                        #   createPolicyEnforcedSandbox (agent-facing wrapper)
  read-page.ts          # bounded symlink-safe UTF-8 paging (readUtf8Page + remote envelope)
  skills-materialize.ts # Skill bundle validation (validateSkillBundle, SKILLS_ROOT)
  chmod-many.ts         # batched permission-application command builder
  kill-process-tree.ts  # detached process-group kill shared by the runtimes
  index.ts              # barrel export (~50 symbols incl. the write-policy surface)
  in-memory/runtime.ts  # InMemorySandboxRuntime
  local/
    runtime.ts          # LocalSandboxRuntime + SandboxManager adapter types
    materialize.ts      # per-session work-dir helpers
test/
  unit/                 # vitest unit specs (incl. the read-page/write-policy/chmod safety net)
```

## Build & test

- `pnpm -F @orca/sandbox-runtime build` — bundle via tsup.
- `pnpm -F @orca/sandbox-runtime test` — vitest unit suite.
- `pnpm -F @orca/sandbox-runtime lint` — ESLint.
