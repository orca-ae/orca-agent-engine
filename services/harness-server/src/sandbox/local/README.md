# LocalSandboxRuntime

`LocalSandboxRuntime` wraps each per-session shell invocation with the host's
native sandbox primitive — `sandbox-exec` on macOS, `bubblewrap` on Linux —
via the `@anthropic-ai/sandbox-runtime` package (the `srt` binary). It is the
sandbox runtime the local stack uses; `SANDBOX_RUNTIME=local` in
`services/dev/.env` selects it.

For the bigger picture (when to use the local stack, prerequisites, env vars,
troubleshooting), see
[`docs/managed-agents/local-stack.md`](../../../../../docs/managed-agents/local-stack.md).
This doc is the developer reference for the runtime itself.

## When to use it

| Runtime     | Selected by                 | Isolation                                 | Use for                                                                      |
| ----------- | --------------------------- | ----------------------------------------- | ---------------------------------------------------------------------------- |
| `local`     | `SANDBOX_RUNTIME=local`     | host-OS sandbox (sandbox-exec/bubblewrap) | local development, e2e suite, contributor laptops, the e2e-stack CI workflow |
| `e2b`       | `SANDBOX_RUNTIME=e2b`       | full VM via E2B's Firecracker fleet       | nightly mount-strategy CI, FUSE-dependent integration tests                  |
| `in-memory` | `SANDBOX_RUNTIME=in-memory` | none — null sandbox                       | unit tests, dev-only chat-only sessions where isolation doesn't matter       |

`LocalSandboxRuntime.capabilities.supportsFuse = false` — files always use
`tarball_prefetch` (on every runtime), and the strategy factory falls back
to `local_memory` for memory stores when this runtime is selected. `srt`
blocks the `mount` syscall, so FUSE strategies can't run here.

## srt prerequisites by OS

| OS      | What `srt` uses           | Install                                                                   |
| ------- | ------------------------- | ------------------------------------------------------------------------- |
| macOS   | `sandbox-exec` (built-in) | `npm install -g @anthropic-ai/sandbox-runtime` — that's it                |
| Linux   | `bubblewrap`              | `apt install bubblewrap` + `npm install -g @anthropic-ai/sandbox-runtime` |
| Windows | not supported by `srt`    | use WSL2 + the Linux row, or `SANDBOX_RUNTIME=in-memory`                  |

The harness logs an error and refuses to start if `SANDBOX_RUNTIME=local` and
`srt` is missing. To bypass, switch to `SANDBOX_RUNTIME=in-memory` (no
isolation — only safe for tests).

## Allow / deny rules

The runtime builds a `SandboxManagerInitConfig` per session in
[`runtime.ts`](./runtime.ts) (`buildSandboxConfig()`). Defaults baked in:

```ts
network:
  allowedDomains    = [...allowedNetworkHosts]   // operator-provided
  deniedDomains     = []
  allowLocalBinding = true                        // harness child procs may bind 127.0.0.1

filesystem:
  allowWrite        = [layout.root, layout.tmp]   // {HARNESS_WORK_DIR}/sessions/{sid}/ + tmp/
  denyRead          = ['/', ...extraDenyReadPaths]
  allowRead         = [layout.root, layout.tmp, ...minimalSystemPaths, ...extraReadPaths]
  denyWrite         = [
    '/tmp/claude', '/private/tmp/claude',
    '~/.npm/_logs', '~/.claude/debug'
  ]

extraReadPaths      default = []
extraDenyReadPaths  default = ['~/.ssh', '~/.aws', '~/.config/gcloud']
```

Operator-provided `allowedNetworkHosts` (from `main.ts`) for the local stack
is `['api.anthropic.com', host(AI_GATEWAY_URL), host(S3_ENDPOINT)]`. No
wildcards. Empty list = no network access.

SRT adds a few shared host write paths and a shared `TMPDIR` by default. The
runtime explicitly denies all of those paths, then resets `HOME`, `TMPDIR`,
`TMP`, and `TEMP` inside the sandbox shell to the canonical per-session root
and `tmp/` directory. `/tmp` is not otherwise auto-allowed, so one session
cannot exchange files with another through a shared temporary directory.

SRT reads default to the entire host unless a broad deny is present. The
explicit `denyRead=['/']` turns `allowRead` into a strict effective allowlist:
the session root, `/bin`, `/usr`, runtime libraries, and specific TLS/name
service files. `~` (the operator's home directory) is **never** auto-allowed,
so the agent cannot read dotfiles, browser profiles, or shell history.

## Path-traversal protection

The harness's `agent_toolset` validates `LocalSandboxFiles` paths with
`resolveUnderRoot()` and rejects traversal before performing I/O:

```ts
function resolveUnderRoot(root: string, p: string): string {
  const trimmed = p.startsWith('/') ? p.slice(1) : p;
  const resolved = resolvePath(root, trimmed);
  if (resolved !== root && !resolved.startsWith(root + pathSep)) {
    throw new Error(`path '${p}' escapes the sandbox work-dir`);
  }
  return resolved;
}
```

Each `read`/`write`/`list`/`delete` then runs in a separate SRT process with
`denyRead=['/']` and only the canonical session root re-opened. The helper
also applies the shared-write deny list, rejects symlink components, and uses
`O_NOFOLLOW` for leaf reads/writes. This kernel boundary remains authoritative
if a path component is swapped between the preflight check and the I/O syscall.

## Per-session lifecycle

1. **`acquire(env)`**. Allocate a session id, mkdir the per-session work-dir
   under `{HARNESS_WORK_DIR}/sessions/{sid}/` (with `tmp/` subdir), build the
   `SandboxManagerInitConfig`, ensure the global `SandboxManager` is
   initialized exactly once (`ensureInitialized` shares one promise across
   concurrent acquires), and return a `LocalSandboxHandle`.
2. **Resource materialization**. The dispatcher runs `materializeResources()`
   which calls into the existing strategies (`TarballPrefetchStrategy`,
   `LocalMemoryStrategy`, `GitCloneStrategy`) — all of which write through
   `SandboxHandle.files`. With the work-dir as the file root, this lands the
   resource bytes on the host FS at the agreed `mount_path`.
3. **`run(call)`**. Each `bash` / `glob` / `grep` is wrapped via
   `manager.wrapWithSandbox(command, binShell, perSessionConfig)`. The
   wrapped string contains the OS-level sandbox invocation, so violations
   are caught by the kernel — not by the runtime. The third arg is critical:
   `SandboxManager.initialize` is global+idempotent, so without per-session
   `customConfig`, every session would inherit session-1's writable
   work-dir.
4. **Timeout enforcement**. `runWrapped` honors `timeout_ms` by SIGKILL-ing
   the child and resolving with `exit_code: 124` (matches GNU `timeout`'s
   convention) plus a stderr note explaining what happened. Without the
   signal-aware branch, a killed process would show `exit_code: 0` and the
   model would think it succeeded.
5. **`runPrivileged()` throws**. The local runtime has no privilege
   boundary — `srt` blocks `sudo`, FUSE strategies are never selected
   against this runtime (the strategy factory enforces that), and any
   attempt to escalate is a programmer error. Throwing here is the second
   line of defense.
6. **`destroy()`**. `rm -rf` the per-session work-dir. `pause` and `resume`
   are no-ops (host FS persists across calls naturally).

## Limitations

### Claude Agent SDK 0.2.x tool-strip gap

When an in-process MCP server is registered, the SDK still exposes its
built-in `Bash`, `Read`, `Write`, `Edit` tools to the model. The harness
defends against this by setting `Options.cwd = layout.root`, so even if the
model picks the built-in `Bash` rather than `mcp__orca__bash`, the resulting
child process is launched with the sandbox work-dir as cwd. Combined with
the kernel-enforced `allowWrite`/`allowRead` lists, this means the agent
can't escape even when it bypasses the in-process tool — it just gets a less
useful error stream.

The proper fix is to tighten the SDK to expose only `mcp__orca__*` tools
when an in-process server is registered. Tracked in
[`docs/managed-agents/local-stack.md`](../../../../../docs/managed-agents/local-stack.md)
"Known limitations".

### No FUSE

`capabilities.supportsFuse = false`. Files always use `tarball_prefetch`;
the strategy factory falls back to `local_memory` for memory stores. If your
test specifically needs the FUSE path (output or memory mounts), run with
`SANDBOX_RUNTIME=e2b`
against the `orca-default` E2B template (which ships `s3fs-fuse` +
`fuse3`) — see [`packages/e2e-tests`](../../../../../packages/e2e-tests/) and
the nightly E2B workflow.

### No Windows host

`srt` doesn't ship a Windows backend. Use WSL2 (Linux row of the prereq
table) or `SANDBOX_RUNTIME=in-memory` for Windows-only contributors.
