# `@orca/sandbox-runtime`

`@orca/sandbox-runtime` is the sandbox abstraction `session-runner` and
Registry share: the `SandboxRuntime` / `SandboxHandle` interfaces plus the two
reusable, non-cloud implementations. It is a TypeScript library imported
in-process; cloud runtimes (E2B, OpenSandbox) live in
[`@orca/cloud-sandbox`](./cloud-sandbox.md) and implement the interface from
here. `harness-server` keeps its own copies of the runtimes (see
[Layout](#layout)); moving it onto this package is listed in
[`roadmap.md`](../roadmap.md#production-hardening).

## Contract

- `SandboxRuntime.acquire(env)` returns a caller-owned `SandboxHandle`:
  `run(ToolCall)` for buffered tool calls (`bash`/`glob`/`grep`, validated once
  by the shared `parseToolCall`), `spawn()` for long-lived processes with live
  stdio + an `exited` promise, `files` for host-side file ops (including the
  bounded, symlink-safe `readUtf8Page`), and lifecycle
  (`pause`/`resume`/`destroy`, destroy idempotent and retryable).
- Optional members are the feature-detection signal: `spawn`, `endpoint`,
  `runPrivileged`, and the write-policy trio are absent on runtimes that cannot
  support them. `hasWritePolicyEnforcement` narrows a handle to
  `WritePolicyCapableHandle` for `createPolicyEnforcedSandbox`.
- The package owns the **sandbox write policy** specified in
  [`../output-write-policy.md`](../output-write-policy.md):
  `buildSandboxWritePolicy` / `validateSandboxWritePolicy` are the only mints of
  the branded `ValidatedSandboxWritePolicy` (canonical roots, no overlaps,
  frozen), which every enforcement surface — the bubblewrap command builder,
  the alias/mount probes, the policy-enforced file wrapper — requires. A
  canonicalizer infrastructure failure propagates as such; only genuine
  containment violations become denials (and feed the `onDenied` audit).
- It also owns Skill-bundle materialization validation
  (`validateSkillBundle`, `SKILLS_ROOT`) and the shared `composeSkillsCatalog`
  formatter from [`../skills.md`](../skills.md). Catalog entries carry the pinned
  package digest so native history fingerprints also bind the Skill version.

## Implementations

- `InMemorySandboxRuntime` — tmpdir-backed, `child_process` on the host;
  tests + dev only (no isolation; policy-enforced Bash is refused outright).
- `LocalSandboxRuntime` — wraps commands with `@anthropic-ai/sandbox-runtime`
  (`srt`: sandbox-exec on macOS, bubblewrap on Linux) around per-session
  work-dirs; powers the local stack. Timeouts and kills signal the whole
  detached process group so wrapper chains cannot orphan the real command.

The Local runtime's opt-in `managedToolFilesystem` mode requires Linux. Its
policy shell maps the handle's private tool directory to `/` read-only, overlays
only declared writable resources and `/mnt/session/outputs`, and exposes a
small read-only set of system executables, libraries and TLS/DNS data. Bash,
glob, grep and file APIs use the same virtual resource paths. Runtime targets
are pre-created as real files/directories; policy roots reject symlink,
hard-link and mount aliases. Reserved runtime paths and Skills cannot become
writable resource roots. Each command receives fresh scratch, device and proc
mounts, isolated user/PID namespaces, and no capabilities.

Managed file helpers also require the successfully prepared policy. Their SRT
profile permits writes only to its writable resource roots; read/list helpers
permit none. The kernel keeps read-only File, Memory and Skill trees protected
even when a concurrent shell replaces an intermediate output directory with a
symlink after the helper's path checks. Trusted materialization precedes policy
preparation and writes directly into the raw private tree.

For a limited network policy, SRT supplies the outer filtered network namespace. Root read-denial masks use
canonical paths, so merged-`/usr` aliases such as `/bin` do not become symlink
mount destinations; ancestor masks retain the same default-deny boundary. The mapped shell retains
its HTTP/SOCKS proxy environment and reaches the loopback proxies started by
SRT; it does not expose their Unix sockets or create another network namespace.
`createManagedToolSandboxManager` gives each command a separate SRT process,
because SRT's proxy domain filter is process-global. Limited mode grants only
the validated model network allow-list, with local binding disabled. An explicit
`networkUnrestricted` policy retains the Environment network without the SRT
network namespace/filter; it cannot be applied to a runtime acquired with a
limited grant. File helpers always use a deny-all network policy, even in an
unrestricted Environment. Policy preparation runs the actual
SRT/seccomp/mapped-bubblewrap chain and checks root identity, read-only root
enforcement (including an attempted numeric chmod of an owned read-only file), isolated
PIDs, the selected network namespace behavior, and output writes;
an unavailable isolation chain fails closed. Filesystems that permit metadata changes
through a read-only bind mount are rejected before model tools are exposed. This includes
`tmpfs` under the pinned OpenSandbox gVisor release. Cloud Environments place managed
tool directories under their workspace on the container root filesystem by default. The caller keeps native worker
homes, history, checkpoints and transfer staging outside the tool directory.
SRT proxy sockets use a short private temporary directory outside that tree so
long worker paths cannot exceed the Unix socket path limit. The runtime removes
that directory after the command exits, including after a timeout.

The standalone Linux probe at `test/integration/managed-local-probe.mjs` exercises
real mapped reads/writes, kernel read-only enforcement, private worker-state
exclusion, independent SRT proxy grants and unrestricted networking. The read-only
input starts with mode 0444, so chmod tests attempt an actual permission change
instead of a no-op; the CI worker and model tools both run as UID 1000. Run it with Node after building the
package; `ORCA_SANDBOX_RUNTIME_ENTRY` selects a deployed package entry inside an
Environment image. `ORCA_MANAGED_PROBE_WORK_DIR` selects the workspace (default: current
directory); CI uses `/home/user/orca-environment`, matching the real Environment workspace.
`ORCA_EXPECT_TMPFS_REJECTION` set to `1` also verifies that the pinned gVisor's `/tmp` tmpfs
fails policy preparation. The unit suite uses a fake SRT manager to inspect command and
configuration generation; it does not establish Linux or gVisor isolation.

The managed Linux isolation probe checks that a numeric `chmod 0644` attempt
leaves a read-only File at mode `0444` in both the tool and worker views, and
that writes remain denied afterward. A command exit status alone is not treated
as evidence that protected state changed.

## Layout

```
packages/sandbox-runtime/
  src/
    sandbox-runtime.ts       # interfaces + parseToolCall + policy brand + capability guard
    write-policy.ts          # policy builder/validator, bubblewrap + probe commands, enforced wrapper
    read-page.ts             # bounded symlink-safe UTF-8 paging (host + remote-envelope forms)
    skills-materialize.ts    # Skill bundle validation for materializers
    chmod-many.ts            # batched permission application command
    kill-process-tree.ts     # process-group kill shared by the runtimes
    in-memory/runtime.ts     # InMemorySandboxRuntime
    local/runtime.ts         # LocalSandboxRuntime (srt-wrapped)
    local/materialize.ts     # per-session work-dir acquire/release
    index.ts                 # public re-exports
  test/unit/                 # incl. the read-page/write-policy/chmod-many safety net
```

`services/session-runner` and Registry import this package.
`services/harness-server` imports only `composeSkillsCatalog` from it; its own
sandbox runtimes and write-policy code live under `src/sandbox/**`.

### Managed resource transfer validation

`resource-transfer.ts` defines the credential-free manifest used for transferring
File, Memory, and Git trees. Its parser rejects noncanonical or overlapping
mounts, reserved output/Skill/system roots, relative-path escapes, duplicate
files, and privileged mode bits. File resources contain exactly one read-only
file. Transfers allow at most 100 mounts, 10,000 files, 512 MiB per file, and
2 GiB in total; each decoded chunk is at most 1 MiB. Manifest digests and strict
base64 decoding provide the shared integrity checks for producers and consumers.

The checkpoint parser applies the same byte and path limits to runner-produced
output and Memory changes. Checkpoints include immutable file identities and
previous content hashes for Memory writes/deletions. Output deletion is excluded:
removing a sandbox output does not delete an already registered File.
