# Sandbox Write Policy and Deliverable Outputs

> Implemented. Companion to
> [`output-capture.md`](./output-capture.md),
> [`mount-strategies.md`](./mount-strategies.md), and
> [`resource-mounting.md`](./resource-mounting.md).

## Decision

`/mnt/session/outputs/` is the sole directory from which a session publishes
downloadable `agent_output` Files. It is the formal deliverable directory and
is the only sandbox path published by `OutputIndexer` or the managed runner
resource checkpoint protocol.

The platform enforces a least-privilege write policy rather than relying on an
agent prompt:

| Sandbox path class                                              | Access     | Persistence / publication                                                                                                                               |
| --------------------------------------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/mnt/session/outputs/**`                                       | Read-write | Registered as session-scoped, downloadable `agent_output` Files.                                                                                        |
| A `memory_store` resource with `access=read_write`              | Read-write | Persisted through the memory-store version path; never published as output Files.                                                                       |
| A `github_repository` resource with `access=read_write`         | Read-write | Cloud execution uses `orca-git-creds`; self-hosted Codex preserves the local working tree with read-only proxy access. Never published as output Files. |
| Uploaded `file` resources and resources with `access=read_only` | Read-only  | No agent write is allowed.                                                                                                                              |
| Runtime-owned ephemeral scratch (`/tmp` and isolated home)      | Read-write | Never indexed or persisted; destroyed with the sandbox.                                                                                                 |
| Every other sandbox path                                        | Read-only  | Scratch/output writes fail.                                                                                                                             |

OS devices, the runtime-owned temporary directory, and provider-owned
ephemeral bookkeeping paths (for example Claude's private temp/debug
directory) are implementation exceptions, not session storage: they are never
indexed, survive no sandbox teardown, and must not overlap `/mnt`,
`/workspace`, or a resource mount. SDK user/project setting sources are
disabled so they cannot add writable paths.

The default access for `memory_store` and `github_repository` remains
`read_write`; callers can set `access=read_only`. This preserves the existing
MemoryStore write-back and Git workflows. Cloud Claude and Codex use the same
dispatcher resource mounts and output indexer in both modes. Self-hosted
Codex `colocated` sessions use Registry resource delivery and `session-runner`
to mount Files, Memory, and Git under a snapshot-verified binding revision.
Read-write Memory is persisted through checkpoints; read-write Git permits
local working-tree changes, while its Registry proxy grants read-only upstream
access. Managed Skills are read-only. New deliverable files belong under
`/mnt/session/outputs/`.

This follows the public Claude Managed Agents deliverable contract: users
retrieve session deliverables from `/mnt/session/outputs/` through the Files
API. The public contract does not require exposing arbitrary sandbox scratch
files, and Orca will not do so.

## Goals

- Make the output directory a reliable publication boundary, not an
  instruction that a model may ignore.
- Ensure no file outside that boundary appears in `files.list(scope_id=…)`.
- Preserve explicitly writable MemoryStore and Git repository resources in
  topologies that mount them.
- Apply the same policy to `separate` and `colocated` harness topologies.
- Fail closed when a selected sandbox runtime cannot enforce the policy.

## Non-goals

- Scanning the full sandbox or inferring which scratch file a user intended to
  download.
- Parsing shell commands to predict writes.
- Changing the Files API, output event shape, or MemoryStore/Git persistence
  contracts.
- Treating an agent's current working directory or a system prompt as a
  security boundary.

## Policy construction

`Dispatcher.spawnRunner` constructs the policy for separate execution and the
shared Claude/Codex sandbox-harness image. Self-hosted Codex colocated execution constructs it
in `RunnerResources` from the Registry-delivered resource manifest before
exposing a model-tool handle. The filesystem portion is:

```ts
interface WritablePath {
  path: string;
  kind: 'session_output' | 'memory_store' | 'github_repository';
}

interface SandboxWritePolicy {
  writablePaths: WritablePath[];
  readonlyPaths: string[];
}
```

`writablePaths` always contains `/mnt/session/outputs`. For cloud execution in
either mode it additionally contains the actual activated mount paths of only those
`memory_store` and `github_repository` resources whose pinned access is
`read_write`. A missing or unrecognized pinned access value fails closed to
`read_only`. Uploaded Files and read-only resources populate `readonlyPaths`.
The shared sandbox-harness image receives its resource and output policy in
`ORCA_SANDBOX_WRITE_POLICY`. Self-hosted Codex colocated adds the manifest's
read-write Memory and Git mount paths, keeps Files, read-only resources, and
Skills read-only, and validates enforcement before committing resources. No
path comes from model input.

The sandbox-harness process can also run as a standalone, unmanaged service,
where an absent `ORCA_SANDBOX_WRITE_POLICY` means no managed write policy.
Managed cloud colocated execution requires dispatcher policy injection and a
runtime probe. Self-hosted Codex colocated execution requires a matching committed
resource revision and a successful Linux SRT/Bubblewrap probe. Its SDK worker,
checkpoint staging, and native history remain outside the model-tool root.
For `limited` networking, tool access is restricted to the configured
Environment hosts and, when Git is attached, the Registry proxy host; upstream
Git hosts are not implicitly granted. Registry also checks that the upstream
repository host is explicitly allowed before minting or serving a Git proxy grant.
Omitted networking or `unrestricted`
preserves the Environment network while keeping the same filesystem and
process isolation. File helpers remain network-denied in both modes.

Paths are canonical absolute paths. A path is permitted only when it equals a
writable root or is a descendant of that root; prefix matching alone is not
sufficient (`/mnt/session/outputs-old` is not a child of
`/mnt/session/outputs`). The runtime must prevent `..` traversal and symlink
escapes before applying the policy.

Writable roots from different policy classes may not overlap. In particular, a
writable resource cannot be mounted at an ancestor of
`/mnt/session/outputs`, because that would silently broaden the deliverable
exception to unrelated paths. Writable/read-only root overlaps are rejected
for the same reason.

## Enforcement

### Direct file tools

The `write`, `edit`, and `delete` implementations validate their
target against `SandboxWritePolicy` before touching the filesystem. A rejected
tool call returns a normal tool error explaining the permitted output path; it
does not fail the whole session. This lets the agent retry at
`/mnt/session/outputs/<name>`.

`read`, `glob`, and `grep` retain read access to their existing permitted
inputs. They do not grant write access.

### Bash and subprocesses

Shell syntax is not a safe enforcement point: a command can create files via
redirects, subprocesses, interpreters, or background processes. Every agent
subprocess must therefore run inside a filesystem boundary that presents:

- the base image and read-only resources as read-only;
- the session output mount as read-write; and
- each declared read-write memory/repository mount as a narrowly scoped
  read-write exception; and
- a runtime-owned, non-indexed temporary directory exposed through `TMPDIR`.

For `LocalSandboxRuntime` this is an `@anthropic-ai/sandbox-runtime`
`bubblewrap`/sandbox-exec profile. E2B and OpenSandbox execute agent commands
through an explicit bubblewrap command with a read-only root and narrow bind
mounts for writable roots. Their images must contain `bubblewrap`; E2B's Orca
template installs it, and OpenSandbox uses the derived image under
`services/harness-server/sandbox-templates/orca-opensandbox/`.

Separated bubblewrap subprocesses receive a fresh tmpfs at `/tmp`, an isolated
home at `/tmp/home`, set `TMPDIR=/tmp`, and expose a minimal synthetic `/dev`
rather than bind-mounting the host device tree. They also unshare user and PID
namespaces, switch to UID/GID 1000, drop every capability, and set
`no_new_privs`, so provider-image sudo/setuid paths cannot recover outer mount
privileges. OpenSandbox drops execd's outer root identity to UID/GID 1000
before creating the nested user namespace. This keeps shell tools, uploaded
resources, and the colocated harness on the same filesystem owner; selecting
UID 1000 only inside bubblewrap would map it to execd's outer root instead.
Keeping the isolated home under
the newly mounted tmpfs avoids depending on a provider image to pre-create a
specific `/home/<user>` mount point after the root becomes read-only. The local
test runtime maps `TMPDIR` to its per-session `tmp/` directory and grants that
path explicitly in the sandbox profile. Its policy profile preserves
operator-configured read-only paths while narrowing only the writable
allow-list. These scratch paths are destroyed with the sandbox and are outside
the `OutputIndexer` scan root.

Every OpenSandbox acquire request also sets the provider-defined
`bootstrap.execd.isolation=enable` and `orca.fuse.device=enable` extensions.
OpenSandbox accepts them only with `secure_runtime.type=gvisor`,
`RuntimeClass/gvisor`, and operator-owned repository or exact-image trust with
an exact entrypoint array. gVisor
implements `/dev/fuse` inside sandbox kernel. Generated Pod adds only
`SETFCAP` and `SYS_ADMIN` for Bubblewrap and FUSE mounts and has no host device, `hostPath`,
privileged container, host namespace, `hostUsers`, or `procMount`. Companion
chart admission enforces same boundary. Agent commands enter nested Bubblewrap
as UID/GID 1000 with zero capabilities, `no_new_privs=1`, isolated PIDs, and
synthetic `/dev`.

"Trusted" is operator policy, not client assertion. Before honoring extensions,
the server matches repository + exact entrypoint against
`ORCA_TRUSTED_SANDBOX_REPOSITORIES`, or the exact image string + entrypoint
against additive legacy `ORCA_TRUSTED_SANDBOX_WORKLOADS`. Unset repository
policy defaults to the two official `ghcr.io/orca-ae` repositories with fixed
entrypoints, and `ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES` trusts the same
images under mirror prefixes; an explicit JSON array replaces the defaults and
`[]` disables repository trust.
Repository trust accepts valid explicit tags, `sha256` digests, or tag + digest,
not bare repositories. Legacy entries require digests unless the local/CI
`ORCA_ALLOW_TAGGED_SANDBOX_IMAGES=true` switch is set. Exact-image-only operators
must disable repository trust explicitly.

Repository trust relies on publisher ACLs and proxy integrity, not cryptographic
release verification: tags remain mutable and this policy installs no signing
infrastructure. See [OpenSandbox trust configuration](../opensandbox/README.md#repository-and-exact-image-trust)
for defaults and custom operator authorization. OpenSandbox API is not
tenant-facing, runtime package
installation rejects before agent isolation, every acquisition uses gVisor
FUSE, and pause/resume rejects before lifecycle or CR patch.

The sandbox-harness image also installs `bubblewrap` and `socat`, the Claude
Agent SDK's Linux sandbox dependencies. Its image entrypoint uses
the OpenSandbox workload's scoped `SYS_ADMIN` capability once to create an
outer namespace with a read-only root and writable output mount. Before Node or
the agent starts, `setpriv` switches to UID/GID 1000 and the namespace drops all
effective capabilities. This keeps the long-running harness non-root without
depending on setuid bubblewrap or unprivileged user namespaces at container
startup. The workload still requires the OpenSandbox isolation extension
described above. Its Claude Agent SDK provider sets `sandbox.enabled=true`,
`failIfUnavailable=true`, and `allowUnsandboxedCommands=false`, passes only the
policy's writable paths, and uses `canUseTool` to return a normal denial for
native `Write`, `Edit`, and `NotebookEdit` targets outside the policy. This
protects native `Bash` and file tools that do not pass through
`mcp__orca__*`.

The image routes Bubblewrap through an `orca-gvisor-bwrap` compatibility shim.
On ordinary kernels it execs the packaged Bubblewrap unchanged. On gVisor it
creates the nested user and network namespaces together with `unshare`,
preserving gVisor's already-usable loopback interface, then runs Bubblewrap for
PID, mount, filesystem, and SDK proxy isolation. It drops only unsupported
cgroup-namespace flags. The per-session network allowlist therefore remains
active on every runtime.

Session setup fails at write-policy probe when nested Bubblewrap is unavailable;
deployment must not disable SDK sandbox.

The platform sets the agent `cwd` to `/mnt/session/outputs/` as a convenience,
while `HOME` remains isolated, ephemeral scratch. Consequently, separated
`glob` and `grep` calls with a relative or omitted root resolve from the output
working directory; explicit absolute roots retain their existing behavior.
The platform also continues to inject the output instruction into the agent
prompt. None of these conveniences is considered enforcement.

### Runtime capability and fail-closed behavior

`SandboxRuntime.capabilities.supportsWritePolicy` describes enforcement by the
runtime adapter for the `separate` topology. A runtime that cannot enforce the
policy must not run an output-capture-enabled separate agent with write-capable
tools. Claude `colocated` enforcement belongs to the sandbox-harness image/provider,
so the dispatcher runs a bubblewrap probe inside that acquired image. Cloud
Codex shares that probe and additionally uses the policy-enforced tool handle.
Self-hosted Codex `colocated` uses the runner's policy-enforced local tool sandbox and probes
it before committing resources. A failed probe prevents agent execution; there
is no advisory or scan-all fallback.

The test-only InMemory runtime implements the same direct-path checks but
denies agent Bash entirely because it has no OS isolation boundary. It must not
pretend that parsing shell text is sufficient enforcement.

## Output lifecycle

For dispatcher-owned execution, the output-capture lifecycle is:

1. Before agent start, the dispatcher creates or mounts
   `/mnt/session/outputs/` and applies the write policy.
2. The agent writes final artifacts under that path.
3. After persisted tool results, `OutputIndexer` scans only that path. It
   registers newly created or changed files through the scoped
   `/internal/v1/workspaces/{workspace}/sessions/{session}/files` route and
   appends `session.output_indexed`.
4. On runner stop, a final scan runs before the output mount is deactivated.
5. Memory and repository writes follow their own watchers/credential-helper
   flows and are excluded from output indexing.

For self-hosted Codex colocated execution, the runner freezes changed output and
read-write Memory files after tool operations. Registry verifies and persists
those bytes before acknowledging the checkpoint and allowing SDK continuation.
Only output files become session-scoped Files and `session.output_indexed`
events; Memory writes and deletions use the Memory store, and Git working-tree
changes remain local to the live tool handle. Pending writes are acknowledged
before shared Memory is refreshed between turns. See
[`services/session-runner.md`](./services/session-runner.md#recovery-and-the-resume-cursor).

Every writable root must exist before the policy probe executes. Non-FUSE
output and memory mounts create their roots through a temporary Files API
marker and remove the marker before agent start. This works consistently for
both InMemory and OpenSandbox without relying on shell commands that may run
outside the intended sandbox filesystem.

For separated runners, the dispatcher retains the unrestricted sandbox handle
only for lifecycle and mount teardown. It passes a distinct policy-enforced
handle to the agent harness, ensuring the MCP file tools and Bash execution use
the restricted view instead of bypassing it through the lifecycle handle.

The existing 500 MB per-object cap, runner-generation deduplication, and
`session.output_indexed` readiness semantics are unchanged.

## Observability and errors

The implementation exposes:

- `harness_sandbox_write_denied_total{runtime,kind}` for rejected paths;
- `harness_sandbox_write_policy_setup_total{runtime,result}` for installation
  of the policy; and
- structured denial logs containing session id, runtime, operation, and
  requested path, but never file contents or credentials.

A policy denial is visible to the agent as a tool error. It is not a
`session.error`, and it does not produce `session.output_indexed`.

## Verification

Unit tests cover canonical path construction, prefix collisions, traversal,
symlink escapes, direct-tool denials, mandatory Claude SDK sandbox settings,
runtime capabilities, and InMemory's Bash denial. The paid real-agent suites
cover both `separate` and `colocated`: each first attempts a forbidden native
or Orca file write, retries under outputs, then verifies a Bash redirect is
denied outside the policy and succeeds relative to the output CWD. The E2E
suites additionally assert that forbidden filenames never appear in the
session Files list.

The broader persistence matrix remains:

- `write /mnt/session/outputs/poem.txt` creates a downloadable scoped File.
- Writes to `/home/user/poem.txt` and `/mnt/ai_coding_poem.txt` fail and
  create no File.
- A bash redirect outside the policy fails; a redirect under outputs succeeds.
- A read-write MemoryStore can write and produces a memory version but no
  output File; its read-only variant rejects writes.
- A read-write Git repository can modify its working tree; separate mode also
  supports pushes. Its files do not appear in the session Files list.
- Claude colocated native `Write`/`Bash` and Codex colocated Orca file/shell
  tools receive the same output/resource write restrictions.
- A runtime that lacks `supportsWritePolicy` fails session setup rather than
  silently weakening the boundary.

Cloud E2E remains environment-gated because it requires real agent service,
S3, and E2B/OpenSandbox credentials. A successful image build or successful
OpenSandbox pod creation is not evidence that bubblewrap can create namespaces:
session setup always executes a runtime probe, and CI must retain the
real-agent OpenSandbox job. The probe also catches servers that do not support
or honor `bootstrap.execd.isolation`.

## Compatibility

The API-visible behavior stays Anthropic-compatible: deliverables are
session-scoped Files, available after the output indexer has registered them.
This policy is stricter internally than a prompt-only convention, but does not
add Orca-specific fields or require clients to use a new upload/publish tool.
