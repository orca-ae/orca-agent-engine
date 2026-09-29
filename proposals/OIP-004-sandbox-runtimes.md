# OIP-004: Sandbox runtimes behind one seam

- *Author(s)*: @jiangpengcheng, @tuteng, @freeznet
- *Status*: Released
- *Proposal time*: 2026-05-03
- *Components*: `packages/sandbox-runtime`, `packages/cloud-sandbox`, harness-server,
  `charts/opensandbox-patches`, the engine Helm chart
- *Discussion*: None (predates the public repository)
- *Implementation*: `packages/sandbox-runtime/src/`, `packages/cloud-sandbox/src/`; harness-server
  `src/sandbox/` (`sandbox-runtime.ts`, `*/runtime.ts`, `mounts/strategy-factory.ts`,
  `environment-trust.ts`), `src/main.ts`, `src/config.ts`, `sandbox-templates/`;
  `charts/opensandbox-patches/files/batchsandbox_provider.py`
- *Released in*: v0.5.0

## TL;DR

Agent tools run model-written code, and the places the engine runs offer different isolation
backends: a developer's host, E2B's hosted microVMs, OpenSandbox on Kubernetes under gVisor, and
AgentENV's Firecracker VMs. One `SandboxRuntime` / `SandboxHandle` seam hides them: adapters
advertise capabilities, harness-server selects one per process from a required `SANDBOX_RUNTIME`,
and setup fails closed when a runtime cannot prove what a session needs. This affects operators who
choose, image and harden a runtime, and runtime authors.

## Background

A session's durable state is its transcript; the sandbox holds only what setup puts there and what
the agent writes. [OIP-002](OIP-002-agent-harnesses-and-execution-modes.md) decides where the loop
runs (`separate` dispatches tool calls from harness-server into the sandbox, `colocated` boots the
harness image as the sandbox); either way harness-server acquires one sandbox per runner and
destroys it on idle. Owning documents: the
[runtime table](../docs/managed-agents/services/harness-server.md#sandbox-runtimes),
[`sandbox-runtime.md`](../docs/managed-agents/libraries/sandbox-runtime.md),
[`cloud-sandbox.md`](../docs/managed-agents/libraries/cloud-sandbox.md) and the
[OpenSandbox guide](../docs/opensandbox/README.md).

## Motivation

The first design named one provider, E2B, with an in-memory double for tests, chosen by whether an
E2B key was present. Self-hosted Kubernetes deployments cannot depend on a hosted service, local
development needs isolation without a cloud account, and some operators already run Firecracker.
The backends differ exactly where the session contract depends on them:

- **Privileged setup.** Memory stores and output capture mount S3 through FUSE, which needs
  `/dev/fuse` and a root step the agent never reaches: E2B offers sudo, OpenSandbox a root exec
  daemon, a host sandbox nothing.
- **Lifecycle and images.** E2B and AgentENV pause, OpenSandbox's snapshot restore loses live
  mounts; E2B boots templates by id, OpenSandbox and AgentENV boot OCI images.

A session must get the same files, memory, outputs and write boundary everywhere, or fail; with the
key-based choice, a missing key silently ran agent code on the service host without isolation.

## Goals

### In scope

- One interface to acquire, drive and destroy a sandbox; capabilities as data, optional methods as
  feature detection, and no caller branching on a provider name.
- Fail closed at runtime selection, acquisition, filesystem-root preflight and the write policy.
- Five runtimes (`local`, `in-memory`, `e2b`, `opensandbox`, `agentenv`), operator-owned images,
  and a server-side trust policy for the OpenSandbox images given extra privilege.

### Out of scope

- Topologies and the in-sandbox bridge ([OIP-002](OIP-002-agent-harnesses-and-execution-modes.md));
  session-runner's sandbox and Registry-launched Environments ([OIP-011](OIP-011-self-hosted-session-runner.md));
  egress ([OIP-003](OIP-003-egress-boundary.md)); installing gVisor, OpenSandbox or AgentENV.
- Listed on [`roadmap.md`](../docs/managed-agents/roadmap.md#deferred-by-design): warm pause, S3-FUSE
  for files, `mountpoint-s3`, and, as [known limitations](../docs/managed-agents/roadmap.md#known-limitations),
  custom `colocated` images (they need a sealed setup namespace) and cloud `endpoint()` coverage.

## Design

### High-level design

```text
 SANDBOX_RUNTIME ─► loadConfig (required) ─► main.ts ─► one SandboxRuntime per process
 Dispatcher.spawnRunner      capabilities: supportsFuse, supportsLocalMemory, supportsWritePolicy
   spec + trust check ─► acquire ─► adapter creates, waits, probes the image (else destroys)
   prepareFilesystemRoots ─► output root ─► output mount (s3fs via runPrivileged | local dir)
   pickStrategy per resource ─► Skills ─► seal the write policy ─► agent handle to the harness
     local: srt on host │ in-memory: tmpdir │ e2b: SDK │ opensandbox: REST + execd │ agentenv: envd
```

### Detailed design

#### The seam

`SandboxRuntime` (harness-server `src/sandbox/sandbox-runtime.ts`) is a static `capabilities`
descriptor plus `acquire(EnvironmentSpec)`, which returns a caller-owned `SandboxHandle`: `run` for
`bash`, `glob` and `grep`; `files` (`write`, `read`, a bounded `readUtf8Page` that fails closed
without trusted read roots, `list`, `chmod`, optional batched `chmodMany`, `delete`); `pause`,
`resume` and an idempotent `destroy`; and `runPrivileged` for trusted mount setup. Optional members
are `endpoint(port)` for the `colocated` bridge, `prepareFilesystemRoots` (planned roots are
canonical and hold no pre-existing mount) and the write-policy trio `prepareWritePolicy`,
`runWithWritePolicy` and `canonicalizePathForPolicy`. `EnvironmentSpec` carries `packages`,
`networking`, `target`, the `colocated` `image`, `entrypoint` and `exposePorts`, `harnessEnv`, and
`fileUploadOwnership` (`ubuntu` for `separate`, `node` for `colocated`, both uid/gid 1000).

Two copies exist. harness-server's in-tree interface backs its five adapters and requires
`runPrivileged`, which runtimes without a privilege boundary implement by throwing. The package
copies make it optional and add `spawn`, a validated-policy brand, one tool-argument parser and
`requiresFuse`; Registry's Environment launchers and session-runner use them (OIP-011), while
harness-server takes only the Skills catalog formatter from the package.

#### Capabilities and fail-closed setup

Three flags drive every runtime-dependent choice (`strategy-factory.ts`). `supportsFuse` selects
`MemoryFuseStrategy` and an s3fs output mount, which needs per-session S3 credentials
(`dispatcher.ts:4452-4463`); an explicit `memory_fuse` on a non-FUSE runtime throws.
`supportsLocalMemory` is the only route to the Files-API `LocalMemoryStrategy`, so a runtime
advertising neither fails the resource rather than weaken persistence. `supportsWritePolicy` gates
`separate` (`dispatcher.ts:4919-4925`). Files always use `TarballPrefetchStrategy`.

Setup runs in a fixed order (`dispatcher.ts:4318-4959`): build the spec and refuse untrusted
Environment inputs, `acquire`, `prepareFilesystemRoots` (a runtime without it fails setup), create
the output root, mount outputs, materialize files, memory stores, repositories and Skills, then seal
the write policy. For `separate`, `createPolicyEnforcedSandbox` runs `prepareWritePolicy` (probes for
ranged reads, filesystem aliases and a real bubblewrap launch) and returns the only handle the agent
sees: its `run` goes through `runWithWritePolicy` and its `runPrivileged` always throws. For
`colocated` the dispatcher runs the alias and bubblewrap probes inside the image, then writes the
ready marker its entrypoint waits for. Any failure destroys the sandbox and appends `session.error`
(`setup_failed`, with the phase) and `session.status_idle` (`retries_exhausted`); see
[fail-closed behavior](../docs/managed-agents/output-write-policy.md#runtime-capability-and-fail-closed-behavior).

#### Runtimes

| `SANDBOX_RUNTIME` | Adapter | Isolation | FUSE | Privileged setup | Pause, resume |
| --- | --- | --- | --- | --- | --- |
| `local` | `LocalSandboxRuntime` | host processes under `srt` (bubblewrap on Linux, `sandbox-exec` on macOS) | no | none | no-op |
| `in-memory` | `InMemorySandboxRuntime` | none: a host temporary directory, for tests | no | none | no-op |
| `e2b` | `E2BSandboxRuntime` | E2B microVM from an operator-built template | yes | sudo to a constrained helper | SDK calls |
| `opensandbox` | `OpenSandboxRuntime` | gVisor Pod per sandbox | always | root in the exec daemon | refused |
| `agentenv` | `AgentEnvRuntime` | Firecracker VM from an OCI image | no; Files API | root in envd | pause, reconnect |

**`local`** wraps every command in `srt`'s `SandboxManager`, passing the session's profile on each
call because the manager's initialization is process-global: reads denied from `/` except the
session work-dir under `HARNESS_WORK_DIR` and minimal system paths, writes limited to that work-dir,
network limited to `api.anthropic.com` and the hosts of `AI_GATEWAY_URL` and `S3_ENDPOINT`
(`main.ts:269-293`). Boot fails unless `srt --version` succeeds. **`in-memory`** runs `bash -c` in a
host temporary directory; it keeps the direct file checks, but policy-enforced Bash returns exit 126
rather than pretending that parsing shell text is a boundary.

**`e2b`** loads `@e2b/code-interpreter` at the first acquisition and creates the sandbox from
`env.image` or `E2B_TEMPLATE_ID` through the SDK's positional template form (its options form drops
a template silently), with `harnessEnv` as its environment, then probes the `orca-default` contract
(`buildE2BPrerequisiteProbeCommand`): UID/GID 1000; `bwrap`, `s3fs`, `fusermount3`, `realpath`,
`setpriv` and `/dev/fuse`; no blanket or `SETENV` sudo rule and no root shell; a mount helper that
refuses bad options and paths. `runPrivileged` runs `sudo` keeping only three `ORCA_S3_*` variables.

**`opensandbox`** speaks OpenSandbox's REST API rather than its SDK: lifecycle under `/v1/sandboxes`
with the `OPEN-SANDBOX-API-KEY` header, commands and files through the sandbox's exec daemon, via the
server when `OPEN_SANDBOX_USE_SERVER_PROXY` is true. Acquisition sends the image and entrypoint (per
acquisition for `colocated`, else `OPEN_SANDBOX_IMAGE` and `OPEN_SANDBOX_ENTRYPOINT`, falling back
to `tail -f /dev/null`), limits, lifetime, `harnessEnv`, exposed ports and the extensions
`bootstrap.execd.isolation=enable` and `orca.fuse.device=enable`. It refuses packages, waits up to
60 s for the daemon, then probes root, `s3fs`, `fusermount3`, an openable `/dev/fuse` and a tmpfs
mount. `runPrivileged` runs as the daemon's root; agent commands drop to UID/GID 1000 with `setpriv`
before bubblewrap. The package copy requests FUSE per acquisition, and Registry's boxes request none.

The server side, `charts/opensandbox-patches`, overlays three patched provider modules on OpenSandbox
`server/v0.2.2`. The provider requires `secure_runtime` `gvisor`, exec isolation plus either FUSE or
an exact `ORCA_TRUSTED_NON_FUSE_WORKLOADS` entry, and a trusted image; its Pods add only `SETFCAP`
and `SYS_ADMIN` and carry no privileged container, `hostUsers`, `procMount`, `hostPath` or host
`/dev/fuse`, because gVisor implements the FUSE device in its own kernel. An admission policy
enforces the Pod shape ([contract](../docs/opensandbox/README.md#provider-security-contract)).

**`agentenv`** (harness-server only) creates secure cold sandboxes (`POST /sandboxes-cold`) through
the AgentENV gateway with `X-API-Key`, deletes one that returns no envd access token, and keeps the
token in the adapter. Commands run through envd's ConnectRPC `process.Process/Start` and files
through its Files API, routed by `x-agentenv-sandbox-id` and `x-agentenv-target-port`. It refuses
packages, a per-acquire entrypoint and exposed ports, so it serves `separate` only, and its guest
contract has no validated FUSE device, so memory and outputs use the Files-API paths.

#### Selecting a runtime

harness-server reads `SANDBOX_RUNTIME` once; `loadConfig` throws when it is missing or unknown
(`config.ts:237-263`), and `main.ts` checks preconditions before serving (`E2B_API_KEY`;
`OPEN_SANDBOX_DOMAIN` and `_IMAGE`; `AGENTENV_BASE_URL`, `_API_KEY` and `_IMAGE`; `srt` on `PATH`;
`main.ts:161-267`). `in-memory` is honored and means no isolation. One runtime serves every session
and is logged at startup; no API field, Environment or session chooses it, only `main.ts` branches
on the kind, and the chart default `opensandbox` spares a self-hosted install an E2B account.

#### Images and trust

Operators build the `separate` images from `services/harness-server/sandbox-templates/`:
`orca-default/` (the E2B template), `orca-opensandbox/` and `orca-agentenv/`. All carry bubblewrap;
the E2B and AgentENV images add the Git credential helper, and the FUSE images add `s3fs`, `fuse3`
and the `orca-s3fs-mount` helper, which mounts only at the output root or one memory-store directory.
`colocated` boots the catalog harness image or `SANDBOX_HARNESS_CLAUDE_CODE_IMAGE`. An Environment
`image` for `colocated` and package installers for any managed sandbox are refused before
acquisition (`environment-trust.ts`): both run code before the write policy seals resource and
Skill roots, racing trusted setup from outside the agent's namespace.

The patched OpenSandbox provider (`batchsandbox_provider.py`) grants isolation only to a trusted
image with its exact entrypoint array. By default it trusts `ghcr.io/orca-ae/orca-opensandbox-code-interpreter`
with `["/opt/code-interpreter/code-interpreter.sh"]` and `ghcr.io/orca-ae/sandbox-harness-claude-code`
with `["/usr/local/bin/orca-sandbox-harness"]`. `ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES` (a JSON
array of registry/namespace prefixes) trusts those two names under each prefix instead;
`ORCA_TRUSTED_SANDBOX_REPOSITORIES` (a JSON array of `{repository, entrypoint}`) replaces them, and
`[]` disables either. Setting both, or invalid JSON, fails server initialization. Additive
`ORCA_TRUSTED_SANDBOX_WORKLOADS` entries match an exact image string and need `@sha256:` unless
`ORCA_ALLOW_TAGGED_SANDBOX_IMAGES=true`. Images need an explicit registry and a tag, digest or both;
prefixes expand to exact repositories. Tags are mutable and no signature is verified
([trust configuration](../docs/opensandbox/README.md#repository-and-exact-image-trust)). The
adapter's fallback entrypoint is untrusted, so `OPEN_SANDBOX_ENTRYPOINT` names the image's own.

## Changes by component

- **Libraries**: `@orca/sandbox-runtime` (interface, in-memory and Local runtimes, write policy,
  bounded reads, Skill and transfer validation); `@orca/cloud-sandbox` (E2B, OpenSandbox).
- **harness-server**: the in-tree seam, five adapters, selection, setup order and templates;
  **registry-service-ts**: Environment launchers over `@orca/cloud-sandbox`.
- **Helm charts**: the engine chart renders runtime settings and provider keys;
  `charts/opensandbox-patches` carries the provider overlay, admission policy and namespace.

## Public-facing changes

### API

None: the runtime is deployment configuration. Environment `packages` and `image` are accepted as
before; a session whose managed sandbox would need them fails setup.

### Events and streaming

None added; setup failures use `session.error` and `session.status_idle` as above.

### Wire protocols

Outbound only: the E2B SDK, OpenSandbox's lifecycle and exec-daemon routes with its two request
extensions, and AgentENV's REST API with envd's process and Files routes.

### Storage

None.

### Configuration

| Setting (harness-server) | Default | Effect |
| --- | --- | --- |
| `SANDBOX_RUNTIME` | required | `local`, `e2b`, `opensandbox`, `agentenv` or `in-memory` |
| `HARNESS_WORK_DIR` | a host directory | root of `local` work-dirs and host-side clones |
| `E2B_API_KEY`, `E2B_TEMPLATE_ID` | unset | E2B key (required for `e2b`) and template |
| `OPEN_SANDBOX_DOMAIN`, `_PROTOCOL`, `_API_KEY` | unset, `http`, unset | server address and key |
| `OPEN_SANDBOX_IMAGE`, `_ENTRYPOINT` | unset | `separate` image; comma-separated entrypoint |
| `OPEN_SANDBOX_TIMEOUT_SECONDS`, `_REQUEST_TIMEOUT_SECONDS` | `1800`, `30` | lifetime; request timeout |
| `OPEN_SANDBOX_USE_SERVER_PROXY`, `_RESOURCE_CPU`, `_RESOURCE_MEMORY` | `true`, `1`, `2Gi` | exec routing; Pod limits |
| `AGENTENV_BASE_URL`, `_API_KEY`, `_IMAGE` | unset | gateway, key and image, required for `agentenv` |
| `AGENTENV_TIMEOUT_SECONDS`, `_REQUEST_TIMEOUT_SECONDS` | `1800`, `180` | lifetime and resume TTL; request timeout |
| `AGENTENV_CPU_COUNT`, `_MEMORY_MB`, `_DISK_SIZE_MB` | unset | VM sizing |
| `SANDBOX_HARNESS_CLAUDE_CODE_IMAGE` | catalog default | `colocated` harness image |

The OpenSandbox server reads the trust variables above. In Helm, `harness.sandboxRuntime`,
`harness.openSandbox.*`, `harness.agentEnv.*`, `harness.e2b.templateId` and `harness.workDir` mirror
the variables, `secrets.values` holds the provider keys, and a malformed
`images.sandboxHarness.claudeCode` digest fails rendering ([runtime defaults](../docs/managed-agents/kubernetes.md#runtime-defaults));
the patches chart has `dataplaneNamespace.create` and `admission.enabled`, both `true`.

### Metrics, logs and traces

`harness_sandbox_write_policy_setup_total{runtime,result}`, `harness_sandbox_write_denied_total{runtime,kind}`
(`runtime` derives from the adapter class name) and `harness_fuse_mount_total{strategy,result}`;
harness-server logs the selected runtime at startup.

## Compatibility

### Upgrade

v0.5.0 is the first public release with this design and adds no migration; harness-server does not
start without `SANDBOX_RUNTIME`. The chart default needs OpenSandbox `server/v0.2.2` with the patches,
a `gvisor` RuntimeClass ([`gvisor-setup.md`](../docs/opensandbox/gvisor-setup.md)) and trusted
images; E2B needs an operator-built `orca-default` template, and the probes refuse stale images.

### Rollback

There is no earlier public release to return to. The runtime is process configuration with no
persisted state; idle sessions reacquire from whichever runtime a restarted replica selects.

### Version skew

harness-server always sends both OpenSandbox extensions; a server that rejects or ignores them fails
acquisition or the FUSE and write-policy probes, never weakening isolation. The patches chart
follows the release tag with `appVersion` `0.2.2`.

## Security considerations

- **Isolation classes.** `in-memory` has none; `local` relies on the host's `srt` profile; E2B and
  AgentENV use microVMs; OpenSandbox uses gVisor Pods under an admission policy. Inside remote
  sandboxes the [write policy](../docs/managed-agents/output-write-policy.md#bash-and-subprocesses)
  is a second boundary for agent processes.
- **Privilege is for setup only.** `runPrivileged` mounts S3 through the image's constrained helper
  from trusted harness code and throws on the agent's handle; the E2B probe refuses a template with
  a blanket or `SETENV` sudo rule or a sudo root shell.
- **Credentials.** Provider keys come from harness-server's environment and never enter a sandbox's
  environment. [Mounts](../docs/managed-agents/mount-strategies.md#memory-store-mounts) pass
  session-scoped S3 credentials transiently to the helper, which writes a root-only profile, starts
  s3fs under a scrubbed environment and deletes the profile.
- **Trusted images.** Only trusted OpenSandbox pairs receive `SETFCAP` and `SYS_ADMIN`; trust is
  operator policy, the OpenSandbox API stays infrastructure-only, and operators who need content
  identity pin digests or use exact-image trust.
- **Setup races.** Custom `colocated` images and package installers are refused, and
  `prepareFilesystemRoots` rejects symlink aliases and pre-existing mounts before any trusted write.

## Testing

- **Unit** (the required `test` job): both packages' suites; harness-server `sandbox-capabilities`,
  `strategy-factory`, `write-policy`, `sandbox-environment-trust`, `environment-spec`,
  `agentenv-sandbox-runtime` and `sandbox-harness-env`; Registry's launcher specs; `render.test.mjs`.
- **Integration**: `local-sandbox-runtime.spec.ts` against real `srt` and bubblewrap, and
  `sandbox-snapshot.spec.ts`; the live `opensandbox-sandbox`, `e2b-sandbox` and `agentenv-sandbox`
  specs self-skip without credentials, as do the E2B mount specs `nightly-e2b.yml` runs.
- **End to end**: the OpenSandbox legs of `e2e-stack.yml` install gVisor on kind, run
  `test_orca_patch.py` against the overlaid pinned server, round-trip S3 through both trusted images,
  then run `pnpm e2e:agent` and `pnpm e2e:agent:sandbox` for Claude, Pi SDK and Codex SDK.

## Alternatives

- **Release with a mode.** The first sketch had `release(handle, "checkpoint" | "destroy")` on the
  runtime; the handle owns `pause`, `resume` and `destroy` instead.
- **Selection by credential presence**, the first plan: a missing E2B key meant no isolation, so the
  runtime became required. **Branching on provider names** was rejected for capability flags.
- **FUSE for file resources.** It needed a grant over the workspace's blob prefix; files are copied
  host-side from exact ids, and a lazy reader is listed on roadmap.md.
- **OpenSandbox on runc.** A host `/dev/fuse` passes the device cgroup only for a privileged
  container, which runc cannot combine with a Pod user namespace, so the earlier overlay ran FUSE
  sessions privileged in the host user namespace; gVisor's in-kernel FUSE needs neither.
- **Kata Containers**, evaluated as a VM-isolated RuntimeClass with full syscall coverage: it needs
  KVM, usually nested virtualization, and FUSE in its guest stayed unproven. Only gVisor is accepted.
- **OpenSandbox pause** by committing the filesystem as an image: restore recreates the Pod without
  live mounts. **Package installs after readiness**, as OpenSandbox once did: they precede sealing.
- **Digest-only trust**, kept as the additive exact policy: repository trust with fixed entrypoints
  lets releases roll forward without an allowlist edit per digest, and prefixes cover mirrors.

## Status notes

- **Two copies of the seam.** harness-server runs its in-tree adapters; the package copies, which
  the library docs name canonical, serve Registry and session-runner. Only the packages validate
  tool arguments, return glob and grep faults instead of empty results, mark a severed OpenSandbox
  command stream, latch E2B and OpenSandbox `destroy()` after the provider confirms, fail loudly
  when the E2B SDK lacks a lifecycle method, retry a failed `srt` start, and let an OpenSandbox
  acquisition skip FUSE.
- **Unreached and partial paths.** Idle destroys the sandbox, so `pause` and the `MountStrategy`
  snapshot hooks run only in `sandbox-snapshot.spec.ts`. Only OpenSandbox implements `colocated`'s
  per-acquire image, entrypoint and port; the E2B adapter treats the image as a template id.
- **E2B lifetime.** The E2B adapter sets no sandbox timeout, so the SDK default applies (five minutes
  in the version the lockfile resolves), unlike the configured OpenSandbox and AgentENV lifetimes.
