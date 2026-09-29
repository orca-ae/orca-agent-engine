# Orca Environment image

The sandbox image for **`target=cloud` `colocated`** sessions. It bakes two
Node 22 binaries at fixed paths:

- **`environment-worker`** at `/opt/orca/environment-worker/dist/main.js` — dials
  the registry worker tunnel, then spawns a `session-runner` per session.
- **`session-runner`** at `/opt/orca/session-runner/dist/main.js` — the
  `colocated` agent loop (`claude` / `claude-code` / `codex-sdk` / `mock` / … providers).

It is a **toolbox image, not a listening service**: it boots and idles. Nothing
inside it starts the worker. Whatever provisions the box execs the worker into it
over the provider's exec channel, with the worker's environment injected:

```
node /opt/orca/environment-worker/dist/main.js
```

The worker then reads `RUNNER_LAUNCH_COMMAND` and spawns
`node /opt/orca/session-runner/dist/main.js` per session, inside the same box.
The variables it expects are
[`services/environment-worker/AGENTS.md`](../environment-worker/AGENTS.md).

## Why this exists

A caller that provisions a cloud sandbox and then only _execs_ into it installs
nothing, so the box has to arrive with both binaries already present. Baking them
is this image's entire job, and the two paths above are its contract. The
**self-hosted** path never needed it: there the operator runs the already-built
binaries from their own checkout directly.

This repository builds the image and the binaries it bakes. The caller that
provisions a box and execs into it lives outside it, and this README describes
only the image's side of that boundary.

## Build

Build context is the **repo root** (the image compiles the two binaries + their
workspace deps from source, like the other service images):

```bash
docker build -f services/environment-image/Dockerfile -t ghcr.io/orca-ae/orca-environment:<tag> .
```

Multi-stage: stage 1 does `pnpm install` + builds `@orca/guardrails`, `@orca/harness-tunnel`,
`@orca/sandbox-runtime`, `@orca/transcript-store-types`, then
`environment-worker` + `session-runner`, then `pnpm deploy --prod` into
self-contained dirs. The runtime uses pinned Node 22 Trixie, Git, SRT from the workspace lockfile, and
the same pinned Bubblewrap build and gVisor compatibility wrapper as the sandbox-harness
image. These executables support isolated model-tool filesystem, PID and network namespaces.
The two deploy directories contain the worker and runner.

Smoke-test locally (both binaries load without the entrypoint guard misfiring):

```bash
docker run --rm ghcr.io/orca-ae/orca-environment:<tag> \
  node -e "require('node:child_process').execFileSync('node',['/opt/orca/session-runner/dist/main.js','--help'],{stdio:'inherit'})" || true
```

Publish:

```bash
docker push ghcr.io/orca-ae/orca-environment:<tag>
```

## Wire it into the registry

These are the settings the provisioning caller reads. They are its contract, not
this image's — the image only guarantees the paths in **Baked contract** below —
so they are recorded here because an operator wiring the two together needs both
halves in one place.

```bash
ORCA_ENVIRONMENT_LAUNCHER_BACKEND=e2b            # or: opensandbox
ORCA_REGISTRY_TUNNEL_URL=wss://<registry-host>   # the worker dials this back
```

### E2B

`e2b.Dockerfile` is a thin `FROM` the pushed image (E2B's `template build` uses
this directory as context and cannot see the monorepo, so the heavy build
happens once above). Build the template:

```bash
cd services/environment-image
e2b auth login
e2b template build            # -> prints a template id
```

Then on the registry:

```bash
E2B_API_KEY=<key>
E2B_ENVIRONMENT_TEMPLATE_ID=<the template id from `e2b template build`>
E2B_ENVIRONMENT_WORKER_COMMAND="node /opt/orca/environment-worker/dist/main.js"
E2B_ENVIRONMENT_RUNNER_COMMAND="node /opt/orca/session-runner/dist/main.js"
# E2B_ENVIRONMENT_BASE_URL=...        # optional (self-hosted E2B)
# E2B_ENVIRONMENT_WORKSPACE_DIR=...   # optional, defaults to /home/user/orca-environment
```

> `E2B_ENVIRONMENT_TEMPLATE_ID` is deliberately distinct from harness-server's
> `E2B_TEMPLATE_ID` (which selects harness-server's own `orca-default` FUSE
> tool-exec sandbox for `separate` mode) — the two use cases need different
> images.

### OpenSandbox

```bash
OPEN_SANDBOX_DOMAIN=<host[:port]>
OPEN_SANDBOX_ENVIRONMENT_IMAGE=ghcr.io/orca-ae/orca-environment:<tag>
OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND="node /opt/orca/environment-worker/dist/main.js"
OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND="node /opt/orca/session-runner/dist/main.js"
# OPEN_SANDBOX_PROTOCOL=https          # optional, defaults to http
# OPEN_SANDBOX_API_KEY=...             # optional
```

## Baked contract

| Path                                        | What                                                                                          |
| ------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `/opt/orca/environment-worker/dist/main.js` | worker entry (+ sibling `worktree-thread.js`, `node_modules`)                                 |
| `/opt/orca/session-runner/dist/main.js`     | runner entry (+ `harness/*/bridge-entry.js`, `harness/pi/orca-extension.mjs`, `node_modules`) |
| `/home/user/orca-environment`               | workspace dir; the default a caller uses unless it overrides the workspace setting            |

The caller injects the worker's dial-back environment (`ORCA_ENVIRONMENT_TOKEN`,
`REGISTRY_TUNNEL_BASE_URL`, `ENVIRONMENT_ID`, identity, workspace, runner
command) at exec time; every one of those is defined in
[`services/environment-worker/AGENTS.md`](../environment-worker/AGENTS.md). The
image bakes only the binaries and the paths above.

## Extending (follow-ups, not needed for `mock` / `claude` / `claude-code`)

- **Native-CLI providers** (`codex` / `cursor` / `pi`): `session-runner` execs
  those real CLIs from `PATH`. Add them in a derived image / a later Dockerfile
  layer if the deployment uses those harnesses.
- **S3-FUSE** (`s3fs`): only needed when a session mounts memory/outputs inside
  the cloud box. Add `s3fs` + the FUSE capability the way harness-server's
  `orca-default` E2B template does. Not required for a `mock`-provider turn.

## Verify end-to-end

Once the template/image is built + the registry is configured with a cloud
backend, a `target=cloud` `colocated` session should round-trip exactly like the
Local e2e (`registry-service-ts/test/integration/cloud-local-launch-e2e.spec.ts`)
— provision → mint token → exec worker → worker dials in → distributor dispatch
→ `session-runner` runs → turn to SSE.

The `codex-sdk` provider includes `@openai/codex-sdk` 0.154.0 and the matching
platform executable in production dependencies, plus
`/opt/orca/session-runner/dist/harness/codex-sdk/worker-entry.js`. No separate
Codex CLI installation is required for this provider. Gateway credentials and
model authorization are configured as described in the
[session-runner documentation](../../docs/managed-agents/services/session-runner.md#codex-sdk).
