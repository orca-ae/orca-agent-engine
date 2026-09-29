# orca-default sandbox template

## What this template is

A custom E2B sandbox template baked with `s3fs-fuse`, `fuse3`, and `bubblewrap`
plus a constrained NOPASSWD `orca-s3fs-mount` helper. The template also
normalizes the upstream Node binary into `/usr/local/bin/node` and proves it
works with `PATH=/usr/local/bin:/usr/bin`, which is the cleared environment
used by bounded `read`/`edit` operations and their setup probe. The upstream
`e2bdev/code-interpreter` image does not ship `s3fs`, so the harness's FUSE
paths (S3-direct output capture, `MemoryFuseStrategy` memory mounts) cannot
run on it. The template's `start_cmd` mounts the session output prefix
read-write at `/mnt/session/outputs` when `ORCA_FUSE_OUTPUTS_ENABLE=1`, and
is a no-op when it is not. File resources are never mounted from inside the
sandbox: the harness delivers them host-side via the tarball-prefetch
strategy (sandbox execution credentials carry no file-blob access).
Git's `credential.helper`, `credential.useHttpPath`, and `safe.directory`
settings are installed system-wide at image build time. The helper returns no
credentials when per-session `ORCA_GIT_CREDS_*` env is absent, so public repos
continue through anonymous Git access.
The image removes the upstream blanket `user NOPASSWD: ALL` rule. Sudo permits
only the mount helper and `umount`; command-scoped `env_keep` admits only the
three `ORCA_S3_*` credential transport variables.
Base `e2bdev/code-interpreter` manifest is digest-pinned; updating it requires
rebuilding template and repeating full spike checklist.

## Build & push

The E2B CLI (v2.10+) merged build + push into a single `template create` step;
the legacy `template build` / `template push` invocations no longer exist.

```bash
npm i -g @e2b/cli            # one-time
e2b auth login               # one-time, sets up the local profile
cd services/harness-server/sandbox-templates/orca-default
e2b template create orca-default \
  --dockerfile e2b.Dockerfile \
  --cmd "/usr/local/bin/orca-start.sh" \
  --ready-cmd true \
  --cpu-count 2 \
  --memory-mb 2048
# Capture the printed `Template created with ID: <id>` line: that id is
# the harness's E2B_TEMPLATE_ID (see below).
```

`--ready-cmd true` is correct for this template: `orca-start.sh` mounts FUSE
filesystems (or no-ops when the env flags are unset) and exits 0; the harness
then drives the agent via `commands.run` after the sandbox is up. The template
exposes no long-running readiness probe.

## Configure the harness

Template ids belong to the E2B account that built them, so this repository
pins none: build the template yourself (above) and set `E2B_TEMPLATE_ID=<id>`
from the `e2b template create` output in the harness `.env` (and production
secrets) so spawned sandboxes use this template. Omitting the variable falls back to the upstream
`e2bdev/code-interpreter` template. That fallback is not valid for
production agents. E2B acquisition now fails closed unless template runs as
UID/GID 1000, exposes `/dev/fuse`, contains FUSE/Bubblewrap helpers, permits
only constrained helper sudo, and denies arbitrary root shell. Dispatcher also
probes agent write boundary before use.

### Rebuilding after runtime contract changes

The git-mount work added `git`, `jq`, and `orca-git-creds`; the output write policy added
`bubblewrap`; progressive Skill disclosure added the hermetic Node prerequisite
for bounded reads and edits. After landing any of these changes, the
operator MUST rebuild the template and update `E2B_TEMPLATE_ID` in:

1. Production secrets (whatever store holds the harness-server's `E2B_TEMPLATE_ID`).
2. GitHub Actions secrets for `nightly-e2b.yml` (the gated CI workflow).

```bash
cd services/harness-server/sandbox-templates/orca-default
e2b template create orca-default \
  --dockerfile e2b.Dockerfile \
  --cmd "/usr/local/bin/orca-start.sh" \
  --ready-cmd true \
  --cpu-count 2 \
  --memory-mb 2048
# Note the printed template id; update everywhere E2B_TEMPLATE_ID is referenced.
```

## Spike checklist

The operator runs through this list once after the first successful build to
confirm the FUSE path is viable on E2B. If any check fails, do not point the
harness at the template.

- [ ] `e2b template create orca-default ...` succeeds (see "Build & push" above for the full v2-CLI command).
- [ ] `e2b sandbox spawn $TEMPLATE_ID` (or the programmatic `Sandbox.create(...)` from `services/harness-server/scripts/spike-orca-default.ts`) succeeds.
- [ ] Inside the sandbox: `/usr/bin/env -i PATH=/usr/local/bin:/usr/bin node --version` succeeds.
- [ ] Inside the sandbox: `which s3fs && s3fs --version` returns a version.
- [ ] Inside the sandbox as the normal `user`, `bwrap --ro-bind / / --bind /mnt/session/outputs /mnt/session/outputs --chdir /mnt/session/outputs -- true` exits 0.
- [ ] Run the programmatic spike (`E2B_TEMPLATE_ID=<id> pnpm -F @orca/harness-server exec tsx scripts/spike-orca-default.ts`) and confirm its FUSE, constrained-sudo, mount-root, and Git-helper checks pass.
- [ ] Against disposable MinIO/S3 credentials, invoke `sudo --preserve-env=ORCA_S3_ACCESS_KEY_ID,ORCA_S3_SECRET_ACCESS_KEY /usr/local/bin/orca-s3fs-mount ...`, write a marker, unmount, remount, and read it back. Confirm `/root/.aws/credentials` is absent afterward and the long-lived `s3fs` environment contains no credential sentinel.
- [ ] `sudo -n /bin/sh -c id` fails, `sudo -l` lists only `orca-s3fs-mount` and `umount`, and attempting to preserve `BASH_ENV` cannot execute a root-owned startup file.

## Historical spike result

This snapshot predates removal of the upstream blanket sudo rule. It proves
E2B FUSE capability only; it does **not** validate the current least-privilege
sudo or agent Bubblewrap boundary. Rebuild the template and run the checklist
above before deployment. Its final two helper checks also record the obsolete
conditional-registration model; current images install the helper at build
time and test anonymous behavior when session env is absent.

**Date:** 2026-05-05  
**Build duration:** 67 s (`e2b template create` against `e2bdev/code-interpreter:latest`).  
**Spike script:** `services/harness-server/scripts/spike-orca-default.ts`.  
**Outcome:** ✅ ALL_PASS (9/9 checks). FUSE + CAP_SYS_ADMIN confirmed available; output capture, memory mounts, and git mounts all unblocked.

```text
spawning sandbox from template <template-id>…
sandbox <sandbox-id> ready; running 9 checks

PASS  git installed (>= 2.30)
       └─ git version 2.47.3
PASS  jq installed
       └─ jq-1.7
PASS  s3fs installed
       └─ /usr/bin/s3fs
PASS  orca-git-creds is executable (mode has +x for all)
       └─ X_OK
PASS  CAP_SYS_ADMIN: tmpfs mount round-trip
       └─ CAP_SYS_ADMIN_OK
PASS  CAP_SYS_ADMIN diagnostics (informational)
       └─ id=uid=1000(user) gid=1000(user) groups=1000(user),27(sudo)
       └─ uid=1000
       └─ CapEff(user)=CapEff:	0000000000000000
       └─ CapBnd(user)=CapBnd:	000001ffffffffff
       └─ CapEff(root)=CapEff:	000001ffffffffff
       └─ CapBnd(root)=CapBnd:	000001ffffffffff
       └─ fusermount3=/usr/bin/fusermount3
       └─ /dev/fuse=crw------- 1 root root 10, 229 May  5 19:09 /dev/fuse
       └─ kernel=6.1.158
PASS  FUSE: s3fs against RustFS unreachable from sandbox (smoke)
       └─ Usage: s3fs BUCKET:[PATH] MOUNTPOINT [OPTION]...
[two obsolete conditional-registration checks omitted; current checks are
 build-time system config + no-env anonymous fallback]

ALL_PASS
```

**Reading the historical capability diagnostics:** the outer sandbox can use
`CAP_SYS_ADMIN` for FUSE. Current image no longer exposes a general sudo path:
only `orca-s3fs-mount` and `umount` may run as root. Agent commands run in a
separate Bubblewrap user/PID namespace as UID/GID 1000 with all capabilities
dropped, `no_new_privs=1`, and no `/dev/fuse`.

Current spike output should instead include
`credential.helper configured system-wide` and
`helper-without-env: anonymous fallback`; it must not expect an empty system
helper.

**The real-clone test is not part of this spike:** it validates helper packaging, system config, and anonymous fallback only. The actual `git push` round-trip needs a real GitHub PAT and a writable test repo — run the git spike checklist below for that.

## Env reference

`start.sh` reads the FUSE variables below. The image-configured
`orca-git-creds` helper reads the two Git variables directly when Git invokes
it; `start.sh` does not register or unregister the helper.

| Variable                   | Required when                             | Purpose                                                                                                                                                                                                                           |
| -------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ORCA_FUSE_OUTPUTS_ENABLE` | always (defaults to `0`)                  | Set to `1` to mount the session output prefix read-write at `/mnt/session/outputs`.                                                                                                                                               |
| `S3_BUCKET`                | `ORCA_FUSE_OUTPUTS_ENABLE=1`              | Bucket holding the session outputs.                                                                                                                                                                                               |
| `S3_ENDPOINT`              | `ORCA_FUSE_OUTPUTS_ENABLE=1`              | S3 endpoint URL (e.g. `http://host.docker.internal:9000` for the dev RustFS, AWS regional endpoint in prod).                                                                                                                      |
| `S3_FORCE_PATH_STYLE`      | optional (defaults to `true`)             | Add s3fs `use_path_request_style` for MinIO/S3-compatible endpoints; set `false` for AWS S3 virtual-hosted addressing.                                                                                                            |
| `SESSION_OUTPUT_PREFIX`    | `ORCA_FUSE_OUTPUTS_ENABLE=1`              | Object-key prefix under `S3_BUCKET` for this session's outputs. Trailing slash recommended.                                                                                                                                       |
| `AWS_ACCESS_KEY_ID`        | `ORCA_FUSE_OUTPUTS_ENABLE=1`              | s3fs credentials (STS-minted in prod). Consumed through a transient mode-0600 AWS profile; not retained in daemon argv/environment.                                                                                               |
| `AWS_SECRET_ACCESS_KEY`    | `ORCA_FUSE_OUTPUTS_ENABLE=1`              | Pair to `AWS_ACCESS_KEY_ID`.                                                                                                                                                                                                      |
| `AWS_SESSION_TOKEN`        | optional                                  | Included in the transient AWS profile for STS temporary credentials.                                                                                                                                                              |
| `ORCA_GIT_CREDS_URL`       | optional                                  | Registry endpoint for the image-configured `orca-git-creds` helper, e.g. `https://registry.orca.svc/v1/git-creds`. When either URL or token is absent, the helper emits no credentials and Git falls through to anonymous access. |
| `ORCA_GIT_CREDS_TOKEN`     | required when `ORCA_GIT_CREDS_URL` is set | Short-lived JWT scoped to this session; the helper sends it as `Authorization: Bearer <token>` to the registry.                                                                                                                   |

## Git spike (run after the checklists above pass)

The git-mount changes add `git`, `jq`, and the `orca-git-creds` credential helper. See [Rebuilding after runtime contract changes](#rebuilding-after-runtime-contract-changes) above for the rebuild and secret-update steps that must happen before this spike runs.

### Git spike checklist

Run inside a fresh sandbox spawned from the new template id:

- [ ] After rebuild, `e2b sandbox spawn $TEMPLATE_ID` succeeds.
- [ ] Inside the sandbox: `git --version` returns >= 2.30 (Debian bookworm ships 2.39+).
- [ ] `jq --version` returns a version (Debian bookworm ships 1.6).
- [ ] `ls -l /usr/local/bin/orca-git-creds` shows mode `-rwxr-xr-x` (owner: root or user).
- [ ] `git config --system --get credential.helper` returns `/usr/local/bin/orca-git-creds` in every fresh sandbox; `credential.useHttpPath` returns `true`.
- [ ] Without `ORCA_GIT_CREDS_URL` and `ORCA_GIT_CREDS_TOKEN`, `printf 'protocol=https\nhost=github.com\npath=orca/test\n\n' | /usr/local/bin/orca-git-creds get` exits 0 with empty stdout, proving anonymous fallback while the helper remains installed.
- [ ] With fake env, `git config --system --get credential.helper` remains `/usr/local/bin/orca-git-creds`; no `start.sh` rerun or privileged Git config is required.
- [ ] **Real-clone smoke test**: with a test workspace + a git credential bound to a public-but-PAT-required GitHub repo + `git_credentials.secret_ref` resolving to `ORCA_TEST_GITHUB_PAT`:
  1. Create a session via the registry with `resources: [{type: 'github_repository', url: '<repo-url>', authorization_token: 'git_cred://<id>'}]`.
  2. Spawn a sandbox via the harness's normal flow.
  3. From inside the sandbox, run `cat /workspace/<repo-name>/README.md` and confirm the bytes match the upstream README.
  4. From inside the sandbox, run `git -C /workspace/<repo-name> push origin <new-branch-name>` (after a `git checkout -b <branch>` + a trivial commit). Confirm the branch lands on github.com.
  5. Confirm via `cat /etc/profile.d/orca-git-creds.sh` that `ORCA_GIT_CREDS_URL` is set + the token is present (the JWT, not the PAT).
  6. Confirm `find / -type f 2>/dev/null | xargs grep -l "ghp_\|github_pat_" 2>/dev/null` returns NO matches (the PAT itself is never persisted; only the JWT is, and JWTs don't start with those prefixes).

### Git spike result

**Date:** 2026-05-05  
**Historical helper behavior:** the 2026-05-05 template confirmed conditional
registration. Current rebuilt templates must instead pass the build-time
system-config and no-env anonymous-fallback checks above.

**Real-clone smoke (PAT + github.com)**: not part of the recorded result, because it needs a workspace and vault holding a real PAT. To run:

1. Create a workspace + an api key + an agent + a git credential bound to a writable test repo. `git_credentials.secret_ref` must resolve to the PAT (e.g., `env://ORCA_TEST_GITHUB_PAT` if your `SecretProvider` is the env-backed one).
2. Set `ORCA_TEST_GITHUB_PAT=<pat>` in the harness's `.env` and restart the service.
3. Create a session via `POST /v1/sessions` with `resources: [{type: 'github_repository', url: '<repo-url>', authorization_token: 'git_cred://<id>'}]`.
4. The harness will spawn an E2B sandbox using your `E2B_TEMPLATE_ID`, clone host-side, stream the working tree into the sandbox, and inject the `ORCA_GIT_CREDS_*` env.
5. From inside the sandbox (via the agent's `bash` tool or a direct `Sandbox.commands.run`), run `cat /workspace/<repo-name>/README.md`, then `cd /workspace/<repo-name> && git checkout -b orca-spike-<timestamp> && date >> README.md && git -c user.email=spike@orca -c user.name=spike commit -am 'spike test' && git push origin orca-spike-<timestamp>`. Confirm the branch lands on github.com.
6. Verify the PAT was never persisted: `find / -type f 2>/dev/null | xargs grep -l 'ghp_\|github_pat_' 2>/dev/null` should return no matches (the JWT in `/etc/profile.d/orca-git-creds.sh` is fine — JWTs don't start with those prefixes).
