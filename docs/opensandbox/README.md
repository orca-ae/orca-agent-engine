# OpenSandbox on Kubernetes

Orca uses one OpenSandbox deployment profile: gVisor `RuntimeClass/gvisor` with
FUSE daemon and `/dev/fuse` inside gVisor sandbox. Host FUSE devices,
privileged sandbox Pods, host user namespaces, and alternate runtime profiles
are unsupported.

Pinned OpenSandbox source:
`207d94c7dc7735c143856fe5c6538b743e478786` (`server/v0.2.2`).

## Topology

```text
opensandbox-system
├── opensandbox-server
└── opensandbox-controller-manager

opensandbox
└── BatchSandbox Pods (runtimeClassName: gvisor)
```

Harness-server talks to one OpenSandbox lifecycle/execd endpoint. Every
acquisition requests isolation + FUSE, verifies runtime prerequisites, then
uses root execd only for mount setup. Agent commands enter Bubblewrap with
UID/GID 1000, zero capabilities, `no_new_privs`, and fresh `/dev` without
`/dev/fuse`.

## Prerequisites

- gVisor installed on every sandbox node;
- `RuntimeClass/gvisor` exists and schedules only to those nodes;
- Kubernetes supports `ValidatingAdmissionPolicy`;
- OpenSandbox server image matches pinned source;
- Orca sandbox images contain Bubblewrap, `s3fs`, and `fuse3`;
- sandbox image references and exact entrypoint arrays satisfy the operator's
  repository or legacy exact-image trust policy.

Node setup: [`gvisor-setup.md`](gvisor-setup.md).

## Install companion chart

```bash
helm upgrade --install opensandbox-patches charts/opensandbox-patches \
  --namespace opensandbox-system \
  --create-namespace
```

For externally owned `opensandbox` Namespace:

```bash
helm upgrade --install opensandbox-patches charts/opensandbox-patches \
  --namespace opensandbox-system \
  --create-namespace \
  --set dataplaneNamespace.create=false
```

Chart creates ConfigMap `opensandbox-server-gvisor-fuse-patch` and default
admission policy/binding `opensandbox-gvisor-fuse`. Admission requires gVisor
and rejects privileged containers, `hostPath`, host network/PID/IPC, and
`hostUsers`.

## Configure OpenSandbox

Set the server config through the upstream OpenSandbox chart values. Required
server config:

```toml
[runtime]
type = "kubernetes"

[kubernetes]
namespace = "opensandbox"
workload_provider = "batchsandbox"

[secure_runtime]
type = "gvisor"
k8s_runtime_class = "gvisor"
```

Mount all four ConfigMap modules into `opensandbox-server`:

```yaml
volumeMounts:
  - name: opensandbox-server-gvisor-fuse-patch
    mountPath: /app/opensandbox_server/services/k8s/batchsandbox_provider.py
    subPath: batchsandbox_provider.py
    readOnly: true
  - name: opensandbox-server-gvisor-fuse-patch
    mountPath: /app/opensandbox_server/services/k8s/provider_common.py
    subPath: provider_common.py
    readOnly: true
  - name: opensandbox-server-gvisor-fuse-patch
    mountPath: /app/opensandbox_server/services/k8s/security_context.py
    subPath: security_context.py
    readOnly: true
  - name: opensandbox-server-gvisor-fuse-patch
    mountPath: /app/opensandbox_server/services/k8s/status_helpers.py
    subPath: status_helpers.py
    readOnly: true
volumes:
  - name: opensandbox-server-gvisor-fuse-patch
    configMap:
      name: opensandbox-server-gvisor-fuse-patch
```

### Repository and exact-image trust

When neither `ORCA_TRUSTED_SANDBOX_REPOSITORIES` nor
`ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES` is set, the server trusts the two
official repositories `ghcr.io/orca-ae/orca-opensandbox-code-interpreter` and
`ghcr.io/orca-ae/sandbox-harness-claude-code` (exact repositories, not a
namespace wildcard). Their exact entrypoint arrays are
`["/opt/code-interpreter/code-interpreter.sh"]` and
`["/usr/local/bin/orca-sandbox-harness"]`, respectively.

To trust mirrors of the official images, set
`ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES` to a JSON array of exact
registry/namespace prefixes. Each prefix trusts
`<prefix>/orca-opensandbox-code-interpreter` and
`<prefix>/sandbox-harness-claude-code` with the same entrypoints. The array
replaces the `ghcr.io/orca-ae` default, so list that prefix too to keep it:

```yaml
env:
  - name: ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES
    value: '["ghcr.io/orca-ae","registry.example.com/mirror/orca"]'
```

Repository-authorized images accept valid explicit tags, digests, and tag +
digest references. Bare repositories, implicit registry aliases, prefix/glob
matches, and non-`sha256` digests are rejected; a digest contains exactly 64
lowercase hexadecimal characters. Entrypoint arguments must also match.

An explicit `ORCA_TRUSTED_SANDBOX_REPOSITORIES` JSON array replaces the official
repositories; setting it together with `ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES`
fails server initialization. For example, an operator can authorize only a custom
repository and entrypoint:

```yaml
env:
  - name: ORCA_TRUSTED_SANDBOX_REPOSITORIES
    value: >-
      [{"repository":"registry.example.com/platform/sandbox","entrypoint":["/usr/local/bin/platform-sandbox"]}]
```

`[]` disables repository trust. `ORCA_TRUSTED_SANDBOX_WORKLOADS` is an additive
legacy exact image-string + entrypoint array policy, not a replacement for the
repository defaults. To restrict authorization to exact immutable images,
disable repository trust explicitly and list the exact deployed references:

```yaml
env:
  - name: ORCA_TRUSTED_SANDBOX_REPOSITORIES
    value: '[]'
  - name: ORCA_TRUSTED_SANDBOX_WORKLOADS
    value: >-
      [{"image":"ghcr.io/orca-ae/orca-opensandbox-code-interpreter@sha256:<digest>","entrypoint":["/opt/code-interpreter/code-interpreter.sh"]},{"image":"ghcr.io/orca-ae/sandbox-harness-claude-code@sha256:<digest>","entrypoint":["/usr/local/bin/orca-sandbox-harness"]}]
```

Replace `<digest>` with each actual digest. Legacy entries require `@sha256:`
unless the server explicitly sets `ORCA_ALLOW_TAGGED_SANDBOX_IMAGES=true`,
reserved for disposable local/CI images. Repository-authorized tags do not
require that switch. Invalid repository policy fails server initialization.

Repository trust assumes publisher ACLs and proxy integrity. Tags are mutable,
not cryptographic verification of a release; no signing infrastructure is
installed by this policy. Digest pinning fixes content identity but does not
independently establish who published it.

### Release repositories

`.github/workflows/release-images.yml` publishes the images
`orca-opensandbox-code-interpreter` and `sandbox-harness-claude-code` to
`ghcr.io/orca-ae`, which the default trust covers. Published managed-agents
charts carry the sandbox-harness repository and pushed manifest digest. An
optional job mirrors each release to `docker.io/<DOCKERHUB_NAMESPACE>` when that
repository variable is set; a mirrored copy is trusted only once its prefix is
listed in `ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES` alongside
`ghcr.io/orca-ae`, or through explicit operator policy.

All five release images build each architecture on a native GitHub-hosted
runner: `ubuntu-24.04` for `linux/amd64` and `ubuntu-24.04-arm` for
`linux/arm64`. Source validation gates both build matrices; QEMU is not used.
Service and sandbox-harness BuildKit caches are isolated by image and platform.
The large OpenSandbox image keeps its per-runner disk cleanup and omits cache
export. Each platform pushes by digest with SBOM and provenance, and a separate
merge job publishes the version/SHA tags (plus `latest` for stable releases).
Tag publication waits for both build matrices to succeed, including OpenSandbox;
individual service tags are not published early. This is a build-success gate,
not an atomic publication of all five images.
The merge verifies the published digest and both Linux architectures before
the packaged chart consumes the sandbox-harness manifest digest. Dry runs (a
manual run with `publish=false`) skip publication and manifest merging while
still building both platforms and verifying the packaged charts.

Namespace secret names and workflow configuration alone cannot independently
prove the deployed publisher: operators verify the actual release reference,
registry ownership/push ACLs, and any proxy's upstream mapping and integrity.

## Provider security contract

Each Orca acquisition creates a fresh execd API credential from 32 random bytes,
encoded as 64 hexadecimal characters. It authenticates management requests for that
sandbox until the sandbox is destroyed; it is not shared across acquisitions.
`EXECD_ACCESS_TOKEN` is a literal environment value in the BatchSandbox and Pod
specifications. Principals with permission to read those resources can read it and
are part of the trusted management boundary. Operators restrict these resource reads
to trusted management principals. The installer and agent tools receive neither this
credential nor a ServiceAccount token. The runtime also probes the local listener without credentials
and requires HTTP 401, so an old execd that ignores authentication fails closed.
The harness entrypoint removes the key before starting its SDK. Tool launchers remove
it before executing Bubblewrap, keeping it out of both the command environment and
the nested PID 1 environment.
Existing sandboxes retain their previous image and authentication configuration;
recreate them when applying the updated runtime and images.

Environment packages use the optional `orca.environment.packages.v1` extension,
whose JSON value contains `version: 1` and the six package-manager arrays. The
provider installs them in an init container using the same operator-trusted base
image, with no runtime environment variables, mounted credentials, ServiceAccount
token or concurrent runtime process. System packages precede language packages.
The installer exports an untrusted rootfs into a fresh 8 GiB `emptyDir`; the main
container mounts it read-only at `/opt/orca-environment-packages`. Templates cannot
inject installer environment variables, writable artifact consumers or sidecar init
containers. The main and installer containers use the same configured image pull
policy, including for `latest` tags, and inherit operator resource limits. They
receive a 16 GiB ephemeral-storage limit when none is set.

The 8 GiB artifact limit includes exported base-image files, installed packages and
retained package caches; it is not 8 GiB reserved for additional packages. Trusted
staging makes a second copy with an independent 8 GiB logical-file-size and 500,000-entry
limit, strips privileged bits and breaks hard links. Reflinks are an optimization, not
a capacity assumption. Budget for both copies plus installation writes, caches, logs
and filesystem overhead under the Pod's ephemeral-storage limits and the node's free
space. Two artifacts near 8 GiB leave no headroom under the default 16 GiB limit.
Storage exhaustion or eviction fails acquisition; partial staging and failed sandboxes
are removed. Operators size the image, package plan and storage limits together.

Package executable directories precede system directories in the agent's `PATH`, so
installed tools can shadow system commands inside the workload boundary. They do not
replace the controller's executables or environment. A Go specification without `@`
is passed to `go install` with `@latest`; explicit versions are preserved. Pin versions
when reproducible package selection is required.

During package preparation, an allocated Pod reports `Allocated` so lifecycle creation
does not wait for installers to finish. A non-zero installer exit, init-container
startup failure or Pod eviction reports `Failed` with a fixed reason, without installer
output or termination messages. A transient `ErrImagePull` permits another pull attempt;
`ImagePullBackOff` and `CrashLoopBackOff` fail acquisition. Both clients use the shared
OpenSandbox request builder and readiness loop. During package preparation they poll
lifecycle status and execd every two seconds, then destroy a failed sandbox as soon as
it is observed. Acquisitions without packages retain the 500 ms readiness interval.
Each package lifecycle read inspects Pod init status through the server's Kubernetes
client and its `[kubernetes] read_qps` / `read_burst` limiter; operators can bound aggregate
Kubernetes read traffic across concurrent acquisitions with those settings. Successful
preparation still requires execd readiness and proof that the extension was honored
before opening a Session.
The package plan uses canonical manager order, valid UTF-8 strings bounded to 1,024
UTF-16 code units, and a SHA-256 revision over compact UTF-8 JSON. Python and TypeScript
validate the same golden fixtures, including Unicode and rejected edge whitespace. The operator images include the
static `orca-environment-probe`; package-enabled execution fails closed when the image
or provider lacks the matching contract. Package-provided shells, loaders and libraries
are never used for this probe or for the trusted controller. Unrestricted Environment
networking is required for package preparation.

Isolation requests must include:

```json
{
  "extensions": {
    "bootstrap.execd.isolation": "enable",
    "orca.fuse.device": "enable"
  }
}
```

Provider emits:

- `runtimeClassName: gvisor`;
- sandbox `capabilities.add: [SETFCAP, SYS_ADMIN]`;
- unconfined seccomp/AppArmor for nested mount isolation;
- no `privileged`, `hostUsers`, `procMount`, host namespace, `hostPath`, or
  `/dev/fuse` volume mount.

Provider rejects pool mode, Windows workloads, mismatched runtime config,
untrusted image/entrypoint, incompatible template security fields, and
isolation without FUSE authorization.

Pause and resume reject in harness and provider before lifecycle/CR patch.
Snapshot restore cannot preserve live in-sandbox FUSE mounts.

## Verify

The stack E2E workflow runs Environment package installation against its real
OpenSandbox server and gVisor kind cluster. The dedicated entry point is
`pnpm -F @orca/harness-server test:e2e:opensandbox`; it requires the live server's
domain and API key, a package fixture image in `OPEN_SANDBOX_IMAGE`, and
`KIND_CLUSTER_NAME`. The fixture image extends the sandbox-harness image using
`services/harness-server/test/fixtures/environment-packages/Dockerfile`.
Both runtime clients install all six managers and exercise malicious installation
hooks, read-only resources, management authentication and failed sandbox cleanup.
Pod inspection explicitly selects the named kind context. The Layer B.1 matrix
also verifies Environment packages through the Registry, real SDK tool dispatch
and the Files download API.

```bash
kubectl get runtimeclass gvisor
helm status opensandbox-patches -n opensandbox-system
kubectl get validatingadmissionpolicy,validatingadmissionpolicybinding \
  opensandbox-gvisor-fuse
kubectl -n opensandbox-system rollout status deployment/opensandbox-server
```

For a running sandbox:

```bash
pod=<sandbox-id>-0
kubectl -n opensandbox get pod "$pod" -o json | jq -e '
  (.spec.containers[] | select(.name == "sandbox")) as $sandbox
  | .spec.runtimeClassName == "gvisor"
    and ((.spec | has("hostUsers")) | not)
    and (($sandbox.securityContext.privileged // false) == false)
    and (($sandbox.securityContext | has("procMount")) | not)
    and ((($sandbox.securityContext.capabilities.add // []) | sort) == ["SETFCAP", "SYS_ADMIN"])
    and ((.spec.volumes // []) | all(has("hostPath") | not))'
kubectl -n opensandbox exec "$pod" -c sandbox -- uname -r
kubectl -n opensandbox exec "$pod" -c sandbox -- \
  sh -c 'test -c /dev/fuse && exec 9<>/dev/fuse'
```

Harness acquisition additionally mounts temporary filesystem. Stack E2E runs
S3 write, unmount, remount, and read-back.

## Harness configuration

```bash
SANDBOX_RUNTIME=opensandbox
OPEN_SANDBOX_DOMAIN=opensandbox-server.opensandbox-system.svc.cluster.local
OPEN_SANDBOX_PROTOCOL=http
OPEN_SANDBOX_API_KEY=<secret>
OPEN_SANDBOX_IMAGE=<registry>/orca-opensandbox-code-interpreter@sha256:<digest>
OPEN_SANDBOX_ENTRYPOINT=/opt/code-interpreter/code-interpreter.sh
OPEN_SANDBOX_USE_SERVER_PROXY=true
```

No FUSE opt-out or runtime profile selector exists.

## Uninstall

```bash
helm uninstall opensandbox -n opensandbox-system
helm uninstall opensandbox-patches -n opensandbox-system
kubectl delete namespace opensandbox --ignore-not-found
kubectl delete namespace opensandbox-system --ignore-not-found
```
