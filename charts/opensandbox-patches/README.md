# opensandbox-patches

Companion chart for OpenSandbox `server/v0.2.2`. Orca supports one Kubernetes
runtime contract: gVisor `RuntimeClass` `gvisor` with FUSE implemented inside
the gVisor sandbox.

Chart renders:

- `opensandbox` dataplane Namespace by default;
- ConfigMap `opensandbox-server-gvisor-fuse-patch`, containing four patched
  OpenSandbox Python modules;
- gVisor Pod `ValidatingAdmissionPolicy` and binding by default.

ConfigMap lands in Helm release namespace, normally `opensandbox-system`.
Admission targets Pods in Namespace `opensandbox`, including externally owned
instances selected through Kubernetes automatic namespace label.

## Runtime contract

Provider initialization fails unless OpenSandbox config contains:

```toml
[secure_runtime]
type = "gvisor"
k8s_runtime_class = "gvisor"
```

Every generated sandbox Pod uses `runtimeClassName: gvisor`. FUSE isolation requests
must include both extensions:

```json
{
  "bootstrap.execd.isolation": "enable",
  "orca.fuse.device": "enable"
}
```

The second extension authorizes gVisor in-sandbox FUSE. It does not expose a
host device. Isolation requests also require operator-owned repository + exact
entrypoint trust, or a legacy exact image + entrypoint match.

When neither `ORCA_TRUSTED_SANDBOX_REPOSITORIES` nor
`ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES` is set, the server trusts exactly
these three official repositories:

| Repository                                          | Exact entrypoint array                          |
| --------------------------------------------------- | ----------------------------------------------- |
| `ghcr.io/orca-ae/orca-opensandbox-code-interpreter` | `["/opt/code-interpreter/code-interpreter.sh"]` |
| `ghcr.io/orca-ae/sandbox-harness-claude-code`       | `["/usr/local/bin/orca-sandbox-harness"]`       |
| `ghcr.io/orca-ae/sandbox-harness-dsh`               | `["/usr/local/bin/orca-sandbox-harness"]`       |

`ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES` trusts the same three images under
mirror registries. It is a JSON array of exact registry/namespace prefixes, for
example `["ghcr.io/orca-ae","registry.example.com/mirror/orca"]`. Each prefix
trusts `<prefix>/orca-opensandbox-code-interpreter`, `<prefix>/sandbox-harness-claude-code`
and `<prefix>/sandbox-harness-dsh` with the entrypoints above. The array
replaces the `ghcr.io/orca-ae` default, so list that prefix too to keep it;
`[]` trusts none of the official images.

Repository matching is exact: no aliases, inferred registry, or wildcards, and a
prefix is expanded to those three repositories, never matched as a string prefix.
Image references must contain a valid explicit tag, digest, or tag + digest;
bare repositories are rejected. Digests must be `sha256:` followed by 64
lowercase hexadecimal characters. The entire entrypoint array must match,
including arguments.

`ORCA_TRUSTED_SANDBOX_REPOSITORIES` is a JSON array of
`{"repository":"registry/namespace/name","entrypoint":["/path/to/entrypoint"]}`
objects that replaces the official repositories; `[]` disables repository
trust. Operators can authorize custom exact repositories and entrypoints this
way. Setting it together with `ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES`, or
setting either to invalid JSON or an invalid entry, fails server initialization.

`ORCA_TRUSTED_SANDBOX_WORKLOADS` remains additive, with JSON entries shaped as
`{"image":"registry/namespace/name@sha256:<digest>","entrypoint":["/path/to/entrypoint"]}`.
This legacy policy matches the exact image string and entrypoint array. Entries
require immutable `@sha256:` references unless the server explicitly sets the
local/CI-only `ORCA_ALLOW_TAGGED_SANDBOX_IMAGES=true`. That switch is not needed
for tags authorized by repository policy. Exact-image-only operators must also
set `ORCA_TRUSTED_SANDBOX_REPOSITORIES=[]`.

Repository trust relies on publisher ACLs and proxy integrity. Tags are mutable;
this policy is not cryptographic verification of a release and installs no
signing infrastructure. See the [deployment guide](../../docs/opensandbox/README.md)
for release repository configuration and operator examples.

Credential-free Environment workers can instead use an explicit
`ORCA_TRUSTED_NON_FUSE_WORKLOADS` array with the same exact `image` + `entrypoint`
shape and digest requirement as `ORCA_TRUSTED_SANDBOX_WORKLOADS`. It defaults to
empty. These workloads still require `bootstrap.execd.isolation=enable`, gVisor,
and all Pod restrictions below, but omit `orca.fuse.device`. A non-FUSE grant
does not authorize FUSE; FUSE requests still need the existing FUSE trust policy.
Repository trust alone never enables a non-FUSE workload.

Generated isolation Pods:

- add only `SETFCAP` and `SYS_ADMIN` to sandbox container for Bubblewrap and FUSE mounts;
- use unconfined seccomp/AppArmor profiles required by nested mount isolation;
- never set `privileged`, `hostUsers`, or `procMount`;
- never mount host `/dev/fuse` or any other `hostPath`;
- reject pool mode and Windows workloads;
- reject request-level network policy/credential proxy in favor of cluster policy;
- reject pause/resume before any BatchSandbox CR patch.

Admission policy requires `runtimeClassName: gvisor` and rejects privileged
containers, all `hostPath` volumes, host network/PID/IPC namespaces, and any
`hostUsers` field. Set `admission.enabled=false` only when equivalent cluster
policy already enforces this contract.

Chart does not install gVisor or create `RuntimeClass/gvisor`. Nodes and runtime
handler must exist before OpenSandbox server starts.

## Native DSH profile preparation

The provider accepts the internal `orca.dsh.profile.v1` extension from the matching
Orca harness release. No profile-specific values are required in this chart.
Registry owns approval through `DSH_APPROVED_PROFILES`; see the
[engine chart configuration](../orca-managed-agents/README.md#native-dsh-profiles).
Deploy the matching engine and DSH sandbox image with this patch before enabling
approvals. OpenSandbox server mounts these Python files through `subPath`; restart
`deployment/opensandbox-server` after upgrading the patch ConfigMap. Updating the
ConfigMap alone does not reload code into existing server Pods.

Each custom-profile Pod has a credential-free init container using the same image
as the runtime, with a 900-second preparation timeout and these fixed resources:

| Resource          | Request | Limit |
| ----------------- | ------- | ----- |
| CPU               | 100m    | 1     |
| Memory            | 512Mi   | 512Mi |
| Ephemeral storage | 2Gi     | 2Gi   |

The dedicated `emptyDir` has a 2Gi size limit. The runtime container also receives
a 2Gi ephemeral-storage limit when none is supplied; Environment package preparation
retains its existing resource settings. Ensure namespace quotas and admission rules
permit the init container, bounded volume and fixed read-only runtime mounts.
Preparation runs before the main container without its runtime credentials; every
init attempt clears the dedicated volume and reinstalls from the retained descriptor.
Configuration, installed modules and the completion marker are mounted read-only at
the private profile path. Worker boot metadata and native history remain writable.

## Release versioning

The release workflow packages this chart alongside `orca-managed-agents` in
the same GitHub Release. The packaged chart `version` follows the release tag
without its `v` prefix (for example, `v0.4.4-rc.9` produces
`opensandbox-patches-0.4.4-rc.9.tgz`). The `appVersion` remains the upstream
OpenSandbox version, currently `0.2.2`. Checked-in chart metadata is unchanged
by packaging. The workflow verifies both fields before publishing.

## Install

Fresh deployment:

```bash
helm upgrade --install opensandbox-patches charts/opensandbox-patches \
  --namespace opensandbox-system \
  --create-namespace
```

Existing externally owned `opensandbox` Namespace:

```bash
helm upgrade --install opensandbox-patches charts/opensandbox-patches \
  --namespace opensandbox-system \
  --create-namespace \
  --set dataplaneNamespace.create=false
```

Mount all ConfigMap keys through upstream `opensandbox-server` chart:

```yaml
opensandbox-server:
  server:
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

`subPath` mounts do not hot-reload. Restart `deployment/opensandbox-server`
after upgrading this chart.

## Controlled egress opt-in

The trusted `orca.sandbox.network` extension carries strict JSON
`{"binding_id":"...","generation":"..."}`. It is accepted only when the provider
has `ORCA_SANDBOX_EGRESS_ENABLED=true`, `ORCA_SANDBOX_EGRESS_CA_CONFIG_MAP`, and the
operator-owned `ORCA_SANDBOX_EGRESS_REGISTRY_URL` internal origin. The old enable
extension is rejected. Without opt-in no network bootstrap is injected.

The provider protects the enforcement label and `orca.ai/network-binding-id` /
`orca.ai/network-generation` annotations on both BatchSandbox and Pod template.
It verifies the trusted offline execd init, entrypoint and exact installer ordering
before adding an image-owned network init. Only this init receives the projected
Pod-bound ServiceAccount token with audience `orca-sandbox-network` and the public
CA ConfigMap. Pod automount remains false. Registry validates token identity, exact
Pod/owner UID and current Cilium NP/CNP/CCNP union and CEP enforcement before issuing
base networking. The provider does not duplicate that authority evaluator.

The bootstrap publishes `/run/orca/sandbox-network/context.json`, a combined trust
bundle and client configuration on a memory-backed volume. Package and DSH installers
follow it as separate init containers and mount the context read-only, as does main.
The same combined CA is mounted read-only at the platform images' native
`/etc/ssl/certs/ca-certificates.crt` path using `subPath: ca-bundle.crt`; bootstrap
keeps its original image roots. This supports Cargo/libcurl proxy CA verification.
A Registry/bootstrap failure prevents installation. Session grants project only
process-local capability/markers over this one Sandbox-lifetime network context.

Enabled egress requires endpoint policy status reporting. Cilium 1.13.2 Helm values
`endpointStatus.enabled: true` and `endpointStatus.status: "policy"` publish it
through agent ConfigMap key `endpoint-status`; preserve existing reporting categories
when adding `policy`. A CEP showing `{enforcing: false, state: '<status disabled>'}`
does not establish whether enforcement is active and fails readiness closed. The
Engine chart reference below links the official versioned Cilium source. This
chart does not reconfigure Cilium, and disabled egress has no reporting prerequisite.
Managed networking allows Environment package and DSH profile preparation after the
network barrier. Managed `gem` plans are rejected before provisioning/download because
RubyGems 3.6.7 / Ruby 3.3.8 cannot use HTTPS proxy transport; non-managed gem plans
retain their existing behavior. Unsupported runtime backends reject managed network acquisition.

See [Engine chart controlled egress](../orca-managed-agents/README.md#controlled-sandbox-egress)
for configuration, namespace-scoped RBAC, legacy-policy exclusions, and the distinction
between endpoint readiness and actual dataplane qualification. The network init must complete before any installer or runtime container starts.

## License

Files under `files/` are modified OpenSandbox sources licensed under Apache
License 2.0. Original headers remain intact. See `NOTICE`.
