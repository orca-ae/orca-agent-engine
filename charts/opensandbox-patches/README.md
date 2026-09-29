# opensandbox-patches

Companion chart for OpenSandbox `server/v0.2.2`. Orca supports one Kubernetes
runtime contract: gVisor `RuntimeClass` `gvisor` with FUSE implemented inside
the gVisor sandbox.

Chart renders:

- `opensandbox` dataplane Namespace by default;
- ConfigMap `opensandbox-server-gvisor-fuse-patch`, containing three patched
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
these two official repositories:

| Repository                                          | Exact entrypoint array                          |
| --------------------------------------------------- | ----------------------------------------------- |
| `ghcr.io/orca-ae/orca-opensandbox-code-interpreter` | `["/opt/code-interpreter/code-interpreter.sh"]` |
| `ghcr.io/orca-ae/sandbox-harness-claude-code`       | `["/usr/local/bin/orca-sandbox-harness"]`       |

`ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES` trusts the same two images under
mirror registries. It is a JSON array of exact registry/namespace prefixes, for
example `["ghcr.io/orca-ae","registry.example.com/mirror/orca"]`. Each prefix
trusts `<prefix>/orca-opensandbox-code-interpreter` and
`<prefix>/sandbox-harness-claude-code` with the entrypoints above. The array
replaces the `ghcr.io/orca-ae` default, so list that prefix too to keep it;
`[]` trusts neither official image.

Repository matching is exact: no aliases, inferred registry, or wildcards, and a
prefix is expanded to those two repositories, never matched as a string prefix.
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
    volumes:
      - name: opensandbox-server-gvisor-fuse-patch
        configMap:
          name: opensandbox-server-gvisor-fuse-patch
```

`subPath` mounts do not hot-reload. Restart `deployment/opensandbox-server`
after upgrading this chart.

## License

Files under `files/` are modified OpenSandbox sources licensed under Apache
License 2.0. Original headers remain intact. See `NOTICE`.
