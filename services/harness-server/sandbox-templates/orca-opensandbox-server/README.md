# Orca OpenSandbox server overlay

OpenSandbox `server/v0.2.2` supplies base Kubernetes provider. Orca runtime
changes come from [`charts/opensandbox-patches`](../../../../charts/opensandbox-patches),
mounted as three ConfigMap-backed Python modules.

Overlay enforces one runtime contract:

- `secure_runtime.type = "gvisor"`;
- `secure_runtime.k8s_runtime_class = "gvisor"`;
- exact trusted image + entrypoint for every isolation request;
- both `bootstrap.execd.isolation=enable` and `orca.fuse.device=enable`;
- `runtimeClassName: gvisor`, only `SETFCAP` + `SYS_ADMIN`, no host FUSE device,
  `hostPath`, privileged container, `hostUsers`, or `procMount`;
- pause/resume rejected before CR patch.

Companion chart also installs default-deny admission for incompatible Pods in
`opensandbox` Namespace. OpenSandbox lifecycle/execd API and API key remain
infrastructure-only.

## Reproducible source build

CI builds pinned upstream source. `0001-pin-server-build-inputs.patch` changes
only `server/Dockerfile`: Python and uv images use digests and mutable uv
bootstrap is removed. Runtime Python remains ConfigMap overlay authority.

```bash
git clone https://github.com/alibaba/OpenSandbox.git /tmp/OpenSandbox
git -C /tmp/OpenSandbox checkout 207d94c7dc7735c143856fe5c6538b743e478786
git -C /tmp/OpenSandbox apply \
  "$PWD/services/harness-server/sandbox-templates/orca-opensandbox-server/0001-pin-server-build-inputs.patch"
docker build \
  -t ghcr.io/orca-ae/opensandbox-server:base-207d94c7 \
  /tmp/OpenSandbox/server
```
