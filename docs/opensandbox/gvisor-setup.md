# gVisor setup for OpenSandbox

Orca OpenSandbox workloads run only through Kubernetes `RuntimeClass/gvisor`.
gVisor supplies `/dev/fuse` inside sandbox kernel; nodes do not expose host
`/dev/fuse` to Pods.

Verified stack:

| Component  | Version                        |
| ---------- | ------------------------------ |
| EKS        | v1.34.4-eks-f69f56f            |
| OS         | Amazon Linux 2023              |
| containerd | 2.1.5–2.2.1 (config version 3) |
| gVisor     | 20260727.0                     |

## Install binaries

Install on every node eligible for OpenSandbox. Pin the release archive and
verify its SHA-512 digest.

```bash
GVISOR_VERSION=20260727.0
ARCH=x86_64
BASE="https://storage.googleapis.com/gvisor/releases/release/${GVISOR_VERSION}/${ARCH}"
GVISOR_SHA512=94a7280655629330f02ff06fbec0493b7f2f4041dd145576daeff2340577cde0fb45fe28e5f1d209f7d7c08f70b9c3e333aa1073063b1d132742120763eaf0ad

cd /tmp
curl -fsSLo gvisor.tar.bz2 --retry 5 --retry-all-errors "${BASE}/gvisor.tar.bz2"
printf '%s  %s\n' "${GVISOR_SHA512}" gvisor.tar.bz2 | sha512sum -c -
tar -xjf gvisor.tar.bz2 -C /usr/local/bin

/usr/local/bin/runsc --version
test -x /usr/local/bin/containerd-shim-runsc-v1
```

## Register containerd runtime

Containerd 2.x config:

```toml
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc]
runtime_type = "io.containerd.runsc.v1"
```

After editing `/etc/containerd/config.toml`:

```bash
systemctl restart containerd
systemctl is-active containerd
```

## Create RuntimeClass

```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
scheduling:
  nodeSelector:
    sandbox-runtime: gvisor
  tolerations:
    - key: sandbox-runtime
      operator: Equal
      value: gvisor
      effect: NoSchedule
```

Label eligible nodes:

```bash
kubectl label node <node-name> sandbox-runtime=gvisor
```

## Verify gVisor and in-sandbox FUSE

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gvisor-fuse-smoke
spec:
  runtimeClassName: gvisor
  restartPolicy: Never
  containers:
    - name: smoke
      image: alpine:3.21
      command: [sh, -c, 'uname -r; test -c /dev/fuse; exec 9<>/dev/fuse; sleep 300']
      securityContext:
        capabilities:
          add: [SETFCAP, SYS_ADMIN]
```

```bash
kubectl apply -f gvisor-fuse-smoke.yaml
kubectl wait --for=condition=Ready pod/gvisor-fuse-smoke --timeout=2m
kubectl exec gvisor-fuse-smoke -- uname -r   # contains gvisor
kubectl exec gvisor-fuse-smoke -- sh -c 'test -c /dev/fuse && exec 9<>/dev/fuse'
kubectl delete pod gvisor-fuse-smoke
```

Pod needs no `privileged`, `hostPath`, `hostUsers`, host namespace, or
`procMount` field.

## Configure OpenSandbox

```toml
[runtime]
type = "kubernetes"

[kubernetes]
namespace = "opensandbox"
workload_provider = "batchsandbox"

[secure_runtime]
type = "gvisor"
docker_runtime = "runsc"
k8s_runtime_class = "gvisor"
```

`charts/opensandbox-patches` validates this configuration during provider
initialization and enforces same Pod contract through admission. OpenSandbox
startup also checks `RuntimeClass/gvisor` exists.

## Operational checks

```bash
kubectl get runtimeclass gvisor -o yaml
kubectl get nodes -l sandbox-runtime=gvisor
kubectl -n opensandbox get pod -o json \
  | jq -e '.items | all(
      .spec.runtimeClassName == "gvisor" and
      (.spec | has("hostUsers") | not) and
      ((.spec.volumes // []) | all(has("hostPath") | not)) and
      ([((.spec.initContainers // [])[]), .spec.containers[],
        ((.spec.ephemeralContainers // [])[])]
        | all((.securityContext.privileged // false) == false))
    )'
```

OpenSandbox acquisition performs stronger runtime probe: `s3fs`,
`fusermount3`, openable gVisor `/dev/fuse`, tmpfs mount/unmount, then real S3
write/unmount/remount/read in stack E2E.
