# Orca OpenSandbox image

`mode=separate` OpenSandbox tool image. Extends
`docker.io/opensandbox/code-interpreter:v1.1.0` with Bubblewrap, `s3fs`, and
`fuse3`. Bubblewrap enforces write policy; S3-FUSE backs memory stores and
session output capture.

```bash
docker build \
  -f services/harness-server/sandbox-templates/orca-opensandbox/Dockerfile \
  -t <registry>/orca-opensandbox-code-interpreter:v1.1.0-write-policy .
docker push <registry>/orca-opensandbox-code-interpreter:v1.1.0-write-policy
```

Configure harness-server with immutable image reference:

```bash
OPEN_SANDBOX_IMAGE=<registry>/orca-opensandbox-code-interpreter@sha256:<digest>
OPEN_SANDBOX_ENTRYPOINT=/opt/code-interpreter/code-interpreter.sh
```

`colocated` uses its catalog-selected `@orca/sandbox-harness` image instead.
The OpenSandbox server must trust both image + entrypoint pairs. By default it
trusts the official `ghcr.io/orca-ae` repositories of both images with exactly
these entrypoints. Mirrors are listed in
`ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES`; other images need an exact entry in
`ORCA_TRUSTED_SANDBOX_REPOSITORIES` or `ORCA_TRUSTED_SANDBOX_WORKLOADS`. See
[repository and exact-image trust](../../../../docs/opensandbox/README.md#repository-and-exact-image-trust).

OpenSandbox supports only `RuntimeClass/gvisor`. gVisor implements `/dev/fuse`
inside sandbox kernel, so Pod has no host `/dev/fuse` mount, `hostPath`,
`privileged`, `hostUsers`, or `procMount`. Server overlay adds only `SETFCAP` and `SYS_ADMIN`
to trusted isolation container for Bubblewrap and FUSE mounts.

Every acquisition requests both `bootstrap.execd.isolation=enable` and
`orca.fuse.device=enable`, then probes `s3fs`, `fusermount3`, gVisor-provided
`/dev/fuse`, and mount/unmount permission. Failure aborts acquisition.

After root mount setup, agent-facing Bubblewrap namespace switches to UID/GID
1000, drops every capability, sets `no_new_privs`, and creates fresh `/dev`
without `/dev/fuse`. Agent file operations reject `/proc`, `/sys`, and `/dev`,
including symlink escapes. S3 mount credentials remain transient and root-only.
