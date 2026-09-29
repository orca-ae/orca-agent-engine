# Orca AgentENV sandbox image

This image is the default userland contract for `SANDBOX_RUNTIME=agentenv`.
AgentENV provides Firecracker, OverlayBD, and envd; the image provides Bash,
Node, Bubblewrap, `realpath`, `setpriv`, Git, and Orca's Git credential helper.
The image keeps the `ubuntu` account at uid/gid 1000; envd applies that account
to materialized resources before agent commands enter Bubblewrap as uid/gid 1000.

Build it from the repository root so the shared credential-helper source is in
the Docker build context:

```bash
docker build \
  -f services/harness-server/sandbox-templates/orca-agentenv/Dockerfile \
  -t registry.example/orca-agentenv:dev \
  .
```

Set the published immutable image reference as `AGENTENV_IMAGE` (or
`harness.agentEnv.image` in the Helm chart). AgentENV cold-starts that OCI
image for each acquired sandbox.

The runtime advertises `supportsFuse=false`. Memory stores and session outputs
are materialized through envd's Files API and indexed from the sandbox
filesystem. The trusted adapter runs as root for setup; agent commands run in
the existing Bubblewrap write-policy namespace as uid/gid 1000 with all
capabilities dropped.
