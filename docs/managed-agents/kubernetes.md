# Kubernetes Deployment

This chart deploys the managed-agents application layer only:

- `registry-service-ts`
- `harness-server`
- external `ghcr.io/orca-ae/orca-ai-gateway`
- `toolset` Deployment running `orca-cli` for interactive management and tests

It also renders an `observabilityExporter.serviceAccount` identity for Registry
TokenReview subject verification. Setting `observabilityExporter.enabled=true`
adds the optional exporter Deployment, ConfigMap, projected token and health
probes. The default `observabilityExporter.stateBackend=kafka` uses Kafka for exporter
state and delivery and needs no exporter database or database Secret. Only explicit
legacy `stateBackend=postgres` requires a separate exporter-owned Postgres Secret
reference. Existing SQL installations retain that mode until controlled cutover;
Kafka mode rejects the old database Secret reference rather than silently migrating.
Kafka state uses a worker-thread SQLite scratch index in a disk-backed `emptyDir` at
`/var/run/orca/exporter-state`, not a persistent database. The volume defaults to `2Gi`
(`observabilityExporter.kafkaStateSizeLimit`); the runtime quota defaults to 1 GiB, with roughly
one third available for database pages after reserving rollback-journal space. Each runtime
rebuilds a fresh index from the existing compacted Kafka checkpoint topic.
The Deployment uses Recreate in Kafka mode. Quiesce every original v1 writer before the
automatic v2 import, including writers outside this Deployment; do not mix v1/v2 images or
roll back to an original v1-only exporter image after v2 state is committed. Keep a v2-capable recovery image and preserve
topics, groups and raw/Avro selection. See the [state upgrade runbook](../../services/observability-exporter/README.md#state-v1-to-v2-upgrade-and-recovery).
The startup probe checks `/healthz`; readiness does not certify assignment restoration or
backlog catch-up. Extending probe timeouts is not a repair for failed restore validation.
No exporter Service, NetworkPolicy, SecretStore permission, or OTLP endpoint is
rendered. See the [exporter configuration](../../services/observability-exporter/README.md#optional-helm-workload-and-image)
for image publication, broker topics/ACLs, legacy database setup and binding configuration.

It does **not** deploy Postgres, Kafka, Pulsar, MinIO, or OpenSandbox. Those are external dependencies wired through Helm values and Kubernetes Secrets.

Registry vault credentials use a persistent Kubernetes SecretStore by default.
The chart creates one dedicated Secret plus a registry-only ServiceAccount,
Role, and RoleBinding. RBAC grants only `get` and `patch` on that named Secret;
the harness and ai-gateway ServiceAccounts cannot read it. The in-process
`LocalSecretStore` is rejected by both Helm validation and Registry runtime
guards in deployed environments.

```yaml
secretStore:
  mode: kubernetes
  kubernetes:
    confirmLocalCutover: false
    secretName: '' # defaults to <release>-secret-store
registry:
  serviceAccount:
    create: true
    name: ''
    annotations: {}
observabilityExporter:
  serviceAccount:
    create: true
    name: ''
    annotations: {}
```

The dedicated Registry ServiceAccount is configured only through
`registry.serviceAccount`. Harness, ai-gateway, and exporter identities
have independent `harness.serviceAccount`, `aiGateway.serviceAccount`, and
`observabilityExporter.serviceAccount` settings. If creation is disabled, each
identity requires an explicit name; Registry, Harness, AI Gateway, exporter,
and enabled toolset names are distinct.

## Internal API authentication

The chart uses Kubernetes workload identity for Registry's internal listener:

```yaml
internalAuth:
  mode: kubernetes_service_account
  audience: orca-registry-internal
  tokenExpirationSeconds: 600
  tokenMountPath: /var/run/secrets/orca/registry-internal
```

Harness, AI Gateway and the enabled exporter receive separate projected ServiceAccount tokens at
`<tokenMountPath>/token`. Tokens are audience-bound, short-lived, rotated by
the kubelet, and reread for each Registry request. Registry verifies them with
the Kubernetes TokenReview API, then permits Harness on general runtime routes
and AI Gateway only on the MCP destination and vault-credential resolvers. The
exporter subject is authorized only for the Session observability
context and secret resolver routes. The chart binds only the Registry ServiceAccount to the built-in
`system:auth-delegator` ClusterRole.
Because that binding is cluster-scoped, the identity installing the chart must
be allowed to create `ClusterRoleBinding` resources and bind the
`system:auth-delegator` role. The binding name includes both release name and
namespace so same-named releases in separate namespaces do not collide.

The projected token is group-readable (`0440`) and therefore requires
`podSecurityContext.fsGroup`; chart validation rejects deployments that remove
that value from the default pod security context.

The chart does not install Istio or cluster-wide network policy. Application
auth works without Istio, but plain HTTP does not encrypt bearer tokens; use
Istio `STRICT` mTLS or equivalent transport security in production and limit
internal listener reachability with `NetworkPolicy`/`AuthorizationPolicy`.

Set `secretStore.mode=none` only when vault credential create/rotation and
resolution are intentionally disabled. `secretStore.mode=local` remains
available to the non-Kubernetes dev stack only.

When upgrading a release whose existing Registry ConfigMap says `local`, Helm
fails before rollout. First verify each credential can be rebuilt from its
source, then set `secretStore.kubernetes.confirmLocalCutover=true` for that
upgrade. After Kubernetes SecretStore is active, recreate or rotate the
credentials into it and reset the flag to `false`. There is no automatic
migration from process memory.

Kubernetes limits one Secret object to roughly 1 MiB. This backend fits the
current self-hosted/test deployment footprint; larger multi-tenant control
planes should use a cloud secret-manager backend rather than increasing
Registry RBAC scope across arbitrary namespace Secrets.

Kubernetes Secret `data` is base64 encoding, not application-layer encryption.
Production clusters must enable Kubernetes/etcd encryption at rest (preferably
KMS envelope encryption) and restrict control-plane/backup access accordingly.

## Chart

The chart lives at:

```bash
charts/orca-managed-agents
```

Render it locally:

```bash
helm template orca-managed-agents charts/orca-managed-agents -f my-values.yaml
```

Install or upgrade:

```bash
helm upgrade --install orca-managed-agents charts/orca-managed-agents \
  --namespace orca-managed-agents \
  --create-namespace \
  -f my-values.yaml
```

The default-enabled toolset starts without a Registry credential, so installation
does not require a toolset Secret. Set `toolset.enabled=false` only when the pod
itself is not wanted.

Assert what the chart actually renders — no cluster required, a few seconds:

```bash
pnpm test:chart:render
```

This runs in CI (`.github/workflows/test-ts.yml`). It is where a claim about the
rendered manifests belongs: the Ingress forwarding only `/v1` and so hiding
`/api`, `/apis` and the `/api/v1` alias was invisible until this suite was wired
into a job.

Regression-test LocalSecretStore cutover guard, Secret data preservation, and
selector-safe toolset renaming across Helm upgrades in a disposable kind
cluster:

```bash
pnpm test:chart:secret-store-upgrade
```

`my-values.yaml` is a user-supplied values file; it is not checked into the repo.

## Orca CLI toolset

The chart runs the released Orca CLI image as a long-running `toolset`
Deployment by default. Set `toolset.enabled=false` to omit it. Credentialless is
the default: the pod receives `ORCA_REGISTRY_URL`, but neither `ORCA_API_KEY` nor
`ORCA_ACCESS_TOKEN`. No empty or placeholder Secret is needed, and the pod can
start before any workspace credential exists.

Exec into the pod, then inject the credential for the target workspace into
that shell only:

```bash
kubectl -n orca-managed-agents exec -it \
  deployment/orca-managed-agents-toolset -- /bin/sh

# inside the pod
read -rsp 'Workspace API key: ' ORCA_API_KEY && echo
export ORCA_API_KEY
orca agent list
unset ORCA_API_KEY
```

`ORCA_REGISTRY_URL` points at the in-cluster Registry host root, without `/v1`,
`/v1/registry`, or `/api/v1`. Unauthenticated commands such as `orca healthz`
work before credential injection. The chart creates no `pods/exec` RBAC grant;
cluster operators decide who may enter the toolset.

For a shared toolset bound to one workspace, optionally persist one API key in
a Secret:

```bash
kubectl -n orca-managed-agents create secret generic orca-toolset-access \
  --from-literal=api-key="$ORCA_API_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
```

```yaml
toolset:
  apiKey:
    secretKeyRef:
      name: orca-toolset-access
      key: api-key
  # Empty defaults to http://<release>-registry:<public-service-port>.
  registryUrl: ''
  # Optional provider credentials or other tools' configuration.
  extraEnvFrom:
    - secretRef:
        name: orca-toolset-provider-credentials
        optional: true
```

Bearer auth remains supported for an OIDC-protected Registry or another front
door with the same contract. Configure `toolset.accessToken` instead of
`toolset.apiKey`; never configure both. When `toolset.registryUrl` is empty,
the in-chart Registry must also advertise at least one OIDC issuer:

```yaml
registry:
  oidcAllowedIssuers: https://issuer.example.com
  oidcAudience: orca-managed-agents

toolset:
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
  registryUrl: ''
```

This path injects `ORCA_ACCESS_TOKEN`, which orca-cli sends as
`Authorization: Bearer`. API-key and Bearer credentials are optional and
mutually exclusive in orca-cli v0.2.0 and in chart validation.

`toolset.nameOverride` overrides only the resource-name base; the chart always
appends `-toolset`. This forces a new Deployment name when migrating from the
older `orca-cli` component selector, which Kubernetes cannot mutate in place.

`toolset.podLabels` may add metadata labels, but must not set the reserved
`app.kubernetes.io/name`, `app.kubernetes.io/instance`, or
`app.kubernetes.io/component` selector keys.

When upgrading a release that still uses the pre-rename `orcaCli` /
`images.orcaCli` values, first migrate the complete values file to `toolset` /
`images.toolset`. Do **not** use `--reuse-values`: Helm would retain the old
schema and omit the new defaults. Reset to the new chart defaults, then apply
the complete migrated overrides:

```bash
helm upgrade orca-managed-agents charts/orca-managed-agents \
  --namespace orca-managed-agents \
  --reset-values \
  -f migrated-values.yaml
```

This self-hosted Registry exposes the core `agent` surface. CLI command trees
for distribution extensions (`connections`, `functions`, `sources`, `sinks`,
and similar) are unavailable unless `/apis` advertises a group that serves
them.

The default image is `ghcr.io/orca-ae/orca-cli:0.2.0`, pinned by the
multi-platform manifest digest published by the CLI's `v0.2.0` release. Clear
`images.toolset.digest` only when deliberately overriding the repository/tag.

Credential semantics matter: `ORCA_API_KEY` becomes `x-api-key`, while
`ORCA_ACCESS_TOKEN` becomes `Authorization: Bearer`. An `orca_...` workspace
API key is not interchangeable with an OIDC access token. Chart validation
accepts zero or one Secret source, rejects both together, and blocks
`toolset.extraEnv` from replacing the auth variables. With zero sources, neither
auth environment variable nor any auth `secretKeyRef` is rendered.

When a persistent Secret is configured, every principal allowed to `exec` into
the pod can use that credential. Secret-backed environment variables are read
when the pod starts; after rotation, restart the Deployment or bump a durable
annotation under `toolset.podAnnotations`. Restrict `pods/exec`, prefer
short-lived least-privilege credentials, and audit interactive access. The
default ClusterIP URL is plaintext HTTP; use mesh mTLS or equivalent transport
encryption plus network policy in production.

## Split-chart Kind E2E

[`pnpm e2e:kind-helm`](../../packages/e2e-tests/scripts/kind-helm-e2e.ts)
black-box tests split deployment: this chart installs Registry and Harness with
`aiGateway.enabled=false`; the pinned Gateway chart
(`oci://ghcr.io/orca-ae/charts/orca-ai-gateway`) installs Gateway as an
independent release. Driver owns initial installs, Helm tests, and
coordinated upgrades using `helm upgrade --install --wait --atomic`.

Coverage uses real linux/amd64 Registry, Harness, and Gateway images, plus real
Kind-local `postgres:16-alpine`, `apache/kafka:3.7.1`, and pinned
`rustfs/rustfs` / `rustfs/rc` images. PostgreSQL, Kafka, and RustFS use static
hostPath-backed PVCs inside the disposable Kind node. A local fixture image
supplies fake Anthropic Messages and fake MCP endpoints. No real
`ANTHROPIC_API_KEY` or external LLM request is involved.
The MCP fixture returns JSON-RPC method-not-found for the optional pre-initialize
`server/discover` request, allowing clients to use the initialize handshake instead.
Authorization and Streamable HTTP Accept checks still apply to that probe; subsequent
notifications and tool requests require the issued MCP session ID.
`pnpm test:chart:render` runs HTTP fixture regressions alongside chart-render checks.

The driver verifies a complete turn through the fake LLM and MCP path, then
mints and calls Gateway directly with a session JWT. It first performs a
ConfigMap-only upgrade while holding the JWT keypair and rollout-version
annotations constant, then separately rotates Registry's signing key and
Gateway's trust anchor while keeping ConfigMaps unchanged. This independently
checks checksum-driven config rollouts, old JWT rejection, newly minted JWT
acceptance, and Agent, Environment, Vault credential, Session transcript, and
MemoryStore persistence. It also restarts Kafka and reads the first-turn marker
back through `@orca/transcript-store`, proving the transcript source of truth
rather than only its PostgreSQL projection survived.

`harness.sandboxRuntime=in-memory` is intentional here: it is lightweight
test-only replacement for a real sandbox. This E2E validates MCP-only routing
and split-chart trust/configuration behavior; it does not require or validate
OpenSandbox.

OpenSandbox Helm runtime coverage lives in
[`e2e-stack.yml`](../../.github/workflows/e2e-stack.yml)'s
`e2e-sandbox-harness` matrix. That job builds a clean Python base image from
the pinned v0.2.2 source, installs `charts/opensandbox-patches` as its own Helm
release, mounts all three ConfigMap modules through the upstream OpenSandbox
chart, and proves running files match ConfigMap. Job installs gVisor in Kind,
creates `RuntimeClass/gvisor`, and verifies one contract: every sandbox Pod uses
gVisor, adds only `SETFCAP` and `SYS_ADMIN`, contains no privileged/hostPath/host namespace/
`hostUsers`/`procMount` escape hatch, opens gVisor-provided `/dev/fuse`, runs
nested Bubblewrap, and completes real S3 write/unmount/remount/read round trips.

### Local prerequisites

Driver builds neither cluster nor images. Before running it, provide:

1. Docker, Kind, `kubectl`, Helm 3, OpenSSL, Node 22, and pnpm 9.
2. A fresh, disposable **single-node** Kind cluster whose current context is
   the matching `kind-<cluster-name>` context. The static hostPath volumes are
   node-local, so the driver rejects multi-node clusters. It obtains an isolated
   kubeconfig directly from Kind and binds every Helm/kubectl operation to it;
   it refuses an existing test namespace or test PVs rather than reusing prior
   state.
3. Linux/amd64 images tagged and loaded into that Kind cluster:
   `orca-registry-service-ts-kind:e2e`, `orca-harness-server-kind:e2e`,
   `orca-ai-gateway-kind:e2e`, `orca-kind-fixture:e2e`, `postgres:16-alpine`,
   `apache/kafka:3.7.1`,
   `rustfs/rustfs:1.0.0`,
   `rustfs/rc:v0.1.36`, and `busybox:1.36`.
4. The Gateway Helm chart, pulled from its public OCI repository at the version
   matching the chart's `images.aiGateway.tag` and unpacked, for example
   `helm pull oci://ghcr.io/orca-ae/charts/orca-ai-gateway --version <version> --untar`.
   Load the matching `ghcr.io/orca-ae/orca-ai-gateway` image as
   `orca-ai-gateway-kind:e2e`.
5. Workspace dependencies installed with `pnpm install`.

Run with an absolute Gateway chart path:

```bash
ORCA_GATEWAY_CHART_DIR=/absolute/path/to/orca-ai-gateway \
KIND_HELM_NAMESPACE=orca-kind-e2e \
pnpm e2e:kind-helm
```

Default local port-forwards use `18080`, `18081`, `18082`, `18090`, `18091`,
and `18099`. Concurrent runs must assign unused values through
`KIND_HELM_REGISTRY_PORT`, `KIND_HELM_REGISTRY_INTERNAL_PORT`,
`KIND_HELM_REGISTRY_ADMIN_PORT`, `KIND_HELM_GATEWAY_PORT`,
`KIND_HELM_FIXTURE_PORT`, and `KIND_HELM_GATEWAY_ADMIN_PORT`.

The driver reads four more variables, two of which the example above uses
without naming them as a set:

| Var                        | Default                      | Meaning                                                                                                                                                             |
| -------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KIND_HELM_NAMESPACE`      | `orca-kind-e2e`              | Namespace the release is installed into.                                                                                                                            |
| `ORCA_MANAGED_CHART_DIR`   | `charts/orca-managed-agents` | This repo's chart. A relative value resolves against the repo root.                                                                                                 |
| `ORCA_GATEWAY_CHART_DIR`   | _(unset)_                    | The unpacked Gateway chart directory. Required — the run asserts it was set, then reads its `Chart.yaml`.                                                           |
| `KIND_HELM_E2E_TIMEOUT_MS` | `180000`                     | How long the driver polls session events waiting for an agent turn to finish. Command, image-load and rollout steps carry their own fixed timeouts and ignore this. |

The driver intentionally leaves the namespace and cluster-scoped resources in
place so failures remain inspectable. Run it only once per disposable cluster,
then delete that cluster explicitly, for example:

```bash
kind delete cluster --name orca-kind-helm-e2e
```

The CI workflow performs this deletion in an `always()` step after collecting
redacted diagnostics.

[`e2e-kind-helm.yml`](../../.github/workflows/e2e-kind-helm.yml) builds the
Registry and Harness images and pulls the released Gateway image in parallel,
imports them in its Kind job, and installs the Gateway chart at its pinned
version as the independent release. Chart rendering plus E2E TypeScript
typecheck/lint run in an ungated job for every pull request. A separate
secret-free Kind job runs `pnpm test:chart:secret-store-upgrade`, covering the
legacy values rejection, selector-safe Deployment replacement, and SecretStore
upgrade persistence. The access-check job skips the Gateway image preparation
and the full split-chart Kind run for fork pull requests, which cannot read the
repository secrets those jobs use.

## Runtime Defaults

The chart defaults to `SANDBOX_RUNTIME=opensandbox` so a self-hosted deployment does not require E2B.

Required OpenSandbox values:

```yaml
harness:
  sandboxRuntime: opensandbox
  openSandbox:
    domain: opensandbox-server.opensandbox-system.svc.cluster.local
    protocol: http
    image: ghcr.io/orca-ae/orca-opensandbox-code-interpreter@sha256:<digest>
    entrypoint: /opt/code-interpreter/code-interpreter.sh
    useServerProxy: 'true'
```

`entrypoint` must be the tool image's trusted entrypoint. Without it the adapter
requests its default `tail -f /dev/null`, which the patched server does not trust,
so sandbox acquisition fails.

Install [`charts/opensandbox-patches`](../../charts/opensandbox-patches) beside
the upstream OpenSandbox chart and mount its three ConfigMap keys over the
server v0.2.2 Kubernetes provider modules. OpenSandbox config must set
`secure_runtime.type=gvisor` and `secure_runtime.k8s_runtime_class=gvisor`.
Every isolation request must match operator repository or exact-image trust
with an exact entrypoint array
and include both `bootstrap.execd.isolation=enable` and
`orca.fuse.device=enable`. Generated Pods use `runtimeClassName=gvisor`, add
only `SETFCAP` and `SYS_ADMIN`, and contain no host FUSE device, `hostPath`, privileged
container, host namespace, `hostUsers`, or `procMount`. Admission installed by
companion chart enforces same outer boundary. Agent commands remain inside
Bubblewrap with no capabilities and synthetic `/dev`.

Sandbox image contains `s3fs` + `fuse3`; gVisor supplies `/dev/fuse` inside
sandbox kernel. Nodes need installed gVisor handler and `RuntimeClass/gvisor`.
Keep OpenSandbox API infrastructure-only.
Existing deployments with an externally created `opensandbox` Namespace set
`dataplaneNamespace.create=false` on the companion chart.
Every acquisition probes both binaries, opens device, and performs temporary
mount/unmount; failure aborts setup.
Agent Bubblewrap namespaces retain no capability or FUSE device. OpenSandbox
intentionally does not fall back to test-only local memory. See
[`../opensandbox/README.md`](../opensandbox/README.md).

Patched server defaults to two exact repositories:
`ghcr.io/orca-ae/orca-opensandbox-code-interpreter` and
`ghcr.io/orca-ae/sandbox-harness-claude-code`. Their entrypoints are respectively
`["/opt/code-interpreter/code-interpreter.sh"]` and
`["/usr/local/bin/orca-sandbox-harness"]`. Valid explicit tags, `sha256` digests,
and tag + digest references are accepted; bare repositories are not.
`ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES`, a JSON array of registry/namespace
prefixes, trusts the same two images under mirror prefixes instead of
`ghcr.io/orca-ae`.

Operator JSON `ORCA_TRUSTED_SANDBOX_REPOSITORIES` entries contain `repository`
and `entrypoint` and replace these defaults (setting both variables fails server
initialization); `[]` disables repository trust.
Custom exact repositories/entrypoints can be operator-authorized. Legacy
`ORCA_TRUSTED_SANDBOX_WORKLOADS` remains additive exact image-string + entrypoint
trust and requires digests unless local/CI
`ORCA_ALLOW_TAGGED_SANDBOX_IMAGES=true` is set. Exact-image-only operators must
set repositories to `[]`. Repository trust assumes publisher ACLs and proxy
integrity: mutable tags are not cryptographic release verification, and the
policy installs no signing infrastructure. See the
[deployment policy and release configuration](../opensandbox/README.md#repository-and-exact-image-trust).

Runtime package installation rejects before
agent Bubblewrap. Pause/resume rejects before lifecycle or CR patch because Pod
reconstruction loses live mounts.

AgentENV is an alternative external runtime:

```yaml
harness:
  sandboxRuntime: agentenv
  agentEnv:
    baseUrl: http://agentenv-gateway.agentenv-system.svc.cluster.local:8080
    image: ghcr.io/your-org/orca-agentenv@sha256:<digest>
    timeoutSeconds: '1800'
    requestTimeoutSeconds: '180'
    cpuCount: '2'
    memoryMB: '2048'
secrets:
  values:
    agentEnvApiKey: <agentenv-api-key>
```

The AgentENV gateway, scheduler, and privileged runtime DaemonSet are deployed
outside this chart. Runtime nodes provide nested KVM, `ublk_drv`, and the
validated host kernel.

`AgentEnvRuntime` cold-starts the configured OCI image with `secure=true`,
executes commands through envd ConnectRPC, and transfers files through envd's
HTTP Files API. The AgentENV API key remains in harness-server; sandbox proxy
traffic uses the per-sandbox envd token. This adapter reports no FUSE support,
so memory stores and execution outputs use the Files API fallback and sandbox
filesystem indexing. The image contract is
[`../../services/harness-server/sandbox-templates/orca-agentenv/README.md`](../../services/harness-server/sandbox-templates/orca-agentenv/README.md).

## External MinIO

MinIO works because the app uses S3-compatible APIs.

```yaml
objectStorage:
  endpoint: http://minio.minio.svc.cluster.local:9000
  # Set only when MinIO also provides the STS API; otherwise leave empty.
  stsEndpoint: http://minio.minio.svc.cluster.local:9000
  bucket: orca-files
  region: us-east-1
  keyPrefix: managed-agents/
  stsRoleArn: arn:aws:iam::123456789012:role/orca-session-s3
```

`objectStorage.endpoint` configures only the S3 data plane. The chart passes
`objectStorage.stsEndpoint` separately to the Harness; when it is empty, the
AWS SDK uses its default STS endpoint. `objectStorage.forcePathStyle` applies
to both host-side AWS SDK clients and sandbox s3fs mounts.

Put the MinIO credentials in the chart-created secret or an existing secret:

```yaml
secrets:
  values:
    s3AccessKeyId: minioadmin
    s3SecretAccessKey: minioadmin
```

Setting both chart-created static credential values while `stsRoleArn` is
empty explicitly enables the non-isolating development fallback. With an
existing Secret, set `objectStorage.allowStaticCredentials=true` to make the
same development-only choice. Production must configure `stsRoleArn`.
Registry and Harness use the AWS SDK default credential chain when static S3
keys are absent, so attach direct object-store access to both workload
identities and allow Harness to assume the session role. For example with EKS
IRSA annotations:

```yaml
objectStorage:
  endpoint: https://s3.us-west-1.amazonaws.com
  forcePathStyle: false
  bucket: <bucket>
  region: us-west-1
  keyPrefix: managed-agents/
  stsRoleArn: arn:aws:iam::123456789012:role/orca-session-s3
```

```yaml
registry:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/orca-registry
harness:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/orca-harness
```

The role selected by `objectStorage.stsRoleArn` is a separate session role. It
trusts the Harness workload role for `sts:AssumeRole`; its base S3 policy covers
the deployment object root, while Harness supplies a narrower per-execution
inline session policy for output and attached MemoryStore prefixes.

For production, prefer an existing application secret:

```yaml
secrets:
  create: false
  existingSecret: orca-managed-agents-secrets
```

By default this Secret also supplies database DSNs. A bootstrap pipeline can
manage DSNs in a separate Secret instead:

```yaml
external:
  databases:
    existingSecret: orca-cell-postgres
secrets:
  create: false
  existingSecret: orca-managed-agents-secrets
```

The database Secret uses fixed keys and is selected consistently by Registry,
the registry migration initContainer/Job, and Harness:

```text
DATABASE_URL
TRANSCRIPT_STORE_DATABASE_URL
FILESTORE_DATABASE_URL
MEMORYSTORE_DATABASE_URL
```

When `external.databases.existingSecret` is unset, those four keys must instead
exist in `secrets.existingSecret` or are generated from
`external.databases.*Url` in the chart-created Secret. When the separate Secret
is set, the chart-created application Secret omits all four DSN keys.

The application Secret must provide these keys when the corresponding feature
is enabled. S3 static keys are optional and should be omitted when using IRSA
or another AWS default-chain source:

```text
SESSION_JWT_PRIVATE_KEY_PEM
SESSION_JWT_PUBLIC_KEY_PEM
ANTHROPIC_API_KEY
S3_ACCESS_KEY_ID
S3_SECRET_ACCESS_KEY
S3_ACCESS_KEY
S3_SECRET_KEY
KAFKA_AUTH_TOKEN
KAFKA_SASL_USERNAME
KAFKA_SASL_PASSWORD
PULSAR_AUTH_TOKEN
PULSAR_OAUTH2_CLIENT_SECRET
PULSAR_OAUTH2_PRIVATE_KEY
OPEN_SANDBOX_API_KEY
E2B_API_KEY
```

When configured, `S3_ACCESS_KEY` / `S3_SECRET_KEY` are read by
`registry-service-ts`, and `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` are read
by `harness-server`. Both services accept either complete alias pair. Partial
or cross-pair configuration is rejected; when both complete pairs are present,
their values must match. When neither pair is configured, AWS SDK default
credential chain is used.

### Credentials delivered by an external pipeline

A provisioning pipeline (bootstrap automation, external-secrets-operator, SOPS)
usually delivers several Secrets, each with its own name and key layout, rather
than one Secret using the chart's key names. Two values cover that without the
chart owning any Secret:

- `<component>.secretKeyRefs` points one env var at a named Secret and key.
- `<component>.extraEnvFrom` injects whole bundles. It is appended after the
  chart ConfigMap and application Secret. Kubernetes resolves duplicate keys in
  `envFrom` order, so a bundle wins there; explicit `secretKeyRefs` and the
  selected database Secret still win because Kubernetes `env` entries take
  precedence over every `envFrom` source.

```yaml
secrets:
  create: false
  existingSecret: '' # no chart Secret at all
registry:
  secretKeyRefs:
    DATABASE_URL:
      name: orca-postgres
      key: registry-url
    FILESTORE_DATABASE_URL:
      name: orca-postgres
      key: filestore-url
    MEMORYSTORE_DATABASE_URL:
      name: orca-postgres
      key: memorystore-url
    SESSION_JWT_PRIVATE_KEY_PEM:
      name: orca-session-jwt
      key: private.pem
harness:
  extraEnvFrom:
    - secretRef:
        name: orca-harness-credentials
sessionJwt:
  publicKeySecret:
    name: orca-session-jwt
    key: public.pem
```

If the Postgres Secret already uses the four fixed DSN key names, set
`external.databases.existingSecret` instead and omit the three database entries
from `registry.secretKeyRefs`; only the Registry private JWT key still needs an
application credential source.

With no chart Secret and no explicit source, Helm fails the render rather than
shipping pods that fall back to the services' localhost DSN defaults. Explicit
`env` entries beat every `envFrom` source, so Registry, Harness, and both
Registry migration modes use the same selected database Secret even when an
extra bundle contains a duplicate DSN.

`sessionJwt.publicKeySecret` is the trust anchor the ai-gateway mounts; the
private half reaches Registry through `registry.secretKeyRefs`, which keeps a
rotated keypair in one externally managed Secret.

Chart-managed JWT public-key changes alter the ai-gateway pod-template
checksum, so Helm rolls it together with Registry. Helm cannot observe data
changes inside an externally managed Secret, and both the Registry env var and
ai-gateway `subPath` mount remain stale until restart. Rotate an external
keypair by updating the Secret and bumping the same durable version annotation
on both workloads in the Helm values, for example:

```yaml
registry:
  podAnnotations:
    orca.ai/session-jwt-key-version: '2026-07-30-1'
aiGateway:
  podAnnotations:
    orca.ai/session-jwt-key-version: '2026-07-30-1'
```

Apply the Secret and annotation change in the same controlled upgrade. For an
imperative rotation, restart both Deployments after the Secret update; keep the
annotation method for GitOps so the rollout trigger remains declared.

## Deployment shape overrides

Each component (`registry`, `harness`, `aiGateway`) accepts the same overrides
for what the values above do not cover:

| Value                                               | Purpose                                                                                                                                                                                                  |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `nameOverride`                                      | Pins the Deployment/Service name when in-cluster DNS names are already baked into callers outside this release. The Registry internal and admin Services append `-internal`/`-admin`.                    |
| `extraVolumes` / `extraVolumeMounts`                | Extra files in the pod: a custom trust bundle, a second JWT trust anchor during rotation, broker TLS material. `pg-ca` is reserved when `external.databases.tls` enables the built-in Postgres CA mount. |
| `extraEnv` / `extraEnvFrom`                         | Additional env entries and bundles.                                                                                                                                                                      |
| `podAnnotations` / `podLabels`                      | Mesh, scrape, or policy metadata.                                                                                                                                                                        |
| `automountServiceAccountToken`                      | Defaults to `true` for Registry (SecretStore + TokenReview API calls) and `false` for Harness and ai-gateway, which authenticate with projected audience-bound tokens instead.                           |
| `startupProbe` / `readinessProbe` / `livenessProbe` | Full probe objects; set one to `null` to drop it. Nested maps deep-merge with the chart defaults, so `initialDelaySeconds: null` is how an inherited key is removed.                                     |

Registry and Harness both run migrations before their listener binds, so both
default to a startup probe with `failureThreshold: 30` and `periodSeconds: 5`
(150s of boot headroom) rather than stretching the liveness probe, which would
otherwise restart the pod mid-migration.

The pod templates carry a `checksum/config` annotation. Registry and Harness
also carry `checksum/secret` when the chart creates the application Secret;
ai-gateway carries a checksum of the chart-managed session-JWT public key. This
makes `helm upgrade` roll affected workloads when rendered configuration
changes. External `envFrom`, `secretKeyRef`, and Secret data updates remain
invisible without the explicit annotation mechanism above.

## Installing the ai-gateway separately

The gateway also ships its own chart (`orca-ai-gateway`). Set
`aiGateway.enabled=false` to skip the copy embedded here: the Deployment,
Service and ConfigMap are dropped, while Registry keeps authorizing the
gateway's ServiceAccount subject on its internal resolver routes.

```yaml
aiGateway:
  enabled: false
  serviceAccount:
    create: false
    name: ai-gateway # the identity created by the other chart
harness:
  aiGatewayUrl: http://ai-gateway:8080/v1/mcp
```

`harness.aiGatewayUrl` becomes required, since this release renders no gateway
Service for the harness to dial.

## External Postgres

The registry, transcript Postgres backend, file store, and memory store each use a Postgres connection string. They can point at separate databases on the same cluster:

```yaml
external:
  databases:
    registryUrl: postgres://orca:REDACTED@postgres.example:5432/registry
    transcriptStoreUrl: postgres://orca:REDACTED@postgres.example:5432/transcriptstore
    filestoreUrl: postgres://orca:REDACTED@postgres.example:5432/filestore
    memorystoreUrl: postgres://orca:REDACTED@postgres.example:5432/memorystore
```

Alternatively, set `external.databases.existingSecret` to a Secret containing
the four fixed DSN keys documented above. This avoids placing per-cell database
credentials in the application Secret while keeping service and migration
workloads on the same connection source.

When PgBouncer is in front of Postgres, `DATABASE_URL`,
`FILESTORE_DATABASE_URL`, and `MEMORYSTORE_DATABASE_URL` must use a direct or
session-pooling endpoint. Their migration paths hold session-level advisory
locks across DDL, which transaction pooling cannot preserve. The Postgres
transcript backend is compatible with transaction pooling: its schema migration
uses `pg_advisory_xact_lock` inside one transaction, and runtime event delivery
polls rows without `LISTEN`/`NOTIFY` session state. RDS Proxy supports the
session-level locks by pinning affected connections; account for the resulting
reduction in multiplexing when sizing capacity.

The registry Deployment runs an initContainer before the service container:

```text
node dist/migrate.js
```

That initContainer applies registry service migrations under a session-scoped
Postgres advisory lock held by one dedicated client for the full migration.
File-store and memory-store migrations still run during registry/harness startup,
but each uses the same dedicated-client locking pattern so concurrent replicas
serialize their DDL. Postgres transcript migrations use a transaction-scoped
advisory lock. The chart also includes an optional standalone migration Job at
`migrations.registry.job.enabled`, but it is disabled by default because the
initContainer is the safer install path. Migration containers receive the same
application Secret and `registry.extraEnvFrom` bundles as Registry. When
`external.databases.existingSecret` is set, their explicit `DATABASE_URL`
reference uses that selected database Secret.

A managed Postgres reached with `sslmode=verify-full` needs its CA bundle inside
the pod, and the DSN's `sslrootcert` must point at the mount path:

```yaml
external:
  databases:
    tls:
      caSecretName: orca-pg-ca
      caKey: root-ca.pem
      mountPath: /etc/orca/pg-ca
```

The chart mounts that CA into Registry, its migration initContainer or Job, and
Harness. Append
`?sslmode=verify-full&sslrootcert=/etc/orca/pg-ca/root-ca.pem` (or the equivalent
query separator) to every affected DSN. Component `extraVolumes` and
`extraVolumeMounts` remain available for nonstandard trust layouts.

## Transcript Backend

Choose one transcript backend:

### Kafka

Kafka transcripts default to raw payload bytes. Optional Avro envelope encoding and
Schema Registry settings live under `transcriptStore.kafka.encoding` and
`transcriptStore.kafka.schemaRegistry`, shared by registry, harness and exporter.
Encoding selects raw `.events` or Avro `.events-avro` topics for all sessions; a
Registry URL alone leaves raw topic selection unchanged. Switching requires
quiescing existing turns and a full coordinated restart of all three services with
the same encoding. This incompatible cutover does not migrate history or existing
session cursors; create fresh sessions and follow the exporter-state precautions below.
See [secure Helm configuration](../operation/kafka-transcript-avro.md) for independent
Registry Secret references, explicit TLS mounts and state isolation, and the [coordinated cutover and
external consumer contract](./libraries/transcript-store.md#optional-kafka-avro-envelope).

```yaml
transcriptStore:
  backend: kafka
  kafka:
    brokers: kafka-bootstrap.kafka.svc.cluster.local:9092
    connectionMode: plaintext
    topicRediscoverIntervalMs: '30000'
```

For SASL/TLS-backed Kafka, set `connectionMode` and the matching secret keys documented in `values.yaml`.

### Postgres

```yaml
transcriptStore:
  backend: postgres
```

For a chart-created Secret, `TRANSCRIPT_STORE_DATABASE_URL` comes from
`external.databases.transcriptStoreUrl` and falls back to the Registry
database URL when a dedicated transcript database is not configured. An
existing Secret must contain `TRANSCRIPT_STORE_DATABASE_URL`; the Harness pod
fails closed at startup when the Postgres backend selects a Secret without it.
When neither the chart nor `external.databases.existingSecret` supplies DSNs,
Helm requires `harness.secretKeyRefs.TRANSCRIPT_STORE_DATABASE_URL` or a
`harness.extraEnvFrom` bundle before rendering the Postgres backend.

### Pulsar

Harness consumers use `KeyShared` with a session key to preserve event ordering
across replicas. When upgrading consumers that use the older `Shared` subscription,
pause new input, drain active turns, stop all consumers attached to the harness
subscription, and start the updated replicas with that same subscription name.
Pulsar changes the subscription type once no consumers are attached; keeping the
name retains its cursor. Do not mix the two consumer types or delete the subscription
to perform this upgrade. See the [Pulsar subscription contract](https://pulsar.apache.org/docs/3.3.x/concepts-messaging/#subscription-types).

```yaml
transcriptStore:
  backend: pulsar
  pulsar:
    serviceUrl: pulsar://pulsar-broker.pulsar.svc.cluster.local:6650
    tenant: public
    namespace: default
    topicPrefix: orca
```

## Dynamic MCP destinations

Chart enables Registry-backed wildcard resolution by default. Optional exact
destinations are emitted first and override wildcard by logical backend name:

```yaml
aiGateway:
  destinations: {} # optional exact-name overrides; "*" is reserved
  destinationResolver:
    urlTemplate: '' # defaults to <registryInternalUrl>/internal/v1/.../mcp-destination/resolve
    timeoutMs: 5000
    egress:
      allowedPrivateHosts: []
      dnsTimeoutMs: 2000
      connectTimeoutMs: 5000
```

Registry resolver auth uses AI Gateway's projected ServiceAccount token file;
no token literal appears in values or ConfigMap. Keep the host allowlist empty
unless a Session MCP endpoint intentionally resolves to a private address or
must use plaintext behind a service mesh. Each entry explicitly permits both
private/special resolution and HTTP for that hostname. Dynamic upstream `3xx`
responses become `502`, proxy environment is ignored, and DNS is pinned for
connect.

## ai-gateway Audit Kafka

The ai-gateway audit sink is configured separately from the registry/harness transcript store settings. Even when `TRANSCRIPT_STORE_BACKEND=postgres` or `pulsar`, the gateway contract expects a Kafka audit sink:

```yaml
aiGateway:
  audit:
    kafka:
      brokers: kafka-bootstrap.kafka.svc.cluster.local:9092
      topicTemplate: orca.{scope.workspace_id}.audit.ai-gateway
      clientId: ai-gateway
      messageTimeoutMs: 5000
      ssl: false
```

Use self-hosted Kafka or another Kafka-compatible broker if you want to avoid paid managed services.

For TLS/SASL-backed Kafka, configure the ai-gateway audit producer directly
under `aiGateway.audit.kafka`; it is separate from the registry/harness
`transcriptStore.kafka` settings:

```yaml
aiGateway:
  audit:
    kafka:
      brokers: kafka-bootstrap.kafka.svc.cluster.local:9093
      ssl: true
      sslCaLocation: /etc/kafka/ca.pem
      sasl:
        enabled: true
        mechanism: PLAIN
        username: public
        password: token:REDACTED
```

## Public Access

Only `registry-service-ts` should be exposed publicly:

```yaml
registry:
  # Ingress-controller source range, not arbitrary client ranges. Fastify
  # accepts X-Forwarded-For only when the immediate proxy is in this allowlist.
  trustedProxyCidrs:
    - 10.42.0.0/16
  ingress:
    enabled: true
    className: nginx
    hosts:
      - host: agents.example.com
        paths:
          - path: /
            pathType: Prefix
```

Alternatively, route through an existing Istio ingress gateway:

```yaml
registry:
  # Istio ingress-gateway source range, not arbitrary client ranges.
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      selector:
        istio: ingressgateway
      tls:
        # Secret visible to the selected ingress gateway workload.
        credentialName: agents-example-com-tls
```

The chart rejects enabling `registry.ingress` and `registry.istio` together.
`registry.istio.gateway.selector` must name the gateway workload labels
explicitly (Helm deep-merges maps, so the chart ships no default).
`registry.trustedProxyCidrs` is required for either exposure mode. Use the
narrowest ingress-controller or Istio ingress-gateway Pod/node IP or CIDR set
available; never use `0.0.0.0/0` or `::/0`. Outside Helm, configure the same
allowlist through `TRUST_PROXY_CIDRS`. Without it, Fastify ignores forwarded
client addresses and legacy API-key source quotas use the direct socket
address.

The `Gateway` and the deny-by-default `AuthorizationPolicy` are created in
`registry.istio.gatewayNamespace` (default `istio-system`), which must be the
namespace of the selected ingress gateway workload: Istio resolves the TLS
`credentialName` Secret from the gateway workload's namespace,
`AuthorizationPolicy` only matches workloads in the policy's own namespace,
and meshes running pilot with `PILOT_SCOPE_GATEWAY_TO_NAMESPACE=true` ignore
`Gateway` resources from other namespaces. The `VirtualService` stays in the
release namespace and references the `Gateway` with a namespace-qualified
name.

Istio exposure also renders a deny-by-default `AuthorizationPolicy` on the
selected gateway workloads (`registry.istio.authorizationPolicy`, enabled by
default): requests to the exposed host outside
`registry.istio.authorizationPolicy.allowedPaths` (default `/v1/*`, `/api`,
`/api/v1/*`, `/apis`, and `/apis/*`) are rejected at the gateway before reaching
the registry. These defaults expose core routes, discovery, the core alias, and
discoverable extension groups while keeping other paths off the public host.
Istio evaluates DENY rules that carry
HTTP-only attributes as always-matching for plain-TCP traffic, so the rule is
additionally scoped to `registry.istio.authorizationPolicy.gatewayHttpsPorts`
(default `443` and `8443`, the standard istio-ingressgateway HTTPS workload
port) and to connections whose TLS SNI equals `registry.istio.host`; on a
shared gateway it therefore cannot deny other tenants' HTTP hosts or
TCP/TLS-passthrough listeners. Prefer a dedicated gateway deployment where
available. Set `enabled: false` to manage gateway policy elsewhere.
The identity running Helm install, upgrade, and uninstall must have RBAC to
manage `networking.istio.io` `Gateway` and `security.istio.io`
`AuthorizationPolicy` resources in that namespace.
Namespace-scoped release automation should set `enabled: false` and provision
the equivalent gateway policy through its cluster-policy pipeline.

`harness-server` and `ai-gateway` remain ClusterIP services. The harness rewrites MCP traffic to the internal ai-gateway service, and ai-gateway resolves destinations and vault credentials through the internal registry service URL. Harness cannot call either gateway-only resolver.

If sessions use `github_repository` mounts, set a URL reachable from the sandbox:

```yaml
harness:
  gitCredsPublicUrl: https://agents.example.com/v1/git-creds
```
