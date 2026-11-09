# Orca Managed Agents Helm chart

The chart deploys the registry, harness, gateway and optional observability exporter
against external infrastructure. See [deployment](../../docs/managed-agents/kubernetes.md)
and [values.yaml](./values.yaml) for the complete configuration.

## Native DSH profiles

Approve Agent-bound native profiles through the existing Registry environment:

```yaml
registry:
  extraEnv:
    - name: DSH_APPROVED_PROFILES
      value: '{"ws_example":["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"]}'
```

Use the exact Orca workspace ID authenticated by the API key and the configuration
digest from the [profile packaging helper](../../examples/dsh-profile/README.md).
The default is `{}`: no custom profiles are approved. Registry validates the JSON
at process startup; approval belongs only on Registry, not harness or Gateway.
An explicit `registry.extraEnv` value overrides the same key supplied through
`registry.extraEnvFrom`. Retain existing environment entries when editing the list;
Helm replaces lists instead of appending them.

Changing this inline value changes the Registry Pod template and triggers rollout.
External ConfigMap or Secret contents are not checksummed by Helm; restart Registry
after updating approval through an external reference. Removing an approval blocks
fresh preparation, including replacement and Trigger launch, after Registry reloads
the configuration; it does not terminate an already running turn.

Deploy matching release versions of Registry, harness-server, the DSH sandbox
image and the companion `opensandbox-patches` chart before adding profile approvals.
The published release chart pins `images.sandboxHarness.dsh.digest`; an explicit
image override can retain an older runtime even after chart upgrade. The patch
ConfigMap uses `subPath` mounts, so restart OpenSandbox server after its upgrade.
Custom profiles use cloud OpenSandbox Environments with unrestricted or managed networking;
they reuse existing Gateway routes, credentials and toolset permissions. No new
networking platform or Kubernetes workload is deployed by the engine chart.

The companion provider creates a credential-free init container and Pod-local
volume for each custom-profile sandbox; its resource requirements and read-only
handoff are described in the [companion chart](../opensandbox-patches/README.md#native-dsh-profile-preparation).
See [profile lifecycle and permissions](../../docs/managed-agents/harness-modes.md#agent-bound-native-profiles)
for retention, admission and checkpoint behavior.

## Kafka transcript encoding

`transcriptStore.kafka.encoding` defaults to `raw`. The optional `schemaRegistry`
block contains `url`, `subject`, `autoRegister`, `authMode`, `caFile`, `certFile`,
`keyFile`, and `requestTimeoutMs`. A URL enables Avro frame decoding but raw mode
still reads and writes only `.events`. Selecting `avro` requires a URL and switches
all session reads and writes to `.events-avro`. Registry defaults are emitted
only when the URL is set, and only to registry, harness and exporter ConfigMaps.
Unchanged chart defaults are valid without a URL; nondefault Registry companion
settings require one and are rejected for non-Kafka transcript backends.

Credentials use `secrets.values.kafkaSchemaRegistryUsername` and
`secrets.values.kafkaSchemaRegistryPassword`, `secrets.existingSecret`, or
component-specific `secretKeyRefs`. Explicit refs take precedence over the shared
Secret. The exporter receives only those two keys, not the entire shared Secret.
Broker credentials are independent. TLS files require explicit read-only
`extraVolumes` and `extraVolumeMounts` on each service; the chart never infers
volumes from file paths.

Changes to chart-managed Secret contents update the pod-template Secret checksum
and roll registry, harness and exporter. Helm cannot checksum externally managed
Secret contents: after rotating `secrets.existingSecret` or `secretKeyRefs` values,
explicitly restart the affected Deployments (or use an external Secret reloader).

Encoding changes require quiescing existing turns and a full coordinated restart of
registry, harness and exporter with the same encoding, not a live rolling update.
This is an incompatible cutover: history and existing session cursors are not
migrated; use fresh sessions. Reverting to raw returns to the old set, excluding new
Avro history. Broker-native exporter state/group namespaces are mode-isolated;
SQL-state exporters require fresh dedicated state or an operator-verified reset.

See [secure Helm examples and cutover state precautions](../../docs/operation/kafka-transcript-avro.md) and
[wire format, coordinated cutover and external consumers](../../docs/managed-agents/libraries/transcript-store.md#optional-kafka-avro-envelope).

### Codex SDK separate Sessions

`codex_sdk` defaults to `separate` for cloud Environments. For direct LLM egress,
set `harness.llmEgressDefault=direct` (the chart default) and bind
`harness.secretKeyRefs.OPENAI_API_KEY` to a provider-key Secret (or use
`secrets.values.openaiApiKey` for local testing). An optional `OPENAI_BASE_URL`
can be supplied through `harness.extraEnv`. Gateway egress instead uses the
Session's scoped JWT and the Gateway OpenAI vault configuration. Set
`harness.llmEgressDefault=gateway` to make that the default for separate Sessions;
`metadata.orca_llm_egress` can still override either default. The chart requires
either `registry.aiGatewayLlmUrl` or an enabled in-chart LLM provider when the
deployment default is `gateway`. A Codex SDK Session needs the OpenAI Responses
route in the selected Gateway image.
Enabling `aiGateway.openai.enabled` configures the in-chart LLM endpoint for both
Registry and harness-server. `registry.aiGatewayLlmUrl` overrides that endpoint
for both services when using an external Gateway. Registry MCP snapshots reuse
`harness.aiGatewayUrl`, trimming trailing slashes and appending `/v1/mcp` when
absent; `registry.aiGatewayMcpUrl` overrides that MCP endpoint verbatim
when Registry runners need a different address.

### Pi SDK Sessions

`pi_sdk` supports cloud `separate` and `colocated`, plus self-hosted `colocated`.
Configure each selected provider under `aiGateway.piProviders` and supply its
API key through `aiGateway.extraEnv` Secret references. A nonempty list also sets
the in-chart LLM endpoint for Registry and harness-server. The separate
`aiGateway.anthropic.enabled`, `aiGateway.openai.enabled`, and
`aiGateway.deepseek.enabled` switches configure legacy Messages, Responses, and
Chat routes for other SDK harnesses; they do not create Pi native routes.
Gateway egress requires the selected route and model in the Session JWT policy.
See [Pi harness](../../docs/managed-agents/libraries/pi-harness.md) for model controls,
direct credentials, usage accounting, and native checkpoints.

## Controlled sandbox egress

`sandboxEgress.enabled` defaults to `false`. Disabled installs and upgrades emit no
egress policies, egress RBAC, provider configuration, CA mounts or egress environment
variables and require no Cilium APIs. The enabled configuration identifies an existing
TLS forward proxy; this chart does not install or upgrade Cilium or that listener.

```yaml
sandboxEgress:
  enabled: true
  namespace: opensandbox
  proxyUrl: https://sandbox-proxy.engine.svc:8443
  caBundleConfigMap: sandbox-egress-public-ca
  providerServiceAccount:
    namespace: opensandbox-system
    name: opensandbox-server
  gateway:
    namespace: engine
    podSelector:
      app: sandbox-proxy
    port: 8443
  dns:
    namespace: kube-system
    podSelector:
      k8s-app: kube-dns
  internalBypass:
    - host: registry-internal.engine.svc
      namespace: engine
      podSelector:
        app.kubernetes.io/component: registry
        app.kubernetes.io/instance: engine
      ports:
        - port: 8081
          protocol: TCP
  git:
    githubRestReadEnabled: false
```

Each bypass entry supplies one exact DNS hostname or IPv4 address and a nonempty
namespaced Pod label selector plus explicit numeric TCP/UDP ports. The Harness
receives `SANDBOX_EGRESS_INTERNAL_BYPASS` as a JSON array of those host strings.
`NO_PROXY` is client configuration; the Cilium policy authorizes endpoints and ports.
Wildcards, CIDRs and suffix exemptions such as `.svc` are rejected. DNS is limited
to the configured endpoints on TCP/UDP 53; the proxy is limited to its TCP port.
Other traffic, including direct public/private IPv4/IPv6 and UDP/443, has no allow rule.
Gateway ingress controls belong to the deployment that owns the forward-proxy listener.

The public ConfigMap must exist in `sandboxEgress.namespace` with key `ca-bundle.crt`.
The provider mounts only that key read-only in the trusted network bootstrap init at
`/etc/orca/sandbox-egress/ca-bundle.crt`; no private key or proxy capability is in
chart values or ConfigMaps. Proxy capabilities are supplied by runtime bootstrap.

The chart creates `orca-sandbox-egress-provider` in the configured provider namespace.
Reference it in the operator-owned OpenSandbox server Deployment `envFrom`, and restart
that Deployment when its data or the Python patch changes. It provides:

- `ORCA_SANDBOX_EGRESS_ENABLED=true`;
- `ORCA_SANDBOX_EGRESS_CA_CONFIG_MAP`, the public CA ConfigMap name;
- `ORCA_SANDBOX_EGRESS_REGISTRY_URL`, the trusted internal Registry origin generated
  from the Registry Service name, namespace and internal Service port.

Registry receives fresh Pod, NetworkPolicy, CNP and CEP reads in the sandbox namespace,
Pod reads in the Harness namespace, BatchSandbox reads in the sandbox namespace and
cluster-wide CCNP listing. The Registry ConfigMap's `SANDBOX_EGRESS_POLICY_SHA256` pins
the canonical static CNP specification. The provider performs no Cilium authority reads.
These roles do not permit mutations. Harness receives its own namespace/name/UID through downward
API `SANDBOX_EGRESS_OWNER_POD_NAMESPACE`, `SANDBOX_EGRESS_OWNER_POD_NAME`, and
`SANDBOX_EGRESS_OWNER_POD_UID`. Enabled settings use explicit ConfigMap key references
so an unrelated `extraEnvFrom` cannot override them; chart-owned `SANDBOX_EGRESS_*` explicit
overrides are rejected in this mode.

Enablement requires the CNP, CEP and CCNP APIs. Offline rendering with explicit
`--api-versions cilium.io/v2/CiliumNetworkPolicy`,
`--api-versions cilium.io/v2/CiliumEndpoint`, and
`--api-versions cilium.io/v2/CiliumClusterwideNetworkPolicy` checks configuration,
not dataplane enforcement. Registry checks that the exact pinned CNP exists before issuing network bootstrap
capabilities and audits the relevant NP/CNP/CCNP allow union on current authorization.
Broad legacy allows must exclude `orca.ai/sandbox-egress=enabled` Pods; an additive
allow cannot narrow another allow. Equivalent or narrower DNS/internal endpoint
rules are accepted. Unknown-effective rules and API failures fail closed. No policy
on unrelated workloads is changed automatically. One static CNP is owned per sandbox
namespace; changing that policy requires draining existing opted-in executions.

Registry network authority requires a matching Pod UID in the CEP owner reference,
`status.state=ready`, `status.policy.egress.enforcing=true`, and
`status.policy.egress.state=enforcing`, followed by a fresh union audit. These are
real CEP fields in the Cilium 1.13.2 and 1.18.3 schemas. CEP does not expose
desired/realized policy revisions: this check proves endpoint readiness, not that
the latest policy revision propagated. Controlled direct-deny/proxy-allow tests on
the actual Cilium/OpenSandbox/gVisor deployment remain required qualification evidence.

The trusted `orca.sandbox.network` extension carries only the registered binding ID and
generation. The provider protects `orca.ai/network-binding-id` and `orca.ai/network-generation`
on BatchSandbox and Pod template metadata. A dedicated image-owned network init receives
an audience `orca-sandbox-network` Pod-bound token; installers and main never mount it.
After Registry validates Pod identity and enforcement, init writes the single Sandbox-lifetime
context and combined trust bundle into a memory-backed `/run/orca/sandbox-network` volume.
Independent package and DSH init containers run afterwards with that volume read-only;
main also mounts it read-only. Session credential grants reference this parent network
and only change per-Session process environment. Session cleanup preserves base networking.
There is no installer-to-runtime capability rotation.

Enabled egress requires Cilium to publish endpoint policy status. In the official
[Cilium 1.13.2 Helm values](https://github.com/cilium/cilium/blob/v1.13.2/install/kubernetes/cilium/values.yaml),
set `endpointStatus.enabled: true` and include `policy` in `endpointStatus.status`
(for example, `endpointStatus.status: "policy"`, retaining any existing reporting categories).
The [Cilium chart template](https://github.com/cilium/cilium/blob/v1.13.2/install/kubernetes/cilium/templates/cilium-configmap.yaml)
writes the agent ConfigMap key `endpoint-status`. Cilium's
[endpoint status implementation](https://github.com/cilium/cilium/blob/v1.13.2/pkg/endpoint/endpoint_status.go)
returns `{enforcing: false, state: '<status disabled>'}` when policy reporting is
disabled. That value is not evidence that dataplane enforcement is off; it is
insufficient readiness evidence, so opted-in preparation remains blocked. This
Engine chart does not change the Cilium installation. Disabled egress requires no
Cilium policy-status reporting configuration.

Registry receives `SANDBOX_EGRESS_PROXY_URL` and JSON `SANDBOX_EGRESS_INTERNAL_BYPASS`
for bootstrap responses. CA configuration belongs to the provider/runtime;
Registry receives no CA path or volume. Runner manifests contain only a `session_egress`
grant referencing the existing Sandbox network context, without duplicate proxy/CA delivery.

Managed networking supports `apt`, `pip`, `npm`, `cargo`, `go` and native DSH/pnpm init.
The combined public CA is also projected read-only onto the Debian/Ubuntu platform images'
native `/etc/ssl/certs/ca-certificates.crt` path for installers and main, so older Cargo
versions verify both TLS legs. Bootstrap retains the image's original roots.
Managed-network `gem` plans are rejected before provisioning/download: RubyGems 3.6.7
with Ruby 3.3.8 sends plaintext CONNECT to an HTTPS proxy and cannot complete transport.
Non-managed `gem` preparation retains its existing behavior.
