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
Custom profiles use cloud OpenSandbox Environments with unrestricted networking;
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
