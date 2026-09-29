# Compatibility of closed components with Orca Agent Engine v0.5.0

Orca Agent Engine v0.5.0 works with three components whose source is not in this
repository. All three are available as public artifacts under Apache-2.0.

| Component      | Artifact                                                                                           | Version tested with v0.5.0 | Notes                                                                                                                                                                                                                                                                                                                          |
| -------------- | -------------------------------------------------------------------------------------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| AI gateway     | Image `ghcr.io/orca-ae/orca-ai-gateway`; Helm chart `oci://ghcr.io/orca-ae/charts/orca-ai-gateway` | `v0.4.3-rc.3`              | The default `images.aiGateway.tag` of `charts/orca-managed-agents` and the local stack's default image. The Pi SDK harness (`pi_sdk`) on native Gateway routes (`aiGateway.piProviders`) requires a gateway build that includes native Pi provider routing. See [the gateway contract](managed-agents/services/ai-gateway.md). |
| CLI `ork`      | Image `ghcr.io/orca-ae/orca-cli`; `brew install orca-ae/tap/ork`                                   | `0.2.0`                    | The chart's default `toolset` image. See [the Orca CLI toolset](managed-agents/kubernetes.md#orca-cli-toolset).                                                                                                                                                                                                                |
| TypeScript SDK | npm package `@runorca/orca-sdk`                                                                    | —                          | Published on npm.                                                                                                                                                                                                                                                                                                              |

The engine's API contract is `services/registry-service-ts/openapi/managed-agents.yaml`;
[`managed-agents/api-groups-and-extensions.md`](managed-agents/api-groups-and-extensions.md)
describes how a client discovers which API versions and extension groups a deployment
serves.
