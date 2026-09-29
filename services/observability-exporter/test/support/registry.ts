// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export function enabledRegistryContext(
  workspaceId = 'ws_registry',
  sessionId = 'ses_registry',
): Record<string, unknown> {
  return {
    schema_version: 1,
    status: 'enabled',
    reason: null,
    organization_id: 'org_registry',
    workspace_id: workspaceId,
    session_id: sessionId,
    agent: { id: 'agt_registry', version: 1 },
    harness: 'claude_code',
    harness_mode: 'colocated',
    selection_source: 'organization_default',
    binding: {
      id: 'aob_registry',
      version: 2,
      scope: 'organization',
      workspace_id: null,
      target: {
        adapter_type: 'otlp_http',
        endpoint_kind: 'traces_endpoint',
        endpoint_class: 'public',
        endpoint_url: 'https://collector.example/api/public/otel/v1/traces',
        external_project_id: 'project-registry',
      },
      lifecycle_status: 'active',
      config: {
        semantic_profile: 'langfuse',
        protocol: 'http/json',
        compression: 'none',
        timeout_ms: 1_000,
        environment: null,
        release: null,
        capture_mode: 'metadata_only',
        sample_rate: 1,
        config_schema_version: 1,
      },
      current_credential_version: 3,
    },
    capture: {
      pinned_mode: 'metadata_only',
      effective_mode: 'metadata_only',
      current_ceilings: {
        platform: 'raw_io',
        organization: 'raw_io',
        workspace: 'metadata_only',
      },
    },
    epochs: { pinned: epochs(1), current: epochs(2) },
  };
}

export function basicRegistrySecret(): Record<string, unknown> {
  return {
    schema_version: 1,
    authorization_id: 'obsauth_0123456789ABCDEFGHJK',
    binding_id: 'aob_registry',
    binding_version: 2,
    credential_version: 3,
    effective_capture_mode: 'metadata_only',
    bundle: {
      adapter_type: 'otlp_http',
      auth: { type: 'basic', username: 'pk-registry', password: 'sk-registry' },
    },
  };
}

function epochs(value: number): Record<string, number> {
  return {
    organization_selection_epoch: value,
    workspace_selection_epoch: value,
    organization_default_revocation_epoch: value,
    organization_revocation_epoch: value,
    workspace_revocation_epoch: value,
    binding_revocation_epoch: value,
    platform_capture_restriction_epoch: value,
    organization_capture_restriction_epoch: value,
    workspace_capture_restriction_epoch: value,
    session_revocation_epoch: value,
  };
}
