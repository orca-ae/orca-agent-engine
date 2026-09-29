// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { internalContract } from '../../src/contracts/internal.contract.js';
import { AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH } from '../../src/domain/agent-observability-validation.js';

const resolveContract = internalContract.resolveAgentObservabilityContext;
const secretContract = internalContract.resolveAgentObservabilitySecret;

function enabledResponse() {
  return {
    schema_version: 1,
    status: 'enabled' as const,
    reason: null,
    organization_id: 'org_context',
    workspace_id: 'ws_context',
    session_id: 'ses_context',
    agent: { id: 'agt_context', version: 1 },
    harness: 'codex',
    harness_mode: 'colocated',
    selection_source: 'organization_default' as const,
    binding: {
      id: 'aob_context',
      version: 1,
      scope: 'organization' as const,
      workspace_id: null,
      target: {
        adapter_type: 'otlp_http' as const,
        endpoint_kind: 'traces_endpoint' as const,
        endpoint_class: 'public' as const,
        endpoint_url: 'https://collector.example/v1/traces',
        external_project_id: 'project-context',
      },
      lifecycle_status: 'active' as const,
      config: {
        semantic_profile: 'langfuse' as const,
        protocol: 'http/protobuf' as const,
        compression: 'none' as const,
        timeout_ms: 5_000,
        environment: 'test',
        release: 'r1',
        capture_mode: 'redacted_io' as const,
        sample_rate: 0.5,
        config_schema_version: 1,
      },
      current_credential_version: 7,
    },
    capture: {
      pinned_mode: 'redacted_io' as const,
      effective_mode: 'metadata_only' as const,
      current_ceilings: {
        platform: 'redacted_io' as const,
        organization: 'redacted_io' as const,
        workspace: 'metadata_only' as const,
      },
    },
    epochs: {
      pinned: epochs(1),
      current: epochs(2),
    },
  };
}

function epochs(value: number) {
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

function basicSecretResponse(username = 'test-user', password = 'test-password') {
  return {
    schema_version: 1,
    authorization_id: 'obsauth_0123456789ABCDEFGHJK',
    binding_id: 'aob_context',
    binding_version: 1,
    credential_version: 7,
    effective_capture_mode: 'metadata_only',
    bundle: {
      adapter_type: 'otlp_http',
      auth: { type: 'basic', username, password },
    },
  };
}

describe('Session observability context internal contract', () => {
  it('uses a workspace/session-scoped POST with an exact empty object body', () => {
    expect(resolveContract.method).toBe('POST');
    expect(resolveContract.path).toBe(
      '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/context/resolve',
    );
    expect(
      resolveContract.pathParams.safeParse({ workspaceId: 'ws_context', sessionId: 'ses_context' })
        .success,
    ).toBe(true);
    expect(
      resolveContract.pathParams.safeParse({ workspaceId: 'ws.context', sessionId: 'ses_context' })
        .success,
    ).toBe(false);
    expect(
      resolveContract.pathParams.safeParse({
        workspaceId: 'w'.repeat(129),
        sessionId: 'ses_context',
      }).success,
    ).toBe(false);
    const maxSessionId = `ses_${'s'.repeat(124)}`;
    expect(maxSessionId).toHaveLength(128);
    expect(
      resolveContract.pathParams.safeParse({ workspaceId: 'ws_context', sessionId: maxSessionId })
        .success,
    ).toBe(true);
    expect(
      resolveContract.pathParams.safeParse({
        workspaceId: 'ws_context',
        sessionId: `${maxSessionId}s`,
      }).success,
    ).toBe(false);
    expect(
      resolveContract.pathParams.safeParse({ workspaceId: 'ws_context', sessionId: 'sesn_context' })
        .success,
    ).toBe(false);
    expect(resolveContract.body.safeParse({}).success).toBe(true);
    expect(resolveContract.body.safeParse({ force: true }).success).toBe(false);
    expect(resolveContract.body.safeParse([]).success).toBe(false);
  });

  it('declares a strict sibling secret release with no secret reference field', () => {
    expect(secretContract.method).toBe('POST');
    expect(secretContract.path).toBe(
      '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/secret/resolve',
    );
    expect(secretContract.pathParams).toBe(resolveContract.pathParams);
    expect(secretContract.body).toBe(resolveContract.body);
    expect(secretContract.body.safeParse({}).success).toBe(true);
    expect(secretContract.body.safeParse({ selector: 'forbidden' }).success).toBe(false);

    const response = basicSecretResponse();
    expect(secretContract.responses[200].safeParse(response).success).toBe(true);
    expect(secretContract.responses[200].safeParse(basicSecretResponse('user:name')).success).toBe(
      false,
    );
    expect(
      secretContract.responses[200].safeParse({
        ...response,
        bundle: { adapter_type: 'otlp_http', auth: { type: 'bearer', token: 'test-token' } },
      }).success,
    ).toBe(true);
    expect(
      secretContract.responses[200].safeParse({
        ...response,
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'custom_headers', headers: { 'x-tenant-key': 'test-value' } },
        },
      }).success,
    ).toBe(true);
    expect(
      secretContract.responses[200].safeParse({
        ...response,
        bundle: {
          adapter_type: 'langfuse_sdk',
          public_key: 'pk-test',
          secret_key: 'sk-test',
        },
      }).success,
    ).toBe(true);
    expect(
      secretContract.responses[200].safeParse({
        ...response,
        secret_ref: 'local:agent_observability/obssec_0123456789ABCDEFGHJK',
      }).success,
    ).toBe(false);
    expect(
      secretContract.responses[200].safeParse({
        ...response,
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'custom_headers', headers: { authorization: 'forbidden' } },
        },
      }).success,
    ).toBe(false);
    expect(
      secretContract.responses[200].safeParse({
        ...response,
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'basic', username: 'test-user', password: 'test-password', token: 'nope' },
        },
      }).success,
    ).toBe(false);
    expect(
      secretContract.responses[200].safeParse({
        ...response,
        bundle: {
          adapter_type: 'langfuse_sdk',
          public_key: 'pk-test',
          secret_key: 'sk-test',
          auth: { type: 'basic' },
        },
      }).success,
    ).toBe(false);
  });

  it.each([
    ['lone high surrogate', '\ud800'],
    ['lone low surrogate', '\udc00'],
  ])('rejects a maximum-length Basic secret containing a %s', (_label, surrogate) => {
    const malformed = `${'\u0800'.repeat(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH - 1)}${surrogate}`;
    expect(malformed).toHaveLength(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH);

    expect(
      secretContract.responses[200].safeParse(basicSecretResponse('valid-user', malformed)).success,
    ).toBe(false);
  });

  it('accepts the explicit non-secret response and rejects secret-shaped additions', () => {
    const response = enabledResponse();
    expect(resolveContract.responses[200].safeParse(response).success).toBe(true);
    expect(
      resolveContract.responses[200].safeParse({
        ...response,
        binding: { ...response.binding!, secret_ref: 'must-never-leak' },
      }).success,
    ).toBe(false);
    expect(
      resolveContract.responses[200].safeParse({ ...response, secret_value: 'must-never-leak' })
        .success,
    ).toBe(false);
    expect(
      resolveContract.responses[200].safeParse({
        ...response,
        authorization: 'Bearer must-never-leak',
      }).success,
    ).toBe(false);
  });

  it('enforces the status/reason relationship', () => {
    const response = enabledResponse();
    expect(
      resolveContract.responses[200].safeParse({ ...response, reason: 'binding_disabled' }).success,
    ).toBe(false);
    expect(
      resolveContract.responses[200].safeParse({
        ...response,
        status: 'suppressed',
        reason: 'binding_disabled',
      }).success,
    ).toBe(true);
    expect(
      resolveContract.responses[200].safeParse({
        ...response,
        status: 'disabled',
        reason: 'session_pin_disabled',
      }).success,
    ).toBe(true);
  });
});
