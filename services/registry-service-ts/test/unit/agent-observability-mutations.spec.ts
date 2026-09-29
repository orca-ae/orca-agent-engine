// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, expectTypeOf, it } from 'vitest';
import {
  AgentObservabilityMutationValidationError,
  agentObservabilityAdminIdempotencyPartition,
  agentObservabilityMutationAuditMetadata,
  agentObservabilityMutationBodyHash,
  agentObservabilityMutationTargetKey,
  parseAgentObservabilityMutationResponse,
  parseAgentObservabilityMutationTarget,
  type FinalizeAgentObservabilityMutationInput,
} from '../../src/domain/agent-observability-mutations.js';

describe('agent observability mutation input kernel', () => {
  it('validates exact target shapes and produces distinct stable target keys', () => {
    const organization = parseAgentObservabilityMutationTarget({
      type: 'organization_setting',
      organizationId: 'org_a',
    });
    const workspace = parseAgentObservabilityMutationTarget({
      type: 'workspace_setting',
      organizationId: 'org_a',
      workspaceId: 'ws_a',
    });
    const binding = parseAgentObservabilityMutationTarget({
      type: 'binding',
      organizationId: 'org_a',
      workspaceId: 'ws_a',
      bindingId: 'aob_a',
      bindingScope: 'workspace',
    });

    expect(
      new Set([
        agentObservabilityMutationTargetKey(organization),
        agentObservabilityMutationTargetKey(workspace),
        agentObservabilityMutationTargetKey(binding),
      ]).size,
    ).toBe(3);
    expect(() =>
      parseAgentObservabilityMutationTarget({
        type: 'organization_setting',
        organizationId: 'org_a',
        workspaceId: 'ws_must_not_be_here',
      }),
    ).toThrow(AgentObservabilityMutationValidationError);
  });

  it('hashes semantic JSON and partitions organization-admin idempotency', () => {
    expect(agentObservabilityMutationBodyHash({ b: [2, { a: true }], a: 1 })).toBe(
      agentObservabilityMutationBodyHash({ a: 1, b: [2, { a: true }] }),
    );
    const identity = {
      organizationId: 'org_a',
      principal: 'admin_a',
      scope: 'PUT observability',
      key: 'key_a',
    };
    expect(agentObservabilityAdminIdempotencyPartition(identity)).not.toBe(
      agentObservabilityAdminIdempotencyPartition({ ...identity, organizationId: 'org_b' }),
    );
    expect(agentObservabilityAdminIdempotencyPartition(identity)).not.toBe(
      agentObservabilityAdminIdempotencyPartition({ ...identity, principal: 'admin_b' }),
    );
  });

  it('caches only exact organization/workspace state contract projections', () => {
    const target = { type: 'organization_setting', organizationId: 'org_a' } as const;
    const response = organizationState('org_a');
    expect(parseAgentObservabilityMutationResponse(target, response)).toEqual(response);
    expect(() =>
      parseAgentObservabilityMutationResponse(target, {
        ...response,
        arbitrary_key: 'raw-observability-secret',
      }),
    ).toThrow(AgentObservabilityMutationValidationError);
    expect(() =>
      parseAgentObservabilityMutationResponse(target, organizationState('org_b')),
    ).toThrow(AgentObservabilityMutationValidationError);
    expect(() =>
      parseAgentObservabilityMutationResponse(
        { type: 'workspace_setting', organizationId: 'org_a', workspaceId: 'ws_a' },
        response,
      ),
    ).toThrow(AgentObservabilityMutationValidationError);
  });

  it('builds fixed audit metadata with no ref or candidate fields', () => {
    const metadata = agentObservabilityMutationAuditMetadata({
      targetType: 'binding',
      generation: 4,
      expectedStateVersion: 'etag-v4',
      expectedConfigVersion: 2,
      expectedCredentialVersion: 7,
    });
    expect(metadata).toEqual({
      target_type: 'binding',
      reservation_generation: 4,
      expected_state_version: 'etag-v4',
      expected_config_version: 2,
      expected_credential_version: 7,
    });
    expect(JSON.stringify(metadata)).not.toMatch(/secret|ref|header|candidate|payload/i);
  });

  it('accepts only an authoritative response status from finalization callers', () => {
    expectTypeOf<FinalizeAgentObservabilityMutationInput>().toHaveProperty('responseStatus');
    expectTypeOf<FinalizeAgentObservabilityMutationInput>().not.toHaveProperty('response');
  });
});

function organizationState(organizationId: string) {
  return {
    type: 'agent_observability' as const,
    scope: 'organization' as const,
    organization_id: organizationId,
    workspace_id: null,
    configured: { capture_ceiling: 'metadata_only' as const, default_binding: null },
    effective: {
      source: 'none' as const,
      status: 'disabled' as const,
      disabled_reason: 'no_default_binding' as const,
      capture_mode: 'metadata_only' as const,
      binding: null,
    },
  };
}
