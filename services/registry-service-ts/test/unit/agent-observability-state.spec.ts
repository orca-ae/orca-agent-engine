// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  OrganizationAgentObservabilityStateSchema,
  AgentObservabilityWorkspacePathParamsSchema,
  WorkspaceAgentObservabilityStateSchema,
  adminAgentObservabilityContract,
} from '../../src/contracts/agent-observability.contract.js';
import {
  agentObservabilityStateEtag,
  type AgentObservabilityStateEtagInput,
} from '../../src/domain/agent-observability-etag.js';
import {
  effectiveCaptureMode,
  resolveOrganizationAgentObservabilityState,
  resolveWorkspaceAgentObservabilityState,
  type AgentObservabilityBindingState,
  type AgentObservabilityOrganizationSettingState,
  type AgentObservabilityPlatformPolicyState,
  type AgentObservabilityWorkspaceSettingState,
} from '../../src/domain/agent-observability-state.js';

const platformPolicy: AgentObservabilityPlatformPolicyState = {
  allowedAdapters: ['otlp_http'],
  allowedEndpointClasses: ['public'],
  maxCaptureMode: 'redacted_io',
  captureRestrictionEpoch: 4,
};

const organizationSetting: AgentObservabilityOrganizationSettingState = {
  organizationId: 'org_state_unit',
  activeDefaultBindingId: 'aob_org_default',
  activeDefaultBindingScope: 'organization',
  selectionEpoch: 2,
  defaultRevocationEpoch: 3,
  organizationRevocationEpoch: 4,
  captureCeiling: 'redacted_io',
  captureRestrictionEpoch: 5,
};

const workspaceSetting: AgentObservabilityWorkspaceSettingState = {
  workspaceId: 'ws_state_unit',
  organizationId: organizationSetting.organizationId,
  mode: 'inherit',
  bindingId: null,
  selectionEpoch: 6,
  revocationEpoch: 7,
  captureCeiling: 'redacted_io',
  captureRestrictionEpoch: 8,
};

function binding(
  overrides: Partial<AgentObservabilityBindingState> = {},
): AgentObservabilityBindingState {
  const base: AgentObservabilityBindingState = {
    id: 'aob_org_default',
    organizationId: organizationSetting.organizationId,
    workspaceId: null,
    scopeType: 'organization',
    adapterType: 'otlp_http',
    endpointKind: 'traces_endpoint',
    endpointClass: 'public',
    endpoint: 'https://collector.example/v1/traces',
    externalProjectId: 'pk_public',
    currentVersion: 3,
    status: 'active',
    revocationEpoch: 9,
    archivedAt: null,
    version: {
      version: 3,
      adapterType: 'otlp_http',
      semanticProfile: 'langfuse',
      protocol: 'http/protobuf',
      compression: 'gzip',
      timeoutMs: 5000,
      environment: 'test',
      release: 'v1',
      captureMode: 'redacted_io',
      sampleRate: 0.5,
      configSchemaVersion: 1,
    },
    credential: {
      configured: true,
      credentialVersion: 2,
      keyHint: '...1234',
      rotatedAt: '2026-08-27T00:00:00.000Z',
    },
  };
  return { ...base, ...overrides };
}

function workspaceInput(
  overrides: {
    platform?: AgentObservabilityPlatformPolicyState;
    organization?: AgentObservabilityOrganizationSettingState;
    workspace?: AgentObservabilityWorkspaceSettingState;
    organizationBinding?: AgentObservabilityBindingState | null;
    workspaceBinding?: AgentObservabilityBindingState | null;
  } = {},
) {
  return {
    organizationStatus: 'active' as const,
    workspaceStatus: 'active' as const,
    platformPolicy: overrides.platform ?? platformPolicy,
    organizationSetting: overrides.organization ?? organizationSetting,
    workspaceSetting: overrides.workspace ?? workspaceSetting,
    organizationBinding:
      overrides.organizationBinding === undefined ? binding() : overrides.organizationBinding,
    workspaceBinding: overrides.workspaceBinding ?? null,
  };
}

describe('agent observability admin contract', () => {
  it('uses strict, listener-local schemas with explicit organization/workspace scope', () => {
    const organization = resolveOrganizationAgentObservabilityState({
      organizationStatus: 'active',
      platformPolicy,
      organizationSetting,
      organizationBinding: binding(),
    });
    const workspace = resolveWorkspaceAgentObservabilityState(workspaceInput());

    expect(OrganizationAgentObservabilityStateSchema.parse(organization)).toEqual(organization);
    expect(WorkspaceAgentObservabilityStateSchema.parse(workspace)).toEqual(workspace);
    expect(organization).toMatchObject({
      type: 'agent_observability',
      scope: 'organization',
      workspace_id: null,
      configured: { default_binding: { id: 'aob_org_default' } },
    });
    expect(organization.configured).not.toHaveProperty('mode');
    expect(workspace).toMatchObject({ type: 'agent_observability', scope: 'workspace' });
    expect(workspace.configured.mode).toBe('inherit');
    expect(
      WorkspaceAgentObservabilityStateSchema.safeParse({
        ...workspace,
        secret_ref: 'must-not-be-accepted',
      }).success,
    ).toBe(false);
    expect(
      WorkspaceAgentObservabilityStateSchema.safeParse({
        ...workspace,
        effective: {
          ...workspace.effective,
          binding: {
            ...workspace.effective.binding!,
            credential: {
              ...workspace.effective.binding!.credential,
              secret_ref: 'must-not-be-accepted',
            },
          },
        },
      }).success,
    ).toBe(false);
    expect(
      AgentObservabilityWorkspacePathParamsSchema.safeParse({ workspaceId: 'w'.repeat(100) })
        .success,
    ).toBe(true);
    expect(
      AgentObservabilityWorkspacePathParamsSchema.safeParse({ workspaceId: 'w'.repeat(101) })
        .success,
    ).toBe(false);
    const disabledOrganization = resolveOrganizationAgentObservabilityState({
      organizationStatus: 'active',
      platformPolicy,
      organizationSetting: {
        ...organizationSetting,
        activeDefaultBindingId: null,
        activeDefaultBindingScope: null,
      },
      organizationBinding: null,
    });
    expect(
      OrganizationAgentObservabilityStateSchema.safeParse({
        ...disabledOrganization,
        effective: { ...disabledOrganization.effective, capture_mode: 'redacted_io' },
      }).success,
    ).toBe(false);
    expect(Object.values(adminAgentObservabilityContract)).toHaveLength(8);
  });
});

describe('agent observability effective-state precedence', () => {
  it('inherits organization default and applies every capture ceiling', () => {
    const response = resolveWorkspaceAgentObservabilityState(
      workspaceInput({
        platform: { ...platformPolicy, maxCaptureMode: 'metadata_only' },
      }),
    );

    expect(response.configured).toMatchObject({ mode: 'inherit', binding: null });
    expect(response.effective).toMatchObject({
      source: 'organization_default',
      status: 'enabled',
      disabled_reason: null,
      capture_mode: 'metadata_only',
      binding: { id: 'aob_org_default' },
    });
  });

  it('uses custom workspace binding instead of organization default', () => {
    const custom = binding({
      id: 'aob_workspace_custom',
      scopeType: 'workspace',
      workspaceId: workspaceSetting.workspaceId,
    });
    const response = resolveWorkspaceAgentObservabilityState(
      workspaceInput({
        workspace: {
          ...workspaceSetting,
          mode: 'custom',
          bindingId: custom.id,
        },
        workspaceBinding: custom,
      }),
    );

    expect(response.configured.binding).toMatchObject({ id: custom.id });
    expect(response.effective).toMatchObject({
      source: 'workspace_custom',
      status: 'enabled',
      binding: { id: custom.id },
    });
  });

  it.each([
    [
      'no organization default',
      'no_default_binding',
      () =>
        resolveOrganizationAgentObservabilityState({
          organizationStatus: 'active',
          platformPolicy,
          organizationSetting: {
            ...organizationSetting,
            activeDefaultBindingId: null,
            activeDefaultBindingScope: null,
          },
          organizationBinding: null,
        }),
    ],
    [
      'no inherited organization default',
      'no_organization_default',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({
            organization: {
              ...organizationSetting,
              activeDefaultBindingId: null,
              activeDefaultBindingScope: null,
            },
            organizationBinding: null,
          }),
        ),
    ],
    [
      'workspace disable',
      'workspace_disabled',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({
            workspace: { ...workspaceSetting, mode: 'disabled', bindingId: null },
            organizationBinding: binding(),
          }),
        ),
    ],
    [
      'organization archive',
      'organization_archived',
      () =>
        resolveOrganizationAgentObservabilityState({
          organizationStatus: 'archived',
          platformPolicy,
          organizationSetting,
          organizationBinding: binding(),
        }),
    ],
    [
      'workspace archive',
      'workspace_archived',
      () =>
        resolveWorkspaceAgentObservabilityState({
          ...workspaceInput(),
          workspaceStatus: 'archived',
        }),
    ],
    [
      'draining binding',
      'binding_draining',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({ organizationBinding: binding({ status: 'draining' }) }),
        ),
    ],
    [
      'disabled binding',
      'binding_disabled',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({ organizationBinding: binding({ status: 'disabled' }) }),
        ),
    ],
    [
      'archived binding',
      'binding_archived',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({
            organizationBinding: binding({
              status: 'archived',
              archivedAt: '2026-08-27T00:00:00.000Z',
            }),
          }),
        ),
    ],
    [
      'invalid binding configuration',
      'binding_configuration_invalid',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({ organizationBinding: binding({ externalProjectId: null }) }),
        ),
    ],
    [
      'adapter denied by platform policy',
      'platform_adapter_disallowed',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({ platform: { ...platformPolicy, allowedAdapters: ['langfuse_sdk'] } }),
        ),
    ],
    [
      'endpoint class denied by platform policy',
      'platform_endpoint_class_disallowed',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({ platform: { ...platformPolicy, allowedEndpointClasses: ['private'] } }),
        ),
    ],
    [
      'credential head absent',
      'credential_not_configured',
      () =>
        resolveWorkspaceAgentObservabilityState(
          workspaceInput({
            organizationBinding: binding({
              credential: {
                configured: false,
                credentialVersion: null,
                keyHint: null,
                rotatedAt: null,
              },
            }),
          }),
        ),
    ],
  ])(
    '%s forces metadata-only capture despite redacted request and ceilings',
    (_label, reason, resolve) => {
      const response = resolve();

      expect(response.effective).toMatchObject({
        status: 'disabled',
        disabled_reason: reason,
        capture_mode: 'metadata_only',
      });
      if (reason === 'workspace_disabled') expect(response.effective.binding).toBeNull();
    },
  );

  it('computes the minimum capture mode', () => {
    expect(effectiveCaptureMode('redacted_io', 'redacted_io', 'redacted_io', 'redacted_io')).toBe(
      'redacted_io',
    );
    expect(effectiveCaptureMode('redacted_io', 'metadata_only', 'redacted_io', 'redacted_io')).toBe(
      'metadata_only',
    );
    expect(effectiveCaptureMode('metadata_only', 'redacted_io', 'redacted_io')).toBe(
      'metadata_only',
    );
  });
});

describe('agent observability ETag', () => {
  it('is deterministic, opaque, domain-versioned, and sensitive to visible authority', () => {
    const initial = etagInput();
    const etag = agentObservabilityStateEtag(initial);

    expect(etag).toBe(agentObservabilityStateEtag(etagInput()));
    expect(etag).toMatch(/^"orca-aos-v1-[A-Za-z0-9_-]{43}"$/);
    expect(etag).not.toContain('aob_org_default');
    expect(etag).not.toContain('secret_ref');

    const changes: AgentObservabilityStateEtagInput[] = [
      { ...initial, platformPolicy: [['langfuse_sdk'], ['public'], 'redacted_io', 4] },
      {
        ...initial,
        organizationSetting: ['aob_org_default', 'organization', 3, 3, 4, 'redacted_io', 5],
      },
      { ...initial, workspaceSetting: ['inherit', null, 7, 7, 'redacted_io', 8] },
      {
        ...initial,
        binding: [
          'aob_org_default',
          'org_state_unit',
          null,
          'organization',
          'otlp_http',
          'traces_endpoint',
          'public',
          'https://collector.example/v1/traces',
          'pk_public',
          3,
          'disabled',
          9,
          null,
        ],
      },
      {
        ...initial,
        binding: [
          'aob_org_default',
          'org_state_unit',
          null,
          'organization',
          'otlp_http',
          'traces_endpoint',
          'public',
          'https://collector.example/v1/traces',
          'pk_public',
          3,
          'active',
          10,
          null,
        ],
      },
      {
        ...initial,
        version: [
          3,
          'otlp_http',
          'langfuse',
          'http/protobuf',
          'gzip',
          6000,
          'test',
          'v1',
          'redacted_io',
          0.5,
          1,
        ],
      },
      { ...initial, credential: [true, 3, '...5678', '2026-08-28T00:00:00.000Z'] },
    ];

    for (const changed of changes) {
      expect(agentObservabilityStateEtag(changed)).not.toBe(etag);
    }
  });
});

function etagInput(): AgentObservabilityStateEtagInput {
  return {
    scope: 'workspace',
    organizationId: 'org_state_unit',
    workspaceId: 'ws_state_unit',
    platformPolicy: [['otlp_http'], ['public'], 'redacted_io', 4],
    organizationSetting: ['aob_org_default', 'organization', 2, 3, 4, 'redacted_io', 5],
    workspaceSetting: ['inherit', null, 6, 7, 'redacted_io', 8],
    binding: [
      'aob_org_default',
      'org_state_unit',
      null,
      'organization',
      'otlp_http',
      'traces_endpoint',
      'public',
      'https://collector.example/v1/traces',
      'pk_public',
      3,
      'active',
      9,
      null,
    ],
    version: [
      3,
      'otlp_http',
      'langfuse',
      'http/protobuf',
      'gzip',
      5000,
      'test',
      'v1',
      'redacted_io',
      0.5,
      1,
    ],
    credential: [true, 2, '...1234', '2026-08-27T00:00:00.000Z'],
  };
}
