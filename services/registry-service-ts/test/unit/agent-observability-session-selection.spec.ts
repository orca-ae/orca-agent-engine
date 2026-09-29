// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  SessionObservabilitySelectionAvailabilityError,
  SessionObservabilitySelectionResourceUnavailableError,
  resolveSessionObservabilitySelection,
  type SessionObservabilityOrganizationSetting,
  type SessionObservabilityPlatformPolicy,
  type SessionObservabilitySelectedBinding,
  type SessionObservabilitySelectionPolicyInput,
  type SessionObservabilityWorkspaceSetting,
} from '../../src/domain/agent-observability-session-selection.js';

const organizationId = 'org_session_selection_unit';
const workspaceId = 'ws_session_selection_unit';

const platformPolicy: SessionObservabilityPlatformPolicy = {
  allowedAdapters: ['otlp_http'],
  allowedEndpointClasses: ['public'],
  maxCaptureMode: 'redacted_io',
  captureRestrictionEpoch: 11,
};

const organizationSetting: SessionObservabilityOrganizationSetting = {
  organizationId,
  activeDefaultBindingId: 'aob_session_selection_default',
  activeDefaultBindingScope: 'organization',
  selectionEpoch: 12,
  defaultRevocationEpoch: 13,
  organizationRevocationEpoch: 14,
  captureCeiling: 'redacted_io',
  captureRestrictionEpoch: 15,
};

const workspaceSetting: SessionObservabilityWorkspaceSetting = {
  organizationId,
  workspaceId,
  mode: 'inherit',
  bindingId: null,
  selectionEpoch: 16,
  revocationEpoch: 17,
  captureCeiling: 'redacted_io',
  captureRestrictionEpoch: 18,
};

function binding(
  overrides: Partial<SessionObservabilitySelectedBinding> = {},
): SessionObservabilitySelectedBinding {
  const base: SessionObservabilitySelectedBinding = {
    id: 'aob_session_selection_default',
    organizationId,
    workspaceId: null,
    scopeType: 'organization',
    adapterType: 'otlp_http',
    endpointKind: 'traces_endpoint',
    endpointClass: 'public',
    endpoint: 'https://collector.example/v1/traces',
    externalProjectId: 'pk_session_selection',
    currentVersion: 3,
    status: 'active',
    revocationEpoch: 19,
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
    credentialConfigured: true,
  };
  return { ...base, ...overrides };
}

function bindingWithVersion(
  versionOverrides: Partial<SessionObservabilitySelectedBinding['version']>,
  bindingOverrides: Partial<SessionObservabilitySelectedBinding> = {},
): SessionObservabilitySelectedBinding {
  const selected = binding(bindingOverrides);
  return { ...selected, version: { ...selected.version, ...versionOverrides } };
}

function input(
  overrides: Partial<{
    organization: SessionObservabilitySelectionPolicyInput['organization'];
    workspace: SessionObservabilitySelectionPolicyInput['workspace'];
    platformPolicy: SessionObservabilityPlatformPolicy;
    organizationSetting: SessionObservabilityOrganizationSetting;
    workspaceSetting: SessionObservabilityWorkspaceSetting;
    organizationBinding: SessionObservabilitySelectedBinding | null;
    workspaceBinding: SessionObservabilitySelectedBinding | null;
  }> = {},
): SessionObservabilitySelectionPolicyInput {
  return {
    organization: { id: organizationId, status: 'active' },
    workspace: { id: workspaceId, organizationId, status: 'active' },
    platformPolicy,
    organizationSetting,
    workspaceSetting,
    organizationBinding: binding(),
    workspaceBinding: null,
    ...overrides,
  };
}

describe('Session observability selection policy', () => {
  it('selects active organization default for inherit', () => {
    const selection = resolveSessionObservabilitySelection(input());

    expect(selection).toMatchObject({
      organizationId,
      workspaceId,
      status: 'active',
      selectionSource: 'organization_default',
      bindingId: 'aob_session_selection_default',
      bindingVersion: 3,
      bindingScope: 'organization',
      bindingWorkspaceId: null,
      bindingRevocationEpoch: 19,
      effectiveCaptureMode: 'redacted_io',
      disabledReason: null,
    });
  });

  it('selects same-workspace custom binding before organization default', () => {
    const custom = binding({
      id: 'aob_session_selection_custom',
      scopeType: 'workspace',
      workspaceId,
    });
    const selection = resolveSessionObservabilitySelection(
      input({
        workspaceSetting: { ...workspaceSetting, mode: 'custom', bindingId: custom.id },
        workspaceBinding: custom,
      }),
    );

    expect(selection).toMatchObject({
      status: 'active',
      selectionSource: 'workspace_custom',
      bindingId: custom.id,
      bindingScope: 'workspace',
      bindingWorkspaceId: workspaceId,
    });
  });

  it.each([
    [
      'workspace disabled',
      input({ workspaceSetting: { ...workspaceSetting, mode: 'disabled', bindingId: null } }),
      'workspace_disabled',
    ],
    [
      'no organization default',
      input({
        organizationSetting: {
          ...organizationSetting,
          activeDefaultBindingId: null,
          activeDefaultBindingScope: null,
        },
        organizationBinding: null,
      }),
      'no_organization_default',
    ],
    [
      'draining binding',
      input({ organizationBinding: binding({ status: 'draining' }) }),
      'binding_draining',
    ],
    [
      'disabled binding',
      input({ organizationBinding: binding({ status: 'disabled' }) }),
      'binding_disabled',
    ],
    [
      'archived binding',
      input({ organizationBinding: binding({ status: 'archived' }) }),
      'binding_archived',
    ],
    [
      'adapter denied by platform',
      input({ platformPolicy: { ...platformPolicy, allowedAdapters: ['langfuse_sdk'] } }),
      'platform_adapter_disallowed',
    ],
    [
      'endpoint class denied by platform',
      input({ platformPolicy: { ...platformPolicy, allowedEndpointClasses: ['private'] } }),
      'platform_endpoint_class_disallowed',
    ],
    [
      'invalid canonical configuration',
      input({ organizationBinding: binding({ endpoint: 'https://collector.example/v1/traces/' }) }),
      'binding_configuration_invalid',
    ],
    [
      'invalid adapter project identity',
      input({ organizationBinding: binding({ externalProjectId: null }) }),
      'binding_configuration_invalid',
    ],
    [
      'missing credential head',
      input({ organizationBinding: binding({ credentialConfigured: false }) }),
      'credential_not_configured',
    ],
  ] as const)('%s produces a metadata-only disabled pin', (_label, policyInput, reason) => {
    const selection = resolveSessionObservabilitySelection(policyInput);

    expect(selection).toEqual(
      expect.objectContaining({
        status: 'disabled',
        selectionSource: 'disabled',
        disabledReason: reason,
        bindingId: null,
        bindingVersion: null,
        bindingScope: null,
        bindingWorkspaceId: null,
        bindingRevocationEpoch: 0,
        effectiveCaptureMode: 'metadata_only',
      }),
    );
  });

  it('does not fall back when a custom target is not selectable', () => {
    const custom = binding({
      id: 'aob_session_selection_custom_no_fallback',
      scopeType: 'workspace',
      workspaceId,
      status: 'draining',
    });
    const selection = resolveSessionObservabilitySelection(
      input({
        workspaceSetting: { ...workspaceSetting, mode: 'custom', bindingId: custom.id },
        workspaceBinding: custom,
      }),
    );

    expect(selection).toMatchObject({
      status: 'disabled',
      selectionSource: 'disabled',
      disabledReason: 'binding_draining',
      bindingId: null,
    });
  });

  it.each([
    ['missing default binding', input({ organizationBinding: null })],
    [
      'cross-owned default binding',
      input({ organizationBinding: binding({ organizationId: 'org_other' }) }),
    ],
  ])('%s fails closed as control-plane corruption', (_label, policyInput) => {
    expect(() => resolveSessionObservabilitySelection(policyInput)).toThrow(
      SessionObservabilitySelectionAvailabilityError,
    );
  });

  it.each([
    ['current/version mismatch', binding({ currentVersion: 2 })],
    ['nonpositive current version', binding({ currentVersion: 0 })],
    ['nonpositive exact version', bindingWithVersion({ version: 0 })],
  ])('%s fails closed before active eligibility', (_label, organizationBinding) => {
    let error: unknown;
    try {
      resolveSessionObservabilitySelection(input({ organizationBinding }));
    } catch (caught) {
      error = caught;
    }

    expect(error).toBeInstanceOf(SessionObservabilitySelectionAvailabilityError);
    expect(error).toMatchObject({ reason: 'invalid_selected_binding_version' });
  });

  it('computes minimum active capture mode across every current ceiling', () => {
    const fullyAllowed = resolveSessionObservabilitySelection(input());
    const platformRestricted = resolveSessionObservabilitySelection(
      input({ platformPolicy: { ...platformPolicy, maxCaptureMode: 'metadata_only' } }),
    );
    const workspaceRestricted = resolveSessionObservabilitySelection(
      input({
        workspaceSetting: { ...workspaceSetting, captureCeiling: 'metadata_only' },
      }),
    );

    expect(fullyAllowed).toMatchObject({ status: 'active', effectiveCaptureMode: 'redacted_io' });
    expect(platformRestricted).toMatchObject({
      status: 'active',
      effectiveCaptureMode: 'metadata_only',
    });
    expect(workspaceRestricted).toMatchObject({
      status: 'active',
      effectiveCaptureMode: 'metadata_only',
    });
  });

  it('makes inactive parents a resource-unavailable error, never a disabled pin', () => {
    expect(() =>
      resolveSessionObservabilitySelection(
        input({ workspace: { id: workspaceId, organizationId, status: 'archived' } }),
      ),
    ).toThrow(SessionObservabilitySelectionResourceUnavailableError);
  });
});
