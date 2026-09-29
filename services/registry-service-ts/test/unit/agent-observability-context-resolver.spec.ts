// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  classifySessionObservabilityContext,
  effectiveSessionObservabilityContextCaptureMode,
  type SessionObservabilityContextClassificationInput,
} from '../../src/domain/agent-observability-context-resolver.js';

function baseInput(): SessionObservabilityContextClassificationInput {
  return {
    pin: {
      workspaceId: 'ws_context',
      sessionId: 'ses_context',
      organizationId: 'org_context',
      bindingId: 'aob_context',
      bindingVersion: 1,
      bindingScope: 'organization',
      bindingWorkspaceId: null,
      selectionSource: 'organization_default',
      status: 'active',
      organizationSelectionEpoch: 7,
      workspaceSelectionEpoch: 8,
      organizationDefaultRevocationEpoch: 9,
      organizationRevocationEpoch: 10,
      workspaceRevocationEpoch: 11,
      bindingRevocationEpoch: 12,
      platformCaptureRestrictionEpoch: 13,
      organizationCaptureRestrictionEpoch: 14,
      workspaceCaptureRestrictionEpoch: 15,
      effectiveCaptureMode: 'redacted_io',
      sessionRevocationEpoch: 0,
      agentId: 'agt_context',
      agentVersion: 1,
      harness: 'codex',
      harnessMode: 'colocated',
      archivedAt: null,
      deletedAt: null,
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
      updatedAt: new Date('2026-01-01T00:00:00.000Z'),
    },
    platformPolicy: {
      allowedAdapters: ['otlp_http'],
      allowedEndpointClasses: ['public'],
      maxCaptureMode: 'redacted_io',
      captureRestrictionEpoch: 13,
    },
    organization: { id: 'org_context', status: 'active' },
    organizationSetting: {
      organizationId: 'org_context',
      selectionEpoch: 7,
      defaultRevocationEpoch: 9,
      organizationRevocationEpoch: 10,
      captureCeiling: 'redacted_io',
      captureRestrictionEpoch: 14,
    },
    workspace: { id: 'ws_context', organizationId: 'org_context', status: 'active' },
    workspaceSetting: {
      organizationId: 'org_context',
      workspaceId: 'ws_context',
      mode: 'inherit',
      selectionEpoch: 8,
      revocationEpoch: 11,
      captureCeiling: 'redacted_io',
      captureRestrictionEpoch: 15,
    },
    binding: {
      id: 'aob_context',
      organizationId: 'org_context',
      workspaceId: null,
      scopeType: 'organization',
      adapterType: 'otlp_http',
      endpointKind: 'traces_endpoint',
      endpointClass: 'public',
      endpoint: 'https://collector.example/v1/traces',
      externalProjectId: 'project-context',
      currentVersion: 3,
      status: 'active',
      revocationEpoch: 12,
      version: {
        version: 1,
        adapterType: 'otlp_http',
        semanticProfile: 'langfuse',
        protocol: 'http/protobuf',
        compression: 'none',
        timeoutMs: 5_000,
        environment: 'test',
        release: 'r1',
        captureMode: 'redacted_io',
        sampleRate: 1,
        configSchemaVersion: 1,
      },
    },
    currentCredentialVersion: 3,
  };
}

describe('Session observability context classification', () => {
  it('enables active and draining pinned bindings without consulting current selection pointers', () => {
    const active = baseInput();
    active.organizationSetting.selectionEpoch += 100;
    active.workspaceSetting.mode = 'disabled';
    active.workspaceSetting.selectionEpoch += 100;
    expect(classifySessionObservabilityContext(active)).toEqual({
      status: 'enabled',
      reason: null,
    });

    const draining = baseInput();
    draining.binding!.status = 'draining';
    expect(classifySessionObservabilityContext(draining)).toEqual({
      status: 'enabled',
      reason: null,
    });
  });

  it('does not revoke a workspace-custom pin when only the organization default is revoked', () => {
    const custom = baseInput();
    custom.pin.selectionSource = 'workspace_custom';
    custom.pin.bindingScope = 'workspace';
    custom.pin.bindingWorkspaceId = custom.pin.workspaceId;
    custom.binding!.scopeType = 'workspace';
    custom.binding!.workspaceId = custom.pin.workspaceId;
    custom.organizationSetting.defaultRevocationEpoch += 1;
    expect(classifySessionObservabilityContext(custom)).toEqual({
      status: 'enabled',
      reason: null,
    });
  });

  it('applies workspace revocation to inherited and workspace-custom pins', () => {
    for (const selectionSource of ['organization_default', 'workspace_custom'] as const) {
      const input = baseInput();
      input.pin.selectionSource = selectionSource;
      if (selectionSource === 'workspace_custom') {
        input.pin.bindingScope = 'workspace';
        input.pin.bindingWorkspaceId = input.pin.workspaceId;
        input.binding!.scopeType = 'workspace';
        input.binding!.workspaceId = input.pin.workspaceId;
      }
      input.workspaceSetting.revocationEpoch += 1;
      expect(classifySessionObservabilityContext(input)).toEqual({
        status: 'suppressed',
        reason: 'workspace_revoked',
      });
    }
  });

  it.each([
    [
      'session_archived',
      (input: SessionObservabilityContextClassificationInput) => (input.pin.status = 'archived'),
    ],
    [
      'session_deleted',
      (input: SessionObservabilityContextClassificationInput) => (input.pin.status = 'deleted'),
    ],
    [
      'organization_archived',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.organization.status = 'archived'),
    ],
    [
      'workspace_archived',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.workspace.status = 'archived'),
    ],
    [
      'session_revoked',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.pin.sessionRevocationEpoch = 1),
    ],
    [
      'organization_revoked',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.organizationSetting.organizationRevocationEpoch += 1),
    ],
    [
      'workspace_revoked',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.workspaceSetting.revocationEpoch += 1),
    ],
    [
      'organization_default_revoked',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.organizationSetting.defaultRevocationEpoch += 1),
    ],
    [
      'binding_revoked',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.binding!.revocationEpoch += 1),
    ],
    [
      'binding_disabled',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.binding!.status = 'disabled'),
    ],
    [
      'binding_archived',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.binding!.status = 'archived'),
    ],
    [
      'platform_adapter_disallowed',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.platformPolicy.allowedAdapters = []),
    ],
    [
      'platform_endpoint_class_disallowed',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.platformPolicy.allowedEndpointClasses = []),
    ],
    [
      'binding_configuration_invalid',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.binding!.externalProjectId = null),
    ],
    [
      'credential_not_configured',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.currentCredentialVersion = null),
    ],
  ])('suppresses %s at fixed precedence', (reason, mutate) => {
    const input = baseInput();
    mutate(input);
    expect(classifySessionObservabilityContext(input)).toEqual({ status: 'suppressed', reason });
  });

  it('keeps lifecycle classification ahead of later revocation and policy checks', () => {
    const input = baseInput();
    input.pin.status = 'archived';
    input.binding!.status = 'disabled';
    input.workspaceSetting.revocationEpoch += 1;
    expect(classifySessionObservabilityContext(input)).toEqual({
      status: 'suppressed',
      reason: 'session_archived',
    });
  });

  it('reports a disabled pin only when no higher-priority suppression applies', () => {
    const disabled = baseInput();
    disabled.pin.status = 'disabled';
    disabled.pin.selectionSource = 'disabled';
    disabled.pin.bindingId = null;
    disabled.pin.bindingVersion = null;
    disabled.pin.bindingScope = null;
    disabled.binding = null;
    disabled.currentCredentialVersion = null;
    expect(classifySessionObservabilityContext(disabled)).toEqual({
      status: 'disabled',
      reason: 'session_pin_disabled',
    });

    disabled.organization.status = 'archived';
    expect(classifySessionObservabilityContext(disabled)).toEqual({
      status: 'suppressed',
      reason: 'organization_archived',
    });
  });
});

describe('Session observability context capture', () => {
  it.each(['metadata_only', 'redacted_io', 'raw_io'] as const)(
    'never expands an existing %s pin when all ceilings expand to raw_io',
    (mode) => {
      const input = baseInput();
      input.pin.effectiveCaptureMode = mode;
      input.platformPolicy.maxCaptureMode = 'raw_io';
      input.organizationSetting.captureCeiling = 'raw_io';
      input.workspaceSetting.captureCeiling = 'raw_io';
      expect(effectiveSessionObservabilityContextCaptureMode(input)).toBe(mode);
      for (const setting of [
        input.platformPolicy,
        input.organizationSetting,
        input.workspaceSetting,
      ]) {
        setting.captureRestrictionEpoch += 1;
        expect(effectiveSessionObservabilityContextCaptureMode(input)).toBe('metadata_only');
        setting.captureRestrictionEpoch -= 1;
      }
    },
  );

  it.each([
    [
      'platform',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.platformPolicy.captureRestrictionEpoch += 1),
    ],
    [
      'organization',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.organizationSetting.captureRestrictionEpoch += 1),
    ],
    [
      'workspace',
      (input: SessionObservabilityContextClassificationInput) =>
        (input.workspaceSetting.captureRestrictionEpoch += 1),
    ],
  ])('sticks to metadata_only after a %s capture restriction advance', (_scope, advance) => {
    const input = baseInput();
    advance(input);

    expect(effectiveSessionObservabilityContextCaptureMode(input)).toBe('metadata_only');
  });

  it('uses current minimum ceilings while every capture restriction epoch matches its pin', () => {
    const fullyAllowed = baseInput();
    expect(effectiveSessionObservabilityContextCaptureMode(fullyAllowed)).toBe('redacted_io');

    const narrowed = baseInput();
    narrowed.workspaceSetting.captureCeiling = 'metadata_only';
    expect(effectiveSessionObservabilityContextCaptureMode(narrowed)).toBe('metadata_only');
  });

  it('does not use selection or revocation epochs to calculate capture', () => {
    const input = baseInput();
    input.organizationSetting.selectionEpoch += 1;
    input.workspaceSetting.selectionEpoch += 1;
    input.organizationSetting.defaultRevocationEpoch += 1;
    input.organizationSetting.organizationRevocationEpoch += 1;
    input.workspaceSetting.revocationEpoch += 1;
    input.binding!.revocationEpoch += 1;
    input.pin.sessionRevocationEpoch += 1;

    expect(effectiveSessionObservabilityContextCaptureMode(input)).toBe('redacted_io');
  });

  it('never expands a metadata-only pin', () => {
    const input = baseInput();
    input.pin.effectiveCaptureMode = 'metadata_only';

    expect(effectiveSessionObservabilityContextCaptureMode(input)).toBe('metadata_only');
    input.platformPolicy.captureRestrictionEpoch += 1;
    expect(effectiveSessionObservabilityContextCaptureMode(input)).toBe('metadata_only');
  });
});
