// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { AGENT_OBSERVABILITY_CONFIG_VERSION_MAX } from '../../src/domain/agent-observability-mutations.js';
import { isAgentObservabilityCaptureModeAtMost } from '../../src/domain/agent-observability-policy.js';
import {
  WorkspaceAgentObservabilityCredentialRotationHeadersSchema,
  WorkspaceAgentObservabilityCredentialRotationRequestSchema,
} from '../../src/contracts/agent-observability.contract.js';
import {
  WorkspaceAgentObservabilityMutationInvariantError,
  WorkspaceAgentObservabilityMutationRequestError,
  assertWorkspaceAgentObservabilityCredentialRotationPrecondition,
  assertWorkspaceAgentObservabilityMutationCapacity,
  assertWorkspaceAgentObservabilityMutationRequest,
  classifyWorkspaceAgentObservabilityMutation,
  parseWorkspaceAgentObservabilityCredentialRotationRequest,
  parseWorkspaceAgentObservabilityIdempotencyKey,
  parseWorkspaceAgentObservabilityIfMatch,
  parseWorkspaceAgentObservabilityPutRequest,
  workspaceAgentObservabilityCredentialRotationBody,
  workspaceAgentObservabilityMutationBody,
  type WorkspaceAgentObservabilityMutationAuthority,
} from '../../src/domain/agent-observability-workspace-mutation.js';

describe('workspace agent observability mutation request boundary', () => {
  it('accepts only strict mode-discriminated request shapes', () => {
    expect(() =>
      parseWorkspaceAgentObservabilityPutRequest({
        mode: 'inherit',
        capture_ceiling: 'metadata_only',
        target: customRequest().target,
      }),
    ).toThrow(WorkspaceAgentObservabilityMutationRequestError);
    expect(() =>
      parseWorkspaceAgentObservabilityPutRequest({
        mode: 'disabled',
        capture_ceiling: 'metadata_only',
        credentials: customRequest().credentials,
      }),
    ).toThrow(WorkspaceAgentObservabilityMutationRequestError);

    const custom = parseWorkspaceAgentObservabilityPutRequest(customRequest());
    expect(custom).toMatchObject({
      mode: 'custom',
      target: {
        adapterType: 'otlp_http',
        endpoint: 'https://collector.example/v1/traces',
      },
      config: { semanticProfile: 'otel_genai' },
    });
    expect(workspaceAgentObservabilityMutationBody(custom)).toEqual({
      mode: 'custom',
      target: {
        adapter_type: 'otlp_http',
        endpoint_kind: 'traces_endpoint',
        endpoint_class: 'public',
        endpoint_url: 'https://collector.example/v1/traces',
        external_project_id: null,
      },
      config: {
        semantic_profile: 'otel_genai',
        protocol: 'http/protobuf',
        compression: 'none',
        timeout_ms: 5000,
        environment: null,
        release: null,
        capture_mode: 'metadata_only',
        sample_rate: 1,
      },
      capture_ceiling: 'metadata_only',
      credentials: { type: 'basic', username: 'project', password: 'secret' },
    });
  });

  it('normalizes idempotency keys and accepts only one strong ETag', () => {
    expect(parseWorkspaceAgentObservabilityIdempotencyKey('  workspace-put-key  ')).toBe(
      'workspace-put-key',
    );
    expect(() => parseWorkspaceAgentObservabilityIdempotencyKey('   ')).toThrow(
      WorkspaceAgentObservabilityMutationRequestError,
    );
    expect(parseWorkspaceAgentObservabilityIfMatch(undefined)).toBeNull();
    expect(parseWorkspaceAgentObservabilityIfMatch('"orca-aos-v1-current"')).toBe(
      '"orca-aos-v1-current"',
    );
    for (const invalid of ['W/"orca-aos-v1-current"', '*', '"one", "two"']) {
      expect(() => parseWorkspaceAgentObservabilityIfMatch(invalid)).toThrow(
        WorkspaceAgentObservabilityMutationRequestError,
      );
    }
  });

  it('accepts only credentials for strict workspace credential rotation', () => {
    const rotation = {
      credentials: { type: 'bearer' as const, token: 'workspace-rotation-secret' },
    };
    expect(
      WorkspaceAgentObservabilityCredentialRotationRequestSchema.safeParse(rotation).success,
    ).toBe(true);
    for (const forbidden of [
      { ...rotation, mode: 'custom' },
      { ...rotation, target: customRequest().target },
      { ...rotation, config: customRequest().config },
      { ...rotation, binding_id: 'aob_forbidden' },
      { ...rotation, secret_ref: 'local:agent_observability/obssec_forbidden' },
    ]) {
      expect(
        WorkspaceAgentObservabilityCredentialRotationRequestSchema.safeParse(forbidden).success,
      ).toBe(false);
      expect(() => parseWorkspaceAgentObservabilityCredentialRotationRequest(forbidden)).toThrow(
        WorkspaceAgentObservabilityMutationRequestError,
      );
    }
    const normalized = parseWorkspaceAgentObservabilityCredentialRotationRequest(rotation);
    expect(workspaceAgentObservabilityCredentialRotationBody(normalized)).toEqual(rotation);
    expect(
      WorkspaceAgentObservabilityCredentialRotationHeadersSchema.safeParse({
        'idempotency-key': ' workspace-rotation-key ',
        'if-match': '"orca-aos-v1-current"',
      }).success,
    ).toBe(true);
    expect(
      WorkspaceAgentObservabilityCredentialRotationHeadersSchema.safeParse({
        'idempotency-key': 'workspace-rotation-key',
      }).success,
    ).toBe(false);
    expect(
      assertWorkspaceAgentObservabilityCredentialRotationPrecondition({
        currentEtag: '"orca-aos-v1-current"',
        ifMatch: null,
      }),
    ).toBe('missing');
  });

  it('orders capture modes from metadata_only through redacted_io', () => {
    expect(isAgentObservabilityCaptureModeAtMost('metadata_only', 'metadata_only')).toBe(true);
    expect(isAgentObservabilityCaptureModeAtMost('metadata_only', 'redacted_io')).toBe(true);
    expect(isAgentObservabilityCaptureModeAtMost('redacted_io', 'redacted_io')).toBe(true);
    expect(isAgentObservabilityCaptureModeAtMost('redacted_io', 'metadata_only')).toBe(false);
  });

  it('classifies custom target, mode, and credential transitions', () => {
    const custom = parseWorkspaceAgentObservabilityPutRequest(customRequest());
    const sameTarget = parseWorkspaceAgentObservabilityPutRequest(
      customRequest({ credentials: undefined, compression: 'gzip' }),
    );
    const current = authority({ mode: 'custom', currentBinding: activeBinding() });

    expect(classifyWorkspaceAgentObservabilityMutation(authority(), custom)).toEqual({
      type: 'initial',
    });
    expect(classifyWorkspaceAgentObservabilityMutation(current, sameTarget)).toEqual({
      type: 'same_target',
      bindingId: 'aob_current',
    });
    expect(
      classifyWorkspaceAgentObservabilityMutation(
        current,
        parseWorkspaceAgentObservabilityPutRequest({
          mode: 'inherit',
          capture_ceiling: 'metadata_only',
        }),
      ),
    ).toEqual({
      type: 'mode_only',
      selectionChanged: true,
      entersDisabled: false,
      previousActiveBindingId: 'aob_current',
    });
    expect(
      classifyWorkspaceAgentObservabilityMutation(
        current,
        parseWorkspaceAgentObservabilityPutRequest({
          mode: 'disabled',
          capture_ceiling: 'metadata_only',
        }),
      ),
    ).toEqual({
      type: 'mode_only',
      selectionChanged: true,
      entersDisabled: true,
      previousActiveBindingId: 'aob_current',
    });

    expect(() =>
      assertWorkspaceAgentObservabilityMutationRequest(custom, {
        type: 'same_target',
        bindingId: 'aob_current',
      }),
    ).toThrow(WorkspaceAgentObservabilityMutationRequestError);
    expect(() =>
      assertWorkspaceAgentObservabilityMutationRequest(
        parseWorkspaceAgentObservabilityPutRequest(customRequest({ credentials: undefined })),
        { type: 'initial' },
      ),
    ).toThrow(WorkspaceAgentObservabilityMutationRequestError);
  });

  it('fails closed before epoch or configuration-version overflow', () => {
    const custom = parseWorkspaceAgentObservabilityPutRequest(
      customRequest({ credentials: undefined }),
    );
    expect(() =>
      assertWorkspaceAgentObservabilityMutationCapacity(
        authority({
          mode: 'custom',
          currentBinding: activeBinding({ configVersion: AGENT_OBSERVABILITY_CONFIG_VERSION_MAX }),
        }),
        custom,
        { type: 'same_target', bindingId: 'aob_current' },
      ),
    ).toThrow(WorkspaceAgentObservabilityMutationInvariantError);
    expect(() =>
      assertWorkspaceAgentObservabilityMutationCapacity(
        authority({ selectionEpoch: Number.MAX_SAFE_INTEGER }),
        parseWorkspaceAgentObservabilityPutRequest(customRequest()),
        { type: 'initial' },
      ),
    ).toThrow(WorkspaceAgentObservabilityMutationInvariantError);
    expect(() =>
      assertWorkspaceAgentObservabilityMutationCapacity(
        authority({ revocationEpoch: Number.MAX_SAFE_INTEGER }),
        parseWorkspaceAgentObservabilityPutRequest({
          mode: 'disabled',
          capture_ceiling: 'metadata_only',
        }),
        {
          type: 'mode_only',
          selectionChanged: true,
          entersDisabled: true,
          previousActiveBindingId: null,
        },
      ),
    ).toThrow(WorkspaceAgentObservabilityMutationInvariantError);
    expect(() =>
      assertWorkspaceAgentObservabilityMutationCapacity(
        authority({
          mode: 'disabled',
          selectionEpoch: Number.MAX_SAFE_INTEGER,
          revocationEpoch: Number.MAX_SAFE_INTEGER,
        }),
        parseWorkspaceAgentObservabilityPutRequest({
          mode: 'disabled',
          capture_ceiling: 'metadata_only',
        }),
        {
          type: 'mode_only',
          selectionChanged: false,
          entersDisabled: false,
          previousActiveBindingId: null,
        },
      ),
    ).not.toThrow();
  });
});

function customRequest(
  options: {
    credentials?: { type: 'basic'; username: string; password: string } | undefined;
    compression?: 'none' | 'gzip';
  } = {},
) {
  return {
    mode: 'custom' as const,
    target: {
      adapter_type: 'otlp_http' as const,
      endpoint_kind: 'traces_endpoint' as const,
      endpoint_class: 'public' as const,
      endpoint_url: 'https://collector.example/v1/traces',
    },
    config: {
      semantic_profile: 'otel_genai' as const,
      protocol: 'http/protobuf' as const,
      compression: options.compression ?? ('none' as const),
      timeout_ms: 5000,
      capture_mode: 'metadata_only' as const,
      sample_rate: 1,
    },
    capture_ceiling: 'metadata_only' as const,
    ...(options.credentials === undefined && Object.hasOwn(options, 'credentials')
      ? {}
      : {
          credentials: options.credentials ?? {
            type: 'basic' as const,
            username: 'project',
            password: 'secret',
          },
        }),
  };
}

function activeBinding(
  overrides: Partial<WorkspaceAgentObservabilityMutationAuthority['currentBinding']> = {},
) {
  return {
    id: 'aob_current',
    status: 'active' as const,
    target: {
      adapterType: 'otlp_http' as const,
      endpointKind: 'traces_endpoint' as const,
      endpointClass: 'public' as const,
      endpoint: 'https://collector.example/v1/traces',
      externalProjectId: null,
    },
    configVersion: 1,
    credentialVersion: 1,
    ...overrides,
  };
}

function authority(
  overrides: Partial<WorkspaceAgentObservabilityMutationAuthority> = {},
): WorkspaceAgentObservabilityMutationAuthority {
  return {
    organizationId: 'org_workspace_mutation_unit',
    workspaceId: 'ws_workspace_mutation_unit',
    state: {} as WorkspaceAgentObservabilityMutationAuthority['state'],
    stateVersion: '"orca-aos-v1-unit"',
    mode: 'inherit',
    currentBinding: null,
    captureCeiling: 'metadata_only',
    captureRestrictionEpoch: 0,
    selectionEpoch: 0,
    revocationEpoch: 0,
    allowedAdapters: ['otlp_http'],
    allowedEndpointClasses: ['public'],
    ...overrides,
  };
}
