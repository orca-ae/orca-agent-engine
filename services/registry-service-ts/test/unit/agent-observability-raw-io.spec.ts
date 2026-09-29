// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  effectiveCaptureMode,
  isAgentObservabilityCaptureModeAtMost,
  type AgentObservabilityCaptureMode,
} from '../../src/domain/agent-observability-policy.js';
import { AgentObservabilityCaptureModeSchema } from '../../src/contracts/agent-observability.contract.js';
import {
  applyOrganizationAgentObservabilityMutation,
  parseOrganizationAgentObservabilityPutRequest,
  type OrganizationAgentObservabilityMutationAuthority,
} from '../../src/domain/agent-observability-organization-mutation.js';
import {
  applyWorkspaceAgentObservabilityMutation,
  assertWorkspaceAgentObservabilityMutationCapacity,
  WorkspaceAgentObservabilityMutationInvariantError,
  type WorkspaceAgentObservabilityMutationAuthority,
} from '../../src/domain/agent-observability-workspace-mutation.js';
import type { DbTransaction } from '../../src/persistence/postgres/client.js';

const modes = ['metadata_only', 'redacted_io', 'raw_io'] as const;

describe('explicit raw IO policy', () => {
  it('accepts raw_io without changing the legacy mode', () => {
    for (const mode of modes) expect(AgentObservabilityCaptureModeSchema.parse(mode)).toBe(mode);
    expect(AgentObservabilityCaptureModeSchema.safeParse('full_io').success).toBe(false);
  });

  it('uses the minimum across the complete capture truth table', () => {
    for (const [i, requested] of modes.entries()) {
      for (const [j, platform] of modes.entries()) {
        expect(isAgentObservabilityCaptureModeAtMost(requested, platform)).toBe(i <= j);
        for (const [k, organization] of modes.entries()) {
          expect(effectiveCaptureMode(requested, platform, organization)).toBe(
            modes[Math.min(i, j, k)],
          );
          for (const [l, workspace] of modes.entries()) {
            expect(effectiveCaptureMode(requested, platform, organization, workspace)).toBe(
              modes[Math.min(i, j, k, l)],
            );
          }
        }
      }
    }
  });

  for (const [i, previous] of modes.entries()) {
    for (const [j, next] of modes.entries()) {
      it(`advances only restriction epochs for ${previous} -> ${next}`, async () => {
        const request = parseOrganizationAgentObservabilityPutRequest({
          target: {
            adapter_type: 'otlp_http',
            endpoint_kind: 'traces_endpoint',
            endpoint_class: 'public',
            endpoint_url: 'https://collector.example/v1/traces',
            external_project_id: null,
          },
          config: {
            semantic_profile: 'otel_genai',
            protocol: 'http/json',
            compression: 'none',
            timeout_ms: 5000,
            environment: null,
            release: null,
            capture_mode: next,
            sample_rate: 1,
          },
          capture_ceiling: next,
        });
        const writes: Record<string, unknown>[] = [];
        const db = {
          insert: () => ({ values: async () => undefined }),
          update: () => ({
            set: (value: Record<string, unknown>) => {
              writes.push(value);
              return { where: () => ({ returning: async () => [{ id: 'aob_raw' }] }) };
            },
          }),
        } as unknown as Pick<DbTransaction, 'insert' | 'update'>;
        const authority = {
          organizationId: 'org_raw',
          workspaceId: 'ws_raw',
          mode: 'inherit',
          captureCeiling: previous,
          captureRestrictionEpoch: 7,
          selectionEpoch: 2,
          revocationEpoch: 3,
          currentBinding: {
            id: 'aob_raw',
            status: 'active',
            configVersion: 1,
            credentialVersion: 1,
          },
        };
        const common = { db, actor: 'test', now: new Date(), stagedActivation: null };
        expect(
          await applyOrganizationAgentObservabilityMutation({
            ...common,
            authority: authority as unknown as OrganizationAgentObservabilityMutationAuthority,
            request,
            kind: { type: 'same_target', bindingId: 'aob_raw' },
          }),
        ).toEqual({ applied: true });
        expect(writes.at(-1)).toMatchObject({
          captureCeiling: next,
          captureRestrictionEpoch: i > j ? 8 : 7,
        });
        const workspaceAuthority = {
          ...authority,
          currentBinding: null,
        } as WorkspaceAgentObservabilityMutationAuthority;
        const workspaceRequest = {
          mode: 'inherit' as const,
          captureCeiling: next as AgentObservabilityCaptureMode,
        };
        const kind = {
          type: 'mode_only' as const,
          selectionChanged: false,
          entersDisabled: false,
          previousActiveBindingId: null,
        };
        expect(
          await applyWorkspaceAgentObservabilityMutation({
            ...common,
            authority: workspaceAuthority,
            request: workspaceRequest,
            kind,
          }),
        ).toEqual({ applied: true });
        expect(writes.at(-1)).toMatchObject({
          captureCeiling: next,
          captureRestrictionEpoch: i > j ? 8 : 7,
          selectionEpoch: 2,
          revocationEpoch: 3,
        });
        const capacity = () =>
          assertWorkspaceAgentObservabilityMutationCapacity(
            { ...workspaceAuthority, captureRestrictionEpoch: Number.MAX_SAFE_INTEGER },
            workspaceRequest,
            kind,
          );
        if (i > j) expect(capacity).toThrow(WorkspaceAgentObservabilityMutationInvariantError);
        else expect(capacity).not.toThrow();
      });
    }
  }
});
