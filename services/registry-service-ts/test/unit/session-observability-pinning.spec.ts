// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  newSessionObservabilityBindingRow,
  type CreateSessionInTransactionInput,
} from '../../src/domain/session-creation.js';
import type { SessionObservabilitySelection } from '../../src/domain/agent-observability-session-selection.js';

const now = new Date('2035-08-18T03:00:00.000Z');
const session = {
  id: 'ses_observability_pin_unit',
  workspaceId: 'ws_observability_pin_unit',
  agentId: 'agt_observability_pin_unit',
  agentVersion: 7,
  now,
} satisfies Pick<
  CreateSessionInTransactionInput,
  'id' | 'workspaceId' | 'agentId' | 'agentVersion' | 'now'
>;

const activeSelection: SessionObservabilitySelection = {
  organizationId: 'org_observability_pin_unit',
  workspaceId: session.workspaceId,
  selectionSource: 'workspace_custom',
  status: 'active',
  bindingId: 'aob_observability_pin_unit',
  bindingVersion: 3,
  bindingScope: 'workspace',
  bindingWorkspaceId: session.workspaceId,
  organizationSelectionEpoch: 11,
  workspaceSelectionEpoch: 12,
  organizationDefaultRevocationEpoch: 13,
  organizationRevocationEpoch: 14,
  workspaceRevocationEpoch: 15,
  bindingRevocationEpoch: 16,
  platformCaptureRestrictionEpoch: 17,
  organizationCaptureRestrictionEpoch: 18,
  workspaceCaptureRestrictionEpoch: 19,
  effectiveCaptureMode: 'redacted_io',
  disabledReason: null,
};

describe('Session observability pin rows', () => {
  it('copies active selection epochs and locked Agent Harness facts', () => {
    expect(
      newSessionObservabilityBindingRow(session, activeSelection, {
        harness: 'codex',
        mode: 'colocated',
      }),
    ).toEqual({
      workspaceId: session.workspaceId,
      sessionId: session.id,
      organizationId: activeSelection.organizationId,
      bindingId: activeSelection.bindingId,
      bindingVersion: activeSelection.bindingVersion,
      bindingScope: activeSelection.bindingScope,
      bindingWorkspaceId: activeSelection.bindingWorkspaceId,
      selectionSource: 'workspace_custom',
      status: 'active',
      organizationSelectionEpoch: 11,
      workspaceSelectionEpoch: 12,
      organizationDefaultRevocationEpoch: 13,
      organizationRevocationEpoch: 14,
      workspaceRevocationEpoch: 15,
      bindingRevocationEpoch: 16,
      platformCaptureRestrictionEpoch: 17,
      organizationCaptureRestrictionEpoch: 18,
      workspaceCaptureRestrictionEpoch: 19,
      effectiveCaptureMode: 'redacted_io',
      sessionRevocationEpoch: 0,
      agentId: session.agentId,
      agentVersion: session.agentVersion,
      harness: 'codex',
      harnessMode: 'colocated',
      archivedAt: null,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
  });

  it('retains no target identity for a disabled selection', () => {
    const disabledSelection: SessionObservabilitySelection = {
      organizationId: activeSelection.organizationId,
      workspaceId: session.workspaceId,
      selectionSource: 'disabled',
      status: 'disabled',
      bindingId: null,
      bindingVersion: null,
      bindingScope: null,
      bindingWorkspaceId: null,
      organizationSelectionEpoch: 21,
      workspaceSelectionEpoch: 22,
      organizationDefaultRevocationEpoch: 23,
      organizationRevocationEpoch: 24,
      workspaceRevocationEpoch: 25,
      bindingRevocationEpoch: 0,
      platformCaptureRestrictionEpoch: 27,
      organizationCaptureRestrictionEpoch: 28,
      workspaceCaptureRestrictionEpoch: 29,
      effectiveCaptureMode: 'metadata_only',
      disabledReason: 'workspace_disabled',
    };

    const row = newSessionObservabilityBindingRow(session, disabledSelection, {
      harness: 'claude_agent_sdk',
      mode: 'separate',
    });

    expect(row).toMatchObject({
      selectionSource: 'disabled',
      status: 'disabled',
      bindingId: null,
      bindingVersion: null,
      bindingScope: null,
      bindingWorkspaceId: null,
      bindingRevocationEpoch: 0,
      effectiveCaptureMode: 'metadata_only',
      sessionRevocationEpoch: 0,
    });
  });
});
