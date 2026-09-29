// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  agentObservabilityOrganizationSettings,
  agentObservabilityWorkspaceSettings,
} from '../persistence/postgres/schema.js';

export const AGENT_OBSERVABILITY_DEFAULT_CAPTURE_MODE = 'metadata_only';

/**
 * Build the stable organization setting row that must commit with its parent.
 * A missing row is corruption, never an implicit inheritance/default signal.
 */
export function newOrganizationObservabilitySettings(organizationId: string, now: Date) {
  return {
    organizationId,
    activeDefaultBindingId: null,
    activeDefaultBindingScope: null,
    selectionEpoch: 0,
    defaultRevocationEpoch: 0,
    organizationRevocationEpoch: 0,
    captureCeiling: AGENT_OBSERVABILITY_DEFAULT_CAPTURE_MODE,
    captureRestrictionEpoch: 0,
    createdAt: now,
    updatedAt: now,
  } satisfies typeof agentObservabilityOrganizationSettings.$inferInsert;
}

/**
 * Build the stable workspace setting row that must commit with its parent.
 * New workspaces inherit the organization default but start with no binding.
 */
export function newWorkspaceObservabilitySettings(
  organizationId: string,
  workspaceId: string,
  now: Date,
) {
  return {
    workspaceId,
    organizationId,
    mode: 'inherit',
    bindingId: null,
    selectionEpoch: 0,
    revocationEpoch: 0,
    captureCeiling: AGENT_OBSERVABILITY_DEFAULT_CAPTURE_MODE,
    captureRestrictionEpoch: 0,
    createdAt: now,
    updatedAt: now,
  } satisfies typeof agentObservabilityWorkspaceSettings.$inferInsert;
}
