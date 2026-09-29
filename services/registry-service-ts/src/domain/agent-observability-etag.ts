// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';

/**
 * Non-secret, authoritative values that make an observability-state response
 * stale. This deliberately excludes credential references and all SecretStore
 * material.
 */
export interface AgentObservabilityStateEtagInput {
  scope: 'organization' | 'workspace';
  organizationId: string;
  workspaceId: string | null;
  platformPolicy: readonly [
    allowedAdapters: readonly string[],
    allowedEndpointClasses: readonly string[],
    maxCaptureMode: string,
    captureRestrictionEpoch: number,
  ];
  organizationSetting: readonly [
    activeDefaultBindingId: string | null,
    activeDefaultBindingScope: string | null,
    selectionEpoch: number,
    defaultRevocationEpoch: number,
    organizationRevocationEpoch: number,
    captureCeiling: string,
    captureRestrictionEpoch: number,
  ];
  workspaceSetting:
    | readonly [
        mode: string,
        bindingId: string | null,
        selectionEpoch: number,
        revocationEpoch: number,
        captureCeiling: string,
        captureRestrictionEpoch: number,
      ]
    | null;
  binding:
    | readonly [
        id: string,
        organizationId: string,
        workspaceId: string | null,
        scopeType: string,
        adapterType: string,
        endpointKind: string,
        endpointClass: string,
        endpoint: string,
        externalProjectId: string | null,
        currentVersion: number,
        status: string,
        revocationEpoch: number,
        archivedAt: string | null,
      ]
    | null;
  version:
    | readonly [
        version: number,
        adapterType: string,
        semanticProfile: string,
        protocol: string,
        compression: string,
        timeoutMs: number,
        environment: string | null,
        release: string | null,
        captureMode: string,
        sampleRate: number,
        configSchemaVersion: number,
      ]
    | null;
  credential:
    | readonly [
        configured: boolean,
        version: number | null,
        keyHint: string | null,
        rotatedAt: string | null,
      ]
    | null;
}

const ETAG_DOMAIN = 'orca-managed-agents:agent-observability-state-etag:v1\0';

/**
 * Strong ETag for one admin state representation. JSON array construction is
 * positional and deterministic; SHA-256 keeps all authoritative tuple values
 * opaque to callers.
 */
export function agentObservabilityStateEtag(input: AgentObservabilityStateEtagInput): string {
  const encoded = JSON.stringify([
    input.scope,
    input.organizationId,
    input.workspaceId,
    [
      [...input.platformPolicy[0]].sort(),
      [...input.platformPolicy[1]].sort(),
      input.platformPolicy[2],
      input.platformPolicy[3],
    ],
    input.organizationSetting,
    input.workspaceSetting,
    input.binding,
    input.version,
    input.credential,
  ]);
  const digest = createHash('sha256').update(ETAG_DOMAIN).update(encoded).digest('base64url');
  return `"orca-aos-v1-${digest}"`;
}
