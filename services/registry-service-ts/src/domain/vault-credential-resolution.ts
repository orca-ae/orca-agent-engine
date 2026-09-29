// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  isCompatibleProviderCredentialScheme,
  isLogicalCredentialId,
  isProviderCredentialProvider,
  isProviderCredentialScheme,
  type ProviderCredentialScheme,
} from './provider-credential.js';

export const CONCRETE_VAULT_CREDENTIAL_ID_PATTERN = /^vcrd_[A-Za-z0-9_-]+$/;

export type RequestedVaultCredentialKind = 'concrete' | 'logical';

export function classifyRequestedVaultCredentialId(
  value: string,
): RequestedVaultCredentialKind | null {
  if (CONCRETE_VAULT_CREDENTIAL_ID_PATTERN.test(value)) return 'concrete';
  if (isLogicalCredentialId(value)) return 'logical';
  return null;
}

export function hasCanonicalRequestedCredentialPath(
  rawUrl: string | undefined,
  decodedCredentialId: string,
): boolean {
  if (rawUrl === undefined) return false;
  const rawPath = rawUrl.split('?', 1)[0]!;
  const marker = '/vault-credentials/';
  const suffix = '/resolve';
  const markerIndex = rawPath.lastIndexOf(marker);
  if (markerIndex < 0 || !rawPath.endsWith(suffix)) return false;
  const rawCredentialId = rawPath.slice(markerIndex + marker.length, -suffix.length);
  return rawCredentialId === decodedCredentialId;
}

export type LogicalCredentialSelection<T> =
  | { status: 'resolved'; row: T }
  | { status: 'not_found' | 'ambiguous' };

export function selectLogicalCredential<T>(rows: readonly T[]): LogicalCredentialSelection<T> {
  if (rows.length === 0) return { status: 'not_found' };
  if (rows.length !== 1) return { status: 'ambiguous' };
  return { status: 'resolved', row: rows[0]! };
}

export interface LogicalCredentialAuthorizationRow {
  workspaceId: string;
  vaultId: string;
  authType: string;
  logicalId: string | null;
  archivedAt: Date | null;
}

export interface LogicalCredentialAuthorization {
  workspaceId: string;
  vaultIds: readonly string[];
  requestedCredentialId: string;
}

export function selectAuthorizedLogicalCredential<T extends LogicalCredentialAuthorizationRow>(
  rows: readonly T[],
  authorization: LogicalCredentialAuthorization,
): LogicalCredentialSelection<T> {
  const authorizedVaultIds = new Set(authorization.vaultIds);
  return selectLogicalCredential(
    rows.filter(
      (row) =>
        row.workspaceId === authorization.workspaceId &&
        row.authType === 'provider' &&
        row.logicalId === authorization.requestedCredentialId &&
        authorizedVaultIds.has(row.vaultId) &&
        row.archivedAt === null,
    ),
  );
}

export interface ProviderResolutionRow {
  id: string;
  vaultId: string;
  authType: string;
  provider: string | null;
  scheme: string | null;
  logicalId: string | null;
  resolutionVersion: string | null;
}

export interface ProviderCredentialResolution {
  credential_id: string;
  vault_id: string;
  version: string;
  scheme: ProviderCredentialScheme;
  secret_value: string;
  ttl_seconds: number;
}

export function providerCredentialResolution(
  row: ProviderResolutionRow,
  secretValue: string,
): ProviderCredentialResolution | null {
  if (
    row.authType !== 'provider' ||
    !isProviderCredentialProvider(row.provider) ||
    !isProviderCredentialScheme(row.scheme) ||
    !isCompatibleProviderCredentialScheme(row.provider, row.scheme) ||
    row.logicalId === null ||
    !isLogicalCredentialId(row.logicalId) ||
    row.resolutionVersion === null ||
    row.resolutionVersion.length === 0
  ) {
    return null;
  }
  return {
    credential_id: row.id,
    vault_id: row.vaultId,
    version: row.resolutionVersion,
    scheme: row.scheme,
    secret_value: secretValue,
    ttl_seconds: 300,
  };
}

export type LogicalResolutionAuditInput =
  | {
      outcome: 'success';
      selectedCredentialId: string;
      credentialVersion: string;
    }
  | {
      outcome: 'denied';
      reason: 'not_found' | 'ambiguous' | 'secret_unavailable' | 'invalid_provider_credential';
    };

export function logicalResolutionAuditMetadata(
  sessionId: string,
  requestedCredentialId: string,
  input: LogicalResolutionAuditInput,
): Record<string, string> {
  const base = {
    session_id: sessionId,
    requested_credential_id: requestedCredentialId,
  };
  if (input.outcome === 'success') {
    return {
      ...base,
      selected_credential_id: input.selectedCredentialId,
      credential_version: input.credentialVersion,
    };
  }
  return { ...base, reason: input.reason };
}
