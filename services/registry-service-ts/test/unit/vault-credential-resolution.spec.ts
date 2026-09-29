// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  classifyRequestedVaultCredentialId,
  hasCanonicalRequestedCredentialPath,
  logicalResolutionAuditMetadata,
  providerCredentialResolution,
  selectAuthorizedLogicalCredential,
  selectLogicalCredential,
} from '../../src/domain/vault-credential-resolution.js';

describe('vault credential resolution domain', () => {
  it('classifies only concrete IDs and namespaced logical aliases', () => {
    expect(classifyRequestedVaultCredentialId('vcrd_concrete')).toBe('concrete');
    expect(classifyRequestedVaultCredentialId('llm:anthropic')).toBe('logical');
    expect(classifyRequestedVaultCredentialId('anthropic')).toBeNull();
    expect(classifyRequestedVaultCredentialId('vcrd/unsafe')).toBeNull();
  });

  it('requires the raw path segment to use the canonical unencoded spelling', () => {
    expect(
      hasCanonicalRequestedCredentialPath(
        '/internal/v1/workspaces/ws_a/sessions/ses_a/vault-credentials/llm:anthropic/resolve',
        'llm:anthropic',
      ),
    ).toBe(true);
    expect(
      hasCanonicalRequestedCredentialPath(
        '/internal/v1/workspaces/ws_a/sessions/ses_a/vault-credentials/llm%3Aanthropic/resolve',
        'llm:anthropic',
      ),
    ).toBe(false);
    expect(
      hasCanonicalRequestedCredentialPath(
        '/internal/v1/workspaces/ws_a/sessions/ses_a/vault-credentials/llm:anthropic/resolve?x=1',
        'llm:anthropic',
      ),
    ).toBe(true);
  });

  it('fails closed unless a logical lookup selects exactly one row', () => {
    expect(selectLogicalCredential([])).toEqual({ status: 'not_found' });
    expect(selectLogicalCredential([{ id: 'a' }, { id: 'b' }])).toEqual({
      status: 'ambiguous',
    });
    expect(selectLogicalCredential([{ id: 'a' }])).toEqual({
      status: 'resolved',
      row: { id: 'a' },
    });
  });

  it('authorizes logical candidates only through the active session vault binding', () => {
    const base = {
      workspaceId: 'ws_a',
      authType: 'provider',
      logicalId: 'llm:anthropic',
      archivedAt: null,
    };
    const rows = [
      { ...base, id: 'vcrd_authorized', vaultId: 'vlt_attached' },
      { ...base, id: 'vcrd_unattached', vaultId: 'vlt_unattached' },
      { ...base, id: 'vcrd_foreign', workspaceId: 'ws_b', vaultId: 'vlt_attached' },
      { ...base, id: 'vcrd_archived', vaultId: 'vlt_attached', archivedAt: new Date() },
      { ...base, id: 'vcrd_mcp', vaultId: 'vlt_attached', authType: 'static_bearer' },
    ];

    expect(
      selectAuthorizedLogicalCredential(rows, {
        workspaceId: 'ws_a',
        vaultIds: ['vlt_attached'],
        requestedCredentialId: 'llm:anthropic',
      }),
    ).toEqual({ status: 'resolved', row: rows[0] });
    expect(
      selectAuthorizedLogicalCredential(rows, {
        workspaceId: 'ws_a',
        vaultIds: ['vlt_other'],
        requestedCredentialId: 'llm:anthropic',
      }),
    ).toEqual({ status: 'not_found' });
  });

  it('fails closed when two authorized vault bindings contain the same logical alias', () => {
    const rows = ['vlt_a', 'vlt_b'].map((vaultId, index) => ({
      id: `vcrd_${index}`,
      workspaceId: 'ws_a',
      vaultId,
      authType: 'provider',
      logicalId: 'llm:anthropic',
      archivedAt: null,
    }));

    expect(
      selectAuthorizedLogicalCredential(rows, {
        workspaceId: 'ws_a',
        vaultIds: ['vlt_a', 'vlt_b'],
        requestedCredentialId: 'llm:anthropic',
      }),
    ).toEqual({ status: 'ambiguous' });
  });

  it.each([
    ['anthropic', 'api_key'],
    ['openai', 'bearer'],
    ['azure_openai', 'api_key'],
    ['azure_openai', 'bearer'],
    ['vertex', 'gcp-service-account'],
    ['bedrock', 'aws-sig-v4'],
  ])('translates %s credentials with canonical scheme %s', (provider, scheme) => {
    expect(
      providerCredentialResolution(
        {
          id: 'vcrd_selected',
          vaultId: 'vlt_provider',
          authType: 'provider',
          provider,
          scheme,
          logicalId: `llm:${provider}`,
          resolutionVersion: 'opaque-version',
        },
        'secret-material',
      ),
    ).toEqual({
      credential_id: 'vcrd_selected',
      vault_id: 'vlt_provider',
      version: 'opaque-version',
      scheme,
      secret_value: 'secret-material',
      ttl_seconds: 300,
    });
  });

  it('rejects corrupt or incompatible provider rows', () => {
    expect(
      providerCredentialResolution(
        {
          id: 'vcrd_selected',
          vaultId: 'vlt_provider',
          authType: 'provider',
          provider: 'anthropic',
          scheme: 'bearer',
          logicalId: 'llm:anthropic',
          resolutionVersion: 'opaque-version',
        },
        'secret-material',
      ),
    ).toBeNull();
  });

  it('builds success and denial audit metadata without secret fields or candidates', () => {
    const success = logicalResolutionAuditMetadata('ses_a', 'llm:anthropic', {
      outcome: 'success',
      selectedCredentialId: 'vcrd_selected',
      credentialVersion: 'opaque-version',
    });
    const denied = logicalResolutionAuditMetadata('ses_a', 'llm:anthropic', {
      outcome: 'denied',
      reason: 'ambiguous',
    });

    expect(success).toEqual({
      session_id: 'ses_a',
      requested_credential_id: 'llm:anthropic',
      selected_credential_id: 'vcrd_selected',
      credential_version: 'opaque-version',
    });
    expect(denied).toEqual({
      session_id: 'ses_a',
      requested_credential_id: 'llm:anthropic',
      reason: 'ambiguous',
    });
    expect(JSON.stringify({ success, denied })).not.toMatch(/secret|access_secret_ref|candidate/i);
  });
});
