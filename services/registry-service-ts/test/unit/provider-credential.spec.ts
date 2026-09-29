// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  LOGICAL_CREDENTIAL_ID_MAX_LENGTH,
  LOGICAL_CREDENTIAL_ID_PREFIX,
  isCompatibleProviderCredentialScheme,
  isLogicalCredentialId,
  type ProviderCredentialProvider,
  type ProviderCredentialScheme,
} from '../../src/domain/provider-credential.js';

describe('provider credential domain', () => {
  it('accepts exactly the llm-namespaced URL-path-safe logical-id grammar', () => {
    const maximum = `${LOGICAL_CREDENTIAL_ID_PREFIX}${'a'.repeat(
      LOGICAL_CREDENTIAL_ID_MAX_LENGTH - LOGICAL_CREDENTIAL_ID_PREFIX.length,
    )}`;
    expect(isLogicalCredentialId('llm:a')).toBe(true);
    expect(isLogicalCredentialId('llm:anthropic.prod_1')).toBe(true);
    expect(maximum).toHaveLength(LOGICAL_CREDENTIAL_ID_MAX_LENGTH);
    expect(isLogicalCredentialId(maximum)).toBe(true);

    for (const value of [
      '',
      'a',
      'llm:',
      'vcrd_concrete_id',
      `${maximum}a`,
      'llm/anthropic',
      'llm?anthropic',
      'llm#anthropic',
      'llm%2Fanthropic',
      'llm anthropic',
      'llm:anthropic\n',
      'llm:模型',
    ]) {
      expect(isLogicalCredentialId(value), value).toBe(false);
    }
  });

  it.each<{
    provider: ProviderCredentialProvider;
    scheme: ProviderCredentialScheme;
  }>([
    { provider: 'anthropic', scheme: 'api_key' },
    { provider: 'openai', scheme: 'bearer' },
    { provider: 'openai_compatible', scheme: 'bearer' },
    { provider: 'azure_openai', scheme: 'api_key' },
    { provider: 'azure_openai', scheme: 'bearer' },
    { provider: 'vertex', scheme: 'gcp-service-account' },
    { provider: 'bedrock', scheme: 'aws-sig-v4' },
  ])('accepts $provider with $scheme', ({ provider, scheme }) => {
    expect(isCompatibleProviderCredentialScheme(provider, scheme)).toBe(true);
  });

  it('rejects a known but provider-incompatible scheme', () => {
    expect(isCompatibleProviderCredentialScheme('anthropic', 'bearer')).toBe(false);
    expect(isCompatibleProviderCredentialScheme('vertex', 'api_key')).toBe(false);
    expect(isCompatibleProviderCredentialScheme('bedrock', 'gcp-service-account')).toBe(false);
  });
});
