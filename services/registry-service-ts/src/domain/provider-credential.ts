// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export const LOGICAL_CREDENTIAL_ID_PREFIX = 'llm:';
export const LOGICAL_CREDENTIAL_ID_MAX_LENGTH = 128;
const LOGICAL_CREDENTIAL_ID_TRAILING_MAX_LENGTH =
  LOGICAL_CREDENTIAL_ID_MAX_LENGTH - LOGICAL_CREDENTIAL_ID_PREFIX.length - 1;
export const LOGICAL_CREDENTIAL_ID_PATTERN = new RegExp(
  `^${LOGICAL_CREDENTIAL_ID_PREFIX}[A-Za-z0-9][A-Za-z0-9._:-]{0,${LOGICAL_CREDENTIAL_ID_TRAILING_MAX_LENGTH}}$`,
);

export const PROVIDER_CREDENTIAL_PROVIDERS = [
  'anthropic',
  'openai',
  'openai_compatible',
  'azure_openai',
  'vertex',
  'bedrock',
] as const;

export type ProviderCredentialProvider = (typeof PROVIDER_CREDENTIAL_PROVIDERS)[number];

export const PROVIDER_CREDENTIAL_SCHEMES = [
  'api_key',
  'bearer',
  'gcp-service-account',
  'aws-sig-v4',
] as const;

export type ProviderCredentialScheme = (typeof PROVIDER_CREDENTIAL_SCHEMES)[number];

const PROVIDER_SCHEMES: Record<
  ProviderCredentialProvider,
  ReadonlySet<ProviderCredentialScheme>
> = {
  anthropic: new Set(['api_key']),
  openai: new Set(['bearer']),
  openai_compatible: new Set(['bearer']),
  azure_openai: new Set(['api_key', 'bearer']),
  vertex: new Set(['gcp-service-account']),
  bedrock: new Set(['aws-sig-v4']),
};

export function isLogicalCredentialId(value: string): boolean {
  const match = LOGICAL_CREDENTIAL_ID_PATTERN.exec(value);
  return match !== null && match[0] === value;
}

export function isProviderCredentialProvider(value: unknown): value is ProviderCredentialProvider {
  return (
    typeof value === 'string' &&
    (PROVIDER_CREDENTIAL_PROVIDERS as readonly string[]).includes(value)
  );
}

export function isProviderCredentialScheme(value: unknown): value is ProviderCredentialScheme {
  return (
    typeof value === 'string' && (PROVIDER_CREDENTIAL_SCHEMES as readonly string[]).includes(value)
  );
}

export function isCompatibleProviderCredentialScheme(
  provider: ProviderCredentialProvider,
  scheme: ProviderCredentialScheme,
): boolean {
  return PROVIDER_SCHEMES[provider].has(scheme);
}
