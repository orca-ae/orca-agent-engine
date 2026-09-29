// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { SecretClient } from '@azure/keyvault-secrets';
import { DefaultAzureCredential } from '@azure/identity';
import { extract } from './payloads.js';
import type { SecretProvider } from './secret-provider.js';

const SCHEME =
  /^azurekv:\/\/([^/]+\.vault\.azure\.net)\/secrets\/([^/#]+)(?:\/([^/#]+))?(?:#(.+))?$/;

export function azureIsSupported(reference: string): boolean {
  return SCHEME.test(reference);
}

export type AzureClientFactory = (vaultUrl: string) => SecretClient;

export class AzureKeyVaultSecretProvider implements SecretProvider {
  constructor(private readonly clientFactory: AzureClientFactory) {}

  static systemDefault(): AzureKeyVaultSecretProvider {
    return new AzureKeyVaultSecretProvider(
      (url) => new SecretClient(url, new DefaultAzureCredential()),
    );
  }

  async resolve(reference: string): Promise<string | null> {
    const m = SCHEME.exec(reference);
    if (!m) return null;
    const [, host, name, version, jsonKey] = m;
    const client = this.clientFactory(`https://${host}`);
    const result = await client.getSecret(name!, version ? { version } : undefined);
    if (!result.value) return null;
    return jsonKey ? extract(result.value, jsonKey) : result.value;
  }
}
