// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import { extract } from './payloads.js';
import type { SecretProvider } from './secret-provider.js';

const SCHEME = /^awssm:\/\/([^/]+)\/([^#]+)(#(.+))?$/;

export function awsIsSupported(reference: string): boolean {
  return SCHEME.test(reference);
}

export type SecretsClientFactory = (region: string) => SecretsManagerClient;

export class AwsSecretsManagerSecretProvider implements SecretProvider {
  constructor(private readonly clientFactory: SecretsClientFactory) {}

  static systemDefault(): AwsSecretsManagerSecretProvider {
    return new AwsSecretsManagerSecretProvider((region) => new SecretsManagerClient({ region }));
  }

  async resolve(reference: string): Promise<string | null> {
    const m = SCHEME.exec(reference);
    if (!m) return null;
    const [, region, name, , jsonKey] = m;
    const client = this.clientFactory(region!);
    const out = await client.send(new GetSecretValueCommand({ SecretId: name }));
    const value =
      out.SecretString ??
      (out.SecretBinary ? Buffer.from(out.SecretBinary as Uint8Array).toString('utf8') : null);
    if (!value) return null;
    return jsonKey ? extract(value, jsonKey) : value;
  }
}
