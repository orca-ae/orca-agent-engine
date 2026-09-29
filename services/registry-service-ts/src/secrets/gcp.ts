// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { extract } from './payloads.js';
import type { SecretProvider } from './secret-provider.js';

const SCHEME = /^gcpsm:\/\/([^/]+)\/([^/#]+)(\/versions\/([^#]+))?(#(.+))?$/;

export function gcpIsSupported(reference: string): boolean {
  return SCHEME.test(reference);
}

export type GcpClientFactory = () => SecretManagerServiceClient;

export class GcpSecretManagerSecretProvider implements SecretProvider {
  constructor(private readonly clientFactory: GcpClientFactory) {}

  static systemDefault(): GcpSecretManagerSecretProvider {
    return new GcpSecretManagerSecretProvider(() => new SecretManagerServiceClient());
  }

  async resolve(reference: string): Promise<string | null> {
    const m = SCHEME.exec(reference);
    if (!m) return null;
    const [, project, secret, , version, , jsonKey] = m;
    const client = this.clientFactory();
    const [response] = await client.accessSecretVersion({
      name: `projects/${project}/secrets/${secret}/versions/${version ?? 'latest'}`,
    });
    const data = response?.payload?.data;
    if (!data) return null;
    const value = typeof data === 'string' ? data : Buffer.from(data).toString('utf8');
    return jsonKey ? extract(value, jsonKey) : value;
  }
}
