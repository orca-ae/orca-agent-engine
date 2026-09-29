// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { SecretProvider } from './secret-provider.js';
import { EnvSecretProvider, envIsSupported } from './env.js';

export interface DelegatedProvider {
  provider: SecretProvider;
  isSupported: (reference: string) => boolean;
}

export class DefaultSecretProvider implements SecretProvider {
  constructor(
    private readonly env: EnvSecretProvider,
    private readonly delegated: DelegatedProvider[],
  ) {}

  async resolve(reference: string): Promise<string | null> {
    const trimmed = reference?.trim();
    if (!trimmed) return null;
    if (envIsSupported(trimmed)) return this.env.resolve(trimmed);
    for (const d of this.delegated) {
      if (d.isSupported(trimmed)) return d.provider.resolve(trimmed);
    }
    return trimmed;
  }
}
