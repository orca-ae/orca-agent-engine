// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { SecretProvider } from './secret-provider.js';

export type EnvLookup = (key: string) => string | null;

export function envIsSupported(reference: string): boolean {
  if (reference.startsWith('env:')) return true;
  if (reference.startsWith('${') && reference.endsWith('}') && reference.length > 3) return true;
  return false;
}

export class EnvSecretProvider implements SecretProvider {
  constructor(private readonly lookup: EnvLookup) {}

  async resolve(reference: string): Promise<string | null> {
    if (reference.startsWith('env:')) {
      const value = this.lookup(reference.slice(4));
      return value && value.trim().length > 0 ? value : null;
    }
    if (reference.startsWith('${') && reference.endsWith('}') && reference.length > 3) {
      const value = this.lookup(reference.slice(2, -1));
      return value && value.trim().length > 0 ? value : null;
    }
    return reference;
  }
}
