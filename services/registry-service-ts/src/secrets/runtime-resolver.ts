// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { DefaultSecretProvider } from './default.js';
import { EnvSecretProvider } from './env.js';
import type { SecretProvider, SecretStore } from './secret-provider.js';

/**
 * Resolve runtime secret references across the write-capable SecretStore and
 * legacy/read-only SecretProvider chain.
 *
 * SecretStore owns opaque `local:` references materialized at the public API
 * boundary. Legacy env/cloud references continue to resolve directly through
 * SecretProvider, so a SecretStore outage cannot break unrelated backends.
 */
export function buildRuntimeSecretResolver(
  secretProvider?: SecretProvider,
  secretStore?: SecretStore,
): SecretProvider {
  const fallback = new DefaultSecretProvider(
    new EnvSecretProvider((key) => process.env[key] ?? null),
    [],
  );

  return {
    async resolve(reference: string): Promise<string | null> {
      const trimmed = reference?.trim();
      if (!trimmed) return null;

      // `DefaultSecretProvider` returns unknown references verbatim. Never let
      // that fallback turn an unresolved opaque local reference into a token.
      const isLocalRef = trimmed.startsWith('local:');
      if (isLocalRef && secretStore) {
        const storeValue = await secretStore.resolve(trimmed);
        if (storeValue !== null) return storeValue;
      }
      if (secretProvider) {
        const providerValue = await secretProvider.resolve(trimmed);
        if (providerValue !== null && !(isLocalRef && providerValue === trimmed)) {
          return providerValue;
        }
      }

      if (isLocalRef) return null;
      return fallback.resolve(trimmed);
    },
  };
}
