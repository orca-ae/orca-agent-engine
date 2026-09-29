// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Provider registry.
//
// A provider (see ./types.ts) drives one underlying agent CLI/SDK in-process and
// maps its output to the canonical stream-json wire. Each provider declares a
// canonical `id`, optional `aliases`, and a `createRuntime` factory; the registry
// builds an id+aliases -> provider lookup so a client can request an agent by any
// of its names (case-insensitively) and exposes the public catalog for the
// `list_harnesses` control request.
//
// Why a hardcoded array instead of filesystem auto-discovery:
//   Scanning a providers directory at startup (`readdirSync` + dynamic
//   `import()`) earns its keep with many drop-in providers, but here the provider
//   set is small, and runtime directory scanning is hostile to a bundled dist:
//   tsup/ESM emits a single file, so there are no per-provider folders to read
//   and dynamic-import specifiers can't be statically analyzed. A static `import`
//   + a literal array is bundler-friendly and keeps the wiring obvious. Adding a
//   provider is a one-line change: import it, then push it onto PROVIDERS.

import type { Provider } from './types.js';
import { claudeProvider } from './claude.js';
import { codexSdkProvider, piSdkProvider } from './codex-sdk.js';

/**
 * Public-facing provider descriptor returned by {@link listProviderMetadata} and
 * surfaced to clients via the `list_harnesses` control request. Decoupled from the
 * internal {@link Provider} shape: `id` is the public harness id (`harnessId` falls
 * back to the canonical `id`) and `name` is the display label.
 */
export interface ProviderMetadata {
  /** Public harness id (`harnessId` || `id`). */
  id: string;
  /** Canonical provider id. */
  providerId: string;
  /** Display name (`displayName` || `id`). */
  name: string;
  /** Alternate request ids. */
  aliases: string[];
}

/**
 * The set of registered providers. Order is irrelevant (lookup is by id/alias and
 * metadata is sorted by name). Add a provider here to register it everywhere; this
 * literal array is the single registration point.
 */
const PROVIDERS: readonly Provider[] = [claudeProvider, codexSdkProvider, piSdkProvider];

/**
 * id + aliases -> provider, keyed lower-case for case-insensitive resolution.
 * Built once at module load; later keys win on collision (last writer wins), so
 * ordering within {@link PROVIDERS} is the tiebreak.
 */
const REGISTRY: ReadonlyMap<string, Provider> = (() => {
  const map = new Map<string, Provider>();
  for (const provider of PROVIDERS) {
    for (const key of [provider.id, ...(provider.aliases ?? [])]) {
      map.set(key.toLowerCase(), provider);
    }
  }
  return map;
})();

/**
 * Resolve an agent id (canonical or alias, case-insensitive) to its provider.
 *
 * @throws Error `unsupported agent: <agent> (known: <ids…>)` when no provider
 *   matches. The subprocess entry catches this and exits 2, which the manager
 *   surfaces upstream as `session.status_error`.
 */
export function resolveProvider(agent: string | null | undefined): Provider {
  const provider = REGISTRY.get(String(agent ?? '').toLowerCase());
  if (!provider) {
    throw new Error(`unsupported agent: ${agent} (known: ${[...REGISTRY.keys()].join(', ')})`);
  }
  return provider;
}

/**
 * Public provider catalog for the `list_harnesses` control request: one entry per
 * distinct provider, sorted by display name. The `{ id, providerId, name, aliases }`
 * shape is stable so clients and future providers stay compatible.
 */
export function listProviderMetadata(): ProviderMetadata[] {
  const seen = new Set<Provider>();
  const metadata: ProviderMetadata[] = [];

  for (const provider of REGISTRY.values()) {
    if (seen.has(provider)) continue;
    seen.add(provider);
    metadata.push({
      id: provider.harnessId || provider.id,
      providerId: provider.id,
      name: provider.displayName || provider.id,
      aliases: [...(provider.aliases ?? [])],
    });
  }

  return metadata.sort((a, b) => a.name.localeCompare(b.name));
}
