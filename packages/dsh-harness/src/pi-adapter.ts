// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Context } from '@deepseek-ai/cordis';
import { PiAiAdapter, type ResolvedPiAiProviderProfile } from '@deepseek-ai/dsh-llm-pi-ai';
import { resolveRetryPolicy } from '@deepseek-ai/dsh-llm';
import { InMemoryCredentialStore } from '@earendil-works/pi-ai';
import { builtinProviders } from '@earendil-works/pi-ai/providers/all';
import { assertDshProtocol } from './transport.js';

/** Official adapter, restricted to the pinned selection and Gateway-owned auth. */
export function registerManagedPi(
  ctx: Context,
  provider: string,
  id: string,
): { api: string; baseUrl: string } {
  const native = builtinProviders().find((item) => item.id === provider);
  const listed = native?.getModels().find((item) => item.id === id);
  if (!native || !listed) throw new Error('DSH installed catalog does not contain selected model');
  // Catalog fallback lists cannot widen the pinned model or its JWT grants.
  const model = { ...listed, compat: { ...listed.compat, allowedFallbackModels: [] } };
  assertDshProtocol(model.api);
  const profile: ResolvedPiAiProviderProfile = {
    provider,
    displayName: provider,
    transport: 'sse',
    streamIdleTimeoutMs: 600000,
    maxRequestImageBytes: 20 * 1024 * 1024,
    requestImagePixelBudget: 4000000,
    requestImageMaxBytes: 4 * 1024 * 1024,
    retryPolicy: resolveRetryPolicy({ mode: 'normal', maxRetries: 0 }, 'DSH managed retry'),
    modelErrors: new Map(),
    configuredMaxTokens: new Map(),
    piProvider: {
      ...native,
      // No source may refresh the selection or discover native credentials.
      refreshModels: undefined,
      auth: {
        apiKey: {
          name: 'Orca managed',
          login: async () => {
            throw new Error('DSH native login is disabled');
          },
          resolve: async () => ({ auth: { apiKey: 'orca-managed' } }),
        },
      },
      getModels: () => [model],
      getAllModels: () => [model],
      streamSimple: (selected, context, options) => {
        if (!options?.sessionId) throw new Error('DSH native request identity unavailable');
        return native.streamSimple(selected, context, {
          ...options,
          headers: {
            ...options.headers,
            'x-deepseek-harness-session-id': options.sessionId,
          },
        });
      },
    },
  };
  const profiles = new Map([[provider, profile]]);
  ctx.llm.registerAdapter(
    [provider],
    new PiAiAdapter({
      profiles: () => profiles,
      resolveApiKey: async () => 'orca-managed',
      auth: {
        credentials: new InMemoryCredentialStore(),
        authContext: { env: async () => undefined, fileExists: async () => false },
      },
    }),
  );
  return { api: model.api, baseUrl: model.baseUrl };
}
