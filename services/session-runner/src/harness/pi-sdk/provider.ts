// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { ProviderRegistry } from '../provider.js';
import { readGatewayLlmEgress } from '../claude/provider.js';
import { CodexSdkHarness, type CodexSdkOptions } from '../codex-sdk/index.js';
import type { CodexCheckpoint } from '@orca/codex-harness';

export function registerPiSdkProvider(
  registry: ProviderRegistry,
  options: Partial<CodexSdkOptions> & {
    credentials?: import('@orca/pi-harness').PiProviderCredentials;
  } = {},
): void {
  registry.register(
    'pi-sdk',
    (snapshot) => {
      const egress = readGatewayLlmEgress(snapshot);
      const direct = options.credentials?.[snapshot.model.provider];
      return new CodexSdkHarness({
        ...options,
        provider: 'pi-sdk',
        apiKey: egress.apiKey ?? direct?.apiKey ?? options.apiKey ?? '',
        ...(egress.baseURL
          ? { piGatewayUrl: egress.baseURL }
          : direct?.baseUrl
            ? { baseUrl: direct.baseUrl }
            : {}),
        ...(snapshot.harness_state
          ? { checkpoint: snapshot.harness_state as CodexCheckpoint }
          : {}),
      });
    },
    { managedResources: true, customTools: true },
  );
}
