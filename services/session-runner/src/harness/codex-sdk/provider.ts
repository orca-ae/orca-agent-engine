// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { ProviderRegistry } from '../provider.js';
import { readGatewayLlmEgress } from '../claude/provider.js';
import { CodexSdkHarness, type CodexSdkOptions } from './index.js';
import type { CodexCheckpoint } from '@orca/codex-harness';

export function registerCodexSdkProvider(
  registry: ProviderRegistry,
  options: Partial<CodexSdkOptions> = {},
): void {
  registry.register(
    'codex-sdk',
    (snapshot) => {
      const egress = readGatewayLlmEgress(snapshot);
      return new CodexSdkHarness({
        ...options,
        apiKey: egress.apiKey ?? options.apiKey ?? '',
        ...(egress.baseURL ? { baseUrl: egress.baseURL } : {}),
        ...(snapshot.harness_state
          ? { checkpoint: snapshot.harness_state as CodexCheckpoint }
          : {}),
      });
    },
    { managedResources: true, customTools: true },
  );
}
