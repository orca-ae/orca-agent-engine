// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { ProviderRegistry } from '../../src/harness/provider.js';
import { registerPiSdkProvider } from '../../src/harness/pi-sdk/provider.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';

const { construct } = vi.hoisted(() => ({ construct: vi.fn() }));
vi.mock('../../src/harness/codex-sdk/index.js', () => ({
  CodexSdkHarness: class {
    constructor(options: unknown) {
      construct(options);
    }
  },
}));

describe.each(['anthropic', 'openai', 'deepseek', 'zai', 'google'] as const)(
  'runner Pi %s credential selection',
  (provider) => {
    it('selects the matching key and lets scoped gateway credentials take precedence', () => {
      const registry = new ProviderRegistry();
      registerPiSdkProvider(registry, {
        credentials: {
          [provider]: { apiKey: `${provider}-test`, baseUrl: `https://${provider}.invalid/v1` },
        },
      });
      const snapshot: RunnerSnapshot = {
        provider: 'pi-sdk',
        model: { provider, id: 'test-model' },
        system: '',
        allowed_tool_names: [],
        allowed_mcp_server_names: [],
        egress: { mode: 'direct' },
      };
      const context = { workspaceId: 'ws_test', sessionId: 'ses_test' };
      registry.build(snapshot, context);
      expect(construct).toHaveBeenLastCalledWith(
        expect.objectContaining({
          apiKey: `${provider}-test`,
          baseUrl: `https://${provider}.invalid/v1`,
        }),
      );
      registry.build(
        {
          ...snapshot,
          egress: {
            mode: 'gateway',
            gateway: { llm_jwt: 'scoped-test', llm_base_url: 'https://gateway.invalid/v1' },
          },
        },
        context,
      );
      expect(construct).toHaveBeenLastCalledWith(
        expect.objectContaining({
          apiKey: 'scoped-test',
          piGatewayUrl: 'https://gateway.invalid/v1',
        }),
      );
    });
  },
);

it('does not substitute an OpenAI credential for missing DeepSeek credentials', () => {
  const registry = new ProviderRegistry();
  registerPiSdkProvider(registry, { credentials: { openai: { apiKey: 'wrong-key' } } });
  registry.build(
    {
      provider: 'pi-sdk',
      model: { provider: 'deepseek', id: 'deepseek-flash' },
      system: '',
      allowed_tool_names: [],
      allowed_mcp_server_names: [],
      egress: {},
    },
    { workspaceId: 'ws_test', sessionId: 'ses_test' },
  );
  expect(construct).toHaveBeenLastCalledWith(expect.objectContaining({ apiKey: '' }));
});
