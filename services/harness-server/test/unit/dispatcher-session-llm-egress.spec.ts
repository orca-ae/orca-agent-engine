// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { TranscriptStore } from '@orca/transcript-store';
import type { RegistryClient, SessionRecord } from '../../src/clients/registry.js';
import type { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import type { ClaudeHarnessOptions } from '../../src/harness/claude/index.js';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import type { CodexSdkHarnessOptions } from '../../src/harness/codex-sdk/index.js';

describe('Dispatcher Session LLM egress selection', () => {
  const build = (gatewayLlmUrl: string, llmEgressDefault: 'direct' | 'gateway' = 'direct') => {
    const mintLlmGatewayJwt = vi.fn().mockResolvedValue({
      token: 'session-jwt',
      expiresAt: Math.floor(Date.now() / 1000) + 300,
    });
    const dispatcher = new Dispatcher({
      groupId: 'test-session-llm-egress',
      store: {} as TranscriptStore,
      anthropicApiKey: 'direct-provider-key',
      anthropicBaseURL: 'https://direct.example',
      modelDefault: 'claude-test',
      gatewayLlmUrl,
      llmEgressDefault,
      registry: { mintLlmGatewayJwt } as unknown as RegistryClient,
    });
    const optionsFor = (metadata?: Record<string, unknown>): ClaudeHarnessOptions => {
      const harness = dispatcher['buildClaudeHarness'](
        'ws_test',
        'ses_test',
        {} as ClaudeAgentSdkAdapter,
        { metadata } as SessionRecord,
      );
      return (harness as unknown as { opts: ClaudeHarnessOptions }).opts;
    };
    return { mintLlmGatewayJwt, optionsFor };
  };

  it('keeps omitted and explicit direct Sessions on the provider endpoint', () => {
    const { optionsFor, mintLlmGatewayJwt } = build('http://gateway.test/v1/llm');
    for (const metadata of [undefined, { orca_llm_egress: 'direct' }]) {
      const options = optionsFor(metadata);
      expect(options.apiKey).toBe('direct-provider-key');
      expect(options.baseURL).toBe('https://direct.example');
      expect(options.llmGatewayJwtProvider).toBeUndefined();
    }
    expect(mintLlmGatewayJwt).not.toHaveBeenCalled();
  });

  it('uses the deployment default when metadata is omitted and lets metadata override it', async () => {
    const { optionsFor } = build('http://gateway.test/v1/llm', 'gateway');
    const inherited = optionsFor();
    expect(inherited.apiKey).toBe('');
    expect(inherited.baseURL).toBe('http://gateway.test/v1/llm');
    await expect(inherited.llmGatewayJwtProvider!.getValidToken()).resolves.toMatchObject({
      token: 'session-jwt',
    });
    inherited.llmGatewayJwtProvider!.close();

    const direct = optionsFor({ orca_llm_egress: 'direct' });
    expect(direct.apiKey).toBe('direct-provider-key');
    expect(direct.baseURL).toBe('https://direct.example');
    expect(direct.llmGatewayJwtProvider).toBeUndefined();
  });

  it.each([
    ['http://gateway.test/v1', 'http://gateway.test'],
    ['http://gateway.test/v1/llm/', 'http://gateway.test/v1/llm'],
  ])('routes a selected Session through %s', async (gatewayLlmUrl, expectedBaseURL) => {
    const { optionsFor, mintLlmGatewayJwt } = build(gatewayLlmUrl);
    const options = optionsFor({ orca_llm_egress: 'gateway' });
    expect(options.apiKey).toBe('');
    expect(options.baseURL).toBe(expectedBaseURL);
    expect(options.llmGatewayJwtProvider).toBeDefined();
    await expect(options.llmGatewayJwtProvider!.getValidToken()).resolves.toMatchObject({
      token: 'session-jwt',
    });
    expect(mintLlmGatewayJwt).toHaveBeenCalledWith('ws_test', 'ses_test', expect.any(AbortSignal));
    options.llmGatewayJwtProvider!.close();
  });
});

describe('Dispatcher SDK turn deadlines', () => {
  const build = (global?: number, overrides?: { dsh_sdk?: number; pi_sdk?: number }) => {
    const dispatcher = new Dispatcher({
      groupId: 'sdk-deadlines',
      store: {} as TranscriptStore,
      anthropicApiKey: '',
      modelDefault: 'unused',
      gatewayLlmUrl: 'http://gateway.test/v1',
      registry: {
        mintLlmGatewayJwt: vi.fn().mockResolvedValue({
          token: 'sdk-jwt',
          expiresAt: Math.floor(Date.now() / 1000) + 660,
        }),
      } as unknown as RegistryClient,
      ...(global === undefined ? {} : { harnessTurnTimeoutMs: global }),
      ...(overrides === undefined ? {} : { harnessTurnTimeoutOverridesMs: overrides }),
    });
    return (id: 'codex_sdk' | 'pi_sdk' | 'dsh_sdk', colocated = true) =>
      (
        dispatcher['buildCodexHarness'](
          'ws_test',
          'ses_test',
          { runtime_revision: 7 } as SessionRecord,
          colocated,
          id,
        ) as unknown as { opts: CodexSdkHarnessOptions }
      ).opts;
  };

  it('keeps the default and applies global and per-harness deadlines in both SDK modes', () => {
    for (const id of ['codex_sdk', 'pi_sdk', 'dsh_sdk'] as const) {
      for (const colocated of id === 'dsh_sdk' ? [true] : [false, true]) {
        const defaults = build()(id, colocated);
        expect(defaults.turnTimeoutMs).toBe(600_000);
        defaults.llmGatewayJwtProvider?.close();
        const configured = build(300_000, { dsh_sdk: 900_000 })(id, colocated);
        expect(configured.turnTimeoutMs).toBe(id === 'dsh_sdk' ? 900_000 : 300_000);
        configured.llmGatewayJwtProvider?.close();
      }
    }
  });

  it('requires the effective deadline plus 30 seconds of JWT validity before DSH execution', async () => {
    const shorter = build(300_000)('dsh_sdk');
    await expect(shorter.llmGatewayJwtProvider!.getValidToken()).resolves.toMatchObject({
      token: 'sdk-jwt',
    });
    shorter.llmGatewayJwtProvider!.close();
    const longer = build(300_000, { dsh_sdk: 900_000 })('dsh_sdk');
    await expect(longer.llmGatewayJwtProvider!.getValidToken()).rejects.toThrow(
      /930000ms.*AI_GATEWAY_LLM_JWT_TTL_SECS/,
    );
    longer.llmGatewayJwtProvider!.close();
  });
});

describe('Dispatcher Codex separate execution', () => {
  const create = (llmEgressDefault: 'direct' | 'gateway' = 'direct') => {
    const mintLlmGatewayJwt = vi
      .fn()
      .mockResolvedValue({ token: 'codex-jwt', expiresAt: Math.floor(Date.now() / 1000) + 660 });
    const harnessTurn = vi.fn(async () => ({
      runtimeRevision: 7,
      ownershipRevision: 1,
      state: null,
      receipt: null,
    }));
    const dispatcher = new Dispatcher({
      groupId: 'codex-separate',
      store: {} as TranscriptStore,
      anthropicApiKey: 'claude-key',
      openaiApiKey: 'openai-key',
      openaiBaseURL: 'https://openai.example/v1',
      modelDefault: 'claude-test',
      gatewayLlmUrl: 'http://gateway.test/v1/',
      llmEgressDefault,
      registry: { mintLlmGatewayJwt, harnessTurn } as unknown as RegistryClient,
    });
    const optionsFor = (metadata?: Record<string, unknown>) => {
      const harness = dispatcher['buildCodexHarness']('ws_test', 'ses_test', {
        runtime_revision: 7,
        ...(metadata ? { metadata } : {}),
      } as SessionRecord);
      return (harness as unknown as { opts: CodexSdkHarnessOptions }).opts;
    };
    return { optionsFor, mintLlmGatewayJwt, harnessTurn };
  };
  it('uses the pinned OpenAI credential for direct egress and preserves the Responses base URL for gateway egress', async () => {
    const h = create();
    expect(h.optionsFor()).toMatchObject({
      apiKey: 'openai-key',
      baseUrl: 'https://openai.example/v1',
    });
    const gateway = h.optionsFor({ orca_llm_egress: 'gateway' });
    expect(gateway.apiKey).toBe('');
    expect(gateway.baseUrl).toBe('http://gateway.test/v1');
    expect(await gateway.llmGatewayJwtProvider!.getValidToken()).toMatchObject({
      token: 'codex-jwt',
    });
    expect(h.mintLlmGatewayJwt).toHaveBeenCalledWith(
      'ws_test',
      'ses_test',
      expect.any(AbortSignal),
    );
    gateway.llmGatewayJwtProvider!.close();
  });
  it('applies the deployment default and preserves an explicit direct override', async () => {
    const h = create('gateway');
    const inherited = h.optionsFor();
    expect(inherited.apiKey).toBe('');
    expect(inherited.baseUrl).toBe('http://gateway.test/v1');
    expect(await inherited.llmGatewayJwtProvider!.getValidToken()).toMatchObject({
      token: 'codex-jwt',
    });
    inherited.llmGatewayJwtProvider!.close();

    expect(h.optionsFor({ orca_llm_egress: 'direct' })).toMatchObject({
      apiKey: 'openai-key',
      baseUrl: 'https://openai.example/v1',
    });
  });
  it('reuses one owner token and advances its fence only after an acknowledged claim', async () => {
    const h = create();
    const opts = h.optionsFor();
    await opts.turns.claim();
    await opts.turns.claim();
    const calls = h.harnessTurn.mock.calls as unknown as Array<
      [{ request: Record<string, unknown> }]
    >;
    expect(calls[0]![0].request).toMatchObject({
      runtimeRevision: 7,
      ownershipRevision: 0,
      action: { type: 'claim', expectedOwnershipRevision: 0 },
    });
    expect(calls[1]![0].request).toMatchObject({
      ownershipRevision: 1,
      ownerToken: calls[0]![0].request.ownerToken,
    });
  });
  it('rejects an invalid egress selection', () => {
    expect(() => create().optionsFor({ orca_llm_egress: 'typo' })).toThrow('orca_llm_egress');
  });
});

describe.each(['anthropic', 'openai', 'deepseek'] as const)(
  'Dispatcher Pi %s credentials',
  (provider) => {
    it('uses the selected direct key and replaces it with gateway auth in colocated mode', () => {
      const dispatcher = new Dispatcher({
        groupId: 'pi-provider',
        store: {} as TranscriptStore,
        anthropicApiKey: 'wrong-fallback',
        modelDefault: 'claude-test',
        gatewayLlmUrl: 'http://gateway.test/v1',
        registry: {} as RegistryClient,
        piProviderCredentials: {
          [provider]: { apiKey: `${provider}-key`, baseUrl: `https://${provider}.invalid/v1` },
        },
      });
      const session = { runtime_revision: 7 } as SessionRecord;
      const direct = dispatcher['buildCodexHarness'](
        'ws_test',
        'ses_test',
        session,
        false,
        'pi_sdk',
        provider,
      ) as unknown as { opts: CodexSdkHarnessOptions };
      expect(direct.opts).toMatchObject({
        apiKey: `${provider}-key`,
        baseUrl: `https://${provider}.invalid/v1`,
        harness: 'pi_sdk',
      });
      const colocated = dispatcher['buildCodexHarness'](
        'ws_test',
        'ses_test',
        session,
        true,
        'pi_sdk',
        provider,
      ) as unknown as { opts: CodexSdkHarnessOptions };
      expect(colocated.opts).toMatchObject({ apiKey: '', piGatewayUrl: 'http://gateway.test/v1' });
      expect(colocated.opts.baseUrl).toBeUndefined();
      expect(colocated.opts.llmGatewayJwtProvider).toBeDefined();
      colocated.opts.llmGatewayJwtProvider!.close();
    });
  },
);
