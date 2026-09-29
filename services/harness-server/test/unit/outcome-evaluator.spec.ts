// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import {
  parseVerdict,
  createMessagesApiOutcomeEvaluator,
} from '../../src/harness/outcome/evaluator.js';

describe('parseVerdict', () => {
  it('parses a clean JSON verdict', () => {
    expect(parseVerdict('{"achieved": true, "reasoning": "done"}')).toEqual({
      achieved: true,
      reasoning: 'done',
    });
  });

  it('extracts JSON embedded in prose / code fences', () => {
    const text = 'Here is my verdict:\n```json\n{"achieved": false, "reasoning": "not yet"}\n```';
    expect(parseVerdict(text)).toEqual({ achieved: false, reasoning: 'not yet' });
  });

  it('treats a non-true achieved value as false', () => {
    expect(parseVerdict('{"achieved": "yes"}')).toEqual({ achieved: false, reasoning: '' });
  });

  it('falls back to a pessimistic verdict on unparseable output', () => {
    const v = parseVerdict('no json here');
    expect(v.achieved).toBe(false);
    expect(v.reasoning).toContain('no json here');
  });
});

describe('createMessagesApiOutcomeEvaluator', () => {
  const criterion = {
    id: 'outcome_x',
    description: 'do x',
    rubric: 'x must be done',
    maxIterations: 3,
    iteration: 0,
  };
  const transcript = [
    { role: 'user' as const, text: 'please do x' },
    { role: 'agent' as const, text: 'x done' },
  ];

  it('calls the Messages API and returns the parsed verdict', async () => {
    const fetchImpl = vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        content: [{ type: 'text', text: '{"achieved": true, "reasoning": "ok"}' }],
      }),
    })) as unknown as typeof fetch;
    const evaluate = createMessagesApiOutcomeEvaluator({
      apiKey: 'sk-test',
      baseURL: 'https://example.test/',
      model: 'm',
      fetchImpl,
    });
    const verdict = await evaluate({ criterion, transcript });
    expect(verdict).toEqual({ achieved: true, reasoning: 'ok' });
    const [url, init] = (fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]!;
    // Trailing slash on baseURL is normalized.
    expect(url).toBe('https://example.test/v1/messages');
    expect((init as { headers: Record<string, string> }).headers['x-api-key']).toBe('sk-test');
  });

  it('authenticates a gateway-selected outcome request with the Session JWT', async () => {
    const fetchImpl = vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ content: [{ type: 'text', text: '{"achieved": true}' }] }),
    })) as unknown as typeof fetch;
    const getToken = vi.fn().mockResolvedValue('gateway-jwt');
    const evaluate = createMessagesApiOutcomeEvaluator({
      apiKey: '',
      baseURL: 'http://gateway.test/v1/llm',
      model: 'claude-test',
      gatewayAuth: { sessionId: 'ses_test', getToken },
      fetchImpl,
    });

    await evaluate({ criterion, transcript });
    const [url, init] = (fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]!;
    expect(url).toBe('http://gateway.test/v1/llm/v1/messages');
    expect((init as { headers: Record<string, string> }).headers).toMatchObject({
      authorization: 'Bearer gateway-jwt',
      'X-Orca-Session-Id': 'ses_test',
    });
    expect((init as { headers: Record<string, string> }).headers).not.toHaveProperty('x-api-key');
    expect(getToken).toHaveBeenCalledOnce();
  });

  it('carries fast speed and effort to the judge and verifies provider speed', async () => {
    const fetchImpl = vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        content: [{ type: 'text', text: '{"achieved": true, "reasoning": "ok"}' }],
        usage: { speed: 'fast' },
      }),
    })) as unknown as typeof fetch;
    const evaluate = createMessagesApiOutcomeEvaluator({
      apiKey: 'sk-test',
      model: 'claude-opus-5',
      speed: 'fast',
      effort: 'high',
      fetchImpl,
    });

    await expect(evaluate({ criterion, transcript })).resolves.toEqual({
      achieved: true,
      reasoning: 'ok',
    });
    const [, init] = (fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]!;
    const request = init as { headers: Record<string, string>; body: string };
    expect(request.headers['anthropic-beta']).toContain('fast-mode-2026-02-01');
    expect(JSON.parse(request.body)).toMatchObject({
      model: 'claude-opus-5',
      speed: 'fast',
      output_config: { effort: 'high' },
    });
  });

  it('rejects an unverified standard-speed judge response for a fast session', async () => {
    const fetchImpl = vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        content: [{ type: 'text', text: '{"achieved": true, "reasoning": "ok"}' }],
        usage: { speed: 'standard' },
      }),
    })) as unknown as typeof fetch;
    const evaluate = createMessagesApiOutcomeEvaluator({
      apiKey: 'sk-test',
      model: 'claude-opus-5',
      speed: 'fast',
      fetchImpl,
    });

    const verdict = await evaluate({ criterion, transcript });
    expect(verdict.achieved).toBe(false);
    expect(verdict.reasoning).toContain('usage.speed="standard"');
  });

  it('returns a pessimistic verdict on a non-2xx response (never throws)', async () => {
    const fetchImpl = vi.fn(async () => ({
      ok: false,
      status: 503,
      json: async () => ({}),
    })) as unknown as typeof fetch;
    const evaluate = createMessagesApiOutcomeEvaluator({ apiKey: 'k', model: 'm', fetchImpl });
    const verdict = await evaluate({ criterion, transcript });
    expect(verdict.achieved).toBe(false);
    expect(verdict.reasoning).toContain('503');
  });

  it('returns a pessimistic verdict on a network error (never throws)', async () => {
    const fetchImpl = vi.fn(async () => {
      throw new Error('ECONNRESET');
    }) as unknown as typeof fetch;
    const evaluate = createMessagesApiOutcomeEvaluator({ apiKey: 'k', model: 'm', fetchImpl });
    const verdict = await evaluate({ criterion, transcript });
    expect(verdict.achieved).toBe(false);
    expect(verdict.reasoning).toContain('ECONNRESET');
  });
});
