// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { validatedProviderFetch } from '@orca/sdk-harness';
import { installManagedFetch, MessagesUsage } from '../../src/transport.js';

const newline = String.fromCharCode(10);
const sse = (...events: unknown[]) =>
  events.map((event) => 'data: ' + JSON.stringify(event) + newline + newline).join('');
const messages = (output = 2) => [
  {
    type: 'message_start',
    message: {
      usage: {
        input_tokens: 4,
        output_tokens: 0,
        cache_read_input_tokens: 3,
        cache_creation_input_tokens: 2,
        cache_creation: { ephemeral_5m_input_tokens: 1, ephemeral_1h_input_tokens: 1 },
      },
    },
  },
  { type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: output } },
  { type: 'message_stop' },
];
afterEach(() => vi.unstubAllGlobals());

it.each(['native Messages', 'shared provider'])(
  '%s preserves omitted cache breakdowns and validates complete cumulative replacements',
  async (parser) => {
    async function observe(delta: Record<string, unknown>) {
      const events: Record<string, unknown>[] = messages();
      events[1] = {
        type: 'message_delta',
        delta: { stop_reason: 'end_turn' },
        usage: { output_tokens: 2, ...delta },
      };
      if (parser === 'native Messages') {
        const usage = new MessagesUsage();
        for (const event of events) usage.event(event);
        return usage.finish();
      }
      const completed = vi.fn();
      const fetch = validatedProviderFetch(
        'anthropic-messages',
        async () => new Response(sse(...events)),
        completed,
      );
      await (await fetch('https://upstream.example/v1/messages')).text();
      return completed.mock.calls[0]?.[0];
    }
    expect(await observe({})).toMatchObject({
      input_tokens: 7,
      cached_input_tokens: 3,
      cache_write_input_tokens: 2,
      cache_write_input_tokens_1h: 1,
    });
    expect(
      await observe({
        cache_creation_input_tokens: 5,
        cache_creation: { ephemeral_5m_input_tokens: 3, ephemeral_1h_input_tokens: 2 },
      }),
    ).toMatchObject({ cache_write_input_tokens: 5, cache_write_input_tokens_1h: 2 });
    await expect(
      observe({
        cache_creation_input_tokens: 5,
        cache_creation: { ephemeral_5m_input_tokens: 3, ephemeral_1h_input_tokens: 1 },
      }),
    ).rejects.toThrow('cache usage');
  },
);

it.each([
  [
    'anthropic-messages',
    '/v1/messages',
    messages(),
    {
      input_tokens: 7,
      output_tokens: 2,
      cached_input_tokens: 3,
      cache_write_input_tokens: 2,
      cache_write_input_tokens_1h: 1,
    },
  ],
  [
    'openai-completions',
    '/chat/completions',
    [
      {
        choices: [{ finish_reason: 'stop' }],
        usage: { prompt_tokens: 7, completion_tokens: 2, prompt_cache_hit_tokens: 3 },
      },
    ],
    { input_tokens: 7, output_tokens: 2, cached_input_tokens: 3 },
  ],
  [
    'openai-responses',
    '/responses',
    [
      {
        type: 'response.completed',
        response: {
          usage: { input_tokens: 7, output_tokens: 2, input_tokens_details: { cached_tokens: 3 } },
        },
      },
    ],
    { input_tokens: 7, output_tokens: 2, cached_input_tokens: 3 },
  ],
  [
    'google-generative-ai',
    '/models/test-model:streamGenerateContent',
    [
      {
        candidates: [{ finishReason: 'STOP' }],
        usageMetadata: {
          promptTokenCount: 7,
          candidatesTokenCount: 1,
          thoughtsTokenCount: 1,
          cachedContentTokenCount: 3,
        },
      },
    ],
    { input_tokens: 7, output_tokens: 2, cached_input_tokens: 3 },
  ],
] as const)(
  'preserves %s bytes and attributes raw terminal usage without leaking upstream auth',
  async (api, path, events, usage) => {
    const wire = sse(...events);
    const send = vi.fn(
      async (_input: unknown, _options: unknown) =>
        new Response(wire, { headers: { 'content-type': 'text/event-stream' } }),
    );
    vi.stubGlobal('fetch', send);
    const completed = vi.fn();
    const started = vi.fn();
    let token = 'scoped-first';
    const release = installManagedFetch(
      () => ({
        origin: 'https://gateway.example',
        token,
        sessionId: 'ses_test',
        provider: 'catalog-provider',
        api,
        baseUrl: 'https://upstream.example',
        model: 'test-model',
      }),
      completed,
      started,
    );
    try {
      for (const next of ['scoped-first', 'scoped-refreshed']) {
        token = next;
        const response = await fetch(
          'https://upstream.example' + path + '?key=upstream-secret&alt=sse',
          {
            method: 'POST',
            body: JSON.stringify({ model: 'test-model' }),
            headers: {
              'x-deepseek-harness-session-id': 'native-child',
              'x-api-key': 'upstream-secret',
              'x-goog-api-key': 'upstream-secret',
              authorization: 'Bearer upstream-secret',
              cookie: 'private-cookie',
            },
          },
        );
        expect(await response.text()).toBe(wire);
        const [target, options] = send.mock.calls.at(-1)! as [URL, RequestInit];
        expect(target.origin).toBe('https://gateway.example');
        expect(target.pathname).toBe('/v1/proxy/catalog-provider' + path);
        expect(target.searchParams.has('key')).toBe(false);
        const headers = new Headers(options.headers);
        expect(headers.get('authorization')).toBe('Bearer ' + next);
        expect(headers.get('x-orca-session-id')).toBe('ses_test');
        for (const name of [
          'x-api-key',
          'x-goog-api-key',
          'cookie',
          'x-deepseek-harness-session-id',
        ])
          expect(headers.has(name)).toBe(false);
        expect(options.redirect).toBe('error');
      }
      expect(started).toHaveBeenCalledTimes(2);
      expect(completed).toHaveBeenCalledTimes(2);
      expect(completed).toHaveBeenLastCalledWith(expect.objectContaining(usage), 'native-child');
    } finally {
      release();
    }
  },
);

it('rejects direct requests and cross-model calls before network I/O', async () => {
  const send = vi.fn();
  vi.stubGlobal('fetch', send);
  const release = installManagedFetch(
    () => ({
      origin: 'https://gateway.example',
      token: 'scoped',
      sessionId: 'ses_test',
      provider: 'anthropic',
      api: 'anthropic-messages',
      baseUrl: 'https://upstream.example',
      model: 'pinned',
    }),
    vi.fn(),
  );
  try {
    await expect(fetch('https://other.example/v1/messages', { method: 'POST' })).rejects.toThrow(
      'unmanaged',
    );
    await expect(
      fetch('https://upstream.example/v1/messages', {
        method: 'POST',
        body: JSON.stringify({ model: 'other' }),
      }),
    ).rejects.toThrow('model');
    for (const body of ['{invalid', 'null'])
      await expect(
        fetch('https://upstream.example/v1/messages', { method: 'POST', body }),
      ).rejects.toThrow('model is invalid');
    await expect(
      fetch('https://upstream.example/v1/messages', {
        method: 'POST',
        body: JSON.stringify({ model: 'pinned' }),
      }),
    ).rejects.toThrow('identity');
    expect(send).not.toHaveBeenCalled();
  } finally {
    release();
  }
});

it('reports malformed Google model path encoding without leaking URI details', async () => {
  const send = vi.fn();
  vi.stubGlobal('fetch', send);
  const release = installManagedFetch(
    () => ({
      origin: 'https://gateway.example',
      token: 'scoped',
      sessionId: 'ses_test',
      provider: 'google',
      api: 'google-generative-ai',
      baseUrl: 'https://upstream.example',
      model: 'pinned',
    }),
    vi.fn(),
  );
  try {
    await expect(
      fetch('https://upstream.example/models/%ZZ:streamGenerateContent', {
        method: 'POST',
        body: '{}',
      }),
    ).rejects.toThrow('model is invalid');
    expect(send).not.toHaveBeenCalled();
  } finally {
    release();
  }
});

it.each([[messages(-1)], [messages().slice(0, 2)]])(
  'never reports missing or invalid terminal usage as zero',
  async (events) => {
    vi.stubGlobal(
      'fetch',
      vi.fn(
        async () =>
          new Response(sse(...events), { headers: { 'content-type': 'text/event-stream' } }),
      ),
    );
    const completed = vi.fn();
    const release = installManagedFetch(
      () => ({
        origin: 'https://gateway.example',
        token: 'scoped',
        sessionId: 'ses_test',
        provider: 'anthropic',
        api: 'anthropic-messages',
        baseUrl: 'https://upstream.example',
        model: 'pinned',
      }),
      completed,
    );
    try {
      const response = await fetch('https://upstream.example/v1/messages', {
        method: 'POST',
        body: JSON.stringify({ model: 'pinned' }),
        headers: { 'x-deepseek-harness-session-id': 'native-child' },
      });
      await expect(response.text()).rejects.toThrow();
      expect(completed).not.toHaveBeenCalled();
    } finally {
      release();
    }
  },
);
