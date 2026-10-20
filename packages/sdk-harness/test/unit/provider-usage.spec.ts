// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it, vi } from 'vitest';
import { validatedProviderFetch } from '../../src/provider-usage.js';

const nl = String.fromCharCode(10);
const wire = (events: unknown[]) =>
  events.map((event) => 'data: ' + JSON.stringify(event) + nl + nl).join('');
const cases = [
  [
    'openai-completions',
    [
      {
        choices: [{ finish_reason: 'stop' }],
        usage: {
          prompt_tokens: 10,
          completion_tokens: 1,
          prompt_tokens_details: { cached_tokens: 3, cache_write_tokens: 2 },
        },
      },
    ],
  ],
  [
    'openai-responses',
    [
      {
        type: 'response.completed',
        response: {
          usage: {
            input_tokens: 10,
            output_tokens: 1,
            input_tokens_details: { cached_tokens: 3, cache_write_tokens: 2 },
          },
        },
      },
    ],
  ],
  [
    'anthropic-messages',
    [
      {
        type: 'message_start',
        message: {
          usage: {
            input_tokens: 5,
            output_tokens: 0,
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 2,
          },
        },
      },
      { type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 1 } },
      { type: 'message_stop' },
    ],
  ],
] as const;

it.each(cases)('keeps %s cache writes out of the plain input bucket', async (api, events) => {
  const completed = vi.fn();
  const response = await validatedProviderFetch(
    api,
    async () => new Response(wire([...events])),
    completed,
  )('https://fixture.example');
  await response.text();
  expect(completed).toHaveBeenCalledTimes(1);
  expect(completed).toHaveBeenCalledWith({
    input_tokens: 8,
    output_tokens: 1,
    cached_input_tokens: 3,
    cache_write_input_tokens: 2,
  });
  const usage = completed.mock.calls[0]![0];
  const plain = usage.input_tokens - usage.cached_input_tokens;
  expect(plain).toBe(5);
  expect(plain + usage.cached_input_tokens + usage.cache_write_input_tokens).toBe(10);
});

it('reports known terminal usage once even when trailing stream bytes are malformed', async () => {
  const completed = vi.fn();
  const response = await validatedProviderFetch(
    'openai-responses',
    async () => new Response(wire([...cases[1][1]]) + 'data: invalid-json' + nl + nl),
    completed,
  )('https://fixture.example');
  await expect(response.text()).rejects.toThrow();
  expect(completed).toHaveBeenCalledTimes(1);
  expect(completed.mock.calls[0]![0].input_tokens).toBe(8);
});

it('preserves Pi validation-only behavior while managed Messages rejects counter rollback', async () => {
  const events = [
    ...cases[2][1].slice(0, 2),
    { type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 0 } },
    { type: 'message_stop' },
  ];
  const send = async () => new Response(wire(events));
  await expect(
    (await validatedProviderFetch('anthropic-messages', send)('https://fixture.example')).text(),
  ).resolves.toEqual(expect.any(String));
  const completed = vi.fn();
  await expect(
    (
      await validatedProviderFetch('anthropic-messages', send, completed)('https://fixture.example')
    ).text(),
  ).rejects.toThrow('Non-cumulative');
  expect(completed).not.toHaveBeenCalled();
});
