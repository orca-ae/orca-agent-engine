// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { generateSummaryWithUsage } from '@earendil-works/pi-coding-agent';
import { retryAssistantCall, normalizeContext, type Model, type Api } from '@earendil-works/pi-ai';
import { nativePiReportingStream, type NativePiReport } from '../../src/native-reporting.js';

const model: Model<Api> = {
  id: 'requested-model',
  name: 'fixture',
  provider: 'anthropic',
  api: 'anthropic-messages',
  baseUrl: 'http://fixture.invalid',
  reasoning: false,
  input: ['text'],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 200000,
  maxTokens: 100,
};
const context = normalizeContext({
  messages: [
    { role: 'user' as const, content: [{ type: 'text' as const, text: 'fixture' }], timestamp: 1 },
  ],
});
const sse = (value: { type: string; [key: string]: unknown }) =>
  'event: ' + value.type + '\ndata: ' + JSON.stringify(value) + '\n\n';
function response(
  input: number,
  output: number,
  options: { trailingError?: boolean; truncated?: boolean; cache?: number } = {},
) {
  const start = sse({
    type: 'message_start',
    message: {
      id: 'msg_fixture',
      type: 'message',
      role: 'assistant',
      model: 'actual-model',
      content: [],
      stop_reason: null,
      stop_sequence: null,
      usage: {
        input_tokens: input,
        output_tokens: 0,
        cache_creation_input_tokens: options.cache ?? 0,
      },
    },
  });
  const end = options.truncated
    ? ''
    : sse({
        type: 'message_delta',
        delta: { stop_reason: 'end_turn', stop_sequence: null },
        usage: { output_tokens: output },
      }) + sse({ type: 'message_stop' });
  const error = options.trailingError
    ? sse({ type: 'error', error: { type: 'overloaded_error', message: 'private upstream text' } })
    : '';
  return new Response(start + end + error, { headers: { 'content-type': 'text/event-stream' } });
}

it('retains distinct real provider attempts before the published retry helper discards the first', async () => {
  const reports: NativePiReport[] = [];
  let requests = 0;
  const stream = nativePiReportingStream(
    (report) => reports.push(report),
    async () => (++requests === 1 ? response(13, 7, { trailingError: true }) : response(17, 11)),
  );
  const result = await retryAssistantCall(
    () => stream(model, context, { apiKey: 'fixture', maxRetries: 5 }).result(),
    { enabled: true, maxRetries: 1, baseDelayMs: 1, maxDelayMs: 1 },
    undefined,
  );
  expect(result.stopReason, JSON.stringify({ result, reports, requests })).toBe('stop');
  expect(requests).toBe(2);
  expect(new Set(reports.map(({ id }) => id)).size).toBe(2);
  expect(
    reports.map(({ complete, model, usage }) => ({
      complete,
      model,
      input: usage?.input_tokens,
      output: usage?.output_tokens,
    })),
  ).toEqual([
    { complete: true, model: 'actual-model', input: 13, output: 7 },
    { complete: true, model: 'actual-model', input: 17, output: 11 },
  ]);
});

it('marks missing final usage incomplete without inventing zero usage', async () => {
  const reports: NativePiReport[] = [];
  const result = await nativePiReportingStream(
    (report) => reports.push(report),
    async () => response(13, 7, { truncated: true }),
  )(model, context, { apiKey: 'fixture' }).result();
  expect(result.stopReason).toBe('error');
  expect(reports).toHaveLength(1);
  expect(reports[0]).toMatchObject({ complete: false, model: 'actual-model' });
  expect(reports[0]).not.toHaveProperty('usage');
});

it('captures internal compaction attempts through the pinned Pi summarization helper', async () => {
  const reports: NativePiReport[] = [];
  let requests = 0;
  const stream = nativePiReportingStream(
    (report) => reports.push(report),
    async () => {
      const attempt = ++requests;
      const raw = response(attempt === 1 ? 13 : 17, attempt === 1 ? 7 : 11, {
        trailingError: attempt === 1,
      });
      const text =
        sse({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }) +
        sse({
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'text_delta', text: 'fixture summary' },
        }) +
        sse({ type: 'content_block_stop', index: 0 });
      return new Response(
        (await raw.text()).replace('event: message_delta', text + 'event: message_delta'),
        { headers: raw.headers },
      );
    },
  );
  const result = await generateSummaryWithUsage(
    [],
    model,
    1000,
    'fixture',
    undefined,
    undefined,
    undefined,
    undefined,
    'off',
    stream,
    undefined,
    { enabled: true, maxRetries: 1, baseDelayMs: 1, maxDelayMs: 1 },
  );
  expect(result.text).toBe('fixture summary');
  expect(requests).toBe(2);
  expect(
    reports.map((report) => [
      report.complete,
      report.usage?.input_tokens,
      report.usage?.output_tokens,
    ]),
  ).toEqual([
    [true, 13, 7],
    [true, 17, 11],
  ]);
  expect(new Set(reports.map((report) => report.id)).size).toBe(2);
});

it('refuses unknown cache-write TTL rather than assigning it to a price bucket', async () => {
  const reports: NativePiReport[] = [];
  const result = await nativePiReportingStream(
    (report) => reports.push(report),
    async () => response(13, 7, { cache: 3 }),
  )(model, context, { apiKey: 'fixture' }).result();
  expect(result.stopReason).toBe('error');
  expect(reports[0]).toMatchObject({ complete: false });
  expect(reports[0]).not.toHaveProperty('usage');
});
