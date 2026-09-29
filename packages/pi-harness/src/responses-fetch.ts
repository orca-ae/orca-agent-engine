// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { assertSdkTerminalUsage } from '@orca/sdk-harness';

/** Pi normalizes missing/negative provider counters to zero. Check the wire first. */
export function validatedProviderFetch(api: string, send: typeof fetch = fetch): typeof fetch {
  if (
    ![
      'openai-responses',
      'openai-completions',
      'anthropic-messages',
      'google-generative-ai',
    ].includes(api)
  )
    throw new Error(`Unsupported Pi usage protocol: ${api}`);
  return async (input, init) => {
    const response = await send(input, init);
    if (!response.ok || !response.body) return response;
    const decoder = new TextDecoder();
    let buffer = '';
    let data: string[] = [];
    let eventBytes = 0;
    let terminal = false;
    let messageUsage: Record<string, unknown> | undefined;
    let messageDelta = false;
    let chatFinished = false;
    let chatUsage = false;
    function dispatch(): void {
      if (!data.length) return;
      const payload = data.join('\n');
      data = [];
      eventBytes = 0;
      if (payload === '[DONE]') return;
      const event = JSON.parse(payload);
      if (api === 'google-generative-ai') {
        const u = event.usageMetadata;
        if (u) {
          const cached = u.cachedContentTokenCount ?? 0;
          const thoughts = u.thoughtsTokenCount ?? 0;
          assertSdkTerminalUsage({
            input_tokens: u.promptTokenCount,
            output_tokens: u.candidatesTokenCount,
            cached_input_tokens: cached,
          });
          if (
            !Number.isSafeInteger(thoughts) ||
            thoughts < 0 ||
            !Number.isSafeInteger(u.candidatesTokenCount + thoughts) ||
            cached > u.promptTokenCount
          )
            throw new Error('Invalid Pi Gemini usage');
          chatUsage = true;
        }
        chatFinished ||=
          event.candidates?.some((candidate: { finishReason?: unknown }) =>
            Boolean(candidate.finishReason),
          ) ?? false;
        terminal = chatFinished && chatUsage;
        return;
      }
      if (api === 'anthropic-messages') {
        if (event.type === 'message_start') messageUsage = event.message?.usage;
        if (event.type === 'message_delta' && event.usage) {
          if (!messageUsage) throw new Error('Pi Anthropic initial usage unavailable');
          messageUsage = { ...messageUsage, ...event.usage };
          messageDelta =
            Boolean(event.delta?.stop_reason) && Object.hasOwn(event.usage, 'output_tokens');
        }
        if (event.type === 'message_start' || event.type === 'message_delta') {
          const u = messageUsage;
          const cached = u?.cache_read_input_tokens === undefined ? 0 : u.cache_read_input_tokens;
          const written =
            u?.cache_creation_input_tokens === undefined ? 0 : u.cache_creation_input_tokens;
          assertSdkTerminalUsage({
            input_tokens: u?.input_tokens,
            output_tokens: u?.output_tokens,
            cached_input_tokens: 0,
            cache_write_input_tokens: written,
          });
          if (
            typeof cached !== 'number' ||
            !Number.isSafeInteger(cached) ||
            cached < 0 ||
            !Number.isSafeInteger((u!.input_tokens as number) + cached)
          )
            throw new Error('Invalid Pi Anthropic cache usage');
          const cache = u?.cache_creation as Record<string, unknown> | undefined;
          if (
            cache?.ephemeral_5m_input_tokens !== undefined &&
            (!Number.isSafeInteger(cache.ephemeral_5m_input_tokens) ||
              (cache.ephemeral_5m_input_tokens as number) < 0)
          )
            throw new Error('Invalid Pi Anthropic cache usage');
          if (
            cache?.ephemeral_5m_input_tokens !== undefined &&
            cache?.ephemeral_1h_input_tokens !== undefined &&
            (cache.ephemeral_5m_input_tokens as number) +
              (cache.ephemeral_1h_input_tokens as number) !==
              written
          )
            throw new Error('Invalid Pi Anthropic cache usage');
          if (cache?.ephemeral_1h_input_tokens !== undefined) {
            assertSdkTerminalUsage({
              input_tokens: u?.input_tokens,
              output_tokens: u?.output_tokens,
              cached_input_tokens: 0,
              cache_write_input_tokens: written,
              cache_write_input_tokens_1h: cache.ephemeral_1h_input_tokens,
            });
          }
        }
        if (event.type === 'message_stop' && messageUsage && messageDelta) terminal = true;
        return;
      }
      if (api === 'openai-completions') {
        chatFinished ||=
          event.choices?.some((choice: { finish_reason?: unknown }) =>
            Boolean(choice.finish_reason),
          ) ?? false;
        if (event.usage) {
          const u = event.usage;
          const rawCached =
            u.prompt_tokens_details?.cached_tokens === undefined
              ? u.prompt_cache_hit_tokens
              : u.prompt_tokens_details.cached_tokens;
          const cached = rawCached === undefined ? 0 : rawCached;
          const written =
            u.prompt_tokens_details?.cache_write_tokens === undefined
              ? 0
              : u.prompt_tokens_details.cache_write_tokens;
          assertSdkTerminalUsage({
            input_tokens: u.prompt_tokens,
            output_tokens: u.completion_tokens,
            cached_input_tokens: cached,
            cache_write_input_tokens: written,
          });
          if (!Number.isSafeInteger(cached + written) || cached + written > u.prompt_tokens)
            throw new Error('Invalid Pi Chat cache usage');
          chatUsage = true;
        }
        terminal = chatFinished && chatUsage;
        return;
      }
      if (event.type !== 'response.completed' && event.type !== 'response.incomplete') return;
      const usage = event.response?.usage;
      const details = usage?.input_tokens_details;
      const cached = details?.cached_tokens === undefined ? 0 : details.cached_tokens;
      const written = details?.cache_write_tokens === undefined ? 0 : details.cache_write_tokens;
      assertSdkTerminalUsage({
        input_tokens: usage?.input_tokens,
        output_tokens: usage?.output_tokens,
        cached_input_tokens: cached,
        cache_write_input_tokens: written,
      });
      if (!Number.isSafeInteger(cached + written) || cached + written > usage.input_tokens)
        throw new Error('Invalid Pi Responses cache usage');
      terminal = true;
    }
    function consume(text: string): void {
      buffer += text;
      let end: number;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end).replace(/\r$/, '');
        buffer = buffer.slice(end + 1);
        if (line === '') dispatch();
        else if (line.startsWith('data:')) {
          data.push(line.slice(5).replace(/^ /, ''));
          eventBytes += line.length;
        }
        if (eventBytes > 32 * 1024 * 1024) throw new Error('Pi Responses event exceeds size limit');
      }
      if (buffer.length > 32 * 1024 * 1024) throw new Error('Pi Responses line exceeds size limit');
    }
    const body = response.body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, controller) {
          consume(decoder.decode(chunk, { stream: true }));
          controller.enqueue(chunk);
        },
        flush() {
          consume(decoder.decode() + '\n\n');
          if (!terminal) throw new Error('Pi Responses terminal usage unavailable');
        },
      }),
    );
    return new Response(body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  };
}
export const validatedResponsesFetch = validatedProviderFetch('openai-responses');
