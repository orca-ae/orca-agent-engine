// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { streamSimple } from '@earendil-works/pi-ai/api/anthropic-messages';
import {
  createAssistantMessageEventStream,
  type Api,
  type Model,
  type SimpleStreamOptions,
  type TranscriptContext,
} from '@earendil-works/pi-ai';
import { validatedProviderFetch, type SdkTerminalUsage } from '@orca/sdk-harness';

export interface NativePiReport {
  id: string;
  provider: 'anthropic';
  model?: string;
  semantics: 'delta';
  complete: boolean;
  usage?: SdkTerminalUsage;
}
function isAnthropicModel(model: Model<Api>): model is Model<'anthropic-messages'> {
  return model.api === 'anthropic-messages';
}

/** One invocation is one request: retries remain in Pi's outer agent/summary loop. */
export function nativePiReportingStream(
  observe: (report: NativePiReport) => void,
  send: typeof fetch = fetch,
  started?: (id: string) => void,
) {
  return (model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions) => {
    if (!isAnthropicModel(model)) throw new Error('Unsupported native Pi usage API');
    const id = randomUUID();
    const result = createAssistantMessageEventStream();
    let known: SdkTerminalUsage | undefined;
    let actualModel: string | undefined;
    let retryReason: string | undefined;
    const wireFetch = validatedProviderFetch(
      'anthropic-messages',
      (input, init) => send(input, { ...init, redirect: 'error' }),
      (usage) => {
        // The shared validator preserves unknown cache TTL as absent. Never infer 5m.
        if (usage.cache_write_input_tokens && usage.cache_write_input_tokens_1h === undefined)
          throw new Error('Native Pi cache write TTL is unavailable');
        known = structuredClone(usage);
      },
      (value) => {
        if (!value || typeof value !== 'object') return;
        const event = value as {
          type?: unknown;
          message?: { model?: unknown };
          error?: { type?: unknown };
        };
        if (event.type === 'message_start' && typeof event.message?.model === 'string')
          actualModel = event.message.model;
        if (event.type === 'error' && event.error?.type === 'overloaded_error')
          retryReason = '503 overloaded';
        if (event.type === 'error' && event.error?.type === 'rate_limit_error')
          retryReason = '429 rate limited';
      },
    );
    void (async () => {
      let reported = false;
      try {
        started?.(id);
        const upstream = streamSimple(model, context, {
          ...options,
          maxRetries: 0,
          fetch: wireFetch,
        });
        for await (const event of upstream) {
          if (event.type === 'done' || event.type === 'error') {
            reported = true;
            observe({
              id,
              provider: 'anthropic',
              ...(actualModel ? { model: actualModel } : {}),
              semantics: 'delta',
              complete: known !== undefined && actualModel !== undefined,
              ...(known ? { usage: known } : {}),
            });
          }
          result.push(
            event.type === 'error' && retryReason
              ? { ...event, error: { ...event.error, errorMessage: retryReason } }
              : event,
          );
        }
      } catch (error) {
        try {
          if (!reported)
            observe({ id, provider: 'anthropic', semantics: 'delta', complete: false });
        } catch {
          // The turn's expected set still contains this missing report and blocks preparation.
        }
        // Preserve an explicit failure instead of returning an empty successful stream.
        result.push({
          type: 'error',
          reason: 'error',
          error: {
            role: 'assistant',
            content: [],
            provider: model.provider,
            model: model.id,
            api: model.api,
            timestamp: Date.now(),
            stopReason: 'error',
            errorMessage: error instanceof Error ? error.message : 'Native usage capture failed',
            usage: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              totalTokens: 0,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
            },
          },
        });
      } finally {
        result.end();
      }
    })();
    return result;
  };
}
