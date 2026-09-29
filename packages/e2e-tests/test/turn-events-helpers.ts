// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';
import type { OrcaClientConfig } from '../src/client.js';
import type { SpendEvent } from './spend-control-helpers.js';

/** Keep one transcript subscription per turn, including sandbox cold-start time. */
export async function collectTurnEvents(
  cfg: OrcaClientConfig,
  sessionId: string,
  seen: Set<string>,
  ready: (events: SpendEvent[]) => boolean,
  label: string,
  timeout: number,
): Promise<SpendEvent[]> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  const events: SpendEvent[] = [];
  try {
    const response = await fetch(
      `${cfg.baseURL}/v1/sessions/${sessionId}/events/stream?from_cursor=0`,
      {
        headers: {
          'x-api-key': cfg.apiKey,
          accept: 'text/event-stream',
          'orca-beta': 'guardrails',
        },
        signal: controller.signal,
      },
    );
    expect(response.status).toBe(200);
    if (!response.body) throw new Error('SSE response had no body');
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    try {
      while (!ready(events)) {
        const chunk = await reader.read();
        if (chunk.done) break;
        buffer += decoder.decode(chunk.value, { stream: true });
        let end: number;
        while ((end = buffer.indexOf('\n\n')) !== -1) {
          const block = buffer.slice(0, end);
          buffer = buffer.slice(end + 2);
          const data = block.split('\n').find((line) => line.startsWith('data: '));
          if (!data) continue;
          const event = JSON.parse(data.slice(6)) as SpendEvent;
          if (!seen.has(event.id)) {
            seen.add(event.id);
            events.push(event);
          }
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
    }
    if (!ready(events)) throw new Error('event stream ended before the turn completed');
    return events;
  } catch (error) {
    throw new Error(`${label} failed; saw: ${JSON.stringify(events)}`, { cause: error });
  } finally {
    clearTimeout(timer);
    controller.abort();
  }
}
