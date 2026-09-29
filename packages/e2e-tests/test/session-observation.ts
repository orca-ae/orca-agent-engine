// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Session-event observation for Layer B specs.
 *
 * Extracted from `real-agent-loop.spec.ts` so every spec that watches a live
 * session reads the same frames the same way. The merge-then-stream-then-settle
 * shape is load-bearing and easy to get subtly wrong: `POST /events` returns
 * after append, so the harness can emit frames before a subscriber opens SSE,
 * and the canonical append/list responses omit the internal cursor. Seeding
 * from the read model, replaying SSE from the beginning, and deduping is what
 * makes an early frame observable at all.
 */
import { expect } from 'vitest';
import { apiCall, type OrcaClientConfig } from '../src/client.js';

export interface SseFrame {
  type: string;
  seq?: string;
  processed_at: string;
  produced_at: string;
  produced_by: string;
  content?: unknown;
  [k: string]: unknown;
}

export interface SseCollectOpts {
  until: (frames: SseFrame[]) => boolean;
  deadlineMs: number;
}

export async function collectSseFrames(
  cfg: OrcaClientConfig,
  sessionId: string,
  opts: SseCollectOpts,
): Promise<SseFrame[]> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), opts.deadlineMs + 5_000);
  try {
    const frames: SseFrame[] = [];
    const seenFrameKeys = new Set<string>();
    const addFrame = (frame: SseFrame): void => {
      const frameId = typeof frame.id === 'string' ? frame.id : '';
      const eventId = typeof frame.event_id === 'string' ? frame.event_id : '';
      const frameKey =
        frameId ||
        (typeof frame.seq === 'string' ? `${frame.seq}:${frame.type}:${eventId}` : eventId);
      if (frameKey) {
        if (seenFrameKeys.has(frameKey)) return;
        seenFrameKeys.add(frameKey);
      }
      frames.push(frame);
    };
    const mergeListedEvents = async (): Promise<boolean> => {
      const before = frames.length;
      let page: string | null = '';
      while (page !== null) {
        const eventsUrl = new URL(`/v1/sessions/${sessionId}/events`, cfg.baseURL);
        eventsUrl.searchParams.set('limit', '1000');
        if (page) eventsUrl.searchParams.set('page', page);
        const listed = await apiCall(cfg, `${eventsUrl.pathname}${eventsUrl.search}`, {
          method: 'GET',
        });
        expect(listed.status, listed.text).toBe(200);
        const body = listed.json<{ data: SseFrame[]; next_page: string | null }>();
        for (const frame of body.data) addFrame(frame);
        page = body.next_page;
      }
      return frames.length > before;
    };
    const settleListedEvents = async (): Promise<void> => {
      const deadline = Date.now() + 5_000;
      let idlePolls = 0;
      while (Date.now() < deadline && idlePolls < 2) {
        const changed = await mergeListedEvents();
        idlePolls = changed ? 0 : idlePolls + 1;
        if (idlePolls < 2) await new Promise((resolve) => setTimeout(resolve, 500));
      }
    };

    // `POST /events` returns after append, while the harness may already emit
    // early frames (tool_use / agent.message) before this helper opens SSE.
    // Canonical append/list responses intentionally omit the internal cursor,
    // so seed from the read model, replay SSE from the beginning, and dedupe.
    await mergeListedEvents();
    if (opts.until(frames)) {
      await settleListedEvents();
      return frames;
    }

    const streamUrl = new URL(`/v1/sessions/${sessionId}/events/stream`, cfg.baseURL);
    streamUrl.searchParams.set('from_cursor', '0');
    const res = await fetch(streamUrl.toString(), {
      method: 'GET',
      headers: { 'x-api-key': cfg.apiKey, accept: 'text/event-stream' },
      signal: ac.signal,
    });
    if (res.status !== 200) {
      throw new Error(`SSE handshake failed: ${res.status}`);
    }
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffered = '';
    const deadline = Date.now() + opts.deadlineMs;

    while (Date.now() < deadline) {
      let chunk: ReadableStreamReadResult<Uint8Array>;
      try {
        chunk = await reader.read();
      } catch {
        break;
      }
      if (chunk.done) break;
      buffered += decoder.decode(chunk.value, { stream: true });

      let idx;
      while ((idx = buffered.indexOf('\n\n')) >= 0) {
        const block = buffered.slice(0, idx);
        buffered = buffered.slice(idx + 2);
        if (block.startsWith(':')) continue;
        const lines = block.split('\n');
        const dataLine = lines.find((line) => line.startsWith('data: '));
        if (!dataLine) continue;
        try {
          const payload = JSON.parse(dataLine.slice(6)) as SseFrame;
          const idLine = lines.find((line) => line.startsWith('id: '));
          if (idLine) payload.seq = idLine.slice(4);
          addFrame(payload);
        } catch {
          /* ignore non-JSON */
        }
      }

      if (opts.until(frames)) break;
    }

    await reader.cancel().catch(() => {});
    await settleListedEvents();
    return frames;
  } finally {
    clearTimeout(timer);
    ac.abort();
  }
}
