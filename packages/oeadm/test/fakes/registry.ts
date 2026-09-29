// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-process fakes for the registry HTTP + SSE surface. Tests drive the CLI's
// client and interactive loop against these — no real stack, no network.
//
// `scriptSse` turns an array of frame objects into a `ReadableStream` shaped
// exactly like the registry's `GET /v1/sessions/:id/stream` body: each frame is
// an SSE block (`id:` / `event:` / `data:` lines, blank-line terminated), with
// optional `:heartbeat` comment lines interleaved to exercise the parser's
// comment handling. `fakeFetch` records every request and answers from a
// scripted route table, so a test asserts the exact POST bodies the CLI sent.

import type { OrcaFetch } from '../../src/client.js';

/** A single recorded HTTP request the CLI made through the injected fetch. */
export interface RecordedRequest {
  method: string;
  url: string;
  headers: Record<string, string>;
  /** Parsed JSON body when the request carried one, else undefined. */
  body?: unknown;
}

/** SSE frame shape as it appears on the wire after `protoToHttpEvent`. */
export interface WireFrame {
  type: string;
  [k: string]: unknown;
}

/**
 * Encode frames as an SSE `ReadableStream<Uint8Array>`, matching the registry's
 * `streamSession` framing (`id:`/`event:`/`data:` + blank line). A frame may be
 * the string `':heartbeat'` to emit a bare comment line the parser must skip.
 */
export function scriptSse(frames: Array<WireFrame | ':heartbeat'>): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const frame of frames) {
        if (frame === ':heartbeat') {
          controller.enqueue(encoder.encode(`:heartbeat\n\n`));
          continue;
        }
        const seq = typeof frame.seq === 'string' ? frame.seq : '0';
        const body = JSON.stringify(frame);
        controller.enqueue(encoder.encode(`id: ${seq}\nevent: ${frame.type}\ndata: ${body}\n\n`));
      }
      controller.close();
    },
  });
}

/**
 * Encode frames but split each SSE block across two chunks at an arbitrary byte
 * boundary, so a test proves the parser reassembles frames that arrive split.
 */
export function scriptSseChunked(
  frames: Array<WireFrame>,
  splitAt = 5,
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const blocks = frames.map((frame) => {
    const seq = typeof frame.seq === 'string' ? frame.seq : '0';
    return `id: ${seq}\nevent: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`;
  });
  const whole = blocks.join('');
  const bytes = encoder.encode(whole);
  return new ReadableStream<Uint8Array>({
    start(controller) {
      const mid = Math.min(splitAt, bytes.length);
      controller.enqueue(bytes.slice(0, mid));
      controller.enqueue(bytes.slice(mid));
      controller.close();
    },
  });
}

/** A scripted route: matches on `METHOD path` and returns a canned response. */
export interface Route {
  status?: number;
  /** JSON body for non-stream routes. */
  json?: unknown;
  /** SSE stream body for the stream route. */
  stream?: ReadableStream<Uint8Array>;
}

export interface FakeRegistry {
  fetch: OrcaFetch;
  requests: RecordedRequest[];
  /** Requests filtered to a single `METHOD path` key. */
  requestsFor(key: string): RecordedRequest[];
}

/**
 * Build a fake fetch over a `METHOD pathname` route table. Unmatched routes
 * throw so a test fails loudly on an unexpected call rather than hanging.
 */
export function fakeRegistry(routes: Record<string, Route | Route[]>): FakeRegistry {
  const requests: RecordedRequest[] = [];
  // For keys with multiple scripted responses, hand them out in call order.
  const cursors = new Map<string, number>();

  const fetchImpl: OrcaFetch = async (input, init) => {
    const url = typeof input === 'string' ? input : input.toString();
    const method = (init?.method ?? 'GET').toUpperCase();
    const pathname = new URL(url).pathname;
    const key = `${method} ${pathname}`;

    const headers: Record<string, string> = {};
    if (init?.headers) {
      for (const [k, v] of Object.entries(init.headers as Record<string, string>)) {
        headers[k.toLowerCase()] = v;
      }
    }
    let body: unknown;
    if (typeof init?.body === 'string' && init.body.length > 0) {
      try {
        body = JSON.parse(init.body);
      } catch {
        body = init.body;
      }
    }
    requests.push({ method, url, headers, body });

    const entry = routes[key];
    if (entry === undefined) {
      throw new Error(`fakeRegistry: no route for ${key}`);
    }
    let route: Route;
    if (Array.isArray(entry)) {
      const idx = cursors.get(key) ?? 0;
      route = entry[Math.min(idx, entry.length - 1)]!;
      cursors.set(key, idx + 1);
    } else {
      route = entry;
    }

    const status = route.status ?? 200;
    if (route.stream !== undefined) {
      return new Response(route.stream, {
        status,
        headers: { 'content-type': 'text/event-stream' },
      });
    }
    const payload = route.json ?? {};
    return new Response(JSON.stringify(payload), {
      status,
      headers: { 'content-type': 'application/json' },
    });
  };

  return {
    fetch: fetchImpl,
    requests,
    requestsFor(key: string) {
      return requests.filter((r) => `${r.method} ${new URL(r.url).pathname}` === key);
    },
  };
}
