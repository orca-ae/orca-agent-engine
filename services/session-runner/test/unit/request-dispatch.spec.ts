// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner's request-dispatch seam.
//
// The registry PUSHES framed HTTP requests over the runner tunnel; the runner
// serves them like a local app and streams a response back. The dispatch seam is
// the registration point the serve loop calls into: the next unit registers a
// turn handler (`POST /v1/runner/turn`), a snapshot handler
// (`POST /v1/runner/snapshot`), and a replay handler (`POST /v1/runner/replay`)
// on it. This spec pins the routing contract, the streaming-response shape, the
// 404 for an unrouted path, the cancellation signal, and the request-body
// surface — independent of the WebSocket framing (covered by the serve spec).

import { describe, it, expect } from 'vitest';
import {
  RouteDispatcher,
  type DispatchRequest,
  type DispatchResponse,
} from '../../src/tunnel/request-dispatch.js';

/** Collect a dispatch response's body stream into one buffer. */
async function drain(res: DispatchResponse): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of res.body) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks.map((c) => Buffer.from(c)));
}

/** A one-line JSON body for a synchronous handler. */
function jsonBody(value: unknown): DispatchResponse {
  const text = `${JSON.stringify(value)}`;
  return {
    status: 200,
    headers: [['content-type', 'application/json']],
    body: (async function* () {
      yield new TextEncoder().encode(text);
    })(),
  };
}

describe('RouteDispatcher', () => {
  it('routes a request to the handler registered for its (method, path)', async () => {
    const seen: DispatchRequest[] = [];
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async (req) => {
      seen.push(req);
      return jsonBody({ ok: true });
    });

    const res = await dispatcher.dispatch({
      method: 'POST',
      path: '/v1/runner/turn',
      queryString: '',
      headers: [['x-orca-session-id', 'ses_1']],
      body: new TextEncoder().encode('{"id":"evt_1"}'),
    });

    expect(res.status).toBe(200);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.path).toBe('/v1/runner/turn');
    expect(Buffer.from(seen[0]!.body).toString('utf8')).toBe('{"id":"evt_1"}');
    expect(Buffer.from(await drain(res)).toString('utf8')).toBe('{"ok":true}');
  });

  it('exposes request headers case-insensitively via header()', async () => {
    const dispatcher = new RouteDispatcher();
    let sessionId: string | undefined;
    dispatcher.register('POST', '/v1/runner/turn', async (req) => {
      // The frame codec lower-cases header names on the wire; the canonical-case
      // constant must still resolve.
      sessionId = req.header('X-Orca-Session-Id');
      return jsonBody({});
    });

    await dispatcher.dispatch({
      method: 'POST',
      path: '/v1/runner/turn',
      queryString: '',
      headers: [['x-orca-session-id', 'ses_42']],
      body: new Uint8Array(0),
    });

    expect(sessionId).toBe('ses_42');
  });

  it('answers an unrouted path with a 404 and a JSON error body, not a throw', async () => {
    const dispatcher = new RouteDispatcher();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: '/v1/runner/does-not-exist',
      queryString: '',
      headers: [],
      body: new Uint8Array(0),
    });
    expect(res.status).toBe(404);
    const body = JSON.parse(Buffer.from(await drain(res)).toString('utf8')) as { error: string };
    expect(body.error).toBe('not_found');
  });

  it('matches the method too — a registered path under a different method is a 404', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => jsonBody({}));
    const res = await dispatcher.dispatch({
      method: 'GET',
      path: '/v1/runner/turn',
      queryString: '',
      headers: [],
      body: new Uint8Array(0),
    });
    expect(res.status).toBe(404);
  });

  it('streams a multi-chunk response body in order', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => ({
      status: 200,
      headers: [['content-type', 'application/x-ndjson']],
      body: (async function* () {
        yield new TextEncoder().encode('{"type":"a"}\n');
        yield new TextEncoder().encode('{"type":"b"}\n');
      })(),
    }));

    const res = await dispatcher.dispatch({
      method: 'POST',
      path: '/v1/runner/turn',
      queryString: '',
      headers: [],
      body: new Uint8Array(0),
    });
    expect(Buffer.from(await drain(res)).toString('utf8')).toBe('{"type":"a"}\n{"type":"b"}\n');
  });

  it('passes the cancellation signal to the handler so a long turn can abort', async () => {
    const dispatcher = new RouteDispatcher();
    const ac = new AbortController();
    let aborted = false;
    dispatcher.register('POST', '/v1/runner/turn', async (_req, signal) => ({
      status: 200,
      headers: [],
      body: (async function* () {
        yield new TextEncoder().encode('first\n');
        // Park until cancelled, then surface that we observed the abort.
        await new Promise<void>((resolve) => {
          if (signal.aborted) {
            resolve();
            return;
          }
          signal.addEventListener('abort', () => resolve(), { once: true });
        });
        aborted = signal.aborted;
      })(),
    }));

    const res = await dispatcher.dispatch(
      {
        method: 'POST',
        path: '/v1/runner/turn',
        queryString: '',
        headers: [],
        body: new Uint8Array(0),
      },
      ac.signal,
    );

    const iterator = res.body[Symbol.asyncIterator]();
    const first = await iterator.next();
    expect(Buffer.from(first.value as Uint8Array).toString('utf8')).toBe('first\n');
    ac.abort();
    await iterator.next();
    expect(aborted).toBe(true);
  });

  it('rejects registering two handlers for the same (method, path)', () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => jsonBody({}));
    expect(() => dispatcher.register('POST', '/v1/runner/turn', async () => jsonBody({}))).toThrow(
      /already registered/,
    );
  });

  it('normalizes the method case so a lowercase frame method still routes', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => jsonBody({ ok: 1 }));
    const res = await dispatcher.dispatch({
      method: 'post',
      path: '/v1/runner/turn',
      queryString: '',
      headers: [],
      body: new Uint8Array(0),
    });
    expect(res.status).toBe(200);
  });
});
