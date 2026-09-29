// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the WS tunnel frame protocol.
//
// Each frame kind round-trips through encode → decode unchanged. Decode rejects
// malformed input cleanly. Body encoding picks utf-8 for text content, base64
// for binary. The dot-separated wire `kind` strings and snake_case wire keys are
// the cross-component contract and are asserted byte-for-byte.

import { describe, it, expect } from 'vitest';
import {
  FrameKind,
  TUNNEL_MAX_MESSAGE_BYTES,
  type Frame,
  type HelloFrame,
  type RequestFrame,
  type ResponseHeadFrame,
  type ResponseBodyFrame,
  type ResponseEndFrame,
  type RequestCancelFrame,
  type PingFrame,
  type PongFrame,
  type WsOpenFrame,
  type WsFrame,
  type WsCloseFrame,
  decodeBody,
  decodeFrame,
  encodeBody,
  encodeFrame,
  isTextContentType,
} from '../../src/frames.js';

// ── Round-trip per frame kind ────────────────────────────

describe('round-trip per frame kind', () => {
  it('hello round trips', () => {
    const f: HelloFrame = {
      kind: FrameKind.Hello,
      runnerVersion: '0.1.2',
      frameProtocolVersion: 1,
      harnesses: ['claude-sdk', 'codex'],
      envs: ['os_sandbox'],
      resumeCursors: { ses_a: 'evt_7', ses_b: 'evt_12' },
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.Hello);
    const h = decoded as HelloFrame;
    expect(h.runnerVersion).toBe('0.1.2');
    expect(h.frameProtocolVersion).toBe(1);
    expect(h.harnesses).toEqual(['claude-sdk', 'codex']);
    expect(h.envs).toEqual(['os_sandbox']);
    // Per-session resume cursors survive the round trip so the owner pod can serve
    // an incremental after={cursor} replay for the bound session.
    expect(h.resumeCursors).toEqual({ ses_a: 'evt_7', ses_b: 'evt_12' });
  });

  it('request round trips with body and query string', () => {
    const f: RequestFrame = {
      kind: FrameKind.Request,
      id: 'req_abc',
      method: 'POST',
      path: '/v1/responses',
      queryString: 'background=true',
      headers: [['content-type', 'application/json']],
      body: '{"agent_id": "agent_x"}',
      encoding: 'utf-8',
      stream: true,
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.Request);
    const r = decoded as RequestFrame;
    expect(r.id).toBe('req_abc');
    expect(r.method).toBe('POST');
    expect(r.path).toBe('/v1/responses');
    expect(r.queryString).toBe('background=true');
    expect(r.headers).toEqual([['content-type', 'application/json']]);
    expect(r.body).toBe('{"agent_id": "agent_x"}');
    expect(r.encoding).toBe('utf-8');
    expect(r.stream).toBe(true);
  });

  it('request round trips with null body', () => {
    // GET requests have no body — encoded as null, decoded as null.
    const f: RequestFrame = {
      kind: FrameKind.Request,
      id: 'req_g',
      method: 'GET',
      path: '/health',
      body: null,
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.Request);
    expect((decoded as RequestFrame).body).toBeNull();
  });

  it('response.head round trips', () => {
    const f: ResponseHeadFrame = {
      kind: FrameKind.ResponseHead,
      id: 'req_abc',
      status: 200,
      headers: [['content-type', 'text/event-stream']],
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.ResponseHead);
    const r = decoded as ResponseHeadFrame;
    expect(r.status).toBe(200);
    expect(r.headers).toEqual([['content-type', 'text/event-stream']]);
  });

  it('response.body round trips utf-8', () => {
    const f: ResponseBodyFrame = {
      kind: FrameKind.ResponseBody,
      id: 'req_abc',
      body: 'data: {...}\n\n',
      encoding: 'utf-8',
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.ResponseBody);
    const r = decoded as ResponseBodyFrame;
    expect(r.body).toBe('data: {...}\n\n');
    expect(r.encoding).toBe('utf-8');
  });

  it('response.body round trips base64', () => {
    // Binary bodies (file downloads) ride as base64 — preserve encoding marker.
    const f: ResponseBodyFrame = {
      kind: FrameKind.ResponseBody,
      id: 'req_x',
      body: 'iVBORw0KGgoAAAA',
      encoding: 'base64',
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.ResponseBody);
    expect((decoded as ResponseBodyFrame).encoding).toBe('base64');
  });

  it('response.end round trips', () => {
    const f: ResponseEndFrame = { kind: FrameKind.ResponseEnd, id: 'req_abc' };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.ResponseEnd);
    expect((decoded as ResponseEndFrame).id).toBe('req_abc');
  });

  it('request.cancel round trips', () => {
    const f: RequestCancelFrame = {
      kind: FrameKind.RequestCancel,
      id: 'req_abc',
      reason: 'client_disconnected',
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.RequestCancel);
    expect((decoded as RequestCancelFrame).reason).toBe('client_disconnected');
  });

  it('ping and pong round trip', () => {
    const p: PingFrame = { kind: FrameKind.Ping, ts: 1709654400000 };
    const decodedP = decodeFrame(encodeFrame(p));
    expect(decodedP.kind).toBe(FrameKind.Ping);
    expect((decodedP as PingFrame).ts).toBe(1709654400000);

    const o: PongFrame = { kind: FrameKind.Pong, ts: 1709654400000 };
    const decodedO = decodeFrame(encodeFrame(o));
    expect(decodedO.kind).toBe(FrameKind.Pong);
    expect((decodedO as PongFrame).ts).toBe(1709654400000);
  });

  it('ws.open round trips', () => {
    const f: WsOpenFrame = {
      kind: FrameKind.WsOpen,
      chId: 'a1b2c3d4',
      path: '/v1/sessions/conv_abc/resources/terminals/terminal_bash_s1/attach',
      queryString: 'cols=80&rows=24',
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.WsOpen);
    const w = decoded as WsOpenFrame;
    expect(w.chId).toBe('a1b2c3d4');
    expect(w.path).toBe('/v1/sessions/conv_abc/resources/terminals/terminal_bash_s1/attach');
    expect(w.queryString).toBe('cols=80&rows=24');
  });

  it('ws.frame round trips utf-8 and base64', () => {
    const u: WsFrame = {
      kind: FrameKind.WsFrame,
      chId: 'ch_1',
      data: '{"type":"resize","cols":80}',
      encoding: 'utf-8',
    };
    const decodedU = decodeFrame(encodeFrame(u));
    expect(decodedU.kind).toBe(FrameKind.WsFrame);
    const wu = decodedU as WsFrame;
    expect(wu.chId).toBe('ch_1');
    expect(wu.data).toBe('{"type":"resize","cols":80}');
    expect(wu.encoding).toBe('utf-8');

    const b: WsFrame = {
      kind: FrameKind.WsFrame,
      chId: 'ch_1',
      data: 'AAEC',
      encoding: 'base64',
    };
    const decodedB = decodeFrame(encodeFrame(b));
    expect((decodedB as WsFrame).encoding).toBe('base64');
  });

  it('ws.close round trips', () => {
    const f: WsCloseFrame = {
      kind: FrameKind.WsClose,
      chId: 'ch_1',
      code: 1001,
      reason: 'going away',
    };
    const decoded = decodeFrame(encodeFrame(f));
    expect(decoded.kind).toBe(FrameKind.WsClose);
    const w = decoded as WsCloseFrame;
    expect(w.chId).toBe('ch_1');
    expect(w.code).toBe(1001);
    expect(w.reason).toBe('going away');
  });
});

// ── Optional-field defaults on decode ────────────────────

describe('optional-field defaults', () => {
  it('hello defaults harnesses, envs, and resume_cursors to empty', () => {
    const decoded = decodeFrame(
      JSON.stringify({ kind: 'hello', runner_version: '1', frame_protocol_version: 1 }),
    ) as HelloFrame;
    expect(decoded.harnesses).toEqual([]);
    expect(decoded.envs).toEqual([]);
    // A runner that presents no cursors (a fresh runner) decodes to an empty map,
    // which the owner pod treats as "fresh full replay".
    expect(decoded.resumeCursors).toEqual({});
  });

  it('request defaults query_string, headers, encoding, stream, and null body', () => {
    const decoded = decodeFrame(
      JSON.stringify({ kind: 'request', id: 'r', method: 'GET', path: '/' }),
    ) as RequestFrame;
    expect(decoded.queryString).toBe('');
    expect(decoded.headers).toEqual([]);
    expect(decoded.encoding).toBe('utf-8');
    expect(decoded.stream).toBe(false);
    expect(decoded.body).toBeNull();
  });

  it('request.cancel defaults reason to client_disconnected', () => {
    const decoded = decodeFrame(
      JSON.stringify({ kind: 'request.cancel', id: 'r' }),
    ) as RequestCancelFrame;
    expect(decoded.reason).toBe('client_disconnected');
  });

  it('response.body defaults encoding to utf-8', () => {
    const decoded = decodeFrame(
      JSON.stringify({ kind: 'response.body', id: 'r', body: 'x' }),
    ) as ResponseBodyFrame;
    expect(decoded.encoding).toBe('utf-8');
  });

  it('ws.open defaults query_string to empty string', () => {
    const decoded = decodeFrame(
      JSON.stringify({ kind: 'ws.open', ch_id: 'c', path: '/p' }),
    ) as WsOpenFrame;
    expect(decoded.queryString).toBe('');
  });

  it('ws.frame defaults encoding to utf-8', () => {
    const decoded = decodeFrame(
      JSON.stringify({ kind: 'ws.frame', ch_id: 'c', data: 'd' }),
    ) as WsFrame;
    expect(decoded.encoding).toBe('utf-8');
  });

  it('ws.close defaults code to 1000 and reason to empty string', () => {
    const decoded = decodeFrame(JSON.stringify({ kind: 'ws.close', ch_id: 'c' })) as WsCloseFrame;
    expect(decoded.code).toBe(1000);
    expect(decoded.reason).toBe('');
  });
});

// ── Decode failure modes ─────────────────────────────────

describe('decode failure modes', () => {
  it('rejects invalid JSON', () => {
    expect(() => decodeFrame('{not json')).toThrow(/not valid JSON/);
  });

  it('rejects a non-object root', () => {
    // A JSON array or scalar isn't a frame; reject loudly.
    expect(() => decodeFrame('[1, 2, 3]')).toThrow(/must be a JSON object/);
  });

  it('rejects a missing kind', () => {
    expect(() => decodeFrame(JSON.stringify({ id: 'x' }))).toThrow(/missing 'kind'/);
  });

  it('rejects an unknown kind', () => {
    // An unknown kind string surfaces an explicit "unknown frame kind" error
    // rather than silently treating it as an empty frame.
    expect(() => decodeFrame(JSON.stringify({ kind: 'fake.unknown_kind' }))).toThrow(
      /unknown frame kind/,
    );
  });

  it('rejects a request missing required fields', () => {
    // A request frame with no method/path is structurally invalid.
    expect(() => decodeFrame(JSON.stringify({ kind: 'request', id: 'x' }))).toThrow();
  });

  const malformed: Array<{ id: string; payload: Record<string, unknown> }> = [
    {
      id: 'hello-harnesses-not-list',
      payload: {
        kind: 'hello',
        runner_version: '1',
        frame_protocol_version: 1,
        harnesses: 123,
      },
    },
    {
      id: 'hello-resume-cursors-not-object',
      payload: {
        kind: 'hello',
        runner_version: '1',
        frame_protocol_version: 1,
        resume_cursors: ['evt_1'],
      },
    },
    {
      id: 'hello-resume-cursors-value-not-string',
      payload: {
        kind: 'hello',
        runner_version: '1',
        frame_protocol_version: 1,
        resume_cursors: { ses_a: 123 },
      },
    },
    {
      id: 'request-headers-not-list',
      payload: { kind: 'request', id: 'r', method: 'GET', path: '/', headers: 123 },
    },
    {
      id: 'request-stream-not-bool',
      payload: { kind: 'request', id: 'r', method: 'GET', path: '/', stream: 'yes' },
    },
    {
      id: 'request-body-not-string',
      payload: { kind: 'request', id: 'r', method: 'GET', path: '/', body: 123 },
    },
    {
      id: 'response-header-not-pair',
      payload: { kind: 'response.head', id: 'r', status: 200, headers: [['ok']] },
    },
    {
      id: 'ws-frame-data-not-string',
      payload: { kind: 'ws.frame', ch_id: 'ch_1', data: 123 },
    },
    {
      id: 'ws-close-code-not-int',
      payload: { kind: 'ws.close', ch_id: 'ch_1', code: '1000' },
    },
  ];

  for (const { id, payload } of malformed) {
    it(`rejects malformed optional field: ${id}`, () => {
      // Bad optional fields raise an error, not an incidental exception.
      expect(() => decodeFrame(JSON.stringify(payload))).toThrow();
    });
  }

  it('rejects a required int field that is a boolean', () => {
    // Booleans are not ints on the wire — ping ts must be a real integer.
    expect(() => decodeFrame(JSON.stringify({ kind: 'ping', ts: true }))).toThrow(
      /missing required int field/,
    );
  });

  it('rejects a required int field that is missing', () => {
    expect(() => decodeFrame(JSON.stringify({ kind: 'pong' }))).toThrow(
      /missing required int field/,
    );
  });

  it('accepts a required int at the safe-integer boundary', () => {
    // 2^53 - 1 is the largest integer JSON.parse round-trips exactly; it must
    // decode unchanged, matching an unbounded-integer peer.
    const max = Number.MAX_SAFE_INTEGER; // 9007199254740991
    const decoded = decodeFrame(`{"kind": "ping", "ts": ${max}}`) as PingFrame;
    expect(decoded.ts).toBe(max);
  });

  it('rejects a required int beyond the safe-integer range', () => {
    // 2^53 + 1 cannot be represented as a JS number — JSON.parse silently
    // rounds it to 2^53, so the decoded value would no longer equal the value
    // on the wire. Reject it loudly instead of passing a corrupted int through.
    // (The literal stays a string here so JSON.parse never sees it pre-rounded.)
    expect(() => decodeFrame('{"kind": "ping", "ts": 9007199254740993}')).toThrow(
      /missing required int field/,
    );
  });

  it('rejects an optional int beyond the safe-integer range', () => {
    // ws.close `code` is the only optional int; it shares the safe-integer
    // guard so an out-of-range, JSON.parse-rounded value is rejected, not
    // silently accepted as a different number than the sender wrote.
    expect(() =>
      decodeFrame('{"kind": "ws.close", "ch_id": "c", "code": 9007199254740993}'),
    ).toThrow(/must be an integer/);
  });

  it('rejects a hello with a non-string runner_version', () => {
    expect(() =>
      decodeFrame(JSON.stringify({ kind: 'hello', runner_version: 1, frame_protocol_version: 1 })),
    ).toThrow(/missing required string field/);
  });

  it('rejects a request.cancel with a non-string reason', () => {
    expect(() =>
      decodeFrame(JSON.stringify({ kind: 'request.cancel', id: 'r', reason: 123 })),
    ).toThrow(/must be a string/);
  });

  it('rejects an unknown encoding on every body-bearing frame kind', () => {
    // `encoding` is a literal union ("utf-8" | "base64"); decode must CHECK
    // membership, not cast — a peer sending "gzip" must fail loudly at the frame
    // boundary (like an unknown kind), not decode into a value the type claims
    // is utf-8 and corrupt the body downstream.
    expect(() =>
      decodeFrame(
        JSON.stringify({ kind: 'request', id: 'r', method: 'GET', path: '/', encoding: 'gzip' }),
      ),
    ).toThrow(/must be "utf-8" or "base64"/);
    expect(() =>
      decodeFrame(JSON.stringify({ kind: 'response.body', id: 'r', body: 'x', encoding: 'gzip' })),
    ).toThrow(/must be "utf-8" or "base64"/);
    expect(() =>
      decodeFrame(JSON.stringify({ kind: 'ws.frame', ch_id: 'c', data: 'x', encoding: 'gzip' })),
    ).toThrow(/must be "utf-8" or "base64"/);
  });

  it('rejects a hello with an env entry that is not a string', () => {
    expect(() =>
      decodeFrame(
        JSON.stringify({
          kind: 'hello',
          runner_version: '1',
          frame_protocol_version: 1,
          envs: ['ok', 123],
        }),
      ),
    ).toThrow(/must be a list of strings/);
  });

  it('rejects a ws.close with a boolean code (bool is not int)', () => {
    expect(() => decodeFrame(JSON.stringify({ kind: 'ws.close', ch_id: 'c', code: true }))).toThrow(
      /must be an integer/,
    );
  });

  // An OPTIONAL field that is *present but JSON-null* is rejected, not
  // defaulted. A null only stands in for "absent" where the schema explicitly
  // allows it (request `body`); for every other optional the type check fires.
  // This pins the boundary against a refactor of the optional* helpers to a
  // `value ?? fallback` form, which would silently swallow an explicit null and
  // break wire parity. (request `body: null` is asserted to stay valid below.)
  const nullOptionalField: Array<{ id: string; payload: Record<string, unknown>; match: RegExp }> =
    [
      {
        id: 'request-query_string-null',
        payload: { kind: 'request', id: 'r', method: 'GET', path: '/', query_string: null },
        match: /must be a string/,
      },
      {
        id: 'request-encoding-null',
        payload: { kind: 'request', id: 'r', method: 'GET', path: '/', encoding: null },
        match: /must be a string/,
      },
      {
        id: 'request-stream-null',
        payload: { kind: 'request', id: 'r', method: 'GET', path: '/', stream: null },
        match: /must be a boolean/,
      },
      {
        id: 'request-headers-null',
        payload: { kind: 'request', id: 'r', method: 'GET', path: '/', headers: null },
        match: /must be a list of header pairs/,
      },
      {
        id: 'response.body-encoding-null',
        payload: { kind: 'response.body', id: 'r', body: 'x', encoding: null },
        match: /must be a string/,
      },
      {
        id: 'request.cancel-reason-null',
        payload: { kind: 'request.cancel', id: 'r', reason: null },
        match: /must be a string/,
      },
      {
        id: 'hello-harnesses-null',
        payload: { kind: 'hello', runner_version: '1', frame_protocol_version: 1, harnesses: null },
        match: /must be a list of strings/,
      },
      {
        id: 'ws.close-code-null',
        payload: { kind: 'ws.close', ch_id: 'c', code: null },
        match: /must be an integer/,
      },
      {
        id: 'ws.open-query_string-null',
        payload: { kind: 'ws.open', ch_id: 'c', path: '/p', query_string: null },
        match: /must be a string/,
      },
    ];

  for (const { id, payload, match } of nullOptionalField) {
    it(`rejects a present-but-null optional field: ${id}`, () => {
      expect(() => decodeFrame(JSON.stringify(payload))).toThrow(match);
    });
  }

  it('accepts a request with an explicit null body (null means absent for body only)', () => {
    // `body` is the one optional where JSON-null is the legal "no body" sentinel
    // — the present-but-null rejection above must NOT extend to it.
    const decoded = decodeFrame(
      JSON.stringify({ kind: 'request', id: 'r', method: 'GET', path: '/', body: null }),
    ) as RequestFrame;
    expect(decoded.body).toBeNull();
  });

  it('rejects an over-length header pair', () => {
    // A header inner list must be exactly [name, value]; a 3-element list is
    // malformed and the length!==2 guard rejects it.
    expect(() =>
      decodeFrame(
        JSON.stringify({ kind: 'response.head', id: 'r', status: 200, headers: [['a', 'b', 'c']] }),
      ),
    ).toThrow(/must be a list of header pairs/);
  });

  it('rejects a non-list header item', () => {
    // Each header entry must itself be a list; a bare scalar is rejected.
    expect(() =>
      decodeFrame(JSON.stringify({ kind: 'response.head', id: 'r', status: 200, headers: [42] })),
    ).toThrow(/must be a list of header pairs/);
  });
});

// ── Encode failure mode ──────────────────────────────────

describe('encode failure mode', () => {
  it('rejects an unknown frame type', () => {
    // A frame object with an unrecognized kind cannot be serialized.
    expect(() => encodeFrame({ kind: 'bogus' } as unknown as Frame)).toThrow(/unknown frame type/);
  });
});

// ── Body encoding helpers ────────────────────────────────

describe('body encoding helpers', () => {
  it('recognizes standard text content types', () => {
    expect(isTextContentType('application/json')).toBe(true);
    expect(isTextContentType('application/json; charset=utf-8')).toBe(true);
    expect(isTextContentType('text/event-stream')).toBe(true);
    expect(isTextContentType('text/plain')).toBe(true);
    expect(isTextContentType('application/jsonl')).toBe(true);
    expect(isTextContentType('application/x-ndjson')).toBe(true);
  });

  it('rejects binary content types', () => {
    expect(isTextContentType('image/png')).toBe(false);
    expect(isTextContentType('application/octet-stream')).toBe(false);
    expect(isTextContentType('application/pdf')).toBe(false);
  });

  it('is case-insensitive for content types', () => {
    expect(isTextContentType('Application/JSON')).toBe(true);
    expect(isTextContentType('TEXT/PLAIN')).toBe(true);
  });

  it('encode_body picks utf-8 for text', () => {
    const [body, encoding] = encodeBody(new TextEncoder().encode('{"x": 1}'), 'application/json');
    expect(encoding).toBe('utf-8');
    expect(body).toBe('{"x": 1}');
  });

  it('encode_body picks base64 for binary', () => {
    const bytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47]); // \x89PNG
    const [body, encoding] = encodeBody(bytes, 'image/png');
    expect(encoding).toBe('base64');
    // decode_body round-trips back to the same bytes.
    expect(decodeBody(body, encoding)).toEqual(bytes);
  });

  it('decode_body round-trips both encodings', () => {
    expect(decodeBody('hello', 'utf-8')).toEqual(new TextEncoder().encode('hello'));
    expect(decodeBody('aGVsbG8=', 'base64')).toEqual(new TextEncoder().encode('hello'));
  });

  it('decode_body rejects an unknown encoding', () => {
    expect(() => decodeBody('x', 'utf-7')).toThrow(/unknown body encoding/);
  });
});

// ── Wire compatibility ───────────────────────────────────

describe('wire compatibility', () => {
  it('encoded kind uses the canonical dot-separated string', () => {
    // The dot-separated kinds are the load-bearing assertion — a refactor that
    // flipped them to "responseEnd" or "RESPONSE_END" would break wire compat
    // with any client outside the test suite.
    expect(JSON.parse(encodeFrame({ kind: FrameKind.Ping, ts: 1 })).kind).toBe('ping');
    expect(JSON.parse(encodeFrame({ kind: FrameKind.Pong, ts: 1 })).kind).toBe('pong');
    expect(JSON.parse(encodeFrame({ kind: FrameKind.ResponseEnd, id: 'x' })).kind).toBe(
      'response.end',
    );
  });

  it('FrameKind enum values match the design-spec strings', () => {
    expect(FrameKind.Hello).toBe('hello');
    expect(FrameKind.Request).toBe('request');
    expect(FrameKind.ResponseHead).toBe('response.head');
    expect(FrameKind.ResponseBody).toBe('response.body');
    expect(FrameKind.ResponseEnd).toBe('response.end');
    expect(FrameKind.RequestCancel).toBe('request.cancel');
    expect(FrameKind.Ping).toBe('ping');
    expect(FrameKind.Pong).toBe('pong');
    expect(FrameKind.WsOpen).toBe('ws.open');
    expect(FrameKind.WsFrame).toBe('ws.frame');
    expect(FrameKind.WsClose).toBe('ws.close');
  });

  it('encodes hello with snake_case wire keys', () => {
    const wire = JSON.parse(
      encodeFrame({
        kind: FrameKind.Hello,
        runnerVersion: '0.1.2',
        frameProtocolVersion: 2,
        harnesses: ['a'],
        envs: ['b'],
      }),
    );
    expect(wire).toEqual({
      kind: 'hello',
      runner_version: '0.1.2',
      frame_protocol_version: 2,
      harnesses: ['a'],
      envs: ['b'],
      resume_cursors: {},
    });
  });

  it('encodes request with snake_case wire keys and full field set', () => {
    const wire = JSON.parse(
      encodeFrame({
        kind: FrameKind.Request,
        id: 'r',
        method: 'POST',
        path: '/p',
        queryString: 'q=1',
        headers: [['a', 'b']],
        body: 'x',
        encoding: 'utf-8',
        stream: true,
      }),
    );
    expect(wire).toEqual({
      kind: 'request',
      id: 'r',
      method: 'POST',
      path: '/p',
      query_string: 'q=1',
      headers: [['a', 'b']],
      body: 'x',
      encoding: 'utf-8',
      stream: true,
    });
  });

  it('encodes ws.open with ch_id wire key', () => {
    const wire = JSON.parse(
      encodeFrame({ kind: FrameKind.WsOpen, chId: 'c', path: '/p', queryString: 'q' }),
    );
    expect(wire).toEqual({ kind: 'ws.open', ch_id: 'c', path: '/p', query_string: 'q' });
  });

  it('encodes ws.frame and ws.close with ch_id wire key', () => {
    expect(
      JSON.parse(encodeFrame({ kind: FrameKind.WsFrame, chId: 'c', data: 'd', encoding: 'utf-8' })),
    ).toEqual({
      kind: 'ws.frame',
      ch_id: 'c',
      data: 'd',
      encoding: 'utf-8',
    });
    expect(
      JSON.parse(encodeFrame({ kind: FrameKind.WsClose, chId: 'c', code: 1000, reason: '' })),
    ).toEqual({
      kind: 'ws.close',
      ch_id: 'c',
      code: 1000,
      reason: '',
    });
  });

  it('encodes request.cancel reason on the wire', () => {
    expect(
      JSON.parse(
        encodeFrame({ kind: FrameKind.RequestCancel, id: 'r', reason: 'client_disconnected' }),
      ),
    ).toEqual({
      kind: 'request.cancel',
      id: 'r',
      reason: 'client_disconnected',
    });
  });

  it('encode fills wire defaults when optional fields are omitted', () => {
    // Supply only the required fields, and the serialized form still carries
    // every default. A peer must always see the full field set regardless of
    // how the sender constructed the frame.
    expect(
      JSON.parse(
        encodeFrame({ kind: FrameKind.Hello, runnerVersion: '1', frameProtocolVersion: 1 }),
      ),
    ).toEqual({
      kind: 'hello',
      runner_version: '1',
      frame_protocol_version: 1,
      harnesses: [],
      envs: [],
      resume_cursors: {},
    });
    expect(
      JSON.parse(encodeFrame({ kind: FrameKind.Request, id: 'r', method: 'GET', path: '/' })),
    ).toEqual({
      kind: 'request',
      id: 'r',
      method: 'GET',
      path: '/',
      query_string: '',
      headers: [],
      body: null,
      encoding: 'utf-8',
      stream: false,
    });
    expect(JSON.parse(encodeFrame({ kind: FrameKind.RequestCancel, id: 'r' }))).toEqual({
      kind: 'request.cancel',
      id: 'r',
      reason: 'client_disconnected',
    });
    expect(JSON.parse(encodeFrame({ kind: FrameKind.ResponseBody, id: 'r', body: 'x' }))).toEqual({
      kind: 'response.body',
      id: 'r',
      body: 'x',
      encoding: 'utf-8',
    });
    expect(JSON.parse(encodeFrame({ kind: FrameKind.WsOpen, chId: 'c', path: '/p' }))).toEqual({
      kind: 'ws.open',
      ch_id: 'c',
      path: '/p',
      query_string: '',
    });
    expect(JSON.parse(encodeFrame({ kind: FrameKind.WsFrame, chId: 'c', data: 'd' }))).toEqual({
      kind: 'ws.frame',
      ch_id: 'c',
      data: 'd',
      encoding: 'utf-8',
    });
    expect(JSON.parse(encodeFrame({ kind: FrameKind.WsClose, chId: 'c' }))).toEqual({
      kind: 'ws.close',
      ch_id: 'c',
      code: 1000,
      reason: '',
    });
  });

  it('decode ignores unknown extra keys on a frame', () => {
    // Decode reads only the keys it knows about; a forward-compatible peer that
    // adds a field must not break older decoders. The extra key is dropped and
    // the decoded frame carries only its declared fields.
    const decoded = decodeFrame(JSON.stringify({ kind: 'ping', ts: 5, extra: 'x' }));
    expect(decoded).toEqual({ kind: FrameKind.Ping, ts: 5 });
    expect((decoded as Record<string, unknown>).extra).toBeUndefined();
  });

  it('encodes an explicit empty-string request body as "" (not coerced to null)', () => {
    // An empty body ('') is distinct from no body (null) on the wire: e.g. a
    // POST with a zero-length payload. The encoder must preserve '' verbatim and
    // a round-trip must keep it a string, not collapse it to null.
    const encoded = encodeFrame({
      kind: FrameKind.Request,
      id: 'r',
      method: 'POST',
      path: '/p',
      body: '',
    });
    expect(JSON.parse(encoded).body).toBe('');
    expect((decodeFrame(encoded) as RequestFrame).body).toBe('');
  });
});

// ── Size limit ───────────────────────────────────────────

describe('size limit', () => {
  it('exposes the 100 MiB tunnel max message size', () => {
    expect(TUNNEL_MAX_MESSAGE_BYTES).toBe(100 * 1024 * 1024);
  });
});
