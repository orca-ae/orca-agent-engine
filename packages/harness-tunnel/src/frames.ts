// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// WebSocket tunnel frame schema.
//
// Eleven frame kinds, all JSON. Frames carrying request/response correlation use
// an `id` field; `hello` / `ping` / `pong` do not.
//
// Body-bearing frames (`request`, `response.body`) carry an explicit `encoding`
// field — `"utf-8"` (default; body is the literal string) or `"base64"` (body is
// base64). Adapters pick `utf-8` for content-types like `application/json` /
// `text/event-stream`; otherwise `base64` for binary payloads.
//
// This module exports a discriminated union of frame objects keyed on `kind`,
// plus `encodeFrame` / `decodeFrame` helpers. The contract is small enough that
// hand-rolled validation in `decodeFrame` is the right level of machinery — no
// schema library. TS field names are camelCase; the JSON wire keys stay
// snake_case (`runner_version`, `frame_protocol_version`, `query_string`,
// `ch_id`) because the wire schema is the cross-component contract.

/** All frame kinds; the value is the JSON wire string. */
export enum FrameKind {
  Hello = 'hello',
  Request = 'request',
  ResponseHead = 'response.head',
  ResponseBody = 'response.body',
  ResponseEnd = 'response.end',
  RequestCancel = 'request.cancel',
  Ping = 'ping',
  Pong = 'pong',
  // WebSocket-channel frames: carry a tunneled WS attach to the runner (e.g. a
  // browser terminal attaching to a runner-side shell). Three frames, all JSON;
  // binary WS payloads ride in ws.frame with encoding="base64".
  WsOpen = 'ws.open',
  WsFrame = 'ws.frame',
  WsClose = 'ws.close',
}

/** One header pair: `[name, value]`. */
export type HeaderPair = [string, string];

/** Body encoding marker: `"utf-8"` (literal string) or `"base64"`. */
export type BodyEncoding = 'utf-8' | 'base64';

// ── Frame shapes ─────────────────────────────────────────

// Fields with a wire default are optional on the TS object — `encodeFrame`
// substitutes the field's wire default, and `decodeFrame` always returns them
// fully populated. Construction mirrors that: supply the required fields, and
// the defaults fill the rest.

/** Runner's first frame on a fresh tunnel. */
export interface HelloFrame {
  kind: FrameKind.Hello;
  /** Runner's semver string, e.g. `"0.1.2"`. */
  runnerVersion: string;
  /** Wire-protocol major. Server refuses on major mismatch. */
  frameProtocolVersion: number;
  /** Names of harness kinds the runner can spawn. Defaults to `[]`. */
  harnesses?: string[];
  /** Names of OS env types the runner supports. Defaults to `[]`. */
  envs?: string[];
  /**
   * Per-session resume cursors the runner presents on (re)connect: a map of
   * `sessionId → lastConsumedEventId` — the stable transcript id of the last
   * event the runner durably consumed for each session it still holds in memory.
   * The owner pod serves an incremental `after={cursor}` resume replay from each
   * named cursor instead of a fresh full replay (the per-session last-consumed id
   * the runner tracks, presented here on (re)connect). Defaults to `{}` —
   * a fresh runner (or one whose state is gone) presents no cursor and gets a
   * full replay. A session absent from the map is treated as fresh.
   */
  resumeCursors?: Readonly<Record<string, string>>;
}

/** Server → runner: execute this HTTP request locally. */
export interface RequestFrame {
  kind: FrameKind.Request;
  id: string;
  method: string;
  path: string;
  /** Defaults to `""`. */
  queryString?: string;
  /** Defaults to `[]`. */
  headers?: HeaderPair[];
  /** `null` when the request has no body (e.g. GET). Defaults to `null`. */
  body?: string | null;
  /** Defaults to `"utf-8"`. */
  encoding?: BodyEncoding;
  /** Defaults to `false`. */
  stream?: boolean;
}

/** Runner → server: status code + response headers. */
export interface ResponseHeadFrame {
  kind: FrameKind.ResponseHead;
  id: string;
  status: number;
  /** Defaults to `[]`. */
  headers?: HeaderPair[];
}

/** Runner → server: a body chunk; repeated for streaming responses. */
export interface ResponseBodyFrame {
  kind: FrameKind.ResponseBody;
  id: string;
  body: string;
  /** Defaults to `"utf-8"`. */
  encoding?: BodyEncoding;
}

/** Runner → server: end of response. */
export interface ResponseEndFrame {
  kind: FrameKind.ResponseEnd;
  id: string;
}

/** Server → runner: abort an in-flight request. */
export interface RequestCancelFrame {
  kind: FrameKind.RequestCancel;
  id: string;
  /** Defaults to `"client_disconnected"`. */
  reason?: string;
}

/** Either direction: tunnel-level keepalive (request half). */
export interface PingFrame {
  kind: FrameKind.Ping;
  ts: number;
}

/** Either direction: keepalive response — echoes the ping's ts. */
export interface PongFrame {
  kind: FrameKind.Pong;
  ts: number;
}

/**
 * Server → runner: open a tunneled WebSocket channel.
 *
 * The runner dispatches its local app at `path` with `queryString` and pumps
 * frames between that endpoint and the server using `chId` for correlation.
 * `chId` is a per-channel id (e.g. `"a1b2c3d4"`), unique within one runner
 * session. `path` is the route on the runner; `queryString` is the URL-encoded
 * query string without the leading `?`.
 */
export interface WsOpenFrame {
  kind: FrameKind.WsOpen;
  chId: string;
  path: string;
  /** Defaults to `""`. */
  queryString?: string;
}

/**
 * Either direction: one WebSocket frame on a channel.
 *
 * `encoding="utf-8"` carries a literal string payload (e.g. a terminal resize
 * JSON message). `encoding="base64"` carries a base64 binary payload (PTY bytes).
 */
export interface WsFrame {
  kind: FrameKind.WsFrame;
  chId: string;
  data: string;
  /** Defaults to `"utf-8"`. */
  encoding?: BodyEncoding;
}

/** Either direction: close a tunneled WebSocket channel. */
export interface WsCloseFrame {
  kind: FrameKind.WsClose;
  chId: string;
  /** Defaults to `1000`. */
  code?: number;
  /** Defaults to `""`. */
  reason?: string;
}

export type Frame =
  | HelloFrame
  | RequestFrame
  | ResponseHeadFrame
  | ResponseBodyFrame
  | ResponseEndFrame
  | RequestCancelFrame
  | PingFrame
  | PongFrame
  | WsOpenFrame
  | WsFrame
  | WsCloseFrame;

// ── Encode ───────────────────────────────────────────────

/**
 * Serialize a frame to its JSON wire form. The output is what goes onto the
 * WebSocket as a text message.
 *
 * @throws TypeError if the frame has an unrecognized `kind`.
 */
export function encodeFrame(frame: Frame): string {
  switch (frame.kind) {
    case FrameKind.Hello:
      return JSON.stringify({
        kind: FrameKind.Hello,
        runner_version: frame.runnerVersion,
        frame_protocol_version: frame.frameProtocolVersion,
        harnesses: [...(frame.harnesses ?? [])],
        envs: [...(frame.envs ?? [])],
        resume_cursors: { ...(frame.resumeCursors ?? {}) },
      });
    case FrameKind.Request:
      return JSON.stringify({
        kind: FrameKind.Request,
        id: frame.id,
        method: frame.method,
        path: frame.path,
        query_string: frame.queryString ?? '',
        headers: (frame.headers ?? []).map((h) => [h[0], h[1]]),
        body: frame.body ?? null,
        encoding: frame.encoding ?? 'utf-8',
        stream: frame.stream ?? false,
      });
    case FrameKind.ResponseHead:
      return JSON.stringify({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status: frame.status,
        headers: (frame.headers ?? []).map((h) => [h[0], h[1]]),
      });
    case FrameKind.ResponseBody:
      return JSON.stringify({
        kind: FrameKind.ResponseBody,
        id: frame.id,
        body: frame.body,
        encoding: frame.encoding ?? 'utf-8',
      });
    case FrameKind.ResponseEnd:
      return JSON.stringify({ kind: FrameKind.ResponseEnd, id: frame.id });
    case FrameKind.RequestCancel:
      return JSON.stringify({
        kind: FrameKind.RequestCancel,
        id: frame.id,
        reason: frame.reason ?? 'client_disconnected',
      });
    case FrameKind.Ping:
      return JSON.stringify({ kind: FrameKind.Ping, ts: frame.ts });
    case FrameKind.Pong:
      return JSON.stringify({ kind: FrameKind.Pong, ts: frame.ts });
    case FrameKind.WsOpen:
      return JSON.stringify({
        kind: FrameKind.WsOpen,
        ch_id: frame.chId,
        path: frame.path,
        query_string: frame.queryString ?? '',
      });
    case FrameKind.WsFrame:
      return JSON.stringify({
        kind: FrameKind.WsFrame,
        ch_id: frame.chId,
        data: frame.data,
        encoding: frame.encoding ?? 'utf-8',
      });
    case FrameKind.WsClose:
      return JSON.stringify({
        kind: FrameKind.WsClose,
        ch_id: frame.chId,
        code: frame.code ?? 1000,
        reason: frame.reason ?? '',
      });
    default: {
      const kind = (frame as { kind?: unknown }).kind;
      throw new TypeError(`unknown frame type: ${String(kind)}`);
    }
  }
}

// ── Decode ───────────────────────────────────────────────

type FrameObject = Record<string, unknown>;

/**
 * Parse a JSON wire frame back into its typed object.
 *
 * @throws Error on malformed JSON, missing `kind`, unknown kind, or missing /
 *   ill-typed required fields for the kind.
 */
export function decodeFrame(text: string): Frame {
  const msg = parseFrameObject(text);
  const kind = parseFrameKind(msg);
  return decodeKnownFrame(kind, msg);
}

/** Parse a JSON frame object, rejecting non-object roots. */
function parseFrameObject(text: string): FrameObject {
  let msg: unknown;
  try {
    msg = JSON.parse(text);
  } catch (exc) {
    const detail = exc instanceof Error ? exc.message : String(exc);
    throw new Error(`frame is not valid JSON: ${detail}`);
  }
  if (typeof msg !== 'object' || msg === null || Array.isArray(msg)) {
    throw new Error(`frame must be a JSON object, got ${describeType(msg)}`);
  }
  return msg as FrameObject;
}

/** Parse the frame `kind` discriminator. */
function parseFrameKind(msg: FrameObject): FrameKind {
  const kind = msg['kind'];
  if (typeof kind !== 'string') {
    throw new Error("frame missing 'kind' field");
  }
  if (!isKnownKind(kind)) {
    throw new Error(`unknown frame kind: ${JSON.stringify(kind)}`);
  }
  return kind;
}

function isKnownKind(kind: string): kind is FrameKind {
  return (Object.values(FrameKind) as string[]).includes(kind);
}

/** Decode a frame with a validated kind. */
function decodeKnownFrame(kind: FrameKind, msg: FrameObject): Frame {
  switch (kind) {
    case FrameKind.Hello:
      return decodeHello(msg);
    case FrameKind.Request:
      return decodeRequest(msg);
    case FrameKind.ResponseHead:
      return decodeResponseHead(msg);
    case FrameKind.ResponseBody:
      return decodeResponseBody(msg);
    case FrameKind.ResponseEnd:
      return { kind: FrameKind.ResponseEnd, id: requiredStr(msg, 'id') };
    case FrameKind.RequestCancel:
      return decodeRequestCancel(msg);
    case FrameKind.Ping:
      return { kind: FrameKind.Ping, ts: requiredInt(msg, 'ts') };
    case FrameKind.Pong:
      return { kind: FrameKind.Pong, ts: requiredInt(msg, 'ts') };
    case FrameKind.WsOpen:
      return decodeWsOpen(msg);
    case FrameKind.WsFrame:
      return decodeWsFrame(msg);
    case FrameKind.WsClose:
      return decodeWsClose(msg);
  }
}

function decodeHello(msg: FrameObject): HelloFrame {
  return {
    kind: FrameKind.Hello,
    runnerVersion: requiredStr(msg, 'runner_version'),
    frameProtocolVersion: requiredInt(msg, 'frame_protocol_version'),
    harnesses: optionalStrList(msg, 'harnesses'),
    envs: optionalStrList(msg, 'envs'),
    resumeCursors: optionalStrMap(msg, 'resume_cursors'),
  };
}

function decodeRequest(msg: FrameObject): RequestFrame {
  return {
    kind: FrameKind.Request,
    id: requiredStr(msg, 'id'),
    method: requiredStr(msg, 'method'),
    path: requiredStr(msg, 'path'),
    queryString: optionalStr(msg, 'query_string', ''),
    headers: optionalHeaders(msg),
    body: optionalBody(msg),
    encoding: parseBodyEncoding(msg, 'encoding'),
    stream: optionalBool(msg, 'stream', false),
  };
}

function decodeResponseHead(msg: FrameObject): ResponseHeadFrame {
  return {
    kind: FrameKind.ResponseHead,
    id: requiredStr(msg, 'id'),
    status: requiredInt(msg, 'status'),
    headers: optionalHeaders(msg),
  };
}

function decodeResponseBody(msg: FrameObject): ResponseBodyFrame {
  return {
    kind: FrameKind.ResponseBody,
    id: requiredStr(msg, 'id'),
    body: requiredStr(msg, 'body'),
    encoding: parseBodyEncoding(msg, 'encoding'),
  };
}

function decodeRequestCancel(msg: FrameObject): RequestCancelFrame {
  return {
    kind: FrameKind.RequestCancel,
    id: requiredStr(msg, 'id'),
    reason: optionalStr(msg, 'reason', 'client_disconnected'),
  };
}

function decodeWsOpen(msg: FrameObject): WsOpenFrame {
  return {
    kind: FrameKind.WsOpen,
    chId: requiredStr(msg, 'ch_id'),
    path: requiredStr(msg, 'path'),
    queryString: optionalStr(msg, 'query_string', ''),
  };
}

function decodeWsFrame(msg: FrameObject): WsFrame {
  return {
    kind: FrameKind.WsFrame,
    chId: requiredStr(msg, 'ch_id'),
    data: requiredStr(msg, 'data'),
    encoding: parseBodyEncoding(msg, 'encoding'),
  };
}

function decodeWsClose(msg: FrameObject): WsCloseFrame {
  return {
    kind: FrameKind.WsClose,
    chId: requiredStr(msg, 'ch_id'),
    code: optionalInt(msg, 'code', 1000),
    reason: optionalStr(msg, 'reason', ''),
  };
}

// ── Field validators ─────────────────────────────────────
//
// Integer fields reject booleans: on the JSON wire a boolean is never a valid
// integer, so the validators exclude `boolean` explicitly.
//
// Integer fields also require a *safe* integer (`Number.isSafeInteger`), i.e.
// magnitude ≤ 2^53 - 1. This pins exact-value parity with an unbounded-integer
// peer: every integer that round-trips through `JSON.parse` without losing
// precision is accepted, and one beyond 2^53 — which `JSON.parse` has already
// silently rounded to the nearest double, so the value on hand is no longer the
// value on the wire — is rejected loudly rather than passed through corrupted.
// `Number.isInteger` would accept that rounded value; `Number.isSafeInteger`
// does not. Every real frame int field (`ts`, `status`, `code`,
// `frame_protocol_version`) is small and well inside this range, so this only
// changes behavior for the corrupted-value case.

function isInteger(val: unknown): val is number {
  return typeof val === 'number' && Number.isSafeInteger(val);
}

function requiredStr(msg: FrameObject, key: string): string {
  const val = msg[key];
  if (typeof val !== 'string') {
    throw new Error(`frame missing required string field: ${JSON.stringify(key)}`);
  }
  return val;
}

function requiredInt(msg: FrameObject, key: string): number {
  const val = msg[key];
  if (!isInteger(val)) {
    throw new Error(`frame missing required int field: ${JSON.stringify(key)}`);
  }
  return val;
}

function optionalStr(msg: FrameObject, key: string, fallback: string): string {
  if (!(key in msg) || msg[key] === undefined) {
    return fallback;
  }
  const val = msg[key];
  if (typeof val !== 'string') {
    throw new Error(`frame field must be a string: ${JSON.stringify(key)}`);
  }
  return val;
}

/**
 * Validate an optional `encoding` field into the {@link BodyEncoding} literal
 * union (default `'utf-8'`), rejecting any other string. Membership is CHECKED,
 * not cast: a decoded frame must actually satisfy its declared type, and an
 * unknown encoding must fail loudly at the frame boundary (like an unknown
 * `kind`) rather than being silently treated as utf-8 by a consumer's two-arm
 * branch and corrupting the body mid-stream.
 */
function parseBodyEncoding(msg: FrameObject, key: string): BodyEncoding {
  const val = optionalStr(msg, key, 'utf-8');
  if (val !== 'utf-8' && val !== 'base64') {
    throw new Error(`frame field ${JSON.stringify(key)} must be "utf-8" or "base64"`);
  }
  return val;
}

function optionalBool(msg: FrameObject, key: string, fallback: boolean): boolean {
  if (!(key in msg) || msg[key] === undefined) {
    return fallback;
  }
  const val = msg[key];
  if (typeof val !== 'boolean') {
    throw new Error(`frame field must be a boolean: ${JSON.stringify(key)}`);
  }
  return val;
}

function optionalInt(msg: FrameObject, key: string, fallback: number): number {
  if (!(key in msg) || msg[key] === undefined) {
    return fallback;
  }
  const val = msg[key];
  if (!isInteger(val)) {
    throw new Error(`frame field must be an integer: ${JSON.stringify(key)}`);
  }
  return val;
}

function optionalBody(msg: FrameObject): string | null {
  const val = msg['body'];
  if (val === undefined || val === null) {
    return null;
  }
  if (typeof val !== 'string') {
    throw new Error("frame field must be a string or null: 'body'");
  }
  return val;
}

function optionalStrList(msg: FrameObject, key: string): string[] {
  if (!(key in msg) || msg[key] === undefined) {
    return [];
  }
  const val = msg[key];
  if (!Array.isArray(val) || !val.every((item) => typeof item === 'string')) {
    throw new Error(`frame field must be a list of strings: ${JSON.stringify(key)}`);
  }
  return [...(val as string[])];
}

function optionalStrMap(msg: FrameObject, key: string): Record<string, string> {
  if (!(key in msg) || msg[key] === undefined) {
    return {};
  }
  const val = msg[key];
  if (val === null || typeof val !== 'object' || Array.isArray(val)) {
    throw new Error(`frame field must be a string→string map: ${JSON.stringify(key)}`);
  }
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(val as Record<string, unknown>)) {
    if (typeof v !== 'string') {
      throw new Error(`frame field must be a string→string map: ${JSON.stringify(key)}`);
    }
    out[k] = v;
  }
  return out;
}

function optionalHeaders(msg: FrameObject): HeaderPair[] {
  if (!('headers' in msg) || msg['headers'] === undefined) {
    return [];
  }
  const val = msg['headers'];
  if (!Array.isArray(val)) {
    throw new Error("frame field must be a list of header pairs: 'headers'");
  }
  const headers: HeaderPair[] = [];
  for (const item of val) {
    if (
      !Array.isArray(item) ||
      item.length !== 2 ||
      !item.every((part) => typeof part === 'string')
    ) {
      throw new Error("frame field must be a list of header pairs: 'headers'");
    }
    headers.push([item[0] as string, item[1] as string]);
  }
  return headers;
}

function describeType(val: unknown): string {
  if (val === null) return 'null';
  if (Array.isArray(val)) return 'array';
  return typeof val;
}

// ── Body encoding helpers ────────────────────────────────

const TEXT_CONTENT_TYPES = [
  'text/',
  'application/json',
  'application/jsonl',
  'application/x-ndjson',
  'text/event-stream',
];

/**
 * Decide whether a body of this content-type can be utf-8-encoded.
 *
 * True for the standard text-shaped types. False otherwise — those bodies must
 * be base64-encoded.
 */
export function isTextContentType(contentType: string): boolean {
  const ct = contentType.toLowerCase();
  return TEXT_CONTENT_TYPES.some((prefix) => ct.startsWith(prefix));
}

/**
 * Return `[encodedBody, encoding]` for a body+content-type pair. Picks utf-8
 * inline for text-shaped content, base64 otherwise.
 */
export function encodeBody(body: Uint8Array, contentType: string): [string, BodyEncoding] {
  if (isTextContentType(contentType)) {
    // `fatal: false` (the default) replaces invalid byte sequences with the
    // U+FFFD replacement character rather than throwing.
    return [new TextDecoder('utf-8').decode(body), 'utf-8'];
  }
  return [base64Encode(body), 'base64'];
}

/** Decode a body string back to bytes per its declared encoding. */
export function decodeBody(body: string, encoding: string): Uint8Array {
  if (encoding === 'utf-8') {
    return new TextEncoder().encode(body);
  }
  if (encoding === 'base64') {
    return base64Decode(body);
  }
  throw new Error(`unknown body encoding: ${JSON.stringify(encoding)}`);
}

function base64Encode(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('base64');
}

function base64Decode(text: string): Uint8Array {
  return new Uint8Array(Buffer.from(text, 'base64'));
}

// ── Size limit ───────────────────────────────────────────

/** Max bytes for a single tunnel WebSocket message (100 MiB). */
export const TUNNEL_MAX_MESSAGE_BYTES = 100 * 1024 * 1024;
