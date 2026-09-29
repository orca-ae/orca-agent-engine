// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  captureJson,
  captureTextContent,
  captureToolName,
  parseCapturedValue,
  parseProjectedIo,
  capturedIoBytes,
  MAX_CAPTURED_VALUE_BYTES,
  MAX_CAPTURED_IO_BYTES_PER_TURN,
  MAX_PENDING_CAPTURED_IO_BYTES,
  IO_VERSION,
} from '../../src/captured-io.js';
import { parseProjectedTrace, parsePinnedDeliveryContext } from '../../src/canonical-validation.js';
import { IO_PROJECTED_TRACE_SCHEMA_VERSION } from '../../src/types.js';
import { completedProjectedTrace } from '../support/events.js';
import { toolSpan } from '../support/tools.js';
import { deterministicChildSpanId } from '../../src/ids.js';

describe('bounded raw I/O foundation', () => {
  it('publishes the fixed byte budgets', () => {
    expect(MAX_CAPTURED_VALUE_BYTES).toBe(8192);
    expect(MAX_CAPTURED_IO_BYTES_PER_TURN).toBe(262144);
    expect(MAX_PENDING_CAPTURED_IO_BYTES).toBe(262144);
  });

  it('is deterministic, preserves JSON primitives and handles Unicode byte sizes', () => {
    const value = { z: [null, false, 2], a: '你好🙂' };
    expect(JSON.parse(captureJson(value).json!)).toEqual(value);
    expect(captureJson(value)).toEqual(captureJson(value));
    expect(captureJson('界'.repeat(2730)).json).toBeDefined();
    expect(captureJson('界'.repeat(2731))).toEqual({ omitted: 'too_large' });
    expect(captureJson('')).toEqual({ json: '""' });
  });

  it('preserves admitted sensitive-looking keys, nested content and strings exactly', () => {
    const value = {
      api_key: 'sk-SYNTHETIC_CANARY',
      password: 'SYNTHETIC_PASSWORD',
      headers: { Authorization: 'Bearer SYNTHETIC_TOKEN', Cookie: 'session=SYNTHETIC' },
      env: { ACCESS_TOKEN: 'SYNTHETIC_TOKEN' },
      nested: { thinking: 'retained', content: [{ type: 'thinking', text: 'retained' }] },
    };
    const captured = captureJson(value);
    expect(JSON.parse(captured.json!)).toEqual(value);
    expect(captured.omitted).toBeUndefined();
    expect(parseCapturedValue(captured)).toEqual(captured);
    const text = JSON.stringify(value);
    expect(captureTextContent(text)).toEqual({ json: JSON.stringify(text) });
    expect(captureTextContent([{ type: 'text', text }])).toEqual({ json: JSON.stringify(text) });
  });

  it('fails closed on unsupported values without invoking accessors or toJSON', () => {
    let called = false;
    const accessor = {
      get content() {
        called = true;
        return 'CANARY_SECRET';
      },
    };
    for (const value of [
      undefined,
      1n,
      NaN,
      Infinity,
      new Date(),
      () => {},
      accessor,
      {
        toJSON() {
          called = true;
        },
      },
    ]) {
      expect(captureJson(value)).toEqual({ omitted: 'unsupported' });
    }
    expect(called).toBe(false);
    const cycle: unknown[] = [];
    cycle.push(cycle);
    expect(captureJson(cycle)).toEqual({ omitted: 'unsupported' });
  });

  it('omits oversized whole values instead of emitting partial JSON', () => {
    for (const value of [
      'x'.repeat(65537) + 'sk-CANARY_SECRET',
      'x'.repeat(8192),
      Array(2049).fill(1),
      { ['k'.repeat(257)]: 'safe' },
    ]) {
      expect(captureJson(value)).toEqual({ omitted: 'too_large' });
    }
    let deep: unknown = 'safe';
    for (let i = 0; i < 18; i++) deep = { child: deep };
    expect(captureJson(deep)).toEqual({ omitted: 'too_large' });
    expect(captureJson(Array(10).fill('x'.repeat(30000)))).toEqual({ omitted: 'too_large' });
  });

  it('extracts only text, drops thinking/signatures and marks mixed content partial', () => {
    expect(captureTextContent('hello')).toEqual({ json: '"hello"' });
    expect(
      captureTextContent([
        { type: 'text', text: 'hello' },
        { type: 'text', text: 'world' },
      ]),
    ).toEqual({ json: '"hello\nworld"'.replace('\n', '\\n') });
    expect(
      captureTextContent([
        { type: 'thinking', thinking: 'CANARY_SECRET' },
        { type: 'text', text: 'safe', signature: 'CANARY_SECRET' },
      ]),
    ).toEqual({ json: '"safe"', omitted: 'partial' });
    for (const content of [
      undefined,
      { text: 'unknown' },
      [],
      [{ type: 'image', source: 'CANARY_SECRET' }],
      [{ type: 'thinking', text: 'CANARY_SECRET' }],
    ]) {
      expect(captureTextContent(content)).toEqual({ omitted: 'unsupported' });
    }
  });

  it('validates tool name grammar without token filtering', () => {
    expect(captureToolName('mcp__weather.get')).toBe('mcp__weather.get');
    expect(captureToolName('sk-CANARY_SECRET')).toBe('sk-CANARY_SECRET');
    expect(captureToolName('Bearer secret')).toBe('Bearer secret');
    for (const value of [null, '', 'bad\nname', 'x'.repeat(257)])
      expect(captureToolName(value)).toBeUndefined();
  });

  it('restores identical JSON bytes without normalizing or enriching', () => {
    const spaced = { json: ' {"password": "SYNTHETIC"} ' };
    expect(parseCapturedValue(spaced)).toEqual(spaced);
    const value = {
      json: '{"z":1,"a":"free text"}',
      omitted: 'partial' as const,
      truncated: true as const,
    };
    expect(parseCapturedValue(value)).toEqual(value);
    const io = {
      version: IO_VERSION,
      input: value,
      output: { omitted: 'unavailable' as const },
      outputScope: 'turn_messages' as const,
    };
    expect(parseProjectedIo(io)).toEqual(io);
    expect(capturedIoBytes(io)).toBe(Buffer.byteLength(JSON.stringify(io)));
  });

  it.each([
    {},
    { json: 'not JSON' },
    { json: '1e309' },
    { json: '"safe"', raw: 'CANARY_SECRET' },
    { json: '"safe"', omitted: 'budget' },
    { truncated: true, omitted: 'too_large' },
    { omitted: 'unknown' },
    { json: undefined },
    { json: '"safe"', truncated: false },
    { json: JSON.stringify('界'.repeat(3000)) },
  ])('rejects malformed or smuggled persisted value %j', (value) => {
    expect(() => parseCapturedValue(value)).toThrow();
  });

  it.each([
    { version: 'unknown' },
    { version: 'orca.observability.redacted-io.v1' },
    { version: IO_VERSION, raw: 'CANARY_SECRET' },
    { version: IO_VERSION, toolName: undefined },
    { version: IO_VERSION, outputScope: 'final' },
    { version: IO_VERSION, outputScope: 'turn_messages' },
    { version: IO_VERSION, input: undefined },
  ])('rejects invalid I/O envelopes %j', (value) => {
    expect(() => parseProjectedIo(value)).toThrow();
  });
});

describe('canonical v1/v2 boundary', () => {
  function contentTrace() {
    const trace = completedProjectedTrace();
    trace.schemaVersion = IO_PROJECTED_TRACE_SCHEMA_VERSION;
    for (const span of [trace.root, ...trace.spans])
      span.metadata['orca.projection.schema_version'] = trace.schemaVersion;
    trace.root.io = { version: IO_VERSION, input: captureTextContent('safe') };
    return trace;
  }

  it('keeps metadata-only v1 output byte-for-byte unchanged', () => {
    const trace = completedProjectedTrace();
    expect(parseProjectedTrace(trace)).toEqual(trace);
    expect(JSON.stringify(parseProjectedTrace(trace))).not.toContain('"io"');
  });

  it('restores v2 root I/O and requires a consistent trace-wide metadata schema', () => {
    const trace = contentTrace();
    expect(parseProjectedTrace(trace)).toEqual(trace);
    trace.root.metadata['orca.projection.schema_version'] = 'orca.observability.projected-trace.v1';
    expect(() => parseProjectedTrace(trace)).toThrow();
  });

  it.each([{}, [42], Array.from({ length: 1025 }, () => 'x'), 'answer'])(
    'rejects root JSON output without its declared turn-message scope: %#',
    (output) => {
      const trace = contentTrace();
      trace.root.io!.output = { json: JSON.stringify(output) };
      expect(() => parseProjectedTrace(trace)).toThrow();
    },
  );

  it('requires root scope without restricting arbitrary tool JSON output', () => {
    const trace = contentTrace();
    trace.root.io!.output = { json: '["answer"]' };
    trace.root.io!.outputScope = 'turn_messages';
    const tool = toolSpan(trace);
    tool.io = { version: IO_VERSION, output: { json: '{"password":"original"}' } };
    trace.spans.push(tool);
    expect(parseProjectedTrace(trace)).toEqual(trace);
  });

  it('rejects I/O on v1 and on model summaries in v2', () => {
    const legacy = completedProjectedTrace();
    legacy.root.io = { version: IO_VERSION, input: captureJson('safe') };
    expect(() => parseProjectedTrace(legacy)).toThrow();
    const trace = contentTrace();
    trace.spans[0]!.io = trace.root.io!;
    expect(() => parseProjectedTrace(trace)).toThrow();
  });

  it('rejects content smuggling outside the v2 I/O envelope', () => {
    const trace = contentTrace();
    expect(() => parseProjectedTrace({ ...trace, input: 'CANARY_SECRET' })).toThrow();
    expect(() =>
      parseProjectedTrace({ ...trace, root: { ...trace.root, output: 'CANARY_SECRET' } }),
    ).toThrow();
    trace.root.io!.toolName = 'tool';
    expect(() => parseProjectedTrace(trace)).toThrow();
  });

  it.each(['root', 'tool'] as const)('rejects an explicitly undefined %s I/O property', (role) => {
    const trace = contentTrace();
    const tool = toolSpan(trace);
    trace.spans.push(tool);
    Object.assign(role === 'root' ? trace.root : tool, { io: undefined });
    expect(() => parseProjectedTrace(trace)).toThrow();
  });

  it('does not add defaults, reorder raw I/O or mutate the supplied trace while validating it', () => {
    const trace = contentTrace();
    trace.root.io!.input = { json: ' { "z": 1, "password": "raw" } ' };
    const tool = toolSpan(trace);
    tool.io = { version: IO_VERSION, output: { json: ' [1, {"x":"界"}] ' } };
    trace.spans.push(tool);
    const original = JSON.stringify(trace);
    const parsed = parseProjectedTrace(trace);
    expect(parsed.root.io?.input?.json).toBe(' { "z": 1, "password": "raw" } ');
    expect(parsed.spans.at(-1)?.io?.output?.json).toBe(' [1, {"x":"界"}] ');
    expect(parsed.root.io?.output).toBeUndefined();
    expect(JSON.stringify(trace)).toBe(original);
  });

  it('accepts persisted raw_io context independently of runtime capabilities', () => {
    const context = {
      organizationId: 'org_test',
      bindingId: 'aob_test',
      bindingVersion: 1,
      adapterType: 'otlp_http',
      endpointKind: 'traces_endpoint',
      endpointClass: 'public',
      endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
      semanticProfile: 'langfuse',
      protocol: 'http/json',
      compression: 'none',
      timeoutMs: 1000,
      captureMode: 'raw_io',
      sampleRate: 1,
      configSchemaVersion: 1,
    };
    expect(parsePinnedDeliveryContext(context)).toEqual(context);
    expect(() => parsePinnedDeliveryContext({ ...context, captureMode: 'raw' })).toThrow();
  });

  it('rejects aggregate I/O above the turn budget even when each value is valid', () => {
    const trace = contentTrace();
    const value = captureJson('x'.repeat(8000));
    for (let index = 0; index < 17; index++) {
      const span = toolSpan(trace);
      span.sourceEventId = 'evt_tool_budget_' + index;
      span.spanId = deterministicChildSpanId(trace.traceId, 'tool', '', span.sourceEventId);
      span.metadata['orca.source.start_event_id'] = span.sourceEventId;
      span.io = { version: IO_VERSION, input: value, output: value };
      trace.spans.push(span);
    }
    expect(
      trace.spans.reduce((sum, span) => sum + (span.io ? capturedIoBytes(span.io) : 0), 0),
    ).toBeGreaterThan(MAX_CAPTURED_IO_BYTES_PER_TURN);
    expect(() => parseProjectedTrace(trace)).toThrow();
    trace.spans.pop();
    expect(parseProjectedTrace(trace)).toEqual(trace);
  });
});
