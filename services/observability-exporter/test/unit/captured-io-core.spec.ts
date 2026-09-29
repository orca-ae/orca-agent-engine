// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  captureJson,
  captureTextContent,
  captureToolName,
  parseCapturedValue,
  parseProjectedIo,
} from '../../src/captured-io.js';
import { isLangfuseHttpJsonContext } from '../../src/delivery-capabilities.js';
import { parseProjectedTrace } from '../../src/canonical-validation.js';
import { parseKafkaProjectedTrace } from '../../src/kafka-validation.js';
import { completedProjectedTrace } from '../support/events.js';
import {
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import {
  IO_VERSION,
  IO_PROJECTED_TRACE_SCHEMA_VERSION,
  type PinnedDeliveryContext,
} from '../../src/types.js';

describe('raw capture core contract', () => {
  function traceWithOutput(output: { json: string } | { omitted: 'too_large' }) {
    const trace = completedProjectedTrace();
    trace.schemaVersion = IO_PROJECTED_TRACE_SCHEMA_VERSION;
    for (const span of [trace.root, ...trace.spans])
      span.metadata['orca.projection.schema_version'] = IO_PROJECTED_TRACE_SCHEMA_VERSION;
    trace.root.io = { version: IO_VERSION, output, outputScope: 'turn_messages' };
    return trace;
  }

  it.each(
    [0, {}, 'single message', ['message', 0], Array(1025).fill('')].map((value) => ({ value })),
  )(
    'rejects invalid turn_messages payloads at canonical and Kafka outbox boundaries: %j',
    ({ value }) => {
      const trace = traceWithOutput({ json: JSON.stringify(value) });
      expect(() => parseProjectedIo(trace.root.io)).toThrow();
      expect(() => parseProjectedTrace(trace)).toThrow();
      expect(() => parseKafkaProjectedTrace(trace)).toThrow();
    },
  );

  it.each([{ json: JSON.stringify(Array(1024).fill('')) }, { omitted: 'too_large' as const }])(
    'admits bounded turn message arrays and explicitly omitted outputs: %j',
    (output) => {
      const trace = traceWithOutput(output);
      expect(parseProjectedTrace(trace).root.io).toEqual(trace.root.io);
      expect(parseKafkaProjectedTrace(trace).root.io).toEqual(trace.root.io);
    },
  );

  it('does not reorder admitted object properties', () => {
    expect(captureJson({ z: 'first', a: { password: 'plain value' } })).toEqual({
      json: '{"z":"first","a":{"password":"plain value"}}',
    });
  });

  it('preserves admitted JSON values and credential-looking strings on capture and replay', () => {
    const value = JSON.parse(
      '{"password":"sk-canary","api_key":"Bearer token","headers":{"Authorization":"Basic abc"},"env":{"SECRET":"value"},"thinking":"data","type":"image","__proto__":{"x":1},"constructor":"ordinary data"}',
    );
    const captured = captureJson(value);
    expect(captured.omitted).toBeUndefined();
    expect(JSON.parse(captured.json!)).toEqual(value);
    expect(parseCapturedValue(captured)).toEqual(captured);
    expect(parseProjectedIo({ version: IO_VERSION, output: captured }).output).toEqual(captured);
  });

  it('does not invoke getters or toJSON', () => {
    const getter = vi.fn(() => 'secret');
    const toJSON = vi.fn(() => 'secret');
    expect(
      captureJson(Object.defineProperty({}, 'password', { enumerable: true, get: getter })),
    ).toEqual({ omitted: 'unsupported' });
    expect(captureJson({ toJSON })).toEqual({ omitted: 'unsupported' });
    const blocks = Object.defineProperty([], '0', { get: getter });
    expect(captureTextContent(blocks)).toEqual({ omitted: 'unsupported' });
    expect(getter).not.toHaveBeenCalled();
    expect(toJSON).not.toHaveBeenCalled();
  });

  it.each([false, true])('does not execute a hidden serializer or getter (array=%s)', (array) => {
    const serialize = vi.fn(() => 'not-the-admitted-value');
    const getter = vi.fn(() => undefined);
    const value = array ? [1] : { z: 1 };
    Object.defineProperty(value, 'toJSON', { configurable: true, value: serialize });
    expect(captureJson(value)).toEqual({ json: array ? '[1]' : '{"z":1}' });
    expect(serialize).not.toHaveBeenCalled();
    Object.defineProperty(value, 'toJSON', { configurable: true, get: getter });
    expect(captureJson(value)).toEqual({ json: array ? '[1]' : '{"z":1}' });
    expect(getter).not.toHaveBeenCalled();
  });

  it('does not allow a hidden serializer to replace validated input with unscanned content', () => {
    const serialize = vi.fn(() => Array(2050).fill(0));
    const value = Object.defineProperty({ z: 1 }, 'toJSON', { value: serialize });
    const captured = captureJson(value);
    expect(captured).toEqual({ json: '{"z":1}' });
    expect(parseCapturedValue(captured)).toEqual(captured);
    expect(serialize).not.toHaveBeenCalled();
  });

  it('selects root text without masking, and preserves display-safe tool names', () => {
    expect(
      captureTextContent([
        { type: 'text', text: 'Bearer abc' },
        { type: 'thinking', text: 'not root text' },
      ]),
    ).toEqual({ json: '"Bearer abc"', omitted: 'partial' });
    expect(captureToolName('工具 lookup sk-canary')).toBe('工具 lookup sk-canary');
    expect(captureToolName('tool\nname')).toBeUndefined();
  });

  it('accepts JSON grammar without rewriting bytes and rejects invalid envelopes', () => {
    const value = { json: ' { "password" : "sk-canary" } ' };
    expect(parseCapturedValue(value)).toEqual(value);
    expect(() => parseCapturedValue({ json: '1e999' })).toThrow();
    expect(() => parseCapturedValue({ json: '{}', extra: true })).toThrow();
    expect(captureJson('x'.repeat(8192))).toEqual({ omitted: 'too_large' });
  });

  it('never authorizes legacy redacted mode and keeps raw downgrades sticky', () => {
    const context = {
      adapterType: 'otlp_http',
      endpointKind: 'traces_endpoint',
      endpointClass: 'public',
      endpointUrl: 'https://example.com/api/public/otel/v1/traces',
      semanticProfile: 'langfuse',
      protocol: 'http/json',
      compression: 'none',
      sampleRate: 1,
      captureMode: 'redacted_io',
    } as PinnedDeliveryContext;
    expect(isLangfuseHttpJsonContext(context, true)).toBe(false);
    expect(isLangfuseHttpJsonContext({ ...context, captureMode: 'raw_io' }, true)).toBe(true);
    expect(() =>
      parseCanonicalProjectionState({
        version: 4,
        captureMode: 'redacted_io',
        pendingInputs: [],
        activeTurn: null,
      }),
    ).toThrow();
    const raw = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      [],
      new Set(),
      undefined,
      'raw_io',
    ).state;
    const lower = reduceCanonicalEventBatch(raw, [], new Set(), undefined, 'metadata_only').state;
    expect(
      reduceCanonicalEventBatch(lower, [], new Set(), undefined, 'raw_io').state.captureMode,
    ).toBe('metadata_only');
  });
});
