// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { deliverKafkaTrace, parsePinnedDeliveryContext } from '../../src/kafka-delivery.js';
import { OtlpEgressPolicyError } from '../../src/egress.js';
import {
  RegistryResolverHttpError,
  RegistryResolverResponseError,
  RegistryResolverScopeError,
  type RegistryObservabilitySecret,
} from '../../src/registry-client.js';
import type { PinnedDeliveryContext } from '../../src/types.js';
import { IO_PROJECTED_TRACE_SCHEMA_VERSION, IO_VERSION } from '../../src/types.js';
import { completedProjectedTrace } from '../support/events.js';
import * as otlpMapper from '../../src/otlp-json.js';
import { parseKafkaDeliveryRecord } from '../../src/kafka-state.js';
import { initialCanonicalProjectionState, reduceCanonicalEventBatch } from '../../src/projector.js';
import { IO_CANARY, rawPrimaryTurnEvents } from '../support/raw-events.js';

const context: PinnedDeliveryContext = {
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
  captureMode: 'metadata_only',
  sampleRate: 1,
  configSchemaVersion: 1,
};

function setup() {
  const secret: RegistryObservabilitySecret = {
    bindingId: context.bindingId,
    bindingVersion: 1,
    effectiveCaptureMode: 'metadata_only',
    auth: { type: 'basic', username: 'public-key', password: 'secret-key' },
  };
  const resolveSecret = vi.fn(async () => secret);
  const otlpFetchImpl = vi.fn<typeof fetch>(async () => Response.json({}));
  const record = { trace: completedProjectedTrace(), deliveryContext: { ...context } };
  const options = {
    registryClient: { resolveSecret },
    registryRequestTimeoutMs: 1000,
    otlpFetchImpl,
  };
  return { secret, resolveSecret, otlpFetchImpl, record, options };
}

function rawTrace() {
  const trace = completedProjectedTrace();
  trace.schemaVersion = IO_PROJECTED_TRACE_SCHEMA_VERSION;
  for (const span of [trace.root, ...trace.spans]) {
    span.metadata['orca.projection.schema_version'] = trace.schemaVersion;
  }
  trace.root.io = { version: IO_VERSION, input: { json: JSON.stringify('safe input') } };
  return trace;
}

describe('deliverKafkaTrace', () => {
  it('sends original admitted raw JSON including synthetic sensitive fields and nested thinking', async () => {
    const s = setup();
    s.record.deliveryContext.captureMode = 'raw_io';
    s.secret.effectiveCaptureMode = 'raw_io';
    s.record.trace = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      rawPrimaryTurnEvents(),
      new Set(),
      undefined,
      'raw_io',
    ).completedTraces[0]!;
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'accepted',
    });
    const body = JSON.parse(s.otlpFetchImpl.mock.calls[0]![1]!.body as string);
    const spans = body.resourceSpans[0].scopeSpans[0].spans;
    const tool = spans.find((span: { name: string }) => span.name === 'read_build');
    const attr = (name: string) =>
      tool.attributes.find((a: { key: string }) => a.key === name).value.stringValue;
    expect(JSON.parse(attr('langfuse.observation.input'))).toEqual({
      build: 42,
      api_key: IO_CANARY,
      password: IO_CANARY,
      headers: { Authorization: 'Bearer ' + IO_CANARY },
      env: { ACCESS_TOKEN: IO_CANARY },
      thinking: 'tool-owned-thinking-field',
    });
    expect(JSON.parse(attr('langfuse.observation.output'))).toEqual([
      { type: 'text', text: 'Unit tests failed.' },
      { type: 'thinking', text: IO_CANARY },
    ]);
    expect(JSON.stringify(body)).not.toMatch(/unfinished-private-delta|private-thinking-canary/);
  });

  it.each(['pin', 'secret'] as const)(
    'legacy redacted_io %s cannot authorize a raw trace',
    async (authority) => {
      const s = setup();
      s.record.trace = rawTrace();
      s.record.deliveryContext.captureMode = authority === 'pin' ? 'redacted_io' : 'raw_io';
      s.secret.effectiveCaptureMode = authority === 'secret' ? 'redacted_io' : 'raw_io';
      expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
        kind: 'terminal',
        reason: 'capture_mode_mismatch',
      });
      expect(s.otlpFetchImpl).not.toHaveBeenCalled();
    },
  );

  it.each([false, true])(
    'passes only persisted attribution to the mapper across retry/restart (labels=%s)',
    async (labels) => {
      const s = setup();
      if (labels)
        Object.assign(s.record.deliveryContext, {
          agentId: 'agt_pinned',
          agentVersion: 7,
          harness: 'claude_code',
          harnessMode: 'colocated',
          environment: 'prod',
          release: 'v1.2.3',
        });
      const persisted = JSON.stringify({ version: 1, ...s.record });
      const resolveContext = vi.fn(async () => ({
        status: 'enabled',
        deliveryContext: {
          ...context,
          agentVersion: 99,
          environment: 'new-environment',
          release: 'new-release',
        },
      }));
      const registryClient = { resolveSecret: s.resolveSecret, resolveContext };
      const mapper = vi.spyOn(otlpMapper, 'encodeLangfuseOtlpJson');
      try {
        for (let restart = 0; restart < 2; restart++) {
          s.otlpFetchImpl.mockResolvedValueOnce(new Response(null, { status: 401 }));
          const restored = parseKafkaDeliveryRecord(JSON.parse(persisted));
          expect(
            await deliverKafkaTrace(restored, {
              ...s.options,
              registryClient,
            }),
          ).toEqual({ kind: 'terminal', reason: 'accepted' });
          expect(mapper).toHaveBeenLastCalledWith(restored.trace, s.record.deliveryContext);
        }
        expect(resolveContext).not.toHaveBeenCalled();
        const bodies = s.otlpFetchImpl.mock.calls.map(([, init]) => init!.body);
        expect(bodies).toHaveLength(4);
        expect(new Set(bodies).size).toBe(1);
        expect(String(bodies[0])).not.toContain('new-release');
        const spans = JSON.parse(bodies[0] as string).resourceSpans[0].scopeSpans[0].spans;
        for (const span of spans) {
          const attributes = new Map(
            span.attributes.map((attribute: { key: string; value: unknown }) => [
              attribute.key,
              attribute.value,
            ]),
          );
          expect(attributes.get('langfuse.release')).toEqual(
            labels ? { stringValue: 'v1.2.3' } : undefined,
          );
          expect(attributes.get('langfuse.observation.metadata.orca.agent.version')).toEqual(
            labels ? { intValue: '7' } : undefined,
          );
        }
      } finally {
        mapper.mockRestore();
      }
    },
  );

  it('delivers metadata under a raw ceiling, including credential refresh', async () => {
    const s = setup();
    s.secret.effectiveCaptureMode = 'raw_io';
    s.otlpFetchImpl.mockResolvedValueOnce(new Response(null, { status: 401 }));
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'accepted',
    });
    expect(s.otlpFetchImpl).toHaveBeenCalledTimes(2);
  });

  it.each(['metadata_only', 'raw_io'] as const)(
    'requires a raw pin for v2 even under a %s secret ceiling',
    async (effectiveCaptureMode) => {
      const s = setup();
      s.record.trace = rawTrace();
      s.secret.effectiveCaptureMode = effectiveCaptureMode;
      expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
        kind: 'terminal',
        reason: 'capture_mode_mismatch',
      });
      expect(s.otlpFetchImpl).not.toHaveBeenCalled();
    },
  );

  it('delivers persisted v2 with fresh raw authority and never rewrites it on downgrade', async () => {
    const s = setup();
    s.record.deliveryContext.captureMode = 'raw_io';
    s.record.trace = rawTrace();
    s.secret.effectiveCaptureMode = 'raw_io';
    const before = JSON.stringify(s.record);
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'accepted',
    });
    s.secret.effectiveCaptureMode = 'metadata_only';
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'capture_mode_mismatch',
    });
    expect(s.otlpFetchImpl).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(s.record)).toBe(before);
  });

  it.each([401, 403])('suppresses v2 if authority tightens on HTTP %s refresh', async (status) => {
    const s = setup();
    s.record.deliveryContext.captureMode = 'raw_io';
    s.record.trace = rawTrace();
    s.resolveSecret.mockResolvedValueOnce({ ...s.secret, effectiveCaptureMode: 'raw_io' });
    s.otlpFetchImpl.mockResolvedValueOnce(new Response(null, { status }));
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'capture_mode_mismatch',
    });
    expect(s.resolveSecret).toHaveBeenCalledTimes(2);
    expect(s.otlpFetchImpl).toHaveBeenCalledTimes(1);
  });

  it('resolves fresh scoped credentials and sends metadata-only OTLP with redirects disabled', async () => {
    const s = setup();
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'accepted',
    });
    expect(s.resolveSecret).toHaveBeenCalledWith({
      workspaceId: s.record.trace.workspaceId,
      sessionId: s.record.trace.sessionId,
      signal: expect.any(AbortSignal),
    });
    const [endpoint, init] = s.otlpFetchImpl.mock.calls[0]!;
    expect(endpoint).toBe(context.endpointUrl);
    expect(init).toMatchObject({
      method: 'POST',
      redirect: 'manual',
      headers: {
        authorization: 'Basic ' + Buffer.from('public-key:secret-key').toString('base64'),
        'content-type': 'application/json',
      },
    });
    expect(JSON.parse(init!.body as string)).toHaveProperty('resourceSpans');
    expect(init!.body).not.toContain('secret-key');
  });

  it.each([401, 403])('refreshes credentials exactly once after HTTP %s', async (status) => {
    const s = setup();
    s.otlpFetchImpl.mockResolvedValueOnce(new Response(null, { status }));
    s.resolveSecret.mockResolvedValueOnce(s.secret).mockResolvedValueOnce({
      ...s.secret,
      auth: { type: 'basic', username: 'rotated', password: 'rotated-secret' },
    });
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'accepted',
    });
    expect(s.resolveSecret).toHaveBeenCalledTimes(2);
    expect(s.otlpFetchImpl).toHaveBeenCalledTimes(2);
    expect(new Headers(s.otlpFetchImpl.mock.calls[1]![1]!.headers).get('authorization')).toBe(
      'Basic ' + Buffer.from('rotated:rotated-secret').toString('base64'),
    );
  });

  it('stops after a second credential rejection', async () => {
    const s = setup();
    s.otlpFetchImpl.mockImplementation(async () => new Response(null, { status: 401 }));
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'credential_rejected',
    });
    expect(s.resolveSecret).toHaveBeenCalledTimes(2);
  });

  it.each([
    [{ bindingId: 'other' }, 'binding_mismatch'],
    [{ bindingVersion: 2 }, 'binding_mismatch'],
    [{ auth: { type: 'unsupported' } }, 'unsupported_credential'],
  ] as const)('rechecks authority on refresh: %s', async (change, reason) => {
    const s = setup();
    s.otlpFetchImpl.mockResolvedValueOnce(new Response(null, { status: 403 }));
    s.resolveSecret
      .mockResolvedValueOnce(s.secret)
      .mockResolvedValueOnce({ ...s.secret, ...change });
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({ kind: 'terminal', reason });
    expect(s.otlpFetchImpl).toHaveBeenCalledTimes(1);
  });

  it.each([429, 502, 503, 504])('retries OTLP HTTP %s with bounded Retry-After', async (status) => {
    const s = setup();
    s.otlpFetchImpl.mockResolvedValue(
      new Response(null, { status, headers: { 'retry-after': '999999' } }),
    );
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'retry',
      retryAfterMs: 3600000,
    });
  });

  it.each([204, 301, 400, 408, 425, 500, 501])(
    'does not retry permanent OTLP HTTP %s',
    async (status) => {
      const s = setup();
      s.otlpFetchImpl.mockResolvedValue(new Response(null, { status }));
      expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
        kind: 'terminal',
        reason: 'permanent_http_status',
      });
    },
  );

  it.each([401, 403, 408, 425, 429, 500, 599])(
    'retries registry HTTP %s without egress',
    async (status) => {
      const s = setup();
      s.resolveSecret.mockRejectedValue(new RegistryResolverHttpError('secret', status));
      expect(await deliverKafkaTrace(s.record, s.options)).toEqual({ kind: 'retry' });
      expect(s.otlpFetchImpl).not.toHaveBeenCalled();
    },
  );

  it.each([
    [new RegistryResolverResponseError('secret'), { kind: 'retry' }],
    [new Error('token unavailable'), { kind: 'retry' }],
    [new RegistryResolverHttpError('secret', 404), { kind: 'terminal', reason: 'registry_denied' }],
    [new RegistryResolverScopeError(), { kind: 'terminal', reason: 'registry_scope_invalid' }],
  ])('classifies registry errors safely', async (error, expected) => {
    const s = setup();
    s.resolveSecret.mockRejectedValue(error);
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual(expected);
  });

  it.each([
    [{}, 'accepted'],
    [{ partialSuccess: { errorMessage: 'sensitive remote warning' } }, 'accepted_with_warning'],
    [
      { partialSuccess: { rejectedSpans: '1', errorMessage: 'sensitive rejection' } },
      'partial_rejection',
    ],
    [{ partialSuccess: { rejectedSpans: '-1' } }, 'invalid_otlp_response'],
  ])('returns only terminal reason for OTLP response %s', async (body, reason) => {
    const s = setup();
    s.otlpFetchImpl.mockResolvedValue(Response.json(body));
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({ kind: 'terminal', reason });
  });

  it('retries network failures but suppresses egress denial', async () => {
    const s = setup();
    s.otlpFetchImpl.mockRejectedValue(new TypeError('network failed'));
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({ kind: 'retry' });
    s.otlpFetchImpl.mockRejectedValue(new OtlpEgressPolicyError());
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'egress_denied',
    });
  });

  it('uses hardened egress by default, denying loopback without network I/O', async () => {
    const s = setup();
    s.record.deliveryContext.endpointUrl = 'https://127.0.0.1/api/public/otel/v1/traces';
    const { otlpFetchImpl: _fetch, ...options } = s.options;
    expect(await deliverKafkaTrace(s.record, options)).toEqual({
      kind: 'terminal',
      reason: 'egress_denied',
    });
  });

  it.each([
    { protocol: 'http/protobuf' },
    { sampleRate: NaN },
    { timeoutMs: 120001 },
    { endpointClass: 'private' },
    { configSchemaVersion: 2 },
    { endpointUrl: 'http://collector.example/api/public/otel/v1/traces' },
    { endpointUrl: 'https://collector.example/api/public/otel/v1/traces?' },
  ])('rejects invalid pinned context before resolving credentials: %s', async (change) => {
    const s = setup();
    Object.assign(s.record.deliveryContext, change);
    expect(() => parsePinnedDeliveryContext(s.record.deliveryContext)).toThrow();
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'invalid_delivery_context',
    });
    expect(s.resolveSecret).not.toHaveBeenCalled();
  });

  it('rejects non-allowlisted metadata before credentials or egress', async () => {
    const s = setup();
    s.record.trace.root.metadata['prompt'] = 'private transcript';
    expect(await deliverKafkaTrace(s.record, s.options)).toEqual({
      kind: 'terminal',
      reason: 'invalid_projected_trace',
    });
    expect(s.resolveSecret).not.toHaveBeenCalled();
    expect(s.otlpFetchImpl).not.toHaveBeenCalled();
  });

  it('propagates cancellation before work and after secret resolution', async () => {
    const s = setup();
    const controller = new AbortController();
    const reason = new Error('shutdown');
    s.resolveSecret.mockImplementation(async () => {
      controller.abort(reason);
      return s.secret;
    });
    await expect(deliverKafkaTrace(s.record, s.options, controller.signal)).rejects.toBe(reason);
    expect(s.otlpFetchImpl).not.toHaveBeenCalled();
    s.resolveSecret.mockClear();
    await expect(deliverKafkaTrace(s.record, s.options, controller.signal)).rejects.toBe(reason);
    expect(s.resolveSecret).not.toHaveBeenCalled();
  });

  it('keeps a complete partial rejection terminal when cancellation arrives at EOF', async () => {
    const s = setup();
    const controller = new AbortController();
    s.otlpFetchImpl.mockResolvedValue(
      new Response(
        new ReadableStream({
          start(stream) {
            stream.enqueue(new TextEncoder().encode('{"partialSuccess":{"rejectedSpans":"1"}}'));
          },
          pull(stream) {
            controller.abort();
            stream.close();
          },
        }),
        { headers: { 'content-type': 'application/json' } },
      ),
    );
    expect(await deliverKafkaTrace(s.record, s.options, controller.signal)).toEqual({
      kind: 'terminal',
      reason: 'partial_rejection',
    });
  });
});
