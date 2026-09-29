// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  IO_PROJECTED_TRACE_SCHEMA_VERSION,
  IO_VERSION,
  type PinnedDeliveryContext,
} from '../../src/types.js';
import { completedProjectedTrace } from '../support/events.js';
import { waitForLitefuseProjectedTrace } from '../support/litefuse.js';
import { toolSpan } from '../support/tools.js';

describe('Litefuse observation read-back', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    vi.useRealTimers();
  });

  function fixture() {
    const trace = completedProjectedTrace();
    trace.schemaVersion = IO_PROJECTED_TRACE_SCHEMA_VERSION;
    trace.root.io = {
      version: IO_VERSION,
      input: { json: '"Question"' },
      output: { json: '["Answer"]' },
      outputScope: 'turn_messages',
    };
    const tool = toolSpan(trace, 'error');
    tool.io = {
      version: IO_VERSION,
      toolName: 'read_build',
      input: { json: '{"build":42}' },
      output: { json: '"failed"' },
    };
    trace.spans.push(tool);
    const observations = [trace.root, ...trace.spans].map((span) => ({
      id: span.spanId,
      traceId: trace.traceId,
      parentObservationId: span.parentSpanId ?? null,
      name: span.io?.toolName ?? span.name,
      type:
        span.observationType === 'agent_turn'
          ? 'AGENT'
          : span.observationType === 'tool'
            ? 'TOOL'
            : 'SPAN',
      startTime: span.startedAt,
      endTime: span.endedAt,
      level: span.status === 'error' ? 'ERROR' : 'DEFAULT',
      usageDetails:
        span.modelSummary === undefined
          ? {}
          : { input: 11, output: 7, cache_creation_input_tokens: 2, input_cached_tokens: 3 },
      costDetails: span.modelSummary === undefined ? {} : { total: 0.001 },
      userId: trace.workspaceId + ':' + trace.userId,
      sessionId: trace.workspaceId + ':' + trace.sessionId,
      metadata: {
        ...trace.root.metadata,
        ...span.metadata,
        'orca.projection.schema_version': trace.schemaVersion,
      },
      input:
        span.io?.input?.json === undefined ? null : (JSON.parse(span.io.input.json) as unknown),
      output:
        span.io?.output?.json === undefined ? null : (JSON.parse(span.io.output.json) as unknown),
    }));
    const credentials = {
      endpoint: 'https://litefuse.example/api/public/otel/v1/traces',
      publicKey: 'test-public',
      secretKey: 'test-secret',
    };
    return { trace, credentials, observations };
  }

  it('verifies root and tool I/O by observation identity', async () => {
    const { trace, credentials, observations } = fixture();
    vi.useFakeTimers();
    vi.stubEnv('LITEFUSE_SMOKE_TIMEOUT_MS', '10');
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => Response.json({ data: observations })),
    );
    const result = expect(
      waitForLitefuseProjectedTrace({ trace, credentials }),
    ).resolves.toBeUndefined();
    await vi.runAllTimersAsync();
    await result;
  });

  it('does not report success when a tool output is missing', async () => {
    const { trace, credentials, observations } = fixture();
    observations[2]!.output = null;
    vi.useFakeTimers();
    vi.stubEnv('LITEFUSE_SMOKE_TIMEOUT_MS', '10');
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => Response.json({ data: observations })),
    );
    const result = expect(waitForLitefuseProjectedTrace({ trace, credentials })).rejects.toThrow(
      'was not queryable',
    );
    await vi.runAllTimersAsync();
    await result;
  });

  it.each(['error level', 'usage', 'cost'] as const)(
    'rejects lost or changed %s',
    async (field) => {
      const { trace, credentials, observations } = fixture();
      if (field === 'error level') observations[2]!.level = 'DEFAULT';
      if (field === 'usage') observations[1]!.usageDetails = {};
      if (field === 'cost') observations[1]!.costDetails = {};
      vi.useFakeTimers();
      vi.stubEnv('LITEFUSE_SMOKE_TIMEOUT_MS', '10');
      vi.stubGlobal(
        'fetch',
        vi.fn(async () => Response.json({ data: observations })),
      );
      const result = expect(waitForLitefuseProjectedTrace({ trace, credentials })).rejects.toThrow(
        'was not queryable',
      );
      await Promise.all([result, vi.runAllTimersAsync()]);
    },
  );

  it.each(['none', 'metadata', 'version', 'environment', 'release'] as const)(
    'checks pinned attribution on every observation (tampered=%s)',
    async (tampered) => {
      const { trace, credentials, observations } = fixture();
      const context: PinnedDeliveryContext = {
        organizationId: 'org_registry',
        bindingId: 'aob_registry',
        bindingVersion: 2,
        adapterType: 'otlp_http',
        endpointKind: 'traces_endpoint',
        endpointClass: 'public',
        endpointUrl: credentials.endpoint,
        semanticProfile: 'langfuse',
        protocol: 'http/json',
        compression: 'none',
        timeoutMs: 1000,
        captureMode: 'raw_io',
        sampleRate: 1,
        configSchemaVersion: 1,
        agentId: 'agt_registry',
        agentVersion: 7,
        harness: 'claude_code',
        harnessMode: 'colocated',
        environment: 'test',
        release: 'release-1',
      };
      for (const observation of observations) {
        Object.assign(observation, { version: '7', environment: 'test', release: 'release-1' });
        Object.assign(observation.metadata, {
          'orca.agent.id': 'agt_registry',
          'orca.agent.version': 7,
          'orca.harness.name': 'claude_code',
          'orca.harness.mode': 'colocated',
          'orca.observability.binding_id': 'aob_registry',
          'orca.observability.binding_version': 2,
          'orca.observability.config_schema_version': 1,
          'orca.deployment.environment': 'test',
          'orca.deployment.release': 'release-1',
        });
      }
      if (tampered === 'metadata')
        Object.assign(observations[2]!.metadata, { 'orca.agent.version': 8 });
      else if (tampered !== 'none') Object.assign(observations[2]!, { [tampered]: 'wrong-value' });
      vi.useFakeTimers();
      vi.stubEnv('LITEFUSE_SMOKE_TIMEOUT_MS', '10');
      vi.stubGlobal(
        'fetch',
        vi.fn(async () => Response.json({ data: observations })),
      );
      const work = waitForLitefuseProjectedTrace({ trace, credentials, context });
      const result =
        tampered !== 'none'
          ? expect(work).rejects.toThrow('was not queryable')
          : expect(work).resolves.toBeUndefined();
      await Promise.all([result, vi.runAllTimersAsync()]);
    },
  );

  it.each([false, true])('checks accepted approval facts (missing=%s)', async (missing) => {
    const { trace, credentials, observations } = fixture();
    const facts = {
      'orca.tool.last_approval.result': 'deny',
      'orca.tool.last_approval.source_event_id': 'evt_confirmation',
      'orca.tool.last_approval.acceptance_event_id': 'evt_confirmation_accepted',
      'orca.tool.last_approval.accepted_at': '2026-01-01T00:00:06.000Z',
    };
    Object.assign(trace.spans[1]!.metadata, facts);
    if (!missing) Object.assign(observations[2]!.metadata, facts);
    vi.useFakeTimers();
    vi.stubEnv('LITEFUSE_SMOKE_TIMEOUT_MS', '10');
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => Response.json({ data: observations })),
    );
    const work = waitForLitefuseProjectedTrace({ trace, credentials });
    const result = missing
      ? expect(work).rejects.toThrow('was not queryable')
      : expect(work).resolves.toBeUndefined();
    await Promise.all([result, vi.runAllTimersAsync()]);
  });

  it.each([false, true])(
    'validates native release through the v1 trace fallback (wrong=%s)',
    async (wrong) => {
      const { trace, credentials, observations } = fixture();
      const context: PinnedDeliveryContext = {
        organizationId: 'org_registry',
        bindingId: 'aob_registry',
        bindingVersion: 2,
        adapterType: 'otlp_http',
        endpointKind: 'traces_endpoint',
        endpointClass: 'public',
        endpointUrl: credentials.endpoint,
        semanticProfile: 'langfuse',
        protocol: 'http/json',
        compression: 'none',
        timeoutMs: 1000,
        captureMode: 'raw_io',
        sampleRate: 1,
        configSchemaVersion: 1,
        release: 'release-1',
      };
      for (const observation of observations)
        Object.assign(observation.metadata, {
          'orca.observability.binding_id': 'aob_registry',
          'orca.observability.binding_version': 2,
          'orca.observability.config_schema_version': 1,
          'orca.deployment.release': 'release-1',
        });
      vi.useFakeTimers();
      vi.stubEnv('LITEFUSE_SMOKE_TIMEOUT_MS', '10');
      vi.stubGlobal(
        'fetch',
        vi.fn(async (input: unknown) => {
          const path = new URL(String(input)).pathname;
          if (path === '/api/public/v2/observations') return new Response(null, { status: 404 });
          if (path === '/api/public/observations') return Response.json({ data: observations });
          return Response.json({
            id: trace.traceId,
            userId: trace.workspaceId + ':' + trace.userId,
            sessionId: trace.workspaceId + ':' + trace.sessionId,
            release: wrong ? 'wrong-release' : 'release-1',
          });
        }),
      );
      const work = waitForLitefuseProjectedTrace({ trace, credentials, context });
      const result = wrong
        ? expect(work).rejects.toThrow('was not queryable')
        : expect(work).resolves.toBeUndefined();
      await Promise.all([result, vi.runAllTimersAsync()]);
    },
  );
});
