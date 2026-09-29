// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { LitefuseOtlpHttpClient } from '../../src/litefuse-client.js';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import {
  initialCanonicalProjectionState,
  projectCanonicalTurns,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import type { PinnedDeliveryContext } from '../../src/types.js';
import { completedPrimaryTurnEvents } from '../support/events.js';
import { IO_CANARY, rawPrimaryTurnEvents } from '../support/raw-events.js';
import {
  assertCanonicalHttpsLitefuseEndpoint,
  requiredLitefuseSmokeCredentials,
  waitForLitefuseProjectedTrace,
} from '../support/litefuse.js';

describe('Litefuse OTLP smoke', () => {
  it('sends raw root and tool I/O and verifies their observation fields', async () => {
    const credentials = requiredLitefuseSmokeCredentials();
    assertCanonicalHttpsLitefuseEndpoint(credentials.endpoint);
    const runId = (process.env['LITEFUSE_SMOKE_RUN_ID'] ?? randomUUID()).replace(/-/gu, '');
    const events = rawPrimaryTurnEvents('_' + runId);
    const now = Date.now();
    events.forEach((event, index) => {
      event.producedAt = new Date(now + index).toISOString();
    });
    const [trace] = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events,
      new Set(),
      {
        algorithmVersion: TRACE_SAMPLING_VERSION,
        bindingId: 'aob_smoke',
        bindingVersion: 1,
        sampleRate: 1,
      },
      'raw_io',
    ).completedTraces;
    expect(trace?.root.io?.input?.json).toBeDefined();
    expect(trace?.root.io?.output?.json).toBeDefined();
    const context: PinnedDeliveryContext = {
      organizationId: 'org_smoke',
      bindingId: 'aob_smoke',
      bindingVersion: 1,
      adapterType: 'otlp_http',
      endpointKind: 'traces_endpoint',
      endpointClass: 'public',
      endpointUrl: credentials.endpoint,
      semanticProfile: 'langfuse',
      protocol: 'http/json',
      compression: 'none',
      timeoutMs: 10000,
      captureMode: 'raw_io',
      sampleRate: 1,
      configSchemaVersion: 1,
      agentId: 'agt_smoke',
      agentVersion: 1,
      harness: 'claude_code',
      harnessMode: 'colocated',
      environment: 'test',
      release: 'rio-attribution-approval-smoke',
    };
    expect(
      trace!.spans.find((span) => span.observationType === 'tool')?.metadata[
        'orca.tool.last_approval.result'
      ],
    ).toBe('allow');
    const request = encodeLangfuseOtlpJson(trace!, context);
    expect(JSON.stringify(request)).toContain(IO_CANARY);
    const client = new LitefuseOtlpHttpClient({
      endpoint: credentials.endpoint,
      publicKey: credentials.publicKey,
      secretKey: credentials.secretKey,
    });
    await client.send(request);
    await waitForLitefuseProjectedTrace({ credentials, trace: trace!, context });
    console.info(`Litefuse raw smoke verified trace ${trace!.traceId}`);
  });

  it('sends deterministic synthetic canonical turn', async () => {
    const credentials = requiredLitefuseSmokeCredentials();
    assertCanonicalHttpsLitefuseEndpoint(credentials.endpoint);
    const runId = (process.env['LITEFUSE_SMOKE_RUN_ID'] ?? randomUUID()).replace(/-/gu, '');
    const events = completedPrimaryTurnEvents('model_observation_kind', `_${runId}`);
    const now = Date.now();
    events.forEach((event, index) => {
      event.producedAt = new Date(now + index).toISOString();
    });
    const [trace] = projectCanonicalTurns(events);
    expect(trace).toBeDefined();

    const client = new LitefuseOtlpHttpClient({
      endpoint: credentials.endpoint,
      publicKey: credentials.publicKey,
      secretKey: credentials.secretKey,
    });
    await client.send(encodeLangfuseOtlpJson(trace!));
    await waitForLitefuseProjectedTrace({ credentials, trace: trace! });
    console.info(`Litefuse smoke verified trace ${trace!.traceId}`);
  });
});
