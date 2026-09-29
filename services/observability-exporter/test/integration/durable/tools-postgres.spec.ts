// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  applyObservabilityExporterMigrations,
  ObservabilityExporterRepository,
} from '../../../src/persistence.js';
import { initialCanonicalProjectionState } from '../../../src/projector.js';
import type { PinnedDeliveryContext, ProjectedTrace } from '../../../src/types.js';
import { completedProjectedTrace, event, TRANSCRIPT_SECRET } from '../../support/events.js';
import { toolSpan } from '../../support/tools.js';

const adminUrl =
  process.env['OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL'] ??
  'postgres://orca:orca@127.0.0.1:5432/postgres';
const context: PinnedDeliveryContext = {
  organizationId: 'org_tools',
  bindingId: 'aob_tools',
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

describe('tool trace persistence (real Postgres)', () => {
  let admin: Pool;
  let pool: Pool;
  let repository: ObservabilityExporterRepository;
  let database: string;
  beforeAll(async () => {
    admin = new Pool({ connectionString: adminUrl });
    database = `obs_tools_${process.pid}_${randomBytes(4).toString('hex')}`;
    await admin.query(`CREATE DATABASE "${database}"`);
    const url = new URL(adminUrl);
    url.pathname = `/${database}`;
    pool = new Pool({ connectionString: url.toString() });
    await applyObservabilityExporterMigrations(pool);
    repository = new ObservabilityExporterRepository(pool);
  });
  beforeEach(async () => {
    await pool.query(
      'TRUNCATE observability_exporter_session_state, observability_exporter_event_inbox, observability_exporter_trace_outbox, observability_exporter_accepted_sources, observability_exporter_conflicts',
    );
  });
  afterAll(async () => {
    await pool?.end();
    await admin?.query(`DROP DATABASE IF EXISTS "${database}"`);
    await admin?.end();
  });

  async function claim() {
    await repository.acceptEvent(
      event(1, 'user.message', { content: TRANSCRIPT_SECRET }, { producedBy: 'client' }),
    );
    const claim = await repository.claimSession('tool-projector', 30000);
    expect(claim).not.toBeNull();
    return claim!;
  }

  it('round-trips old v1 traces unchanged and persists completed/incomplete tool children', async () => {
    for (const withTools of [false, true]) {
      const lease = await claim();
      const trace = completedProjectedTrace();
      if (withTools)
        trace.spans.push(
          ...(['success', 'error', 'incomplete'] as const).map((outcome) =>
            toolSpan(trace, outcome),
          ),
        );
      await repository.completeProjection(lease, '2', initialCanonicalProjectionState(), [
        { trace, deliveryContext: context },
      ]);
      const item = await repository.claimOutbox('tool-delivery', 30000);
      expect(item?.trace).toEqual(trace);
      expect(JSON.stringify(item)).not.toContain(TRANSCRIPT_SECRET);
      await repository.markOutboxDelivered(item!);
      await pool.query(
        'TRUNCATE observability_exporter_session_state, observability_exporter_event_inbox, observability_exporter_trace_outbox',
      );
    }
  });

  it.each([
    [
      'span identity',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.spanId = 'a'.repeat(16);
      },
    ],
    [
      'parent identity',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.parentSpanId = 'b'.repeat(16);
      },
    ],
    [
      'name content',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.name = TRANSCRIPT_SECRET;
      },
    ],
    [
      'metadata content',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.metadata['input'] = TRANSCRIPT_SECRET;
      },
    ],
    [
      'invalid family',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.metadata['orca.tool.family'] = TRANSCRIPT_SECRET;
      },
    ],
    [
      'invalid outcome',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.metadata['orca.tool.outcome'] = TRANSCRIPT_SECRET;
      },
    ],
    [
      'status mismatch',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.status = 'unset';
      },
    ],
    [
      'source mismatch',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.metadata['orca.source.start_event_id'] = 'evt_other';
      },
    ],
    [
      'missing end source',
      (trace: ProjectedTrace) => {
        delete trace.spans[0]!.metadata['orca.source.end_event_id'];
      },
    ],
    [
      'model fields',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.modelSummary = { provider: TRANSCRIPT_SECRET };
      },
    ],
    [
      'duplicate source',
      (trace: ProjectedTrace) => {
        trace.spans.push(trace.spans[0]!);
      },
    ],
  ])('rejects %s atomically before cursor/outbox writes', async (_label, mutate) => {
    const lease = await claim();
    const trace = completedProjectedTrace();
    trace.spans = [toolSpan(trace)];
    mutate(trace);
    await expect(
      repository.completeProjection(lease, '2', initialCanonicalProjectionState(), [
        { trace, deliveryContext: context },
      ]),
    ).rejects.toThrow();
    expect(
      (await pool.query('SELECT next_seq::text FROM observability_exporter_session_state')).rows,
    ).toEqual([{ next_seq: '0' }]);
    expect(
      (await pool.query('SELECT count(*)::int AS count FROM observability_exporter_trace_outbox'))
        .rows,
    ).toEqual([{ count: 0 }]);
  });
});
