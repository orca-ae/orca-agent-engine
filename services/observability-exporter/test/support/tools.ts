// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { deterministicChildSpanId } from '../../src/ids.js';
import type { ProjectedSpan, ProjectedTrace } from '../../src/types.js';

export function toolSpan(
  trace: ProjectedTrace,
  outcome: 'success' | 'error' | 'incomplete' = 'success',
): ProjectedSpan {
  const sourceEventId = `evt_tool_${outcome}`;
  return {
    spanId: deterministicChildSpanId(trace.traceId, 'tool', '', sourceEventId),
    parentSpanId: trace.root.spanId,
    sourceEventId,
    subpath: '',
    name: 'orca.agent.tool',
    observationType: 'tool',
    startedAt: trace.root.startedAt,
    endedAt: trace.root.endedAt,
    status: outcome === 'incomplete' ? 'unset' : outcome === 'error' ? 'error' : 'ok',
    metadata: {
      observation_type: 'tool',
      'orca.projection.schema_version': trace.schemaVersion,
      'orca.thread.subpath': '',
      'orca.source.start_event_id': sourceEventId,
      'orca.source.end_event_id': `evt_tool_end_${outcome}`,
      'orca.tool.family': 'local',
      'orca.tool.outcome': outcome,
    },
  };
}
