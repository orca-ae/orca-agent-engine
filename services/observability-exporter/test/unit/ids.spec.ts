// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  deterministicChildSpanId,
  deterministicRootSpanId,
  deterministicTraceId,
} from '../../src/ids.js';

describe('deterministic observability IDs', () => {
  it('matches stable seed-layout golden vectors', () => {
    const traceId = deterministicTraceId('ws_test', 'ses_test', 'evt_anchor');
    expect(traceId).toBe('e04de008523214627c36f16e426ba173');
    expect(deterministicRootSpanId(traceId)).toBe('0267231042c6957e');
    expect(deterministicChildSpanId(traceId, 'turn_model_summary', '', 'evt_model_start')).toBe(
      '3b7b98d5bba25697',
    );
  });

  it('includes canonical subpath in child identity', () => {
    const traceId = deterministicTraceId('ws_test', 'ses_test', 'evt_anchor');
    expect(deterministicChildSpanId(traceId, 'turn_model_summary', '', 'evt_model_start')).not.toBe(
      deterministicChildSpanId(traceId, 'turn_model_summary', 'subagents/child', 'evt_model_start'),
    );
  });
});
