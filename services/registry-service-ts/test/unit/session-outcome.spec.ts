// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { foldOutcomeEvaluations } from '../../src/domain/session-outcome.js';

describe('foldOutcomeEvaluations', () => {
  it('returns an empty view for no evaluations', () => {
    expect(foldOutcomeEvaluations([])).toEqual({ outcome: null, outcome_evaluations: [] });
  });

  it('keeps the latest verdict per outcome and the most-recent overall', () => {
    const view = foldOutcomeEvaluations([
      { outcome_id: 'o1', result: 'needs_revision', explanation: 'not yet', iteration: 0 },
      { outcome_id: 'o2', result: 'satisfied', explanation: 'ok', iteration: 0 },
      { outcome_id: 'o1', result: 'satisfied', explanation: 'now done', iteration: 1 },
    ]);
    expect(view.outcome_evaluations).toEqual([
      { outcome_id: 'o1', result: 'satisfied', explanation: 'now done', iteration: 1 },
      { outcome_id: 'o2', result: 'satisfied', explanation: 'ok', iteration: 0 },
    ]);
    // Most recent overall is the last-seen record (o1's second evaluation).
    expect(view.outcome).toEqual({
      outcome_id: 'o1',
      result: 'satisfied',
      explanation: 'now done',
      iteration: 1,
    });
  });

  it('skips malformed payloads and defaults missing fields', () => {
    const view = foldOutcomeEvaluations([
      null,
      { result: 'satisfied' },
      { outcome_id: 'o3' },
      'garbage',
    ]);
    expect(view.outcome_evaluations).toEqual([
      { outcome_id: 'o3', result: 'failed', explanation: '', iteration: 0 },
    ]);
    expect(view.outcome).toEqual({
      outcome_id: 'o3',
      result: 'failed',
      explanation: '',
      iteration: 0,
    });
  });
});
