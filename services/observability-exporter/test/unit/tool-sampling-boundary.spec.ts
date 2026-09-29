// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { deterministicTraceId } from '../../src/ids.js';
import {
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { isTraceSampled, TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import type { ProjectedTrace } from '../../src/types.js';
import { event, SESSION_ID, WORKSPACE_ID } from '../support/events.js';

describe('tool observation payload access is gated, except client acceptance-control metadata', () => {
  it.each([0, 0.5, 1])(
    'checks actual payload reads at rate %s across durable batches',
    (sampleRate) => {
      const policy = {
        algorithmVersion: TRACE_SAMPLING_VERSION,
        bindingId: 'aob_tool_boundary',
        bindingVersion: 1,
        sampleRate,
      };
      const decisions = new Set<boolean>();
      for (let turn = 0; turn < 12; turn += 1) {
        const anchor = `evt_boundary_${turn}`;
        const sampled = isTraceSampled(
          policy,
          deterministicTraceId(WORKSPACE_ID, SESSION_ID, anchor),
        );
        decisions.add(sampled);
        const sources = [
          event(1, 'user.message', {}, { id: anchor, producedBy: 'client' }),
          event(2, 'session.user_event_processed', { user_event_id: anchor }),
          event(3, 'session.status_running'),
          event(4, 'agent.tool_use', {
            id: 'private-native',
            name: 'private-name',
            input: 'private-input',
          }),
          event(5, 'agent.mcp_tool_use', {
            tool_use_id: 'private-mcp-native',
            name: 'private-name',
          }),
          event(6, 'agent.custom_tool_use', { name: 'private-name', input: 'private-input' }),
          event(7, 'agent.mcp_tool_result', {
            tool_use_id: 'private-mcp-native',
            content: 'private-output',
          }),
          event(8, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
          event(
            9,
            'user.tool_result',
            { tool_use_id: 'evt_4', content: 'private-output' },
            { producedBy: 'client' },
          ),
          event(
            10,
            'user.custom_tool_result',
            { custom_tool_use_id: 'evt_6', content: 'private-output' },
            { producedBy: 'client' },
          ),
          event(11, 'session.user_event_processed', { user_event_id: 'evt_10' }),
          event(12, 'session.user_event_processed', { user_event_id: 'evt_9' }),
          event(13, 'session.status_running'),
          event(14, 'span.outcome_evaluation_start', {
            outcome_id: 'private-outcome',
            iteration: 0,
          }),
          event(15, 'span.outcome_evaluation_end', {
            outcome_evaluation_start_id: 'evt_14',
            outcome_id: 'private-outcome',
            iteration: 0,
            result: 'satisfied',
          }),
          event(16, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
        ];
        const reads = new Map<string, number>();
        const observations = sources.filter(
          (source) =>
            source.kind.includes('tool') || source.kind.startsWith('span.outcome_evaluation_'),
        );
        for (const source of observations) {
          const payload = source.payload;
          reads.set(source.id, 0);
          // Count reads rather than throwing: payload parsers intentionally catch malformed input.
          Object.defineProperty(source, 'payload', {
            get() {
              reads.set(source.id, reads.get(source.id)! + 1);
              return payload;
            },
          });
        }
        let state = initialCanonicalProjectionState();
        const accepted = new Set<string>();
        const completed: ProjectedTrace[] = [];
        for (const source of sources) {
          const reduced = reduceCanonicalEventBatch(state, [source], accepted, policy);
          expect(reduced.issues).toEqual([]);
          for (const id of reduced.acceptedSourceIds) accepted.add(id);
          completed.push(...reduced.completedTraces);
          state = parseCanonicalProjectionState(JSON.parse(JSON.stringify(reduced.state)));
          expect(JSON.stringify(state)).not.toContain('private-');
          if (!sampled) {
            for (const input of state.pendingInputs) {
              expect(input).not.toHaveProperty('toolResult');
              expect(input).not.toHaveProperty('toolTraceId');
            }
            expect(state.activeTurn?.tools.uses ?? []).toEqual([]);
            expect(state.activeTurn?.tools.results ?? []).toEqual([]);
            expect(state.activeTurn?.openEvaluations ?? []).toEqual([]);
            expect(state.activeTurn?.completedEvaluations ?? []).toEqual([]);
          }
        }
        expect(completed).toHaveLength(sampled ? 1 : 0);
        expect(JSON.stringify(completed)).not.toContain('private-');
        if (sampled) {
          expect(completed[0]!.spans).toHaveLength(4);
          expect([...reads.values()].every((count) => count > 0)).toBe(true);
        } else {
          for (const source of observations) {
            // Client results must be read for their optional companion acceptance ID.
            expect(reads.get(source.id)).toBe(source.producedBy === 'client' ? 1 : 0);
          }
          expect(state.sampling?.suppressed?.turnCount).toBe('1');
        }
      }
      expect(decisions).toEqual(
        sampleRate === 0.5 ? new Set([true, false]) : new Set([sampleRate === 1]),
      );
    },
  );
});
