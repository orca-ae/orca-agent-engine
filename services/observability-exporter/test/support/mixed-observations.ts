// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { PinnedDeliveryContext } from '../../src/types.js';
import { event, TRANSCRIPT_SECRET } from './events.js';

export const PRIVATE_OUTCOME = 'outcome_patient_alice_private_diagnosis';
export const mixedDeliveryContext: PinnedDeliveryContext = {
  organizationId: 'org_mixed',
  bindingId: 'aob_mixed',
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

/** At seq 12: pending client receipt, open/completed model and evaluator coexist. */
export function mixedObservationEvents() {
  const modelStart = (seq: number) =>
    event(seq, 'span.model_request_start', {
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-test',
    });
  const modelEnd = (seq: number, start: string) =>
    event(seq, 'span.model_request_end', {
      model_observation_kind: 'turn_model_summary',
      model_request_start_id: start,
      model_usage: { input_tokens: 11, output_tokens: 7 },
      is_error: false,
    });
  const evalStart = (seq: number) =>
    event(seq, 'span.outcome_evaluation_start', { outcome_id: PRIVATE_OUTCOME, iteration: 0 });
  const evalEnd = (seq: number, start: string, result: string) =>
    event(seq, 'span.outcome_evaluation_end', {
      outcome_id: PRIVATE_OUTCOME,
      iteration: 0,
      outcome_evaluation_start_id: start,
      result,
      explanation: TRANSCRIPT_SECRET,
      usage: { input_tokens: 999 },
    });
  return [
    event(1, 'user.message', { content: TRANSCRIPT_SECRET }, { producedBy: 'client' }),
    event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
    modelStart(3),
    event(4, 'agent.tool_use', { name: TRANSCRIPT_SECRET, input: TRANSCRIPT_SECRET }),
    evalStart(5),
    evalEnd(6, 'evt_5', 'interrupted'),
    modelEnd(7, 'evt_3'),
    evalStart(8),
    modelStart(9),
    event(10, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
    event(
      11,
      'user.tool_result',
      { tool_use_id: 'evt_4', content: TRANSCRIPT_SECRET, is_error: true },
      { producedBy: 'client' },
    ),
    event(12, 'span.outcome_evaluation_ongoing', { explanation: TRANSCRIPT_SECRET }),
    event(13, 'session.user_event_processed', { user_event_id: 'evt_11' }),
    event(14, 'session.status_running'),
    evalEnd(15, 'evt_8', 'failed'),
    modelEnd(16, 'evt_9'),
    event(17, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
  ];
}
