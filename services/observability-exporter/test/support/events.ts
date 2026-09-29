// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from '@orca/transcript-store-types';
import { projectCanonicalTurns } from '../../src/projector.js';
import type { ProjectedTrace } from '../../src/types.js';

export const WORKSPACE_ID = 'ws_observability';
export const SESSION_ID = 'ses_observability';
export const USER_ID = 'user_observability';
export const TRANSCRIPT_SECRET = 'content-that-must-not-be-exported';

export function event(
  seq: number,
  kind: string,
  payload: Record<string, unknown> = {},
  options: Partial<
    Pick<Event, 'id' | 'workspaceId' | 'sessionId' | 'subpath' | 'producedBy' | 'userId'>
  > = {},
): Event {
  return {
    id: options.id ?? `evt_${seq}`,
    workspaceId: options.workspaceId ?? WORKSPACE_ID,
    sessionId: options.sessionId ?? SESSION_ID,
    subpath: options.subpath ?? '',
    seq,
    producedAt: new Date(Date.UTC(2026, 0, 1, 0, 0, seq)).toISOString(),
    producedBy: options.producedBy ?? 'harness',
    kind,
    payload: Buffer.from(JSON.stringify({ type: kind, ...payload })),
    idempotencyKey: `key_${seq}`,
    ...(options.userId === undefined ? {} : { userId: options.userId }),
  };
}

export function completedPrimaryTurnEvents(
  label: 'model_observation_kind' | 'observation_type' = 'model_observation_kind',
  namespace = '',
): Event[] {
  if (namespace !== '' && !/^_[A-Za-z0-9_-]+$/u.test(namespace)) {
    throw new Error('event namespace must be empty or an underscore-prefixed identifier');
  }
  const id = (base: string) => `${base}${namespace}`;
  const sessionId = `${SESSION_ID}${namespace}`;
  const turnEvent = (
    seq: number,
    kind: string,
    payload: Record<string, unknown> = {},
    options: Parameters<typeof event>[3] = {},
  ) => event(seq, kind, payload, { ...options, sessionId });
  return [
    turnEvent(
      1,
      'user.message',
      { content: [{ type: 'text', text: TRANSCRIPT_SECRET }] },
      { id: id('evt_user_turn'), producedBy: 'client', userId: USER_ID },
    ),
    turnEvent(
      2,
      'user.message',
      { content: [{ type: 'text', text: 'queued and unaccepted' }] },
      { id: id('evt_queued'), producedBy: 'client', userId: 'user_queued' },
    ),
    turnEvent(
      3,
      'session.user_event_processed',
      { user_event_id: id('evt_user_turn') },
      { id: id('evt_accept') },
    ),
    turnEvent(4, 'session.status_running'),
    turnEvent(
      5,
      'span.model_request_start',
      { [label]: 'turn_model_summary', provider: 'anthropic', model: 'claude-test' },
      { id: id('evt_model_start') },
    ),
    turnEvent(
      6,
      'agent.message',
      { content: [{ type: 'text', text: 'subagent secret must stay absent' }] },
      { id: id('evt_child_message'), subpath: 'subagents/child_1' },
    ),
    turnEvent(
      7,
      'span.model_request_end',
      {
        [label]: 'turn_model_summary',
        model_request_start_id: id('evt_model_start'),
        is_error: false,
        model_usage: {
          input_tokens: 11,
          output_tokens: 7,
          cache_creation_input_tokens: 2,
          cache_read_input_tokens: 3,
        },
        total_cost_usd: 0.001,
      },
      { id: id('evt_model_end') },
    ),
    turnEvent(8, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
  ];
}

export function completedProjectedTrace(): ProjectedTrace {
  const trace = projectCanonicalTurns(completedPrimaryTurnEvents())[0];
  if (trace === undefined) throw new Error('expected completed synthetic turn');
  return trace;
}
