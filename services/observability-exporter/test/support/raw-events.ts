// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { event, SESSION_ID, USER_ID } from './events.js';

// Deliberately fake; raw capture preserves this token-shaped value, including in remote smoke.
export const IO_CANARY = 'sk-ant-api03-SYNTHETIC_TEST_ONLY_' + 'Z'.repeat(32);
export const IO_INPUT = 'Check build 42.';
export const IO_OUTPUT = 'Build 42 failed its unit tests.';
export const IO_TOOL_NAME = 'read_build';

/** Only synthetic data. Shared by the real Kafka and optional remote smoke tests. */
export function rawPrimaryTurnEvents(namespace = '') {
  const sessionId = SESSION_ID + namespace;
  const nativeId = 'native_build' + namespace;
  const make = (seq: number, kind: string, payload: Record<string, unknown>) =>
    event(seq, kind, payload, {
      id: 'evt_io_' + seq + namespace,
      sessionId,
      ...(kind.startsWith('user.') ? { producedBy: 'client', userId: USER_ID } : {}),
    });
  return [
    make(1, 'user.message', { content: [{ type: 'text', text: IO_INPUT }] }),
    make(2, 'session.user_event_processed', { user_event_id: 'evt_io_1' + namespace }),
    make(3, 'session.status_running', {}),
    make(4, 'span.model_request_start', {
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-test',
    }),
    make(5, 'agent.tool_use', {
      tool_use_id: nativeId,
      name: IO_TOOL_NAME,
      input: {
        build: 42,
        api_key: IO_CANARY,
        password: IO_CANARY,
        headers: { Authorization: 'Bearer ' + IO_CANARY },
        env: { ACCESS_TOKEN: IO_CANARY },
        thinking: 'tool-owned-thinking-field',
      },
    }),
    make(6, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
    make(7, 'user.tool_confirmation', { tool_use_id: nativeId, result: 'allow' }),
    make(8, 'session.user_event_processed', { user_event_id: 'evt_io_7' + namespace }),
    make(9, 'session.status_running', {}),
    make(10, 'agent.tool_result', {
      tool_use_id: nativeId,
      content: [
        { type: 'text', text: 'Unit tests failed.' },
        { type: 'thinking', text: IO_CANARY },
      ],
      is_error: true,
    }),
    make(11, 'agent.message', {
      partial: true,
      content: [{ type: 'text', text: 'unfinished-private-delta' }],
    }),
    make(12, 'agent.message', {
      content: [
        { type: 'thinking', thinking: 'private-thinking-canary', signature: 'private-signature' },
      ],
    }),
    make(13, 'agent.message', { content: [{ type: 'text', text: IO_OUTPUT }] }),
    make(14, 'span.model_request_end', {
      model_observation_kind: 'turn_model_summary',
      model_request_start_id: 'evt_io_4' + namespace,
      model_usage: { input_tokens: 11, output_tokens: 7 },
      is_error: false,
    }),
    make(15, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
  ];
}
