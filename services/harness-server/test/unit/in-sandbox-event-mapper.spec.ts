// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { mapSandboxEvent } from '../../src/harness/in-sandbox/event-mapper.js';
import type { RawSandboxEvent } from '../../src/harness/in-sandbox/transport.js';
import { expectUniqueAgentEventEnvelopes } from '../support/model-summary.js';

function raw(type: string, extra: Record<string, unknown> = {}): RawSandboxEvent {
  return {
    id: `evt_${type.replace(/\./g, '_')}`,
    session_id: 'ses_01',
    created_at: '2026-06-06T00:00:00.000Z',
    type,
    ...extra,
  };
}

function map(event: RawSandboxEvent) {
  return mapSandboxEvent(
    event,
    event.type === 'session.status_idle'
      ? {
          modelRequestStartId: 'evt_model_start',
          provider: 'anthropic',
          model: 'claude-test-model',
        }
      : undefined,
  );
}

describe('mapSandboxEvent', () => {
  it('drops user.message (registry already recorded it)', () => {
    expect(map(raw('user.message', { content: 'hello' }))).toEqual([]);
  });

  it('maps agent.message 1:1 and preserves id', () => {
    const evt = raw('agent.message', { content: [{ type: 'text', text: 'hi' }] });
    const out = map(evt);
    expect(out).toHaveLength(1);
    expect(out[0]!.kind).toBe('agent.message');
    expect(out[0]!.id).toBe(evt.id);
    expect((out[0]!.payload as { content: unknown }).content).toEqual([
      { type: 'text', text: 'hi' },
    ]);
  });

  it('maps agent.thinking 1:1 and preserves id', () => {
    const evt = raw('agent.thinking', { content: [{ type: 'thinking', thinking: 'hmm' }] });
    const out = map(evt);
    expect(out).toHaveLength(1);
    expect(out[0]!.kind).toBe('agent.thinking');
    expect(out[0]!.id).toBe(evt.id);
  });

  it('maps agent.message with missing content to empty array', () => {
    const out = map(raw('agent.message'));
    expect((out[0]!.payload as { content: unknown }).content).toEqual([]);
  });

  it('maps agent.tool_use and preserves id', () => {
    const evt = raw('agent.tool_use', { name: 'bash', input: { cmd: 'ls' }, tool_use_id: 'tu_01' });
    const out = map(evt);
    expect(out).toHaveLength(1);
    expect(out[0]!.kind).toBe('agent.tool_use');
    expect(out[0]!.id).toBe(evt.id);
    const p = out[0]!.payload as { name: string; input: unknown; tool_use_id: string };
    expect(p.name).toBe('bash');
    expect(p.input).toEqual({ cmd: 'ls' });
    expect(p.tool_use_id).toBe('tu_01');
  });

  it('maps agent.tool_use with missing input to empty object', () => {
    const out = map(raw('agent.tool_use', { name: 'read', tool_use_id: 'tu_02' }));
    expect((out[0]!.payload as { input: unknown }).input).toEqual({});
  });

  it('maps agent.custom_tool_use and preserves id in the payload', () => {
    const evt = raw('agent.custom_tool_use', {
      name: 'lookup_ticket',
      input: { ticket_id: 'T-123' },
    });
    const out = map(evt);
    expect(out.map((e) => e.kind)).toEqual(['agent.custom_tool_use', 'session.status_idle']);
    expect(out[0]!.id).toBe(evt.id);
    const p = out[0]!.payload as { id: string; name: string; input: unknown };
    expect(p.id).toBe(evt.id);
    expect(p.name).toBe('lookup_ticket');
    expect(p.input).toEqual({ ticket_id: 'T-123' });
    expect(out[1]!.id).toBe(`${evt.id}:idle`);
    expect(
      (out[1]!.payload as { stop_reason: { type: string; event_ids?: string[] } }).stop_reason,
    ).toEqual({ type: 'requires_action', event_ids: [evt.id] });
  });

  it('rejects a custom-tool required action without a canonical event id', () => {
    expect(() =>
      mapSandboxEvent({
        ...raw('agent.custom_tool_use', { name: 'lookup_ticket' }),
        id: 'toolu_not_an_event',
      }),
    ).toThrow('agent.custom_tool_use requires a canonical sandbox event id');
  });

  it.each([
    ['agent.message', { content: [] }],
    ['agent.thinking', { content: [] }],
    ['agent.tool_use', { name: 'read', tool_use_id: 'toolu_1' }],
    ['agent.tool_result', { tool_use_id: 'toolu_1', content: 'result' }],
    ['session.status_idle', { usage: {} }],
    ['session.status_error', { error: 'failed' }],
  ])('rejects a malformed raw id for %s', (type, extra) => {
    expect(() => map({ ...raw(type, extra), id: 'not_canonical' })).toThrow(
      'invalid AgentEvent.id',
    );
  });

  it('maps agent.tool_result and preserves id', () => {
    const evt = raw('agent.tool_result', { tool_use_id: 'tu_01', content: 'out', is_error: false });
    const out = map(evt);
    expect(out).toHaveLength(1);
    expect(out[0]!.kind).toBe('agent.tool_result');
    expect(out[0]!.id).toBe(evt.id);
    const p = out[0]!.payload as { tool_use_id: string; content: unknown; is_error: boolean };
    expect(p.tool_use_id).toBe('tu_01');
    expect(p.content).toBe('out');
    expect(p.is_error).toBe(false);
  });

  it('coerces is_error to boolean', () => {
    const out = map(raw('agent.tool_result', { tool_use_id: 'tu_03', is_error: 1 }));
    expect((out[0]!.payload as { is_error: boolean }).is_error).toBe(true);
  });

  it('uses the event id when a tool result omits tool_use_id', () => {
    const evt = raw('agent.tool_result', { content: 'out' });
    const out = map(evt);
    expect((out[0]!.payload as { tool_use_id: string }).tool_use_id).toBe(evt.id);
  });

  it('maps per-assistant usage with model and managed subagent attribution', () => {
    const evt = raw('agent.usage', {
      usage: { input_tokens: 12, output_tokens: 3 },
      model: 'claude-subagent-model',
      subagent_id: 'agt_researcher',
    });
    expect(mapSandboxEvent(evt)).toEqual([
      {
        kind: 'agent.usage',
        id: evt.id,
        // Canonical envelopes carry a subpath; a parent-agent event uses ''.
        subpath: '',
        payload: {
          usage: { input_tokens: 12, output_tokens: 3 },
          model: 'claude-subagent-model',
          subagent_id: 'agt_researcher',
        },
      },
    ]);
  });

  it('fans out session.status_idle to usage + model_request_end + session.status_idle{end_turn}', () => {
    const evt = raw('session.status_idle', {
      usage: { input_tokens: 10, output_tokens: 5 },
      total_cost_usd: 0.001,
    });
    const out = map(evt);
    expect(out.map((e) => e.kind)).toEqual([
      'agent.usage',
      'span.model_request_end',
      'session.status_idle',
    ]);
    // Internal usage sink carries the raw usage and preserves the frame id.
    expect(out[0]!.id).toBe(evt.id);
    expect((out[0]!.payload as { usage: unknown }).usage).toEqual({
      input_tokens: 10,
      output_tokens: 5,
    });
    // Public token accounting rides on the span.
    expect(out[1]!.id).toBe(`${evt.id}:model_end`);
    expect(out[1]!.payload).toMatchObject({
      model_request_start_id: 'evt_model_start',
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-test-model',
      model_usage: { input_tokens: 10, output_tokens: 5 },
    });
    // Terminal idle carries stop_reason end_turn.
    expect(out[2]!.id).toBe(`${evt.id}:idle`);
    expect((out[2]!.payload as { stop_reason: { type: string } }).stop_reason.type).toBe(
      'end_turn',
    );
  });

  it('maps session.status_idle with missing usage to zeroed span', () => {
    const out = map(raw('session.status_idle'));
    expect((out[0]!.payload as { usage: unknown }).usage).toEqual({});
    const modelUsage = (out[1]!.payload as { model_usage: Record<string, number> }).model_usage;
    expect(modelUsage.input_tokens).toBe(0);
    expect((out[2]!.payload as { stop_reason: { type: string } }).stop_reason.type).toBe(
      'end_turn',
    );
  });

  it('rejects a terminal model summary without its start envelope id', () => {
    expect(() => mapSandboxEvent(raw('session.status_idle'))).toThrow(
      'session.status_idle requires an open turn model summary',
    );
  });

  it('keeps aggregate usage on the span without double-counting per-assistant reports', () => {
    const out = map(
      raw('session.status_idle', {
        usage: { input_tokens: 12, output_tokens: 3 },
        total_cost_usd: 0.01,
        usage_already_reported: true,
      }),
    );
    expect(out.map((event) => event.kind)).toEqual([
      'span.model_request_end',
      'session.status_idle',
    ]);
    expect(
      (out[0]!.payload as { model_usage: { input_tokens: number } }).model_usage.input_tokens,
    ).toBe(12);
  });

  it('derives the public cache-creation total from TTL counters when the flat total is absent', () => {
    const out = map(
      raw('session.status_idle', {
        usage: {
          cache_creation: {
            ephemeral_1h_input_tokens: 13,
            ephemeral_5m_input_tokens: 17,
          },
        },
      }),
    );
    const modelUsage = (out[1]!.payload as { model_usage: Record<string, number> }).model_usage;
    expect(modelUsage.cache_creation_input_tokens).toBe(30);
  });

  it('prefers TTL counters over a conflicting flat cache-creation total', () => {
    const out = map(
      raw('session.status_idle', {
        usage: {
          cache_creation_input_tokens: 12,
          cache_creation: {
            ephemeral_1h_input_tokens: 5,
            ephemeral_5m_input_tokens: 20,
          },
        },
      }),
    );
    const modelUsage = (out[1]!.payload as { model_usage: Record<string, number> }).model_usage;
    expect(modelUsage.cache_creation_input_tokens).toBe(25);
  });

  it('fans out session.status_error to session.error + session.status_idle{retries_exhausted}', () => {
    const evt = raw('session.status_error', { error: 'model timed out' });
    const out = map(evt);
    expect(out.map((e) => e.kind)).toEqual(['session.error', 'session.status_idle']);
    expect(out.some((event) => event.kind === 'span.model_request_end')).toBe(false);
    const err = out[0]!.payload as {
      error: { message: string };
      retry_status: { will_retry: boolean };
    };
    expect(err.error.message).toBe('model timed out');
    expect(err.retry_status.will_retry).toBe(false);
    expect(out[1]!.id).toBe(`${evt.id}:idle`);
    expect((out[1]!.payload as { stop_reason: { type: string } }).stop_reason.type).toBe(
      'retries_exhausted',
    );
  });

  it('maps session.status_error with missing error to fallback string', () => {
    const out = map(raw('session.status_error'));
    expect((out[0]!.payload as { error: { message: string } }).error.message).toBe('unknown error');
  });

  it('drops unknown event types (returns [])', () => {
    expect(map(raw('session.status_paused'))).toEqual([]);
    expect(map(raw('unknown.event'))).toEqual([]);
    expect(map(raw(''))).toEqual([]);
  });

  it('gives every mapped sibling a unique canonical envelope on the primary path', () => {
    const events = [
      ...map(raw('agent.message', { content: [] })),
      ...map(raw('agent.tool_use', { name: 'read', tool_use_id: 'toolu_1' })),
      ...map(raw('agent.custom_tool_use', { name: 'client_tool' })),
      ...map(raw('session.status_idle', { usage: {} })),
      ...map(raw('session.status_error', { error: 'failed' })),
    ];

    expectUniqueAgentEventEnvelopes(events);
    expect(events.every((event) => event.subpath === '')).toBe(true);
  });
});
