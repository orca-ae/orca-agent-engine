// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { stableSessionThreadId } from '@orca/agent-event-contract';
import { DshEventProjection, nativeThreadId } from '../../src/harness/codex-sdk/dsh-events.js';

const child = 'subagents/11111111-1111-4111-8111-111111111111';
const grandchild = 'subagents/22222222-2222-4222-8222-222222222222';
function declaration(subpath: string, parent = '') {
  return {
    id: `evt_${subpath}`,
    subpath,
    kind: 'session.thread_created',
    payload: { subpath, parent_subpath: parent, turn_event_id: 'evt_turn' },
  };
}

describe('DSH canonical tree admission', () => {
  it('validates bounded names and structured idle reasons', () => {
    const projection = new DshEventProjection('ws', 'ses', []);
    const named = declaration(child);
    projection.validate(
      { ...named, payload: { ...named.payload, agent_name: 'reviewer' } },
      'evt_turn',
    );
    const idle = {
      id: 'evt_idle',
      subpath: child,
      kind: 'session.thread_status_idle',
      payload: {
        subpath: child,
        parent_subpath: '',
        turn_event_id: 'evt_turn',
        stop_reason: 'interrupted',
        native_reason: 'aborted',
      },
    };
    expect(() =>
      projection.validate({ ...idle, payload: { ...idle.payload, stop_reason: {} } }, 'evt_turn'),
    ).toThrow();
    expect(() =>
      projection.validate({ ...idle, kind: 'session.thread_status_terminated' }, 'evt_turn'),
    ).toThrow();
    expect(projection.validate(idle, 'evt_turn').payload.stop_reason).toBe('interrupted');
    expect(() =>
      projection.validate(
        {
          ...named,
          id: 'evt_bad_name',
          payload: { ...named.payload, agent_name: 'x'.repeat(257) },
        },
        'evt_turn',
      ),
    ).toThrow();
  });

  it('admits a bounded parent settlement notice, never a child-authored sent message', () => {
    const projection = new DshEventProjection('ws', 'ses', []);
    projection.validate(declaration(child), 'evt_turn');
    const notice = {
      id: 'evt_notice',
      subpath: '',
      kind: 'agent.thread_message_received',
      payload: {
        turn_event_id: 'evt_turn',
        native_message_id: 'notice-1',
        message_origin: 'runtime_settlement',
        sender_subpath: child,
        recipient_subpath: '',
        content: [{ type: 'text', text: 'Child ' + nativeThreadId(child) + ' settled.' }],
      },
    };
    expect(() =>
      projection.validate({ ...notice, kind: 'agent.thread_message_sent' }, 'evt_turn'),
    ).toThrow();
    expect(() =>
      projection.validate(
        {
          ...notice,
          payload: { ...notice.payload, content: [{ type: 'text', text: 'private summary' }] },
        },
        'evt_turn',
      ),
    ).toThrow();
    expect(() =>
      projection.validate(
        { ...notice, payload: { ...notice.payload, run_end_id: 'invented' } },
        'evt_turn',
      ),
    ).toThrow();
    const admitted = projection.validate(notice, 'evt_turn');
    expect(admitted.payload).toMatchObject({
      sender_thread_id: stableSessionThreadId('ws', 'ses', child),
      recipient_thread_id: stableSessionThreadId('ws', 'ses', ''),
    });
    expect(() => projection.validate(notice, 'evt_turn')).toThrow();
  });

  it('allowlists only bounded compaction attribution without raw content', () => {
    const projection = new DshEventProjection('ws', 'ses', []);
    const compacted = {
      id: 'evt_compacted',
      subpath: '',
      kind: 'agent.thread_context_compacted',
      payload: {
        turn_event_id: 'evt_turn',
        native_session_id: 'root',
        native_compaction_id: 'compact-1',
        native_source: { session_id: 'root', seq: 13 },
        source_event_seqs: [10, 11, 13],
      },
    };
    expect(() =>
      projection.validate(
        { ...compacted, payload: { ...compacted.payload, summary: 'private' } },
        'evt_turn',
      ),
    ).toThrow();
    expect(() =>
      projection.validate(
        { ...compacted, payload: { ...compacted.payload, source_event_seqs: [13, 11, 10] } },
        'evt_turn',
      ),
    ).toThrow();
    expect(projection.validate(compacted, 'evt_turn').kind).toBe('agent.thread_context_compacted');
  });

  it('restores private parent-first lineage without replaying creation events', () => {
    const projection = new DshEventProjection('ws', 'ses', [
      'spawn',
      'fork',
      'send_message',
      'interrupt_agent',
    ]);
    projection.restoreTree([
      { subpath: child, parentSubpath: '' },
      { subpath: grandchild, parentSubpath: child },
    ]);
    expect(
      projection.assertOrigin({
        eventId: 'evt_restored_tool',
        subpath: grandchild,
        parentSubpath: child,
      }).subpath,
    ).toBe(grandchild);
    expect(() => projection.restoreTree([])).toThrow();
    expect(() =>
      new DshEventProjection('ws', 'ses', []).restoreTree([
        { subpath: grandchild, parentSubpath: child },
      ]),
    ).toThrow();
  });

  it('derives stable public IDs and preserves nested lineage', () => {
    const projection = new DshEventProjection('ws', 'ses', []);
    projection.validate(declaration(child), 'evt_turn');
    const event = projection.validate(declaration(grandchild, child), 'evt_turn');
    expect(event.payload).toMatchObject({
      session_thread_id: stableSessionThreadId('ws', 'ses', grandchild),
      parent_thread_id: stableSessionThreadId('ws', 'ses', child),
    });
    expect(projection.subtree(child)).toEqual([child, grandchild]);
    expect(nativeThreadId(child)).toBe('11111111-1111-4111-8111-111111111111');
    expect(() => nativeThreadId('subagents/sth_public')).toThrow();
  });

  it('rejects undeclared origins, wrong source and unauthorized canonical kinds', () => {
    const projection = new DshEventProjection('ws', 'ses', []);
    expect(() => projection.assertOrigin({ eventId: 'evt_tool', subpath: child })).toThrow();
    expect(() => projection.validate(declaration(grandchild, child), 'evt_turn')).toThrow();
    expect(() => projection.validate(declaration(child), 'evt_other')).toThrow();
    expect(() =>
      projection.validate(
        {
          id: 'evt_usage',
          subpath: '',
          kind: 'agent.usage',
          payload: { turn_event_id: 'evt_turn' },
        },
        'evt_turn',
      ),
    ).toThrow();
    expect(() =>
      projection.validate(
        {
          id: 'evt_bash',
          subpath: '',
          kind: 'agent.tool_use',
          payload: { name: 'bash', input: {}, turn_event_id: 'evt_turn' },
        },
        'evt_turn',
      ),
    ).toThrow();
  });

  it('projects thinking only on the originating thread and validates self-only summary counts', () => {
    const projection = new DshEventProjection('ws', 'ses', []);
    projection.validate(declaration(child), 'evt_turn');
    const thinking = projection.validate(
      {
        id: 'evt_thinking',
        subpath: child,
        kind: 'agent.thinking',
        payload: { turn_event_id: 'evt_turn', content: [{ type: 'thinking', thinking: 'reason' }] },
      },
      'evt_turn',
    );
    expect(thinking.subpath).toBe(child);
    projection.validate(
      {
        id: 'evt_model',
        subpath: child,
        kind: 'span.model_request_start',
        payload: { turn_event_id: 'evt_turn', model_observation_kind: 'turn_model_summary' },
      },
      'evt_turn',
    );
    const end = {
      id: 'evt_end',
      subpath: child,
      kind: 'span.model_request_end',
      payload: {
        turn_event_id: 'evt_turn',
        model_observation_kind: 'turn_model_summary',
        model_request_start_id: 'evt_model',
        is_error: false,
        model_usage: {
          input_tokens: 2,
          output_tokens: 1,
          cache_read_input_tokens: 0,
          cache_creation_input_tokens: 0,
        },
      },
    };
    expect(() => projection.validate({ ...end, subpath: '' }, 'evt_turn')).toThrow();
    for (const invalid of [-1, 0.5, 1, null, '0'])
      expect(() =>
        projection.validate(
          {
            ...end,
            payload: {
              ...end.payload,
              model_usage: { ...end.payload.model_usage, cache_creation_input_tokens_1h: invalid },
            },
          },
          'evt_turn',
        ),
      ).toThrow('Invalid DSH model usage');
    expect(() => projection.validate(end, 'evt_turn')).not.toThrow();
    projection.beginTurn();
    projection.validate(
      {
        id: 'evt_model',
        subpath: child,
        kind: 'span.model_request_start',
        payload: { turn_event_id: 'evt_turn', model_observation_kind: 'turn_model_summary' },
      },
      'evt_turn',
    );
    const split = {
      ...end,
      payload: {
        ...end.payload,
        model_usage: {
          ...end.payload.model_usage,
          cache_creation_input_tokens: 3,
          cache_creation_input_tokens_1h: 2,
        },
      },
    };
    expect(projection.validate(split, 'evt_turn').payload).toMatchObject({
      model_usage: { cache_creation_input_tokens: 3, cache_creation_input_tokens_1h: 2 },
    });
  });
});
