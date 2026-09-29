// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  AgentRuntimeSignalKind,
  CANONICAL_AGENT_EVENT_KINDS,
  InternalTranscriptEventKind,
} from '@orca/agent-event-contract';
import {
  CLAUDE_SESSION_EVENT_TYPES,
  eventBatchIdempotencyKeys,
  httpEventToProto,
  isPublicTranscriptEvent,
  oidcTranscriptUserId,
  protoToHttpEvent,
  toPublicHttpEvent,
  transcriptEventVisibility,
} from '../../src/domain/events.js';

describe('event mapping', () => {
  it('includes every canonical producer kind but excludes runtime and marker kinds', () => {
    for (const kind of CANONICAL_AGENT_EVENT_KINDS) {
      expect(CLAUDE_SESSION_EVENT_TYPES.has(kind)).toBe(true);
    }
    expect(CLAUDE_SESSION_EVENT_TYPES.has(AgentRuntimeSignalKind.usage)).toBe(false);
    expect(CLAUDE_SESSION_EVENT_TYPES.has(InternalTranscriptEventKind.userEventProcessed)).toBe(
      false,
    );
    expect([...CLAUDE_SESSION_EVENT_TYPES]).toEqual([
      'user.message',
      'user.interrupt',
      'user.tool_confirmation',
      'user.custom_tool_result',
      'user.define_outcome',
      'user.tool_result',
      'system.message',
      'agent.custom_tool_use',
      'agent.message',
      'agent.thinking',
      'agent.mcp_tool_use',
      'agent.mcp_tool_result',
      'agent.tool_use',
      'agent.tool_result',
      'agent.thread_message_received',
      'agent.thread_message_sent',
      'agent.thread_context_compacted',
      'session.error',
      'session.status_rescheduled',
      'session.status_running',
      'session.status_idle',
      'session.status_terminated',
      'session.thread_created',
      'session.thread_status_running',
      'session.thread_status_idle',
      'session.thread_status_terminated',
      'session.thread_status_rescheduled',
      'session.deleted',
      'session.updated',
      'span.model_request_start',
      'span.model_request_end',
      'span.outcome_evaluation_start',
      'span.outcome_evaluation_ongoing',
      'span.outcome_evaluation_end',
    ]);
  });

  it('derives opaque issuer-qualified OIDC Transcript attribution', () => {
    const issuerOne = 'https://issuer-one.example';
    const issuerTwo = 'https://issuer-two.example';
    const subject = 'user@example.com';
    const first = oidcTranscriptUserId(issuerOne, subject);

    expect(first).toMatch(/^oidc_user_[0-9a-f]{64}$/);
    expect(oidcTranscriptUserId(issuerOne, subject)).toBe(first);
    expect(oidcTranscriptUserId(issuerTwo, subject)).not.toBe(first);
    expect(first).not.toContain(issuerOne);
    expect(first).not.toContain(subject);
  });

  it('stamps a non-empty trusted user ID on client events only', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      userId: 'user_verified',
      idempotencyKey: 'idem-user',
      input: { type: 'user.message', content: [{ type: 'text', text: 'hello' }] },
    });

    expect(event.userId).toBe('user_verified');
    expect(protoToHttpEvent(event)).not.toHaveProperty('userId');
  });

  it('ignores a trusted user ID supplied for harness events', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'harness',
      userId: 'user_verified',
      idempotencyKey: 'idem-harness-user',
      input: { type: 'agent.message', content: [{ type: 'text', text: 'hello' }] },
    });

    expect(event).not.toHaveProperty('userId');
  });

  it('omits missing and empty trusted user IDs', () => {
    for (const userId of [undefined, '']) {
      const event = httpEventToProto({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        producedBy: 'client',
        userId,
        idempotencyKey: 'idem-optional-user',
        input: { type: 'user.message', content: [{ type: 'text', text: 'hello' }] },
      });

      expect(event).not.toHaveProperty('userId');
    }
  });

  it('keeps caller-supplied user_id in payload separate from trusted attribution', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      userId: 'user_verified',
      idempotencyKey: 'idem-spoofed-user',
      input: {
        type: 'user.message',
        content: [{ type: 'text', text: 'hello' }],
        user_id: 'user_payload_spoof',
      },
    });

    expect(event.userId).toBe('user_verified');
    expect(JSON.parse(Buffer.from(event.payload).toString('utf8'))).toEqual({
      type: 'user.message',
      content: [{ type: 'text', text: 'hello' }],
      user_id: 'user_payload_spoof',
      processed_at: null,
    });
    expect(protoToHttpEvent(event)).toMatchObject({ user_id: 'user_payload_spoof' });
    expect(protoToHttpEvent(event)).not.toHaveProperty('userId');
  });

  it('bounds a batch request ID before copying it into every event', () => {
    const oversizedRequestId = 'r'.repeat(700 * 1024);
    const keys = eventBatchIdempotencyKeys(oversizedRequestId, 3);

    expect(Buffer.byteLength(oversizedRequestId) * keys.length).toBeGreaterThan(2 * 1024 * 1024);
    expect(keys).toEqual([
      expect.stringMatching(/^sha256:[0-9a-f]{64}:0$/),
      expect.stringMatching(/^sha256:[0-9a-f]{64}:1$/),
      expect.stringMatching(/^sha256:[0-9a-f]{64}:2$/),
    ]);
    expect(Buffer.byteLength(keys.join(''))).toBeLessThan(256);
    expect(eventBatchIdempotencyKeys('request-1', 1)).toEqual([
      expect.stringMatching(/^sha256:[0-9a-f]{64}:0$/),
    ]);
    expect(eventBatchIdempotencyKeys(undefined, 2)).toEqual(['', '']);
  });

  it('round-trips a user.message', () => {
    const incoming = { type: 'user.message', content: [{ type: 'text', text: 'hello' }] };
    const now = new Date('2026-07-10T12:00:00.000Z');
    const e = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: 'idem-1',
      input: incoming,
      now: () => now,
    });
    expect(e.kind).toBe('user.message');
    expect(e.id).toMatch(/^evt_[0-9a-f-]+$/);
    expect(e.subpath).toBe('');
    expect(e.workspaceId).toBe('ws_x');
    expect(e.sessionId).toBe('ses_y');
    expect(e.producedBy).toBe('client');
    expect(e.idempotencyKey).toBe('idem-1');
    expect(JSON.parse(Buffer.from(e.payload).toString('utf8'))).toEqual({
      ...incoming,
      processed_at: null,
    });

    const back = protoToHttpEvent({ ...e, seq: 7 });
    expect(back.id).toBe(e.id);
    expect(back.type).toBe('user.message');
    expect(back.content).toEqual(incoming.content);
    expect(back.seq).toBe('7');
    expect(back.processed_at).toBeNull();
    expect(Object.hasOwn(back, 'subpath')).toBe(false);
  });

  it('overrides payload envelope fields with canonical metadata', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: 'idem-1',
      now: () => new Date('2026-07-10T12:00:00.000Z'),
      input: {
        type: 'user.message',
        content: [{ type: 'text', text: 'hello' }],
        processed_at: null,
        produced_at: 'spoofed',
      },
    });

    const output = protoToHttpEvent({ ...event, seq: 7 });
    expect(output.processed_at).toBeNull();
    expect(output.produced_at).toBe('2026-07-10T12:00:00.000Z');
  });

  it('canonicalizes legacy nested agent.message payloads on read', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'harness',
      idempotencyKey: 'legacy-1',
      now: () => new Date('2026-07-10T12:00:00.000Z'),
      input: {
        type: 'agent.message',
        message: {
          content: [
            { type: 'text', text: 'legacy text' },
            { type: 'tool_use', id: 'toolu_ignored', name: 'Bash', input: {} },
          ],
        },
        processed_at: null,
        uuid: 'raw-uuid',
        session_id: 'raw-session',
        parent_tool_use_id: 'toolu_parent',
        source_event_id: 'evt_source',
        session_thread_id: 'sth_worker',
      },
    });

    const output = protoToHttpEvent({ ...event, seq: 7 });
    expect(output.content).toEqual([{ type: 'text', text: 'legacy text' }]);
    expect(Object.hasOwn(output, 'message')).toBe(false);
    expect(Object.hasOwn(output, 'uuid')).toBe(false);
    expect(Object.hasOwn(output, 'session_id')).toBe(false);
    expect(output.parent_tool_use_id).toBe('toolu_parent');
    expect(output.source_event_id).toBe('evt_source');
    expect(output.session_thread_id).toBe('sth_worker');
    expect(output.processed_at).toBe('2026-07-10T12:00:00.000Z');
  });

  it('canonicalizes top-level agent.message payloads without leaking SDK envelope fields', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'harness',
      idempotencyKey: 'canonical-1',
      input: {
        type: 'agent.message',
        content: [
          { type: 'text', text: 'canonical text' },
          { type: 'tool_use', id: 'toolu_ignored', name: 'Bash', input: {} },
        ],
        message: { content: [{ type: 'text', text: 'stale nested text' }] },
        uuid: 'raw-uuid',
        session_id: 'raw-session',
        parent_tool_use_id: 'toolu_parent',
        source_event_id: 'evt_source',
      },
    });

    const output = protoToHttpEvent({ ...event, seq: 7 });
    expect(output.content).toEqual([{ type: 'text', text: 'canonical text' }]);
    expect(Object.hasOwn(output, 'message')).toBe(false);
    expect(Object.hasOwn(output, 'uuid')).toBe(false);
    expect(Object.hasOwn(output, 'session_id')).toBe(false);
    expect(output.parent_tool_use_id).toBe('toolu_parent');
    expect(output.source_event_id).toBe('evt_source');
  });

  it('preserves client passthrough fields on top-level user.message content', () => {
    const incoming = {
      type: 'user.message',
      content: [{ type: 'text', text: 'hello' }],
      message: {
        content: [{ type: 'text', text: 'do not canonicalize this nested value' }],
        client_field: 'preserve me',
      },
      uuid: 'client-uuid',
      session_id: 'client-session',
    };
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: 'idem-client-fields',
      input: incoming,
    });

    const output = protoToHttpEvent({ ...event, seq: 7 });
    expect(output.content).toEqual(incoming.content);
    expect(output.message).toEqual(incoming.message);
    expect(output.uuid).toBe('client-uuid');
    expect(output.session_id).toBe('client-session');
  });

  it('canonicalizes legacy nested user.message content without dropping passthrough fields', () => {
    const payload = {
      type: 'user.message',
      message: {
        content: [{ type: 'text', text: 'legacy client message' }],
        client_field: 'preserve me',
      },
      uuid: 'client-uuid',
      session_id: 'client-session',
    };
    const output = protoToHttpEvent({
      id: 'evt_legacy_user',
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      seq: 7,
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'client',
      kind: 'user.message',
      payload: Buffer.from(JSON.stringify(payload), 'utf8'),
      idempotencyKey: '',
    });

    expect(output.content).toEqual(payload.message.content);
    expect(output.message).toEqual(payload.message);
    expect(output.uuid).toBe('client-uuid');
    expect(output.session_id).toBe('client-session');
  });

  it('falls back to canonical metadata for a legacy null payload', () => {
    const output = protoToHttpEvent({
      id: 'evt_null_payload',
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      seq: 9,
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'harness',
      kind: 'agent.message',
      payload: Buffer.from('null', 'utf8'),
      idempotencyKey: '',
    });

    expect(output).toMatchObject({
      id: 'evt_null_payload',
      type: 'agent.message',
      processed_at: '2026-07-10T12:00:00.000Z',
      produced_at: '2026-07-10T12:00:00.000Z',
      produced_by: 'harness',
      seq: '9',
    });
    expect(output.content).toBeUndefined();
  });

  it('treats legacy client events without queue metadata as processed', () => {
    const output = protoToHttpEvent({
      id: 'evt_legacy_client',
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      seq: 10,
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'client',
      kind: 'user.message',
      payload: Buffer.from(
        JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'old' }] }),
        'utf8',
      ),
      idempotencyKey: '',
    });

    expect(output.processed_at).toBe('2026-07-10T12:00:00.000Z');
  });

  it('honors caller-provided event id', () => {
    const e = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: '',
      input: {
        id: 'evt_caller',
        type: 'user.tool_confirmation',
        tool_use_id: 'toolu_1',
        approved: true,
      },
    });
    expect(e.id).toBe('evt_caller');
  });

  it('replaces a malformed caller event id with a canonical generated id', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: '',
      input: { id: 'evt_', type: 'user.message', content: [{ type: 'text', text: 'hello' }] },
    });

    expect(event.id).toMatch(/^evt_.+/);
    expect(event.id).not.toBe('evt_');
  });

  it('accepts the Claude user.tool_confirmation result shape', () => {
    for (const input of [
      { type: 'user.tool_confirmation', tool_use_id: 'toolu_1', result: 'allow' },
      {
        type: 'user.tool_confirmation',
        tool_use_id: 'toolu_1',
        result: 'deny',
        deny_message: 'no',
      },
    ]) {
      expect(() =>
        httpEventToProto({
          workspaceId: 'ws_x',
          sessionId: 'ses_y',
          producedBy: 'client',
          idempotencyKey: '',
          input,
        }),
      ).not.toThrow();
    }
  });

  it('rejects an invalid user.tool_confirmation result value', () => {
    expect(() =>
      httpEventToProto({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        producedBy: 'client',
        idempotencyKey: '',
        input: { type: 'user.tool_confirmation', tool_use_id: 'toolu_1', result: 'maybe' },
      }),
    ).toThrow(/result/);
  });

  it('rejects system.message until the harness can apply it', () => {
    expect(() =>
      httpEventToProto({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        producedBy: 'client',
        idempotencyKey: '',
        input: { type: 'system.message', content: [{ type: 'text', text: 'be concise' }] },
      }),
    ).toThrow(/unsupported client event\.type: system\.message/);
  });

  it('accepts system.message and user.tool_result in Claude validation mode', () => {
    for (const input of [
      { type: 'system.message', content: [{ type: 'text', text: 'be concise' }] },
      { type: 'user.tool_result', tool_use_id: 'evt_tool' },
    ]) {
      expect(() =>
        httpEventToProto({
          workspaceId: 'ws_x',
          sessionId: 'ses_y',
          producedBy: 'client',
          idempotencyKey: '',
          input,
          validationMode: 'claude',
        }),
      ).not.toThrow();
    }
  });

  it('accepts Claude nullable outcome iterations and confirmation messages', () => {
    for (const input of [
      {
        type: 'user.define_outcome',
        description: 'ship it',
        rubric: { type: 'text', content: 'done' },
        max_iterations: null,
      },
      {
        type: 'user.tool_confirmation',
        tool_use_id: 'evt_tool',
        result: 'deny',
        deny_message: null,
      },
    ]) {
      expect(() =>
        httpEventToProto({
          workspaceId: 'ws_x',
          sessionId: 'ses_y',
          producedBy: 'client',
          idempotencyKey: '',
          input,
          validationMode: 'claude',
        }),
      ).not.toThrow();
    }
  });

  it('marks user.define_outcome accepted at append time', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: '',
      input: {
        type: 'user.define_outcome',
        description: 'ship it',
        rubric: { type: 'text', content: 'done' },
      },
      now: () => new Date('2026-07-23T01:02:03.000Z'),
      validationMode: 'claude',
    });

    expect(protoToHttpEvent(event).processed_at).toBe('2026-07-23T01:02:03.000Z');
  });

  it('strips Orca transcript envelope fields from the Claude event view', () => {
    expect(
      toPublicHttpEvent(
        {
          id: 'evt_1',
          type: 'user.message',
          content: [{ type: 'text', text: 'hello' }],
          processed_at: null,
          produced_at: '2026-07-10T12:00:00.000Z',
          produced_by: 'client',
          seq: '12',
          subpath: 'threads/child',
        },
        false,
      ),
    ).toEqual({
      id: 'evt_1',
      type: 'user.message',
      content: [{ type: 'text', text: 'hello' }],
      processed_at: null,
    });
  });

  it('keeps turn-summary metadata private to the Orca event view', () => {
    const startEvent = {
      id: 'evt_model_start',
      type: 'span.model_request_start',
      processed_at: '2026-07-10T12:00:00.000Z',
      produced_at: '2026-07-10T12:00:00.000Z',
      produced_by: 'harness',
      seq: '14',
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-opus',
    };
    expect(toPublicHttpEvent(startEvent, false)).toEqual({
      id: 'evt_model_start',
      type: 'span.model_request_start',
      processed_at: '2026-07-10T12:00:00.000Z',
    });
    expect(toPublicHttpEvent(startEvent, true)).toBe(startEvent);

    const event = {
      id: 'evt_model_end',
      type: 'span.model_request_end',
      processed_at: '2026-07-10T12:00:00.000Z',
      produced_at: '2026-07-10T12:00:00.000Z',
      produced_by: 'harness',
      seq: '15',
      model_request_start_id: 'evt_model_start',
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model_usage: {
        input_tokens: 1,
        output_tokens: 2,
        cache_creation_input_tokens: 3,
        cache_read_input_tokens: 4,
      },
      is_error: false,
      model: 'claude-opus',
      total_cost_usd: 0.125,
    };

    expect(toPublicHttpEvent(event, false)).toEqual({
      id: 'evt_model_end',
      type: 'span.model_request_end',
      processed_at: '2026-07-10T12:00:00.000Z',
      model_request_start_id: 'evt_model_start',
      model_usage: event.model_usage,
      is_error: false,
    });
    expect(toPublicHttpEvent(event, true)).toBe(event);
    expect(toPublicHttpEvent(event, true)).toMatchObject({
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-opus',
      total_cost_usd: 0.125,
    });
  });

  it('normalizes legacy session errors to the Claude nested retry shape', () => {
    expect(
      toPublicHttpEvent(
        {
          id: 'evt_error',
          type: 'session.error',
          error: { type: 'processing_error', message: 'runner failed' },
          retry_status: { will_retry: false },
          processed_at: '2026-07-10T12:00:00.000Z',
          produced_at: '2026-07-10T12:00:00.000Z',
          produced_by: 'harness',
          seq: '13',
        },
        false,
      ),
    ).toEqual({
      id: 'evt_error',
      type: 'session.error',
      processed_at: '2026-07-10T12:00:00.000Z',
      error: {
        type: 'unknown_error',
        message: 'runner failed',
        retry_status: { type: 'exhausted' },
      },
    });
  });

  it('preserves required fields on discriminated session error variants', () => {
    expect(
      toPublicHttpEvent(
        {
          id: 'evt_error',
          type: 'session.error',
          error: {
            type: 'credential_host_unreachable_error',
            message: 'blocked',
            retry_status: { type: 'terminal' },
            credential_id: 'cred_1',
            vault_id: 'vlt_1',
            internal_detail: 'do not expose',
          },
          processed_at: '2026-07-10T12:00:00.000Z',
          produced_at: '2026-07-10T12:00:00.000Z',
          produced_by: 'harness',
          seq: '14',
        },
        false,
      ),
    ).toEqual({
      id: 'evt_error',
      type: 'session.error',
      processed_at: '2026-07-10T12:00:00.000Z',
      error: {
        type: 'credential_host_unreachable_error',
        message: 'blocked',
        retry_status: { type: 'terminal' },
        credential_id: 'cred_1',
        vault_id: 'vlt_1',
      },
    });
  });

  it('downgrades incomplete discriminated session errors to unknown_error', () => {
    expect(
      toPublicHttpEvent(
        {
          id: 'evt_error',
          type: 'session.error',
          error: {
            type: 'mcp_connection_failed_error',
            message: 'connection failed without a server name',
            retry_status: { type: 'exhausted' },
          },
          processed_at: '2026-07-10T12:00:00.000Z',
          produced_at: '2026-07-10T12:00:00.000Z',
          produced_by: 'harness',
          seq: '14',
        },
        false,
      ),
    ).toEqual({
      id: 'evt_error',
      type: 'session.error',
      processed_at: '2026-07-10T12:00:00.000Z',
      error: {
        type: 'unknown_error',
        message: 'connection failed without a server name',
        retry_status: { type: 'exhausted' },
      },
    });
  });

  it('uses tool event ids as result references and normalizes string result content', () => {
    const use = toPublicHttpEvent(
      {
        id: 'evt_internal_use',
        type: 'agent.mcp_tool_use',
        tool_use_id: 'toolu_sdk_1',
        name: 'search',
        input: { q: 'orca' },
        mcp_server_name: 'docs',
        source_event_id: 'evt_source',
        processed_at: '2026-07-10T12:00:00.000Z',
        produced_at: '2026-07-10T12:00:00.000Z',
        produced_by: 'harness',
        seq: '15',
      },
      false,
    );
    const result = toPublicHttpEvent(
      {
        id: 'evt_internal_result',
        type: 'agent.mcp_tool_result',
        tool_use_id: 'toolu_sdk_1',
        content: 'done',
        mcp_server_name: 'internal-only',
        processed_at: '2026-07-10T12:00:01.000Z',
        produced_at: '2026-07-10T12:00:01.000Z',
        produced_by: 'harness',
        seq: '16',
      },
      false,
    );

    expect(result).toEqual({
      id: 'evt_internal_result',
      type: 'agent.mcp_tool_result',
      processed_at: '2026-07-10T12:00:01.000Z',
      mcp_tool_use_id: use.id,
      content: [{ type: 'text', text: 'done' }],
    });
    expect(use).not.toHaveProperty('tool_use_id');
    expect(use).not.toHaveProperty('source_event_id');
  });

  it('canonicalizes thread message direction, ids, and rich content', () => {
    const content = [
      { type: 'text', text: 'see attachment' },
      { type: 'image', source: { type: 'url', url: 'https://example.test/image.png' } },
      { type: 'document', source: { type: 'text', media_type: 'text/plain', data: 'doc' } },
    ];
    expect(
      toPublicHttpEvent(
        {
          id: 'evt_thread',
          type: 'agent.thread_message_sent',
          session_thread_id: 'sth_child',
          source_event_type: 'agent.message',
          content,
          processed_at: '2026-07-10T12:00:00.000Z',
          produced_at: '2026-07-10T12:00:00.000Z',
          produced_by: 'harness',
          seq: '17',
        },
        false,
      ),
    ).toEqual({
      id: 'evt_thread',
      type: 'agent.thread_message_sent',
      processed_at: '2026-07-10T12:00:00.000Z',
      to_session_thread_id: 'sth_child',
      content,
    });
  });

  it('strips transcript metadata from delta preview envelopes', () => {
    expect(
      toPublicHttpEvent(
        {
          id: 'evt_transport',
          type: 'event_start',
          event: { id: 'evt_preview', type: 'agent.message', content: [] },
          processed_at: '2026-07-10T12:00:00.000Z',
          produced_at: '2026-07-10T12:00:00.000Z',
          produced_by: 'harness',
          seq: '18',
        },
        false,
      ),
    ).toEqual({
      type: 'event_start',
      event: { id: 'evt_preview', type: 'agent.message' },
    });
    expect(
      toPublicHttpEvent(
        {
          id: 'evt_transport_delta',
          type: 'event_delta',
          event_id: 'evt_preview',
          delta: { type: 'content_delta', content: { type: 'text', text: 'hi' } },
          processed_at: '2026-07-10T12:00:00.000Z',
          produced_at: '2026-07-10T12:00:00.000Z',
          produced_by: 'harness',
          seq: '19',
        },
        false,
      ),
    ).toEqual({
      type: 'event_delta',
      event_id: 'evt_preview',
      delta: { type: 'content_delta', content: { type: 'text', text: 'hi' } },
    });
  });

  it('treats non-driving public extension events as immediately processed', () => {
    const event = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: '',
      now: () => new Date('2026-07-10T12:00:00.000Z'),
      input: { type: 'orca.extension_event', payload: { ok: true } },
    });

    expect(JSON.parse(Buffer.from(event.payload).toString('utf8')).processed_at).toBe(
      '2026-07-10T12:00:00.000Z',
    );
    expect(protoToHttpEvent({ ...event, seq: 7 }).processed_at).toBe('2026-07-10T12:00:00.000Z');
  });

  it('normalizes the deprecated approved alias to result in the persisted payload', () => {
    const e = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: '',
      input: { type: 'user.tool_confirmation', tool_use_id: 'toolu_1', approved: false },
    });
    const payload = JSON.parse(Buffer.from(e.payload).toString('utf8')) as Record<string, unknown>;
    expect(payload.result).toBe('deny');
    expect(payload.approved).toBe(false);
  });

  it('accepts Claude custom_tool_result ids and normalizes the deprecated tool_use_id alias', () => {
    expect(() =>
      httpEventToProto({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          type: 'user.custom_tool_result',
          custom_tool_use_id: 'evt_custom_1',
          content: [{ type: 'text', text: 'ok' }],
        },
      }),
    ).not.toThrow();

    const legacy = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: '',
      input: {
        type: 'user.custom_tool_result',
        tool_use_id: 'evt_custom_legacy',
        content: [{ type: 'text', text: 'ok' }],
      },
    });
    const payload = JSON.parse(Buffer.from(legacy.payload).toString('utf8')) as Record<
      string,
      unknown
    >;
    expect(payload.custom_tool_use_id).toBe('evt_custom_legacy');
    expect(payload.tool_use_id).toBe('evt_custom_legacy');
  });

  it('stamps subpath if input has one (subagents)', () => {
    const e = httpEventToProto({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      producedBy: 'client',
      idempotencyKey: '',
      input: {
        type: 'user.message',
        subpath: 'subagents/x/0',
        content: [{ type: 'text', text: 'hello' }],
      },
    });
    expect(e.subpath).toBe('subagents/x/0');
  });

  it('rejects events with missing type', () => {
    expect(() =>
      httpEventToProto({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        producedBy: 'client',
        idempotencyKey: '',
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        input: { content: [] } as any,
      }),
    ).toThrow(/type.*required/);
  });

  it('rejects client-authored reserved harness events', () => {
    expect(() =>
      httpEventToProto({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        producedBy: 'client',
        idempotencyKey: '',
        input: { type: 'harness.claude.session_entry', sdk_entry: { type: 'assistant' } },
      }),
    ).toThrow(/reserved event\.type prefix/);
  });

  it('classifies harness replay state as internal transcript events', () => {
    expect(transcriptEventVisibility('harness.claude.session_entry')).toBe('internal');
    expect(transcriptEventVisibility('session.deferred_user_message')).toBe('internal');
    expect(transcriptEventVisibility('session.deferred_user_message_submitted')).toBe('internal');
    expect(transcriptEventVisibility('session.user_event_processed')).toBe('internal');
    expect(transcriptEventVisibility('session.user_event_completed')).toBe('internal');
    expect(isPublicTranscriptEvent({ kind: 'harness.claude.session_entry' })).toBe(false);
    expect(isPublicTranscriptEvent({ kind: 'session.deferred_user_message' })).toBe(false);
    expect(isPublicTranscriptEvent({ kind: 'session.user_event_completed' })).toBe(false);
    expect(isPublicTranscriptEvent({ kind: 'agent.message' })).toBe(true);
    expect(isPublicTranscriptEvent({ kind: 'session.resource_mounted' })).toBe(true);
    expect(isPublicTranscriptEvent({ kind: 'session.setup_failed' })).toBe(true);
  });
});
