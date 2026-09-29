// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  CLAUDE_SESSION_ENTRY_EVENT_KIND,
  entryToEvent,
  eventToEntry,
  isClaudeSessionEntryEvent,
  publicEntryToEvent,
} from '../../src/harness/claude/event-mapper.js';
import type { AgentEventId, AgentEventSubpath } from '@orca/agent-event-contract';

describe('Claude SDK SessionStoreEntry ↔ Event', () => {
  it('round-trips a user message entry', () => {
    const entry = {
      type: 'user',
      uuid: '01933d4a-1234-7000-8000-abcdef000001',
      content: [{ type: 'text', text: 'hi' }],
    };
    const e = entryToEvent({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      producedBy: 'harness',
      entry,
    });
    expect(e.id).toBe(entry.uuid);
    expect(e.kind).toBe(CLAUDE_SESSION_ENTRY_EVENT_KIND);
    expect(JSON.parse(Buffer.from(e.payload).toString('utf8'))).toEqual({
      type: CLAUDE_SESSION_ENTRY_EVENT_KIND,
      sdk_entry: entry,
    });
    expect(eventToEntry(e)).toEqual(entry);
  });

  it('mints a UUIDv7 if entry has none', () => {
    const e = entryToEvent({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      producedBy: 'harness',
      entry: { type: 'assistant', content: [] },
    });
    expect(e.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}/);
  });

  it('preserves subpath', () => {
    const e = entryToEvent({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: 'subagents/code-reviewer/0',
      producedBy: 'harness',
      entry: { type: 'user', content: [] },
    });
    expect(e.subpath).toBe('subagents/code-reviewer/0');
  });

  it('keeps native SDK IDs and legacy subpaths open', () => {
    const e = entryToEvent({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: 'threads/legacy-sdk-thread',
      producedBy: 'harness',
      entry: { type: 'assistant', uuid: 'native-sdk-uuid' },
    });

    expect(e.id).toBe('native-sdk-uuid');
    expect(e.subpath).toBe('threads/legacy-sdk-thread');
  });

  it('identifies only wrapped SDK session entries as replayable', () => {
    const replayable = entryToEvent({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      producedBy: 'harness',
      entry: { type: 'assistant', uuid: '01933d4a-1234-7000-8000-abcdef000002' },
    });
    const publicEvent = {
      ...replayable,
      kind: 'agent.message',
      payload: Buffer.from(JSON.stringify({ type: 'agent.message' }), 'utf8'),
    };

    expect(isClaudeSessionEntryEvent(replayable)).toBe(true);
    expect(isClaudeSessionEntryEvent(publicEvent)).toBe(false);
  });

  it('keeps public harness events out of the SDK replay envelope', () => {
    const event = publicEntryToEvent({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      producedBy: 'harness',
      eventId: 'evt_public_message',
      entry: {
        type: 'agent.message',
        uuid: '01933d4a-1234-7000-8000-abcdef000003',
        content: [{ type: 'text', text: 'hi' }],
      },
    });

    expect(event.id).toBe('evt_public_message');
    expect(event.kind).toBe('agent.message');
    expect(JSON.parse(Buffer.from(event.payload).toString('utf8'))).toEqual({
      type: 'agent.message',
      uuid: '01933d4a-1234-7000-8000-abcdef000003',
      content: [{ type: 'text', text: 'hi' }],
    });
    expect(isClaudeSessionEntryEvent(event)).toBe(false);
  });

  it('preserves managed-agent public event ids for blocking control events', () => {
    const event = publicEntryToEvent({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      producedBy: 'harness',
      eventId: 'evt_permission_123',
      entry: {
        id: 'evt_permission_123',
        type: 'agent.tool_use',
        name: 'mcp__orca__bash',
        input: { command: 'pwd' },
      },
    });

    expect(event.id).toBe('evt_permission_123');
    expect(event.kind).toBe('agent.tool_use');
  });

  it('prefers a valid envelope ID over payload IDs without changing payload bytes', () => {
    const entry = {
      type: 'agent.tool_use',
      uuid: 'evt_payload_uuid',
      id: 'evt_payload_id',
      tool_use_id: 'toolu_payload_relation',
      input: { command: 'pwd' },
    };
    const event = publicEntryToEvent({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      subpath: '',
      producedBy: 'harness',
      eventId: 'evt_envelope_id',
      entry,
    });

    expect(event.id).toBe('evt_envelope_id');
    expect(JSON.parse(Buffer.from(event.payload).toString('utf8'))).toEqual(entry);
  });

  it('rejects a missing envelope ID instead of deriving one from payload IDs', () => {
    expect(() =>
      publicEntryToEvent({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        subpath: '',
        producedBy: 'harness',
        eventId: undefined as never,
        entry: { type: 'agent.message', uuid: 'evt_payload_uuid', id: 'evt_payload_id' },
      }),
    ).toThrow('invalid public AgentEvent id');
  });

  it('rejects a malformed explicit public event envelope ID', () => {
    expect(() =>
      publicEntryToEvent({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        subpath: '',
        producedBy: 'harness',
        eventId: 'toolu_not_an_event_id' as AgentEventId,
        entry: { type: 'agent.message', uuid: 'evt_payload_uuid', id: 'evt_payload_id' },
      }),
    ).toThrow('invalid public AgentEvent id');
  });

  it('rejects a noncanonical public event subpath', () => {
    expect(() =>
      publicEntryToEvent({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        subpath: 'threads/legacy-sdk-thread' as AgentEventSubpath,
        producedBy: 'harness',
        eventId: 'evt_invalid_subpath',
        entry: { type: 'agent.message' },
      }),
    ).toThrow('invalid public AgentEvent subpath');
  });
});
