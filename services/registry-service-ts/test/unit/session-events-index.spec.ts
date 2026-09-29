// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  catchUpSessionEventsIndex,
  indexTranscriptEvents,
  indexTranscriptEventsFromStore,
  indexRowToHttpEvent,
  isValidSessionEventsCursor,
} from '../../src/events/session-events-index.js';
import { parseEventsLimit } from '../../src/api/sessions.routes.js';
import {
  legacyAgentMessageToolProjectionEvents,
  primaryThreadProjectionEvents,
  streamableEventsForSubpath,
} from '../../src/domain/thread-projection.js';
import { sessionEventsIndex } from '../../src/persistence/postgres/schema.js';

describe('session events index', () => {
  it('stores the authenticated guardrail subject only on client user events', async () => {
    const { db, insertedRows } = fakeDb();
    await indexTranscriptEvents(
      db,
      [
        makeEvent({
          id: 'evt_subject_user',
          producedBy: 'client',
          kind: 'user.message',
          payload: { type: 'user.message', content: [{ type: 'text', text: 'hello' }] },
        }),
        makeEvent({
          id: 'evt_subject_agent',
          producedBy: 'harness',
          kind: 'agent.message',
          payload: { type: 'agent.message', content: [{ type: 'text', text: 'hello' }] },
        }),
      ],
      { guardrailSubject: 'api-key:key_123' },
    );

    expect(insertedRows.find((row) => row.eventId === 'evt_subject_user')).toMatchObject({
      guardrailSubject: 'api-key:key_123',
    });
    expect(insertedRows.find((row) => row.eventId === 'evt_subject_agent')).not.toHaveProperty(
      'guardrailSubject',
    );
  });

  it('projects rescheduled lifecycle events from child threads to the primary thread', () => {
    const projections = primaryThreadProjectionEvents(
      makeEvent({
        id: 'evt_rescheduled',
        subpath: 'threads/sth_worker',
        producedBy: 'harness',
        kind: 'session.thread_status_rescheduled',
        payload: { type: 'session.thread_status_rescheduled' },
      }),
    );

    expect(projections).toHaveLength(1);
    expect(projections[0]).toMatchObject({
      subpath: '',
      kind: 'session.thread_status_rescheduled',
    });
  });

  it('bounds catch-up work to the requested page budget', async () => {
    const events = makeEvents(20);
    const readOptions: unknown[] = [];
    const { db, insertedRows } = fakeDb();
    const store = {
      read: async function* (
        _workspaceId: string,
        _sessionId: string,
        opts: { maxEvents: number },
      ): AsyncIterable<Event> {
        readOptions.push(opts);
        for (const event of events.slice(0, opts.maxEvents)) yield event;
      },
    } as unknown as TranscriptStore;

    const indexed = await catchUpSessionEventsIndex(db, store, 'ws_index', 'ses_index', {
      maxEvents: 5,
    });

    expect(indexed).toBe(5);
    expect(readOptions).toEqual([expect.objectContaining({ fromCursor: '', maxEvents: 5 })]);
    expect(insertedRows).toHaveLength(5);
  });

  it('normalizes events limit query values', () => {
    expect(parseEventsLimit(undefined)).toBe(100);
    expect(parseEventsLimit('')).toBe(100);
    expect(parseEventsLimit('nan')).toBe(100);
    expect(parseEventsLimit('0')).toBe(100);
    expect(parseEventsLimit('-1')).toBe(100);
    expect(parseEventsLimit('1.5')).toBe(100);
    expect(parseEventsLimit('2e3')).toBe(100);
    expect(parseEventsLimit('25')).toBe(25);
    expect(parseEventsLimit('2000')).toBe(1000);
  });

  it('indexes read-back transcript events with backend-assigned seq values', async () => {
    const readOptions: unknown[] = [];
    const { db, insertedRows } = fakeDb();
    const storeEvents = makeEvents(2).map((event, index) => ({
      ...event,
      seq: 41 + index,
    }));
    const store = {
      read: async function* (
        _workspaceId: string,
        _sessionId: string,
        opts: { fromCursor: string; maxEvents: number },
      ): AsyncIterable<Event> {
        readOptions.push(opts);
        for (const event of storeEvents) yield event;
      },
    } as unknown as TranscriptStore;

    const indexed = await indexTranscriptEventsFromStore(db, store, 'ws_index', 'ses_index', {
      fromCursor: '41',
      subpath: '*',
    });

    expect(indexed).toBe(2);
    expect(readOptions).toEqual([expect.objectContaining({ fromCursor: '41', maxEvents: 0 })]);
    expect(insertedRows).toEqual([
      expect.objectContaining({ eventId: 'evt_index_0', seq: 41 }),
      expect.objectContaining({ eventId: 'evt_index_1', seq: 42 }),
    ]);
  });

  it('filters bounded read-back indexing to requested event ids', async () => {
    const readOptions: unknown[] = [];
    const { db, insertedRows } = fakeDb();
    const storeEvents = [
      { ...makeEvents(1)[0]!, id: 'old_0', seq: 40 },
      { ...makeEvents(1)[0]!, id: 'evt_target_0', seq: 41 },
      { ...makeEvents(1)[0]!, id: 'old_1', seq: 42 },
      { ...makeEvents(1)[0]!, id: 'evt_target_1', seq: 43 },
    ];
    const store = {
      read: async function* (
        _workspaceId: string,
        _sessionId: string,
        opts: { fromCursor: string; maxEvents: number },
      ): AsyncIterable<Event> {
        readOptions.push(opts);
        for (const event of storeEvents.slice(0, opts.maxEvents)) yield event;
      },
    } as unknown as TranscriptStore;

    const indexed = await indexTranscriptEventsFromStore(db, store, 'ws_index', 'ses_index', {
      eventIds: ['evt_target_0', 'evt_target_1'],
      fromCursor: '40',
      maxEvents: 4,
      subpath: '*',
    });

    expect(indexed).toBe(2);
    expect(readOptions).toEqual([expect.objectContaining({ fromCursor: '40', maxEvents: 4 })]);
    expect(insertedRows).toEqual([
      expect.objectContaining({ eventId: 'evt_target_0', seq: 41 }),
      expect.objectContaining({ eventId: 'evt_target_1', seq: 43 }),
    ]);
  });

  it('indexes completed user-event markers as internal only', async () => {
    const { db, insertedRows } = fakeDb();
    await indexTranscriptEvents(db, [
      makeEvent({
        id: 'evt_completed_marker',
        producedBy: 'harness',
        kind: 'session.user_event_completed',
        payload: { user_event_id: 'evt_source' },
      }),
    ]);

    expect(insertedRows).toEqual([
      expect.objectContaining({
        eventId: 'evt_completed_marker',
        kind: 'session.user_event_completed',
        visibility: 'internal',
      }),
    ]);
  });

  it('indexes primary-thread projections for public subthread messages', async () => {
    const { db, insertedRows } = fakeDb();
    await indexTranscriptEvents(db, [
      makeEvent({
        id: 'evt_sub_agent_1',
        subpath: 'threads/sth_worker',
        seq: 50,
        producedBy: 'harness',
        kind: 'agent.message',
        payload: { type: 'agent.message', content: [{ type: 'text', text: 'done' }] },
      }),
    ]);

    expect(insertedRows).toEqual([
      expect.objectContaining({
        eventId: 'evt_sub_agent_1',
        subpath: 'threads/sth_worker',
        kind: 'agent.message',
        visibility: 'public',
      }),
      expect.objectContaining({
        subpath: '',
        kind: 'agent.thread_message_received',
        visibility: 'public',
        payload: expect.objectContaining({
          type: 'agent.thread_message_received',
          session_thread_id: expect.stringMatching(/^sth_/),
          source_event_id: 'evt_sub_agent_1',
          source_subpath: 'threads/sth_worker',
          content: [{ type: 'text', text: 'done' }],
        }),
      }),
    ]);
  });

  it('indexes thread-visible and primary projections for Claude SDK subpath entries', async () => {
    const { db, insertedRows } = fakeDb();
    await indexTranscriptEvents(db, [
      makeEvent({
        id: '01933d4a-1234-7000-8000-abcdef000002',
        subpath: 'threads/sth_worker',
        seq: 51,
        producedBy: 'harness',
        kind: 'harness.claude.session_entry',
        payload: {
          type: 'harness.claude.session_entry',
          sdk_entry: {
            type: 'assistant',
            parent_tool_use_id: 'toolu_parent',
            message: {
              content: [
                { type: 'thinking', thinking: 'internal' },
                { type: 'text', text: 'sdk done' },
                { type: 'tool_use', id: 'toolu_1', name: 'Bash', input: {} },
              ],
            },
            uuid: 'raw-uuid',
          },
        },
      }),
    ]);

    expect(insertedRows).toEqual([
      expect.objectContaining({
        eventId: '01933d4a-1234-7000-8000-abcdef000002',
        subpath: 'threads/sth_worker',
        kind: 'harness.claude.session_entry',
        visibility: 'internal',
      }),
      expect.objectContaining({
        subpath: 'threads/sth_worker',
        kind: 'agent.message',
        visibility: 'public',
        payload: expect.objectContaining({
          type: 'agent.message',
          source_event_id: '01933d4a-1234-7000-8000-abcdef000002',
          parent_tool_use_id: 'toolu_parent',
          content: [{ type: 'text', text: 'sdk done' }],
        }),
      }),
      expect.objectContaining({
        subpath: 'threads/sth_worker',
        kind: 'agent.tool_use',
        visibility: 'public',
        payload: expect.objectContaining({
          type: 'agent.tool_use',
          source_event_id: '01933d4a-1234-7000-8000-abcdef000002',
          parent_tool_use_id: 'toolu_parent',
          name: 'Bash',
          input: {},
          tool_use_id: 'toolu_1',
        }),
      }),
      expect.objectContaining({
        subpath: '',
        kind: 'agent.thread_message_received',
        visibility: 'public',
        payload: expect.objectContaining({
          type: 'agent.thread_message_received',
          source_event_id: '01933d4a-1234-7000-8000-abcdef000002',
          source_subpath: 'threads/sth_worker',
          parent_tool_use_id: 'toolu_parent',
          content: [{ type: 'text', text: 'sdk done' }],
        }),
      }),
    ]);
    for (const row of insertedRows.slice(1) as Array<{ payload: Record<string, unknown> }>) {
      expect(row.payload).not.toHaveProperty('message');
      expect(row.payload).not.toHaveProperty('uuid');
    }
    expect(
      (insertedRows as Array<{ projectionOrdinal: number }>).map((row) => row.projectionOrdinal),
    ).toEqual([0, 1, 2, 3]);
  });

  it('projects string-valued Claude SDK child prompts onto the primary stream', async () => {
    const source = makeEvent({
      id: '01933d4a-1234-7000-8000-abcdef000003',
      subpath: 'subagents/agent-worker',
      seq: 52,
      producedBy: 'harness',
      kind: 'harness.claude.session_entry',
      payload: {
        type: 'harness.claude.session_entry',
        sdk_entry: {
          type: 'user',
          parent_tool_use_id: 'toolu_delegate',
          message: {
            role: 'user',
            content: 'Return the worker marker.',
          },
          uuid: 'raw-user-uuid',
        },
      },
    });

    const streamed = streamableEventsForSubpath(source, '');
    expect(streamed).toHaveLength(1);
    expect(streamed[0]).toMatchObject({
      subpath: '',
      kind: 'agent.thread_message_sent',
    });
    expect(JSON.parse(Buffer.from(streamed[0]!.payload).toString('utf8'))).toMatchObject({
      type: 'agent.thread_message_sent',
      session_thread_id: expect.stringMatching(/^sth_/),
      source_event_id: '01933d4a-1234-7000-8000-abcdef000003',
      source_subpath: 'subagents/agent-worker',
      parent_tool_use_id: 'toolu_delegate',
      content: [{ type: 'text', text: 'Return the worker marker.' }],
    });

    const { db, insertedRows } = fakeDb();
    await indexTranscriptEvents(db, [source]);
    expect(insertedRows).toEqual([
      expect.objectContaining({
        eventId: '01933d4a-1234-7000-8000-abcdef000003',
        subpath: 'subagents/agent-worker',
        kind: 'harness.claude.session_entry',
        visibility: 'internal',
      }),
      expect.objectContaining({
        subpath: 'subagents/agent-worker',
        kind: 'user.message',
        visibility: 'public',
        payload: expect.objectContaining({
          type: 'user.message',
          parent_tool_use_id: 'toolu_delegate',
          content: [{ type: 'text', text: 'Return the worker marker.' }],
        }),
      }),
      expect.objectContaining({
        subpath: '',
        kind: 'agent.thread_message_sent',
        visibility: 'public',
        payload: expect.objectContaining({
          type: 'agent.thread_message_sent',
          session_thread_id: expect.stringMatching(/^sth_/),
          source_event_id: '01933d4a-1234-7000-8000-abcdef000003',
          source_subpath: 'subagents/agent-worker',
          parent_tool_use_id: 'toolu_delegate',
          content: [{ type: 'text', text: 'Return the worker marker.' }],
        }),
      }),
    ]);
  });

  it('serializes indexed events with canonical processed_at metadata', () => {
    const output = indexRowToHttpEvent({
      eventId: 'evt_index_1',
      kind: 'agent.message',
      payload: { type: 'spoofed', processed_at: null, produced_at: 'spoofed' },
      subpath: '',
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'harness',
      seq: 7,
    } as typeof sessionEventsIndex.$inferSelect);

    expect(output).toMatchObject({
      id: 'evt_index_1',
      type: 'agent.message',
      processed_at: '2026-07-10T12:00:00.000Z',
      produced_at: '2026-07-10T12:00:00.000Z',
      produced_by: 'harness',
      seq: '7',
    });
    expect(Object.hasOwn(output, 'subpath')).toBe(false);
  });

  it('keeps newly queued client events unprocessed', () => {
    const output = indexRowToHttpEvent({
      eventId: 'evt_queued_1',
      kind: 'user.message',
      payload: {
        type: 'user.message',
        content: [{ type: 'text', text: 'queued' }],
        processed_at: null,
      },
      processedAt: null,
      subpath: '',
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'client',
      seq: 7,
    } as typeof sessionEventsIndex.$inferSelect);

    expect(output.processed_at).toBeNull();
  });

  it('applies harness processing markers to queued events', async () => {
    const { db, updatedRows } = fakeDb();
    await indexTranscriptEvents(db, [
      makeEvent({
        id: 'evt_queued_1',
        producedBy: 'client',
        kind: 'user.message',
        payload: {
          type: 'user.message',
          content: [{ type: 'text', text: 'queued' }],
          processed_at: null,
        },
      }),
      makeEvent({
        id: 'evt_processed_marker',
        producedAt: '2026-07-10T12:00:01.000Z',
        producedBy: 'harness',
        kind: 'session.user_event_processed',
        payload: { user_event_id: 'evt_queued_1' },
      }),
    ]);

    expect(updatedRows).toHaveLength(1);
    expect(updatedRows[0]).toHaveProperty('processedAt');
    expect(updatedRows[0]).toHaveProperty('processedMarkerSeq');
  });

  it('reconciles a processing marker that is projected before its client event', async () => {
    const { db, updatedRows } = fakeDb();
    await indexTranscriptEvents(db, [
      makeEvent({
        id: 'evt_processed_marker',
        producedAt: '2026-07-10T12:00:01.000Z',
        producedBy: 'harness',
        kind: 'session.user_event_processed',
        payload: { user_event_id: 'evt_queued_1' },
      }),
    ]);
    await indexTranscriptEvents(db, [
      makeEvent({
        id: 'evt_queued_1',
        producedBy: 'client',
        kind: 'user.message',
        payload: {
          type: 'user.message',
          content: [{ type: 'text', text: 'queued' }],
          processed_at: null,
        },
      }),
    ]);

    // The marker projection attempts the update immediately; indexing the
    // target later runs the same earliest-marker reconciliation again.
    expect(updatedRows).toHaveLength(2);
  });

  it('coalesces duplicate processing markers onto the earliest transcript marker', async () => {
    const { db, updatedRows } = fakeDb();
    await indexTranscriptEvents(db, [
      makeEvent({
        id: 'evt_processed_marker_1',
        seq: 2,
        producedAt: '2026-07-10T12:00:01.000Z',
        producedBy: 'harness',
        kind: 'session.user_event_processed',
        payload: { user_event_id: 'evt_queued_1' },
      }),
      makeEvent({
        id: 'evt_processed_marker_2',
        seq: 3,
        producedAt: '2026-07-10T12:00:02.000Z',
        producedBy: 'harness',
        kind: 'session.user_event_processed',
        payload: { user_event_id: 'evt_queued_1' },
      }),
    ]);

    expect(updatedRows).toHaveLength(1);
  });

  it('falls back to canonical metadata for non-record indexed payloads', () => {
    const output = indexRowToHttpEvent({
      eventId: 'evt_array_payload',
      kind: 'agent.message',
      payload: ['unexpected'],
      subpath: '',
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'harness',
      seq: 7,
    } as typeof sessionEventsIndex.$inferSelect);

    expect(output).toMatchObject({
      id: 'evt_array_payload',
      type: 'agent.message',
      processed_at: '2026-07-10T12:00:00.000Z',
      produced_at: '2026-07-10T12:00:00.000Z',
      produced_by: 'harness',
      seq: '7',
    });
    expect(output).not.toHaveProperty('0');
    expect(output.content).toBeUndefined();
  });

  it('indexes non-record transcript payloads as minimal envelopes', async () => {
    const { db, insertedRows } = fakeDb();
    await indexTranscriptEvents(db, [
      makeEvent({
        id: 'evt_array_payload',
        kind: 'user.interrupt',
        payload: Buffer.from(JSON.stringify(['unexpected']), 'utf8'),
      }),
    ]);

    expect(insertedRows).toEqual([
      expect.objectContaining({
        eventId: 'evt_array_payload',
        payload: { type: 'user.interrupt' },
      }),
    ]);
  });

  it('canonicalizes legacy nested message payloads from index rows', () => {
    const output = indexRowToHttpEvent({
      eventId: 'evt_legacy_1',
      kind: 'agent.message',
      payload: {
        type: 'agent.message',
        message: {
          content: [
            { type: 'text', text: 'legacy indexed text' },
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
      subpath: 'threads/sth_worker',
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'harness',
      seq: 7,
    } as typeof sessionEventsIndex.$inferSelect);

    expect(output.content).toEqual([{ type: 'text', text: 'legacy indexed text' }]);
    expect(Object.hasOwn(output, 'message')).toBe(false);
    expect(Object.hasOwn(output, 'uuid')).toBe(false);
    expect(Object.hasOwn(output, 'session_id')).toBe(false);
    expect(output.parent_tool_use_id).toBe('toolu_parent');
    expect(output.source_event_id).toBe('evt_source');
    expect(output.session_thread_id).toBe('sth_worker');
    expect(output.processed_at).toBe('2026-07-10T12:00:00.000Z');
    expect(output.subpath).toBe('threads/sth_worker');
  });

  it('preserves legacy tool blocks until their standalone projections are backfilled', () => {
    const output = indexRowToHttpEvent({
      eventId: 'evt_legacy_pending',
      kind: 'agent.message',
      payload: {
        type: 'agent.message',
        message: {
          content: [
            { type: 'text', text: 'legacy indexed text' },
            { type: 'tool_use', id: 'toolu_pending', name: 'Bash', input: { cmd: 'pwd' } },
          ],
        },
      },
      projectionVersion: 0,
      subpath: '',
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'harness',
      seq: 7,
    } as typeof sessionEventsIndex.$inferSelect);

    expect(output.content).toEqual([
      { type: 'text', text: 'legacy indexed text' },
      { type: 'tool_use', id: 'toolu_pending', name: 'Bash', input: { cmd: 'pwd' } },
    ]);
  });

  it('reconstructs legacy tool projections in content order', () => {
    const projections = legacyAgentMessageToolProjectionEvents(
      makeEvent({
        id: 'evt_legacy_tools',
        producedBy: 'harness',
        kind: 'agent.message',
        payload: {
          type: 'agent.message',
          message: {
            content: [
              { type: 'text', text: 'before tools' },
              { type: 'tool_use', id: 'toolu_1', name: 'Bash', input: { cmd: 'pwd' } },
              {
                type: 'mcp_tool_use',
                id: 'toolu_2',
                name: 'mcp__github__search_code',
                input: { q: 'needle' },
              },
            ],
          },
        },
      }),
    );

    expect(projections.map((event) => event.kind)).toEqual([
      'agent.tool_use',
      'agent.mcp_tool_use',
    ]);
    expect(
      projections.map((event) => JSON.parse(Buffer.from(event.payload).toString('utf8'))),
    ).toEqual([
      expect.objectContaining({ name: 'Bash', tool_use_id: 'toolu_1' }),
      expect.objectContaining({
        name: 'search_code',
        mcp_server_name: 'github',
        tool_use_id: 'toolu_2',
      }),
    ]);
  });

  it('preserves client passthrough fields for top-level indexed user.message content', () => {
    const output = indexRowToHttpEvent({
      eventId: 'evt_client_1',
      kind: 'user.message',
      payload: {
        type: 'user.message',
        content: [{ type: 'text', text: 'client message' }],
        message: {
          content: [{ type: 'text', text: 'do not canonicalize this nested value' }],
          client_field: 'preserve me',
        },
        uuid: 'client-uuid',
        session_id: 'client-session',
      },
      subpath: '',
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'client',
      seq: 7,
    } as typeof sessionEventsIndex.$inferSelect);

    expect(output.content).toEqual([{ type: 'text', text: 'client message' }]);
    expect(output.message).toEqual({
      content: [{ type: 'text', text: 'do not canonicalize this nested value' }],
      client_field: 'preserve me',
    });
    expect(output.uuid).toBe('client-uuid');
    expect(output.session_id).toBe('client-session');
  });

  it('canonicalizes legacy agent.thread_message_sent payloads without dropping source fields', () => {
    const output = indexRowToHttpEvent({
      eventId: 'evt_legacy_thread_1',
      kind: 'agent.thread_message_sent',
      payload: {
        type: 'agent.thread_message_sent',
        message: { content: [{ type: 'text', text: 'legacy thread text' }] },
        uuid: 'raw-uuid',
        session_id: 'raw-session',
        source_event_id: 'evt_source',
        source_subpath: 'threads/sth_worker',
        session_thread_id: 'sth_worker',
      },
      subpath: '',
      producedAt: '2026-07-10T12:00:00.000Z',
      producedBy: 'harness',
      seq: 8,
    } as typeof sessionEventsIndex.$inferSelect);

    expect(output.content).toEqual([{ type: 'text', text: 'legacy thread text' }]);
    expect(output.source_event_id).toBe('evt_source');
    expect(output.source_subpath).toBe('threads/sth_worker');
    expect(output.session_thread_id).toBe('sth_worker');
    expect(Object.hasOwn(output, 'message')).toBe(false);
    expect(Object.hasOwn(output, 'uuid')).toBe(false);
    expect(Object.hasOwn(output, 'session_id')).toBe(false);
  });

  it('projects tool-only assistant entries as public tool events', async () => {
    const { db, insertedRows } = fakeDb();
    await indexTranscriptEvents(db, [
      makeEvent({
        subpath: 'threads/sth_worker',
        kind: 'harness.claude.session_entry',
        payload: {
          type: 'harness.claude.session_entry',
          sdk_entry: {
            type: 'assistant',
            message: {
              content: [
                { type: 'tool_use', id: 'toolu_only', name: 'Bash', input: { cmd: 'pwd' } },
              ],
            },
          },
        },
      }),
    ]);

    expect(insertedRows).toHaveLength(2);
    expect(insertedRows[1]).toEqual(
      expect.objectContaining({
        subpath: 'threads/sth_worker',
        kind: 'agent.tool_use',
        payload: expect.objectContaining({
          name: 'Bash',
          input: { cmd: 'pwd' },
          tool_use_id: 'toolu_only',
        }),
      }),
    );
  });

  it('validates events cursors before query construction', () => {
    expect(isValidSessionEventsCursor('')).toBe(true);
    expect(isValidSessionEventsCursor('0')).toBe(true);
    expect(isValidSessionEventsCursor('123')).toBe(true);
    expect(isValidSessionEventsCursor('123:evt_abc')).toBe(true);
    expect(isValidSessionEventsCursor('123:2:evt_abc')).toBe(true);
    expect(isValidSessionEventsCursor('abc')).toBe(false);
    expect(isValidSessionEventsCursor('-1')).toBe(false);
    expect(isValidSessionEventsCursor('1.5')).toBe(false);
    expect(isValidSessionEventsCursor('1:')).toBe(false);
    expect(isValidSessionEventsCursor('1:ordinal:evt_a')).toBe(false);
    expect(isValidSessionEventsCursor('1:2:')).toBe(false);
    expect(isValidSessionEventsCursor('1:2:evt:a')).toBe(false);
    expect(isValidSessionEventsCursor(String(Number.MAX_SAFE_INTEGER))).toBe(true);
    expect(isValidSessionEventsCursor(String(Number.MAX_SAFE_INTEGER + 1))).toBe(false);
    expect(isValidSessionEventsCursor('9223372036854775807')).toBe(false);
    expect(isValidSessionEventsCursor('9'.repeat(100))).toBe(false);
  });
});

function fakeDb(): { db: DbClient; insertedRows: unknown[]; updatedRows: unknown[] } {
  const insertedRows: unknown[] = [];
  const updatedRows: unknown[] = [];
  const db = {
    insert: (table: unknown) => ({
      values: (rows: unknown[] | unknown) => {
        if (table === sessionEventsIndex) {
          insertedRows.push(...(Array.isArray(rows) ? rows : [rows]));
        }
        return {
          onConflictDoNothing: async () => undefined,
          onConflictDoUpdate: async () => undefined,
        };
      },
    }),
    update: () => ({
      set: (values: unknown) => ({
        where: async () => {
          updatedRows.push(values);
        },
      }),
    }),
    execute: async () => {
      updatedRows.push({ processedAt: 'derived by SQL', processedMarkerSeq: 'derived by SQL' });
    },
    select: () => ({
      from: () => ({
        where: () => {
          const limited = { limit: async () => [] };
          return {
            ...limited,
            orderBy: () => limited,
          };
        },
      }),
    }),
  };
  return { db: db as unknown as DbClient, insertedRows, updatedRows };
}

function makeEvents(count: number): Event[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `evt_index_${index}`,
    workspaceId: 'ws_index',
    sessionId: 'ses_index',
    subpath: '',
    seq: index,
    producedAt: new Date(0).toISOString(),
    producedBy: 'client',
    kind: 'user.message',
    payload: new Uint8Array([index]),
    idempotencyKey: '',
  }));
}

function makeEvent(overrides: Omit<Partial<Event>, 'payload'> & { payload?: unknown }): Event {
  const payload = overrides.payload ?? { type: overrides.kind ?? 'user.message' };
  return {
    id: 'evt_index_custom',
    workspaceId: 'ws_index',
    sessionId: 'ses_index',
    subpath: '',
    seq: 0,
    producedAt: new Date(0).toISOString(),
    producedBy: 'client',
    kind: 'user.message',
    idempotencyKey: '',
    ...overrides,
    payload: payload instanceof Uint8Array ? payload : Buffer.from(JSON.stringify(payload), 'utf8'),
  };
}
