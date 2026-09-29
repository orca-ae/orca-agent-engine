// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import type { FastifyReply } from 'fastify';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import {
  BoundedBuffer,
  shouldEmitClaudeStreamFrame,
  shouldEmitDeltaFrame,
  streamSession,
} from '../../src/streaming/sse.js';

describe('BoundedBuffer', () => {
  it('returns "ok" until size reached', () => {
    const buf = new BoundedBuffer({ size: 4, dropAgeMs: 1000, now: () => Date.now() });
    for (let i = 0; i < 4; i++) {
      expect(buf.push({ seq: String(i), body: '{}', type: 'agent.message' })).toBe('ok');
    }
  });

  it('waits while full until the oldest event reaches the drop age', () => {
    let now = 1000;
    const buf = new BoundedBuffer({ size: 2, dropAgeMs: 100, now: () => now });
    buf.push({ seq: '0', body: '{}', type: 'agent.message' });
    now = 1050;
    buf.push({ seq: '1', body: '{}', type: 'agent.message' });
    expect(buf.push({ seq: '2', body: '{}', type: 'agent.message' })).toEqual({
      kind: 'wait',
      retryAfterMs: 50,
    });
    now = 1100;
    expect(buf.push({ seq: '2', body: '{}', type: 'agent.message' })).toEqual({
      kind: 'drop',
    });
  });

  it('drains and ages: shift lets older items leave, then "ok" again', () => {
    let now = 1000;
    const buf = new BoundedBuffer({ size: 2, dropAgeMs: 100, now: () => now });
    buf.push({ seq: '0', body: '{}', type: 'agent.message' });
    buf.push({ seq: '1', body: '{}', type: 'agent.message' });
    expect(buf.shift()?.seq).toBe('0');
    now = 2000;
    expect(buf.push({ seq: '2', body: '{}', type: 'agent.message' })).toBe('ok');
  });
});

describe('slow consumers', () => {
  it('waits for drain during the grace period without losing an event', async () => {
    const writes: string[] = [];
    let blocked = true;
    let ended = false;
    const raw = Object.assign(new EventEmitter(), {
      writable: true,
      setHeader: () => undefined,
      writeHead: () => undefined,
      write: (chunk: string) => {
        writes.push(chunk);
        return !blocked;
      },
      end: () => {
        ended = true;
      },
    });
    const events: Event[] = [1, 2, 3].map((seq) => ({
      id: `evt_${seq}`,
      workspaceId: 'ws_slow',
      sessionId: 'ses_slow',
      subpath: '',
      seq,
      producedAt: '2026-07-24T00:00:00.000Z',
      producedBy: 'harness-server',
      kind: 'agent.message',
      payload: new TextEncoder().encode(
        JSON.stringify({ content: [{ type: 'text', text: `message ${seq}` }] }),
      ),
      idempotencyKey: `evt_${seq}`,
    }));
    let yielded = 0;
    const store = {
      async *tail() {
        for (const event of events) {
          yielded += 1;
          yield event;
        }
      },
    } as unknown as TranscriptStore;

    const streaming = streamSession(
      { raw } as unknown as FastifyReply,
      store,
      'ws_slow',
      'ses_slow',
      '',
      '',
      { bufferSize: 1, dropAgeMs: 1_000, heartbeatMs: 60_000 },
    );

    await vi.waitFor(() => expect(yielded).toBe(3));
    expect(ended).toBe(false);
    blocked = false;
    raw.emit('drain');
    await streaming;

    expect(writes.filter((chunk) => chunk.includes('event: agent.message'))).toHaveLength(3);
    expect(ended).toBe(true);
  });

  it('aborts the tail immediately after dropping a slow consumer', async () => {
    const writes: string[] = [];
    let ended = false;
    let tailClosed = false;
    const raw = Object.assign(new EventEmitter(), {
      writable: true,
      setHeader: () => undefined,
      writeHead: () => undefined,
      write: (chunk: string) => {
        writes.push(chunk);
        return false;
      },
      end: () => {
        ended = true;
      },
    });
    const events: Event[] = [1, 2, 3].map((seq) => ({
      id: `evt_${seq}`,
      workspaceId: 'ws_drop',
      sessionId: 'ses_drop',
      subpath: '',
      seq,
      producedAt: '2026-07-24T00:00:00.000Z',
      producedBy: 'harness-server',
      kind: 'agent.message',
      payload: new TextEncoder().encode(
        JSON.stringify({ content: [{ type: 'text', text: `message ${seq}` }] }),
      ),
      idempotencyKey: `evt_${seq}`,
    }));
    const store = {
      async *tail() {
        try {
          for (const event of events) yield event;
          await new Promise(() => undefined);
        } finally {
          tailClosed = true;
        }
      },
    } as unknown as TranscriptStore;

    await streamSession(
      { raw } as unknown as FastifyReply,
      store,
      'ws_drop',
      'ses_drop',
      'cursor-before-stream',
      '',
      { bufferSize: 1, dropAgeMs: 0, heartbeatMs: 60_000, orcaBeta: true },
    );

    expect(writes.join('')).toContain('event: drop');
    expect(writes.join('')).toContain('"last_seq":"1"');
    expect(tailClosed).toBe(true);
    expect(ended).toBe(true);
  });
});

describe('event delta selection', () => {
  it('emits preview frames only when their event type was requested', () => {
    const requested = new Set(['agent.message']);
    const openPreviews = new Map<string, 'agent.message' | 'agent.thinking'>();

    expect(
      shouldEmitDeltaFrame(
        {
          type: 'event_start',
          event: { id: 'evt_message', type: 'agent.message' },
        },
        requested,
        openPreviews,
      ),
    ).toBe(true);
    expect(
      shouldEmitDeltaFrame(
        { type: 'event_delta', event_id: 'evt_message', delta: { type: 'content_delta' } },
        requested,
        openPreviews,
      ),
    ).toBe(true);
    expect(
      shouldEmitDeltaFrame(
        {
          type: 'event_start',
          event: { id: 'evt_thinking', type: 'agent.thinking' },
        },
        requested,
        openPreviews,
      ),
    ).toBe(false);
  });

  it('keeps default streams within the Claude event union', () => {
    expect(shouldEmitClaudeStreamFrame('agent.message')).toBe(true);
    expect(shouldEmitClaudeStreamFrame('event_start')).toBe(true);
    expect(shouldEmitClaudeStreamFrame('orca.extension_event')).toBe(false);
    expect(shouldEmitClaudeStreamFrame('harness.claude.session_entry')).toBe(false);
  });
});

describe('terminal session events', () => {
  it('drains a queued session.deleted frame before closing a backpressured stream', async () => {
    const writes: string[] = [];
    let blocked = true;
    let ended = false;
    const raw = Object.assign(new EventEmitter(), {
      writable: true,
      setHeader: () => undefined,
      writeHead: () => undefined,
      write: (chunk: string) => {
        writes.push(chunk);
        return !blocked;
      },
      end: () => {
        ended = true;
      },
    });
    const events: Event[] = [
      {
        id: 'evt_message',
        workspaceId: 'ws_terminal_drain',
        sessionId: 'ses_terminal_drain',
        subpath: '',
        seq: 1,
        producedAt: '2026-07-23T01:02:02.000Z',
        producedBy: 'harness-server',
        kind: 'agent.message',
        payload: new TextEncoder().encode(
          JSON.stringify({ content: [{ type: 'text', text: 'before deletion' }] }),
        ),
        idempotencyKey: 'evt_message',
      },
      {
        id: 'evt_deleted',
        workspaceId: 'ws_terminal_drain',
        sessionId: 'ses_terminal_drain',
        subpath: '',
        seq: 2,
        producedAt: '2026-07-23T01:02:03.000Z',
        producedBy: 'registry-service',
        kind: 'session.deleted',
        payload: new Uint8Array(),
        idempotencyKey: 'evt_deleted',
      },
    ];
    let yielded = 0;
    const store = {
      async *tail() {
        for (const event of events) {
          yielded += 1;
          yield event;
        }
      },
    } as unknown as TranscriptStore;

    const streaming = streamSession(
      { raw } as unknown as FastifyReply,
      store,
      'ws_terminal_drain',
      'ses_terminal_drain',
      '',
      '',
      { bufferSize: 1, dropAgeMs: 1_000, heartbeatMs: 60_000 },
    );

    await vi.waitFor(() => expect(yielded).toBe(2));
    expect(writes.join('')).not.toContain('event: session.deleted');
    expect(ended).toBe(false);

    blocked = false;
    raw.emit('drain');
    await streaming;

    expect(writes.join('')).toContain('event: session.deleted');
    expect(ended).toBe(true);
  });

  it('broadcasts session.deleted to a thread stream and closes it', async () => {
    const writes: string[] = [];
    let ended = false;
    const raw = Object.assign(new EventEmitter(), {
      writable: true,
      setHeader: () => undefined,
      writeHead: () => undefined,
      write: (chunk: string) => {
        writes.push(chunk);
        return true;
      },
      end: () => {
        ended = true;
      },
    });
    let consumedPastDeletion = false;
    let tailSubpath: string | undefined;
    const deleted: Event = {
      id: 'evt_deleted',
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      subpath: '',
      seq: 42,
      producedAt: '2026-07-23T01:02:03.000Z',
      producedBy: 'registry-service',
      kind: 'session.deleted',
      payload: new Uint8Array(),
      idempotencyKey: 'evt_deleted',
    };
    const store = {
      async *tail(_workspaceId: string, _sessionId: string, opts: { subpath: string }) {
        tailSubpath = opts.subpath;
        yield deleted;
        consumedPastDeletion = true;
        yield { ...deleted, id: 'evt_late', kind: 'agent.message', seq: 43 };
      },
    } as unknown as TranscriptStore;

    await streamSession(
      { raw } as unknown as FastifyReply,
      store,
      'ws_test',
      'ses_test',
      '',
      'threads/child',
      { bufferSize: 10, dropAgeMs: 1_000, heartbeatMs: 60_000 },
    );

    expect(writes.join('')).toContain('event: session.deleted');
    expect(writes.join('')).toContain('"type":"session.deleted"');
    expect(tailSubpath).toBe('*');
    expect(consumedPastDeletion).toBe(false);
    expect(ended).toBe(true);
  });
});
