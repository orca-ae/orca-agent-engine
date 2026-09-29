// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyReply } from 'fastify';
import type { TranscriptStore } from '@orca/transcript-store';
import { AgentEventKind } from '@orca/agent-event-contract';
import {
  CLAUDE_SESSION_EVENT_TYPES,
  protoToHttpEvent,
  toPublicHttpEvent,
} from '../domain/events.js';
import { streamableEventsForSubpath } from '../domain/thread-projection.js';
import { sseConnectionsActive, sseBufferDepth, sseDropTotal } from '../metrics.js';

export interface BufferedEvent {
  seq: string;
  body: string;
  type: string;
  enqueuedAt?: number;
}

export interface BoundedBufferOptions {
  size: number;
  dropAgeMs: number;
  now?: () => number;
}

export type PushOutcome = 'ok' | { kind: 'wait'; retryAfterMs: number } | { kind: 'drop' };

export class BoundedBuffer {
  private q: BufferedEvent[] = [];
  private now: () => number;
  constructor(private readonly opts: BoundedBufferOptions) {
    this.now = opts.now ?? (() => Date.now());
  }
  push(e: BufferedEvent): PushOutcome {
    if (this.q.length >= this.opts.size) {
      const now = this.now();
      const oldest = this.q[0]!;
      const ageMs = now - (oldest.enqueuedAt ?? now);
      if (ageMs < this.opts.dropAgeMs) {
        return { kind: 'wait', retryAfterMs: this.opts.dropAgeMs - ageMs };
      }
      return { kind: 'drop' };
    }
    this.q.push({ ...e, enqueuedAt: this.now() });
    return 'ok';
  }
  shift(): BufferedEvent | undefined {
    return this.q.shift();
  }
  size(): number {
    return this.q.length;
  }
}

export interface SseBridgeOptions {
  bufferSize: number;
  dropAgeMs: number;
  heartbeatMs: number;
  eventDeltas?: ReadonlySet<DeltaEventType>;
  orcaBeta?: boolean;
}

type DeltaEventType = typeof AgentEventKind.message | typeof AgentEventKind.thinking;

export function shouldEmitClaudeStreamFrame(type: string): boolean {
  return type === 'event_start' || type === 'event_delta' || CLAUDE_SESSION_EVENT_TYPES.has(type);
}

/** Select only the preview frames explicitly requested through event_deltas. */
export function shouldEmitDeltaFrame(
  frame: Record<string, unknown>,
  requested: ReadonlySet<string>,
  openPreviews: Map<string, DeltaEventType>,
): boolean {
  if (frame.type === 'event_start') {
    const preview = frame.event;
    if (!preview || typeof preview !== 'object' || Array.isArray(preview)) return false;
    const id = (preview as Record<string, unknown>).id;
    const type = (preview as Record<string, unknown>).type;
    if (
      typeof id !== 'string' ||
      (type !== AgentEventKind.message && type !== AgentEventKind.thinking) ||
      !requested.has(type)
    ) {
      if (typeof id === 'string') openPreviews.delete(id);
      return false;
    }
    openPreviews.set(id, type);
    return true;
  }
  if (frame.type === 'event_delta') {
    const eventId = frame.event_id;
    return typeof eventId === 'string' && openPreviews.has(eventId);
  }
  if (typeof frame.id === 'string') openPreviews.delete(frame.id);
  return true;
}

/**
 * Open an SSE stream that mirrors `store.tail(workspaceId, sessionId)` to the
 * client. Implements drop-and-resync backpressure (see
 * docs/operation/sse-backpressure.md).
 */
export async function streamSession(
  reply: FastifyReply,
  store: TranscriptStore,
  workspaceId: string,
  sessionId: string,
  fromCursor: string,
  subpath: string,
  opts: SseBridgeOptions,
): Promise<void> {
  // writeHead-only headers are not retained by a real ServerResponse.getHeader().
  // Keep the stream type observable on finish/close for HTTP lifetime metrics.
  reply.raw.setHeader('content-type', 'text/event-stream');
  reply.raw.writeHead(200, {
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'x-accel-buffering': 'no',
  });
  sseConnectionsActive.inc();
  const buf = new BoundedBuffer({ size: opts.bufferSize, dropAgeMs: opts.dropAgeMs });
  const requestedDeltas = opts.eventDeltas ?? new Set<DeltaEventType>();
  const openPreviews = new Map<string, DeltaEventType>();
  const ac = new AbortController();
  let writeBlocked = false;
  let lastWrittenSeq = fromCursor;
  reply.raw.on('close', () => ac.abort());

  const heartbeat = setInterval(() => {
    if (reply.raw.writable && !writeBlocked) {
      writeBlocked = !reply.raw.write(`:heartbeat\n\n`);
    }
  }, opts.heartbeatMs);

  const flush = (): void => {
    if (writeBlocked) return;

    let drained = false;
    while (buf.size() > 0 && reply.raw.writable) {
      const e = buf.shift()!;
      const wroteAll = reply.raw.write(`id: ${e.seq}\nevent: ${e.type}\ndata: ${e.body}\n\n`);
      // Node accepted the complete frame even when it reports backpressure.
      lastWrittenSeq = e.seq;
      drained = true;
      if (!wroteAll) {
        writeBlocked = true;
        break;
      }
    }
    if (drained) sseBufferDepth.observe(buf.size());
  };

  const onDrain = (): void => {
    writeBlocked = false;
    flush();
  };
  reply.raw.on('drain', onDrain);

  const waitForCapacity = (retryAfterMs: number): Promise<void> => {
    if (ac.signal.aborted) return Promise.resolve();

    return new Promise((resolve) => {
      let settled = false;

      const finish = (): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reply.raw.off('drain', finish);
        ac.signal.removeEventListener('abort', finish);
        resolve();
      };

      reply.raw.once('drain', finish);
      ac.signal.addEventListener('abort', finish, { once: true });
      const timer = setTimeout(finish, Math.max(0, retryAfterMs));
    });
  };

  try {
    let dropped = false;
    let terminal = false;
    for await (const event of store.tail(workspaceId, sessionId, {
      fromCursor,
      // Root lifecycle events terminate every session/thread stream. Tail the
      // full session, then apply the requested thread projection below, so an
      // exact-subpath backend filter cannot hide session.deleted.
      subpath: '*',
      signal: ac.signal,
    })) {
      if (dropped) break;
      for (const publicEvent of streamableEventsForSubpath(event, subpath)) {
        const http = protoToHttpEvent(publicEvent);
        if (
          !(opts.orcaBeta && opts.eventDeltas === undefined) &&
          !shouldEmitDeltaFrame(http, requestedDeltas, openPreviews)
        ) {
          continue;
        }
        if (!opts.orcaBeta && !shouldEmitClaudeStreamFrame(http.type)) {
          continue;
        }
        const wire = toPublicHttpEvent(http, opts.orcaBeta === true);
        const bufferedEvent = {
          seq: http.seq,
          body: JSON.stringify(wire),
          type: http.type,
        };

        while (!ac.signal.aborted) {
          const outcome = buf.push(bufferedEvent);
          if (outcome === 'ok') {
            flush();
            break;
          }
          if (outcome.kind === 'wait') {
            await waitForCapacity(outcome.retryAfterMs);
            continue;
          }

          sseDropTotal.inc({ reason: 'client-too-slow' });
          if (opts.orcaBeta) {
            reply.raw.write(
              `event: drop\ndata: ${JSON.stringify({ reason: 'client-too-slow', last_seq: lastWrittenSeq })}\n\n`,
            );
          }
          dropped = true;
          ac.abort();
          break;
        }
        if (dropped || ac.signal.aborted) break;
        if (http.type === 'session.deleted') {
          terminal = true;
          while (buf.size() > 0 && !ac.signal.aborted) {
            if (!writeBlocked) flush();
            if (buf.size() === 0) break;
            await waitForCapacity(opts.dropAgeMs);
            if (writeBlocked && buf.size() > 0) {
              sseDropTotal.inc({ reason: 'client-too-slow' });
              if (opts.orcaBeta) {
                reply.raw.write(
                  `event: drop\ndata: ${JSON.stringify({ reason: 'client-too-slow', last_seq: lastWrittenSeq })}\n\n`,
                );
              }
              dropped = true;
              ac.abort();
            }
          }
          break;
        }
      }
      if (dropped || ac.signal.aborted || terminal) break;
    }
  } finally {
    clearInterval(heartbeat);
    sseConnectionsActive.dec();
    reply.raw.off('drain', onDrain);
    reply.raw.end();
  }
}
