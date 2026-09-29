// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { SseFrameParser, parseSseStream, type SseFrame } from '../../src/sse.js';
import { scriptSse, scriptSseChunked } from '../fakes/registry.js';

describe('SseFrameParser', () => {
  it('parses a single data frame terminated by a blank line', () => {
    const parser = new SseFrameParser();
    const frames = parser.push('id: 1\nevent: agent.message\ndata: {"type":"agent.message"}\n\n');
    expect(frames).toEqual([{ type: 'agent.message', cursor: '1' }]);
  });

  /**
   * The `id:` line is the ONLY cursor channel the default wire view leaves
   * intact. The registry strips `seq` out of the JSON body for every request
   * without `orca-beta`, yet writes `id: <seq>` on every frame regardless — so a
   * parser that reads only `data:` hands the loop nothing to resume from, which
   * is how the one-shot reconnect came to tail the live head instead.
   */
  it('lifts the SSE id line onto the frame even when the body carries no seq', () => {
    const parser = new SseFrameParser();
    const frames = parser.push('id: 41\nevent: agent.message\ndata: {"type":"agent.message"}\n\n');
    expect(frames[0]?.cursor).toBe('41');
    expect(frames[0]?.['seq']).toBeUndefined();
  });

  it('leaves cursor undefined for a block with no id line', () => {
    const parser = new SseFrameParser();
    const frames = parser.push('event: agent.message\ndata: {"type":"agent.message"}\n\n');
    expect(frames[0]?.cursor).toBeUndefined();
  });

  it('ignores an empty id line rather than recording an unusable cursor', () => {
    const parser = new SseFrameParser();
    const frames = parser.push('id: \ndata: {"type":"agent.message"}\n\n');
    expect(frames[0]?.cursor).toBeUndefined();
  });

  it('skips heartbeat comment blocks', () => {
    const parser = new SseFrameParser();
    const frames = parser.push(':heartbeat\n\ndata: {"type":"agent.turn_completed"}\n\n');
    expect(frames).toEqual([{ type: 'agent.turn_completed' }]);
  });

  it('reassembles a frame delivered across two pushes', () => {
    const parser = new SseFrameParser();
    expect(parser.push('data: {"type":"age')).toEqual([]);
    const frames = parser.push('nt.message","seq":"7"}\n\n');
    expect(frames).toEqual([{ type: 'agent.message', seq: '7' }]);
  });

  it('ignores non-JSON data lines without throwing', () => {
    const parser = new SseFrameParser();
    const frames = parser.push('data: not-json\n\ndata: {"type":"ok"}\n\n');
    expect(frames).toEqual([{ type: 'ok' }]);
  });

  it('drops a data line with no type field', () => {
    const parser = new SseFrameParser();
    const frames = parser.push('data: {"seq":"1"}\n\n');
    expect(frames).toEqual([]);
  });
});

describe('parseSseStream', () => {
  it('yields every frame from a scripted stream in order', async () => {
    const stream = scriptSse([
      { type: 'agent.message', seq: '1', content: [{ type: 'text', text: 'hi' }] },
      ':heartbeat',
      { type: 'agent.turn_completed', seq: '2' },
    ]);
    const seen: SseFrame[] = [];
    for await (const frame of parseSseStream(stream)) {
      seen.push(frame);
    }
    expect(seen.map((f) => f.type)).toEqual(['agent.message', 'agent.turn_completed']);
  });

  it('reassembles frames split across chunk boundaries', async () => {
    const stream = scriptSseChunked(
      [
        { type: 'agent.message', seq: '1' },
        { type: 'agent.turn_completed', seq: '2' },
      ],
      3,
    );
    const seen: SseFrame[] = [];
    for await (const frame of parseSseStream(stream)) {
      seen.push(frame);
    }
    expect(seen.map((f) => f.type)).toEqual(['agent.message', 'agent.turn_completed']);
  });
});
