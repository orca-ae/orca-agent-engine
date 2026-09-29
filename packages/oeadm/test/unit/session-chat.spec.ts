// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { runSessionChat, type ChatIo } from '../../src/session-chat.js';
import { noColor } from '../../src/colors.js';
import { SseFrameParser, type SseFrame } from '../../src/sse.js';
// The registry's OWN serializers, not a local imitation of them. The resume
// defect survived a full suite because these specs hand-wrote `seq` onto their
// frames — a field the registry deletes before the frame reaches this client —
// so the corpus agreed with the code and both were wrong. Building the corpus
// through `protoToHttpEvent` + `toPublicHttpEvent` means the day the registry
// changes what it puts on the wire, these tests change with it.
import {
  protoToHttpEvent,
  toPublicHttpEvent,
} from '../../../../services/registry-service-ts/src/domain/events.js';

/**
 * A recording session client the chat loop drives. `frames` is the scripted SSE
 * tail: an async iterable the loop reads turn-by-turn. Posts are recorded so the
 * test asserts the exact wire bodies (turn text, confirmation verdicts).
 */
interface FakeChatClient {
  messages: string[];
  confirmations: Array<{ toolUseId: string; decision: 'allow' | 'deny' }>;
  postUserMessage(sessionId: string, text: string): Promise<void>;
  postToolConfirmation(
    sessionId: string,
    toolUseId: string,
    decision: 'allow' | 'deny',
  ): Promise<void>;
  stream(sessionId: string, opts?: { fromCursor?: string }): AsyncIterable<SseFrame>;
}

/**
 * Build a fake client whose stream yields `frames`. The stream paces itself on a
 * gate so frames only appear once the loop has posted the turn that "produces"
 * them — but for these unit tests a simple eager async generator suffices: the
 * loop consumes until each `agent.turn_completed`, which is the turn boundary.
 */
function fakeChatClient(frames: SseFrame[]): FakeChatClient {
  const client: FakeChatClient = {
    messages: [],
    confirmations: [],
    async postUserMessage(_sessionId, text) {
      this.messages.push(text);
    },
    async postToolConfirmation(_sessionId, toolUseId, decision) {
      this.confirmations.push({ toolUseId, decision });
    },
    async *stream() {
      for (const f of frames) {
        yield f;
      }
    },
  };
  return client;
}

/** A scripted IO: hands out `lines` in order, then EOF (null). Records output. */
function scriptedIo(
  lines: string[],
  confirmAnswers: boolean[],
): ChatIo & { output: string[]; prompts: string[] } {
  const pendingLines = [...lines];
  const pendingConfirms = [...confirmAnswers];
  const output: string[] = [];
  const prompts: string[] = [];
  return {
    output,
    prompts,
    async readLine(prompt: string): Promise<string | null> {
      prompts.push(prompt);
      if (pendingLines.length === 0) return null;
      return pendingLines.shift()!;
    },
    async confirm(question: string): Promise<boolean> {
      prompts.push(question);
      return pendingConfirms.length === 0 ? false : pendingConfirms.shift()!;
    },
    write(text: string): void {
      output.push(text);
    },
  };
}

describe('runSessionChat', () => {
  it('posts each read line as a user.message turn and renders the reply', async () => {
    const client = fakeChatClient([
      { type: 'agent.message', content: [{ type: 'text', text: 'Answer one' }] },
      { type: 'agent.turn_completed' },
    ]);
    const io = scriptedIo(['first question'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    expect(client.messages).toEqual(['first question']);
    expect(io.output.join('\n')).toContain('Answer one');
  });

  it('drives multiple turns until stdin reaches EOF', async () => {
    const client = fakeChatClient([
      { type: 'agent.message', content: [{ type: 'text', text: 'reply A' }] },
      { type: 'agent.turn_completed' },
      { type: 'agent.message', content: [{ type: 'text', text: 'reply B' }] },
      { type: 'agent.turn_completed' },
    ]);
    const io = scriptedIo(['q1', 'q2'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    expect(client.messages).toEqual(['q1', 'q2']);
    const joined = io.output.join('\n');
    expect(joined).toContain('reply A');
    expect(joined).toContain('reply B');
  });

  it('prompts allow/deny on requires_action and posts an allow confirmation', async () => {
    const client = fakeChatClient([
      {
        type: 'agent.requires_action',
        action: 'tool_confirmation',
        tool_use_id: 'tool_42',
        tool_name: 'Bash',
      },
      { type: 'agent.tool_result', tool_use_id: 'tool_42', content: 'done', is_error: false },
      { type: 'agent.message', content: [{ type: 'text', text: 'finished' }] },
      { type: 'agent.turn_completed' },
    ]);
    const io = scriptedIo(['run a command'], [true]);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    expect(client.confirmations).toEqual([{ toolUseId: 'tool_42', decision: 'allow' }]);
    // The allow/deny question names the tool.
    expect(io.prompts.some((p) => p.includes('Bash'))).toBe(true);
    expect(io.output.join('\n')).toContain('finished');
  });

  it('posts a deny confirmation when the user declines', async () => {
    const client = fakeChatClient([
      {
        type: 'agent.requires_action',
        action: 'tool_confirmation',
        tool_use_id: 'tool_7',
        tool_name: 'WriteFile',
      },
      { type: 'agent.turn_completed' },
    ]);
    const io = scriptedIo(['try to write'], [false]);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    expect(client.confirmations).toEqual([{ toolUseId: 'tool_7', decision: 'deny' }]);
  });

  it('renders tool_use and tool_result frames within a turn', async () => {
    const client = fakeChatClient([
      {
        type: 'agent.tool_use',
        name: 'mcp__orca__bash',
        input: { command: 'ls' },
        tool_use_id: 't1',
      },
      { type: 'agent.tool_result', tool_use_id: 't1', content: 'a\nb', is_error: false },
      { type: 'agent.turn_completed' },
    ]);
    const io = scriptedIo(['list files'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    const joined = io.output.join('\n');
    expect(joined).toContain('mcp__orca__bash');
    expect(joined).toContain('a\nb');
  });

  it('does not post a turn for an empty input line but keeps looping', async () => {
    const client = fakeChatClient([
      { type: 'agent.message', content: [{ type: 'text', text: 'only real turn' }] },
      { type: 'agent.turn_completed' },
    ]);
    const io = scriptedIo(['', '   ', 'real'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    expect(client.messages).toEqual(['real']);
  });

  it('surfaces an agent.error frame and ends the turn', async () => {
    const client = fakeChatClient([
      { type: 'agent.error', message: 'model exploded' },
      { type: 'agent.turn_completed' },
    ]);
    const io = scriptedIo(['go'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    expect(io.output.join('\n')).toContain('model exploded');
  });
});

/**
 * A dead tail is NOT a completed turn.
 *
 * A posted turn is committed server-side the moment it is accepted. When the tail
 * drops mid-turn the answer is still coming and this process has merely stopped
 * listening — so returning identically to `agent.turn_completed` made the loop
 * re-prompt, accept further questions it could never show answers to, and exit 0.
 * Reproduced on the shipped code: 3 questions posted, 1 answer shown, status 0.
 */
describe('runSessionChat — a tail that ends mid-turn', () => {
  /** One transcript event, as the store holds it before the registry serializes it. */
  interface StoredEvent {
    seq: number;
    kind: string;
    payload: Record<string, unknown>;
  }

  /**
   * Produce the frames a REAL registry stream delivers to THIS client.
   *
   * Every step is the registry's own: `protoToHttpEvent` stamps the envelope,
   * `toPublicHttpEvent(_, false)` applies the default (non-`orca-beta`) view —
   * which DELETES `seq` from the body — the block is framed exactly as
   * `streamSession` writes it (`id:`/`event:`/`data:` + blank line, with the seq
   * on the `id:` line), and the CLI's own `SseFrameParser` reads it back.
   *
   * Hand-writing `seq: '7'` onto a frame object, as this suite used to, skips all
   * four and asserts against a shape the registry has never emitted.
   */
  function wireFrames(events: StoredEvent[]): SseFrame[] {
    const parser = new SseFrameParser();
    const frames: SseFrame[] = [];
    for (const event of events) {
      const http = protoToHttpEvent({
        id: `evt_${event.seq}`,
        seq: event.seq,
        kind: event.kind,
        subpath: '',
        workspaceId: 'ws_1',
        sessionId: 'ses_1',
        idempotencyKey: '',
        producedAt: '2026-01-01T00:00:00.000Z',
        producedBy: 'harness',
        payload: new TextEncoder().encode(JSON.stringify(event.payload)),
      });
      const body = JSON.stringify(toPublicHttpEvent(http, false));
      frames.push(...parser.push(`id: ${http.seq}\nevent: ${http.type}\ndata: ${body}\n\n`));
    }
    return frames;
  }

  /**
   * A client whose tail can be opened more than once: `tails[0]` is the initial
   * stream, `tails[1]` answers the resume. Every `stream()` call records the
   * cursor it was asked to resume from.
   */
  function reconnectingClient(tails: SseFrame[][]): FakeChatClient & {
    opens: Array<{ fromCursor?: string }>;
  } {
    const opens: Array<{ fromCursor?: string }> = [];
    let opened = 0;
    return {
      opens,
      messages: [],
      confirmations: [],
      async postUserMessage(_sessionId, text) {
        this.messages.push(text);
      },
      async postToolConfirmation(_sessionId, toolUseId, decision) {
        this.confirmations.push({ toolUseId, decision });
      },
      stream(_sessionId: string, opts?: { fromCursor?: string }): AsyncIterable<SseFrame> {
        opens.push(opts?.fromCursor !== undefined ? { fromCursor: opts.fromCursor } : {});
        const frames = tails[opened] ?? [];
        opened += 1;
        return (async function* () {
          for (const f of frames) yield f;
        })();
      },
    };
  }

  it('the real wire view carries no seq — only the SSE id line does', () => {
    const [frame] = wireFrames([
      { seq: 7, kind: 'agent.message', payload: { content: [{ type: 'text', text: 'x' }] } },
    ]);
    // This is the fact the old corpus contradicted. `toPublicHttpEvent` deletes
    // `seq` for every request without `orca-beta`, so a resume that reads
    // `frame['seq']` reads `undefined` on every frame ever delivered.
    expect(frame?.['seq']).toBeUndefined();
    expect(frame?.cursor).toBe('7');
  });

  it('tells the operator, resumes one past the last seq, and finishes the same turn', async () => {
    const client = reconnectingClient([
      // The tail dies after one partial frame — no turn_completed.
      wireFrames([
        {
          seq: 7,
          kind: 'agent.message',
          payload: { content: [{ type: 'text', text: 'thinking…' }] },
        },
      ]),
      // The resumed tail carries the rest of that same turn.
      wireFrames([
        {
          seq: 8,
          kind: 'agent.message',
          payload: { content: [{ type: 'text', text: 'the answer' }] },
        },
        { seq: 9, kind: 'agent.turn_completed', payload: {} },
      ]),
    ]);
    const io = scriptedIo(['q1'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    // The operator is told the difference between "the agent finished" and "we
    // stopped listening". Silence here is the whole defect.
    expect(io.output.join('\n')).toMatch(/stream ended before the turn completed/i);
    // 8, not 7. `from_cursor` is INCLUSIVE (`seq >= $cursor` in postgres-store),
    // so resuming at the last seq RENDERED replays it — a duplicate line, or a
    // replayed `agent.requires_action` that re-prompts the operator and POSTs a
    // second verdict for an already-decided tool_use_id.
    expect(client.opens).toEqual([{}, { fromCursor: '8' }]);
    expect(io.output.join('\n')).toContain('the answer');
  });

  it('throws rather than re-prompting when the resumed tail dies too', async () => {
    const client = reconnectingClient([
      wireFrames([
        {
          seq: 3,
          kind: 'agent.message',
          payload: { content: [{ type: 'text', text: 'partial' }] },
        },
      ]),
      [],
    ]);
    // Three lines are scripted; only the first may ever be posted. Accepting the
    // other two is precisely the "committed with nothing shown back" failure.
    const io = scriptedIo(['q1', 'q2', 'q3'], []);

    await expect(
      runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor }),
    ).rejects.toThrow(/ended before the turn completed/i);

    expect(client.messages).toEqual(['q1']);
    expect(client.opens).toHaveLength(2);
  });

  it('resumes without a cursor when the tail died before any frame arrived', async () => {
    const client = reconnectingClient([
      [],
      wireFrames([{ seq: 1, kind: 'agent.turn_completed', payload: {} }]),
    ]);
    const io = scriptedIo(['q1'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    // No seq was ever seen, so there is nothing to resume FROM. An omitted
    // `from_cursor` is the empty string, which the store turns into
    // `highWatermark + 1` — the live head, NOT the beginning; a frame emitted
    // inside the gap is lost, and the second drain's `stream_ended` reports it.
    // Inventing a cursor would be worse: it would skip or replay arbitrarily.
    expect(client.opens).toEqual([{}, {}]);
  });

  it('does not advance the cursor past a non-numeric id line', async () => {
    // The registry always writes a decimal seq, so this only guards a proxy or a
    // future framing change. Keeping the last known-good cursor beats sending a
    // value the store parses to null and silently treats as "tail the head".
    const parser = new SseFrameParser();
    const garbled = parser.push('id: not-a-seq\ndata: {"type":"agent.message"}\n\n');
    const client = reconnectingClient([
      [
        ...wireFrames([
          { seq: 4, kind: 'agent.message', payload: { content: [{ type: 'text', text: 'a' }] } },
        ]),
        ...garbled,
      ],
      wireFrames([{ seq: 6, kind: 'agent.turn_completed', payload: {} }]),
    ]);
    const io = scriptedIo(['q1'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    expect(client.opens).toEqual([{}, { fromCursor: '5' }]);
  });

  it('keeps driving further turns after a successful resume', async () => {
    const client = reconnectingClient([
      wireFrames([
        { seq: 1, kind: 'agent.message', payload: { content: [{ type: 'text', text: 'a' }] } },
      ]),
      wireFrames([
        { seq: 2, kind: 'agent.turn_completed', payload: {} },
        { seq: 3, kind: 'agent.message', payload: { content: [{ type: 'text', text: 'b' }] } },
        { seq: 4, kind: 'agent.turn_completed', payload: {} },
      ]),
    ]);
    const io = scriptedIo(['q1', 'q2'], []);

    await runSessionChat({ sessionId: 'ses_1', client, io, colors: noColor });

    expect(client.messages).toEqual(['q1', 'q2']);
    expect(io.output.join('\n')).toContain('b');
  });
});
