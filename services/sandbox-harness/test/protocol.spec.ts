// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Writable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import {
  contentToText,
  resultFrame,
  StreamJsonServer,
  type Frame,
  type Session,
} from '../src/protocol.js';

class CaptureWritable extends Writable {
  private readonly chunks: string[] = [];

  _write(
    chunk: Buffer | string,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    this.chunks.push(Buffer.isBuffer(chunk) ? chunk.toString('utf8') : chunk);
    callback();
  }

  text(): string {
    return this.chunks.join('');
  }
}

async function handleLine(server: StreamJsonServer, line: string): Promise<void> {
  await (
    server as unknown as {
      handleLine(line: string): Promise<void>;
    }
  ).handleLine(line);
}

function buildSession(overrides: Partial<Session> = {}): Session {
  return {
    sessionId: 'sess_1',
    turns: 3,
    handleControl: () => undefined,
    async *runTurn(): AsyncGenerator<Frame> {
      // no-op test session
    },
    ...overrides,
  };
}

describe('contentToText', () => {
  it('joins non-empty text blocks with newlines and ignores non-text blocks', () => {
    expect(
      contentToText([
        { type: 'text', text: 'first block' },
        { type: 'image', text: 'ignored' },
        { type: 'text', text: '' },
        { type: 'text', text: 'second block' },
      ]),
    ).toBe('first block\nsecond block');
  });
});

describe('StreamJsonServer custom tool result handling', () => {
  it('logs a diagnostic without a terminal result when user.custom_tool_result has no match', async () => {
    const stdout = new CaptureWritable();
    const stderr = new CaptureWritable();
    const server = new StreamJsonServer({
      session: buildSession({ handleCustomToolResult: () => false }),
      stdout,
      stderr,
    });

    await handleLine(
      server,
      JSON.stringify({
        type: 'user.custom_tool_result',
        custom_tool_use_id: 'evt_missing',
        content: [{ type: 'text', text: 'stale' }],
      }),
    );

    expect(stdout.text()).toBe('');
    expect(stderr.text()).toContain(
      'No pending custom tool use matches user.custom_tool_result custom_tool_use_id=evt_missing',
    );
  });

  it('does not emit an error result when user.custom_tool_result is applied', async () => {
    const stdout = new CaptureWritable();
    const stderr = new CaptureWritable();
    const handled: unknown[] = [];
    const server = new StreamJsonServer({
      session: buildSession({
        handleCustomToolResult: (message) => {
          handled.push(message);
          return true;
        },
      }),
      stdout,
      stderr,
    });

    await handleLine(
      server,
      JSON.stringify({
        type: 'user.custom_tool_result',
        custom_tool_use_id: 'evt_custom_1',
        content: [{ type: 'text', text: 'ok' }],
      }),
    );

    expect(handled).toHaveLength(1);
    expect(stdout.text()).toBe('');
    expect(stderr.text()).toBe('');
  });

  it('does not fabricate a terminal result for duplicate custom_tool_result delivery', async () => {
    const stdout = new CaptureWritable();
    const stderr = new CaptureWritable();
    const seen = new Set<string>();
    const server = new StreamJsonServer({
      session: buildSession({
        handleCustomToolResult: (message) => {
          const id = message.custom_tool_use_id ?? message.tool_use_id ?? '';
          if (seen.has(id)) return false;
          seen.add(id);
          return true;
        },
      }),
      stdout,
      stderr,
    });
    const line = JSON.stringify({
      type: 'user.custom_tool_result',
      custom_tool_use_id: 'evt_custom_1',
      content: [{ type: 'text', text: 'ok' }],
    });

    await handleLine(server, line);
    await handleLine(server, line);

    expect(stdout.text()).toBe('');
    expect(stderr.text()).toContain(
      'No pending custom tool use matches user.custom_tool_result custom_tool_use_id=evt_custom_1',
    );
  });
});

describe('StreamJsonServer turn completion barrier', () => {
  it('writes turn_complete for a rejected overlapping turn', async () => {
    const stdout = new CaptureWritable();
    let releaseTurn: (() => void) | undefined;
    const turn = new Promise<void>((resolve) => {
      releaseTurn = resolve;
    });
    const server = new StreamJsonServer({
      session: buildSession({
        async *runTurn(): AsyncGenerator<Frame> {
          await turn;
          yield resultFrame({ sessionId: 'sess_1' });
        },
      }),
      stdout,
    });

    await handleLine(
      server,
      JSON.stringify({ type: 'user', turn_id: 'turn_1', message: { content: 'first' } }),
    );
    await handleLine(
      server,
      JSON.stringify({ type: 'user', turn_id: 'turn_2', message: { content: 'second' } }),
    );

    expect(
      stdout
        .text()
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line)),
    ).toEqual([
      expect.objectContaining({
        type: 'result',
        is_error: true,
        result: 'A turn is already in progress',
      }),
      { type: 'system', subtype: 'turn_complete', session_id: 'sess_1', turn_id: 'turn_2' },
    ]);

    releaseTurn!();
    await new Promise((resolve) => setImmediate(resolve));
  });

  it('writes turn_complete after terminal result and deferred runTurn cleanup', async () => {
    const stdout = new CaptureWritable();
    let releaseCleanup: (() => void) | undefined;
    const cleanup = new Promise<void>((resolve) => {
      releaseCleanup = resolve;
    });
    const server = new StreamJsonServer({
      session: buildSession({
        async *runTurn(): AsyncGenerator<Frame> {
          yield resultFrame({ sessionId: 'sess_1' });
          await cleanup;
        },
      }),
      stdout,
    });

    await handleLine(
      server,
      JSON.stringify({ type: 'user', turn_id: 'turn_1', message: { content: 'first' } }),
    );
    await new Promise((resolve) => setImmediate(resolve));
    expect(
      stdout
        .text()
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line).type),
    ).toEqual(['result']);

    releaseCleanup!();
    await new Promise((resolve) => setImmediate(resolve));
    expect(
      stdout
        .text()
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line)),
    ).toEqual([
      expect.objectContaining({ type: 'result' }),
      { type: 'system', subtype: 'turn_complete', session_id: 'sess_1', turn_id: 'turn_1' },
    ]);
  });
});
