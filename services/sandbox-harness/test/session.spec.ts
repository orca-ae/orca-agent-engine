// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import { Session } from '../src/session.js';
import type { Provider, WireFrame } from '../src/providers/types.js';

describe('Session terminal result handling', () => {
  it('synthesizes one error result and retains assistant history when runtime throws before a result', async () => {
    const provider: Provider = {
      id: 'fixture',
      createRuntime: () => ({
        model: 'fixture-model',
        async *runTurn(): AsyncGenerator<WireFrame> {
          yield {
            type: 'assistant',
            message: { content: [{ type: 'text', text: 'retained answer' }] },
            parent_tool_use_id: null,
          };
          throw new Error('provider failed');
        },
      }),
    };
    const session = new Session({ provider, stderr: { write: () => true } });

    const frames = [];
    for await (const frame of session.runTurn({ prompt: 'question', content: 'question' })) {
      frames.push(frame);
    }

    const results = frames.filter((frame) => frame.type === 'result');
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({ is_error: true, result: 'provider failed' });
    expect(session.history).toEqual([
      { role: 'user', text: 'question' },
      { role: 'assistant', text: 'retained answer' },
    ]);
  });

  it('does not synthesize a second result when runtime cleanup throws after a result', async () => {
    const provider: Provider = {
      id: 'fixture',
      createRuntime: () => ({
        model: 'fixture-model',
        async *runTurn(): AsyncGenerator<WireFrame> {
          yield {
            type: 'assistant',
            message: { content: [{ type: 'text', text: 'retained answer' }] },
            parent_tool_use_id: null,
          };
          yield {
            type: 'result',
            subtype: 'success',
            session_id: 'provider-session',
            duration_ms: 0,
            duration_api_ms: 0,
            is_error: false,
            num_turns: 1,
            total_cost_usd: 0,
            usage: {},
            result: 'retained answer',
          };
          throw new Error('cleanup failed');
        },
      }),
    };
    const diagnostics: string[] = [];
    const session = new Session({
      provider,
      stderr: {
        write: (chunk) => diagnostics.push(chunk),
      },
    });

    const frames = [];
    for await (const frame of session.runTurn({ prompt: 'question', content: 'question' })) {
      frames.push(frame);
    }

    expect(frames.filter((frame) => frame.type === 'result')).toHaveLength(1);
    expect(diagnostics).toEqual(['runtime error after terminal result: cleanup failed\n']);
    expect(session.history).toEqual([
      { role: 'user', text: 'question' },
      { role: 'assistant', text: 'retained answer' },
    ]);
  });
});

describe('Session model controls', () => {
  it('keeps the current model when the runtime rejects set_model', async () => {
    let runtimeModel = 'claude-opus-5';
    const provider: Provider = {
      id: 'fixture',
      createRuntime: () => ({
        get model(): string {
          return runtimeModel;
        },
        setModel(model: string): void {
          if (model === 'claude-sonnet-4-6') {
            throw new Error('active fast mode does not support claude-sonnet-4-6');
          }
          runtimeModel = model;
        },
        async *runTurn(): AsyncGenerator<WireFrame> {
          // No-op fixture.
        },
      }),
    };
    const session = new Session({
      provider,
      model: runtimeModel,
      stderr: { write: () => true },
    });

    await expect(
      session.handleControl({ subtype: 'set_model', model: 'claude-sonnet-4-6' }),
    ).rejects.toThrow(/active fast mode/);
    expect(session.model).toBe('claude-opus-5');
    expect(runtimeModel).toBe('claude-opus-5');
  });
});
