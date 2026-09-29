// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { buildReplayTurns } from '../../src/harness/in-sandbox/replay.js';

describe('buildReplayTurns', () => {
  it('returns [] for empty entries', () => {
    expect(buildReplayTurns([])).toEqual([]);
  });

  it('maps user.message with string content → user turn', () => {
    const result = buildReplayTurns([{ type: 'user.message', content: 'hello there' }]);
    expect(result).toEqual([{ role: 'user', text: 'hello there' }]);
  });

  it('maps agent.message with text block array → assistant turn', () => {
    const result = buildReplayTurns([
      { type: 'agent.message', content: [{ type: 'text', text: 'I can help with that.' }] },
    ]);
    expect(result).toEqual([{ role: 'assistant', text: 'I can help with that.' }]);
  });

  it('skips tool_use entries', () => {
    const result = buildReplayTurns([
      { type: 'user.message', content: 'run ls' },
      { type: 'agent.tool_use', name: 'Bash', input: { command: 'ls' } },
      { type: 'agent.message', content: [{ type: 'text', text: 'done' }] },
    ]);
    expect(result).toEqual([
      { role: 'user', text: 'run ls' },
      { role: 'assistant', text: 'done' },
    ]);
  });

  it('skips agent.tool_result entries', () => {
    const result = buildReplayTurns([
      { type: 'agent.tool_result', tool_use_id: 'tu_1', content: 'ok' },
    ]);
    expect(result).toEqual([]);
  });

  it('skips agent.turn_completed entries', () => {
    const result = buildReplayTurns([{ type: 'agent.turn_completed', usage: {} }]);
    expect(result).toEqual([]);
  });

  it('drops empty-text turns (empty string content)', () => {
    const result = buildReplayTurns([{ type: 'user.message', content: '' }]);
    expect(result).toEqual([]);
  });

  it('drops empty-text turns (blocks with no text-type blocks)', () => {
    const result = buildReplayTurns([
      { type: 'agent.message', content: [{ type: 'tool_use', id: 'tu_1' }] },
    ]);
    expect(result).toEqual([]);
  });

  it('drops empty-text turns (array with only empty text blocks)', () => {
    const result = buildReplayTurns([
      { type: 'agent.message', content: [{ type: 'text', text: '' }] },
    ]);
    expect(result).toEqual([]);
  });

  it('joins multiple text blocks with newline', () => {
    const result = buildReplayTurns([
      {
        type: 'agent.message',
        content: [
          { type: 'text', text: 'part one' },
          { type: 'tool_use', id: 'tu_2' }, // non-text block — skipped
          { type: 'text', text: 'part two' },
        ],
      },
    ]);
    expect(result).toEqual([{ role: 'assistant', text: 'part one\npart two' }]);
  });

  it('handles mixed sequence and preserves order', () => {
    const result = buildReplayTurns([
      { type: 'user.message', content: 'first user message' },
      { type: 'agent.message', content: [{ type: 'text', text: 'first assistant reply' }] },
      { type: 'agent.tool_use', name: 'Bash', input: {} },
      { type: 'agent.tool_result', tool_use_id: 'tu_3', content: 'result' },
      { type: 'user.message', content: 'second user message' },
      { type: 'agent.message', content: [{ type: 'text', text: 'second assistant reply' }] },
      { type: 'agent.turn_completed', usage: {} },
    ]);
    expect(result).toEqual([
      { role: 'user', text: 'first user message' },
      { role: 'assistant', text: 'first assistant reply' },
      { role: 'user', text: 'second user message' },
      { role: 'assistant', text: 'second assistant reply' },
    ]);
  });

  it('handles missing content field gracefully → drops turn', () => {
    const result = buildReplayTurns([{ type: 'user.message' }]);
    expect(result).toEqual([]);
  });

  it('handles null content gracefully → drops turn', () => {
    const result = buildReplayTurns([{ type: 'agent.message', content: null }]);
    expect(result).toEqual([]);
  });
});
