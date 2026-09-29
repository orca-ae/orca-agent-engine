// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { renderFrame } from '../../src/render.js';
import { noColor } from '../../src/colors.js';

const c = noColor;

describe('renderFrame', () => {
  it('renders agent.message text blocks as assistant text', () => {
    const out = renderFrame(
      { type: 'agent.message', content: [{ type: 'text', text: 'Hello there' }] },
      c,
    );
    expect(out).not.toBeNull();
    expect(out).toContain('Hello there');
  });

  it('renders a thinking block distinctly from plain text', () => {
    const out = renderFrame(
      { type: 'agent.message', content: [{ type: 'thinking', thinking: 'pondering' }] },
      c,
    );
    expect(out).not.toBeNull();
    expect(out!.toLowerCase()).toContain('thinking');
    expect(out).toContain('pondering');
  });

  it('renders agent.tool_use with the tool name and input', () => {
    const out = renderFrame(
      {
        type: 'agent.tool_use',
        name: 'mcp__orca__bash',
        input: { command: 'ls' },
        tool_use_id: 't1',
      },
      c,
    );
    expect(out).not.toBeNull();
    expect(out).toContain('mcp__orca__bash');
    expect(out).toContain('ls');
  });

  it('renders agent.tool_result content', () => {
    const out = renderFrame(
      { type: 'agent.tool_result', tool_use_id: 't1', content: 'file-a\nfile-b', is_error: false },
      c,
    );
    expect(out).not.toBeNull();
    expect(out).toContain('file-a');
  });

  it('marks an errored tool_result', () => {
    const out = renderFrame(
      { type: 'agent.tool_result', tool_use_id: 't1', content: 'boom', is_error: true },
      c,
    );
    expect(out).not.toBeNull();
    expect(out!.toLowerCase()).toContain('error');
  });

  it('renders agent.error message', () => {
    const out = renderFrame({ type: 'agent.error', message: 'the model failed' }, c);
    expect(out).not.toBeNull();
    expect(out).toContain('the model failed');
  });

  it('returns null for turn_completed (no visible line; it is a control marker)', () => {
    expect(renderFrame({ type: 'agent.turn_completed' }, c)).toBeNull();
  });

  it('returns null for a requires_action frame (handled by the confirmation prompt)', () => {
    expect(
      renderFrame(
        {
          type: 'agent.requires_action',
          action: 'tool_confirmation',
          tool_use_id: 't1',
          tool_name: 'Bash',
        },
        c,
      ),
    ).toBeNull();
  });

  it('returns null for internal/unknown frames like agent.usage', () => {
    expect(renderFrame({ type: 'agent.usage', usage: { input_tokens: 1 } }, c)).toBeNull();
  });
});
