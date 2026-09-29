// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Direct spec for the shared tool-call parser: every runtime
// adapter routes through parseToolCall, so its validation and narrowing are
// contract, not implementation detail — pinned here once instead of
// indirectly through one runtime's behavior.
import { describe, expect, it } from 'vitest';
import { parseToolCall } from '../../src/sandbox-runtime.js';

describe('parseToolCall', () => {
  it('narrows a valid bash call, preserving timeout_ms only when present', () => {
    expect(parseToolCall({ tool: 'bash', args: { command: 'echo hi' } })).toEqual({
      tool: 'bash',
      args: { command: 'echo hi' },
    });
    expect(parseToolCall({ tool: 'bash', args: { command: 'sleep 1', timeout_ms: 250 } })).toEqual({
      tool: 'bash',
      args: { command: 'sleep 1', timeout_ms: 250 },
    });
  });

  it('narrows glob and grep with their own tool tags', () => {
    expect(parseToolCall({ tool: 'glob', args: { pattern: '*.ts' } })).toEqual({
      tool: 'glob',
      args: { pattern: '*.ts' },
    });
    expect(parseToolCall({ tool: 'grep', args: { pattern: 'todo', root: '/src' } })).toEqual({
      tool: 'grep',
      args: { pattern: 'todo', root: '/src' },
    });
  });

  it('rejects a known tool with a wrong or missing arg key', () => {
    // The exact bug the parser exists to prevent: {cmd: ...} compiled clean
    // under the old per-adapter casts and shipped undefined to an exec API.
    expect(parseToolCall({ tool: 'bash', args: { cmd: 'echo oops' } })).toEqual({
      tool: 'invalid',
      error: "bash tool call requires a string 'command'",
    });
    expect(parseToolCall({ tool: 'glob', args: {} })).toEqual({
      tool: 'invalid',
      error: "glob tool call requires a string 'pattern'",
    });
    expect(parseToolCall({ tool: 'grep', args: { pattern: 7 } })).toEqual({
      tool: 'invalid',
      error: "grep tool call requires a string 'pattern'",
    });
  });

  it('rejects wrong-typed optional args', () => {
    expect(parseToolCall({ tool: 'bash', args: { command: 'x', timeout_ms: '5s' } })).toEqual({
      tool: 'invalid',
      error: "bash tool call 'timeout_ms' must be a positive finite number",
    });
    // Non-finite/non-positive numbers pass typeof but poison downstream
    // transports (NaN request timeouts) — the parser rejects them.
    for (const bad of [Number.NaN, Infinity, -1, 0]) {
      expect(parseToolCall({ tool: 'bash', args: { command: 'x', timeout_ms: bad } })).toEqual({
        tool: 'invalid',
        error: "bash tool call 'timeout_ms' must be a positive finite number",
      });
    }
    expect(parseToolCall({ tool: 'grep', args: { pattern: 'x', root: 42 } })).toEqual({
      tool: 'invalid',
      error: "grep tool call 'root' must be a string",
    });
  });

  it('tolerates non-object args (null, string) without throwing', () => {
    expect(parseToolCall({ tool: 'bash', args: null })).toEqual({
      tool: 'invalid',
      error: "bash tool call requires a string 'command'",
    });
    expect(parseToolCall({ tool: 'glob', args: 'not-an-object' })).toEqual({
      tool: 'invalid',
      error: "glob tool call requires a string 'pattern'",
    });
  });

  it('passes unknown tools through as the exit-127 fallback arm', () => {
    expect(parseToolCall({ tool: 'browse', args: { url: 'x' } })).toEqual({
      tool: 'other',
      name: 'browse',
    });
  });
});
