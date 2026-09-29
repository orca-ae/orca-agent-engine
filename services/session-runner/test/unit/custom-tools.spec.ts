// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { registerCodexSdkProvider } from '../../src/harness/codex-sdk/provider.js';
import { FakeAgentHarness } from './support/fake-agent-harness.js';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { PendingCustomToolResults } from '../../src/pending-custom-tool-results.js';
import {
  CustomToolResultSchema,
  parseCustomToolResult,
  type CustomToolResult,
} from '../../src/custom-tools.js';
import { parseSnapshotBody } from '../../src/snapshot.js';
import { buildSessionStartInput, ProviderRegistry } from '../../src/harness/provider.js';

const definition = {
  name: 'lookup_ticket',
  description: 'Find a ticket',
  input_schema: {
    type: 'object',
    properties: { id: { type: 'string' } },
    required: ['id'],
    additionalProperties: false,
  },
};
const fullContent = [
  { type: 'text', text: 'Ticket found' },
  { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'aGVsbG8=' } },
  {
    type: 'document',
    title: 'Ticket',
    context: null,
    source: { type: 'file', file_id: 'file_ticket' },
  },
  {
    type: 'search_result',
    title: 'Result',
    source: 'https://tickets.test',
    content: [{ type: 'text', text: 'Match' }],
  },
];
function reply(id: string, isError = false): CustomToolResult {
  return CustomToolResultSchema.parse({
    type: 'user.custom_tool_result',
    custom_tool_use_id: id,
    content: fullContent,
    is_error: isError,
  });
}
const bytes = (value: unknown) => Buffer.from(JSON.stringify(value));
afterEach(() => vi.useRealTimers());

describe('custom callback snapshot', () => {
  it('preserves the description and complete schema through parsing and provider projection', () => {
    const snapshot = parseSnapshotBody(
      bytes({
        provider: 'codex-sdk',
        allowed_tool_names: ['lookup_ticket'],
        custom_tools: [definition],
      }),
    );
    expect(
      buildSessionStartInput(snapshot, { workspaceId: 'ws', sessionId: 'ses' }).agentSnapshot
        .custom_tools,
    ).toEqual([definition]);
  });
  it.each(
    [
      null,
      {},
      [definition, definition],
      [{ ...definition, description: '' }],
      [{ ...definition, input_schema: { type: 'string' } }],
      [{ ...definition, input_schema: { type: 'object', required: [2] } }],
      ...['bash', 'web_fetch', 'sys_terminal_launch', 'mcp__tickets__lookup'].map((name) => [
        { ...definition, name },
      ]),
    ].map((custom_tools) => ({ custom_tools })),
  )('rejects malformed, duplicate, or reserved definitions: %j', ({ custom_tools }) => {
    expect(() =>
      parseSnapshotBody(
        bytes({
          provider: 'codex-sdk',
          allowed_tool_names: [
            'lookup_ticket',
            'bash',
            'web_fetch',
            'sys_terminal_launch',
            'mcp__tickets__lookup',
          ],
          custom_tools,
        }),
      ),
    ).toThrow('invalid custom_tools');
  });
  it('requires explicit provider capability instead of silently ignoring callbacks', () => {
    const registry = new ProviderRegistry();
    const factory = vi.fn(() => new FakeAgentHarness());
    registry.register('unsupported', factory);
    registerCodexSdkProvider(registry);
    const snapshot = parseSnapshotBody(
      bytes({
        provider: 'unsupported',
        allowed_tool_names: ['lookup_ticket'],
        custom_tools: [definition],
      }),
    );
    const context = { workspaceId: 'ws', sessionId: 'ses' };
    expect(() => registry.build(snapshot, context)).toThrow('does not support custom tools');
    expect(factory).not.toHaveBeenCalled();
    expect(() => registry.build({ ...snapshot, provider: 'codex-sdk' }, context)).not.toThrow();
  });

  it('rejects a definition excluded from the allowlist', () => {
    expect(() =>
      parseSnapshotBody(
        bytes({ provider: 'codex-sdk', allowed_tool_names: [], custom_tools: [definition] }),
      ),
    ).toThrow('invalid custom_tools');
  });
});

describe('pending custom tool results', () => {
  it.each([false, true])(
    'preserves every content block and is_error=%s; first result wins',
    async (isError) => {
      const pending = new PendingCustomToolResults();
      expect(pending.resolve(reply('unknown'))).toBe(false);
      const waiting = pending.park('evt_one', new AbortController().signal);
      expect(pending.hasPending()).toBe(true);
      expect(pending.resolve(reply('evt_one', isError))).toBe(true);
      expect(pending.resolve(reply('evt_one', !isError))).toBe(false);
      expect(await waiting).toEqual(reply('evt_one', isError));
      expect(pending.hasPending()).toBe(false);
    },
  );
  it('times out and ignores late results without buffering them', async () => {
    vi.useFakeTimers();
    const pending = new PendingCustomToolResults(10);
    const waiting = pending.park('evt_old', new AbortController().signal);
    const rejection = expect(waiting).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(10);
    await rejection;
    expect(pending.resolve(reply('evt_old'))).toBe(false);
    expect(pending.hasPending()).toBe(false);
  });
  it('aborts and resets all waits; an old generation cannot satisfy a fresh call', async () => {
    const pending = new PendingCustomToolResults();
    const abort = new AbortController();
    const cancelled = pending.park('evt_cancelled', abort.signal);
    const rejected = expect(cancelled).rejects.toThrow('aborted');
    abort.abort();
    await rejected;
    const old = pending.park('evt_old', new AbortController().signal);
    const abandoned = expect(old).rejects.toThrow('abandoned');
    pending.reset();
    await abandoned;
    const fresh = pending.park('evt_fresh', new AbortController().signal);
    expect(pending.resolve(reply('evt_old'))).toBe(false);
    expect(pending.hasPending()).toBe(true);
    pending.resolve(reply('evt_fresh'));
    await fresh;
    expect(pending.hasPending()).toBe(false);
  });
  it('bounds outstanding waits and releases the table on reset', async () => {
    const pending = new PendingCustomToolResults();
    const waits = Array.from({ length: 256 }, (_, i) =>
      pending.park(`evt_${i}`, new AbortController().signal).catch((error: Error) => error.message),
    );
    expect(() => pending.park('evt_overflow', new AbortController().signal)).toThrow(
      'too many pending',
    );
    pending.reset();
    expect(await Promise.all(waits)).toEqual(Array(256).fill('custom tool callback abandoned'));
    expect(pending.hasPending()).toBe(false);
  });

  it('refuses duplicate parking without abandoning the first wait', async () => {
    const pending = new PendingCustomToolResults();
    const first = pending.park('evt_one', new AbortController().signal);
    expect(() => pending.park('evt_one', new AbortController().signal)).toThrow('already pending');
    pending.resolve(reply('evt_one'));
    await first;
  });
});

describe('custom result wire parser', () => {
  it('accepts Registry transcript envelope fields without changing public blocks', () => {
    expect(
      parseCustomToolResult(
        bytes({ ...reply('evt_one', true), id: 'evt_client', processed_at: null }),
      ),
    ).toEqual(reply('evt_one', true));
    expect(
      parseCustomToolResult(
        bytes({ type: 'user.custom_tool_result', custom_tool_use_id: 'evt_one', is_error: null }),
      ),
    ).toEqual({ type: 'user.custom_tool_result', custom_tool_use_id: 'evt_one', is_error: null });
  });
  it.each([
    {},
    { type: 'user.message', custom_tool_use_id: 'evt' },
    { type: 'user.custom_tool_result', custom_tool_use_id: '' },
    { ...reply('evt'), is_error: 'false' },
    { ...reply('evt'), content: [{ type: 'image', source: { type: 'url', url: 'invalid' } }] },
    {
      ...reply('evt'),
      content: [{ type: 'document', source: { type: 'file', file_id: 'other' } }],
    },
    { ...reply('evt'), content: [{ type: 'text', text: 1 }] },
    { ...reply('evt'), content: [{ type: 'unknown' }] },
  ])('rejects malformed public results: %j', (value) =>
    expect(() => parseCustomToolResult(bytes(value))).toThrow(),
  );
});
