// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { expectTicketCallbackRoundTrip } from './custom-tool-result-assertions.js';

const marker = 'colocated-custom-opaque-result-token';
const text = marker + ' status=ready';
const expected = { callId: 'evt_custom_call', marker, text };
const callback = {
  type: 'user.custom_tool_result',
  custom_tool_use_id: expected.callId,
  content: [{ type: 'text', text }],
};

describe('custom-tool ticket callback assertions', () => {
  it.each([
    text,
    'Here is the result:\n> ' + marker + ' status=**ready**',
    marker + ': **Status:** READY',
    '**Status:** `ready`\n**Details:** ' + marker,
  ])('accepts a completed callback regardless of model formatting: %s', (assistant) => {
    expectTicketCallbackRoundTrip(assistant, [callback], expected);
  });

  it.each(['The ticket is ready.', marker + ' status=failed'])(
    'rejects missing result evidence: %s',
    (assistant) => {
      expect(() => expectTicketCallbackRoundTrip(assistant, [callback], expected)).toThrow();
    },
  );

  it.each([
    { name: 'missing', events: [] },
    { name: 'duplicated', events: [callback, callback] },
    { name: 'wrong call ID', events: [{ ...callback, custom_tool_use_id: 'evt_other_call' }] },
    {
      name: 'changed content',
      events: [{ ...callback, content: [{ type: 'text', text: marker + ' status=failed' }] }],
    },
    {
      name: 'extra content',
      events: [
        { ...callback, content: [...callback.content, { type: 'text', text: 'extra content' }] },
      ],
    },
  ])('rejects a $name callback', ({ events }) => {
    expect(() => expectTicketCallbackRoundTrip(text, events, expected)).toThrow();
  });
});
