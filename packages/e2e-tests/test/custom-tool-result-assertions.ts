// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';

/** Verify the ticket callback's wire payload separately from the model's presentation. */
export function expectTicketCallbackRoundTrip(
  assistant: string,
  events: ReadonlyArray<Record<string, unknown>>,
  expected: { callId: string; marker: string; text: string },
): void {
  const callbacks = events.filter((event) => event.type === 'user.custom_tool_result');
  expect(callbacks).toHaveLength(1);
  expect(callbacks[0]?.custom_tool_use_id).toBe(expected.callId);
  expect(callbacks[0]?.content).toEqual([{ type: 'text', text: expected.text }]);
  expect(assistant).toContain(expected.marker);
  expect(assistant).toMatch(/\bready\b/i);
}
