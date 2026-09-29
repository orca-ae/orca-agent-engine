// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import { parseEventsLimit } from '../../src/api/sessions.routes.js';
import { isValidSessionEventsCursor } from '../../src/events/session-events-index.js';

describe('session events query helpers', () => {
  it('normalizes event limit query values', () => {
    expect(parseEventsLimit(undefined)).toBe(100);
    expect(parseEventsLimit('')).toBe(100);
    expect(parseEventsLimit('nan')).toBe(100);
    expect(parseEventsLimit('0')).toBe(100);
    expect(parseEventsLimit('-1')).toBe(100);
    expect(parseEventsLimit('1.5')).toBe(100);
    expect(parseEventsLimit('2e3')).toBe(100);
    expect(parseEventsLimit('25')).toBe(25);
    expect(parseEventsLimit('2000')).toBe(1000);
  });

  it('accepts only bounded numeric cursors', () => {
    expect(isValidSessionEventsCursor('')).toBe(true);
    expect(isValidSessionEventsCursor('0')).toBe(true);
    expect(isValidSessionEventsCursor('123')).toBe(true);
    expect(isValidSessionEventsCursor('123:evt_abc')).toBe(true);
    expect(isValidSessionEventsCursor('123:2:evt_abc')).toBe(true);
    expect(isValidSessionEventsCursor('abc')).toBe(false);
    expect(isValidSessionEventsCursor('-1')).toBe(false);
    expect(isValidSessionEventsCursor('1.5')).toBe(false);
    expect(isValidSessionEventsCursor(String(Number.MAX_SAFE_INTEGER))).toBe(true);
    expect(isValidSessionEventsCursor(String(Number.MAX_SAFE_INTEGER + 1))).toBe(false);
    expect(isValidSessionEventsCursor('9223372036854775807')).toBe(false);
    expect(isValidSessionEventsCursor('9'.repeat(100))).toBe(false);
    expect(isValidSessionEventsCursor('123:x:evt_abc')).toBe(false);
  });
});
