// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  DEFAULT_TRIGGER_TIMEZONE,
  nextTriggerOccurrence,
  renderCronTriggerTitle,
  TriggerScheduleError,
} from '../../src/domain/trigger-cron.js';

describe('cron Trigger schedules', () => {
  it('computes the first future five-field occurrence in the configured zone', () => {
    expect(
      nextTriggerOccurrence(
        '  15   9  * * 1-5 ',
        'Asia/Shanghai',
        new Date('2026-08-17T01:14:00.000Z'),
      ).toISOString(),
    ).toBe('2026-08-17T01:15:00.000Z');
    expect(
      nextTriggerOccurrence(
        '* * * * *',
        DEFAULT_TRIGGER_TIMEZONE,
        new Date('2026-08-17T01:14:00Z'),
      ).toISOString(),
    ).toBe('2026-08-17T01:15:00.000Z');
  });

  it.each([
    ['@daily', 'exactly five cron fields'],
    ['0 0 1 1 * 2027', 'exactly five cron fields'],
    ['0 0 L * *', 'does not support cron macros'],
    ['0 0 ? * *', 'does not support cron macros'],
    ['0 0 * JAN *', 'supports only numeric values'],
  ])('rejects unsupported expression %s', (expression, message) => {
    expect(() =>
      nextTriggerOccurrence(expression, DEFAULT_TRIGGER_TIMEZONE, new Date('2026-01-01T00:00:00Z')),
    ).toThrowError(message);
  });

  it('rejects invalid IANA time zones with a stable API error', () => {
    expect(() =>
      nextTriggerOccurrence('* * * * *', 'Not/A_Zone', new Date('2026-01-01T00:00:00Z')),
    ).toThrowError(new TriggerScheduleError('source.timezone must be a valid IANA time zone'));
  });

  it('uses deterministic cron-parser DST behavior', () => {
    // The missing 02:30 wall-clock time is normalized to 03:30 on spring-forward.
    expect(
      nextTriggerOccurrence(
        '30 2 * * *',
        'America/New_York',
        new Date('2026-03-08T05:00:00Z'),
      ).toISOString(),
    ).toBe('2026-03-08T07:30:00.000Z');

    // A repeated 01:30 wall-clock time fires once, at the earlier UTC instant.
    expect(
      nextTriggerOccurrence(
        '30 1 * * *',
        'America/New_York',
        new Date('2026-11-01T04:00:00Z'),
      ).toISOString(),
    ).toBe('2026-11-01T05:30:00.000Z');
  });

  it('renders the cron fields supported by session.title_template', () => {
    expect(
      renderCronTriggerTitle(
        '${trigger.name}: {{payload}} (${future.context})',
        'daily-report',
        'ready',
      ),
    ).toBe('daily-report: ready (${future.context})');
    expect(renderCronTriggerTitle(null, 'daily-report', 'ready')).toBeNull();
    expect(() => renderCronTriggerTitle('${payload}', 'large', 'x'.repeat(1_025))).toThrowError(
      'renders beyond the 1024 character Session title limit',
    );
  });
});
