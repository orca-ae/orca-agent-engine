// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { CronExpressionParser } from 'cron-parser';

export const DEFAULT_TRIGGER_TIMEZONE = 'Etc/UTC';
export const TRIGGER_MISFIRE_GRACE_MS = 5 * 60 * 1_000;

export class TriggerScheduleError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TriggerScheduleError';
  }
}

export class TriggerSessionTemplateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TriggerSessionTemplateError';
  }
}

export function renderCronTriggerTitle(
  template: string | null,
  triggerName: string,
  payload: string,
): string | null {
  if (template === null) return null;
  const values = { 'trigger.name': triggerName, payload };
  let rendered = template;
  for (const [name, value] of Object.entries(values)) {
    rendered = rendered.replaceAll(`\${${name}}`, value).replaceAll(`{{${name}}}`, value);
  }
  if (rendered.length > 1_024) {
    throw new TriggerSessionTemplateError(
      'session.title_template renders beyond the 1024 character Session title limit',
    );
  }
  return rendered;
}

/**
 * Validate the deliberately small v1 cron dialect and return its first future
 * UTC occurrence. `cron-parser` owns calendar, IANA-zone, and DST evaluation;
 * the checks here keep unsupported six-field/special/macro syntax out.
 */
export function nextTriggerOccurrence(expression: string, timezone: string, after: Date): Date {
  const normalized = expression.trim().replace(/\s+/g, ' ');
  const fields = normalized.split(' ');
  if (fields.length !== 5) {
    throw new TriggerScheduleError('source.schedule must contain exactly five cron fields');
  }
  if (normalized.startsWith('@') || /[?L#H]/i.test(normalized)) {
    throw new TriggerScheduleError(
      'source.schedule does not support cron macros or ?, L, #, and H syntax',
    );
  }
  if (fields.some((field) => !/^[0-9*,/-]+$/.test(field))) {
    throw new TriggerScheduleError(
      'source.schedule supports only numeric values, *, lists, ranges, and steps',
    );
  }
  validateTimeZone(timezone);

  try {
    return CronExpressionParser.parse(normalized, {
      currentDate: after,
      tz: timezone,
    })
      .next()
      .toDate();
  } catch (error) {
    if (error instanceof TriggerScheduleError) throw error;
    throw new TriggerScheduleError(
      `invalid source.schedule: ${error instanceof Error ? error.message : 'unable to parse cron'}`,
    );
  }
}

function validateTimeZone(timezone: string): void {
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: timezone }).format(new Date(0));
  } catch {
    throw new TriggerScheduleError('source.timezone must be a valid IANA time zone');
  }
}
