// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export const MAX_DELIVERY_RETRY_DELAY_MS = 60 * 60 * 1_000;

const BASE_RETRY_DELAY_MS = 1_000;
const MAX_BACKOFF_DELAY_MS = 5 * 60 * 1_000;
const JITTER_FRACTION = 0.25;
const MAX_RETRY_AFTER_HEADER_LENGTH = 128;
const WEEKDAY_NAMES: readonly string[] = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const WEEKDAY_LONG_NAMES: readonly string[] = [
  'Sunday',
  'Monday',
  'Tuesday',
  'Wednesday',
  'Thursday',
  'Friday',
  'Saturday',
];
const MONTH_NAMES: readonly string[] = [
  'Jan',
  'Feb',
  'Mar',
  'Apr',
  'May',
  'Jun',
  'Jul',
  'Aug',
  'Sep',
  'Oct',
  'Nov',
  'Dec',
];
const WEEKDAY = `(?:${WEEKDAY_NAMES.join('|')})`;
const WEEKDAY_LONG = `(?:${WEEKDAY_LONG_NAMES.join('|')})`;
const MONTH = `(?:${MONTH_NAMES.join('|')})`;
const IMF_FIXDATE_PATTERN = new RegExp(
  `^(${WEEKDAY}), ([0-9]{2}) (${MONTH}) ([0-9]{4}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) GMT$`,
  'u',
);
const RFC_850_DATE_PATTERN = new RegExp(
  `^(${WEEKDAY_LONG}), ([0-9]{2})-(${MONTH})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) GMT$`,
  'u',
);
const ASCTIME_DATE_PATTERN = new RegExp(
  `^(${WEEKDAY}) (${MONTH}) ( [0-9]|[0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) ([0-9]{4})$`,
  'u',
);

/** Parse Retry-After without retaining its raw value. Result is a bounded minimum delay. */
export function parseRetryAfterMs(raw: string | null, nowMs = Date.now()): number | undefined {
  if (raw === null || raw.length > MAX_RETRY_AFTER_HEADER_LENGTH || !Number.isSafeInteger(nowMs)) {
    return undefined;
  }
  const value = raw.trim();
  if (value.length === 0) return undefined;

  if (/^[0-9]+$/u.test(value)) {
    const seconds = BigInt(value);
    const maximumSeconds = BigInt(MAX_DELIVERY_RETRY_DELAY_MS / 1_000);
    if (seconds >= maximumSeconds) return MAX_DELIVERY_RETRY_DELAY_MS;
    return Number(seconds) * 1_000;
  }

  const retryAt = parseHttpDate(value, nowMs);
  if (retryAt === undefined) return undefined;
  return Math.min(MAX_DELIVERY_RETRY_DELAY_MS, Math.max(0, retryAt - nowMs));
}

function parseHttpDate(value: string, nowMs: number): number | undefined {
  const imf = IMF_FIXDATE_PATTERN.exec(value);
  if (imf !== null) {
    return utcTimestamp(imf[1]!, imf[2]!, imf[3]!, imf[4]!, imf[5]!, imf[6]!, imf[7]!);
  }

  const rfc850 = RFC_850_DATE_PATTERN.exec(value);
  if (rfc850 !== null) {
    const current = new Date(nowMs);
    const currentYear = current.getUTCFullYear();
    if (!Number.isFinite(currentYear)) return undefined;
    const shortYear = Number(rfc850[4]);
    let fullYear = Math.floor((currentYear + 50 - shortYear) / 100) * 100 + shortYear;
    const candidate = utcTimestamp(
      rfc850[1]!,
      rfc850[2]!,
      rfc850[3]!,
      String(fullYear),
      rfc850[5]!,
      rfc850[6]!,
      rfc850[7]!,
      false,
    );
    if (candidate === undefined) return undefined;
    const cutoff = new Date(nowMs);
    cutoff.setUTCFullYear(currentYear + 50);
    if (candidate > cutoff.getTime()) fullYear -= 100;
    return utcTimestamp(
      rfc850[1]!,
      rfc850[2]!,
      rfc850[3]!,
      String(fullYear),
      rfc850[5]!,
      rfc850[6]!,
      rfc850[7]!,
    );
  }

  const asctime = ASCTIME_DATE_PATTERN.exec(value);
  if (asctime === null) return undefined;
  return utcTimestamp(
    asctime[1]!,
    asctime[3]!,
    asctime[2]!,
    asctime[7]!,
    asctime[4]!,
    asctime[5]!,
    asctime[6]!,
  );
}

function utcTimestamp(
  weekday: string,
  dayText: string,
  monthName: string,
  yearText: string,
  hourText: string,
  minuteText: string,
  secondText: string,
  validateWeekday = true,
): number | undefined {
  const weekdayIndex = WEEKDAY_NAMES.indexOf(weekday);
  const longWeekdayIndex = WEEKDAY_LONG_NAMES.indexOf(weekday);
  const expectedWeekday = weekdayIndex === -1 ? longWeekdayIndex : weekdayIndex;
  const month = MONTH_NAMES.indexOf(monthName);
  const day = Number(dayText);
  const year = Number(yearText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  if (expectedWeekday === -1 || month === -1) return undefined;

  const parsed = new Date(0);
  parsed.setUTCFullYear(year, month, day);
  parsed.setUTCHours(hour, minute, second, 0);
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month ||
    parsed.getUTCDate() !== day ||
    parsed.getUTCHours() !== hour ||
    parsed.getUTCMinutes() !== minute ||
    parsed.getUTCSeconds() !== second ||
    (validateWeekday && parsed.getUTCDay() !== expectedWeekday)
  ) {
    return undefined;
  }
  const timestamp = parsed.getTime();
  return Number.isFinite(timestamp) ? timestamp : undefined;
}

/** Exponential retry delay with bounded jitter and optional server minimum. */
export function deliveryRetryDelayMs(
  attemptCount: number,
  retryAfterMs: number | undefined,
  random: () => number = Math.random,
): number {
  if (!Number.isSafeInteger(attemptCount) || attemptCount < 0) {
    throw new Error('observability exporter retry attempt count is invalid');
  }
  if (
    retryAfterMs !== undefined &&
    (!Number.isSafeInteger(retryAfterMs) ||
      retryAfterMs < 0 ||
      retryAfterMs > MAX_DELIVERY_RETRY_DELAY_MS)
  ) {
    throw new Error('observability exporter Retry-After delay is invalid');
  }
  const randomValue = random();
  if (!Number.isFinite(randomValue) || randomValue < 0 || randomValue >= 1) {
    throw new Error('observability exporter retry jitter source is invalid');
  }

  const exponent = Math.min(attemptCount, 30);
  const exponential = Math.min(MAX_BACKOFF_DELAY_MS, BASE_RETRY_DELAY_MS * 2 ** exponent);
  const jitterWindow = Math.floor(exponential * JITTER_FRACTION);
  const jitter = Math.floor(jitterWindow * randomValue);
  const backoff =
    exponential === MAX_BACKOFF_DELAY_MS
      ? exponential - jitterWindow + jitter
      : Math.min(MAX_BACKOFF_DELAY_MS, exponential + jitter);
  return Math.max(backoff, retryAfterMs ?? 0);
}
