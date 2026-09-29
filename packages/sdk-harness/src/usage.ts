// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Complete raw SDK counters, before converting total input into uncached input. */
export interface SdkTerminalUsage {
  input_tokens: number;
  output_tokens: number;
  cached_input_tokens: number;
  cache_write_input_tokens?: number;
  cache_write_input_tokens_1h?: number;
}

export function assertSdkTerminalUsage(value: unknown): asserts value is SdkTerminalUsage {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Invalid managed SDK terminal usage');
  const usage = value as Record<string, unknown>;
  const validCount = (count: unknown): count is number =>
    typeof count === 'number' && Number.isSafeInteger(count) && count >= 0;
  if (
    !validCount(usage.input_tokens) ||
    !validCount(usage.output_tokens) ||
    !validCount(usage.cached_input_tokens) ||
    usage.cached_input_tokens > usage.input_tokens ||
    (Object.hasOwn(usage, 'cache_write_input_tokens') &&
      !validCount(usage.cache_write_input_tokens)) ||
    (Object.hasOwn(usage, 'cache_write_input_tokens_1h') &&
      (!validCount(usage.cache_write_input_tokens_1h) ||
        usage.cache_write_input_tokens_1h >
          ((usage.cache_write_input_tokens as number | undefined) ?? 0))) ||
    !Number.isSafeInteger(
      usage.input_tokens +
        usage.output_tokens +
        ((usage.cache_write_input_tokens as number | undefined) ?? 0),
    )
  )
    throw new Error('Invalid managed SDK terminal usage');
}
