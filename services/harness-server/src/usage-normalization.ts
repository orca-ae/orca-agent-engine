// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface CacheCreationUsage {
  ephemeral_1h_input_tokens: number;
  ephemeral_5m_input_tokens: number;
}

/**
 * Preserve the provider's TTL breakdown when it carries tokens. Some frames
 * expose only a flat cache_creation_input_tokens total, and the SDK's default
 * usage object includes an all-zero nested block; in both cases the TTL cannot
 * be recovered, so retain the historical 5m fallback instead of dropping tokens.
 */
export function cacheCreationUsageFromRaw(usage: Record<string, unknown>): CacheCreationUsage {
  const nested = objectRecord(usage.cache_creation);
  const ephemeral1h = tokenCount(nested?.ephemeral_1h_input_tokens);
  const ephemeral5m = tokenCount(nested?.ephemeral_5m_input_tokens);
  if (ephemeral1h + ephemeral5m > 0) {
    return {
      ephemeral_1h_input_tokens: ephemeral1h,
      ephemeral_5m_input_tokens: ephemeral5m,
    };
  }
  return {
    ephemeral_1h_input_tokens: 0,
    ephemeral_5m_input_tokens: tokenCount(usage.cache_creation_input_tokens),
  };
}

/** The public model span uses one cache-creation total rather than a TTL split. */
export function cacheCreationTotalFromRaw(usage: Record<string, unknown>): number {
  const cacheCreation = cacheCreationUsageFromRaw(usage);
  return cacheCreation.ephemeral_1h_input_tokens + cacheCreation.ephemeral_5m_input_tokens;
}

function objectRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function isTokenCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}

function tokenCount(value: unknown): number {
  return isTokenCount(value) ? Math.floor(value) : 0;
}
