// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Orca Session metadata extension for choosing the separate harness's model exit. */
export const SESSION_LLM_EGRESS_KEY = 'orca_llm_egress';

export function sessionLlmEgressMetadataError(metadata: Record<string, unknown>): string | null {
  const value = metadata[SESSION_LLM_EGRESS_KEY];
  if (value === undefined || value === 'direct' || value === 'gateway') return null;
  return `metadata.${SESSION_LLM_EGRESS_KEY} must be "direct" or "gateway"`;
}
