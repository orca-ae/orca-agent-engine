// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { MAX_MODEL_ID_LENGTH } from '../contracts/model-wire.js';

/** Match the Gateway YAML ACL's exact/simple-star glob semantics. */
export function llmModelPatternMatches(pattern: string, model: string): boolean {
  if (pattern === '*') return true;
  const startsWithStar = pattern.startsWith('*');
  const endsWithStar = pattern.endsWith('*');
  if (startsWithStar && endsWithStar) return model.includes(pattern.replace(/^\*+|\*+$/g, ''));
  if (startsWithStar) return model.endsWith(pattern.replace(/^\*+/, ''));
  if (endsWithStar) return model.startsWith(pattern.replace(/\*+$/, ''));
  return pattern === model;
}

/** Narrow deployment policy patterns to the session's concrete model ids. */
export function intersectLlmModels(patterns: string[], sessionModels: string[]): string[] {
  return (
    [...new Set(sessionModels)]
      // JWT model claims are consumed as Gateway ACL patterns, so only concrete
      // session model ids may cross this boundary. The length check also
      // protects JWT minting from oversized legacy snapshots.
      .filter((model) => model.length <= MAX_MODEL_ID_LENGTH && !model.includes('*'))
      .filter((model) => patterns.some((pattern) => llmModelPatternMatches(pattern, model)))
  );
}
