// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { DSH_LLM_ROUTE, validateHarnessModel } from '@orca/harness-catalog';
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

/** One fixed native Messages route/model, narrowed by deployment ACLs in both mint paths. */
export function dshLlmGrants(
  policy: { routes: string[]; models: string[] } | undefined,
  model: { provider: string; id: string; effort?: string; speed?: string },
): { llm_routes: string[]; llm_models: string[] } | null {
  if (
    validateHarnessModel({ harness: 'dsh_sdk' }, model) ||
    !policy?.routes.includes(DSH_LLM_ROUTE)
  )
    return null;
  const models = intersectLlmModels(policy.models, [model.id]);
  return models.length ? { llm_routes: [DSH_LLM_ROUTE], llm_models: models } : null;
}
