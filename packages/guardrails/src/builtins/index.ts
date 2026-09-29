// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { BuiltinEvaluator } from '../engine.js';
import { costBudget, subagentCostBudget, tokenBudget, userDailyCostBudget } from './cost.js';
import { detectLoop, detectThrashing } from './context.js';
import { githubPolicy } from './github.js';
import { gcalendarPolicy, gdrivePolicy, gmailPolicy } from './google.js';
import {
  askOnOsTools,
  blockSkills,
  blockTools,
  denyPiiInLlmRequest,
  headlessSubagentPurposeGuard,
  maxToolCallsPerSession,
  readOnlyOs,
  requireApprovalForTools,
  spawnBounds,
} from './safety.js';
import { blastRadius, blockWorkingDirChanges, worktreeGuard } from './shell.js';

/**
 * The evaluator registry.
 *
 * A catalog entry with no evaluator here is a guardrail an operator can create
 * and that never fires — the failure mode this package exists to prevent — so a
 * test asserts the two stay in step.
 */
export const BUILTIN_EVALUATORS: ReadonlyMap<string, BuiltinEvaluator> = new Map<
  string,
  BuiltinEvaluator
>([
  ['block_tools', blockTools],
  ['require_approval_for_tools', requireApprovalForTools],
  ['ask_on_os_tools', askOnOsTools],
  ['read_only_os', readOnlyOs],
  ['block_skills', blockSkills],
  ['headless_subagent_purpose_guard', headlessSubagentPurposeGuard],
  ['max_tool_calls_per_session', maxToolCallsPerSession],
  ['spawn_bounds', spawnBounds],
  ['deny_pii_in_llm_request', denyPiiInLlmRequest],
  ['token_budget', tokenBudget],
  ['cost_budget', costBudget],
  ['user_daily_cost_budget', userDailyCostBudget],
  ['subagent_cost_budget', subagentCostBudget],
  ['detect_loop', detectLoop],
  ['detect_thrashing', detectThrashing],
  ['github_policy', githubPolicy],
  ['gdrive_policy', gdrivePolicy],
  ['gmail_policy', gmailPolicy],
  ['gcalendar_policy', gcalendarPolicy],
  ['blast_radius', blastRadius],
  ['block_working_dir_changes', blockWorkingDirChanges],
  ['worktree_guard', worktreeGuard],
]);

export * from './safety.js';
export * from './cost.js';
export * from './context.js';
export * from './shell.js';
export * from './github.js';
export * from './google.js';
