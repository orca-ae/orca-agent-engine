// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { stripDatedSnapshotSuffix } from './pricing.js';

/** Claude models whose current Agent SDK runtime supports managed fast mode. */
export const CLAUDE_FAST_MODE_MODEL_IDS = [
  'claude-opus-5-5',
  'claude-opus-5',
  'claude-opus-4-8',
] as const;

export type ClaudeFastModeModelId = (typeof CLAUDE_FAST_MODE_MODEL_IDS)[number];

export const CLAUDE_MODEL_EFFORT_LEVELS = ['low', 'medium', 'high', 'xhigh', 'max'] as const;
export type ClaudeModelEffort = (typeof CLAUDE_MODEL_EFFORT_LEVELS)[number];

export interface ClaudeModelEffortCapability {
  supportedEfforts: readonly ClaudeModelEffort[];
  defaultEffort: ClaudeModelEffort;
}

const ALL_EFFORT_LEVELS = CLAUDE_MODEL_EFFORT_LEVELS;
const EFFORT_LEVELS_WITHOUT_XHIGH = ['low', 'medium', 'high', 'max'] as const;
const LEGACY_EFFORT_LEVELS = ['low', 'medium', 'high'] as const;

/**
 * Anthropic effort availability is model-specific. Keep this catalog shared by
 * Registry and both Claude runtimes so invalid controls fail before inference.
 * A trailing dated snapshot suffix (for example `-20251101`) has the same
 * capability as its base alias.
 */
const CLAUDE_MODEL_EFFORT_CAPABILITIES: Readonly<Record<string, ClaudeModelEffortCapability>> = {
  'claude-opus-5-5': {
    supportedEfforts: ALL_EFFORT_LEVELS,
    defaultEffort: 'medium',
  },
  'claude-opus-5': {
    supportedEfforts: ALL_EFFORT_LEVELS,
    defaultEffort: 'high',
  },
  'claude-opus-4-8': {
    supportedEfforts: ALL_EFFORT_LEVELS,
    defaultEffort: 'high',
  },
  'claude-opus-4-7': {
    supportedEfforts: ALL_EFFORT_LEVELS,
    defaultEffort: 'high',
  },
  'claude-opus-4-6': {
    supportedEfforts: EFFORT_LEVELS_WITHOUT_XHIGH,
    defaultEffort: 'high',
  },
  'claude-sonnet-5': {
    supportedEfforts: ALL_EFFORT_LEVELS,
    defaultEffort: 'high',
  },
  'claude-sonnet-4-6': {
    supportedEfforts: EFFORT_LEVELS_WITHOUT_XHIGH,
    defaultEffort: 'high',
  },
  'claude-fable-5': {
    supportedEfforts: ALL_EFFORT_LEVELS,
    defaultEffort: 'high',
  },
  'claude-opus-4-5': {
    supportedEfforts: LEGACY_EFFORT_LEVELS,
    defaultEffort: 'high',
  },
};

const CLAUDE_FAST_MODE_MODELS = new Set<string>(CLAUDE_FAST_MODE_MODEL_IDS);

export function isClaudeFastModeModel(modelId: string): modelId is ClaudeFastModeModelId {
  return CLAUDE_FAST_MODE_MODELS.has(modelId);
}

export function getClaudeModelEffortCapability(
  modelId: string,
): ClaudeModelEffortCapability | null {
  // `hasOwn`, not a bare index: a modelId of `__proto__`/`constructor`/`toString`
  // would otherwise resolve to an `Object.prototype` member — a truthy value that
  // then throws when `.supportedEfforts` is read.
  const key = stripDatedSnapshotSuffix(modelId);
  return Object.hasOwn(CLAUDE_MODEL_EFFORT_CAPABILITIES, key)
    ? CLAUDE_MODEL_EFFORT_CAPABILITIES[key]!
    : null;
}

export function isClaudeModelEffortSupported(
  modelId: string,
  effort: string,
): effort is ClaudeModelEffort {
  const capability = getClaudeModelEffortCapability(modelId);
  return capability?.supportedEfforts.includes(effort as ClaudeModelEffort) ?? false;
}
