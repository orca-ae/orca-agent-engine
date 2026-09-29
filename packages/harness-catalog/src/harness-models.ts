// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { resolveHarnessAnnotation, type HarnessType } from './catalog.js';
import claudePrices from './model-prices.seed.json' with { type: 'json' };
import { stripDatedSnapshotSuffix } from './pricing.js';
import { getClaudeModelEffortCapability } from './model-controls.js';
import piModels from './pi-models.json' with { type: 'json' };
import piModelApis from './pi-model-apis.json' with { type: 'json' };
import codexModels from './codex-models.json' with { type: 'json' };

// Pinned with @openai/codex-sdk 0.154.0, from OpenAI's
// codex-rs/models-manager/models.json at rust-v0.154.0 (API-supported entries).
export const CODEX_SDK_MODELS: Readonly<Record<string, readonly string[]>> = codexModels;

/** Pi 0.87.0's bundled models, with existing Orca OpenAI aliases retained. */
export const PI_SDK_MODELS: Readonly<Record<string, Readonly<Record<string, readonly string[]>>>> =
  {
    ...piModels,
    openai: { ...piModels.openai, ...CODEX_SDK_MODELS },
  };
export function piModelEfforts(provider: string, id: string): readonly string[] | undefined {
  const models = Object.hasOwn(PI_SDK_MODELS, provider) ? PI_SDK_MODELS[provider] : undefined;
  return models && Object.hasOwn(models, id) ? models[id] : undefined;
}
export const PI_MODEL_APIS: Readonly<Record<string, Readonly<Record<string, string>>>> =
  piModelApis;
export function piModelApi(provider: string, id: string): string | undefined {
  if (provider === 'openai' && Object.hasOwn(CODEX_SDK_MODELS, id)) return 'openai-responses';
  const models = Object.hasOwn(PI_MODEL_APIS, provider) ? PI_MODEL_APIS[provider] : undefined;
  return models && Object.hasOwn(models, id) ? models[id] : undefined;
}
export function piLlmRoute(provider: string, id: string): string {
  const api = piModelApi(provider, id);
  if (!api) throw new Error(`Unsupported Pi model: ${provider}/${id}`);
  return `llm-pi-${provider}-${api}`;
}

export interface HarnessModelPolicy {
  modelsByProvider?: typeof PI_SDK_MODELS;
  defaultProvider: string;
  providers: readonly string[];
  models?: Readonly<Record<string, readonly string[]>>;
}

// Missing entries preserve the operator-defined model contracts of the older CLI
// providers. New SDK integrations add their policy here, independently of routing.
export const CLAUDE_SDK_MODELS: Readonly<Record<string, readonly string[]>> = Object.fromEntries(
  [
    ...Object.keys(claudePrices.models),
    'claude-opus-4',
    'claude-opus-4-1',
    'claude-sonnet-4',
    'claude-sonnet-4-5',
    'claude-3-5-sonnet',
    'claude-3-7-sonnet',
    'claude-3-5-haiku',
    'claude-3-haiku',
    'claude-3-opus',
  ].map((id) => [id, getClaudeModelEffortCapability(id)?.supportedEfforts ?? []]),
);

export const HARNESS_MODEL_POLICIES: Partial<Record<HarnessType, HarnessModelPolicy>> = {
  claude_agent_sdk: {
    defaultProvider: 'anthropic',
    providers: ['anthropic'],
    models: CLAUDE_SDK_MODELS,
  },
  claude_agent_sdk_persistent: {
    defaultProvider: 'anthropic',
    providers: ['anthropic'],
    models: CLAUDE_SDK_MODELS,
  },
  pi_sdk: {
    defaultProvider: 'openai',
    providers: Object.keys(PI_SDK_MODELS),
    modelsByProvider: PI_SDK_MODELS,
  },
  codex_sdk: { defaultProvider: 'openai', providers: ['openai'], models: CODEX_SDK_MODELS },
};

export function defaultHarnessModelProvider(harness: HarnessType): string {
  return HARNESS_MODEL_POLICIES[harness]?.defaultProvider ?? 'anthropic';
}

export function validateHarnessModel(
  metadata: Record<string, unknown> | null | undefined,
  model: { provider: string; id: string; speed?: string; effort?: string },
): string | null {
  const selected = resolveHarnessAnnotation(metadata);
  if ('error' in selected) return selected.error;
  const policy = HARNESS_MODEL_POLICIES[selected.harness];
  if (!policy) return null;
  if (!policy.providers.includes(model.provider)) {
    return `harness '${selected.harness}' supports model providers: ${policy.providers.join(', ')}`;
  }
  if (selected.harness === 'pi_sdk') {
    const efforts = piModelEfforts(model.provider, model.id);
    if (!efforts)
      return `model '${model.provider}/${model.id}' is not supported by harness 'pi_sdk'`;
    if (model.speed && model.speed !== 'standard')
      return 'pi_sdk does not support model.speed fast';
    if (model.effort && !efforts.includes(model.effort))
      return `model.effort '${model.effort}' is not supported for ${model.provider}/${model.id}`;
    return null;
  }
  if (policy.models) {
    const id = model.provider === 'anthropic' ? stripDatedSnapshotSuffix(model.id) : model.id;
    if (!Object.hasOwn(policy.models, id)) {
      return `model '${model.id}' is not supported by harness '${selected.harness}'`;
    }
    if (model.effort && !policy.models[id]!.includes(model.effort)) {
      return `model.effort '${model.effort}' is not supported for ${model.id}; supported levels are ${policy.models[id]!.join(', ')}`;
    }
    if (model.provider === 'openai' && model.speed && model.speed !== 'standard') {
      return `model.speed '${model.speed}' is not supported by harness '${selected.harness}'`;
    }
  }
  return null;
}

/** Compare effective types, including legacy agents whose absent annotation means Claude. */
export function validateHarnessUpdate(
  before: Record<string, unknown>,
  after: Record<string, unknown>,
): string | null {
  const previous = resolveHarnessAnnotation(before);
  const next = resolveHarnessAnnotation(after);
  if ('error' in previous) return previous.error;
  if ('error' in next) return next.error;
  if (
    previous.harness !== next.harness ||
    (Object.hasOwn(before, 'harness') && !Object.hasOwn(after, 'harness'))
  ) {
    return 'metadata.harness is immutable; create another agent to select a different harness';
  }
  return null;
}

// Keep these decoded limits aligned with packages/codex-harness/src/worker.ts.
// Base64 expands the payload by ~4/3; the private HTTP route allows 25 MiB
// for the encoded files and JSON envelope, not 25 MiB of native history.
const MAX_CODEX_CHECKPOINT_BYTES = 16 * 1024 * 1024;
const MAX_CODEX_CHECKPOINT_FILES = 32;

/** Private state received from a runner, bounded before persistence or restoration. */
export function validateCodexCheckpoint(state: unknown): void {
  if (!state || typeof state !== 'object' || Array.isArray(state))
    throw new Error('invalid Codex checkpoint');
  const value = state as Record<string, unknown>;
  if (
    value.format !== undefined ||
    value.version !== 1 ||
    typeof value.threadId !== 'string' ||
    !/^[a-zA-Z0-9-]{1,128}$/.test(value.threadId) ||
    !value.files ||
    typeof value.files !== 'object' ||
    Array.isArray(value.files) ||
    (value.instructionsSha256 !== undefined &&
      (typeof value.instructionsSha256 !== 'string' ||
        !/^[a-f0-9]{64}$/.test(value.instructionsSha256)))
  )
    throw new Error('invalid Codex checkpoint');
  const files = Object.entries(value.files as Record<string, unknown>);
  if (!files.length || files.length > MAX_CODEX_CHECKPOINT_FILES)
    throw new Error('invalid Codex checkpoint files');
  let size = 0;
  for (const [path, data] of files) {
    if (
      !/^sessions\/[0-9]{4}\/[0-9]{2}\/[0-9]{2}\/rollout-[A-Za-z0-9_.-]+\.jsonl$/.test(path) ||
      !path.includes(value.threadId) ||
      typeof data !== 'string' ||
      data.length % 4 !== 0 ||
      !/^[A-Za-z0-9+/]*={0,2}$/.test(data)
    )
      throw new Error('invalid Codex checkpoint file');
    size += (data.length / 4) * 3 - (data.endsWith('==') ? 2 : data.endsWith('=') ? 1 : 0);
  }
  if (size > MAX_CODEX_CHECKPOINT_BYTES) throw new Error('Codex checkpoint exceeds size limit');
}

/** Preserve pre-immutability version pins; enforce identity on newly stamped snapshots. */
export function bindStoredHarness(
  metadata: Record<string, unknown> | null | undefined,
  harnessType: string,
  versionHarnessType?: unknown,
): Record<string, unknown> {
  // Legacy versions predate the immutable Agent column: an older version may
  // legitimately name a different harness from the Agent's current metadata.
  // New snapshots always stamp harness_type and must match the immutable row.
  const legacy = versionHarnessType === undefined;
  const versionType = legacy ? (metadata?.harness ?? 'claude_agent_sdk') : versionHarnessType;
  if (
    !legacy &&
    (versionType !== harnessType ||
      (metadata?.harness !== undefined && metadata.harness !== harnessType))
  ) {
    throw new Error('agent version harness conflicts with immutable Agent harness type');
  }
  const bound = { ...metadata, harness: versionType };
  const selected = resolveHarnessAnnotation(bound);
  if ('error' in selected) throw new Error(selected.error);
  return bound;
}
