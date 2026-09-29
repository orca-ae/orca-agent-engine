// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  validateHarnessModel,
  CODEX_SDK_MODELS,
  piModelEfforts,
  CLAUDE_FAST_MODE_MODEL_IDS,
  CLAUDE_MODEL_EFFORT_LEVELS,
  getClaudeModelEffortCapability,
  isClaudeFastModeModel,
  isClaudeModelEffortSupported,
} from '@orca/harness-catalog';
import { z } from 'zod';

/**
 * Edge-translation for the `model` field per managed-agents-2026-04-01.
 *
 * Claude accepts the model as either a bare string id or a structured
 * `{ id, speed?, effort? }` object. The orca internal representation keeps
 * `{ provider, id }` columns plus nullable `speed` and `effort` controls.
 *
 * - Inbound: accept string or model objects and normalize effort's string /
 *   `{type}` forms to the internal scalar representation.
 * - Outbound: default (Claude) clients receive `{ id, speed, effort? }`; first-party
 *   `orca-beta` clients keep the legacy `{ provider, id }` shape.
 */

export const MODEL_SPEEDS = ['standard', 'fast'] as const;
export type ModelSpeed = (typeof MODEL_SPEEDS)[number];
export const MODEL_EFFORTS = [...CLAUDE_MODEL_EFFORT_LEVELS, 'ultra'] as const;
export type ModelEffort = (typeof MODEL_EFFORTS)[number];

export interface StoredModel {
  provider: string;
  id: string;
  speed?: ModelSpeed;
  effort?: ModelEffort;
}

export interface ModelRosterEntry {
  harness?: string;
  label: string;
  model: StoredModel;
}

export const DEFAULT_MODEL_PROVIDER = 'anthropic';
export const FAST_MODE_MODEL_IDS = CLAUDE_FAST_MODE_MODEL_IDS;
export const MAX_MODEL_ID_LENGTH = 256;

const ModelSpeedSchema = z.enum(MODEL_SPEEDS);
const ModelEffortSchema = z.enum(MODEL_EFFORTS);
/**
 * `.nullable()` on the union rather than a `z.null()` branch inside it.
 *
 * The two parse identically, but they render differently: the third branch came
 * out of the OpenAPI generator as a plain nullable string, which overlaps the
 * enum branch. Every valid effort value then matched two branches of a `oneOf`,
 * so a client validating against the published document rejected values this
 * service accepts. As a nullable union it renders as one nullable enum.
 */
const ModelEffortInputSchema = z
  .union([ModelEffortSchema, z.object({ type: ModelEffortSchema }).strict()])
  .nullable();

/**
 * Request-side union accepted on agent create/update.
 *
 * There was a third branch requiring `provider` alongside `id`. It could never
 * decide an outcome: the object branch below is `.passthrough()` and requires
 * only `id`, so everything the provider branch accepted this one already
 * accepted, with the same parsed output — including `provider: 42` and
 * `provider: ""`, which it was written to reject and never saw. Dead at runtime,
 * and harmful on the wire: `{ provider, id }` matched both branches, so a `oneOf`
 * that demands exactly one match made the published contract **reject a value
 * this service accepts**. Deleted rather than made disjoint, because there is no
 * behaviour to preserve.
 *
 * `AgentCreate` is `safeParse`d in `src/api/agents.routes.ts`, so this schema is
 * a live validator and not documentation — the equivalence above is the reason
 * the deletion is safe, not an aside.
 */
export const ModelInput = z.union([
  z.string().min(1).max(MAX_MODEL_ID_LENGTH),
  z
    .object({
      id: z.string().min(1).max(MAX_MODEL_ID_LENGTH),
      speed: ModelSpeedSchema.nullable().optional(),
      effort: ModelEffortInputSchema.optional(),
    })
    .passthrough(),
]);

/** Response-side union emitted on agent serialization (default vs orca-beta). */
export const ModelOutput = z.union([
  z
    .object({
      id: z.string(),
      speed: ModelSpeedSchema.optional(),
      effort: z.object({ type: ModelEffortSchema }).optional(),
    })
    .strict(),
  z.object({ provider: z.string(), id: z.string() }).strict(),
]);

export function normalizeModelForStorage(
  input: unknown,
  defaultProvider: string = DEFAULT_MODEL_PROVIDER,
): StoredModel | { error: string } {
  if (typeof input === 'string') {
    if (input.trim().length === 0) return { error: 'model must be a non-empty string' };
    if (input.length > MAX_MODEL_ID_LENGTH) {
      return { error: `model id must be at most ${MAX_MODEL_ID_LENGTH} characters` };
    }
    if (input.includes('*')) return { error: 'model id must not contain wildcard characters' };
    return { provider: defaultProvider, id: input };
  }
  if (input && typeof input === 'object' && !Array.isArray(input)) {
    const obj = input as Record<string, unknown>;
    if (typeof obj.id !== 'string' || obj.id.length === 0) {
      return { error: 'model.id is required' };
    }
    if (obj.id.length > MAX_MODEL_ID_LENGTH) {
      return { error: `model id must be at most ${MAX_MODEL_ID_LENGTH} characters` };
    }
    if (obj.id.includes('*')) return { error: 'model id must not contain wildcard characters' };
    const provider =
      typeof obj.provider === 'string' && obj.provider.length > 0 ? obj.provider : defaultProvider;
    if (obj.speed !== undefined && obj.speed !== 'standard' && obj.speed !== 'fast') {
      if (obj.speed !== null) return { error: "model.speed must be 'standard' or 'fast'" };
    }
    const effort = normalizeEffort(obj.effort);
    if ('error' in effort) {
      return effort;
    }
    return {
      provider,
      id: obj.id,
      ...(obj.speed != null ? { speed: obj.speed as ModelSpeed } : {}),
      ...(effort.value !== undefined ? { effort: effort.value } : {}),
    };
  }
  return { error: 'model must be a string or an object with an id' };
}

/**
 * Session model overrides replace the pinned model. Omitted or null controls
 * therefore use the replacement model's provider defaults instead of inheriting
 * controls from the pinned agent.
 */
export function mergeModelOverrideForStorage(
  base: StoredModel,
  input: unknown,
  harness?: string,
): StoredModel | { error: string } {
  const normalized = normalizeModelForStorage(input, base.provider);
  if ('error' in normalized) return normalized;
  return validateModelControlsForStorage(normalized, harness);
}

export function validateModelControlsForStorage(
  model: StoredModel,
  harness?: string,
): StoredModel | { error: string } {
  // Pi owns its effort catalog and omitted-effort default, even for Claude models.
  if (harness === 'pi_sdk') {
    const error = validateHarnessModel({ harness }, model);
    return error ? { error } : model;
  }
  if (model.provider === 'openai' && Object.hasOwn(CODEX_SDK_MODELS, model.id)) {
    if (model.speed && model.speed !== 'standard')
      return { error: 'OpenAI models do not support model.speed fast' };
    if (model.effort && !CODEX_SDK_MODELS[model.id]!.includes(model.effort)) {
      return { error: `model.effort '${model.effort}' is not supported for ${model.id}` };
    }
    return model;
  }
  const piEfforts = piModelEfforts(model.provider, model.id);
  if (piEfforts && (model.provider !== 'anthropic' || !getClaudeModelEffortCapability(model.id))) {
    if (model.speed && model.speed !== 'standard')
      return { error: `${model.provider} model does not support fast mode` };
    if (model.effort && !piEfforts.includes(model.effort))
      return {
        error: `model.effort '${model.effort}' is not supported for ${model.provider}/${model.id}`,
      };
    return model;
  }
  if (
    model.provider !== DEFAULT_MODEL_PROVIDER &&
    (model.speed !== undefined || model.effort !== undefined)
  ) {
    return {
      error: 'model.speed/model.effort runtime controls are supported only for Anthropic models',
    };
  }
  if (model.speed === 'fast' && !isClaudeFastModeModel(model.id)) {
    return {
      error:
        `model.speed 'fast' is supported only for Anthropic models ` +
        FAST_MODE_MODEL_IDS.join(' and '),
    };
  }
  const effortCapability =
    model.provider === DEFAULT_MODEL_PROVIDER ? getClaudeModelEffortCapability(model.id) : null;
  if (model.effort !== undefined) {
    if (!effortCapability) {
      return { error: `model.effort is not supported for Anthropic model ${model.id}` };
    }
    if (!isClaudeModelEffortSupported(model.id, model.effort)) {
      return {
        error:
          `model.effort '${model.effort}' is not supported for Anthropic model ${model.id}; ` +
          `supported levels are ${effortCapability.supportedEfforts.join(', ')}`,
      };
    }
  }
  if (model.effort === undefined && effortCapability) {
    return { ...model, effort: effortCapability.defaultEffort };
  }
  return model;
}

/** Validate per-agent controls plus Claude SDK's session-wide speed invariant. */
export function validateModelRosterForStorage(entries: ModelRosterEntry[]): string | null {
  for (const entry of entries) {
    const validated = validateModelControlsForStorage(entry.model, entry.harness);
    if ('error' in validated) return `${entry.label}: ${validated.error}`;
  }
  if (entries.length < 2) return null;

  const speeds = entries.map((entry) => ({
    label: entry.label,
    speed: entry.model.speed ?? 'standard',
  }));
  const requested = speeds[0]!.speed;
  if (speeds.some((entry) => entry.speed !== requested)) {
    return (
      'Claude Agent SDK fast mode is session-wide; mixed model.speed values are unsupported ' +
      `(${speeds.map((entry) => `${entry.label}=${entry.speed}`).join(', ')})`
    );
  }
  return null;
}

export function modelToApi(
  model: StoredModel,
  orcaBeta: boolean,
  harness?: string,
): Record<string, unknown> {
  if (orcaBeta) return { provider: model.provider, id: model.id };
  const defaultEffort =
    harness !== 'pi_sdk' && model.provider === DEFAULT_MODEL_PROVIDER
      ? getClaudeModelEffortCapability(model.id)?.defaultEffort
      : undefined;
  const effort = model.effort ?? defaultEffort;
  return {
    id: model.id,
    speed: model.speed ?? 'standard',
    ...(effort ? { effort: { type: effort } } : {}),
  };
}

function normalizeEffort(value: unknown): { value: ModelEffort | undefined } | { error: string } {
  if (value === undefined || value === null) return { value: undefined };
  const effort =
    typeof value === 'string'
      ? value
      : value && typeof value === 'object' && !Array.isArray(value)
        ? (value as Record<string, unknown>).type
        : undefined;
  if (typeof effort !== 'string' || !MODEL_EFFORTS.includes(effort as ModelEffort)) {
    return { error: `model.effort must be one of ${MODEL_EFFORTS.join(', ')}` };
  }
  return { value: effort as ModelEffort };
}
