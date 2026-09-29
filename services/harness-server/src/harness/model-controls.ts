// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  CLAUDE_FAST_MODE_MODEL_IDS,
  getClaudeModelEffortCapability,
  isClaudeFastModeModel,
  isClaudeModelEffortSupported,
} from '@orca/harness-catalog';
import {
  MODEL_EFFORTS,
  MODEL_SPEEDS,
  type ModelEffort,
  type ModelSpeed,
  type SessionStartInput,
} from './agent-harness.js';

type AgentSnapshot = SessionStartInput['agentSnapshot'];
type SubagentSnapshot = NonNullable<AgentSnapshot['multiagent']>['agents'][number];

interface ModelControlSource {
  label: string;
  provider: string | undefined;
  modelId: string | undefined;
  speed: unknown;
  effort: unknown;
}

/**
 * Claude Agent SDK fast mode is session-wide, while Managed Agents stores speed
 * per agent. Uniform coordinator rosters map exactly; mixed speeds cannot be
 * represented without silently changing one agent's requested behavior.
 */
export function resolveClaudeSessionSpeed(snapshot: AgentSnapshot): ModelSpeed {
  const sources: ModelControlSource[] = [
    modelControlSource('primary agent', snapshot),
    ...(snapshot.multiagent?.agents.map((agent) =>
      modelControlSource(`subagent ${agent.name} (${agent.id})`, agent, snapshot.model_id),
    ) ?? []),
  ];

  for (const source of sources) validateModelControlSource(source);

  const speeds = sources.map(({ label, speed }) => ({
    label,
    speed: (speed ?? 'standard') as ModelSpeed,
  }));
  const requested = speeds[0]?.speed ?? 'standard';
  const conflicting = speeds.find((entry) => entry.speed !== requested);
  if (conflicting) {
    throw new Error(
      `Claude Agent SDK fast mode is session-wide; mixed model.speed values are unsupported ` +
        `(${speeds.map((entry) => `${entry.label}=${entry.speed}`).join(', ')})`,
    );
  }
  return requested;
}

function modelControlSource(
  label: string,
  snapshot: AgentSnapshot | SubagentSnapshot,
  inheritedModelId?: string,
): ModelControlSource {
  const modelId =
    snapshot.model_id === undefined || snapshot.model_id === 'inherit'
      ? inheritedModelId
      : snapshot.model_id;
  return {
    label,
    provider: snapshot.model_provider,
    modelId,
    speed: snapshot.model_speed,
    effort: snapshot.model_effort,
  };
}

function validateModelControlSource(source: ModelControlSource): void {
  if (source.speed !== undefined && !MODEL_SPEEDS.includes(source.speed as ModelSpeed)) {
    throw new Error(`${source.label} has unsupported model.speed ${JSON.stringify(source.speed)}`);
  }
  if (source.effort !== undefined && !MODEL_EFFORTS.includes(source.effort as ModelEffort)) {
    throw new Error(
      `${source.label} has unsupported model.effort ${JSON.stringify(source.effort)}`,
    );
  }
  if (
    source.provider !== undefined &&
    source.provider !== 'anthropic' &&
    (source.speed !== undefined || source.effort !== undefined)
  ) {
    throw new Error(
      `${source.label} uses provider ${source.provider}; model.speed/model.effort runtime controls ` +
        `are supported only by the Anthropic Claude harness`,
    );
  }
  if (
    source.speed === 'fast' &&
    (source.provider === undefined || source.provider === 'anthropic') &&
    !isClaudeFastModeModel(source.modelId ?? '')
  ) {
    throw new Error(
      `${source.label} requests fast mode for unsupported model ${source.modelId ?? '<missing>'}; ` +
        `supported models are ${CLAUDE_FAST_MODE_MODEL_IDS.join(' and ')}`,
    );
  }
  if (
    source.effort !== undefined &&
    (source.provider === undefined || source.provider === 'anthropic')
  ) {
    const modelId = source.modelId ?? '';
    const capability = getClaudeModelEffortCapability(modelId);
    if (!capability) {
      throw new Error(
        `${source.label} requests model.effort for unsupported model ${modelId || '<missing>'}`,
      );
    }
    if (!isClaudeModelEffortSupported(modelId, source.effort as string)) {
      throw new Error(
        `${source.label} requests model.effort ${JSON.stringify(source.effort)} for model ` +
          `${modelId}; supported levels are ${capability.supportedEfforts.join(', ')}`,
      );
    }
  }
}
