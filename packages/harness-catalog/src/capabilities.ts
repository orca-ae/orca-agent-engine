// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  resolveHarnessAnnotation,
  type HarnessMode,
  type HarnessType,
  type ResolvedHarness,
} from './catalog.js';
import { HARNESS_MODEL_POLICIES, type HarnessModelPolicy } from './harness-models.js';

export interface HarnessCapabilities {
  supportedTargets: readonly ('cloud' | 'self_hosted')[];
  /** Cloud execution path; self-hosted sessions always use the Registry runner. */
  cloudExecutionOwner: 'registry' | 'harness-server';
  /** Null preserves an operator-defined provider's existing model contract. */
  models: HarnessModelPolicy | null;
  modelControls: 'claude' | 'codex' | 'provider';
  /** Null leaves feature admission to the existing provider. */
  managedFeatures: { skills: boolean; multiagent: boolean } | null;
  nativeResume: boolean;
  /**
   * Whether harness-server's dispatcher builds its host-side agent toolset over the
   * write-policy-enforced sandbox for this harness's adapter.
   */
  needsSandboxToolset: boolean;
  needsMcpRewrite: boolean;
  supportsClientToolExecution: boolean;
  /** Null preserves the provider's existing admission and enforcement contract. */
  guardrails: {
    phases: readonly string[];
    stateful: boolean;
    /** When present, stateful rules may use only these phases. */
    statefulPhases?: readonly string[];
    subagents: boolean;
  } | null;
}

const runnerCapabilities: HarnessCapabilities = {
  supportedTargets: ['cloud', 'self_hosted'],
  cloudExecutionOwner: 'registry',
  models: null,
  modelControls: 'provider',
  managedFeatures: null,
  nativeResume: false,
  needsSandboxToolset: false,
  needsMcpRewrite: false,
  supportsClientToolExecution: false,
  guardrails: null,
};

/** Default-mode policies. Use resolveHarnessCapabilities for a pinned selection. */
export const HARNESS_CAPABILITIES: Readonly<Record<HarnessType, HarnessCapabilities>> = {
  claude_agent_sdk: {
    ...runnerCapabilities,
    cloudExecutionOwner: 'harness-server',
    models: HARNESS_MODEL_POLICIES.claude_agent_sdk!,
    modelControls: 'claude',
    managedFeatures: { skills: true, multiagent: true },
    needsSandboxToolset: true,
    needsMcpRewrite: true,
    supportsClientToolExecution: true,
  },
  claude_agent_sdk_persistent: {
    ...runnerCapabilities,
    cloudExecutionOwner: 'harness-server',
    models: HARNESS_MODEL_POLICIES.claude_agent_sdk_persistent!,
    modelControls: 'claude',
    managedFeatures: { skills: true, multiagent: true },
  },
  claude_code: {
    ...runnerCapabilities,
    // Cloud sessions run on the sandbox-harness HTTP bridge, which provides FUSE
    // resources, native SDK subagents and stateful budget enforcement. The runner
    // does not match it on these, so cloud execution stays with harness-server.
    cloudExecutionOwner: 'harness-server',
    modelControls: 'claude',
  },
  codex_sdk: {
    ...runnerCapabilities,
    supportedTargets: ['cloud'],
    cloudExecutionOwner: 'harness-server',
    models: HARNESS_MODEL_POLICIES.codex_sdk!,
    modelControls: 'codex',
    managedFeatures: { skills: true, multiagent: false },
    nativeResume: true,
    needsSandboxToolset: true,
    needsMcpRewrite: true,
    guardrails: {
      phases: ['request', 'tool_call', 'tool_result'],
      stateful: true,
      statefulPhases: ['request'],
      subagents: false,
    },
  },
  pi_sdk: {
    ...runnerCapabilities,
    supportedTargets: ['cloud'],
    cloudExecutionOwner: 'harness-server',
    models: HARNESS_MODEL_POLICIES.pi_sdk!,
    modelControls: 'provider',
    managedFeatures: { skills: true, multiagent: false },
    nativeResume: true,
    needsSandboxToolset: true,
    needsMcpRewrite: true,
    guardrails: {
      phases: ['request', 'tool_call', 'tool_result'],
      stateful: true,
      statefulPhases: ['request'],
      subagents: false,
    },
  },
  codex: runnerCapabilities,
  cursor: runnerCapabilities,
  pi: runnerCapabilities,
  custom: runnerCapabilities,
  mock: runnerCapabilities,
};

/** Deployment-specific differences without provider switches in consumers. */
const MODE_CAPABILITY_OVERRIDES: Partial<
  Record<HarnessType, Partial<Record<HarnessMode, Partial<HarnessCapabilities>>>>
> = {
  codex_sdk: {
    colocated: {
      supportedTargets: ['cloud', 'self_hosted'],
      cloudExecutionOwner: 'harness-server',
      // Cloud SDKs share sandbox-harness resources; self-hosted targets retain the runner.
      managedFeatures: { skills: true, multiagent: false },
      needsSandboxToolset: true,
      needsMcpRewrite: true,
      guardrails: {
        phases: ['request'],
        stateful: true,
        statefulPhases: ['request'],
        subagents: false,
      },
    },
  },
  pi_sdk: {
    colocated: {
      supportedTargets: ['cloud', 'self_hosted'],
      cloudExecutionOwner: 'harness-server',
      // Cloud SDKs share sandbox-harness resources; self-hosted targets retain the runner.
      managedFeatures: { skills: true, multiagent: false },
      needsSandboxToolset: true,
      needsMcpRewrite: true,
      guardrails: {
        phases: ['request'],
        stateful: true,
        statefulPhases: ['request'],
        subagents: false,
      },
    },
  },
};

export function resolveHarnessCapabilities(selection: ResolvedHarness): HarnessCapabilities {
  const validated = resolveHarnessAnnotation({ ...selection });
  if ('error' in validated) throw new Error(validated.error);
  return {
    ...HARNESS_CAPABILITIES[validated.harness],
    ...MODE_CAPABILITY_OVERRIDES[validated.harness]?.[validated.mode],
  };
}

/** Validate a deployment without changing which service owns its events. */
export function validateHarnessDeployment(
  target: string,
  selection: ResolvedHarness,
): string | null {
  const supported = resolveHarnessCapabilities(selection).supportedTargets;
  return supported.some((entry) => entry === target)
    ? null
    : `${selection.harness}/${selection.mode} does not support environment target '${target}' (supported: ${supported.join(', ')})`;
}

/** Fail closed when any part of an attached policy cannot be enforced. */
export function validateHarnessGuardrails(
  selection: ResolvedHarness,
  guardrails: readonly {
    id: string;
    name?: string;
    phases: readonly string[];
    stateful: boolean;
    rule?: unknown;
    subagent_id?: unknown;
    subagentId?: unknown;
  }[],
): string | null {
  const capabilities = resolveHarnessCapabilities(selection);
  const policy = capabilities.guardrails;
  if (policy === null) return null;
  const unsupported = guardrails.flatMap((guardrail) => {
    const rule = guardrail.rule as { kind?: unknown; builtin?: unknown } | null | undefined;
    // Managed Skill denial is enforced before catalog/file materialization,
    // independently of the provider's general tool-call interception support.
    const skillMaterialization =
      capabilities.managedFeatures?.skills === true &&
      !guardrail.stateful &&
      rule?.kind === 'builtin' &&
      rule.builtin === 'block_skills' &&
      guardrail.phases.length > 0 &&
      guardrail.phases.every((phase) => phase === 'tool_call');
    const reasons = [
      ...(guardrail.stateful && !policy.stateful ? ['stateful rules are not supported'] : []),
      ...(guardrail.stateful && policy.statefulPhases
        ? guardrail.phases
            .filter((phase) => !policy.statefulPhases!.includes(phase))
            .map((phase) => `stateful phase ${phase} is not supported`)
        : []),
      ...((guardrail.subagent_id !== undefined || guardrail.subagentId !== undefined) &&
      !policy.subagents
        ? ['subagent-scoped rules are not supported']
        : []),
      ...guardrail.phases
        .filter((phase) => !policy.phases.includes(phase) && !skillMaterialization)
        .map((phase) => `phase ${phase} is not supported`),
    ];
    return reasons.length
      ? [`${guardrail.name ?? guardrail.id} (${guardrail.id}): ${reasons.join(', ')}`]
      : [];
  });
  return unsupported.length
    ? `${selection.harness}/${selection.mode} cannot enforce guardrail: ${unsupported.join('; ')}`
    : null;
}

export function validateHarnessFeatures(
  metadata: Record<string, unknown> | null | undefined,
  config: { skills?: readonly unknown[] | null | undefined; multiagent?: unknown },
): string | null {
  const selection = resolveHarnessAnnotation(metadata);
  if ('error' in selection) return selection.error;
  const features = resolveHarnessCapabilities(selection).managedFeatures;
  if (!features) return null;
  if (!features.skills && config.skills?.length)
    return `${selection.harness} does not support managed Skills`;
  if (!features.multiagent && config.multiagent != null)
    return `${selection.harness} does not support multiagent rosters`;
  return null;
}
