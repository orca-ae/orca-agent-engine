// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Routing seam: pick the AgentHarness implementation for a session from its
 * resolved harness annotation. The selection logic is a pure function so it can
 * be unit-tested without standing up the dispatcher; the dispatcher supplies the
 * concrete builders.
 */
import {
  resolveHarnessAnnotation,
  type HarnessType,
  type HarnessMode,
  type ResolvedHarness,
} from '@orca/harness-catalog';
import type { AgentHarness } from './agent-harness.js';
import type { SessionRecord } from '../clients/registry.js';

export interface HarnessProviderContext {
  workspaceId: string;
  sessionId: string;
  session?: SessionRecord | null | undefined;
  selection?: ResolvedHarness;
  modelProvider?: string | undefined;
}

export interface HarnessProvider {
  id: HarnessType;
  modes?: readonly HarnessMode[];
  build(context: HarnessProviderContext): AgentHarness;
}

export type HarnessProviderRegistry = ReadonlyMap<HarnessType, HarnessProvider>;

export function createHarnessProviderRegistry(
  providers: readonly HarnessProvider[],
): HarnessProviderRegistry {
  const registry = new Map<HarnessType, HarnessProvider>();
  for (const provider of providers) {
    if (registry.has(provider.id)) throw new Error(`duplicate harness provider '${provider.id}'`);
    registry.set(provider.id, provider);
  }
  return registry;
}

export interface SelectHarnessInput {
  /** Test override (the dispatcher's `harnessFactory`). When set, it wins. */
  override?: (() => AgentHarness) | undefined;
  /** Resolved `(harness, mode)` from the agent's annotation. */
  selection: ResolvedHarness;
  providers: HarnessProviderRegistry;
  context: HarnessProviderContext;
}

export function selectHarness(input: SelectHarnessInput): AgentHarness {
  if (input.override) return input.override();
  const validated = resolveHarnessAnnotation({
    harness: input.selection.harness,
    mode: input.selection.mode,
  });
  if ('error' in validated) throw new Error(validated.error);
  const provider = input.providers.get(input.selection.harness);
  if (!provider) {
    throw new Error(`no harness-server provider registered for '${input.selection.harness}'`);
  }
  if (provider.modes && !provider.modes.includes(validated.mode)) {
    throw new Error(
      `harness-server provider '${validated.harness}' does not support '${validated.mode}'`,
    );
  }
  return provider.build({ ...input.context, selection: validated });
}
