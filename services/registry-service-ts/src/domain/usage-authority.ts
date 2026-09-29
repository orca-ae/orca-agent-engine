// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { resolveHarnessAnnotation } from '@orca/harness-catalog';
import { PreparedAgentSnapshotSchema } from '../contracts/internal.contract.js';

export type AuthoritativeUsageCaller = 'harness' | 'ai-gateway';

/**
 * Select the single usage writer for a pinned AgentVersion.
 *
 * Colocation only proves that LLM traffic crosses AI Gateway. Registry usage
 * writeback is a separately deployed sink, so Harness remains authoritative
 * until operators explicitly enable that sink.
 */
export function authoritativeUsageCallerForSnapshot(
  snapshot: unknown,
  gatewayRegistryUsageEnabled = false,
): AuthoritativeUsageCaller | null {
  const parsed = PreparedAgentSnapshotSchema.safeParse(snapshot);
  if (!parsed.success) return null;
  const harness = resolveHarnessAnnotation(parsed.data.metadata);
  if ('error' in harness) return null;
  return gatewayRegistryUsageEnabled &&
    harness.mode === 'colocated' &&
    harness.harness !== 'codex_sdk' &&
    harness.harness !== 'pi_sdk'
    ? 'ai-gateway'
    : 'harness';
}

/** Self-hosted Codex SDK usage is committed by the authenticated Registry bridge itself. */
export function registryOwnsCodexUsage(snapshot: unknown, target: string = 'cloud'): boolean {
  const parsed = PreparedAgentSnapshotSchema.safeParse(snapshot);
  if (!parsed.success) return false;
  const harness = resolveHarnessAnnotation(parsed.data.metadata);
  return (
    target === 'self_hosted' &&
    !('error' in harness) &&
    (harness.harness === 'codex_sdk' || harness.harness === 'pi_sdk') &&
    harness.mode === 'colocated'
  );
}
