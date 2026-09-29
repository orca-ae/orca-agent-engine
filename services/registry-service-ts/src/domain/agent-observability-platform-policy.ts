// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { eq } from 'drizzle-orm';
import {
  PlatformAgentObservabilityPolicySchema,
  PlatformAgentObservabilityPutRequestSchema,
  type PlatformAgentObservabilityPolicy,
  type PlatformAgentObservabilityPutRequest,
} from '../contracts/platform-agent-observability.contract.js';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import { agentObservabilityPlatformPolicy } from '../persistence/postgres/schema.js';
import { captureRestrictionEpochAfterReplacement } from './agent-observability-policy.js';

const UNAVAILABLE = 'agent observability platform policy unavailable';

export function platformAgentObservabilityPolicyEtag(
  policy: PlatformAgentObservabilityPolicy,
): string {
  const canonical = PlatformAgentObservabilityPolicySchema.parse(policy);
  const hash = createHash('sha256')
    .update('orca:platform-observability-policy:v1\0')
    .update(JSON.stringify(canonical))
    .digest('base64url');
  return '"orca-aop-v1-' + hash + '"';
}

function policyWire(
  row: typeof agentObservabilityPlatformPolicy.$inferSelect | undefined,
): PlatformAgentObservabilityPolicy {
  if (!row || row.id !== 'default') throw new Error(UNAVAILABLE);
  return PlatformAgentObservabilityPolicySchema.parse({
    type: 'agent_observability_platform_policy',
    allowed_adapters: row.allowedAdapters,
    allowed_endpoint_classes: row.allowedEndpointClasses,
    max_capture_mode: row.maxCaptureMode,
    capture_restriction_epoch: row.captureRestrictionEpoch,
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  });
}

export async function loadPlatformAgentObservabilityPolicy(
  db: Pick<DbClient, 'select'>,
): Promise<PlatformAgentObservabilityPolicy> {
  const [row] = await db
    .select()
    .from(agentObservabilityPlatformPolicy)
    .where(eq(agentObservabilityPlatformPolicy.id, 'default'))
    .limit(1);
  return policyWire(row);
}

export async function replacePlatformAgentObservabilityPolicy(
  tx: DbTransaction,
  request: PlatformAgentObservabilityPutRequest,
  ifMatch: string,
): Promise<
  | { kind: 'stale' }
  | {
      kind: 'success';
      before: PlatformAgentObservabilityPolicy;
      after: PlatformAgentObservabilityPolicy;
      changed: boolean;
    }
> {
  // This is the same singleton authority that context resolution, pinning and
  // tenant mutations lock FOR SHARE. Never upgrade a previously acquired share lock.
  const [row] = await tx
    .select()
    .from(agentObservabilityPlatformPolicy)
    .where(eq(agentObservabilityPlatformPolicy.id, 'default'))
    .for('update')
    .limit(1);
  const before = policyWire(row);
  if (platformAgentObservabilityPolicyEtag(before) !== ifMatch) return { kind: 'stale' };
  const current = PlatformAgentObservabilityPutRequestSchema.parse({
    allowed_adapters: before.allowed_adapters,
    allowed_endpoint_classes: before.allowed_endpoint_classes,
    max_capture_mode: before.max_capture_mode,
  });
  if (JSON.stringify(current) === JSON.stringify(request)) {
    return { kind: 'success', before, after: before, changed: false };
  }
  const epoch = captureRestrictionEpochAfterReplacement(
    before.max_capture_mode,
    request.max_capture_mode,
    before.capture_restriction_epoch,
  );
  if (!Number.isSafeInteger(epoch)) throw new Error(UNAVAILABLE);
  // Preserve distinct ETags across A -> B -> A even inside one clock tick or
  // after wall-clock rollback; no schema revision column is needed.
  const updatedAt = new Date(Math.max(Date.now(), Date.parse(before.updated_at) + 1));
  const [updated] = await tx
    .update(agentObservabilityPlatformPolicy)
    .set({
      allowedAdapters: request.allowed_adapters,
      allowedEndpointClasses: request.allowed_endpoint_classes,
      maxCaptureMode: request.max_capture_mode,
      captureRestrictionEpoch: epoch,
      updatedAt,
    })
    .where(eq(agentObservabilityPlatformPolicy.id, 'default'))
    .returning();
  return { kind: 'success', before, after: policyWire(updated), changed: true };
}
