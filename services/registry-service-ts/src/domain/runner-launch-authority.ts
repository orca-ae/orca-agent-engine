// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull } from 'drizzle-orm';
import type { DbClient } from '../persistence/postgres/client.js';
import { environmentClaims, environments } from '../persistence/postgres/schema.js';
import {
  egressOwnerAuthority,
  type EgressOwnerAuthority,
} from '../contracts/sandbox-egress.contract.js';
import { hashEnvironmentToken } from './environment-token.js';
import type { EgressSandboxLocator } from './sandbox-egress-authority.js';

export type RunnerLaunchAuthority = Extract<EgressOwnerAuthority, { kind: 'registry' }>;
export type WorkerLaunchAuthority = Omit<RunnerLaunchAuthority, 'kind' | 'runner_id'>;

export function runnerLaunchAuthority(value: unknown): RunnerLaunchAuthority | null {
  const parsed = egressOwnerAuthority.safeParse(value);
  return parsed.success && parsed.data.kind === 'registry' ? parsed.data : null;
}

export function sameWorkerLaunch(a: WorkerLaunchAuthority, b: WorkerLaunchAuthority): boolean {
  return (
    a.environment_id === b.environment_id &&
    a.launch_digest === b.launch_digest &&
    a.sandbox_handle_id === b.sandbox_handle_id &&
    a.owner_pod === b.owner_pod &&
    a.worker_conn_id === b.worker_conn_id &&
    a.sandbox_pod.namespace === b.sandbox_pod.namespace &&
    a.sandbox_pod.name === b.sandbox_pod.name &&
    a.sandbox_pod.uid === b.sandbox_pod.uid
  );
}

/** The raw token comes only from the route-authenticated handshake, never worker hello. */
export async function authenticatedWorkerLaunch(
  db: Pick<DbClient, 'select'>,
  locate: EgressSandboxLocator,
  environmentId: string,
  token: string,
  ownerPod: string,
  workerConnId: string,
): Promise<WorkerLaunchAuthority> {
  const [environment] = await db
    .select()
    .from(environments)
    .where(
      and(
        eq(environments.id, environmentId),
        eq(environments.target, 'cloud'),
        isNull(environments.archivedAt),
        isNull(environments.deletedAt),
      ),
    )
    .limit(1);
  if (
    !environment?.cloudSandboxHandleId ||
    environment.environmentTokenDigest !== hashEnvironmentToken(token) ||
    !environment.environmentTokenExpiresAt ||
    environment.environmentTokenExpiresAt.getTime() <= Date.now()
  )
    throw new Error(
      'managed worker requires current launch-token provenance; reprovision Environment',
    );
  return {
    environment_id: environmentId,
    launch_digest: environment.environmentTokenDigest,
    sandbox_handle_id: environment.cloudSandboxHandleId,
    sandbox_pod: await locate(environment.cloudSandboxHandleId),
    owner_pod: ownerPod,
    worker_conn_id: workerConnId,
  };
}

/** Call with the transaction client while holding Environment/claim locks when binding. */
export async function currentWorkerLaunch(
  db: Pick<DbClient, 'select'>,
  locate: EgressSandboxLocator,
  ttlMs: number,
  source: WorkerLaunchAuthority,
): Promise<boolean> {
  const [environment] = await db
    .select()
    .from(environments)
    .where(
      and(
        eq(environments.id, source.environment_id),
        eq(environments.target, 'cloud'),
        isNull(environments.deletedAt),
        isNull(environments.archivedAt),
      ),
    )
    .limit(1);
  if (
    !environment ||
    environment.cloudSandboxHandleId !== source.sandbox_handle_id ||
    environment.environmentTokenDigest !== source.launch_digest ||
    !environment.environmentTokenExpiresAt ||
    environment.environmentTokenExpiresAt.getTime() <= Date.now()
  )
    return false;
  const [claim] = await db
    .select()
    .from(environmentClaims)
    .where(eq(environmentClaims.environmentId, source.environment_id))
    .limit(1);
  if (
    !claim ||
    claim.ownerPod !== source.owner_pod ||
    claim.workerConnId !== source.worker_conn_id ||
    Date.now() - claim.lastPing.getTime() > ttlMs
  )
    return false;
  const pod = await locate(source.sandbox_handle_id);
  return (
    pod.namespace === source.sandbox_pod.namespace &&
    pod.name === source.sandbox_pod.name &&
    pod.uid === source.sandbox_pod.uid
  );
}
