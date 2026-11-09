// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes } from 'node:crypto';
import { and, eq, isNull } from 'drizzle-orm';
import { z } from 'zod';
import type { SandboxNetworkRequest } from '@orca/sandbox-runtime';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import {
  sandboxNetworkBindings,
  environments,
  workspaces,
  sessions,
} from '../persistence/postgres/schema.js';
import { SandboxEgressError } from './sandbox-egress.js';
import { loadSessionExecutionOwner } from './session-harness-binding.js';
import {
  networkPod,
  SandboxNetworkAuthenticationError,
  type NetworkPod,
  type SandboxNetworkAuthority,
} from './sandbox-network-authority.js';
import { canonicalPolicy } from './sandbox-network-enforcement.js';

const ownerSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('environment') }).strict(),
  z.object({ kind: z.literal('harness'), session_id: z.string(), pod: networkPod }).strict(),
]);
const hash = (value: string) => createHash('sha256').update(value).digest('hex');
const opaque = () => randomBytes(32).toString('base64url');
type NetworkBinding = typeof sandboxNetworkBindings.$inferSelect;
type ReadDb = Pick<DbClient, 'select'>;
function invalid(): never {
  throw new SandboxEgressError('invalid_capability');
}
export class SandboxNetworkService {
  constructor(
    private readonly db: DbClient,
    private readonly options: {
      authority: SandboxNetworkAuthority;
      proxyUrl: string;
      internalBypass: string[];
    },
  ) {}

  private async ownerCurrent(row: NetworkBinding, db: ReadDb = this.db) {
    const [live] = await db
      .select({ environment: environments })
      .from(environments)
      .innerJoin(workspaces, eq(workspaces.id, environments.workspaceId))
      .where(
        and(
          eq(environments.id, row.environmentId),
          eq(environments.workspaceId, row.workspaceId),
          eq(environments.target, 'cloud'),
          isNull(environments.deletedAt),
          isNull(environments.archivedAt),
          eq(workspaces.status, 'active'),
          isNull(workspaces.archivedAt),
        ),
      )
      .limit(1);
    if (!live || row.revokedAt) invalid();
    const owner = ownerSchema.parse(row.owner);
    if (owner.kind === 'harness') {
      const [session] = await db
        .select()
        .from(sessions)
        .where(and(eq(sessions.id, owner.session_id), eq(sessions.workspaceId, row.workspaceId)))
        .limit(1);
      if (
        !session ||
        session.environmentId !== row.environmentId ||
        session.deletedAt ||
        session.archivedAt ||
        session.status === 'terminated' ||
        (await loadSessionExecutionOwner(db, row.workspaceId, session.id)) !== 'harness-server' ||
        !(await this.options.authority.ownerLive(owner.pod))
      )
        invalid();
    }
    return { ...live, owner };
  }
  private async register(
    environmentId: string,
    workspaceId: string | undefined,
    owner: z.infer<typeof ownerSchema>,
    tx: DbTransaction,
  ): Promise<SandboxNetworkRequest> {
    const [environment] = await tx
      .select()
      .from(environments)
      .where(eq(environments.id, environmentId))
      .for('update');
    if (!environment || (workspaceId && environment.workspaceId !== workspaceId)) invalid();
    const ownerSubject =
      owner.kind === 'environment' ? `environment:${environmentId}` : `harness:${owner.session_id}`;
    const row = {
      id: 'snb_' + opaque(),
      workspaceId: environment.workspaceId,
      environmentId,
      ownerSubject,
      owner,
      generation: opaque(),
      capabilityDigest: null,
      workload: null,
      pod: null,
      revokedAt: null,
    };
    await this.ownerCurrent(row, tx);
    await tx
      .update(sandboxNetworkBindings)
      .set({ revokedAt: new Date() })
      .where(
        and(
          eq(sandboxNetworkBindings.ownerSubject, ownerSubject),
          isNull(sandboxNetworkBindings.revokedAt),
        ),
      );
    await tx.insert(sandboxNetworkBindings).values(row);
    return { binding_id: row.id, generation: row.generation };
  }
  registerEnvironmentNetwork(environmentId: string): Promise<SandboxNetworkRequest> {
    return this.db.transaction((tx) =>
      this.register(environmentId, undefined, { kind: 'environment' }, tx),
    );
  }
  registerHarnessNetwork(
    workspaceId: string,
    sessionId: string,
    pod: NetworkPod,
  ): Promise<SandboxNetworkRequest> {
    return this.db.transaction(async (tx) => {
      const [session] = await tx
        .select()
        .from(sessions)
        .where(and(eq(sessions.id, sessionId), eq(sessions.workspaceId, workspaceId)))
        .for('update');
      if (!session?.environmentId) invalid();
      return this.register(
        session.environmentId,
        workspaceId,
        { kind: 'harness', session_id: sessionId, pod: networkPod.parse(pod) },
        tx,
      );
    });
  }
  private async revoke(
    subject: string,
    bindingId: string,
    generation: string,
    workspaceId?: string,
  ) {
    await this.db
      .update(sandboxNetworkBindings)
      .set({ revokedAt: new Date() })
      .where(
        and(
          eq(sandboxNetworkBindings.id, bindingId),
          eq(sandboxNetworkBindings.ownerSubject, subject),
          eq(sandboxNetworkBindings.generation, generation),
          ...(workspaceId ? [eq(sandboxNetworkBindings.workspaceId, workspaceId)] : []),
          isNull(sandboxNetworkBindings.revokedAt),
        ),
      );
    return { revoked: true as const };
  }
  revokeEnvironmentNetwork(environmentId: string, bindingId: string, generation: string) {
    return this.revoke(`environment:${environmentId}`, bindingId, generation);
  }
  revokeHarnessNetwork(
    workspaceId: string,
    sessionId: string,
    bindingId: string,
    generation: string,
  ) {
    return this.revoke(`harness:${sessionId}`, bindingId, generation, workspaceId);
  }

  async bootstrap(bindingId: string, generation: string, token: string) {
    let bound;
    try {
      bound = await this.options.authority.bootstrap(token, { binding_id: bindingId, generation });
    } catch (error) {
      throw new SandboxEgressError(
        error instanceof SandboxNetworkAuthenticationError
          ? 'invalid_capability'
          : 'dependency_unavailable',
      );
    }
    const capability = opaque();
    await this.db.transaction(async (tx) => {
      const [row] = await tx
        .select()
        .from(sandboxNetworkBindings)
        .where(eq(sandboxNetworkBindings.id, bindingId))
        .for('update');
      if (!row || row.generation !== generation) invalid();
      await this.ownerCurrent(row, tx);
      if (
        row.pod &&
        (canonicalPolicy(row.pod) !== canonicalPolicy(bound.pod) ||
          canonicalPolicy(row.workload) !== canonicalPolicy(bound.workload))
      )
        invalid();
      // A retry by the same Pod fences only an unpublished/retried bootstrap capability.
      await tx
        .update(sandboxNetworkBindings)
        .set({ pod: bound.pod, workload: bound.workload, capabilityDigest: hash(capability) })
        .where(eq(sandboxNetworkBindings.id, bindingId));
    });
    return {
      binding_id: bindingId,
      generation,
      proxy_url: this.options.proxyUrl,
      proxy_capability: capability,
      internal_bypass: [...this.options.internalBypass],
    };
  }
  async current(bindingId: string, generation?: string, db: ReadDb = this.db) {
    const [row] = await db
      .select()
      .from(sandboxNetworkBindings)
      .where(eq(sandboxNetworkBindings.id, bindingId))
      .limit(1);
    if (
      !row ||
      (generation !== undefined && row.generation !== generation) ||
      !row.capabilityDigest
    )
      invalid();
    await this.ownerCurrent(row, db);
    const bound = { pod: networkPod.parse(row.pod), workload: networkPod.parse(row.workload) };
    try {
      if (
        !(await this.options.authority.current(bound, {
          binding_id: row.id,
          generation: row.generation,
        }))
      )
        invalid();
    } catch (error) {
      if (error instanceof SandboxEgressError) throw error;
      throw new SandboxEgressError('dependency_unavailable');
    }
    // Enforcement calls can overlap replacement/revocation on another Registry replica.
    const [latest] = await db
      .select()
      .from(sandboxNetworkBindings)
      .where(eq(sandboxNetworkBindings.id, bindingId))
      .limit(1);
    if (
      !latest ||
      latest.revokedAt ||
      latest.generation !== row.generation ||
      latest.capabilityDigest !== row.capabilityDigest
    )
      invalid();
    const live = await this.ownerCurrent(latest, db);
    return { binding: latest, ...live, ...bound };
  }
  async fromCapability(capability: string) {
    const capabilityDigest = hash(capability);
    const [row] = await this.db
      .select({ id: sandboxNetworkBindings.id })
      .from(sandboxNetworkBindings)
      .where(eq(sandboxNetworkBindings.capabilityDigest, capabilityDigest))
      .limit(1);
    if (!row) return undefined;
    const current = await this.current(row.id);
    if (current.binding.capabilityDigest !== capabilityDigest) invalid();
    return current;
  }
  async parentFor(
    workspaceId: string,
    environmentId: string,
    sessionId: string,
    sandboxId: string,
    pod: NetworkPod,
    ownerPod?: NetworkPod,
    db: ReadDb = this.db,
  ) {
    const subject = ownerPod ? `harness:${sessionId}` : `environment:${environmentId}`;
    const [row] = await db
      .select()
      .from(sandboxNetworkBindings)
      .where(
        and(
          eq(sandboxNetworkBindings.ownerSubject, subject),
          isNull(sandboxNetworkBindings.revokedAt),
        ),
      )
      .limit(1);
    if (!row) invalid();
    const current = await this.current(row.id, undefined, db);
    if (
      row.workspaceId !== workspaceId ||
      row.environmentId !== environmentId ||
      current.workload.name !== sandboxId ||
      current.pod.namespace !== pod.namespace ||
      current.pod.name !== pod.name ||
      current.pod.uid !== pod.uid ||
      (ownerPod &&
        (current.owner.kind !== 'harness' ||
          current.owner.pod.namespace !== ownerPod.namespace ||
          current.owner.pod.name !== ownerPod.name ||
          current.owner.pod.uid !== ownerPod.uid))
    )
      invalid();
    return row;
  }
}
