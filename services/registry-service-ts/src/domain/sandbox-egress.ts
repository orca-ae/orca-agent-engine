// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes } from 'node:crypto';
import type { SessionEgressGrant } from '@orca/sandbox-runtime';
import type { SandboxNetworkService } from './sandbox-network.js';
import { and, eq, inArray, isNull, ne } from 'drizzle-orm';
import { z } from 'zod';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import {
  authenticatedWorkerLaunch,
  currentWorkerLaunch,
  runnerLaunchAuthority,
  sameWorkerLaunch,
  type WorkerLaunchAuthority,
} from './runner-launch-authority.js';
import {
  environments,
  environmentClaims,
  sandboxEgressBindings,
  sessions,
  sessionResources,
  vaults,
  vaultCredentials,
  workspaces,
} from '../persistence/postgres/schema.js';
import {
  egressOwnerAuthority,
  type EgressOwnerAuthority,
  type egressResolveRequest,
  type EgressResolveResponse,
} from '../contracts/sandbox-egress.contract.js';
import type { SecretProvider } from '../secrets/secret-provider.js';
import { loadSessionExecutionOwner } from './session-harness-binding.js';
import { loadGitProxyBinding } from './git-proxy-capability.js';
import { newId } from './versioning.js';
import type {
  EgressLivenessVerifier,
  EgressSandboxLocator,
  HarnessEgressAuthority,
} from './sandbox-egress-authority.js';
import {
  decodeEgressBasic,
  egressMarkers,
  protectedEgressHeaders,
  gitEgressOperation,
  vaultEgressTargets,
  type EgressCredential,
} from './sandbox-egress-credentials.js';

export class SandboxEgressError extends Error {
  constructor(
    readonly type:
      | 'invalid_capability'
      | 'permission_denied'
      | 'dependency_unavailable'
      | 'invalid_request',
  ) {
    super(type.replaceAll('_', ' '));
  }
}
function deny(): never {
  throw new SandboxEgressError('permission_denied');
}
function invalid(): never {
  throw new SandboxEgressError('invalid_capability');
}
const digest = (value: string) => createHash('sha256').update(value).digest('hex');
const opaque = () => randomBytes(32).toString('base64url');
interface BindingCredentials {
  owner: EgressOwnerAuthority;
  mappings: EgressCredential[];
  restEnabled: boolean;
}
type Binding = typeof sandboxEgressBindings.$inferSelect;
export interface SandboxEgressOptions {
  networks: SandboxNetworkService;
  githubRestReadEnabled: boolean;
  environmentClaimTtlMs: number;
  verifyLiveness: EgressLivenessVerifier;
  locateSandbox: EgressSandboxLocator;
}

/** Tokens are returned once; retrying issue always fences the previous generation. */
export class SandboxEgressService {
  get networks() {
    return this.options.networks;
  }
  constructor(
    private readonly db: DbClient,
    private readonly secrets: SecretProvider,
    private readonly options: SandboxEgressOptions,
  ) {}

  async issueHarnessEgress(
    workspaceId: string,
    sessionId: string,
    sandboxId: string,
    ownerPod: HarnessEgressAuthority['owner_pod'],
  ) {
    let sandboxPod: HarnessEgressAuthority['sandbox_pod'];
    try {
      sandboxPod = await this.options.locateSandbox(sandboxId);
    } catch {
      throw new SandboxEgressError('dependency_unavailable');
    }
    return this.issueEgress(workspaceId, sessionId, {
      kind: 'harness',
      owner_pod: ownerPod,
      sandbox_pod: sandboxPod,
      sandbox_handle_id: sandboxId,
    });
  }

  async issueRunnerEgress(workspaceId: string, sessionId: string, runnerId: string) {
    const { session } = await this.session(workspaceId, sessionId);
    const owner = runnerLaunchAuthority(session.runnerLaunchAuthority);
    if (!owner)
      throw new Error(
        'managed runner provenance unavailable; reconnect worker or reprovision Environment',
      );
    if (owner.runner_id !== runnerId || session.runnerId !== runnerId) invalid();
    return this.issueEgress(workspaceId, sessionId, owner);
  }

  authenticatedWorkerLaunch(
    environmentId: string,
    token: string,
    ownerPod: string,
    workerConnId: string,
  ) {
    return authenticatedWorkerLaunch(
      this.db,
      this.options.locateSandbox,
      environmentId,
      token,
      ownerPod,
      workerConnId,
    );
  }

  async requiresManagedWorker(environmentId: string): Promise<boolean> {
    const [row] = await this.db
      .select({ target: environments.target })
      .from(environments)
      .where(eq(environments.id, environmentId))
      .limit(1);
    return row?.target === 'cloud';
  }

  private async lockWorkerLaunch(tx: DbTransaction, source: WorkerLaunchAuthority) {
    await tx
      .select()
      .from(environments)
      .where(eq(environments.id, source.environment_id))
      .for('share');
    await tx
      .select()
      .from(environmentClaims)
      .where(eq(environmentClaims.environmentId, source.environment_id))
      .for('share');
    if (
      !(await currentWorkerLaunch(
        tx,
        this.options.locateSandbox,
        this.options.environmentClaimTtlMs,
        source,
      ))
    )
      throw new Error('managed worker launch was replaced; reconnect or reprovision Environment');
  }

  async bindRunnerLaunch(
    sessionId: string,
    runnerId: string,
    source: WorkerLaunchAuthority,
    send: () => void,
  ): Promise<boolean> {
    const guard = async (tx: DbTransaction, bind: boolean) => {
      const [session] = await tx
        .select()
        .from(sessions)
        .where(eq(sessions.id, sessionId))
        .for('update');
      if (!session || session.environmentId !== source.environment_id)
        throw new Error('managed runner Session is unavailable');
      await this.session(session.workspaceId, sessionId, tx);
      if ((await loadSessionExecutionOwner(tx, session.workspaceId, sessionId)) !== 'registry')
        throw new Error('managed runner Session belongs to Harness');
      await this.lockWorkerLaunch(tx, source);
      const authority = { ...source, kind: 'registry' as const, runner_id: runnerId };
      if (bind) {
        if (session.runnerId !== null)
          throw new Error('managed runner requires teardown and a fresh binding');
        await tx
          .update(sessions)
          .set({
            runnerId,
            hostEnvironmentId: source.environment_id,
            distributionState: 'pending',
            runnerLaunchAuthority: authority,
          })
          .where(eq(sessions.id, sessionId));
      } else {
        const persisted = runnerLaunchAuthority(session.runnerLaunchAuthority);
        if (
          session.runnerId !== runnerId ||
          !persisted ||
          persisted.runner_id !== runnerId ||
          !sameWorkerLaunch(persisted, source)
        )
          throw new Error('managed runner binding was replaced');
      }
    };
    // Commit the binding before the runner can dial; recheck under locks through enqueue.
    await this.db.transaction((tx) => guard(tx, true));
    await this.db.transaction(async (tx) => {
      await guard(tx, false);
      send();
    });
    return true;
  }

  async retireStaleRunnerLaunches(
    environmentId: string,
    source: WorkerLaunchAuthority | undefined,
  ): Promise<string[]> {
    if (source && source.environment_id !== environmentId)
      throw new Error('managed worker environment mismatch');
    const rows = await this.db
      .select({ id: sessions.id })
      .from(sessions)
      .where(
        and(
          eq(sessions.environmentId, environmentId),
          isNull(sessions.deletedAt),
          isNull(sessions.archivedAt),
          ne(sessions.status, 'terminated'),
        ),
      );
    const retired: string[] = [];
    for (const row of rows)
      await this.db.transaction(async (tx) => {
        const [session] = await tx
          .select()
          .from(sessions)
          .where(eq(sessions.id, row.id))
          .for('update');
        if (!session) return;
        if (
          session.deletedAt ||
          session.archivedAt ||
          session.status === 'terminated' ||
          (await loadSessionExecutionOwner(tx, session.workspaceId, session.id)) !== 'registry'
        )
          return;
        if (source) await this.lockWorkerLaunch(tx, source);
        const previous = runnerLaunchAuthority(session.runnerLaunchAuthority);
        if (
          source &&
          previous?.runner_id === session.runnerId &&
          sameWorkerLaunch(previous, source)
        )
          return;
        if (session.runnerId) retired.push(session.runnerId);
        await tx
          .update(sessions)
          .set({ distributionState: source && !session.runnerId ? 'pending' : 'failed' })
          .where(eq(sessions.id, session.id));
        await tx
          .update(sandboxEgressBindings)
          .set({ revokedAt: new Date() })
          .where(
            and(
              eq(sandboxEgressBindings.sessionId, session.id),
              isNull(sandboxEgressBindings.revokedAt),
            ),
          );
      });
    return retired;
  }

  async appliesToSession(workspaceId: string, sessionId: string): Promise<boolean> {
    const [row] = await this.db
      .select({ target: environments.target })
      .from(sessions)
      .innerJoin(environments, eq(environments.id, sessions.environmentId))
      .where(and(eq(sessions.id, sessionId), eq(sessions.workspaceId, workspaceId)))
      .limit(1);
    return row?.target === 'cloud';
  }

  async assertCurrentGrant(
    workspaceId: string,
    sessionId: string,
    bindingId: string,
    generation: string,
  ) {
    const [binding] = await this.db
      .select()
      .from(sandboxEgressBindings)
      .where(
        and(
          eq(sandboxEgressBindings.id, bindingId),
          eq(sandboxEgressBindings.sessionId, sessionId),
        ),
      )
      .limit(1);
    const current = await this.current(binding, generation);
    if (current.workspaceId !== workspaceId) deny();
  }

  private async session(
    workspaceId: string,
    sessionId: string,
    db: Pick<DbClient, 'select'> = this.db,
  ) {
    const [row] = await db
      .select({ session: sessions, environment: environments })
      .from(sessions)
      .innerJoin(workspaces, eq(workspaces.id, sessions.workspaceId))
      .innerJoin(
        environments,
        and(eq(environments.id, sessions.environmentId), eq(environments.workspaceId, workspaceId)),
      )
      .where(
        and(
          eq(sessions.id, sessionId),
          eq(sessions.workspaceId, workspaceId),
          isNull(sessions.deletedAt),
          isNull(sessions.archivedAt),
          ne(sessions.status, 'terminated'),
          eq(workspaces.status, 'active'),
          isNull(workspaces.archivedAt),
          isNull(environments.deletedAt),
          isNull(environments.archivedAt),
          eq(environments.target, 'cloud'),
        ),
      )
      .limit(1);
    if (!row) invalid();
    return row;
  }

  private async authority(
    workspaceId: string,
    sessionId: string,
    owner: EgressOwnerAuthority,
    issuing = false,
    db: Pick<DbClient, 'select'> = this.db,
  ) {
    const { session, environment } = await this.session(workspaceId, sessionId, db);
    const executionOwner = await loadSessionExecutionOwner(db, workspaceId, sessionId);
    if (owner.kind === 'harness') {
      if (
        executionOwner !== 'harness-server' ||
        (!issuing && session.sandboxHandleId !== owner.sandbox_handle_id)
      )
        invalid();
      let live: boolean;
      try {
        const associated = await this.options.locateSandbox(owner.sandbox_handle_id);
        live =
          associated.uid === owner.sandbox_pod.uid &&
          associated.name === owner.sandbox_pod.name &&
          associated.namespace === owner.sandbox_pod.namespace &&
          (await this.options.verifyLiveness(owner));
      } catch {
        throw new SandboxEgressError('dependency_unavailable');
      }
      if (!live) invalid();
    } else {
      const persisted = runnerLaunchAuthority(session.runnerLaunchAuthority);
      if (
        executionOwner !== 'registry' ||
        session.runnerId !== owner.runner_id ||
        session.distributionState !== 'assigned' ||
        session.hostEnvironmentId !== owner.environment_id ||
        !persisted ||
        persisted.runner_id !== owner.runner_id ||
        !sameWorkerLaunch(persisted, owner) ||
        environment.id !== owner.environment_id ||
        environment.environmentTokenDigest !== owner.launch_digest ||
        environment.cloudSandboxHandleId !== owner.sandbox_handle_id ||
        !environment.environmentTokenExpiresAt ||
        environment.environmentTokenExpiresAt.getTime() <= Date.now()
      )
        invalid();
      let associated: HarnessEgressAuthority['sandbox_pod'];
      try {
        associated = await this.options.locateSandbox(owner.sandbox_handle_id);
      } catch {
        throw new SandboxEgressError('dependency_unavailable');
      }
      if (
        associated.uid !== owner.sandbox_pod.uid ||
        associated.name !== owner.sandbox_pod.name ||
        associated.namespace !== owner.sandbox_pod.namespace
      )
        invalid();
      const [claim] = await db
        .select()
        .from(environmentClaims)
        .where(eq(environmentClaims.environmentId, owner.environment_id))
        .limit(1);
      if (
        !claim ||
        claim.ownerPod !== owner.owner_pod ||
        claim.workerConnId !== owner.worker_conn_id ||
        Date.now() - claim.lastPing.getTime() > this.options.environmentClaimTtlMs
      )
        invalid();
    }
    return { session, environment };
  }

  async issueEgress(
    workspaceId: string,
    sessionId: string,
    owner: EgressOwnerAuthority,
  ): Promise<SessionEgressGrant> {
    owner = egressOwnerAuthority.parse(owner);
    const { session, environment: sessionEnvironment } = await this.authority(
      workspaceId,
      sessionId,
      owner,
      true,
    );
    const parent = await this.networks.parentFor(
      workspaceId,
      sessionEnvironment.id,
      sessionId,
      owner.sandbox_handle_id,
      owner.sandbox_pod,
      owner.kind === 'harness' ? owner.owner_pod : undefined,
    );
    const mappings: EgressCredential[] = [];
    const environment: Record<string, string> = {};
    const gitMarker = `orca_git_${opaque()}`;
    const resources = await this.db
      .select()
      .from(sessionResources)
      .where(
        and(
          eq(sessionResources.workspaceId, workspaceId),
          eq(sessionResources.sessionId, sessionId),
          eq(sessionResources.type, 'github_repository'),
          isNull(sessionResources.deletedAt),
          isNull(sessionResources.detachedAt),
        ),
      );
    for (const resource of resources) {
      const binding = await loadGitProxyBinding(this.db, {
        workspaceId,
        sessionId,
        resourceId: resource.id,
      });
      if (!binding || new URL(binding.scope.repoUrl).hostname !== 'github.com') deny();
      mappings.push({
        repositoryUrl: binding.scope.repoUrl,
        marker: gitMarker,
        environmentName: 'GH_TOKEN',
        source: { kind: 'git_resource', resourceId: resource.id },
        targets: ['github.com', ...(this.options.githubRestReadEnabled ? ['api.github.com'] : [])],
      });
    }
    const selectedNames = new Set<string>();
    for (const vaultId of session.vaultIds) {
      const [vault] = await this.db
        .select()
        .from(vaults)
        .where(
          and(
            eq(vaults.id, vaultId),
            eq(vaults.workspaceId, workspaceId),
            isNull(vaults.deletedAt),
            isNull(vaults.archivedAt),
          ),
        )
        .limit(1);
      if (!vault) deny();
      const rows = await this.db
        .select()
        .from(vaultCredentials)
        .where(
          and(
            eq(vaultCredentials.workspaceId, workspaceId),
            eq(vaultCredentials.vaultId, vaultId),
            eq(vaultCredentials.authType, 'environment_variable'),
            isNull(vaultCredentials.deletedAt),
            isNull(vaultCredentials.archivedAt),
          ),
        );
      for (const row of rows) {
        if (!row.secretName) deny();
        if (selectedNames.has(row.secretName)) continue;
        selectedNames.add(row.secretName);
        if (
          !/^[A-Za-z_][A-Za-z0-9_]*$/.test(row.secretName) ||
          /^(?:GH_|GIT_|GITHUB_TOKEN$|ORCA_GIT_|ORCA_EGRESS_BOOTSTRAP_|HTTPS?_PROXY$|NO_PROXY$|ALL_PROXY$|SSL_CERT_|CURL_|SSH_ASKPASS$|NODE_EXTRA_CA_CERTS$|NODE_TLS_REJECT_UNAUTHORIZED$|REQUESTS_CA_BUNDLE$|PIP_CERT$|NPM_CONFIG_|CARGO_HTTP_|APT_CONFIG$)/i.test(
            row.secretName,
          )
        )
          deny();
        const targets = vaultEgressTargets(row.networking, row.authConfig);
        if (!targets) deny();
        const marker = `orca_vault_${opaque()}`;
        mappings.push({
          marker,
          environmentName: row.secretName,
          source: { kind: 'vault_environment_variable', vaultId, credentialId: row.id },
          targets,
        });
        environment[row.secretName] = marker;
      }
    }
    const capability = opaque();
    if (mappings.length > 128) deny();
    const id = newId('egb');
    const generation = opaque();
    const credentials: BindingCredentials = {
      owner,
      mappings,
      restEnabled: this.options.githubRestReadEnabled,
    };
    await this.db.transaction(async (tx) => {
      // The Session lock serializes replacement across Registry replicas.
      await tx
        .select()
        .from(sessions)
        .where(and(eq(sessions.id, sessionId), eq(sessions.workspaceId, workspaceId)))
        .for('update');
      await this.authority(workspaceId, sessionId, owner, true, tx);
      const latestParent = await this.networks.parentFor(
        workspaceId,
        sessionEnvironment.id,
        sessionId,
        owner.sandbox_handle_id,
        owner.sandbox_pod,
        owner.kind === 'harness' ? owner.owner_pod : undefined,
        tx,
      );
      if (latestParent.id !== parent.id) invalid();
      await tx
        .update(sandboxEgressBindings)
        .set({ revokedAt: new Date() })
        .where(
          and(
            eq(sandboxEgressBindings.sessionId, sessionId),
            isNull(sandboxEgressBindings.revokedAt),
          ),
        );
      if (owner.kind === 'harness')
        await tx
          .update(sessions)
          .set({ sandboxHandleId: owner.sandbox_handle_id })
          .where(eq(sessions.id, sessionId));
      await tx.insert(sandboxEgressBindings).values({
        id,
        networkBindingId: parent.id,
        sessionId,
        executionGeneration: generation,
        capabilityDigest: digest(capability),
        credentials,
      });
    });
    return {
      network_binding_id: parent.id,
      binding_id: id,
      generation,
      proxy_capability: capability,
      placeholders: environment,
      ...(resources.length ? { git: { marker: gitMarker, username: 'x-access-token' } } : {}),
    };
  }

  async revokeEgress(
    workspaceId: string,
    sessionId: string,
    bindingId: string,
    generation: string,
  ) {
    await this.db
      .update(sandboxEgressBindings)
      .set({ revokedAt: new Date() })
      .where(
        and(
          eq(sandboxEgressBindings.id, bindingId),
          eq(sandboxEgressBindings.sessionId, sessionId),
          eq(sandboxEgressBindings.executionGeneration, generation),
          isNull(sandboxEgressBindings.revokedAt),
          inArray(
            sandboxEgressBindings.sessionId,
            this.db
              .select({ id: sessions.id })
              .from(sessions)
              .where(eq(sessions.workspaceId, workspaceId)),
          ),
        ),
      );
    return { revoked: true as const };
  }

  private async current(binding: Binding | undefined, generation?: string) {
    if (
      !binding ||
      binding.revokedAt ||
      (generation !== undefined && binding.executionGeneration !== generation)
    )
      invalid();
    const [session] = await this.db
      .select({ workspaceId: sessions.workspaceId })
      .from(sessions)
      .where(eq(sessions.id, binding.sessionId))
      .limit(1);
    if (!session) invalid();
    const credentials = binding.credentials as BindingCredentials;
    if (
      !egressOwnerAuthority.safeParse(credentials?.owner).success ||
      !Array.isArray(credentials.mappings)
    )
      invalid();
    const live = await this.authority(session.workspaceId, binding.sessionId, credentials.owner);
    if (!binding.networkBindingId) invalid();
    const parent = await this.networks.current(binding.networkBindingId);
    if (
      parent.binding.workspaceId !== session.workspaceId ||
      parent.binding.environmentId !== live.environment.id ||
      parent.workload.name !== credentials.owner.sandbox_handle_id ||
      parent.pod.uid !== credentials.owner.sandbox_pod.uid ||
      parent.pod.name !== credentials.owner.sandbox_pod.name ||
      parent.pod.namespace !== credentials.owner.sandbox_pod.namespace
    )
      invalid();
    return {
      binding,
      credentials,
      workspaceId: session.workspaceId,
      ...live,
      environment: parent.environment,
    };
  }

  private targetAllowed(networking: unknown, host: string) {
    const net = networking as { type?: string; allowed_hosts?: string[] };
    if (
      net?.type !== 'unrestricted' &&
      !(
        net?.type === 'limited' &&
        net.allowed_hosts?.some((allowedHost) => allowedHost.toLowerCase() === host.toLowerCase())
      )
    )
      deny();
  }

  private async checkVaultSelection(
    workspaceId: string,
    vaultIds: string[],
    mapping: EgressCredential,
  ) {
    if (mapping.source.kind !== 'vault_environment_variable' || !vaultIds.length) deny();
    const rows = await this.db
      .select({ id: vaultCredentials.id, vaultId: vaultCredentials.vaultId })
      .from(vaultCredentials)
      .innerJoin(
        vaults,
        and(
          eq(vaults.id, vaultCredentials.vaultId),
          eq(vaults.workspaceId, workspaceId),
          isNull(vaults.archivedAt),
          isNull(vaults.deletedAt),
        ),
      )
      .where(
        and(
          eq(vaultCredentials.workspaceId, workspaceId),
          inArray(vaultCredentials.vaultId, vaultIds),
          eq(vaultCredentials.authType, 'environment_variable'),
          eq(vaultCredentials.secretName, mapping.environmentName),
          isNull(vaultCredentials.archivedAt),
          isNull(vaultCredentials.deletedAt),
        ),
      );
    rows.sort((a, b) => vaultIds.indexOf(a.vaultId) - vaultIds.indexOf(b.vaultId));
    if (rows[0]?.id !== mapping.source.credentialId) deny();
  }

  async authorizeProxy(capability: string, target: { host: string; port: 80 | 443 }) {
    const network = await this.networks.fromCapability(capability);
    if (network) {
      this.targetAllowed(network.environment.networking, target.host);
      return {
        binding_id: network.binding.id,
        generation: network.binding.generation,
        scope: 'sandbox' as const,
        decision: 'passthrough' as const,
        managed_auth: false,
      };
    }
    const [binding] = await this.db
      .select()
      .from(sandboxEgressBindings)
      .where(eq(sandboxEgressBindings.capabilityDigest, digest(capability)))
      .limit(1);
    const current = await this.current(binding);
    this.targetAllowed(current.environment.networking, target.host);
    const mappings = current.credentials.mappings.filter((mapping) =>
      mapping.targets.includes(target.host),
    );
    let authorizedSources = 0;
    for (const mapping of mappings) {
      if (mapping.source.kind === 'git_resource') {
        const git = await loadGitProxyBinding(this.db, {
          workspaceId: current.workspaceId,
          sessionId: current.binding.sessionId,
          resourceId: mapping.source.resourceId,
        });
        if (git && git.scope.repoUrl === mapping.repositoryUrl) authorizedSources++;
      } else {
        try {
          await this.checkVaultSelection(current.workspaceId, current.session.vaultIds, mapping);
          authorizedSources++;
        } catch (error) {
          if (!(error instanceof SandboxEgressError) || error.type !== 'permission_denied')
            throw error;
        }
      }
    }
    if (mappings.length && !authorizedSources) deny();
    // Classification is frozen; current source revocation never downgrades MITM to passthrough.
    const managed = mappings.length > 0;
    return {
      binding_id: current.binding.id,
      generation: current.binding.executionGeneration,
      scope: 'session' as const,
      decision: managed ? ('mitm' as const) : ('passthrough' as const),
      managed_auth: managed,
    };
  }

  async resolveRequest(
    request: z.infer<typeof egressResolveRequest>,
  ): Promise<EgressResolveResponse> {
    if (!['sandbox', 'session'].includes(request.scope)) invalid();
    if (
      !request.path_and_query.startsWith('/') ||
      request.path_and_query.startsWith('//') ||
      /[\r\n\0#\\]/.test(request.path_and_query)
    )
      deny();
    if (request.scope === 'sandbox') {
      const current = await this.networks.current(request.binding_id, request.generation);
      this.targetAllowed(current.environment.networking, request.target.host);
      return { replacements: [], credential_version: null, request_body_policy: 'stream' };
    }
    const [binding] = await this.db
      .select()
      .from(sandboxEgressBindings)
      .where(eq(sandboxEgressBindings.id, request.binding_id))
      .limit(1);
    const current = await this.current(binding, request.generation);
    const workspaceId = current.workspaceId,
      sessionId = current.binding.sessionId;
    this.targetAllowed(current.environment.networking, request.target.host);
    const names = new Set<string>();
    const headers = request.candidate_headers.map((header) => {
      if (names.has(header.name) || protectedEgressHeaders.has(header.name)) deny();
      names.add(header.name);
      const basic = header.name === 'authorization' && /^[ \t]*Basic(?:\s|$)/i.test(header.value);
      const original = basic ? decodeEgressBasic(header.value) : header.value;
      if (original === null) deny();
      const markers = egressMarkers(original);
      for (const marker of markers) {
        const known = current.credentials.mappings.filter(
          (mapping) => mapping.marker === marker[0],
        );
        if (!known.length) deny();
        if (
          known.some((mapping) => mapping.source.kind === 'git_resource') &&
          (header.name !== 'authorization' ||
            !(basic
              ? original === 'x-access-token:' + marker[0]
              : /^(?:Bearer|token) /i.test(original) &&
                original.slice(original.indexOf(' ') + 1) === marker[0]))
        )
          deny();
      }
      return { ...header, basic, original, markers };
    });
    const mappings = current.credentials.mappings.filter((mapping) =>
      mapping.targets.includes(request.target.host),
    );
    const replacements: Array<{ header: string; value: string }> = [];
    const versions: string[] = [];
    const sourceRechecks: Array<() => Promise<void>> = [];
    const resolved = new Map<EgressCredential, string | null>();
    let matchedGit = false;
    const resolveMapping = async (mapping: EgressCredential): Promise<string | null> => {
      if (resolved.has(mapping)) return resolved.get(mapping)!;
      let ref: string;
      let version: string;
      if (mapping.source.kind === 'git_resource') {
        const source = mapping.source;
        if (!mapping.repositoryUrl) deny();
        if (
          !gitEgressOperation(
            mapping.repositoryUrl,
            request.target.host,
            request.method,
            request.path_and_query,
            current.credentials.restEnabled,
          )
        )
          return null;
        const git = await loadGitProxyBinding(this.db, {
          workspaceId,
          sessionId,
          resourceId: mapping.source.resourceId,
        });
        if (!git || git.scope.repoUrl !== mapping.repositoryUrl) deny();
        matchedGit = true;
        ref = git.secretRef;
        version = git.scope.credentialRevision;
        sourceRechecks.push(async () => {
          const latest = await loadGitProxyBinding(this.db, {
            workspaceId,
            sessionId,
            resourceId: source.resourceId,
          });
          if (
            !latest ||
            latest.secretRef !== ref ||
            latest.scope.credentialRevision !== version ||
            latest.scope.repoUrl !== git.scope.repoUrl
          )
            deny();
        });
      } else {
        const source = mapping.source;
        if (!current.session.vaultIds.includes(source.vaultId)) deny();
        await this.checkVaultSelection(workspaceId, current.session.vaultIds, mapping);
        const [row] = await this.db
          .select({ credential: vaultCredentials })
          .from(vaultCredentials)
          .innerJoin(
            vaults,
            and(
              eq(vaults.id, vaultCredentials.vaultId),
              eq(vaults.workspaceId, workspaceId),
              isNull(vaults.archivedAt),
              isNull(vaults.deletedAt),
            ),
          )
          .where(
            and(
              eq(vaultCredentials.id, source.credentialId),
              eq(vaultCredentials.vaultId, source.vaultId),
              eq(vaultCredentials.workspaceId, workspaceId),
              eq(vaultCredentials.authType, 'environment_variable'),
              isNull(vaultCredentials.archivedAt),
              isNull(vaultCredentials.deletedAt),
            ),
          )
          .limit(1);
        if (!row || row.credential.secretName !== mapping.environmentName) deny();
        const targets = vaultEgressTargets(row.credential.networking, row.credential.authConfig);
        if (
          !mapping.targets.includes(request.target.host) ||
          !targets?.includes(request.target.host)
        ) {
          resolved.set(mapping, null);
          return null;
        }
        ref = row.credential.accessSecretRef;
        version = digest(JSON.stringify([ref, row.credential.updatedAt.toISOString()]));
        sourceRechecks.push(async () => {
          const [latest] = await this.db
            .select()
            .from(vaultCredentials)
            .innerJoin(
              vaults,
              and(
                eq(vaults.id, vaultCredentials.vaultId),
                isNull(vaults.archivedAt),
                isNull(vaults.deletedAt),
              ),
            )
            .where(
              and(
                eq(vaultCredentials.id, source.credentialId),
                eq(vaultCredentials.workspaceId, workspaceId),
                eq(vaultCredentials.vaultId, source.vaultId),
                isNull(vaultCredentials.archivedAt),
                isNull(vaultCredentials.deletedAt),
              ),
            )
            .limit(1);
          const credential = latest?.vault_credentials;
          if (
            !credential ||
            credential.accessSecretRef !== ref ||
            digest(JSON.stringify([ref, credential.updatedAt.toISOString()])) !== version ||
            !vaultEgressTargets(credential.networking, credential.authConfig)?.includes(
              request.target.host,
            )
          )
            deny();
        });
      }
      let secret: string | null;
      try {
        secret = await this.secrets.resolve(ref);
      } catch {
        throw new SandboxEgressError('dependency_unavailable');
      }
      if (!secret || /[\r\n\0]/.test(secret))
        throw new SandboxEgressError('dependency_unavailable');
      versions.push(version);
      resolved.set(mapping, secret);
      return secret;
    };
    // Validate the Git route and require its authentication even with no candidates.
    const gitMappings = mappings.filter((mapping) => mapping.source.kind === 'git_resource');
    if (
      gitMappings.length &&
      !gitMappings.some(
        (mapping) =>
          mapping.repositoryUrl &&
          gitEgressOperation(
            mapping.repositoryUrl,
            request.target.host,
            request.method,
            request.path_and_query,
            current.credentials.restEnabled,
          ),
      )
    )
      deny();
    for (const header of headers) {
      let value = '';
      let offset = 0;
      let changed = false;
      for (const marker of header.markers) {
        const candidates = current.credentials.mappings.filter(
          (mapping) => mapping.marker === marker[0],
        );
        const mapping = candidates.find(
          (mapping) =>
            mapping.source.kind !== 'git_resource' ||
            (mapping.targets.includes(request.target.host) &&
              mapping.repositoryUrl &&
              gitEgressOperation(
                mapping.repositoryUrl,
                request.target.host,
                request.method,
                request.path_and_query,
                current.credentials.restEnabled,
              )),
        );
        if (!mapping) deny();
        // HTTP80 never reads a secret; all substitution uses only the original input.
        const secret = request.target.port === 443 ? await resolveMapping(mapping) : null;
        if (
          secret !== null &&
          header.basic &&
          marker.index! < header.original.indexOf(':') &&
          secret.includes(':')
        )
          deny();
        value +=
          header.original.slice(offset, marker.index) +
          (secret === null
            ? marker[0]
            : header.basic
              ? Buffer.from(secret).toString('latin1')
              : secret);
        offset = marker.index! + marker[0].length;
        changed ||= secret !== null;
      }
      if (changed) {
        value += header.original.slice(offset);
        if (header.basic) value = 'Basic ' + Buffer.from(value, 'latin1').toString('base64');
        if (Buffer.byteLength(value) > 16384 || /[\r\n\0]/.test(value)) deny();
        replacements.push({ header: header.name, value });
      }
    }
    if (gitMappings.length && !matchedGit) deny();
    // Recheck current generation and permissions after the asynchronous secret read.
    const [latest] = await this.db
      .select()
      .from(sandboxEgressBindings)
      .where(eq(sandboxEgressBindings.id, request.binding_id))
      .limit(1);
    const rechecked = await this.current(latest, request.generation);
    this.targetAllowed(rechecked.environment.networking, request.target.host);
    const referencedVaultMappings = current.credentials.mappings.filter(
      (mapping) =>
        mapping.source.kind === 'vault_environment_variable' &&
        headers.some((header) => header.markers.some((marker) => marker[0] === mapping.marker)),
    );
    for (const mapping of new Set([...mappings, ...referencedVaultMappings]))
      if (mapping.source.kind === 'vault_environment_variable')
        await this.checkVaultSelection(workspaceId, rechecked.session.vaultIds, mapping);
    await Promise.all(sourceRechecks.map((recheck) => recheck()));
    return {
      replacements,
      credential_version: versions.length ? digest(JSON.stringify(versions)) : null,
      request_body_policy:
        matchedGit && request.method === 'POST'
          ? ('git_upload_pack' as const)
          : ('stream' as const),
    };
  }
}
