// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { ParsedSkillsPush } from './skills-materialize.js';
import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  open,
  opendir,
  realpath,
  rename,
  writeFile,
  rm,
} from 'node:fs/promises';
import { dirname, join, posix } from 'node:path';
import {
  buildSandboxWritePolicy,
  planSkillBundleChmod,
  createPolicyEnforcedSandbox,
  createManagedToolSandboxRuntime,
  decodeResourceChunk,
  parseResourceManifest,
  resourceFilePath,
  resourceManifestDigest,
  hasWritePolicyEnforcement,
  resolveOpenedDescriptorPath,
  RESOURCE_CHUNK_BYTES,
  RESOURCE_MAX_FILE_BYTES,
  RESOURCE_MAX_FILES,
  RESOURCE_MAX_TOTAL_BYTES,
  OUTPUT_RESOURCE_ID,
  GIT_PROXY_AUTH_ROOT,
  resourceCheckpointDigest,
  type ResourceFileDescriptor,
  type ResourceManifest,
  type GitProxyCapability,
  type SandboxHandle,
  type ValidatedSandboxWritePolicy,
  type ResourceCheckpoint,
  type ChangedResourceFile,
  type DeletedResourceFile,
} from './sandbox/seam.js';

export type { ResourceCheckpoint } from './sandbox/seam.js';
interface IncomingFile {
  descriptor: ResourceFileDescriptor;
  local: string;
  received: number;
}
interface Incoming {
  manifest: ResourceManifest;
  signature: string;
  domains: string[];
  unrestricted: boolean;
  directory: string;
  files: Map<string, IncomingFile>;
}
interface Active {
  manifest: ResourceManifest;
  signature: string;
  root: string;
  raw: SandboxHandle;
  tools: SandboxHandle;
  baseline: Map<string, string>;
  gitExpiresAt: number;
  domains: string[];
  unrestricted: boolean;
}
interface Frozen {
  checkpoint: ResourceCheckpoint;
  digest: string;
  directory: string;
  files: Map<string, string>;
  nextBaseline: Map<string, string>;
}

export interface RunnerResourcesOptions {
  /** Private runner directory, outside every model-tool handle. */
  workspaceDir: string;
  acquire?: (domains: string[]) => Promise<SandboxHandle>;
  enforce?: (raw: SandboxHandle, policy: ValidatedSandboxWritePolicy) => Promise<SandboxHandle>;
}

/** Credential-free resource staging and immutable change checkpoints for one session. */
export class RunnerResources {
  private sessionId: string | undefined;
  private incoming: Incoming | undefined;
  private active: Active | undefined;
  private frozen: Frozen | undefined;
  private queuedTools = 0;
  private failed = false;
  private skillsFailed = false;
  private queue: Promise<unknown> = Promise.resolve();
  private ackQueue: Promise<void> = Promise.resolve();
  private closed = false;
  private ack: { resolve(): void; reject(error: Error): void } | undefined;
  private lastAck: { id: string; digest: string } | undefined;

  constructor(private readonly options: RunnerResourcesOptions) {}

  /** No unfinished delivery or failed checkpoint can become a new provider's tool surface. */
  get ready(): boolean {
    return this.readyForSnapshot && !this.skillsFailed;
  }

  /** A new turn needs its full ten-minute execution window; in-flight tools only need a live grant. */
  get readyForTurn(): boolean {
    return this.ready && this.active!.gitExpiresAt > Date.now() / 1000 + 600;
  }

  /** A failed Skill delivery can be repaired by the next snapshot. */
  get readyForSnapshot(): boolean {
    return (
      this.active !== undefined &&
      this.active.gitExpiresAt > Date.now() / 1000 &&
      !this.incoming &&
      !this.frozen &&
      !this.failed &&
      !this.closed
    );
  }

  get revision(): string | undefined {
    return this.active?.manifest.revision;
  }

  get toolSandbox(): SandboxHandle | undefined {
    return this.active?.tools;
  }

  get pendingCheckpoint(): ResourceCheckpoint | undefined {
    return this.frozen === undefined ? undefined : structuredClone(this.frozen.checkpoint);
  }

  /** The caller's authenticated tunnel header pins all subsequent requests. */
  private bind(sessionId: string): void {
    if (this.closed) throw new Error('resource controller stopped');
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(sessionId)) throw new Error('invalid resource session');
    if (this.sessionId !== undefined && this.sessionId !== sessionId)
      throw new Error('resource session mismatch');
    this.sessionId = sessionId;
  }

  private serialize<T>(work: () => Promise<T>): Promise<T> {
    const pending = this.queue.then(() => {
      if (this.closed) throw new Error('resource controller stopped');
      return work();
    });
    this.queue = pending.catch(() => {});
    return pending;
  }

  async push(sessionId: string, body: unknown): Promise<{ committed: boolean; revision?: string }> {
    this.bind(sessionId);
    // Reject a concurrent reconfiguration rather than queueing it behind an ACK
    // whose Registry caller is itself waiting on resource delivery.
    if (this.queuedTools > 0) throw new Error('cannot configure resources during a tool operation');
    return this.serialize(async () => {
      const message = record(body);
      if (message.type === 'status')
        return this.active
          ? { committed: true, revision: this.active.manifest.revision }
          : { committed: false };
      if (message.type === 'manifest') return this.begin(message);
      if (message.type === 'git_capabilities') return this.replaceGitCapabilities(message);
      if (message.type !== 'file_chunk' && message.type !== 'commit')
        throw new Error('unknown resource operation');
      if (typeof message.revision !== 'string') throw new Error('missing resource revision');
      if (!this.incoming && this.active?.manifest.revision === message.revision)
        return { committed: true };
      const incoming = this.incoming;
      if (!incoming || message.revision !== incoming.manifest.revision)
        throw new Error('resource revision is not staged');
      if (message.type === 'file_chunk') {
        const file = incoming.files.get(fileKey(message.resource_id, message.path));
        if (!file) throw new Error('undeclared resource file');
        const bytes = decodeResourceChunk(message.content_base64);
        const offset = message.offset;
        if (
          typeof offset !== 'number' ||
          !Number.isSafeInteger(offset) ||
          offset < 0 ||
          offset > file.received ||
          offset + bytes.length > file.descriptor.size_bytes
        )
          throw new Error('invalid resource chunk offset');
        const handle = await open(file.local, constants.O_RDWR | constants.O_NOFOLLOW);
        try {
          if (offset < file.received) {
            if (offset + bytes.length > file.received)
              throw new Error('overlapping resource chunk');
            const existing = Buffer.alloc(bytes.length);
            const { bytesRead } = await handle.read(existing, 0, existing.length, offset);
            if (bytesRead !== bytes.length || !existing.equals(bytes))
              throw new Error('resource chunk replay differs');
          } else {
            await writeAll(handle, bytes, offset);
            file.received += bytes.length;
          }
        } finally {
          await handle.close();
        }
        return { committed: false };
      }
      if (message.manifest_sha256 !== resourceManifestDigest(incoming.manifest))
        throw new Error('resource manifest digest mismatch');
      await this.commit(incoming);
      return { committed: true };
    });
  }

  private async begin(message: Record<string, unknown>): Promise<{ committed: boolean }> {
    const manifest = parseResourceManifest(message.manifest);
    const retainedGitIds = message.retained_git_resource_ids ?? [];
    if (!Array.isArray(retainedGitIds) || retainedGitIds.some((id) => typeof id !== 'string'))
      throw new Error('invalid retained Git resources');
    // Retention pins binding authority, not original file hashes: a fresh Registry
    // generation may have prepared newer upstream descriptors while this live
    // checkout (including local edits) was deliberately retained. The signature
    // below still verifies every mount, access grant and network domain.
    for (const id of retainedGitIds) {
      const resource = manifest.resources.find((item) => item.resource_id === id);
      const activeResource = this.active?.manifest.resources.find(
        (item) => item.resource_id === id,
      );
      if (
        this.active?.manifest.revision !== manifest.revision ||
        resource?.kind !== 'github_repository' ||
        activeResource?.kind !== 'github_repository'
      )
        throw new Error('retained Git resource is not active');
    }
    const domains = message.network_allowed_domains ?? [];
    if (!Array.isArray(domains) || domains.some((domain) => typeof domain !== 'string'))
      throw new Error('invalid resource network policy');
    if (
      message.network_unrestricted !== undefined &&
      typeof message.network_unrestricted !== 'boolean'
    )
      throw new Error('invalid resource network policy');
    const unrestricted = message.network_unrestricted === true;
    const policy = policyFor(manifest, domains as string[], unrestricted);
    const normalizedDomains = [...(policy.networkAllowedDomains ?? [])];
    const signature = hashJson({
      resources: manifest.resources.map(({ files: _files, ...binding }) => binding),
      domains: normalizedDomains,
      unrestricted,
    });
    if (this.active?.manifest.revision === manifest.revision) {
      if (this.active.signature !== signature) throw new Error('resource binding revision differs');
      if (this.frozen) throw new Error('resource checkpoint requires acknowledgement');
      if (!this.memoryChanged(manifest)) {
        await this.removeIncoming();
        return { committed: true };
      }
      if (this.failed) throw new Error('failed resource checkpoint cannot be refreshed');
    }
    if (this.frozen) throw new Error('resource checkpoint requires acknowledgement');
    await this.removeIncoming();
    const directory = await this.privateDirectory('incoming-');
    const incoming: Incoming = {
      manifest,
      signature,
      domains: normalizedDomains,
      unrestricted,
      directory,
      files: new Map(),
    };
    this.incoming = incoming;
    for (const resource of manifest.resources) {
      if (retainedGitIds.includes(resource.resource_id)) continue;
      for (const descriptor of resource.files) {
        const local = join(directory, String(incoming.files.size));
        const handle = await open(local, 'wx', 0o600);
        await handle.close();
        incoming.files.set(fileKey(resource.resource_id, descriptor.path), {
          descriptor,
          local,
          received: 0,
        });
      }
    }
    return { committed: false };
  }

  private async commit(incoming: Incoming): Promise<void> {
    for (const file of incoming.files.values()) {
      if (
        file.received !== file.descriptor.size_bytes ||
        (await digestFile(file.local)).sha256 !== file.descriptor.sha256
      )
        throw new Error('resource file integrity mismatch');
    }
    if (this.active?.manifest.revision === incoming.manifest.revision) {
      await this.refreshMemory(incoming);
      return;
    }
    const raw = await (this.options.acquire?.(incoming.domains) ??
      createManagedToolSandboxRuntime({
        harnessWorkDir: join(this.options.workspaceDir, 'model-tools'),
        networkAllowedDomains: incoming.domains,
        networkUnrestricted: incoming.unrestricted,
      }).acquire({}));
    try {
      const rawRoot = (raw as SandboxHandle & { rootDir?(): string }).rootDir?.();
      if (!rawRoot) throw new Error('managed tool sandbox has no private root');
      const root = await realpath(rawRoot);
      const baseline = new Map<string, string>();
      for (const resource of incoming.manifest.resources) {
        if (resource.kind !== 'file')
          await mkdir(join(root, resource.mount_path), { recursive: true });
        for (const file of resource.files) {
          const target = join(root, resourceFilePath(resource, file));
          await mkdir(dirname(target), { recursive: true });
          await copyFile(
            incoming.files.get(fileKey(resource.resource_id, file.path))!.local,
            target,
            constants.COPYFILE_EXCL,
          );
          await chmod(target, file.mode & 0o777);
          if (resource.kind === 'memory_store' && resource.access === 'read_write')
            baseline.set(fileKey(resource.resource_id, file.path), file.sha256);
        }
      }
      await mkdir(join(root, '/mnt/session/outputs'), { recursive: true });
      await mkdir(join(root, '/workspace/skills'), { recursive: true });
      await mkdir(join(root, GIT_PROXY_AUTH_ROOT), { recursive: true });
      await chmod(join(root, GIT_PROXY_AUTH_ROOT), 0o555);
      const tools = await (this.options.enforce ?? enforceToolPolicy)(
        raw,
        policyFor(incoming.manifest, incoming.domains, incoming.unrestricted),
      );
      await this.removeIncoming();
      const previous = this.active;
      this.active = undefined;
      await destroyActive(previous);
      this.active = {
        manifest: incoming.manifest,
        signature: incoming.signature,
        root,
        raw,
        tools,
        baseline,
        domains: incoming.domains,
        unrestricted: incoming.unrestricted,
        gitExpiresAt: incoming.manifest.resources.some((r) => r.kind === 'github_repository')
          ? 0
          : Number.POSITIVE_INFINITY,
      };
      this.failed = false;
      this.skillsFailed = false;
    } catch (error) {
      await raw.destroy();
      throw error;
    }
  }

  private memoryChanged(manifest: ResourceManifest): boolean {
    const active = this.active!;
    return manifest.resources.some((resource) => {
      if (resource.kind !== 'memory_store') return false;
      const before =
        resource.access === 'read_write'
          ? [...active.baseline].flatMap(([key, sha]) => {
              const [id, path] = JSON.parse(key) as [string, string];
              return id === resource.resource_id ? [[path, sha]] : [];
            })
          : active.manifest.resources
              .find((r) => r.resource_id === resource.resource_id)!
              .files.map((file) => [file.path, file.sha256]);
      const after = resource.files.map((file) => [file.path, file.sha256]);
      return hashJson(before.sort()) !== hashJson(after.sort());
    });
  }

  /** Refresh shared Memory between turns without discarding outputs, Git or Skills. */
  private async refreshMemory(incoming: Incoming): Promise<void> {
    const active = this.active!;
    if (this.frozen || this.failed) throw new Error('resource checkpoint requires acknowledgement');
    // Never discard local changes that have not passed the persistence barrier.
    const scanBudget = { entries: 0 };
    for (const resource of active.manifest.resources) {
      if (resource.kind !== 'memory_store' || resource.access !== 'read_write') continue;
      const mount = join(active.root, resource.mount_path);
      if ((await realpath(mount)) !== mount) throw new Error('Memory mount is aliased');
      const observed = new Map<string, string>();
      for await (const path of walkFiles(mount, '', scanBudget)) {
        observed.set(
          fileKey(resource.resource_id, path),
          (await digestFile(join(active.root, resource.mount_path, path))).sha256,
        );
      }
      for (const [key, sha] of active.baseline) {
        if ((JSON.parse(key) as string[])[0] !== resource.resource_id) continue;
        if (observed.get(key) !== sha)
          throw new Error('unacknowledged Memory changes cannot be refreshed');
        observed.delete(key);
      }
      if (observed.size) throw new Error('unacknowledged Memory changes cannot be refreshed');
    }
    const staging = await mkdtemp(join(active.root, '.memory-refresh-'));
    const replaced: Array<{ mount: string; backup: string }> = [];
    let rollbackFailed = false;
    try {
      for (const [index, resource] of incoming.manifest.resources.entries()) {
        if (resource.kind !== 'memory_store') continue;
        const mount = join(active.root, resource.mount_path);
        if ((await realpath(mount)) !== mount || !(await lstat(mount)).isDirectory())
          throw new Error('Memory mount is aliased');
        const next = join(staging, `next-${index}`);
        await mkdir(next);
        for (const file of resource.files) {
          const target = join(next, file.path);
          await mkdir(dirname(target), { recursive: true });
          await copyFile(
            incoming.files.get(fileKey(resource.resource_id, file.path))!.local,
            target,
            constants.COPYFILE_EXCL,
          );
          await chmod(target, file.mode & 0o777);
        }
        const backup = join(staging, `previous-${index}`);
        await rename(mount, backup);
        replaced.push({ mount, backup });
        await rename(next, mount);
      }
      const nextBaseline = new Map(
        [...active.baseline].filter(
          ([key]) => (JSON.parse(key) as string[])[0] === OUTPUT_RESOURCE_ID,
        ),
      );
      for (const resource of incoming.manifest.resources) {
        if (resource.kind !== 'memory_store' || resource.access !== 'read_write') continue;
        for (const file of resource.files)
          nextBaseline.set(fileKey(resource.resource_id, file.path), file.sha256);
      }
      active.baseline = nextBaseline;
      active.manifest = incoming.manifest;
    } catch (error) {
      try {
        for (const { mount, backup } of replaced.reverse()) {
          await rm(mount, { recursive: true, force: true });
          await rename(backup, mount);
        }
      } catch (rollbackError) {
        rollbackFailed = true;
        this.failed = true;
        throw new Error('Memory refresh rollback failed', { cause: rollbackError });
      }
      throw error;
    } finally {
      // Preserve backup bytes if a filesystem failure prevented rollback.
      if (!rollbackFailed) await rm(staging, { recursive: true, force: true });
    }
    await this.removeIncoming();
  }

  private async replaceGitCapabilities(
    message: Record<string, unknown>,
  ): Promise<{ committed: boolean }> {
    const active = this.active;
    if (!active || this.incoming || this.frozen || message.revision !== active.manifest.revision)
      throw new Error('Git capability resource revision is not active');
    // Once refresh starts for this binding, even a rejected replacement must
    // block local execution until a complete valid grant set is acknowledged.
    active.gitExpiresAt = 0;
    const expected = new Set(
      active.manifest.resources
        .filter((resource) => resource.kind === 'github_repository')
        .map((resource) => resource.resource_id),
    );
    if (!Array.isArray(message.capabilities) || message.capabilities.length !== expected.size)
      throw new Error('Git capability bindings differ');
    const seen = new Set<string>();
    const capabilities = message.capabilities.map((value): GitProxyCapability => {
      const raw = record(value);
      const id = raw.resource_id;
      if (typeof id !== 'string' || !expected.has(id) || seen.has(id))
        throw new Error('Git capability bindings differ');
      seen.add(id);
      if (
        typeof raw.remote_url !== 'string' ||
        raw.remote_url.length > 4096 ||
        /[\s"\\]/.test(raw.remote_url)
      )
        throw new Error('invalid Git proxy URL');
      const url = new URL(raw.remote_url);
      if (
        !['http:', 'https:'].includes(url.protocol) ||
        url.username ||
        url.password ||
        url.search ||
        url.hash ||
        url.pathname !== `/v1/git-proxy/${id}` ||
        url.href !== raw.remote_url
      )
        throw new Error('invalid Git proxy URL');
      if (!active.unrestricted && !active.domains.includes(url.hostname))
        throw new Error('Git proxy host is not allowed');
      if (
        typeof raw.authorization_header !== 'string' ||
        raw.authorization_header.length > 16_384 ||
        !/^Authorization: Bearer [A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(
          raw.authorization_header,
        )
      )
        throw new Error('invalid Git proxy authorization');
      if (
        typeof raw.expires_at !== 'number' ||
        !Number.isSafeInteger(raw.expires_at) ||
        raw.expires_at <= Date.now() / 1000 + 600 ||
        raw.expires_at > Date.now() / 1000 + 3600
      )
        throw new Error('invalid Git proxy expiration');
      return {
        resource_id: id,
        remote_url: raw.remote_url,
        authorization_header: raw.authorization_header,
        expires_at: raw.expires_at,
      };
    });
    // All bindings are validated before changing any file. Only this trusted path
    // can write the directory; model tools see it as a read-only policy root.
    const root = join(active.root, GIT_PROXY_AUTH_ROOT);
    if ((await realpath(root)) !== root || !(await lstat(root)).isDirectory())
      throw new Error('Git capability root is aliased');
    await chmod(root, 0o700);
    try {
      for (const capability of capabilities) {
        const temporary = join(root, `${capability.resource_id}.${randomUUID()}`);
        try {
          await writeFile(
            temporary,
            [
              `[http "${capability.remote_url}"]`,
              `\textraHeader = "${capability.authorization_header}"`,
              '\tfollowRedirects = false',
              '',
            ].join('\n'),
            { flag: 'wx', mode: 0o444 },
          );
          await rename(temporary, join(root, `${capability.resource_id}.config`));
        } finally {
          await rm(temporary, { force: true });
        }
      }
    } finally {
      await chmod(root, 0o555);
    }
    active.gitExpiresAt = Math.min(...capabilities.map((capability) => capability.expires_at));
    return { committed: true };
  }

  /** Replace verified Skill files through the trusted host while all model tools are idle. */
  async replaceSkills(sessionId: string, push: ParsedSkillsPush): Promise<void> {
    this.bind(sessionId);
    if (this.queuedTools > 0) throw new Error('cannot configure Skills during a tool operation');
    return this.serialize(async () => {
      const active = this.active;
      if (!active || this.frozen) throw new Error('resources are not ready for Skills');
      const workspace = join(active.root, 'workspace');
      if ((await realpath(workspace)) !== workspace || !(await lstat(workspace)).isDirectory())
        throw new Error('Skill workspace is aliased');
      const root = join(workspace, 'skills');
      if ((await realpath(root)) !== root || !(await lstat(root)).isDirectory())
        throw new Error('Skill root is aliased');
      const staged = await mkdtemp(join(workspace, '.skills-'));
      try {
        for (const file of push.files) {
          const target = join(staged, file.skill, file.path);
          await mkdir(dirname(target), { recursive: true });
          await writeFile(target, file.content, { flag: 'wx', mode: 0o600 });
        }
        const modes = push.skills.flatMap((name) =>
          planSkillBundleChmod(
            join(staged, name),
            push.files
              .filter((file) => file.skill === name)
              .map(({ path, mode }) => ({ path, mode })),
          ),
        );
        modes.sort((a, b) => b.path.split('/').length - a.path.split('/').length);
        for (const entry of modes) await chmod(entry.path, entry.mode);
        await chmod(staged, 0o555);
        const backup = join(workspace, `.skills-backup-${randomUUID()}`);
        await rename(root, backup);
        try {
          await rename(staged, root);
        } catch (error) {
          await rename(backup, root);
          throw error;
        }
        await removeSkillsTree(backup);
        this.skillsFailed = false;
      } catch (error) {
        this.skillsFailed = true;
        await removeSkillsTree(staged);
        throw error;
      }
    });
  }

  /** Serialize mutation, freeze changed bytes, then wait for Registry persistence. */
  async runTool<T>(
    operation: () => Promise<T>,
    publish: (checkpoint: ResourceCheckpoint, digest: string) => void,
    signal: AbortSignal,
  ): Promise<T> {
    this.queuedTools++;
    return this.serialize(async () => {
      if (!this.ready) throw new Error('resources are not ready for another tool');
      if (signal.aborted) throw new Error('resource operation aborted');
      let resourcesCommitted = false;
      try {
        let result: T | undefined;
        let operationError: unknown;
        let operationFailed = false;
        try {
          result = await operation();
        } catch (error) {
          operationFailed = true;
          operationError = error;
        }
        // A failed tool may still have written files before failing.
        if (this.closed) throw new Error('resource controller stopped');
        const frozen = await this.freeze();
        if (this.closed) throw new Error('resource controller stopped');
        if (frozen && this.frozen) {
          await new Promise<void>((resolve, reject) => {
            const aborted = (): void =>
              settle(new Error('resource checkpoint acknowledgement interrupted'));
            const settle = (error?: Error): void => {
              signal.removeEventListener('abort', aborted);
              this.ack = undefined;
              if (error) reject(error);
              else resolve();
            };
            this.ack = { resolve: () => settle(), reject: (error) => settle(error) };
            signal.addEventListener('abort', aborted, { once: true });
            if (signal.aborted) {
              aborted();
              return;
            }
            try {
              publish(structuredClone(frozen.checkpoint), frozen.digest);
            } catch (error) {
              settle(
                error instanceof Error ? error : new Error('resource checkpoint publish failed'),
              );
            }
          });
        }
        resourcesCommitted = true;
        if (operationFailed) throw operationError;
        return result as T;
      } catch (error) {
        // Incomplete freezing/accounting cannot become successful on a later turn.
        if (!resourcesCommitted) this.failed = true;
        throw error;
      }
    }).finally(() => {
      this.queuedTools--;
    });
  }

  private async freeze(): Promise<Frozen | undefined> {
    const active = this.active!;
    const nextBaseline = new Map<string, string>();
    const files: ChangedResourceFile[] = [];
    const localFiles = new Map<string, string>();
    const directory = await this.privateDirectory('checkpoint-');
    let bytes = 0;
    let count = 0;
    const scanBudget = { entries: 0 };
    try {
      const roots = [
        { resource_id: OUTPUT_RESOURCE_ID, mount_path: '/mnt/session/outputs' },
        ...active.manifest.resources.filter(
          (resource) => resource.kind === 'memory_store' && resource.access === 'read_write',
        ),
      ];
      for (const resource of roots) {
        const mount = join(active.root, resource.mount_path);
        for await (const relative of walkFiles(mount, '', scanBudget)) {
          const key = fileKey(resource.resource_id, relative);
          const source = join(mount, relative);
          const frozenPath = join(directory, String(count++));
          if (count > RESOURCE_MAX_FILES) throw new Error('resource checkpoint exceeds file limit');
          const descriptor = await digestFile(source, frozenPath);
          bytes += descriptor.size_bytes;
          if (bytes > RESOURCE_MAX_TOTAL_BYTES)
            throw new Error('resource checkpoint exceeds byte limit');
          nextBaseline.set(key, descriptor.sha256);
          const previous = active.baseline.get(key);
          if (previous === descriptor.sha256) {
            await rm(frozenPath);
            continue;
          }
          files.push({
            resource_id: resource.resource_id,
            path: relative,
            ...descriptor,
            ...(previous === undefined ? {} : { previous_sha256: previous }),
          });
          localFiles.set(key, frozenPath);
        }
      }
      const deleted: DeletedResourceFile[] = [];
      for (const [key, previous] of active.baseline) {
        const [resource_id, path] = JSON.parse(key) as [string, string];
        if (resource_id !== OUTPUT_RESOURCE_ID && !nextBaseline.has(key))
          deleted.push({ resource_id, path, previous_sha256: previous });
      }
      if (files.length === 0 && deleted.length === 0) {
        active.baseline = nextBaseline;
        await rm(directory, { recursive: true });
        return undefined;
      }
      const checkpoint: ResourceCheckpoint = {
        version: 1,
        checkpoint_id: `rchk_${randomUUID()}`,
        revision: active.manifest.revision,
        files,
        deleted,
      };
      this.frozen = {
        checkpoint,
        digest: resourceCheckpointDigest(checkpoint),
        directory,
        files: localFiles,
        nextBaseline,
      };
      return this.frozen;
    } catch (error) {
      await rm(directory, { recursive: true });
      throw error;
    }
  }

  async changes(sessionId: string, body: unknown): Promise<unknown> {
    this.bind(sessionId);
    const message = record(body);
    const frozen = this.frozen;
    if (message.type === 'pending')
      return frozen
        ? { checkpoint: structuredClone(frozen.checkpoint), manifest_sha256: frozen.digest }
        : { checkpoint: null };
    if (!frozen || message.checkpoint_id !== frozen.checkpoint.checkpoint_id)
      throw new Error('resource checkpoint is unavailable');
    if (message.type === 'manifest')
      return { ...structuredClone(frozen.checkpoint), manifest_sha256: frozen.digest };
    if (message.type !== 'file_chunk') throw new Error('unknown checkpoint operation');
    const key = fileKey(message.resource_id, message.path);
    const file = frozen.files.get(key);
    const descriptor = frozen.checkpoint.files.find(
      (entry) => fileKey(entry.resource_id, entry.path) === key,
    );
    const offset = message.offset;
    if (
      !file ||
      !descriptor ||
      typeof offset !== 'number' ||
      !Number.isSafeInteger(offset) ||
      offset < 0 ||
      offset >= descriptor.size_bytes
    )
      throw new Error('invalid checkpoint chunk');
    const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const buffer = Buffer.alloc(Math.min(RESOURCE_CHUNK_BYTES, descriptor.size_bytes - offset));
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, offset);
      if (bytesRead !== buffer.length) throw new Error('checkpoint bytes truncated');
      return { content_base64: buffer.toString('base64'), offset };
    } finally {
      await handle.close();
    }
  }

  /** Runs outside the serialized tool queue: the parked tool is waiting on this ACK. */
  async acknowledge(sessionId: string, body: unknown): Promise<void> {
    this.bind(sessionId);
    const pending = this.ackQueue.then(() => this.applyAcknowledgement(body));
    this.ackQueue = pending.catch(() => {});
    await pending;
  }

  private async applyAcknowledgement(body: unknown): Promise<void> {
    if (this.closed) throw new Error('resource controller stopped');
    const message = record(body);
    if (
      this.lastAck !== undefined &&
      message.checkpoint_id === this.lastAck.id &&
      message.manifest_sha256 === this.lastAck.digest
    )
      return;
    const frozen = this.frozen;
    if (
      !frozen ||
      message.checkpoint_id !== frozen.checkpoint.checkpoint_id ||
      message.manifest_sha256 !== frozen.digest
    )
      throw new Error('resource checkpoint acknowledgement differs');
    await rm(frozen.directory, { recursive: true, force: true });
    this.active!.baseline = frozen.nextBaseline;
    this.lastAck = { id: frozen.checkpoint.checkpoint_id, digest: frozen.digest };
    this.frozen = undefined;
    this.failed = false;
    this.ack?.resolve();
  }

  async close(): Promise<void> {
    this.closed = true;
    this.ack?.reject(new Error('resource controller stopped'));
    await this.queue;
    await this.ackQueue;
    await this.removeIncoming();
    if (this.frozen) await rm(this.frozen.directory, { recursive: true });
    this.frozen = undefined;
    await destroyActive(this.active);
    this.active = undefined;
  }

  private async privateDirectory(prefix: string): Promise<string> {
    const root = join(this.options.workspaceDir, 'private-resources');
    await mkdir(root, { recursive: true, mode: 0o700 });
    if (!(await lstat(root)).isDirectory())
      throw new Error('resource staging directory is not private');
    return realpath(await mkdtemp(join(root, prefix)));
  }

  private async removeIncoming(): Promise<void> {
    if (this.incoming) await rm(this.incoming.directory, { recursive: true });
    this.incoming = undefined;
  }
}

function policyFor(manifest: ResourceManifest, domains: string[], unrestricted = false) {
  return buildSandboxWritePolicy(
    manifest.resources.map((resource) => ({
      path: resource.mount_path,
      kind: resource.kind,
      access: resource.access,
    })) as Parameters<typeof buildSandboxWritePolicy>[0],
    {
      networkAllowedDomains: domains,
      networkUnrestricted: unrestricted,
      includeSkillsRoot: true,
      includeGitProxyRoot: true,
    },
  );
}
async function enforceToolPolicy(
  raw: SandboxHandle,
  policy: ValidatedSandboxWritePolicy,
): Promise<SandboxHandle> {
  if (!hasWritePolicyEnforcement(raw))
    throw new Error('managed tool sandbox cannot enforce resource policy');
  return createPolicyEnforcedSandbox(raw, policy);
}
function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('resource message must be an object');
  return value as Record<string, unknown>;
}
function fileKey(resource: unknown, path: unknown): string {
  if (typeof resource !== 'string' || typeof path !== 'string')
    throw new Error('invalid resource file identity');
  return JSON.stringify([resource, path]);
}
function hashJson(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
async function writeAll(
  handle: Awaited<ReturnType<typeof open>>,
  buffer: Buffer,
  offset: number,
): Promise<void> {
  let written = 0;
  while (written < buffer.length) {
    const { bytesWritten } = await handle.write(
      buffer,
      written,
      buffer.length - written,
      offset + written,
    );
    if (bytesWritten === 0) throw new Error('resource file write stalled');
    written += bytesWritten;
  }
}
async function digestFile(
  file: string,
  copyTo?: string,
): Promise<Omit<ResourceFileDescriptor, 'path'>> {
  const input = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  let output: Awaited<ReturnType<typeof open>> | undefined;
  try {
    const before = await input.stat();
    if ((await resolveOpenedDescriptorPath(input.fd, file, before)) !== file)
      throw new Error('resource file path is aliased');
    if (!before.isFile() || before.nlink !== 1 || before.size > RESOURCE_MAX_FILE_BYTES)
      throw new Error('resource file is not a bounded regular file');
    if (copyTo !== undefined) output = await open(copyTo, 'wx', 0o600);
    const hash = createHash('sha256');
    const buffer = Buffer.alloc(RESOURCE_CHUNK_BYTES);
    let offset = 0;
    while (offset < before.size) {
      const { bytesRead } = await input.read(
        buffer,
        0,
        Math.min(buffer.length, before.size - offset),
        offset,
      );
      if (bytesRead === 0) throw new Error('resource file changed while freezing');
      const chunk = buffer.subarray(0, bytesRead);
      hash.update(chunk);
      if (output) await writeAll(output, chunk, offset);
      offset += bytesRead;
    }
    const after = await input.stat();
    if (
      before.size !== after.size ||
      before.mtimeMs !== after.mtimeMs ||
      before.ctimeMs !== after.ctimeMs
    )
      throw new Error('resource file changed while freezing');
    return { sha256: hash.digest('hex'), size_bytes: offset, mode: before.mode & 0o777 };
  } finally {
    await input.close();
    await output?.close();
  }
}
async function* walkFiles(
  root: string,
  relative: string,
  budget: { entries: number },
): AsyncGenerator<string> {
  const stat = await lstat(join(root, relative));
  if (!stat.isDirectory() || stat.isSymbolicLink())
    throw new Error('resource directory is aliased');
  for await (const entry of await opendir(join(root, relative))) {
    if (++budget.entries > RESOURCE_MAX_FILES * 10)
      throw new Error('resource checkpoint exceeds directory scan limit');
    const path = relative ? `${relative}/${entry.name}` : entry.name;
    if (
      Buffer.from(path).toString() !== path ||
      path.length > 4096 ||
      posix.normalize(path) !== path ||
      [...path].some(
        (char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127 || char === '\\',
      )
    )
      throw new Error('resource checkpoint path is not canonical');
    if (entry.isDirectory()) yield* walkFiles(root, path, budget);
    else if (entry.isFile()) yield path;
    else throw new Error('resource checkpoint contains a link or device');
  }
}

/** Unlink unexpected aliases instead of following them during trusted cleanup. */
async function removeSkillsTree(path: string): Promise<void> {
  let stat;
  try {
    stat = await lstat(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
    throw error;
  }
  if (stat.isDirectory() && !stat.isSymbolicLink()) {
    await chmod(path, 0o700);
    for await (const entry of await opendir(path)) await removeSkillsTree(join(path, entry.name));
  }
  await rm(path, { recursive: true, force: true });
}

/** Release hardened Skill directories before runtimes remove their private root. */
async function destroyActive(active: Active | undefined): Promise<void> {
  if (!active) return;
  // workspace is a direct child of the canonical private root. The cleanup
  // unlinks aliases and never follows them into another directory.
  await removeSkillsTree(join(active.root, 'workspace'));
  await removeSkillsTree(join(active.root, '.orca'));
  await active.raw.destroy();
}
