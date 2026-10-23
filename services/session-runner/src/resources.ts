// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { ParsedSkillsPush } from './skills-materialize.js';
import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import {
  appendFile,
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
  readFile,
  readdir,
} from 'node:fs/promises';
import { basename, dirname, join, posix } from 'node:path';
import {
  NATIVE_USAGE_SOURCE,
  parseNativeCompletion,
  nativeCompletionIsComplete,
  type NativeCompletion,
} from './sandbox/seam.js';
import {
  buildSandboxWritePolicy,
  planSkillBundleChmod,
  createPolicyEnforcedSandbox,
  createManagedToolSandboxRuntime,
  decodeResourceChunk,
  parseResourceManifest,
  parseResourceCheckpoint,
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
  placementId?: string;
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
  sessionSandbox?: import('./sandbox/seam.js').SandboxRuntime;
  /** Private runner directory, outside every model-tool handle. */
  workspaceDir: string;
  acquire?: (domains: string[]) => Promise<SandboxHandle>;
  enforce?: (raw: SandboxHandle, policy: ValidatedSandboxWritePolicy) => Promise<SandboxHandle>;
}

/** Credential-free resource staging and immutable change checkpoints for one session. */
export class RunnerResources {
  readonly executionId = randomUUID();
  private nativePlacement = false;
  private nativeTurn:
    | {
        turnId: string;
        phase: 'running' | 'frozen' | 'prepared' | 'completed' | 'incomplete';
        completion?: NativeCompletion;
        checkpointId?: string;
        digest?: string;
      }
    | undefined;
  private recoveredNativeCompletion: NativeCompletion | undefined;
  private nativeStarted = false;
  private nativeJournalBytes = 0;
  private nativeJournalEntries = 0;
  private nativeJournalEvents = 0;
  private checkedNativeRecovery = false;
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
    return (
      this.ready &&
      (!this.nativeTurn || this.nativeTurn.phase === 'completed') &&
      this.active!.gitExpiresAt > Date.now() / 1000 + 600
    );
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

  /** Only the authorized mount extension advertises executable host paths. */
  projectNativePublicValue(value: unknown): unknown {
    const root = this.active?.root;
    if (!root) throw new Error('Native public projection has no placement');
    if (typeof value === 'string')
      return value.replaceAll(root + '/', '/').replaceAll(root, '[session sandbox]');
    if (Array.isArray(value)) return value.map((item) => this.projectNativePublicValue(item));
    if (value && typeof value === 'object')
      return Object.fromEntries(
        Object.entries(value).map(([key, item]) => [key, this.projectNativePublicValue(item)]),
      );
    return value;
  }
  get placement():
    | {
        id: string;
        sandbox: SandboxHandle;
        policy: ValidatedSandboxWritePolicy;
        resolvedPaths: Record<string, string>;
        guidance: string;
        outputs: string;
      }
    | undefined {
    const active = this.active;
    if (!active?.placementId) return undefined;
    return {
      id: active.placementId,
      sandbox: active.raw,
      policy: policyFor(active.manifest, [], false),
      resolvedPaths: Object.fromEntries(
        active.manifest.resources.map((r) => [r.resource_id, join(active.root, r.mount_path)]),
      ),
      guidance: active.manifest.resources
        .map((r) => r.mount_path + ' => ' + join(active.root, r.mount_path))
        .join('\n'),
      outputs: join(active.root, '/mnt/session/outputs'),
    };
  }

  async beginNativeTurn(turnId: string): Promise<void> {
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(turnId))
      throw new Error('Invalid accepted native turn identity');
    if (
      !this.placement ||
      this.frozen ||
      (this.nativeTurn && this.nativeTurn.phase !== 'completed')
    )
      throw new Error('Native completion blocks next turn');
    if (this.nativeTurn?.phase === 'completed') {
      await rm(
        join(this.options.workspaceDir, 'private-native-turn', this.nativeTurn.turnId + '.json'),
        { force: true },
      );
    }
    this.nativeTurn = { turnId, phase: 'running' };
    this.nativeJournalBytes = this.nativeJournalEntries = this.nativeJournalEvents = 0;
    this.nativeStarted = true;
    const root = join(this.options.workspaceDir, 'private-native-turn');
    await mkdir(root, { recursive: true, mode: 0o700 });
    await writeFile(
      join(root, turnId + '.json'),
      JSON.stringify({
        version: 1,
        turnId,
        executionId: this.executionId,
        placementId: this.placement!.id,
        revision: this.revision,
        root: this.active!.root,
        phase: 'running',
      }),
      { flag: 'wx', mode: 0o600 },
    );
  }

  /** Append deltas before forwarding; the byte cap includes JSON framing and report metadata. */
  async retainNativeReports(value: {
    turnId?: string;
    expected?: string[];
    reports?: NativeCompletion['reports'];
    events?: Array<Record<string, unknown>>;
    closed?: boolean;
    outcome?: NativeCompletion['outcome'];
  }): Promise<void> {
    if (!this.nativeTurn || !this.nativeStarted)
      throw new Error('Native report log has no accepted turn');
    const path = join(
      this.options.workspaceDir,
      'private-native-turn',
      this.nativeTurn.turnId + '.reports.ndjson',
    );
    const serialized = JSON.stringify({ ...value, turnId: this.nativeTurn.turnId }) + '\n';
    const bytes = Buffer.byteLength(serialized);
    const events = value.events?.length ?? 0;
    if (
      this.nativeJournalBytes + bytes > 8 * 1024 * 1024 ||
      this.nativeJournalEntries >= 13001 ||
      this.nativeJournalEvents + events > 10000
    )
      throw new Error('Native report log exceeds limit');
    await appendFile(path, serialized, { mode: 0o600 });
    this.nativeJournalBytes += bytes;
    this.nativeJournalEntries++;
    this.nativeJournalEvents += events;
  }

  freezeNativeTurn(
    completion: NativeCompletion,
  ): Promise<{ checkpoint: ResourceCheckpoint; digest: string }> {
    return this.serialize(async () => {
      if (
        !this.placement ||
        this.nativeTurn?.turnId !== completion.turn_id ||
        this.nativeTurn.phase !== 'running' ||
        completion.execution_id !== this.executionId ||
        completion.placement_id !== this.placement.id
      )
        throw new Error('Native turn identity differs');
      let frozen: Frozen | undefined;
      try {
        frozen = await this.freeze(parseNativeCompletion(completion));
      } catch (error) {
        this.nativeTurn.phase = 'incomplete';
        this.recoveredNativeCompletion = parseNativeCompletion({
          ...completion,
          events: undefined,
          report_set_sealed: false,
        });
        await this.writeNativeHeader({ phase: 'incomplete' });
        throw error;
      }
      if (!frozen) throw new Error('Native turn has no completion checkpoint');
      this.nativeTurn = {
        turnId: completion.turn_id,
        phase: 'frozen',
        completion,
        checkpointId: frozen.checkpoint.checkpoint_id,
        digest: frozen.digest,
      };
      return { checkpoint: structuredClone(frozen.checkpoint), digest: frozen.digest };
    });
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

  async push(
    sessionId: string,
    body: unknown,
  ): Promise<{ committed: boolean; revision?: string; [key: string]: unknown }> {
    this.bind(sessionId);
    await this.restoreNativeEvidence();
    // Reject a concurrent reconfiguration rather than queueing it behind an ACK
    // whose Registry caller is itself waiting on resource delivery.
    if (this.queuedTools > 0) throw new Error('cannot configure resources during a tool operation');
    return this.serialize(async () => {
      const message = record(body);
      if (message.type === 'status' && this.options.sessionSandbox) {
        if (this.nativeTurn?.phase === 'running' && this.frozen) this.nativeTurn.phase = 'frozen';
        const placement = this.placement;
        return {
          committed: this.active !== undefined,
          ...(this.revision ? { revision: this.revision } : {}),
          supported_placements: ['session_sandbox'],
          usage_source: NATIVE_USAGE_SOURCE,
          completion_version: 1,
          execution_id: this.executionId,
          ...(placement
            ? {
                placement_id: placement.id,
                resolved_paths: placement.resolvedPaths,
                outputs_directory: placement.outputs,
              }
            : {}),
          turn: this.nativeTurn
            ? {
                turn_id: this.nativeTurn.turnId,
                phase: this.nativeTurn.phase,
                checkpoint_id: this.nativeTurn.checkpointId,
                manifest_sha256: this.nativeTurn.digest,
              }
            : null,
        };
      }
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
    const native = message.placement === 'session_sandbox';
    if (message.placement !== undefined && !native)
      throw new Error('Unsupported resource placement');
    if (
      native &&
      (!this.options.sessionSandbox ||
        manifest.resources.some(
          (r) =>
            r.kind !== 'file' ||
            r.mount_path === '/.pi' ||
            r.mount_path.startsWith('/.pi/') ||
            r.mount_path === '/.pi-agent-home' ||
            r.mount_path.startsWith('/.pi-agent-home/'),
        ))
    )
      throw new Error('Native placement is unsupported or overlaps provider configuration');
    if (this.active && this.nativePlacement !== native)
      throw new Error('Cannot switch active resource semantics');
    if (this.nativeTurn && this.nativeTurn.phase !== 'completed')
      throw new Error('Native completion blocks refresh');
    this.nativePlacement = native;
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
    if (this.nativePlacement) {
      await this.commitNative(incoming);
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

  private async commitNative(incoming: Incoming): Promise<void> {
    const raw = await this.options.sessionSandbox!.acquire({});
    try {
      const rootValue = (raw as SandboxHandle & { rootDir?(): string }).rootDir?.();
      if (!rootValue || !raw.spawn)
        throw new Error('Native placement requires an executable Local sandbox');
      const root = await realpath(rootValue);
      for (const resource of incoming.manifest.resources) {
        for (const file of resource.files) {
          await raw.files.write(
            resourceFilePath(resource, file),
            await import('node:fs').then(({ createReadStream }) =>
              createReadStream(incoming.files.get(fileKey(resource.resource_id, file.path))!.local),
            ),
          );
          await raw.files.chmod(resourceFilePath(resource, file), 0o444);
        }
      }
      // Create via Files, then prove process writes are visible through that same API.
      const sentinel = '/mnt/session/outputs/.orca-placement-' + randomUUID();
      await raw.files.write(sentinel, Buffer.from('baseline'));
      const reads = incoming.manifest.resources.flatMap((r) =>
        r.files.map((f) => ({ path: join(root, resourceFilePath(r, f)), sha: f.sha256 })),
      );
      const probeConfig = '/.orca/native-placement-probe.json';
      await raw.files.write(
        probeConfig,
        Buffer.from(JSON.stringify({ reads, output: join(root, sentinel) })),
      );
      const source =
        'const fs=require("node:fs"),c=require("node:crypto");' +
        '(async()=>{const input=JSON.parse(fs.readFileSync(process.argv[1]));for(const f of input.reads){const h=c.createHash("sha256");for await(const bytes of fs.createReadStream(f.path))h.update(bytes);if(h.digest("hex")===f.sha){}else{throw Error("placement unreadable");}}fs.writeFileSync(input.output,"verified");})().catch(()=>{process.exitCode=1;});';
      const quote = (s: string) => "'" + s.replace(/'/g, "'\\''") + "'";
      const probe = await raw.run({
        tool: 'bash',
        args: {
          command:
            quote(process.execPath) + ' -e ' + quote(source) + ' ' + quote(join(root, probeConfig)),
          timeout_ms: 10000,
        },
      });
      if (probe.exit_code !== 0 || (await raw.files.read(sentinel)).toString() !== 'verified')
        throw new Error('Files-to-process placement verification failed');
      await raw.files.delete(sentinel);
      await raw.files.delete(probeConfig);
      await raw.files.write('/.orca/native-placement', Buffer.from('private'));
      const tools = await createPolicyEnforcedSandbox(
        raw as Parameters<typeof createPolicyEnforcedSandbox>[0],
        policyFor(incoming.manifest, [], false),
      );
      await this.removeIncoming();
      await destroyActive(this.active);
      this.active = {
        manifest: incoming.manifest,
        signature: incoming.signature,
        root,
        raw,
        tools,
        baseline: new Map(),
        domains: [],
        unrestricted: false,
        gitExpiresAt: Number.POSITIVE_INFINITY,
        placementId: randomUUID(),
      };
      this.failed = false;
    } catch (error) {
      await raw.destroy();
      throw error;
    }
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

  private async freeze(completion?: NativeCompletion): Promise<Frozen | undefined> {
    if (completion) return this.freezeNativeFiles(completion);
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
      if (!completion && files.length === 0 && deleted.length === 0) {
        active.baseline = nextBaseline;
        await rm(directory, { recursive: true });
        return undefined;
      }
      const checkpoint: ResourceCheckpoint = {
        version: 1,
        ...(completion ? { completion } : {}),
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

  private async freezeNativeFiles(completion: NativeCompletion): Promise<Frozen> {
    const active = this.active!;
    const directory = await this.privateDirectory('native-checkpoint-');
    const nextBaseline = new Map<string, string>();
    const localFiles = new Map<string, string>();
    const files: ChangedResourceFile[] = [];
    let total = 0,
      entries = 0,
      count = 0;
    const walk = async (relative: string): Promise<void> => {
      if (++entries > RESOURCE_MAX_FILES * 4 || relative.split('/').length > 128)
        throw new Error('Native output scan exceeds limits');
      const logical = posix.join('/mnt/session/outputs', relative);
      // Type checks refuse links/devices before the Files API can read a candidate.
      const stat = await lstat(join(active.root, logical));
      if (
        stat.isSymbolicLink() ||
        (!stat.isDirectory() && !stat.isFile()) ||
        (stat.isFile() && stat.nlink !== 1)
      )
        throw new Error('Native output is aliased');
      if (stat.isDirectory()) {
        for (const name of await active.raw.files.list(logical)) {
          if (name === '.' || name === '..' || name.includes('/') || name.includes('\\'))
            throw new Error('Invalid native output entry');
          await walk(posix.join(relative, name));
        }
        return;
      }
      if (
        ++count > RESOURCE_MAX_FILES ||
        stat.size > RESOURCE_MAX_FILE_BYTES ||
        total + stat.size > RESOURCE_MAX_TOTAL_BYTES
      )
        throw new Error('Native output transfer exceeds limits');
      const bytes = await active.raw.files.read(logical);
      if (bytes.length !== stat.size) throw new Error('Native output changed during capture');
      total += bytes.length;
      const sha256 = createHash('sha256').update(bytes).digest('hex');
      const key = fileKey(OUTPUT_RESOURCE_ID, relative);
      nextBaseline.set(key, sha256);
      if (active.baseline.get(key) === sha256) return;
      const local = join(directory, String(files.length));
      await writeFile(local, bytes, { flag: 'wx', mode: 0o600 });
      files.push({
        resource_id: OUTPUT_RESOURCE_ID,
        path: relative,
        sha256,
        size_bytes: bytes.length,
        mode: stat.mode & 0o777,
      });
      localFiles.set(key, local);
    };
    try {
      await walk('');
      const checkpoint: ResourceCheckpoint = {
        version: 1,
        checkpoint_id: 'rchk_' + randomUUID(),
        revision: active.manifest.revision,
        files,
        deleted: [],
        completion,
      };
      await writeFile(join(directory, 'checkpoint.json'), JSON.stringify(checkpoint), {
        flag: 'wx',
        mode: 0o600,
      });
      const digest = resourceCheckpointDigest(checkpoint);
      await this.writeNativeHeader({ phase: 'frozen', checkpoint, digest, directory });
      this.frozen = { checkpoint, digest, directory, files: localFiles, nextBaseline };
      return this.frozen;
    } catch (error) {
      if (this.nativeTurn) this.nativeTurn.phase = 'incomplete';
      await rm(directory, { recursive: true });
      throw error;
    }
  }

  async changes(sessionId: string, body: unknown): Promise<unknown> {
    this.bind(sessionId);
    await this.restoreNativeEvidence();
    const message = record(body);
    const frozen = this.frozen;
    if (message.type === 'pending')
      return frozen
        ? { checkpoint: structuredClone(frozen.checkpoint), manifest_sha256: frozen.digest }
        : {
            checkpoint: null,
            ...(this.recoveredNativeCompletion
              ? { incomplete_completion: this.recoveredNativeCompletion }
              : {}),
          };
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
    await this.restoreNativeEvidence();
    const pending = this.ackQueue.then(() => this.applyAcknowledgement(body));
    this.ackQueue = pending.catch(() => {});
    await pending;
  }

  private async applyAcknowledgement(body: unknown): Promise<void> {
    if (this.closed) throw new Error('resource controller stopped');
    const message = record(body);
    if (this.nativeTurn) {
      if (
        this.nativeTurn.checkpointId !== message.checkpoint_id ||
        this.nativeTurn.digest !== message.manifest_sha256
      )
        throw new Error('Native completion acknowledgement differs');
      if (message.completion_phase === 'completed') {
        if (this.nativeTurn.phase !== 'prepared' && this.nativeTurn.phase !== 'completed')
          throw new Error('Native completion was not prepared');
        await this.writeNativeHeader({ phase: 'completed' });
        await this.cleanupNativeEvidence();
        this.nativeTurn.phase = 'completed';
        delete this.nativeTurn.completion;
        return;
      }
      if (message.completion_phase !== 'prepared')
        throw new Error('Native completion receipt acknowledgement missing');
      if (this.nativeTurn.phase === 'prepared' || this.nativeTurn.phase === 'completed') return;
      if (!this.nativeTurn.completion || !nativeCompletionIsComplete(this.nativeTurn.completion))
        throw new Error('Incomplete native evidence cannot be acknowledged');
    }
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
    if (this.nativeTurn)
      await this.writeNativeHeader({
        phase: 'prepared',
        checkpoint: frozen.checkpoint,
        digest: frozen.digest,
      });
    await rm(frozen.directory, { recursive: true, force: true });
    if (this.active) this.active.baseline = frozen.nextBaseline;
    this.lastAck = { id: frozen.checkpoint.checkpoint_id, digest: frozen.digest };
    this.frozen = undefined;
    if (this.nativeTurn) this.nativeTurn.phase = 'prepared';
    this.failed = false;
    this.ack?.resolve();
  }

  async close(): Promise<void> {
    this.closed = true;
    this.ack?.reject(new Error('resource controller stopped'));
    await this.queue;
    await this.ackQueue;
    await this.removeIncoming();
    const retainedNative =
      this.nativePlacement &&
      this.nativeTurn !== undefined &&
      this.nativeTurn.phase !== 'completed';
    if (this.frozen && !retainedNative) await rm(this.frozen.directory, { recursive: true });
    this.frozen = undefined;
    if (!retainedNative) await destroyActive(this.active);
    this.active = undefined;
  }

  private async writeNativeHeader(update: Record<string, unknown>): Promise<void> {
    if (!this.nativeTurn) throw new Error('Native evidence has no turn');
    const path = join(
      this.options.workspaceDir,
      'private-native-turn',
      this.nativeTurn.turnId + '.json',
    );
    const previous = JSON.parse(await readFile(path, 'utf8')) as Record<string, unknown>;
    const temporary = path + '.' + randomUUID();
    await writeFile(temporary, JSON.stringify({ ...previous, ...update }), {
      flag: 'wx',
      mode: 0o600,
    });
    await rename(temporary, path);
  }

  /** Keep one small ACK tombstone until the next admitted turn, including across restarts. */
  private async cleanupNativeEvidence(): Promise<void> {
    const turn = this.nativeTurn!;
    const path = join(this.options.workspaceDir, 'private-native-turn', turn.turnId + '.json');
    const header = JSON.parse(await readFile(path, 'utf8')) as Record<string, unknown>;
    if (header.phase !== 'completed') throw new Error('Native cleanup requires completed evidence');
    if (typeof header.directory === 'string') {
      const root = await realpath(join(this.options.workspaceDir, 'private-resources'));
      if (
        dirname(header.directory) !== root ||
        !/^native-checkpoint-[A-Za-z0-9]+$/.test(header.directory.slice(root.length + 1))
      )
        throw new Error('Native cleanup directory escapes private storage');
      await rm(header.directory, { recursive: true, force: true });
    }
    if (typeof header.root === 'string' && this.active?.root !== header.root) {
      // A restarted runner has no live handle for the old, now durably completed placement.
      const workspace = await realpath(this.options.workspaceDir);
      if (
        !header.root.startsWith(workspace + '/') ||
        basename(dirname(header.root)) !== 'sessions' ||
        !/^sbx_local_[A-Za-z0-9_-]+$/.test(basename(header.root))
      )
        throw new Error('Native cleanup sandbox escapes its workspace');
      try {
        if ((await realpath(header.root)) !== header.root)
          throw new Error('Native cleanup sandbox is aliased');
        await rm(header.root, { recursive: true, force: true });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
    }
    for (const suffix of ['.reports.json', '.reports.ndjson'])
      await rm(join(this.options.workspaceDir, 'private-native-turn', turn.turnId + suffix), {
        force: true,
      });
    const evidenceDirectory = await opendir(dirname(path));
    let entries = 0;
    for await (const entry of evidenceDirectory) {
      if (++entries > RESOURCE_MAX_FILES) throw new Error('Native evidence cleanup exceeds limits');
      for (const prefix of [turn.turnId + '.json.', turn.turnId + '.reports.json.']) {
        if (
          entry.name.startsWith(prefix) &&
          /^[a-f0-9-]{36}$/.test(entry.name.slice(prefix.length))
        )
          await rm(join(dirname(path), entry.name), { force: true });
      }
    }
    const temporary = path + '.' + randomUUID();
    await writeFile(
      temporary,
      JSON.stringify({
        version: 1,
        turnId: turn.turnId,
        phase: 'completed',
        checkpointId: turn.checkpointId,
        digest: turn.digest,
      }),
      { flag: 'wx', mode: 0o600 },
    );
    await rename(temporary, path);
  }

  private async restoreNativeEvidence(): Promise<void> {
    if (this.checkedNativeRecovery) return;
    const directory = join(this.options.workspaceDir, 'private-native-turn');
    let names: string[];
    try {
      const type = await lstat(directory);
      if (!type.isDirectory() || type.isSymbolicLink())
        throw new Error('Native evidence directory is aliased');
      names = await readdir(directory);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        this.checkedNativeRecovery = true;
        return;
      }
      throw error;
    }
    const headers = names.filter((name) => /^[A-Za-z0-9_-]{1,128}\.json$/.test(name));
    if (headers.length > 1)
      throw new Error('Multiple unacknowledged native turns require operator recovery');
    if (!headers.length) {
      this.checkedNativeRecovery = true;
      return;
    }
    const path = join(directory, headers[0]!);
    const type = await lstat(path);
    if (!type.isFile() || type.size > 8 * 1024 * 1024)
      throw new Error('Native evidence file is not bounded');
    const header = JSON.parse(await readFile(path, 'utf8')) as Record<string, unknown>;
    if (
      header.version !== 1 ||
      typeof header.turnId !== 'string' ||
      headers[0] !== header.turnId + '.json'
    )
      throw new Error('Invalid native evidence identity');
    if (header.phase === 'completed') {
      const checkpointId =
        header.checkpointId ?? (header.checkpoint as ResourceCheckpoint | undefined)?.checkpoint_id;
      if (
        typeof checkpointId !== 'string' ||
        !/^[A-Za-z0-9_-]{1,128}$/.test(checkpointId) ||
        typeof header.digest !== 'string' ||
        !/^[a-f0-9]{64}$/.test(header.digest)
      )
        throw new Error('Invalid completed native evidence');
      this.nativeTurn = {
        turnId: header.turnId,
        phase: 'completed',
        checkpointId,
        digest: header.digest,
      };
      await this.cleanupNativeEvidence();
    } else if (header.phase === 'frozen' || header.phase === 'prepared') {
      const checkpoint = parseResourceCheckpoint(header.checkpoint);
      const completion = checkpoint.completion;
      if (
        !completion ||
        completion.turn_id !== header.turnId ||
        header.digest !== resourceCheckpointDigest(checkpoint)
      )
        throw new Error('Native retained evidence differs');
      this.nativePlacement = true;
      this.nativeStarted = true;
      this.nativeTurn = {
        turnId: header.turnId,
        phase: header.phase,
        completion,
        checkpointId: checkpoint.checkpoint_id,
        digest: header.digest as string,
      };
      if (header.phase === 'frozen') {
        if (typeof header.directory !== 'string')
          throw new Error('Native frozen bytes unavailable');
        const root = await realpath(join(this.options.workspaceDir, 'private-resources'));
        const frozenRoot = await realpath(header.directory);
        if (!frozenRoot.startsWith(root + '/native-checkpoint-'))
          throw new Error('Native checkpoint directory escapes private storage');
        const saved = parseResourceCheckpoint(
          JSON.parse(await readFile(join(frozenRoot, 'checkpoint.json'), 'utf8')),
        );
        if (resourceCheckpointDigest(saved) !== header.digest)
          throw new Error('Retained native checkpoint changed');
        this.frozen = {
          checkpoint,
          digest: header.digest as string,
          directory: frozenRoot,
          files: new Map(
            checkpoint.files.map((file, index) => [
              fileKey(file.resource_id, file.path),
              join(frozenRoot, String(index)),
            ]),
          ),
          nextBaseline: new Map(),
        };
      }
    } else {
      // Report/byte loss before freezing is not permission to replay an accepted model call.
      this.nativeTurn = { turnId: header.turnId, phase: 'incomplete' };
      try {
        const journal = names.includes(header.turnId + '.reports.ndjson');
        const reportPath = join(
          directory,
          header.turnId + (journal ? '.reports.ndjson' : '.reports.json'),
        );
        const reportType = await lstat(reportPath);
        if (
          !reportType.isFile() ||
          reportType.isSymbolicLink() ||
          reportType.size > 8 * 1024 * 1024
        )
          throw new Error('Invalid retained reports');
        const raw = await readFile(reportPath, 'utf8');
        let retained: Record<string, unknown>;
        if (journal) {
          // Recover only complete entries; a torn final append never seals the report set.
          const end = raw.lastIndexOf('\n');
          if (end < 0) throw new Error('Truncated native journal');
          const lines = raw.slice(0, end).split('\n');
          if (lines.length > 13001) throw new Error('Native journal entry limit exceeded');
          const expected = new Set<string>();
          const reports = new Map<string, NativeCompletion['reports'][number]>();
          const events: Array<Record<string, unknown>> = [];
          retained = { turnId: header.turnId, outcome: 'failed' };
          for (const line of lines) {
            const entry = JSON.parse(line) as {
              turnId: string;
              expected?: string[];
              reports?: NativeCompletion['reports'];
              events?: Array<Record<string, unknown>>;
              outcome?: string;
            };
            if (entry.turnId !== header.turnId) throw new Error('Retained turn identity differs');
            for (const id of entry.expected ?? []) expected.add(id);
            for (const report of entry.reports ?? []) {
              if (
                reports.has(report.id) &&
                JSON.stringify(reports.get(report.id)) !== JSON.stringify(report)
              )
                throw new Error('Retained report payload differs');
              reports.set(report.id, report);
            }
            events.push(...(entry.events ?? []));
            if (entry.outcome !== undefined) retained.outcome = entry.outcome;
          }
          Object.assign(retained, {
            expected: [...expected],
            reports: [...reports.values()],
            events,
          });
        } else retained = JSON.parse(raw) as Record<string, unknown>;
        if (retained.turnId !== header.turnId) throw new Error('Retained turn identity differs');
        this.recoveredNativeCompletion = parseNativeCompletion({
          version: 1,
          source: NATIVE_USAGE_SOURCE,
          turn_id: header.turnId,
          execution_id: header.executionId,
          placement_id: header.placementId,
          outcome: retained.outcome,
          report_set_sealed: false,
          expected_reports: retained.expected,
          reports: retained.reports,
          events: retained.events,
          error:
            'Native process lost before output freezing; accounting or artifacts may be incomplete',
        });
      } catch {
        // Keep malformed evidence private for operator recovery; it cannot authorize accounting.
        this.recoveredNativeCompletion = undefined;
      }
    }
    this.checkedNativeRecovery = true;
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
