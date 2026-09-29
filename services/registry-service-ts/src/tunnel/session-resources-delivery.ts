// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdtemp, open, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, posix } from 'node:path';
import type { FileRecord, FileStore } from '@orca/file-store';
import { MemoryConflictError, type MemoryStore } from '@orca/memory-store';
import {
  decodeResourceChunk,
  OUTPUT_RESOURCE_ID,
  parseResourceCheckpoint,
  parseResourceManifest,
  RESOURCE_CHUNK_BYTES,
  resourceCheckpointDigest,
  resourceManifestDigest,
  type ResourceCheckpoint,
  type ResourceFileDescriptor,
  type ResourceManifest,
  type GitProxyCapability,
} from '@orca/sandbox-runtime';
import type { Event, TranscriptStore } from '@orca/transcript-store';

// The authenticated runner tunnel is the only transport. No store identifiers or
// credentials are accepted from the runner; binding ids resolve through Registry.
export const RUNNER_RESOURCES_PATH = '/v1/runner/resources';
export const RUNNER_RESOURCE_CHANGES_PATH = '/v1/runner/resource-changes';
export const RUNNER_RESOURCE_ACK_PATH = '/v1/runner/resource-changes/ack';

export interface PreparedRunnerResources {
  manifest: ResourceManifest;
  networkAllowedDomains: string[];
  networkUnrestricted?: boolean;
  memoryStores: ReadonlyMap<string, string>;
  /** Registry-owned File identities; never sent to the runner. */
  fileIds?: ReadonlyMap<string, string>;
  refreshGitCapabilities?(): Promise<GitProxyCapability[]>;
  retainedGitResourceIds?: string[];
  open(resourceId: string, path: string): Promise<NodeJS.ReadableStream>;
  close(): Promise<void>;
}

export interface SessionResourcesDeliveryOptions {
  workspaceId: string;
  sessionId: string;
  fileStore: FileStore;
  memoryStore?: MemoryStore;
  store: TranscriptStore;
  /** Resolves trusted bindings and opens bytes in the Registry, never the runner. */
  prepare(retainedManifest?: ResourceManifest): Promise<PreparedRunnerResources>;
  /** Recheck owner and binding revision before every durable mutation and ACK. */
  isCurrent(revision: string): Promise<boolean>;
  /** A bounded JSON round trip over the session's authenticated tunnel. */
  request(path: string, body: unknown): Promise<unknown>;
}

/** Registry side of the staged-resource and persist-before-continuation protocol. */
export class SessionResourcesDelivery {
  private prepared: PreparedRunnerResources | undefined;
  private queue: Promise<unknown> = Promise.resolve();
  private knownOutputEvents: Set<string> | undefined;
  private knownMountEvents: Set<string> | undefined;
  private delivered = false;

  constructor(private readonly options: SessionResourcesDeliveryOptions) {}

  get revision(): string | undefined {
    return this.prepared?.manifest.revision;
  }

  async deliver(): Promise<{ version: 1; revision: string }> {
    this.delivered = false;
    let retainedManifest: ResourceManifest | undefined;
    if (this.prepared) {
      const status = record(await this.options.request(RUNNER_RESOURCES_PATH, { type: 'status' }));
      if (status.committed === true && status.revision === this.prepared.manifest.revision)
        retainedManifest = this.prepared.manifest;
    }
    const prepared = await this.options.prepare(retainedManifest);
    try {
      const manifest = parseResourceManifest(prepared.manifest);
      await this.assertCurrent(manifest.revision);
      this.prepared = { ...prepared, manifest };
      // Settle the runner's frozen writes before preparing replacement Memory
      // bytes. The manifest prepared above may predate those durable writes.
      const pending = record(
        await this.options.request(RUNNER_RESOURCE_CHANGES_PATH, { type: 'pending' }),
      );
      if (pending.checkpoint !== null && pending.checkpoint !== undefined) {
        const checkpoint = parseResourceCheckpoint(pending.checkpoint);
        await this.commit(checkpoint.checkpoint_id, pending.manifest_sha256);
        return await this.deliver();
      }
      const response = record(
        await this.options.request(RUNNER_RESOURCES_PATH, {
          type: 'manifest',
          manifest,
          network_allowed_domains: prepared.networkAllowedDomains,
          network_unrestricted: prepared.networkUnrestricted === true,
          retained_git_resource_ids: prepared.retainedGitResourceIds ?? [],
        }),
      );
      if (response.committed !== true) {
        for (const resource of manifest.resources) {
          if (prepared.retainedGitResourceIds?.includes(resource.resource_id)) continue;
          for (const file of resource.files) {
            const source = await prepared.open(resource.resource_id, file.path);
            const digest = createHash('sha256');
            let offset = 0;
            for await (const bytes of chunks(source)) {
              offset += bytes.length;
              if (offset > file.size_bytes)
                throw new Error('resource source exceeds declared size');
              digest.update(bytes);
              await this.options.request(RUNNER_RESOURCES_PATH, {
                type: 'file_chunk',
                revision: manifest.revision,
                resource_id: resource.resource_id,
                path: file.path,
                offset: offset - bytes.length,
                content_base64: bytes.toString('base64'),
              });
            }
            if (offset !== file.size_bytes || digest.digest('hex') !== file.sha256)
              throw new Error('resource source integrity mismatch');
          }
        }
        await this.assertCurrent(manifest.revision);
        const committed = record(
          await this.options.request(RUNNER_RESOURCES_PATH, {
            type: 'commit',
            revision: manifest.revision,
            manifest_sha256: resourceManifestDigest(manifest),
          }),
        );
        if (committed.committed !== true) throw new Error('runner did not commit resources');
      }
      this.prepared = { ...prepared, manifest };
      // Repair output File insert / transcript append ACK ambiguity before a turn.
      await this.reconcileOutputs();
      await this.refreshGitCapabilities();
      this.delivered = true;
      return { version: 1, revision: manifest.revision };
    } finally {
      await prepared.close();
    }
  }

  /** Refresh after byte transfer and again after turn preparation, immediately before dispatch. */
  async refreshGitCapabilities(): Promise<void> {
    const prepared = this.prepared;
    if (!prepared?.refreshGitCapabilities) return;
    await this.assertCurrent(prepared.manifest.revision);
    const capabilities = await prepared.refreshGitCapabilities();
    await this.assertCurrent(prepared.manifest.revision);
    const refreshed = record(
      await this.options.request(RUNNER_RESOURCES_PATH, {
        type: 'git_capabilities',
        revision: prepared.manifest.revision,
        capabilities,
      }),
    );
    if (refreshed.committed !== true) throw new Error('runner rejected Git capabilities');
  }

  /** Called only after Skills, snapshot and recovery also acknowledged preparation. */
  async publishMounted(): Promise<void> {
    const prepared = this.prepared;
    if (!this.delivered || !prepared) throw new Error('resources were not delivered');
    await this.assertCurrent(prepared.manifest.revision);
    if (!this.knownMountEvents) {
      const known = new Set<string>();
      for await (const prior of this.options.store.read(
        this.options.workspaceId,
        this.options.sessionId,
        { fromCursor: '', maxEvents: 0, subpath: '*' },
      )) {
        if (prior.kind === 'session.resource_mounted') known.add(prior.id);
      }
      this.knownMountEvents = known;
    }
    for (const resource of prepared.manifest.resources) {
      if (resource.kind !== 'file') continue;
      const fileId = prepared.fileIds?.get(resource.resource_id);
      if (!fileId) throw new Error('mounted File identity is missing');
      const id = stableId(
        'evt',
        this.options.workspaceId,
        this.options.sessionId,
        prepared.manifest.revision,
        resource.resource_id,
        'resource_mounted',
      );
      if (this.knownMountEvents.has(id)) continue;
      await this.assertCurrent(prepared.manifest.revision);
      try {
        await this.options.store.append(this.options.workspaceId, this.options.sessionId, [
          {
            id,
            workspaceId: this.options.workspaceId,
            sessionId: this.options.sessionId,
            subpath: '',
            seq: 0,
            producedAt: new Date().toISOString(),
            producedBy: 'harness',
            kind: 'session.resource_mounted',
            idempotencyKey: id,
            payload: Buffer.from(
              JSON.stringify({
                type: 'session.resource_mounted',
                resource_id: resource.resource_id,
                file_id: fileId,
                mount_path: resource.mount_path,
                mount_strategy: 'tarball_prefetch',
              }),
            ),
          },
        ]);
        this.knownMountEvents.add(id);
      } catch (error) {
        // Retry reads the durable transcript when the append succeeded but its ACK was lost.
        this.knownMountEvents = undefined;
        throw error;
      }
    }
  }

  /** Serial even if a reconnect races an old stream's checkpoint callback. */
  commit(checkpointId: unknown, digest: unknown): Promise<void> {
    const pending = this.queue.then(() => this.commitCheckpoint(checkpointId, digest));
    this.queue = pending.catch(() => {});
    return pending;
  }

  private async commitCheckpoint(checkpointId: unknown, digest: unknown): Promise<void> {
    const prepared = this.prepared;
    if (!prepared) throw new Error('resources were not delivered');
    if (typeof checkpointId !== 'string' || typeof digest !== 'string')
      throw new Error('invalid resource checkpoint control');
    const response = record(
      await this.options.request(RUNNER_RESOURCE_CHANGES_PATH, {
        type: 'manifest',
        checkpoint_id: checkpointId,
      }),
    );
    const checkpoint = parseResourceCheckpoint(response);
    if (
      checkpoint.checkpoint_id !== checkpointId ||
      checkpoint.revision !== prepared.manifest.revision ||
      resourceCheckpointDigest(checkpoint) !== digest ||
      response.manifest_sha256 !== digest
    )
      throw new Error('resource checkpoint identity differs');
    await this.assertCurrent(checkpoint.revision);
    // Validate the entire scope before writing even the first output. A runner
    // cannot redirect bytes to an unattached store or change a read-only mount.
    for (const file of [...checkpoint.files, ...checkpoint.deleted]) {
      if (file.resource_id === OUTPUT_RESOURCE_ID) continue;
      const binding = prepared.manifest.resources.find(
        (resource) => resource.resource_id === file.resource_id,
      );
      if (
        binding?.kind !== 'memory_store' ||
        binding.access !== 'read_write' ||
        !prepared.memoryStores.has(file.resource_id) ||
        !this.options.memoryStore
      )
        throw new Error('checkpoint targets an undeclared writable resource');
    }
    const deletionTargets = await this.deletionTargets(checkpoint);
    // File -> directory replacement requires deletion before child writes.
    for (const deleted of checkpoint.deleted) {
      await this.assertCurrent(checkpoint.revision);
      const storeId = prepared.memoryStores.get(deleted.resource_id)!;
      const memoryStore = this.options.memoryStore!;
      await this.assertMemoryStore(storeId);
      const versionId = stableId(
        'memver',
        'delete',
        this.options.workspaceId,
        this.options.sessionId,
        checkpointId,
        deleted.resource_id,
        deleted.path,
      );
      const receipt = await memoryStore.getVersion(this.options.workspaceId, storeId, versionId);
      if (receipt) {
        if (
          receipt.path !== deleted.path ||
          receipt.sha256 !== deleted.previous_sha256 ||
          receipt.writtenBySessionId !== this.options.sessionId ||
          receipt.writtenByEventId !== checkpointId
        )
          throw new Error('memory deletion receipt differs');
      }
      const memoryId = deletionTargets.get(JSON.stringify([deleted.resource_id, deleted.path]));
      // Even an already-absent path has a durable intent. A subsequent replay
      // must not reinterpret it as a newly created occurrence at that path.
      if (memoryId === null) continue;
      if (!memoryId) throw new Error('memory deletion intent is missing');
      const memory = await memoryStore.getMemory(this.options.workspaceId, storeId, memoryId);
      if (!memory && !receipt) continue;
      if (receipt && receipt.memoryId !== memoryId)
        throw new Error('memory deletion receipt targets another occurrence');
      if (memory && memory.currentSha256 !== deleted.previous_sha256)
        throw new Error('memory changed before checkpoint deletion');
      await this.assertCurrent(checkpoint.revision);
      await memoryStore.deleteMemory(this.options.workspaceId, storeId, memoryId, {
        sessionId: this.options.sessionId,
        previousSha256: deleted.previous_sha256,
        previousPath: deleted.path,
        versionId,
        writtenByEventId: checkpointId,
      });
    }
    const directory = await mkdtemp(join(tmpdir(), 'orca-resource-commit-'));
    try {
      for (const [index, file] of checkpoint.files.entries()) {
        await this.assertCurrent(checkpoint.revision);
        const local = join(directory, String(index));
        await this.fetchFile(checkpoint, file, local);
        await this.assertCurrent(checkpoint.revision);
        if (file.resource_id === OUTPUT_RESOURCE_ID) {
          const output = await this.options.fileStore.create({
            id: stableId(
              'file',
              this.options.workspaceId,
              this.options.sessionId,
              checkpointId,
              file.path,
            ),
            workspaceId: this.options.workspaceId,
            scopeId: this.options.sessionId,
            purpose: 'agent_output',
            downloadable: true,
            filename: posix.basename(file.path),
            mimeType: 'application/octet-stream',
            expectedSizeBytes: file.size_bytes,
            metadata: {
              orca_resource_protocol: '1',
              output_path: file.path,
              checkpoint_id: checkpointId,
            },
            content: createReadStream(local),
          });
          await this.appendOutput(output);
        } else {
          const storeId = prepared.memoryStores.get(file.resource_id)!;
          const memoryStore = this.options.memoryStore!;
          await this.assertMemoryStore(storeId);
          const versionId = stableId(
            'memver',
            this.options.workspaceId,
            this.options.sessionId,
            checkpointId,
            file.resource_id,
            file.path,
          );
          const write = async (conflict: boolean) => {
            await this.assertCurrent(checkpoint.revision);
            return memoryStore.writeMemory({
              workspaceId: this.options.workspaceId,
              storeId,
              path: file.path,
              sizeBytes: file.size_bytes,
              sha256: file.sha256,
              versionId: conflict ? `${versionId}_conflict` : versionId,
              writtenBySessionId: this.options.sessionId,
              writtenByEventId: checkpointId,
              ...(!conflict && file.previous_sha256
                ? { previousSha256: file.previous_sha256 }
                : {}),
              content: createReadStream(local),
            });
          };
          // A prior conflict write may have succeeded before its ACK was lost.
          const conflict = await memoryStore.getVersion(
            this.options.workspaceId,
            storeId,
            `${versionId}_conflict`,
          );
          try {
            await write(conflict !== null);
          } catch (error) {
            if (!(error instanceof MemoryConflictError)) throw error;
            await this.appendMemoryConflict(
              checkpoint,
              file.resource_id,
              file.path,
              storeId,
              error,
            );
            await write(true);
          }
        }
        await rm(local);
      }
      await this.assertCurrent(checkpoint.revision);
      await this.options.request(RUNNER_RESOURCE_ACK_PATH, {
        checkpoint_id: checkpointId,
        manifest_sha256: digest,
      });
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  private async appendMemoryConflict(
    checkpoint: ResourceCheckpoint,
    resourceId: string,
    path: string,
    storeId: string,
    conflict: MemoryConflictError,
  ): Promise<void> {
    const id = stableId(
      'evt',
      this.options.workspaceId,
      this.options.sessionId,
      checkpoint.checkpoint_id,
      resourceId,
      path,
      'memory_conflict',
    );
    // Read durable history on every retry, including append-success/ACK-loss.
    for await (const event of this.options.store.read(
      this.options.workspaceId,
      this.options.sessionId,
      { fromCursor: '', maxEvents: 0, subpath: '*' },
    )) {
      if (event.id !== id) continue;
      const body = record(JSON.parse(Buffer.from(event.payload).toString('utf8')));
      if (
        event.kind !== 'session.memory_conflict' ||
        body.store_id !== storeId ||
        body.path !== path ||
        body.expected_sha256 !== conflict.expectedSha ||
        body.observed_sha256 !== conflict.observedSha ||
        body.written_by_session_id !== this.options.sessionId
      )
        throw new Error('memory conflict event identity differs');
      return;
    }
    await this.assertCurrent(checkpoint.revision);
    await this.options.store.append(this.options.workspaceId, this.options.sessionId, [
      {
        id,
        workspaceId: this.options.workspaceId,
        sessionId: this.options.sessionId,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'harness',
        kind: 'session.memory_conflict',
        idempotencyKey: id,
        payload: Buffer.from(
          JSON.stringify({
            type: 'session.memory_conflict',
            store_id: storeId,
            path,
            expected_sha256: conflict.expectedSha,
            observed_sha256: conflict.observedSha,
            written_by_session_id: this.options.sessionId,
          }),
        ),
      },
    ]);
  }

  private async deletionTargets(
    checkpoint: ResourceCheckpoint,
  ): Promise<Map<string, string | null>> {
    if (checkpoint.deleted.length === 0) return new Map();
    const id = stableId(
      'evt',
      this.options.workspaceId,
      this.options.sessionId,
      checkpoint.checkpoint_id,
      'deletions',
    );
    const digest = resourceCheckpointDigest(checkpoint);
    const read = async (): Promise<Map<string, string | null> | undefined> => {
      for await (const event of this.options.store.read(
        this.options.workspaceId,
        this.options.sessionId,
        { fromCursor: '', maxEvents: 0, subpath: '*' },
      )) {
        if (event.id !== id) continue;
        const body = record(JSON.parse(Buffer.from(event.payload).toString('utf8')));
        if (
          event.kind !== 'harness.resource_deletions' ||
          body.manifest_sha256 !== digest ||
          !Array.isArray(body.targets)
        )
          throw new Error('memory deletion intent differs');
        const targets = new Map<string, string | null>();
        for (const [index, value] of body.targets.entries()) {
          const target = record(value);
          const deleted = checkpoint.deleted[index];
          if (
            !deleted ||
            target.resource_id !== deleted.resource_id ||
            target.path !== deleted.path ||
            (target.memory_id !== null &&
              (typeof target.memory_id !== 'string' ||
                !/^mem_[A-Za-z0-9_-]+$/.test(target.memory_id)))
          )
            throw new Error('invalid memory deletion intent');
          targets.set(
            JSON.stringify([deleted.resource_id, deleted.path]),
            target.memory_id as string | null,
          );
        }
        if (targets.size !== checkpoint.deleted.length)
          throw new Error('incomplete memory deletion intent');
        return targets;
      }
      return undefined;
    };
    const existing = await read();
    if (existing) return existing;
    const targets = [];
    for (const deleted of checkpoint.deleted) {
      const storeId = this.prepared!.memoryStores.get(deleted.resource_id)!;
      await this.assertMemoryStore(storeId);
      const memory = await this.options.memoryStore!.getMemoryByPath(
        this.options.workspaceId,
        storeId,
        deleted.path,
      );
      if (memory && memory.currentSha256 !== deleted.previous_sha256)
        throw new Error('memory changed before checkpoint deletion');
      targets.push({
        resource_id: deleted.resource_id,
        path: deleted.path,
        memory_id: memory?.id ?? null,
      });
    }
    await this.assertCurrent(checkpoint.revision);
    await this.options.store.append(this.options.workspaceId, this.options.sessionId, [
      {
        id,
        workspaceId: this.options.workspaceId,
        sessionId: this.options.sessionId,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'registry',
        kind: 'harness.resource_deletions',
        idempotencyKey: id,
        payload: Buffer.from(JSON.stringify({ manifest_sha256: digest, targets })),
      },
    ]);
    // Read back the first logical intent, also across append-ACK ambiguity.
    const persisted = await read();
    if (!persisted) throw new Error('memory deletion intent was not persisted');
    return persisted;
  }

  private async fetchFile(
    checkpoint: ResourceCheckpoint,
    file: ResourceFileDescriptor & { resource_id: string },
    local: string,
  ): Promise<void> {
    const handle = await open(local, 'wx', 0o600);
    const hash = createHash('sha256');
    try {
      for (let offset = 0; offset < file.size_bytes; ) {
        const response = record(
          await this.options.request(RUNNER_RESOURCE_CHANGES_PATH, {
            type: 'file_chunk',
            checkpoint_id: checkpoint.checkpoint_id,
            resource_id: file.resource_id,
            path: file.path,
            offset,
          }),
        );
        const bytes = decodeResourceChunk(response.content_base64);
        if (
          response.offset !== offset ||
          bytes.length === 0 ||
          offset + bytes.length > file.size_bytes
        )
          throw new Error('invalid resource checkpoint chunk');
        hash.update(bytes);
        let written = 0;
        while (written < bytes.length) {
          const result = await handle.write(
            bytes,
            written,
            bytes.length - written,
            offset + written,
          );
          if (result.bytesWritten === 0) throw new Error('resource checkpoint write stalled');
          written += result.bytesWritten;
        }
        offset += bytes.length;
      }
      if (hash.digest('hex') !== file.sha256)
        throw new Error('checkpoint content integrity mismatch');
    } finally {
      await handle.close();
    }
  }

  async reconcileOutputs(): Promise<void> {
    let cursor: string | undefined;
    do {
      const page = await this.options.fileStore.list(this.options.workspaceId, {
        scopeId: this.options.sessionId,
        purpose: 'agent_output',
        limit: 100,
        ...(cursor ? { cursor } : {}),
      });
      for (const file of page.items) {
        if (file.archivedAt === null && file.metadata.orca_resource_protocol === '1')
          await this.appendOutput(file);
      }
      cursor = page.nextCursor ?? undefined;
    } while (cursor);
  }

  private async appendOutput(file: FileRecord): Promise<void> {
    if (!this.revision) throw new Error('resources were not delivered');
    await this.assertCurrent(this.revision);
    const id = stableId('evt', file.id, 'output_indexed');
    // Backend append ACKs can be lost. The File row is the durable receipt;
    // reconcile by logical event id instead of promising broker exactly-once.
    if (!this.knownOutputEvents) {
      const known = new Set<string>();
      for await (const prior of this.options.store.read(
        this.options.workspaceId,
        this.options.sessionId,
        { fromCursor: '', maxEvents: 0, subpath: '*' },
      )) {
        if (prior.kind === 'session.output_indexed') known.add(prior.id);
      }
      this.knownOutputEvents = known;
    }
    if (this.knownOutputEvents.has(id)) return;
    const event: Event = {
      id,
      workspaceId: this.options.workspaceId,
      sessionId: this.options.sessionId,
      subpath: '',
      seq: 0,
      producedAt: file.createdAt.toISOString(),
      producedBy: 'harness',
      kind: 'session.output_indexed',
      idempotencyKey: id,
      payload: Buffer.from(
        JSON.stringify({
          type: 'session.output_indexed',
          file_id: file.id,
          key: file.metadata.output_path,
          sha256: file.sha256,
          size_bytes: file.sizeBytes,
        }),
      ),
    };
    try {
      await this.assertCurrent(this.revision);
      await this.options.store.append(this.options.workspaceId, this.options.sessionId, [event]);
      this.knownOutputEvents.add(id);
    } catch (error) {
      this.knownOutputEvents = undefined;
      throw error;
    }
  }

  private async assertMemoryStore(storeId: string): Promise<void> {
    const store = await this.options.memoryStore!.getStore(this.options.workspaceId, storeId);
    if (!store || store.archivedAt !== null)
      throw new Error('attached memory store is unavailable');
  }

  private async assertCurrent(revision: string): Promise<void> {
    if (!(await this.options.isCurrent(revision)))
      throw new Error('resource execution ownership or binding changed');
  }
}

function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('invalid runner resource response');
  return value as Record<string, unknown>;
}

function stableId(prefix: string, ...identity: string[]): string {
  return `${prefix}_${createHash('sha256').update(JSON.stringify(identity)).digest('hex')}`;
}

async function* chunks(source: NodeJS.ReadableStream): AsyncGenerator<Buffer> {
  for await (const chunk of source as AsyncIterable<Buffer>) {
    const bytes = Buffer.from(chunk);
    for (let offset = 0; offset < bytes.length; offset += RESOURCE_CHUNK_BYTES)
      yield bytes.subarray(offset, offset + RESOURCE_CHUNK_BYTES);
  }
}
