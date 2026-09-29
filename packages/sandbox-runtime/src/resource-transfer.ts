// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { posix as path } from 'node:path';
import { OUTPUT_WRITABLE_PATH, SKILLS_ROOT } from './write-policy.js';

/** Bounds shared by Registry producers and credential-free runner consumers. */
export const RESOURCE_CHUNK_BYTES = 1024 * 1024;
export const RESOURCE_MAX_FILE_BYTES = 512 * 1024 * 1024;
export const RESOURCE_MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024;
export const RESOURCE_MAX_FILES = 10_000;
export const RESOURCE_MAX_MOUNTS = 100;
export const RESOURCE_CHECKPOINT_EVENT = 'orca.resource_checkpoint';
export const OUTPUT_RESOURCE_ID = 'session_outputs';

export interface ChangedResourceFile extends ResourceFileDescriptor {
  resource_id: string;
  previous_sha256?: string;
}
export interface DeletedResourceFile {
  resource_id: string;
  path: string;
  previous_sha256: string;
}
export interface ResourceCheckpoint {
  version: 1;
  checkpoint_id: string;
  revision: string;
  files: ChangedResourceFile[];
  deleted: DeletedResourceFile[];
}

export interface ResourceFileDescriptor {
  /** Relative to the resource mount; empty only for a File mounted at an exact path. */
  path: string;
  sha256: string;
  size_bytes: number;
  mode: number;
}
export interface ResourceMountDescriptor {
  /** Opaque binding identity. Storage locations and credentials never cross this wire. */
  resource_id: string;
  kind: 'file' | 'memory_store' | 'github_repository';
  mount_path: string;
  access: 'read_only' | 'read_write';
  files: ResourceFileDescriptor[];
}
export interface ResourceManifest {
  version: 1;
  /** Stable resource-binding revision; reconnect must not overwrite live local changes. */
  revision: string;
  resources: ResourceMountDescriptor[];
}
/** A read-only Git proxy grant, never an upstream repository credential. */
export interface GitProxyCapability {
  resource_id: string;
  remote_url: string;
  authorization_header: string;
  expires_at: number;
}
export type ResourcePush =
  | { type: 'manifest'; manifest: ResourceManifest; network_allowed_domains?: string[] }
  | {
      type: 'file_chunk';
      revision: string;
      resource_id: string;
      path: string;
      offset: number;
      content_base64: string;
    }
  | { type: 'commit'; revision: string; manifest_sha256: string }
  | { type: 'git_capabilities'; revision: string; capabilities: GitProxyCapability[] };

const SHA256 = /^[a-f0-9]{64}$/;
const RESERVED_ROOTS = [
  '/.orca',
  OUTPUT_WRITABLE_PATH,
  SKILLS_ROOT,
  '/tmp',
  '/dev',
  '/proc',
  '/sys',
  '/etc',
  '/usr',
  '/bin',
  '/sbin',
  '/lib',
  '/lib64',
];

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('resource transfer requires an object');
  }
  return value as Record<string, unknown>;
}
function cleanPath(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length <= 4096 &&
    Buffer.from(value, 'utf8').toString('utf8') === value &&
    ![...value].some(
      (char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127 || char === '\\',
    )
  );
}
function within(candidate: string, root: string): boolean {
  return candidate === root || candidate.startsWith(`${root}/`);
}
function overlaps(left: string, right: string): boolean {
  return left === '/' || right === '/' || within(left, right) || within(right, left);
}

/** Validate at both ends, including resource-to-resource and file-to-directory aliases. */
export function parseResourceManifest(value: unknown): ResourceManifest {
  const raw = object(value);
  if (
    raw.version !== 1 ||
    typeof raw.revision !== 'string' ||
    !SHA256.test(raw.revision) ||
    !Array.isArray(raw.resources) ||
    raw.resources.length > RESOURCE_MAX_MOUNTS
  ) {
    throw new Error('invalid resource manifest');
  }
  let totalBytes = 0;
  let totalFiles = 0;
  const ids = new Set<string>();
  const mounts: string[] = [];
  const resources = raw.resources.map((item): ResourceMountDescriptor => {
    const resource = object(item);
    const id = resource.resource_id;
    const kind = resource.kind;
    const mount = resource.mount_path;
    const access = resource.access;
    if (
      typeof id !== 'string' ||
      !/^[A-Za-z0-9_-]{1,128}$/.test(id) ||
      id === OUTPUT_RESOURCE_ID ||
      ids.has(id) ||
      typeof kind !== 'string' ||
      !['file', 'memory_store', 'github_repository'].includes(kind) ||
      !cleanPath(mount) ||
      !mount.startsWith('/') ||
      path.normalize(mount) !== mount ||
      mount.endsWith('/') ||
      RESERVED_ROOTS.some((root) => overlaps(mount, root)) ||
      mounts.some((other) => overlaps(mount, other)) ||
      (access !== 'read_only' && access !== 'read_write') ||
      (kind === 'file' && access !== 'read_only') ||
      !Array.isArray(resource.files)
    ) {
      throw new Error('invalid resource binding or mount path');
    }
    ids.add(id);
    mounts.push(mount);
    const paths = new Set<string>();
    const files = resource.files.map((item): ResourceFileDescriptor => {
      const file = object(item);
      const relative = file.path;
      if (
        !cleanPath(relative) ||
        (kind === 'file'
          ? relative !== ''
          : relative === '' ||
            relative === '.' ||
            path.isAbsolute(relative) ||
            relative.split('/').some((segment) => segment === '..' || segment === '.') ||
            path.normalize(relative) !== relative ||
            relative.endsWith('/')) ||
        paths.has(relative) ||
        typeof file.sha256 !== 'string' ||
        !SHA256.test(file.sha256) ||
        !Number.isSafeInteger(file.size_bytes) ||
        Number(file.size_bytes) < 0 ||
        Number(file.size_bytes) > RESOURCE_MAX_FILE_BYTES ||
        !Number.isInteger(file.mode) ||
        Number(file.mode) < 0 ||
        Number(file.mode) > 0o777
      ) {
        throw new Error('invalid resource file descriptor');
      }
      paths.add(relative);
      totalFiles++;
      totalBytes += Number(file.size_bytes);
      if (totalFiles > RESOURCE_MAX_FILES || totalBytes > RESOURCE_MAX_TOTAL_BYTES) {
        throw new Error('resource manifest exceeds transfer limits');
      }
      return {
        path: relative,
        sha256: file.sha256,
        size_bytes: Number(file.size_bytes),
        mode: Number(file.mode),
      };
    });
    for (const entry of paths) {
      let ancestor = path.dirname(entry);
      while (ancestor !== '.') {
        if (paths.has(ancestor)) {
          throw new Error('resource files overlap as file and directory');
        }
        ancestor = path.dirname(ancestor);
      }
    }
    if (kind === 'file' && files.length !== 1)
      throw new Error('File resource requires exactly one file');
    return {
      resource_id: id,
      kind: kind as ResourceMountDescriptor['kind'],
      mount_path: mount,
      access,
      files,
    };
  });
  return { version: 1, revision: raw.revision, resources };
}

export function resourceManifestDigest(manifest: ResourceManifest): string {
  return createHash('sha256')
    .update(JSON.stringify(parseResourceManifest(manifest)))
    .digest('hex');
}

export function resourceFilePath(
  resource: ResourceMountDescriptor,
  file: ResourceFileDescriptor,
): string {
  return file.path === '' ? resource.mount_path : `${resource.mount_path}/${file.path}`;
}

/** Validate runner-produced changes before Registry opens any staging path or store. */
export function parseResourceCheckpoint(value: unknown): ResourceCheckpoint {
  const raw = object(value);
  if (
    raw.version !== 1 ||
    typeof raw.checkpoint_id !== 'string' ||
    !/^rchk_[A-Za-z0-9_-]{1,128}$/.test(raw.checkpoint_id) ||
    typeof raw.revision !== 'string' ||
    !SHA256.test(raw.revision) ||
    !Array.isArray(raw.files) ||
    !Array.isArray(raw.deleted) ||
    raw.files.length + raw.deleted.length > RESOURCE_MAX_FILES
  )
    throw new Error('invalid resource checkpoint');
  const seen = new Map<string, Set<string>>();
  function identity(entry: Record<string, unknown>): { resource_id: string; path: string } {
    const id = entry.resource_id;
    const relative = entry.path;
    if (
      typeof id !== 'string' ||
      !/^[A-Za-z0-9_-]{1,128}$/.test(id) ||
      !cleanPath(relative) ||
      relative === '' ||
      relative === '.' ||
      path.isAbsolute(relative) ||
      relative.split('/').some((part) => part === '.' || part === '..') ||
      path.normalize(relative) !== relative ||
      relative.endsWith('/')
    )
      throw new Error('invalid checkpoint file identity');
    const paths = seen.get(id) ?? new Set<string>();
    if (paths.has(relative)) throw new Error('duplicate checkpoint file');
    paths.add(relative);
    seen.set(id, paths);
    return { resource_id: id, path: relative };
  }
  let bytes = 0;
  const files = raw.files.map((item): ChangedResourceFile => {
    const entry = object(item);
    const file = identity(entry);
    if (
      typeof entry.sha256 !== 'string' ||
      !SHA256.test(entry.sha256) ||
      typeof entry.size_bytes !== 'number' ||
      !Number.isSafeInteger(entry.size_bytes) ||
      entry.size_bytes < 0 ||
      entry.size_bytes > RESOURCE_MAX_FILE_BYTES ||
      typeof entry.mode !== 'number' ||
      !Number.isInteger(entry.mode) ||
      entry.mode < 0 ||
      entry.mode > 0o777 ||
      (entry.previous_sha256 !== undefined &&
        (typeof entry.previous_sha256 !== 'string' || !SHA256.test(entry.previous_sha256)))
    )
      throw new Error('invalid checkpoint file descriptor');
    bytes += entry.size_bytes;
    if (bytes > RESOURCE_MAX_TOTAL_BYTES) throw new Error('resource checkpoint exceeds byte limit');
    return {
      ...file,
      sha256: entry.sha256,
      size_bytes: entry.size_bytes,
      mode: entry.mode,
      ...(entry.previous_sha256 === undefined
        ? {}
        : { previous_sha256: entry.previous_sha256 as string }),
    };
  });
  const deleted = raw.deleted.map((item): DeletedResourceFile => {
    const entry = object(item);
    const file = identity(entry);
    if (
      file.resource_id === OUTPUT_RESOURCE_ID ||
      typeof entry.previous_sha256 !== 'string' ||
      !SHA256.test(entry.previous_sha256)
    )
      throw new Error('invalid checkpoint deletion');
    return { ...file, previous_sha256: entry.previous_sha256 };
  });
  // Deleted ancestors may validly be replaced by a directory containing a new
  // file. Only live file/file aliases conflict; Registry applies deletions first.
  const live = new Set(files.map((file) => JSON.stringify([file.resource_id, file.path])));
  for (const file of files) {
    let ancestor = path.dirname(file.path);
    while (ancestor !== '.') {
      if (live.has(JSON.stringify([file.resource_id, ancestor])))
        throw new Error('checkpoint files overlap');
      ancestor = path.dirname(ancestor);
    }
  }
  return { version: 1, checkpoint_id: raw.checkpoint_id, revision: raw.revision, files, deleted };
}

export function resourceCheckpointDigest(checkpoint: ResourceCheckpoint): string {
  return createHash('sha256')
    .update(JSON.stringify(parseResourceCheckpoint(checkpoint)))
    .digest('hex');
}

/** Strict bounded base64 avoids tolerant decoding accepting changed transfer bytes. */
export function decodeResourceChunk(value: unknown): Buffer {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > Math.ceil(RESOURCE_CHUNK_BYTES / 3) * 4
  ) {
    throw new Error('invalid resource chunk size');
  }
  const bytes = Buffer.from(value, 'base64');
  if (
    bytes.length === 0 ||
    bytes.length > RESOURCE_CHUNK_BYTES ||
    bytes.toString('base64') !== value
  ) {
    throw new Error('invalid resource chunk encoding');
  }
  return bytes;
}
