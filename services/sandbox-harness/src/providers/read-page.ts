// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { constants, type Stats } from 'node:fs';
import { open, realpath, stat as statPath, statfs, type FileHandle } from 'node:fs/promises';
import { posix as path } from 'node:path';
import type { SandboxWritePolicy } from '../write-policy.js';
import { isReadable } from '../write-policy.js';

export const DEFAULT_READ_LIMIT_BYTES = 100_000;
export const MAX_READ_LIMIT_BYTES = 100_000;

const MAX_UTF8_CODE_POINT_BYTES = 4;
const FORBIDDEN_FILESYSTEM_TYPES = new Set([
  0x9fa0, // procfs
  0x62656572, // sysfs
  0x1cd1, // devpts
]);

export interface ReadPageInput {
  offset?: number;
  limit?: number;
}

export interface ReadPageMetadata {
  offset: number;
  limit: number;
  bytes_read: number;
  total_bytes: number;
  offset_unit: 'utf8_bytes';
  truncation: boolean;
  next_offset: number | null;
}

export interface ReadPage {
  content: string;
  metadata: ReadPageMetadata;
}

interface OpenedRoot {
  handle: FileHandle;
  path: string;
  requestedPath: string;
  stat: Stats;
}

/**
 * Keep this wire behavior aligned with
 * services/harness-server/src/sandbox/read-page.ts. The packages cannot import
 * each other because sandbox-harness is also the image entrypoint.
 */
export function readUtf8Page(bytes: Buffer, input: ReadPageInput): ReadPage {
  const offset = input.offset ?? 0;
  const limit = input.limit ?? DEFAULT_READ_LIMIT_BYTES;
  assertOffset(offset, bytes);
  assertLimit(limit);

  let end = Math.min(bytes.length, offset + limit);
  if (end < bytes.length) {
    while (end > offset && isUtf8ContinuationByte(bytes[end]!)) end -= 1;
    if (end === offset) {
      end = Math.min(bytes.length, offset + 1);
      while (end < bytes.length && isUtf8ContinuationByte(bytes[end]!)) end += 1;
    }
  }

  const truncation = end < bytes.length;
  const metadata: ReadPageMetadata = {
    offset,
    limit,
    bytes_read: end - offset,
    total_bytes: bytes.length,
    offset_unit: 'utf8_bytes',
    truncation,
    next_offset: truncation ? end : null,
  };
  const text = bytes.toString('utf8', offset, end);
  const includeMetadata = truncation || input.offset !== undefined || input.limit !== undefined;
  return {
    content: includeMetadata ? appendMetadata(text, metadata) : text,
    metadata,
  };
}

/**
 * Read one page from an already materialized session file without loading the
 * whole file into the harness process. The opened descriptor is checked
 * against the policy roots before any bytes are returned, so symlink aliases
 * cannot escape into `/proc`, image files, or injected credentials.
 */
export async function readUtf8FilePage(
  filePath: string,
  policy: SandboxWritePolicy,
  input: ReadPageInput,
): Promise<ReadPage> {
  const offset = input.offset ?? 0;
  const limit = input.limit ?? DEFAULT_READ_LIMIT_BYTES;
  assertOffsetValue(offset);
  assertLimit(limit);

  const lexicalRoots = matchingReadableRoots(policy, filePath);
  if (lexicalRoots.length === 0) {
    throw new Error(`read denied for ${filePath}; path is outside session resource roots`);
  }

  const openedRoots: OpenedRoot[] = [];
  let handle: FileHandle | undefined;
  try {
    for (const root of lexicalRoots) {
      const rootHandle = await open(root, constants.O_RDONLY | constants.O_NONBLOCK);
      try {
        const rootStat = await rootHandle.stat();
        if (!rootStat.isDirectory() && !rootStat.isFile()) {
          throw new Error(`read denied for ${filePath}; readable root has an invalid type`);
        }
        openedRoots.push({
          handle: rootHandle,
          path: await resolveOpenedDescriptorPath(rootHandle.fd, root, rootStat),
          requestedPath: root,
          stat: rootStat,
        });
      } catch (cause) {
        await rootHandle.close().catch(() => undefined);
        throw cause;
      }
    }

    handle = await openTargetFromRoots(filePath, openedRoots);
    const stat = await handle.stat();
    if (!stat.isFile() || !Number.isSafeInteger(stat.size) || stat.size < 0) {
      throw new Error(`read denied for ${filePath}; path is not a regular file`);
    }
    if (stat.nlink !== 1) {
      throw new Error(`read denied for ${filePath}; hard-linked files are not readable`);
    }
    const openedPath = await resolveOpenedDescriptorPath(handle.fd, filePath, stat);
    const currentRoots = await Promise.all(
      openedRoots.map(async (root) => ({
        ...root,
        path: await resolveOpenedDescriptorPath(root.handle.fd, root.requestedPath, root.stat),
      })),
    );
    if (!currentRoots.some((root) => openedTargetIsWithinRoot(openedPath, stat, root))) {
      throw new Error(`read denied for ${filePath}; path escapes its session resource root`);
    }
    const filesystem = await statfs(
      process.platform === 'linux' ? `/proc/self/fd/${handle.fd}` : openedPath,
    );
    if (FORBIDDEN_FILESYSTEM_TYPES.has(Number(filesystem.type))) {
      throw new Error(`read denied for ${filePath}; pseudo-filesystems are not readable`);
    }
    if (offset > stat.size) {
      throw new Error(`offset ${offset} exceeds file size ${stat.size} bytes`);
    }

    const remaining = stat.size - offset;
    const requestedBytes = Math.min(remaining, limit + MAX_UTF8_CODE_POINT_BYTES - 1);
    const bytes = Buffer.alloc(requestedBytes);
    let bytesRead = 0;
    while (bytesRead < requestedBytes) {
      const result = await handle.read(
        bytes,
        bytesRead,
        requestedBytes - bytesRead,
        offset + bytesRead,
      );
      if (result.bytesRead === 0) break;
      bytesRead += result.bytesRead;
    }
    if (bytesRead !== requestedBytes) {
      throw new Error(`file changed while reading ${filePath}; retry the read`);
    }
    const pageBytes = bytes.subarray(0, bytesRead);
    if (offset < stat.size && isUtf8ContinuationByte(pageBytes[0]!)) {
      throw new Error(`offset ${offset} is not a UTF-8 code point boundary`);
    }

    let pageLength = Math.min(limit, pageBytes.length);
    if (offset + pageLength < stat.size) {
      while (pageLength > 0 && isUtf8ContinuationByte(pageBytes[pageLength]!)) {
        pageLength -= 1;
      }
      if (pageLength === 0) {
        pageLength = 1;
        while (pageLength < pageBytes.length && isUtf8ContinuationByte(pageBytes[pageLength]!)) {
          pageLength += 1;
        }
      }
    }

    const truncation = offset + pageLength < stat.size;
    const metadata: ReadPageMetadata = {
      offset,
      limit,
      bytes_read: pageLength,
      total_bytes: stat.size,
      offset_unit: 'utf8_bytes',
      truncation,
      next_offset: truncation ? offset + pageLength : null,
    };
    const text = pageBytes.toString('utf8', 0, pageLength);
    const includeMetadata = truncation || input.offset !== undefined || input.limit !== undefined;
    return {
      content: includeMetadata ? appendMetadata(text, metadata) : text,
      metadata,
    };
  } finally {
    await handle?.close().catch(() => undefined);
    await Promise.all(
      openedRoots.map(async (root) => await root.handle.close().catch(() => undefined)),
    );
  }
}

function assertOffset(offset: number, bytes: Buffer): void {
  assertOffsetValue(offset);
  if (offset > bytes.length) {
    throw new Error(`offset ${offset} exceeds file size ${bytes.length} bytes`);
  }
  if (offset < bytes.length && isUtf8ContinuationByte(bytes[offset]!)) {
    throw new Error(`offset ${offset} is not a UTF-8 code point boundary`);
  }
}

function assertOffsetValue(offset: number): void {
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new Error('offset must be a non-negative integer UTF-8 byte position');
  }
}

function assertLimit(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_READ_LIMIT_BYTES) {
    throw new Error(`limit must be an integer between 1 and ${MAX_READ_LIMIT_BYTES} bytes`);
  }
}

function isUtf8ContinuationByte(byte: number): boolean {
  return (byte & 0xc0) === 0x80;
}

function appendMetadata(content: string, metadata: ReadPageMetadata): string {
  const separator = content.length === 0 || content.endsWith('\n') ? '' : '\n';
  return `${content}${separator}[orca_read ${JSON.stringify(metadata)}]`;
}

function matchingReadableRoots(policy: SandboxWritePolicy, filePath: string): string[] {
  if (!path.isAbsolute(filePath) || filePath.includes('\0') || !isReadable(policy, filePath)) {
    return [];
  }
  const candidate = path.normalize(filePath);
  return [...policy.writablePaths.map((root) => root.path), ...policy.readonlyPaths]
    .map((root) => path.normalize(root))
    .filter((root) => isPathWithin(candidate, root));
}

async function openTargetFromRoots(
  candidate: string,
  roots: OpenedRoot[],
  platform = process.platform,
): Promise<FileHandle> {
  if (platform !== 'linux') {
    return await open(candidate, constants.O_RDONLY | constants.O_NONBLOCK);
  }

  let lastError: unknown;
  for (const root of roots) {
    const descriptorPath = root.stat.isFile()
      ? `/proc/self/fd/${root.handle.fd}`
      : `/proc/self/fd/${root.handle.fd}/${path.relative(root.requestedPath, candidate)}`;
    try {
      return await open(descriptorPath, constants.O_RDONLY | constants.O_NONBLOCK);
    } catch (cause) {
      lastError = cause;
    }
  }
  throw new Error(`read denied for ${candidate}; target could not be opened from a readable root`, {
    cause: lastError,
  });
}

function openedTargetIsWithinRoot(
  openedPath: string,
  targetStat: Stats,
  root: OpenedRoot,
): boolean {
  return descriptorTargetIsWithinRoot(
    openedPath,
    targetStat,
    root.path,
    root.stat,
    root.stat.isFile(),
  );
}

/** Keep this authorization rule aligned with harness-server's remote helper. */
export function descriptorTargetIsWithinRoot(
  openedPath: string,
  targetStat: Pick<Stats, 'dev' | 'ino'>,
  rootPath: string,
  rootStat: Pick<Stats, 'dev' | 'ino'>,
  rootIsFile: boolean,
): boolean {
  if (rootIsFile) {
    return targetStat.dev === rootStat.dev && targetStat.ino === rootStat.ino;
  }
  // gVisor can expose ordinary directory descendants with different synthetic
  // st_dev values. The target was opened through the trusted root descriptor;
  // resolved path containment is the portable directory-root boundary.
  return isPathWithin(openedPath, rootPath);
}

export async function resolveOpenedDescriptorPath(
  fd: number,
  requestedPath: string,
  openedStat: { dev: number; ino: number },
  platform = process.platform,
): Promise<string> {
  if (platform === 'linux') {
    try {
      return await realpath(`/proc/self/fd/${fd}`);
    } catch (cause) {
      throw new Error(
        `read denied for ${requestedPath}; Linux descriptor identity is unavailable`,
        { cause },
      );
    }
  }
  if (platform === 'darwin') {
    const canonical = await realpath(requestedPath);
    const current = await statPath(canonical);
    if (current.dev !== openedStat.dev || current.ino !== openedStat.ino) {
      throw new Error(`read denied for ${requestedPath}; file changed during identity check`);
    }
    return canonical;
  }
  throw new Error(
    `read denied for ${requestedPath}; descriptor verification is unsupported on ${platform}`,
  );
}

function isPathWithin(candidate: string, root: string): boolean {
  return candidate === root || candidate.startsWith(root === '/' ? '/' : `${root}/`);
}
