// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyRequest } from 'fastify';
import { strFromU8, Unzip, UnzipInflate, type UnzipFile } from 'fflate';
import { load as loadYaml } from 'js-yaml';

export interface SkillArchiveFile {
  path: string;
  content_base64: string;
  mime_type?: string | null;
  mode?: number;
}

export interface ParsedSkillUpload {
  name: string;
  description: string;
  directory: string;
  displayTitle: string | null;
  files: SkillArchiveFile[];
}

export interface ParseSkillUploadOptions {
  maxTotalBytes?: number;
  maxFiles?: number;
  allowDisplayTitle?: boolean;
}

export class SkillUploadError extends Error {
  constructor(
    message: string,
    readonly statusCode: 400 | 413 = 400,
  ) {
    super(message);
    this.name = 'SkillUploadError';
  }
}

const MAX_SKILL_DISPLAY_TITLE_LENGTH = 255;
const MAX_SKILL_DESCRIPTION_LENGTH = 1024;
const SAFE_SKILL_NAME = /^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/;
const RESERVED_SKILL_NAME_SEGMENTS = new Set(['anthropic', 'claude']);
const XML_TAG = /<\/?[A-Za-z][A-Za-z0-9:_-]*(?:\s[^<>]*)?\/?>/;
const SAFE_FILE_MODE = 0o644;
const SAFE_EXECUTABLE_MODE = 0o755;

interface MultipartPart {
  type: 'file' | 'field';
  filename?: string;
  mimetype?: string;
  fieldname: string;
  toBuffer?: () => Promise<Buffer>;
  value?: unknown;
}

interface UploadedFilePart {
  filename: string;
  mimetype: string | null;
  buffer: Buffer;
}

const ZIP_MIME_TYPES = new Set(['application/zip', 'application/x-zip-compressed']);

const CRC32_TABLE = new Uint32Array(256);
for (let i = 0; i < CRC32_TABLE.length; i += 1) {
  let c = i;
  for (let bit = 0; bit < 8; bit += 1) {
    c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  }
  CRC32_TABLE[i] = c >>> 0;
}

export function normalizeArchivePath(rawPath: string): string {
  const normalized = rawPath.replaceAll('\\', '/');
  if (
    normalized === '' ||
    normalized.startsWith('/') ||
    /^[A-Za-z]:\//.test(normalized) ||
    normalized.includes('\0')
  ) {
    throw new SkillUploadError('invalid file path');
  }
  const segments = normalized.split('/');
  if (segments.some((segment) => segment === '' || segment === '.' || segment === '..')) {
    throw new SkillUploadError('invalid file path');
  }
  return segments.join('/');
}

function isZipUpload(part: UploadedFilePart): boolean {
  if (ZIP_MIME_TYPES.has(part.mimetype ?? '')) return true;
  if (part.filename.toLowerCase().endsWith('.zip')) return true;
  if (part.buffer.length < 4) return false;
  const signature = part.buffer.readUInt32LE(0);
  return signature === 0x04034b50 || signature === 0x06054b50 || signature === 0x08074b50;
}

function extractZipFiles(
  archive: Buffer,
  maxFileBytes: number,
  maxTotalBytes: number,
  maxFiles: number,
): SkillArchiveFile[] {
  const archiveModes = readZipEntryModes(archive, maxFiles);
  let fileCount = 0;
  let declaredTotalBytes = 0;
  let extractedTotalBytes = 0;
  let sawEntry = false;
  let failure: SkillUploadError | null = null;
  const paths = new Set<string>();
  const activeFiles = new Set<UnzipFile>();
  const extracted: Array<SkillArchiveFile | undefined> = [];

  const fail = (error: SkillUploadError): void => {
    if (failure !== null) return;
    failure = error;
    for (const file of activeFiles) file.terminate();
    activeFiles.clear();
  };

  const unzip = new Unzip((file) => {
    sawEntry = true;
    if (failure !== null) return;

    const isDirectory = file.name.endsWith('/');
    let path: string | null = null;
    if (!isDirectory) {
      try {
        path = normalizeArchivePath(file.name);
      } catch (error) {
        fail(error instanceof SkillUploadError ? error : new SkillUploadError('invalid file path'));
        return;
      }
      if (paths.has(path)) {
        fail(new SkillUploadError(`zip archive contains duplicate file path: ${path}`));
        return;
      }
      paths.add(path);
      fileCount += 1;
      if (fileCount > maxFiles) {
        fail(
          new SkillUploadError(
            `skill upload exceeds ${maxFiles} file${maxFiles === 1 ? '' : 's'} limit`,
            413,
          ),
        );
        return;
      }
    }

    if (file.originalSize !== undefined) {
      if (file.originalSize > maxFileBytes) {
        fail(new SkillUploadError(`file exceeds ${maxFileBytes} byte limit`, 413));
        return;
      }
      declaredTotalBytes += file.originalSize;
      if (declaredTotalBytes > maxTotalBytes) {
        fail(new SkillUploadError(`skill upload exceeds ${maxTotalBytes} byte total limit`, 413));
        return;
      }
    }

    const outputIndex = isDirectory ? -1 : extracted.length;
    if (!isDirectory) extracted.push(undefined);
    const chunks: Buffer[] = [];
    let extractedFileBytes = 0;
    activeFiles.add(file);
    file.ondata = (error, chunk, final) => {
      if (failure !== null) return;
      if (error) {
        fail(new SkillUploadError('invalid zip archive'));
        return;
      }
      if (chunk && chunk.length > 0) {
        extractedFileBytes += chunk.length;
        extractedTotalBytes += chunk.length;
        if (extractedFileBytes > maxFileBytes) {
          fail(new SkillUploadError(`file exceeds ${maxFileBytes} byte limit`, 413));
          return;
        }
        if (extractedTotalBytes > maxTotalBytes) {
          fail(new SkillUploadError(`skill upload exceeds ${maxTotalBytes} byte total limit`, 413));
          return;
        }
        if (!isDirectory) chunks.push(Buffer.from(chunk));
      }
      if (final) {
        activeFiles.delete(file);
        if (!isDirectory && path !== null) {
          extracted[outputIndex] = {
            path,
            content_base64: Buffer.concat(chunks, extractedFileBytes).toString('base64'),
            mime_type: null,
            mode: archiveModes.get(file.name) ?? SAFE_FILE_MODE,
          };
        }
      }
    };
    try {
      file.start();
    } catch {
      fail(new SkillUploadError('invalid zip archive'));
    }
  });
  unzip.register(UnzipInflate);

  try {
    // Feed bounded compressed chunks so a highly-compressible entry cannot
    // force one unbounded synchronous inflate allocation before ondata can
    // enforce the actual output byte limits.
    const chunkBytes = 16 * 1024;
    for (let offset = 0; offset < archive.length; offset += chunkBytes) {
      const end = Math.min(offset + chunkBytes, archive.length);
      unzip.push(archive.subarray(offset, end), end === archive.length);
      if (failure !== null) throw failure;
    }
  } catch (error) {
    if (error instanceof SkillUploadError) throw error;
    throw new SkillUploadError('invalid zip archive');
  }

  if (failure !== null) throw failure;
  if (!sawEntry) throw new SkillUploadError('invalid zip archive');
  if (activeFiles.size > 0 || extracted.some((file) => file === undefined)) {
    throw new SkillUploadError('invalid zip archive');
  }
  return extracted as SkillArchiveFile[];
}

export async function parseSkillUpload(
  req: FastifyRequest,
  maxFileBytes: number,
  options: ParseSkillUploadOptions = {},
): Promise<ParsedSkillUpload> {
  const maxTotalBytes = options.maxTotalBytes ?? maxFileBytes;
  const maxFiles = options.maxFiles ?? 128;
  const partsIterable = (
    req as unknown as {
      parts: (opts?: {
        preservePath?: boolean;
        limits?: { fileSize?: number };
      }) => AsyncIterableIterator<MultipartPart>;
    }
  ).parts({ preservePath: true, limits: { fileSize: maxFileBytes } });

  const uploadedParts: UploadedFilePart[] = [];
  let totalBytes = 0;
  let displayTitle: string | null = null;
  try {
    for await (const part of partsIterable) {
      if (part.type === 'field') {
        if (part.fieldname === 'workspace_id' || part.fieldname === 'workspaceId') {
          throw new SkillUploadError(
            'workspace_id is derived from authentication and must not be supplied',
          );
        }
        if (part.fieldname === 'display_title') {
          if (options.allowDisplayTitle === false) {
            throw new SkillUploadError(
              'display_title is not supported when creating a skill version',
            );
          }
          if (typeof part.value !== 'string') {
            throw new SkillUploadError(
              `display_title must contain 1-${MAX_SKILL_DISPLAY_TITLE_LENGTH} characters`,
            );
          }
          const title = part.value.trim();
          if (title.length === 0 || title.length > MAX_SKILL_DISPLAY_TITLE_LENGTH) {
            throw new SkillUploadError(
              `display_title must contain 1-${MAX_SKILL_DISPLAY_TITLE_LENGTH} characters`,
            );
          }
          displayTitle = title;
        }
        continue;
      }
      if (!part.toBuffer) throw new SkillUploadError('missing file stream');
      if (uploadedParts.length >= maxFiles) {
        throw new SkillUploadError(
          `skill upload exceeds ${maxFiles} file${maxFiles === 1 ? '' : 's'} limit`,
          413,
        );
      }
      const buffer = await part.toBuffer();
      totalBytes += buffer.length;
      if (totalBytes > maxTotalBytes) {
        throw new SkillUploadError(`skill upload exceeds ${maxTotalBytes} byte total limit`, 413);
      }
      uploadedParts.push({
        filename: part.filename ?? '',
        mimetype: part.mimetype ?? null,
        buffer,
      });
    }
  } catch (err) {
    if (err instanceof SkillUploadError) throw err;
    const e = err as { code?: string; statusCode?: number };
    if (e.code === 'FST_REQ_FILE_TOO_LARGE' || e.statusCode === 413) {
      throw new SkillUploadError(`file exceeds ${maxFileBytes} byte limit`, 413);
    }
    throw err;
  }

  if (uploadedParts.length === 0) throw new SkillUploadError('no files provided');
  const files =
    uploadedParts.length === 1 && isZipUpload(uploadedParts[0]!)
      ? extractZipFiles(uploadedParts[0]!.buffer, maxFileBytes, maxTotalBytes, maxFiles)
      : uploadedParts.map((part) => ({
          path: normalizeArchivePath(part.filename),
          content_base64: part.buffer.toString('base64'),
          mime_type: part.mimetype,
          mode: SAFE_FILE_MODE,
        }));
  if (files.length === 0) throw new SkillUploadError('no files provided');
  const paths = new Set(files.map((file) => file.path));
  if (paths.size !== files.length) {
    throw new SkillUploadError('skill upload contains duplicate file paths');
  }
  const topLevelDirs = new Set(files.map((file) => file.path.split('/')[0]));
  if (topLevelDirs.size !== 1) {
    throw new SkillUploadError('all files must share a top-level directory');
  }
  const directory = topLevelDirs.values().next().value as string;
  const skillFile = files.find((file) => file.path === `${directory}/SKILL.md`);
  if (!skillFile) throw new SkillUploadError('uploaded skill must include root SKILL.md');

  const skillContent = Buffer.from(skillFile.content_base64, 'base64').toString('utf8');
  const parsed = parseSkillMarkdown(skillContent);
  if (parsed.name === undefined || !SAFE_SKILL_NAME.test(parsed.name)) {
    throw new SkillUploadError(
      'SKILL.md frontmatter name must be 1-64 lowercase letters, numbers, or hyphens and must start and end with a letter or number',
    );
  }
  if (parsed.name.split('-').some((segment) => RESERVED_SKILL_NAME_SEGMENTS.has(segment))) {
    throw new SkillUploadError(
      'SKILL.md frontmatter name must not contain reserved anthropic or claude segments',
    );
  }
  if (
    parsed.description === undefined ||
    parsed.description.trim().length === 0 ||
    parsed.description.length > MAX_SKILL_DESCRIPTION_LENGTH
  ) {
    throw new SkillUploadError(
      `SKILL.md frontmatter description must contain 1-${MAX_SKILL_DESCRIPTION_LENGTH} characters`,
    );
  }
  if (XML_TAG.test(parsed.description)) {
    throw new SkillUploadError('SKILL.md frontmatter description must not contain XML tags');
  }
  if (normalizeSkillDirectoryName(directory) !== normalizeSkillDirectoryName(parsed.name)) {
    throw new SkillUploadError(
      'top-level skill directory must match the SKILL.md frontmatter name',
    );
  }
  return {
    name: parsed.name,
    description: parsed.description,
    directory,
    displayTitle,
    files,
  };
}

function normalizeSkillDirectoryName(value: string): string {
  return value.toLowerCase().replaceAll('_', '-');
}

export function buildZip(files: SkillArchiveFile[]): Buffer {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  let offset = 0;

  for (const file of files) {
    const path = normalizeArchivePath(file.path);
    const pathBuffer = Buffer.from(path);
    const generalPurposeFlags = [...path].some((character) => character.codePointAt(0)! > 0x7f)
      ? 0x0800
      : 0;
    const content = Buffer.from(file.content_base64, 'base64');
    const crc = crc32(content);

    const localHeader = Buffer.alloc(30);
    localHeader.writeUInt32LE(0x04034b50, 0);
    localHeader.writeUInt16LE(20, 4);
    localHeader.writeUInt16LE(generalPurposeFlags, 6);
    localHeader.writeUInt16LE(0, 8);
    localHeader.writeUInt16LE(0, 10);
    localHeader.writeUInt16LE(0, 12);
    localHeader.writeUInt32LE(crc, 14);
    localHeader.writeUInt32LE(content.length, 18);
    localHeader.writeUInt32LE(content.length, 22);
    localHeader.writeUInt16LE(pathBuffer.length, 26);
    localHeader.writeUInt16LE(0, 28);

    localParts.push(localHeader, pathBuffer, content);

    const centralHeader = Buffer.alloc(46);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE((3 << 8) | 20, 4);
    centralHeader.writeUInt16LE(20, 6);
    centralHeader.writeUInt16LE(generalPurposeFlags, 8);
    centralHeader.writeUInt16LE(0, 10);
    centralHeader.writeUInt16LE(0, 12);
    centralHeader.writeUInt16LE(0, 14);
    centralHeader.writeUInt32LE(crc, 16);
    centralHeader.writeUInt32LE(content.length, 20);
    centralHeader.writeUInt32LE(content.length, 24);
    centralHeader.writeUInt16LE(pathBuffer.length, 28);
    centralHeader.writeUInt16LE(0, 30);
    centralHeader.writeUInt16LE(0, 32);
    centralHeader.writeUInt16LE(0, 34);
    centralHeader.writeUInt16LE(0, 36);
    centralHeader.writeUInt32LE((safeFileMode(file.mode) << 16) >>> 0, 38);
    centralHeader.writeUInt32LE(offset, 42);
    centralParts.push(centralHeader, pathBuffer);

    offset += localHeader.length + pathBuffer.length + content.length;
  }

  const centralDirectory = Buffer.concat(centralParts);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(0, 4);
  end.writeUInt16LE(0, 6);
  end.writeUInt16LE(files.length, 8);
  end.writeUInt16LE(files.length, 10);
  end.writeUInt32LE(centralDirectory.length, 12);
  end.writeUInt32LE(offset, 16);
  end.writeUInt16LE(0, 20);

  return Buffer.concat([...localParts, centralDirectory, end]);
}

function safeFileMode(mode: number | undefined): number {
  return mode !== undefined && (mode & 0o111) !== 0 ? SAFE_EXECUTABLE_MODE : SAFE_FILE_MODE;
}

function readZipEntryModes(archive: Buffer, maxFiles: number): Map<string, number> {
  const minimumEocdBytes = 22;
  const maximumCommentBytes = 0xffff;
  const searchStart = Math.max(0, archive.length - minimumEocdBytes - maximumCommentBytes);
  let eocdOffset = -1;
  for (let offset = archive.length - minimumEocdBytes; offset >= searchStart; offset -= 1) {
    if (archive.readUInt32LE(offset) !== 0x06054b50) continue;
    const diskNumber = archive.readUInt16LE(offset + 4);
    const centralDisk = archive.readUInt16LE(offset + 6);
    const entriesOnDisk = archive.readUInt16LE(offset + 8);
    const totalEntries = archive.readUInt16LE(offset + 10);
    const centralSize = archive.readUInt32LE(offset + 12);
    const centralOffset = archive.readUInt32LE(offset + 16);
    const commentLength = archive.readUInt16LE(offset + 20);
    if (
      diskNumber === 0 &&
      centralDisk === 0 &&
      entriesOnDisk === totalEntries &&
      offset + minimumEocdBytes + commentLength === archive.length &&
      centralOffset + centralSize === offset
    ) {
      eocdOffset = offset;
      break;
    }
  }
  if (eocdOffset < 0) throw new SkillUploadError('invalid zip archive');

  const entryCount = archive.readUInt16LE(eocdOffset + 10);
  const centralSize = archive.readUInt32LE(eocdOffset + 12);
  const centralOffset = archive.readUInt32LE(eocdOffset + 16);
  if (
    entryCount > maxFiles * 2 + 32 ||
    centralOffset > archive.length ||
    centralSize > archive.length - centralOffset
  ) {
    throw new SkillUploadError('invalid zip archive');
  }

  const modes = new Map<string, number>();
  let offset = centralOffset;
  for (let index = 0; index < entryCount; index += 1) {
    if (offset + 46 > archive.length || archive.readUInt32LE(offset) !== 0x02014b50) {
      throw new SkillUploadError('invalid zip archive');
    }
    const madeByOs = archive.readUInt16LE(offset + 4) >>> 8;
    const filenameLength = archive.readUInt16LE(offset + 28);
    const extraLength = archive.readUInt16LE(offset + 30);
    const commentLength = archive.readUInt16LE(offset + 32);
    const nextOffset = offset + 46 + filenameLength + extraLength + commentLength;
    if (nextOffset > archive.length) throw new SkillUploadError('invalid zip archive');
    const flags = archive.readUInt16LE(offset + 8);
    const name = strFromU8(
      archive.subarray(offset + 46, offset + 46 + filenameLength),
      (flags & 0x0800) === 0,
    );
    const attrs = archive.readUInt32LE(offset + 38);
    const unixMode = madeByOs === 3 ? (attrs >>> 16) & 0xffff : SAFE_FILE_MODE;
    modes.set(name, safeFileMode(unixMode));
    offset = nextOffset;
  }
  if (offset !== centralOffset + centralSize) throw new SkillUploadError('invalid zip archive');
  return modes;
}

function parseSkillMarkdown(content: string): {
  name?: string;
  description?: string;
  body: string;
} {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/.exec(content);
  if (!match) throw new SkillUploadError('SKILL.md must contain YAML frontmatter');

  let metadata: unknown;
  try {
    metadata = loadYaml(match[1] ?? '');
  } catch (err) {
    throw new SkillUploadError(
      `invalid SKILL.md YAML frontmatter: ${err instanceof Error ? err.message : 'parse error'}`,
    );
  }
  if (metadata !== undefined && !isRecord(metadata)) {
    throw new SkillUploadError('SKILL.md YAML frontmatter must be an object');
  }
  const name = metadata?.['name'];
  const description = metadata?.['description'];
  if (name !== undefined && typeof name !== 'string') {
    throw new SkillUploadError('SKILL.md frontmatter name must be a string');
  }
  if (description !== undefined && typeof description !== 'string') {
    throw new SkillUploadError('SKILL.md frontmatter description must be a string');
  }
  return {
    ...(name === undefined ? {} : { name }),
    ...(description === undefined ? {} : { description }),
    body: content.slice(match[0].length),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function crc32(buffer: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc = CRC32_TABLE[(crc ^ byte) & 0xff]! ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}
