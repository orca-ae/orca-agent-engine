// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type { SdkCheckpoint } from '@orca/sdk-harness';
import { sessionFormatCatalog } from '@deepseek-ai/dsh-session-format-catalog';

export const DSH_SDK_VERSION = '0.2.1-alpha.2';
export const PROFILE = 'orca-managed-tree-v2';
export const MAX_STATE_BYTES = 16 * 1024 * 1024;
export const MAX_STATE_FILES = 129;
export const NATIVE_ID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const digest = (system: string): string => createHash('sha256').update(system).digest('hex');

export function decodeCheckpoint(checkpoint: SdkCheckpoint): Record<string, Buffer> {
  if (
    checkpoint.version !== 1 ||
    checkpoint.format !== 'dsh_sdk' ||
    checkpoint.sdkVersion !== DSH_SDK_VERSION ||
    !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(checkpoint.threadId) ||
    !/^[a-f0-9]{64}$/.test(checkpoint.instructionsSha256 ?? '')
  )
    throw new Error('Incompatible DSH checkpoint');
  const files: Record<string, Buffer> = Object.create(null);
  if (Object.keys(checkpoint.files).length > MAX_STATE_FILES)
    throw new Error('DSH checkpoint exceeds file limit');
  let bytes = 0;
  for (const [name, encoded] of Object.entries(checkpoint.files)) {
    if (
      name !== 'orca.json' &&
      !(
        /^[A-Za-z0-9_.~%-]+\/[a-f0-9-]+\/session\.v4\.jsonl$/.test(name) &&
        !name.split('/').some((part) => part === '.' || part === '..') &&
        NATIVE_ID.test(name.split('/')[1]!)
      )
    )
      throw new Error('Invalid DSH native file path');
    if (
      typeof encoded !== 'string' ||
      encoded.length > Math.ceil(MAX_STATE_BYTES / 3) * 4 ||
      !/^[A-Za-z0-9+/]*={0,2}$/.test(encoded) ||
      encoded.length % 4 !== 0
    )
      throw new Error('Invalid DSH file encoding');
    const decodedSize =
      (encoded.length / 4) * 3 - (encoded.endsWith('==') ? 2 : encoded.endsWith('=') ? 1 : 0);
    if (bytes + decodedSize > MAX_STATE_BYTES) throw new Error('DSH checkpoint exceeds 16 MiB');
    const data = Buffer.from(encoded, 'base64');
    if (data.toString('base64') !== encoded) throw new Error('Invalid DSH file encoding');
    bytes += data.length;
    if (bytes > MAX_STATE_BYTES) throw new Error('DSH checkpoint exceeds 16 MiB');
    files[name] = data;
  }
  const manifest = JSON.parse(files['orca.json']?.toString('utf8') ?? 'null');
  if (
    manifest?.profile !== PROFILE ||
    manifest?.sdkVersion !== DSH_SDK_VERSION ||
    manifest?.threadId !== checkpoint.threadId ||
    manifest?.instructionsSha256 !== checkpoint.instructionsSha256 ||
    Object.keys(files).length < 2
  )
    throw new Error('Incompatible DSH native profile');
  validateTree(checkpoint.threadId, files);
  return files;
}

/** Decode with DSH's installed codec; catalogs and own suffixes are the only membership authority. */
export function validateTree(rootId: string, files: Record<string, Buffer>): void {
  const sessions = new Map<
    string,
    ReturnType<ReturnType<typeof sessionFormatCatalog.createRestore>['finish']>
  >();
  for (const [name, data] of Object.entries(files)) {
    if (name === 'orca.json') continue;
    if (!data.length || data.at(-1) !== 10) throw new Error('Truncated DSH native log');
    const rows = data
      .toString('utf8')
      .trimEnd()
      .split('\n')
      .map((line) => JSON.parse(line));
    if (rows[0]?.version !== 4) throw new Error('Incompatible DSH native generation');
    const restore = sessionFormatCatalog.createRestore(rows[0], {
      recovery: 'strict',
      validation: 'current',
    });
    for (const row of rows.slice(1)) restore.decodeRow(row);
    const artifact = restore.finish();
    const id = artifact.header.id;
    if (id !== name.split('/')[1] || sessions.has(id))
      throw new Error('Invalid or duplicate DSH native identity');
    sessions.set(id, artifact);
  }
  const reached = new Set<string>();
  const visit = (id: string, parent?: string): void => {
    const artifact = sessions.get(id);
    if (!artifact || reached.has(id) || artifact.header.parentSession !== parent)
      throw new Error('Invalid DSH reachable lineage');
    reached.add(id);
    for (const event of artifact.events.slice(artifact.inheritedEventCount)) {
      if (event.type !== 'subagent/catalog') continue;
      const child = event.data as { version?: number; mode?: string; childId?: string };
      if (
        child.version !== 0 ||
        child.mode !== 'continuable' ||
        !NATIVE_ID.test(child.childId ?? '')
      )
        throw new Error('Unsupported DSH child catalog');
      visit(child.childId!, id);
    }
  };
  visit(rootId);
  if (reached.size !== sessions.size) throw new Error('Unrelated DSH native files');
}

export function assertDshCheckpointInstructions(checkpoint: SdkCheckpoint, system: string): void {
  decodeCheckpoint(checkpoint);
  if (checkpoint.instructionsSha256 !== digest(system))
    throw new Error('DSH instructions differ from checkpoint');
}

export function captureCheckpoint(
  threadId: string,
  system: string,
  nativeFiles: Record<string, Buffer>,
  model?: string,
): SdkCheckpoint {
  const instructionsSha256 = digest(system);
  const files = {
    ...nativeFiles,
    'orca.json': Buffer.from(
      JSON.stringify({
        profile: PROFILE,
        sdkVersion: DSH_SDK_VERSION,
        threadId,
        instructionsSha256,
        model,
      }),
    ),
  };
  const checkpoint: SdkCheckpoint = {
    version: 1,
    format: 'dsh_sdk',
    sdkVersion: DSH_SDK_VERSION,
    threadId,
    instructionsSha256,
    files: Object.fromEntries(
      Object.entries(files).map(([name, data]) => [name, data.toString('base64')]),
    ),
  };
  decodeCheckpoint(checkpoint);
  return checkpoint;
}
