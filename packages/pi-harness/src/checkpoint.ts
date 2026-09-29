// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { validatePiCheckpoint, piModelApi } from '@orca/harness-catalog';
import type { SdkCheckpoint } from '@orca/sdk-harness';
import type { FileEntry } from '@earendil-works/pi-coding-agent';

export const digest = (system: string): string => createHash('sha256').update(system).digest('hex');
export interface PiHistory {
  model: string;
  modelProvider?: string;
  system: string;
  entries: FileEntry[];
}
export function decodePiCheckpoint(checkpoint: SdkCheckpoint): PiHistory {
  validatePiCheckpoint(checkpoint);
  const bytes = Buffer.from(checkpoint.files['session.json']!, 'base64');
  if (bytes.toString('base64') !== checkpoint.files['session.json'])
    throw new Error('noncanonical Pi checkpoint');
  const history = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) as PiHistory;
  const provider = history.modelProvider ?? 'openai';
  const supportedModel = typeof history.model === 'string' && piModelApi(provider, history.model);
  if (checkpoint.sdkVersion === '0.87.0' && !supportedModel && typeof history.model === 'string')
    throw new Error(
      `Pi 0.87.0 checkpoint model '${provider}/${history.model}' is unavailable in Pi 0.87.1; start a new session with a supported model`,
    );
  if (
    typeof history.model !== 'string' ||
    !supportedModel ||
    typeof history.system !== 'string' ||
    !Array.isArray(history.entries) ||
    !history.entries.length ||
    history.entries[0]?.type !== 'session' ||
    history.entries[0].version !== 3 ||
    history.entries[0].id !== checkpoint.threadId ||
    digest(history.system) !== checkpoint.instructionsSha256
  )
    throw new Error('invalid Pi history');
  const ids = new Set<string>();
  for (const entry of history.entries.slice(1)) {
    if (
      entry.type === 'session' ||
      typeof entry.id !== 'string' ||
      ids.has(entry.id) ||
      (entry.parentId !== null && !ids.has(entry.parentId))
    )
      throw new Error('invalid Pi history tree');
    ids.add(entry.id);
  }
  return history;
}
export function capturePiCheckpoint(threadId: string, history: PiHistory): SdkCheckpoint {
  const checkpoint: SdkCheckpoint = {
    version: 1,
    format: 'pi_sdk',
    sdkVersion: '0.87.1',
    threadId,
    instructionsSha256: digest(history.system),
    files: { 'session.json': Buffer.from(JSON.stringify(history)).toString('base64') },
  };
  decodePiCheckpoint(checkpoint);
  return checkpoint;
}
export function assertPiCheckpointInstructions(checkpoint: SdkCheckpoint, system: string): void {
  if (decodePiCheckpoint(checkpoint).system !== system)
    throw new Error('Pi checkpoint instructions differ from pinned Agent');
}
export function transitionPiCheckpointInstructions(
  checkpoint: SdkCheckpoint,
  system: string,
  accepts: (previous: string) => boolean,
): SdkCheckpoint {
  const history = decodePiCheckpoint(checkpoint);
  if (history.system === system) return checkpoint;
  if (!accepts(history.system))
    throw new Error('Pi instruction transition is not a pinned Skill policy change');
  // Pi projects its pinned resource loader into the next request's system sections.
  return capturePiCheckpoint(checkpoint.threadId, { ...history, system });
}
