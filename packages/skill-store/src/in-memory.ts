// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  cloneBundleRecord,
  cloneSkillBundle,
  decodeSkillBundle,
  encodeSkillBundle,
} from './codec.js';
import { skillBundleKey } from './key.js';
import type { SkillStore } from './store.js';
import {
  SkillBundleIntegrityError,
  type SkillBundle,
  type SkillBundleInputFile,
  SkillBundleNotFoundError,
  type SkillBundleRecord,
} from './types.js';

/** Heap-backed implementation intended for tests and local development. */
export class InMemorySkillStore implements SkillStore {
  private readonly objects = new Map<string, Buffer>();

  async put(
    workspaceId: string,
    versionId: string,
    files: SkillBundleInputFile[],
  ): Promise<SkillBundleRecord> {
    const encoded = encodeSkillBundle(files);
    const key = skillBundleKey('', workspaceId, versionId, encoded.bundle.record.sha256);
    const existing = this.objects.get(key);
    if (existing && !existing.equals(encoded.bytes)) {
      throw new SkillBundleIntegrityError(`digest collision at ${key}`);
    }
    if (!existing) this.objects.set(key, Buffer.from(encoded.bytes));
    return cloneBundleRecord(encoded.bundle.record);
  }

  async open(workspaceId: string, versionId: string, sha256: string): Promise<SkillBundle> {
    const key = skillBundleKey('', workspaceId, versionId, sha256);
    const bytes = this.objects.get(key);
    if (!bytes) throw new SkillBundleNotFoundError(workspaceId, versionId, sha256);
    return cloneSkillBundle(decodeSkillBundle(Buffer.from(bytes), sha256));
  }

  async delete(workspaceId: string, versionId: string, sha256: string): Promise<void> {
    this.objects.delete(skillBundleKey('', workspaceId, versionId, sha256));
  }

  async close(): Promise<void> {
    this.objects.clear();
  }

  /** Test helper exposing only the number of immutable objects. */
  size(): number {
    return this.objects.size;
  }
}
