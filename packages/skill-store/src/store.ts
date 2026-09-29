// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { SkillBundle, SkillBundleInputFile, SkillBundleRecord } from './types.js';

/**
 * Immutable, content-addressed storage for one pinned skill-version bundle.
 *
 * A backend stores exactly one canonical envelope object per
 * `(workspaceId, versionId, sha256)`. Repeated puts of the same logical bundle
 * are idempotent.
 */
export interface SkillStore {
  put(
    workspaceId: string,
    versionId: string,
    files: SkillBundleInputFile[],
  ): Promise<SkillBundleRecord>;
  open(workspaceId: string, versionId: string, sha256: string): Promise<SkillBundle>;
  /** Idempotent: deleting an already-absent exact object must succeed. */
  delete(workspaceId: string, versionId: string, sha256: string): Promise<void>;
  close(): Promise<void>;
}
