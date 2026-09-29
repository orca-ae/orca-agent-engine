// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface SkillBundleInputFile {
  path: string;
  content: Buffer;
  mode?: number;
  mimeType?: string | null;
}

export interface SkillBundleManifestEntry {
  path: string;
  sizeBytes: number;
  sha256: string;
  mode: number;
  mimeType: string | null;
}

export interface SkillBundleRecord {
  /** SHA-256 of the exact canonical envelope bytes stored by the backend. */
  sha256: string;
  /** Byte length of the exact canonical envelope stored by the backend. */
  sizeBytes: number;
  files: SkillBundleManifestEntry[];
}

export interface SkillBundleFile extends SkillBundleManifestEntry {
  content: Buffer;
}

export interface SkillBundle {
  record: SkillBundleRecord;
  files: SkillBundleFile[];
}

export class SkillBundleValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SkillBundleValidationError';
  }
}

export class SkillBundleIntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SkillBundleIntegrityError';
  }
}

export class SkillBundleNotFoundError extends Error {
  constructor(
    readonly workspaceId: string,
    readonly versionId: string,
    readonly sha256: string,
  ) {
    super(`skill bundle not found: ${workspaceId}/${versionId}/${sha256}`);
    this.name = 'SkillBundleNotFoundError';
  }
}
