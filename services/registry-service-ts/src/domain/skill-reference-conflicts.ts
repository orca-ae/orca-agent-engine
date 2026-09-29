// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface ResolvedSkillReference {
  id: string;
  name: string;
  packageSha256: string;
}

export type SkillReferenceConflict =
  | { type: 'duplicate_version'; skillVersionId: string }
  | { type: 'different_packages'; name: string };

export function findSkillReferenceConflict(
  versions: readonly ResolvedSkillReference[],
  packageByName = new Map<string, string>(),
): SkillReferenceConflict | null {
  const seenVersionIds = new Set<string>();
  for (const version of versions) {
    if (seenVersionIds.has(version.id)) {
      return { type: 'duplicate_version', skillVersionId: version.id };
    }
    seenVersionIds.add(version.id);

    const existingDigest = packageByName.get(version.name);
    if (existingDigest !== undefined && existingDigest !== version.packageSha256) {
      return { type: 'different_packages', name: version.name };
    }
    packageByName.set(version.name, version.packageSha256);
  }
  return null;
}
