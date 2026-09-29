// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { posix as path } from 'node:path';

export const RESERVED_SKILLS_ROOT = '/workspace/skills';

const RESERVED_SKILLS_ROOT_ERROR = `mount_path must not overlap reserved Skill root ${RESERVED_SKILLS_ROOT}`;
const ABSOLUTE_MOUNT_PATH_ERROR = 'mount_path must be an absolute path';
const CONTROL_CHARACTER_MOUNT_PATH_ERROR = 'mount_path must not contain control characters';

/**
 * Reject a resource mount that could replace, hide, or populate the trusted
 * Skill tree. Normalizing first closes `..`, duplicate-slash, and trailing
 * slash aliases of the same sandbox path.
 */
export function validateResourceMountPath(mountPath: string): string | null {
  if (!path.isAbsolute(mountPath)) return ABSOLUTE_MOUNT_PATH_ERROR;
  if (
    [...mountPath].some((character) => {
      const codePoint = character.codePointAt(0)!;
      return codePoint <= 0x1f || codePoint === 0x7f;
    })
  ) {
    return CONTROL_CHARACTER_MOUNT_PATH_ERROR;
  }
  const normalized = normalizeAbsolutePath(mountPath);
  return pathsOverlap(normalized, RESERVED_SKILLS_ROOT) ? RESERVED_SKILLS_ROOT_ERROR : null;
}

function normalizeAbsolutePath(value: string): string {
  const normalized = path.normalize(value);
  return normalized.length > 1 ? normalized.replace(/\/+$/, '') : normalized;
}

function pathsOverlap(left: string, right: string): boolean {
  return isWithin(left, right) || isWithin(right, left);
}

function isWithin(candidate: string, root: string): boolean {
  return root === '/'
    ? candidate.startsWith('/')
    : candidate === root || candidate.startsWith(`${root}/`);
}
