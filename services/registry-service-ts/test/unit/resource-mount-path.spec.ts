// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  RESERVED_SKILLS_ROOT,
  validateResourceMountPath,
} from '../../src/domain/resource-mount-path.js';

describe('Session resource mount paths', () => {
  it.each(['relative/path', './workspace/project', '../workspace/project'])(
    'rejects a relative path: %s',
    (mountPath) => {
      expect(validateResourceMountPath(mountPath)).toBe('mount_path must be an absolute path');
    },
  );

  it.each(['/workspace/project\u0000escape', '/workspace/project\nescape'])(
    'rejects control characters: %s',
    (mountPath) => {
      expect(validateResourceMountPath(mountPath)).toBe(
        'mount_path must not contain control characters',
      );
    },
  );

  it.each([
    RESERVED_SKILLS_ROOT,
    `${RESERVED_SKILLS_ROOT}/`,
    `${RESERVED_SKILLS_ROOT}/references/guide.md`,
    '/workspace/project/../skills',
    '/workspace',
    '/workspace/',
    '/',
  ])('rejects a path that intersects the reserved Skill root: %s', (mountPath) => {
    expect(validateResourceMountPath(mountPath)).toBe(
      `mount_path must not overlap reserved Skill root ${RESERVED_SKILLS_ROOT}`,
    );
  });

  it.each([
    '/workspace/project',
    '/workspace/skills-extra',
    '/workspace/skill',
    '/mnt/session/uploads/input.txt',
  ])('accepts a disjoint path: %s', (mountPath) => {
    expect(validateResourceMountPath(mountPath)).toBeNull();
  });
});
