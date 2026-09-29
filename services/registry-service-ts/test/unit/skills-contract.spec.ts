// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { skillsContract } from '../../src/contracts/skills.contract.js';

describe('skills contract', () => {
  it('allows a Skill to remain after its final public version is deleted', () => {
    const parsed = skillsContract.get.responses[200].safeParse({
      id: 'skill_test',
      created_at: '2026-07-29T00:00:00.000Z',
      display_title: null,
      latest_version: null,
      source: 'custom',
      type: 'skill',
      updated_at: '2026-07-29T00:00:01.000Z',
    });

    expect(parsed.success).toBe(true);
  });

  it('declares the official parent-delete precondition and pinned-session conflict', () => {
    expect(skillsContract.delete.responses).toHaveProperty('400');
    expect(skillsContract.delete.responses).toHaveProperty('409');
    expect(skillsContract.deleteVersion.responses).not.toHaveProperty('409');
    expect(skillsContract.create.responses).toHaveProperty('413');
    expect(skillsContract.getVersionContent.responses).toHaveProperty('500');
  });

  it('requires has_more in both official list envelopes', () => {
    const skillPage = skillsContract.list.responses[200].safeParse({
      data: [],
      has_more: false,
      next_page: null,
    });
    const versionPage = skillsContract.listVersions.responses[200].safeParse({
      data: [],
      has_more: false,
      next_page: null,
    });

    expect(skillPage.success).toBe(true);
    expect(versionPage.success).toBe(true);
    expect(
      skillsContract.list.responses[200].safeParse({ data: [], next_page: null }).success,
    ).toBe(false);
    expect(
      skillsContract.listVersions.responses[200].safeParse({ data: [], next_page: null }).success,
    ).toBe(false);
  });
});
