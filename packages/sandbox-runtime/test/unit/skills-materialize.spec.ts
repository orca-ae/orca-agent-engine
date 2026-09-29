// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  composeSkillsCatalog,
  isPinnedSkillsCatalog,
  type MaterializableBundle,
  type MaterializableSkillDescriptor,
  planSkillBundleChmod,
  skillBundleDirectories,
  uniqueSkillMaterializations,
  validateSkillBundle,
  validateSkillBundlePath,
  validateSkillDescriptor,
} from '../../src/skills-materialize.js';

function descriptor(
  over: Partial<MaterializableSkillDescriptor> = {},
): MaterializableSkillDescriptor {
  const content = Buffer.from('# Skill\n');
  return {
    id: 'sklv_a',
    skill_id: 'skl_a',
    source: 'anthropic',
    version_identifier: '1',
    name: 'alpha',
    description: 'Alpha skill.',
    entrypoint: 'SKILL.md',
    package_sha256: createHash('sha256').update(content).digest('hex'),
    package_size_bytes: content.length,
    ...over,
  };
}

function bundleFor(
  d: MaterializableSkillDescriptor,
  files: Array<{ path: string; content: Buffer; mode?: number }>,
): MaterializableBundle {
  const entries = files
    .map((f) => ({
      path: f.path,
      content: f.content,
      sizeBytes: f.content.length,
      sha256: createHash('sha256').update(f.content).digest('hex'),
      mode: f.mode ?? 0o644,
      mimeType: null,
    }))
    .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
  return {
    record: {
      sha256: d.package_sha256,
      sizeBytes: d.package_size_bytes,
      files: entries.map(({ content: _c, ...m }) => m),
    },
    files: entries,
  };
}

describe('shared skills-materialize core', () => {
  it('validates a well-formed descriptor and rejects unsafe names', () => {
    expect(() => validateSkillDescriptor(descriptor())).not.toThrow();
    expect(() => validateSkillDescriptor(descriptor({ name: 'Bad Name' }))).toThrow(/unsafe/);
    expect(() =>
      validateSkillDescriptor(descriptor({ entrypoint: 'README.md' as 'SKILL.md' })),
    ).toThrow(/entrypoint/);
  });

  it('rejects unsafe bundle paths (traversal, absolute, backslash)', () => {
    for (const bad of ['../escape', '/abs', 'a\\b', '..', 'a/../b']) {
      expect(() => validateSkillBundlePath(bad, 'alpha')).toThrow(/unsafe/);
    }
    expect(() => validateSkillBundlePath('SKILL.md', 'alpha')).not.toThrow();
    expect(() => validateSkillBundlePath('references/spec.md', 'alpha')).not.toThrow();
  });

  it('validates a bundle whose content digests match the manifest', () => {
    const d = descriptor();
    const skillMd = Buffer.from('# Skill\n');
    const helper = Buffer.from('print(1)\n');
    const bundle = bundleFor(
      { ...d, package_sha256: d.package_sha256, package_size_bytes: d.package_size_bytes },
      [
        { path: 'SKILL.md', content: skillMd },
        { path: 'scripts/run.py', content: helper, mode: 0o755 },
      ],
    );
    // Re-anchor the record digest/size on the descriptor (bundle-level integrity).
    bundle.record.sha256 = d.package_sha256;
    bundle.record.sizeBytes = d.package_size_bytes;
    expect(() => validateSkillBundle(d, bundle)).not.toThrow();

    // A tampered content byte (same length) fails the per-file digest check.
    bundle.files[0]!.content = Buffer.from('# SkilX\n');
    expect(() => validateSkillBundle(d, bundle)).toThrow(/content digest/);
  });

  it('rejects a bundle missing its entrypoint', () => {
    const d = descriptor();
    const bundle = bundleFor(d, [{ path: 'notes.md', content: Buffer.from('x') }]);
    bundle.record.sha256 = d.package_sha256;
    bundle.record.sizeBytes = d.package_size_bytes;
    expect(() => validateSkillBundle(d, bundle)).toThrow(/missing SKILL\.md/);
  });

  it('plans read-only modes: exec bit -> 0o555, else 0o444, dirs 0o555 deepest-first', () => {
    const plan = planSkillBundleChmod('/plugin/skills/alpha', [
      { path: 'SKILL.md', mode: 0o644 },
      { path: 'scripts/run.py', mode: 0o755 },
    ]);
    const byPath = new Map(plan.map((e) => [e.path, e.mode]));
    expect(byPath.get('/plugin/skills/alpha/SKILL.md')).toBe(0o444);
    expect(byPath.get('/plugin/skills/alpha/scripts/run.py')).toBe(0o555);
    expect(byPath.get('/plugin/skills/alpha/scripts')).toBe(0o555);
    expect(byPath.get('/plugin/skills/alpha')).toBe(0o555);

    // Directories precede their parents (deepest-first) so an in-order apply keeps
    // each parent writable until its own chmod.
    const dirs = skillBundleDirectories('/plugin/skills/alpha', [{ path: 'a/b/c.txt' }]);
    expect(dirs.indexOf('/plugin/skills/alpha/a/b')).toBeLessThan(
      dirs.indexOf('/plugin/skills/alpha/a'),
    );
    expect(dirs.indexOf('/plugin/skills/alpha/a')).toBeLessThan(
      dirs.indexOf('/plugin/skills/alpha'),
    );
  });

  it('dedups materializations by name and rejects a name -> two-digest collision', () => {
    const a = descriptor({ name: 'alpha', id: 'sklv_a', package_sha256: 'a'.repeat(64) });
    const a2 = descriptor({ name: 'alpha', id: 'sklv_a2', package_sha256: 'a'.repeat(64) });
    expect(uniqueSkillMaterializations([a, a2])).toHaveLength(1);

    const clash = descriptor({ name: 'alpha', id: 'sklv_b', package_sha256: 'b'.repeat(64) });
    expect(() => uniqueSkillMaterializations([a, clash])).toThrow(/name collision/);
  });
});

it('accepts only exact policy-filtered catalogs from the unchanged Agent pins', () => {
  const skills = [descriptor(), descriptor({ name: 'beta' })];
  for (const base of ['', 'Pinned instructions']) {
    for (const selected of [[], [skills[0]!], [skills[1]!], skills])
      expect(isPinnedSkillsCatalog(composeSkillsCatalog(base, selected), base, skills)).toBe(true);
    expect(isPinnedSkillsCatalog(composeSkillsCatalog('changed', skills), base, skills)).toBe(
      false,
    );
    expect(
      isPinnedSkillsCatalog(
        composeSkillsCatalog(base, [descriptor({ name: 'unbound' })]),
        base,
        skills,
      ),
    ).toBe(false);
    expect(
      isPinnedSkillsCatalog(
        composeSkillsCatalog(base, [descriptor({ package_sha256: '0'.repeat(64) })]),
        base,
        skills,
      ),
    ).toBe(false);
    expect(
      isPinnedSkillsCatalog(composeSkillsCatalog(base, skills) + '\ninjected', base, skills),
    ).toBe(false);
  }
});
