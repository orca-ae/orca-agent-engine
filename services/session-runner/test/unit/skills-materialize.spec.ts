// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the colocated runner's Skill materializer — the parse + the native
// `--plugin-dir` plugin layout it writes from the owner pod's skills push.

import { afterEach, describe, expect, it } from 'vitest';
import { chmod, mkdtemp, readFile, readdir, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  RUNNER_SKILLS_PLUGIN_NAME,
  SkillsPushParseError,
  materializeSkillsPlugin,
  parseSkillsPushBody,
} from '../../src/skills-materialize.js';

const DIR = 'skills-plugin';

interface FileSpec {
  skill: string;
  path: string;
  mode?: number;
  content: string;
}

/** Build a skills push body (one manifest line + one line per file). */
function pushBody(skills: string[], files: FileSpec[]): Uint8Array {
  const lines: string[] = [JSON.stringify({ type: 'skills_manifest', dir: DIR, skills })];
  for (const f of files) {
    lines.push(
      JSON.stringify({
        type: 'skill_file',
        skill: f.skill,
        path: f.path,
        mode: f.mode ?? 0o644,
        mime_type: null,
        content_base64: Buffer.from(f.content).toString('base64'),
      }),
    );
  }
  return new TextEncoder().encode(`${lines.join('\n')}\n`);
}

const created: string[] = [];
async function freshWorkspace(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'orca-skills-mat-'));
  created.push(dir);
  return dir;
}

afterEach(async () => {
  // The materializer hardens the tree read-only; loosen before removing.
  for (const dir of created.splice(0)) {
    await chmodTree(dir).catch(() => undefined);
    await rm(dir, { recursive: true, force: true }).catch(() => undefined);
  }
});

async function chmodTree(dir: string): Promise<void> {
  await chmod(dir, 0o700).catch(() => undefined);
  const entries = await readdir(dir, { withFileTypes: true }).catch(() => []);
  for (const e of entries) {
    const p = join(dir, e.name);
    if (e.isDirectory()) await chmodTree(p);
    else await chmod(p, 0o600).catch(() => undefined);
  }
}

describe('parseSkillsPushBody', () => {
  it('parses a well-formed push (manifest + files)', () => {
    const push = parseSkillsPushBody(
      pushBody(
        ['alpha'],
        [
          { skill: 'alpha', path: 'SKILL.md', content: '# Alpha\n' },
          { skill: 'alpha', path: 'scripts/run.py', mode: 0o755, content: 'print(1)\n' },
        ],
      ),
    );
    expect(push.dir).toBe(DIR);
    expect(push.skills).toEqual(['alpha']);
    expect(push.files.map((f) => f.path).sort()).toEqual(['SKILL.md', 'scripts/run.py']);
  });

  it('rejects an empty body, a non-manifest first line, and an unsafe dir', () => {
    expect(() => parseSkillsPushBody(new TextEncoder().encode(''))).toThrow(SkillsPushParseError);
    expect(() =>
      parseSkillsPushBody(new TextEncoder().encode(`${JSON.stringify({ type: 'skill_file' })}\n`)),
    ).toThrow(/not a skills manifest/);
    const badDir = new TextEncoder().encode(
      `${JSON.stringify({ type: 'skills_manifest', dir: '../escape', skills: [] })}\n`,
    );
    expect(() => parseSkillsPushBody(badDir)).toThrow(/dir is unsafe/);
  });

  it('rejects a traversal file path, an unknown skill, and a skill with no files', () => {
    expect(() =>
      parseSkillsPushBody(
        pushBody(['alpha'], [{ skill: 'alpha', path: '../escape', content: 'x' }]),
      ),
    ).toThrow(/unsafe/);
    expect(() =>
      parseSkillsPushBody(pushBody(['alpha'], [{ skill: 'beta', path: 'SKILL.md', content: 'x' }])),
    ).toThrow(/unknown skill/);
    expect(() => parseSkillsPushBody(pushBody(['alpha'], []))).toThrow(/no files/);
  });
});

describe('materializeSkillsPlugin', () => {
  it('writes the native --plugin-dir layout (.claude-plugin/plugin.json + skills/<name>/…)', async () => {
    const workspaceDir = await freshWorkspace();
    const push = parseSkillsPushBody(
      pushBody(
        ['alpha'],
        [
          { skill: 'alpha', path: 'SKILL.md', content: '# Alpha\n' },
          { skill: 'alpha', path: 'references/spec.md', content: 'spec\n' },
        ],
      ),
    );
    const pluginDir = await materializeSkillsPlugin({ workspaceDir, push });
    expect(pluginDir).toBe(join(workspaceDir, DIR));

    // The REQUIRED manifest with a kebab name.
    const manifest = JSON.parse(
      await readFile(join(pluginDir, '.claude-plugin', 'plugin.json'), 'utf8'),
    );
    expect(manifest.name).toBe(RUNNER_SKILLS_PLUGIN_NAME);
    expect(manifest.name).not.toContain(' ');

    // The Skill files under skills/<name>/…
    expect(await readFile(join(pluginDir, 'skills', 'alpha', 'SKILL.md'), 'utf8')).toBe(
      '# Alpha\n',
    );
    expect(
      await readFile(join(pluginDir, 'skills', 'alpha', 'references', 'spec.md'), 'utf8'),
    ).toBe('spec\n');
  });

  it('applies the read-only mode policy (files 0o444/0o555 by exec bit, dirs 0o555)', async () => {
    const workspaceDir = await freshWorkspace();
    const push = parseSkillsPushBody(
      pushBody(
        ['alpha'],
        [
          { skill: 'alpha', path: 'SKILL.md', content: '# Alpha\n' },
          { skill: 'alpha', path: 'scripts/run.py', mode: 0o755, content: 'print(1)\n' },
        ],
      ),
    );
    const pluginDir = await materializeSkillsPlugin({ workspaceDir, push });
    const mode = async (p: string): Promise<number> => (await stat(p)).mode & 0o777;
    expect(await mode(join(pluginDir, 'skills', 'alpha', 'SKILL.md'))).toBe(0o444);
    expect(await mode(join(pluginDir, 'skills', 'alpha', 'scripts', 'run.py'))).toBe(0o555);
    expect(await mode(join(pluginDir, 'skills', 'alpha', 'scripts'))).toBe(0o555);
    expect(await mode(join(pluginDir, 'skills', 'alpha'))).toBe(0o555);
    expect(await mode(join(pluginDir, '.claude-plugin', 'plugin.json'))).toBe(0o444);
    expect(await mode(pluginDir)).toBe(0o555);
  });

  it('delete-and-rebuilds a prior (hardened) tree — newest-wins, stale files removed', async () => {
    const workspaceDir = await freshWorkspace();
    await materializeSkillsPlugin({
      workspaceDir,
      push: parseSkillsPushBody(
        pushBody(
          ['alpha', 'beta'],
          [
            { skill: 'alpha', path: 'SKILL.md', content: 'first-alpha\n' },
            { skill: 'beta', path: 'SKILL.md', content: 'first-beta\n' },
          ],
        ),
      ),
    });
    // A re-delivery with only `alpha`, new content — must replace the whole tree.
    const pluginDir = await materializeSkillsPlugin({
      workspaceDir,
      push: parseSkillsPushBody(
        pushBody(['alpha'], [{ skill: 'alpha', path: 'SKILL.md', content: 'second-alpha\n' }]),
      ),
    });
    expect(await readFile(join(pluginDir, 'skills', 'alpha', 'SKILL.md'), 'utf8')).toBe(
      'second-alpha\n',
    );
    // The stale `beta` skill is gone (delete-and-rebuild, not merge).
    await expect(stat(join(pluginDir, 'skills', 'beta'))).rejects.toThrow();
  });
});
