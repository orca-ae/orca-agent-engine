// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The COLOCATED runner's Skill materializer — turns the owner pod's skills PUSH
// into a native `--plugin-dir` plugin under the runner workspace.
//
// The owner pod PUSHES the session's Skill bundle bytes over the tunnel (it holds
// `@orca/skill-store`; the runner does not). This module parses that NDJSON push and
// writes it to disk in the layout the `claude` CLI's `--plugin-dir` expects — VERIFIED
// against the Agent SDK bundle + the plugin-structure contract:
//
//   <pluginDir>/
//   ├── .claude-plugin/plugin.json     # REQUIRED manifest: { name: <kebab> }
//   └── skills/<skill-name>/<path>     # each Skill's bundle files (incl. SKILL.md)
//
// The CLI auto-discovers `skills/<name>/SKILL.md` under a loaded plugin. This is a
// DIFFERENT layout from the harness-server's RAW tree (`<SKILLS_ROOT>/<name>/…`, read
// by the in-process Agent SDK): the colocated CLI needs the plugin WRAPPER, synthesized
// here. The pure validators + read-only chmod PLAN come from `@orca/sandbox-runtime`'s
// shared core, which the registry push side also uses. harness-server's writer keeps its
// own copy of the validators, so a change to either copy has to be made to both. No
// `@orca/skill-store` dependency — the bytes arrive verified.

import { chmod, mkdir, readdir, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import {
  assertSafeSkillName,
  isSafeSkillName,
  planSkillBundleChmod,
  validateSkillBundlePath,
  type SandboxFileMode,
} from './sandbox/seam.js';
import {
  validateSkillBundle,
  uniqueSkillMaterializations,
  MAX_SKILL_BINDINGS,
  type MaterializableSkillDescriptor,
  type MaterializableBundle,
} from './sandbox/seam.js';

/** The synthesized plugin manifest name (kebab-case, no spaces — the CLI requires it). */
export const RUNNER_SKILLS_PLUGIN_NAME = 'orca-session-skills';

/** One file of a Skill, as parsed from the push (its skill-relative path + bytes). */
export interface ParsedSkillFile {
  /** The owning Skill's name (a safe kebab segment). */
  skill: string;
  /** The file path RELATIVE to the Skill root (e.g. `SKILL.md`, `scripts/run.py`). */
  path: string;
  /** The bundle file's POSIX mode (drives the read-only exec-bit chmod). */
  mode: number;
  /** The file's mime type, or null. */
  mimeType: string | null;
  /** The exact decoded bytes. */
  content: Buffer;
}

/** The parsed skills push: the target subdir, the Skill names, and every file. */
export interface ParsedSkillsPush {
  descriptors?: MaterializableSkillDescriptor[];
  bundles?: Record<string, MaterializableBundle['record']>;
  /** The conventional plugin SUBDIR (a single safe segment) to stage under the workspace. */
  dir: string;
  /** The Skill names in the push (from the manifest). */
  skills: string[];
  /** Every Skill file to write. */
  files: ParsedSkillFile[];
}

/** Thrown when the skills push body is malformed (the handler maps it to a 400). */
export class SkillsPushParseError extends Error {
  constructor(message: string) {
    super(`skills push parse failed: ${message}`);
    this.name = 'SkillsPushParseError';
  }
}

/**
 * Parse the skills push body (NDJSON: one manifest line, then one line per file) into
 * a {@link ParsedSkillsPush}, validating every path is safe BEFORE any write. Rejects a
 * body with no manifest line, an unsafe `dir` / Skill name / file path, a file whose
 * Skill is not named in the manifest, or a Skill named in the manifest with no files.
 */
export function parseSkillsPushBody(body: Uint8Array): ParsedSkillsPush {
  const text = Buffer.from(body).toString('utf8');
  const lines = text
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => l.length > 0);
  if (lines.length === 0) {
    throw new SkillsPushParseError('empty body');
  }

  const manifest = parseJsonObject(lines[0]!, 'manifest');
  if (manifest['type'] !== 'skills_manifest') {
    throw new SkillsPushParseError('first line is not a skills manifest');
  }
  const dir = manifest['dir'];
  if (typeof dir !== 'string' || !isSafeSkillName(dir)) {
    throw new SkillsPushParseError(`manifest dir is unsafe: ${String(dir)}`);
  }
  const skills = manifest['skills'];
  if (!Array.isArray(skills) || !skills.every((s): s is string => typeof s === 'string')) {
    throw new SkillsPushParseError('manifest skills is not a string array');
  }
  for (const name of skills) assertSafeSkillNameChecked(name);
  const skillSet = new Set(skills);

  const files: ParsedSkillFile[] = [];
  const skillsWithFiles = new Set<string>();
  for (let i = 1; i < lines.length; i += 1) {
    const obj = parseJsonObject(lines[i]!, `file line ${i}`);
    if (obj['type'] !== 'skill_file') {
      throw new SkillsPushParseError(`line ${i} is not a skill_file`);
    }
    const skill = obj['skill'];
    if (typeof skill !== 'string' || !skillSet.has(skill)) {
      throw new SkillsPushParseError(`file line ${i} names an unknown skill: ${String(skill)}`);
    }
    const path = obj['path'];
    if (typeof path !== 'string') {
      throw new SkillsPushParseError(`file line ${i} has a non-string path`);
    }
    // Reuse the shared path-safety gate — rejects `..`, absolute, backslash, NUL, etc.
    validateSkillBundlePath(path, skill);
    const contentB64 = obj['content_base64'];
    if (typeof contentB64 !== 'string') {
      throw new SkillsPushParseError(`file line ${i} has a non-string content_base64`);
    }
    const mode = obj['mode'];
    if (typeof mode !== 'number' || !Number.isInteger(mode) || mode < 0 || mode > 0o777) {
      throw new SkillsPushParseError(`file line ${i} has an invalid mode`);
    }
    const mimeType = obj['mime_type'];
    if (mimeType !== null && typeof mimeType !== 'string') {
      throw new SkillsPushParseError(`file line ${i} has an invalid mime_type`);
    }
    files.push({
      skill,
      path,
      mode,
      mimeType: mimeType ?? null,
      content: Buffer.from(contentB64, 'base64'),
    });
    skillsWithFiles.add(skill);
  }
  for (const name of skills) {
    if (!skillsWithFiles.has(name)) {
      throw new SkillsPushParseError(`skill ${name} has no files`);
    }
  }
  const result: ParsedSkillsPush = { dir, skills, files };
  if (manifest.descriptors !== undefined || manifest.bundles !== undefined) {
    if (
      !Array.isArray(manifest.descriptors) ||
      manifest.descriptors.length > MAX_SKILL_BINDINGS ||
      !manifest.bundles ||
      typeof manifest.bundles !== 'object' ||
      Array.isArray(manifest.bundles)
    )
      throw new SkillsPushParseError('invalid managed Skill manifest');
    try {
      const descriptors = uniqueSkillMaterializations(
        manifest.descriptors as MaterializableSkillDescriptor[],
      );
      if (
        new Set(skills).size !== skills.length ||
        descriptors.length !== skills.length ||
        descriptors.some((descriptor) => !skillSet.has(descriptor.name))
      )
        throw new Error('Skill descriptor names differ');
      const bundles = manifest.bundles as Record<string, MaterializableBundle['record']>;
      if (Object.keys(bundles).length !== skills.length)
        throw new Error('Skill bundle names differ');
      for (const descriptor of descriptors) {
        const record = bundles[descriptor.name];
        if (!record || !Array.isArray(record.files)) throw new Error('missing Skill bundle');
        const ownFiles = files.filter((file) => file.skill === descriptor.name);
        validateSkillBundle(descriptor, {
          record,
          files: ownFiles.map((file, index) => ({
            ...record.files[index]!,
            ...file,
            sizeBytes: file.content.length,
          })),
        });
      }
      result.descriptors = manifest.descriptors as MaterializableSkillDescriptor[];
      result.bundles = bundles;
    } catch (error) {
      throw new SkillsPushParseError(
        error instanceof Error ? error.message : 'Skill validation failed',
      );
    }
  }
  return result;
}

/**
 * Materialize a parsed skills push into a native `--plugin-dir` plugin under
 * `workspaceDir`, returning the absolute plugin dir (the value the loop records as
 * `skills_plugin_dir`).
 *
 * DELETE-AND-REBUILD (idempotent, newest-wins): a prior tree — possibly hardened
 * read-only from an earlier delivery — is chmod'd writable then removed before the fresh
 * tree is written, so a re-delivery replaces it cleanly. After writing, the shared
 * chmod PLAN hardens the tree read-only (files 0o444/0o555 by exec bit, directories
 * 0o555), applied DEEPEST-FIRST so each parent stays writable until its own turn.
 */
export async function materializeSkillsPlugin(input: {
  workspaceDir: string;
  push: ParsedSkillsPush;
}): Promise<string> {
  const pluginDir = join(input.workspaceDir, input.push.dir);

  // Newest-wins: remove any prior (possibly read-only) tree before rebuilding.
  await removeTreeRobust(pluginDir);
  await mkdir(pluginDir, { recursive: true });

  // The REQUIRED plugin manifest — `.claude-plugin/plugin.json` with a kebab name.
  const manifestDir = join(pluginDir, '.claude-plugin');
  await mkdir(manifestDir, { recursive: true });
  await writeFile(
    join(manifestDir, 'plugin.json'),
    `${JSON.stringify({
      name: RUNNER_SKILLS_PLUGIN_NAME,
      description: 'Orca session skills (materialized by the session runner).',
      version: '0.0.0',
    })}\n`,
  );

  // The Skill files at `skills/<name>/<path>`.
  const skillsRoot = join(pluginDir, 'skills');
  for (const file of input.push.files) {
    const target = join(skillsRoot, file.skill, file.path);
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, file.content);
  }

  await applyReadOnlyPlan(pluginDir, skillsRoot, manifestDir, input.push);
  return pluginDir;
}

/**
 * Compute + apply the read-only chmod plan for the whole plugin tree, deepest-first.
 *
 * Per-Skill file + directory modes come from the shared `planSkillBundleChmod` (the
 * SAME plan the harness writer applies), plus the plugin wrapper entries (`plugin.json`
 * 0o444; `.claude-plugin`, `skills`, and the plugin root 0o555). Every entry is applied
 * in DESCENDING path depth so a directory is hardened only after all its descendants.
 */
async function applyReadOnlyPlan(
  pluginDir: string,
  skillsRoot: string,
  manifestDir: string,
  push: ParsedSkillsPush,
): Promise<void> {
  const entries: SandboxFileMode[] = [];
  for (const name of push.skills) {
    const skillRoot = join(skillsRoot, name);
    const files = push.files
      .filter((f) => f.skill === name)
      .map((f) => ({ path: f.path, mode: f.mode }));
    entries.push(...planSkillBundleChmod(skillRoot, files));
  }
  entries.push(
    { path: join(manifestDir, 'plugin.json'), mode: 0o444 },
    { path: manifestDir, mode: 0o555 },
    { path: skillsRoot, mode: 0o555 },
    { path: pluginDir, mode: 0o555 },
  );
  // Deepest-first so a parent stays writable until every child under it is hardened.
  entries.sort((a, b) => depth(b.path) - depth(a.path));
  for (const entry of entries) {
    await chmod(entry.path, entry.mode);
  }
}

/**
 * Remove a directory tree that may have been hardened read-only by an earlier
 * materialization. `fs.rm` cannot unlink entries inside a 0o555 directory, so every
 * directory is first chmod'd writable (0o700) top-down, then the tree is removed. A
 * non-existent tree is a clean no-op.
 */
async function removeTreeRobust(dir: string): Promise<void> {
  await chmodTreeWritable(dir);
  await rm(dir, { recursive: true, force: true });
}

/** Recursively chmod a tree to owner-writable (dirs 0o700, files 0o600). Best-effort. */
async function chmodTreeWritable(dir: string): Promise<void> {
  let entries;
  try {
    // A dir must be writable+executable for its children to be unlinked/traversed.
    await chmod(dir, 0o700);
    entries = await readdir(dir, { withFileTypes: true });
  } catch {
    return; // does not exist / not a dir — nothing to loosen.
  }
  for (const entry of entries) {
    const child = join(dir, entry.name);
    if (entry.isDirectory()) {
      await chmodTreeWritable(child);
    } else {
      await chmod(child, 0o600).catch(() => undefined);
    }
  }
}

function depth(path: string): number {
  return path.split('/').length;
}

function parseJsonObject(line: string, label: string): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    throw new SkillsPushParseError(`${label} is not valid JSON`);
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new SkillsPushParseError(`${label} is not a JSON object`);
  }
  return parsed as Record<string, unknown>;
}

function assertSafeSkillNameChecked(name: string): void {
  try {
    assertSafeSkillName(name);
  } catch {
    throw new SkillsPushParseError(`manifest skill name is unsafe: ${name}`);
  }
}

/** Match exact snapshot pins before filtering; never advertise bytes from a different push. */
export function selectManagedSkills(
  push: ParsedSkillsPush | undefined,
  expected: readonly MaterializableSkillDescriptor[],
  blocked: (skill: MaterializableSkillDescriptor) => boolean,
): ParsedSkillsPush {
  if (expected.length === 0 && push === undefined)
    return { dir: 'skills-plugin', skills: [], files: [], descriptors: [], bundles: {} };
  if (!push?.descriptors || !push.bundles) throw new Error('managed Skills were not delivered');
  const signature = (skill: MaterializableSkillDescriptor): string =>
    JSON.stringify([
      skill.id,
      skill.skill_id,
      skill.source,
      skill.version_identifier,
      skill.name,
      skill.description,
      skill.entrypoint,
      skill.package_sha256,
      skill.package_size_bytes,
    ]);
  // Compare the complete binding multiset before collapsing shared physical trees.
  // Two pins can use the same name/digest but differ in identity or catalog metadata.
  const pinned = expected.map(signature).sort();
  const delivered = push.descriptors.map(signature).sort();
  if (
    pinned.length !== delivered.length ||
    pinned.some((value, index) => value !== delivered[index])
  )
    throw new Error('managed Skill snapshot pins differ from delivered bytes');
  const descriptors = uniqueSkillMaterializations(expected).filter((skill) => !blocked(skill));
  const names = new Set(descriptors.map((skill) => skill.name));
  return {
    ...push,
    descriptors,
    skills: [...names],
    files: push.files.filter((file) => names.has(file.skill)),
  };
}
