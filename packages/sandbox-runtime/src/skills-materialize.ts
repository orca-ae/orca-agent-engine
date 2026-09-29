// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Shared Skill-bundle materialization CORE — the pure integrity-validation +
// path-safety + chmod-plan logic the Skill write paths must agree on.
//
// Progressive Skill disclosure has TWO write paths that must never drift:
//   - the harness-server path materializes Registry-pinned bundles through a
//     `SandboxHandle` into the reserved sandbox tree; and
//   - the session-runner path receives the SAME bundle bytes pushed over the
//     runner tunnel and materializes them under the runner workspace.
// The session-runner writer and the registry PUSH side (which opens bundles from
// `@orca/skill-store` and streams their bytes) use this core. harness-server does
// not: `services/harness-server/src/sandbox/skills/materialize.ts` keeps its own
// copy of these validators and imports only `composeSkillsCatalog` from here, so a
// change to either copy has to be made to both. The core has NO `@orca/skill-store`
// dependency (the bundle/descriptor shapes are declared STRUCTURALLY so a caller
// passes its own `SkillBundle` / `SkillDescriptor` in): the runner links
// `@orca/sandbox-runtime` but MUST NOT link `@orca/skill-store`, so the core cannot
// reference it either.
//
// Owns: descriptor validation, bundle integrity validation, per-file path safety,
// the name-collision / exact-pin dedup, and the read-only chmod PLAN (a list of
// `{path, mode}` a caller then applies through whatever primitive it has — a
// `SandboxHandle.chmodMany` on the harness side, `node:fs.chmod` on the runner
// side). It does NOT perform I/O — the writer stays with each caller.

import { createHash } from 'node:crypto';
import { posix as path } from 'node:path';
import { SKILLS_ROOT } from './write-policy.js';
import type { SandboxFileMode } from './sandbox-runtime.js';

/**
 * The maximum number of Skill bundle bindings a single materialization may carry.
 * A hard ceiling so a runaway binding set cannot spike memory while bundles are
 * decoded one at a time. Shared so every write path enforces the same cap.
 */
export const MAX_SKILL_BINDINGS = 500;

/**
 * The exact immutable Skill bundle pin one agent references in a Session — the
 * STRUCTURAL shape the validators read. Declared here (not imported) so the core
 * takes no dependency on the registry contract or the harness client type; the
 * harness-server's `SkillDescriptor` and the registry's `PreparedSkillDescriptor`
 * are both structurally assignable to it.
 */
export interface MaterializableSkillDescriptor {
  id: string;
  skill_id: string;
  source: 'anthropic' | 'custom';
  version_identifier: string;
  name: string;
  description: string;
  entrypoint: 'SKILL.md';
  package_sha256: string;
  package_size_bytes: number;
}

/** One manifest entry of a decoded bundle (the STRUCTURAL `@orca/skill-store` shape). */
export interface MaterializableBundleManifestEntry {
  path: string;
  sizeBytes: number;
  sha256: string;
  mode: number;
  mimeType: string | null;
}

/** One decoded bundle file: a manifest entry plus its exact bytes. */
export interface MaterializableBundleFile extends MaterializableBundleManifestEntry {
  content: Buffer;
}

/** A fully decoded, integrity-checked bundle (the STRUCTURAL `SkillBundle` shape). */
export interface MaterializableBundle {
  record: {
    sha256: string;
    sizeBytes: number;
    files: MaterializableBundleManifestEntry[];
  };
  files: MaterializableBundleFile[];
}

/**
 * The de-duplicated set of Skill NAMES to materialize (one bundle per name), with
 * the exact-pin key each name resolves to. A name that maps to two different bundle
 * digests across the binding set is a collision and throws.
 */
export function uniqueSkillMaterializations<T extends MaterializableSkillDescriptor>(
  skills: readonly T[],
): T[] {
  const digestByName = new Map<string, string>();
  const descriptorByName = new Map<string, T>();
  const sizeByDigest = new Map<string, number>();

  for (const descriptor of skills) {
    validateSkillDescriptor(descriptor);
    const existingDigest = digestByName.get(descriptor.name);
    if (existingDigest && existingDigest !== descriptor.package_sha256) {
      throw new Error(
        `skill name collision: ${descriptor.name} resolves to multiple bundle digests`,
      );
    }
    const existingSize = sizeByDigest.get(descriptor.package_sha256);
    if (existingSize !== undefined && existingSize !== descriptor.package_size_bytes) {
      throw new Error(`skill bundle ${descriptor.package_sha256} has conflicting package sizes`);
    }
    digestByName.set(descriptor.name, descriptor.package_sha256);
    sizeByDigest.set(descriptor.package_sha256, descriptor.package_size_bytes);
    descriptorByName.set(descriptor.name, descriptorByName.get(descriptor.name) ?? descriptor);
  }
  return [...descriptorByName.values()];
}

/** The de-duplicated set of EXACT pins (id + digest) across a binding set. */
export function uniqueSkillExactPins<T extends MaterializableSkillDescriptor>(
  skills: readonly T[],
): T[] {
  const descriptors = new Map<string, T>();
  for (const descriptor of skills) {
    descriptors.set(
      skillExactPinKey(descriptor),
      descriptors.get(skillExactPinKey(descriptor)) ?? descriptor,
    );
  }
  return [...descriptors.values()];
}

/** The exact-pin key of a descriptor: its version id joined with its bundle digest. */
export function skillExactPinKey(descriptor: MaterializableSkillDescriptor): string {
  return `${descriptor.id}:${descriptor.package_sha256}`;
}

/** Validate a Skill descriptor's identity + package fields (throws on the first fault). */
export function validateSkillDescriptor(descriptor: MaterializableSkillDescriptor): void {
  for (const [field, value] of [
    ['id', descriptor.id],
    ['skill_id', descriptor.skill_id],
    ['version_identifier', descriptor.version_identifier],
    ['description', descriptor.description],
  ] as const) {
    if (typeof value !== 'string' || value.length === 0) {
      throw new Error(`skill descriptor ${field} must be a non-empty string`);
    }
  }
  if (descriptor.source !== 'custom' && descriptor.source !== 'anthropic') {
    throw new Error(`skill descriptor source is invalid for ${descriptor.id}`);
  }
  if (!isSafeSkillName(descriptor.name)) {
    throw new Error(`skill descriptor name is unsafe: ${descriptor.name}`);
  }
  if (descriptor.entrypoint !== 'SKILL.md') {
    throw new Error(`skill descriptor entrypoint is unsupported for ${descriptor.name}`);
  }
  if (!/^[0-9a-f]{64}$/.test(descriptor.package_sha256)) {
    throw new Error(`skill descriptor package digest is invalid for ${descriptor.name}`);
  }
  if (!Number.isSafeInteger(descriptor.package_size_bytes) || descriptor.package_size_bytes <= 0) {
    throw new Error(`skill descriptor package size is invalid for ${descriptor.name}`);
  }
}

/** Whether a Skill name is a safe kebab-case directory segment. */
export function isSafeSkillName(name: string): boolean {
  return /^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/.test(name);
}

/** Assert a Skill name is a safe kebab-case directory segment (throws otherwise). */
export function assertSafeSkillName(name: string): void {
  if (!isSafeSkillName(name)) {
    throw new Error(`skill name is unsafe: ${name}`);
  }
}

/**
 * Assert a bundle-relative file path is safe to write under a Skill root: a
 * relative POSIX path with no `..`, no absolute prefix, no backslash, no NUL, and
 * canonical (already normalized). Throws on the first violation.
 */
export function validateSkillBundlePath(filePath: string, skillName: string): void {
  if (
    filePath.length === 0 ||
    filePath === '.' ||
    filePath.includes('\0') ||
    filePath.includes('\\') ||
    path.isAbsolute(filePath) ||
    filePath.split('/').includes('..') ||
    path.normalize(filePath).replace(/^\.\//, '') !== filePath
  ) {
    throw new Error(`skill bundle path is unsafe for ${skillName}: ${filePath}`);
  }
}

/**
 * Validate a decoded bundle against its descriptor: digest + size + manifest match,
 * every file path safe + unique + collision-free, content bytes hash to their
 * manifest digest, modes in range, and the entrypoint present. Throws on the first
 * violation. This is the integrity gate the harness + registry push sides run
 * before ANY byte is written (the runner receives already-validated files).
 */
export function validateSkillBundle(
  descriptor: MaterializableSkillDescriptor,
  bundle: MaterializableBundle,
): void {
  if (bundle.record.sha256 !== descriptor.package_sha256) {
    throw new Error(`skill bundle digest mismatch for ${descriptor.name}`);
  }
  if (bundle.record.sizeBytes !== descriptor.package_size_bytes) {
    throw new Error(`skill bundle size mismatch for ${descriptor.name}`);
  }
  if (bundle.record.files.length !== bundle.files.length || bundle.record.files.length === 0) {
    throw new Error(`skill bundle manifest mismatch for ${descriptor.name}`);
  }

  const seen = new Set<string>();
  let hasEntrypoint = false;
  for (let index = 0; index < bundle.files.length; index += 1) {
    const file = bundle.files[index]!;
    const manifest = bundle.record.files[index]!;
    validateSkillBundlePath(file.path, descriptor.name);
    if (index > 0 && bundle.files[index - 1]!.path >= file.path) {
      throw new Error(`skill bundle manifest order is invalid for ${descriptor.name}`);
    }
    if (seen.has(file.path)) {
      throw new Error(`skill bundle contains duplicate path ${file.path}`);
    }
    for (const previous of seen) {
      if (file.path.startsWith(`${previous}/`) || previous.startsWith(`${file.path}/`)) {
        throw new Error(`skill bundle contains a file/directory collision at ${file.path}`);
      }
    }
    seen.add(file.path);
    assertManifestEntry(manifest, file, descriptor.name);
    if (!Buffer.isBuffer(file.content) || file.content.length !== file.sizeBytes) {
      throw new Error(`skill bundle size mismatch for ${descriptor.name}/${file.path}`);
    }
    if (sha256(file.content) !== file.sha256) {
      throw new Error(`skill bundle content digest mismatch for ${descriptor.name}/${file.path}`);
    }
    if (!Number.isInteger(file.mode) || file.mode < 0 || file.mode > 0o777) {
      throw new Error(`skill bundle mode is invalid for ${descriptor.name}/${file.path}`);
    }
    if (
      file.mimeType !== null &&
      (typeof file.mimeType !== 'string' || file.mimeType.length === 0)
    ) {
      throw new Error(`skill bundle mime type is invalid for ${descriptor.name}/${file.path}`);
    }
    if (file.path === descriptor.entrypoint) hasEntrypoint = true;
  }
  if (!hasEntrypoint) {
    throw new Error(`skill bundle ${descriptor.name} is missing ${descriptor.entrypoint}`);
  }
}

/**
 * The read-only chmod PLAN for one materialized Skill under `skillRoot`: each file
 * mapped to `0o555` when its bundle mode carries an execute bit, else `0o444`, then
 * every directory under `skillRoot` (the root itself included) mapped to `0o555`,
 * DEEPEST-FIRST so a caller applying the plan in order hardens children before
 * their parents. The caller applies the returned modes through whatever primitive
 * it owns; the plan itself is pure.
 */
export function planSkillBundleChmod(
  skillRoot: string,
  files: readonly { path: string; mode: number }[],
): SandboxFileMode[] {
  return [
    ...files.map(
      (file): SandboxFileMode => ({
        path: `${skillRoot}/${file.path}`,
        mode: (file.mode & 0o111) !== 0 ? 0o555 : 0o444,
      }),
    ),
    ...skillBundleDirectories(skillRoot, files).map(
      (directory): SandboxFileMode => ({ path: directory, mode: 0o555 }),
    ),
  ];
}

/**
 * Every directory (deepest-first) implied by a bundle's files under `skillRoot`,
 * the root included. Ordered so a chmod applied in sequence hardens the deepest
 * directory first, keeping each parent writable until its own turn.
 */
export function skillBundleDirectories(
  skillRoot: string,
  files: readonly { path: string }[],
): string[] {
  const directories = new Set<string>([skillRoot]);
  for (const file of files) {
    let parent = path.dirname(file.path);
    while (parent !== '.') {
      directories.add(`${skillRoot}/${parent}`);
      parent = path.dirname(parent);
    }
  }
  return [...directories].sort(
    (left, right) => right.split('/').length - left.split('/').length || left.localeCompare(right),
  );
}

function assertManifestEntry(
  manifest: MaterializableBundleManifestEntry,
  file: MaterializableBundleManifestEntry,
  skillName: string,
): void {
  if (
    manifest.path !== file.path ||
    manifest.sizeBytes !== file.sizeBytes ||
    manifest.sha256 !== file.sha256 ||
    manifest.mode !== file.mode ||
    manifest.mimeType !== file.mimeType
  ) {
    throw new Error(`skill bundle manifest mismatch for ${skillName}/${file.path}`);
  }
}

function sha256(content: Buffer): string {
  return createHash('sha256').update(content).digest('hex');
}

/** Shared progressive-disclosure catalog; the digest also pins native resume instructions. */
export function composeSkillsCatalog(
  agentSystem: string,
  skills: readonly MaterializableSkillDescriptor[],
): string {
  if (skills.length === 0) return agentSystem;
  for (const skill of skills) validateSkillDescriptor(skill);
  const catalog = [
    '<available_skills>',
    "The following Skills are available as read-only files. When a Skill is relevant, use the read tool to open its SKILL.md entrypoint, then read only the referenced files needed for the task. Do not assume a Skill's instructions without reading its entrypoint.",
    ...skills.map(skillCatalogRow),
    '</available_skills>',
  ].join('\n');
  return agentSystem.length > 0 ? `${agentSystem}\n\n${catalog}` : catalog;
}

/** Accept only the unchanged Agent instructions plus a subset of its pinned Skills. */
export function isPinnedSkillsCatalog(
  system: string,
  agentSystem: string,
  skills: readonly MaterializableSkillDescriptor[],
): boolean {
  const rows = new Set(
    system.slice(agentSystem.length > 0 ? agentSystem.length + 2 : 0).split('\n'),
  );
  return (
    system ===
    composeSkillsCatalog(
      agentSystem,
      skills.filter((skill) => rows.has(skillCatalogRow(skill))),
    )
  );
}

function skillCatalogRow(skill: MaterializableSkillDescriptor): string {
  return JSON.stringify({
    name: skill.name,
    description: skill.description,
    path: `${SKILLS_ROOT}/${skill.name}/${skill.entrypoint}`,
    package_sha256: skill.package_sha256,
  });
}
