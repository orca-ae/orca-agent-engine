// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { posix as path } from 'node:path';
import type { SkillBundle, SkillBundleManifestEntry, SkillStore } from '@orca/skill-store';
import type { SkillDescriptor } from '../../clients/registry.js';
import type { SandboxFileMode, SandboxHandle } from '../sandbox-runtime.js';

export const SKILLS_ROOT = '/workspace/skills';

interface MaterializeSkillsInput {
  workspaceId: string;
  sandbox: SandboxHandle;
  skillStore: SkillStore | undefined;
  skills: readonly SkillDescriptor[];
}

/**
 * Copy exact Registry-pinned Skill bundles through the trusted Harness into
 * the sandbox. The sandbox receives only verified bytes, never object-store
 * credentials or object keys.
 */
export async function materializeSkills(input: MaterializeSkillsInput): Promise<void> {
  // Sandbox images are an untrusted input to the execution snapshot. Remove
  // any baked-in or left-over files before deciding whether this Session has
  // Skills, so only exact Registry pins can ever populate the reserved tree.
  await input.sandbox.files.delete(SKILLS_ROOT);
  if (input.skills.length === 0) return;
  if (!input.skillStore) {
    throw new Error('SkillStore is unavailable');
  }
  if (input.skills.length > 500) {
    throw new Error('skill binding count exceeds the 500 binding limit');
  }

  const materializations = uniqueMaterializations(input.skills);
  const materializationPinByName = new Map(
    materializations.map((descriptor) => [descriptor.name, exactPinKey(descriptor)]),
  );
  const materializedNames = new Set<string>();
  let wroteSkillTree = false;

  try {
    // Process one exact pin at a time. SkillStore.open returns a fully decoded
    // bundle, so retaining all 500 bundles here would otherwise permit a
    // multi-gigabyte Harness memory spike.
    for (const descriptor of uniqueExactPins(input.skills)) {
      let bundle: SkillBundle;
      try {
        bundle = await input.skillStore.open(
          input.workspaceId,
          descriptor.id,
          descriptor.package_sha256,
        );
      } catch (error) {
        throw new Error(
          `failed to open skill bundle ${descriptor.id}@${descriptor.version_identifier}: ${errorMessage(error)}`,
          { cause: error },
        );
      }
      validateBundle(descriptor, bundle);

      if (
        materializationPinByName.get(descriptor.name) === exactPinKey(descriptor) &&
        !materializedNames.has(descriptor.name)
      ) {
        // Mark the tree dirty before the first write so a write/chmod failure
        // also removes a partially materialized bundle.
        wroteSkillTree = true;
        await writeBundle(input.sandbox, descriptor, bundle);
        materializedNames.add(descriptor.name);
      }
    }
    await chmodMany(input.sandbox, SKILLS_ROOT, [{ path: SKILLS_ROOT, mode: 0o555 }]);
  } catch (error) {
    if (wroteSkillTree) {
      try {
        await input.sandbox.files.delete(SKILLS_ROOT);
      } catch (cleanupError) {
        throw new AggregateError(
          [error, cleanupError],
          `skill materialization failed and ${SKILLS_ROOT} cleanup also failed`,
        );
      }
    }
    throw error;
  }
}

async function writeBundle(
  sandbox: SandboxHandle,
  descriptor: SkillDescriptor,
  bundle: SkillBundle,
): Promise<void> {
  const skillRoot = `${SKILLS_ROOT}/${descriptor.name}`;
  for (const file of bundle.files) {
    await sandbox.files.write(`${skillRoot}/${file.path}`, file.content);
  }
  await chmodMany(sandbox, skillRoot, [
    ...bundle.files.map(
      (file): SandboxFileMode => ({
        path: `${skillRoot}/${file.path}`,
        mode: (file.mode & 0o111) !== 0 ? 0o555 : 0o444,
      }),
    ),
    ...skillDirectories(skillRoot, bundle.files).map(
      (directory): SandboxFileMode => ({ path: directory, mode: 0o555 }),
    ),
  ]);
}

async function chmodMany(
  sandbox: SandboxHandle,
  root: string,
  entries: readonly SandboxFileMode[],
): Promise<void> {
  if (sandbox.files.chmodMany) {
    await sandbox.files.chmodMany(root, entries);
    return;
  }
  for (const entry of entries) {
    await sandbox.files.chmod(entry.path, entry.mode);
  }
}

function uniqueMaterializations(skills: readonly SkillDescriptor[]): SkillDescriptor[] {
  const digestByName = new Map<string, string>();
  const descriptorByName = new Map<string, SkillDescriptor>();
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

function uniqueExactPins(skills: readonly SkillDescriptor[]): SkillDescriptor[] {
  const descriptors = new Map<string, SkillDescriptor>();
  for (const descriptor of skills) {
    descriptors.set(
      exactPinKey(descriptor),
      descriptors.get(exactPinKey(descriptor)) ?? descriptor,
    );
  }
  return [...descriptors.values()];
}

function exactPinKey(descriptor: SkillDescriptor): string {
  return `${descriptor.id}:${descriptor.package_sha256}`;
}

export function validateSkillDescriptor(descriptor: SkillDescriptor): void {
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
  if (!/^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/.test(descriptor.name)) {
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

export function assertMountPathOutsideSkillsRoot(mountPath: string): void {
  if (!path.isAbsolute(mountPath)) {
    throw new Error(`resource mount path must be absolute: ${mountPath}`);
  }
  if (
    [...mountPath].some((character) => {
      const codePoint = character.codePointAt(0)!;
      return codePoint <= 0x1f || codePoint === 0x7f;
    })
  ) {
    throw new Error('resource mount path contains control characters');
  }
  const normalized = normalizeAbsolutePath(mountPath);
  if (pathsOverlap(normalized, SKILLS_ROOT)) {
    throw new Error(
      `resource mount path overlaps reserved Skill root ${SKILLS_ROOT}: ${mountPath}`,
    );
  }
}

/**
 * Resolve both sides through existing sandbox symlinks before a resource
 * write or mount. Syntactic validation alone cannot detect an image-provided
 * alias such as `/mnt/input -> /workspace/skills`.
 */
export async function assertCanonicalPathOutsideSkillsRoot(
  sandbox: SandboxHandle,
  targetPath: string,
): Promise<void> {
  assertMountPathOutsideSkillsRoot(targetPath);
  if (!sandbox.canonicalizePathForPolicy) {
    throw new Error('sandbox handle cannot canonicalize filesystem paths');
  }

  const lexicalTarget = normalizeAbsolutePath(targetPath);
  const canonicalTarget = normalizeCanonicalPath(
    await sandbox.canonicalizePathForPolicy(lexicalTarget),
    targetPath,
  );
  const canonicalSkillsRoot = normalizeCanonicalPath(
    await sandbox.canonicalizePathForPolicy(SKILLS_ROOT),
    SKILLS_ROOT,
  );

  for (const candidate of new Set([lexicalTarget, canonicalTarget])) {
    for (const reservedRoot of new Set([SKILLS_ROOT, canonicalSkillsRoot])) {
      if (pathsOverlap(candidate, reservedRoot)) {
        throw new Error(
          `filesystem path resolves across reserved Skill root ${SKILLS_ROOT}: ${targetPath} -> ${canonicalTarget}`,
        );
      }
    }
  }
}

function normalizeCanonicalPath(value: string, requestedPath: string): string {
  if (!path.isAbsolute(value)) {
    throw new Error(
      `sandbox returned a non-absolute canonical path for ${requestedPath}: ${value}`,
    );
  }
  return normalizeAbsolutePath(value);
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

function validateBundle(descriptor: SkillDescriptor, bundle: SkillBundle): void {
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
    validateBundlePath(file.path, descriptor.name);
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

function validateBundlePath(filePath: string, skillName: string): void {
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

function assertManifestEntry(
  manifest: SkillBundleManifestEntry,
  file: SkillBundleManifestEntry,
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

function skillDirectories(skillRoot: string, files: readonly SkillBundleManifestEntry[]): string[] {
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

function sha256(content: Buffer): string {
  return createHash('sha256').update(content).digest('hex');
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
