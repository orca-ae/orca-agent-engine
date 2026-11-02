// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { load, CORE_SCHEMA } from 'js-yaml';

export const DSH_PROFILE_CAPABILITY = 'dsh_profile_v1' as const;
export const DSH_PROFILE_MAX_BYTES = 1024 * 1024;
export const DSH_PROFILE_FILENAMES = [
  'cordis.patch.yml',
  'package.json',
  'pnpm-lock.yaml',
  'pnpm-workspace.yaml',
] as const;
export type DshProfileFiles = Record<(typeof DSH_PROFILE_FILENAMES)[number], string>;

/** Private immutable Agent-version data; never project its files on the public API. */
export interface DshProfileDescriptor {
  version: 1;
  source_file_id: string;
  source_sha256: string;
  config_sha256: string;
  files: DshProfileFiles;
}

// Keep business modules below the image package so native peer imports find its shared runtime.
export const DSH_PROFILE_HOME = '/app/packages/dsh-harness/.orca-dsh';
export const DSH_PROFILE_DIRECTORY = DSH_PROFILE_HOME + '/profiles/orca-managed';
export const DSH_PROFILE_EXTENSION = 'orca.dsh.profile.v1';
export const DSH_PROFILE_TIMEOUT_MS = 15 * 60 * 1000;

export function validateDshProfileAnnotation(metadata: Record<string, unknown>): void {
  if (metadata.dsh_profile === undefined) return;
  if (metadata.harness !== 'dsh_sdk' || metadata.mode !== 'colocated')
    throw new Error('metadata.dsh_profile requires dsh_sdk/colocated');
  if (
    typeof metadata.dsh_profile !== 'string' ||
    !/^file_[A-Za-z0-9_-]{1,200}$/.test(metadata.dsh_profile)
  )
    throw new Error('Invalid DSH profile source File ID');
}

/** Same parser as the pinned DSH loader, with business data restricted to YAML core. */
export function parseDshProfileYaml(text: string): unknown {
  if (Buffer.byteLength(text, 'utf8') > DSH_PROFILE_MAX_BYTES)
    throw new Error('DSH profile YAML exceeds 1 MiB');
  let depth = 0;
  let nodes = 0;
  const value = load(text, {
    schema: CORE_SCHEMA,
    listener(event, raw) {
      const state = raw as unknown as { anchor: string | null };
      if (event === 'open') {
        if (++depth > 64 || ++nodes > 100_000) throw new Error('DSH profile YAML is too complex');
      } else {
        depth--;
        if (state.anchor !== null) throw new Error('DSH profile YAML anchors are forbidden');
      }
    },
  });
  const visit = (item: unknown): void => {
    if (!item || typeof item !== 'object') return;
    if (Object.hasOwn(item, '<<')) throw new Error('DSH profile YAML merge keys are forbidden');
    // The native loader also recognizes this plain-object form without a YAML tag.
    if (Object.hasOwn(item, '__jsExpr')) throw new Error('DSH profile expressions are forbidden');
    for (const child of Object.values(item)) visit(child);
  };
  visit(value);
  return value;
}

function profileObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Invalid DSH profile configuration object');
  return value as Record<string, unknown>;
}

function onlyProfileKeys(value: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(value).some((key) => !keys.includes(key)))
    throw new Error('Unsupported DSH profile configuration field');
}

/** Structural admission only; installed bundle ownership/schemas are checked in the sandbox. */
export function validateDshProfileConfiguration(files: DshProfileFiles): void {
  parseDshProfileFiles(files);
  const manifest = profileObject(JSON.parse(files['package.json']));
  onlyProfileKeys(manifest, ['name', 'version', 'private', 'description', 'dependencies', 'dsh']);
  if (manifest.private !== true) throw new Error('DSH profile must be private');
  const dependencies = profileObject(manifest.dependencies ?? {});
  const exact =
    /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;
  const packageName = /^(?:@[a-z0-9._-]+\/)?[a-z0-9][a-z0-9._-]*$/;
  for (const [name, version] of Object.entries(dependencies)) {
    if (!packageName.test(name) || typeof version !== 'string' || !exact.test(version))
      throw new Error('DSH profile requires exact npm dependencies');
  }
  const dsh = profileObject(manifest.dsh);
  onlyProfileKeys(dsh, ['profile']);
  const profile = profileObject(dsh.profile);
  onlyProfileKeys(profile, ['bundles']);
  if (
    !Array.isArray(profile.bundles) ||
    new Set(profile.bundles).size !== profile.bundles.length ||
    profile.bundles.some((name) => typeof name !== 'string' || !Object.hasOwn(dependencies, name))
  )
    throw new Error('DSH profile bundles must select declared business dependencies');

  const workspace = profileObject(parseDshProfileYaml(files['pnpm-workspace.yaml']));
  onlyProfileKeys(workspace, ['packages', 'nodeLinker', 'autoInstallPeers']);
  if (
    workspace.nodeLinker !== 'hoisted' ||
    workspace.autoInstallPeers !== false ||
    JSON.stringify(workspace.packages) !== JSON.stringify(['.'])
  )
    throw new Error('DSH profile requires the managed pnpm workspace settings');

  const lock = profileObject(parseDshProfileYaml(files['pnpm-lock.yaml']));
  onlyProfileKeys(lock, ['lockfileVersion', 'settings', 'importers', 'packages', 'snapshots']);
  if (String(lock.lockfileVersion) !== '9.0')
    throw new Error('DSH profile requires pnpm lockfile 9.0');
  const settings = profileObject(lock.settings);
  onlyProfileKeys(settings, ['autoInstallPeers', 'excludeLinksFromLockfile']);
  if (settings.autoInstallPeers !== false || settings.excludeLinksFromLockfile !== false)
    throw new Error('Unsupported DSH profile lock settings');
  const importers = profileObject(lock.importers);
  onlyProfileKeys(importers, ['.']);
  const root = profileObject(importers['.']);
  onlyProfileKeys(root, ['dependencies']);
  const pins = profileObject(root.dependencies ?? {});
  if (JSON.stringify(Object.keys(pins).sort()) !== JSON.stringify(Object.keys(dependencies).sort()))
    throw new Error('DSH profile lock dependencies mismatch');
  for (const [name, version] of Object.entries(dependencies)) {
    const pin = profileObject(pins[name]);
    onlyProfileKeys(pin, ['specifier', 'version']);
    if (
      pin.specifier !== version ||
      typeof pin.version !== 'string' ||
      pin.version.split('(')[0] !== version
    )
      throw new Error('DSH profile lock pin mismatch');
  }
  const packages = profileObject(lock.packages ?? {});
  const snapshots = profileObject(lock.snapshots ?? {});
  for (const [name, raw] of Object.entries(pins)) {
    const pin = profileObject(raw);
    if (
      !Object.hasOwn(packages, name + '@' + String(pin.version).split('(')[0]) ||
      !Object.hasOwn(snapshots, name + '@' + String(pin.version))
    )
      throw new Error('DSH profile dependency lacks a locked package or snapshot');
  }
  for (const [name, raw] of Object.entries(packages)) {
    if (/^(?:@deepseek-ai\/(?:cordis|dsh-tools)|cordis)@/.test(name))
      throw new Error('DSH profile cannot install independent runtime copies');
    const separator = name.lastIndexOf('@');
    if (
      separator <= 0 ||
      !packageName.test(name.slice(0, separator)) ||
      !exact.test(name.slice(separator + 1))
    )
      throw new Error('DSH profile lock requires registry package versions');
    const entry = profileObject(raw);
    onlyProfileKeys(entry, [
      'resolution',
      'engines',
      'cpu',
      'os',
      'libc',
      'deprecated',
      'hasBin',
      'peerDependencies',
      'peerDependenciesMeta',
      'optional',
    ]);
    const resolution = profileObject(entry.resolution);
    onlyProfileKeys(resolution, ['integrity', 'tarball']);
    if (
      typeof resolution.integrity !== 'string' ||
      !/^sha(?:256|384|512)-[A-Za-z0-9+/]+={0,2}$/.test(resolution.integrity)
    )
      throw new Error('DSH profile lock requires package integrity');
    if (resolution.tarball !== undefined) {
      const url = new URL(String(resolution.tarball));
      if (url.origin !== 'https://registry.npmjs.org' || url.username || url.password)
        throw new Error('DSH profile package registry origin is forbidden');
    }
  }
  for (const [name, raw] of Object.entries(snapshots)) {
    if (!Object.hasOwn(packages, name.split('(')[0]!))
      throw new Error('DSH profile snapshot lacks a locked package');
    const entry = profileObject(raw);
    onlyProfileKeys(entry, [
      'dependencies',
      'optionalDependencies',
      'transitivePeerDependencies',
      'optional',
    ]);
    for (const field of ['dependencies', 'optionalDependencies']) {
      for (const [dependency, reference] of Object.entries(profileObject(entry[field] ?? {}))) {
        if (
          !packageName.test(dependency) ||
          typeof reference !== 'string' ||
          !Object.hasOwn(packages, dependency + '@' + reference.split('(')[0])
        )
          throw new Error('DSH profile transitive dependency is not locked');
      }
    }
  }
  const patch = parseDshProfileYaml(files['cordis.patch.yml']);
  if (!Array.isArray(patch)) throw new Error('DSH profile patch must be a native patch array');
  for (const operation of patch) {
    const row = profileObject(operation);
    if (Object.hasOwn(row, 'insert')) {
      onlyProfileKeys(row, ['insert']);
      if (!Array.isArray(row.insert)) throw new Error('DSH profile insert requires flat entries');
      for (const item of row.insert) {
        const inserted = profileObject(item);
        onlyProfileKeys(inserted, ['id', 'name', 'config', 'disabled']);
        if (
          typeof inserted.id !== 'string' ||
          !/^[A-Za-z0-9_.-]+$/.test(inserted.id) ||
          typeof inserted.name !== 'string' ||
          !packageName.test(inserted.name)
        )
          throw new Error('DSH profile insert requires an explicit ID and npm module');
      }
    } else {
      onlyProfileKeys(row, ['id', 'config', 'disabled']);
      if (typeof row.id !== 'string' || !/^[A-Za-z0-9_.-]+$/.test(row.id))
        throw new Error('Unsupported DSH profile patch operation');
    }
  }
}

/** Bundle layers own their inserted IDs; only the final profile may configure those IDs. */
export function validateDshBusinessLayers(
  layers: readonly string[],
  managedEntries: readonly { id: string; name: string }[],
): void {
  const protectedIds = new Set(managedEntries.map((entry) => entry.id));
  const protectedModules = new Set(managedEntries.map((entry) => entry.name));
  const owners = new Map<string, number>();
  for (const [index, text] of layers.entries()) {
    const operations = parseDshProfileYaml(text);
    if (!Array.isArray(operations)) throw new Error('DSH business layer must be a patch array');
    for (const raw of operations) {
      const operation = profileObject(raw);
      if (Object.hasOwn(operation, 'insert')) {
        onlyProfileKeys(operation, ['insert']);
        if (!Array.isArray(operation.insert))
          throw new Error('DSH business insert requires flat entries');
        for (const value of operation.insert) {
          const row = profileObject(value);
          onlyProfileKeys(row, ['id', 'name', 'config', 'disabled']);
          if (
            typeof row.id !== 'string' ||
            !/^[A-Za-z0-9_.-]+$/.test(row.id) ||
            typeof row.name !== 'string' ||
            row.name.length === 0 ||
            protectedIds.has(row.id) ||
            protectedModules.has(row.name) ||
            owners.has(row.id)
          )
            throw new Error('DSH business layer cannot replace a managed or owned entry');
          if (row.disabled !== undefined && typeof row.disabled !== 'boolean')
            throw new Error('DSH business enablement must be boolean');
          owners.set(row.id, index);
        }
      } else {
        onlyProfileKeys(operation, ['id', 'config', 'disabled']);
        const id = operation.id;
        if (
          typeof id !== 'string' ||
          protectedIds.has(id) ||
          !owners.has(id) ||
          (index !== layers.length - 1 && owners.get(id) !== index)
        )
          throw new Error('DSH business override must target its own declared entry');
        if (operation.disabled !== undefined && typeof operation.disabled !== 'boolean')
          throw new Error('DSH business enablement must be boolean');
      }
    }
  }
}

export function parseDshProfileFiles(value: unknown): DshProfileFiles {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Invalid DSH profile files');
  const files = value as Record<string, unknown>;
  if (JSON.stringify(Object.keys(files).sort()) !== JSON.stringify(DSH_PROFILE_FILENAMES))
    throw new Error('DSH profile requires exactly four native configuration files');
  let size = 0;
  for (const name of DSH_PROFILE_FILENAMES) {
    const contents = files[name];
    if (typeof contents !== 'string') throw new Error('DSH profile files must be strings');
    if (Buffer.from(contents, 'utf8').toString('utf8') !== contents)
      throw new Error('DSH profile files must contain well-formed Unicode');
    size += Buffer.byteLength(contents, 'utf8');
  }
  if (size > DSH_PROFILE_MAX_BYTES) throw new Error('DSH profile configuration exceeds 1 MiB');
  return Object.fromEntries(
    DSH_PROFILE_FILENAMES.map((name) => [name, files[name]]),
  ) as DshProfileFiles;
}

export function dshProfileConfigSha256(files: DshProfileFiles): string {
  return createHash('sha256')
    .update(JSON.stringify([1, DSH_PROFILE_FILENAMES.map((name) => [name, files[name]])]))
    .digest('hex');
}

export function parseDshProfileDescriptor(value: unknown): DshProfileDescriptor {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Invalid DSH profile descriptor');
  const raw = value as Record<string, unknown>;
  const keys = ['config_sha256', 'files', 'source_file_id', 'source_sha256', 'version'];
  if (JSON.stringify(Object.keys(raw).sort()) !== JSON.stringify(keys) || raw.version !== 1)
    throw new Error('Unsupported DSH profile descriptor');
  if (
    typeof raw.source_file_id !== 'string' ||
    !/^file_[A-Za-z0-9_-]{1,200}$/.test(raw.source_file_id)
  )
    throw new Error('Invalid DSH profile source File ID');
  for (const name of ['source_sha256', 'config_sha256']) {
    if (typeof raw[name] !== 'string' || !/^[0-9a-f]{64}$/.test(raw[name] as string))
      throw new Error('Invalid DSH profile digest');
  }
  const files = parseDshProfileFiles(raw.files);
  if (dshProfileConfigSha256(files) !== raw.config_sha256)
    throw new Error('DSH profile configuration digest mismatch');
  return {
    version: 1,
    source_file_id: raw.source_file_id,
    source_sha256: raw.source_sha256 as string,
    config_sha256: raw.config_sha256,
    files,
  };
}
