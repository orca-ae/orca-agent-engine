// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { readFile, lstat, readdir, realpath } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { Context } from '@deepseek-ai/cordis';
import Tools from '@deepseek-ai/dsh-tools';
import type { DshHarnessProfile } from '@orca/sdk-harness';
import { DSH_SDK_VERSION } from './checkpoint.js';
import * as profileCatalog from '@orca/harness-catalog';
import {
  DSH_PROFILE_DIRECTORY,
  DSH_PROFILE_HOME,
  DSH_PROFILE_FILENAMES,
  DSH_PROFILE_MAX_BYTES,
  parseDshProfileDescriptor,
  type DshProfileDescriptor,
} from '@orca/harness-catalog';

const sha256 = (value: string): string => createHash('sha256').update(value).digest('hex');
type Entry = { id: string; name: string; config?: unknown; disabled?: boolean };
type Patch = { insert?: Entry[] };
interface NativeBoot {
  resolvePluginResource(specifier: string, parentURL: string): string;
  resolveBundleDir(bin: string, name: string, anchor: string, profileDir: string): string;
  readProfileManifest(bin: string, dir: string): { dsh?: { bundle?: unknown } };
  bundlePatchPaths(dir: string, bundle: unknown): string[];
  loadProfileDirectory(
    bin: string,
    dir: string,
    anchor: string,
  ): {
    skippedBundles: unknown[];
    layers: Array<{ patchPaths: string[]; patches: Patch[] }>;
    patches: Patch[];
  };
  loadOverlayPatches(bin: string, path: string): Patch[];
  composeEntries(layers: Patch[][]): Entry[];
}

// Resolve the pinned loader from DSH itself; do not select another YAML/parser version.
async function nativeBoot(): Promise<NativeBoot> {
  const require = createRequire(
    createRequire(import.meta.url).resolve('@deepseek-ai/dsh/package.json'),
  );
  return import(pathToFileURL(require.resolve('@deepseek-ai/dsh-app-boot')).href);
}

async function trustedProfileEntries(path: string): Promise<Entry[]> {
  const boot = await nativeBoot();
  return boot.composeEntries([boot.loadOverlayPatches('orca', path)]);
}

/** Read-only provider preparation has finished before this credential-bearing process exists. */
export async function validateMountedProfile(
  descriptor: DshProfileDescriptor,
  trustedPath: string,
): Promise<void> {
  const profile = parseDshProfileDescriptor(descriptor);
  if (
    typeof profileCatalog.validateDshProfileConfiguration !== 'function' ||
    typeof profileCatalog.validateDshBusinessLayers !== 'function'
  )
    throw new Error('Custom DSH profile validation is unavailable');
  profileCatalog.validateDshProfileConfiguration(profile.files);
  try {
    await lstat(join(DSH_PROFILE_HOME, 'cordis.patch.yml'));
    throw new Error('Ambient DSH home patches are not supported');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  const mounts = (await readFile('/proc/self/mountinfo', 'utf8'))
    .split('\n')
    .map((line) => line.split(' '));
  for (const name of [...DSH_PROFILE_FILENAMES, 'node_modules', '.orca-profile-revision']) {
    if (
      !mounts.some(
        (fields) =>
          fields[4] === join(DSH_PROFILE_DIRECTORY, name) && fields[5]?.split(',').includes('ro'),
      )
    )
      throw new Error('DSH profile requires read-only provider mounts');
  }
  if (
    !(await lstat(join(DSH_PROFILE_DIRECTORY, '.orca-profile-revision'))).isFile() ||
    (await readFile(join(DSH_PROFILE_DIRECTORY, '.orca-profile-revision'), 'utf8')) !==
      profile.config_sha256
  )
    throw new Error('DSH profile revision mismatch');
  for (const name of DSH_PROFILE_FILENAMES) {
    const path = join(DSH_PROFILE_DIRECTORY, name);
    const stat = await lstat(path);
    if (
      !stat.isFile() ||
      stat.size > 1024 * 1024 ||
      (await readFile(path, 'utf8')) !== profile.files[name]
    )
      throw new Error('DSH mounted configuration differs from approved revision');
  }
  if (!(await lstat(join(DSH_PROFILE_DIRECTORY, 'node_modules'))).isDirectory())
    throw new Error('DSH profile dependencies must be mounted at the actual profile path');
  // Independent runtime packages change native service ownership, even with compatible versions.
  const image = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
  const shared = new Set(
    Object.keys(image.dependencies).filter((name) => name.startsWith('@deepseek-ai/')),
  );
  const seen = new Set<string>();
  const inspect = async (modules: string): Promise<void> => {
    let names: string[];
    try {
      names = await readdir(modules);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      throw error;
    }
    for (const name of names) {
      if (name.startsWith('.')) continue;
      const packages = name.startsWith('@')
        ? (await readdir(join(modules, name))).map((part) => name + '/' + part)
        : [name];
      for (const pkg of packages) {
        if (shared.has(pkg)) throw new Error('Independent DSH runtime package in profile');
        const path = await realpath(join(modules, pkg));
        if (seen.has(path)) continue;
        seen.add(path);
        await inspect(join(path, 'node_modules'));
      }
    }
  };
  await inspect(join(DSH_PROFILE_DIRECTORY, 'node_modules'));
  await selectedProfileEntries(profile, trustedPath);
}

async function selectedProfileEntries(
  profile: DshProfileDescriptor,
  trustedPath: string,
): Promise<Entry[]> {
  const require = createRequire(import.meta.url);
  const boot = await nativeBoot();
  const anchor = require.resolve('@deepseek-ai/dsh/package.json');
  const trusted = await trustedProfileEntries(trustedPath);
  const layers: string[] = [];
  const layerPaths: string[] = [];
  let bytes = 0;
  for (const name of JSON.parse(profile.files['package.json']).dsh.profile.bundles as string[]) {
    const dir = boot.resolveBundleDir('orca', name, anchor, DSH_PROFILE_DIRECTORY);
    if ((await lstat(join(dir, 'package.json'))).size > DSH_PROFILE_MAX_BYTES)
      throw new Error('DSH bundle manifest exceeds limit');
    const manifest = boot.readProfileManifest('orca', dir);
    if (!manifest.dsh?.bundle) throw new Error('DSH selected bundle has no patch');
    for (const path of boot.bundlePatchPaths(dir, manifest.dsh.bundle)) {
      const stat = await lstat(path);
      bytes += stat.size;
      if (!stat.isFile() || bytes > DSH_PROFILE_MAX_BYTES)
        throw new Error('DSH business layers exceed limit');
      layers.push(await readFile(path, 'utf8'));
      layerPaths.push(path);
    }
  }
  // Bound and validate core data before even the native expression-aware parser sees business YAML.
  profileCatalog.validateDshBusinessLayers([...layers, profile.files['cordis.patch.yml']], trusted);
  const protectedModules = new Set(
    await Promise.all(
      trusted.map((entry) =>
        realpath(boot.resolvePluginResource(entry.name, pathToFileURL(trustedPath).href)),
      ),
    ),
  );
  for (const [index, text] of [...layers, profile.files['cordis.patch.yml']].entries()) {
    const parentURL = pathToFileURL(
      layerPaths[index] ?? join(DSH_PROFILE_DIRECTORY, 'cordis.patch.yml'),
    ).href;
    const operations = profileCatalog.parseDshProfileYaml(text) as Array<{
      insert?: Array<{ name: string }>;
    }>;
    for (const operation of operations)
      for (const row of operation.insert ?? []) {
        const module = await realpath(boot.resolvePluginResource(row.name, parentURL));
        if (protectedModules.has(module))
          throw new Error('DSH business module aliases a managed module');
      }
  }
  const loaded = boot.loadProfileDirectory('orca', DSH_PROFILE_DIRECTORY, anchor);
  if (loaded.skippedBundles.length) throw new Error('DSH selected bundle was skipped');
  return boot.composeEntries([
    ...loaded.layers.map((layer) => layer.patches),
    loaded.patches,
    boot.loadOverlayPatches('orca', trustedPath),
  ]);
}

/** Consistency after imports/schema checks; approved code remains inside the trust boundary. */
export async function validateEffectiveProfile(
  ctx: Context,
  trustedPath: string,
  profile: DshProfileDescriptor,
): Promise<void> {
  const loader = ctx.get('loader');
  if (!loader || !(ctx instanceof Context) || !(ctx.tools instanceof Tools))
    throw new Error('DSH shared runtime identity mismatch');
  const expected = await selectedProfileEntries(profile, trustedPath);
  const remaining = new Map(expected.map((row) => [row.id, row]));
  // The pinned launcher owns one bootstrap Include; configured IDs belong to its subtree.
  const tree = loader.resolve('include').subtree;
  if (!tree) throw new Error('DSH effective profile tree is missing');
  for (const entry of tree.entries()) {
    if (entry.parent.tree !== tree) throw new Error('DSH profile entries must remain flat');
    const wanted = remaining.get(entry.options.id);
    if (!wanted || !isDeepStrictEqual(entry.options, wanted))
      throw new Error('DSH effective configuration differs from reviewed composition');
    // Cordis 4.0.5-alpha.1 exposes FiberState only as an ambient const enum (ACTIVE=2).
    if (!entry.disabled && (!entry.fiber || entry.fiber.state !== 2))
      throw new Error('DSH required profile entry is inactive');
    remaining.delete(entry.options.id);
  }
  if (remaining.size) throw new Error('DSH required profile entry is missing');
}

export async function profileIdentity(
  profile: DshProfileDescriptor,
  toolManifestSha256: string,
  trustedPath: string,
): Promise<DshHarnessProfile> {
  const require = createRequire(import.meta.url);
  return {
    version: 1,
    config_sha256: profile.config_sha256,
    lock_sha256: sha256(profile.files['pnpm-lock.yaml']),
    tool_manifest_sha256: toolManifestSha256,
    runtime_compatibility: sha256(
      JSON.stringify([
        DSH_SDK_VERSION,
        await readFile(trustedPath, 'utf8'),
        await readFile(
          join(dirname(require.resolve('@orca/dsh-harness/plugin')), 'plugin.js'),
          'utf8',
        ),
      ]),
    ),
  };
}
