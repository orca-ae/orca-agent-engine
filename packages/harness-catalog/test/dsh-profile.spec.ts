// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { dump, load, JSON_SCHEMA } from 'js-yaml';
import {
  dshProfileConfigSha256,
  parseDshProfileDescriptor,
  parseDshProfileFiles,
  parseDshProfileYaml,
  validateDshBusinessLayers,
  validateDshProfileConfiguration,
  validateHarnessFeatures,
  type DshProfileFiles,
} from '../src/index.js';

const lines = (...values: string[]) => values.join(String.fromCharCode(10));
function fixture(): DshProfileFiles {
  return {
    'package.json': JSON.stringify({
      private: true,
      dependencies: { business: '1.0.0' },
      dsh: { profile: { bundles: ['business'] } },
    }),
    'pnpm-workspace.yaml': lines('packages: [.]', 'nodeLinker: hoisted', 'autoInstallPeers: false'),
    'pnpm-lock.yaml': lines(
      'lockfileVersion: "9.0"',
      'settings: {autoInstallPeers: false, excludeLinksFromLockfile: false}',
      'importers:',
      '  .:',
      '    dependencies:',
      '      business: {specifier: 1.0.0, version: 1.0.0}',
      'packages:',
      '  business@1.0.0:',
      '    resolution: {integrity: sha512-YQ==}',
      'snapshots:',
      '  business@1.0.0: {}',
    ),
    'cordis.patch.yml': '[]',
  };
}

describe('native DSH profile admission', () => {
  it('hashes exact texts in fixed filename order and verifies retained identity', () => {
    const files = fixture();
    expect(validateDshProfileConfiguration(files)).toBeUndefined();
    const reordered = Object.fromEntries(Object.entries(files).reverse()) as DshProfileFiles;
    expect(dshProfileConfigSha256(reordered)).toBe(dshProfileConfigSha256(files));
    expect(dshProfileConfigSha256({ ...files, 'cordis.patch.yml': ' []' })).not.toBe(
      dshProfileConfigSha256(files),
    );
    const descriptor = {
      version: 1,
      source_file_id: 'file_profile',
      source_sha256: 'a'.repeat(64),
      config_sha256: dshProfileConfigSha256(files),
      files,
    };
    expect(parseDshProfileDescriptor(descriptor)).toEqual(descriptor);
    expect(() =>
      parseDshProfileDescriptor({ ...descriptor, config_sha256: 'b'.repeat(64) }),
    ).toThrow(/digest/);
    expect(() => parseDshProfileDescriptor({ ...descriptor, extra: true })).toThrow();
  });

  it('bounds four exact string-valued files', () => {
    expect(() => parseDshProfileFiles({ ...fixture(), extra: '' })).toThrow();
    expect(() => parseDshProfileFiles({ ...fixture(), 'cordis.patch.yml': 1 })).toThrow();
    expect(() =>
      parseDshProfileFiles({ ...fixture(), 'cordis.patch.yml': '汉'.repeat(400_000) }),
    ).toThrow(/1 MiB/);
  });

  it.each([
    lines('a: 1', 'a: 2'),
    'a: !!js process.exit()',
    'a: &anchor 1',
    'a: *alias',
    '{<<: {a: 1}}',
    'a: !!timestamp 2020-01-01',
    'a: !custom text',
    'a: {__jsExpr: process.exit()}',
  ])('rejects unsafe YAML %s', (yaml) => {
    expect(() => parseDshProfileYaml(yaml)).toThrow();
  });

  it('preserves core scalar semantics and quoted tokens', () => {
    expect(
      parseDshProfileYaml(lines('a: yes', 'b: true', 'c: 010', 'd: null', 'e: "&anchor !!js"')),
    ).toEqual({ a: 'yes', b: true, c: 10, d: null, e: '&anchor !!js' });
  });

  it.each([
    'value: 2020-01-01',
    'value: .inf',
    'value: 0x10',
    'value: 1e3',
    'value: !!str 123',
    'value: [null, true, false, 012, 0o12]',
  ])('agrees with the pinned loader JSON schema for core input %s', (text) => {
    // DSH app-boot 0.2.1-alpha.2 uses JSON_SCHEMA.extend(JsExpr).
    expect(parseDshProfileYaml(text)).toEqual(load(text, { schema: JSON_SCHEMA }));
  });

  it.each([
    (f: DshProfileFiles) => {
      f['package.json'] = f['package.json'].replace('1.0.0', '^1.0.0');
    },
    (f: DshProfileFiles) => {
      f['package.json'] = JSON.stringify({
        ...JSON.parse(f['package.json']),
        scripts: { install: 'bad' },
      });
    },
    (f: DshProfileFiles) => {
      f['pnpm-workspace.yaml'] = lines(f['pnpm-workspace.yaml'], 'overrides: {}');
    },
    (f: DshProfileFiles) => {
      f['pnpm-workspace.yaml'] = f['pnpm-workspace.yaml'].replace('hoisted', 'isolated');
    },
    (f: DshProfileFiles) => {
      f['pnpm-lock.yaml'] = f['pnpm-lock.yaml'].replace(
        'resolution: {integrity: sha512-YQ==}',
        lines('requiresBuild: true', '    resolution: {integrity: sha512-YQ==}'),
      );
    },
    (f: DshProfileFiles) => {
      f['pnpm-lock.yaml'] = f['pnpm-lock.yaml'].replace(
        'integrity: sha512-YQ==',
        'tarball: https://evil.test/a.tgz, integrity: sha512-YQ==',
      );
    },
    (f: DshProfileFiles) => {
      f['pnpm-lock.yaml'] = f['pnpm-lock.yaml'].replace(
        'integrity: sha512-YQ==',
        'tarball: file:./package',
      );
    },
    (f: DshProfileFiles) => {
      f['pnpm-lock.yaml'] = f['pnpm-lock.yaml'].replaceAll(
        'business@1.0.0',
        '@deepseek-ai/cordis@4.0.5-alpha.1',
      );
    },
    (f: DshProfileFiles) => {
      f['cordis.patch.yml'] = '- remove: tools';
    },
  ])('rejects forbidden sources/settings/build/patch controls', (mutate) => {
    const files = fixture();
    mutate(files);
    expect(() => validateDshProfileConfiguration(files)).toThrow();
  });

  it('protects template IDs/modules and ordered business ownership', () => {
    const managed = [
      { id: 'tools', name: '@deepseek-ai/dsh-tools' },
      { id: 'new-managed', name: 'managed-module' },
    ];
    const bundle = '- insert: [{id: business.probe, name: ./index.js}]';
    expect(() =>
      validateDshBusinessLayers(
        [bundle, '- {id: business.probe, config: {label: EXPECTED}}'],
        managed,
      ),
    ).not.toThrow();
    for (const patch of [
      '- {id: tools, config: {}}',
      '- insert: [{id: new-managed, name: evil}]',
      '- insert: [{id: evil, name: managed-module}]',
      '- remove: business.probe',
      '- {id: unknown, disabled: true}',
      '- insert: [{id: business.probe, name: other}]',
      '- insert: [{id: group, name: evil, group: []}]',
    ])
      expect(() => validateDshBusinessLayers([bundle, patch], managed)).toThrow();
    expect(() =>
      validateDshBusinessLayers([bundle, '- {id: business.probe, config: {}}', '[]'], managed),
    ).toThrow(/own/);
  });

  it('reserves the annotation for colocated DSH', () => {
    expect(validateHarnessFeatures({ dsh_profile: 'file_p' }, {})).toMatch(/requires/);
    expect(
      validateHarnessFeatures({ harness: 'dsh_sdk', mode: 'separate', dsh_profile: 'file_p' }, {}),
    ).toMatch(/requires/);
    expect(
      validateHarnessFeatures({ harness: 'dsh_sdk', mode: 'colocated', dsh_profile: '../bad' }, {}),
    ).toMatch(/File ID/);
  });

  it('admits native flat patches, locked transitives and public npm tarballs', () => {
    const files = fixture();
    files['cordis.patch.yml'] = dump([
      { insert: [{ id: 'business.extra', name: 'business', config: {}, disabled: false }] },
      { id: 'business.extra', disabled: true },
    ]);
    const lock = load(files['pnpm-lock.yaml']) as {
      packages: Record<string, { resolution: { integrity: string; tarball?: string } }>;
      snapshots: Record<
        string,
        { dependencies?: Record<string, string>; optionalDependencies?: Record<string, string> }
      >;
    };
    lock.packages['business@1.0.0']!.resolution.tarball =
      'https://registry.npmjs.org/business/-/business-1.0.0.tgz';
    lock.packages['helper@2.0.0'] = { resolution: { integrity: 'sha512-YQ==' } };
    lock.snapshots['business@1.0.0'] = {
      dependencies: { helper: '2.0.0' },
      optionalDependencies: { helper: '2.0.0' },
    };
    lock.snapshots['helper@2.0.0'] = {};
    files['pnpm-lock.yaml'] = dump(lock);
    expect(() => validateDshProfileConfiguration(files)).not.toThrow();
  });

  it.each([
    ['invalid package metadata', 'package.json', { private: false }],
    ['non-string dependency', 'package.json', { dependencies: { business: 1 } }],
    ['undeclared bundle', 'package.json', { dsh: { profile: { bundles: ['missing'] } } }],
    [
      'duplicate bundles',
      'package.json',
      { dsh: { profile: { bundles: ['business', 'business'] } } },
    ],
    ['peer auto-install', 'pnpm-workspace.yaml', { autoInstallPeers: true }],
    ['workspace expansion', 'pnpm-workspace.yaml', { packages: ['**'] }],
    ['old lockfile', 'pnpm-lock.yaml', { lockfileVersion: '6.0' }],
    ['unsupported settings', 'pnpm-lock.yaml', { settings: { autoInstallPeers: true } }],
    ['missing pins', 'pnpm-lock.yaml', { importers: { '.': {} } }],
    [
      'wrong pin',
      'pnpm-lock.yaml',
      {
        importers: {
          '.': { dependencies: { business: { specifier: '2.0.0', version: '2.0.0' } } },
        },
      },
    ],
    [
      'non-string pin',
      'pnpm-lock.yaml',
      { importers: { '.': { dependencies: { business: { specifier: '1.0.0', version: 1 } } } } },
    ],
    ['missing packages', 'pnpm-lock.yaml', { packages: {} }],
    ['missing snapshots', 'pnpm-lock.yaml', { snapshots: {} }],
    [
      'unlocked snapshot',
      'pnpm-lock.yaml',
      { snapshots: { 'business@1.0.0': {}, 'extra@1.0.0': {} } },
    ],
    [
      'unlocked transitive',
      'pnpm-lock.yaml',
      { snapshots: { 'business@1.0.0': { dependencies: { extra: '1.0.0' } } } },
    ],
    [
      'non-string transitive',
      'pnpm-lock.yaml',
      { snapshots: { 'business@1.0.0': { optionalDependencies: { extra: 1 } } } },
    ],
  ] as const)('rejects %s', (_label, filename, changes) => {
    const files = fixture();
    const original = load(files[filename]) as object;
    files[filename] =
      filename === 'package.json'
        ? JSON.stringify({ ...original, ...changes })
        : dump({ ...original, ...changes });
    expect(() => validateDshProfileConfiguration(files)).toThrow();
  });

  it.each([
    'null',
    '{}',
    '- insert: {}',
    '- insert: [{name: business}]',
    '- insert: [{id: "bad/id", name: business}]',
    '- insert: [{id: extra, name: ./local.js}]',
    '- {id: "bad/id"}',
  ])('rejects malformed native profile patch %s', (patch) => {
    expect(() =>
      validateDshProfileConfiguration({ ...fixture(), 'cordis.patch.yml': patch }),
    ).toThrow();
  });

  it.each([
    '{}',
    '- insert: {}',
    '- insert: [{id: "bad/id", name: ./local.js}]',
    '- insert: [{id: probe, name: ""}]',
    '- insert: [{id: probe, name: ./local.js, disabled: "yes"}]',
    '- insert: [{id: probe, name: ./local.js}]\n- {id: probe, disabled: "yes"}',
  ])('rejects invalid installed business layer %s', (patch) => {
    expect(() => validateDshBusinessLayers([patch], [])).toThrow();
  });

  it('bounds YAML complexity and rejects malformed retained text/descriptors', () => {
    expect(() => parseDshProfileYaml('x'.repeat(1024 * 1024 + 1))).toThrow(/1 MiB/);
    expect(() => parseDshProfileYaml('['.repeat(65) + ']'.repeat(65))).toThrow(/complex/);
    expect(() => parseDshProfileYaml('- 0\n'.repeat(100_001))).toThrow(/complex/);
    expect(() => parseDshProfileFiles(null)).toThrow(/files/);
    expect(() => parseDshProfileFiles({ ...fixture(), 'cordis.patch.yml': '\uD800' })).toThrow(
      /Unicode/,
    );
    expect(() => parseDshProfileDescriptor(null)).toThrow(/descriptor/);
    const files = fixture();
    const descriptor = {
      version: 1,
      source_file_id: 'file_p',
      source_sha256: 'a'.repeat(64),
      config_sha256: dshProfileConfigSha256(files),
      files,
    };
    expect(() => parseDshProfileDescriptor({ ...descriptor, source_file_id: 1 })).toThrow(
      /File ID/,
    );
    expect(() => parseDshProfileDescriptor({ ...descriptor, source_sha256: 'bad' })).toThrow(
      /digest/,
    );
  });
});
