// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import {
  chmodSync,
  linkSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  symlinkSync,
  truncateSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import type { Packages } from '../../src/sandbox-runtime.js';
import {
  normalizeEnvironmentPackages,
  encodeEnvironmentPackages,
  environmentPackagesRevision,
  currentEnvironmentPackagesRevision,
  ENVIRONMENT_PACKAGES_ENV,
  ENVIRONMENT_PACKAGE_COPY_SCRIPT,
  assertEnvironmentPackageMountPaths,
  buildEnvironmentPackageBoundaryProbe,
  environmentPackageShellCommand,
} from '../../src/environment-packages.js';
import { buildSandboxWritePolicy, buildBubblewrapCommand } from '../../src/write-policy.js';
import { parseResourceManifest } from '../../src/resource-transfer.js';

const dirs: string[] = [];
function fixture() {
  const dir = mkdtempSync(join(tmpdir(), 'orca-package-test-'));
  dirs.push(dir);
  const source = join(dir, 'source');
  const target = join(dir, 'target');
  mkdirSync(source);
  return { dir, source, target };
}
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const planFixtures = JSON.parse(
  readFileSync(
    new URL(
      '../../../../charts/opensandbox-patches/test/environment-packages.json',
      import.meta.url,
    ),
    'utf8',
  ),
) as {
  valid: Array<{ name: string; packages: Packages; normalized: Packages; revision: string }>;
  invalid: Array<{ name: string; packages: unknown }>;
};

describe('isolated environment packages', () => {
  it.each(planFixtures.valid)('matches the provider golden plan: $name', (fixture) => {
    expect(normalizeEnvironmentPackages(fixture.packages)).toEqual(fixture.normalized);
    expect(environmentPackagesRevision(fixture.packages)).toBe(fixture.revision);
  });
  it.each(planFixtures.invalid)('rejects the provider golden input: $name', (fixture) => {
    expect(() => normalizeEnvironmentPackages(fixture.packages)).toThrow(/specification/);
  });
  it('bounds specifications in UTF-16 units, including astral characters', () => {
    expect(normalizeEnvironmentPackages({ npm: ['🚀'.repeat(512)] }).npm).toHaveLength(1);
    expect(() => normalizeEnvironmentPackages({ npm: ['🚀'.repeat(513)] })).toThrow(
      /specification/,
    );
  });
  it('canonicalizes manager keys while preserving package installation order', () => {
    const packages = {
      apt: ['jq', 'curl', 'jq'],
      cargo: ['ripgrep'],
      gem: ['rake'],
      go: ['example.com/tool@v1.0.0'],
      npm: ['typescript'],
      pip: ['requests'],
    };
    expect(normalizeEnvironmentPackages(packages).apt).toEqual(['jq', 'curl', 'jq']);
    expect(environmentPackagesRevision(packages)).toBe(
      environmentPackagesRevision(Object.fromEntries(Object.entries(packages).reverse())),
    );
    expect(environmentPackagesRevision(packages)).not.toBe(
      environmentPackagesRevision({ ...packages, apt: ['curl', 'jq'] }),
    );
    expect(JSON.parse(encodeEnvironmentPackages(packages)!)).toEqual({
      version: 1,
      packages: normalizeEnvironmentPackages(packages),
    });
    expect(
      currentEnvironmentPackagesRevision({ [ENVIRONMENT_PACKAGES_ENV]: JSON.stringify(packages) }),
    ).toBe(environmentPackagesRevision(packages));
    expect(encodeEnvironmentPackages({ pip: [], npm: [] })).toBeUndefined();
    expect(() => encodeEnvironmentPackages(packages, { type: 'limited' })).toThrow(/unrestricted/);
  });

  it.each(['', ' requests', '--index-url=evil', 'name\nother', 'x'.repeat(1025)])(
    'rejects ambiguous specification %j',
    (spec) => {
      expect(() => normalizeEnvironmentPackages({ pip: [spec] })).toThrow(/specification/);
    },
  );

  it('copies only workload data, strips privileged modes and breaks hard links', () => {
    const { source, target } = fixture();
    writeFileSync(join(source, 'tool'), 'dependency');
    chmodSync(join(source, 'tool'), 0o4755);
    linkSync(join(source, 'tool'), join(source, 'other'));
    mkdirSync(join(source, 'workspace'));
    writeFileSync(join(source, 'workspace', 'fake-skill'), 'untrusted');
    execFileSync(process.execPath, ['-e', ENVIRONMENT_PACKAGE_COPY_SCRIPT, source, target]);
    expect(readFileSync(join(target, 'tool'), 'utf8')).toBe('dependency');
    expect(statSync(join(target, 'tool')).mode & 0o7000).toBe(0);
    expect(statSync(join(target, 'tool')).nlink).toBe(1);
    expect(() => readFileSync(join(target, 'workspace', 'fake-skill'))).toThrow();
    expect(readFileSync(join(target, '.orca', 'package-readonly'), 'utf8')).toBe('readonly');
  });

  it('rejects source aliases, special files and oversized artifacts before tools start', () => {
    const { dir, source, target } = fixture();
    const alias = join(dir, 'alias');
    symlinkSync(source, alias);
    expect(() =>
      execFileSync(process.execPath, ['-e', ENVIRONMENT_PACKAGE_COPY_SCRIPT, alias, target], {
        stdio: 'pipe',
      }),
    ).toThrow();
    execFileSync('mkfifo', [join(source, 'pipe')]);
    expect(() =>
      execFileSync(process.execPath, ['-e', ENVIRONMENT_PACKAGE_COPY_SCRIPT, source, target], {
        stdio: 'pipe',
      }),
    ).toThrow();
    rmSync(join(source, 'pipe'));
    writeFileSync(join(source, 'oversized'), '');
    truncateSync(join(source, 'oversized'), 8 * 1024 * 1024 * 1024 + 1);
    expect(() =>
      execFileSync(process.execPath, ['-e', ENVIRONMENT_PACKAGE_COPY_SCRIPT, source, target], {
        stdio: 'pipe',
      }),
    ).toThrow(/package filesystem exceeds size limit/);
    expect(() => statSync(target)).toThrow();
  });

  it('protects provider files from resource aliases and executes an immutable probe directly', () => {
    for (const path of ['/', '/opt', '/opt/orca-environment-rootfs/x'])
      expect(() => assertEnvironmentPackageMountPaths([path])).toThrow();
    const policy = buildSandboxWritePolicy([], { environmentPackages: true });
    const probe = buildEnvironmentPackageBoundaryProbe(policy);
    expect(probe).toContain("'/usr/local/libexec/orca-environment-probe'");
    expect(probe).not.toContain("'bash'");
    const shell = buildBubblewrapCommand('python -c "print(1)"', policy);
    expect(shell).toContain("'/opt/orca-environment-rootfs'");
    expect(shell).toContain('/opt/orca-packages/python/bin');
  });

  it('requires version 2 manifests for package-pinned resources', () => {
    const revision = 'a'.repeat(64);
    expect(
      parseResourceManifest({
        version: 2,
        revision,
        environment_packages_revision: revision,
        resources: [],
      }).version,
    ).toBe(2);
    expect(() => parseResourceManifest({ version: 2, revision, resources: [] })).toThrow();
    expect(() =>
      parseResourceManifest({
        version: 1,
        revision,
        environment_packages_revision: revision,
        resources: [],
      }),
    ).toThrow();
  });

  it('preserves shell metacharacters through nested SDK quoting', () => {
    const command = `printf '%s\\n' '\u4f60\u597d! "quotes" $HOME'`;
    const encoded = environmentPackageShellCommand(command);
    expect(encoded).not.toContain('!');
    expect(
      execFileSync('/bin/bash', ['--noprofile', '--norc', '-c', encoded], { encoding: 'utf8' }),
    ).toBe('\u4f60\u597d! "quotes" $HOME\n');
  });
});
