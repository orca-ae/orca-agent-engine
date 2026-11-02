// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { runInNewContext } from 'node:vm';
import * as crypto from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { DSH_PROFILE_DIRECTORY, dshProfileConfigSha256 } from '@orca/harness-catalog';
import { buildDshProfileProbeCommand, prepareDshProfilePlan } from '../../src/dsh-profile.js';
import { buildOpenSandboxAcquireRequest } from '../../src/opensandbox-lifecycle.js';

const files = {
  'cordis.patch.yml': '[]',
  'package.json': '{}',
  'pnpm-lock.yaml': '',
  'pnpm-workspace.yaml': '',
};
const profile = {
  version: 1 as const,
  source_file_id: 'file_test',
  source_sha256: 'a'.repeat(64),
  config_sha256: dshProfileConfigSha256(files),
  files,
};

describe('native DSH preparation', () => {
  it('serializes the retained descriptor without exposing it as runtime environment', () => {
    const { body, dshProfilePlan } = buildOpenSandboxAcquireRequest(
      { dshProfile: profile },
      {
        image: 'image',
        timeoutSeconds: 60,
        execdAccessToken: 'b'.repeat(64),
      },
    );
    expect(dshProfilePlan).toEqual(profile);
    expect(body.extensions).toMatchObject({ 'orca.dsh.profile.v1': JSON.stringify(profile) });
    expect(JSON.stringify(body.env)).not.toContain('file_test');
    expect(body.timeout).toBe(960);
    expect(() => prepareDshProfilePlan(profile, { type: 'limited' })).toThrow('unrestricted');
    expect(() => prepareDshProfilePlan({ ...profile, config_sha256: 'b'.repeat(64) })).toThrow(
      'digest',
    );
  });

  it.each([
    'valid',
    'missing-modules',
    'writable-modules',
    'symlink-modules',
    'revision',
    'contents',
  ])('checks returned mount and configuration evidence: %s', (scenario) => {
    const names = [...Object.keys(files), 'node_modules', '.orca-profile-revision'];
    const mounts = names
      .filter((name) => scenario !== 'missing-modules' || name !== 'node_modules')
      .map(
        (name) =>
          '1 0 0:1 / ' +
          DSH_PROFILE_DIRECTORY +
          '/' +
          name +
          ' ' +
          (scenario === 'writable-modules' && name === 'node_modules' ? 'rw' : 'ro') +
          ' - tmpfs tmpfs rw',
      )
      .join(String.fromCharCode(10));
    const fs = {
      readFileSync(path: string) {
        if (path === '/proc/self/mountinfo') return mounts;
        if (path.endsWith('/.orca-profile-revision'))
          return scenario === 'revision' ? 'wrong' : profile.config_sha256;
        const name = path.slice(DSH_PROFILE_DIRECTORY.length + 1) as keyof typeof files;
        return scenario === 'contents' ? 'tampered' : files[name];
      },
      statSync() {
        return { size: 10 };
      },
      lstatSync(path: string) {
        return {
          isSymbolicLink: () => scenario === 'symlink-modules' && path.endsWith('/node_modules'),
          isDirectory: () => path.endsWith('/node_modules'),
          isFile: () => !path.endsWith('/node_modules'),
        };
      },
    };
    const command = buildDshProfileProbeCommand(profile);
    const quote = String.fromCharCode(39);
    const script = command
      .slice(command.indexOf(quote) + 1, -1)
      .replaceAll(quote + '"' + quote + '"' + quote, quote);
    const run = () =>
      runInNewContext(script, { require: (name: string) => (name === 'node:fs' ? fs : crypto) });
    if (scenario === 'valid') expect(run).not.toThrow();
    else expect(run).toThrow();
  });
});
