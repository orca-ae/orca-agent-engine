// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdir, mkdtemp, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  assertNoNestedMountsUnderPolicyRoots,
  assertNoWritableAliasToSkills,
  bubblewrapProbeArgs,
  claudeSandboxSettings,
  isReadable,
  isWritable,
  OUTPUT_WRITABLE_PATH,
  parseSandboxWritePolicy,
  SANDBOX_WRITE_POLICY_ENV,
  writePolicyPermissionHandler,
} from '../src/write-policy.js';

const encodedPolicy = JSON.stringify({
  writablePaths: [
    { path: OUTPUT_WRITABLE_PATH, kind: 'session_output' },
    { path: '/workspace/repo', kind: 'github_repository' },
  ],
  readonlyPaths: ['/mnt/inputs/input.txt'],
  networkAllowedDomains: ['github.com', 'registry.example.com'],
});

describe('in-sandbox write policy', () => {
  it('strictly decodes the dispatcher policy', () => {
    const policy = parseSandboxWritePolicy({ [SANDBOX_WRITE_POLICY_ENV]: encodedPolicy });
    expect(policy).toBeDefined();
    expect(isWritable(policy!, '/mnt/session/outputs/poem.txt')).toBe(true);
    expect(isWritable(policy!, '/mnt/session/outputs-old/poem.txt')).toBe(false);
    expect(isWritable(policy!, '/workspace/repo/src/index.ts')).toBe(true);
    expect(isReadable(policy!, '/workspace/repo/src/index.ts')).toBe(true);
    expect(isReadable(policy!, '/mnt/inputs/input.txt')).toBe(true);
    expect(isReadable(policy!, '/proc/self/environ')).toBe(false);
    expect(policy!.networkAllowedDomains).toEqual(['github.com', 'registry.example.com']);
  });

  it('rejects nested mounts below readable or writable session roots', () => {
    const policy = parseSandboxWritePolicy({ [SANDBOX_WRITE_POLICY_ENV]: encodedPolicy })!;
    expect(() =>
      assertNoNestedMountsUnderPolicyRoots(
        policy,
        [
          '24 1 0:20 / / rw - overlay overlay rw',
          '25 24 0:21 / /workspace/repo/alias rw - ext4 /dev/root rw',
        ].join('\n'),
      ),
    ).toThrow(/contains nested mount.*workspace\/repo\/alias/);
    expect(() =>
      assertNoNestedMountsUnderPolicyRoots(
        policy,
        [
          '24 1 0:20 / / rw - overlay overlay rw',
          '25 24 0:21 / /mnt/inputs/input.txt/nested rw - ext4 /dev/root rw',
        ].join('\n'),
      ),
    ).toThrow(/contains nested mount.*mnt\/inputs/);
  });

  it('allows a policy root to be a mount point itself', () => {
    const policy = parseSandboxWritePolicy({ [SANDBOX_WRITE_POLICY_ENV]: encodedPolicy })!;
    expect(() =>
      assertNoNestedMountsUnderPolicyRoots(
        policy,
        [
          '24 1 0:20 / / rw - overlay overlay rw',
          '25 24 0:21 / /workspace/repo rw - fuse repo rw',
          '26 24 0:22 / /mnt/inputs/input.txt rw - fuse file rw',
        ].join('\n'),
      ),
    ).not.toThrow();
  });

  it('fails closed for malformed, non-canonical, or incomplete policies', () => {
    expect(() => parseSandboxWritePolicy({ [SANDBOX_WRITE_POLICY_ENV]: '{' })).toThrow(
      /invalid ORCA_SANDBOX_WRITE_POLICY/,
    );
    expect(() =>
      parseSandboxWritePolicy({
        [SANDBOX_WRITE_POLICY_ENV]: JSON.stringify({
          writablePaths: [{ path: '/mnt/session/../other', kind: 'session_output' }],
          readonlyPaths: [],
        }),
      }),
    ).toThrow(/not canonical/);
    expect(() =>
      parseSandboxWritePolicy({
        [SANDBOX_WRITE_POLICY_ENV]: JSON.stringify({
          writablePaths: [],
          readonlyPaths: [],
        }),
      }),
    ).toThrow(/is not writable/);
    expect(() =>
      parseSandboxWritePolicy({
        [SANDBOX_WRITE_POLICY_ENV]: JSON.stringify({
          writablePaths: [
            { path: OUTPUT_WRITABLE_PATH, kind: 'session_output' },
            { path: '/mnt/session', kind: 'memory_store' },
          ],
          readonlyPaths: [],
        }),
      }),
    ).toThrow(/writable roots overlap/);
    expect(() =>
      parseSandboxWritePolicy({
        [SANDBOX_WRITE_POLICY_ENV]: JSON.stringify({
          writablePaths: [{ path: OUTPUT_WRITABLE_PATH, kind: 'session_output' }],
          readonlyPaths: [],
          networkAllowedDomains: ['github.com/path'],
        }),
      }),
    ).toThrow(/networkAllowedDomains\[0\] is malformed/);
  });

  it('configures the Claude SDK sandbox as mandatory and non-bypassable', () => {
    const policy = parseSandboxWritePolicy({ [SANDBOX_WRITE_POLICY_ENV]: encodedPolicy })!;
    const settings = claudeSandboxSettings(policy);
    expect(settings).toMatchObject({
      enabled: true,
      failIfUnavailable: true,
      allowUnsandboxedCommands: false,
      enableWeakerNestedSandbox: true,
      filesystem: { allowWrite: [OUTPUT_WRITABLE_PATH, '/workspace/repo'] },
      network: {
        allowedDomains: ['github.com', 'registry.example.com'],
      },
    });
    expect(settings.network).not.toHaveProperty('allowManagedDomainsOnly');
  });

  it('probes the same nested user namespace shape used by the Claude SDK', () => {
    const policy = parseSandboxWritePolicy({ [SANDBOX_WRITE_POLICY_ENV]: encodedPolicy })!;
    expect(bubblewrapProbeArgs(policy)).toEqual([
      '--die-with-parent',
      '--ro-bind',
      '/',
      '/',
      '--dev-bind',
      '/dev',
      '/dev',
      '--unshare-user',
      '--bind',
      '/proc',
      '/proc',
      '--bind',
      OUTPUT_WRITABLE_PATH,
      OUTPUT_WRITABLE_PATH,
      '--bind',
      '/workspace/repo',
      '/workspace/repo',
      '--chdir',
      OUTPUT_WRITABLE_PATH,
      '--',
      'true',
    ]);
  });

  it('rejects a writable filesystem alias to the reserved Skill tree', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-skill-alias-'));
    try {
      const skills = join(root, 'skills');
      const writable = join(root, 'writable');
      const safe = join(root, 'safe');
      await mkdir(skills);
      await mkdir(safe);
      await symlink(skills, writable);

      expect(() =>
        assertNoWritableAliasToSkills(
          {
            writablePaths: [{ path: writable, kind: 'github_repository' }],
            readonlyPaths: [skills],
          },
          skills,
        ),
      ).toThrow(/aliases reserved Skill root/);
      expect(() =>
        assertNoWritableAliasToSkills(
          {
            writablePaths: [{ path: safe, kind: 'github_repository' }],
            readonlyPaths: [skills],
          },
          skills,
        ),
      ).not.toThrow();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('denies native Write/Edit paths outside the policy and allows a retry under outputs', async () => {
    const policy = parseSandboxWritePolicy({ [SANDBOX_WRITE_POLICY_ENV]: encodedPolicy })!;
    const canUseTool = writePolicyPermissionHandler(policy);
    const context = { signal: new AbortController().signal, toolUseID: 'tool-1' };

    await expect(
      canUseTool('Write', { file_path: '/mnt/ai_coding_poem.txt' }, context),
    ).resolves.toMatchObject({ behavior: 'deny' });
    await expect(
      canUseTool('Edit', { file_path: '/mnt/session/outputs/poem.txt' }, context),
    ).resolves.toMatchObject({ behavior: 'allow' });
    await expect(
      canUseTool(
        'Bash',
        { command: 'touch /mnt/forbidden' },
        {
          ...context,
          blockedPath: '/mnt/forbidden',
        },
      ),
    ).resolves.toMatchObject({ behavior: 'deny' });
  });
});
