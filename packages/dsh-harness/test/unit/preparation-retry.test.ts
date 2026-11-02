// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { expect, it } from 'vitest';
import { dshProfileConfigSha256, type DshProfileFiles } from '@orca/harness-catalog';

const image = process.env.ORCA_DSH_PROFILE_IMAGE;
const execute = promisify(execFile);
it.skipIf(!image)(
  'restarts native preparation after partial failure on the same volume',
  async () => {
    const fixture = await mkdtemp(join(tmpdir(), 'orca-profile-retry-'));
    const volume = 'orca-profile-retry-' + randomUUID();
    const container = volume + '-init';
    const docker = (args: string[]) =>
      execute('docker', args, { encoding: 'utf8', timeout: 30_000 });
    const files: DshProfileFiles = {
      'package.json': JSON.stringify({
        name: 'orca-profile-retry-test',
        private: true,
        dependencies: {},
        dsh: { profile: { bundles: [] } },
      }),
      'cordis.patch.yml': '[]\n',
      'pnpm-workspace.yaml': 'packages:\n  - .\nautoInstallPeers: false\nnodeLinker: hoisted\n',
      'pnpm-lock.yaml':
        "lockfileVersion: '9.0'\n\nsettings:\n  autoInstallPeers: false\n  excludeLinksFromLockfile: false\n\nimporters:\n\n  .: {}\n",
    };
    const digest = dshProfileConfigSha256(files);
    try {
      await writeFile(
        join(fixture, 'profile.json'),
        JSON.stringify({
          version: 1,
          source_file_id: 'file_retry_test',
          source_sha256: 'a'.repeat(64),
          config_sha256: digest,
          files,
        }),
      );
      // Fail after configuration/scratch creation, without requiring a network failure.
      await writeFile(join(fixture, 'wrong-pnpm.json'), '{"version":"0.0.0"}');
      await docker(['volume', 'create', volume]);
      const attempt = async (fault: boolean) => {
        const args = [
          'run',
          '--rm',
          '--name',
          container,
          '--network',
          'none',
          '--mount',
          'type=bind,src=' + fixture + ',dst=/fixture,readonly',
          '--mount',
          'type=volume,src=' + volume + ',dst=/app/packages/dsh-harness/.orca-dsh,volume-nocopy',
        ];
        if (fault)
          args.push(
            '--mount',
            'type=bind,src=' +
              join(fixture, 'wrong-pnpm.json') +
              ',dst=/usr/local/lib/node_modules/pnpm/package.json,readonly',
          );
        args.push(
          image!,
          '/bin/sh',
          '-c',
          'node /app/packages/dsh-harness/profile/prepare.mjs < /fixture/profile.json',
        );
        return docker(args);
      };
      await expect(attempt(true)).rejects.toMatchObject({ code: 1 });
      const inspect = (script: string) =>
        docker([
          'run',
          '--rm',
          '--network',
          'none',
          '--mount',
          'type=volume,src=' + volume + ',dst=/state',
          image!,
          'node',
          '-e',
          script,
        ]);
      await inspect(
        "const fs=require('node:fs'),a=require('node:assert/strict'); a(fs.existsSync('/state/profiles/orca-managed/package.json')); a(fs.existsSync('/state/.prepare')); a(!fs.existsSync('/state/profiles/orca-managed/.orca-profile-revision'));",
      );
      expect((await attempt(false)).stdout).toContain('preparation complete');
      // A retry after the marker was written must also rebuild, never reuse partial state.
      expect((await attempt(false)).stdout).toContain('preparation complete');
      const result = await inspect(
        "const fs=require('node:fs'); console.log(fs.readFileSync('/state/profiles/orca-managed/.orca-profile-revision','utf8')); if(fs.existsSync('/state/.prepare'))process.exit(1);",
      );
      expect(result.stdout.trim()).toBe(digest);
    } finally {
      await docker(['rm', '-f', container]).catch((error: { stderr?: string }) => {
        if (!error.stderr?.includes('No such container')) throw error;
      });
      await docker(['volume', 'rm', volume]);
      await rm(fixture, { recursive: true, force: true });
    }
  },
  100_000,
);
