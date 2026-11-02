// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, writeFile, rm, realpath, cp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';
import {
  dshProfileConfigSha256,
  DSH_PROFILE_DIRECTORY,
  type DshProfileFiles,
} from '@orca/harness-catalog';

// Explicit opt-in: use the built DSH image or an isolated runtime with Linux native addons.
// The bundle is preinstalled fixture code; installer/provider qualification belongs to separate tests.
const runtime = process.env.ORCA_DSH_PROFILE_RUNTIME;
const image = process.env.ORCA_DSH_PROFILE_IMAGE;
it.skipIf(!runtime && !image)(
  'runs a real custom bridge/tool, denies missing grants and restores frozen identity',
  async () => {
    const fixture = await realpath(await mkdtemp(join(tmpdir(), 'orca-custom-native-')));
    const root = fileURLToPath(new URL('../../../../', import.meta.url));
    const business = join(fixture, 'node_modules/orca-profile-fixture');
    const put = async (name: string, text: string) => writeFile(join(fixture, name), text);
    try {
      await mkdir(business, { recursive: true });
      const files: DshProfileFiles = {
        'package.json': JSON.stringify({
          name: 'orca-profile-test',
          private: true,
          dependencies: { 'orca-profile-fixture': '1.0.0' },
          dsh: { profile: { bundles: ['orca-profile-fixture'] } },
        }),
        'cordis.patch.yml': '[]\n',
        'pnpm-workspace.yaml': 'packages:\n  - .\nautoInstallPeers: false\nnodeLinker: hoisted\n',
        'pnpm-lock.yaml':
          "lockfileVersion: '9.0'\nsettings:\n  autoInstallPeers: false\n  excludeLinksFromLockfile: false\nimporters:\n  .:\n    dependencies:\n      orca-profile-fixture:\n        specifier: 1.0.0\n        version: 1.0.0\npackages:\n  orca-profile-fixture@1.0.0:\n    resolution:\n      integrity: sha512-" +
          createHash('sha512').update('fixture').digest('base64') +
          '\nsnapshots:\n  orca-profile-fixture@1.0.0: {}\n',
      };
      for (const [name, text] of Object.entries(files)) await put(name, text);
      const profile = {
        version: 1,
        source_file_id: 'file_native_test',
        source_sha256: 'a'.repeat(64),
        config_sha256: dshProfileConfigSha256(files),
        files,
      };
      await put('descriptor.json', JSON.stringify(profile));
      await put('.orca-profile-revision', profile.config_sha256);
      await writeFile(
        join(business, 'package.json'),
        JSON.stringify({
          name: 'orca-profile-fixture',
          version: '1.0.0',
          type: 'module',
          main: 'index.mjs',
          peerDependencies: {
            '@deepseek-ai/cordis': '4.0.5-alpha.1',
            '@deepseek-ai/dsh-tools': '0.2.1-alpha.2',
          },
          dsh: { bundle: { patch: 'cordis.patch.yml' } },
        }),
      );
      await writeFile(
        join(business, 'cordis.patch.yml'),
        '- insert:\n    - id: business.probe\n      name: ./index.mjs\n',
      );
      await writeFile(
        join(business, 'index.mjs'),
        [
          "import assert from 'node:assert/strict';",
          "import { appendFileSync, existsSync, readFileSync } from 'node:fs';",
          "import { Context } from '@deepseek-ai/cordis';",
          "import Tools from '@deepseek-ai/dsh-tools';",
          "export const inject = ['tools'];",
          'export function apply(ctx) {',
          '  assert(ctx instanceof Context); assert(ctx.tools instanceof Tools);',
          "  ctx.on('agent/created', ({agent}) => {",
          '    if (agent.session.header.parentSession) return;',
          "    ctx.tools.register({ name: 'business_probe', description: 'Approved fixture tool',",
          "      parameters: {type:'object', properties:{n:{type:existsSync('/tmp/orca-fixture-schema-type') ? readFileSync('/tmp/orca-fixture-schema-type','utf8') : 'integer'}},required:['n'],additionalProperties:false},",
          "      output: {schema:{type:'string'}, render: (_args,value) => [{type:'text',text:value}]},",
          "      execute: async (args,exec) => { exec.signal.throwIfAborted(); appendFileSync('/tmp/business-effects', args.n + String.fromCharCode(10));",
          "        if (args.n === 7) ctx.tools.register({name:'late_probe',description:'Unsupported late registration',parameters:{type:'object'},output:{schema:{type:'string'},render:()=>[]},execute:async()=> 'unexpected'});",
          '        return String(args.n); } });',
          '  });',
          '}',
        ].join('\n'),
      );
      // Keep each container's fixture immutable, including across Docker bind-mount caches.
      const aliasModules = join(fixture, 'alias-node_modules');
      await cp(join(fixture, 'node_modules'), aliasModules, { recursive: true });
      await writeFile(
        join(aliasModules, 'orca-profile-fixture/cordis.patch.yml'),
        JSON.stringify([
          {
            insert: [
              {
                id: 'business.probe',
                name: '/app/packages/dsh-harness/node_modules/@deepseek-ai/dsh-tools/lib/index.js',
              },
            ],
          },
        ]),
      );
      const container = 'orca-custom-profile-' + randomUUID();
      const args = ['run', '--rm', '--name', container, '--network', 'none', '--memory', '512m'];
      const mount = (src: string, dst: string) =>
        args.push('--mount', 'type=bind,src=' + src + ',dst=' + dst + ',readonly');
      mount(fixture, '/fixture');
      if (runtime) {
        mount(resolve(runtime, 'node_modules'), '/app/packages/dsh-harness/node_modules');
        mount(join(root, 'node_modules/.pnpm'), '/app/node_modules/.pnpm');
        for (const name of ['sdk-harness', 'harness-catalog']) {
          mount(join(root, 'packages', name), '/app/packages/' + name);
        }
        for (const name of ['dist', 'profile', 'package.json'])
          mount(join(root, 'packages/dsh-harness', name), '/app/packages/dsh-harness/' + name);
      }
      for (const name of [...Object.keys(files), 'node_modules', '.orca-profile-revision'])
        mount(join(fixture, name), DSH_PROFILE_DIRECTORY + '/' + name);
      mount(
        join(root, 'packages/dsh-harness/test/support/custom-profile-driver.mjs'),
        '/driver.mjs',
      );
      const imageIndex = args.length;
      args.push(
        image ?? 'node:22.23.2-bookworm',
        'node',
        ...(runtime
          ? [
              '--input-type=module',
              '-e',
              "import {mkdirSync,symlinkSync} from 'node:fs'; mkdirSync('/app/node_modules/@orca',{recursive:true}); for (const name of ['sdk-harness','harness-catalog']) symlinkSync('/app/packages/'+name,'/app/node_modules/@orca/'+name); await import('/driver.mjs');",
            ]
          : ['/driver.mjs']),
      );
      const run = () =>
        new Promise<{ code: number | null; output: string }>((done, reject) => {
          const child = spawn('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] });
          let output = '';
          child.stdout.on('data', (data) => {
            output = (output + data).slice(-65536);
          });
          child.stderr.on('data', (data) => {
            output = (output + data).slice(-65536);
          });
          const timer = setTimeout(() => child.kill('SIGTERM'), 90_000);
          child.on('error', reject);
          child.on('close', (code) => {
            clearTimeout(timer);
            done({ code, output });
          });
        });
      const result = await run();
      expect(result.code, container + ': ' + result.output).toBe(0);
      process.stdout.write(result.output);
      expect(result.output).toContain('"toolExecuted":true');
      expect(result.output).toContain('"coldRestore":true');
      const moduleMount = args.findIndex((value) =>
        value.startsWith('type=bind,src=' + join(fixture, 'node_modules') + ','),
      );
      expect(moduleMount).toBeGreaterThan(-1);
      args[moduleMount] =
        'type=bind,src=' +
        aliasModules +
        ',dst=' +
        DSH_PROFILE_DIRECTORY +
        '/node_modules,readonly';
      args[3] = container + '-alias';
      args.splice(imageIndex, 0, '-e', 'ORCA_EXPECT_MANAGED_ALIAS=1');
      const alias = await run();
      expect(alias.code, args[3] + ': ' + alias.output).toBe(0);
      process.stdout.write(alias.output);
    } finally {
      await rm(fixture, { recursive: true, force: true });
    }
  },
  100_000,
);
