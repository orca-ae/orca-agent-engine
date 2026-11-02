// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { spawn, execFileSync } from 'node:child_process';
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';
import { validateSandboxWritePolicy } from '@orca/sandbox-runtime';
import { isWritable } from '../src/write-policy.js';

it('keeps private DSH boot paths outside the model-tool write policy', () => {
  const policy = validateSandboxWritePolicy({
    writablePaths: [{ path: '/mnt/session/outputs', kind: 'session_output' }],
    readonlyPaths: [],
  });
  for (const path of [
    '/app/packages/dsh-harness/.orca-dsh/trusted.patch.yml',
    '/app/packages/dsh-harness/.orca-dsh/profiles/orca-managed/cordis.yml',
    '/app/packages/dsh-harness/.orca-dsh/sessions/native/session.v4.jsonl',
  ])
    expect(isWritable(policy, path)).toBe(false);
  expect(isWritable(policy, '/mnt/session/outputs/result')).toBe(true);
});

// Opt in with an actual sandbox-harness image; Docker needs namespace privileges for this check.
const image = process.env.ORCA_DSH_NAMESPACE_IMAGE;
it.skipIf(!image)(
  'keeps UID 1000 DSH boot state writable and retained artifacts read-only in the real entrypoint namespace',
  async () => {
    const fixture = await mkdtemp(join(tmpdir(), 'orca-dsh-namespace-'));
    const home = '/app/packages/dsh-harness/.orca-dsh';
    const profile = home + '/profiles/orca-managed';
    const artifacts = [
      'package.json',
      'pnpm-lock.yaml',
      'pnpm-workspace.yaml',
      'cordis.patch.yml',
      '.orca-profile-revision',
      'node_modules',
    ];
    const entrypoint = fileURLToPath(new URL('../docker-entrypoint.sh', import.meta.url));
    const policy = validateSandboxWritePolicy({
      writablePaths: [{ path: '/mnt/session/outputs', kind: 'session_output' }],
      readonlyPaths: [],
    });
    try {
      await chmod(fixture, 0o755);
      await mkdir(join(fixture, 'node_modules'), { recursive: true });
      await writeFile(join(fixture, 'node_modules/retained.js'), 'retained');
      for (const name of artifacts.filter((name) => name !== 'node_modules'))
        await writeFile(join(fixture, name), 'retained');
      const probe = [
        "import assert from 'node:assert/strict'; import {writeFileSync,mkdirSync,readFileSync,mkdtempSync} from 'node:fs';",
        'const home=' + JSON.stringify(home) + '; const profile=' + JSON.stringify(profile) + ';',
        'assert.equal(process.getuid(),1000);',
        'const custom=process.env.ORCA_NAMESPACE_CUSTOM==="1";',
        'if(custom){',
        '  mkdirSync(home+"/sessions/thread",{recursive:true});',
        '  for(const path of [home+"/.worker",home+"/trusted.patch.yml",home+"/sessions/thread/session.v4.jsonl",profile+"/cordis.yml"]) writeFileSync(path,"writable");',
        '  const mounts=readFileSync("/proc/self/mountinfo","utf8").split(String.fromCharCode(10)).map(line=>line.split(" "));',
        '  for(const name of ' +
          JSON.stringify(artifacts) +
          '){const path=profile+"/"+name; assert(mounts.some(fields=>fields[4]===path&&fields[5].split(",").includes("ro")),path);}',
        '  for(const name of ["package.json","pnpm-lock.yaml","pnpm-workspace.yaml","cordis.patch.yml",".orca-profile-revision","node_modules/retained.js"]) assert.throws(()=>writeFileSync(profile+"/"+name,"forbidden"),error=>error.code==="EROFS");',
        '}else{',
        '  assert.throws(()=>writeFileSync(home+"/trusted.patch.yml","forbidden"),error=>error.code==="EROFS");',
        '}',
        'const temporary=mkdtempSync("/tmp/orca-default-"); writeFileSync(temporary+"/state","default writable");',
        'assert.throws(()=>writeFileSync("/app/model-write","forbidden"),error=>error.code==="EROFS");',
        'console.log(JSON.stringify({uid:process.getuid(),node:process.version,custom,bootMetadataWritable:custom,retainedReadOnly:custom,defaultTmpWritable:true,appOutsidePrivateHomeReadOnly:true}));',
      ].join('\n');
      await writeFile(join(fixture, 'probe.js'), probe);
      const identity = execFileSync('docker', ['image', 'inspect', image!, '--format', '{{.Id}}'], {
        encoding: 'utf8',
      }).trim();
      for (const custom of [false, true]) {
        const container = 'orca-dsh-namespace-' + randomUUID();
        const args = [
          'run',
          '--rm',
          '--name',
          container,
          '--privileged',
          '--network',
          'none',
          '--entrypoint',
          '/bin/sh',
          '-e',
          'ORCA_SANDBOX_WRITE_POLICY=' + JSON.stringify(policy),
          '-e',
          'ORCA_NAMESPACE_CUSTOM=' + (custom ? '1' : '0'),
        ];
        const mount = (source: string, target: string) =>
          args.push('--mount', 'type=bind,src=' + source + ',dst=' + target + ',readonly');
        mount(fixture, '/checks');
        mount(entrypoint, '/entrypoint.sh');
        mount(join(fixture, 'probe.js'), '/app/services/sandbox-harness/dist/index.js');
        if (custom) for (const name of artifacts) mount(join(fixture, name), profile + '/' + name);
        args.push(
          image!,
          '-c',
          'mkdir -p ' +
            profile +
            ' /mnt/session/outputs; touch /tmp/orca-sandbox-harness-ready; exec /bin/sh /entrypoint.sh',
        );
        const result = await new Promise<{ code: number | null; output: string }>(
          (done, reject) => {
            const child = spawn('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] });
            let output = '';
            child.stdout.on('data', (data) => {
              output = (output + data).slice(-65536);
            });
            child.stderr.on('data', (data) => {
              output = (output + data).slice(-65536);
            });
            const timer = setTimeout(() => child.kill('SIGTERM'), 30_000);
            child.on('error', (error) => {
              clearTimeout(timer);
              reject(error);
            });
            child.on('close', (code) => {
              clearTimeout(timer);
              done({ code, output });
            });
          },
        );
        expect(result.code, container + ': ' + result.output).toBe(0);
        process.stdout.write(JSON.stringify({ image: identity }) + '\n' + result.output);
      }
    } finally {
      await rm(fixture, { recursive: true, force: true });
    }
  },
  90_000,
);
