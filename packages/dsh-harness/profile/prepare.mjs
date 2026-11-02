// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Image-owned, credential-free init entrypoint. The provider supplies only a retained descriptor.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { dirname, join, relative, isAbsolute } from 'node:path';
import { lstat, mkdir, readFile, readdir, realpath, rm, writeFile } from 'node:fs/promises';
import {
  DSH_PROFILE_DIRECTORY,
  DSH_PROFILE_HOME,
  DSH_PROFILE_FILENAMES,
  DSH_PROFILE_TIMEOUT_MS,
  parseDshProfileDescriptor,
  validateDshProfileConfiguration,
} from '@orca/harness-catalog';

const require = createRequire(import.meta.url);
let child;
function stop() {
  if (child?.pid) {
    try {
      process.kill(-child.pid, 'SIGKILL');
    } catch {
      /* already exited */
    }
  }
}
const deadline = setTimeout(() => {
  stop();
  process.exit(1);
}, DSH_PROFILE_TIMEOUT_MS - 5000);
for (const signal of ['SIGTERM', 'SIGINT'])
  process.on(signal, () => {
    stop();
    process.exit(1);
  });

async function run(executable, args, env) {
  await new Promise((resolve, reject) => {
    child = spawn(executable, args, {
      cwd: DSH_PROFILE_DIRECTORY,
      env,
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    // Drain without retaining or exposing package-controlled diagnostics or environment.
    child.stdout.resume();
    child.stderr.resume();
    child.once('error', reject);
    child.once('close', (code) => {
      child = undefined;
      if (code === 0) resolve();
      else reject(new Error('native DSH installer exited unsuccessfully'));
    });
  });
}

try {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of process.stdin) {
    bytes += chunk.length;
    if (bytes > 7 * 1024 * 1024) throw new Error('profile descriptor exceeds limit');
    chunks.push(chunk);
  }
  const input = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks));
  const profile = parseDshProfileDescriptor(JSON.parse(input));
  validateDshProfileConfiguration(profile.files);
  const cliManifestPath = require.resolve('@deepseek-ai/dsh/package.json');
  if (JSON.parse(await readFile(cliManifestPath, 'utf8')).version !== '0.2.1-alpha.2')
    throw new Error('unsupported native DSH version');
  // Init retries share this Pod-private volume; the runtime has not started yet.
  for (const name of await readdir(DSH_PROFILE_HOME))
    await rm(join(DSH_PROFILE_HOME, name), { recursive: true, force: true });
  await mkdir(DSH_PROFILE_DIRECTORY, { recursive: true });
  for (const name of DSH_PROFILE_FILENAMES)
    await writeFile(join(DSH_PROFILE_DIRECTORY, name), profile.files[name], {
      flag: 'wx',
      mode: 0o644,
    });

  const scratch = join(DSH_PROFILE_HOME, '.prepare');
  await mkdir(scratch);
  // An allowlist excludes runtime credentials, proxy settings and ambient npm configuration.
  const env = {
    PATH: '/usr/local/bin:/usr/bin:/bin',
    HOME: scratch,
    DSH_HOME: DSH_PROFILE_HOME,
    CI: 'true',
    DSH_TELEMETRY_DISABLED: '1',
    NPM_CONFIG_USERCONFIG: join(scratch, 'npmrc'),
    NPM_CONFIG_GLOBALCONFIG: join(scratch, 'global-npmrc'),
    XDG_CACHE_HOME: scratch,
    XDG_CONFIG_HOME: scratch,
    TMPDIR: scratch,
    NODE_OPTIONS: '--require=' + fileURLToPath(new URL('./registry-origin.cjs', import.meta.url)),
  };
  await writeFile(env.NPM_CONFIG_USERCONFIG, '');
  await writeFile(env.NPM_CONFIG_GLOBALCONFIG, '');
  // The image ships this executable; Corepack must not download an ambient version.
  const pnpmManifest = JSON.parse(
    await readFile('/usr/local/lib/node_modules/pnpm/package.json', 'utf8'),
  );
  if (pnpmManifest.version !== '9.15.9') throw new Error('unsupported pnpm version');
  await run(
    process.execPath,
    [
      join(dirname(cliManifestPath), 'lib/bin.js'),
      'plugin',
      '--profile',
      'orca-managed',
      'install',
      '--frozen-lockfile',
      '--ignore-scripts',
      '--registry=https://registry.npmjs.org',
      '--config.auto-install-peers=false',
      '--config.node-linker=hoisted',
      '--store-dir=' + join(scratch, 'store'),
      '--fetch-retries=0',
      '--fetch-timeout=60000',
    ],
    env,
  );
  for (const name of DSH_PROFILE_FILENAMES)
    if ((await readFile(join(DSH_PROFILE_DIRECTORY, name), 'utf8')) !== profile.files[name])
      throw new Error('native installation changed retained configuration');

  const root = join(DSH_PROFILE_DIRECTORY, 'node_modules');
  await mkdir(root, { recursive: true });
  const image = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
  const shared = new Set(
    Object.keys(image.dependencies).filter((name) => name.startsWith('@deepseek-ai/')),
  );
  const installed = [];
  let retainedBytes = 0;
  let retainedFiles = 0;
  async function inspect(path) {
    const stat = await lstat(path);
    if (++retainedFiles > 50000) throw new Error('profile dependencies exceed file limit');
    if (stat.isSymbolicLink()) {
      const target = await realpath(path);
      const within = relative(root, target);
      if (within.startsWith('..') || isAbsolute(within))
        throw new Error('profile dependency escapes module tree');
    } else if (stat.isDirectory()) {
      for (const name of await readdir(path)) await inspect(join(path, name));
    } else if (stat.isFile()) {
      retainedBytes += stat.size;
      if (retainedBytes > 1024 * 1024 * 1024)
        throw new Error('profile dependencies exceed size limit');
      if (/\/node_modules\/(?:@[^/]+\/)?[^/]+\/package\.json$/.test(path)) {
        const manifest = JSON.parse(await readFile(path, 'utf8'));
        if (shared.has(manifest.name))
          throw new Error('profile contains an independent managed runtime copy');
        installed.push([path, manifest]);
      }
    } else throw new Error('profile dependencies contain a special file');
  }
  await inspect(root);
  for (const [path, manifest] of installed) {
    const consumer = createRequire(path);
    for (const name of Object.keys(manifest.peerDependencies ?? {})) {
      if (
        shared.has(name) &&
        (await realpath(consumer.resolve(name))) !== (await realpath(require.resolve(name)))
      )
        throw new Error('profile peer does not resolve to the image runtime');
    }
  }
  const manifest = JSON.parse(profile.files['package.json']);
  for (const bundle of manifest.dsh?.profile?.bundles ?? []) {
    const entry = installed.find(([, value]) => value.name === bundle);
    if (!entry?.[1].dsh?.bundle?.patch) throw new Error('selected bundle was not installed');
  }
  await rm(scratch, { recursive: true, force: true });
  await rm(join(DSH_PROFILE_DIRECTORY, '.plugin-manager'), { recursive: true, force: true });
  await writeFile(join(DSH_PROFILE_DIRECTORY, '.orca-profile-revision'), profile.config_sha256, {
    flag: 'wx',
    mode: 0o644,
  });
  process.stdout.write('DSH profile native preparation complete' + String.fromCharCode(10));
} catch {
  stop();
  process.stderr.write('DSH profile native preparation failed' + String.fromCharCode(10));
  process.exitCode = 1;
} finally {
  clearTimeout(deadline);
}
