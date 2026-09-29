// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { lstat, mkdir, mkdtemp, opendir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import { promisify } from 'node:util';
import {
  RESOURCE_MAX_FILES,
  RESOURCE_MAX_FILE_BYTES,
  RESOURCE_MAX_TOTAL_BYTES,
  type ResourceFileDescriptor,
} from '@orca/sandbox-runtime';
import { validateGitRepositoryUrl } from './git-credentials.js';

export interface RunnerGitSnapshot {
  files: ResourceFileDescriptor[];
  open(path: string): NodeJS.ReadableStream;
  close(): Promise<void>;
}

export interface GitSnapshotInput {
  url: string;
  /** Trusted Registry proxy endpoint; the checkout process never receives a PAT. */
  source: { url: string; authorizationHeader: string };
  checkout?: Record<string, unknown>;
  /** Registry's scoped Git proxy, when one is configured; never an upstream PAT. */
  remoteUrl?: string;
  /** Reserved read-only capability include, authored by the trusted producer. */
  proxyResourceId?: string;
}

export type GitSnapshotExec = (
  args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv },
) => Promise<void>;
const execute = promisify(execFile);

/** Prepare a complete, credential-free checkout in a private Registry directory. */
export async function prepareRunnerGitSnapshot(
  input: GitSnapshotInput,
  run: GitSnapshotExec = async (args, options) => {
    try {
      await execute('git', args, { ...options, timeout: 120_000, maxBuffer: 1024 * 1024 });
    } catch {
      // Git diagnostics can include authentication context; only expose the phase.
      throw new Error('failed to prepare attached Git repository');
    }
  },
): Promise<RunnerGitSnapshot> {
  const invalidUrl = validateGitRepositoryUrl(input.url);
  if (invalidUrl) throw new Error(invalidUrl);
  const source = new URL(input.source.url);
  if (
    input.source.url.length > 4096 ||
    input.source.authorizationHeader.length > 16_384 ||
    !['http:', 'https:'].includes(source.protocol) ||
    source.username ||
    source.password ||
    source.search ||
    source.hash ||
    !/^\/v1\/git-proxy\/sesrsc_[A-Za-z0-9_-]+$/.test(source.pathname) ||
    !/^Authorization: Bearer [A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(
      input.source.authorizationHeader,
    )
  )
    throw new Error('invalid Git proxy source');
  const checkout = parseCheckout(input.checkout);
  const directory = await mkdtemp(join(tmpdir(), 'orca-git-resource-'));
  const root = join(directory, 'repository');
  const home = join(directory, 'home');
  await mkdir(home);
  const env: NodeJS.ProcessEnv = {
    PATH: process.env.PATH ?? '/usr/bin:/bin',
    HOME: home,
    GIT_CONFIG_NOSYSTEM: '1',
    GIT_CONFIG_GLOBAL: '/dev/null',
    GIT_TERMINAL_PROMPT: '0',
    GIT_LFS_SKIP_SMUDGE: '1',
    GIT_CONFIG_COUNT: '4',
    GIT_CONFIG_KEY_0: 'credential.helper',
    GIT_CONFIG_VALUE_0: '',
    GIT_CONFIG_KEY_1: 'http.followRedirects',
    GIT_CONFIG_VALUE_1: 'false',
    GIT_CONFIG_KEY_2: 'core.hooksPath',
    GIT_CONFIG_VALUE_2: '/dev/null',
    GIT_CONFIG_KEY_3: 'protocol.file.allow',
    GIT_CONFIG_VALUE_3: 'never',
  };
  for (const name of ['SSL_CERT_FILE', 'SSL_CERT_DIR', 'GIT_SSL_CAINFO']) {
    if (process.env[name]) env[name] = process.env[name];
  }
  try {
    if (checkout?.type === 'branch')
      await run(['check-ref-format', '--branch', checkout.value], { cwd: directory, env });
    await run(['init', '--template=', '--initial-branch=main', root], { cwd: directory, env });
    // Authentication exists only in this trusted child's environment, never its
    // argv, repository config, output files or the runner's snapshot.
    await run(
      [
        '--config-env=http.extraHeader=ORCA_GIT_RESOURCE_AUTH',
        '-C',
        root,
        'fetch',
        '--no-tags',
        '--depth=1',
        '--no-recurse-submodules',
        '--',
        input.source.url,
        checkout?.type === 'branch' ? `refs/heads/${checkout.value}` : (checkout?.value ?? 'HEAD'),
      ],
      {
        cwd: directory,
        env: {
          ...env,
          ORCA_GIT_RESOURCE_AUTH: input.source.authorizationHeader,
        },
      },
    );
    await run(['-C', root, 'checkout', '--detach', 'FETCH_HEAD', '--'], { cwd: directory, env });
    if (checkout?.type === 'branch')
      await run(['-C', root, 'checkout', '-B', checkout.value, 'FETCH_HEAD', '--'], {
        cwd: directory,
        env,
      });
    // This config is authored here, rather than trusting repo-selected includes,
    // hooks, filters, partial-clone promisor settings or credential helpers.
    const remoteUrl = input.remoteUrl ?? input.url;
    if (/[\r\n\0"\\]/.test(remoteUrl)) throw new Error('invalid Git snapshot remote');
    if (
      input.proxyResourceId !== undefined &&
      !/^[A-Za-z0-9_-]{1,128}$/.test(input.proxyResourceId)
    )
      throw new Error('invalid Git proxy resource');
    await writeFile(
      join(root, '.git/config'),
      [
        '[core]',
        '\trepositoryformatversion = 0',
        '\tfilemode = true',
        '\tbare = false',
        '\thooksPath = /dev/null',
        '[remote "origin"]',
        `\turl = "${remoteUrl}"`,
        '\tfetch = +refs/heads/*:refs/remotes/origin/*',
        ...(input.proxyResourceId === undefined
          ? []
          : ['[include]', `\tpath = /.orca/git/${input.proxyResourceId}.config`]),
        '',
      ].join('\n'),
      { mode: 0o600 },
    );
    // FETCH_HEAD contains only the credential-free proxy URL. No submodule
    // command or repository-controlled filter is executed during preparation.
    const files = await describeGitFiles(root);
    const paths = new Set(files.map((file) => file.path));
    return {
      files,
      open(path) {
        if (!paths.has(path)) throw new Error('undeclared Git snapshot file');
        return createReadStream(join(root, path));
      },
      close: () => rm(directory, { recursive: true, force: true }),
    };
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

function parseCheckout(
  value: Record<string, unknown> | undefined,
): { type: 'branch' | 'commit'; value: string } | undefined {
  if (value === undefined) return undefined;
  if (typeof value.value !== 'string' || value.value.length > 1024 || /[\r\n\0]/.test(value.value))
    throw new Error('invalid Git checkout');
  if (value.type === 'commit' && /^(?:[a-fA-F0-9]{40}|[a-fA-F0-9]{64})$/.test(value.value))
    return { type: 'commit', value: value.value };
  if (value.type === 'branch' && value.value.length > 0 && !value.value.startsWith('-'))
    return { type: 'branch', value: value.value };
  throw new Error('invalid Git checkout');
}

async function describeGitFiles(root: string): Promise<ResourceFileDescriptor[]> {
  const files: ResourceFileDescriptor[] = [];
  let total = 0;
  let entries = 0;
  async function scan(directory: string): Promise<void> {
    for await (const entry of await opendir(directory)) {
      if (++entries > 100_000) throw new Error('Git snapshot exceeds entry limit');
      const path = join(directory, entry.name);
      const stat = await lstat(path);
      if (stat.isDirectory()) {
        await scan(path);
        continue;
      }
      if (!stat.isFile() || stat.nlink !== 1)
        throw new Error('Git snapshot contains unsupported links or special files');
      total += stat.size;
      if (
        stat.size > RESOURCE_MAX_FILE_BYTES ||
        total > RESOURCE_MAX_TOTAL_BYTES ||
        files.length >= RESOURCE_MAX_FILES
      )
        throw new Error('Git snapshot exceeds transfer limits');
      const hash = createHash('sha256');
      for await (const bytes of createReadStream(path)) hash.update(bytes);
      files.push({
        path: relative(root, path),
        sha256: hash.digest('hex'),
        size_bytes: stat.size,
        mode: stat.mode & 0o777,
      });
    }
  }
  await scan(root);
  return files.sort((left, right) => left.path.localeCompare(right.path));
}
