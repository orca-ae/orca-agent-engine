// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { afterEach, describe, expect, it } from 'vitest';
import {
  prepareRunnerGitSnapshot,
  type GitSnapshotExec,
  type RunnerGitSnapshot,
} from '../../src/domain/runner-git-snapshot.js';

const exec = promisify(execFile);
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
const url = 'https://github.com/example/private.git';
const token = 'unit.test.signature';
const source = {
  url: 'https://registry.example/v1/git-proxy/sesrsc_git',
  authorizationHeader: `Authorization: Bearer ${token}`,
};

async function repository() {
  const root = await mkdtemp(join(tmpdir(), 'orca-git-snapshot-test-'));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  const git = async (...args: string[]) =>
    (
      await exec('git', ['-C', root, ...args], {
        env: {
          PATH: process.env.PATH,
          HOME: root,
          GIT_CONFIG_NOSYSTEM: '1',
          GIT_CONFIG_GLOBAL: '/dev/null',
        },
      })
    ).stdout.trim();
  await git('init', '--initial-branch=main');
  await git('config', 'user.name', 'Test');
  await git('config', 'user.email', 'test@example.com');
  await writeFile(join(root, 'README.md'), 'first');
  await git('add', '.');
  await git('commit', '-m', 'first');
  const first = await git('rev-parse', 'HEAD');
  await git('checkout', '-b', 'feature');
  await writeFile(join(root, 'README.md'), 'feature');
  await git('add', '.');
  await git('commit', '-m', 'feature');
  const calls: Array<{ args: string[]; env: NodeJS.ProcessEnv }> = [];
  const run: GitSnapshotExec = async (args, options) => {
    calls.push({ args, env: options.env });
    // Real Git checkout test using a local fixture instead of network/provider credentials.
    await exec(
      'git',
      args.map((arg) => (arg === source.url ? root : arg)),
      {
        ...options,
        env: { ...options.env, GIT_CONFIG_VALUE_3: 'always' },
      },
    );
  };
  return { root, first, git, calls, run };
}
async function contents(snapshot: RunnerGitSnapshot, path: string) {
  let value = '';
  for await (const chunk of snapshot.open(path)) value += String(chunk);
  return value;
}

describe('Registry Git snapshot', () => {
  it('sets the scoped proxy origin and a read-only external auth include without embedding credentials', async () => {
    const fixture = await repository();
    const remoteUrl = 'https://registry.example/v1/git-proxy/sesrsc_git';
    const snapshot = await prepareRunnerGitSnapshot(
      { url, source, remoteUrl, proxyResourceId: 'sesrsc_git' },
      fixture.run,
    );
    cleanup.push(snapshot.close);
    const config = await contents(snapshot, '.git/config');
    expect(config).toContain(remoteUrl);
    expect(config).toContain('path = /.orca/git/sesrsc_git.config');
    expect(config).not.toContain(token);
    expect(config).not.toContain('extraHeader');
  });
  it('checks out the requested branch using transient trusted credentials and exports sanitized Git data', async () => {
    const fixture = await repository();
    const snapshot = await prepareRunnerGitSnapshot(
      { url, source, checkout: { type: 'branch', value: 'feature' } },
      fixture.run,
    );
    cleanup.push(snapshot.close);
    expect(await contents(snapshot, 'README.md')).toBe('feature');
    expect(await contents(snapshot, '.git/HEAD')).toContain('refs/heads/feature');
    const config = await contents(snapshot, '.git/config');
    expect(config).toContain(url);
    expect(config).not.toContain(token);
    expect(config).not.toContain('extraHeader');
    expect(snapshot.files.some((file) => file.path.startsWith('.git/hooks/'))).toBe(false);
    expect(fixture.calls.every((call) => !call.args.join(' ').includes(token))).toBe(true);
    const credentials = fixture.calls.filter((call) => call.env.ORCA_GIT_RESOURCE_AUTH);
    expect(credentials).toHaveLength(1);
    expect(credentials[0]!.args).toContain('fetch');
    expect(credentials[0]!.args).toContain(source.url);
    expect(credentials[0]!.args).not.toContain(url);
    expect(credentials[0]!.env.ORCA_GIT_RESOURCE_AUTH).toBe(source.authorizationHeader);
    expect(credentials[0]!.env).not.toHaveProperty('ORCA_RUNNER_TUNNEL_TOKEN');
  });

  it('actually checks out an older commit instead of the branch tip', async () => {
    const fixture = await repository();
    const snapshot = await prepareRunnerGitSnapshot(
      { url, source, checkout: { type: 'commit', value: fixture.first } },
      fixture.run,
    );
    cleanup.push(snapshot.close);
    expect(await contents(snapshot, 'README.md')).toBe('first');
    expect((await contents(snapshot, '.git/HEAD')).trim()).toBe(fixture.first);
    expect(() => snapshot.open('../home')).toThrow('undeclared');
  });

  it('rejects links and cleans private preparation directories on failure', async () => {
    const fixture = await repository();
    await symlink('/etc/passwd', join(fixture.root, 'unsafe'));
    await fixture.git('add', '.');
    await fixture.git('commit', '-m', 'link');
    await expect(prepareRunnerGitSnapshot({ url, source }, fixture.run)).rejects.toThrow(
      'unsupported links',
    );
    const directory = fixture.calls[0]!.args.at(-1)!;
    await expect(readFile(join(directory, '.git/config'))).rejects.toMatchObject({
      code: 'ENOENT',
    });
  });

  it.each([
    { url: 'file:///tmp/repo', source },
    { url: 'https://secret@example.com/repo/test', source },
    { url, source, checkout: { type: 'branch', value: '--upload-pack=bad' } },
    { url, source, checkout: { type: 'commit', value: 'HEAD~1' } },
    { url, source: { ...source, url } },
    { url, source: { ...source, authorizationHeader: 'Authorization: Basic raw-pat' } },
  ])('rejects unsupported URLs and checkout parameters before executing Git', async (input) => {
    let called = false;
    await expect(
      prepareRunnerGitSnapshot(input, async () => {
        called = true;
      }),
    ).rejects.toThrow();
    expect(called).toBe(false);
  });
});
