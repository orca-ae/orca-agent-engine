// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { buildSandboxEgressGitCloneCommand } from '../../src/sandbox-egress-git.js';

describe('sandbox Git preparation process', () => {
  let root: string;
  let repository: string;
  let environment: NodeJS.ProcessEnv;
  const remote = 'https://github.com/fixture/repository.git';
  const git = (args: string[], cwd: string) =>
    execFileSync('git', ['-c', 'safe.directory=' + cwd, ...args], {
      cwd,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    }).trim();

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'orca-egress-git-'));
    repository = join(root, 'origin');
    mkdirSync(repository);
    git(['init', '-b', 'main'], repository);
    git(['config', 'user.name', 'Fixture'], repository);
    git(['config', 'user.email', 'fixture@example.test'], repository);
    writeFileSync(join(repository, 'README.md'), 'first');
    git(['add', '.'], repository);
    git(['commit', '-m', 'first'], repository);
    const bin = join(root, 'bin');
    mkdirSync(bin);
    if (process.platform === 'darwin') {
      // Production images use GNU mv; exercise the same implementation on macOS.
      const gmv = execFileSync('which', ['gmv'], { encoding: 'utf8' }).trim();
      symlinkSync(gmv, join(bin, 'mv'));
    }
    environment = {
      ...process.env,
      PATH: bin + ':' + process.env.PATH,
      GIT_CONFIG_NOSYSTEM: '1',
      GIT_CONFIG_GLOBAL: '/dev/null',
      GIT_CONFIG_COUNT: '1',
      GIT_CONFIG_KEY_0: 'url.file://' + repository + '.insteadOf',
      GIT_CONFIG_VALUE_0: remote,
    };
  });

  afterEach(() => rmSync(root, { recursive: true, force: true }));

  it('checks out an older commit atomically while preserving the original HTTPS remote', () => {
    const commit = git(['rev-parse', 'HEAD'], repository);
    writeFileSync(join(repository, 'README.md'), 'second');
    git(['commit', '-am', 'second'], repository);
    const mountPath = join(root, 'mounted repo');
    mkdirSync(mountPath);
    const command = buildSandboxEgressGitCloneCommand({
      url: remote,
      mountPath,
      checkout: { type: 'commit', value: commit },
    });
    execFileSync('bash', ['-c', command], { env: environment });
    expect(git(['rev-parse', 'HEAD'], mountPath)).toBe(commit);
    expect(readFileSync(join(mountPath, 'README.md'), 'utf8')).toBe('first');
    expect(git(['config', '--get', 'remote.origin.url'], mountPath)).toBe(remote);
    expect(readdirSync(root).some((name) => name.startsWith('.orca-git-'))).toBe(false);
  });

  it('preserves existing mount contents and removes staging after a failed branch checkout', () => {
    const mountPath = join(root, 'mounted');
    mkdirSync(mountPath);
    writeFileSync(join(mountPath, 'keep'), 'user work');
    const command = buildSandboxEgressGitCloneCommand({
      url: remote,
      mountPath,
      checkout: { type: 'branch', value: 'missing' },
    });
    expect(() =>
      execFileSync('bash', ['-c', command], { env: environment, stdio: 'pipe' }),
    ).toThrow();
    expect(readFileSync(join(mountPath, 'keep'), 'utf8')).toBe('user work');
    expect(readdirSync(root).some((name) => name.startsWith('.orca-git-'))).toBe(false);
  });
});
