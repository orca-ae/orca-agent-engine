// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  asLocalSandboxHandle,
  LocalSandboxRuntime,
  type SandboxManagerLike,
} from '../../../src/local/runtime.js';

const fixture = vi.hoisted(() => ({ root: '' }));
vi.mock('../../../src/environment-packages.js', async (importOriginal) => {
  const original = await importOriginal<typeof import('../../../src/environment-packages.js')>();
  const fs = await import('node:fs');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  fixture.root = fs.mkdtempSync(join(tmpdir(), 'orca-package-acquire-'));
  // The real copy script runs in a real child, gated by the parent's event loop.
  const gate = `{
    const fs = require('node:fs');
    fs.writeFileSync(${JSON.stringify(join(fixture.root, 'started'))}, '');
    const deadline = Date.now() + 3000;
    while (!fs.existsSync(${JSON.stringify(join(fixture.root, 'release'))})) {
      if (Date.now() > deadline) throw new Error('parent event loop did not release package copy');
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
    }
  }\n`;
  return {
    ...original,
    ENVIRONMENT_PACKAGES_EXEC_ROOT: join(fixture.root, 'source'),
    ENVIRONMENT_PACKAGE_COPY_SCRIPT: gate + original.ENVIRONMENT_PACKAGE_COPY_SCRIPT,
  };
});

beforeEach(() => {
  rmSync(fixture.root, { recursive: true, force: true });
  mkdirSync(join(fixture.root, 'source'), { recursive: true });
  writeFileSync(join(fixture.root, 'source', 'tool'), 'installed dependency');
});
afterAll(() => rmSync(fixture.root, { recursive: true, force: true }));

function createRuntime(manager: SandboxManagerLike) {
  // Staging uses portable filesystem/process APIs. The fake manager never runs
  // namespace tools; select only the constructor's Linux guard on other hosts.
  const platform = Object.getOwnPropertyDescriptor(process, 'platform')!;
  try {
    Object.defineProperty(process, 'platform', { value: 'linux' });
    return new LocalSandboxRuntime({
      harnessWorkDir: join(fixture.root, 'work'),
      allowedNetworkHosts: [],
      managedToolFilesystem: true,
      useEnvironmentPackages: true,
      manager,
    });
  } finally {
    Object.defineProperty(process, 'platform', platform);
  }
}

function createManager(): SandboxManagerLike {
  return {
    initialize: vi.fn(async () => {}),
    wrapWithSandbox: vi.fn(async (command) => command),
  };
}

describe('LocalSandboxRuntime prepared package staging', () => {
  it('keeps the event loop responsive and waits for the copy before initialization', async () => {
    const manager = createManager();
    let settled = false;
    const pending = createRuntime(manager)
      .acquire({})
      .finally(() => {
        settled = true;
      });
    void pending.catch(() => {});
    try {
      await expect
        .poll(() => existsSync(join(fixture.root, 'started')), { timeout: 5000, interval: 10 })
        .toBe(true);
      expect(settled).toBe(false);
      expect(manager.initialize).not.toHaveBeenCalled();
      writeFileSync(join(fixture.root, 'release'), '');
      const sandbox = await pending;
      const root = asLocalSandboxHandle(sandbox).rootDir();
      expect(readFileSync(join(root, 'tool'), 'utf8')).toBe('installed dependency');
      expect(statSync(root).mode & 0o777).toBe(0o700);
      expect(manager.initialize).toHaveBeenCalledOnce();
    } finally {
      writeFileSync(join(fixture.root, 'release'), '');
      await (await pending.catch(() => undefined))?.destroy();
    }
  }, 10_000);

  it('cleans the private root after a child failure without initializing the manager', async () => {
    const manager = createManager();
    writeFileSync(join(fixture.root, 'release'), '');
    // Source validation fails before the copy script's own cleanup block.
    rmSync(join(fixture.root, 'source'), { recursive: true, force: true });
    writeFileSync(join(fixture.root, 'source'), 'not a directory');
    await expect(createRuntime(manager).acquire({})).rejects.toThrow(/invalid package source/);
    expect(manager.initialize).not.toHaveBeenCalled();
    expect(readdirSync(join(fixture.root, 'work', 'sessions'))).toEqual([]);
  });
});
