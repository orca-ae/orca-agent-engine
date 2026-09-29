// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { runInNewContext } from 'node:vm';
import { describe, expect, it, vi } from 'vitest';

const require = createRequire(import.meta.url);
const root = dirname(require.resolve('pulsar-client/package.json'));
const source = readFileSync(join(root, 'orca-build.cjs'), 'utf8');
const archive = Buffer.from('verified archive fixture');
const checksum = createHash('sha512').update(archive).digest('hex');

function install(
  fetch: ReturnType<typeof vi.fn>,
  {
    platform = 'darwin',
    run = vi.fn(),
  }: { platform?: string; run?: ReturnType<typeof vi.fn> } = {},
) {
  const copy = vi.fn();
  const write = vi.fn();
  const cleanup = vi.fn();
  const sleep = vi.fn().mockResolvedValue(undefined);
  const timeout = vi.fn(() => new AbortController().signal);
  const mockRequire = Object.assign(
    (name: string) => {
      if (name === 'node:child_process') return { execFileSync: run };
      if (name === 'node:timers/promises') return { setTimeout: sleep };
      if (name === 'node:fs') {
        return {
          copyFileSync: copy,
          cpSync: vi.fn(),
          mkdirSync: vi.fn(),
          mkdtempSync: () => '/tmp/orca-pulsar-build-fixture',
          rmSync: cleanup,
          writeFileSync: write,
        };
      }
      return require(name);
    },
    { resolve: (name: string) => `/fixture/node_modules/${name}` },
  );
  // Run the installed build script's real main/download/checksum path. Only
  // network, time and filesystem/compiler I/O are doubles; pin a fixture archive.
  const result = runInNewContext(
    `${source.slice(0, source.indexOf('\nmain().catch'))}
artifacts[fixtureTarget][1] = fixtureChecksum;
main();`,
    {
      require: mockRequire,
      __dirname: root,
      process: {
        platform,
        arch: 'arm64',
        execPath: process.execPath,
        report: { getReport: () => ({ header: { glibcVersionRuntime: '2.36' } }) },
      },
      console: { log: vi.fn(), warn: vi.fn() },
      Buffer,
      AbortSignal: { timeout },
      fetch,
      fixtureChecksum: checksum,
      fixtureTarget: platform === 'linux' ? 'linux-glibc-arm64' : 'darwin-arm64',
    },
  ) as Promise<void>;
  return { result, run, copy, write, cleanup, sleep, timeout };
}

function response(bytes = archive) {
  return { ok: true, status: 200, arrayBuffer: async () => bytes };
}

describe('patched Pulsar native build', () => {
  it.each(['darwin', 'linux'])(
    'does not impose the archive timeout on %s compilation',
    async (platform) => {
      const build = install(vi.fn().mockResolvedValue(response()), { platform });
      await expect(build.result).resolves.toBeUndefined();
      const calls = build.run.mock.calls;
      expect(calls.slice(0, -1).every(([, , options]) => options.timeout === 300_000)).toBe(true);
      expect(calls.at(-1)).toEqual([
        process.execPath,
        ['/fixture/node_modules/@mapbox/node-pre-gyp/bin/node-pre-gyp', 'rebuild'],
        { cwd: '/tmp/orca-pulsar-build-fixture', stdio: 'inherit' },
      ]);
      expect(build.copy).toHaveBeenCalledWith(
        '/tmp/orca-pulsar-build-fixture/lib/binding/pulsar.node',
        join(root, 'lib/binding/pulsar.node'),
      );
      expect(build.cleanup).toHaveBeenCalledOnce();
    },
  );

  it('propagates compiler failure without retrying or copying an unfinished binding', async () => {
    const failure = new Error('compiler exited with status 1');
    const run = vi.fn((command: string) => {
      if (command === process.execPath) throw failure;
    });
    const fetch = vi.fn().mockResolvedValue(response());
    const build = install(fetch, { platform: 'linux', run });
    await expect(build.result).rejects.toBe(failure);
    expect(fetch).toHaveBeenCalledOnce();
    expect(run).toHaveBeenCalledTimes(3);
    expect(build.copy.mock.calls.some(([path]) => path.endsWith('pulsar.node'))).toBe(false);
    expect(build.cleanup).toHaveBeenCalledOnce();
  });

  it('recovers from the archive timeout seen in release image builds', async () => {
    const fetch = vi
      .fn()
      .mockRejectedValueOnce(
        new DOMException('The operation was aborted due to timeout', 'TimeoutError'),
      )
      .mockResolvedValueOnce(response());
    const build = install(fetch);
    await expect(build.result).resolves.toBeUndefined();
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(build.timeout.mock.calls).toEqual([[300_000], [300_000]]);
    expect(build.sleep).toHaveBeenCalledWith(1_000);
    expect(build.write).toHaveBeenCalledWith('/tmp/orca-pulsar-build-fixture/archive', archive);
    expect(build.run).toHaveBeenCalledTimes(2);
    expect(build.cleanup).toHaveBeenCalledOnce();
  });

  it('restarts the complete download when reading the response body fails', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: async () => Promise.reject(new TypeError('terminated')),
      })
      .mockResolvedValueOnce(response());
    await expect(install(fetch).result).resolves.toBeUndefined();
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it.each([408, 429, 500, 502, 503, 504])('retries HTTP %s', async (status) => {
    const cancel = vi.fn().mockResolvedValue(undefined);
    const fetch = vi
      .fn()
      .mockResolvedValueOnce({ ok: false, status, body: { cancel } })
      .mockResolvedValueOnce(response());
    await expect(install(fetch).result).resolves.toBeUndefined();
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it.each([403, 404])('does not retry permanent HTTP %s failures', async (status) => {
    const fetch = vi
      .fn()
      .mockResolvedValue({ ok: false, status, body: { cancel: async () => undefined } });
    const build = install(fetch);
    await expect(build.result).rejects.toThrow(`HTTP ${status}`);
    expect(fetch).toHaveBeenCalledOnce();
    expect(build.sleep).not.toHaveBeenCalled();
    expect(build.run).not.toHaveBeenCalled();
    expect(build.cleanup).toHaveBeenCalledOnce();
  });

  it('fails after three attempts without compiling or writing an unverified archive', async () => {
    const fetch = vi.fn().mockRejectedValue(new TypeError('fetch failed'));
    const build = install(fetch);
    await expect(build.result).rejects.toThrow(/download failed.*3 attempts/);
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(build.sleep.mock.calls).toEqual([[1_000], [2_000]]);
    expect(build.write).not.toHaveBeenCalled();
    expect(build.run).not.toHaveBeenCalled();
    expect(build.cleanup).toHaveBeenCalledOnce();
  });

  it('does not retry or build an archive with a checksum mismatch', async () => {
    const fetch = vi.fn().mockResolvedValue(response(Buffer.from('corrupt archive')));
    const build = install(fetch);
    await expect(build.result).rejects.toThrow('archive checksum mismatch');
    expect(fetch).toHaveBeenCalledOnce();
    expect(build.write).not.toHaveBeenCalled();
    expect(build.run).not.toHaveBeenCalled();
    expect(build.cleanup).toHaveBeenCalledOnce();
  });
});
