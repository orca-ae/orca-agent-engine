// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const files = vi.hoisted(() => ({ values: new Map(), writes: [], rename: vi.fn() }));
vi.mock('node:fs/promises', () => ({
  readFile: vi.fn(async (path) => {
    if (!files.values.has(path))
      throw Object.assign(new Error('missing fixture'), { code: 'ENOENT' });
    return files.values.get(path);
  }),
  writeFile: vi.fn(async (path, bytes, options) => files.writes.push({ path, bytes, options })),
  rename: files.rename,
  mkdir: vi.fn(),
}));
import { bootstrap, networkEnvironment } from '../../bootstrap/network.mjs';

const root = '/run/orca/sandbox-network';
const response = {
  binding_id: 'network',
  generation: 'one',
  proxy_url: 'https://proxy.example:8443',
  proxy_capability: 'base-only',
  internal_bypass: ['registry.example'],
};
beforeEach(() => {
  files.writes.length = 0;
  files.rename.mockClear();
  files.values = new Map([
    ['/var/run/orca/sandbox-network/token', 'pod-bound-token'],
    ['/etc/ssl/certs/ca-certificates.crt', '-----BEGIN CERTIFICATE-----system'],
    ['/etc/orca/sandbox-egress/ca-bundle.crt', '-----BEGIN CERTIFICATE-----managed'],
  ]);
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe('trusted network bootstrap barrier', () => {
  it('uses only the dedicated token and publishes context last after trust and client config', async () => {
    const fetch = vi.fn(async () => Response.json(response));
    vi.stubGlobal('fetch', fetch);
    await bootstrap('http://registry.example:8081', 'network', 'one');
    const [url, options] = fetch.mock.calls[0];
    expect(url.pathname).toBe('/internal/v1/sandbox-networks/network/bootstrap');
    expect(options.headers.authorization).toBe('Bearer pod-bound-token');
    expect(JSON.parse(options.body)).toEqual({ generation: 'one' });
    expect(files.writes.at(-1).path).toBe(root + '/context.json.tmp');
    expect(files.rename).toHaveBeenCalledWith(root + '/context.json.tmp', root + '/context.json');
    const context = JSON.parse(files.writes.at(-1).bytes);
    expect(context.ca_bundle_path).toBe(root + '/ca-bundle.crt');
    expect(files.writes.find((file) => file.path === context.ca_bundle_path).bytes).toContain(
      'system',
    );
    expect(files.writes.find((file) => file.path === context.ca_bundle_path).bytes).toContain(
      'managed',
    );
    expect(JSON.stringify(files.writes)).not.toContain('pod-bound-token');
    expect(networkEnvironment(context).HTTPS_PROXY).toContain('base-only');
    expect(networkEnvironment(context).GH_TOKEN).toBeUndefined();
  });
  it.each([401, 403, 404])(
    'never publishes context after Registry rejection %s',
    async (status) => {
      vi.stubGlobal(
        'fetch',
        vi.fn(async () => new Response('private', { status })),
      );
      await expect(bootstrap('http://registry.example', 'network', 'one')).rejects.toThrow(
        'denied',
      );
      expect(files.writes).toEqual([]);
      expect(files.rename).not.toHaveBeenCalled();
    },
  );
  it('waits for authoritative enforcement without making installer output available', async () => {
    vi.useFakeTimers();
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response('', { status: 503 }))
      .mockResolvedValueOnce(Response.json(response));
    vi.stubGlobal('fetch', fetch);
    const pending = bootstrap('http://registry.example', 'network', 'one');
    await vi.advanceTimersByTimeAsync(499);
    expect(files.writes).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    await pending;
    expect(fetch).toHaveBeenCalledTimes(2);
  });
  it.each([
    { ...response, generation: 'successor' },
    { ...response, workspace_id: 'old-wire' },
  ])('rejects wrong generation and old wire', async (value) => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => Response.json(value)),
    );
    await expect(bootstrap('http://registry.example', 'network', 'one')).rejects.toThrow(
      'identity mismatch',
    );
    expect(files.writes).toEqual([]);
  });
  it('does not publish context when the public trust mount is unavailable', async () => {
    files.values.delete('/etc/orca/sandbox-egress/ca-bundle.crt');
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => Response.json(response)),
    );
    await expect(bootstrap('http://registry.example', 'network', 'one')).rejects.toThrow();
    expect(files.writes).toEqual([]);
  });
  it('preserves an already published Sandbox capability on same-Pod init retry', async () => {
    const fetch = vi.fn();
    vi.stubGlobal('fetch', fetch);
    files.values.set(
      root + '/context.json',
      JSON.stringify({ ...response, ca_bundle_path: root + '/ca-bundle.crt' }),
    );
    await bootstrap('http://registry.example', 'network', 'one');
    expect(fetch).not.toHaveBeenCalled();
    expect(files.writes).toEqual([]);
    await expect(bootstrap('http://registry.example', 'network', 'successor')).rejects.toThrow(
      'generation mismatch',
    );
  });
  it('bounds Registry outage retries and never publishes context on timeout', async () => {
    vi.useFakeTimers();
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        throw new Error('unreachable');
      }),
    );
    const outcome = bootstrap('http://registry.example', 'network', 'one').catch((error) => error);
    await vi.advanceTimersByTimeAsync(60000);
    expect((await outcome).message).toContain('timed out');
    expect(files.writes).toEqual([]);
    expect(files.rename).not.toHaveBeenCalled();
  });
});
