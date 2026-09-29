// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createExporterHealthServer } from '../../src/health.js';

describe('exporter health HTTP boundary', () => {
  const servers: ReturnType<typeof createExporterHealthServer>[] = [];
  afterEach(async () => {
    await Promise.all(
      servers.splice(0).map(
        (server) =>
          new Promise<void>((resolve, reject) => {
            server.close((error) => (error ? reject(error) : resolve()));
            server.closeAllConnections();
          }),
      ),
    );
  });

  it('keeps dependency failure out of liveness and fails both probes while stopping', async () => {
    let live = true;
    const server = createExporterHealthServer({
      isLive: () => live,
      checkReady: async () => {
        throw new Error('secret database details');
      },
    });
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const ready = await fetch(`${origin}/readyz`);
    expect(ready.status).toBe(503);
    expect(await ready.text()).toBe('unavailable\n');
    expect((await fetch(`${origin}/healthz`)).status).toBe(200);
    live = false;
    expect((await fetch(`${origin}/healthz`)).status).toBe(503);
    expect((await fetch(`${origin}/readyz`)).status).toBe(503);
  });

  it.each(['ready', 'unready', 'error'] as const)(
    'coalesces overlapping readiness probes and retries after %s',
    async (outcome) => {
      let finishCheck!: (ready: boolean) => void;
      let failCheck!: (error: Error) => void;
      const pending = new Promise<boolean>((resolve, reject) => {
        finishCheck = resolve;
        failCheck = reject;
      });
      const checkReady = vi.fn(async () => true).mockReturnValueOnce(pending);
      const server = createExporterHealthServer({ isLive: () => true, checkReady });
      servers.push(server);
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      let arrivals = 0;
      const bothArrived = new Promise<void>((resolve) => {
        server.on('request', (request) => {
          if (request.url === '/readyz' && ++arrivals === 2) resolve();
        });
      });
      const responses = Promise.all([fetch(`${origin}/readyz`), fetch(`${origin}/readyz`)]);
      await bothArrived;
      expect(checkReady).toHaveBeenCalledTimes(1);
      expect((await fetch(`${origin}/healthz`)).status).toBe(200);
      expect(checkReady).toHaveBeenCalledTimes(1);
      if (outcome === 'error') failCheck(new Error('secret database details'));
      else finishCheck(outcome === 'ready');
      for (const response of await responses) {
        expect(response.status).toBe(outcome === 'ready' ? 200 : 503);
        expect(await response.text()).toBe(outcome === 'ready' ? 'ok\n' : 'unavailable\n');
      }
      expect((await fetch(`${origin}/readyz`)).status).toBe(200);
      expect(checkReady).toHaveBeenCalledTimes(2);
    },
  );

  it.each(['/healthz', '/readyz'])(
    'rejects POST %s without checking dependencies',
    async (path) => {
      const checkReady = vi.fn(async () => true);
      const server = createExporterHealthServer({ isLive: () => true, checkReady });
      servers.push(server);
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      expect((await fetch(`${origin}${path}`, { method: 'POST' })).status).toBe(404);
      expect(checkReady).not.toHaveBeenCalled();
    },
  );

  it('is live during startup but becomes ready only after initialization and dependency checks', async () => {
    let initialized = false;
    const checkReady = vi.fn(async () => initialized);
    const server = createExporterHealthServer({ isLive: () => true, checkReady });
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    expect((await fetch(`${origin}/healthz`)).status).toBe(200);
    expect(checkReady).not.toHaveBeenCalled();
    expect((await fetch(`${origin}/readyz`)).status).toBe(503);
    initialized = true;
    expect((await fetch(`${origin}/readyz`)).status).toBe(200);
    expect((await fetch(`${origin}/missing`)).status).toBe(404);
  });
});
