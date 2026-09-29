// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import Fastify from 'fastify';
import { request } from 'node:http';
import { mapReadItems } from '../../src/domain/read-concurrency.js';
import {
  createReadAdmission,
  registerReadAdmission,
  type ReadAdmissionOptions,
} from '../../src/middleware/read-admission.js';
import { registerClaudePublicEdge } from '../../src/middleware/claude-edge.js';
import { rewriteApiV1Alias } from '../../src/middleware/api-v1-alias.js';

const defaults: ReadAdmissionOptions = {
  maxConcurrent: 2,
  maxPerWorkspace: 1,
  maxQueued: 4,
  maxQueuedPerWorkspace: 2,
  queueTimeoutMs: 1000,
};
afterEach(() => {
  vi.useRealTimers();
});

describe('bounded, workspace-fair read admission', () => {
  it('limits a noisy workspace while other workspaces use available slots', async () => {
    const admission = createReadAdmission(defaults);
    const first = await admission.acquire('a');
    let secondReady = false;
    const second = admission.acquire('a').then((release) => {
      secondReady = true;
      return release;
    });
    const other = await admission.acquire('b');
    expect(secondReady).toBe(false);
    other();
    first();
    (await second)();
    admission.close();
  });

  it('rotates queued workspaces instead of draining one tenant first, with idempotent release', async () => {
    const admission = createReadAdmission({ ...defaults, maxConcurrent: 1 });
    const first = await admission.acquire('a');
    const order: string[] = [];
    const a1 = admission.acquire('a').then((release) => {
      order.push('a1');
      return release;
    });
    const a2 = admission.acquire('a').then((release) => {
      order.push('a2');
      return release;
    });
    const b1 = admission.acquire('b').then((release) => {
      order.push('b1');
      return release;
    });
    first();
    first();
    (await a1)();
    (await b1)();
    (await a2)();
    expect(order).toEqual(['a1', 'b1', 'a2']);
    admission.close();
  });

  it('bounds global and tenant queues and removes canceled work without consuming a slot', async () => {
    const admission = createReadAdmission({
      ...defaults,
      maxConcurrent: 1,
      maxQueued: 2,
      maxQueuedPerWorkspace: 1,
    });
    const first = await admission.acquire('a');
    const abort = new AbortController();
    const canceled = admission.acquire('b', abort.signal);
    await expect(admission.acquire('b')).rejects.toMatchObject({ reason: 'capacity' });
    const c = admission.acquire('c');
    await expect(admission.acquire('d')).rejects.toMatchObject({ reason: 'capacity' });
    const assertion = expect(canceled).rejects.toMatchObject({ reason: 'aborted' });
    abort.abort();
    await assertion;
    first();
    (await c)();
    admission.close();
  });

  it('expires queues and drains waiters during shutdown', async () => {
    vi.useFakeTimers();
    const admission = createReadAdmission({ ...defaults, maxConcurrent: 1 });
    const first = await admission.acquire('a');
    const expires = expect(admission.acquire('b')).rejects.toMatchObject({ reason: 'timeout' });
    await vi.advanceTimersByTimeAsync(1000);
    await expires;
    const closing = expect(admission.acquire('b')).rejects.toMatchObject({ reason: 'closed' });
    admission.close();
    await closing;
    first();
    await expect(admission.acquire('c')).rejects.toMatchObject({ reason: 'closed' });
  });

  it('preserves unlimited behavior when disabled and rejects invalid bounds', async () => {
    const admission = createReadAdmission({ ...defaults, maxConcurrent: 0 });
    for (const release of await Promise.all(
      Array.from({ length: 20 }, () => admission.acquire('a')),
    ))
      release();
    admission.close();
    expect(() => createReadAdmission({ ...defaults, maxConcurrent: -1 })).toThrow('invalid');
    expect(() => createReadAdmission({ ...defaults, queueTimeoutMs: 0 })).toThrow('invalid');
  });

  it('leaves light reads, writes and SSE outside the gate and uses the compatible error envelope', async () => {
    const app = Fastify();
    app.addHook('preHandler', async (req) => {
      req.auth = { workspaceId: 'ws_test', principal: 'test', scopes: [], authMethod: 'api-key' };
    });
    registerClaudePublicEdge(app);
    registerReadAdmission(app, { ...defaults, maxConcurrent: 1, maxQueued: 0 });
    let finish!: () => void;
    let started!: () => void;
    const running = new Promise<void>((resolve) => {
      started = resolve;
    });
    app.get('/v1/sessions', async () => {
      started();
      await new Promise<void>((resolve) => {
        finish = resolve;
      });
      return { ok: true };
    });
    app.get('/api', async () => ({ ok: true }));
    app.post('/v1/sessions', async () => ({ ok: true }));
    app.get('/v1/sessions/:id/stream', async () => ({ ok: true }));
    try {
      const first = app.inject('/v1/sessions');
      // inject is lazy until consumed.
      const completion = Promise.resolve(first);
      await running;
      const rejected = await app.inject('/v1/sessions');
      expect(rejected.statusCode).toBe(429);
      expect(rejected.headers['retry-after']).toBe('1');
      expect(rejected.json()).toMatchObject({ type: 'error', error: { type: 'rate_limit_error' } });
      expect((await app.inject('/api')).statusCode).toBe(200);
      expect((await app.inject({ method: 'POST', url: '/v1/sessions' })).statusCode).toBe(200);
      expect((await app.inject('/v1/sessions/ses_test/stream')).statusCode).toBe(200);
      finish();
      expect((await completion).statusCode).toBe(200);
    } finally {
      finish?.();
      await app.close();
    }
  });

  it.each(['GET', 'HEAD'] as const)(
    'keeps disconnected %s handlers in flight until their current work settles',
    async (method) => {
      const app = Fastify();
      app.addHook('preHandler', async (req) => {
        req.auth = { workspaceId: 'ws_test', principal: 'test', scopes: [], authMethod: 'api-key' };
      });
      registerReadAdmission(app, { ...defaults, maxConcurrent: 1, maxQueued: 0 });
      let finish!: () => void;
      let started!: () => void;
      let disconnected!: () => void;
      const running = new Promise<void>((resolve) => {
        started = resolve;
      });
      const closed = new Promise<void>((resolve) => {
        disconnected = resolve;
      });
      let calls = 0;
      app.get('/v1/sessions', async (_req, reply) => {
        calls++;
        if (calls === 1) {
          reply.raw.once('close', disconnected);
          started();
          await new Promise<void>((resolve) => {
            finish = resolve;
          });
        }
        return { ok: true };
      });
      const address = await app.listen({ host: '127.0.0.1', port: 0 });
      const client = request(`${address}/v1/sessions`, { method });
      // Destroying an HTTP request without a response emits ECONNRESET locally.
      client.on('error', () => {});
      client.end();
      try {
        await running;
        client.destroy();
        await closed;
        expect((await app.inject('/v1/sessions')).statusCode).toBe(429);
        expect(calls).toBe(1);
        finish();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect((await app.inject('/v1/sessions')).statusCode).toBe(200);
        expect(calls).toBe(2);
      } finally {
        client.destroy();
        finish?.();
        await app.close();
      }
    },
  );

  it.each(['/v1/sessions', '/api/v1/sessions'])(
    'shares the GET admission budget with HEAD at %s',
    async (url) => {
      const app = Fastify({ rewriteUrl: rewriteApiV1Alias });
      registerClaudePublicEdge(app);
      app.addHook('preHandler', async (req) => {
        req.auth = { workspaceId: 'ws_test', principal: 'test', scopes: [], authMethod: 'api-key' };
      });
      registerReadAdmission(app, { ...defaults, maxConcurrent: 1, maxQueued: 0 });
      let release!: () => void;
      let started!: () => void;
      const running = new Promise<void>((resolve) => {
        started = resolve;
      });
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      let calls = 0;
      app.get('/v1/sessions', async () => {
        calls++;
        if (calls === 1) {
          started();
          await held;
        }
        return { ok: true };
      });
      const first = Promise.resolve(app.inject(url));
      try {
        await running;
        expect((await app.inject(url)).statusCode).toBe(429);
        const head = await app.inject({ method: 'HEAD', url });
        expect(head.statusCode).toBe(429);
        expect(head.headers['retry-after']).toBe('1');
        expect(head.body).toBe('');
        expect(calls).toBe(1);
        release();
        await first;
        expect((await app.inject({ method: 'HEAD', url })).statusCode).toBe(200);
        expect(calls).toBe(2);
      } finally {
        release();
        await first;
        await app.close();
      }
    },
  );

  it('retains the admission lease while siblings of a failed read are still running', async () => {
    const app = Fastify();
    app.addHook('preHandler', async (req) => {
      req.auth = { workspaceId: 'ws_test', principal: 'test', scopes: [], authMethod: 'api-key' };
    });
    registerReadAdmission(app, { ...defaults, maxConcurrent: 1, maxQueued: 0 });
    let release!: () => void;
    let started!: () => void;
    const running = new Promise<void>((resolve) => {
      started = resolve;
    });
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    let calls = 0;
    let active = 0;
    app.get('/v1/sessions', async () => {
      calls++;
      return mapReadItems([0, 1, 2], 2, async (item) => {
        if (item === 0) throw new Error('read failed');
        active++;
        started();
        try {
          await held;
          return item;
        } finally {
          active--;
        }
      });
    });
    const first = Promise.resolve(app.inject('/v1/sessions'));
    try {
      await running;
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(active).toBe(1);
      expect((await app.inject('/v1/sessions')).statusCode).toBe(429);
      expect(calls).toBe(1);
      release();
      expect((await first).statusCode).toBe(500);
      expect(active).toBe(0);
      expect((await app.inject('/v1/sessions')).statusCode).toBe(500);
    } finally {
      release();
      await first;
      await app.close();
    }
  });

  it('releases leases when a later pre-handler or the handler rejects', async () => {
    const app = Fastify();
    app.addHook('preHandler', async (req) => {
      req.auth = { workspaceId: 'ws_test', principal: 'test', scopes: [], authMethod: 'api-key' };
    });
    registerReadAdmission(app, { ...defaults, maxConcurrent: 1, maxQueued: 0 });
    let rejected = false;
    app.addHook('preHandler', async () => {
      if (!rejected) {
        rejected = true;
        throw new Error('pre-handler failure');
      }
    });
    app.get('/v1/sessions', async () => {
      throw new Error('handler failure');
    });
    try {
      for (let i = 0; i < 3; i++) {
        expect((await app.inject('/v1/sessions')).statusCode).toBe(500);
      }
    } finally {
      await app.close();
    }
  });
});
