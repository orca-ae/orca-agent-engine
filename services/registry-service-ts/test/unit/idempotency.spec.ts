// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeEach } from 'vitest';
import Fastify from 'fastify';
import {
  buildIdempotencyPreHandler,
  buildIdempotencyResponseHook,
  computeBodyHash,
  type IdempotencyStore,
  idempotencyHandler,
} from '../../src/middleware/idempotency.js';

describe('idempotency', () => {
  it('reuses the original body hash when the handler normalizes the parsed body', async () => {
    const cached = new Map<string, NonNullable<Awaited<ReturnType<IdempotencyStore['get']>>>>();
    const store: IdempotencyStore = {
      async get(workspace, scope, key) {
        return cached.get(`${workspace}/${scope}/${key}`) ?? null;
      },
      async put(workspace, scope, key, value) {
        cached.set(`${workspace}/${scope}/${key}`, value);
      },
    };
    const app = Fastify();
    app.addHook('preHandler', async (req) => {
      req.auth = { workspaceId: 'ws_test', principal: 'test', scopes: [], authMethod: 'api-key' };
    });
    app.addHook('preHandler', buildIdempotencyPreHandler(store));
    app.addHook('onSend', buildIdempotencyResponseHook(store));
    let writes = 0;
    app.post('/v1/test', async (req) => {
      (req.body as Record<string, unknown>).normalized = true;
      return { writes: ++writes };
    });
    try {
      for (let i = 0; i < 2; i++) {
        const response = await app.inject({
          method: 'POST',
          url: '/v1/test',
          headers: { 'idempotency-key': 'same' },
          payload: { original: true },
        });
        expect(response.statusCode).toBe(200);
        expect(response.json()).toEqual({ writes: 1 });
      }
      const conflict = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: { 'idempotency-key': 'same' },
        payload: { original: false },
      });
      expect(conflict.statusCode).toBe(409);
      expect(writes).toBe(1);
    } finally {
      await app.close();
    }
  });

  describe('computeBodyHash', () => {
    it('returns the same sha256 for identical bodies', () => {
      const a = computeBodyHash(Buffer.from('{"a":1}'));
      const b = computeBodyHash(Buffer.from('{"a":1}'));
      expect(a).toBe(b);
      expect(a).toMatch(/^[a-f0-9]{64}$/);
    });
    it('returns a different sha256 for different bodies', () => {
      expect(computeBodyHash(Buffer.from('{"a":1}'))).not.toBe(
        computeBodyHash(Buffer.from('{"a":2}')),
      );
    });
  });

  describe('idempotencyHandler', () => {
    let store: Map<string, { status: number; body: string; bodyHash: string; expiresAt: Date }>;
    let mockStore: IdempotencyStore;

    beforeEach(() => {
      store = new Map();
      mockStore = {
        get: async (workspaceId, scope, key) => store.get(`${workspaceId}|${scope}|${key}`) ?? null,
        put: async (workspaceId, scope, key, value) => {
          store.set(`${workspaceId}|${scope}|${key}`, value);
        },
      };
    });

    it('returns null on first call (no cached response)', async () => {
      const result = await idempotencyHandler(
        mockStore,
        'ws_x',
        'POST /v1/agents',
        'idem-1',
        Buffer.from('{}'),
      );
      expect(result).toBeNull();
    });

    it('returns cached response on second call with same body', async () => {
      await idempotencyHandler(mockStore, 'ws_x', 'POST /v1/agents', 'idem-1', Buffer.from('{}'));
      // Simulate the post-handler save:
      await mockStore.put('ws_x', 'POST /v1/agents', 'idem-1', {
        status: 201,
        body: '{"ok":true}',
        bodyHash: computeBodyHash(Buffer.from('{}')),
        expiresAt: new Date(Date.now() + 60_000),
      });
      const result = await idempotencyHandler(
        mockStore,
        'ws_x',
        'POST /v1/agents',
        'idem-1',
        Buffer.from('{}'),
      );
      expect(result).toEqual({ status: 201, body: '{"ok":true}' });
    });

    it('returns conflict marker on second call with different body', async () => {
      await mockStore.put('ws_x', 'POST /v1/agents', 'idem-1', {
        status: 201,
        body: '{"ok":true}',
        bodyHash: computeBodyHash(Buffer.from('{"first":true}')),
        expiresAt: new Date(Date.now() + 60_000),
      });
      const result = await idempotencyHandler(
        mockStore,
        'ws_x',
        'POST /v1/agents',
        'idem-1',
        Buffer.from('{"different":true}'),
      );
      expect(result).toEqual({
        status: 409,
        body: { error: 'idempotency-key reused with different body' },
      });
    });

    it('treats expired entries as cache miss', async () => {
      await mockStore.put('ws_x', 'POST /v1/agents', 'idem-1', {
        status: 201,
        body: '{"ok":true}',
        bodyHash: computeBodyHash(Buffer.from('{}')),
        expiresAt: new Date(Date.now() - 1000), // expired
      });
      const result = await idempotencyHandler(
        mockStore,
        'ws_x',
        'POST /v1/agents',
        'idem-1',
        Buffer.from('{}'),
      );
      expect(result).toBeNull();
    });
  });

  describe('buildIdempotencyResponseHook', () => {
    it('skips caching when Fastify supplies no payload', async () => {
      let putCalled = false;
      const store: IdempotencyStore = {
        get: async () => null,
        put: async () => {
          putCalled = true;
          throw new Error('put should not be called');
        },
      };
      const hook = buildIdempotencyResponseHook(store);
      const payload = await hook(
        requestStub(),
        { statusCode: 200 } as Parameters<typeof hook>[1],
        undefined,
      );

      expect(payload).toBeUndefined();
      expect(putCalled).toBe(false);
    });

    it('does not fail the successful response when cache write fails', async () => {
      const warnings: unknown[] = [];
      const store: IdempotencyStore = {
        get: async () => null,
        put: async () => {
          throw new Error('db unavailable');
        },
      };
      const hook = buildIdempotencyResponseHook(store);
      const payload = await hook(
        requestStub(warnings),
        { statusCode: 200 } as Parameters<typeof hook>[1],
        '{"ok":true}',
      );

      expect(payload).toBe('{"ok":true}');
      expect(warnings).toHaveLength(1);
    });
  });

  describe('buildIdempotencyPreHandler', () => {
    it('sends cached responses without continuing the handler chain', async () => {
      const store: IdempotencyStore = {
        get: async () => ({
          status: 201,
          body: '{"ok":true}',
          bodyHash: computeBodyHash(Buffer.from(JSON.stringify({ events: [] }))),
          expiresAt: new Date(Date.now() + 60_000),
        }),
        put: async () => {},
      };
      const sent: unknown[] = [];
      const preHandler = buildIdempotencyPreHandler(store);

      await preHandler(requestStub(), {
        code: (status: number) => {
          sent.push({ status });
          return {
            send: (body: unknown) => sent.push({ body }),
          };
        },
      } as unknown as Parameters<typeof preHandler>[1]);

      expect(sent).toEqual([{ status: 201 }, { body: '{"ok":true}' }]);
    });
  });
});

function requestStub(warnings: unknown[] = []) {
  return {
    method: 'POST',
    headers: { 'idempotency-key': 'idem-1' },
    auth: { workspaceId: 'ws_x' },
    routeOptions: { url: '/v1/sessions/:id/events' },
    url: '/v1/sessions/ses_x/events',
    body: { events: [] },
    log: {
      warn: (...args: unknown[]) => warnings.push(args),
    },
  } as unknown as Parameters<ReturnType<typeof buildIdempotencyResponseHook>>[0];
}
