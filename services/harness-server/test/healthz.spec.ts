// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, afterAll } from 'vitest';
import { buildHealthzServer } from '../src/healthz.js';

describe('harness-server /healthz', () => {
  const app = buildHealthzServer();
  afterAll(() => app.close());

  it('returns 200 ok', async () => {
    const res = await app.inject({ method: 'GET', url: '/healthz' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: 'ok', service: 'harness-server' });
  });

  it('returns 200 on /readyz', async () => {
    const res = await app.inject({ method: 'GET', url: '/readyz' });
    expect(res.statusCode).toBe(200);
  });

  it('returns 503 readiness details without changing liveness', async () => {
    const unavailable = buildHealthzServer(() => ({
      ready: false,
      reasons: ['kafka_topics_unjoined'],
    }));
    try {
      const ready = await unavailable.inject({ method: 'GET', url: '/readyz' });
      const live = await unavailable.inject({ method: 'GET', url: '/healthz' });

      expect(ready.statusCode).toBe(503);
      expect(ready.json()).toEqual({
        status: 'not_ready',
        service: 'harness-server',
        reasons: ['kafka_topics_unjoined'],
      });
      expect(live.statusCode).toBe(200);
    } finally {
      await unavailable.close();
    }
  });
});
