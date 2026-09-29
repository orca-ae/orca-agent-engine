// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, afterAll } from 'vitest';
import { buildHealthzServer } from '../src/healthz.js';

describe('registry-service-ts /healthz', () => {
  const app = buildHealthzServer();
  afterAll(() => app.close());

  it('returns 200 ok', async () => {
    const res = await app.inject({ method: 'GET', url: '/healthz' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: 'ok', service: 'registry-service-ts' });
  });

  it('returns 200 on /readyz', async () => {
    const res = await app.inject({ method: 'GET', url: '/readyz' });
    expect(res.statusCode).toBe(200);
  });
});
