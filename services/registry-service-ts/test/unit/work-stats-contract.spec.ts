// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { environmentsContract } from '../../src/contracts/environments.contract.js';

/**
 * Schema-level contract coverage for the public work-queue-stats surface
 * (`GET /v1/environments/:id/work_stats`). The handler is hand-mounted on the
 * Fastify app (environments.routes.ts) rather than served from ts-rest, so this
 * spec pins the declared method / path / params / response shape in the contract
 * — catching drift on the published work_stats wire shape that nothing else
 * covers. The field names mirror Anthropic's work-queue claim/lease/stats shape:
 * `depth`, `in_flight`, `worker_connected`.
 */

describe('environmentsContract — work_stats route shape', () => {
  it('declares work_stats as GET /v1/environments/:id/work_stats', () => {
    const route = environmentsContract.workStats;
    expect(route.method).toBe('GET');
    expect(route.path).toBe('/v1/environments/:id/work_stats');
  });

  it('requires an env_… path param', () => {
    const route = environmentsContract.workStats;
    expect(route.pathParams.safeParse({ id: 'env_abc' }).success).toBe(true);
    expect(route.pathParams.safeParse({ id: 'ses_abc' }).success).toBe(false);
    expect(route.pathParams.safeParse({ id: '' }).success).toBe(false);
  });

  it('declares the 200 body as { depth, in_flight, worker_connected }', () => {
    const ok = environmentsContract.workStats.responses[200];
    expect(ok.safeParse({ depth: 3, in_flight: 2, worker_connected: true }).success).toBe(true);
    expect(ok.safeParse({ depth: 0, in_flight: 0, worker_connected: false }).success).toBe(true);
  });

  it('rejects negative or non-integer counts (queue counts are non-negative integers)', () => {
    const ok = environmentsContract.workStats.responses[200];
    expect(ok.safeParse({ depth: -1, in_flight: 0, worker_connected: false }).success).toBe(false);
    expect(ok.safeParse({ depth: 1.5, in_flight: 0, worker_connected: false }).success).toBe(false);
    expect(ok.safeParse({ depth: 0, in_flight: -2, worker_connected: false }).success).toBe(false);
  });

  it('requires the worker_connected boolean and both count fields', () => {
    const ok = environmentsContract.workStats.responses[200];
    expect(ok.safeParse({ depth: 0, in_flight: 0 }).success).toBe(false);
    expect(ok.safeParse({ in_flight: 0, worker_connected: false }).success).toBe(false);
    expect(ok.safeParse({ depth: 0, worker_connected: false }).success).toBe(false);
    expect(ok.safeParse({ depth: 0, in_flight: 0, worker_connected: 'yes' }).success).toBe(false);
  });

  it('declares a 404 for an unknown environment', () => {
    const notFound = environmentsContract.workStats.responses[404];
    expect(notFound.safeParse({ error: 'not found' }).success).toBe(true);
  });
});
