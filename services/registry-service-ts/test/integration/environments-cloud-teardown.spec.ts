// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Integration coverage for I3 — archive/delete best-effort tearing down a
// `target=cloud` environment's launcher-level box via
// `EnvironmentLaunchTrigger.terminate`. Kept in its OWN file (rather than
// added to `environments.spec.ts`) so its fake lifecycle is scoped to just
// these tests and can never affect the many `environmentLaunchLifecycle`-less
// tests in that file.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildApp } from '../../src/server.js';
import type { EnvironmentLaunchTrigger } from '../../src/api/sessions.routes.js';

/** Records every `terminate` call; `ensureLaunched`/`relaunch` are unused by these routes but required by the interface. */
class FakeLifecycle implements EnvironmentLaunchTrigger {
  readonly terminateCalls: string[] = [];
  terminateShouldFail = false;

  async ensureLaunched(): Promise<{ status: string; error?: string }> {
    return { status: 'launched' };
  }

  async relaunch(): Promise<{ status: string; error?: string }> {
    return { status: 'launched' };
  }

  async terminate(environmentId: string): Promise<void> {
    this.terminateCalls.push(environmentId);
    if (this.terminateShouldFail) {
      throw new Error('provider terminate API is down');
    }
  }
}

describe('Environments cloud teardown on archive/delete (integration, I3)', () => {
  let app: FastifyInstance;
  let apiKey: string;
  let lifecycle: FakeLifecycle;

  beforeAll(async () => {
    const { db } = await getTestDb();
    lifecycle = new FakeLifecycle();
    app = buildApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      environmentLaunchLifecycle: lifecycle,
    });
    await app.ready();
    apiKey = await createTestApiKey(db, uniqueWorkspace('env-teardown'));
  });
  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  async function createEnvironment(
    target: string | null,
  ): Promise<{ id: string; target: string | null }> {
    // `orca-beta`: this helper reports the resolved `target` back to its
    // callers, and `target` is an Orca-only field the create response carries
    // only on the beta branch — the default response is Anthropic's
    // `BetaEnvironment` projection, which has no `target`.
    const res = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json', 'orca-beta': '1' },
      payload: { name: `teardown-${Date.now()}-${Math.random()}`, ...(target ? { target } : {}) },
    });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    return { id: body.id as string, target: (body.target ?? null) as string | null };
  }

  it('archiving a target=cloud environment calls lifecycle.terminate(id)', async () => {
    const { id } = await createEnvironment('cloud');
    const archive = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    expect(lifecycle.terminateCalls).toContain(id);
  });

  it('archiving a self_hosted environment does NOT call lifecycle.terminate', async () => {
    const { id } = await createEnvironment('self_hosted');
    lifecycle.terminateCalls.length = 0;
    const archive = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    expect(lifecycle.terminateCalls).not.toContain(id);
  });

  // `create` defaults an omitted `target` to `cloud` (`body.target ?? 'cloud'`
  // in `environments.routes.ts`, since e3c4aa64 aligned the public API with
  // Claude Managed Agents), so "no target set" is no longer a reachable state
  // through this route — the created environment IS a cloud one and teardown
  // must fire. `self_hosted` above remains the case that must not terminate.
  it('archiving an environment created without an explicit target terminates it (target defaults to cloud)', async () => {
    const { id, target } = await createEnvironment(null);
    expect(target).toBe('cloud');
    lifecycle.terminateCalls.length = 0;
    const archive = await app.inject({
      method: 'POST',
      url: `/v1/environments/${id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    expect(lifecycle.terminateCalls).toContain(id);
  });

  it('deleting a target=cloud environment calls lifecycle.terminate(id)', async () => {
    const { id } = await createEnvironment('cloud');
    const del = await app.inject({
      method: 'DELETE',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(del.statusCode).toBe(200);
    expect(lifecycle.terminateCalls).toContain(id);
  });

  it('deleting a self_hosted environment does NOT call lifecycle.terminate', async () => {
    const { id } = await createEnvironment('self_hosted');
    lifecycle.terminateCalls.length = 0;
    const del = await app.inject({
      method: 'DELETE',
      url: `/v1/environments/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(del.statusCode).toBe(200);
    expect(lifecycle.terminateCalls).not.toContain(id);
  });

  it('a terminate failure does not fail the archive request (best-effort)', async () => {
    const { id } = await createEnvironment('cloud');
    lifecycle.terminateShouldFail = true;
    try {
      const archive = await app.inject({
        method: 'POST',
        url: `/v1/environments/${id}/archive`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {},
      });
      expect(archive.statusCode).toBe(200);
      expect(archive.json().archived_at).toBeTruthy();
      expect(lifecycle.terminateCalls).toContain(id);
    } finally {
      lifecycle.terminateShouldFail = false;
    }
  });

  it('a terminate failure does not fail the delete request (best-effort)', async () => {
    const { id } = await createEnvironment('cloud');
    lifecycle.terminateShouldFail = true;
    try {
      const del = await app.inject({
        method: 'DELETE',
        url: `/v1/environments/${id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {},
      });
      expect(del.statusCode).toBe(200);
      expect(lifecycle.terminateCalls).toContain(id);
    } finally {
      lifecycle.terminateShouldFail = false;
    }
  });
});
