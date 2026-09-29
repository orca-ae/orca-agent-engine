// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

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

/**
 * The `multiagent` (Anthropic thread-model coordinator) field on the agent
 * contract: create-time validation, roster snapshotting with pinned versions,
 * the ≤20 cap, and one-level delegation.
 */
describe('Agents multiagent field (integration)', () => {
  let app: FastifyInstance;
  let apiKey: string;

  beforeAll(async () => {
    const { db } = await getTestDb();
    app = buildApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
    apiKey = await createTestApiKey(db, uniqueWorkspace('multiagent'));
  });
  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  async function createAgent(payload: Record<string, unknown>) {
    return app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        ...payload,
      },
    });
  }

  it('creates a plain agent with multiagent = null', async () => {
    const res = await createAgent({ name: `plain-${Date.now()}` });
    expect(res.statusCode).toBe(200);
    expect(res.json().multiagent).toBeNull();
  });

  it('creates a coordinator and snapshots the roster with pinned versions', async () => {
    const researcher = await createAgent({ name: `researcher-${Date.now()}` });
    const writer = await createAgent({ name: `writer-${Date.now()}` });
    const researcherId = researcher.json().id;
    const writerId = writer.json().id;

    const coordinator = await createAgent({
      name: `coordinator-${Date.now()}`,
      multiagent: {
        type: 'coordinator',
        agents: [
          { type: 'agent', id: researcherId },
          { type: 'agent', id: writerId },
          { type: 'self' },
        ],
      },
    });
    expect(coordinator.statusCode).toBe(200);
    const body = coordinator.json();
    // `resolveMultiagentForStorage` canonicalizes a `{ type: 'self' }` entry
    // into a concrete, version-pinned reference to the coordinator itself, so
    // the stored roster is fully resolved and immutable. The contract's roster
    // union admits both forms; the live route always emits the resolved one.
    expect(body.multiagent).toEqual({
      type: 'coordinator',
      agents: [
        { type: 'agent', id: researcherId, version: 1 },
        { type: 'agent', id: writerId, version: 1 },
        { type: 'agent', id: body.id, version: 1 },
      ],
    });

    // Round-trips on GET.
    const got = await app.inject({
      method: 'GET',
      url: `/v1/agents/${body.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(got.json().multiagent).toEqual(body.multiagent);
  });

  it('surfaces the snapshot only at the top-level multiagent field, not inside metadata', async () => {
    const leaf = await createAgent({ name: `meta-leaf-${Date.now()}` });
    const leafId = leaf.json().id;
    const coord = await createAgent({
      name: `meta-coord-${Date.now()}`,
      multiagent: { type: 'coordinator', agents: [{ type: 'agent', id: leafId }] },
      metadata: { team: 'research' },
    });
    // Create response: the internal storage key is stripped from the user-facing
    // metadata blob; only the top-level `multiagent` field carries the snapshot.
    expect(coord.json().multiagent).not.toBeNull();
    expect(coord.json().metadata).toEqual({ team: 'research' });
    expect(coord.json().metadata).not.toHaveProperty('multiagent');

    // GET is consistent — no double surfacing, caller metadata preserved.
    const got = await app.inject({
      method: 'GET',
      url: `/v1/agents/${coord.json().id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(got.json().metadata).not.toHaveProperty('multiagent');
    expect(got.json().metadata).toEqual({ team: 'research' });
    expect(got.json().multiagent.agents[0].id).toBe(leafId);

    // A plain (single-agent) agent's metadata is untouched.
    const plain = await createAgent({
      name: `meta-plain-${Date.now()}`,
      metadata: { team: 'ops' },
    });
    expect(plain.json().metadata).toEqual({ team: 'ops' });
    expect(plain.json().multiagent).toBeNull();
  });

  it('pins the roster agent CURRENT version at create (snapshot is immutable)', async () => {
    const roster = await createAgent({ name: `roster-${Date.now()}` });
    const rosterId = roster.json().id;
    // Bump the roster agent to v2.
    const bumped = await app.inject({
      method: 'POST',
      url: `/v1/agents/${rosterId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { system: 'v2' },
    });
    expect(bumped.json().version).toBe(2);

    // A coordinator created NOW pins the current (v2).
    const coord = await createAgent({
      name: `coord-pin-${Date.now()}`,
      multiagent: { type: 'coordinator', agents: [{ type: 'agent', id: rosterId }] },
    });
    expect(coord.json().multiagent.agents[0]).toEqual({
      type: 'agent',
      id: rosterId,
      version: 2,
    });
  });

  it('honors an explicit version pin', async () => {
    const roster = await createAgent({ name: `pinned-${Date.now()}` });
    const rosterId = roster.json().id;
    await app.inject({
      method: 'POST',
      url: `/v1/agents/${rosterId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { system: 'v2' },
    });
    const coord = await createAgent({
      name: `coord-explicit-${Date.now()}`,
      multiagent: { type: 'coordinator', agents: [{ type: 'agent', id: rosterId, version: 1 }] },
    });
    expect(coord.json().multiagent.agents[0].version).toBe(1);
  });

  it('rejects a roster referencing an agent not in the workspace', async () => {
    const res = await createAgent({
      name: `bad-roster-${Date.now()}`,
      multiagent: { type: 'coordinator', agents: [{ type: 'agent', id: 'agt_missing_xyz' }] },
    });
    expect(res.statusCode).toBe(400);
    // The live resolver is `resolveMultiagentForStorage` in `agents.routes.ts`,
    // not the dormant `src/domain/multiagent.ts` copy this spec was written
    // against, so the wording names the reference rather than the workspace.
    // `/v1` errors are wrapped by `registerClaudePublicEdge` (e3c4aa64) into
    // `{type, error: {type, message}, request_id}` — hence `.error.message`.
    expect(res.json().error.message).toBe('multiagent referenced agent agt_missing_xyz not found');
  });

  it('rejects a roster larger than 20', async () => {
    const agents = Array.from({ length: 21 }, () => ({ type: 'agent', id: 'agt_x' }));
    const res = await createAgent({
      name: `too-big-${Date.now()}`,
      multiagent: { type: 'coordinator', agents },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects a two-level coordinator (a roster agent that is itself a coordinator)', async () => {
    const leaf = await createAgent({ name: `leaf-${Date.now()}` });
    const leafId = leaf.json().id;
    const midCoord = await createAgent({
      name: `mid-${Date.now()}`,
      multiagent: { type: 'coordinator', agents: [{ type: 'agent', id: leafId }] },
    });
    expect(midCoord.statusCode).toBe(200);
    const midId = midCoord.json().id;

    const topCoord = await createAgent({
      name: `top-${Date.now()}`,
      multiagent: { type: 'coordinator', agents: [{ type: 'agent', id: midId }] },
    });
    expect(topCoord.statusCode).toBe(400);
    expect(topCoord.json().error.message).toBe(
      'multiagent referenced agents must not themselves have multiagent set',
    );
  });

  it('rejects an empty roster', async () => {
    const res = await createAgent({
      name: `empty-${Date.now()}`,
      multiagent: { type: 'coordinator', agents: [] },
    });
    expect(res.statusCode).toBe(400);
  });

  it('re-snapshots the roster on update and can clear it', async () => {
    const a = await createAgent({ name: `upd-a-${Date.now()}` });
    const b = await createAgent({ name: `upd-b-${Date.now()}` });
    const aId = a.json().id;
    const bId = b.json().id;

    const coord = await createAgent({
      name: `upd-coord-${Date.now()}`,
      multiagent: { type: 'coordinator', agents: [{ type: 'agent', id: aId }] },
    });
    const coordId = coord.json().id;

    // Re-roster to [b].
    const rerostered = await app.inject({
      method: 'POST',
      url: `/v1/agents/${coordId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { multiagent: { type: 'coordinator', agents: [{ type: 'agent', id: bId }] } },
    });
    expect(rerostered.json().multiagent.agents[0].id).toBe(bId);

    // Omitting `multiagent` on a later update preserves the snapshot.
    const preserved = await app.inject({
      method: 'POST',
      url: `/v1/agents/${coordId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { system: 'touch' },
    });
    expect(preserved.json().multiagent.agents[0].id).toBe(bId);

    // Explicit null clears it.
    const cleared = await app.inject({
      method: 'POST',
      url: `/v1/agents/${coordId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { multiagent: null },
    });
    expect(cleared.json().multiagent).toBeNull();
  });
});
