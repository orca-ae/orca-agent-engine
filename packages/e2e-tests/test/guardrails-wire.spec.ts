// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — Layer A: the guardrail control plane on a live stack.
 *
 * `policy.runorca.ai/v1` is an extension group rather than part of `/v1`, so
 * it exercises edges core routes never touch: discovery has to advertise it,
 * the Claude error envelope has to reach a non-`/v1` base, and the
 * workspace-selector guard has to cover `/apis/` as well. Unit and integration
 * suites cover the handlers; this spec is the first thing that proves the
 * group is actually *served* — mounted, discoverable, and reachable through
 * the same listener split every other route obeys.
 *
 * It also pins the halves of guardrail behavior that only exist once two
 * listeners are real: an organization-tier rule minted on the admin plane and
 * visible-but-immutable from the workspace plane, and one workspace unable to
 * see another's rules.
 *
 * No model, no sandbox — `guardrails-agent.spec.ts` covers enforcement.
 * Everything here is control plane, so it runs in the cheap `test:wire` lane.
 */
import { randomUUID } from 'node:crypto';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedOrganizationAdminApiKey, seedWorkspaceApiKey } from '../src/seed.js';
import {
  archiveWorkspace,
  provisionWorkspace,
  type ProvisionedWorkspace,
} from './environment-helpers.js';

const GUARDRAILS = '/apis/policy.runorca.ai/v1/guardrails';
const GUARDRAIL_TYPES = '/apis/policy.runorca.ai/v1/guardrailtypes';
const ADMIN_GUARDRAILS = '/v1/organizations/guardrails';
const adminBaseURL = process.env['ORCA_ADMIN_BASE_URL'] ?? 'http://localhost:8082';

interface GuardrailResponse {
  id: string;
  type: 'guardrail';
  name: string;
  enabled: boolean;
  phases: string[];
  scope: 'organization' | 'workspace' | 'explicit';
  rule: Record<string, unknown>;
  archived_at: string | null;
}

interface ErrorEnvelope {
  type: 'error';
  error: { type: string; message: string };
  request_id?: string | null;
}

describe('Layer A: guardrail control plane (live registry, two listeners)', () => {
  let cfg: OrcaClientConfig;
  let adminCfg: OrcaClientConfig;
  let otherWorkspace: ProvisionedWorkspace | undefined;
  const runId = randomUUID().replaceAll('-', '').slice(0, 12);
  const created: { guardrails: string[]; orgGuardrails: string[]; agents: string[] } = {
    guardrails: [],
    orgGuardrails: [],
    agents: [],
  };

  async function createGuardrail(body: Record<string, unknown>) {
    const res = await apiCall(cfg, GUARDRAILS, { method: 'POST', body: JSON.stringify(body) });
    if (res.status === 201) created.guardrails.push(res.json<GuardrailResponse>().id);
    return res;
  }

  beforeAll(async () => {
    const seeded = await seedWorkspaceApiKey();
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    const seededAdmin = await seedOrganizationAdminApiKey();
    adminCfg = buildClientFromConfig({ baseURL: adminBaseURL, apiKey: seededAdmin.apiKey });
  });

  afterAll(async () => {
    for (const id of created.agents) {
      await apiCall(cfg, `/v1/agents/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => undefined);
    }
    // Guardrails delete last: the route refuses one an agent still names, so
    // dropping the agents first is what lets this cleanup succeed at all.
    for (const id of created.guardrails) {
      await apiCall(cfg, `${GUARDRAILS}/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => undefined);
    }
    for (const id of created.orgGuardrails) {
      await apiCall(adminCfg, `${ADMIN_GUARDRAILS}/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => undefined);
    }
    if (otherWorkspace) {
      await archiveWorkspace(adminCfg, otherWorkspace.id).catch(() => undefined);
    }
  });

  it('advertises the group in discovery and serves its resource list', async () => {
    // The group list and the resource lists are registered from one table, so
    // they cannot disagree. A wildcard `/apis/:group/:version` would answer 200
    // with an empty list for any group name a client invented — turning "not
    // served here" into "served, and empty" — so an unknown group must 404.
    const groups = await apiCall(cfg, '/apis');
    expect(groups.status, groups.text).toBe(200);
    expect(groups.text).toContain('policy.runorca.ai');

    const resources = await apiCall(cfg, '/apis/policy.runorca.ai/v1');
    expect(resources.status, resources.text).toBe(200);
    const listed = resources.json<{ resources: Array<{ name: string; kind: string }> }>();
    expect(listed.resources.map((r) => r.name).sort()).toEqual(['guardrails', 'guardrailtypes']);

    const invented = await apiCall(cfg, '/apis/nosuch.runorca.ai/v1');
    expect(invented.status).toBe(404);
  });

  it('serves the builtin catalog the harness enforces against', async () => {
    const res = await apiCall(cfg, GUARDRAIL_TYPES);
    expect(res.status, res.text).toBe(200);
    const body = res.json<{ data: Array<{ name: string; phases: string[]; internal?: boolean }> }>();
    const names = body.data.map((entry) => entry.name);
    expect(names).toContain('block_tools');
    expect(names).toContain('cost_budget');
    // `tool_permission_policy` is the seed of the fold, not a rule anyone
    // authors, so the served catalog must not advertise it.
    expect(names).not.toContain('tool_permission_policy');
  });

  it('round-trips a guardrail and defaults its phases from the catalog', async () => {
    const res = await createGuardrail({
      name: `no-shell-${runId}`,
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    });
    expect(res.status, res.text).toBe(201);
    const guardrail = res.json<GuardrailResponse>();
    expect(guardrail).toMatchObject({
      id: expect.stringMatching(/^grd_/),
      type: 'guardrail',
      enabled: true,
      phases: ['tool_call'],
      scope: 'workspace',
      archived_at: null,
    });

    const read = await apiCall(cfg, `${GUARDRAILS}/${guardrail.id}`);
    expect(read.status).toBe(200);
    expect(read.json<GuardrailResponse>().id).toBe(guardrail.id);

    const listed = await apiCall(cfg, `${GUARDRAILS}?limit=100`);
    expect(listed.status).toBe(200);
    expect(listed.json<{ data: GuardrailResponse[] }>().data.map((g) => g.id)).toContain(
      guardrail.id,
    );
  });

  it('applies the Claude error envelope to a group path, not only to /v1', async () => {
    // The group lives off `/v1`, and the edge adapter that rewrites errors was
    // scoped to `/v1` before this group landed. A 400 here arriving in some
    // other shape would mean a generated client cannot parse our errors.
    const res = await createGuardrail({
      name: `bad-${runId}`,
      rule: { kind: 'builtin', builtin: 'no_such_builtin' },
    });
    expect(res.status, res.text).toBe(400);
    const body = res.json<ErrorEnvelope>();
    expect(body.type).toBe('error');
    expect(body.error.type).toBe('invalid_request_error');
    expect(body.error.message).toContain('no_such_builtin');
  });

  it('refuses a guardrail authored onto a phase no enforcement point fires', async () => {
    // An expression rule has no catalog entry to be screened against, so every
    // modelled phase used to be accepted here — including two that reach no
    // runtime at all. Storing one would be a rule that can never evaluate.
    for (const phase of ['response', 'llm_response']) {
      const res = await createGuardrail({
        name: `unfired-${phase}-${runId}`,
        phases: [phase],
        rule: {
          kind: 'expression',
          expression: 'event.tool.name == "Bash"',
          on_false: 'deny',
        },
      });
      expect(res.status, res.text).toBe(400);
      expect(res.json<ErrorEnvelope>().error.message).toContain(phase);
    }
  });

  it('rejects a workspace_id the credential never authorized on a group path', async () => {
    // `rejectExplicitWorkspaceSelector` had to grow past `/v1` for this group:
    // resources here derive their workspace from the credential exactly as core
    // does, so a guard stopping at `/v1` would have let this body through.
    const res = await apiCall(cfg, GUARDRAILS, {
      method: 'POST',
      body: JSON.stringify({
        name: `smuggled-${runId}`,
        workspace_id: 'ws_someone_else',
        rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
      }),
    });
    expect(res.status, res.text).toBe(400);
  });

  it('refuses to delete a guardrail an agent still names', async () => {
    // A reference must not dangle while it still looks enforced. `enabled:
    // false` is the dial for turning a rule off in place.
    const guardrailRes = await createGuardrail({
      name: `referenced-${runId}`,
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    });
    expect(guardrailRes.status, guardrailRes.text).toBe(201);
    const guardrail = guardrailRes.json<GuardrailResponse>();

    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      // `guardrail_ids` is an Orca-only extension on a Claude-compatible shape,
      // so it is gated behind the beta header like every other one.
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify({
        name: `guardrailed-${runId}`,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-5-20250929' },
        system: '',
        tools: [{ type: 'agent_toolset_20260401' }],
        mcp_servers: [],
        skills: [],
        guardrail_ids: [guardrail.id],
        metadata: {},
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    created.agents.push(agentRes.json<{ id: string }>().id);

    const refused = await apiCall(cfg, `${GUARDRAILS}/${guardrail.id}`, {
      method: 'DELETE',
      body: JSON.stringify({}),
    });
    expect(refused.status, refused.text).toBe(409);
  });

  it('reports an agent guardrail reference back and keeps it across an unrelated edit', async () => {
    // Two failures live here, and both look like nothing from the outside.
    // The reference has to survive the round trip — an API that accepts a
    // guardrail id and then reports none has told the operator the rule is
    // gone. And it has to survive an edit that never mentions it: a rename
    // rewrites the version snapshot, and composition reads that snapshot, so
    // dropping the field there stops enforcement for a rule nobody touched.
    const guardrailRes = await createGuardrail({
      name: `sticky-${runId}`,
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
    });
    expect(guardrailRes.status, guardrailRes.text).toBe(201);
    const guardrailId = guardrailRes.json<GuardrailResponse>().id;

    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify({
        name: `sticky-agent-${runId}`,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-5-20250929' },
        system: '',
        tools: [{ type: 'agent_toolset_20260401' }],
        mcp_servers: [],
        skills: [],
        guardrail_ids: [guardrailId],
        metadata: {},
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    const agent = agentRes.json<{ id: string; guardrail_ids?: string[] }>();
    created.agents.push(agent.id);
    expect(agent.guardrail_ids).toEqual([guardrailId]);

    const reread = await apiCall(cfg, `/v1/agents/${agent.id}`, {
      headers: { 'orca-beta': 'guardrails' },
    });
    expect(reread.status, reread.text).toBe(200);
    expect(reread.json<{ guardrail_ids?: string[] }>().guardrail_ids).toEqual([guardrailId]);

    const renamed = await apiCall(cfg, `/v1/agents/${agent.id}`, {
      method: 'POST',
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify({ name: `sticky-agent-renamed-${runId}` }),
    });
    expect(renamed.status, renamed.text).toBe(200);
    expect(renamed.json<{ guardrail_ids?: string[] }>().guardrail_ids).toEqual([guardrailId]);

    // The row and the version snapshot are two different stores, and only the
    // snapshot drives enforcement — `prepare-execution` composes from it, not
    // from the column. Reading the new version explicitly is the only
    // assertion that covers that path; the responses above are all served from
    // the row and stay green even when the snapshot loses the field.
    const version = renamed.json<{ version: number }>().version;
    const versioned = await apiCall(cfg, `/v1/agents/${agent.id}?version=${version}`, {
      headers: { 'orca-beta': 'guardrails' },
    });
    expect(versioned.status, versioned.text).toBe(200);
    expect(versioned.json<{ guardrail_ids?: string[] }>().guardrail_ids).toEqual([guardrailId]);
  });

  it('keeps one workspace out of another workspace guardrails', async () => {
    // A second *real* workspace, not a second key. `seedWorkspaceApiKey()`
    // writes a fixed workspace id and rotates its key, so calling it again
    // would hand back another credential for this same workspace — and
    // invalidate the one this spec is already using.
    otherWorkspace = await provisionWorkspace(adminCfg, `Guardrail isolation ${runId}`);
    const otherCfg = otherWorkspace.cfg;
    const mine = await createGuardrail({
      name: `isolated-${runId}`,
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    });
    expect(mine.status, mine.text).toBe(201);
    const guardrailId = mine.json<GuardrailResponse>().id;

    const cross = await apiCall(otherCfg, `${GUARDRAILS}/${guardrailId}`);
    expect(cross.status).toBe(404);
    const crossList = await apiCall(otherCfg, `${GUARDRAILS}?limit=100`);
    expect(crossList.status).toBe(200);
    expect(crossList.json<{ data: GuardrailResponse[] }>().data.map((g) => g.id)).not.toContain(
      guardrailId,
    );
  });

  describe('organization tier', () => {
    it('is minted on the admin listener and is read-only from the workspace one', async () => {
      // This is the tier that makes guardrails governance rather than
      // configuration: the one a workspace administrator can see and cannot
      // remove. Both halves need two real listeners to mean anything.
      const res = await apiCall(adminCfg, ADMIN_GUARDRAILS, {
        method: 'POST',
        body: JSON.stringify({
          name: `org-wide-${runId}`,
          rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
        }),
      });
      expect(res.status, res.text).toBe(201);
      const guardrail = res.json<GuardrailResponse>();
      created.orgGuardrails.push(guardrail.id);
      expect(guardrail.scope).toBe('organization');

      // Readable from the workspace plane — an operator must be able to see
      // what applies to them.
      const visible = await apiCall(cfg, `${GUARDRAILS}/${guardrail.id}`);
      expect(visible.status, visible.text).toBe(200);
      expect(visible.json<GuardrailResponse>().scope).toBe('organization');

      // Every mutation from that plane is refused — checked at each mutation
      // point rather than as a read-path filter, so a route added later cannot
      // silently acquire the ability to remove a rule its author never owned.
      const update = await apiCall(cfg, `${GUARDRAILS}/${guardrail.id}`, {
        method: 'POST',
        body: JSON.stringify({ enabled: false }),
      });
      expect(update.status, update.text).toBe(403);
      const archive = await apiCall(cfg, `${GUARDRAILS}/${guardrail.id}/archive`, {
        method: 'POST',
        body: JSON.stringify({}),
      });
      expect(archive.status, archive.text).toBe(403);
      const remove = await apiCall(cfg, `${GUARDRAILS}/${guardrail.id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      });
      expect(remove.status, remove.text).toBe(403);
    });

    it('refuses to mint an organization-scoped guardrail from the workspace plane', async () => {
      const res = await createGuardrail({
        name: `escalation-${runId}`,
        scope: 'organization',
        rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
      });
      expect(res.status, res.text).toBe(403);
    });

    it('keeps the admin credential off the public listener and vice versa', async () => {
      const adminOnPublic = buildClientFromConfig({
        baseURL: cfg.baseURL,
        apiKey: adminCfg.apiKey,
      });
      const workspaceOnAdmin = buildClientFromConfig({
        baseURL: adminBaseURL,
        apiKey: cfg.apiKey,
      });
      expect((await apiCall(adminOnPublic, GUARDRAILS)).status).toBe(401);
      expect((await apiCall(workspaceOnAdmin, ADMIN_GUARDRAILS)).status).toBe(401);
    });
  });
});
