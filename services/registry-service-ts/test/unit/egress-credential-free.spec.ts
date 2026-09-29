// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the STRUCTURAL credential-free guarantee.
//
// The egress config + snapshot are secret-free BY CONSTRUCTION (vault-id
// references + caller-minted JWTs only). The earlier test guard enforced that by
// serialize-and-string-match (assert the JSON does not contain `real_secret` /
// `password` / `ghp_` / `sk-`). That heuristic would NOT catch a NOVEL
// secret-shaped field name — e.g. a future `api_key` / `client_secret` /
// `vault_secret` field accidentally threaded into a rewritten server or a sidecar
// source. `assertEgressCredentialFree` / `assertSnapshotCredentialFree` close that
// gap STRUCTURALLY: any object key outside the known secret-free allow-list is a
// violation regardless of its string contents, and the only credential-shaped
// values (the scoped JWTs) are shape-checked so a raw secret can't pose as one.
//
// These tests pin BOTH halves: the guarantee passes on the real builders' output,
// and — the load-bearing part — it FAILS on a smuggled novel field that a pattern
// match would sail past.

import { describe, it, expect } from 'vitest';
import {
  buildGatewayEgress,
  buildSidecarEgress,
  type EgressConfig,
} from '../../src/domain/credential-egress.js';
import { buildAgentSnapshot } from '../../src/domain/agent-snapshot.js';
import {
  assertEgressCredentialFree,
  assertSnapshotCredentialFree,
  EgressNotCredentialFreeError,
} from '../../src/domain/egress-credential-free.js';

const GATEWAY_EGRESS = (): EgressConfig =>
  buildGatewayEgress({
    sessionId: 'ses_cf_1',
    gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
    gatewayLlmUrl: 'https://ai-gateway.internal/llm',
    sessionJwt: 'eyJhbGci.eyJzdWIi.c2ln',
    llmJwt: 'eyJhbGci.eyJsbG0i.c2ln',
    mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
    vaultByUrl: new Map([['https://github.example/mcp', 'vlt_gh']]),
  });

const SIDECAR_EGRESS = (): EgressConfig =>
  buildSidecarEgress({
    bindings: [
      {
        host: 'github.com',
        scheme: 'basic',
        vaultId: 'vlt_gh',
        username: 'x-access-token',
        injectEnv: ['GH_TOKEN'],
      },
      { host: 'api.example.com', scheme: 'bearer', vaultId: 'vlt_api' },
    ],
  });

/** Deep-clone an egress config so a test mutation can't leak across cases. */
function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

describe('assertEgressCredentialFree — passes on the real builders', () => {
  it('accepts the gateway egress the builder produces', () => {
    expect(() => assertEgressCredentialFree(GATEWAY_EGRESS())).not.toThrow();
  });

  it('accepts the sidecar egress the builder produces', () => {
    expect(() => assertEgressCredentialFree(SIDECAR_EGRESS())).not.toThrow();
  });

  it('accepts a gateway egress with zero MCP servers + the empty-jwt sentinel', () => {
    // The zero-server gateway path leaves `session_jwt=''` and no servers; the
    // empty token is a valid no-credential sentinel, not a raw secret.
    const egress = buildGatewayEgress({
      sessionId: 'ses_cf_2',
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      sessionJwt: '',
      mcpServers: [],
      vaultByUrl: new Map(),
    });
    expect(() => assertEgressCredentialFree(egress)).not.toThrow();
  });
});

describe('assertEgressCredentialFree — catches a NOVEL secret-shaped field (the gap)', () => {
  it('rejects an inlined secret on a sidecar source — even named to dodge a pattern match', () => {
    const egress = clone(SIDECAR_EGRESS());
    if (egress.mode !== 'sidecar') throw new Error('unreachable');
    // A novel field a string-match for real_secret/password/ghp_/sk- would MISS.
    (egress.sidecar.entries[0]!.source as unknown as Record<string, unknown>).api_key =
      'AKIA-not-a-known-pattern';
    expect(() => assertEgressCredentialFree(egress)).toThrow(EgressNotCredentialFreeError);
    expect(() => assertEgressCredentialFree(egress)).toThrow(
      /unexpected field 'egress\.sidecar\.entries\[0\]\.source\.api_key'/,
    );
  });

  it('rejects a top-level secret field smuggled onto a sidecar entry', () => {
    const egress = clone(SIDECAR_EGRESS());
    if (egress.mode !== 'sidecar') throw new Error('unreachable');
    (egress.sidecar.entries[1]! as unknown as Record<string, unknown>).client_secret = 'whatever';
    expect(() => assertEgressCredentialFree(egress)).toThrow(
      /unexpected field 'egress\.sidecar\.entries\[1\]\.client_secret'/,
    );
  });

  it('rejects a source whose kind is not a vault reference (an inlined-secret source)', () => {
    const egress = clone(SIDECAR_EGRESS());
    if (egress.mode !== 'sidecar') throw new Error('unreachable');
    (egress.sidecar.entries[0]!.source as unknown as Record<string, unknown>).kind = 'inline';
    expect(() => assertEgressCredentialFree(egress)).toThrow(/source\.kind must be 'vault'/);
  });

  it('rejects a novel secret-shaped field on a rewritten gateway server', () => {
    const egress = clone(GATEWAY_EGRESS());
    if (egress.mode !== 'gateway') throw new Error('unreachable');
    (egress.gateway.mcp_servers['github']! as unknown as Record<string, unknown>).bearer_token =
      'opaque-but-unexpected';
    expect(() => assertEgressCredentialFree(egress)).toThrow(
      /unexpected field 'egress\.gateway\.mcp_servers\.github\.bearer_token'/,
    );
  });

  it('rejects a secret smuggled as an unknown request header (e.g. X-Api-Key)', () => {
    const egress = clone(GATEWAY_EGRESS());
    if (egress.mode !== 'gateway') throw new Error('unreachable');
    egress.gateway.mcp_servers['github']!.headers['X-Api-Key'] = 'sekret';
    expect(() => assertEgressCredentialFree(egress)).toThrow(
      /unexpected header 'egress\.gateway\.mcp_servers\.github\.headers\.X-Api-Key'/,
    );
  });

  it('rejects an Authorization header carrying a non-JWT raw secret', () => {
    const egress = clone(GATEWAY_EGRESS());
    if (egress.mode !== 'gateway') throw new Error('unreachable');
    // A raw token that is NOT three base64url segments — a leaked PAT, say.
    egress.gateway.mcp_servers['github']!.headers['Authorization'] =
      'Bearer ghp_aRealLookingPatToken';
    expect(() => assertEgressCredentialFree(egress)).toThrow(/non-JWT-shaped token/);
  });

  it('rejects a raw secret parked in the session_jwt field', () => {
    const egress = clone(GATEWAY_EGRESS());
    if (egress.mode !== 'gateway') throw new Error('unreachable');
    egress.gateway.session_jwt = 'sk-this-is-a-raw-secret-not-a-jwt';
    expect(() => assertEgressCredentialFree(egress)).toThrow(/session_jwt is not JWT-shaped/);
  });

  it('rejects a top-level extra field on the egress envelope', () => {
    const egress = clone(GATEWAY_EGRESS()) as unknown as Record<string, unknown>;
    egress.credentials = { token: 'leak' };
    expect(() => assertEgressCredentialFree(egress as unknown as EgressConfig)).toThrow(
      /unexpected field 'egress\.credentials'/,
    );
  });

  it('rejects a gateway egress that also carries a sidecar block (mode confusion)', () => {
    const egress = clone(GATEWAY_EGRESS()) as unknown as Record<string, unknown>;
    egress.sidecar = { entries: [] };
    // `sidecar` IS an allowed root key (the union), but not alongside `gateway`.
    expect(() => assertEgressCredentialFree(egress as unknown as EgressConfig)).toThrow(
      /must not carry a 'sidecar' block/,
    );
  });
});

describe('assertSnapshotCredentialFree', () => {
  function snapshot(egress: EgressConfig) {
    return buildAgentSnapshot({
      agent: {
        model: { provider: 'anthropic', id: 'claude-opus-4' },
        system: 'You are a helpful agent.',
        tools: [{ type: 'agent_toolset' }, { type: 'mcp_toolset', mcp_server_name: 'github' }],
      },
      skills: [],
      provider: 'claude',
      egress,
    });
  }

  it('accepts a full snapshot built by the pure builder (gateway + sidecar)', () => {
    expect(() => assertSnapshotCredentialFree(snapshot(GATEWAY_EGRESS()))).not.toThrow();
    expect(() => assertSnapshotCredentialFree(snapshot(SIDECAR_EGRESS()))).not.toThrow();
  });

  it('rejects a novel secret-shaped field smuggled onto the snapshot envelope', () => {
    const snap = snapshot(GATEWAY_EGRESS()) as unknown as Record<string, unknown>;
    snap.provider_api_key = 'leaked';
    expect(() => assertSnapshotCredentialFree(snap as never)).toThrow(
      /unexpected field 'snapshot\.provider_api_key'/,
    );
  });

  it('propagates an egress-level violation through the snapshot check', () => {
    const snap = snapshot(SIDECAR_EGRESS());
    if (snap.egress.mode !== 'sidecar') throw new Error('unreachable');
    (snap.egress.sidecar.entries[0]!.source as unknown as Record<string, unknown>).secret = 'x';
    expect(() => assertSnapshotCredentialFree(snap)).toThrow(
      /unexpected field 'egress\.sidecar\.entries\[0\]\.source\.secret'/,
    );
  });

  // The coordinator fold nests a full sub-snapshot per roster member; the walk must
  // recurse into each so a secret smuggled into a member snapshot fails loud too.
  function coordinatorSnapshot() {
    const member = snapshot(GATEWAY_EGRESS());
    const coord = snapshot(GATEWAY_EGRESS());
    coord.multiagent = {
      type: 'coordinator',
      primary_thread_id: '',
      agents: [
        { agent_name: 'researcher', snapshot: member },
        { type: 'self', agent_name: 'coordinator' },
      ],
    };
    return coord;
  }

  it('accepts a folded coordinator snapshot (nested member sub-snapshots walked)', () => {
    expect(() => assertSnapshotCredentialFree(coordinatorSnapshot())).not.toThrow();
  });

  it('rejects an unexpected key on the multiagent block', () => {
    const snap = coordinatorSnapshot() as unknown as { multiagent: Record<string, unknown> };
    snap.multiagent.credentials = 'leaked';
    expect(() => assertSnapshotCredentialFree(snap as never)).toThrow(
      /unexpected field 'snapshot\.multiagent\.credentials'/,
    );
  });

  it('rejects a novel secret-shaped field smuggled onto a roster member sub-snapshot', () => {
    // The load-bearing recursion: a secret hidden on a MEMBER's snapshot (not the
    // top-level one) is still caught, at the nested member path.
    const snap = coordinatorSnapshot();
    const member = snap.multiagent!.agents[0]! as unknown as { snapshot: Record<string, unknown> };
    member.snapshot.provider_api_key = 'leaked';
    expect(() => assertSnapshotCredentialFree(snap)).toThrow(
      /unexpected field 'snapshot\.multiagent\.agents\[0\]\.snapshot\.provider_api_key'/,
    );
  });

  it('propagates a member egress violation through the nested member snapshot walk', () => {
    const snap = coordinatorSnapshot();
    const member = snap.multiagent!.agents[0]! as unknown as {
      snapshot: { egress: Record<string, unknown> };
    };
    (member.snapshot.egress.gateway as Record<string, unknown>).api_key = 'leaked';
    expect(() => assertSnapshotCredentialFree(snap)).toThrow(
      /unexpected field 'egress\.gateway\.api_key'/,
    );
  });
});

describe('managed resource snapshot credential boundary', () => {
  it('accepts only a protocol version and content-free binding digest', () => {
    const snapshot = buildAgentSnapshot({
      agent: { model: { provider: 'openai', id: 'gpt-5.4' }, system: '', tools: [] },
      skills: [],
      provider: 'codex-sdk',
      egress: GATEWAY_EGRESS(),
    });
    Object.assign(snapshot, { managed_resources: { version: 1, revision: 'a'.repeat(64) } });
    expect(() => assertSnapshotCredentialFree(snapshot)).not.toThrow();
    Object.assign(snapshot.managed_resources!, { private_store_token: 'secret' });
    expect(() => assertSnapshotCredentialFree(snapshot)).toThrow('unexpected field');
    Object.assign(snapshot, { managed_resources: { version: 1, revision: 'not-a-digest' } });
    expect(() => assertSnapshotCredentialFree(snapshot)).toThrow('invalid managed resource');
  });
});

describe('managed Codex snapshot additions', () => {
  function managedSnapshot() {
    return {
      ...buildAgentSnapshot({
        agent: {
          model: { provider: 'openai', id: 'gpt-5.4' },
          system: '',
          tools: [
            {
              type: 'custom',
              name: 'lookup',
              description: 'Look up a record',
              input_schema: { type: 'object', properties: { api_key: { type: 'string' } } },
            },
          ],
        },
        skills: [],
        provider: 'codex-sdk',
        egress: GATEWAY_EGRESS(),
      }),
      managed_resources: { version: 1 as const, revision: 'a'.repeat(64) },
      request_guardrails_owner: 'registry' as const,
    };
  }
  it('admits managed accounting and callback schemas as data', () => {
    expect(() => assertSnapshotCredentialFree(managedSnapshot())).not.toThrow();
  });
  it.each([
    { request_guardrails_owner: 'runner' },
    { provider: 'claude' },
    { managed_resources: undefined },
    { multiagent: {} },
  ])('rejects invalid accounting ownership: %j', (overrides) => {
    expect(() =>
      assertSnapshotCredentialFree({ ...managedSnapshot(), ...overrides } as never),
    ).toThrow('invalid request guardrails owner');
  });
  it.each([
    { credentials: 'secret' },
    { name: 'undeclared' },
    { name: '' },
    { input_schema: { type: 'string' } },
    { description: '' },
  ])('rejects malformed or credential-bearing callback envelopes: %j', (overrides) => {
    const snapshot = managedSnapshot();
    Object.assign(snapshot.custom_tools![0]!, overrides);
    expect(() => assertSnapshotCredentialFree(snapshot)).toThrow();
  });
  it('rejects duplicate callback declarations', () => {
    const snapshot = managedSnapshot();
    snapshot.custom_tools!.push(snapshot.custom_tools![0]!);
    expect(() => assertSnapshotCredentialFree(snapshot)).toThrow('invalid or disallowed');
  });
});

it.each([
  { tool_permissions: { mcp__orca__read: 'sometimes' } },
  { tool_permissions: ['always_allow'] },
  { default_tool_permission: { api_key: 'secret' } },
])('rejects malformed tool permission snapshots: %j', (fields) => {
  const snapshot = buildAgentSnapshot({
    agent: { model: { provider: 'openai', id: 'gpt-5.4' }, system: '', tools: [] },
    skills: [],
    provider: 'codex-sdk',
    egress: GATEWAY_EGRESS(),
  });
  expect(() => assertSnapshotCredentialFree({ ...snapshot, ...fields } as never)).toThrow(
    /permission/,
  );
});
