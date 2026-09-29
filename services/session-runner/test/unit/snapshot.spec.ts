// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner-side snapshot parser.
//
// The owner pod pushes the credential-free snapshot as a SINGLE NDJSON line; the
// runner parses it into a RunnerSnapshot before configuring the provider. This
// spec pins the wire shape the runner consumes — the load-bearing `provider`
// field, the model + tool/mcp allowlists, the opaque egress passthrough — and the
// error cases the snapshot handler maps to a non-2xx ack.

import { readFileSync } from 'node:fs';
import { registrySourcePath } from './support/registry-source-pin.js';
import { describe, it, expect } from 'vitest';
import { parseSnapshotBody, SnapshotParseError } from '../../src/snapshot.js';

/** Encode a snapshot the way the registry delivers it: one JSON line + newline. */
function deliver(snapshot: unknown): Uint8Array {
  return new TextEncoder().encode(`${JSON.stringify(snapshot)}\n`);
}

/**
 * A minimal WELL-FORMED snapshot: the two fields the parser requires — the
 * load-bearing `provider`, and `allowed_tool_names` (`[]` = this agent runs no orca
 * tool). Everything else defaults. Used so a spec about some OTHER field does not
 * accidentally also assert what a missing allowlist means; that question has exactly
 * one home, `allowed-tool-names-empty.spec.ts`.
 */
function minimal(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return { provider: 'claude', allowed_tool_names: [], ...overrides };
}

describe('parseSnapshotBody', () => {
  it('parses the full registry snapshot shape (model, provider, system, allowlists, egress)', () => {
    const snap = parseSnapshotBody(
      deliver({
        model: { provider: 'anthropic', id: 'claude-sonnet-4' },
        provider: 'claude',
        system: 'You are helpful.',
        allowed_tool_names: ['bash', 'read'],
        allowed_mcp_server_names: ['github'],
        egress: {
          mode: 'gateway',
          gateway: { mcp_base_url: 'https://gw', session_jwt: 'a.b.c', mcp_servers: {} },
        },
      }),
    );
    expect(snap.provider).toBe('claude');
    expect(snap.model).toEqual({ provider: 'anthropic', id: 'claude-sonnet-4' });
    expect(snap.system).toBe('You are helpful.');
    expect(snap.allowed_tool_names).toEqual(['bash', 'read']);
    expect(snap.allowed_mcp_server_names).toEqual(['github']);
    // The egress block is carried verbatim (opaque to the loop; the provider reads it).
    expect(snap.egress).toEqual({
      mode: 'gateway',
      gateway: { mcp_base_url: 'https://gw', session_jwt: 'a.b.c', mcp_servers: {} },
    });
  });

  it('defaults every OPTIONAL field so a minimal chat-only snapshot still parses', () => {
    const snap = parseSnapshotBody(deliver(minimal()));
    expect(snap.provider).toBe('claude');
    expect(snap.model).toEqual({ provider: '', id: '' });
    expect(snap.system).toBe('');
    // `allowed_tool_names` is NOT defaulted — `minimal()` supplies it, because the
    // parser requires it. See `allowed-tool-names-empty.spec.ts` for why.
    expect(snap.allowed_tool_names).toEqual([]);
    expect(snap.allowed_mcp_server_names).toEqual([]);
    // No per-tool policy → empty map + no default (the provider is then fail-closed).
    expect(snap.tool_permissions).toEqual({});
    expect(snap.default_tool_permission).toBeUndefined();
    // No staged skills bundle → the plugin-dir is absent (the provider then loads
    // skills solely from the composed prompt + allowlist).
    expect(snap.skills_plugin_dir).toBeUndefined();
    expect(snap.egress).toBeNull();
  });

  it('parses the skills plugin-dir when the producer staged a skills bundle', () => {
    const snap = parseSnapshotBody(
      deliver(minimal({ provider: 'claude-code', skills_plugin_dir: '/snap/skills' })),
    );
    expect(snap.skills_plugin_dir).toBe('/snap/skills');
  });

  it('drops a non-string / empty skills plugin-dir (treated as no staged bundle)', () => {
    // An empty string is not a usable dir — treat as absent (a native-CLI provider
    // then emits no `--plugin-dir`, rather than a dangling flag with an empty value).
    expect(
      parseSnapshotBody(deliver(minimal({ provider: 'claude-code', skills_plugin_dir: '' })))
        .skills_plugin_dir,
    ).toBeUndefined();
    // A non-string is malformed → dropped fail-safe.
    expect(
      parseSnapshotBody(deliver(minimal({ provider: 'claude-code', skills_plugin_dir: 7 })))
        .skills_plugin_dir,
    ).toBeUndefined();
    expect(
      parseSnapshotBody(
        deliver(minimal({ provider: 'claude-code', skills_plugin_dir: { dir: '/x' } })),
      ).skills_plugin_dir,
    ).toBeUndefined();
  });

  it('carries the custom-CLI spec verbatim for the `custom` provider (opaque to the loop)', () => {
    const customSpec = {
      command: 'my-agent',
      argv: ['run', '--session', '{sessionId}'],
      stdout: { mode: 'jsonLine', text: { type_equals: 'assistant', text_field: 'text' } },
    };
    const snap = parseSnapshotBody(
      deliver(minimal({ provider: 'custom', custom_spec: customSpec })),
    );
    // The custom spec is carried verbatim (opaque; the custom harness parses + validates it).
    expect(snap.custom_spec).toEqual(customSpec);
  });

  it('omits custom_spec when the snapshot carries none (every non-custom provider)', () => {
    const snap = parseSnapshotBody(deliver(minimal()));
    expect(snap.custom_spec).toBeUndefined();
  });

  it('parses the per-tool permission policy map + default policy', () => {
    const snap = parseSnapshotBody(
      deliver({
        ...minimal(),
        tool_permissions: { Bash: 'always_ask', Read: 'always_allow' },
        default_tool_permission: 'always_allow',
      }),
    );
    expect(snap.tool_permissions).toEqual({ Bash: 'always_ask', Read: 'always_allow' });
    expect(snap.default_tool_permission).toBe('always_allow');
  });

  it.each([
    { tool_permissions: { Bash: 'sometimes' }, default_tool_permission: 'always_allow' },
    { default_tool_permission: 'bogus' },
    { tool_permissions: ['x'] },
    { tool_permissions: 'nope' },
    { tool_permissions: null },
  ])(
    'rejects malformed permission policies rather than falling back to auto-allow: %j',
    (fields) => {
      expect(() => parseSnapshotBody(deliver(minimal(fields)))).toThrow(SnapshotParseError);
    },
  );

  it('preserves explicit deny policies', () => {
    const snap = parseSnapshotBody(
      deliver(
        minimal({
          tool_permissions: { 'mcp__orca__*': 'always_deny', mcp__orca__read: 'always_allow' },
          default_tool_permission: 'always_deny',
        }),
      ),
    );
    expect(snap.tool_permissions['mcp__orca__*']).toBe('always_deny');
    expect(snap.default_tool_permission).toBe('always_deny');
  });

  it('tolerates the trailing newline the delivery body always carries', () => {
    // A body WITHOUT the trailing newline parses identically (the trim handles both).
    const withNewline = parseSnapshotBody(deliver(minimal()));
    const withoutNewline = parseSnapshotBody(
      new TextEncoder().encode('{"provider":"claude","allowed_tool_names":[]}'),
    );
    expect(withoutNewline.provider).toBe(withNewline.provider);
  });

  it('drops non-string entries from the tool + mcp allowlists', () => {
    const snap = parseSnapshotBody(
      deliver({
        provider: 'claude',
        allowed_tool_names: ['bash', 7, null],
        allowed_mcp_server_names: [1, 'x'],
      }),
    );
    expect(snap.allowed_tool_names).toEqual(['bash']);
    expect(snap.allowed_mcp_server_names).toEqual(['x']);
  });

  it('rejects an empty body', () => {
    expect(() => parseSnapshotBody(new TextEncoder().encode(''))).toThrow(SnapshotParseError);
  });

  it('rejects a multi-line body (the snapshot is one NDJSON line)', () => {
    expect(() =>
      parseSnapshotBody(new TextEncoder().encode('{"provider":"claude"}\n{"x":1}\n')),
    ).toThrow(/single NDJSON line/);
  });

  it('rejects non-JSON', () => {
    expect(() => parseSnapshotBody(new TextEncoder().encode('not json'))).toThrow(
      SnapshotParseError,
    );
  });

  it('rejects a non-object body (array / scalar)', () => {
    expect(() => parseSnapshotBody(deliver([1, 2, 3]))).toThrow(/not a JSON object/);
    expect(() => parseSnapshotBody(deliver(42))).toThrow(/not a JSON object/);
  });

  it('rejects a snapshot with no provider — the one field that cannot be reconstructed', () => {
    expect(() => parseSnapshotBody(deliver({ model: { provider: 'anthropic', id: 'x' } }))).toThrow(
      /provider/,
    );
  });
});

describe('parseSnapshotBody — multiagent (coordinator roster)', () => {
  /** A minimal well-formed roster-member sub-snapshot (the roster agent's own config). */
  function memberSnapshot(overrides: Record<string, unknown> = {}): Record<string, unknown> {
    return {
      model: { provider: 'anthropic', id: 'claude-haiku-4' },
      provider: 'claude',
      system: 'sub sys',
      allowed_tool_names: ['read'],
      allowed_mcp_server_names: [],
      egress: { mode: 'gateway' },
      ...overrides,
    };
  }

  it('omits multiagent for a single-agent snapshot (the feature is purely additive)', () => {
    // The overwhelmingly common case: a plain agent snapshot has no multiagent block,
    // so the runner drives it exactly as before (single-agent path unchanged).
    expect(parseSnapshotBody(deliver(minimal())).multiagent).toBeUndefined();
  });

  it('parses a coordinator roster: each member carries its own resolved sub-snapshot', () => {
    const snap = parseSnapshotBody(
      deliver({
        ...minimal(),
        model: { provider: 'anthropic', id: 'claude-sonnet-4' },
        multiagent: {
          type: 'coordinator',
          primary_thread_id: 'sth_primary',
          agents: [
            { agent_name: 'researcher', snapshot: memberSnapshot({ system: 'research' }) },
            { agent_name: 'writer', snapshot: memberSnapshot({ system: 'write' }) },
          ],
        },
      }),
    );
    expect(snap.multiagent).toBeDefined();
    expect(snap.multiagent!.type).toBe('coordinator');
    // The primary thread row id (parent for spawned child threads) is carried verbatim.
    expect(snap.multiagent!.primaryThreadId).toBe('sth_primary');
    expect(snap.multiagent!.agents).toHaveLength(2);
    expect(snap.multiagent!.agents.map((a) => a.agentName)).toEqual(['researcher', 'writer']);
    // Each member's sub-snapshot is fully parsed into a RunnerSnapshot the runner can
    // build a subagent harness from (its own model/system/provider/egress).
    const researcher = snap.multiagent!.agents[0]!;
    expect(researcher.snapshot.provider).toBe('claude');
    expect(researcher.snapshot.system).toBe('research');
    expect(researcher.snapshot.model).toEqual({ provider: 'anthropic', id: 'claude-haiku-4' });
    expect(researcher.snapshot.allowed_tool_names).toEqual(['read']);
  });

  it('resolves a `{ type: "self" }` roster member against the coordinator snapshot', () => {
    // A `self` member delegates the coordinator back into its own definition: no
    // member snapshot is carried, so the runner reuses the coordinator's own snapshot
    // (minus its multiagent block, so the self-thread is not itself a coordinator).
    const snap = parseSnapshotBody(
      deliver({
        ...minimal(),
        system: 'coordinator sys',
        model: { provider: 'anthropic', id: 'claude-sonnet-4' },
        allowed_tool_names: ['bash', 'read'],
        multiagent: {
          type: 'coordinator',
          primary_thread_id: 'sth_primary',
          agents: [{ type: 'self', agent_name: 'self' }],
        },
      }),
    );
    const self = snap.multiagent!.agents[0]!;
    expect(self.agentName).toBe('self');
    // The self member's snapshot is the coordinator's own config (same provider/system/model).
    expect(self.snapshot.provider).toBe('claude');
    expect(self.snapshot.system).toBe('coordinator sys');
    expect(self.snapshot.model).toEqual({ provider: 'anthropic', id: 'claude-sonnet-4' });
    expect(self.snapshot.allowed_tool_names).toEqual(['bash', 'read']);
    // Crucially the self snapshot is NOT itself a coordinator (one-level delegation):
    // the runner strips the multiagent block so a self subagent gets no delegation tool.
    expect(self.snapshot.multiagent).toBeUndefined();
  });

  it('defaults a member agent_name from its snapshot when the member omits one', () => {
    const snap = parseSnapshotBody(
      deliver({
        ...minimal(),
        multiagent: {
          type: 'coordinator',
          primary_thread_id: 'sth_primary',
          agents: [{ snapshot: memberSnapshot() }],
        },
      }),
    );
    // A member with no explicit name falls back to a stable placeholder so the
    // `session.thread_created` event always carries an agent_name (the projector
    // defaults to 'agent' too, so this stays consistent with the read model).
    expect(snap.multiagent!.agents[0]!.agentName).toBe('agent');
  });

  it('rejects a malformed multiagent block instead of silently downgrading to single-agent', () => {
    // A PRESENT-but-malformed block is a parse error, not a silent drop. Dropping it would
    // run a coordinator as a plain single agent — no roster, no delegation tool, every
    // delegation the agent was configured for simply gone — and the push would still ack
    // 200, so nothing would ever retry. Throwing makes the handler ack non-2xx, so the
    // owner pod records the delivery undelivered and re-pushes (the same treatment an
    // unknown `provider` already gets).
    const bad = (multiagent: unknown): unknown => ({ ...minimal(), multiagent });
    expect(() => parseSnapshotBody(deliver(bad('nope')))).toThrow(SnapshotParseError);
    // Version skew — a `type` this runner does not know — must NOT be read as "not a
    // coordinator"; it is the case a silent drop would hide most completely.
    expect(() => parseSnapshotBody(deliver(bad({ type: 'not-coordinator', agents: [] })))).toThrow(
      /multiagent\.type/,
    );
    expect(() => parseSnapshotBody(deliver(bad({ type: 'coordinator' })))).toThrow(
      /multiagent\.agents/,
    );
    expect(() => parseSnapshotBody(deliver(bad({ type: 'coordinator', agents: [] })))).toThrow(
      /multiagent\.agents/,
    );
    // An agent member with neither a snapshot nor `type:self` is unusable → the whole push
    // is rejected, naming the offending member by index.
    expect(() =>
      parseSnapshotBody(deliver(bad({ type: 'coordinator', agents: [{ agent_name: 'x' }] }))),
    ).toThrow(/agents\[0\]/);
    // A member whose own sub-snapshot is unparseable names both the index and the field.
    expect(() =>
      parseSnapshotBody(
        deliver(bad({ type: 'coordinator', agents: [{ snapshot: { system: 'no provider' } }] })),
      ),
    ).toThrow(/agents\[0\] snapshot: .*provider/);
  });

  it('treats an absent or null multiagent block as a plain single-agent snapshot', () => {
    // Only ABSENT is fail-safe: there is no roster to lose, so there is nothing to report.
    expect(parseSnapshotBody(deliver(minimal())).multiagent).toBeUndefined();
    expect(parseSnapshotBody(deliver(minimal({ multiagent: null }))).multiagent).toBeUndefined();
  });
});

describe('guardrails on the snapshot', () => {
  const guardrail = {
    id: 'grd_1',
    name: 'No shells',
    tier: 'workspace',
    phases: ['tool_call'],
    rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    stateful: false,
  };

  it('carries the composed guardrails and their restored state', () => {
    const snap = parseSnapshotBody(
      deliver(
        minimal({
          guardrails: [{ ...guardrail, state_scope: 'session', subagent_id: 'agt_child' }],
          guardrail_state: { tool_calls: 3 },
        }),
      ),
    );
    expect(snap.guardrails).toEqual([
      { ...guardrail, state_scope: 'session', subagent_id: 'agt_child' },
    ]);
    expect(snap.guardrail_state).toEqual({ tool_calls: 3 });
  });

  it('leaves both absent for a producer that predates guardrails', () => {
    // The field is optional so an older owner pod keeps working, and absence
    // must read as "no guardrails" rather than as an empty-but-present block.
    const snap = parseSnapshotBody(deliver(minimal()));
    expect(snap.guardrails).toBeUndefined();
    expect(snap.guardrail_state).toBeUndefined();
  });

  it('throws on a malformed guardrail rather than dropping it', () => {
    // A dropped guardrail is the failure this whole surface exists to prevent:
    // the operator believes a rule applies and the runtime never sees it, with
    // no trace but its absence. Throwing acks the push non-2xx so the owner pod
    // retries — the same posture a malformed `multiagent` block already takes.
    const cases: Array<[string, unknown]> = [
      ['not an array', minimal({ guardrails: {} })],
      ['null array', minimal({ guardrails: null })],
      ['entry not an object', minimal({ guardrails: ['nope'] })],
      ['missing id', minimal({ guardrails: [{ ...guardrail, id: '' }] })],
      ['missing tier', minimal({ guardrails: [{ ...guardrail, tier: undefined }] })],
      ['unknown tier', minimal({ guardrails: [{ ...guardrail, tier: 'owner' }] })],
      ['empty phases', minimal({ guardrails: [{ ...guardrail, phases: [] }] })],
      ['phases not strings', minimal({ guardrails: [{ ...guardrail, phases: [1] }] })],
      ['missing rule', minimal({ guardrails: [{ ...guardrail, rule: undefined }] })],
      [
        'unknown state scope',
        minimal({ guardrails: [{ ...guardrail, state_scope: 'workspace' }] }),
      ],
      ['empty subagent id', minimal({ guardrails: [{ ...guardrail, subagent_id: '' }] })],
      ['non-string subagent id', minimal({ guardrails: [{ ...guardrail, subagent_id: 7 }] })],
      ['stateful not a boolean', minimal({ guardrails: [{ ...guardrail, stateful: 'yes' }] })],
    ];
    for (const [label, body] of cases) {
      expect(() => parseSnapshotBody(deliver(body)), label).toThrow(SnapshotParseError);
    }
  });

  it('throws on malformed restored state rather than resetting it silently', () => {
    for (const state of [null, [], 'empty']) {
      expect(() => parseSnapshotBody(deliver(minimal({ guardrail_state: state })))).toThrow(
        /guardrail_state/,
      );
    }
  });
});

it('accepts Registry request delegation only for a managed single-agent Codex snapshot', () => {
  const wire = minimal({
    provider: 'codex-sdk',
    request_guardrails_owner: 'registry',
    managed_resources: { version: 1, revision: 'a'.repeat(64) },
  });
  expect(parseSnapshotBody(deliver(wire)).request_guardrails_owner).toBe('registry');
  for (const fields of [
    { provider: 'claude' },
    { managed_resources: undefined },
    { multiagent: {} },
    { request_guardrails_owner: 'runner' },
  ]) {
    expect(() => parseSnapshotBody(deliver({ ...wire, ...fields }))).toThrow(
      /request_guardrails_owner/,
    );
  }
});

it('pins the private Registry request owner field against Registry source', () => {
  const registrySource = readFileSync(registrySourcePath('domain/agent-snapshot.ts'), 'utf8');
  const declaration = /\b(request_guardrails_owner)\?: '([^']+)'/.exec(registrySource);
  expect(declaration).not.toBeNull();
  const parsed = parseSnapshotBody(
    deliver(
      minimal({
        provider: 'codex-sdk',
        managed_resources: { version: 1, revision: 'a'.repeat(64) },
        [declaration![1]!]: declaration![2],
      }),
    ),
  );
  expect(parsed.request_guardrails_owner).toBe(declaration![2]);
});
