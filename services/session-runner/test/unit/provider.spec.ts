// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the provider-dispatch seam.
//
// The snapshot carries the harness/provider selection (the launch frame omits it),
// so the runner constructs the harness by dispatching on `snapshot.provider`. This
// spec pins the registry (register / has / providerNames / build), the
// unknown-provider fail-fast (the runner-side capability mismatch), and the
// snapshot→SessionStartInput projection the provider's `start` consumes.

import { describe, it, expect } from 'vitest';
import {
  ProviderRegistry,
  UnknownProviderError,
  buildSessionStartInput,
} from '../../src/harness/provider.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';
import { FakeAgentHarness } from './support/fake-agent-harness.js';

const SNAPSHOT: RunnerSnapshot = {
  model: { provider: 'anthropic', id: 'claude-sonnet-4' },
  provider: 'claude',
  system: 'sys',
  allowed_tool_names: ['bash', 'read'],
  allowed_mcp_server_names: ['github'],
  tool_permissions: {},
  egress: { mode: 'gateway' },
};

describe('ProviderRegistry', () => {
  it('builds the harness via the factory registered for snapshot.provider', () => {
    const built = new FakeAgentHarness();
    const registry = new ProviderRegistry();
    let seenSnapshot: RunnerSnapshot | undefined;
    let seenCtx: { workspaceId: string; sessionId: string } | undefined;
    registry.register('claude', (snapshot, ctx) => {
      seenSnapshot = snapshot;
      seenCtx = ctx;
      return built;
    });

    const harness = registry.build(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' });
    expect(harness).toBe(built);
    expect(seenSnapshot).toBe(SNAPSHOT);
    expect(seenCtx).toEqual({ workspaceId: 'ws_1', sessionId: 'ses_1' });
  });

  it('reports registered provider names (for the tunnel hello advertise)', () => {
    const registry = new ProviderRegistry();
    registry.register('claude', () => new FakeAgentHarness());
    expect(registry.has('claude')).toBe(true);
    expect(registry.has('codex')).toBe(false);
    expect(registry.providerNames()).toEqual(['claude']);
  });

  it('throws UnknownProviderError when no factory is registered for the snapshot provider', () => {
    const registry = new ProviderRegistry();
    registry.register('claude', () => new FakeAgentHarness());
    expect(() =>
      registry.build({ ...SNAPSHOT, provider: 'codex' }, { workspaceId: 'ws', sessionId: 'ses' }),
    ).toThrow(UnknownProviderError);
  });

  it('rejects double-registration of the same provider name (a wiring bug)', () => {
    const registry = new ProviderRegistry();
    registry.register('claude', () => new FakeAgentHarness());
    expect(() => registry.register('claude', () => new FakeAgentHarness())).toThrow(
      /already registered/,
    );
  });
});

describe('buildSessionStartInput', () => {
  it('projects the snapshot into the harness boot context', () => {
    const input = buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' });
    expect(input.workspaceId).toBe('ws_1');
    expect(input.sessionId).toBe('ses_1');
    expect(input.agentSnapshot).toEqual({
      model_provider: 'anthropic',
      model_id: 'claude-sonnet-4',
      system: 'sys',
      allowed_tool_names: ['bash', 'read'],
    });
    // Enabled mcp_toolset server names become remote toolsets (mcp__<server>__*).
    expect(input.remoteMcpToolsets).toEqual([{ serverName: 'github' }]);
    // No staged skills bundle on the base snapshot → the plugin-dir is absent from the
    // projected agentSnapshot (the strict toEqual above already pins this).
    expect(input.agentSnapshot.skills_plugin_dir).toBeUndefined();
  });

  it("projects the snapshot's skills_plugin_dir onto agentSnapshot for native --plugin-dir discovery", () => {
    const input = buildSessionStartInput(
      { ...SNAPSHOT, skills_plugin_dir: '/snap/skills' },
      { workspaceId: 'ws', sessionId: 'ses' },
    );
    // The plugin-dir rides the boot context alongside the composed system prompt, so a
    // native-CLI provider surfaces it as `--plugin-dir`.
    expect(input.agentSnapshot.skills_plugin_dir).toBe('/snap/skills');
  });

  it('omits remoteMcpToolsets when no mcp servers survived the intersection', () => {
    const input = buildSessionStartInput(
      { ...SNAPSHOT, allowed_mcp_server_names: [] },
      { workspaceId: 'ws', sessionId: 'ses' },
    );
    expect(input.remoteMcpToolsets).toBeUndefined();
  });

  it('omits toolPermissions when the snapshot carries no policy (provider stays fail-closed)', () => {
    // No per-tool map and no default → no resolver, so the harness falls back to its
    // fail-closed "park everything" path (existing behavior preserved).
    const input = buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws', sessionId: 'ses' });
    expect(input.toolPermissions).toBeUndefined();
  });

  it('projects a per-tool permission resolver: per-tool entry → snapshot default → fail-closed always_ask', () => {
    const snapshot: RunnerSnapshot = {
      ...SNAPSHOT,
      tool_permissions: { Bash: 'always_ask', Read: 'always_allow' },
      default_tool_permission: 'always_allow',
    };
    const input = buildSessionStartInput(snapshot, { workspaceId: 'ws', sessionId: 'ses' });
    expect(input.toolPermissions).toBeDefined();
    // Per-tool entries win.
    expect(input.toolPermissions?.policyFor('Bash')).toBe('always_ask');
    expect(input.toolPermissions?.policyFor('Read')).toBe('always_allow');
    // A tool not in the map falls back to the snapshot default.
    expect(input.toolPermissions?.policyFor('Edit')).toBe('always_allow');
  });

  it('a resolver with a map but no default fails closed (unclassified tool → always_ask)', () => {
    const snapshot: RunnerSnapshot = {
      ...SNAPSHOT,
      tool_permissions: { Read: 'always_allow' },
    };
    const input = buildSessionStartInput(snapshot, { workspaceId: 'ws', sessionId: 'ses' });
    expect(input.toolPermissions?.policyFor('Read')).toBe('always_allow');
    // No default + not in the map → fail-closed always_ask.
    expect(input.toolPermissions?.policyFor('Bash')).toBe('always_ask');
  });

  it('builds a resolver from a default policy alone (no per-tool entries)', () => {
    const snapshot: RunnerSnapshot = { ...SNAPSHOT, default_tool_permission: 'always_allow' };
    const input = buildSessionStartInput(snapshot, { workspaceId: 'ws', sessionId: 'ses' });
    expect(input.toolPermissions).toBeDefined();
    expect(input.toolPermissions?.policyFor('anything')).toBe('always_allow');
  });

  it('lifts the rewritten gateway MCP server map from egress onto mcpServers', () => {
    // The registry rewrites each agent MCP server to point at the ai-gateway and
    // embeds the map under egress.gateway.mcp_servers (credential-free: scoped JWT,
    // not an upstream secret). The generic projection lifts it onto the typed
    // SessionStartInput.mcpServers field so every provider reads it uniformly.
    const snapshot: RunnerSnapshot = {
      ...SNAPSHOT,
      egress: {
        mode: 'gateway',
        gateway: {
          mcp_base_url: 'https://gw.example/mcp',
          session_jwt: 'jwt-abc',
          mcp_servers: {
            github: {
              type: 'http',
              url: 'https://gw.example/mcp',
              headers: {
                'X-Orca-Backend': 'github',
                'X-Orca-Session-Id': 'ses_1',
                Authorization: 'Bearer jwt-abc',
              },
              // Extra registry-stamped field is preserved, not stripped.
              alwaysLoad: true,
            },
          },
        },
      },
    };
    const input = buildSessionStartInput(snapshot, { workspaceId: 'ws_1', sessionId: 'ses_1' });
    expect(input.mcpServers).toEqual({
      github: {
        type: 'http',
        url: 'https://gw.example/mcp',
        headers: {
          'X-Orca-Backend': 'github',
          'X-Orca-Session-Id': 'ses_1',
          Authorization: 'Bearer jwt-abc',
        },
        alwaysLoad: true,
      },
    });
  });

  it('omits mcpServers when the gateway egress carries no rewritten servers (chat-only)', () => {
    // SNAPSHOT's egress is `{ mode: 'gateway' }` with no mcp_servers map — a
    // chat-only / no-MCP session leaves mcpServers absent.
    const input = buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws', sessionId: 'ses' });
    expect(input.mcpServers).toBeUndefined();
  });

  it('omits mcpServers for non-gateway, absent, or malformed egress', () => {
    const ctx = { workspaceId: 'ws', sessionId: 'ses' };
    // Sidecar egress carries no rewritten gateway MCP map.
    expect(
      buildSessionStartInput(
        { ...SNAPSHOT, egress: { mode: 'sidecar', sidecar: { entries: [] } } },
        ctx,
      ).mcpServers,
    ).toBeUndefined();
    // Absent egress.
    expect(buildSessionStartInput({ ...SNAPSHOT, egress: null }, ctx).mcpServers).toBeUndefined();
    // Malformed mcp_servers (not an object).
    expect(
      buildSessionStartInput(
        { ...SNAPSHOT, egress: { mode: 'gateway', gateway: { mcp_servers: 'nope' } } },
        ctx,
      ).mcpServers,
    ).toBeUndefined();
  });

  it('drops a malformed rewritten server entry while keeping well-formed ones', () => {
    const snapshot: RunnerSnapshot = {
      ...SNAPSHOT,
      egress: {
        mode: 'gateway',
        gateway: {
          mcp_servers: {
            github: {
              type: 'http',
              url: 'https://gw.example/mcp',
              headers: { Authorization: 'Bearer jwt' },
            },
            // Missing `url` + non-object headers — must be dropped, not admitted.
            broken: { type: 'http' },
          },
        },
      },
    };
    const input = buildSessionStartInput(snapshot, { workspaceId: 'ws', sessionId: 'ses' });
    expect(Object.keys(input.mcpServers ?? {})).toEqual(['github']);
  });
});

it('resolves exact and server policies without treating separators in tool names as server names', () => {
  const input = buildSessionStartInput(
    {
      ...SNAPSHOT,
      tool_permissions: {
        'mcp__orca__*': 'always_deny',
        mcp__orca__read: 'always_allow',
        'mcp__github__*': 'always_allow',
        mcp__github__delete: 'always_deny',
      },
      default_tool_permission: 'always_ask',
    },
    { workspaceId: 'ws_test', sessionId: 'ses_test' },
  );
  expect(input.toolPermissions!.policyFor('mcp__orca__read')).toBe('always_allow');
  expect(input.toolPermissions!.policyFor('mcp__orca__bash')).toBe('always_deny');
  expect(input.toolPermissions!.policyFor('mcp__github__delete')).toBe('always_deny');
  expect(input.toolPermissions!.policyFor('mcp__github__get__item')).toBe('always_allow');
  expect(input.toolPermissions!.policyFor('mcp__github_other__read')).toBe('always_ask');
});
