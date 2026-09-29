// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { selectHarness, createHarnessProviderRegistry } from '../../src/harness/registry.js';
import type { AgentHarness } from '../../src/harness/agent-harness.js';

const fakeHarness = (label: string): AgentHarness => ({
  // minimal stand-in; identity tracked via a tagged property
  start: async () => {},
  submit: async () => {},
  stop: async () => {},
  events: async function* () {},
  // @ts-expect-error test tag
  __label: label,
});

const context = { workspaceId: 'ws', sessionId: 'ses' };
const providers = createHarnessProviderRegistry([
  { id: 'claude_agent_sdk', build: () => fakeHarness('claude') },
  { id: 'claude_code', build: () => fakeHarness('sandbox') },
]);

describe('selectHarness', () => {
  it('runs Codex only through its registered separate provider', () => {
    const codex = fakeHarness('codex-sdk');
    const registry = createHarnessProviderRegistry([
      { id: 'codex_sdk', modes: ['separate'], build: () => codex },
    ]);
    expect(
      selectHarness({
        selection: { harness: 'codex_sdk', mode: 'separate' },
        providers: registry,
        context,
      }),
    ).toBe(codex);
    expect(() =>
      selectHarness({
        selection: { harness: 'codex_sdk', mode: 'colocated' },
        providers: registry,
        context,
      }),
    ).toThrow("does not support 'colocated'");
  });
  it('uses the override (test harnessFactory) when provided, ignoring annotation', () => {
    const h = selectHarness({
      override: () => fakeHarness('override'),
      selection: { harness: 'claude_code', mode: 'colocated' },
      providers,
      context,
    });
    expect((h as unknown as { __label: string }).__label).toBe('override');
  });

  it('routes the Claude SDK identity to its registered provider', () => {
    const h = selectHarness({
      selection: { harness: 'claude_agent_sdk', mode: 'separate' },
      providers,
      context,
    });
    expect((h as unknown as { __label: string }).__label).toBe('claude');
  });

  it('passes the session context to the selected provider', () => {
    let captured: unknown;
    const h = selectHarness({
      selection: { harness: 'claude_code', mode: 'colocated' },
      context,
      providers: createHarnessProviderRegistry([
        {
          id: 'claude_code',
          build: (input) => {
            captured = input;
            return fakeHarness('sandbox');
          },
        },
      ]),
    });
    expect((h as unknown as { __label: string }).__label).toBe('sandbox');
    expect(captured).toEqual({
      ...context,
      selection: { harness: 'claude_code', mode: 'colocated' },
    });
  });

  it('does not substitute Claude for an unregistered separate harness', () => {
    expect(() =>
      selectHarness({
        selection: { harness: 'claude_agent_sdk_persistent', mode: 'separate' },
        providers,
        context,
      }),
    ).toThrow(/no harness-server provider registered.*claude_agent_sdk_persistent/);
  });

  it('selects a second separate provider by identity', () => {
    const persistent = fakeHarness('persistent');
    expect(
      selectHarness({
        selection: { harness: 'claude_agent_sdk_persistent', mode: 'separate' },
        providers: createHarnessProviderRegistry([
          ...providers.values(),
          { id: 'claude_agent_sdk_persistent', build: () => persistent },
        ]),
        context,
      }),
    ).toBe(persistent);
  });

  it('refuses duplicate registrations and invalid topology', () => {
    expect(() =>
      createHarnessProviderRegistry([...providers.values(), ...providers.values()]),
    ).toThrow(/duplicate/);
    expect(() =>
      selectHarness({
        selection: { harness: 'claude_agent_sdk', mode: 'colocated' },
        providers,
        context,
      }),
    ).toThrow(/does not support mode/);
  });
});
