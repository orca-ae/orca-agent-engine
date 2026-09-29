// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.resetModules();
});

it.each([
  [
    'claude_agent_sdk',
    'anthropic',
    'claude-sonnet-4-5-20250929',
    'ANTHROPIC_API_KEY',
    'claude_code',
  ],
  ['pi_sdk', 'anthropic', 'claude-sonnet-4-6', 'ANTHROPIC_API_KEY', 'pi_sdk'],
  ['codex_sdk', 'openai', 'gpt-5.4', 'OPENAI_API_KEY', 'codex_sdk'],
])(
  'selects model, credentials and colocated identity for %s',
  async (harness, provider, id, key, colocated) => {
    vi.stubEnv('ORCA_E2E_AGENT_HARNESS', harness);
    vi.stubEnv('ANTHROPIC_API_KEY', '');
    vi.stubEnv('OPENAI_API_KEY', '');
    vi.stubEnv('ORCA_E2E_SANDBOX_HARNESS_MODEL', undefined);
    const config = await import('../../../../packages/e2e-tests/test/real-agent-config.js');
    expect(config.REAL_AGENT.model).toMatchObject({ provider, id });
    expect(config.REAL_AGENT.keyVariable).toBe(key);
    expect(config.COLOCATED_AGENT.harness).toBe(colocated);
    expect(config.COLOCATED_AGENT.model.provider).toBe(provider);
    expect(config.requireRealAgentKey).toThrow(key);
    vi.stubEnv(key, 'fixture-provider-key');
    expect(config.requireRealAgentKey).not.toThrow();
    if (harness === 'pi_sdk') expect(config.COLOCATED_AGENT.model.id).toBe(id);
  },
);

it('launches Pi budget and colocated scenarios without unsupported multiagent suites', () => {
  const dir = mkdtempSync(join(tmpdir(), 'orca-e2e-selector-'));
  try {
    writeFileSync(
      join(dir, 'vitest'),
      '#!/bin/sh\nprintf "%s\\n" "$ORCA_E2E_REAL_CUSTOM_TOOL" "$@"\n',
      { mode: 0o755 },
    );
    const script = fileURLToPath(
      new URL('../../../../packages/e2e-tests/scripts/run-agent-tests.sh', import.meta.url),
    );
    const result = execFileSync('/bin/bash', [script], {
      env: {
        ...process.env,
        PATH: `${dir}:/usr/bin:/bin`,
        ORCA_E2E_AGENT_HARNESS: 'pi_sdk',
        ORCA_E2E_SANDBOX_HARNESS: '1',
        ORCA_E2E_REAL_CUSTOM_TOOL: '',
      },
      encoding: 'utf8',
    });
    expect(result.split('\n')[0]).toBe('1');
    expect(result).toContain('test/real-agent-loop.spec.ts');
    expect(result).toContain('test/guardrails-budget-agent.spec.ts');
    expect(result).toContain('test/sandbox-harness-agent.spec.ts');
    expect(result).not.toContain('multiagent');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

it('keeps SSE-observed events across turns when the event-list index lags', async () => {
  const { collectTurnEvents } =
    await import('../../../../packages/e2e-tests/test/turn-events-helpers.js');
  const first = [
    { id: 'evt_reply', type: 'agent.message' },
    { id: 'evt_idle', type: 'session.status_idle' },
  ];
  const denied = [
    { id: 'evt_denied', type: 'session.error', error: { type: 'policy_denied' } },
    { id: 'evt_idle2', type: 'session.status_idle' },
  ];
  const response = (events: unknown[]) =>
    new Response(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(''), {
      status: 200,
    });
  vi.stubGlobal(
    'fetch',
    vi
      .fn()
      .mockResolvedValueOnce(response(first))
      .mockResolvedValueOnce(response([...first, ...denied])),
  );
  const cfg = {
    baseURL: 'http://fixture.invalid',
    apiKey: 'fixture',
  } as import('../../../../packages/e2e-tests/src/client.js').OrcaClientConfig;
  const seen = new Set<string>();
  const ready = (events: { type: string }[]) =>
    events.some((e) => e.type === 'session.status_idle');
  expect(await collectTurnEvents(cfg, 'ses_fixture', seen, ready, 'first', 1000)).toEqual(first);
  // The list projection is still empty. SSE replay must not recount its model output.
  expect(await collectTurnEvents(cfg, 'ses_fixture', seen, ready, 'second', 1000)).toEqual(denied);
});
