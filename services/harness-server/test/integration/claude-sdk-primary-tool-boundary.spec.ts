// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';

const execFileAsync = promisify(execFile);
const probePath = fileURLToPath(
  new URL('../fixtures/sdk-primary-tool-boundary-probe.mjs', import.meta.url),
);

describe('Claude Agent SDK primary/subagent tool boundary', () => {
  it('hides a child-only inline MCP tool from primary while exposing it to the child', async () => {
    const result = (await runProbe('worker')) as {
      primaryTools: string[];
      childTools: string[];
      primaryRoster: string;
      agentHookTargets: string[];
      primaryCatalogCount: number;
      primaryTurnSystemCount: number;
      childCatalogCount: number;
      childTurnSystemCount: number;
    };

    expect(result.primaryTools).toEqual(['Agent']);
    expect(result.primaryRoster).toContain('- worker:');
    expect(result.primaryRoster.match(/- __orca_primary:/g)).toHaveLength(1);
    expect(result.primaryRoster).toContain(
      'Internal primary managed-agent boundary. Never delegate to this agent.',
    );
    expect(result.childTools).toEqual(['mcp__orca__read']);
    expect(result.agentHookTargets).toEqual(['worker']);
    expect(result.primaryCatalogCount).toBe(1);
    expect(result.primaryTurnSystemCount).toBe(1);
    expect(result.childCatalogCount).toBe(0);
    expect(result.childTurnSystemCount).toBe(0);
  }, 15_000);

  it('denies delegation to the synthetic primary boundary before spawning a child', async () => {
    const result = (await runProbe('self')) as {
      requestTools: string[][];
      agentHookTargets: string[];
      deniedToolResult: boolean;
    };

    expect(result.agentHookTargets).toEqual(['__orca_primary']);
    expect(result.requestTools).toEqual([['Agent'], ['Agent']]);
    expect(result.deniedToolResult).toBe(true);
  }, 15_000);

  it('keeps an explicitly tool-less child isolated from the global MCP surface', async () => {
    const result = (await runProbe('no-tools')) as {
      primaryTools: string[];
      childTools: string[];
      agentHookTargets: string[];
    };

    expect(result.primaryTools).toEqual(['Agent']);
    expect(result.childTools).toEqual([]);
    expect(result.agentHookTargets).toEqual(['observer']);
  }, 15_000);

  it('provides runtime child ids that isolate two child permission policies', async () => {
    const result = (await runProbe('child-policies')) as {
      permissionDecisions: Array<{
        toolName: string;
        agentID: string | null;
        agentType: string | null;
        decision: 'allow' | 'deny';
      }>;
      subagentLifecycle: Array<{
        event: 'start' | 'stop';
        agentID: string;
        agentType: string;
      }>;
      readHandlerCalls: number;
      hasDeniedToolResult: boolean;
    };

    expect(result.permissionDecisions).toEqual([
      {
        toolName: 'mcp__orca__read',
        agentID: expect.any(String),
        agentType: 'allowed-reader',
        decision: 'allow',
      },
      {
        toolName: 'mcp__orca__read',
        agentID: expect.any(String),
        agentType: 'denied-reader',
        decision: 'deny',
      },
    ]);
    expect(result.permissionDecisions[0]!.agentID).not.toBe(result.permissionDecisions[1]!.agentID);
    expect(result.subagentLifecycle).toEqual([
      {
        event: 'start',
        agentID: result.permissionDecisions[0]!.agentID,
        agentType: 'allowed-reader',
      },
      {
        event: 'stop',
        agentID: result.permissionDecisions[0]!.agentID,
        agentType: 'allowed-reader',
      },
      {
        event: 'start',
        agentID: result.permissionDecisions[1]!.agentID,
        agentType: 'denied-reader',
      },
      {
        event: 'stop',
        agentID: result.permissionDecisions[1]!.agentID,
        agentType: 'denied-reader',
      },
    ]);
    expect(result.readHandlerCalls).toBe(1);
    expect(result.hasDeniedToolResult).toBe(true);
  }, 15_000);
});

async function runProbe(
  scenario: 'worker' | 'self' | 'no-tools' | 'child-policies',
): Promise<unknown> {
  const env = { ...process.env, NODE_ENV: 'production' };
  delete env['VITEST'];
  delete env['VITEST_WORKER_ID'];

  const { stdout } = await execFileAsync(process.execPath, [probePath, scenario], {
    env,
    timeout: 12_000,
  });
  return JSON.parse(stdout);
}
