// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { describe, expect, it } from 'vitest';

const execFileAsync = promisify(execFile);
const probePath = fileURLToPath(
  new URL('../fixtures/sdk-mcp-auth-rotation-probe.mjs', import.meta.url),
);

interface ProbeResult {
  sdkVersion: string;
  initializeCount: number;
  wire: Array<{
    phase: number;
    method: string;
    rpc: string | null;
    token: string;
    sessionId: string | null;
  }>;
  calls: Array<{ phase: number; token: string; sessionId: string; value: string | null }>;
  orcaCalls: number;
  llm: Array<{
    phase: number;
    tools: string[];
    hasConversationMarker: boolean;
    hasStateMarker: boolean;
  }>;
  updates: Array<{ added: string[]; removed: string[]; errors: Record<string, string> }>;
  statuses: Array<Array<{ name: string; status: string }>>;
  results: Array<{ subtype: string; isError: boolean }>;
}

describe('Claude Agent SDK 0.3.283 MCP Authorization rotation contract', () => {
  it('reinitializes remote state on header changes, but retains conversation and explicitly retained orca', async () => {
    // This characterizes the real SDK, not a mocked Query or an assertion that
    // hot replacement is lossless. All provider/MCP requests target loopback.
    const { stdout } = await execFileAsync(process.execPath, [probePath], {
      env: { PATH: process.env['PATH'], NODE_ENV: 'production' },
      timeout: 30_000,
      maxBuffer: 1024 * 1024,
    });
    const result = JSON.parse(stdout) as ProbeResult;

    expect(result.sdkVersion).toBe('0.3.283');
    expect(result.results).toEqual([
      { subtype: 'success', isError: false },
      { subtype: 'success', isError: false },
      { subtype: 'success', isError: false },
    ]);
    expect(result.initializeCount).toBe(2);
    // 0.3.283 discovers each remote transport before initialization, without
    // a session id; discovery must use the credential for that rotation.
    expect(result.wire.filter((entry) => entry.rpc === 'server/discover')).toEqual([
      { phase: 0, method: 'POST', rpc: 'server/discover', token: 'old', sessionId: null },
      { phase: 1, method: 'POST', rpc: 'server/discover', token: 'new', sessionId: null },
    ]);
    expect(result.wire.filter((entry) => entry.rpc === 'initialize')).toEqual([
      { phase: 0, method: 'POST', rpc: 'initialize', token: 'old', sessionId: null },
      { phase: 1, method: 'POST', rpc: 'initialize', token: 'new', sessionId: null },
    ]);
    expect(result.calls).toEqual([
      { phase: 0, token: 'old', sessionId: 'probe-session-1', value: 'STATE_MARKER' },
      // Reapplying the exact same configuration does not reconnect.
      { phase: 1, token: 'old', sessionId: 'probe-session-1', value: 'STATE_MARKER' },
      // Changing just Authorization creates a new transport session with empty state.
      { phase: 2, token: 'new', sessionId: 'probe-session-2', value: null },
    ]);
    const rotationIndex = result.wire.findIndex((entry) => entry.token === 'new');
    expect(rotationIndex).toBeGreaterThan(0);
    expect(result.wire.slice(0, rotationIndex).every((entry) => entry.token === 'old')).toBe(true);
    expect(result.wire.slice(rotationIndex).every((entry) => entry.token === 'new')).toBe(true);
    for (const entry of result.wire) {
      if (entry.rpc === 'server/discover' || entry.rpc === 'initialize') continue;
      expect(entry.sessionId).toBe(entry.token === 'old' ? 'probe-session-1' : 'probe-session-2');
    }
    expect(result.updates).toHaveLength(2);
    for (const update of result.updates) {
      expect(update.errors).toEqual({});
      expect(update.removed).not.toContain('orca');
    }
    expect(result.statuses).toHaveLength(3);
    for (const statuses of result.statuses) {
      expect(statuses).toEqual(
        expect.arrayContaining([
          { name: 'orca', status: 'connected' },
          { name: 'remote', status: 'connected' },
        ]),
      );
    }
    expect(result.orcaCalls).toBe(3);
    expect(result.llm).toHaveLength(9);
    for (const request of result.llm) {
      expect(request.tools).toEqual(
        expect.arrayContaining(['mcp__orca__ping', 'mcp__remote__state']),
      );
      expect(request.hasConversationMarker).toBe(true);
    }
    // Historical tool output stays in LLM context even though the MCP state is gone.
    expect(
      result.llm
        .filter((request) => request.phase === 2)
        .every((request) => request.hasStateMarker),
    ).toBe(true);
  }, 35_000);
});
