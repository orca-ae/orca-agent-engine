// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the tool-confirmation wiring on the session loop — the seam between
// the harness's `canUseTool` (which parks a verdict) and the registry-delivered
// `user.tool_confirmation` (which resolves it).
//
// Two halves are exercised in-process:
//   - the CONFIRMATION VERDICT PARSE (`parseToolConfirmation`): reading the
//     `tool_use_id` + allow/deny decision out of the transcript event body the
//     registry pushes;
//   - the LOOP GATE: `confirmTool` (the canUseTool-shaped callback the harness
//     calls) parks a verdict keyed by tool-use id and returns the SDK permission
//     result once `resolveToolConfirmation` (driven by the confirmation route)
//     settles it — allow → proceed, deny → a clean denial.
//
// No harness or tunnel: the loop's gate + the parse are pure, so the round-trip is
// asserted directly.

import { describe, it, expect } from 'vitest';
import { ProviderRegistry } from '../../src/harness/provider.js';
import { SessionLoop } from '../../src/session-loop.js';
import { parseToolConfirmation, ToolConfirmationParseError } from '../../src/tool-confirmation.js';
import { FakeAgentHarness } from './support/fake-agent-harness.js';

const WS = 'ws_conf';
const SES = 'ses_conf';

function loopWith(harness: FakeAgentHarness): SessionLoop {
  const providers = new ProviderRegistry();
  providers.register('claude', () => harness);
  return new SessionLoop({ workspaceId: WS, providers });
}

function confirmationBody(value: Record<string, unknown>): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

describe('parseToolConfirmation — reads the verdict off the transcript event', () => {
  it('parses an ALLOW decision keyed by tool_use_id', () => {
    const v = parseToolConfirmation(
      confirmationBody({
        type: 'user.tool_confirmation',
        tool_use_id: 'toolu_1',
        decision: 'allow',
      }),
    );
    expect(v).toEqual({ toolUseId: 'toolu_1', approved: true, decision: 'allow' });
  });

  it('parses a DENY decision', () => {
    const v = parseToolConfirmation(
      confirmationBody({
        type: 'user.tool_confirmation',
        tool_use_id: 'toolu_2',
        decision: 'deny',
      }),
    );
    expect(v).toEqual({ toolUseId: 'toolu_2', approved: false, decision: 'deny' });
  });

  it('treats a missing decision as an AMBIGUOUS denial (fail-closed, but flagged)', () => {
    const v = parseToolConfirmation(
      confirmationBody({ type: 'user.tool_confirmation', tool_use_id: 'toolu_3' }),
    );
    // No explicit allow ⇒ deny. A gated tool must never proceed on an ambiguous verdict.
    // The decision classifies `ambiguous` (no recognizable verdict) so a decision-less
    // push is observable apart from a deliberate deny — but `approved` stays false.
    expect(v).toEqual({ toolUseId: 'toolu_3', approved: false, decision: 'ambiguous' });
  });

  it('treats an unknown/foreign decision string as an AMBIGUOUS denial', () => {
    const v = parseToolConfirmation(
      confirmationBody({ tool_use_id: 'toolu_4', decision: 'maybe-later' }),
    );
    // A `decision` the runner does not recognize is fail-closed (not an allow) and
    // flagged ambiguous — the producer sent something unexpected.
    expect(v).toEqual({ toolUseId: 'toolu_4', approved: false, decision: 'ambiguous' });
  });

  it('classifies the deny spellings (deny/reject/decline + approved:false) as an explicit DENY', () => {
    // An explicit refusal (in any recognized spelling) is `deny`, NOT ambiguous — the
    // operator can tell a deliberate deny apart from a malformed verdict.
    for (const decision of ['deny', 'reject', 'decline', 'DENY', 'Reject']) {
      const v = parseToolConfirmation(confirmationBody({ tool_use_id: 'td', decision }));
      expect(v).toEqual({ toolUseId: 'td', approved: false, decision: 'deny' });
    }
    expect(parseToolConfirmation(confirmationBody({ tool_use_id: 'tb', approved: false }))).toEqual(
      {
        toolUseId: 'tb',
        approved: false,
        decision: 'deny',
      },
    );
  });

  it('accepts an "approve"/"accept"/boolean allow spelling (verdict normalization)', () => {
    expect(
      parseToolConfirmation(confirmationBody({ tool_use_id: 't1', decision: 'approve' })),
    ).toEqual({ toolUseId: 't1', approved: true, decision: 'allow' });
    expect(
      parseToolConfirmation(confirmationBody({ tool_use_id: 't2', decision: 'accept' })),
    ).toEqual({ toolUseId: 't2', approved: true, decision: 'allow' });
    expect(parseToolConfirmation(confirmationBody({ tool_use_id: 't3', approved: true }))).toEqual({
      toolUseId: 't3',
      approved: true,
      decision: 'allow',
    });
  });

  it('rejects a body with no tool_use_id (cannot route the verdict)', () => {
    expect(() => parseToolConfirmation(confirmationBody({ decision: 'allow' }))).toThrow(
      ToolConfirmationParseError,
    );
  });

  it('rejects a malformed body', () => {
    expect(() => parseToolConfirmation(new TextEncoder().encode('not json'))).toThrow(
      ToolConfirmationParseError,
    );
    expect(() => parseToolConfirmation(new TextEncoder().encode('[]'))).toThrow(
      ToolConfirmationParseError,
    );
    expect(() => parseToolConfirmation(new TextEncoder().encode(''))).toThrow(
      ToolConfirmationParseError,
    );
  });
});

describe('SessionLoop — tool-confirmation gate (canUseTool ⇄ user.tool_confirmation)', () => {
  it('confirmTool parks until the delivered verdict ALLOWS, then proceeds', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());

    // The harness would call this from canUseTool; it parks on the tool-use id.
    const gate = loop.confirmTool('Bash', { command: 'ls' }, { toolUseId: 'toolu_a' });
    expect(loop.hasPendingApproval()).toBe(true);

    // The registry delivers the user's allow verdict (rode the transcript).
    const resolved = loop.resolveToolConfirmation('toolu_a', true);
    expect(resolved).toBe(true);

    const result = await gate;
    expect(result).toEqual({ behavior: 'allow', updatedInput: { command: 'ls' } });
    expect(loop.hasPendingApproval()).toBe(false);
    await loop.stop();
  });

  it('confirmTool parks until the delivered verdict DENIES, then returns a clean denial', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());

    const gate = loop.confirmTool('Bash', { command: 'rm -rf /' }, { toolUseId: 'toolu_b' });
    loop.resolveToolConfirmation('toolu_b', false);

    const result = await gate;
    expect(result.behavior).toBe('deny');
    if (result.behavior === 'deny') {
      expect(typeof result.message).toBe('string');
      expect(result.message.length).toBeGreaterThan(0);
    }
    await loop.stop();
  });

  it('resolveToolConfirmation for an unknown tool-use id is a no-op', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());
    expect(loop.resolveToolConfirmation('toolu_missing', true)).toBe(false);
    await loop.stop();
  });

  it('a teardown (stop) denies an outstanding confirmation so the tool call is never stuck', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());
    const gate = loop.confirmTool('Bash', {}, { toolUseId: 'toolu_c' });
    // Stop the loop while a verdict is parked — it must resolve (to a denial), not hang.
    await loop.stop();
    const result = await gate;
    expect(result.behavior).toBe('deny');
  });

  it('routes parallel confirmations to their own tool-use ids', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());
    const a = loop.confirmTool('Bash', {}, { toolUseId: 'toolu_p1' });
    const b = loop.confirmTool('Read', {}, { toolUseId: 'toolu_p2' });
    loop.resolveToolConfirmation('toolu_p2', true);
    loop.resolveToolConfirmation('toolu_p1', false);
    expect((await a).behavior).toBe('deny');
    expect((await b).behavior).toBe('allow');
    await loop.stop();
  });
});

/** A delivered snapshot body (one NDJSON line) for the `claude` provider. */
function snapshotBody(overrides: Record<string, unknown> = {}): Uint8Array {
  return new TextEncoder().encode(
    `${JSON.stringify({
      model: { provider: 'anthropic', id: 'claude-sonnet-4' },
      provider: 'claude',
      system: 'sys',
      allowed_tool_names: ['bash'],
      allowed_mcp_server_names: [],
      egress: { mode: 'gateway' },
      ...overrides,
    })}\n`,
  );
}
