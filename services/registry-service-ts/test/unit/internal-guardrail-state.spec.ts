// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify from 'fastify';
import { describe, expect, it } from 'vitest';
import { buildInternalAuth, type InternalAuthVerifier } from '../../src/auth/internal-auth.js';
import type { InternalServicePrincipal } from '../../src/auth/principal.js';
import { internalContract } from '../../src/contracts/internal.contract.js';
import { parseGuardrailStateBody } from '../../src/api/internal.routes.js';

const STATE_PATH = '/internal/v1/workspaces/ws_a/sessions/ses_a/guardrail-state';

function fixedVerifier(caller: InternalServicePrincipal['caller']): InternalAuthVerifier {
  return {
    async verify(): Promise<InternalServicePrincipal> {
      return { caller, subject: `test-${caller}` };
    },
  };
}

async function authApp(caller: InternalServicePrincipal['caller']) {
  const server = Fastify({ logger: false });
  server.addHook('preHandler', buildInternalAuth(fixedVerifier(caller)));
  server.post('/internal/v1/workspaces/:workspaceId/sessions/:id/guardrail-state', async () => ({
    applied: 0,
  }));
  await server.ready();
  return server;
}

describe('guardrail state route caller restriction', () => {
  it('is reachable by Harness and AI Gateway but refused to the exporter', async () => {
    for (const [caller, status] of [
      ['harness', 200],
      ['shared', 200],
      ['ai-gateway', 200],
      ['observability-exporter', 403],
    ] as const) {
      const server = await authApp(caller);
      try {
        const response = await server.inject({
          method: 'POST',
          url: STATE_PATH,
          headers: { authorization: 'Bearer any-token-value-here' },
          payload: { updates: [] },
        });
        expect(response.statusCode, `caller ${caller}`).toBe(status);
      } finally {
        await server.close();
      }
    }
  });
});

describe('guardrail state contract', () => {
  const route = internalContract.applyGuardrailState;

  it('describes the persisted scopes and rejects unknown fields', () => {
    expect(
      route.body.parse({
        updates: [{ scope: 'session', key: 'tool_calls', action: 'increment', value: 1 }],
      }),
    ).toEqual({
      updates: [{ scope: 'session', key: 'tool_calls', action: 'increment', value: 1 }],
    });
    expect(() =>
      route.body.parse({ updates: [{ scope: 'session', key: 'k', action: 'nope' }] }),
    ).toThrow();
    expect(() =>
      route.body.parse({ updates: [{ scope: 'session', key: 'k', action: 'set', extra: 1 }] }),
    ).toThrow();
    // `turn` is deliberately absent from the wire vocabulary: it is never
    // persisted, so a caller must not be able to express it as a durable write.
    expect(() =>
      route.body.parse({ updates: [{ scope: 'turn', key: 'k', action: 'increment' }] }),
    ).toThrow();
  });

  it('carries the subject and window a cross-session counter is keyed by', () => {
    expect(
      route.body.parse({
        subject: 'usr_alice',
        window: '2026-08-01',
        updates: [{ scope: 'subject_window', key: 'spend_usd', action: 'increment', value: 0.5 }],
      }),
    ).toMatchObject({ subject: 'usr_alice', window: '2026-08-01' });
  });
});

describe('parseGuardrailStateBody', () => {
  function parse(raw: unknown) {
    return parseGuardrailStateBody(raw);
  }

  it('accepts a session-scoped batch and preserves update order', () => {
    const result = parse({
      updates: [
        { scope: 'session', key: 'a', action: 'increment', value: 2 },
        { scope: 'session', key: 'b', action: 'set', value: 'x' },
      ],
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.batch.updates.map((u) => u.key)).toEqual(['a', 'b']);
    expect(result.batch.subject).toBeNull();
    expect(result.batch.window).toBeNull();
  });

  it('rejects turn scope rather than accepting a write it would drop', () => {
    const result = parse({ updates: [{ scope: 'turn', key: 'a', action: 'increment' }] });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toMatch(/turn/);
  });

  it('rejects the whole batch when any update is malformed', () => {
    for (const updates of [
      [
        { scope: 'session', key: 'ok', action: 'increment' },
        { scope: 'turn', key: 'a', action: 'set' },
      ],
      [{ scope: 'session', key: '', action: 'increment' }],
      [{ scope: 'session', key: 'a', action: 'multiply' }],
      [{ scope: 'elsewhere', key: 'a', action: 'set' }],
      [{ scope: 'session', key: 'a', action: 'set', extra: true }],
      [{ scope: 'session', key: 'a' }],
      ['not-an-object'],
      [{ scope: 'session', key: 'x'.repeat(1024), action: 'increment' }],
    ]) {
      expect(parse({ updates }).ok, JSON.stringify(updates)).toBe(false);
    }
    expect(parse({}).ok).toBe(false);
    expect(parse(null).ok).toBe(false);
    expect(parse({ updates: {} }).ok).toBe(false);
    expect(parse({ updates: [], extra: 1 }).ok).toBe(false);
  });

  it('requires a subject and window for a cross-session counter', () => {
    const update = { scope: 'subject_window', key: 'spend', action: 'increment', value: 1 };
    expect(parse({ updates: [update] }).ok).toBe(false);
    expect(parse({ subject: 'usr_a', updates: [update] }).ok).toBe(false);
    expect(parse({ window: '2026-08-01', updates: [update] }).ok).toBe(false);
    expect(parse({ subject: 'usr_a', window: '2026-08-01', updates: [update] }).ok).toBe(true);
  });

  it('refuses counter writes the numeric counter table cannot represent', () => {
    const base = { subject: 'usr_a', window: '2026-08-01' };
    expect(
      parse({
        ...base,
        updates: [{ scope: 'subject_window', key: 'k', action: 'append', value: 1 }],
      }).ok,
    ).toBe(false);
    expect(
      parse({
        ...base,
        updates: [{ scope: 'subject_window', key: 'k', action: 'set', value: 'text' }],
      }).ok,
    ).toBe(false);
    expect(
      parse({ ...base, updates: [{ scope: 'subject_window', key: 'k', action: 'set', value: 3 }] })
        .ok,
    ).toBe(true);
    expect(
      parse({ ...base, updates: [{ scope: 'subject_window', key: 'k', action: 'delete' }] }).ok,
    ).toBe(true);
  });

  it('accepts an empty batch as a no-op', () => {
    const result = parse({ updates: [] });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.batch.updates).toEqual([]);
  });
});
