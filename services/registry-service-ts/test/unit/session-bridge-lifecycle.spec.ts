// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Postgres-FREE regression coverage for the owner-pod session event bridge
// server-boundary wiring — the fast-lane companion to
// test/integration/session-bridge-lifecycle.spec.ts.
//
// The integration spec proves the SAME two fixes end-to-end against a real
// Drizzle-backed Postgres, but it is gated on the dev stack (`test:integration`,
// the CI `integration` job) so it does NOT run in the fast `pnpm test` gate.
// That left both fixes below with no regression test on the pre-merge path a
// broken change actually lands through. These specs close that gap by exercising
// the REAL production code (`buildBoundSessionResolver`, `buildApp`'s `onClose`
// wiring) with in-process fakes — no DB, no sockets — so a revert fails here, in
// the fast gate, not only behind infra.
//
//   1. RESOLVER FILTER (`buildBoundSessionResolver`). The resolver EXCLUDES a
//      distribution_state=`failed` binding so the owner-pod bridge manager no-ops
//      for a session the distributor just failed on a connect-time capability
//      mismatch. The filter is a SQL-level predicate (`ne(distribution_state,
//      'failed')`), so the Postgres-free way to pin it is to run the real resolver
//      against a fake `pg` pool that CAPTURES the compiled query, then assert the
//      compiled WHERE carries the `distribution_state <> $n` exclusion bound to
//      `DISTRIBUTION_FAILED`. Reverting the `ne(...)` filter drops that predicate
//      from the compiled SQL, failing the assertion.
//   2. SHUTDOWN DRAIN (`buildApp` `onClose`). `buildApp` registers an `onClose`
//      hook that calls the owner-pod manager's `stopAll`, so `app.close()` (the
//      SIGINT/SIGTERM shutdown in main.ts) stops every live bridge instead of
//      leaking it past process teardown. Driven here over a STUB db whose resolver
//      returns a bound session, so `onRunnerConnect` starts a real bridge and
//      `app.close()` must drain it. Removing the `onClose` hook leaves the bridge
//      running (size stays 1), failing the assertion.

import { describe, it, expect } from 'vitest';
import { FrameKind, decodeFrame } from '@orca/harness-tunnel';
import { drizzle } from 'drizzle-orm/node-postgres';
import type { Pool } from 'pg';
import * as schema from '../../src/persistence/postgres/schema.js';
import { buildBoundSessionResolver } from '../../src/api/sessions.routes.js';
import { DISTRIBUTION_FAILED } from '../../src/tunnel/session-distributor.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  buildApp,
  getSessionEventBridges,
  getTunnelRegistry,
  type BuildAppOptions,
} from '../../src/server.js';

// ── B1: buildBoundSessionResolver excludes a FAILED binding ──
//
// A fake `pg` pool that records every compiled query Drizzle executes and
// resolves it to zero rows. `drizzle(pool)` compiles the resolver's
// select/where/limit and calls `pool.query({ text, ... }, paramsArray)` — the
// captured `text` is the parameterized SQL, `params` the bound values.
interface CapturedQuery {
  text: string;
  params: unknown[];
}

function buildCapturingResolver(rows: unknown[] = []): {
  resolver: ReturnType<typeof buildBoundSessionResolver>;
  captured: CapturedQuery[];
} {
  const captured: CapturedQuery[] = [];
  const fakePool = {
    query(config: unknown, params?: unknown[]) {
      const text =
        typeof config === 'object' && config !== null && 'text' in config
          ? String((config as { text: unknown }).text)
          : String(config);
      captured.push({ text, params: params ?? [] });
      return Promise.resolve({ rows, rowCount: rows.length });
    },
  };
  const db: DbClient = drizzle(fakePool as unknown as Pool, { schema });
  return { resolver: buildBoundSessionResolver(db), captured };
}

describe('buildBoundSessionResolver — FAILED-binding exclusion (Postgres-free)', () => {
  it('compiles a WHERE that excludes distribution_state=failed', async () => {
    const { resolver, captured } = buildCapturingResolver();

    await resolver.resolveBoundSession('runner_x');

    expect(captured).toHaveLength(1);
    const { text, params } = captured[0]!;
    // The B1 filter: a `<>` (Drizzle's `ne`) predicate on distribution_state,
    // bound to the FAILED sentinel. Both vanish from the compiled query if the
    // `ne(sessions.distributionState, DISTRIBUTION_FAILED)` filter is reverted.
    expect(text).toMatch(/distribution_state"\s*<>\s*\$\d/);
    expect(params).toContain(DISTRIBUTION_FAILED);
  });

  it('still resolves a bound (workspace, session) for an admitted row', async () => {
    // Drizzle runs this query in `rowMode: 'array'`, so a returned row is a
    // positional array in the projection order [id, workspace_id].
    const { resolver } = buildCapturingResolver([['ses_9', 'ws_9']]);

    const bound = await resolver.resolveBoundSession('runner_y');

    expect(bound).toEqual({ workspaceId: 'ws_9', sessionId: 'ses_9' });
  });
});

// ── B3: buildApp's onClose hook drains the owner-pod bridge manager ──
//
// A stub db whose resolver query resolves to one bound session, so the manager's
// `onRunnerConnect` starts a real bridge. Everything else buildApp needs is a
// minimal stub — no DB/Kafka/sockets are touched on the ready → connect → close
// path this test drives.
function buildStubBoundDb(): DbClient {
  const boundChain = {
    from() {
      return this;
    },
    leftJoin() {
      return this;
    },
    innerJoin() {
      return this;
    },
    where() {
      return this;
    },
    limit() {
      // The bound session uses a Registry-owned self-hosted environment and a
      // valid pinned Agent snapshot, so production ownership checks also pass.
      return Promise.resolve([
        {
          id: 'ses_close',
          workspaceId: 'ws_close',
          agentId: 'agent_close',
          agentVersion: 1,
          environmentId: 'env_close',
          resolvedEnvironmentId: 'env_close',
          target: 'self_hosted',
          harnessType: 'claude_agent_sdk',
          snapshot: { harness_type: 'claude_agent_sdk', metadata: {} },
        },
      ]);
    },
  };
  return {
    select() {
      return boundChain;
    },
  } as unknown as DbClient;
}

function stubStore(): BuildAppOptions['store'] {
  return {
    async append() {
      return [];
    },
    async *read() {
      /* empty transcript */
    },
    async *tail() {
      /* empty transcript */
    },
    async archive() {
      /* no-op */
    },
    async close() {
      /* no-op */
    },
  } as unknown as BuildAppOptions['store'];
}

function stubFileStore(): BuildAppOptions['fileStore'] {
  return {
    async create() {
      throw new Error('stub fileStore');
    },
    async get() {
      return null;
    },
    async list() {
      return { items: [], nextCursor: null };
    },
    async open() {
      return null;
    },
    async archive() {
      /* no-op */
    },
    async delete() {
      /* no-op */
    },
    async close() {
      /* no-op */
    },
  } as unknown as BuildAppOptions['fileStore'];
}

function buildStubApp(db: DbClient): ReturnType<typeof buildApp> {
  return buildApp({
    db,
    oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
    store: stubStore(),
    sse: { bufferSize: 256, dropAgeMs: 5000, heartbeatMs: 15000 },
    jwtMinter: { mint: () => 'stub-token' } as unknown as BuildAppOptions['jwtMinter'],
    fileStore: stubFileStore(),
  });
}

describe('buildApp onClose — owner-pod bridge drain (Postgres-free)', () => {
  it('app.close() stops every live bridge via the onClose stopAll hook', async () => {
    const app = buildStubApp(buildStubBoundDb());
    await app.ready();

    const manager = getSessionEventBridges(app);
    // The bound stub db resolves the runner to a session, so the manager starts a
    // real bridge (recovery + snapshot are no-ops: empty stub store, no provider).
    const registry = getTunnelRegistry(app);
    const session = registry.register(
      'runner_close',
      {
        sendText: async () => {},
        receiveText: () => new Promise<string>(() => {}),
      },
      {
        kind: FrameKind.Hello,
        runnerVersion: 'test',
        frameProtocolVersion: 1,
        harnesses: [],
        envs: [],
      },
    );
    void (async () => {
      for (;;) {
        const raw = await session.outboundQueue.get();
        if (raw === null) return;
        const frame = decodeFrame(raw);
        if (frame.kind !== FrameKind.Request) continue;
        registry.routeResponseFrame('runner_close', {
          kind: FrameKind.ResponseHead,
          id: frame.id,
          status: 200,
          headers: [],
        });
        registry.routeResponseFrame('runner_close', { kind: FrameKind.ResponseEnd, id: frame.id });
      }
    })();
    await manager.onRunnerConnect('runner_close', {});
    expect(manager.size).toBe(1);

    // The onClose hook must drain it. Without the hook the bridge leaks past
    // shutdown and size stays 1.
    await app.close();
    expect(manager.size).toBe(0);
  });

  it('app.close() is a clean no-op when no bridge was ever started', async () => {
    const app = buildStubApp(buildStubBoundDb());
    await app.ready();

    const manager = getSessionEventBridges(app);
    expect(manager.size).toBe(0);

    await app.close();
    expect(manager.size).toBe(0);
  });
});
