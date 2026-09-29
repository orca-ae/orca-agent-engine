// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { and, eq } from 'drizzle-orm';
import type { Event } from '@orca/transcript-store';
import { SHARED_USAGE_KEYS } from '@orca/guardrails';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  environments,
  agentVersions,
  guardrailCounters,
  guardrailState,
  modelPrices,
  organizations,
  workspaces,
  sessionEventsIndex,
  sessionHarnessStates,
  sessions,
  sessionUsageEvents,
} from '../../src/persistence/postgres/schema.js';
import {
  CodexRunnerAccounting,
  CODEX_USAGE_PENDING_PREFIX,
} from '../../src/domain/codex-runner-accounting.js';
import type { AgentSnapshot, SnapshotGuardrail } from '../../src/domain/agent-snapshot.js';
import { httpEventToProto } from '../../src/domain/events.js';
import {
  closeTestDb,
  getTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

const usage = {
  input_tokens: 10,
  output_tokens: 5,
  cache_read_input_tokens: 2,
  cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
};
const rule = (builtin: string, params: Record<string, unknown>): SnapshotGuardrail => ({
  id: 'grd_budget',
  name: 'budget',
  tier: 'workspace',
  phases: ['request'],
  stateful: true,
  rule: { kind: 'builtin', builtin, params },
});

const describeSdk = describe.each([
  ['codex_sdk', 'openai'],
  ['pi_sdk', 'openai'],
  ['pi_sdk', 'anthropic'],
  ['pi_sdk', 'deepseek'],
] as const);
describeSdk(
  'Registry-owned %s %s durable request accounting (PostgreSQL)',
  (sdkHarness, provider) => {
    const checkpoint = {
      version: 1,
      threadId: 'budget-thread',
      ...(sdkHarness === 'pi_sdk'
        ? { format: 'pi_sdk', sdkVersion: '0.87.0', instructionsSha256: 'a'.repeat(64) }
        : {}),
      files:
        sdkHarness === 'pi_sdk'
          ? { 'session.json': 'YQ==' }
          : { 'sessions/2026/09/20/rollout-budget-thread.jsonl': 'YQ==' },
    };

    let db: DbClient;
    let workspaceId: string;
    let agentId: string;
    const model =
      provider === 'anthropic'
        ? 'claude-sonnet-4-6'
        : provider === 'deepseek'
          ? 'deepseek-flash'
          : 'codex-auto-review';
    const overrideModel =
      provider === 'anthropic'
        ? 'claude-opus-4-6'
        : provider === 'deepseek'
          ? 'deepseek-v4-pro'
          : 'gpt-5.4-mini';
    const pricingOrganization = `org_codex_accounting_${randomUUID()}`;
    let now = new Date('2026-09-20T23:59:00Z');
    const snapshot: AgentSnapshot = {
      provider: sdkHarness === 'pi_sdk' ? 'pi-sdk' : 'codex-sdk',
      model: { provider, id: model },
      system: '',
      allowed_tool_names: [],
      allowed_mcp_server_names: [],
      egress: {
        mode: 'gateway',
        gateway: { mcp_base_url: 'http://gateway', session_jwt: 'test', mcp_servers: {} },
      },
    };
    const budgetSnapshot = {
      ...snapshot,
      guardrails: [rule('token_budget', { max_total_tokens: 100000 })],
    };
    beforeAll(async () => {
      ({ db } = await getTestDb());
      workspaceId = uniqueWorkspace('codex_budget');
      await db
        .insert(organizations)
        .values({ id: pricingOrganization, name: pricingOrganization, status: 'active' });
      await db.insert(workspaces).values({
        id: workspaceId,
        organizationId: pricingOrganization,
        name: 'Codex accounting',
        createdBy: 'integration-test',
        status: 'active',
      });
      agentId = `agt_${randomUUID()}`;
      const stored = {
        harness_type: sdkHarness,
        id: agentId,
        version: 1,
        name: 'Codex',
        model: snapshot.model,
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        multiagent: null,
        metadata: { harness: sdkHarness, mode: 'colocated' },
      };
      await db.insert(agents).values({
        id: agentId,
        workspaceId,
        name: 'Codex',
        metadata: { harness: sdkHarness, mode: 'colocated' },
        harnessType: sdkHarness,
        modelProvider: provider,
        modelId: model,
      });
      await db
        .insert(agentVersions)
        .values({ id: `av_${randomUUID()}`, workspaceId, agentId, version: 1, snapshot: stored });
      await db
        .insert(modelPrices)
        .values({
          provider,
          modelId: model,
          organizationId: pricingOrganization,
          source: 'operator',
          inputPerMillionTokens: 1,
          outputPerMillionTokens: 2,
          cacheReadPerMillionTokens: 0.5,
        })
        .onConflictDoNothing();
      await db
        .insert(modelPrices)
        .values({
          provider,
          modelId: overrideModel,
          organizationId: pricingOrganization,
          source: 'operator',
          inputPerMillionTokens: 3,
          outputPerMillionTokens: 4,
          cacheReadPerMillionTokens: 1,
        })
        .onConflictDoNothing();
    });
    afterAll(closeTestDb);

    async function fixture(
      options: { db?: DbClient; current?: () => boolean; target?: 'cloud' | 'self_hosted' } = {},
    ) {
      const sessionId = `ses_${randomUUID()}`,
        runnerId = `runner_${randomUUID()}`;
      const environmentId = `env_${randomUUID()}`;
      await db.insert(environments).values({
        id: environmentId,
        workspaceId,
        name: environmentId,
        target: options.target ?? 'self_hosted',
      });
      await db.insert(sessions).values({
        environmentId,
        id: sessionId,
        workspaceId,
        agentId,
        agentVersion: 1,
        runnerId,
        distributionState: 'assigned',
      });
      let seq = 0;
      async function event(subject: string | null = 'actor:alice'): Promise<Event> {
        const event = httpEventToProto({
          workspaceId,
          sessionId,
          producedBy: 'client',
          idempotencyKey: '',
          input: {
            id: `evt_${randomUUID()}`,
            type: 'user.message',
            content: [{ type: 'text', text: 'run' }],
            subject: 'spoofed',
          },
        });
        await db.insert(sessionEventsIndex).values({
          workspaceId,
          sessionId,
          eventId: event.id,
          seq: ++seq,
          producedAt: now.toISOString(),
          producedBy: 'client',
          kind: 'user.message',
          visibility: 'public',
          guardrailSubject: subject,
        });
        return event;
      }
      const accounting = new CodexRunnerAccounting(
        options.db ?? db,
        { workspaceId, sessionId, runnerId, isCurrent: options.current ?? (() => true) },
        () => now,
      );
      const states = () =>
        db
          .select()
          .from(guardrailState)
          .where(
            and(
              eq(guardrailState.workspaceId, workspaceId),
              eq(guardrailState.sessionId, sessionId),
            ),
          );
      const pending = async () =>
        (await states()).filter((s) => s.key.startsWith(CODEX_USAGE_PENDING_PREFIX));
      const row = async () =>
        (await db.select().from(sessions).where(eq(sessions.id, sessionId)))[0]!;
      return { sessionId, runnerId, accounting, event, states, pending, row };
    }
    async function finish(f: Awaited<ReturnType<typeof fixture>>, event: Event) {
      await f.accounting.usage(event.id, { usage });
      await f.accounting.checkpoint(event.id, checkpoint);
      await f.accounting.complete(event.id);
    }

    it.each([overrideModel, { id: overrideModel, provider, effort: 'high' }])(
      'uses the persisted session model override and its price: %j',
      async (override) => {
        const f = await fixture(),
          event = await f.event();
        await db
          .update(sessions)
          .set({ agentOverrides: { model: override } })
          .where(eq(sessions.id, f.sessionId));
        const effective = {
          ...budgetSnapshot,
          model: {
            provider,
            id: overrideModel,
            ...(typeof override === 'object' ? { effort: override.effort } : {}),
          },
        };
        await f.accounting.prepare(event, effective);
        expect(await f.pending()).toHaveLength(1);
        await expect(f.accounting.usage(event.id, { usage, model })).rejects.toThrow('Invalid');
        const recorded = await f.accounting.usage(event.id, { usage, model: overrideModel });
        expect(recorded).toMatchObject({ model: overrideModel, provider });
        expect((await f.row()).usageCostNanoUsd).toBe(52000n);
        await f.accounting.checkpoint(event.id, checkpoint);
        await f.accounting.complete(event.id);
        expect(await f.pending()).toHaveLength(0);
      },
    );

    it('reads effective model overrides only after acquiring the Session lock', async () => {
      const f = await fixture(),
        event = await f.event();
      let release!: () => void, acquired!: () => void;
      const locked = new Promise<void>((resolve) => {
        acquired = resolve;
      });
      const changing = db.transaction(async (tx) => {
        await tx.select().from(sessions).where(eq(sessions.id, f.sessionId)).for('update');
        await tx
          .update(sessions)
          .set({ agentOverrides: { model: overrideModel } })
          .where(eq(sessions.id, f.sessionId));
        acquired();
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      });
      await locked;
      const preparing = f.accounting.prepare(event, {
        ...budgetSnapshot,
        model: { provider, id: overrideModel },
      });
      release();
      await changing;
      await preparing;
      await finish(f, event);
      expect((await f.row()).usageCostNanoUsd).toBe(52000n);
    });

    it('retains pending accounting when preparation is interrupted while waiting for the Session lock', async () => {
      const f = await fixture(),
        event = await f.event();
      let release!: () => void, acquired!: () => void;
      const locked = new Promise<void>((resolve) => {
        acquired = resolve;
      });
      const held = db.transaction(async (tx) => {
        await tx.select().from(sessions).where(eq(sessions.id, f.sessionId)).for('update');
        acquired();
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      });
      await locked;
      const preparing = f.accounting.prepare(event, budgetSnapshot);
      const rejected = expect(preparing).rejects.toThrow('preparation was not acknowledged');
      f.accounting.interrupt();
      release();
      await held;
      await rejected;
      expect(await f.pending()).toHaveLength(1);
      await expect(f.accounting.prepare(await f.event(), budgetSnapshot)).rejects.toThrow(
        'pending usage',
      );
    });

    it.each([
      ['forged override', undefined, { provider, id: overrideModel }],
      ['stale pinned model', { model: overrideModel }, { provider, id: model }],
      [
        'foreign provider',
        {
          model: { provider: provider === 'anthropic' ? 'openai' : 'anthropic', id: overrideModel },
        },
        { provider, id: overrideModel },
      ],
      ['unsupported model', { model: 'not-in-pinned-sdk' }, { provider, id: 'not-in-pinned-sdk' }],
    ])('refuses %s before persisting a marker', async (_, agentOverrides, deliveredModel) => {
      const f = await fixture(),
        event = await f.event();
      if (agentOverrides)
        await db.update(sessions).set({ agentOverrides }).where(eq(sessions.id, f.sessionId));
      await expect(
        f.accounting.prepare(event, { ...budgetSnapshot, model: deliveredModel }),
      ).rejects.toThrow('preparation was not acknowledged');
      expect(await f.pending()).toHaveLength(0);
      expect((await f.row()).usageInputTokens).toBe(0);
    });

    it('persists pending before execution, deduplicates usage, prices the pinned provider and denies the next token-capped request', async () => {
      const f = await fixture(),
        event = await f.event();
      const guarded = { ...snapshot, guardrails: [rule('token_budget', { max_total_tokens: 17 })] };
      await f.accounting.prepare(event, guarded);
      expect(await f.pending()).toHaveLength(1);
      await f.accounting.usage(event.id, { usage, guardrail_usage_recorded: true });
      await f.accounting.usage(event.id, { usage, id: 'evt_untrusted_duplicate' });
      expect((await f.row()).usageInputTokens).toBe(10);
      expect((await f.row()).usageCostNanoUsd).toBe(21000n);
      expect(
        (await f.states()).find((s) => s.key === SHARED_USAGE_KEYS.totalTokens)?.valueNum,
      ).toBe(17);
      await f.accounting.checkpoint(event.id, checkpoint);
      expect(await f.pending()).toHaveLength(1);
      await f.accounting.complete(event.id);
      expect(await f.pending()).toHaveLength(0);
      await expect(f.accounting.prepare(await f.event(), guarded)).rejects.toThrow(
        /budget of 17 tokens/,
      );
      expect(
        await db
          .select()
          .from(sessionUsageEvents)
          .where(eq(sessionUsageEvents.sessionId, f.sessionId)),
      ).toHaveLength(1);
    });

    it('counts usage without guardrails and refuses forged identity, partial usage, model or thread reports', async () => {
      const f = await fixture(),
        event = await f.event();
      await expect(
        f.accounting.prepare({ ...event, workspaceId: 'foreign' }, snapshot),
      ).rejects.toThrow(/identity/);
      await expect(
        f.accounting.prepare(event, { ...snapshot, model: { ...snapshot.model, id: 'other' } }),
      ).rejects.toThrow(/not acknowledged/);
      await f.accounting.prepare(event, snapshot);
      for (const bad of [
        { usage: {} },
        { usage, model: 'other' },
        { usage, provider: provider === 'anthropic' ? 'openai' : 'anthropic' },
        { usage, thread_id: 'sth_other' },
        { usage, turn_event_id: 'evt_other' },
      ])
        await expect(f.accounting.usage(event.id, bad)).rejects.toThrow(/Invalid/);
      await finish(f, event);
      expect((await f.row()).usageInputTokens).toBe(10);
      expect(await f.pending()).toHaveLength(0);
    });

    it('uses authenticated actor counters across sessions and refreshes UTC daily windows', async () => {
      const first = await fixture(),
        second = await fixture(),
        third = await fixture();
      const guarded = {
        ...snapshot,
        guardrails: [rule('user_daily_cost_budget', { max_cost_usd: 0.00002 })],
      };
      const event = await first.event('actor:daily');
      await first.accounting.prepare(event, guarded);
      await finish(first, event);
      await expect(
        second.accounting.prepare(await second.event('actor:daily'), guarded),
      ).rejects.toThrow(/Today's spend/);
      const other = await third.event('actor:other');
      await third.accounting.prepare(other, guarded);
      await finish(third, other);
      expect(
        await db
          .select()
          .from(guardrailCounters)
          .where(
            and(
              eq(guardrailCounters.workspaceId, workspaceId),
              eq(guardrailCounters.subject, 'spoofed'),
            ),
          ),
      ).toHaveLength(0);
      now = new Date('2026-09-21T00:01:00Z');
      await second.accounting.prepare(await second.event('actor:daily'), guarded);
    });

    it.each(['missing_usage', 'interrupt', 'failure'])(
      'keeps pending after %s and reconnect blocks the next request',
      async (reason) => {
        const f = await fixture(),
          event = await f.event();
        await f.accounting.prepare(event, budgetSnapshot);
        if (reason !== 'missing_usage') await f.accounting.usage(event.id, { usage });
        if (reason === 'interrupt') f.accounting.interrupt();
        if (reason === 'failure') f.accounting.fail();
        await f.accounting.checkpoint(event.id, checkpoint);
        await expect(f.accounting.complete(event.id)).rejects.toThrow(/not acknowledged/);
        expect(await f.pending()).toHaveLength(1);
        const recovered = new CodexRunnerAccounting(db, {
          ...f,
          workspaceId,
          isCurrent: () => true,
        });
        await expect(recovered.prepare(await f.event(), budgetSnapshot)).rejects.toThrow(
          /pending usage/,
        );
      },
    );

    it('continues unguarded sessions after missing usage or interruption, including reconnect', async () => {
      const f = await fixture(),
        first = await f.event();
      await f.accounting.prepare(first, snapshot);
      f.accounting.interrupt();
      await f.accounting.checkpoint(first.id, checkpoint);
      await f.accounting.complete(first.id);
      expect(await f.pending()).toHaveLength(0);
      const second = await f.event();
      await f.accounting.prepare(second, snapshot);
      await finish(f, second);
      const recovered = new CodexRunnerAccounting(db, { ...f, workspaceId, isCurrent: () => true });
      await recovered.prepare(await f.event(), snapshot);
      expect((await f.row()).usageInputTokens).toBe(10);
    });

    it('fails closed without accepted event subject and after runner/lifecycle changes', async () => {
      const f = await fixture();
      await expect(f.accounting.prepare(await f.event(null), budgetSnapshot)).rejects.toThrow(
        /not acknowledged/,
      );
      expect(await f.pending()).toHaveLength(0);
      const event = await f.event();
      await f.accounting.prepare(event, budgetSnapshot);
      await db.update(sessions).set({ runnerId: 'new-runner' }).where(eq(sessions.id, f.sessionId));
      await expect(f.accounting.usage(event.id, { usage })).rejects.toThrow(/binding/);
      await expect(f.accounting.checkpoint(event.id, checkpoint)).rejects.toThrow(/ownership/);
      expect((await f.row()).usageInputTokens).toBe(0);
    });

    it('rolls back state and usage when the generation changes while a transaction is waiting', async () => {
      let current = true;
      let entered: (() => void) | undefined;
      const f = await fixture({
          current: () => {
            entered?.();
            return current;
          },
        }),
        event = await f.event();
      await f.accounting.prepare(event, budgetSnapshot);
      let release!: () => void, acquired!: () => void;
      const locked = new Promise<void>((resolve) => {
        acquired = resolve;
      });
      const held = db.transaction(async (tx) => {
        await tx.select().from(sessions).where(eq(sessions.id, f.sessionId)).for('update');
        acquired();
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      });
      await locked;
      const started = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const write = f.accounting.usage(event.id, { usage });
      await started;
      current = false;
      release();
      await held;
      await expect(write).rejects.toThrow(/retired|current/);
      expect((await f.row()).usageInputTokens).toBe(0);
      expect(await f.pending()).toHaveLength(1);
    });

    it.each(['prepare', 'usage', 'checkpoint', 'delete'])(
      'retains the durable marker when %s acknowledgement fails',
      async (stage) => {
        let failNext = false;
        const flaky = new Proxy(db, {
          get(target, key) {
            if (key !== 'transaction') return Reflect.get(target, key);
            return async (callback: Parameters<DbClient['transaction']>[0]) => {
              if (failNext) {
                failNext = false;
                throw new Error('database unavailable');
              }
              return target.transaction(callback);
            };
          },
        });
        const f = await fixture({ db: flaky }),
          event = await f.event();
        if (stage === 'prepare') {
          failNext = true;
          await expect(f.accounting.prepare(event, budgetSnapshot)).rejects.toThrow(
            /not acknowledged/,
          );
          expect(await f.pending()).toHaveLength(0);
          expect((await f.row()).usageInputTokens).toBe(0);
          return;
        }
        await f.accounting.prepare(event, budgetSnapshot);
        if (stage === 'usage') {
          failNext = true;
          await expect(f.accounting.usage(event.id, { usage })).rejects.toThrow(/unavailable/);
        } else {
          await f.accounting.usage(event.id, { usage });
          if (stage === 'checkpoint') {
            failNext = true;
            await expect(f.accounting.checkpoint(event.id, checkpoint)).rejects.toThrow(
              /unavailable/,
            );
          } else {
            await f.accounting.checkpoint(event.id, checkpoint);
            failNext = true;
            await expect(f.accounting.complete(event.id)).rejects.toThrow(/unavailable/);
          }
        }
        expect(await f.pending()).toHaveLength(1);
        await expect(f.accounting.prepare(await f.event(), budgetSnapshot)).rejects.toThrow(
          /pending usage/,
        );
      },
    );

    it.each(['prepare', 'usage', 'checkpoint', 'delete'])(
      'handles committed %s writes with a lost ACK without spending twice',
      async (stage) => {
        let loseNext = false;
        const flaky = new Proxy(db, {
          get(target, key) {
            if (key !== 'transaction') return Reflect.get(target, key);
            return async (callback: Parameters<DbClient['transaction']>[0]) => {
              const result = await target.transaction(callback);
              if (loseNext) {
                loseNext = false;
                throw new Error('ACK lost after commit');
              }
              return result;
            };
          },
        });
        const f = await fixture({ db: flaky }),
          event = await f.event();
        if (stage === 'prepare') {
          loseNext = true;
          await expect(f.accounting.prepare(event, budgetSnapshot)).rejects.toThrow(
            /not acknowledged/,
          );
          expect(await f.pending()).toHaveLength(1);
          await expect(f.accounting.prepare(await f.event(), budgetSnapshot)).rejects.toThrow(
            /pending usage/,
          );
          return;
        }
        await f.accounting.prepare(event, budgetSnapshot);
        if (stage === 'usage') {
          loseNext = true;
          await expect(f.accounting.usage(event.id, { usage })).rejects.toThrow(/ACK lost/);
          expect((await f.row()).usageInputTokens).toBe(10);
          await f.accounting.usage(event.id, { usage });
        } else await f.accounting.usage(event.id, { usage });
        if (stage === 'checkpoint') {
          loseNext = true;
          await expect(f.accounting.checkpoint(event.id, checkpoint)).rejects.toThrow(/ACK lost/);
          expect(
            await db
              .select()
              .from(sessionHarnessStates)
              .where(eq(sessionHarnessStates.sessionId, f.sessionId)),
          ).toHaveLength(1);
          await expect(f.accounting.complete(event.id)).rejects.toThrow(/not acknowledged/);
          expect(await f.pending()).toHaveLength(1);
          return;
        }
        await f.accounting.checkpoint(event.id, checkpoint);
        if (stage === 'delete') loseNext = true;
        await f.accounting.complete(event.id);
        expect(await f.pending()).toHaveLength(0);
        expect((await f.row()).usageInputTokens).toBe(10);
        await f.accounting.prepare(await f.event(), budgetSnapshot);
      },
    );

    it('retains a guarded marker when an interrupt arrives while completion waits for the session lock', async () => {
      const f = await fixture(),
        event = await f.event();
      await f.accounting.prepare(event, budgetSnapshot);
      await f.accounting.usage(event.id, { usage });
      await f.accounting.checkpoint(event.id, checkpoint);
      let release!: () => void, acquired!: () => void;
      const locked = new Promise<void>((resolve) => {
        acquired = resolve;
      });
      const held = db.transaction(async (tx) => {
        await tx.select().from(sessions).where(eq(sessions.id, f.sessionId)).for('update');
        acquired();
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      });
      await locked;
      const completion = f.accounting.complete(event.id);
      f.accounting.interrupt();
      release();
      await held;
      await expect(completion).rejects.toThrow(/interrupted/);
      expect(await f.pending()).toHaveLength(1);
    });

    it('unpriced usage denies the next request when on_unpriced asks', async () => {
      await db
        .update(modelPrices)
        .set({ deletedAt: new Date() })
        .where(
          and(
            eq(modelPrices.provider, provider),
            eq(modelPrices.modelId, model),
            eq(modelPrices.organizationId, pricingOrganization),
          ),
        );
      try {
        const f = await fixture(),
          event = await f.event();
        const guarded = {
          ...snapshot,
          guardrails: [rule('cost_budget', { max_cost_usd: 1, on_unpriced: 'ask' })],
        };
        await f.accounting.prepare(event, guarded);
        await finish(f, event);
        expect((await f.row()).usageHasUnpriced).toBe(true);
        await expect(f.accounting.prepare(await f.event(), guarded)).rejects.toThrow(
          /no price data/,
        );
      } finally {
        await db
          .update(modelPrices)
          .set({ deletedAt: null })
          .where(
            and(
              eq(modelPrices.provider, provider),
              eq(modelPrices.modelId, model),
              eq(modelPrices.organizationId, pricingOrganization),
            ),
          );
      }
    });

    it('refuses retired Registry accounting on a cloud Codex Session', async () => {
      const f = await fixture({ target: 'cloud' });
      await expect(f.accounting.prepare(await f.event(), budgetSnapshot)).rejects.toMatchObject({
        cause: { message: 'session is not Registry-owned Codex' },
      });
      expect(await f.pending()).toHaveLength(0);
    });

    it.each(['cloud', 'self_hosted'] as const)(
      'authorizes the correct Codex usage writer for %s',
      async (target) => {
        const f = await fixture({ target });
        const app = buildCombinedTestApp({
          db,
          oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
          store: buildStubStore(),
          sse: STUB_SSE_CONFIG,
          jwtMinter: buildTestJwtMinter(),
          fileStore: buildStubFileStore(),
        });
        try {
          const response = await app.inject({
            method: 'POST',
            url: `/internal/v1/workspaces/${workspaceId}/sessions/${f.sessionId}/usage`,
            payload: { usage, model, provider },
          });
          expect(response.statusCode).toBe(target === 'self_hosted' ? 403 : 200);
          expect((await f.row()).usageInputTokens).toBe(target === 'self_hosted' ? 0 : 10);
        } finally {
          await app.close();
        }
      },
    );
  },
);
