// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { and, eq, isNull, ne } from 'drizzle-orm';
import {
  BUILTIN_EVALUATORS,
  compileGuardrailRule,
  evaluateGuardrails,
  evaluateGuardrailExpression,
  InMemoryGuardrailStateStore,
  type PreparedGuardrail,
} from '@orca/guardrails';
import { validateHarnessGuardrails, validateHarnessModel } from '@orca/harness-catalog';
import { normalizeModelForStorage } from '../contracts/model-wire.js';
import type { Event } from '@orca/transcript-store';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agentVersions,
  guardrailState,
  sessions,
  workspaces,
} from '../persistence/postgres/schema.js';
import type { AgentSnapshot, SnapshotGuardrail } from './agent-snapshot.js';
import { loadSessionHarnessBinding, loadSessionExecutionOwner } from './session-harness-binding.js';
import { saveRunnerHarnessState } from './harness-state.js';
import {
  applyCounterStateUpdate,
  applySessionStateUpdate,
  loadGuardrailSubjectWindowState,
  loadGuardrailTurnSubject,
  parseUsageDelta,
  persistUsage,
  priceUsageDelta,
  utcDateWindow,
  type DbTransaction,
  type UsageDelta,
} from './usage-accounting.js';

export const CODEX_USAGE_PENDING_PREFIX = 'codex_sdk_usage_pending:';

/** A refused message leaves the independent confirmation/interrupt followers alive. */
export class RunnerRequestDenied extends Error {
  constructor(
    message: string,
    readonly errorType = 'policy_denied',
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export interface RunnerTurnAccounting {
  prepare(event: Event, snapshot: AgentSnapshot): Promise<void>;
  usage(turnEventId: string, line: Record<string, unknown>): Promise<Record<string, unknown>>;
  checkpoint(turnEventId: string, state: unknown): Promise<void>;
  complete(turnEventId: string): Promise<void>;
  interrupt(): void;
  fail(): void;
}

export interface RunnerAccountingContext {
  workspaceId: string;
  sessionId: string;
  runnerId: string;
  isCurrent(): boolean;
}

export function codexUsageEventId(
  workspaceId: string,
  sessionId: string,
  turnEventId: string,
): string {
  return `evt_${createHash('sha256')
    .update(JSON.stringify([workspaceId, sessionId, turnEventId]))
    .digest('hex')}`;
}

/** SDK reports are complete, integral deltas; legacy internal producers retain their parser. */
export function parseCodexUsage(raw: unknown): UsageDelta | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const u = raw as Record<string, unknown>;
  const cache = u.cache_creation as Record<string, unknown> | undefined;
  if (!cache || typeof cache !== 'object' || Array.isArray(cache)) return null;
  if (
    ![
      u.input_tokens,
      u.output_tokens,
      u.cache_read_input_tokens,
      cache.ephemeral_1h_input_tokens,
      cache.ephemeral_5m_input_tokens,
    ].every((v) => typeof v === 'number' && Number.isSafeInteger(v) && v >= 0)
  )
    return null;
  const result = parseUsageDelta(raw);
  return result && Number.isSafeInteger(Object.values(result).reduce((a, b) => a + b, 0))
    ? result
    : null;
}

export function compileCodexRequestGuardrails(
  guards: readonly SnapshotGuardrail[],
): PreparedGuardrail[] {
  const prepared = guards.map((g): PreparedGuardrail => {
    const rule = g.rule as PreparedGuardrail['guardrail']['rule'];
    const scope = g.tier === 'organization' || g.tier === 'workspace' ? g.tier : 'explicit';
    const compiled = compileGuardrailRule(rule, scope);
    if (!compiled.ok)
      throw new Error(
        `Invalid guardrail ${g.id}: ${compiled.errors.map((e) => e.message).join(', ')}`,
      );
    if (rule.kind === 'builtin') {
      if (rule.builtin === 'subagent_cost_budget')
        throw new Error('Codex does not support subagent budget rules');
      if (
        ['ask_thresholds', 'ask_thresholds_usd'].some(
          (k) => Array.isArray(compiled.params[k]) && compiled.params[k].length > 0,
        )
      )
        throw new Error('Codex request budgets do not support soft approval thresholds');
    }
    return {
      guardrail: {
        id: g.id,
        name: g.name,
        enabled: true,
        scope,
        phases: g.phases as PreparedGuardrail['guardrail']['phases'],
        rule:
          rule.kind === 'builtin' &&
          ['cost_budget', 'user_daily_cost_budget'].includes(rule.builtin)
            ? {
                ...rule,
                params: {
                  ...rule.params,
                  on_unpriced: rule.params?.on_unpriced === 'allow' ? 'allow' : 'deny',
                },
              }
            : rule,
      },
      tier: g.tier as PreparedGuardrail['tier'],
      stateful: compiled.stateful,
      ...(compiled.stateScope ? { stateScope: compiled.stateScope } : {}),
      ...(g.subagent_id ? { subagentId: g.subagent_id } : {}),
    };
  });
  const error = validateHarnessGuardrails(
    { harness: 'codex_sdk', mode: 'colocated' },
    prepared.map((g) => ({
      id: g.guardrail.id,
      phases: g.guardrail.phases,
      rule: g.guardrail.rule,
      stateful: g.stateful,
      subagentId: g.subagentId,
    })),
  );
  if (error) throw new Error(error);
  return prepared;
}

/** One serial bridge's durable request/usage lane. All mutations lock the current assignment. */
export class CodexRunnerAccounting implements RunnerTurnAccounting {
  private active:
    | {
        turnEventId: string;
        usageEventId: string;
        model: string;
        provider: string;
        subject: string;
        guarded: boolean;
        usageAcknowledged: boolean;
        checkpointAcknowledged: boolean;
        failed: boolean;
      }
    | undefined;
  constructor(
    private readonly db: DbClient,
    private readonly context: RunnerAccountingContext,
    private readonly now: () => Date = () => new Date(),
  ) {}

  private async locked<T>(run: (tx: DbTransaction) => Promise<T>): Promise<T> {
    return this.db.transaction(async (tx) => {
      const c = this.context;
      if (!c.isCurrent()) throw new Error('runner generation retired');
      const [row] = await tx
        .select()
        .from(sessions)
        .where(
          and(
            eq(sessions.workspaceId, c.workspaceId),
            eq(sessions.id, c.sessionId),
            eq(sessions.runnerId, c.runnerId),
            eq(sessions.distributionState, 'assigned'),
            isNull(sessions.deletedAt),
            isNull(sessions.archivedAt),
            ne(sessions.status, 'terminated'),
          ),
        )
        .for('update');
      if (!row || !c.isCurrent()) throw new Error('runner session binding is no longer current');
      const [workspace] = await tx
        .select({ status: workspaces.status })
        .from(workspaces)
        .where(eq(workspaces.id, c.workspaceId));
      if (workspace?.status !== 'active') throw new Error('workspace is not active');
      const selection = await loadSessionHarnessBinding(
        tx as unknown as DbClient,
        c.workspaceId,
        row.agentId,
        row.agentVersion,
      );
      if (
        (selection.harness !== 'codex_sdk' && selection.harness !== 'pi_sdk') ||
        selection.mode !== 'colocated' ||
        (await loadSessionExecutionOwner(tx as unknown as DbClient, c.workspaceId, c.sessionId)) !==
          'registry'
      )
        throw new Error('session is not Registry-owned Codex');
      const result = await run(tx);
      if (!c.isCurrent()) throw new Error('runner generation retired');
      return result;
    });
  }

  async prepare(event: Event, snapshot: AgentSnapshot): Promise<void> {
    const c = this.context;
    if (
      (snapshot.provider !== 'codex-sdk' && snapshot.provider !== 'pi-sdk') ||
      (snapshot.provider === 'codex-sdk' && snapshot.model.provider !== 'openai') ||
      !snapshot.model.id ||
      snapshot.multiagent ||
      event.subpath ||
      event.workspaceId !== c.workspaceId ||
      event.sessionId !== c.sessionId ||
      event.producedBy !== 'client' ||
      !event.id.startsWith('evt_')
    )
      throw new RunnerRequestDenied(
        'Invalid Codex request identity',
        'guardrail_usage_unavailable',
      );
    const guards = compileCodexRequestGuardrails(snapshot.guardrails ?? []);
    const active = {
      turnEventId: event.id,
      usageEventId: codexUsageEventId(c.workspaceId, c.sessionId, event.id),
      model: snapshot.model.id,
      provider: snapshot.model.provider,
      subject: '',
      guarded: guards.some((g) => g.stateful),
      usageAcknowledged: false,
      checkpointAcknowledged: false,
      failed: false,
    };
    this.active = active;
    try {
      await this.locked(async (tx) => {
        const db = tx as unknown as DbClient;
        const [pin] = await tx
          .select({ snapshot: agentVersions.snapshot, overrides: sessions.agentOverrides })
          .from(sessions)
          .innerJoin(
            agentVersions,
            and(
              eq(agentVersions.workspaceId, sessions.workspaceId),
              eq(agentVersions.agentId, sessions.agentId),
              eq(agentVersions.version, sessions.agentVersion),
            ),
          )
          .where(and(eq(sessions.workspaceId, c.workspaceId), eq(sessions.id, c.sessionId)));
        const pinned = pin?.snapshot as { model?: { provider?: string; id?: string } } | undefined;
        const overrides = pin?.overrides as { model?: unknown } | null | undefined;
        const model = normalizeModelForStorage(
          overrides?.model ?? pinned?.model,
          pinned?.model?.provider,
        );
        if ('error' in model) throw new Error(model.error);
        const modelError = validateHarnessModel(
          { harness: snapshot.provider === 'pi-sdk' ? 'pi_sdk' : 'codex_sdk', mode: 'colocated' },
          model,
        );
        if (modelError) throw new Error(modelError);
        if (model.provider !== snapshot.model.provider || model.id !== snapshot.model.id)
          throw new Error('Codex model does not match persisted Session and pinned AgentVersion');
        // The Session row is locked: the persisted effective model owns both the
        // request policy and price selection, never the runner's usage identity.
        active.model = model.id;
        active.provider = model.provider;
        const rows = await tx
          .select()
          .from(guardrailState)
          .where(
            and(
              eq(guardrailState.workspaceId, c.workspaceId),
              eq(guardrailState.sessionId, c.sessionId),
            ),
          );
        const state = Object.fromEntries(
          rows.map((row) => [row.key, row.valueNum ?? row.valueJson]),
        );
        if (
          active.guarded &&
          Object.keys(state).some((key) => key.startsWith(CODEX_USAGE_PENDING_PREFIX))
        )
          throw new RunnerRequestDenied(
            'Codex pending usage accounting blocks the next request',
            'guardrail_usage_unavailable',
          );
        const subject = await loadGuardrailTurnSubject(db, c.workspaceId, c.sessionId, event.id);
        if (!subject) throw new Error('Authenticated turn subject is unavailable');
        active.subject = subject;
        const now = this.now();
        const daily = await loadGuardrailSubjectWindowState(
          db,
          c.workspaceId,
          subject,
          utcDateWindow(now),
        );
        const store = new InMemoryGuardrailStateStore({ session: state, subject_window: daily });
        const payload = JSON.parse(Buffer.from(event.payload).toString('utf8')) as {
          content?: unknown;
          text?: unknown;
        };
        const userText =
          typeof payload.content === 'string'
            ? payload.content
            : Array.isArray(payload.content)
              ? payload.content
                  .filter((b) => b?.type === 'text')
                  .map((b) => b.text)
                  .join('\n')
              : typeof payload.text === 'string'
                ? payload.text
                : '';
        const decision = evaluateGuardrails(
          guards,
          { phase: 'request', sessionId: c.sessionId, modelId: active.model, userText },
          { builtins: BUILTIN_EVALUATORS, expression: evaluateGuardrailExpression, store },
        );
        if (decision.verdict !== 'allow')
          throw new RunnerRequestDenied(decision.reasons.join('; ') || 'Request denied by policy.');
        for (const update of decision.stateUpdates) {
          if (update.scope === 'session')
            await applySessionStateUpdate(
              tx,
              c.workspaceId,
              c.sessionId,
              { ...update, scope: 'session' },
              now,
            );
          else if (update.scope === 'subject_window')
            await applyCounterStateUpdate(
              tx,
              c.workspaceId,
              subject,
              utcDateWindow(now),
              { ...update, scope: 'subject_window' },
              now,
            );
        }
        if (active.guarded)
          await applySessionStateUpdate(
            tx,
            c.workspaceId,
            c.sessionId,
            {
              scope: 'session',
              key: `${CODEX_USAGE_PENDING_PREFIX}${event.id}`,
              action: 'set',
              value: { version: 1, turnEventId: event.id, usageEventId: active.usageEventId },
            },
            now,
          );
      });
      if (active.failed) throw new Error('Codex turn interrupted during request preparation');
    } catch (error) {
      if (error instanceof RunnerRequestDenied) throw error;
      throw new RunnerRequestDenied(
        'Codex durable request preparation was not acknowledged',
        'guardrail_usage_unavailable',
        { cause: error },
      );
    }
  }

  private turn(turnEventId: string) {
    if (!this.active || this.active.turnEventId !== turnEventId)
      throw new Error('Codex accounting turn mismatch');
    return this.active;
  }

  async usage(
    turnEventId: string,
    line: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const turn = this.turn(turnEventId),
      c = this.context;
    const usage = parseCodexUsage(line.usage);
    if (
      !usage ||
      line.subpath ||
      line.subagent_id ||
      line.thread_id ||
      (line.model !== undefined && line.model !== turn.model) ||
      (line.provider !== undefined && line.provider !== turn.provider) ||
      (line.turn_event_id !== undefined && line.turn_event_id !== turnEventId)
    )
      throw new Error('Invalid Codex usage identity or counters');
    await this.locked(async (tx) => {
      const costNanoUsd = await priceUsageDelta(
        tx as unknown as DbClient,
        c.workspaceId,
        turn.provider,
        turn.model,
        usage,
      );
      const outcome = await persistUsage(tx, {
        workspaceId: c.workspaceId,
        sessionId: c.sessionId,
        usage,
        costNanoUsd,
        now: this.now(),
        turnSubject: turn.subject,
        usageEventId: turn.usageEventId,
      });
      if (outcome !== 'applied' && outcome !== 'replayed')
        throw new Error('Codex usage was not recorded');
    });
    turn.usageAcknowledged = true;
    // Registry owns all identity fields, including the stable dedup id. A runner's
    // guardrail_usage_recorded claim is deliberately discarded.
    return {
      type: 'agent.usage',
      id: turn.usageEventId,
      usage: line.usage,
      model: turn.model,
      provider: turn.provider,
      turn_event_id: turnEventId,
    };
  }

  async checkpoint(turnEventId: string, state: unknown): Promise<void> {
    const turn = this.turn(turnEventId),
      c = this.context;
    await saveRunnerHarnessState(
      this.db,
      c.workspaceId,
      c.sessionId,
      c.runnerId,
      state,
      c.isCurrent,
    );
    turn.checkpointAcknowledged = turn.usageAcknowledged;
  }

  async complete(turnEventId: string): Promise<void> {
    const turn = this.turn(turnEventId),
      c = this.context;
    if (!turn.guarded) return;
    if (!turn.usageAcknowledged || !turn.checkpointAcknowledged || turn.failed)
      throw new RunnerRequestDenied(
        'Codex terminal usage or checkpoint was not acknowledged',
        'guardrail_usage_unavailable',
      );
    // Idempotent deletion also reconciles a lost deletion ACK: the authoritative
    // absence is success, while lifecycle/generation checks still run first.
    try {
      await this.locked(async (tx) => {
        if (turn.failed) throw new Error('Codex turn interrupted before marker commit');
        await applySessionStateUpdate(
          tx,
          c.workspaceId,
          c.sessionId,
          {
            scope: 'session',
            key: `${CODEX_USAGE_PENDING_PREFIX}${turnEventId}`,
            action: 'delete',
          },
          this.now(),
        );
        if (turn.failed) throw new Error('Codex turn interrupted during marker commit');
      });
    } catch (error) {
      const cleared = await this.locked(async (tx) => {
        const rows = await tx
          .select({ key: guardrailState.key })
          .from(guardrailState)
          .where(
            and(
              eq(guardrailState.workspaceId, c.workspaceId),
              eq(guardrailState.sessionId, c.sessionId),
              eq(guardrailState.key, `${CODEX_USAGE_PENDING_PREFIX}${turnEventId}`),
            ),
          );
        return rows.length === 0;
      });
      if (!cleared) throw error;
    }
  }
  interrupt(): void {
    if (this.active) this.active.failed = true;
  }
  fail(): void {
    if (this.active) this.active.failed = true;
  }
}
