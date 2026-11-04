// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  policyAppends,
  policyPruned,
  policyEvaluations,
  policyEvaluationLatency,
} from './dogwood-metrics.js';
import { lockPolicyStateRevision } from './policy-state-revision.js';
import { createHash } from 'node:crypto';
import { and, eq, gte, isNull, sql, asc } from 'drizzle-orm';
import {
  canonicalTool,
  finalizeOutcome,
  type Rule,
  type PolicyCall,
  type PolicyEvent,
  type EvaluationResult,
} from '@runorca/dogwood';
import { evaluateDogwoodDecision, mergeDogwoodEvents } from '@orca/guardrails';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import {
  guardrailPolicyHeads as heads,
  guardrailPolicyBindings as bindings,
  guardrailPolicyEvents as events,
  guardrailPolicyCalls as calls,
  guardrailPolicyBatches as batches,
  guardrailState,
  guardrailCounters,
  sessions,
  workspaces,
  vaults,
  sessionEventsIndex,
} from '../persistence/postgres/schema.js';
import type {
  PolicyBinding,
  StoredPolicyCall,
  StoredPolicyEvent,
  DogwoodPrepared,
} from './dogwood-types.js';
import type {
  PolicyAppendBody,
  PolicyAppendResponse,
} from '../contracts/policy-events.contract.js';
import {
  applySessionStateUpdate,
  applyCounterStateUpdate,
  loadGuardrailTurnSubject,
  parseGuardrailStateBody,
  utcDateWindow,
  utcMonthWindow,
  WORKSPACE_BUDGET_SUBJECT,
  guardrailCounterWindow,
} from './usage-accounting.js';

async function measuredEvaluation(input: Parameters<typeof evaluateDogwoodDecision>[0]) {
  const finish = policyEvaluationLatency.startTimer();
  try {
    const result = await evaluateDogwoodDecision(input);
    policyEvaluations.inc({ verdict: result.verdict });
    return result;
  } finally {
    finish();
  }
}
const retention = 24 * 60 * 60 * 1000;
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object')
    return `{${Object.entries(value)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`)
      .join(',')}}`;
  return JSON.stringify(value);
}
export function policyBundleId(
  guardrails: DogwoodPrepared[],
  scope: { workspaceId: string; sessionId: string; runtimeRevision: number },
): string {
  return `registry-${createHash('sha256').update(canonicalJson({ scope, guardrails })).digest('hex')}`;
}
function key(ws: string, id: string) {
  return and(eq(bindings.workspaceId, ws), eq(bindings.sessionId, id));
}
function callKey(ws: string, id: string, tool: string) {
  return and(eq(calls.workspaceId, ws), eq(calls.sessionId, id), eq(calls.toolUseId, tool));
}
export class PolicyLogError extends Error {
  constructor(
    readonly code:
      | 'head_conflict'
      | 'stale_epoch'
      | 'bundle_conflict'
      | 'decision_changed'
      | 'decision_expired',
    message: string,
    readonly now: string,
    readonly head: { session: string; principal: string | null },
  ) {
    super(message);
  }
}
/** Deliberate boundary status for unavailable or incompatible prepared policy context. */
export class PolicyContextError extends Error {
  constructor(
    readonly status: 403 | 404 | 409 | 503,
    message: string,
  ) {
    super(message);
    this.name = 'PolicyContextError';
  }
}

/** A longer prepared namespace owns its key even if another ID is its prefix. */
export function policyStateOwner(guardrails: readonly DogwoodPrepared[], stateKey: string) {
  let owner: DogwoodPrepared | undefined;
  for (const guardrail of guardrails)
    if (
      stateKey.startsWith(`g:${guardrail.id}:`) &&
      (!owner || guardrail.id.length > owner.id.length)
    )
      owner = guardrail;
  return owner;
}

async function activeSession(tx: DbTransaction, ws: string, id: string) {
  const [row] = await tx
    .select()
    .from(sessions)
    .innerJoin(workspaces, eq(workspaces.id, sessions.workspaceId))
    .where(
      and(
        eq(sessions.workspaceId, ws),
        eq(sessions.id, id),
        isNull(sessions.deletedAt),
        isNull(sessions.archivedAt),
        eq(workspaces.status, 'active'),
      ),
    )
    .for('no key update', { of: sessions });
  if (!row || row.sessions.status === 'terminated')
    throw new PolicyContextError(404, 'policy session unavailable');
  return row.sessions;
}
async function lockedHeads(tx: DbTransaction, ws: string, id: string, binding: PolicyBinding) {
  const stateRevision = await lockPolicyStateRevision(tx, ws);
  const principalKey = binding.principal.type === 'Vault' ? binding.principal.id : null;
  const locks = [
    ...(principalKey ? [{ scope: 'principal', subject: principalKey }] : []),
    { scope: 'session', subject: id },
  ];
  const found = [];
  for (const lock of locks) {
    await tx
      .insert(heads)
      .values({ workspaceId: ws, ...lock })
      .onConflictDoNothing();
    const [row] = await tx
      .select()
      .from(heads)
      .where(
        and(
          eq(heads.workspaceId, ws),
          eq(heads.scope, lock.scope),
          eq(heads.subject, lock.subject),
        ),
      )
      .for('update');
    found.push(row!);
  }
  const session = found.at(-1)!;
  const principal = principalKey ? found[0]! : null;
  return {
    session,
    principal,
    principalKey,
    stateRevision,
    wire: { session: session.head.toString(), principal: principal?.head.toString() ?? null },
  };
}
async function history(
  tx: DbTransaction,
  ws: string,
  id: string,
  principalKey: string | null,
  now: number,
) {
  return tx
    .select()
    .from(events)
    .where(
      and(
        eq(events.workspaceId, ws),
        principalKey ? eq(events.principalKey, principalKey) : eq(events.sessionId, id),
        gte(events.timestampMs, now - retention),
      ),
    )
    .orderBy(asc(events.timestampMs), asc(principalKey ? events.principalSeq : events.seq));
}
async function appendRows(
  tx: DbTransaction,
  ws: string,
  id: string,
  h: Awaited<ReturnType<typeof lockedHeads>>,
  rows: PolicyEvent[],
  time: number,
): Promise<StoredPolicyEvent[]> {
  const written: StoredPolicyEvent[] = [];
  for (const event of rows) {
    h.session.head++;
    if (h.principal) h.principal.head++;
    const payload = { ...event, timestamp_ms: time };
    await tx.insert(events).values({
      workspaceId: ws,
      sessionId: id,
      seq: h.session.head,
      principalKey: h.principalKey,
      principalSeq: h.principal?.head ?? null,
      timestampMs: time,
      payload,
    });
    written.push({ seq: h.session.head.toString(), event: payload });
  }
  for (const row of [h.session, ...(h.principal ? [h.principal] : [])])
    await tx
      .update(heads)
      .set({ head: row.head, lastTime: time })
      .where(
        and(eq(heads.workspaceId, ws), eq(heads.scope, row.scope), eq(heads.subject, row.subject)),
      );
  h.wire = { session: h.session.head.toString(), principal: h.principal?.head.toString() ?? null };
  return written;
}
function outcomeEvents(
  binding: PolicyBinding,
  call: PolicyCall,
  now: number,
  outcome: 'allowed' | 'denied' | 'response' | 'failed' | 'interrupted',
  output?: unknown,
) {
  // Every rule's manifest contributes only its declared result projection. Merge fields for the one shared response.
  const all = binding.guardrails
    .filter((g) => isDogwood(g.rule))
    .flatMap((g) => {
      const result = finalizeOutcome({
        guardrail_id: g.id,
        rule: g.rule as Rule,
        call,
        now_ms: now,
        outcome,
        ...(output !== undefined ? { output } : {}),
      });
      if (result.errors.length) throw new Error(result.errors.join('; '));
      return result.events;
    });
  return mergeDogwoodEvents(all);
}
export function isDogwood(rule: unknown): rule is Rule {
  return !!rule && typeof rule === 'object' && 'kind' in rule && rule.kind === 'dogwood';
}

/** Called only at a real turn/owner preparation, in the same snapshot transaction. */
export async function bindPolicyTurn(
  tx: DbTransaction,
  input: {
    workspaceId: string;
    sessionId: string;
    owner: string;
    turnEventId?: string;
    enforcer: 'harness' | 'ai-gateway';
    guardrails: DogwoodPrepared[];
    resources: { id: string; name: string }[];
    mcpServers: { name: string; url: string }[];
    now: () => Date;
  },
) {
  const ws = input.workspaceId,
    id = input.sessionId;
  const session = await activeSession(tx, ws, id);
  const [existing] = await tx.select().from(bindings).where(key(ws, id));
  const old = existing?.data as PolicyBinding | undefined;
  const turnEventId = input.turnEventId ?? old?.turnEventId ?? null;
  // An inspection refresh cannot rotate ownership or mutate the prepared policy.
  if (old && old.owner === input.owner && old.turnEventId === turnEventId)
    return { enforcer: old.enforcer, epoch: existing!.epoch.toString(), bundle_id: old.bundleId };
  const now = input.now().getTime();
  const parents = [{ type: 'Workspace' as const, id: ws }];
  let principal: PolicyBinding['principal'] = { type: 'Unattributed', id };
  if (session.vaultIds[0]) {
    const [vault] = await tx
      .select()
      .from(vaults)
      .where(
        and(
          eq(vaults.workspaceId, ws),
          eq(vaults.id, session.vaultIds[0]),
          isNull(vaults.deletedAt),
          isNull(vaults.archivedAt),
        ),
      );
    if (!vault) throw new PolicyContextError(409, 'policy principal unavailable');
    principal = {
      type: 'Vault',
      id: vault.id,
      parents,
      attributes: {
        display_name: vault.displayName,
        metadata: Object.entries(vault.metadata as Record<string, unknown>)
          .map(([k, v]) => `${k}=${String(v)}`)
          .sort(),
      },
    };
  }
  const subject = input.turnEventId
    ? await loadGuardrailTurnSubject(tx as unknown as DbClient, ws, id, input.turnEventId)
    : null;
  if (input.turnEventId && !subject)
    throw new PolicyContextError(403, 'policy turn is not Registry authenticated');
  let data: PolicyBinding = {
    bundleId: policyBundleId(input.guardrails, {
      workspaceId: ws,
      sessionId: id,
      runtimeRevision: session.runtimeRevision,
    }),
    owner: input.owner,
    turnEventId: input.turnEventId ?? null,
    subject,
    principal,
    resources: input.resources.map((r) => ({
      type: 'Agent',
      id: r.id,
      parents,
      attributes: { name: r.name },
    })),
    mcpServers: input.mcpServers,
    enforcer: input.enforcer,
    runtimeRevision: session.runtimeRevision,
    guardrails: input.guardrails,
  };
  const manifests = input.guardrails
    .filter((g) => isDogwood(g.rule))
    .flatMap((g) => (g.rule as Rule).mcpManifests ?? []);
  for (const m of manifests)
    if (
      manifests.some(
        (other) =>
          (other.server === m.server && other.url !== m.url) ||
          (other.url === m.url && other.server !== m.server),
      )
    )
      throw new PolicyContextError(
        409,
        'Dogwood MCP manifests have ambiguous canonical identities',
      );
  if (old && old.turnEventId === turnEventId) data = { ...old, owner: input.owner };
  if (old) {
    const h = await lockedHeads(tx, ws, id, old);
    if (now < Math.max(h.session.lastTime, h.principal?.lastTime ?? 0))
      throw new PolicyContextError(503, 'policy clock moved backwards');
    const unfinished = await tx
      .select()
      .from(calls)
      .where(and(eq(calls.workspaceId, ws), eq(calls.sessionId, id)));
    for (const row of unfinished) {
      const call = row.data as StoredPolicyCall;
      if (call.status !== 'authorized' && call.status !== 'asked') continue;
      if (call.status === 'authorized')
        await appendRows(tx, ws, id, h, outcomeEvents(old, call.call, now, 'interrupted'), now);
      await tx
        .update(calls)
        .set({ data: { ...call, status: 'interrupted' } })
        .where(callKey(ws, id, row.toolUseId));
    }
  }
  const epoch = (existing?.epoch ?? 0n) + 1n;
  await tx
    .insert(bindings)
    .values({ workspaceId: ws, sessionId: id, epoch, data })
    .onConflictDoUpdate({
      target: [bindings.workspaceId, bindings.sessionId],
      set: { epoch, data },
    });
  return { enforcer: input.enforcer, epoch: epoch.toString(), bundle_id: data.bundleId };
}
async function privateState(
  tx: DbTransaction,
  ws: string,
  id: string,
  b: PolicyBinding,
  now: Date,
) {
  const session = await tx
    .select()
    .from(guardrailState)
    .where(and(eq(guardrailState.workspaceId, ws), eq(guardrailState.sessionId, id)));
  const counters = await tx
    .select()
    .from(guardrailCounters)
    .where(
      and(
        eq(guardrailCounters.workspaceId, ws),
        sql`${guardrailCounters.subject} in (${b.subject ?? ''}, ${WORKSPACE_BUDGET_SUBJECT})`,
        sql`${guardrailCounters.window} in (${utcDateWindow(now)}, ${utcMonthWindow(now)})`,
      ),
    );
  const scoped = (scope: string) =>
    Object.fromEntries(
      counters
        .filter((r) => {
          if (scope === 'workspace_window')
            return r.subject === WORKSPACE_BUDGET_SUBJECT && r.window === utcMonthWindow(now);
          if (r.subject !== b.subject) return false;
          const owner = policyStateOwner(b.guardrails, r.key);
          return r.window === guardrailCounterWindow('subject_window', r.key, now, owner?.rule);
        })
        .map((r) => [r.key, r.valueNum]),
    );
  return {
    session: Object.fromEntries(session.map((r) => [r.key, r.valueNum ?? r.valueJson])),
    subject_window: scoped('subject_window'),
    workspace_window: scoped('workspace_window'),
  };
}
export async function readPolicyEvents(
  db: DbClient,
  ws: string,
  id: string,
  query: { history?: 'session' | 'principal' | undefined; since?: string | undefined },
  clock: () => Date,
  caller: string,
) {
  return db.transaction(async (tx) => {
    await activeSession(tx, ws, id);
    const [row] = await tx.select().from(bindings).where(key(ws, id));
    if (!row) throw new PolicyContextError(409, 'policy binding unavailable');
    const b = row.data as PolicyBinding;
    if (caller !== 'shared' && caller !== b.enforcer)
      throw new PolicyContextError(403, 'policy enforcer mismatch');
    const h = await lockedHeads(tx, ws, id, b),
      now = clock();
    const principal = query.history === 'principal' ? h.principalKey : null;
    const retained = await history(tx, ws, id, principal, now.getTime());
    const split = b.subject?.indexOf(':') ?? -1;
    return {
      state_revision: h.stateRevision.toString(),
      session_id: id,
      resources: b.resources,
      mcp_servers: b.mcpServers,
      ...(b.subject && split > 0
        ? { subject: { kind: b.subject.slice(0, split), id: b.subject.slice(split + 1) } }
        : {}),
      now: now.toISOString(),
      epoch: row.epoch.toString(),
      bundle_id: b.bundleId,
      core_version: '0.1.0' as const,
      head: h.wire,
      principal: b.principal,
      resource: b.resources[0]!,
      turn_event_id: b.turnEventId,
      day_window: utcDateWindow(now),
      month_window: utcMonthWindow(now),
      events: retained
        .filter((r) => (principal ? r.principalSeq! : r.seq) > BigInt(query.since ?? '0'))
        .map((r) => ({
          seq: (principal ? r.principalSeq! : r.seq).toString(),
          event: r.payload as PolicyEvent,
        })),
      guardrail_state: await privateState(tx, ws, id, b, now),
      guardrails: b.guardrails.filter((g) => isDogwood(g.rule)),
    };
  });
}
function stampedCall(
  candidate: PolicyCall,
  b: PolicyBinding,
  id: string,
  tool: string,
): PolicyCall {
  const resource = b.resources.find((r) => r.id === candidate.resource.id);
  if (!resource) throw new Error('resource is not in pinned roster');
  const mapped = canonicalTool({ harness: 'orca', tool: candidate.action });
  let mcp: PolicyCall['mcp'];
  if (mapped.is_mcp) {
    const manifestServers = b.guardrails
      .filter((g) => isDogwood(g.rule))
      .flatMap((g) => (g.rule as Rule).mcpManifests ?? []);
    const match = [...manifestServers]
      .sort((a, b) => b.server.length - a.server.length)
      .find(
        (m) =>
          mapped.action.startsWith(`mcp__${m.server}__`) &&
          b.mcpServers.some((s) => s.url === m.url),
      );
    if (!match) throw new Error('MCP action has no prepared endpoint/manifest binding');
    mcp = { server: match.server, server_host: new URL(match.url).hostname };
  }
  if (b.enforcer === 'ai-gateway' && !mcp) throw new Error('Gateway cannot govern built-in tools');
  const colon = b.subject?.indexOf(':') ?? -1;
  return {
    action: mapped.action,
    principal: b.principal,
    resource,
    request_id: tool,
    session_id: id,
    input: candidate.input,
    ...(b.subject && colon > 0
      ? { subject: { kind: b.subject.slice(0, colon), id: b.subject.slice(colon + 1) } }
      : {}),
    ...(mcp ? { mcp } : {}),
  };
}
function report(id: string, r: EvaluationResult) {
  return {
    guardrail_id: id,
    verdict: r.verdict,
    deciding_rules: r.deciding_rules.map((v) => v.rule_id),
    errors: r.errors,
  };
}
async function applyPolicyState(
  tx: DbTransaction,
  ws: string,
  id: string,
  b: PolicyBinding,
  body: PolicyAppendBody,
  now: number,
) {
  const parsed = parseGuardrailStateBody({
    updates: body.state_updates ?? [],
    ...(b.turnEventId ? { turn_event_id: b.turnEventId } : {}),
    ...(body.window ? { window: body.window } : {}),
  });
  if (!parsed.ok) throw new Error(parsed.error);
  for (const update of parsed.batch.updates) {
    const owner = policyStateOwner(b.guardrails, update.key);
    if (
      !owner ||
      isDogwood(owner.rule) ||
      update.key.slice(`g:${owner.id}:`.length).startsWith('usage.') ||
      update.key.includes('usage_')
    )
      throw new Error('policy state update is not a private prepared rule key');
    if (
      update.scope !== owner.state_scope &&
      !(owner.state_scope === undefined && update.scope === 'session')
    )
      throw new Error('policy state scope mismatch');
    if (update.scope === 'session')
      await applySessionStateUpdate(tx, ws, id, update, new Date(now));
    else {
      if (!b.subject || body.turn_event_id !== b.turnEventId)
        throw new Error('state update requires the active authenticated turn');
      const window = guardrailCounterWindow(update.scope, update.key, new Date(now), owner.rule);
      if (body.window !== window) throw new Error('state window expired');
      await applyCounterStateUpdate(
        tx,
        ws,
        update.scope === 'workspace_window' ? WORKSPACE_BUDGET_SUBJECT : b.subject,
        window,
        update,
        new Date(now),
      );
    }
  }
}
export async function appendPolicyEvents(
  db: DbClient,
  ws: string,
  id: string,
  body: PolicyAppendBody,
  clock: () => Date,
  caller: string,
): Promise<PolicyAppendResponse> {
  return db
    .transaction<PolicyAppendResponse>(async (tx) => {
      // Session and principal locking order is shared with preparation and read paths.
      await activeSession(tx, ws, id);
      const [binding] = await tx.select().from(bindings).where(key(ws, id));
      if (!binding) throw new PolicyContextError(409, 'policy binding unavailable');
      const b = binding.data as PolicyBinding;
      if (caller !== 'shared' && caller !== b.enforcer)
        throw new PolicyContextError(403, 'policy enforcer mismatch');
      const h = await lockedHeads(tx, ws, id, b);
      const digest = createHash('sha256').update(canonicalJson(body)).digest('hex');
      const fail = (code: PolicyLogError['code'], message: string): never => {
        throw new PolicyLogError(code, message, clock().toISOString(), h.wire);
      };
      const [previous] = await tx
        .select()
        .from(batches)
        .where(
          and(
            eq(batches.workspaceId, ws),
            eq(batches.sessionId, id),
            eq(batches.batchId, body.batch_id),
          ),
        );
      if (previous) {
        if (previous.digest !== digest) fail('decision_changed', 'batch payload changed');
        return {
          ...(previous.response as PolicyAppendResponse),
          disposition: 'replayed',
          now: clock().toISOString(),
          head: h.wire,
        };
      }
      if (binding.epoch.toString() !== body.epoch) fail('stale_epoch', 'policy owner changed');
      if (b.bundleId !== body.bundle_id) fail('bundle_conflict', 'prepared policy changed');
      if (canonicalJson(h.wire) !== canonicalJson(body.expected_head))
        fail('head_conflict', 'policy history changed');
      if (body.decision || body.state_updates?.length) {
        const mixed = b.guardrails.some((g) => !isDogwood(g.rule) && g.stateful);
        if (
          ((mixed || body.state_updates?.length) && body.expected_state_revision === undefined) ||
          (body.expected_state_revision !== undefined &&
            body.expected_state_revision !== h.stateRevision.toString())
        )
          fail('head_conflict', 'guardrail state changed');
      }
      const now = clock().getTime();
      const mixed = b.guardrails.some((g) => !isDogwood(g.rule) && g.stateful);
      const checkPeriods = (time: number) => {
        if ((body.decision && mixed) || body.state_updates?.length) {
          if (
            body.day_window !== utcDateWindow(new Date(time)) ||
            body.month_window !== utcMonthWindow(new Date(time))
          )
            fail('head_conflict', 'guardrail state period changed');
        }
      };
      checkPeriods(now);
      if (now < Math.max(h.session.lastTime, h.principal?.lastTime ?? 0))
        fail('decision_expired', 'policy clock moved backwards');
      const retained = await history(tx, ws, id, h.principalKey, now);
      const [stored] = await tx
        .select()
        .from(calls)
        .where(callKey(ws, id, body.tool_use_id));
      let call = stored?.data as StoredPolicyCall | undefined;
      let rows: PolicyEvent[] = [],
        validBefore: number | null = null;
      if (body.decision) {
        const decision = body.decision;
        if (call && call.status !== 'asked')
          fail('decision_changed', 'tool_use_id already recorded');
        const candidate = stampedCall(decision.call as PolicyCall, b, id, body.tool_use_id);
        if (
          call &&
          call.callDigest !== createHash('sha256').update(canonicalJson(candidate)).digest('hex')
        )
          fail('decision_changed', 'pending call changed');
        const governing = [
          ...new Map(
            b.guardrails
              .filter(
                (g) =>
                  isDogwood(g.rule) && (!g.subagent_id || g.subagent_id === candidate.resource.id),
              )
              .map((g) => [g.id, g]),
          ).values(),
        ];
        const results: { g: DogwoodPrepared; r: EvaluationResult }[] = [];
        for (const g of governing) {
          const rule = g.rule as Rule;
          const log = retained
            .filter((e) => rule.history === 'principal' || e.sessionId === id)
            .map((e) => e.payload as PolicyEvent);
          results.push({
            g,
            r: await measuredEvaluation({
              guardrail_id: g.id,
              rule,
              call: candidate,
              now_ms: now,
              history: log,
            }),
          });
        }
        if (
          canonicalJson(results.map(({ g, r }) => report(g.id, r))) !==
          canonicalJson(decision.dogwood)
        )
          fail('decision_changed', 'Dogwood result changed');
        const enforced = results.filter(({ g }) => g.mode !== 'observe');
        let minimum: 'allow' | 'ask' | 'deny' = enforced.some(
          ({ r }) => r.verdict === 'deny' || r.errors.length,
        )
          ? 'deny'
          : enforced.some(({ r }) => r.verdict === 'ask')
            ? 'ask'
            : 'allow';
        const asking = Object.fromEntries(
          enforced
            .filter(({ r }) => r.verdict === 'ask')
            .map(({ g, r }) => [g.id, r.deciding_rules.filter((v) => v.ask).map((v) => v.rule_id)]),
        );
        const approvals: PolicyEvent[] = [];
        if (decision.confirmation) {
          if (
            !call ||
            call.status !== 'asked' ||
            call.epoch !== body.epoch ||
            call.bundleId !== body.bundle_id ||
            !call.publicToolUseId ||
            decision.confirmation.tool_use_id !== call.publicToolUseId ||
            canonicalJson(call.asking) !== canonicalJson(asking)
          )
            fail('decision_changed', 'confirmation no longer names the current ask');
          const [confirmation] = await tx
            .select()
            .from(sessionEventsIndex)
            .where(
              and(
                eq(sessionEventsIndex.workspaceId, ws),
                eq(sessionEventsIndex.sessionId, id),
                eq(sessionEventsIndex.eventId, decision.confirmation.event_id),
                eq(sessionEventsIndex.kind, 'user.tool_confirmation'),
                eq(sessionEventsIndex.producedBy, 'client'),
              ),
            );
          const payload = confirmation?.payload as
            | { tool_use_id?: string; result?: string }
            | undefined;
          if (
            !confirmation?.guardrailSubject ||
            payload?.tool_use_id !== call!.publicToolUseId ||
            !['allow', 'deny'].includes(payload?.result ?? '')
          )
            fail('decision_changed', 'confirmation is not Registry authenticated');
          if (minimum === 'deny') fail('decision_changed', 'confirmation cannot override a deny');
          const approved = payload!.result === 'allow';
          for (const { g } of enforced.filter(({ r }) => r.verdict === 'ask')) {
            const finalized = finalizeOutcome({
              guardrail_id: g.id,
              rule: g.rule as Rule,
              call: candidate,
              now_ms: now,
              outcome: approved ? 'approved' : 'refused',
              asking_rule_ids: asking[g.id] ?? [],
              approver: confirmation!.guardrailSubject!,
            });
            if (finalized.errors.length) fail('decision_changed', 'approval cannot be recorded');
            approvals.push(...finalized.events.filter((e) => e.kind === 'approval'));
          }
          minimum = approved ? 'allow' : 'deny';
        }
        if (
          (minimum === 'deny' && decision.verdict !== 'deny') ||
          (minimum === 'ask' && decision.verdict === 'allow')
        )
          fail('decision_changed', 'fold cannot weaken Dogwood');
        const finite = enforced
          .map(({ r }) => r.valid_before)
          .filter((v): v is number => v !== null);
        validBefore = finite.length ? Math.min(...finite) : null;
        rows =
          decision.verdict === 'ask'
            ? []
            : mergeDogwoodEvents([
                ...approvals,
                ...outcomeEvents(
                  b,
                  candidate,
                  now,
                  decision.verdict === 'allow' ? 'allowed' : 'denied',
                ),
              ]);
        // Enforcers may send raw alternatives; identity/time fields are Registry-owned.
        const normalized = (items: PolicyEvent[]) =>
          items.map((e) => ({
            ...e,
            principal: candidate.principal,
            resource: candidate.resource,
            session_id: id,
            request_id: body.tool_use_id,
            timestamp_ms: 0,
            ...(candidate.subject ? { subject: candidate.subject } : {}),
            ...(candidate.mcp ? { mcp: candidate.mcp } : {}),
          }));
        if (
          canonicalJson(
            normalized(body.events.filter((e) => e.kind !== 'approval') as PolicyEvent[]),
          ) !== canonicalJson(normalized(rows.filter((e) => e.kind !== 'approval')))
        )
          fail('decision_changed', 'event set changed');
        call = {
          call: {
            ...candidate,
            input: outcomeEvents(b, candidate, now, 'allowed')[0]?.input ?? {},
          },
          callDigest: createHash('sha256').update(canonicalJson(candidate)).digest('hex'),
          status:
            decision.verdict === 'ask'
              ? 'asked'
              : decision.verdict === 'allow'
                ? 'authorized'
                : 'denied',
          epoch: body.epoch,
          bundleId: body.bundle_id,
          asking,
          ...(decision.public_tool_use_id ? { publicToolUseId: decision.public_tool_use_id } : {}),
          ...(call?.publicToolUseId && decision.confirmation
            ? { publicToolUseId: call.publicToolUseId }
            : {}),
        };
        if (decision.verdict === 'allow') await applyPolicyState(tx, ws, id, b, body, now);
        else if (body.state_updates?.length)
          throw new Error('non-allow policy decisions cannot mutate state');
      } else {
        if (!call || call.status !== 'authorized')
          fail('decision_changed', 'completion requires an uncompleted authorized request');
        rows = outcomeEvents(b, call!.call, now, body.outcome!, body.output);
        await applyPolicyState(tx, ws, id, b, body, now);
        call = { ...call!, status: body.outcome! };
      }
      if (body.decision) {
        const pending = await tx
          .select({ count: sql<number>`count(*)::int` })
          .from(calls)
          .where(
            and(
              eq(calls.workspaceId, ws),
              eq(calls.sessionId, id),
              sql`${calls.data}->>'status' = 'authorized'`,
            ),
          );
        if (
          retained.filter((e) => e.sessionId === id).length +
            rows.length +
            (pending[0]?.count ?? 0) +
            (body.decision.verdict === 'allow' ? 1 : 0) >
          10000
        )
          fail('decision_changed', 'policy event capacity reached');
      }
      const appended = clock().getTime();
      checkPeriods(appended);
      if (appended < now || (body.decision && validBefore !== null && appended >= validBefore))
        fail('decision_expired', 'decision validity elapsed before append');
      const written = await appendRows(tx, ws, id, h, rows, appended);
      await tx
        .insert(calls)
        .values({ workspaceId: ws, sessionId: id, toolUseId: body.tool_use_id, data: call! })
        .onConflictDoUpdate({
          target: [calls.workspaceId, calls.sessionId, calls.toolUseId],
          set: { data: call! },
        });
      const response: PolicyAppendResponse = {
        ...(body.state_updates?.length
          ? { guardrail_state: await privateState(tx, ws, id, b, new Date(appended)) }
          : {}),
        state_revision: (await lockPolicyStateRevision(tx, ws)).toString(),
        disposition: 'created',
        receipt: {
          batch_id: body.batch_id,
          tool_use_id: body.tool_use_id,
          epoch: body.epoch,
          bundle_id: b.bundleId,
          committed_at: new Date(appended).toISOString(),
          valid_before:
            body.decision && validBefore !== null ? new Date(validBefore).toISOString() : null,
        },
        now: new Date(appended).toISOString(),
        head: h.wire,
        events: written,
      };
      await tx
        .insert(batches)
        .values({ workspaceId: ws, sessionId: id, batchId: body.batch_id, digest, response });
      return response;
    })
    .then(
      (response) => {
        policyAppends.inc({ result: response.disposition });
        return response;
      },
      (error) => {
        policyAppends.inc({ result: error instanceof PolicyLogError ? error.code : 'invalid' });
        throw error;
      },
    );
}
/** Bounded timer work: callers retain ownership of scheduling and injected time. */
export async function prunePolicyEvents(
  db: DbClient,
  clock: () => Date,
  limit = 1000,
): Promise<number> {
  const result = await db.execute(
    sql`delete from guardrail_policy_events where (workspace_id,session_id,seq) in (select workspace_id,session_id,seq from guardrail_policy_events where timestamp_ms < ${clock().getTime() - retention} order by timestamp_ms limit ${Math.min(10000, Math.max(1, limit))})`,
  );
  policyPruned.inc(result.rowCount ?? 0);
  return result.rowCount ?? 0;
}
