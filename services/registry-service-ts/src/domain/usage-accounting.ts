// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Shared Registry usage and durable guardrail accounting. Both authenticated
// internal producers and Registry-owned runner bridges use this same state path.
import { and, eq, inArray, isNull, like, or, sql } from 'drizzle-orm';
import {
  computeCostNanoUsd,
  resolveModelPricing,
  stripDatedSnapshotSuffix,
  type ModelPriceEntry,
  type PriceSource,
} from '@orca/harness-catalog';
import { SHARED_USAGE_KEYS, STATE_ACTIONS, type StateAction } from '@orca/guardrails';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agentTriggerFires,
  agentTriggers,
  guardrailCounters,
  guardrailState,
  modelPrices,
  sessionEventsIndex,
  sessionUsageEvents,
  sessionThreads,
  sessions,
  workspaces,
} from '../persistence/postgres/schema.js';

/** One usage report's token counts, all buckets normalized to a number. */
export interface UsageDelta {
  inputTokens: number;
  outputTokens: number;
  cacheReadInputTokens: number;
  cacheCreationEphemeral1hInputTokens: number;
  cacheCreationEphemeral5mInputTokens: number;
}

export function parseUsageDelta(raw: unknown): UsageDelta | null {
  // Internal callers should emit the complete managed-agent usage shape. Missing
  // fields are treated as zero; malformed counters reject the whole delta so
  // public session usage never silently incorporates partial bad data.
  if (!raw || typeof raw !== 'object') return null;
  const usage = raw as Record<string, unknown>;
  const cacheCreation =
    usage.cache_creation && typeof usage.cache_creation === 'object'
      ? (usage.cache_creation as Record<string, unknown>)
      : {};
  const out = {
    inputTokens: numberField(usage.input_tokens),
    outputTokens: numberField(usage.output_tokens),
    cacheReadInputTokens: numberField(usage.cache_read_input_tokens),
    cacheCreationEphemeral1hInputTokens: numberField(cacheCreation.ephemeral_1h_input_tokens),
    cacheCreationEphemeral5mInputTokens: numberField(cacheCreation.ephemeral_5m_input_tokens),
  };
  return Object.values(out).every((n) => n !== null)
    ? {
        inputTokens: out.inputTokens!,
        outputTokens: out.outputTokens!,
        cacheReadInputTokens: out.cacheReadInputTokens!,
        cacheCreationEphemeral1hInputTokens: out.cacheCreationEphemeral1hInputTokens!,
        cacheCreationEphemeral5mInputTokens: out.cacheCreationEphemeral5mInputTokens!,
      }
    : null;
}

function numberField(raw: unknown): number | null {
  if (raw === undefined || raw === null) return 0;
  if (typeof raw !== 'number' || !Number.isFinite(raw) || raw < 0) return null;
  return Math.floor(raw);
}

/**
 * Exact cost of one usage delta in nano-USD, or `null` when it cannot be priced.
 *
 * `null` is *unpriced*, which is deliberately not zero. Two cases produce it,
 * and neither may be recorded as $0.00: a model with no price data, and a delta
 * that consumed nothing — an empty flush must not turn an unpriced session into
 * a priced one. A model that IS priced and consumed tokens yields a number,
 * even when that number is 0.
 */
/** Load the candidate rows for one delta and price it against them. */
export async function priceUsageDelta(
  db: DbClient,
  workspaceId: string,
  provider: string,
  modelId: string,
  usage: UsageDelta,
): Promise<bigint | null> {
  const { entries, organizationId } = await loadModelPriceEntries(
    db,
    workspaceId,
    provider,
    modelId,
  );
  return usageCostNanoUsd(usage, provider, modelId, entries, organizationId);
}

export function usageCostNanoUsd(
  usage: UsageDelta,
  provider: string,
  modelId: string,
  entries: readonly ModelPriceEntry[],
  organizationId?: string,
): bigint | null {
  if (!hasUsage(usage)) return null;

  const pricing = resolveModelPricing(provider, modelId, entries, organizationId);
  if (pricing === null) return null;

  return computeCostNanoUsd(usage, pricing);
}

const PRICE_SOURCES: ReadonlySet<string> = new Set<PriceSource>(['operator', 'upstream', 'seed']);

/** Escape the wildcards Postgres `LIKE` would otherwise read as a pattern. */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, '\\$&');
}

/**
 * The price rows a model identity could resolve against: the id itself, its
 * dated-snapshot base, and its family prefix, all within the named provider.
 * Resolution order and the ambiguity guard live in `resolveModelPricing`; this
 * only narrows the rows it has to consider. An empty result means unpriced,
 * which is a supported state.
 *
 * The organization id rides back with the rows because the library needs it to
 * admit operator overrides: an operator row reaches a price only for the
 * organization that owns it, and resolving without one would drop every
 * override on the floor rather than apply it.
 */
async function loadModelPriceEntries(
  db: DbClient,
  workspaceId: string,
  provider: string,
  modelId: string,
): Promise<{ entries: ModelPriceEntry[]; organizationId: string | undefined }> {
  const base = stripDatedSnapshotSuffix(modelId);
  const lastDash = base.lastIndexOf('-');
  const familyPrefix = lastDash > 0 ? base.slice(0, lastDash) : null;

  const candidates = [eq(modelPrices.modelId, modelId)];
  if (base !== modelId) candidates.push(eq(modelPrices.modelId, base));
  if (familyPrefix !== null) {
    candidates.push(like(modelPrices.modelId, `${escapeLikePattern(familyPrefix)}-%`));
  }

  const rows = await db
    .select({
      provider: modelPrices.provider,
      organizationId: modelPrices.organizationId,
      workspaceOrganizationId: workspaces.organizationId,
      modelId: modelPrices.modelId,
      source: modelPrices.source,
      inputPerMillionTokens: modelPrices.inputPerMillionTokens,
      outputPerMillionTokens: modelPrices.outputPerMillionTokens,
      cacheReadPerMillionTokens: modelPrices.cacheReadPerMillionTokens,
      cacheWritePerMillionTokens: modelPrices.cacheWritePerMillionTokens,
    })
    .from(modelPrices)
    .innerJoin(workspaces, eq(workspaces.id, workspaceId))
    .where(
      and(
        isNull(modelPrices.deletedAt),
        eq(modelPrices.provider, provider),
        or(...candidates),
        or(
          eq(modelPrices.organizationId, ''),
          eq(modelPrices.organizationId, workspaces.organizationId),
        ),
      ),
    );
  const entries = rows
    .filter((row): row is typeof row & { source: PriceSource } => PRICE_SOURCES.has(row.source))
    .map((row) => ({
      provider: row.provider,
      organizationId: row.organizationId,
      modelId: row.modelId,
      source: row.source,
      inputPerMillionTokens: row.inputPerMillionTokens,
      outputPerMillionTokens: row.outputPerMillionTokens,
      ...(row.cacheReadPerMillionTokens !== null
        ? { cacheReadPerMillionTokens: row.cacheReadPerMillionTokens }
        : {}),
      ...(row.cacheWritePerMillionTokens !== null
        ? { cacheWritePerMillionTokens: row.cacheWritePerMillionTokens }
        : {}),
    }));
  // Every row is joined to the same workspace, so any of them carries its
  // organization. No rows at all means nothing to narrow.
  return { entries, organizationId: rows[0]?.workspaceOrganizationId };
}

/**
 * The `SET` clause that accumulates one usage delta. Every counter is an
 * in-SQL add, so concurrent reports cannot lose each other.
 *
 * A priced delta adds exact nano-USD. A non-empty unpriced delta leaves any
 * known spend intact and sets a durable marker, so a partially priced session
 * cannot be mistaken for a completely measured one.
 */
export function usageIncrement(
  table: typeof sessions | typeof sessionThreads,
  usage: UsageDelta,
  costNanoUsd: bigint | null,
) {
  return {
    usageInputTokens: sql`${table.usageInputTokens} + ${usage.inputTokens}`,
    usageOutputTokens: sql`${table.usageOutputTokens} + ${usage.outputTokens}`,
    usageCacheReadInputTokens: sql`${table.usageCacheReadInputTokens} + ${usage.cacheReadInputTokens}`,
    usageCacheCreationEphemeral1hInputTokens: sql`${table.usageCacheCreationEphemeral1hInputTokens} + ${usage.cacheCreationEphemeral1hInputTokens}`,
    usageCacheCreationEphemeral5mInputTokens: sql`${table.usageCacheCreationEphemeral5mInputTokens} + ${usage.cacheCreationEphemeral5mInputTokens}`,
    ...(costNanoUsd === null
      ? hasUsage(usage)
        ? { usageHasUnpriced: true }
        : {}
      : {
          usageCostNanoUsd: sql`coalesce(${table.usageCostNanoUsd}, 0) + ${costNanoUsd}`,
        }),
  };
}

/**
 * Runtime-owned usage facts consumed by budget guardrails. They are persisted
 * in the same transaction as the canonical session/thread usage rows so a
 * prepare response and a live acknowledgment can never disagree about whether
 * a cap has already been crossed.
 */
export function usageGuardrailStateUpdates(
  usage: UsageDelta,
  costNanoUsd: bigint | null,
  subagentId?: string,
): GuardrailStateWrite[] {
  const updates: GuardrailStateWrite[] = [];
  const totalTokens = usageTokenTotal(usage);
  if (totalTokens > 0) {
    updates.push({
      scope: 'session',
      key: SHARED_USAGE_KEYS.totalTokens,
      action: 'increment',
      value: totalTokens,
    });
  }

  if (costNanoUsd !== null) {
    const costUsd = Number(costNanoUsd) / 1_000_000_000;
    updates.push({
      scope: 'session',
      key: SHARED_USAGE_KEYS.sessionCostUsd,
      action: 'increment',
      value: costUsd,
    });
    if (subagentId) {
      updates.push({
        scope: 'session',
        key: `${SHARED_USAGE_KEYS.subagentCostPrefix}${subagentId}`,
        action: 'increment',
        value: costUsd,
      });
    }
  } else if (totalTokens > 0) {
    updates.push({
      scope: 'session',
      key: SHARED_USAGE_KEYS.sessionHasUnpriced,
      action: 'set',
      value: true,
    });
    if (subagentId) {
      updates.push({
        scope: 'session',
        key: `${SHARED_USAGE_KEYS.subagentHasUnpricedPrefix}${subagentId}`,
        action: 'set',
        value: true,
      });
    }
  }
  return updates;
}

/**
 * The `subject_window` half of {@link usageGuardrailStateUpdates}.
 *
 * An unpriced turn still has to leave a mark. `user_daily_cost_budget` reads
 * this flag to know some of today's spend went unmeasured; without it a day
 * that began on a model with no price data is indistinguishable from a day
 * that cost nothing, and the cap fails open across sessions.
 *
 * The flag is a number, not the boolean its session-scoped twin uses:
 * `guardrail_counters.value_num` is `double precision NOT NULL`, and
 * `parseGuardrailStateBody` refuses a non-numeric `set` at this scope.
 */
export function usageCounterStateUpdates(
  usage: UsageDelta,
  costNanoUsd: bigint | null,
): GuardrailStateWrite[] {
  if (costNanoUsd !== null) {
    return [
      {
        scope: 'subject_window',
        key: SHARED_USAGE_KEYS.dailyCostUsd,
        action: 'increment',
        value: Number(costNanoUsd) / 1_000_000_000,
      },
    ];
  }
  if (usageTokenTotal(usage) === 0) return [];
  return [
    {
      scope: 'subject_window',
      key: SHARED_USAGE_KEYS.dailyCostUnpriced,
      action: 'set',
      value: 1,
    },
  ];
}

function usageTokenTotal(usage: UsageDelta): number {
  return (
    usage.inputTokens +
    usage.outputTokens +
    usage.cacheReadInputTokens +
    usage.cacheCreationEphemeral1hInputTokens +
    usage.cacheCreationEphemeral5mInputTokens
  );
}

export async function loadGuardrailUsageState(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  subagentId?: string,
): Promise<Record<string, unknown>> {
  const keys = [
    SHARED_USAGE_KEYS.totalTokens,
    SHARED_USAGE_KEYS.sessionCostUsd,
    SHARED_USAGE_KEYS.sessionHasUnpriced,
    ...(subagentId
      ? [
          `${SHARED_USAGE_KEYS.subagentCostPrefix}${subagentId}`,
          `${SHARED_USAGE_KEYS.subagentHasUnpricedPrefix}${subagentId}`,
        ]
      : []),
  ];
  const rows = await db
    .select({
      key: guardrailState.key,
      valueNum: guardrailState.valueNum,
      valueJson: guardrailState.valueJson,
    })
    .from(guardrailState)
    .where(
      and(
        eq(guardrailState.workspaceId, workspaceId),
        eq(guardrailState.sessionId, sessionId),
        inArray(guardrailState.key, keys),
      ),
    );
  return Object.fromEntries(rows.map((row) => [row.key, row.valueNum ?? row.valueJson]));
}

export async function loadGuardrailTurnSubject(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  eventId: string,
): Promise<string | null> {
  const rows = await db
    .select({ subject: sessionEventsIndex.guardrailSubject })
    .from(sessionEventsIndex)
    .where(
      and(
        eq(sessionEventsIndex.workspaceId, workspaceId),
        eq(sessionEventsIndex.sessionId, sessionId),
        eq(sessionEventsIndex.eventId, eventId),
        eq(sessionEventsIndex.producedBy, 'client'),
      ),
    )
    .limit(1);
  if (rows[0]?.subject) return rows[0].subject;

  // A cron turn has no live public request to authenticate when it fires. Its
  // subject was stamped when Registry accepted the trigger definition, then
  // resolved through the Registry-owned fire/event association here.
  const triggerRows = await db
    .select({ subject: agentTriggers.guardrailSubject })
    .from(agentTriggerFires)
    .innerJoin(
      agentTriggers,
      and(
        isNull(agentTriggers.deletedAt),
        eq(agentTriggers.workspaceId, agentTriggerFires.workspaceId),
        eq(agentTriggers.id, agentTriggerFires.triggerId),
      ),
    )
    .where(
      and(
        eq(agentTriggerFires.workspaceId, workspaceId),
        eq(agentTriggerFires.sessionId, sessionId),
        eq(agentTriggerFires.eventId, eventId),
      ),
    )
    .limit(1);
  return triggerRows[0]?.subject ?? null;
}

export async function loadGuardrailSubjectWindowState(
  db: DbClient,
  workspaceId: string,
  subject: string,
  window: string,
): Promise<Record<string, unknown>> {
  const rows = await db
    .select({ key: guardrailCounters.key, valueNum: guardrailCounters.valueNum })
    .from(guardrailCounters)
    .where(
      and(
        eq(guardrailCounters.workspaceId, workspaceId),
        eq(guardrailCounters.subject, subject),
        eq(guardrailCounters.window, window),
      ),
    );
  return Object.fromEntries(rows.map((row) => [row.key, row.valueNum]));
}

export function utcDateWindow(now: Date): string {
  return now.toISOString().slice(0, 10);
}

function hasUsage(usage: UsageDelta): boolean {
  return (
    usage.inputTokens +
      usage.outputTokens +
      usage.cacheReadInputTokens +
      usage.cacheCreationEphemeral1hInputTokens +
      usage.cacheCreationEphemeral5mInputTokens >
    0
  );
}

/** Scopes that survive the turn they were written in. `turn` is not one. */
export type PersistedStateScope = 'session' | 'subject_window';

export interface GuardrailStateWrite {
  scope: PersistedStateScope;
  key: string;
  action: StateAction;
  value?: unknown;
}

export interface GuardrailStateBatch {
  /** Non-null whenever the batch carries a `subject_window` update. */
  subject: string | null;
  window: string | null;
  updates: GuardrailStateWrite[];
}

export type GuardrailStateBodyResult =
  | { ok: true; batch: GuardrailStateBatch }
  | { ok: false; error: string };

const MAX_STATE_KEY_LENGTH = 256;
const MAX_STATE_SUBJECT_LENGTH = 256;
const MAX_STATE_WINDOW_LENGTH = 64;
const STATE_BODY_FIELDS = ['subject', 'window', 'updates'];
const STATE_UPDATE_FIELDS = ['scope', 'key', 'action', 'value'];

function isPlainObject(raw: unknown): raw is Record<string, unknown> {
  return raw !== null && typeof raw === 'object' && !Array.isArray(raw);
}

function isCountable(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * The delta an `increment` contributes, matching `@orca/guardrails`'s in-memory
 * store exactly: an absent value means one, and a value that is not a finite
 * number contributes nothing while still normalizing the key to a number.
 * Persisted and in-memory state must agree, because a prepared runtime seeds
 * one from the other.
 */
function incrementDelta(value: unknown): number {
  if (value === undefined) return 1;
  return isCountable(value) ? value : 0;
}

function bad(error: string): GuardrailStateBodyResult {
  return { ok: false, error };
}

function optionalBoundedString(
  raw: unknown,
  max: number,
): { ok: true; value: string | null } | { ok: false } {
  if (raw === undefined || raw === null) return { ok: true, value: null };
  if (typeof raw !== 'string' || raw.length === 0 || raw.length > max) return { ok: false };
  return { ok: true, value: raw };
}

/**
 * Validate a guardrail state flush. The whole batch is accepted or refused —
 * a partially applied flush would leave the caller unable to say which of its
 * counters moved, and the counters exist precisely to be trusted.
 *
 * Anything the store cannot durably represent is refused rather than dropped:
 * `turn` scope (never persisted), and non-numeric writes to the numeric
 * cross-session counter table. Silently accepting either would acknowledge a
 * write that never happened.
 */
export function parseGuardrailStateBody(raw: unknown): GuardrailStateBodyResult {
  if (!isPlainObject(raw)) return bad('invalid guardrail state payload');
  if (Object.keys(raw).some((key) => !STATE_BODY_FIELDS.includes(key))) {
    return bad('unexpected field in guardrail state payload');
  }

  const subject = optionalBoundedString(raw['subject'], MAX_STATE_SUBJECT_LENGTH);
  if (!subject.ok) return bad('invalid subject');
  const window = optionalBoundedString(raw['window'], MAX_STATE_WINDOW_LENGTH);
  if (!window.ok) return bad('invalid window');

  const rawUpdates = raw['updates'];
  if (!Array.isArray(rawUpdates)) return bad('updates must be an array');

  const updates: GuardrailStateWrite[] = [];
  for (const entry of rawUpdates) {
    if (!isPlainObject(entry)) return bad('invalid state update');
    if (Object.keys(entry).some((key) => !STATE_UPDATE_FIELDS.includes(key))) {
      return bad('unexpected field in state update');
    }

    const scope = entry['scope'];
    if (scope === 'turn') {
      return bad('turn-scoped state is never persisted and must not be flushed');
    }
    if (scope !== 'session' && scope !== 'subject_window') return bad('unknown state scope');

    const key = entry['key'];
    if (typeof key !== 'string' || key.length === 0 || key.length > MAX_STATE_KEY_LENGTH) {
      return bad('invalid state key');
    }

    const action = entry['action'];
    if (typeof action !== 'string' || !(STATE_ACTIONS as readonly string[]).includes(action)) {
      return bad('unknown state action');
    }

    const hasValue =
      Object.prototype.hasOwnProperty.call(entry, 'value') && entry['value'] !== undefined;
    const value = entry['value'];

    if (scope === 'subject_window') {
      // `guardrail_counters` holds one number per key. Refusing the writes it
      // cannot hold is what keeps a cross-session budget honest.
      if (action === 'append') return bad('subject_window counters hold numbers, not arrays');
      if (action === 'set' && hasValue && !isCountable(value)) {
        return bad('subject_window counters hold numbers');
      }
    }

    updates.push({ scope, key, action: action as StateAction, ...(hasValue ? { value } : {}) });
  }

  if (updates.some((update) => update.scope === 'subject_window')) {
    if (subject.value === null || window.value === null) {
      return bad('subject and window are required for subject_window updates');
    }
  }

  return { ok: true, batch: { subject: subject.value, window: window.value, updates } };
}

/** The transaction handle `db.transaction` hands its callback. */
export type DbTransaction = Parameters<Parameters<DbClient['transaction']>[0]>[0];

const SESSION_STATE_KEY = [
  guardrailState.workspaceId,
  guardrailState.sessionId,
  guardrailState.key,
];

/**
 * Apply one session-scoped update as an atomic delta.
 *
 * Semantics mirror `applyStateUpdate` in `@orca/guardrails`: a valueless `set`
 * removes the key, an `increment` over a non-numeric value replaces it with the
 * delta, and an `append` onto anything that is not an array replaces it with a
 * single-element one.
 */
export async function applySessionStateUpdate(
  tx: DbTransaction,
  workspaceId: string,
  sessionId: string,
  update: GuardrailStateWrite,
  now: Date,
): Promise<void> {
  const { key, action, value } = update;
  const rowKey = and(
    eq(guardrailState.workspaceId, workspaceId),
    eq(guardrailState.sessionId, sessionId),
    eq(guardrailState.key, key),
  );

  switch (action) {
    case 'delete':
      await tx.delete(guardrailState).where(rowKey);
      return;

    case 'set': {
      if (value === undefined) {
        // No value means no key, so in-memory state round trips through JSON
        // with no entry holding `undefined`.
        await tx.delete(guardrailState).where(rowKey);
        return;
      }
      const numeric = isCountable(value);
      await tx
        .insert(guardrailState)
        .values({
          workspaceId,
          sessionId,
          key,
          valueNum: numeric ? value : null,
          valueJson: numeric ? null : value,
          updatedAt: now,
        })
        .onConflictDoUpdate({
          target: SESSION_STATE_KEY,
          set: {
            valueNum: sql`excluded.value_num`,
            valueJson: sql`excluded.value_json`,
            updatedAt: now,
          },
        });
      return;
    }

    case 'increment': {
      const delta = incrementDelta(value);
      await tx
        .insert(guardrailState)
        .values({ workspaceId, sessionId, key, valueNum: delta, valueJson: null, updatedAt: now })
        .onConflictDoUpdate({
          target: SESSION_STATE_KEY,
          // The add happens in SQL. Reading the row first and writing the sum
          // back is exactly the race this table's shape exists to avoid.
          set: {
            valueNum: sql`coalesce(${guardrailState.valueNum}, 0) + ${delta}`,
            valueJson: sql`null`,
            updatedAt: now,
          },
        });
      return;
    }

    case 'append': {
      if (value === undefined) return; // nothing to append
      await tx
        .insert(guardrailState)
        .values({ workspaceId, sessionId, key, valueNum: null, valueJson: [value], updatedAt: now })
        .onConflictDoUpdate({
          target: SESSION_STATE_KEY,
          set: {
            valueJson: sql`case when jsonb_typeof(${guardrailState.valueJson}) = 'array' then ${guardrailState.valueJson} || excluded.value_json else excluded.value_json end`,
            valueNum: sql`null`,
            updatedAt: now,
          },
        });
      return;
    }

    default:
      throw new Error(`unhandled guardrail state action: ${String(action)}`);
  }
}

const COUNTER_STATE_KEY = [
  guardrailCounters.workspaceId,
  guardrailCounters.subject,
  guardrailCounters.window,
  guardrailCounters.key,
];

/**
 * Apply one cross-session counter update. The window is part of the key, so a
 * rollover starts a fresh counter without anyone having to reset the old one.
 */
export async function applyCounterStateUpdate(
  tx: DbTransaction,
  workspaceId: string,
  subject: string,
  window: string,
  update: GuardrailStateWrite,
  now: Date,
): Promise<void> {
  const { key, action, value } = update;
  const rowKey = and(
    eq(guardrailCounters.workspaceId, workspaceId),
    eq(guardrailCounters.subject, subject),
    eq(guardrailCounters.window, window),
    eq(guardrailCounters.key, key),
  );

  switch (action) {
    case 'delete':
      await tx.delete(guardrailCounters).where(rowKey);
      return;

    case 'set': {
      if (value === undefined) {
        await tx.delete(guardrailCounters).where(rowKey);
        return;
      }
      // `parseGuardrailStateBody` refuses a non-numeric value for this scope.
      await tx
        .insert(guardrailCounters)
        .values({ workspaceId, subject, window, key, valueNum: value as number, updatedAt: now })
        .onConflictDoUpdate({
          target: COUNTER_STATE_KEY,
          set: { valueNum: sql`excluded.value_num`, updatedAt: now },
        });
      return;
    }

    case 'increment': {
      const delta = incrementDelta(value);
      await tx
        .insert(guardrailCounters)
        .values({ workspaceId, subject, window, key, valueNum: delta, updatedAt: now })
        .onConflictDoUpdate({
          target: COUNTER_STATE_KEY,
          set: {
            valueNum: sql`${guardrailCounters.valueNum} + ${delta}`,
            updatedAt: now,
          },
        });
      return;
    }

    default:
      // `append` never reaches here; the parser refuses it for this scope.
      throw new Error(`unhandled guardrail counter action: ${String(action)}`);
  }
}

/** One shared transaction body for both internal producers and Registry runner accounting. */
export async function persistUsage(
  tx: DbTransaction,
  input: {
    workspaceId: string;
    sessionId: string;
    usage: UsageDelta;
    costNanoUsd: bigint | null;
    now: Date;
    threadId?: string;
    subagentId?: string;
    usageEventId?: string;
    turnSubject: string | null;
  },
) {
  const {
    workspaceId,
    sessionId: id,
    usage,
    costNanoUsd,
    now,
    threadId,
    subagentId,
    usageEventId,
    turnSubject,
  } = input;
  if (typeof threadId === 'string') {
    // Resolve the thread before writing anything so an unknown thread
    // rejects the whole report rather than recording spend it cannot
    // attribute. Archived threads still accept a final flush, for the same
    // reason terminated sessions do.
    const threads = await tx
      .select({ id: sessionThreads.id })
      .from(sessionThreads)
      .where(
        and(
          isNull(sessionThreads.deletedAt),
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, id),
          eq(sessionThreads.id, threadId),
        ),
      )
      .limit(1);
    if (threads.length === 0) return 'thread_not_found' as const;
  }

  if (typeof usageEventId === 'string') {
    const existingSessions = await tx
      .select({ id: sessions.id })
      .from(sessions)
      .where(
        and(isNull(sessions.deletedAt), eq(sessions.workspaceId, workspaceId), eq(sessions.id, id)),
      )
      .limit(1);
    if (existingSessions.length === 0) return 'session_not_found' as const;
    const inserted = await tx
      .insert(sessionUsageEvents)
      .values({ workspaceId, sessionId: id, eventId: usageEventId })
      .onConflictDoNothing()
      .returning({ eventId: sessionUsageEvents.eventId });
    if (inserted.length === 0) return 'replayed' as const;
  }

  const rows = await tx
    .update(sessions)
    .set({
      ...usageIncrement(sessions, usage, costNanoUsd),
      updatedAt: now,
    })
    // A runner's final usage flush can race with its terminal state update
    // or a public archive. Counters remain writable for any existing row;
    // hard-deleted sessions still fail closed via the zero-row result.
    .where(
      and(isNull(sessions.deletedAt), eq(sessions.workspaceId, workspaceId), eq(sessions.id, id)),
    )
    .returning({ id: sessions.id });
  if (rows.length === 0) return 'session_not_found' as const;

  if (typeof threadId === 'string') {
    await tx
      .update(sessionThreads)
      .set({
        ...usageIncrement(sessionThreads, usage, costNanoUsd),
        updatedAt: now,
      })
      .where(
        and(
          isNull(sessionThreads.deletedAt),
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, id),
          eq(sessionThreads.id, threadId),
        ),
      );
  }
  for (const update of usageGuardrailStateUpdates(
    usage,
    costNanoUsd,
    typeof subagentId === 'string' ? subagentId : undefined,
  )) {
    await applySessionStateUpdate(tx, workspaceId, id, update, now);
  }
  if (turnSubject !== null) {
    for (const update of usageCounterStateUpdates(usage, costNanoUsd)) {
      await applyCounterStateUpdate(tx, workspaceId, turnSubject, utcDateWindow(now), update, now);
    }
  }
  return 'applied' as const;
}
