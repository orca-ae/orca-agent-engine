// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import {
  canonicalTool,
  finalizeOutcome,
  readClock,
  type Rule,
  type Entity,
  type PolicyCall,
  type PolicyEvent,
  type EvaluationResult,
} from '@runorca/dogwood';
import {
  mergeDogwoodEvents,
  evaluateDogwoodDecision,
  maxVerdict,
  type StateUpdate,
  type Verdict,
  type ObservedGuardrailVerdict,
} from '@orca/guardrails';
import type { PolicyReadBody, PolicyAppendBody, PolicyAppendResponse } from '@orca/guardrails';

export interface DogwoodTransport {
  prepare(turnEventId: string): Promise<{ epoch: string; bundle_id: string } | null>;
  read(history: 'session' | 'principal'): Promise<PolicyReadBody>;
  append(body: PolicyAppendBody): Promise<PolicyAppendResponse>;
}
export class DogwoodConflict extends Error {}
export interface DogwoodDecision {
  verdict: Verdict;
  reason?: string;
  release: () => boolean;
  interrupt: () => Promise<void>;
}
interface ToolAttempt {
  active: boolean;
  group: string;
  resourceId: string;
  authorized: boolean;
  settled: Promise<void>;
  finish: () => void;
  released: boolean;
  epoch: string;
  bundle: string;
  start?: ReturnType<typeof readClock>;
  receipt?: PolicyAppendResponse['receipt'];
}
export interface DogwoodCallInput {
  group?: string;
  toolUseId: string;
  toolName: string;
  input: Record<string, unknown>;
  resourceId?: string;
  publicToolUseId: string;
  signal: AbortSignal;
  confirmation?: { event_id: string; tool_use_id: string; approved: boolean };
  other: () => {
    verdict: Verdict;
    updates: readonly StateUpdate[];
    observed?: readonly ObservedGuardrailVerdict[];
    reason?: string;
  };
}
export class DogwoodEnforcer {
  private binding: { epoch: string; bundle_id: string } | null = null;
  private attempts = new Map<string, ToolAttempt>();
  private tail: Promise<unknown> = Promise.resolve();
  private clockHighWater = 0;
  private retired = false;
  private manifestBundle: string | undefined;
  private mirrorRevision = -1n;
  private mirrorTime = -1;
  private pendingCompletions = new Map<string, PolicyAppendBody>();
  private completionEvaluators = new Map<string, () => readonly StateUpdate[]>();
  constructor(
    private readonly transport: DogwoodTransport,
    private readonly hooks: {
      mirror: (state: NonNullable<PolicyReadBody['guardrail_state']>) => void;
      applyTurn?: (updates: readonly StateUpdate[]) => void;
      inspectManifests?: (view: PolicyReadBody) => Promise<void>;
      evidence: (payload: Record<string, unknown>) => void;
      /** Must be qualified for the complete dispatch interval, independent of bundle leases. */
      clockErrorBoundMs?: number;
      clock?: typeof readClock;
    },
  ) {}
  async startTurn(turnEventId: string): Promise<void> {
    this.retired = true;
    for (const attempt of this.attempts.values()) attempt.active = false;
    this.binding = await this.transport.prepare(turnEventId);
    if (!this.binding) throw new Error('Dogwood preparation did not bind an enforcer');
    this.retired = false;
    if (this.hooks.inspectManifests && this.manifestBundle !== this.binding.bundle_id) {
      const view = await this.transport.read('session');
      this.check(view);
      await this.hooks.inspectManifests(view);
      this.manifestBundle = this.binding.bundle_id;
    }
  }
  private serial<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.tail.then(fn, fn);
    this.tail = next.catch(() => undefined);
    return next;
  }
  private clock() {
    const sample = (this.hooks.clock ?? readClock)();
    if (sample.monotonic_ms < this.clockHighWater)
      throw new Error('Dogwood monotonic clock moved backwards');
    this.clockHighWater = sample.monotonic_ms;
    return sample;
  }
  private check(binding: PolicyReadBody, completion = false) {
    if (
      (!completion && this.retired) ||
      !this.binding ||
      binding.epoch !== this.binding.epoch ||
      binding.bundle_id !== this.binding.bundle_id
    ) {
      this.retired = true;
      throw new Error('Dogwood ownership or prepared policy changed');
    }
  }
  private mirror(
    view: {
      guardrail_state?: PolicyReadBody['guardrail_state'];
      state_revision?: string | undefined;
      now: string;
    },
    strict = false,
  ) {
    if (!view.guardrail_state || view.state_revision === undefined) {
      if (strict) throw new Error('Dogwood state snapshot is incomplete');
      return;
    }
    const revision = BigInt(view.state_revision),
      time = Date.parse(view.now);
    if (!Number.isSafeInteger(time)) throw new Error('Dogwood state clock is invalid');
    if (strict && time < this.mirrorTime) throw new Error('Registry state clock moved backwards');
    if (revision < this.mirrorRevision || time < this.mirrorTime) return;
    this.hooks.mirror(view.guardrail_state);
    this.mirrorRevision = revision;
    this.mirrorTime = time;
  }
  async decide(input: DogwoodCallInput): Promise<DogwoodDecision> {
    const invocationBinding = this.binding;
    const assertInvocation = () => {
      if (!invocationBinding || this.binding !== invocationBinding || this.retired)
        throw new Error('Dogwood invocation ownership changed');
    };
    return this.serial(async () => {
      assertInvocation();
      const earlier = [...this.attempts.values()].filter(
        (a) =>
          a.authorized &&
          a.active &&
          a.group !== (input.group ?? input.toolUseId) &&
          a.resourceId === (input.resourceId ?? ''),
      );
      if (earlier.length) {
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
          await Promise.race([
            Promise.all(earlier.map((a) => a.settled)),
            new Promise((_, reject) => {
              timer = setTimeout(
                () => reject(new Error('Earlier Dogwood results remain unrecorded')),
                5000,
              );
            }),
          ]);
        } finally {
          if (timer) clearTimeout(timer);
        }
      }

      for (let retry = 0; retry < 3; retry++) {
        assertInvocation();
        const view = await this.transport.read('principal');
        assertInvocation();
        this.check(view);
        this.mirror(view, true);
        if (input.signal.aborted) throw new Error('Dogwood call cancelled');
        const resource =
          view.resources?.find((r) => r.id === input.resourceId) ??
          (input.resourceId ? undefined : view.resource);
        if (!resource) throw new Error('Dogwood acting agent is not prepared');
        const applicable = [
          ...new Map(
            view.guardrails
              .filter((g) => !g.subagent_id || g.subagent_id === resource.id)
              .map((g) => [g.id, g]),
          ).values(),
        ];
        const manifests = applicable.flatMap((g) => (g.rule as Rule).mcpManifests ?? []);
        const remote =
          input.toolName.startsWith('mcp__') && !input.toolName.startsWith('mcp__orca__');
        const server = remote
          ? view.mcp_servers
              ?.filter((s) => input.toolName.startsWith(`mcp__${s.name}__`))
              .sort((a, b) => b.name.length - a.name.length)[0]
          : undefined;
        if (remote && !server) throw new Error('Dogwood MCP endpoint is not prepared');
        const mapping = canonicalTool({
          harness: 'orca',
          tool: input.toolName,
          ...(server ? { server_url: server.url, manifests } : {}),
        });
        const manifest = server ? manifests.find((m) => m.url === server.url) : undefined;
        const subject = view.subject;
        const call: PolicyCall = {
          action: mapping.action,
          principal: view.principal as Entity,
          resource: resource as Entity,
          request_id: input.toolUseId,
          session_id: view.session_id ?? view.events[0]?.event.session_id ?? '',
          input: input.input,
          ...(subject ? { subject } : {}),
          ...(manifest
            ? { mcp: { server: manifest.server, server_host: new URL(manifest.url).hostname } }
            : {}),
        };
        if (!call.session_id) throw new Error('Dogwood session identity missing');
        const now = Date.parse(view.now),
          results: { id: string; mode: string; r: EvaluationResult; rule: Rule }[] = [];
        for (const g of applicable) {
          const rule = g.rule as Rule;
          results.push({
            id: g.id,
            mode: g.mode ?? 'enforce',
            rule,
            r: await evaluateDogwoodDecision({
              guardrail_id: g.id,
              rule,
              call,
              now_ms: now,
              history: view.events
                .filter(
                  (e) => rule.history === 'principal' || e.event.session_id === call.session_id,
                )
                .map((e) => e.event as PolicyEvent),
            }),
          });
        }
        assertInvocation();
        // Usage acknowledgments can arrive while WASM runs. The synchronous builtin fold
        // reads exactly this GET snapshot; Registry CAS then fences every later mutation.
        this.hooks.mirror(view.guardrail_state!);
        const other = input.other();
        const { updates } = other;
        let verdict = other.verdict;
        for (const { mode, r } of results) {
          if (mode !== 'observe' && r.verdict !== 'abstain')
            verdict = maxVerdict(verdict, r.errors.length ? 'deny' : r.verdict);
        }
        // Gateway has no confirmation path. Harness approvals apply only to a current persisted ask.
        if (input.confirmation) {
          if (verdict === 'ask') verdict = input.confirmation.approved ? 'allow' : 'deny';
          else if (verdict === 'allow' && !input.confirmation.approved) verdict = 'deny';
        }
        const evidence = {
          phase: 'tool_call',
          tool_use_id: input.publicToolUseId,
          decision: verdict,
          enforcer: 'harness',
          guardrails: [
            ...(other.observed ?? []).map((row) => ({
              guardrail_id: row.guardrailId,
              mode: row.mode,
              verdict: row.verdict,
              ...(row.reason ? { reason: row.reason } : {}),
            })),
            ...results.map(({ id, mode, r }) => ({
              guardrail_id: id,
              mode,
              verdict: r.verdict === 'abstain' ? 'allow' : r.verdict,
              deciding_rules: r.deciding_rules.map((v) => v.rule_id),
              errors: r.errors.slice(0, 100).map(
                (e) =>
                  e
                    .split('')
                    .map((c) => (c.charCodeAt(0) < 32 || c.charCodeAt(0) === 127 ? ' ' : c))
                    .join('')
                    .trim()
                    .slice(0, 1024) || 'Dogwood evaluation error',
              ),
            })),
          ],
        };
        const outcome = verdict === 'allow' ? 'allowed' : 'denied';
        const alternatives =
          verdict === 'ask'
            ? []
            : results.flatMap(({ id, rule }) => {
                const result = finalizeOutcome({
                  guardrail_id: id,
                  rule,
                  call,
                  now_ms: now,
                  outcome,
                });
                if (result.errors.length) throw new Error('Dogwood event projection failed');
                return result.events;
              });
        const rows = mergeDogwoodEvents(alternatives);
        const persistent = verdict === 'allow' ? updates.filter((u) => u.scope !== 'turn') : [];
        const window = persistent.some(
          (u) => u.scope === 'workspace_window' || u.key.includes('monthly'),
        )
          ? view.month_window
          : view.day_window;
        const body: PolicyAppendBody = {
          batch_id: randomUUID(),
          tool_use_id: input.toolUseId,
          ...(view.state_revision ? { expected_state_revision: view.state_revision } : {}),
          day_window: view.day_window,
          month_window: view.month_window,
          expected_head: view.head,
          epoch: view.epoch,
          bundle_id: view.bundle_id,
          decision: {
            call,
            verdict,
            dogwood: results.map(({ id, r }) => ({
              guardrail_id: id,
              verdict: r.verdict,
              deciding_rules: r.deciding_rules.map((v) => v.rule_id),
              errors: r.errors,
            })),
            public_tool_use_id: input.publicToolUseId,
            ...(input.confirmation
              ? {
                  confirmation: {
                    event_id: input.confirmation.event_id,
                    tool_use_id: input.confirmation.tool_use_id,
                  },
                }
              : {}),
          },
          events: rows,
          state_updates: persistent as PolicyAppendBody['state_updates'],
          ...(view.turn_event_id ? { turn_event_id: view.turn_event_id } : {}),
          window,
        };
        let finish!: () => void;
        const settled = new Promise<void>((resolve) => {
          finish = resolve;
        });
        const attempt: ToolAttempt = {
          group: input.group ?? input.toolUseId,
          resourceId: input.resourceId ?? '',
          authorized: verdict === 'allow',
          settled,
          finish,
          active: true,
          released: false,
          epoch: view.epoch,
          bundle: view.bundle_id,
        };
        try {
          attempt.start = this.clock();
        } catch {
          /* A null receipt requires no qualified elapsed clock. */
        }
        this.attempts.set(input.toolUseId, attempt);
        input.signal.addEventListener(
          'abort',
          () => {
            attempt.active = false;
          },
          { once: true },
        );
        let response: PolicyAppendResponse;
        try {
          response = await this.transport.append(body);
        } catch (error) {
          attempt.active = false;
          if (error instanceof DogwoodConflict && !input.confirmation) continue;
          if (!(error instanceof DogwoodConflict)) await this.reconcile(body, input.toolUseId);
          throw error;
        }
        attempt.receipt = response.receipt;
        if (verdict === 'allow' && response.disposition === 'created')
          this.hooks.applyTurn?.(updates.filter((u) => u.scope === 'turn'));
        if (!input.confirmation) this.hooks.evidence(evidence);
        if (
          response.receipt.epoch === this.binding?.epoch &&
          response.receipt.bundle_id === this.binding?.bundle_id &&
          response.guardrail_state
        )
          this.mirror(response);
        else if (persistent.length) {
          attempt.active = false;
          await this.complete(input.toolUseId, 'interrupted');
          throw new Error('Dogwood acknowledgment omitted authoritative state');
        }
        const release = () => {
          if (
            verdict !== 'allow' ||
            response.disposition !== 'created' ||
            !attempt.active ||
            attempt.released ||
            input.signal.aborted ||
            this.retired ||
            this.binding?.epoch !== attempt.epoch ||
            this.binding?.bundle_id !== attempt.bundle
          )
            return false;
          const receipt = attempt.receipt!;
          if (
            receipt.tool_use_id !== input.toolUseId ||
            receipt.batch_id !== body.batch_id ||
            receipt.epoch !== attempt.epoch ||
            receipt.bundle_id !== attempt.bundle
          )
            return false;
          if (receipt.valid_before !== null) {
            const u = this.hooks.clockErrorBoundMs,
              start = attempt.start;
            if (u === undefined || !Number.isFinite(u) || u < 0 || !start) return false;
            try {
              const current = this.clock();
              if (current.boot_id !== start.boot_id || current.epoch_id !== start.epoch_id)
                return false;
              const interval = Date.parse(receipt.valid_before) - Date.parse(receipt.committed_at);
              if (
                !Number.isFinite(interval) ||
                current.monotonic_ms >= start.monotonic_ms + interval - u
              )
                return false;
            } catch {
              return false;
            }
          }
          attempt.released = true;
          return true;
        };
        if (response.disposition === 'replayed') {
          attempt.active = false;
          await this.reconcile(body, input.toolUseId);
          throw new Error('Historical Dogwood receipt cannot release a tool');
        }
        return {
          verdict,
          reason:
            results
              .flatMap((v) =>
                v.r.deciding_rules
                  .filter((r) => r.effect === 'forbid')
                  .map((r) => r.description ?? ''),
              )
              .filter(Boolean)
              .join(' ') ||
            other.reason ||
            'Tool blocked by managed policy.',
          release,
          interrupt: async () => {
            attempt.active = false;
            await this.complete(input.toolUseId, 'interrupted');
          },
        };
      }
      throw new Error('Dogwood history remained contended');
    });
  }
  private async reconcile(body: PolicyAppendBody, tool: string) {
    // Invalidate before the first retry: a late original created response cannot revive release.
    const attempt = this.attempts.get(tool);
    if (attempt) attempt.active = false;
    for (let n = 0; n < 2; n++)
      try {
        await this.transport.append(body);
        await this.complete(tool, 'interrupted');
        return;
      } catch (error) {
        if (error instanceof DogwoodConflict) return;
      }
  }
  private async sendCompletion(body: PolicyAppendBody): Promise<PolicyAppendResponse> {
    let failure: unknown;
    for (let n = 0; n < 3; n++)
      try {
        return await this.transport.append(body);
      } catch (error) {
        if (error instanceof DogwoodConflict) throw error;
        failure = error;
      }
    throw failure;
  }
  async complete(
    tool: string,
    outcome: 'response' | 'failed' | 'interrupted',
    output?: unknown,
    other?: () => readonly StateUpdate[],
  ) {
    if (other) this.completionEvaluators.set(tool, other);
    for (let retry = 0; retry < 3; retry++) {
      const view = await this.transport.read('session');
      this.check(view, true);
      this.mirror(view, true);
      const prior = view.events.filter((e) => e.event.request_id === tool),
        attempt = this.attempts.get(tool);
      const finished = prior.some(
        (e) =>
          e.event.kind === 'response' ||
          (e.event.kind === 'error' && e.event.error_type !== 'denied'),
      );
      if (finished) {
        this.pendingCompletions.delete(tool);
        this.completionEvaluators.delete(tool);
        if (attempt) {
          attempt.active = false;
          attempt.finish();
        }
        return;
      }
      if (!prior.some((e) => e.event.kind === 'request') && !attempt?.authorized) return;
      let body = this.pendingCompletions.get(tool);
      if (!body) {
        const request = prior.find((e) => e.event.kind === 'request')?.event;
        let projectedOutput: unknown;
        if (outcome === 'response' && request) {
          const projected = mergeDogwoodEvents(
            view.guardrails.flatMap((g) => {
              const result = finalizeOutcome({
                guardrail_id: g.id,
                rule: g.rule as Rule,
                call: request,
                now_ms: Date.parse(view.now),
                outcome,
                output,
              });
              if (result.errors.length) throw new Error('Dogwood result projection failed');
              return result.events;
            }),
          );
          if (projected[0]?.output) projectedOutput = { structuredContent: projected[0].output };
        }
        const updates = (this.completionEvaluators.get(tool)?.() ?? []).filter(
          (u) => u.scope !== 'turn',
        ) as PolicyAppendBody['state_updates'];
        body = {
          day_window: view.day_window,
          month_window: view.month_window,
          batch_id: `${tool}:${outcome}`,
          tool_use_id: tool,
          expected_head: view.head,
          expected_state_revision: view.state_revision,
          epoch: view.epoch,
          bundle_id: view.bundle_id,
          events: [],
          outcome,
          ...(projectedOutput !== undefined ? { output: projectedOutput } : {}),
          state_updates: updates,
          ...(view.turn_event_id ? { turn_event_id: view.turn_event_id } : {}),
          window: updates?.some((u) => u.scope === 'workspace_window' || u.key.includes('monthly'))
            ? view.month_window
            : view.day_window,
        };
        this.pendingCompletions.set(tool, body);
      }
      try {
        const response = await this.sendCompletion(body);
        if (
          response.receipt.epoch === this.binding?.epoch &&
          response.receipt.bundle_id === this.binding?.bundle_id
        )
          this.mirror(response);
        this.pendingCompletions.delete(tool);
        this.completionEvaluators.delete(tool);
        if (attempt) {
          attempt.active = false;
          attempt.finish();
        }
        return;
      } catch (error) {
        if (!(error instanceof DogwoodConflict)) throw error;
        this.pendingCompletions.delete(tool);
      }
    }
    throw new Error('Dogwood completion remains pending');
  }
  async stop() {
    this.retired = true;
    for (const a of this.attempts.values()) {
      a.active = false;
      a.finish();
    }
    await Promise.allSettled(
      [...this.attempts]
        .filter(([, a]) => a.authorized && a.receipt)
        .map(([id]) => {
          const pending = this.pendingCompletions.get(id);
          return this.complete(id, pending?.outcome ?? 'interrupted', pending?.output);
        }),
    );
  }
}
