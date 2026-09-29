// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Guardrail wiring for the session loop.
//
// The runner is a client: it holds no database, no broker, and no durable state store.
// `@orca/guardrails` is built for exactly that caller — the engine drops
// stateful rules when no store is supplied, but that partial mode is NOT safe
// for a runtime promising enforcement. The runner therefore admits only
// stateless `request` rules and managed-resource `block_skills` materialization.
// Managed Codex delegates request rules to Registry durable preflight. Other
// stateful rules and unsupported phases are rejected at snapshot apply.

import {
  BUILTIN_EVALUATORS,
  SKILL_LOAD_TOOL,
  InMemoryGuardrailStateStore,
  evaluateGuardrails,
  evaluateGuardrailExpression,
  type Decision,
  type GuardrailEvent,
  type PreparedGuardrail,
} from '@orca/guardrails';
import type { RunnerSnapshot, SnapshotGuardrail } from './snapshot.js';

/** The only phase the runner can prove is enforced before side effects. */
const WIRED_PHASE = 'request';

export class GuardrailUnsupportedError extends Error {
  constructor(message: string) {
    super(`session-runner cannot enforce guardrail: ${message}`);
    this.name = 'GuardrailUnsupportedError';
  }
}

export interface PreparedRunnerGuardrails {
  prepared: PreparedGuardrail[];
  store: InMemoryGuardrailStateStore;
}

/**
 * Map the snapshot's guardrails onto the engine's shape, refusing every rule
 * this runner cannot yet enforce completely.
 *
 * `scope` is set to `explicit` for every entry: the engine does not read it —
 * tier is what orders the fold — and the snapshot does not carry it, so
 * inventing a value here would be a second source of truth for something
 * nothing consults.
 */
export function prepareRunnerGuardrails(snapshot: RunnerSnapshot): PreparedRunnerGuardrails {
  const guardrails = snapshot.guardrails ?? [];
  const delegated = snapshot.request_guardrails_owner === 'registry';
  if (
    delegated &&
    ((snapshot.provider !== 'codex-sdk' && snapshot.provider !== 'pi-sdk') ||
      !snapshot.managed_resources ||
      snapshot.multiagent)
  )
    throw new GuardrailUnsupportedError('invalid Registry request delegation');
  const unsupported = guardrails.filter(
    (guardrail) =>
      (guardrail.stateful && !delegated) ||
      guardrail.subagent_id !== undefined ||
      guardrail.phases.some(
        (phase) => phase !== WIRED_PHASE && !isManagedSkillRule(snapshot, guardrail),
      ),
  );
  if (unsupported.length > 0) {
    throw new GuardrailUnsupportedError(
      unsupported
        .map((guardrail) => {
          const reasons = [
            ...(guardrail.stateful
              ? ['stateful rules require durable Registry write-through']
              : []),
            ...(guardrail.subagent_id !== undefined
              ? ['subagent-scoped request enforcement is not wired']
              : []),
            ...guardrail.phases
              .filter((phase) => phase !== WIRED_PHASE)
              .map((phase) => `phase ${phase} is not wired`),
          ];
          return `${guardrail.name} (${guardrail.id}): ${reasons.join(', ')}`;
        })
        .join('; '),
    );
  }

  return {
    prepared: guardrails
      .filter((g) => !delegated || !g.phases.every((phase) => phase === WIRED_PHASE))
      .map(toPrepared),
    store: new InMemoryGuardrailStateStore(
      snapshot.guardrail_state ? { session: { ...snapshot.guardrail_state } } : {},
    ),
  };
}

function toPrepared(guardrail: SnapshotGuardrail): PreparedGuardrail {
  return {
    guardrail: {
      id: guardrail.id,
      name: guardrail.name,
      enabled: true,
      phases: guardrail.phases as PreparedGuardrail['guardrail']['phases'],
      scope: 'explicit',
      rule: guardrail.rule as PreparedGuardrail['guardrail']['rule'],
    },
    tier: guardrail.tier as PreparedGuardrail['tier'],
    stateful: guardrail.stateful,
    ...(guardrail.state_scope
      ? {
          stateScope: guardrail.state_scope as NonNullable<PreparedGuardrail['stateScope']>,
        }
      : {}),
    ...(guardrail.subagent_id ? { subagentId: guardrail.subagent_id } : {}),
  };
}

/**
 * Evaluate the `request` phase for a turn.
 *
 * The store carries the snapshot's restored state for evaluator consistency,
 * but stateful request enforcement belongs to Registry and is removed from
 * this local fold. No local mutation is presented as durable enforcement.
 */
export function evaluateRequestPhase(
  guardrails: PreparedGuardrail[],
  store: InMemoryGuardrailStateStore,
  event: GuardrailEvent,
): Decision {
  return evaluateGuardrails(guardrails, event, {
    builtins: BUILTIN_EVALUATORS,
    expression: evaluateGuardrailExpression,
    store,
  });
}

/** This one stateless tool rule runs before any Skill bytes become visible. */
function isManagedSkillRule(snapshot: RunnerSnapshot, guardrail: SnapshotGuardrail): boolean {
  const rule = guardrail.rule as { kind?: string; builtin?: string };
  return (
    snapshot.managed_resources !== undefined &&
    rule?.kind === 'builtin' &&
    rule.builtin === 'block_skills' &&
    guardrail.phases.every((phase) => phase === 'tool_call')
  );
}

export function managedSkillIsBlocked(
  snapshot: RunnerSnapshot,
  sessionId: string,
  name: string,
): boolean {
  const guards = (snapshot.guardrails ?? []).filter((guard) => isManagedSkillRule(snapshot, guard));
  if (guards.length === 0) return false;
  const decision = evaluateGuardrails(
    guards.map(toPrepared),
    {
      phase: 'tool_call',
      sessionId,
      tool: { name: SKILL_LOAD_TOOL, input: { skill: name } },
    },
    { builtins: BUILTIN_EVALUATORS, readOnly: true },
  );
  return decision.verdict === 'deny';
}
