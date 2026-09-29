// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { DEFAULT_ALLOWED_PURPOSES } from '../catalog.js';
import type { BuiltinEvaluator } from '../engine.js';
import {
  OS_SHELL_TOOLS,
  OS_TOOLS,
  OS_WRITE_TOOLS,
  SKILL_LOAD_TOOL,
  SUBAGENT_DISPATCH_TOOLS,
  matchesAnyToolPattern,
} from '../tool-names.js';
import {
  allowWith,
  ask,
  counter,
  deny,
  increment,
  numberParam,
  stringList,
  stringParam,
  toolMatches,
  toolName,
} from './helpers.js';

/**
 * Tool and argument predicates.
 *
 * Every evaluator here abstains — returns `undefined` — when it has no opinion,
 * which the engine treats as identical to allowing. Returning an explicit
 * `allow` is reserved for the case where the evaluator also wants to record
 * state, because a counter that only advances on the calls it blocks would
 * never reach its own limit.
 */

export const blockTools: BuiltinEvaluator = (ctx) => {
  const tools = stringList(ctx.params, 'tools');
  if (!toolMatches(ctx, tools)) return undefined;
  const reason = stringParam(ctx.params, 'reason');
  return deny(reason ?? `Tool ${toolName(ctx)} is blocked.`);
};

export const requireApprovalForTools: BuiltinEvaluator = (ctx) => {
  const tools = stringList(ctx.params, 'tools');
  if (!toolMatches(ctx, tools)) return undefined;
  return ask(`Tool ${toolName(ctx)} requires approval.`);
};

export const askOnOsTools: BuiltinEvaluator = (ctx) => {
  if (!toolMatches(ctx, OS_TOOLS)) return undefined;
  return ask(`${toolName(ctx)} touches the filesystem or shell and requires approval.`);
};

export const readOnlyOs: BuiltinEvaluator = (ctx) => {
  // Shell counts as mutating regardless of the command: deciding otherwise
  // would mean parsing every invocation to prove it is read-only, and a wrong
  // answer there allows a write.
  const mutating = [...OS_WRITE_TOOLS, ...OS_SHELL_TOOLS];
  if (!toolMatches(ctx, mutating)) return undefined;
  // `reason`, the name the catalog declares for this type's message.
  const reason = stringParam(ctx.params, 'reason');
  return deny(reason ?? `${toolName(ctx)} can modify the filesystem; this agent is read-only.`);
};


export const blockSkills: BuiltinEvaluator = (ctx) => {
  // Glob patterns, matched case-insensitively, as the catalog advertises —
  // `deploy-*` must block `deploy-prod`, not only an exact `deploy-*` literal.
  const blocked = stringList(ctx.params, 'blocked').map((s) => s.toLowerCase());
  if (blocked.length === 0) return undefined;
  // Only the skill-loading tool names a skill. Without this gate the rule fires
  // on any tool whose input happens to carry a matching `name` — a Drive
  // `create_file {name: "deploy"}` denied as a blocked skill.
  if (toolName(ctx) !== SKILL_LOAD_TOOL) return undefined;
  const input = ctx.event.tool?.input ?? {};
  // The current tool names the skill `skill`; older CLIs used `name`/`command`.
  // All three are safe to read now that the tool itself is pinned.
  const requested = input['skill'] ?? input['name'] ?? input['command'];
  if (typeof requested !== 'string') return undefined;
  const normalized = requested.trim().toLowerCase();
  // A skill may be named plugin-qualified (`plugin:skill`) or directory-scoped
  // (`dir:skill`) — both denote the same skill, whose bare name is the segment
  // after the last `:` or `/`. Match that as well as the full name, so
  // `deploy-*` blocks `gstack:deploy-prod`, not only a bare `deploy-prod`.
  const bareName = normalized.split(/[:/]/).pop() ?? normalized;
  if (!matchesAnyToolPattern(normalized, blocked) && !matchesAnyToolPattern(bareName, blocked)) {
    return undefined;
  }
  return deny(`Skill ${requested} is blocked.`);
};

export const headlessSubagentPurposeGuard: BuiltinEvaluator = (ctx) => {
  if (!toolMatches(ctx, SUBAGENT_DISPATCH_TOOLS)) return undefined;
  // An omitted parameter enforces the default the catalog advertises — a valid
  // guardrail must guard. An explicit empty list is an author declining to
  // constrain, and abstains.
  const allowed =
    ctx.params['allowed_purposes'] === undefined
      ? DEFAULT_ALLOWED_PURPOSES
      : stringList(ctx.params, 'allowed_purposes');
  if (allowed.length === 0) return undefined;
  const purpose = ctx.event.tool?.input?.['purpose'];
  if (typeof purpose === 'string' && allowed.includes(purpose)) return undefined;
  const reason = stringParam(ctx.params, 'deny_reason');
  return deny(reason ?? `Subagent dispatch must declare a purpose: ${allowed.join(', ')}.`);
};

const TOOL_CALL_COUNTER = 'tool_calls';

export const maxToolCallsPerSession: BuiltinEvaluator = (ctx) => {
  const limit = numberParam(ctx.params, 'limit', 100);
  const used = counter(ctx.state, TOOL_CALL_COUNTER);
  if (used >= limit) {
    // No state update on the denied call. Advancing a counter for work that
    // never happened would make the recorded total disagree with reality.
    return deny(`This session has reached its limit of ${limit} tool calls.`);
  }
  return allowWith([increment('session', TOOL_CALL_COUNTER)]);
};

const DISPATCH_COUNTER = 'dispatches';

export const spawnBounds: BuiltinEvaluator = (ctx) => {
  const dispatchTools = stringList(ctx.params, 'dispatch_tools');
  const tools = dispatchTools.length > 0 ? dispatchTools : SUBAGENT_DISPATCH_TOOLS;
  if (!toolMatches(ctx, tools)) return undefined;

  const max = numberParam(ctx.params, 'max_dispatches_per_turn', 5);
  const used = counter(ctx.state, DISPATCH_COUNTER);
  if (used >= max) {
    return deny(`This turn has already dispatched ${max} subagents.`);
  }
  // Turn scope: the cap is per turn, so the counter has to reset with it.
  return allowWith([increment('turn', DISPATCH_COUNTER)]);
};

/**
 * Patterns for data that should not reach a model.
 *
 * These are shape matchers, not validators — a string shaped like a national id
 * is treated as one. Over-matching costs an author a false positive they can
 * see and narrow; under-matching leaks silently, which nobody sees.
 */
const PII_PATTERNS: Readonly<Record<string, RegExp>> = {
  ssn: /\b\d{3}-\d{2}-\d{4}\b/,
  // 13-19 digits: PANs run to 19 (Maestro), so a 16-digit cap let the longest
  // cards through — the trailing `\b` cannot match with another digit after it.
  credit_card: /\b(?:\d[ -]*?){13,19}\b/,
  // Bounded quantifiers (a local part is ≤64 and a domain ≤255 by spec), not
  // `+`: the unbounded form backtracks quadratically over a long run of domain
  // characters with no valid TLD, and this pattern scans attacker-controlled text.
  email: /\b[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,255}\.[A-Za-z]{2,24}\b/,
  phone: /\b(?:\+?\d{1,2}[ .-]?)?\(?\d{3}\)?[ .-]?\d{3}[ .-]?\d{4}\b/,
};

const DEFAULT_PII_TYPES = Object.keys(PII_PATTERNS);

export const denyPiiInLlmRequest: BuiltinEvaluator = (ctx) => {
  const configured = stringList(ctx.params, 'pii_types');
  const types = configured.length > 0 ? configured : DEFAULT_PII_TYPES;
  const text = candidateText(
    ctx.event.userText,
    ctx.event.tool?.input,
    ctx.event.serializedRequest,
  );
  if (!text) return undefined;

  for (const type of types) {
    const pattern = PII_PATTERNS[type];
    if (pattern?.test(text)) {
      return deny(`Message appears to contain ${type.replace(/_/g, ' ')} and was blocked.`);
    }
  }
  return undefined;
};

function candidateText(
  userText: string | undefined,
  input: unknown,
  serializedRequest: string | undefined,
): string {
  // Both, not either: scanning the tool input only when there is no user text
  // let a request carry personal data in a tool argument as long as it also
  // carried any user message.
  const parts: string[] = [];
  if (typeof userText === 'string' && userText.length > 0) parts.push(userText);
  if (input && typeof input === 'object') parts.push(JSON.stringify(input));
  if (typeof serializedRequest === 'string' && serializedRequest.length > 0) {
    parts.push(serializedRequest);
  }
  return parts.join(' ');
}
