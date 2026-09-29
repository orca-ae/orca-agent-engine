// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Verdict } from './lattice.js';
import type { Phase, Scope, StateScope } from './types.js';

/**
 * The builtin guardrail type catalog.
 *
 * This is the single description of what a builtin is: the phases it fires on,
 * whether it needs state, the verdicts it can produce, and a JSON Schema for its
 * parameters. Registry validates authored guardrails against these schemas and
 * serves the same entries verbatim as `guardrailtypes`, so a client that renders
 * a form from the catalog cannot construct a guardrail the server will reject.
 *
 * See `docs/managed-agents/guardrails.md` for the model.
 */

/**
 * Purposes a dispatch may declare when `headless_subagent_purpose_guard` is
 * configured without an `allowed_purposes` list. Published in the catalog and
 * enforced by the evaluator from this one definition, so the advertised
 * default and the enforced one cannot drift apart.
 */
export const DEFAULT_ALLOWED_PURPOSES: readonly string[] = [
  'implement',
  'review',
  'explore',
  'search',
];

/**
 * The subset of JSON Schema the parameter descriptions use.
 *
 * Deliberately local and small. Parameters are flat records of scalars and
 * arrays of scalars, so the whole vocabulary needed is a type, a constraint or
 * two, and a default — and a dependency-free type keeps the package pure.
 */
export interface JsonSchema {
  type?: 'object' | 'array' | 'string' | 'number' | 'integer' | 'boolean';
  description?: string;
  /** Present on `object` schemas. */
  properties?: Record<string, JsonSchema>;
  /** Property names that must be supplied. Every entry appears in `properties`. */
  required?: readonly string[];
  /** Present on `array` schemas. */
  items?: JsonSchema;
  /** Closed on every parameter object, so an unrecognised key is an authoring error. */
  additionalProperties?: boolean;
  enum?: readonly unknown[];
  default?: unknown;
  minimum?: number;
  maximum?: number;
  exclusiveMinimum?: number;
  minItems?: number;
}

export interface GuardrailTypeEntry {
  /** Wire name, as it appears in `rule.builtin`. */
  name: string;
  /** Human label for a catalog listing or a generated form. */
  title: string;
  description: string;
  /** The evaluation points this builtin fires on. */
  phases: readonly Phase[];
  /**
   * Whether evaluation reads accumulated state. Stateless builtins can run
   * anywhere, including where no state store is reachable.
   */
  stateful: boolean;
  /** Present exactly when `stateful`. Where the accumulated state lives. */
  stateScope?: StateScope;
  /** Restricts where an authored rule may attach; absent means every scope. */
  allowedScopes?: readonly Scope[];
  /**
   * The verdicts this builtin can produce. `allow` never appears: a guardrail
   * with no opinion abstains, which is already indistinguishable from allowing.
   */
  verdicts: readonly Exclude<Verdict, 'allow'>[];
  paramsSchema: JsonSchema;
  /**
   * Set when the builtin is machinery rather than something a user authors. The
   * public catalog omits these; lookup still resolves them because the engine
   * evaluates them.
   */
  internal?: boolean;
  /**
   * Names of which at least one must be supplied, checked at compile time. For a
   * type that enforces nothing until one of several parameters is set — a budget
   * with no cap and no threshold — where a flat schema cannot say "one of these".
   */
  requireAtLeastOneOf?: readonly string[];
}

/** Shorthand for a closed parameter object. */
function params(properties: Record<string, JsonSchema>, required?: readonly string[]): JsonSchema {
  return required && required.length > 0
    ? { type: 'object', properties, required, additionalProperties: false }
    : { type: 'object', properties, additionalProperties: false };
}

const stringArray = (description: string, extra: Partial<JsonSchema> = {}): JsonSchema => ({
  type: 'array',
  items: { type: 'string' },
  description,
  ...extra,
});

/** The three cost budgets share a parameter vocabulary; only the scope differs. */
function costParams(required?: readonly string[]): JsonSchema {
  return params(
    {
      max_cost_usd: {
        type: 'number',
        exclusiveMinimum: 0,
        description:
          'Hard cap in USD. At or above it the budget denies, subject to the model gate below.',
      },
      ask_thresholds_usd: {
        type: 'array',
        items: { type: 'number', exclusiveMinimum: 0 },
        description:
          'Soft thresholds in USD. Each asks once, the first time accumulated spend crosses it.',
      },
      expensive_models: stringArray(
        'Models the hard cap applies to. With a list, reaching the cap denies only while the ' +
          'session is on a matching model and allows again on a cheaper one; with no list, every ' +
          'model is blocked. An unreadable model counts as blocked.',
      ),
      on_unpriced: {
        type: 'string',
        enum: ['ask', 'deny', 'allow'],
        default: 'ask',
        description:
          'Verdict when usage cannot be priced. Ask allows one request to reach a confirmation ' +
          'and denies the next request if no approval was recorded.',
      },
    },
    required,
  );
}

const CATALOG: readonly GuardrailTypeEntry[] = [
  // ---------------------------------------------------------------- tools
  {
    name: 'tool_permission_policy',
    title: 'Tool permission policy',
    description:
      "Applies the agent's per-tool permission policy. This is the seed of the composition " +
      'fold rather than a rule anyone writes, which is what makes a permission policy and a ' +
      'guardrail one mechanism instead of two.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['ask', 'deny'],
    paramsSchema: params({}),
    internal: true,
  },
  {
    name: 'require_approval_for_tools',
    title: 'Require approval for tools',
    description:
      'Escalates the named tools to an approval prompt. The same predicate a permission ' +
      'policy expresses, but authored at a tier the agent cannot relax.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['ask'],
    paramsSchema: params(
      {
        tools: stringArray('Tool names or glob patterns that require approval before running.', {
          minItems: 1,
        }),
      },
      ['tools'],
    ),
  },
  {
    name: 'block_tools',
    title: 'Block tools',
    description: 'Denies the named tools outright.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['deny'],
    paramsSchema: params(
      {
        tools: stringArray('Tool names or glob patterns to deny.', { minItems: 1 }),
        reason: {
          type: 'string',
          description: 'Message returned to the agent in place of the tool result.',
        },
      },
      ['tools'],
    ),
  },
  {
    name: 'ask_on_os_tools',
    title: 'Ask before operating-system tools',
    description:
      'Asks for approval before any filesystem or shell tool runs, whatever its arguments.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['ask'],
    paramsSchema: params({}),
  },
  {
    name: 'read_only_os',
    title: 'Read-only operating system',
    description:
      'Denies the tools that mutate the filesystem, leaving reads and searches available.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['deny'],
    paramsSchema: params({
      reason: {
        type: 'string',
        description: 'Message returned to the agent when a mutating tool is denied.',
      },
    }),
  },
  {
    name: 'block_skills',
    title: 'Block Skills',
    description: 'Prevents the named Skills from being loaded into a session.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['deny'],
    paramsSchema: params(
      {
        blocked: stringArray('Skill names or glob patterns that may not be loaded.', {
          minItems: 1,
        }),
      },
      ['blocked'],
    ),
  },
  {
    name: 'headless_subagent_purpose_guard',
    title: 'Subagent purpose guard',
    description:
      'Requires a dispatch to declare one of the allowed purposes, so delegated work stays ' +
      'attributable rather than opaque.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['deny'],
    paramsSchema: params({
      allowed_purposes: stringArray('Purposes a dispatch may declare.', {
        default: [...DEFAULT_ALLOWED_PURPOSES],
      }),
      deny_reason: {
        type: 'string',
        description: 'Message returned to the agent when a dispatch declares no allowed purpose.',
      },
    }),
  },
  {
    name: 'deny_pii_in_llm_request',
    title: 'Deny personal data in model requests',
    description:
      'Pattern-scans a user message and outbound request metadata for personal data and denies ' +
      'before it reaches the model.',
    phases: ['request', 'llm_request'],
    stateful: false,
    verdicts: ['deny'],
    paramsSchema: params({
      pii_types: {
        type: 'array',
        items: { type: 'string', enum: ['ssn', 'credit_card', 'email', 'phone'] },
        default: ['ssn', 'credit_card', 'email', 'phone'],
        description: 'Categories of personal data to scan for.',
      },
    }),
  },
  {
    name: 'max_tool_calls_per_session',
    title: 'Maximum tool calls per session',
    description: 'Caps how many tool calls one session may make.',
    phases: ['tool_call'],
    stateful: true,
    stateScope: 'session',
    verdicts: ['deny'],
    paramsSchema: params({
      limit: {
        type: 'integer',
        minimum: 1,
        default: 100,
        description: 'Maximum tool calls allowed in the session.',
      },
    }),
  },
  {
    name: 'token_budget',
    title: 'Token budget',
    description:
      'Caps total tokens consumed by a session. Counts tokens rather than money, so it holds ' +
      'even for a model with no price data.',
    phases: ['request', 'tool_call'],
    stateful: true,
    stateScope: 'session',
    verdicts: ['ask', 'deny'],
    paramsSchema: params(
      {
        max_total_tokens: {
          type: 'integer',
          minimum: 1,
          description: 'Hard cap on tokens accumulated by the session.',
        },
        ask_thresholds: {
          type: 'array',
          items: { type: 'integer', minimum: 1 },
          description:
            'Soft thresholds in tokens. Each asks once, the first time usage crosses it.',
        },
      },
      ['max_total_tokens'],
    ),
  },
  {
    name: 'spawn_bounds',
    title: 'Subagent dispatch bounds',
    description:
      'Caps how many subagents one turn may dispatch. Turn-scoped, so the allowance is restored ' +
      'at the next turn rather than exhausted for the session.',
    phases: ['tool_call'],
    stateful: true,
    stateScope: 'turn',
    verdicts: ['deny'],
    paramsSchema: params({
      max_dispatches_per_turn: {
        type: 'integer',
        minimum: 1,
        default: 5,
        description: 'Maximum dispatches allowed within a single turn.',
      },
      dispatch_tools: stringArray(
        'Tools that count as a dispatch. Defaults to the runtime dispatch tools.',
      ),
    }),
  },
  {
    name: 'detect_loop',
    title: 'Detect repeated tool calls',
    description:
      'Detects a repeating tool-and-argument cycle within a sliding window of recent calls.',
    phases: ['tool_call'],
    stateful: true,
    stateScope: 'session',
    verdicts: ['ask', 'deny'],
    paramsSchema: params({
      threshold: {
        type: 'integer',
        minimum: 2,
        default: 3,
        description: 'How many identical calls within the window count as a loop.',
      },
      window: {
        type: 'integer',
        minimum: 1,
        default: 10,
        description: 'How many recent tool calls to consider.',
      },
      action: {
        type: 'string',
        enum: ['ask', 'deny'],
        default: 'ask',
        description: 'Verdict returned once a loop is detected.',
      },
    }),
  },
  {
    name: 'detect_thrashing',
    title: 'Detect thrashing',
    description:
      'Detects a run of consecutive failing tool results, the signal that an agent is retrying ' +
      'rather than progressing.',
    phases: ['tool_result'],
    stateful: true,
    stateScope: 'session',
    // Only `deny` here. This fires after the tool has already run, so there is
    // nothing left to approve — offering `ask` would advertise a verdict the
    // engine is obliged to degrade, and a client rendering this catalog would
    // show a choice that does not exist.
    verdicts: ['deny'],
    paramsSchema: params({
      consecutive_threshold: {
        type: 'integer',
        minimum: 1,
        default: 3,
        description: 'How many consecutive failures count as thrashing.',
      },
      window: {
        type: 'integer',
        minimum: 1,
        default: 10,
        description: 'How many recent tool results to consider.',
      },
      // No `action` parameter, deliberately. Every other detector offers a
      // choice of verdict; this one fires after the tool has already run, where
      // `deny` (suppressing the result) is the only thing left to do.
    }),
  },

  // ---------------------------------------------------------------- shell
  {
    name: 'blast_radius',
    title: 'Shell blast radius',
    description:
      'Classifies a shell command as safe, risky, or catastrophic by parsing it — unwrapping ' +
      'wrappers and splitting chained commands — rather than matching literal text, which ' +
      'nesting would bypass.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['ask', 'deny'],
    paramsSchema: params({
      gate_pushes: {
        type: 'boolean',
        default: true,
        description: 'Treat pushes to a remote as risky.',
      },
      risky_action: {
        type: 'string',
        enum: ['ask', 'deny'],
        default: 'ask',
        description:
          'Verdict for a risky command. Catastrophic commands always deny, whatever this says.',
      },
      deny_reason: {
        type: 'string',
        description: 'Message returned to the agent when a command is denied.',
      },
    }),
  },
  {
    name: 'block_working_dir_changes',
    title: 'Block working directory changes',
    description:
      'Blocks commands that move the working directory out from under the other shell ' +
      'guardrails, which would otherwise defeat any path-based rule.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['ask', 'deny'],
    paramsSchema: params({
      block_cd: {
        type: 'boolean',
        default: true,
        description: 'Block directory changes, including directory-stack and per-command forms.',
      },
      block_worktree: {
        type: 'boolean',
        default: true,
        description: 'Block worktree moves that relocate the checkout.',
      },
      allowed_dirs: stringArray('Directories a change may target despite the blocks above.'),
      action: {
        type: 'string',
        enum: ['deny', 'ask'],
        default: 'deny',
        description: 'Verdict for a blocked directory change.',
      },
    }),
  },
  {
    name: 'worktree_guard',
    title: 'Worktree guard',
    description: 'Denies writes that resolve outside an allowed root.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['deny'],
    paramsSchema: params({
      allowed_root: {
        type: 'string',
        default: '.worktrees',
        description: 'Root that writes must stay within.',
      },
      deny_reason: {
        type: 'string',
        description: 'Message returned to the agent when a write leaves the allowed root.',
      },
    }),
  },

  // --------------------------------------------------------- integrations
  {
    name: 'github_policy',
    title: 'GitHub policy',
    description:
      'Restricts repository reads and writes. Covers both the integration tools and repository ' +
      'commands issued through a shell, because covering one alone looks enforced and is not.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['ask', 'deny'],
    paramsSchema: params({
      read_all: {
        type: 'boolean',
        default: true,
        description: 'Allow reads from any repository. When false, only read_repos is readable.',
      },
      read_repos: stringArray('Repositories readable when read_all is false.'),
      write_repos: stringArray('Repositories that may be written to.'),
      write_branches: stringArray('Branch names or glob patterns that may be written to.'),
    }),
  },
  {
    name: 'gdrive_policy',
    title: 'Google Drive policy',
    description:
      'Restricts Drive access and contains confidential material: it observes results to track ' +
      'the files a session created, so writes to those stay allowed while writes that would ' +
      'copy a confidential file read earlier are denied.',
    phases: ['tool_call', 'tool_result'],
    stateful: true,
    stateScope: 'session',
    verdicts: ['ask', 'deny'],
    paramsSchema: params({
      read_all: {
        type: 'boolean',
        default: true,
        description: 'Allow reads of any file. When false, only read_files is readable.',
      },
      read_files: stringArray('Files readable when read_all is false.'),
      allow_create: {
        type: 'boolean',
        default: false,
        description: 'Allow creating new files.',
      },
      write_files: stringArray('Existing files that may be modified.'),
      comment_files: stringArray('Files that may be commented on without being modified.'),
      confidential_files: stringArray(
        'Files whose contents may not be written elsewhere once read in the session.',
      ),
      write_down_action: {
        type: 'string',
        enum: ['deny', 'ask'],
        default: 'deny',
        description: 'Verdict for a write that would move confidential content to a lower tier.',
      },
    }),
  },
  {
    name: 'gmail_policy',
    title: 'Gmail policy',
    description: 'Restricts mail access. Sending is off unless it is turned on explicitly.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['deny'],
    paramsSchema: params({
      allow_read: { type: 'boolean', default: true, description: 'Allow reading and searching.' },
      allow_send: { type: 'boolean', default: false, description: 'Allow sending mail.' },
      allow_drafts: {
        type: 'boolean',
        default: true,
        description: 'Allow creating and editing drafts.',
      },
      allow_modify: {
        type: 'boolean',
        default: false,
        description: 'Allow modifying existing mail, such as labelling or deleting.',
      },
    }),
  },
  {
    name: 'gcalendar_policy',
    title: 'Google Calendar policy',
    description: 'Restricts calendar access. Read-only unless writes are turned on explicitly.',
    phases: ['tool_call'],
    stateful: false,
    verdicts: ['deny'],
    paramsSchema: params({
      allow_read: {
        type: 'boolean',
        default: true,
        description: 'Allow reading and searching events.',
      },
      allow_create_events: {
        type: 'boolean',
        default: false,
        description: 'Allow creating events.',
      },
      allow_modify_events: {
        type: 'boolean',
        default: false,
        description: 'Allow modifying or deleting existing events.',
      },
    }),
  },

  // ----------------------------------------------------------------- cost
  {
    name: 'cost_budget',
    title: 'Session cost budget',
    description:
      'Caps what one session may spend in USD. Asks once at each soft threshold, then denies at ' +
      'the cap. By default, unpriced usage asks at a tool call and runs unmetered only after ' +
      'approval. At a request boundary it first records a pending acknowledgment and allows; ' +
      'another unapproved request is denied.',
    phases: ['request', 'tool_call'],
    stateful: true,
    stateScope: 'session',
    verdicts: ['ask', 'deny'],
    paramsSchema: costParams(),
    requireAtLeastOneOf: ['max_cost_usd', 'ask_thresholds_usd'],
  },
  {
    name: 'user_daily_cost_budget',
    title: 'Daily cost budget per principal',
    description:
      "Caps a principal's spend per UTC day across sessions. The principal is the " +
      'authenticated user when there is one and the API key otherwise, so a key-only ' +
      'deployment still gets a coherent cap rather than none.',
    phases: ['request', 'tool_call'],
    stateful: true,
    stateScope: 'subject_window',
    allowedScopes: ['workspace', 'organization'],
    verdicts: ['ask', 'deny'],
    paramsSchema: costParams(['max_cost_usd']),
  },
  {
    name: 'subagent_cost_budget',
    title: 'Subagent cost budget',
    description:
      'Caps spend attributed to one runtime-provided acting subagent identity. The managed Claude ' +
      'harness uses the persistent Agent ID, so its dispatches share a counter within the Session.',
    phases: ['request', 'tool_call'],
    stateful: true,
    stateScope: 'session',
    verdicts: ['ask', 'deny'],
    paramsSchema: costParams(),
    requireAtLeastOneOf: ['max_cost_usd', 'ask_thresholds_usd'],
  },
];

/** Every builtin, internal entries included. */
export const GUARDRAIL_CATALOG: readonly GuardrailTypeEntry[] = CATALOG;

const BY_NAME: ReadonlyMap<string, GuardrailTypeEntry> = new Map(
  CATALOG.map((entry) => [entry.name, entry]),
);

/**
 * Resolves a builtin by wire name, internal entries included — the engine has to
 * evaluate those even though nobody authors them.
 */
export function getGuardrailType(name: string): GuardrailTypeEntry | undefined {
  return BY_NAME.get(name);
}

const PUBLIC_CATALOG: readonly GuardrailTypeEntry[] = CATALOG.filter((entry) => !entry.internal);

/**
 * The public catalog: everything a user can author. This is what
 * `guardrailtypes` serves, so internal machinery stays out of it.
 */
export function listGuardrailTypes(): readonly GuardrailTypeEntry[] {
  return PUBLIC_CATALOG;
}
