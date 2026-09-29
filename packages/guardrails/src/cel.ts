// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { celEnv, isCelError, parse, plan } from '@bufbuild/cel';
import type { GuardrailEvent, UsageSnapshot } from './types.js';

/**
 * Expression rules.
 *
 * An expression guardrail is the escape hatch from the builtin catalog: a
 * predicate over the event, written in CEL, evaluated with no ability to reach
 * the host. Three properties are load-bearing and are all established here.
 *
 * 1. **Compilation happens when the rule is written.** `compileExpression`
 *    parses, validates, and plans up front so the API can answer 400 to a bad
 *    expression. Nothing about an expression is discovered for the first time
 *    mid-session, where the only options left are to deny or to let it through.
 *
 * 2. **Statefulness is derived, not declared.** It is read off the parse tree,
 *    so an author cannot mislabel a rule that reads state as stateless and have
 *    it evaluated in the pass that never loads state.
 *
 * 3. **A non-boolean result is a failure.** A guardrail expression answers a
 *    yes/no question; anything else means the rule does not say what its author
 *    thought it said. `evaluate` throws rather than coercing, which leaves the
 *    fail-open/fail-closed choice with the engine, where the phase is known.
 *
 * See `docs/managed-agents/guardrails.md`, "Rules" and "Stateless and stateful
 * guardrails".
 */

/**
 * Cap on expression source length. Bounds parse cost and the size of any
 * literal collection an author can write into a comprehension.
 */
export const MAX_EXPRESSION_LENGTH = 4096;

/**
 * Cap on parse-tree depth. Source length does not bound it — `1+1+…+1` is a
 * flat string and a tree thousands deep — and analysis recurses per level, so
 * an unbounded tree overflows the stack at authoring time. The cap is far above
 * any hand-written predicate and is checked before analysis, so a pathological
 * tree is a 400 rather than a thrown `RangeError`.
 */
export const MAX_AST_DEPTH = 128;

/**
 * Nested comprehensions are refused outright. Each one iterates a whole
 * collection, so nesting multiplies — and the collection can be `event`-derived
 * data the model controls, so the exponent is unbounded at authoring time no
 * matter how shallow the nest: a depth-2 nest over a model-supplied N-element
 * list is N², which reaches seconds of event-loop-blocking work the
 * non-preemptive evaluator cannot interrupt. A single comprehension is linear in
 * its collection and stays allowed; anything deeper is rejected, so cost is
 * bounded by the input size rather than a power of it.
 */
export const MAX_COMPREHENSION_DEPTH = 1;

/** Default per-evaluation time budget. See `evaluate` for what it guarantees. */
export const DEFAULT_EVAL_TIMEOUT_MS = 50;

/** The single variable an expression may reference. */
export const ACTIVATION_ROOT = 'event';

/**
 * The top-level activation fields. An expression that selects anything else off
 * the root is rejected at compile time — that is the "references an unknown
 * field" rejection the API contract promises.
 *
 * Only the first level is checkable. Everything below it (`event.tool.input.*`,
 * `event.usage.*`, `event.state.*`) is data whose keys depend on the tool, the
 * model, and what the guardrail itself has written, so it cannot be known at
 * authoring time.
 */
export const ACTIVATION_FIELDS = [
  'phase',
  'tool',
  'result',
  'session',
  'usage',
  'model',
  'state',
] as const;

const ACTIVATION_FIELD_SET: ReadonlySet<string> = new Set<string>(ACTIVATION_FIELDS);

const STATE_FIELD = 'state';

/** CEL's index operator, `a[b]`, as it appears in the parse tree. */
const INDEX_OPERATOR = '_[_]';

/** Why an evaluation did not produce a verdict. */
export type EvaluationFailureKind =
  /** The expression errored: a missing field, a bad conversion, a type mismatch. */
  | 'runtime'
  /** The expression completed but did not answer the yes/no question. */
  | 'non_boolean'
  /** The evaluation ran past its time budget. */
  | 'timeout';

/**
 * Raised when an expression does not yield a verdict. The engine catches this
 * and applies the phase's fail mode; this module deliberately does not decide
 * whether a failure allows or denies.
 */
export class ExpressionEvaluationError extends Error {
  override readonly name = 'ExpressionEvaluationError';

  readonly kind: EvaluationFailureKind;

  constructor(kind: EvaluationFailureKind, message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.kind = kind;
  }
}

export interface CompiledExpression {
  /** Evaluate against an activation. Returns the boolean result. */
  evaluate(activation: Record<string, unknown>, opts?: { timeoutMs?: number }): boolean;
  /** True when the expression reads any `event.state.*` field. */
  stateful: boolean;
  /**
   * The `event.state.*` keys it reads, if determinable.
   *
   * Empty with `stateful: true` means the expression reads state in a way whose
   * keys cannot be resolved statically — a computed index, or the whole map at
   * once. A caller that prefetches state must read all of it in that case.
   */
  stateKeys: readonly string[];
}

export interface CompileError {
  message: string;
}

export type CompileResult =
  | { ok: true; compiled: CompiledExpression }
  | { ok: false; error: CompileError };

/**
 * The evaluation environment: the CEL standard library and nothing else. It
 * holds no per-expression state, so one instance serves every compilation.
 */
const ENV = celEnv();

/** Compile at authoring time. Never throws — returns a discriminated result. */
export function compileExpression(source: string): CompileResult {
  if (typeof source !== 'string' || source.trim().length === 0) {
    return failed('expression is empty');
  }
  if (source.length > MAX_EXPRESSION_LENGTH) {
    return failed(
      `expression is ${source.length} characters, over the ${MAX_EXPRESSION_LENGTH} character limit`,
    );
  }

  let parsed: ReturnType<typeof parse>;
  try {
    parsed = parse(source);
  } catch (error) {
    return failed(messageOf(error));
  }

  const root = parsed.expr as unknown as AstNode | undefined;

  // Bound the tree before analysing it: the bounds walk stops at the cap, so it
  // cannot overflow, and a rejection here means analysis never recurses into a
  // tree deep enough to throw.
  const bounds = boundsRejection(root, 0, 0);
  if (bounds !== undefined) {
    return failed(bounds);
  }

  let analysis: Analysis;
  try {
    analysis = analyze(root);
  } catch (error) {
    // Analysis is total for every tree the bounds check admits; a throw here
    // means the parse tree took a shape it does not model, which is a refusal,
    // not a crash of the authoring endpoint.
    return failed(messageOf(error));
  }
  if (analysis.rejection !== undefined) {
    return failed(analysis.rejection);
  }

  let planned: PlannedEvaluator;
  try {
    // The planner types its bindings against a declared variable set. We declare
    // none and bind a single dynamic root instead, so the activation is typed at
    // this boundary rather than by the environment.
    planned = plan(ENV, parsed) as PlannedEvaluator;
  } catch (error) {
    return failed(messageOf(error));
  }

  const stateKeys: readonly string[] = analysis.keysComplete ? [...analysis.stateKeys] : [];

  return {
    ok: true,
    compiled: {
      stateful: analysis.stateful,
      stateKeys,
      evaluate: (activation, opts) => evaluatePlanned(planned, activation, opts?.timeoutMs),
    },
  };
}

type PlannedEvaluator = (bindings: Record<string, unknown>) => unknown;

/**
 * What the time budget does and does not guarantee.
 *
 * The CEL evaluator used here runs synchronously and exposes no interrupt, step
 * limit, or cancellation hook. Nothing in this process can preempt an
 * evaluation once it has started, so the budget is **detection, not
 * preemption**: elapsed time is measured and an overrun is reported as a
 * failure the engine can act on, but the work has already been done by then.
 *
 * That is a usable guarantee because CEL is total. It has no recursion and no
 * unbounded loops — comprehensions iterate a collection that is already in
 * memory — so an evaluation terminates. The exposure is a slow evaluation, not
 * a hung one, and the two ways to be slow are a comprehension over a large
 * value from the event and a `matches()` pattern that backtracks. An overrun
 * therefore shows up as a `timeout` failure on the call that caused it, and a
 * rule that keeps overrunning is visible rather than silent.
 */
function evaluatePlanned(
  planned: PlannedEvaluator,
  activation: Record<string, unknown>,
  timeoutMs: number = DEFAULT_EVAL_TIMEOUT_MS,
): boolean {
  const startedAt = performance.now();
  let result: unknown;
  try {
    result = planned(activation);
  } catch (error) {
    throw new ExpressionEvaluationError(
      'runtime',
      `expression evaluation failed: ${messageOf(error)}`,
      {
        cause: error,
      },
    );
  }
  const elapsedMs = performance.now() - startedAt;

  // Checked before the result: an evaluation that blew its budget is unusable
  // whatever it returned, and the overrun is the fact worth surfacing.
  if (elapsedMs > timeoutMs) {
    throw new ExpressionEvaluationError(
      'timeout',
      `expression evaluation took ${elapsedMs.toFixed(1)}ms, over the ${timeoutMs}ms budget`,
    );
  }

  if (isCelError(result)) {
    throw new ExpressionEvaluationError(
      'runtime',
      `expression evaluation failed: ${result.message}`,
      {
        cause: result,
      },
    );
  }
  if (typeof result !== 'boolean') {
    throw new ExpressionEvaluationError(
      'non_boolean',
      `expression produced ${describeValue(result)}; a guardrail expression must produce a boolean`,
    );
  }
  return result;
}

/** Build the activation object from an event. */
export function buildActivation(
  event: GuardrailEvent,
  state?: Readonly<Record<string, unknown>>,
): Record<string, unknown> {
  const session: Record<string, unknown> = { id: event.sessionId };
  if (event.agentId !== undefined) {
    session['agent_id'] = event.agentId;
  }
  if (event.turnIndex !== undefined) {
    session['turn_index'] = event.turnIndex;
  }

  const activation: Record<string, unknown> = {
    phase: event.phase,
    session,
    state: { ...state },
  };
  if (event.tool !== undefined) {
    activation['tool'] = { name: event.tool.name, input: event.tool.input };
  }
  if (event.result !== undefined) {
    activation['result'] = event.result;
  }
  if (event.usage !== undefined) {
    activation['usage'] = usageActivation(event.usage);
  }
  if (event.modelId !== undefined) {
    activation['model'] = { id: event.modelId };
  }

  return { [ACTIVATION_ROOT]: activation };
}

/**
 * The activation is the wire-facing view of the event, so it uses the API's
 * snake_case names rather than the library's camelCase ones.
 *
 * Fields the runtime could not measure are omitted rather than defaulted to
 * zero. Absent cost is not zero cost, and a budget expression that reads
 * `event.usage.total_cost_usd` on an unpriced model should error — which the
 * engine's fail mode turns into a refusal — instead of quietly passing at $0.
 */
const USAGE_FIELDS: readonly (readonly [keyof UsageSnapshot, string])[] = [
  ['inputTokens', 'input_tokens'],
  ['outputTokens', 'output_tokens'],
  ['cacheReadInputTokens', 'cache_read_input_tokens'],
  ['cacheCreationEphemeral5mInputTokens', 'cache_creation_ephemeral_5m_input_tokens'],
  ['cacheCreationEphemeral1hInputTokens', 'cache_creation_ephemeral_1h_input_tokens'],
  ['totalTokens', 'total_tokens'],
  ['totalCostUsd', 'total_cost_usd'],
];

function usageActivation(usage: UsageSnapshot): Record<string, unknown> {
  const exposed: Record<string, unknown> = {};
  for (const [field, name] of USAGE_FIELDS) {
    const value = usage[field];
    if (value !== undefined) {
      exposed[name] = value;
    }
  }
  return exposed;
}

/* -------------------------------------------------------------------------- */
/* Parse-tree analysis                                                        */
/* -------------------------------------------------------------------------- */

/**
 * A structural view of the CEL parse tree.
 *
 * These mirror `cel.expr.Expr`. They are declared locally rather than imported
 * so this package depends on the evaluator alone and not on the schema package
 * the evaluator happens to pin. The cost of that choice is that a change to the
 * tree's shape would not be a type error, so `visit` validates every node it
 * touches and rejects anything it does not recognise. Drift therefore surfaces
 * as an expression the API refuses to accept, never as an expression that reads
 * state while reporting itself stateless.
 */
interface AstNode {
  readonly exprKind: AstKind;
}

type AstKind =
  | { readonly case: 'constExpr'; readonly value: AstConstant }
  | { readonly case: 'identExpr'; readonly value: AstIdent }
  | { readonly case: 'selectExpr'; readonly value: AstSelect }
  | { readonly case: 'callExpr'; readonly value: AstCall }
  | { readonly case: 'listExpr'; readonly value: AstList }
  | { readonly case: 'structExpr'; readonly value: AstStruct }
  | { readonly case: 'comprehensionExpr'; readonly value: AstComprehension }
  | { readonly case: undefined; readonly value?: undefined };

interface AstConstant {
  readonly constantKind?: { readonly case?: string; readonly value?: unknown };
}
interface AstIdent {
  readonly name?: unknown;
}
interface AstSelect {
  readonly operand?: AstNode;
  readonly field?: unknown;
  readonly testOnly?: unknown;
}
interface AstCall {
  readonly target?: AstNode;
  readonly function?: unknown;
  readonly args?: readonly AstNode[];
}
interface AstList {
  readonly elements?: readonly AstNode[];
}
interface AstStruct {
  readonly entries?: readonly {
    readonly keyKind?: { readonly case?: string; readonly value?: unknown };
    readonly value?: AstNode;
  }[];
}
interface AstComprehension {
  readonly iterVar?: unknown;
  readonly iterVar2?: unknown;
  readonly accuVar?: unknown;
  readonly iterRange?: AstNode;
  readonly accuInit?: AstNode;
  readonly loopCondition?: AstNode;
  readonly loopStep?: AstNode;
  readonly result?: AstNode;
}

interface Analysis {
  readonly stateful: boolean;
  readonly stateKeys: ReadonlySet<string>;
  /** False when at least one state read could not be resolved to a literal key. */
  readonly keysComplete: boolean;
  /** Set when the expression must be refused; the message explains why. */
  readonly rejection: string | undefined;
}

/** The child nodes of one AST node, for a depth walk that does not interpret them. */
function astChildren(node: AstNode): (AstNode | undefined)[] {
  const kind = node.exprKind as AstKind | undefined;
  switch (kind?.case) {
    case 'selectExpr':
      return [kind.value.operand];
    case 'callExpr':
      return [kind.value.target, ...(kind.value.args ?? [])];
    case 'listExpr':
      return [...(kind.value.elements ?? [])];
    case 'structExpr':
      return (kind.value.entries ?? []).flatMap((entry) => [
        entry.keyKind?.case === 'mapKey' ? (entry.keyKind.value as AstNode | undefined) : undefined,
        entry.value,
      ]);
    case 'comprehensionExpr': {
      const c = kind.value;
      return [c.iterRange, c.accuInit, c.loopCondition, c.loopStep, c.result];
    }
    default:
      return [];
  }
}

/**
 * Refuse a tree that is too deep or nests too many comprehensions, before
 * anything recurses into it. The walk itself returns at `MAX_AST_DEPTH`, so it
 * bounds its own recursion — a 2000-deep tree is refused at depth 129, never
 * walked to the bottom.
 */
function boundsRejection(
  node: AstNode | undefined,
  depth: number,
  comprehensions: number,
): string | undefined {
  if (node === undefined) return undefined;
  if (depth > MAX_AST_DEPTH) {
    return `expression nests more than ${MAX_AST_DEPTH} levels deep`;
  }
  const here =
    (node.exprKind as AstKind | undefined)?.case === 'comprehensionExpr'
      ? comprehensions + 1
      : comprehensions;
  if (here > MAX_COMPREHENSION_DEPTH) {
    return `expression nests more than ${MAX_COMPREHENSION_DEPTH} comprehensions, whose evaluation cost multiplies`;
  }
  for (const child of astChildren(node)) {
    const rejection = boundsRejection(child, depth + 1, here);
    if (rejection !== undefined) return rejection;
  }
  return undefined;
}

function analyze(root: AstNode | undefined): Analysis {
  if (root === undefined) {
    return {
      stateful: false,
      stateKeys: EMPTY_SCOPE,
      keysComplete: true,
      rejection: 'expression is empty',
    };
  }
  const analyzer = new Analyzer();
  analyzer.visit(root, EMPTY_SCOPE);
  return analyzer.result();
}

const EMPTY_SCOPE: ReadonlySet<string> = new Set<string>();

class Analyzer {
  private readonly keys = new Set<string>();
  private stateful = false;
  private keysComplete = true;
  private rejection: string | undefined;

  result(): Analysis {
    return {
      stateful: this.stateful,
      stateKeys: this.keys,
      keysComplete: this.keysComplete,
      rejection: this.rejection,
    };
  }

  /**
   * `bound` holds the names a comprehension has introduced. They shadow the
   * activation root, so `[1].exists(event, event > 0)` is an ordinary
   * comprehension rather than a reference to the event.
   */
  visit(node: AstNode | undefined, bound: ReadonlySet<string>): void {
    // Absent sub-nodes are ordinary: a free function call has no target, a
    // comprehension may have no second iteration variable. The root is checked
    // for presence by `analyze` instead.
    if (this.rejection !== undefined || node === undefined) {
      return;
    }

    const kind = node.exprKind as AstKind | undefined;
    switch (kind?.case) {
      case 'constExpr':
        return;

      case 'identExpr': {
        const name = kind.value.name;
        if (typeof name !== 'string') {
          this.reject('unrecognised expression structure');
          return;
        }
        this.visitIdent(name, bound);
        return;
      }

      case 'selectExpr': {
        this.visitSelect(kind.value, bound);
        return;
      }

      case 'callExpr': {
        this.visitCall(kind.value, bound);
        return;
      }

      case 'listExpr': {
        for (const element of kind.value.elements ?? []) {
          this.visit(element, bound);
        }
        return;
      }

      case 'structExpr': {
        for (const entry of kind.value.entries ?? []) {
          if (entry.keyKind?.case === 'mapKey') {
            this.visit(entry.keyKind.value as AstNode | undefined, bound);
          }
          this.visit(entry.value, bound);
        }
        return;
      }

      case 'comprehensionExpr': {
        this.visitComprehension(kind.value, bound);
        return;
      }

      default:
        this.reject('unrecognised expression structure');
    }
  }

  private visitIdent(name: string, bound: ReadonlySet<string>): void {
    const resolved = unqualify(name);
    if (bound.has(resolved) || isInternalName(resolved)) {
      return;
    }
    if (resolved !== ACTIVATION_ROOT) {
      this.reject(
        `unknown variable "${resolved}"; an expression may only reference "${ACTIVATION_ROOT}"`,
      );
      return;
    }
    // The whole event read as one value — `size(event)`, or the index form
    // `event['state']`, which reaches a field without a selection node. State
    // is inside it either way, so this counts as a state read with no
    // resolvable keys. Over-reporting costs a state read; under-reporting would
    // run a stateful rule in the stateless pass, which is a correctness bug.
    // The index form also escapes the unknown-field check above, so a bad key
    // written that way surfaces as an evaluation error instead of a 400.
    this.markStateReadOfUnknownKeys();
  }

  private visitSelect(select: AstSelect, bound: ReadonlySet<string>): void {
    const field = select.field;
    if (typeof field !== 'string') {
      this.reject('unrecognised expression structure');
      return;
    }

    // `event.state.<key>`, including the `has(event.state.<key>)` form, which
    // parses as the same selection with `testOnly` set.
    if (this.isStateMap(select.operand, bound)) {
      this.stateful = true;
      this.keys.add(field);
      return;
    }

    if (this.isActivationRoot(select.operand, bound)) {
      if (field === STATE_FIELD) {
        // `event.state` as a whole: `size(event.state)`, `has(event.state)`.
        this.markStateReadOfUnknownKeys();
        return;
      }
      if (!ACTIVATION_FIELD_SET.has(field)) {
        this.reject(
          `the activation has no field "${ACTIVATION_ROOT}.${field}"; available fields are ${ACTIVATION_FIELDS.join(', ')}`,
        );
      }
      return;
    }

    this.visit(select.operand, bound);
  }

  private visitCall(call: AstCall, bound: ReadonlySet<string>): void {
    const args = call.args ?? [];
    const target = args[0];

    if (call.function === INDEX_OPERATOR && args.length === 2 && this.isStateMap(target, bound)) {
      this.stateful = true;
      const index = args[1];
      const literal = stringConstant(index);
      if (literal === undefined) {
        this.markStateReadOfUnknownKeys();
      } else {
        this.keys.add(literal);
      }
      // The index may itself reference the activation, e.g. `event.state[event.tool.name]`.
      this.visit(index, bound);
      return;
    }

    if (
      call.function === INDEX_OPERATOR &&
      args.length === 2 &&
      this.isActivationRoot(target, bound)
    ) {
      const index = args[1];
      const field = stringConstant(index);
      if (field === undefined || field === STATE_FIELD) {
        // A computed root key may select state; selecting `state` explicitly
        // exposes the whole map. Neither case has a determinable key set.
        this.markStateReadOfUnknownKeys();
      } else if (!ACTIVATION_FIELD_SET.has(field)) {
        this.reject(
          `the activation has no field "${ACTIVATION_ROOT}.${field}"; available fields are ${ACTIVATION_FIELDS.join(', ')}`,
        );
      }
      // A computed index can itself read state, e.g. `event[event.state.key]`.
      this.visit(index, bound);
      return;
    }

    this.visit(call.target, bound);
    for (const arg of args) {
      this.visit(arg, bound);
    }
  }

  private visitComprehension(comprehension: AstComprehension, bound: ReadonlySet<string>): void {
    this.visit(comprehension.iterRange, bound);
    this.visit(comprehension.accuInit, bound);

    const inner = new Set(bound);
    for (const name of [comprehension.iterVar, comprehension.iterVar2, comprehension.accuVar]) {
      if (typeof name === 'string' && name.length > 0) {
        inner.add(name);
      }
    }
    this.visit(comprehension.loopCondition, inner);
    this.visit(comprehension.loopStep, inner);
    this.visit(comprehension.result, inner);
  }

  /** True when `node` is the `event.state` map itself. */
  private isStateMap(node: AstNode | undefined, bound: ReadonlySet<string>): boolean {
    const kind = node?.exprKind;
    if (kind?.case !== 'selectExpr') {
      return false;
    }
    return (
      kind.value.field === STATE_FIELD &&
      kind.value.testOnly !== true &&
      this.isActivationRoot(kind.value.operand, bound)
    );
  }

  private isActivationRoot(node: AstNode | undefined, bound: ReadonlySet<string>): boolean {
    const kind = node?.exprKind;
    if (kind?.case !== 'identExpr' || typeof kind.value.name !== 'string') {
      return false;
    }
    const name = unqualify(kind.value.name);
    return name === ACTIVATION_ROOT && !bound.has(name);
  }

  private markStateReadOfUnknownKeys(): void {
    this.stateful = true;
    this.keysComplete = false;
  }

  private reject(message: string): void {
    this.rejection ??= message;
  }
}

/** CEL allows a leading dot to force an absolute name: `.event` is `event`. */
function unqualify(name: string): string {
  return name.startsWith('.') ? name.slice(1) : name;
}

/** Names the parser generates for macro expansion, e.g. the `@result` accumulator. */
function isInternalName(name: string): boolean {
  return name.startsWith('@');
}

function stringConstant(node: AstNode | undefined): string | undefined {
  const kind = node?.exprKind;
  if (kind?.case !== 'constExpr') {
    return undefined;
  }
  const constant = kind.value.constantKind;
  return constant?.case === 'stringValue' && typeof constant.value === 'string'
    ? constant.value
    : undefined;
}

function failed(message: string): CompileResult {
  return { ok: false, error: { message } };
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function describeValue(value: unknown): string {
  if (value === null) {
    return 'null';
  }
  if (typeof value === 'object') {
    return 'a non-boolean value';
  }
  return `a ${typeof value}`;
}
