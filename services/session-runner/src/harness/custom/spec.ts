// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The generic custom-provider SPEC PARSER — the declarative CLI-agent spec, validated.
//
// The `custom` provider lets an operator register ANY CLI agent WITHOUT writing a bespoke provider:
// the agent snapshot carries a declarative `custom_spec` block describing how to launch the CLI and
// how to read its stdout. This module turns that opaque block into a typed, validated
// {@link CustomAgentSpec} the launcher + normalizer consume. The spec is deliberately provider-
// shaped — it never names any concrete CLI; it is the neutral vocabulary a self-hosted operator
// declares their tool in.
//
// A spec describes:
//   - `command` — the CLI binary (a name on the sandbox PATH, or an absolute path);
//   - `argv` — the argument template, with `{...}` placeholders the launcher substitutes
//     (`{sessionId}` / `{workspaceId}` / `{model}` / `{systemPrompt}` / `{sandboxRoot}` /
//     `{bridgeCommand}` / `{bridgeArgsJson}`);
//   - `env` — extra process env (values may carry the same placeholders);
//   - `cwd` — a sandbox-visible working directory;
//   - `stdin` — how a user turn is written to the CLI's stdin (default: the raw user text as one
//     line; or a JSON `template` object with `{userText}` serialized as one JSON line);
//   - `stdout` — how the CLI's stdout maps to Orca-native agent events (`text` mode: every stdout
//     line is agent text; `jsonLine` mode: field rules mapping JSON frames to
//     text/tool_use/tool_result/usage/turn_completed + an optional approval request);
//   - `approvals` — the opt-in that routes a CLI-raised approval request to the human gate, plus
//     the stdin response frame template the harness writes back. The `stdout.approval_request`
//     rule and this `approvals` block are the two halves of one loop and MUST be declared together
//     (the parser rejects a spec that carries only one half).
//
// The parser is PURE (no I/O) and FAIL-FAST: a malformed spec throws {@link CustomSpecError} rather
// than silently degrading, so a broken operator config surfaces as a clean capability error at
// session start rather than a mysterious runtime hang.

/** Thrown when the `custom_spec` block is absent or structurally invalid. */
export class CustomSpecError extends Error {
  constructor(message: string) {
    super(`custom agent spec invalid: ${message}`);
    this.name = 'CustomSpecError';
  }
}

/**
 * How a user turn is written to the CLI's stdin.
 *   - `raw` — write the user text followed by a newline (the simplest line-based CLI);
 *   - `json` — serialize the `template` object as one JSON line, substituting `{userText}` in any
 *     string field with the turn's text.
 */
export type CustomStdinSpec = { mode: 'raw' } | { mode: 'json'; template: Record<string, unknown> };

/** A field rule matching a JSON stdout frame carrying assistant TEXT. */
export interface CustomTextRule {
  /** Match when the frame's `type` field equals this (omit to match any object frame). */
  type_equals?: string;
  /** The frame field holding the assistant text (default `text`). */
  text_field: string;
}

/** A field rule matching a JSON stdout frame carrying a TOOL CALL. */
export interface CustomToolUseRule {
  type_equals?: string;
  /** The frame field holding the tool name (default `name`). */
  name_field: string;
  /** The frame field holding the tool input object (default `input`). */
  input_field: string;
  /** The frame field holding the call id (default `id`). */
  id_field: string;
}

/** A field rule matching a JSON stdout frame carrying a TOOL RESULT. */
export interface CustomToolResultRule {
  type_equals?: string;
  /** The frame field holding the originating call id (default `tool_use_id`). */
  id_field: string;
  /** The frame field holding the result content (default `content`). */
  content_field: string;
  /** The frame field holding the error flag (default `is_error`). */
  error_field: string;
}

/** A field rule matching a JSON stdout frame carrying token USAGE. */
export interface CustomUsageRule {
  type_equals?: string;
  /** The frame field holding the input-token count (default `input_tokens`). */
  input_tokens_field: string;
  /** The frame field holding the output-token count (default `output_tokens`). */
  output_tokens_field: string;
}

/** A field rule matching a JSON stdout frame that ENDS the turn. */
export interface CustomTurnCompletedRule {
  /** Match when the frame's `type` field equals this. */
  type_equals: string;
}

/** A field rule matching a JSON stdout frame that RAISES an approval request. */
export interface CustomApprovalRule {
  type_equals?: string;
  /** The frame field holding the approval request id (default `request_id`). */
  id_field: string;
  /** The frame field holding the tool name being approved (default `tool`). */
  name_field: string;
  /** The frame field holding the tool input object, when present (default `input`). */
  input_field: string;
}

/** The `text` stdout mapping: every non-blank stdout line is agent text. */
export interface CustomTextStdout {
  mode: 'text';
  /** An optional sentinel line that ENDS the turn; absent → the turn ends on stream EOF only. */
  end_sentinel?: string;
}

/** The `jsonLine` stdout mapping: each stdout line is a JSON frame matched against field rules. */
export interface CustomJsonLineStdout {
  mode: 'jsonLine';
  text?: CustomTextRule;
  tool_use?: CustomToolUseRule;
  tool_result?: CustomToolResultRule;
  usage?: CustomUsageRule;
  turn_completed?: CustomTurnCompletedRule;
  approval_request?: CustomApprovalRule;
}

/** The stdout mapping — either plain text or JSON-line field rules. */
export type CustomStdoutSpec = CustomTextStdout | CustomJsonLineStdout;

/**
 * The approvals opt-in: when set, an approval frame the CLI raises (matched by the stdout
 * mapping's `approval_request` rule) is routed to the human gate, and the harness writes this
 * response frame back to the CLI's stdin. The response `template` is a JSON object whose string
 * fields may carry `{requestId}` (the approval's id) and `{decision}` (substituted with
 * {@link allow_value} / {@link deny_value}).
 */
export interface CustomApprovalsSpec {
  /** The stdin response frame template written back after a verdict. */
  response: Record<string, unknown>;
  /** The `{decision}` substitution when the gate ALLOWS (default `allow`). */
  allow_value: string;
  /** The `{decision}` substitution when the gate DENIES (default `deny`). */
  deny_value: string;
}

/** A parsed, validated custom-agent spec. */
export interface CustomAgentSpec {
  command: string;
  argv: string[];
  env: Record<string, string>;
  cwd?: string;
  stdin: CustomStdinSpec;
  stdout: CustomStdoutSpec;
  approvals?: CustomApprovalsSpec;
}

/**
 * Parse + validate the opaque `custom_spec` block into a {@link CustomAgentSpec}.
 *
 * @throws CustomSpecError when the block is absent, not an object, missing a non-blank `command`,
 *   or declares an unknown stdout mode.
 */
export function parseCustomAgentSpec(raw: unknown): CustomAgentSpec {
  if (raw === null || raw === undefined || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new CustomSpecError('spec must be a JSON object');
  }
  const obj = raw as Record<string, unknown>;

  const command = obj['command'];
  if (typeof command !== 'string' || command.trim().length === 0) {
    throw new CustomSpecError('`command` is required and must be a non-empty string');
  }

  const spec: CustomAgentSpec = {
    command,
    argv: stringArray(obj['argv']),
    env: stringRecord(obj['env']),
    stdin: parseStdin(obj['stdin']),
    stdout: parseStdout(obj['stdout']),
  };
  const cwd = obj['cwd'];
  if (typeof cwd === 'string' && cwd.length > 0) {
    spec.cwd = cwd;
  }
  const approvals = parseApprovals(obj['approvals']);
  if (approvals !== undefined) {
    spec.approvals = approvals;
  }
  // Fail-fast on the approval wiring: the stdout `approval_request` rule (which routes a CLI-raised
  // approval to the human gate) and the top-level `approvals` block (which supplies the stdin
  // response frame the harness writes back after a verdict) are two halves of ONE loop and MUST be
  // declared together. A spec with only one half is malformed:
  //   - a rule with NO `approvals` block would emit the gate signal then have no frame to write
  //     back, so a CLI that blocks awaiting the decision frame HANGS that turn;
  //   - an `approvals` block with NO rule is dead config — no frame ever routes to the gate.
  // Surface it as a clean capability error at parse time rather than a mysterious runtime stall.
  assertApprovalPairing(spec.stdout, spec.approvals);
  return spec;
}

/**
 * Enforce that the stdout `approval_request` rule and the top-level `approvals` block are declared
 * as a pair (both or neither). Called after both are parsed.
 *
 * @throws CustomSpecError when exactly one half of the approval loop is present.
 */
function assertApprovalPairing(
  stdout: CustomStdoutSpec,
  approvals: CustomApprovalsSpec | undefined,
): void {
  const hasRule = stdout.mode === 'jsonLine' && stdout.approval_request !== undefined;
  const hasApprovals = approvals !== undefined;
  if (hasRule && !hasApprovals) {
    throw new CustomSpecError(
      '`stdout.approval_request` requires a top-level `approvals` block (the stdin response frame ' +
        'template written back after a verdict); declaring the rule without it would leave a ' +
        'blocking CLI awaiting a decision frame that is never written',
    );
  }
  if (hasApprovals && !hasRule) {
    throw new CustomSpecError(
      '`approvals` requires a `stdout.approval_request` rule (jsonLine mode) that routes a ' +
        'CLI-raised approval frame to the gate; declaring `approvals` without the rule is dead ' +
        'config — no approval is ever routed',
    );
  }
}

/** Parse the stdin spec; absent → `raw` mode (write the user text as one line). */
function parseStdin(raw: unknown): CustomStdinSpec {
  if (raw === null || raw === undefined) {
    return { mode: 'raw' };
  }
  if (typeof raw !== 'object' || Array.isArray(raw)) {
    throw new CustomSpecError('`stdin` must be an object when set');
  }
  const obj = raw as Record<string, unknown>;
  // A `template` object means JSON-line input; otherwise raw.
  const template = obj['template'];
  if (template !== undefined) {
    if (template === null || typeof template !== 'object' || Array.isArray(template)) {
      throw new CustomSpecError('`stdin.template` must be an object');
    }
    return { mode: 'json', template: template as Record<string, unknown> };
  }
  if (obj['mode'] === 'json') {
    throw new CustomSpecError('`stdin.mode = json` requires a `template` object');
  }
  return { mode: 'raw' };
}

/** Parse the stdout mapping; absent → the `text` mode (every stdout line is agent text). */
function parseStdout(raw: unknown): CustomStdoutSpec {
  if (raw === null || raw === undefined) {
    return { mode: 'text' };
  }
  if (typeof raw !== 'object' || Array.isArray(raw)) {
    throw new CustomSpecError('`stdout` must be an object when set');
  }
  const obj = raw as Record<string, unknown>;
  const mode = obj['mode'] ?? 'text';
  if (mode === 'text') {
    const out: CustomTextStdout = { mode: 'text' };
    const sentinel = obj['end_sentinel'];
    if (typeof sentinel === 'string' && sentinel.length > 0) {
      out.end_sentinel = sentinel;
    }
    return out;
  }
  if (mode === 'jsonLine') {
    return parseJsonLineStdout(obj);
  }
  throw new CustomSpecError(
    `unknown stdout mode '${String(mode)}' (expected 'text' or 'jsonLine')`,
  );
}

/** Parse a `jsonLine` stdout mapping's field rules (each rule optional, defaulted). */
function parseJsonLineStdout(obj: Record<string, unknown>): CustomJsonLineStdout {
  const out: CustomJsonLineStdout = { mode: 'jsonLine' };

  const text = ruleObject(obj['text']);
  if (text !== undefined) {
    out.text = { text_field: stringField(text['text_field'], 'text'), ...typeMatch(text) };
  }
  const toolUse = ruleObject(obj['tool_use']);
  if (toolUse !== undefined) {
    out.tool_use = {
      name_field: stringField(toolUse['name_field'], 'name'),
      input_field: stringField(toolUse['input_field'], 'input'),
      id_field: stringField(toolUse['id_field'], 'id'),
      ...typeMatch(toolUse),
    };
  }
  const toolResult = ruleObject(obj['tool_result']);
  if (toolResult !== undefined) {
    out.tool_result = {
      id_field: stringField(toolResult['id_field'], 'tool_use_id'),
      content_field: stringField(toolResult['content_field'], 'content'),
      error_field: stringField(toolResult['error_field'], 'is_error'),
      ...typeMatch(toolResult),
    };
  }
  const usage = ruleObject(obj['usage']);
  if (usage !== undefined) {
    out.usage = {
      input_tokens_field: stringField(usage['input_tokens_field'], 'input_tokens'),
      output_tokens_field: stringField(usage['output_tokens_field'], 'output_tokens'),
      ...typeMatch(usage),
    };
  }
  const done = ruleObject(obj['turn_completed']);
  if (done !== undefined) {
    const typeEquals = done['type_equals'];
    if (typeof typeEquals !== 'string' || typeEquals.length === 0) {
      throw new CustomSpecError('`stdout.turn_completed.type_equals` is required');
    }
    out.turn_completed = { type_equals: typeEquals };
  }
  const approval = ruleObject(obj['approval_request']);
  if (approval !== undefined) {
    out.approval_request = {
      id_field: stringField(approval['id_field'], 'request_id'),
      name_field: stringField(approval['name_field'], 'tool'),
      input_field: stringField(approval['input_field'], 'input'),
      ...typeMatch(approval),
    };
  }
  return out;
}

/** Parse the approvals opt-in; absent → `undefined` (no approval routing). */
function parseApprovals(raw: unknown): CustomApprovalsSpec | undefined {
  if (raw === null || raw === undefined) {
    return undefined;
  }
  if (typeof raw !== 'object' || Array.isArray(raw)) {
    throw new CustomSpecError('`approvals` must be an object when set');
  }
  const obj = raw as Record<string, unknown>;
  const response = obj['response'];
  if (
    response === null ||
    response === undefined ||
    typeof response !== 'object' ||
    Array.isArray(response)
  ) {
    throw new CustomSpecError(
      '`approvals.response` must be an object (the stdin response frame template)',
    );
  }
  return {
    response: response as Record<string, unknown>,
    allow_value: stringField(obj['allow_value'], 'allow'),
    deny_value: stringField(obj['deny_value'], 'deny'),
  };
}

// ── field coercers ──────────────────────────────────────────────────────────────

/** `{ type_equals }` spread — included only when the rule pins a `type` match. */
function typeMatch(rule: Record<string, unknown>): { type_equals?: string } {
  const value = rule['type_equals'];
  return typeof value === 'string' && value.length > 0 ? { type_equals: value } : {};
}

/** A rule sub-object, or `undefined` when the key is absent / not an object. */
function ruleObject(raw: unknown): Record<string, unknown> | undefined {
  if (raw === null || raw === undefined || typeof raw !== 'object' || Array.isArray(raw)) {
    return undefined;
  }
  return raw as Record<string, unknown>;
}

/** A string field value, or the fallback when absent / not a non-empty string. */
function stringField(raw: unknown, fallback: string): string {
  return typeof raw === 'string' && raw.length > 0 ? raw : fallback;
}

/** Coerce a value to a string array, dropping non-string entries. */
function stringArray(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.filter((v): v is string => typeof v === 'string');
}

/** Coerce a value to a `Record<string,string>`, keeping only string values. */
function stringRecord(raw: unknown): Record<string, string> {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    return {};
  }
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (typeof value === 'string') {
      out[key] = value;
    }
  }
  return out;
}
