// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The tool-confirmation wire contract — parsing the verdict the registry pushes,
// and the permission-result the harness gate returns.
//
// When a turn is parked on a gated tool call, the registry pushes the client's
// `user.tool_confirmation` transcript event to the runner's confirmation route. This
// module reads the load-bearing fields off that event: the `tool_use_id` (which
// in-flight tool call the verdict is for) and the allow/deny decision. The decision
// is fail-CLOSED: only an explicit allow proceeds; anything else (deny, a missing or
// foreign decision) is a denial, so a gated tool never runs on an ambiguous verdict.
//
// The parse ALSO classifies the decision tri-state (`allow` / `deny` / `ambiguous`)
// alongside the fail-closed `approved` boolean — purely for observability. The gate's
// behavior keys off `approved` (deny and ambiguous both deny), but the tri-state lets
// the confirmation handler log a decision-less / malformed verdict distinctly from a
// deliberate deny, so an operator can tell a bad producer apart from a real refusal.
//
// The {@link ToolPermissionResult} shape mirrors the Claude Agent SDK's
// `PermissionResult` (the `canUseTool` return), so the harness's gate maps a verdict
// to it directly — `allow` → `{ behavior: 'allow', updatedInput }`, `deny` →
// `{ behavior: 'deny', message }`. Keeping the shape here (not importing the SDK
// type) lets the loop + the fake harness speak it without an SDK dependency.

/**
 * How a confirmation body's decision field classified, independent of the
 * fail-closed {@link ToolConfirmationVerdict.approved} collapse.
 *
 * The gate's BEHAVIOR keys off `approved` alone (only `allow` proceeds); this
 * classification is purely OBSERVABILITY so the handler can log an `ambiguous`
 * verdict (a body that carried no recognizable decision at all) distinctly from
 * an explicit `deny` — both deny the tool, but the operator wants to tell a
 * deliberate refusal apart from a producer that sent a malformed/decision-less
 * verdict. It never changes the outcome: `deny` and `ambiguous` both map to
 * `approved: false`.
 */
export type ConfirmationDecision = 'allow' | 'deny' | 'ambiguous';

/** The verdict parsed from a `user.tool_confirmation` event. */
export interface ToolConfirmationVerdict {
  /** The in-flight tool call's `tool_use_id` this verdict is for (the routing key). */
  toolUseId: string;
  /** `true` only on an explicit allow; `false` on deny / missing / foreign decision. */
  approved: boolean;
  /**
   * The decision the body carried, classified tri-state for OBSERVABILITY only.
   * `allow` / `deny` are explicit recognized verdicts; `ambiguous` is a body with
   * no recognizable decision (no `decision` string and no `approved` boolean) —
   * still a denial (`approved` is `false`), but flagged so the handler can log a
   * decision-less push apart from a deliberate `deny`. Does NOT relax fail-closed.
   */
  decision: ConfirmationDecision;
}

/**
 * The permission decision a tool-confirmation gate returns — the same shape as the
 * Claude Agent SDK's `canUseTool` result. `allow` carries the (possibly unchanged)
 * tool input; `deny` carries a human-readable reason.
 */
export type ToolPermissionResult =
  | { behavior: 'allow'; updatedInput: Record<string, unknown> }
  | { behavior: 'deny'; message: string };

/** Thrown when a confirmation body is not a routable `user.tool_confirmation`. */
export class ToolConfirmationParseError extends Error {
  constructor(message: string) {
    super(`tool confirmation parse failed: ${message}`);
    this.name = 'ToolConfirmationParseError';
  }
}

/**
 * Parse a pushed `user.tool_confirmation` body into a {@link ToolConfirmationVerdict}.
 *
 * The body is the transcript event JSON. The `tool_use_id` is REQUIRED (without it
 * the verdict cannot be routed to a parked tool call) — its absence is a
 * {@link ToolConfirmationParseError} the handler maps to a 400. The decision is
 * normalized fail-closed: `decision` of `allow`/`approve`/`accept` (any case), or an
 * `approved: true` boolean, is the only path to `approved: true`; everything else
 * (an explicit `deny`, an unknown string, or no decision at all) is `approved: false`.
 *
 * @param body The confirmation push body (JSON).
 */
export function parseToolConfirmation(body: Uint8Array): ToolConfirmationVerdict {
  const text = Buffer.from(body).toString('utf8').trim();
  if (text.length === 0) {
    throw new ToolConfirmationParseError('empty body');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new ToolConfirmationParseError(err instanceof Error ? err.message : 'invalid JSON');
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new ToolConfirmationParseError('body is not a JSON object');
  }
  const obj = parsed as Record<string, unknown>;
  const toolUseId = obj['tool_use_id'];
  if (typeof toolUseId !== 'string' || toolUseId.length === 0) {
    throw new ToolConfirmationParseError("missing 'tool_use_id'");
  }
  const decision = classifyDecision(obj);
  // Fail-closed: ONLY an explicit `allow` proceeds. `deny` and `ambiguous` both
  // collapse to `approved: false` — the gate's behavior is unchanged; the tri-state
  // is carried alongside for observability (the handler logs `ambiguous` distinctly).
  return { toolUseId, approved: decision === 'allow', decision };
}

/**
 * Classify a confirmation object's decision tri-state. Recognizes the allow
 * spellings (`decision: 'allow' | 'approve' | 'accept'`, case-insensitive) and an
 * `approved: true` boolean as `allow`; the deny spellings (`decision: 'deny' |
 * 'reject' | 'decline'`, case-insensitive) and an `approved: false` boolean as
 * `deny`; everything else — no `decision` string AND no `approved` boolean, or an
 * unknown `decision` string — as `ambiguous`.
 *
 * The fail-closed contract is preserved by the CALLER, which treats only `allow` as
 * approved: an `ambiguous` (or unknown) verdict is still a denial. The deny / unknown
 * split exists purely so the handler can tell a deliberate refusal apart from a
 * decision-less push when logging; it never relaxes the gate.
 */
function classifyDecision(obj: Record<string, unknown>): ConfirmationDecision {
  const decision = obj['decision'];
  if (typeof decision === 'string') {
    const normalized = decision.trim().toLowerCase();
    if (normalized === 'allow' || normalized === 'approve' || normalized === 'accept') {
      return 'allow';
    }
    if (normalized === 'deny' || normalized === 'reject' || normalized === 'decline') {
      return 'deny';
    }
    // A `decision` string that is neither an allow nor a deny spelling: the producer
    // sent something the runner does not recognize — fail-closed denial, flagged.
    return 'ambiguous';
  }
  // No `decision` string: some producers carry the verdict as a boolean `approved`.
  // Honor it as a secondary spelling (still fail-closed: only `true` is an allow).
  if (typeof obj['approved'] === 'boolean') {
    return obj['approved'] === true ? 'allow' : 'deny';
  }
  // No recognizable decision at all (no decision string, no approved boolean): the
  // body is decision-less. Fail-closed denial, but flagged `ambiguous` so a malformed
  // verdict is observable apart from a deliberate deny.
  return 'ambiguous';
}

/** The default denial message the gate returns when a tool call is denied. */
export const TOOL_DENIED_MESSAGE = 'Tool use denied by user confirmation.';

/** The denial message the gate returns when no verdict arrived before the deadline. */
export const TOOL_DENIED_TIMEOUT_MESSAGE = 'Tool use denied: confirmation timed out.';
