// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The runner-tunnel application protocol the runner SERVES — the client half of
// the registry's owner-pod push contract.
//
// The registry PUSHES ten request kinds over the runner tunnel and the runner
// serves each at the matching path; these constants are the cross-component
// contract, single-sourced here on the runner side to match the registry's
// `session-event-bridge.ts` / `session-snapshot-delivery.ts` / `session-recovery.ts` /
// `session-skills-delivery.ts` / `session-resources-delivery.ts` EXACTLY (same path,
// same header names). Drift on either side breaks delivery, so the literals are
// pinned against the registry source by `test/unit/protocol-registry-pin.spec.ts`.

/**
 * Turn route: the owner pod POSTs one user turn here (body = the user event's
 * transcript payload). The runner drives the harness and streams agent events
 * back as the NDJSON response body. Matches the registry's `RUNNER_TURN_PATH`.
 */
export const RUNNER_TURN_PATH = '/v1/runner/turn';

/**
 * Snapshot route: the owner pod POSTs the credential-free agent snapshot here
 * (body = a single NDJSON line) before the first turn. The runner configures the
 * provider and acks 200. Matches the registry's `RUNNER_SNAPSHOT_PATH`.
 */
export const RUNNER_SNAPSHOT_PATH = '/v1/runner/snapshot';

/**
 * Skills route: the owner pod PUSHES the session's Skill bundle bytes here (body =
 * NDJSON — one manifest line then one line per file) BEFORE the snapshot, because the
 * colocated runner holds no object-store credentials + no `@orca/skill-store` and so
 * cannot pull bundles itself. The runner materializes them to a native `--plugin-dir`
 * plugin under its workspace, records the plugin dir, and acks 200 (400 on a malformed
 * body, 500 on a write failure so the owner pod records it undelivered + retries on the
 * next reconnect). Matches the registry's `RUNNER_SKILLS_PATH`
 * (`registry-service-ts/src/tunnel/session-skills-delivery.ts`). Distinct from the
 * snapshot route so a runner routes a skills push independently of the snapshot.
 */
export const RUNNER_SKILLS_PATH = '/v1/runner/skills';

/**
 * Replay route: the owner pod PUSHES a resume replay here (body = NDJSON, one
 * persisted-event JSON per line) on (re)connect. The runner applies each event,
 * deduping by stable id, and acks 200. Matches the registry's `RUNNER_REPLAY_PATH`.
 */
export const RUNNER_REPLAY_PATH = '/v1/runner/replay';

/**
 * Confirmation route: the owner pod PUSHES a tool-confirmation verdict here (body =
 * the client's `user.tool_confirmation` transcript event JSON) while a turn is
 * parked on a gated tool call. The runner resolves the parked verdict (keyed by the
 * event's `tool_use_id`) so the harness's `canUseTool` proceeds (allow) or returns a
 * clean denial (deny), and acks 200. Distinct from the turn route because a
 * confirmation is NOT a new turn — it is the verdict for an in-flight tool call
 * within the current turn — so the runner routes it independently of a turn drive.
 *
 * The verdict's durable source of truth is the transcript (it survives a restart and
 * is re-served by recovery); this push is just the LIVE delivery that unblocks the
 * parked tool call. A re-pushed confirmation (a flapping reconnect) is idempotent on
 * the runner (the parking's first verdict wins). The registry-side PRODUCER is the
 * owner-pod event bridge's confirmation follower
 * (`registry-service-ts/src/tunnel/session-event-bridge.ts`, `RUNNER_CONFIRMATION_PATH`
 * + `pushConfirmation`): it follows the transcript for `user.tool_confirmation`
 * events and POSTs each here (the SAME path + session header), running CONCURRENTLY
 * with the turn driver so a verdict reaches a turn parked on a gated tool call. This
 * constant pins the path the runner serves to match that producer EXACTLY.
 */
export const RUNNER_CONFIRMATION_PATH = '/v1/runner/confirmation';

/**
 * Interrupt route: the owner pod PUSHES a `user.interrupt` here (body = the client's
 * `user.interrupt` transcript event JSON) while a turn is in flight. The runner
 * ABORTS the in-flight turn — firing the harness's SDK abort signal + force-closing
 * the live query so a blocked `query()` unwinds — WITHOUT tearing down the harness
 * (the next turn reuses it), and acks 200.
 *
 * Distinct from the turn route for the same structural reason the confirmation route
 * is: a `user.interrupt` is NOT a new turn — it is an out-of-band control signal that
 * must PREEMPT the turn currently in flight. Turns are SERIAL (the owner pod drives
 * one at a time, and the in-flight turn blocks the owner-pod turn loop inside its
 * streaming POST), so an interrupt delivered as a turn body would queue BEHIND the
 * very turn it is meant to unblock — a deadlock. Routing it independently lets it
 * reach the runner while a turn is parked/blocked, exactly as the confirmation route
 * does for a parked tool call. A re-pushed interrupt (a flapping reconnect) is
 * idempotent: aborting an already-finished or already-aborted turn is a harmless
 * no-op.
 *
 * The registry-side PRODUCER is the owner-pod event bridge's interrupt follower
 * (`registry-service-ts/src/tunnel/session-event-bridge.ts`), the sibling of its
 * `user.tool_confirmation` follower: it follows the transcript for `user.interrupt`
 * events and POSTs each here (the SAME path + session header), running CONCURRENTLY
 * with the turn driver so the interrupt reaches a turn blocked on the model. This
 * constant pins the path the runner serves so that producer matches it EXACTLY.
 * Single-sourced here on the runner side.
 */
export const RUNNER_INTERRUPT_PATH = '/v1/runner/interrupt';

/**
 * Header carrying the session id on every pushed request. Lower-cased on the wire
 * by the frame codec; the canonical-case constant matches the registry's
 * `RUNNER_SESSION_HEADER` so the runner's case-insensitive lookup resolves it.
 */
export const RUNNER_SESSION_HEADER = 'X-Orca-Session-Id';

/**
 * Header carrying the runner's presented cursor on a replay push (the
 * `after={cursor}` slice the owner served; empty = a fresh full replay). Matches
 * the registry's `RUNNER_RESUME_CURSOR_HEADER`.
 *
 * INTENTIONALLY NOT CONSUMED by the runner. The runner sources its resume position
 * SOLELY from the stable ids of the events it has actually applied — `applyReplay`
 * advances {@link SessionLoop.resumeCursors} to the last id in the replay BODY, and
 * the next (re)connect's hello re-advertises that last truly-applied id. On a
 * caught-up frame the registry sends a zero-byte body and carries the cursor only in
 * this header; the runner advances no cursor for that frame and instead re-presents
 * its last applied id on reconnect, which the registry's `sliceAfterCursor` re-serves
 * idempotently. So body-id-derivation is exactly-once on its own — the header is
 * informational (observer / registry-side observer-contiguity), and this constant is
 * exported only to pin the wire name to the registry's. Do NOT read it to set the
 * cursor; that would couple the runner to a value it must derive from applied state.
 */
export const RUNNER_RESUME_CURSOR_HEADER = 'X-Orca-Resume-Cursor';

/** NDJSON content type for the turn/replay/snapshot bodies + agent-event stream. */
export const NDJSON_CONTENT_TYPE = 'application/x-ndjson';

/** Credential-free managed resource delivery and independent persistence acknowledgement. */
export const RUNNER_RESOURCES_PATH = '/v1/runner/resources';
export const RUNNER_RESOURCE_CHANGES_PATH = '/v1/runner/resource-changes';
export const RUNNER_RESOURCE_ACK_PATH = '/v1/runner/resource-changes/ack';

/** Independent client callback result delivery; pinned to Registry's session-event-bridge. */
export const RUNNER_CUSTOM_TOOL_RESULT_PATH = '/v1/runner/custom-tool-result';
