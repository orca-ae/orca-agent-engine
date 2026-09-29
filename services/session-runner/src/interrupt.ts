// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The interrupt wire contract — validating the `user.interrupt` body the registry
// pushes to abort the in-flight turn.
//
// When a client wants to stop a running turn, it appends a `user.interrupt` to the
// transcript; the owner-pod event bridge follows it and POSTs the event to the
// runner's interrupt route (concurrently with the turn driver, so it can preempt a
// turn blocked on the model). This module reads that body.
//
// Unlike a `user.tool_confirmation`, a `user.interrupt` carries NO routing key and NO
// parameters — it is a bare control signal whose only meaning is "abort whatever turn
// is in flight". So the parse is a pure well-formedness check (a JSON object, the
// public `user.*` event shape); there is nothing to extract. A malformed body is a
// {@link InterruptParseError} the handler maps to a 400; a well-formed one drives the
// loop's {@link SessionLoop.interruptTurn}.

/** Thrown when an interrupt body is not a well-formed `user.interrupt` event object. */
export class InterruptParseError extends Error {
  constructor(message: string) {
    super(`interrupt parse failed: ${message}`);
    this.name = 'InterruptParseError';
  }
}

/**
 * Validate a pushed `user.interrupt` body — a well-formedness check only.
 *
 * The body is the transcript event JSON. A `user.interrupt` carries no routing key
 * and no parameters (it aborts the turn currently in flight), so there is nothing to
 * return: the parse succeeds for any JSON object and throws an
 * {@link InterruptParseError} for an empty body, invalid JSON, or a non-object body
 * (the handler maps that to a 400). An empty object `{}` is accepted — the interrupt
 * needs no fields. Validating the shape (rather than ignoring the body) keeps a
 * garbage push from being silently treated as an interrupt.
 *
 * @param body The interrupt push body (JSON).
 */
export function parseUserInterrupt(body: Uint8Array): void {
  const text = Buffer.from(body).toString('utf8').trim();
  if (text.length === 0) {
    throw new InterruptParseError('empty body');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new InterruptParseError(err instanceof Error ? err.message : 'invalid JSON');
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new InterruptParseError('body is not a JSON object');
  }
}
