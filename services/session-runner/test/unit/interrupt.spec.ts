// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the interrupt body validator.
//
// A `user.interrupt` carries no routing key and no parameters — it is a bare control
// signal whose only meaning is "abort the in-flight turn". So the parser is a pure
// well-formedness check: any JSON object passes; an empty body / invalid JSON / a
// non-object body throws (the handler maps that to a 400).

import { describe, it, expect } from 'vitest';
import { parseUserInterrupt, InterruptParseError } from '../../src/interrupt.js';

function body(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

describe('parseUserInterrupt', () => {
  it('accepts a well-formed user.interrupt event object', () => {
    expect(() => parseUserInterrupt(body('{"type":"user.interrupt"}'))).not.toThrow();
  });

  it('accepts an empty object (the interrupt needs no fields)', () => {
    expect(() => parseUserInterrupt(body('{}'))).not.toThrow();
  });

  it('tolerates surrounding whitespace / a trailing newline (delivery body shape)', () => {
    expect(() => parseUserInterrupt(body('  {"type":"user.interrupt"}\n'))).not.toThrow();
  });

  it('rejects an empty body', () => {
    expect(() => parseUserInterrupt(body(''))).toThrow(InterruptParseError);
    expect(() => parseUserInterrupt(body('   '))).toThrow(/empty body/);
  });

  it('rejects invalid JSON', () => {
    expect(() => parseUserInterrupt(body('not json'))).toThrow(InterruptParseError);
  });

  it('rejects a non-object body (array / scalar)', () => {
    expect(() => parseUserInterrupt(body('[]'))).toThrow(/not a JSON object/);
    expect(() => parseUserInterrupt(body('42'))).toThrow(/not a JSON object/);
    expect(() => parseUserInterrupt(body('null'))).toThrow(/not a JSON object/);
  });
});
