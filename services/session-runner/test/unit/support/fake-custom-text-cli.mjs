#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A FAKE PLAIN-TEXT custom CLI for the generic `custom` provider spec.
//
// It stands in for the SIMPLEST possible operator-registered CLI: one that reads a user prompt on
// stdin (one line per turn) and prints assistant text lines on stdout, terminating each turn with a
// sentinel line. No JSON, no MCP — this proves the `custom` provider's PLAIN-TEXT stdout mapping
// (every stdout line → agent text) end to end.
//
// Protocol (mirrors what the text-mode custom spec declares):
//   - one user turn per stdin line; dispatch on the FIRST word:
//       · "say <rest>"   → print "<rest>" then the end sentinel,
//       · "two <rest>"   → print two text lines ("<rest>" and "<rest>!") then the sentinel,
//   - the end sentinel line is `<<TURN_END>>` (the spec's `end_sentinel`).
//
// It writes each argv it was launched with to `--argv-out` (when given) so the spec can assert the
// launch argv the provider emitted (placeholder substitution) reached the CLI.

import { createInterface } from 'node:readline';
import { writeFileSync } from 'node:fs';

const END_SENTINEL = '<<TURN_END>>';

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

const argvOut = argValue('--argv-out');
if (argvOut) {
  writeFileSync(argvOut, JSON.stringify(process.argv.slice(2)), 'utf8');
}

function emit(line) {
  process.stdout.write(`${line}\n`);
}

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

rl.on('line', (line) => {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    return;
  }
  const [verb, ...rest] = trimmed.split(' ');
  const body = rest.join(' ');
  if (verb === 'two') {
    emit(body);
    emit(`${body}!`);
    emit(END_SENTINEL);
    return;
  }
  // Default ("say" or anything else): echo one text line + the sentinel.
  emit(body.length > 0 ? body : verb);
  emit(END_SENTINEL);
});

rl.on('close', () => {
  process.exit(0);
});
