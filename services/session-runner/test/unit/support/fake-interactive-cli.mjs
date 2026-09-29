#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A FAKE interactive terminal program for the sys_terminal_* specs.
//
// It stands in for the kind of interactive program an operator drives inside a
// tmux pane (a REPL, a pager, a shell). It is deliberately tiny and dependency
// free so it runs unchanged inside a `send-keys`-driven pane, and it exercises
// the two send modes the sys_terminal_send tool must support:
//
//   - LITERAL TEXT + submit: it prints a `> ` prompt, and for every line it
//     reads it echoes `you said: <line>` back. That proves typing characters
//     into the pane and pressing Enter (a submit) reaches the program and its
//     rendered output is captured by sys_terminal_read.
//   - A KEY CHORD: it installs a SIGINT handler (Ctrl-C / `C-c`). On the first
//     SIGINT it prints `interrupted!` and keeps running; that proves a control
//     chord sent via sys_terminal_send is delivered to the foreground program
//     (not swallowed), and its reaction renders in the pane.
//
// The line `quit` makes it print `bye` and exit 0, so a test can drive a clean
// shutdown through the pane before sys_terminal_close.

import { createInterface } from 'node:readline';

process.stdout.write('READY\n');
process.stdout.write('> ');

// A visible reaction to Ctrl-C proves the chord reached the foreground program.
// Keep running after the first interrupt so the pane stays alive for a read.
process.on('SIGINT', () => {
  process.stdout.write('\ninterrupted!\n> ');
});

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on('line', (line) => {
  const text = line.trim();
  if (text === 'quit') {
    process.stdout.write('bye\n');
    rl.close();
    return;
  }
  process.stdout.write(`you said: ${text}\n> `);
});
rl.on('close', () => process.exit(0));
