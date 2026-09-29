#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A FAKE native CLI for the tmux-sandbox + native-cli-launcher specs.
//
// It stands in for a real native coding CLI (the kind the native-CLI providers wire: a
// long-lived process that speaks newline-delimited JSON — "stream-json" — over
// stdin/stdout). No real CLI binary is needed to prove the launch framework:
// this script emits a `ready` line on boot, then for every stdin line it reads
// it echoes back a `reply` line carrying the same text, and exits `0` when it
// sees a line whose text is `bye`. That is enough to assert the three things the
// launcher must deliver: a streamed stdout of JSON lines, a working stdin, and
// clean termination.
//
// Kept deliberately tiny + dependency-free (plain Node readline) so it runs
// unchanged inside a tmux pane driven by `send-keys`.

import { createInterface } from 'node:readline';

function emit(obj) {
  process.stdout.write(`${JSON.stringify(obj)}\n`);
}

// Announce readiness first so a consumer can synchronize on a known line before
// it starts feeding stdin (the launcher's `lines()` iterator yields this first).
emit({ type: 'ready', pid: process.pid });

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

let seq = 0;
rl.on('line', (line) => {
  const text = line;
  emit({ type: 'reply', seq: seq++, text });
  if (text === 'bye') {
    emit({ type: 'done' });
    rl.close();
  }
});

rl.on('close', () => {
  process.exit(0);
});
