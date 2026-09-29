// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The production {@link ChatIo}: a terminal front-end over node's built-in
// `readline` (no external prompt library — node built-ins preferred). Line input
// and the yes/no confirmation gate both read from stdin; output is written to
// stdout with a trailing newline.
//
// This module owns the operator's AUTHORIZATION decision: `confirm` is what
// session-chat turns into a `user.tool_confirmation` verdict, so its answer
// parsing and its fail-closed EOF handling are security-relevant branches, not
// plumbing. `terminal-io.spec.ts` drives them over real streams. An earlier
// version of this header claimed the module had "no branching logic to unit
// test", and while no spec imported it, inverting `isAffirmative` to `return
// true` and the EOF path to `resolve(true)` left the whole suite green.

import { createInterface, type Interface } from 'node:readline';
import type { Readable, Writable } from 'node:stream';
import type { ChatIo } from './session-chat.js';

/** Streams the terminal IO binds to (defaults to process stdio). */
export interface TerminalStreams {
  input?: Readable;
  output?: Writable;
}

/**
 * Build a {@link ChatIo} backed by `readline`. The returned object also exposes
 * `close()` to release the readline interface when the loop ends.
 */
export function createTerminalIo(streams: TerminalStreams = {}): ChatIo & { close(): void } {
  const input = streams.input ?? process.stdin;
  const output = streams.output ?? process.stdout;
  const rl: Interface = createInterface({ input, output, terminal: false });

  return {
    readLine(prompt: string): Promise<string | null> {
      return new Promise<string | null>((resolve) => {
        // `question` invokes its callback with the line, or — if stdin closes
        // first — the interface emits `close` and the callback never fires, so
        // resolve `null` on close to signal EOF to the loop.
        let settled = false;
        const onClose = (): void => {
          if (!settled) {
            settled = true;
            resolve(null);
          }
        };
        rl.once('close', onClose);
        rl.question(prompt, (answer) => {
          settled = true;
          rl.removeListener('close', onClose);
          resolve(answer);
        });
      });
    },

    confirm(question: string): Promise<boolean> {
      return new Promise<boolean>((resolve) => {
        let settled = false;
        const onClose = (): void => {
          if (!settled) {
            settled = true;
            // Fail closed: a closed stdin denies the gated tool.
            resolve(false);
          }
        };
        rl.once('close', onClose);
        rl.question(question, (answer) => {
          settled = true;
          rl.removeListener('close', onClose);
          resolve(isAffirmative(answer));
        });
      });
    },

    write(text: string): void {
      output.write(`${text}\n`);
    },

    close(): void {
      rl.close();
    },
  };
}

/** True for `y` / `yes` (case-insensitive); everything else is a deny (fail closed). */
function isAffirmative(answer: string): boolean {
  const normalized = answer.trim().toLowerCase();
  return normalized === 'y' || normalized === 'yes';
}
