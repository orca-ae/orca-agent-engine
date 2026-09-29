// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The tool-authorization gate, exercised through the REAL terminal IO.
//
// `session-chat.spec.ts` asserts both verdicts already — but against a scripted
// `ChatIo` that hands back pre-decided booleans, so it proves the loop routes a
// verdict, never that the terminal produces the right one. The seam's CONSUMER
// was covered and its security-relevant PRODUCER was not: `isAffirmative` could
// be replaced with `return true`, and the EOF handler's documented fail-closed
// `resolve(false)` with `resolve(true)`, and all 71 tests still passed. Every
// gated tool would have been auto-approved.
//
// So these drive `createTerminalIo` over in-memory streams and assert what the
// operator actually typed maps to.

import { PassThrough } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { createTerminalIo } from '../../src/terminal-io.js';

/** Ask the real gate one question and answer it with `typed`. */
async function confirmWith(typed: string): Promise<boolean> {
  const input = new PassThrough();
  const output = new PassThrough();
  const io = createTerminalIo({ input, output });
  // `confirm` registers the readline question synchronously, so the answer must
  // be written after the call — a line written earlier has no listener yet.
  const verdict = io.confirm('Allow tool Bash? [y/N] ');
  input.write(`${typed}\n`);
  try {
    return await verdict;
  } finally {
    io.close();
  }
}

describe('createTerminalIo().confirm — the tool-authorization gate', () => {
  // Only an explicit yes allows. Case and surrounding whitespace are normalized
  // because a terminal answer carries both.
  it.each(['y', 'Y', 'yes', 'YES', ' y ', '  YES  ', 'Yes'])(
    'allows the gated tool for %j',
    async (typed) => {
      expect(await confirmWith(typed)).toBe(true);
    },
  );

  // Deny by DEFAULT: the prompt is `[y/N]`, so a bare Enter is a deny, and so is
  // anything that merely looks affirmative. `yep` and `yolo` both start with `y`
  // — a prefix or `startsWith` test would allow them, which is the failure this
  // row set exists to catch.
  it.each(['n', 'N', 'no', '', '   ', 'yolo', 'yep', 'ya', 'yeah', 'sure', 'ok', '1', 'true'])(
    'denies the gated tool for %j',
    async (typed) => {
      expect(await confirmWith(typed)).toBe(false);
    },
  );

  // A closed stdin is not consent. The gate must resolve, and resolve DENY:
  // hanging would wedge the session, and allowing would let a piped or
  // backgrounded `oeadm run` silently authorize every tool it is asked about.
  it('fails closed when stdin closes before the question is answered', async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const io = createTerminalIo({ input, output });

    const verdict = io.confirm('Allow tool Bash? [y/N] ');
    input.end();

    expect(await verdict).toBe(false);
    io.close();
  });

  it('reads consecutive verdicts from one interface, in order', async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const io = createTerminalIo({ input, output });

    const first = io.confirm('Allow tool Bash? [y/N] ');
    input.write('y\n');
    expect(await first).toBe(true);

    const second = io.confirm('Allow tool WriteFile? [y/N] ');
    input.write('n\n');
    expect(await second).toBe(false);

    io.close();
  });
});

describe('createTerminalIo().readLine', () => {
  it('resolves the typed line', async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const io = createTerminalIo({ input, output });

    const line = io.readLine('you> ');
    input.write('what is 2 + 2?\n');

    expect(await line).toBe('what is 2 + 2?');
    io.close();
  });

  // `null` — not `''` — is the loop's EOF signal; an empty string means "the
  // operator pressed Enter", which re-prompts instead of ending the session.
  it('resolves null at EOF so the chat loop ends rather than looping on a blank', async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const io = createTerminalIo({ input, output });

    const line = io.readLine('you> ');
    input.end();

    expect(await line).toBeNull();
    io.close();
  });
});

describe('createTerminalIo().write', () => {
  it('writes the text with a trailing newline to the output stream', async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const chunks: Buffer[] = [];
    output.on('data', (c: Buffer) => chunks.push(c));

    const io = createTerminalIo({ input, output });
    io.write('hello');
    io.close();

    expect(Buffer.concat(chunks).toString()).toContain('hello\n');
  });
});
