#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `oeadm` binary entry. This file exists solely to run {@link main} when the
// CLI is invoked directly; all logic lives in the library modules so it stays
// unit tested. Kept as a dedicated, self-contained entry (the build does not
// code-split it) so the top-level invocation is never hoisted out of the file
// node actually executes.

import { main } from './main.js';

main(process.argv.slice(2))
  .then((code) => {
    if (code !== 0) process.exitCode = code;
  })
  .catch((err: unknown) => {
    process.stderr.write(`fatal: ${err instanceof Error ? err.message : String(err)}\n`);
    process.exitCode = 1;
  });
