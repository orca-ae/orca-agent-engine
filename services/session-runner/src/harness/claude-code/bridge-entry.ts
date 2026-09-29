// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `claude-code` bridge entrypoint — the child process the headless `claude` binary
// spawns as its `orca` MCP server.
//
// The `claude-code` provider wires the runner's native-CLI tool-bridge into the CLI via
// `--mcp-config` as `{ command: node, args: [<this module>, --root <root> …] }`. The CLI
// launches THIS process and speaks MCP-over-stdio to it.
//
// This file holds ONLY the "am I the process entry point" guard; the actual reattach-sandbox +
// serve-bridge logic (+ the flag constants) lives in the GUARD-FREE `bridge-runtime.ts` sibling,
// which this module delegates to. That split is load-bearing, not cosmetic:
// `claude-code/index.ts` (and every sibling native-CLI provider's `index.ts`) imports the flag
// constants to build the bridge child's argv, and `main.ts`'s import graph reaches every one of
// those `index.ts` modules on EVERY runner boot (it registers all providers unconditionally).
// Before the split, that import chain reached THIS module too — and tsup's multi-entry
// `--no-splitting` build bundles each entry point (`main.ts`, `bridge-entry.ts`, …) fully
// self-contained, so importing even one named export from a module drags its ENTIRE body —
// top-level side effects included — into the importer's bundle. Once embedded in `dist/main.js`,
// this guard's `import.meta.url` resolved to `dist/main.js`'s OWN url (bundling collapses
// separate source modules into one real ES module), which always equals `process.argv[1]` for a
// `node dist/main.js` launch — so the guard misfired on EVERY runner boot, self-invoking
// {@link runBridgeEntry} against the runner's own argv and crashing it with "--root is required"
// before any session/provider ever ran. Keeping the guard in this leaf-only module (imported by
// nothing except a human/CLI running `node bridge-entry.js --root …`) keeps it out of
// `main.ts`'s bundle entirely.
//
// It self-invokes {@link runBridgeEntry} when run as the entry script (`node bridge-entry.js
// --root <dir>` — the real usage: the `claude` CLI spawns this file as its MCP bridge child) and
// stays inert when imported (vitest, or a sibling provider's own `bridge-entry.ts`). It is a real
// shipped entrypoint — it runs for every `claude-code` session — not a test fake.

import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { runBridgeEntry } from './bridge-runtime.js';

// Re-export the runtime entry point + flag names so an existing importer of this module (a test
// injecting this file as the bridge command, or a sibling provider's own `bridge-entry.ts`
// re-exporting through it) keeps working unchanged — mirrors the sibling providers' own
// re-export of the shared runtime. A pure `export … from` re-export (no separate local import
// for the flags) so there is no unused-local binding to appease.
export {
  runBridgeEntry,
  BRIDGE_ROOT_FLAG,
  BRIDGE_TMUX_SOCKET_FLAG,
  BRIDGE_ALLOWED_TOOLS_FLAG,
} from './bridge-runtime.js';

/**
 * Whether this module is the process entry point (vs imported by a test). The bridge
 * entry is launched as `node <this module> …` by the CLI; when imported (vitest) it must
 * NOT auto-run. Compares the resolved entry script against this module's own file path.
 */
function isEntryModule(): boolean {
  if (typeof process === 'undefined' || !Array.isArray(process.argv)) {
    return false;
  }
  const entry = process.argv[1];
  if (entry === undefined) {
    return false;
  }
  return resolve(entry) === fileURLToPath(import.meta.url);
}

if (isEntryModule()) {
  runBridgeEntry().catch((err: unknown) => {
    process.stderr.write(`claude-code bridge entry failed: ${String(err)}\n`);
    process.exit(1);
  });
}
