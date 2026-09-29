// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A FAKE native-CLI HOST for the native-cli-bridge spec, run as a SUBPROCESS.
//
// The real shape: a native coding CLI launched by the framework is handed the
// runner's native-CLI tool-bridge as its MCP server, and the CLI connects to
// that bridge over STDIO. This script stands in for the *bridge side of that
// wiring* so the spec can drive it with a real MCP `Client` over a real stdio
// pipe — exactly how a native CLI subprocess would reach it — with NO real Codex
// / Claude-Code binary.
//
// It:
//   1. acquires a per-session sandbox from a runtime chosen by argv
//      (`--runtime in-memory|tmux`),
//   2. writes that sandbox's host-side root dir to the path in `--root-file` so
//      the PARENT test can inspect what the bridge's tools wrote (proving the
//      calls executed IN THE SANDBOX, not on the parent's cwd),
//   3. serves the native-CLI tool-bridge over this process's stdin/stdout, so an
//      MCP `Client` connected to this subprocess drives the sandbox-bound tools.
//
// Run via `tsx` (it imports the runner's TypeScript source directly). It is a
// TEST fake — the FAKE is the native CLI; the bridge + sandbox it exercises are
// the real shipped implementations.

import { writeFileSync } from 'node:fs';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { TmuxSandboxRuntime } from '../../../src/sandbox/tmux-sandbox.js';
import type { SandboxHandle, SandboxRuntime } from '../../../src/sandbox/seam.js';
import { serveNativeCliBridge } from '../../../src/mcp/native-cli-bridge.js';

function argValue(flag: string): string | undefined {
  const idx = process.argv.indexOf(flag);
  return idx >= 0 ? process.argv[idx + 1] : undefined;
}

function makeRuntime(kind: string | undefined): SandboxRuntime {
  return kind === 'tmux' ? new TmuxSandboxRuntime() : new InMemorySandboxRuntime();
}

async function main(): Promise<void> {
  const runtime = makeRuntime(argValue('--runtime'));
  const sandbox: SandboxHandle = await runtime.acquire({});

  // Publish the sandbox root so the parent test can assert on files the bridge's
  // tools created inside the sandbox. `rootDir()` is present on the two reusable
  // runtimes' handles; this fake only ever uses those.
  const rootFile = argValue('--root-file');
  const rootDir = (sandbox as { rootDir?: () => string }).rootDir?.();
  if (rootFile && typeof rootDir === 'string') {
    writeFileSync(rootFile, rootDir, 'utf8');
  }

  // Serve the bridge over this process's stdio. Returns once the transport is
  // connected; the process then lives until stdin closes (the parent Client
  // disconnects) or it is killed.
  const bridge = await serveNativeCliBridge(sandbox);

  const shutdown = async (): Promise<void> => {
    await bridge.close().catch(() => {});
    await sandbox.destroy().catch(() => {});
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
  // When the MCP client closes the stdio transport, end the process cleanly.
  bridge.onClose(() => void shutdown());
}

main().catch((err: unknown) => {
  process.stderr.write(`fake-native-cli-bridge-host failed: ${String(err)}\n`);
  process.exit(1);
});
