// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Entry point for the `oeadm` binary. `dispatch` is the pure command router (no
// network, no process globals) so it is unit tested directly; `main` wires the
// real client + terminal IO to the routed handlers and is the `bin` target.
//
// One CLI, many subcommands (a single `oeadm` binary that dispatches on the first
// positional):
//   oeadm run    --agent <id> --environment <id>
//   oeadm attach --session <id> [--terminal <id>]
//   oeadm env    create --name <n> [--target self_hosted]
//   oeadm worker --environment <id> --registry <url> ...   (ENVIRONMENT_KEY in the env)

import { resolveClientConfig, OrcaClient } from './client.js';
import { paletteFor, type Palette } from './colors.js';
import { runCommand } from './commands/run.js';
import { attachCommand } from './commands/attach.js';
import { envCommand } from './commands/env.js';
import { workerCommand } from './commands/worker.js';
import { createTerminalIo } from './terminal-io.js';

/** The four subcommand handlers, injected into {@link dispatch} for testing. */
export interface Dispatchers {
  run(args: string[]): Promise<void>;
  attach(args: string[]): Promise<void>;
  env(args: string[]): Promise<void>;
  worker(args: string[]): Promise<void>;
}

// The `worker` line deliberately shows the Env Key arriving through
// `ENVIRONMENT_KEY` rather than `--env-key`. Both work — the flag is kept for
// scripts that already pass it — but a value on the command line is visible in
// `ps` output to every user on the host and is written verbatim into shell
// history, and an Env Key authenticates a worker to the registry. Advertising the
// flag made the exposed path the obvious one; the env var is what the usage now
// teaches.
const USAGE = `oeadm — a client for the Orca Managed Agents registry.

Usage:
  oeadm run    --agent <id> [--environment <id>]      Create a session and chat interactively.
  oeadm attach --session <id> [--terminal <id>]       Join an existing session (or a running terminal).
  oeadm env    create --name <n> [--target <t>]       Create an environment; prints its env_key once.
  oeadm worker --environment <id> --registry <url> \\
               --workspace-dir <dir> \\
               --runner-command <cmd>                 Run the self-hosted environment worker.

Environment:
  ORCA_BASE_URL    Registry base URL (default http://localhost:8080).
  ORCA_API_KEY     Workspace api key (required for run/attach/env).
  ENVIRONMENT_KEY  Env Key for \`oeadm worker\`. Prefer this over the --env-key flag:
                   a key passed as an argument is visible in \`ps\` and in shell history.`;

/**
 * Route `argv` (already stripped of node + script) to a subcommand handler.
 * Returns the process exit code. Unknown / help / empty input prints usage;
 * unknown is a non-zero exit, help and empty are zero.
 */
export async function dispatch(
  argv: readonly string[],
  dispatchers: Dispatchers,
  write: (text: string) => void,
): Promise<number> {
  const [command, ...rest] = argv;

  if (command === undefined || command === '--help' || command === '-h' || command === 'help') {
    write(USAGE);
    return 0;
  }

  switch (command) {
    case 'run':
      await dispatchers.run(rest);
      return 0;
    case 'attach':
      await dispatchers.attach(rest);
      return 0;
    case 'env':
      await dispatchers.env(rest);
      return 0;
    case 'worker':
      await dispatchers.worker(rest);
      return 0;
    default:
      write(`unknown command: ${command}\n\n${USAGE}`);
      return 1;
  }
}

/**
 * Process-level seams, so the two things that decide what the SHELL sees — the
 * error→exit-1 mapping and `oeadm worker`'s exit-code propagation — are unit
 * tested rather than assumed. Both were previously reachable only through the
 * real terminal, the real network and the real `process`, and both survived
 * being deleted with the whole suite green.
 */
export interface CliIo {
  stdout: (text: string) => void;
  stderr: (text: string) => void;
  colors: Palette;
  /** Record a non-zero code a subcommand produced without throwing. */
  setExitCode: (code: number) => void;
}

/**
 * Route `argv` and map ANY thrown error to exit 1, printing `error: <message>`
 * on stderr.
 *
 * This is the CLI's whole failure contract: a subcommand reports failure by
 * throwing, and this is the one place that becomes a non-zero status. Nothing
 * else in the CLI reads an exit code, so if this returns 0 for a thrown error,
 * every failing subcommand reports success to the shell.
 */
export async function run(
  argv: readonly string[],
  dispatchers: Dispatchers,
  io: CliIo,
): Promise<number> {
  try {
    return await dispatch(argv, dispatchers, io.stdout);
  } catch (err) {
    io.stderr(io.colors.red(`error: ${err instanceof Error ? err.message : String(err)}`));
    return 1;
  }
}

/**
 * Build the four subcommand handlers over `io` and a worker launcher.
 *
 * `runWorker` is a seam because `oeadm worker` is the one subcommand that reports
 * failure with a CODE rather than a throw: it proxies a long-running child, and
 * that child's exit status is the operator's result. Propagating it is a single
 * statement that no test could reach while `workerCommand` was called directly.
 */
export function buildDispatchers(
  io: CliIo,
  runWorker: (args: string[]) => Promise<number>,
): Dispatchers {
  return {
    run: async (args) => {
      const client = new OrcaClient(resolveClientConfig());
      const terminal = createTerminalIo();
      try {
        await runCommand({ args, client, io: terminal, colors: io.colors });
      } finally {
        terminal.close();
      }
    },
    attach: async (args) => {
      const config = resolveClientConfig();
      const client = new OrcaClient(config);
      const terminal = createTerminalIo();
      try {
        await attachCommand({ args, client, io: terminal, colors: io.colors, config });
      } finally {
        terminal.close();
      }
    },
    env: async (args) => {
      const client = new OrcaClient(resolveClientConfig());
      await envCommand({ args, client, write: io.stdout });
    },
    worker: async (args) => {
      const code = await runWorker(args);
      if (code !== 0) io.setExitCode(code);
    },
  };
}

/**
 * Wire the real dependencies and run the CLI. Resolves the client config lazily
 * per command (so `worker`, which needs no api key, runs without `ORCA_API_KEY`)
 * and returns the process exit code. Errors are printed and mapped to exit 1.
 */
export async function main(argv: readonly string[]): Promise<number> {
  const io: CliIo = {
    stdout: (text) => {
      process.stdout.write(`${text}\n`);
    },
    stderr: (text) => {
      process.stderr.write(`${text}\n`);
    },
    colors: paletteFor(process.stdout),
    setExitCode: (code) => {
      process.exitCode = code;
    },
  };

  return run(
    argv,
    buildDispatchers(io, (args) => workerCommand({ args })),
    io,
  );
}
