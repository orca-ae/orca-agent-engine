// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// `oeadm env create --name <n> [--target self_hosted]` — create an environment
// over `POST /v1/environments` and print the returned `env_key` exactly ONCE
// (the registry echoes the raw key only on create; it is unrecoverable
// afterward). The operator copies it to wire a worker (`oeadm worker`).
//
// `env` is a command group; today it has a single `create` subcommand.

import { parseArgs, requireString, optionalString } from '../args.js';
import type { OrcaClient } from '../client.js';

/** Default environment target for the self-hosted worker flow this CLI serves. */
const DEFAULT_TARGET = 'self_hosted';

/** Options for the `env` command group dispatcher. */
export interface EnvCommandOptions {
  args: readonly string[];
  client: OrcaClient;
  write: (text: string) => void;
}

/**
 * Dispatch the `env` subcommand. Only `create` is defined; anything else (or
 * nothing) throws a usage error the caller surfaces.
 */
export async function envCommand(opts: EnvCommandOptions): Promise<void> {
  const sub = opts.args[0];
  if (sub === 'create') {
    return envCreateCommand({ args: opts.args.slice(1), client: opts.client, write: opts.write });
  }
  throw new Error(`unknown env subcommand: ${sub ?? '(none)'} — expected \`env create\``);
}

/** Options for {@link envCreateCommand}. */
export interface EnvCreateCommandOptions {
  args: readonly string[];
  client: OrcaClient;
  write: (text: string) => void;
}

/**
 * Create an environment from `--name` (default target `self_hosted`, overridable
 * with `--target`) and print the id + the one-time `env_key`. Throws when
 * `--name` is missing.
 */
export async function envCreateCommand(opts: EnvCreateCommandOptions): Promise<void> {
  const { options } = parseArgs(opts.args);
  const name = requireString(options, 'name');
  const target = optionalString(options, 'target') ?? DEFAULT_TARGET;

  const env = await opts.client.createEnvironment({ name, target });

  opts.write(`environment ${env.id} (${env.name}) target=${env.target ?? 'null'}`);
  if (typeof env.env_key !== 'string' || env.env_key.length === 0) {
    // THROW, not warn. Handing back the one-time key is this command's entire
    // purpose — the registry echoes the raw key on create and never again, so a
    // create that returns none has produced an environment no worker can ever be
    // wired to, and the failure is unrecoverable from here. Exiting 0 with a
    // warning buried in stdout let a script treat that as success.
    throw new Error(
      `environment ${env.id} was created but the registry returned no env_key. ` +
        'The raw key is echoed only on create, so this environment cannot be wired to a ' +
        'worker; rotate its key, or delete it and create another.',
    );
  }
  // Printed exactly ONCE — the registry never returns the raw key again. The
  // worker-wiring hint below references the key by name, not by value, so the
  // raw secret appears in the output a single time.
  opts.write('');
  opts.write('env_key (shown once — save it to wire a worker):');
  opts.write(`  ${env.env_key}`);
  opts.write('');
  opts.write(
    `Wire a worker with:  ENVIRONMENT_KEY=<env_key> oeadm worker --environment ${env.id} ` +
      `--registry <url> --workspace-dir <dir> --runner-command <cmd>`,
  );
}
