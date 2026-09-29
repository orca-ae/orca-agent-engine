// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A minimal, dependency-free argument parser (node built-ins preferred over an
// external arg library). It recognizes GNU-style long flags — `--flag value`,
// `--flag=value`, and a bare `--flag` (boolean true) — and collects everything
// else as positionals in order. That is exactly the surface the CLI needs:
// `oeadm run --agent X --environment Y`, `oeadm env create --name Z`.

/** Parsed argv: order-preserving positionals plus a flag→value map. */
export interface ParsedArgs {
  positionals: string[];
  options: Record<string, string | boolean>;
}

/**
 * Parse an argv slice (already stripped of `node` + script). A long flag with no
 * following value — because argv ended or the next token is itself a flag —
 * becomes boolean `true`; otherwise it takes the next token as its value.
 * `--flag=value` binds inline. Non-flag tokens are positionals.
 */
export function parseArgs(argv: readonly string[]): ParsedArgs {
  const positionals: string[] = [];
  const options: Record<string, string | boolean> = {};

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i]!;
    if (!token.startsWith('--')) {
      positionals.push(token);
      continue;
    }
    const body = token.slice(2);
    const eq = body.indexOf('=');
    if (eq >= 0) {
      options[body.slice(0, eq)] = body.slice(eq + 1);
      continue;
    }
    const next = argv[i + 1];
    if (next === undefined || next.startsWith('--')) {
      options[body] = true;
    } else {
      options[body] = next;
      i++;
    }
  }

  return { positionals, options };
}

/**
 * Read a required string option, throwing a caller-friendly error naming the
 * missing flag. A boolean-present flag (`--name` with no value) counts as
 * missing — these options always carry a value.
 */
export function requireString(options: Record<string, string | boolean>, name: string): string {
  const value = options[name];
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`missing required option: --${name}`);
  }
  return value;
}

/** Read an optional string option, or `undefined` when absent / boolean. */
export function optionalString(
  options: Record<string, string | boolean>,
  name: string,
): string | undefined {
  const value = options[name];
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}
