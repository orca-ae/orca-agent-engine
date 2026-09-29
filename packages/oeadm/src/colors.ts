// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Tiny ANSI palette. The CLI's terminal styling is intentionally dependency-free
// (node built-ins preferred over `chalk`): a `Palette` is a set of string→string
// wrappers, and {@link noColor} is the identity palette tests assert against for
// stable, escape-free output.

/** A styling palette: each entry wraps text in an ANSI style (or passes through). */
export interface Palette {
  bold(text: string): string;
  dim(text: string): string;
  cyan(text: string): string;
  green(text: string): string;
  yellow(text: string): string;
  red(text: string): string;
  magenta(text: string): string;
}

const RESET = '[0m';

function wrap(code: string): (text: string) => string {
  const open = `[${code}m`;
  return (text: string) => `${open}${text}${RESET}`;
}

/** ANSI-colored palette for an interactive TTY. */
export const ansiColor: Palette = {
  bold: wrap('1'),
  dim: wrap('2'),
  cyan: wrap('36'),
  green: wrap('32'),
  yellow: wrap('33'),
  red: wrap('31'),
  magenta: wrap('35'),
};

/** Identity palette — no escapes. Used when stdout is not a TTY, and in tests. */
export const noColor: Palette = {
  bold: (t) => t,
  dim: (t) => t,
  cyan: (t) => t,
  green: (t) => t,
  yellow: (t) => t,
  red: (t) => t,
  magenta: (t) => t,
};

/**
 * Pick a palette for a stream: colored when the stream is an interactive TTY and
 * `NO_COLOR` is not set (the de-facto standard env opt-out), else the identity
 * palette. Keeps color decisions out of the render functions.
 */
export function paletteFor(
  stream: { isTTY?: boolean },
  env: NodeJS.ProcessEnv = process.env,
): Palette {
  if (env['NO_COLOR'] !== undefined && env['NO_COLOR'] !== '') return noColor;
  return stream.isTTY ? ansiColor : noColor;
}
