// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { BuiltinEvaluator, EvaluatorContext } from '../engine.js';
import {
  commandName,
  isShellInterpreter,
  parseShellCommands,
  type ParsedCommand,
  type Redirection,
} from '../shell.js';
import { OS_SHELL_TOOLS } from '../tool-names.js';
import { ask, deny, stringList, stringParam, toolMatches } from './helpers.js';

/**
 * Shell predicates.
 *
 * Every rule here decides on *parsed commands* rather than on the command
 * string. Substring matching is the wrong tool twice over: `git commit -m
 * "rm -rf /"` trips it when nothing dangerous is happening, and
 * `sudo bash -c "rm -rf /"` slips past it while something dangerous is. See
 * `../shell.ts` for what the reader can and cannot resolve; the short version
 * is that unresolvable text stays literal, so a rule that turns a word into a
 * path must treat what it cannot resolve as outside anywhere it allows.
 */

function booleanParam(params: Record<string, unknown>, key: string, fallback: boolean): boolean {
  const value = params[key];
  return typeof value === 'boolean' ? value : fallback;
}

function enumParam<T extends string>(
  params: Record<string, unknown>,
  key: string,
  allowed: readonly T[],
  fallback: T,
): T {
  const value = params[key];
  return typeof value === 'string' && (allowed as readonly string[]).includes(value)
    ? (value as T)
    : fallback;
}

/**
 * The shell invocation this event carries, or `undefined` when there is none to
 * judge — a different tool, or a shell call with no command in it. Abstaining
 * on the second is deliberate: a call that runs nothing has no blast radius,
 * and denying it would report a violation nobody committed.
 */
function shellInvocation(
  ctx: EvaluatorContext,
): { command: string; commands: ParsedCommand[] } | undefined {
  if (!toolMatches(ctx, OS_SHELL_TOOLS)) return undefined;
  const command = ctx.event.tool?.input?.['command'];
  if (typeof command !== 'string' || command.trim().length === 0) return undefined;
  return { command, commands: parseShellCommands(command) };
}

// ------------------------------------------------------------------- flags

/** The letters of a short-option bundle: `-rf` gives `rf`, `--force` gives ''. */
function shortLetters(token: string): string {
  if (token.length < 2 || !token.startsWith('-') || token.startsWith('--')) return '';
  return token.slice(1);
}

function hasShortFlag(argv: readonly string[], letters: readonly string[]): boolean {
  return argv.some(
    (token, index) => index > 0 && letters.some((letter) => shortLetters(token).includes(letter)),
  );
}

function hasLongFlag(argv: readonly string[], names: readonly string[]): boolean {
  return argv.some(
    (token, index) =>
      index > 0 && names.some((name) => token === name || token.startsWith(`${name}=`)),
  );
}

function hasFlag(
  argv: readonly string[],
  letters: readonly string[],
  names: readonly string[],
): boolean {
  return hasShortFlag(argv, letters) || hasLongFlag(argv, names);
}

/** Words that are not options and are not values of an option. */
function operands(argv: readonly string[], valueFlags: readonly string[] = []): string[] {
  const found: string[] = [];
  let optionsEnded = false;
  for (let i = 1; i < argv.length; i++) {
    const token = argv[i] as string;
    if (!optionsEnded && token === '--') {
      optionsEnded = true;
      continue;
    }
    if (!optionsEnded && token.startsWith('-') && token.length > 1) {
      if (optionConsumesNext(token, valueFlags)) i += 1;
      continue;
    }
    found.push(token);
  }
  return found;
}

/** Whether an option's value is the following token rather than attached to it. */
function optionConsumesNext(token: string, valueFlags: readonly string[]): boolean {
  if (valueFlags.includes(token)) return true;
  if (!token.startsWith('-') || token.startsWith('--')) return false;
  for (const flag of valueFlags) {
    if (flag.length !== 2 || !flag.startsWith('-')) continue;
    const index = token.indexOf(flag[1] as string, 1);
    if (index !== -1) return index === token.length - 1;
  }
  return false;
}

// -------------------------------------------------------------------- git

/**
 * The subcommand of a git invocation, past any global options. `git -C /elsewhere
 * push --force` is a push, and a rule that only reads `argv[1]` does not see it.
 */
const GIT_GLOBAL_VALUE_FLAGS = [
  '-C',
  '-c',
  '--config-env',
  '--git-dir',
  '--work-tree',
  '--namespace',
  '--exec-path',
];

function gitSubcommand(argv: readonly string[]): string | undefined {
  if (commandName(argv) !== 'git') return undefined;
  return operands(argv, GIT_GLOBAL_VALUE_FLAGS)[0];
}

/** Operands of a git subcommand, past the subcommand itself. */
function gitOperands(argv: readonly string[]): string[] {
  return operands(argv, GIT_GLOBAL_VALUE_FLAGS).slice(1);
}

// ---------------------------------------------------------------- paths

/**
 * Resolve `.` and `..` lexically. Nothing here touches a filesystem or a
 * working directory — the package performs no I/O — so a word that depends on
 * either (`$HOME/x`, `~/x`, `*`) stays literal and will not match any root.
 */
function normalizePath(path: string): string {
  const absolute = path.startsWith('/');
  const out: string[] = [];
  for (const segment of path.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') {
      const top = out[out.length - 1];
      if (out.length > 0 && top !== '..') out.pop();
      else if (!absolute) out.push('..');
      continue;
    }
    out.push(segment);
  }
  return (absolute ? '/' : '') + out.join('/');
}

/**
 * True when `path` is `root` or sits under it. An absolute path is never inside
 * a relative root and vice versa: without a working directory the two cannot be
 * compared, and guessing would be guessing in the permissive direction.
 */
function isWithin(path: string, root: string): boolean {
  if (path.length === 0) return false;
  const target = normalizePath(path);
  const base = normalizePath(root);
  if (base === '' || base === '/')
    return base === '/'
      ? target.startsWith('/')
      : !target.startsWith('..') && !target.startsWith('/');
  return target === base || target.startsWith(`${base}/`);
}

function isWithinAny(path: string, roots: readonly string[]): boolean {
  return roots.some((root) => isWithin(path, root));
}

// ------------------------------------------------------------ blast radius

/** Devices that sink or source data without destroying anything. */
const HARMLESS_DEVICES: ReadonlySet<string> = new Set([
  '/dev/null',
  '/dev/zero',
  '/dev/stdout',
  '/dev/stderr',
  '/dev/stdin',
  '/dev/tty',
  '/dev/random',
  '/dev/urandom',
]);

function isDestructiveDevice(path: string): boolean {
  return path.startsWith('/dev/') && !HARMLESS_DEVICES.has(path) && !path.startsWith('/dev/fd/');
}

/** A `/dev` sink that swallows what is written to it, containing nothing. */
function isHarmlessSink(path: string): boolean {
  return path.startsWith('/dev/') && !isDestructiveDevice(path);
}

/** Redirection operators that write, including the `>&`/`2>&` forms. */
const WRITE_REDIRECTION = /^(\d*>>?|\d*>\||&>>?|\d*>&)$/;

/**
 * A `>&`/`2>&` whose target is a bare descriptor number (`2>&1`, `>&2`) only
 * duplicates a descriptor — it writes no file. One whose target is a name
 * (`>& /etc/motd`) does write, so it is not excused here.
 */
const DESCRIPTOR_DUP = /^\d*>&$/;

/** Targets of every writing redirection the command carries. */
function redirectionTargets(redirections: readonly Redirection[]): string[] {
  const targets: string[] = [];
  for (const { op, target } of redirections) {
    if (!WRITE_REDIRECTION.test(op)) continue;
    if (DESCRIPTOR_DUP.test(op) && /^\d+$/.test(target)) continue;
    // Redirecting into `/dev/null` and its kin discards the output; there is no
    // file to contain, so it is not an escaping write.
    if (isHarmlessSink(target)) continue;
    targets.push(target);
  }
  return targets;
}

/** An identifier character, for reading a fork-bomb function name backwards. */
const IDENT_CHAR = /[A-Za-z0-9_]/;
/** What may precede a function name: the start of a command, not a glued token. */
const FORK_BOUNDARY = /[\s;&|(){}]/;

/** Advance past whitespace that may separate the shape's tokens — a body may
 * span lines (`f() {\n f|f& \n}`), so newlines count here, unlike the name scan. */
function skipBlanks(s: string, i: number): number {
  while (i < s.length && /\s/.test(s[i] as string)) i += 1;
  return i;
}

/**
 * A shell function that calls itself twice in the background — `f(){f|f&}`, the
 * classic resource-exhaustion one-liner. Scans for `(` and checks the exact shape
 * around it in constant time per candidate, rather than a backreference regex:
 * the regex form backtracks over a long word run, and any fixed bound on the name
 * length just moves the evasion out past it.
 *
 * The scan runs over the raw command, not a whitespace-stripped copy. Stripping
 * whitespace glued the preceding token onto the name — so a definition on its own
 * line (`echo hi\nb(){ b|b& };b`) or after an assignment (`X=1 b(){…}`), the
 * normal way to write a multi-line script, read as a different name and slipped
 * through. Blanks between the shape's own tokens are tolerated by skipping them.
 */
function isForkBomb(command: string): boolean {
  for (let i = command.indexOf('('); i !== -1; i = command.indexOf('(', i + 1)) {
    // The name sits immediately left of `(`, across optional blanks (`f (){`): a
    // run of identifier characters, or the single-character name `:`.
    let j = i - 1;
    while (j >= 0 && (command[j] === ' ' || command[j] === '\t')) j -= 1;
    const nameEnd = j + 1;
    while (j >= 0 && IDENT_CHAR.test(command[j] as string)) j -= 1;
    let name = command.slice(j + 1, nameEnd);
    let before = j;
    if (name.length === 0) {
      if (nameEnd > 0 && command[nameEnd - 1] === ':') {
        name = ':';
        before = nameEnd - 2;
      } else continue;
    }
    // The name must begin a command — preceded by the start of the string or a
    // separator — so a suffix of a preceding token is not read as the name.
    if (before >= 0 && !FORK_BOUNDARY.test(command[before] as string)) continue;
    // Body: `( ) { name | name & }`, blanks tolerated between every token.
    let k = skipBlanks(command, i + 1);
    if (command[k] !== ')') continue;
    k = skipBlanks(command, k + 1);
    if (command[k] !== '{') continue;
    k = skipBlanks(command, k + 1);
    if (command.slice(k, k + name.length) !== name) continue;
    k = skipBlanks(command, k + name.length);
    if (command[k] !== '|') continue;
    k = skipBlanks(command, k + 1);
    if (command.slice(k, k + name.length) !== name) continue;
    k = skipBlanks(command, k + name.length);
    if (command[k] !== '&') continue;
    return true;
  }
  return false;
}

function isCatastrophic(cmd: ParsedCommand): boolean {
  const name = commandName(cmd.argv);

  // Recursive removal. Force is not required: `-f` only suppresses the prompt,
  // it does not reduce what is deleted, so `rm -r /` is as final as `rm -rf /`.
  // The target is not inspected on purpose: a recursive `rm` with a path this
  // reader cannot resolve is precisely the case that has to be caught, so
  // narrowing by target would narrow it away.
  if (name === 'rm' && hasFlag(cmd.argv, ['r', 'R'], ['--recursive'])) {
    return true;
  }

  // `find <paths> -delete` removes every match under its paths — a recursive
  // deletion by another name, as final as `rm -rf`.
  if (name === 'find' && cmd.argv.includes('-delete')) {
    return true;
  }

  // Writing an image over a device, whether through dd, redirection, or an
  // ordinary writer such as cp/tee/truncate/shred.
  if (writeTargets(cmd)?.some(isDestructiveDevice)) return true;

  // Making a filesystem, which discards whatever was there.
  if (name.startsWith('mkfs')) return true;

  return false;
}

const DOWNLOADERS: ReadonlySet<string> = new Set(['curl', 'wget', 'fetch', 'aria2c', 'httpie']);
const REMOTE_URL = /^[a-z][a-z0-9+.-]*:\/\//i;

/**
 * Commands that read or search their arguments rather than fetch them: a URL
 * passed to one of these is a pattern or a filename, not a download, so it must
 * not turn `grep https://… | sh` into a spurious download-into-shell.
 */
const NON_DOWNLOADERS: ReadonlySet<string> = new Set([
  'echo',
  'printf',
  'grep',
  'egrep',
  'fgrep',
  'rg',
  'cat',
  'awk',
  'sed',
  'cut',
  'tr',
  'head',
  'tail',
  'sort',
  'uniq',
]);

function isRemoteDownload(cmd: ParsedCommand): boolean {
  const name = commandName(cmd.argv);
  if (DOWNLOADERS.has(name)) return true;
  // A URL argument marks an unknown command as a probable downloader, but never a
  // command whose job is to read or print its arguments.
  if (NON_DOWNLOADERS.has(name)) return false;
  return cmd.argv.slice(1).some((arg) => REMOTE_URL.test(arg));
}

/** Redirections that feed a command's input — where a fetched script would enter a shell. */
const INPUT_REDIRECTIONS: ReadonlySet<string> = new Set(['<', '<<', '<<-', '<<<', '<&', '<>']);

/** `source` and `.` run their argument as shell code in the current shell. */
const SHELL_SOURCERS: ReadonlySet<string> = new Set(['source', '.']);

/** A command that executes shell code given as input — an interpreter or a sourcer. */
function executesShellInput(argv: readonly string[]): boolean {
  return isShellInterpreter(argv) || SHELL_SOURCERS.has(commandName(argv));
}

/**
 * A remote download executed unseen by a shell, so nothing about it can be gated.
 *
 * The direct form is a pipe — `curl … | sh`. Process substitution splits the two
 * apart: `sh < <(curl …)` and `bash <(curl …)` surface the downloader in its own
 * pipeline and leave the shell interpreter reading from an input redirection the
 * reader could not resolve (an empty target). A shell fed an unresolvable input
 * while a downloader is present is the same fetch-and-run, just not through a pipe.
 */
function pipesDownloadIntoShell(commands: readonly ParsedCommand[]): boolean {
  const byPipeline = new Map<number, ParsedCommand[]>();
  for (const cmd of commands) {
    const stages = byPipeline.get(cmd.pipeline) ?? [];
    stages.push(cmd);
    byPipeline.set(cmd.pipeline, stages);
  }
  for (const stages of byPipeline.values()) {
    if (stages.length < 2) continue;
    if (stages.some(isRemoteDownload) && stages.some((s) => isShellInterpreter(s.argv)))
      return true;
  }

  if (!commands.some(isRemoteDownload)) return false;
  return commands.some(
    (cmd) =>
      executesShellInput(cmd.argv) &&
      cmd.redirections.some((r) => INPUT_REDIRECTIONS.has(r.op) && r.target === ''),
  );
}

/** Git subcommands that rewrite history rather than adding to it. */
const HISTORY_REWRITES: ReadonlySet<string> = new Set(['rebase', 'filter-branch', 'filter-repo']);

function riskyDescription(cmd: ParsedCommand, gatePushes: boolean): string | undefined {
  const name = commandName(cmd.argv);
  const subcommand = gitSubcommand(cmd.argv);

  if (subcommand !== undefined) {
    // `gate_pushes` covers pushes as a class, force pushes included: a push is
    // the point at which local damage becomes everyone's.
    if (subcommand === 'push' && gatePushes) return 'pushes to a remote';
    if (HISTORY_REWRITES.has(subcommand)) return 'rewrites git history';
    if (subcommand === 'commit' && hasLongFlag(cmd.argv, ['--amend']))
      return 'rewrites git history';
    if (subcommand === 'reflog' && gitOperands(cmd.argv)[0] === 'expire') {
      return 'discards the git reflog';
    }
    if (subcommand === 'reset' && hasLongFlag(cmd.argv, ['--hard'])) {
      return 'discards uncommitted work';
    }
    return undefined;
  }

  if (name === 'chmod' || name === 'chown' || name === 'chgrp') {
    if (hasFlag(cmd.argv, ['R'], ['--recursive'])) return 'changes permissions across a tree';
    // A world-writable mode is a mass change whether or not it recurses.
    if (name === 'chmod' && operands(cmd.argv).some((o) => /^0?777$/.test(o) || o === 'a+rwx')) {
      return 'grants unrestricted permissions';
    }
  }
  return undefined;
}

export const blastRadius: BuiltinEvaluator = (ctx) => {
  const invocation = shellInvocation(ctx);
  if (!invocation) return undefined;
  const { command, commands } = invocation;
  const configuredReason = stringParam(ctx.params, 'deny_reason');

  // A command nested past what the reader can follow hides whatever it would run
  // (see `unresolved` in ../shell.ts), so it is as good as catastrophic: a chain
  // that deep is never legitimate, and abstaining would pass it straight through.
  const unresolved = commands.find((cmd) => cmd.unresolved);
  if (unresolved) {
    return deny(
      configuredReason ??
        `\`${unresolved.raw}\` nests too deeply to inspect, so what it would run cannot be judged.`,
    );
  }

  const catastrophic = commands.find(isCatastrophic);
  if (catastrophic || isForkBomb(command) || pipesDownloadIntoShell(commands)) {
    const what = catastrophic ? `\`${catastrophic.raw}\`` : 'This command';
    return deny(configuredReason ?? `${what} is irreversible at the scale of the machine.`);
  }

  const gatePushes = booleanParam(ctx.params, 'gate_pushes', true);
  for (const cmd of commands) {
    const description = riskyDescription(cmd, gatePushes);
    if (description === undefined) continue;
    const reason = `\`${cmd.raw}\` ${description}.`;
    return enumParam(ctx.params, 'risky_action', ['ask', 'deny'] as const, 'ask') === 'deny'
      ? deny(configuredReason ?? reason)
      : ask(reason);
  }
  return undefined;
};

// ------------------------------------------------- working directory changes

/** Commands that move the shell's own working directory. */
const DIRECTORY_COMMANDS: ReadonlySet<string> = new Set(['cd', 'chdir', 'pushd', 'popd']);

/** Worktree subcommands that create or relocate a checkout. */
const WORKTREE_MOVES: ReadonlySet<string> = new Set(['add', 'move', 'remove']);
const WORKTREE_VALUE_FLAGS = ['-b', '-B', '--reason'];

interface DirectoryChange {
  description: string;
  /** Absent when the destination is not in the command — `cd`, `cd -`, `popd`. */
  target?: string;
}

/**
 * The directory of a global `git -C <dir>`, `''` when the flag names none, or
 * `undefined` when there is no global `-C`. The scan stops at the first
 * non-option word — the subcommand — so a subcommand's own `-C` is not read as
 * git's global directory flag.
 */
function globalDashC(argv: readonly string[]): string | undefined {
  for (let i = 1; i < argv.length; i++) {
    const token = argv[i] as string;
    if (!token.startsWith('-')) return undefined; // reached the subcommand
    if (token === '-C') return argv[i + 1] ?? '';
    if (GIT_GLOBAL_VALUE_FLAGS.includes(token)) i += 1; // skip this flag's value
  }
  return undefined;
}

function directoryChange(
  cmd: ParsedCommand,
  blockCd: boolean,
  blockWorktree: boolean,
): DirectoryChange | undefined {
  const name = commandName(cmd.argv);

  if (blockCd && DIRECTORY_COMMANDS.has(name)) {
    const target = operands(cmd.argv)[0];
    // `cd` alone goes home and `cd -` goes back; neither destination is in the
    // command, so neither can be shown to be allowed.
    return target === undefined || target === '-'
      ? { description: `\`${cmd.raw}\` moves the working directory` }
      : { description: `\`${cmd.raw}\` moves the working directory`, target };
  }

  const subcommand = gitSubcommand(cmd.argv);
  if (subcommand === undefined) return undefined;

  if (blockCd) {
    // `git -C dir` is a per-command working directory — but only as a *global*
    // option, before the subcommand. `git commit -C <commit>` (reuse a message)
    // and `git branch -C <old> <new>` (copy a branch) carry their own `-C` that
    // is not a directory change, so the scan stops at the subcommand.
    const globalDir = globalDashC(cmd.argv);
    if (globalDir !== undefined) {
      const description = `\`${cmd.raw}\` runs against another directory`;
      return globalDir === '' ? { description } : { description, target: globalDir };
    }
  }

  if (blockWorktree && subcommand === 'worktree') {
    const rawWorktreeArgs = argsAfterGitSubcommand(cmd.argv);
    const [action, firstPath, secondPath] = operands(
      ['worktree', ...rawWorktreeArgs],
      WORKTREE_VALUE_FLAGS,
    );
    if (action !== undefined && WORKTREE_MOVES.has(action)) {
      const description = `\`${cmd.raw}\` relocates a worktree`;
      // `add` and `remove` name one relevant path. `move` names the existing
      // worktree first and its destination second; only the destination decides
      // whether the relocation stays inside an allowed directory.
      const path = action === 'move' ? secondPath : firstPath;
      return path === undefined ? { description } : { description, target: path };
    }
  }
  return undefined;
}

/** Raw arguments after Git's global options and the selected subcommand. */
function argsAfterGitSubcommand(argv: readonly string[]): string[] {
  if (commandName(argv) !== 'git') return [];
  let optionsEnded = false;
  for (let i = 1; i < argv.length; i++) {
    const token = argv[i] as string;
    if (!optionsEnded && token === '--') {
      optionsEnded = true;
      continue;
    }
    if (!optionsEnded && token.startsWith('-') && token.length > 1) {
      if (optionConsumesNext(token, GIT_GLOBAL_VALUE_FLAGS)) i += 1;
      continue;
    }
    return argv.slice(i + 1);
  }
  return [];
}

export const blockWorkingDirChanges: BuiltinEvaluator = (ctx) => {
  const invocation = shellInvocation(ctx);
  if (!invocation) return undefined;

  const blockCd = booleanParam(ctx.params, 'block_cd', true);
  const blockWorktree = booleanParam(ctx.params, 'block_worktree', true);
  const allowed = stringList(ctx.params, 'allowed_dirs');

  for (const cmd of invocation.commands) {
    // A command nested too deep to read (see `unresolved` in ../shell.ts) could
    // move the shell anywhere, so it cannot be shown to stay put.
    if (cmd.unresolved) {
      const reason = `\`${cmd.raw}\` nests too deeply to inspect, so it cannot be shown to leave the working directory in place.`;
      return enumParam(ctx.params, 'action', ['deny', 'ask'] as const, 'deny') === 'ask'
        ? ask(reason)
        : deny(reason);
    }
    const change = directoryChange(cmd, blockCd, blockWorktree);
    if (!change) continue;
    if (change.target !== undefined && isWithinAny(change.target, allowed)) continue;
    const reason = `${change.description}, which the other shell guardrails are written against.`;
    return enumParam(ctx.params, 'action', ['deny', 'ask'] as const, 'deny') === 'ask'
      ? ask(reason)
      : deny(reason);
  }
  return undefined;
};

// ---------------------------------------------------------- worktree guard

interface WriteSpec {
  /** `all` when every operand is written (`rm a b`), `last` for a destination. */
  operands: 'all' | 'last';
  /** Options that take the following word as their value. */
  valueFlags?: readonly string[];
  /** Options whose value is itself a destination (`cp -t dir …`). */
  targetFlags?: readonly string[];
  /** Leading operands that are not paths: chmod's mode, chown's owner. */
  skipOperands?: number;
  /** Only writes when this exact word is present — `find` without `-delete`. */
  requiresFlag?: readonly string[];
}

const COPY_FLAGS = ['-t', '--target-directory', '-S', '--suffix'];

/** Rsync options whose value may be supplied as the following token. */
const RSYNC_VALUE_FLAGS = [
  '-@',
  '-B',
  '-e',
  '-f',
  '-M',
  '-T',
  '--address',
  '--backup-dir',
  '--block-size',
  '--bwlimit',
  '--checksum-choice',
  '--checksum-seed',
  '--cc',
  '--chmod',
  '--chown',
  '--compare-dest',
  '--compress-choice',
  '--compress-level',
  '--compress-threads',
  '--confine-root',
  '--config',
  '--contimeout',
  '--copy-as',
  '--copy-dest',
  '--debug',
  '--dparam',
  '--early-input',
  '--exclude',
  '--exclude-from',
  '--files-from',
  '--filter',
  '--groupmap',
  '--iconv',
  '--include',
  '--include-from',
  '--info',
  '--link-dest',
  '--log-file',
  '--log-file-format',
  '--log-format',
  '--max-alloc',
  '--max-delete',
  '--max-size',
  '--min-size',
  '--modify-window',
  '--out-format',
  '--outbuf',
  '--only-write-batch',
  '--partial-dir',
  '--password-file',
  '--port',
  '--protocol',
  '--read-batch',
  '--remote-option',
  '--rsync-path',
  '--rsh',
  '--sockopts',
  '--skip-compress',
  '--stderr',
  '--stop-after',
  '--stop-at',
  '--suffix',
  '--temp-dir',
  '--timeout',
  '--time-limit',
  '--usermap',
  '--write-batch',
  '--zc',
  '--zl',
  '--zt',
];

/** GNU `install` options whose value may follow as a separate word. */
const INSTALL_VALUE_FLAGS = [
  '-g',
  '-m',
  '-o',
  '-S',
  '-t',
  '--group',
  '--mode',
  '--owner',
  '--strip-program',
  '--suffix',
  '--target-directory',
];

/** `find` tests whose value is a pattern rather than a path it would touch. */
const FIND_TEST_FLAGS = [
  '-name',
  '-iname',
  '-path',
  '-ipath',
  '-regex',
  '-iregex',
  '-type',
  '-size',
  '-perm',
  '-user',
  '-group',
  '-mtime',
  '-ctime',
  '-atime',
  '-mmin',
  '-newer',
  '-maxdepth',
  '-mindepth',
];

/**
 * Interpreters that run code supplied inline, and the flags that introduce it.
 * The code can write anywhere and its destination is inside the string, not in
 * argv — so such a command cannot be shown to write only within a root, and
 * `writeTargets` reports it as an unresolvable write (escaping) rather than as no
 * write at all. `python script.py` (a file, not inline code) stays out: its
 * writes are the script's, gated when the script itself was created.
 */
const INLINE_CODE_INTERPRETERS: Readonly<Record<string, readonly string[]>> = {
  python: ['-c'],
  python2: ['-c'],
  python3: ['-c'],
  perl: ['-e', '-E'],
  ruby: ['-e'],
  node: ['-e', '--eval', '-p', '--print'],
  nodejs: ['-e', '--eval'],
  php: ['-r'],
  pwsh: ['-c', '-command', '-e', '-encodedcommand'],
  powershell: ['-c', '-command', '-e', '-encodedcommand'],
  osascript: ['-e'],
  lua: ['-e'],
  rscript: ['-e'],
  bun: ['-e', '--eval'],
  elixir: ['-e'],
  julia: ['-e'],
  groovy: ['-e'],
};

/** `awk` variants: the program is a positional, and it writes only via a redirection. */
const AWK_NAMES: ReadonlySet<string> = new Set(['awk', 'gawk', 'mawk', 'nawk', 'busybox-awk']);
/** A `>`/`>>` in an awk program followed by a filename token (quote, slash, `$`, `~`). */
const AWK_REDIRECTS_TO_FILE = />\s*["'/$~]/;

function runsInlineCode(argv: readonly string[]): boolean {
  const name = commandName(argv).toLowerCase();
  // `deno` runs inline code through an `eval` subcommand rather than a flag.
  if (name === 'deno') return argv.slice(1).find((word) => !word.startsWith('-')) === 'eval';
  // `awk`'s program is always inline, but it writes only when it redirects to a
  // file; a read-only `awk '{print $1}'` in a pipeline must not be denied.
  if (AWK_NAMES.has(name)) return argv.slice(1).some((word) => AWK_REDIRECTS_TO_FILE.test(word));
  const flags = INLINE_CODE_INTERPRETERS[name];
  if (flags === undefined) return false;
  return argv.slice(1).some((word) => {
    const lower = word.toLowerCase();
    return flags.some(
      (flag) => lower === flag || (flag.length === 2 && lower.length > 2 && lower.startsWith(flag)),
    );
  });
}

/**
 * How each write utility names what it writes. `git`, package managers, and
 * compilers are not here: what they write is not in their arguments, so this
 * rule says nothing about them and a deployment that needs them contained
 * needs a tool-level rule as well. `tar` is out for the same reason — its
 * destination lives inside an archive, not in argv.
 */
const WRITE_COMMANDS: Readonly<Record<string, WriteSpec>> = {
  rm: { operands: 'all' },
  unlink: { operands: 'all' },
  shred: { operands: 'all' },
  touch: { operands: 'all', valueFlags: ['-d', '-r', '-t', '--date', '--reference', '--time'] },
  mkdir: { operands: 'all', valueFlags: ['-m', '--mode'] },
  rmdir: { operands: 'all' },
  truncate: { operands: 'all', valueFlags: ['-s', '--size', '-r', '--reference'] },
  tee: { operands: 'all' },
  // A move deletes its source, so both ends are writes.
  mv: { operands: 'all', valueFlags: COPY_FLAGS, targetFlags: ['-t', '--target-directory'] },
  cp: { operands: 'last', valueFlags: COPY_FLAGS, targetFlags: ['-t', '--target-directory'] },
  ln: { operands: 'last', valueFlags: COPY_FLAGS, targetFlags: ['-t', '--target-directory'] },
  // `rsync`/`install` name their destination as the last operand, like `cp`.
  rsync: { operands: 'last', valueFlags: RSYNC_VALUE_FLAGS },
  install: {
    operands: 'last',
    valueFlags: INSTALL_VALUE_FLAGS,
    targetFlags: ['-t', '--target-directory'],
  },
  chmod: { operands: 'all', valueFlags: ['--reference'], skipOperands: 1 },
  chown: { operands: 'all', valueFlags: ['--reference'], skipOperands: 1 },
  chgrp: { operands: 'all', valueFlags: ['--reference'], skipOperands: 1 },
  // `find -delete` removes every match under the paths it was given. What
  // `find -exec` runs is reported by the reader as a command of its own.
  find: { operands: 'all', valueFlags: FIND_TEST_FLAGS, requiresFlag: ['-delete'] },
};

/** A utility that only writes under an option it was not given is reading. */
function armed(argv: readonly string[], spec: WriteSpec): boolean {
  return spec.requiresFlag === undefined || spec.requiresFlag.some((flag) => argv.includes(flag));
}

const SED_SCRIPT_FLAGS = ['-e', '-f', '--expression', '--file'];

/**
 * Files `sed` edits in place, or `undefined` when it is only reading. Its first
 * operand is the script rather than a path, unless the script was given as an
 * option instead — in which case every operand is a file.
 */
function sedTargets(argv: readonly string[]): string[] | undefined {
  // `-i`, `-i.bak`, and the long `--in-place` / `--in-place=SUFFIX` spellings.
  if (!hasShortFlag(argv, ['i']) && !hasLongFlag(argv, ['--in-place'])) return undefined;
  const words = operands(argv, SED_SCRIPT_FLAGS);
  return hasFlag(argv, ['e', 'f'], ['--expression', '--file']) ? words : words.slice(1);
}

/**
 * Paths a command writes to. `undefined` means it writes nothing; an empty
 * array means it writes somewhere this reader could not name, which the caller
 * treats as escaping — a write whose destination is unknown is not a write
 * anyone can show to be contained.
 */
function writeTargets(cmd: ParsedCommand): string[] | undefined {
  const redirections = redirectionTargets(cmd.redirections);
  const name = commandName(cmd.argv);

  // An interpreter running inline code writes to a destination inside the code,
  // not in argv, so it cannot be shown to stay in the root: an unresolvable write.
  if (runsInlineCode(cmd.argv)) return [];

  if (name === 'dd') {
    // Without `of=`, dd writes to standard output; a redirection there is
    // caught below like any other.
    const target = operands(cmd.argv)
      .find((operand) => operand.startsWith('of='))
      ?.slice('of='.length);
    if (target !== undefined) return [target, ...redirections];
  }

  if (name === 'sed') {
    const files = sedTargets(cmd.argv);
    if (files !== undefined) return [...files, ...redirections];
  }

  const spec = WRITE_COMMANDS[name];
  if (spec === undefined || !armed(cmd.argv, spec)) {
    return redirections.length > 0 ? redirections : undefined;
  }

  const referenceMode =
    (name === 'chmod' || name === 'chown' || name === 'chgrp') &&
    hasLongFlag(cmd.argv, ['--reference']);
  const words = operands(cmd.argv, spec.valueFlags ?? []).slice(
    referenceMode ? 0 : (spec.skipOperands ?? 0),
  );
  const flagged = flaggedTargets(cmd.argv, spec.targetFlags ?? []);

  // `rsync --remove-source-files` deletes every source it transfers, and
  // `install -d` creates every directory operand, so both write every operand
  // rather than only the normal copy destination.
  const everyOperand =
    spec.operands === 'all' ||
    (name === 'rsync' && cmd.argv.includes('--remove-source-files')) ||
    (name === 'install' && hasFlag(cmd.argv, ['d'], ['--directory']));

  let targets: string[];
  if (everyOperand) targets = [...words, ...flagged, ...redirections];
  else if (flagged.length > 0) targets = [...flagged, ...redirections];
  else if (words.length > 0) targets = [words[words.length - 1] as string, ...redirections];
  else targets = redirections;

  // No target and no redirection: the command writes to standard output or
  // nothing (a `tee` with no file), not to a location that escapes the root.
  return targets.length > 0 ? targets : undefined;
}

/**
 * Destinations named by a target-flag, in every spelling: `-t dir`, `-tdir`,
 * `--target-directory dir`, and `--target-directory=dir`. Matching only the
 * space-separated form let the attached spellings write outside the root unseen.
 */
function flaggedTargets(argv: readonly string[], flags: readonly string[]): string[] {
  const found: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i] as string;
    for (const flag of flags) {
      if (token === flag) {
        const value = argv[i + 1];
        if (value !== undefined) found.push(value);
      } else if (flag.startsWith('--')) {
        if (token.startsWith(`${flag}=`)) found.push(token.slice(flag.length + 1));
      } else if (flag.length === 2 && token.length > 2 && token.startsWith(flag)) {
        found.push(token.slice(2));
      }
    }
  }
  return found;
}

export const worktreeGuard: BuiltinEvaluator = (ctx) => {
  const invocation = shellInvocation(ctx);
  if (!invocation) return undefined;
  const root = stringParam(ctx.params, 'allowed_root') ?? '.worktrees';
  const configuredReason = stringParam(ctx.params, 'deny_reason');

  for (const cmd of invocation.commands) {
    // A command nested past what the reader can follow (see `unresolved` in
    // ../shell.ts) may write anywhere, so it cannot be shown to stay in the root.
    if (cmd.unresolved) {
      return deny(
        configuredReason ??
          `\`${cmd.raw}\` nests too deeply to inspect, so it cannot be shown to write only within ${root}.`,
      );
    }
    const targets = writeTargets(cmd);
    if (targets === undefined) continue;
    if (targets.length === 0) {
      return deny(
        configuredReason ??
          `\`${cmd.raw}\` writes to a location that cannot be resolved, so it cannot be shown to ` +
            `stay within ${root}.`,
      );
    }
    const escaping = targets.find((target) => !isWithin(target, root));
    if (escaping === undefined) continue;
    return deny(
      configuredReason ??
        `\`${cmd.raw}\` writes to ${escaping || 'an unnamed location'}, outside ${root}.`,
    );
  }
  return undefined;
};
