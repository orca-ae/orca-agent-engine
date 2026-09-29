// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A heuristic reader for shell command strings.
 *
 * A guardrail that matches literal text in a command is bypassed by nesting:
 * `rm -rf /` is caught, `sudo bash -c "rm -rf /"` is not, and neither is
 * `x=1; eval "$(printf 'rm -rf /')"`. This module exists so the shell
 * guardrails match on *commands* rather than on substrings — it flattens a
 * command string into every invocation that string would actually run, seeing
 * through chaining, grouping, wrappers, nested shells, and substitutions.
 *
 * It is not a shell, and it does not try to be one. It performs no expansion of
 * variables, globs, aliases, functions, or `source`d files, and it cannot know
 * what a substitution will produce. Two rules keep those limits from becoming
 * holes:
 *
 *  1. When the text cannot be resolved — an unbalanced quote, an unterminated
 *     substitution, a variable in command position — the reader keeps going and
 *     reports the fragment rather than throwing or dropping it. A parser that
 *     throws hands the caller a decision it has no basis for; a parser that
 *     drops silently under-reports, and an under-reporting parser makes a
 *     guardrail that misses exactly the invocation it was written for.
 *  2. Unresolvable text is left literal (`$HOME/x` stays `$HOME/x`). Callers
 *     that resolve a token to a path must therefore treat it as *outside* any
 *     allowed location, because it might be.
 *
 * Known limits, all of which over-report rather than under-report: a heredoc
 * body is read as commands; `source`d and piped-in scripts are invisible
 * (their *content* is not in the string, so nothing can be said about it); the
 * output of a substitution is unknown, though the substitution's own commands
 * are reported; `[[ ]]` tests, `case` patterns, and function bodies are
 * flattened into whatever commands they contain.
 */

/** A redirection the command carries, kept apart from its words. */
export interface Redirection {
  /** The operator with its descriptor prefix, e.g. `>`, `>>`, `2>`, `&>`, `2>&`. */
  op: string;
  /** The word the redirection points at, `''` when the command ended first. */
  target: string;
}

/** One invocation the command string would run. */
export interface ParsedCommand {
  /** Words of the command, wrappers stripped, quotes removed. */
  argv: string[];
  /** The source text this came from, before wrappers were stripped. */
  raw: string;
  /**
   * Group id shared by the stages of one pipeline. `a | b` yields two commands
   * with the same id; `a; b` yields two with different ids. Flattening loses
   * the pipe otherwise, and some rules — a remote download piped into a shell,
   * say — are about the pipe rather than about either stage alone.
   */
  pipeline: number;
  /**
   * The command's redirections, extracted from its words at parse time. Kept
   * apart because an operator and a word that spells one the same way — a quoted
   * `'>'` operand — are otherwise indistinguishable once flattened, and a rule
   * that mistakes one for the other either drops the operand behind it or reads
   * the operator as the command.
   */
  redirections: Redirection[];
  /**
   * Set when the reader hit its nesting bound with this command's payload still
   * unparsed, so what it would actually run was never surfaced. A chain nested
   * that deep is never legitimate; callers must fail closed on it rather than
   * abstain, since every name-based rule would otherwise pass it through.
   */
  unresolved?: true;
}

/**
 * The command being invoked, without its directory. Rules compare against this
 * rather than `argv[0]`, since `/bin/rm` and `rm` are one command and a rule
 * that only knows the second is bypassed by typing the first.
 */
export function commandName(argv: readonly string[]): string {
  const head = argv[0];
  if (head === undefined || head.length === 0) return '';
  const slash = head.lastIndexOf('/');
  return slash === -1 ? head : head.slice(slash + 1);
}

/**
 * Every command the string would run, flattened, in source order. Nested
 * payloads follow the invocation that carries them.
 */
export function parseShellCommands(command: string): ParsedCommand[] {
  // A command longer than any real invocation is refused as one unresolved
  // command rather than parsed: several matchers (the fork-bomb regex, the
  // repo-URL regex) are super-linear, so an adversarial megabyte would block the
  // evaluator, and a command that cannot be read must fail closed anyway.
  if (command.length > MAX_COMMAND_LENGTH) {
    return [
      { argv: [], raw: command.slice(0, 80), pipeline: 0, redirections: [], unresolved: true },
    ];
  }
  return parseAt(command, 0, { next: 0 });
}

/** Bounds recursion through nested shells, substitutions, and `eval`. */
const MAX_DEPTH = 8;

/** No real command line is this long; past it the string is treated as unresolvable. */
const MAX_COMMAND_LENGTH = 64 * 1024;

interface PipelineCounter {
  next: number;
}

interface Segment {
  tokens: string[];
  raw: string;
  pipeline: number;
  redirections: Redirection[];
}

function parseAt(source: string, depth: number, counter: PipelineCounter): ParsedCommand[] {
  if (depth > MAX_DEPTH) {
    // Past the nesting bound with text still unparsed. Dropping it silently would
    // let a payload buried deeper than this — `eval ×9 rm -rf /`, nine-deep
    // `$(…)` — slip every name-based rule by never surfacing its command. Report
    // an unresolved sentinel instead and let callers fail closed on it.
    const raw = source.trim();
    return raw.length > 0
      ? [{ argv: [], raw, pipeline: counter.next++, redirections: [], unresolved: true }]
      : [];
  }
  const { segments, substitutions } = tokenize(source, counter);
  const commands: ParsedCommand[] = [];

  for (const segment of segments) {
    // `env -S "rm -rf /home"` / `env --split-string=…` splits the string into a
    // command; parse it so the real invocation surfaces rather than sitting as
    // one unsplit word `env` strips down to.
    const split = envSplitString(segment.tokens);
    if (split !== undefined) commands.push(...parseAt(split, depth + 1, counter));

    const argv = stripWrappers(segment.tokens);
    if (argv.length === 0 && segment.redirections.length === 0) continue;
    commands.push(
      ...expand(argv, segment.raw, segment.pipeline, segment.redirections, depth, counter),
    );
  }

  for (const substitution of substitutions) {
    commands.push(...parseAt(substitution, depth + 1, counter));
  }

  // A literal script piped or fed into a shell runs unseen otherwise: `echo 'rm
  // -rf /' | sh` and `sh <<< 'rm -rf /'` carry the payload in the string, so it
  // can be surfaced (a fetched or decoded script — `curl … | sh`, `base64 -d |
  // sh` — cannot, and stays the documented limit).
  commands.push(...pipedShellScripts(commands, depth, counter));
  return commands;
}

/**
 * Scripts a shell interpreter runs from its standard input within a pipeline: a
 * here-string it is given, or the literal text an `echo`/`printf` upstream of it
 * emits. Returned as commands to append, parsed one level deeper.
 */
function pipedShellScripts(
  commands: readonly ParsedCommand[],
  depth: number,
  counter: PipelineCounter,
): ParsedCommand[] {
  // No depth short-circuit here: recursing through `parseAt` at `depth + 1` lets
  // it emit its unresolved sentinel past the bound, so a deeply nested
  // `echo … | sh` fails closed like every other over-deep payload.
  const byPipeline = new Map<number, ParsedCommand[]>();
  for (const cmd of commands) {
    const stages = byPipeline.get(cmd.pipeline) ?? [];
    stages.push(cmd);
    byPipeline.set(cmd.pipeline, stages);
  }

  const scripts: ParsedCommand[] = [];
  for (const stages of byPipeline.values()) {
    const fedShell = stages.find(
      (s) => isShellInterpreter(s.argv) && !hasCommandStringFlag(s.argv),
    );
    if (fedShell === undefined) continue;

    // A here-string on the shell itself: `sh <<< 'rm -rf /'`.
    for (const redirection of fedShell.redirections) {
      if (redirection.op === '<<<' && redirection.target.length > 0) {
        scripts.push(...parseAt(redirection.target, depth + 1, counter));
      }
    }
    // A literal emitter upstream in the same pipeline: `echo 'rm -rf /' | sh`.
    for (const stage of stages) {
      if (stage === fedShell) continue;
      const emitted = literalOutput(stage.argv);
      if (emitted !== undefined) scripts.push(...parseAt(emitted, depth + 1, counter));
    }
  }
  return scripts;
}

/** True when a shell was given a command string with `-c`, so it is not reading stdin. */
function hasCommandStringFlag(argv: readonly string[]): boolean {
  return argv.some((token, index) => index > 0 && /^-[a-zA-Z]*c[a-zA-Z]*$/.test(token));
}

/** The literal text an `echo`/`printf` emits, or `undefined` for anything else. */
function literalOutput(argv: readonly string[]): string | undefined {
  const name = commandName(argv);
  if (name === 'echo') {
    // Only a leading `-n`/`-e`/`-E` run is an option; after it, every word is
    // output, including one that starts with `-` (`echo rm -rf /`).
    let i = 1;
    while (i < argv.length && /^-[neE]+$/.test(argv[i] as string)) i += 1;
    const rest = argv.slice(i);
    return rest.length > 0 ? rest.join(' ') : undefined;
  }
  if (name === 'printf') {
    let rest = argv.slice(1);
    // A leading bare format specifier (`printf '%s' 'rm …'`) emits its argument
    // verbatim. A trailing escaped whitespace sequence (`%s\n`) changes only
    // separation, so it must not hide the command from a downstream shell.
    if (rest.length > 1 && /^%[-#0-9. ]*[sb](?:\\[nrt])*$/.test(rest[0] as string)) {
      rest = rest.slice(1);
    }
    return rest.length > 0 ? rest.join(' ') : undefined;
  }
  return undefined;
}

/** One command, plus whatever it would run in turn. */
function expand(
  argv: string[],
  raw: string,
  pipeline: number,
  redirections: Redirection[],
  depth: number,
  counter: PipelineCounter,
): ParsedCommand[] {
  const alias = resolveInlineGitAlias(argv);
  argv = alias.argv;

  // The carrying invocation is reported alongside what it carries. It costs one
  // entry and leaves a rule that wants to gate `bash -c` itself something to
  // match on.
  const head: ParsedCommand = { argv, raw, pipeline, redirections };
  const commands: ParsedCommand[] = [head];
  if (alias.unresolved) head.unresolved = true;

  // A command whose name cannot be resolved fails closed. Three shapes reach
  // here: a name still carrying an unexpanded substitution or variable (`$(…)`,
  // a backtick, `$IFS`); an empty name; and a name that is really a whole unsplit
  // command string, which shows up as an `argv[0]` carrying whitespace (`env -S
  // "rm -rf /home"` — the payload is parsed separately, but this word must not
  // pass as a command in its own right). A name-based rule would otherwise
  // abstain on whatever these actually run.
  const name0 = argv[0] ?? '';
  if (argv.length > 0 && (/[$`\s]/.test(name0) || commandName(argv) === '')) {
    head.unresolved = true;
  }

  const payload = nestedPayload(argv);
  const embedded = embeddedArgvs(argv);

  // At the nesting bound this invocation is still reported, but whatever it would
  // run in turn is about to be dropped. Mark it unresolved so callers fail closed
  // rather than clear a command whose payload was never inspected.
  if (depth > MAX_DEPTH) {
    if (payload !== undefined || embedded.length > 0) head.unresolved = true;
    return commands;
  }

  if (payload !== undefined) commands.push(...parseAt(payload, depth + 1, counter));
  for (const embeddedArgv of embedded) {
    // Its own pipeline: what `find` runs is not a stage of the pipeline `find`
    // itself sits in. It carries no redirections of its own.
    commands.push(...expand(embeddedArgv, raw, counter.next++, [], depth + 1, counter));
  }
  return commands;
}

// ------------------------------------------------------------------ tokenize

const WHITESPACE = new Set([' ', '\t', '\r']);

/**
 * Split into segments at every operator that starts a new command, keeping
 * quoted text intact and collecting the interiors of command substitutions to
 * be parsed separately.
 */
function tokenize(
  source: string,
  counter: PipelineCounter,
): { segments: Segment[]; substitutions: string[] } {
  const segments: Segment[] = [];
  const substitutions: string[] = [];

  let tokens: string[] = [];
  let redirections: Redirection[] = [];
  // The operator awaiting its target word. A word flushed while this is set is
  // that target, not an argv word.
  let pendingRedirect: string | undefined;
  let buf = '';
  let hasBuf = false;
  let segmentStart = 0;
  let pipeline = counter.next++;
  let i = 0;

  const flush = (): void => {
    if (!hasBuf) return;
    if (pendingRedirect !== undefined) {
      redirections.push({ op: pendingRedirect, target: buf });
      pendingRedirect = undefined;
    } else {
      tokens.push(buf);
    }
    buf = '';
    hasBuf = false;
  };

  /** Close the current command. `samePipeline` keeps `|` stages together. */
  const endSegment = (end: number, nextStart: number, samePipeline: boolean): void => {
    flush();
    // A redirection whose target the command ended before naming (`cmd >`).
    if (pendingRedirect !== undefined) {
      redirections.push({ op: pendingRedirect, target: '' });
      pendingRedirect = undefined;
    }
    if (tokens.length > 0 || redirections.length > 0) {
      segments.push({
        tokens,
        raw: source.slice(segmentStart, end).trim(),
        pipeline,
        redirections,
      });
      tokens = [];
      redirections = [];
    }
    if (!samePipeline) pipeline = counter.next++;
    segmentStart = nextStart;
  };

  const append = (text: string): void => {
    buf += text;
    hasBuf = true;
  };

  while (i < source.length) {
    const ch = source[i] as string;

    if (ch === '\\') {
      const next = source[i + 1];
      if (next === undefined) {
        append('\\');
        i += 1;
      } else if (next === '\n') {
        i += 2; // line continuation
      } else {
        append(next);
        i += 2;
      }
      continue;
    }

    if (ch === "'") {
      const close = source.indexOf("'", i + 1);
      // An unbalanced quote takes the rest of the string: dropping it would
      // hide whatever follows from every rule.
      append(source.slice(i + 1, close === -1 ? source.length : close));
      i = close === -1 ? source.length : close + 1;
      continue;
    }

    if (ch === '"') {
      i += 1;
      hasBuf = true;
      while (i < source.length && source[i] !== '"') {
        const inner = source[i] as string;
        if (inner === '\\' && i + 1 < source.length) {
          const escaped = source[i + 1] as string;
          // A backslash-newline is a line continuation even inside double quotes:
          // bash removes both, so keeping them would garble the word.
          if (escaped === '\n') {
            i += 2;
            continue;
          }
          append('"\\$`'.includes(escaped) ? escaped : inner + escaped);
          i += 2;
          continue;
        }
        // Substitutions stay live inside double quotes.
        const expansion = readExpansion(source, i);
        if (expansion) {
          append(expansion.text);
          substitutions.push(...(expansion.inners ?? []));
          i = expansion.end;
          continue;
        }
        append(inner);
        i += 1;
      }
      if (source[i] === '"') i += 1;
      continue;
    }

    if (ch === '$' || ch === '`') {
      // `$'…'` ANSI-C quoting decodes C escapes, so `$'\x72\x6d'` runs as `rm`.
      // Reading it literally would leave `\x72\x6d` at argv[0], matching no rule
      // and letting a name-based guard abstain on the command it exists to catch.
      if (ch === '$' && source[i + 1] === "'") {
        const quoted = readAnsiCQuote(source, i + 1);
        append(quoted.text);
        i = quoted.end;
        continue;
      }
      // `$"…"` locale quoting expands like a double-quoted string; the `$` is just
      // a prefix, so drop it and let the double-quote handler read the rest.
      if (ch === '$' && source[i + 1] === '"') {
        i += 1;
        continue;
      }
      const expansion = readExpansion(source, i);
      if (expansion) {
        append(expansion.text);
        substitutions.push(...(expansion.inners ?? []));
        i = expansion.end;
        continue;
      }
      append(ch);
      i += 1;
      continue;
    }

    if (ch === '#' && !hasBuf) {
      const newline = source.indexOf('\n', i);
      i = newline === -1 ? source.length : newline;
      continue;
    }

    if (WHITESPACE.has(ch)) {
      flush();
      i += 1;
      continue;
    }

    if (ch === '\n' || ch === ';') {
      endSegment(i, i + 1, false);
      i += 1;
      continue;
    }

    if (ch === '&') {
      if (source[i + 1] === '&') {
        endSegment(i, i + 2, false);
        i += 2;
        continue;
      }
      if (source[i + 1] !== '>') {
        endSegment(i, i + 1, false);
        i += 1;
        continue;
      }
      // `&>` is a redirection, not the background operator.
    }

    if (ch === '|') {
      if (source[i + 1] === '|') {
        endSegment(i, i + 2, false);
        i += 2;
      } else if (source[i + 1] === '&') {
        endSegment(i, i + 2, true);
        i += 2;
      } else {
        endSegment(i, i + 1, true);
        i += 1;
      }
      continue;
    }

    // Grouping only scopes the commands inside it, and scope is not something
    // a flattened view keeps, so the parentheses are simply command borders.
    if (ch === '(' || ch === ')') {
      endSegment(i, i + 1, false);
      i += 1;
      continue;
    }

    const redirection = readRedirection(source, i);
    if (redirection) {
      // `2>file` — the descriptor belongs to the operator, not to the word.
      const fd = hasBuf && /^\d+$/.test(buf) ? buf : '';
      if (fd) {
        buf = '';
        hasBuf = false;
      }
      flush();
      // Held apart from the words: the next word flushed is this operator's
      // target, and a quoted `'>'` reaches the word path instead and stays an
      // operand. A pending operator with no target yet is closed out first.
      if (pendingRedirect !== undefined) redirections.push({ op: pendingRedirect, target: '' });
      pendingRedirect = fd + redirection.op;
      i = redirection.end;
      continue;
    }

    append(ch);
    i += 1;
  }

  endSegment(source.length, source.length, false);
  return { segments, substitutions };
}

/** Longest first, so `>>` is never read as two `>`. */
const REDIRECTIONS = ['&>>', '<<<', '<<-', '&>', '>>', '<<', '>|', '>&', '<&', '>', '<'] as const;

function readRedirection(source: string, i: number): { op: string; end: number } | undefined {
  for (const op of REDIRECTIONS) {
    if (source.startsWith(op, i)) return { op, end: i + op.length };
  }
  return undefined;
}

/**
 * Read `$(…)`, `` `…` ``, or `$((…))` at `i`. The substitution's text stays in
 * the surrounding word (it is unresolvable, so it must not look like a path
 * anyone recognises) while its interior is handed back to be parsed as
 * commands. Arithmetic itself runs no command, but command substitutions nested
 * inside it do and are returned as interiors too.
 */
function readExpansion(
  source: string,
  i: number,
): { text: string; inners?: string[]; end: number } | undefined {
  if (source.startsWith('$((', i)) {
    const close = matchingArithmeticClose(source, i + 3);
    const end = close === -1 ? source.length : close + 2;
    const arithmetic = source.slice(i + 3, close === -1 ? source.length : close);
    const inners = nestedSubstitutions(arithmetic);
    return { text: source.slice(i, end), ...(inners.length > 0 ? { inners } : {}), end };
  }
  if (source.startsWith('$(', i)) {
    const close = matchingParen(source, i + 1);
    const end = close === -1 ? source.length : close + 1;
    const inner = source.slice(i + 2, close === -1 ? source.length : close);
    return { text: source.slice(i, end), inners: [inner], end };
  }
  if (source[i] === '`') {
    const close = source.indexOf('`', i + 1);
    const end = close === -1 ? source.length : close + 1;
    const inner = source.slice(i + 1, close === -1 ? source.length : close);
    return { text: source.slice(i, end), inners: [inner], end };
  }
  return undefined;
}

/** Index of the first `)` in the pair closing an arithmetic expansion. */
function matchingArithmeticClose(source: string, start: number): number {
  let depth = 0;
  let quote: string | undefined;
  for (let i = start; i < source.length; i++) {
    const ch = source[i] as string;
    if (quote !== undefined) {
      if (ch === '\\' && quote === '"') i += 1;
      else if (ch === quote) quote = undefined;
      continue;
    }
    if (ch === '\\') {
      i += 1;
      continue;
    }
    if (ch === "'" || ch === '"') {
      quote = ch;
      continue;
    }
    if (source.startsWith('$(', i) && !source.startsWith('$((', i)) {
      const close = matchingParen(source, i + 1);
      if (close === -1) return -1;
      i = close;
      continue;
    }
    if (ch === '(') depth += 1;
    else if (ch === ')' && depth > 0) depth -= 1;
    else if (ch === ')' && source[i + 1] === ')') return i;
  }
  return -1;
}

/** Command substitutions nested inside an arithmetic expansion. */
function nestedSubstitutions(source: string): string[] {
  const found: string[] = [];
  for (let i = 0; i < source.length; ) {
    const expansion = readExpansion(source, i);
    if (!expansion) {
      i += 1;
      continue;
    }
    found.push(...(expansion.inners ?? []));
    i = expansion.end;
  }
  return found;
}

/**
 * Decode a `$'…'` ANSI-C quoted string. `open` is the opening quote's index (the
 * character after the `$`). Bash decodes C escapes here, so the reader must too
 * or the real command name stays hidden behind `\x`/`\NNN` sequences. Returns the
 * decoded text and the index just past the closing quote (or end of string when
 * the quote is unbalanced, mirroring how the plain single-quote reader degrades).
 */
function readAnsiCQuote(source: string, open: number): { text: string; end: number } {
  let out = '';
  let i = open + 1;
  while (i < source.length) {
    const ch = source[i] as string;
    if (ch === "'") return { text: out, end: i + 1 };
    if (ch === '\\' && i + 1 < source.length) {
      const decoded = decodeAnsiCEscape(source, i + 1);
      out += decoded.text;
      i = decoded.end;
      continue;
    }
    out += ch;
    i += 1;
  }
  return { text: out, end: i };
}

/** The single-character ANSI-C escapes. */
const ANSI_C_SIMPLE: Readonly<Record<string, string>> = {
  a: '\x07',
  b: '\b',
  e: '\x1b',
  E: '\x1b',
  f: '\f',
  n: '\n',
  r: '\r',
  t: '\t',
  v: '\v',
  '\\': '\\',
  "'": "'",
  '"': '"',
  '?': '?',
};

/** Decode one escape whose backslash sat at `i-1`; returns its text and next index. */
function decodeAnsiCEscape(source: string, i: number): { text: string; end: number } {
  const c = source[i] as string;
  const simple = ANSI_C_SIMPLE[c];
  if (simple !== undefined) return { text: simple, end: i + 1 };

  if (c === 'x') {
    const hex = /^[0-9A-Fa-f]{1,2}/.exec(source.slice(i + 1, i + 3));
    if (hex) return { text: fromCharSafe(parseInt(hex[0], 16)), end: i + 1 + hex[0].length };
  } else if (c === 'u' || c === 'U') {
    const width = c === 'u' ? 4 : 8;
    const hex = new RegExp(`^[0-9A-Fa-f]{1,${width}}`).exec(source.slice(i + 1, i + 1 + width));
    if (hex) return { text: fromCodePointSafe(parseInt(hex[0], 16)), end: i + 1 + hex[0].length };
  } else if (c >= '0' && c <= '7') {
    const octal = /^[0-7]{1,3}/.exec(source.slice(i, i + 3));
    if (octal) {
      return { text: fromCharSafe(parseInt(octal[0], 8) & 0xff), end: i + octal[0].length };
    }
  }
  // An unrecognised escape keeps its backslash, the way bash leaves it in `$'…'`.
  return { text: `\\${c}`, end: i + 1 };
}

/** A NUL decodes to nothing (bash cannot hold one in a string); over-reports the rest. */
function fromCharSafe(code: number): string {
  return code === 0 ? '' : String.fromCharCode(code);
}

function fromCodePointSafe(code: number): string {
  if (code <= 0 || code > 0x10ffff) return '';
  try {
    return String.fromCodePoint(code);
  } catch {
    return '';
  }
}

/**
 * Index of the `)` closing the `(` at `open`, or -1. Nesting-aware, and blind to
 * parens inside quotes: in `$(x=')'; rm -rf /)` the quoted `)` is literal text,
 * and counting it would close the substitution early and hide `rm -rf /`.
 */
function matchingParen(source: string, open: number): number {
  let depth = 0;
  let quote: string | undefined;
  for (let i = open; i < source.length; i++) {
    const ch = source[i];
    if (quote !== undefined) {
      // Only the matching close quote ends it; `\` escapes inside `"` alone.
      if (ch === '\\' && quote === '"') i += 1;
      else if (ch === quote) quote = undefined;
      continue;
    }
    if (ch === '\\') i += 1;
    else if (ch === "'" || ch === '"') quote = ch;
    else if (ch === '(') depth += 1;
    else if (ch === ')') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
}

// -------------------------------------------------------------- unwrapping

/**
 * Words that carry no command of their own. Dropping them puts the real
 * command at `argv[0]`, which is where every rule looks for it.
 */
const KEYWORDS: ReadonlySet<string> = new Set([
  '{',
  '}',
  '!',
  'if',
  'then',
  'else',
  'elif',
  'fi',
  'while',
  'until',
  'do',
  'done',
  'case',
  'esac',
  'time',
  'nohup',
  'setsid',
]);

interface WrapperSpec {
  /** Options that take the following word as their value. */
  valueFlags?: readonly string[];
  /** Drop leading `VAR=value` words. */
  assignments?: boolean;
  /** Drop one leading word that looks like a duration (`timeout 30s cmd`). */
  duration?: boolean;
  /** Drop this many operands before the wrapped command. */
  leadingOperands?: number;
}

/** Commands whose own arguments are another command. */
const WRAPPERS: Readonly<Record<string, WrapperSpec>> = {
  sudo: {
    valueFlags: [
      '-a',
      '--auth-type',
      '-C',
      '--close-from',
      '-c',
      '--login-class',
      '-D',
      '--chdir',
      '-g',
      '--group',
      '-h',
      '--host',
      '-p',
      '--prompt',
      '-R',
      '--chroot',
      '-r',
      '--role',
      '-t',
      '--type',
      '-T',
      '--command-timeout',
      '-u',
      '--user',
      '-U',
      '--other-user',
    ],
    assignments: true,
  },
  doas: { valueFlags: ['-u', '-C'] },
  // `-C`/`--chdir` and `-a`/`--argv0` each consume the following word; without
  // them a space-separated `env -C /tmp rm -rf /etc` leaves `/tmp` at argv[0].
  env: {
    valueFlags: ['-u', '--unset', '-C', '--chdir', '-a', '--argv0', '-S', '--split-string'],
    assignments: true,
  },
  xargs: {
    valueFlags: [
      '-I',
      '-n',
      '-L',
      '-P',
      '-s',
      '-d',
      '-a',
      '-E',
      '--max-args',
      '--max-chars',
      '--max-procs',
      '--delimiter',
      '--arg-file',
      '--process-slot-var',
    ],
  },
  chroot: { valueFlags: ['--groups', '--userspec'], leadingOperands: 1 },
  nice: { valueFlags: ['-n', '--adjustment'] },
  ionice: { valueFlags: ['-c', '-n', '-p'] },
  stdbuf: { valueFlags: ['-i', '-o', '-e'] },
  timeout: { valueFlags: ['-s', '--signal', '-k', '--kill-after'], duration: true },
  // `command`/`builtin` force a plain command past a function or alias; `exec`
  // replaces the shell with one. Each carries the real command after its own
  // options (`command -p`, `exec -a name`), so peel those to reach argv[0].
  command: {},
  builtin: {},
  exec: { valueFlags: ['-a'] },
  // Applet multiplexers: `busybox rm …`, `toybox rm …` run the applet named in
  // their first operand. Dropping the multiplexer makes the applet the head,
  // after which the usual shell / eval / name handling applies.
  busybox: {},
  toybox: {},
};

const ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/;
const DURATION = /^\d+(\.\d+)?[smhd]?$/;

const GIT_GLOBAL_VALUE_FLAGS: ReadonlySet<string> = new Set([
  '-C',
  '-c',
  '--config-env',
  '--git-dir',
  '--work-tree',
  '--namespace',
  '--exec-path',
]);

interface GitAliasResolution {
  argv: string[];
  unresolved: boolean;
}

/**
 * Expand an alias defined by this invocation's own `git -c alias.x=...` option.
 * The expansion stays behind the original global options so the Git-aware
 * guardrails continue to find it normally. Shell aliases (`!command`) and
 * expansions that are not one plain argv are marked unresolved: Git will run
 * them, but this reader cannot safely claim what they do.
 */
function resolveInlineGitAlias(tokens: readonly string[]): GitAliasResolution {
  const original = [...tokens];
  if (commandName(original) !== 'git') return { argv: original, unresolved: false };

  const aliases = new Map<string, string>();
  let subcommandIndex = -1;
  for (let i = 1; i < original.length; i++) {
    const token = original[i] as string;
    if (!token.startsWith('-')) {
      subcommandIndex = i;
      break;
    }

    let config: string | undefined;
    if (token === '-c') {
      config = original[i + 1];
      i += 1;
    } else if (token.startsWith('-c') && !token.startsWith('--')) {
      config = token.slice(2);
    } else if (GIT_GLOBAL_VALUE_FLAGS.has(token)) {
      i += 1;
    }

    const match = config === undefined ? undefined : /^alias\.([^=]+)=(.*)$/i.exec(config);
    if (match !== undefined && match !== null) {
      aliases.set(match[1] as string, match[2] as string);
    }
  }

  if (subcommandIndex === -1) return { argv: original, unresolved: false };
  const prefix = original.slice(0, subcommandIndex);
  let effective = original.slice(subcommandIndex);
  const seen = new Set<string>();

  // Git resolves aliases recursively. Follow the same chain, but bound it and
  // fail closed on cycles or expansions that need a shell to interpret them.
  for (let depth = 0; depth < 16; depth++) {
    const subcommand = effective[0] as string;
    const expansion = aliases.get(subcommand);
    if (expansion === undefined) {
      return { argv: [...prefix, ...effective], unresolved: false };
    }
    if (seen.has(subcommand) || expansion.startsWith('!')) {
      return { argv: original, unresolved: true };
    }
    seen.add(subcommand);

    const parsed = tokenize(expansion, { next: 0 });
    const segment = parsed.segments[0];
    if (
      parsed.segments.length !== 1 ||
      parsed.substitutions.length !== 0 ||
      segment === undefined ||
      segment.tokens.length === 0 ||
      segment.redirections.length !== 0
    ) {
      return { argv: original, unresolved: true };
    }
    effective = [...segment.tokens, ...effective.slice(1)];
  }

  return { argv: original, unresolved: true };
}

/** Peel keywords, environment assignments, and wrappers off the front. */
function stripWrappers(tokens: readonly string[]): string[] {
  let argv = [...tokens];
  // Each pass consumes at least the head word, so the token count bounds it.
  for (let pass = 0; pass <= tokens.length && argv.length > 0; pass++) {
    const head = argv[0] as string;
    if (KEYWORDS.has(head) || KEYWORDS.has(commandName(argv)) || ASSIGNMENT.test(head)) {
      argv = argv.slice(1);
      continue;
    }
    const spec = WRAPPERS[commandName(argv)];
    if (!spec) break;
    argv = dropOptions(argv.slice(1), spec);
  }
  return argv;
}

function dropOptions(argv: readonly string[], spec: WrapperSpec): string[] {
  let i = 0;
  while (i < argv.length) {
    const token = argv[i] as string;
    if (token === '--') {
      i += 1;
      break;
    }
    if (spec.assignments && ASSIGNMENT.test(token)) {
      i += 1;
      continue;
    }
    if (token.startsWith('-') && token.length > 1) {
      i += optionConsumesNext(token, spec.valueFlags ?? []) ? 2 : 1;
      continue;
    }
    break;
  }
  if (spec.duration && i < argv.length && DURATION.test(argv[i] as string)) i += 1;
  i += Math.min(spec.leadingOperands ?? 0, argv.length - i);
  return argv.slice(i);
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

/** Shells that run a command string given to them. */
const SHELLS: ReadonlySet<string> = new Set(['sh', 'bash', 'zsh', 'ksh', 'dash', 'ash']);

/** True when this command is an interpreter that would run text piped to it. */
export function isShellInterpreter(argv: readonly string[]): boolean {
  return SHELLS.has(commandName(argv));
}

/** The command string a nested shell or `eval` would run, if there is one. */
function nestedPayload(argv: readonly string[]): string | undefined {
  const name = commandName(argv);
  if (SHELLS.has(name)) {
    // A short-option run containing `c` anywhere: `-c`, `-lc`, `-ec`, and also
    // `-cx` where `c` is bundled before another flag. The word after it is the
    // command string.
    const flag = argv.findIndex(
      (token, index) => index > 0 && /^-[a-zA-Z]*c[a-zA-Z]*$/.test(token),
    );
    if (flag === -1) return undefined;
    return argv[flag + 1];
  }
  if (name === 'su') {
    // `su … -c <command>` runs its argument through a login shell, like `-c`
    // above; the user operand, if any, sits before the flag.
    const flag = argv.findIndex(
      (token, index) => index > 0 && (token === '-c' || token === '--command'),
    );
    return flag === -1 ? undefined : argv[flag + 1];
  }
  if (name === 'eval') {
    // `eval rm -rf x` and `eval "rm -rf x"` run the same thing; joining covers
    // both. Quoting inside the payload is re-read on the next pass.
    const rest = argv.slice(1);
    return rest.length > 0 ? rest.join(' ') : undefined;
  }
  return undefined;
}

/** env options that consume the following word (besides `-S`, handled directly). */
const ENV_VALUE_FLAGS: ReadonlySet<string> = new Set([
  '-u',
  '--unset',
  '-C',
  '--chdir',
  '-a',
  '--argv0',
]);

/**
 * The command one `env` invocation runs via `-S`/`--split-string` (its head is
 * `env`), or undefined if this `env` has no `-S`.
 *
 * `env` splits the `-S` string into words *and appends the operands that follow
 * it*: `env -S rm -rf /etc` runs `rm -rf /etc` — `rm` from the split string,
 * `-rf /etc` from the trailing argv. So the value alone is not the command; the
 * value joined with the rest is. An empty `-S ""` then contributes nothing and
 * the trailing `env -S …` re-parses on its own.
 */
function envSplitPayload(argv: readonly string[]): string | undefined {
  for (let i = 1; i < argv.length; i++) {
    const token = argv[i] as string;
    let value: string;
    let restFrom: number;
    if (token === '-S' || token === '--split-string') {
      value = argv[i + 1] ?? '';
      restFrom = i + 2;
    } else if (token.startsWith('--split-string=')) {
      value = token.slice('--split-string='.length);
      restFrom = i + 1;
    } else if (token.startsWith('-S') && token.length > 2) {
      value = token.slice(2);
      restFrom = i + 1;
    } else if (ENV_VALUE_FLAGS.has(token)) {
      // env's other value-taking options consume the following word; skip it, or
      // `env -C /tmp -S "…"` would read `/tmp` as the command and miss the `-S`.
      i += 1;
      continue;
    } else if (token.startsWith('-') || ASSIGNMENT.test(token)) {
      // An attached-value option (`--chdir=/tmp`) or an assignment does not
      // consume a following word; keep scanning for `-S`.
      continue;
    } else {
      // A plain word means this `env` runs it directly; no `-S` on this env.
      return undefined;
    }
    const rest = restFrom < argv.length ? argv.slice(restFrom) : [];
    const command = [value, ...rest].join(' ').trim();
    return command.length > 0 ? command : undefined;
  }
  return undefined;
}

/**
 * The command string `env -S`/`env --split-string` re-splits and runs, read from
 * the raw tokens before `env`'s wrapper stripping consumes them. `env` runs the
 * string as a command, so `env -S "rm -rf /home"` is `rm -rf /home`; without this
 * the string reaches `argv[0]` as one unsplit word that matches no rule.
 *
 * The `-S` may sit on any `env` in a chain — `env env -S "…"` runs `env -S "…"`,
 * whose own `-S` carries the payload. So each `env` is scanned before being
 * peeled; peeling the wrapper alone and stopping at the first `env` let the inner
 * `-S` slip past while `stripWrappers` swallowed it, leaving nothing to inspect.
 */
function envSplitString(tokens: readonly string[]): string | undefined {
  let argv: readonly string[] = tokens;
  for (let pass = 0; pass <= tokens.length && argv.length > 0; pass++) {
    const head = argv[0] as string;
    if (KEYWORDS.has(head) || KEYWORDS.has(commandName(argv)) || ASSIGNMENT.test(head)) {
      argv = argv.slice(1);
      continue;
    }
    if (commandName(argv) === 'env') {
      const payload = envSplitPayload(argv);
      if (payload !== undefined) return payload;
      // This `env` has no `-S`; peel it and look at the command it wraps, which
      // may be another `env` that does.
      argv = dropOptions(argv.slice(1), WRAPPERS['env'] as WrapperSpec);
      continue;
    }
    const spec = WRAPPERS[commandName(argv)];
    if (!spec) return undefined;
    argv = dropOptions(argv.slice(1), spec);
  }
  return undefined;
}

/** `find` actions that run a command per match, and what terminates one. */
const FIND_ACTIONS: ReadonlySet<string> = new Set(['-exec', '-execdir', '-ok', '-okdir']);
const FIND_ACTION_END: ReadonlySet<string> = new Set([';', '+']);

/**
 * The command `find -exec` runs, already split into words.
 *
 * Returned as words rather than as text to re-read: the words came out of the
 * tokenizer once already, and re-joining them would lose the quoting that told
 * them apart.
 */
function embeddedArgvs(argv: readonly string[]): string[][] {
  if (commandName(argv) !== 'find') return [];

  const found: string[][] = [];
  for (let i = 1; i < argv.length; i++) {
    if (!FIND_ACTIONS.has(argv[i] as string)) continue;
    const rest = argv.slice(i + 1);
    const end = rest.findIndex((token) => FIND_ACTION_END.has(token));
    const stripped = stripWrappers(end === -1 ? rest : rest.slice(0, end));
    if (stripped.length > 0) found.push(stripped);
    if (end === -1) break;
    i += end + 1;
  }
  return found;
}
