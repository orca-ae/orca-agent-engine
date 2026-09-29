// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { BuiltinEvaluator, EvaluatorContext } from '../engine.js';
import { commandName, parseShellCommands } from '../shell.js';
import { OS_SHELL_TOOLS, matchesAnyToolPattern, parseMcpToolName } from '../tool-names.js';
import type { GuardrailOutcome } from '../types.js';
import { ask, deny, stringList, toolMatches } from './helpers.js';

/**
 * Repository access, over both surfaces that reach a repository.
 *
 * An agent gets at a repository two ways: the integration's own tools, and
 * `git` or `gh` in a shell. They are the same capability wearing different
 * clothes, so this evaluator gates both from one policy. Covering one alone
 * would be worse than covering neither — a guardrail that stops `git push` to
 * `main` while `mcp__github__create_or_update_file` writes the same branch
 * reports as enforced, appears in an audit as enforced, and is not.
 *
 * Where the target cannot be established, the verdict is `ask`, never silence.
 * A push to a remote named `origin` genuinely does not say which repository it
 * writes; treating "I cannot tell" as "allowed" would put every unrecognised
 * argument shape outside the policy.
 */

/**
 * Substrings identifying the integration anywhere in an MCP server name, for the
 * hosted forms (`mycorp-github`, `gitlab-cloud`). The bare `git*` family is
 * matched by prefix instead — see `serverIsGitFamily` — so `digit` and `legit`,
 * which merely contain the letters, are not swept in.
 */
const GITHUB_SERVERS: readonly string[] = ['github', 'gitlab', 'gitea'];

/** A `git`-family server: `git`, `github`, `gitlab`, `gitea`, `github-enterprise`. */
function serverIsGitFamily(server: string): boolean {
  return server.startsWith('git') || GITHUB_SERVERS.some((hint) => server.includes(hint));
}

/**
 * Whole tool-name words that mark a call as this family when the server does
 * not. A short, tight set on purpose — matched as whole words, not substrings,
 * so an analytics `get_report` is not swept in by `repo`.
 */
const GITHUB_TOOL_WORDS: readonly string[] = ['github', 'git', 'repo', 'repository', 'gist'];

interface McpCall {
  words: readonly string[];
  input: Record<string, unknown>;
}

/** Lower-case word split that survives both `create_issue` and `createIssue`. */
function words(value: string): string[] {
  return value
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((word) => word.length > 0);
}

/** True when the tool's own words name this family, e.g. `github_*` or `*_repo`. */
function githubToolWords(toolWords: readonly string[]): boolean {
  if (toolWords.some((word) => GITHUB_TOOL_WORDS.includes(word))) return true;
  // `pull_request`, split into two ordinary words, is distinctive as the pair.
  return toolWords.includes('pull') && toolWords.includes('request');
}

function githubCall(ctx: EvaluatorContext): McpCall | undefined {
  const name = ctx.event.tool?.name;
  if (name === undefined) return undefined;
  const parsed = parseMcpToolName(name);
  if (!parsed) return undefined;
  const server = parsed.serverName.toLowerCase().replace(/[^a-z0-9]/g, '');
  const toolWords = words(parsed.toolName);
  // Either the server or the tool name may carry the family; neither is required.
  if (!serverIsGitFamily(server) && !githubToolWords(toolWords)) return undefined;
  return { words: toolWords, input: ctx.event.tool?.input ?? {} };
}

function flag(params: Record<string, unknown>, key: string, fallback: boolean): boolean {
  const value = params[key];
  return typeof value === 'boolean' ? value : fallback;
}

/**
 * Repository names are matched case-insensitively, which is how the host itself
 * resolves them — `Acme/App` and `acme/app` are one repository, and a policy
 * that missed the second spelling would be bypassed by capitalisation. Branch
 * names are matched as written, because git does distinguish them.
 */
function matchesRepo(repo: string, patterns: readonly string[]): boolean {
  return matchesAnyToolPattern(
    repo.toLowerCase(),
    patterns.map((pattern) => pattern.toLowerCase()),
  );
}

// ------------------------------------------------------------- the decisions

function decideRead(repo: string | undefined, ctx: EvaluatorContext): GuardrailOutcome | undefined {
  if (flag(ctx.params, 'read_all', true)) return undefined;
  const readable = stringList(ctx.params, 'read_repos');
  // With reads restricted and nothing listed, no repository is readable — so
  // whether this one can be identified makes no difference to the answer.
  if (readable.length === 0) return deny('This agent may not read any repository.');
  if (repo === undefined) {
    return ask('This call names no repository that can be checked against the policy.');
  }
  return matchesRepo(repo, readable)
    ? undefined
    : deny(`Reading ${repo} is outside this agent's repository access.`);
}

function decideWrite(
  repo: string | undefined,
  branches: readonly string[],
  ctx: EvaluatorContext,
): GuardrailOutcome | undefined {
  const writable = stringList(ctx.params, 'write_repos');
  if (writable.length === 0) return deny('This agent may not write to any repository.');

  // Everything determinable is checked before anything ambiguous, so a call that
  // is plainly forbidden denies rather than degrading into a question.
  if (repo !== undefined && !matchesRepo(repo, writable)) {
    return deny(`Writing to ${repo} is outside this agent's repository access.`);
  }
  // Every branch the call names must pass. Reading only the first key would let a
  // decoy `branch` on the allowlist launder the real destination in `base`/`ref`.
  const allowed = stringList(ctx.params, 'write_branches');
  if (allowed.length > 0) {
    for (const branch of branches) {
      if (!matchesAnyToolPattern(branch, allowed)) {
        return deny(`Writing to branch ${branch} is outside this agent's repository access.`);
      }
    }
  }

  if (repo === undefined) {
    return ask('This write names no repository that can be checked against the policy.');
  }
  if (branches.length === 0 && allowed.length > 0) {
    // A write that names no branch lands on the repository's default branch,
    // which this evaluator cannot see. That is precisely the branch such a
    // policy is usually written to protect.
    return ask('This write names no branch that can be checked against the policy.');
  }
  return undefined;
}

// ------------------------------------------------------- repository from args

/**
 * `host/owner/name`, `host:owner/name`, with or without a scheme, a `.git`
 * suffix, or a trailing path. Deliberately not restricted to one host: a policy
 * about repositories should not stop applying because the deployment is
 * self-hosted.
 */
const REPO_URL = /(?:^|[@/])[\w.-]+\.[\w.-]+[:/]([\w.-]+)\/([\w.-]+?)(?:\.git)?(?:\/|$)/;

/** `repos/owner/name/...` as an API path names a repository too. */
const API_PATH = /^\/?repos\/([\w.-]+)\/([\w.-]+)/;

const SLUG = /^[\w.-]+\/[\w.-]+$/;

/** No real repository URL is this long; the bound keeps REPO_URL off a huge token. */
const MAX_REPO_URL_LENGTH = 2048;

function repoFromUrlLike(value: string): string | undefined {
  // `REPO_URL`'s two unbounded `[\w.-]+` groups around a `.` backtrack
  // polynomially, so a long dotted token would burn CPU on every evaluation.
  if (value.length > MAX_REPO_URL_LENGTH) return undefined;
  const match = REPO_URL.exec(value);
  return match ? `${match[1]}/${match[2]}` : undefined;
}

/**
 * A Git remote's complete repository path. Unlike web/API URLs, GitLab remotes
 * may contain arbitrarily deep subgroup paths, and every segment is part of the
 * repository identity checked by policy.
 */
function repoFromGitRemote(value: string): string | undefined {
  if (value.length > MAX_REPO_URL_LENGTH) return undefined;

  let path: string | undefined;
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(value)) {
    try {
      const url = new URL(value);
      if (url.hostname.length === 0) return undefined;
      path = url.pathname;
    } catch {
      return undefined;
    }
  } else {
    const scp = /^(?:[^@\s/:]+@)?[^:\s/]+:([^?#]+)$/.exec(value);
    path = scp?.[1];
  }

  if (path === undefined) return undefined;
  const segments = path.replace(/^\/+|\/+$/g, '').split('/');
  if (segments.length < 2) return undefined;
  segments[segments.length - 1] = stripGitSuffix(segments[segments.length - 1] as string);
  return segments.every((segment) => /^[\w.-]+$/.test(segment)) ? segments.join('/') : undefined;
}

function repoFromToken(value: string): string | undefined {
  const api = API_PATH.exec(value);
  if (api) return `${api[1]}/${api[2]}`;
  const url = repoFromUrlLike(value);
  if (url) return url;
  return SLUG.test(value) ? stripGitSuffix(value) : undefined;
}

function stripGitSuffix(value: string): string {
  return value.endsWith('.git') ? value.slice(0, -'.git'.length) : value;
}

const OWNER_KEYS: readonly string[] = [
  'owner',
  'org',
  'organization',
  'owner_name',
  'ownerName',
  'repo_owner',
  'repoOwner',
];

const NAME_KEYS: readonly string[] = ['repo', 'repository', 'repo_name', 'repoName', 'name'];

const SLUG_KEYS: readonly string[] = [
  'full_name',
  'fullName',
  'nameWithOwner',
  'repo_full_name',
  'repository_full_name',
];

const URL_KEYS: readonly string[] = [
  'url',
  'repo_url',
  'repoUrl',
  'html_url',
  'clone_url',
  'git_url',
  'ssh_url',
  'repository_url',
];

/** Non-empty string values a set of keys carries, all of them, not just the first. */
function stringArgs(input: Record<string, unknown>, keys: readonly string[]): string[] {
  const found: string[] = [];
  for (const key of keys) {
    const value = input[key];
    if (typeof value === 'string' && value.length > 0) found.push(value);
  }
  return found;
}

/**
 * Every repository a call resolves to, and whether it also names a repository
 * this reader could not resolve. Each key shape is scanned in full, not just its
 * first key — a decoy `repository`/`owner`/`url` beside an allowlisted one would
 * otherwise be dropped — and the access check then requires every resolved repo
 * to pass while an unresolved one degrades to `ask`. A bare name with no owner
 * stays unresolved: it cannot be compared against an `owner/name` policy without
 * inventing the missing half.
 */
function reposFromInput(input: Record<string, unknown>): { repos: string[]; unresolved: boolean } {
  const repos = new Set<string>();
  let unresolved = false;

  for (const slug of stringArgs(input, SLUG_KEYS)) repos.add(stripGitSuffix(slug));

  const names = stringArgs(input, NAME_KEYS);
  const owners = stringArgs(input, OWNER_KEYS);
  for (const name of names) {
    if (name.includes('/')) repos.add(stripGitSuffix(name));
    else if (owners.length > 0) {
      for (const owner of owners) repos.add(`${owner}/${stripGitSuffix(name)}`);
    }
    // A bare name with no owner names a repository that cannot be checked.
    else unresolved = true;
  }

  for (const url of stringArgs(input, URL_KEYS)) {
    const repo = repoFromUrlLike(url);
    if (repo !== undefined) repos.add(repo);
    // A url present but unparseable (too long to match, an unknown host) names a
    // repository too; treat it as unresolved rather than silently ignoring it.
    else unresolved = true;
  }

  return { repos: [...repos], unresolved };
}

/**
 * Branch arguments name a *destination*. `head` is excluded on purpose: on a
 * proposed change it identifies where the content came from, and treating it as
 * a destination would deny work on a permitted branch.
 */
const BRANCH_KEYS: readonly string[] = [
  'branch',
  'branch_name',
  'branchName',
  'ref',
  'base',
  'base_branch',
  'baseBranch',
  'target_branch',
  'targetBranch',
];

function stripRefPrefix(ref: string): string {
  return ref.startsWith('refs/heads/') ? ref.slice('refs/heads/'.length) : ref;
}

/**
 * Every destination branch the call names, across all keys — not just the first.
 * A decoy `branch` on the allowlist beside the real target in `base`/`ref` would
 * otherwise be the only branch checked, laundering the write past the policy.
 */
function branchesFromInput(input: Record<string, unknown>): string[] {
  return stringArgs(input, BRANCH_KEYS).map(stripRefPrefix);
}

// ------------------------------------------------------------ integration arm

const READ_VERBS: readonly string[] = [
  'get',
  'list',
  'search',
  'read',
  'fetch',
  'download',
  'describe',
  'view',
  'find',
  'query',
];

/** Consolidated GitHub MCP tools whose operation verb appears at the end. */
const COMPOUND_READ_TOOLS: ReadonlySet<string> = new Set(['issue read', 'pull request read']);

function mcpOutcome(call: McpCall, ctx: EvaluatorContext): GuardrailOutcome | undefined {
  // An unrecognised verb is a write. A new tool the evaluator has never seen is
  // the one case where guessing "read" would open a hole nobody would notice.
  const reading =
    READ_VERBS.includes(call.words[0] ?? '') || COMPOUND_READ_TOOLS.has(call.words.join(' '));
  const branches = reading ? [] : branchesFromInput(call.input);
  const decide = (repo: string | undefined): GuardrailOutcome | undefined =>
    reading ? decideRead(repo, ctx) : decideWrite(repo, branches, ctx);

  const { repos, unresolved } = reposFromInput(call.input);
  if (repos.length === 0) return decide(undefined);

  // Every repository the call names must pass; the strictest verdict is the
  // verdict for the call, so an allowlisted decoy cannot clear a forbidden one.
  // An unresolved repository beside them degrades to an ask, never silence.
  let asked: GuardrailOutcome | undefined = unresolved ? decide(undefined) : undefined;
  for (const repo of repos) {
    const outcome = decide(repo);
    if (outcome?.verdict === 'deny') return outcome;
    if (outcome?.verdict === 'ask') asked ??= outcome;
  }
  return asked;
}

// ------------------------------------------------------------------ shell arm

/** Global options that swallow the following word before the subcommand. */
const GIT_VALUE_GLOBALS: readonly string[] = [
  '-C',
  '-c',
  '--git-dir',
  '--work-tree',
  '--namespace',
  '--exec-path',
  '--config-env',
];

/** `push` options that swallow the following word. */
const GIT_PUSH_VALUE_FLAGS: readonly string[] = [
  '-o',
  '--push-option',
  '--repo',
  '--exec',
  '--receive-pack',
];

/** Every `git clone` option that consumes the following argv token. */
const GIT_CLONE_VALUE_FLAGS: readonly string[] = [
  '-j',
  '--jobs',
  '--template',
  '--reference',
  '--reference-if-able',
  '-o',
  '--origin',
  '-b',
  '--branch',
  '-u',
  '--upload-pack',
  '--depth',
  '--shallow-since',
  '--shallow-exclude',
  '--separate-git-dir',
  '-c',
  '--config',
  '--server-option',
  '--filter',
  '--bundle-uri',
];

/** Every `git fetch` option whose value may be the following argv token. */
const GIT_FETCH_VALUE_FLAGS: readonly string[] = [
  '--upload-pack',
  '-j',
  '--jobs',
  '--depth',
  '--shallow-since',
  '--shallow-exclude',
  '--deepen',
  '--refmap',
  '-o',
  '--server-option',
  '--negotiation-tip',
  '--filter',
];

/** `git pull` adds merge-strategy values to the fetch option set. */
const GIT_PULL_VALUE_FLAGS: readonly string[] = [
  '--cleanup',
  '-s',
  '--strategy',
  '-X',
  '--strategy-option',
  '--upload-pack',
  '-j',
  '--jobs',
  '--depth',
  '--shallow-since',
  '--shallow-exclude',
  '--deepen',
  '--refmap',
  '-o',
  '--server-option',
  '--negotiation-tip',
];

/** Every `git ls-remote` option whose value may be the following argv token. */
const GIT_LS_REMOTE_VALUE_FLAGS: readonly string[] = [
  '--upload-pack',
  '--sort',
  '-o',
  '--server-option',
];

const GIT_READ_VALUE_FLAGS: Readonly<Record<string, readonly string[]>> = {
  fetch: GIT_FETCH_VALUE_FLAGS,
  pull: GIT_PULL_VALUE_FLAGS,
  'ls-remote': GIT_LS_REMOTE_VALUE_FLAGS,
};

function gitSubcommand(
  args: readonly string[],
): { name: string; rest: readonly string[] } | undefined {
  let i = 0;
  while (i < args.length) {
    const token = args[i] as string;
    if (!token.startsWith('-')) return { name: token, rest: args.slice(i + 1) };
    i += GIT_VALUE_GLOBALS.includes(token) ? 2 : 1;
  }
  return undefined;
}

function positionals(args: readonly string[], valueFlags: readonly string[]): string[] {
  const found: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const token = args[i] as string;
    if (token === '--') {
      found.push(...args.slice(i + 1));
      break;
    }
    if (token.startsWith('-') && token.length > 1) {
      if (valueFlags.includes(token)) i += 1;
      continue;
    }
    found.push(token);
  }
  return found;
}

/**
 * The branch a refspec writes to. `src:dst` writes `dst`, a leading `+` is a
 * force marker, and a bare name is both sides at once. Reading the source
 * instead would compare the wrong name against the policy — `HEAD:main` is a
 * write to `main` however the local side is spelled.
 */
function refspecBranch(refspec: string): string {
  const withoutForce = refspec.startsWith('+') ? refspec.slice(1) : refspec;
  const colon = withoutForce.indexOf(':');
  return stripRefPrefix(colon === -1 ? withoutForce : withoutForce.slice(colon + 1));
}

function gitOutcome(argv: readonly string[], ctx: EvaluatorContext): GuardrailOutcome | undefined {
  const sub = gitSubcommand(argv.slice(1));
  if (!sub) return undefined;

  if (sub.name === 'push') {
    const args = positionals(sub.rest, GIT_PUSH_VALUE_FLAGS);
    // `git push <remote> <refspec>...`. A remote name is an alias configured
    // outside this command, so only a URL identifies the repository here.
    const remote = args[0];
    const repo = remote === undefined ? undefined : repoFromGitRemote(remote);
    const refspecs = args.slice(1);
    // One push can carry several refspecs, and the strictest verdict any of them
    // earns is the verdict for the push: `git push url feature main` is a write
    // to `main` however permitted the `feature` refspec beside it is.
    if (refspecs.length === 0) return decideWrite(repo, [], ctx);
    let asked: GuardrailOutcome | undefined;
    for (const refspec of refspecs) {
      const outcome = decideWrite(repo, [refspecBranch(refspec)], ctx);
      if (outcome?.verdict === 'deny') return outcome;
      if (outcome?.verdict === 'ask') asked ??= outcome;
    }
    return asked;
  }

  if (sub.name === 'clone') {
    // `git clone [options] <repository> [directory]`: after every value-taking
    // option is removed, the first positional is the one and only remote. Do
    // not search for the first URL-shaped word — reference/template values may
    // themselves look like allowed repositories and launder the real source.
    const remote = positionals(sub.rest, GIT_CLONE_VALUE_FLAGS)[0];
    return decideRead(remote === undefined ? undefined : repoFromGitRemote(remote), ctx);
  }

  const readValueFlags = GIT_READ_VALUE_FLAGS[sub.name];
  if (readValueFlags !== undefined) {
    // Each read subcommand has a different option grammar. Once its values are
    // removed, the first positional is the repository; an earlier URL-shaped
    // option value must never launder the actual remote.
    const remote = positionals(sub.rest, readValueFlags)[0];
    return decideRead(remote === undefined ? undefined : repoFromGitRemote(remote), ctx);
  }

  // Everything else is local work. The filesystem guardrails cover that ground,
  // and repeating them here would deny commits in the name of a repository
  // policy that has nothing to say about them.
  return undefined;
}

/** Command-line groups that reach no repository. */
const GH_IGNORED: readonly string[] = [
  'auth',
  'config',
  'alias',
  'extension',
  'completion',
  'help',
  'version',
];

const GH_READ_VERBS: readonly string[] = [
  'view',
  'list',
  'status',
  'diff',
  'checks',
  'clone',
  'download',
  'search',
  'browse',
];

// Long forms only: the short `-b`/`-B` are overloaded across gh subcommands
// (`-b` is `--body` on `gh issue/pr create`), so reading a body string as a
// branch would deny the wrong thing. Missing a branch flag degrades to the
// unnamed-branch ask, which is safe; misreading one is not.
const GH_BRANCH_FLAGS: readonly string[] = ['--base', '--branch', '--target'];

const GH_REPO_FLAGS: readonly string[] = ['--repo', '-R'];
const GH_METHOD_FLAGS: readonly string[] = ['--method', '-X'];
/** `gh api` body fields, which can carry a repo api path as their value. */
const GH_FIELD_FLAGS: readonly string[] = ['-f', '--field', '-F', '--raw-field'];
/** Every `gh api` option that consumes the following argv token. */
const GH_API_VALUE_FLAGS: readonly string[] = [
  '--cache',
  '-F',
  '--field',
  '-H',
  '--header',
  '--hostname',
  '--input',
  '-q',
  '--jq',
  '-X',
  '--method',
  '-p',
  '--preview',
  '-f',
  '--raw-field',
  '-t',
  '--template',
];
/**
 * Field keys whose value is a repository — a full slug/path, or (crossed with an
 * owner field) a bare name.
 */
const REPO_FIELD_KEYS: ReadonlySet<string> = new Set([
  'repo',
  'repository',
  'full_name',
  'fullname',
  'namewithowner',
  'slug',
  'name',
  'repo_name',
  'reponame',
]);

/** Every value the named flags carry, so a decoy `--branch` cannot hide a `--base`. */
function flagValues(args: readonly string[], flags: readonly string[]): string[] {
  const found: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const token = args[i] as string;
    if (flags.includes(token)) {
      const value = args[i + 1];
      if (value !== undefined && !value.startsWith('-')) found.push(value);
    }
    for (const name of flags) {
      if (token.startsWith(`${name}=`)) found.push(token.slice(name.length + 1));
    }
  }
  return found;
}

/**
 * Positional words, skipping any word that reads as a flag's value. The flags
 * this command line accepts are not knowable here, so the rule is positional: a
 * plain word directly after a flag belongs to that flag.
 */
function ghPositionals(args: readonly string[]): string[] {
  const found: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const token = args[i] as string;
    if (token === '--') {
      found.push(...args.slice(i + 1));
      break;
    }
    if (token.startsWith('-') && token.length > 1) {
      const next = args[i + 1];
      if (next !== undefined && !next.startsWith('-')) i += 1;
      continue;
    }
    found.push(token);
  }
  return found;
}

/** Effective method of `gh api`, following gh's own defaulting rules. */
function ghApiReads(args: readonly string[]): boolean {
  const explicit = flagValues(args, GH_METHOD_FLAGS).pop();
  if (explicit !== undefined) {
    const method = explicit.toUpperCase();
    return method === 'GET' || method === 'HEAD';
  }

  const hasFields = flagValues(args, GH_FIELD_FLAGS).length > 0;
  const hasInput = args.some((arg) => arg === '--input' || arg.startsWith('--input='));
  return !hasFields && !hasInput;
}

function ghOutcome(argv: readonly string[], ctx: EvaluatorContext): GuardrailOutcome | undefined {
  const args = argv.slice(1);
  let words = ghPositionals(args);
  const group = words[0];
  if (group === undefined || GH_IGNORED.includes(group)) return undefined;
  // The endpoint is a security boundary for `gh api`, so parse that subcommand
  // from its declared option arity. The generic parser must remain conservative
  // for arbitrary extension commands, but it cannot guess that `--verbose`,
  // `--silent`, `--paginate`, and friends are valueless.
  if (group === 'api') words = positionals(args, GH_API_VALUE_FLAGS);

  // Every repository the command names — the `--repo`/`-R` flags and every
  // positional that resolves (a slug, a url, or a `repos/owner/name` api path) —
  // not just the first. Taking one let an allowlisted `--repo` launder the real
  // target in an `gh api repos/victim/secret/...` path beside it.
  const repos = new Set<string>();
  for (const value of flagValues(args, GH_REPO_FLAGS)) repos.add(stripGitSuffix(value));
  for (const word of words) {
    const found = repoFromToken(word);
    if (found !== undefined) repos.add(found);
  }
  // A repo can also hide in a `-f`/--field value. A repo-keyed field
  // (`-f repo=victim/secret`) is read as a slug or path; a bare name there is
  // crossed with any owner field (`-f owner=victim -f repo=secret`), mirroring
  // the MCP resolver; any other field is read only for the anchored
  // `repos/owner/name` api-path shape, so `-f ref=heads/main` is not mistaken.
  const fieldOwners: string[] = [];
  const fieldNames: string[] = [];
  for (const field of flagValues(args, GH_FIELD_FLAGS)) {
    const eq = field.indexOf('=');
    if (eq <= 0) continue;
    const key = field.slice(0, eq).toLowerCase();
    const value = field.slice(eq + 1);
    if (REPO_FIELD_KEYS.has(key)) {
      const found = repoFromToken(value);
      if (found !== undefined) repos.add(found);
      else fieldNames.push(value);
    } else if (OWNER_KEYS.includes(key)) {
      fieldOwners.push(value);
    } else {
      const api = API_PATH.exec(value);
      if (api) repos.add(`${api[1]}/${api[2]}`);
    }
  }
  for (const owner of fieldOwners) {
    for (const name of fieldNames) repos.add(`${owner}/${stripGitSuffix(name)}`);
  }

  // Read vs write is decided by the subcommand — the command group and, where
  // there is one, its action — never by any positional. `gh api` is the
  // exception because its effective HTTP method is the operation: GET by
  // default, POST when fields/body are supplied, with --method/-X overriding.
  const action = words[1];
  const reading =
    group === 'api'
      ? ghApiReads(args)
      : GH_READ_VERBS.includes(group) || (action !== undefined && GH_READ_VERBS.includes(action));
  const branches = reading ? [] : flagValues(args, GH_BRANCH_FLAGS).map(stripRefPrefix);
  const decide = (repo: string | undefined): GuardrailOutcome | undefined =>
    reading ? decideRead(repo, ctx) : decideWrite(repo, branches, ctx);

  // `gh api … --input <file>`/`--input -` carries an opaque request body that can
  // name the true target repo (a GraphQL query, say). It cannot be inspected, so
  // a write carrying one is treated as naming an unresolvable repo: it asks
  // rather than passing on the strength of an allowlisted `--repo` beside it.
  const opaqueBody =
    group === 'api' && args.some((arg) => arg === '--input' || arg.startsWith('--input='));

  // The strictest verdict any named repo earns is the verdict for the call, so an
  // allowlisted decoy cannot clear a forbidden one; an unresolved repo asks.
  if (repos.size === 0) return decide(undefined);
  let asked: GuardrailOutcome | undefined = opaqueBody && !reading ? decide(undefined) : undefined;
  for (const repo of repos) {
    const outcome = decide(repo);
    if (outcome?.verdict === 'deny') return outcome;
    if (outcome?.verdict === 'ask') asked ??= outcome;
  }
  return asked;
}

function shellOutcome(ctx: EvaluatorContext): GuardrailOutcome | undefined {
  if (!toolMatches(ctx, OS_SHELL_TOOLS)) return undefined;
  const command = ctx.event.tool?.input?.['command'];
  if (typeof command !== 'string' || command.length === 0) return undefined;

  // One command string can run several invocations. The strictest verdict any
  // of them earns is the verdict for the string: a denied push is not made
  // acceptable by the permitted commands chained around it.
  let asked: GuardrailOutcome | undefined;
  for (const parsed of parseShellCommands(command)) {
    // A command nested past what the reader can follow (see `unresolved` in
    // ../shell.ts) may be a push to a forbidden repository; its argv is empty,
    // so name matching would silently pass it. Fail closed to an approval, the
    // same degradation this policy uses whenever it cannot resolve a target.
    if (parsed.unresolved) {
      asked ??= ask(
        'A command nested too deeply to inspect could reach a repository; approve it explicitly.',
      );
      continue;
    }
    const name = commandName(parsed.argv);
    const outcome =
      name === 'git'
        ? gitOutcome(parsed.argv, ctx)
        : name === 'gh'
          ? ghOutcome(parsed.argv, ctx)
          : undefined;
    if (outcome?.verdict === 'deny') return outcome;
    if (outcome?.verdict === 'ask') asked ??= outcome;
  }
  return asked;
}

export const githubPolicy: BuiltinEvaluator = (ctx) => {
  const call = githubCall(ctx);
  if (call) return mcpOutcome(call, ctx);
  return shellOutcome(ctx);
};
