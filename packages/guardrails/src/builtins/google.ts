// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { BuiltinEvaluator, EvaluatorContext } from '../engine.js';
import { matchesAnyToolPattern, parseMcpToolName } from '../tool-names.js';
import type { StateUpdate } from '../types.js';
import { allowWith, ask, deny, stringList, stringParam } from './helpers.js';

/**
 * Predicates for the Google integrations: Drive — with Docs, Sheets and Slides,
 * which are Drive files under different tools — plus Gmail and Calendar.
 *
 * Two decisions shape everything below.
 *
 * The first is that a call is recognised by its MCP *server* rather than by an
 * enumerated list of tool names. A list would have to be complete to be worth
 * anything, and the day a server adds `export_file` the guardrail that names
 * fifteen tools silently stops covering the sixteenth. Matching the server and
 * then classifying the tool by its own verb fails the other way: an unfamiliar
 * verb is treated as a mutation, so a new tool arrives gated rather than open.
 *
 * The second is that server matching is deliberately loose — a server whose name
 * contains `mail` is treated as mail. Over-matching costs an author a denial
 * they can see and narrow; under-matching produces a guardrail that is enabled,
 * reports no problems, and enforces nothing.
 */

/** Substrings that identify each family in an MCP server name. */
const DRIVE_SERVERS: readonly string[] = ['drive', 'docs', 'sheet', 'slide'];
const MAIL_SERVERS: readonly string[] = ['mail'];
const CALENDAR_SERVERS: readonly string[] = ['calendar', 'gcal'];

/**
 * Stems that identify each family in a *tool* name's own words, matched as a
 * substring of a word. A unified server (`google_workspace`, `gsuite`, a bare
 * `google`) leaves the server hint unmatched, and then a guardrail keyed on the
 * server alone is enabled but enforces nothing — the worse failure. So the tool
 * name is a second, independent path to recognition, and it matches by stem
 * (`gdrive` and `document` both carry the family) rather than an exact word list
 * that has to enumerate every spelling. The cost is over-matching an unrelated
 * tool that happens to contain a stem (`docker`), which is a denial the author
 * sees and narrows — the safe direction.
 */
const DRIVE_TOOL_WORDS: readonly string[] = [
  'drive',
  'doc',
  'sheet',
  'spreadsheet',
  'slide',
  'presentation',
];
const MAIL_TOOL_WORDS: readonly string[] = ['mail'];
const CALENDAR_TOOL_WORDS: readonly string[] = ['calendar', 'gcal'];

/** Exact leading service words used by unified Google MCP servers. */
const DRIVE_SERVICE_PREFIXES: readonly string[] = [
  'drive',
  'gdrive',
  'doc',
  'docs',
  'document',
  'sheet',
  'sheets',
  'spreadsheet',
  'slide',
  'slides',
  'presentation',
];
const MAIL_SERVICE_PREFIXES: readonly string[] = ['gmail', 'mail'];
const CALENDAR_SERVICE_PREFIXES: readonly string[] = ['calendar', 'gcal'];

/**
 * Server-name substrings that mark a *unified* Google deployment — one server
 * fronting Drive, Gmail and Calendar together, so its own name carries no
 * per-service hint. Only on such a server do the weak tool words below apply.
 */
const GOOGLE_FAMILY_SERVERS: readonly string[] = [
  ...DRIVE_SERVERS,
  ...MAIL_SERVERS,
  ...CALENDAR_SERVERS,
  'google',
  'gsuite',
  'gworkspace',
  'workspace',
  'gapps',
];

/**
 * `message`/`event` are the words Gmail and Calendar use for their own resources
 * (`messages.send`, `events.list`), but each is also a generic word an unrelated
 * server uses (Slack messages, analytics events). So they recognise a family only
 * on a Google-family server: enough to gate a unified server's mail/calendar
 * tools, without a `gmail_policy` reaching into a Slack messaging tool.
 */
const MAIL_WEAK_TOOL_WORDS: readonly string[] = ['message'];
const CALENDAR_WEAK_TOOL_WORDS: readonly string[] = ['event'];

interface IntegrationCall {
  /** The tool name split into lower-case words, snake_case or camelCase. */
  words: readonly string[];
  input: Record<string, unknown>;
}

/** Lower-case word split that survives both `read_file` and `readFile`. */
function words(value: string): string[] {
  return value
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((word) => word.length > 0);
}

function integrationCall(
  ctx: EvaluatorContext,
  servers: readonly string[],
  toolWords: readonly string[],
  servicePrefixes: readonly string[],
  weakToolWords: readonly string[] = [],
): IntegrationCall | undefined {
  const name = ctx.event.tool?.name;
  if (name === undefined) return undefined;
  const parsed = parseMcpToolName(name);
  if (!parsed) return undefined;
  const server = parsed.serverName.toLowerCase().replace(/[^a-z0-9]/g, '');
  const toolNameWords = words(parsed.toolName);
  const serverMatch = servers.some((hint) => server.includes(hint));
  // Either path recognises the family; neither is required. A tool word carrying
  // a stem (`gdrive`, `document`, `spreadsheet`) is enough, so a unified server's
  // own tools are gated rather than silently unguarded.
  const toolMatch = toolNameWords.some((word) => toolWords.some((stem) => word.includes(stem)));
  // Generic words (`message`, `event`) recognise the family only on a Google
  // server, so they cover a unified deployment's tools without matching an
  // unrelated server that happens to use the same word.
  const weakMatch =
    weakToolWords.length > 0 &&
    GOOGLE_FAMILY_SERVERS.some((hint) => server.includes(hint)) &&
    toolNameWords.some((word) => weakToolWords.some((stem) => word.includes(stem)));
  if (!serverMatch && !toolMatch && !weakMatch) return undefined;
  // Unified servers commonly prefix the operation with its service
  // (`calendar_create_event`, `drive_copy_file`, `gmail_list_messages`). Strip
  // only a recognised family prefix; unknown leading words retain the safe
  // mutation fallback below.
  const first = toolNameWords[0];
  const operationWords =
    first !== undefined && servicePrefixes.includes(first)
      ? toolNameWords.slice(1)
      : toolNameWords;
  return { words: operationWords, input: ctx.event.tool?.input ?? {} };
}

function verbOf(call: IntegrationCall): string {
  return call.words[0] ?? '';
}

function flag(params: Record<string, unknown>, key: string, fallback: boolean): boolean {
  const value = params[key];
  return typeof value === 'boolean' ? value : fallback;
}

function stringArrayState(state: Readonly<Record<string, unknown>>, key: string): string[] {
  const value = state[key];
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : [];
}

/** True when any identifier matches any pattern. Patterns accept `*`. */
function matchesAny(values: readonly string[], patterns: readonly string[]): boolean {
  return values.some((value) => matchesAnyToolPattern(value, patterns));
}

// --------------------------------------------------------------------- Drive

const DRIVE_READ_VERBS: readonly string[] = [
  'get',
  'list',
  'search',
  'read',
  'download',
  'fetch',
  'describe',
  'view',
  'find',
  'query',
  'export',
];

/** Verbs that produce a *new* file rather than changing an existing one. */
const DRIVE_CREATE_VERBS: readonly string[] = ['create', 'copy', 'duplicate', 'upload', 'import'];

type DriveOperation = 'read' | 'comment' | 'create' | 'write';

function driveOperation(call: IntegrationCall): DriveOperation {
  const verb = verbOf(call);
  if (DRIVE_READ_VERBS.includes(verb)) return 'read';
  // Checked before `create`, so `create_comment` is a comment and not a new file.
  if (call.words.includes('comment') || call.words.includes('comments')) return 'comment';
  if (DRIVE_CREATE_VERBS.includes(verb)) return 'create';
  // Anything unrecognised is a mutation. A new tool this evaluator has never
  // heard of is exactly the case where guessing "read" would be a silent hole.
  return 'write';
}

function isDriveCopy(call: IntegrationCall): boolean {
  const verb = verbOf(call);
  return verb === 'copy' || verb === 'duplicate';
}

/**
 * Argument names that identify a file.
 *
 * Ids, names and titles are all here because an operator writes the identifier
 * they know — a Drive id in one policy, a document title in another — and a
 * guardrail that only understood ids would quietly not apply to the second.
 */
const DRIVE_FILE_KEYS: readonly string[] = [
  'file_id',
  'fileId',
  'file',
  'files',
  'file_ids',
  'fileIds',
  'document_id',
  'documentId',
  'doc_id',
  'docId',
  'spreadsheet_id',
  'spreadsheetId',
  'presentation_id',
  'presentationId',
  'id',
  'ids',
  'name',
  'title',
  'file_name',
  'fileName',
  'path',
];

/**
 * Every identifier a call carries, flattened. Used where a call names one file
 * and any spelling of it is as good as another: a read whose id and title are
 * two aliases of the same file, or the source of a copy.
 */
function fileTargets(input: Record<string, unknown>): string[] {
  const found: string[] = [];
  for (const key of DRIVE_FILE_KEYS) {
    const value = input[key];
    if (typeof value === 'string' && value.length > 0) found.push(value);
    else if (Array.isArray(value)) {
      for (const item of value) if (typeof item === 'string' && item.length > 0) found.push(item);
    }
  }
  return found;
}

/**
 * Keys whose value names a file by its human-readable name rather than its id.
 * On a mutation that also carries an id, this is the *new* name being assigned,
 * not an identifier of the file being changed — so it must not be matched
 * against an access list, or renaming a forbidden file into an allowlisted name
 * would launder it in.
 */
const DRIVE_NAME_KEYS: readonly string[] = ['name', 'title', 'file_name', 'fileName'];

/** Id-type keys: every file key that is not a human-readable name. */
const DRIVE_ID_KEYS: readonly string[] = DRIVE_FILE_KEYS.filter(
  (key) => !DRIVE_NAME_KEYS.includes(key),
);

/** Every non-empty string a set of keys carries, scalars and array elements alike. */
function collectStrings(input: Record<string, unknown>, keys: readonly string[]): string[] {
  const found: string[] = [];
  for (const key of keys) {
    const value = input[key];
    if (typeof value === 'string' && value.length > 0) found.push(value);
    else if (Array.isArray(value)) {
      for (const item of value) if (typeof item === 'string' && item.length > 0) found.push(item);
    }
  }
  return found;
}

/**
 * The distinct files a call names, each of which the access check must clear
 * independently.
 *
 * Id-type keys are authoritative: two different id values are two different
 * files, so a permitted id can never clear a forbidden one beside it — this is
 * what closes the laundering where `{file_id: forbidden, document_id: allowed}`
 * slipped through an any-match. True aliases (one id under both `file_id` and
 * `document_id`) carry the same value and collapse.
 *
 * A human-readable `name`/`title` counts as a file except in one case: a
 * *mutation* that also carries an id, where the name is the value a rename
 * assigns rather than a file to check. A read never renames, so its name is a
 * file addressed by title and must be checked — otherwise a permitted id decoy
 * beside a forbidden title would launder the read.
 */
function fileIdentifiers(input: Record<string, unknown>, operation: DriveOperation): string[] {
  const ids = collectStrings(input, DRIVE_ID_KEYS);
  const names = collectStrings(input, DRIVE_NAME_KEYS);
  const mutation = operation === 'write' || operation === 'comment';
  const identifiers = mutation && ids.length > 0 ? ids : [...ids, ...names];
  return [...new Set(identifiers)];
}

/** The source a copy reads; a name beside an id is normally the destination name. */
function copySourceIdentifiers(input: Record<string, unknown>): string[] {
  const ids = collectStrings(input, DRIVE_ID_KEYS);
  return [...new Set(ids.length > 0 ? ids : collectStrings(input, DRIVE_NAME_KEYS))];
}

/** A file is reachable when its identifier is on the list or was created here. */
function identifierPermitted(
  id: string,
  patterns: readonly string[],
  created: readonly string[],
): boolean {
  return matchesAnyToolPattern(id, patterns) || created.includes(id);
}

/** Keys a create's result carries the new file's id under. */
const DRIVE_RESULT_ID_KEYS: readonly string[] = [
  'id',
  'file_id',
  'fileId',
  'document_id',
  'documentId',
  'spreadsheet_id',
  'spreadsheetId',
  'presentation_id',
  'presentationId',
];

/**
 * The new file's id from a create result — only the top level of the result
 * object. Recursing would harvest an id from caller-echoed nested structure
 * (`{id: real, appProperties: {id: victim}}`), poisoning the created set so a
 * later write to the victim is exempt. A server that wraps its id deeper simply
 * records nothing, which fails safe: the created-file exemption is an allowance,
 * so missing it only makes a later write go through the normal access check.
 */
function idsInResult(value: unknown): string[] {
  if (value === null || typeof value !== 'object') return [];
  const found: string[] = [];
  for (const [key, nested] of Object.entries(value as Record<string, unknown>)) {
    if (typeof nested === 'string' && DRIVE_RESULT_ID_KEYS.includes(key) && nested.length > 0) {
      found.push(nested);
    }
  }
  return found;
}

/**
 * File ids a successful read returned, including ids in search/list wrappers.
 * Unlike the created-file set, recording a nested id here grants no capability:
 * it can only mark the session as carrying confidential data, so recursively
 * inspecting recognised id keys fails in the restrictive direction.
 */
function idsInReadResult(value: unknown): string[] {
  const found = new Set<string>();
  const pending: unknown[] = [value];
  const seen = new WeakSet<object>();

  while (pending.length > 0) {
    const current = pending.pop();
    if (current === null || typeof current !== 'object' || seen.has(current)) continue;
    seen.add(current);

    if (Array.isArray(current)) {
      pending.push(...current);
      continue;
    }
    for (const [key, nested] of Object.entries(current as Record<string, unknown>)) {
      if (typeof nested === 'string' && DRIVE_RESULT_ID_KEYS.includes(key) && nested.length > 0) {
        found.add(nested);
      } else if (nested !== null && typeof nested === 'object') {
        pending.push(nested);
      }
    }
  }
  return [...found];
}

/** Files read this session that the policy calls confidential. */
const CONFIDENTIAL_READ_KEY = 'gdrive_confidential_read';
/** Files this session created, which stay writable without being listed. */
const CREATED_KEY = 'gdrive_created';

function append(key: string, value: string): StateUpdate {
  return { scope: 'session', key, action: 'append', value };
}

function driveResultFailed(result: unknown): boolean {
  if (result === null || typeof result !== 'object') return false;
  const record = result as Record<string, unknown>;
  const error = record['error'];
  return (
    record['is_error'] === true ||
    record['isError'] === true ||
    record['ok'] === false ||
    record['success'] === false ||
    (error !== undefined && error !== null && error !== false && error !== '')
  );
}

/**
 * The result phase, where the evaluator learns what actually happened: which
 * confidential files the session has now read, and which files it created.
 * Neither is knowable at `tool_call` — the call had not run yet, and a create
 * has no id until the server assigns one.
 */
function observeDrive(
  call: IntegrationCall,
  operation: DriveOperation,
  targets: readonly string[],
  ctx: EvaluatorContext,
): ReturnType<BuiltinEvaluator> {
  if (driveResultFailed(ctx.event.result)) return undefined;

  const updates: StateUpdate[] = [];
  const confidential = stringList(ctx.params, 'confidential_files');

  const reading = operation === 'read' || isDriveCopy(call);
  if (reading) {
    const observed = new Set([...targets, ...idsInReadResult(ctx.event.result)]);
    for (const target of observed) {
      if (matchesAnyToolPattern(target, confidential)) {
        updates.push(append(CONFIDENTIAL_READ_KEY, target));
      }
    }
  }

  if (operation === 'create') {
    // Only the server-assigned id the result carries. The input identifier is
    // not recorded: on a copy it is the source, and on a plain create it is an
    // agent-chosen name — recording either would let a `create {name: <victim
    // id>}` mark a file the session never made as created and so writable. A
    // later write must use the returned id, not a name it asserted.
    for (const id of idsInResult(ctx.event.result)) updates.push(append(CREATED_KEY, id));
  }

  return updates.length > 0 ? allowWith(updates) : undefined;
}

export const gdrivePolicy: BuiltinEvaluator = (ctx) => {
  const call = integrationCall(ctx, DRIVE_SERVERS, DRIVE_TOOL_WORDS, DRIVE_SERVICE_PREFIXES);
  if (!call) return undefined;

  const operation = driveOperation(call);
  const targets = fileTargets(call.input);

  if (ctx.event.phase === 'tool_result') return observeDrive(call, operation, targets, ctx);
  if (ctx.event.phase !== 'tool_call') return undefined;

  const confidential = stringList(ctx.params, 'confidential_files');
  const created = stringArrayState(ctx.state, CREATED_KEY);

  const identifiers = fileIdentifiers(call.input, operation);

  if (operation === 'read') {
    return driveReadAccessDenial(identifiers, ctx);
  }

  if (isDriveCopy(call)) {
    const sourceDenial = driveReadAccessDenial(copySourceIdentifiers(call.input), ctx);
    if (sourceDenial) return sourceDenial;
  }

  // A mutation is checked against every distinct file it names, so no single
  // permitted id clears a call that also names a forbidden one.
  const accessDenial = driveAccessDenial(operation, identifiers, created, ctx);
  if (accessDenial) return accessDenial;

  return containment(call, identifiers, confidential, ctx);
};

/** The read-list half, shared by ordinary reads and the source side of a copy. */
function driveReadAccessDenial(
  identifiers: readonly string[],
  ctx: EvaluatorContext,
): ReturnType<BuiltinEvaluator> {
  if (flag(ctx.params, 'read_all', true)) return undefined;
  const readable = stringList(ctx.params, 'read_files');
  if (readable.length === 0) return deny('This agent may not read Drive files.');
  if (identifiers.length === 0) {
    return ask('This read names no file that can be checked against the policy.');
  }
  const forbidden = identifiers.find((id) => !matchesAnyToolPattern(id, readable));
  if (forbidden !== undefined) {
    return deny(`Reading ${forbidden} is outside this agent's Drive access.`);
  }
  return undefined;
}

/** The access-list half: may this agent touch every file this call names? */
function driveAccessDenial(
  operation: DriveOperation,
  identifiers: readonly string[],
  created: readonly string[],
  ctx: EvaluatorContext,
): ReturnType<BuiltinEvaluator> {
  if (operation === 'create') {
    return flag(ctx.params, 'allow_create', false)
      ? undefined
      : deny('This agent may not create Drive files.');
  }

  const known = identifiers.length > 0;
  const writable = stringList(ctx.params, 'write_files');
  if (operation === 'comment') {
    // A file this agent may rewrite is one it may certainly comment on.
    const commentable = [...stringList(ctx.params, 'comment_files'), ...writable];
    if (commentable.length === 0 && created.length === 0) {
      return deny('This agent may not comment on Drive files.');
    }
    if (!known) return ask('This comment names no file that can be checked against the policy.');
    const forbidden = identifiers.find((id) => !identifierPermitted(id, commentable, created));
    if (forbidden !== undefined) {
      return deny(`Commenting on ${forbidden} is outside this agent's Drive access.`);
    }
    return undefined;
  }

  if (writable.length === 0 && created.length === 0) {
    return deny('This agent may not modify Drive files.');
  }
  if (!known) return ask('This write names no file that can be checked against the policy.');
  const forbidden = identifiers.find((id) => !identifierPermitted(id, writable, created));
  if (forbidden !== undefined) {
    return deny(`Writing to ${forbidden} is outside this agent's Drive access.`);
  }
  return undefined;
}

/**
 * The containment half: confidential content must not end up somewhere less
 * protected. Once a session has read a confidential file, every write it makes
 * to a destination that is not itself confidential can carry that content, and
 * no access list catches it — the destination may be perfectly writable.
 *
 * A file the session created is *not* an exemption here. It is an exemption
 * from the access list, because a session must be able to write what it just
 * made; a brand new file is still a lower tier than the confidential one, so
 * copying protected content into it is the breach this rule exists for.
 */
function containment(
  call: IntegrationCall,
  identifiers: readonly string[],
  confidential: readonly string[],
  ctx: EvaluatorContext,
): ReturnType<BuiltinEvaluator> {
  if (confidential.length === 0) return undefined;

  // A copy names its source, and its destination is a new file that no argument
  // identifies — so a copy of a confidential file is a write-down on its own,
  // with no earlier read needed.
  const verb = verbOf(call);
  const copying = verb === 'copy' || verb === 'duplicate';
  const carrying =
    stringArrayState(ctx.state, CONFIDENTIAL_READ_KEY).length > 0 ||
    (copying && matchesAny(fileTargets(call.input), confidential));
  if (!carrying) return undefined;

  // The destination is exempt only when every file it names is itself
  // confidential, so an attacker-supplied extra id or decoy title matching the
  // pattern cannot buy the exemption for a write whose real destination is a
  // lower tier.
  const destinationConfidential =
    !copying &&
    identifiers.length > 0 &&
    identifiers.every((id) => matchesAnyToolPattern(id, confidential));
  if (destinationConfidential) return undefined;

  const reason =
    'This session has read a confidential Drive file; writing to a file outside the ' +
    'confidential set would move that content to a lower tier.';
  return stringParam(ctx.params, 'write_down_action') === 'ask' ? ask(reason) : deny(reason);
}

// --------------------------------------------------------------------- Gmail

const MAIL_READ_VERBS: readonly string[] = [
  'get',
  'list',
  'search',
  'read',
  'fetch',
  'download',
  'view',
  'find',
  'query',
];

const MAIL_SEND_WORDS: readonly string[] = ['send', 'forward', 'reply'];

type MailCapability = 'read' | 'send' | 'drafts' | 'modify';

function mailCapability(call: IntegrationCall): MailCapability {
  // Sending is checked first so that sending a draft is a send, not a draft
  // edit — the difference is whether mail leaves the account.
  if (call.words.some((word) => MAIL_SEND_WORDS.includes(word))) return 'send';
  if (MAIL_READ_VERBS.includes(verbOf(call))) return 'read';
  if (call.words.includes('draft') || call.words.includes('drafts')) return 'drafts';
  return 'modify';
}

const MAIL_RULES: Readonly<
  Record<MailCapability, { param: string; fallback: boolean; deny: string }>
> = {
  read: { param: 'allow_read', fallback: true, deny: 'This agent may not read mail.' },
  send: { param: 'allow_send', fallback: false, deny: 'This agent may not send mail.' },
  drafts: { param: 'allow_drafts', fallback: true, deny: 'This agent may not work on drafts.' },
  modify: {
    param: 'allow_modify',
    fallback: false,
    deny: 'This agent may not modify existing mail.',
  },
};

export const gmailPolicy: BuiltinEvaluator = (ctx) => {
  const call = integrationCall(
    ctx,
    MAIL_SERVERS,
    MAIL_TOOL_WORDS,
    MAIL_SERVICE_PREFIXES,
    MAIL_WEAK_TOOL_WORDS,
  );
  if (!call) return undefined;
  const rule = MAIL_RULES[mailCapability(call)];
  // Deny, never ask: a capability switch has no target a human could approve
  // one instance of, so an approval prompt would just be a slower `false`.
  return flag(ctx.params, rule.param, rule.fallback) ? undefined : deny(rule.deny);
};

// ------------------------------------------------------------------ Calendar

const CALENDAR_READ_VERBS: readonly string[] = [
  'get',
  'list',
  'search',
  'read',
  'fetch',
  'view',
  'find',
  'query',
  // Proposing a time inspects availability and writes nothing.
  'suggest',
];

const CALENDAR_CREATE_VERBS: readonly string[] = ['create', 'add', 'quick', 'insert'];

type CalendarCapability = 'read' | 'create' | 'modify';

function calendarCapability(call: IntegrationCall): CalendarCapability {
  const verb = verbOf(call);
  if (CALENDAR_READ_VERBS.includes(verb)) return 'read';
  if (CALENDAR_CREATE_VERBS.includes(verb)) return 'create';
  return 'modify';
}

const CALENDAR_RULES: Readonly<
  Record<CalendarCapability, { param: string; fallback: boolean; deny: string }>
> = {
  read: { param: 'allow_read', fallback: true, deny: 'This agent may not read the calendar.' },
  create: {
    param: 'allow_create_events',
    fallback: false,
    deny: 'This agent may not create calendar events.',
  },
  modify: {
    param: 'allow_modify_events',
    fallback: false,
    deny: 'This agent may not modify existing calendar events.',
  },
};

export const gcalendarPolicy: BuiltinEvaluator = (ctx) => {
  const call = integrationCall(
    ctx,
    CALENDAR_SERVERS,
    CALENDAR_TOOL_WORDS,
    CALENDAR_SERVICE_PREFIXES,
    CALENDAR_WEAK_TOOL_WORDS,
  );
  if (!call) return undefined;
  const rule = CALENDAR_RULES[calendarCapability(call)];
  return flag(ctx.params, rule.param, rule.fallback) ? undefined : deny(rule.deny);
};
