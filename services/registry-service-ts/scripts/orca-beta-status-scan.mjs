// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Which operations answer a *different HTTP status* under the `orca-beta`
 * header — read out of the route sources, never maintained by hand.
 *
 * A hand-maintained list is how one of these was missed: nothing about a stale
 * list looks different from a correct one. This module derives the list from the
 * only place the behaviour actually lives, `reply.code(...)`, so a conditional
 * status that exists in the code cannot be absent from the list.
 *
 * ## What this scan covers, and what it does not
 *
 * This matters more than the mechanism. A green run here means *one* thing, and
 * reading it as "the wire agrees with the spec" would be exactly the
 * over-trusted check this framework exists to remove.
 *
 * **Covered — the status axis, and only for the syntactic forms below.** A
 * `reply.code(<dialect condition> ? A : B)` and an
 * `if (<dialect condition>) ... reply.code(A)`, including a direct dialect
 * boolean term or the credential-visibility helper whose second argument is
 * that term. All are pinned by count
 * ({@link EXPECTED_STATUS_SITES}, {@link EXPECTED_SUCCESS_STATUS_SITES}), so a
 * third form, or a further site, fails the scan instead of quietly exempting
 * itself. The remaining sites are `400`s reachable only by default-dialect
 * clients; no success status depends on the dialect.
 *
 * **Not covered — response shape.** The dialect branch for shape is *not* at the
 * `reply.code` call; it is an argument to a mapper — `storeToApi(created,
 * orcaBeta)`, `modelToApi(model, orcaBeta)`, `threadToApi(db, row, session,
 * orcaBeta)`. A scan keyed on `reply.code` structurally cannot see it: the two
 * axes branch at different call sites, so one signal cannot cover both. There
 * are far more shape branches than status branches, and none of them are
 * counted here.
 *
 * **Not covered here — type and nullability agreement.** Whether the `string`
 * our spec declares is the `string | null` Anthropic declares is a third axis.
 * This scan cannot see it. The offline differ's oasdiff schema rows compare
 * types and nullability of request and success bodies, and the live SDK suite
 * (`test/integration/sdk-conformance.spec.ts`) validates default-dialect
 * responses against Anthropic's vendored response schemas, including
 * nullability, at every depth.
 *
 * **Not covered — statuses the scan refuses to guess at.** Nine
 * `reply.code(<expression>)` call sites pass a variable rather than a literal
 * (idempotent-replay, an error-mapping helper, and so on). The scan cannot
 * resolve them and does not pretend to; it counts them
 * ({@link EXPECTED_OPAQUE_SITES}) so a tenth forces a human to look.
 *
 * Pure module, no I/O: the caller reads the sources and passes them in, the same
 * split `conformance-core.mjs` uses so a unit test can drive it directly.
 */
import { operationKey } from './lib/normalize-operation.mjs';

/** Fastify route-registration methods this scan recognizes. */
const ROUTE_METHODS = ['get', 'post', 'put', 'delete', 'patch', 'options', 'head'];

/**
 * Every `reply.code(...)` whose status depends on the `orca-beta` header.
 *
 * **This pin and the exception list derived from this scan are one mechanism,
 * not two.** `test/unit/conformance.spec.ts` lets a scanned site excuse an
 * operation from "success codes match Anthropic's". Without this pin, a
 * developer adding another conditional status would extend the exception list by
 * writing the code — the invariant would justify whatever the code happened to
 * do. Keep both halves or neither.
 *
 * Eleven today, all reachable only by default-dialect clients: five `400`s from
 * stricter session-event validation, one `400` for creating a provider
 * credential without `orca-beta`, and five `404`s that hide provider
 * credentials from the default dialect. They remain counted so a scan that
 * quietly discarded the error sites cannot still present itself as complete.
 *
 * The fifth session-event `400` splits an unreadable harness annotation out of
 * "this session's execution harness does not support `user.tool_result`" — same
 * operation, same already-declared `400` status, distinct message naming the
 * cause the caller can actually act on.
 */
export const EXPECTED_STATUS_SITES = 11;

/**
 * Of those, the sites answering a 2xx that depends on the dialect.
 *
 * Pinned at zero separately from the total so adding a success branch cannot be
 * hidden by deleting one of the error branches.
 */
export const EXPECTED_SUCCESS_STATUS_SITES = 0;

/**
 * `reply.code(<expression>)` sites whose status is not a literal.
 *
 * Pinned for the same reason: an unresolvable site is a hole in this scan's
 * coverage, and a hole that can grow silently is not a documented limitation,
 * it is a bug waiting to be called a feature. Six existing sites are error paths —
 * `middleware/idempotency.ts` (replayed response), `api/sessions.routes.ts`
 * (event-resolution failure), `api/skills.routes.ts`, `api/guardrails.routes.ts`
 * and `api/model-prices.routes.ts` (each a local `fail(reply, req, status, …)`
 * error-response helper) and `api/platform.routes.ts` (pre-serialized admin
 * response). Organization-default and workspace observability PUT sites each
 * forward a typed application-service success status, which can be a
 * lock-protected cached success or finalization result. They are
 * control-plane-only and do not branch on `orca-beta`. They are opaque to this
 * status-dialect scan, not unaccounted for.
 */
// Includes the Git read proxy's error helper, whose status is a fixed error-only
// union (400/401/404/413/415/502) and cannot change a dialect's success response.
export const EXPECTED_OPAQUE_SITES = 9;

/** Does this fragment of source name the `orca-beta` dialect at all? */
const mentionsOrcaBeta = (text) => /isOrcaBetaRequest|orcaBeta/.test(text);

/**
 * Which dialect a condition selects for.
 *
 * `orcaBeta` / `isOrcaBetaRequest(req)` guard the `orca-beta` path; their
 * negation guards the default path. The same polarity applies to
 * `isCredentialVisible(row, <dialect>)`, which is true for provider credentials
 * only under `orca-beta`. The direct term may appear after another boolean
 * conjunct. A condition that mentions the dialect any other way returns `null`
 * and the caller reports it rather than guessing.
 */
export function dialectOf(condition) {
  const visibility = condition.match(
    /(!?)\s*isCredentialVisible\s*\([^,]+,\s*(?:isOrcaBetaRequest\s*\([^)]*\)|orcaBeta)\s*\)/,
  );
  if (visibility) return visibility[1] === '!' ? 'default' : 'orca-beta';

  const match = condition.match(/(?:^|&&|\|\|)\s*(!?)\s*(?:isOrcaBetaRequest\s*\(|orcaBeta\b)/);
  if (!match) return null;
  return match[1] === '!' ? 'default' : 'orca-beta';
}

/**
 * Read a balanced `(...)` starting at `open`, skipping over string literals,
 * template literals and comments so a stray parenthesis inside a message does
 * not desynchronize the reader.
 *
 * Regular-expression literals are *not* skipped. A regexp containing an
 * unbalanced parenthesis would desynchronize this reader — but not silently:
 * spans would stop containing their sites and {@link scanOrcaBetaStatuses}
 * reports an unattributed site as a problem rather than dropping it.
 */
function readBalanced(text, open) {
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '/' && text[i + 1] === '/') {
      const nl = text.indexOf('\n', i);
      i = nl === -1 ? text.length : nl;
      continue;
    }
    if (ch === '/' && text[i + 1] === '*') {
      const close = text.indexOf('*/', i + 2);
      i = close === -1 ? text.length : close + 1;
      continue;
    }
    if (ch === "'" || ch === '"' || ch === '`') {
      i += 1;
      while (i < text.length && text[i] !== ch) i += text[i] === '\\' ? 2 : 1;
      continue;
    }
    if (ch === '(') depth += 1;
    else if (ch === ')') {
      depth -= 1;
      if (depth === 0) return { end: i, inner: text.slice(open + 1, i) };
    }
  }
  return null;
}

/** Read a balanced `{...}` block starting at `open`, with the same skipping. */
function readBlock(text, open) {
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '/' && text[i + 1] === '/') {
      const nl = text.indexOf('\n', i);
      i = nl === -1 ? text.length : nl;
      continue;
    }
    if (ch === '/' && text[i + 1] === '*') {
      const close = text.indexOf('*/', i + 2);
      i = close === -1 ? text.length : close + 1;
      continue;
    }
    if (ch === "'" || ch === '"' || ch === '`') {
      i += 1;
      while (i < text.length && text[i] !== ch) i += text[i] === '\\' ? 2 : 1;
      continue;
    }
    if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return text.length;
}

const lineOf = (text, index) => text.slice(0, index).split('\n').length;

/** Every `app.get('/v1/...', …)` style registration in one file, with its span. */
function routeRegistrations(text) {
  const spans = [];
  const pattern = new RegExp(`\\b\\w+\\s*\\.\\s*(${ROUTE_METHODS.join('|')})\\s*\\(`, 'g');
  for (const match of text.matchAll(pattern)) {
    const open = match.index + match[0].length - 1;
    const balanced = readBalanced(text, open);
    if (!balanced) continue;
    const path = balanced.inner.match(/^\s*(['"`])(\/[^'"`]*)\1/);
    if (!path) continue;
    spans.push({
      method: match[1].toUpperCase(),
      path: path[2],
      start: match.index,
      end: balanced.end,
    });
  }
  return spans;
}

/**
 * `if (<condition mentioning orca-beta>)` consequents, as `[start, end)` ranges.
 *
 * A `reply.code(<literal>)` inside one of these is dialect-dependent even though
 * nothing on the call itself says so.
 */
function orcaBetaGuardRanges(text, file, problems) {
  const ranges = [];
  for (const match of text.matchAll(/\bif\s*\(/g)) {
    const open = match.index + match[0].length - 1;
    const condition = readBalanced(text, open);
    if (!condition || !mentionsOrcaBeta(condition.inner)) continue;
    let cursor = condition.end + 1;
    while (cursor < text.length && /\s/.test(text[cursor])) cursor += 1;
    const closed = text[cursor] === '{' ? readBlock(text, cursor) : text.indexOf(';', cursor);
    const end = closed === -1 ? text.length : closed;

    const dialect = dialectOf(condition.inner);
    if (!dialect) {
      // Most dialect branches decide shape, not status, and are none of this
      // scan's business — reporting every one of them would bury the case that
      // matters. Complain only when a status is actually hiding behind a
      // condition the scan cannot assign to a dialect.
      if (/\breply\s*\.\s*code\s*\(/.test(text.slice(cursor, end))) {
        problems.push(
          `${file}:${lineOf(text, match.index)}: \`if (${condition.inner.trim()})\` guards a ` +
            '`reply.code` and mentions the `orca-beta` dialect in a shape this scan does not ' +
            'recognize, so it cannot tell which dialect that status belongs to. Rewrite the ' +
            'condition or extend `dialectOf`.',
        );
      }
      continue;
    }
    ranges.push({ dialect, start: cursor, end });
  }
  return ranges;
}

/**
 * Find every `orca-beta`-conditional status in the given sources and attribute
 * each to the operation whose handler encloses it.
 *
 * `sources` is `[{ file, text }]`. Returns `{ sites, opaque, byOperation,
 * problems }`; `problems` is non-empty when the scan found something it could
 * not account for, which callers must treat as a failure rather than a warning.
 */
export function scanOrcaBetaStatuses(sources) {
  const sites = [];
  const opaque = [];
  const problems = [];

  if (!Array.isArray(sources) || sources.length === 0) {
    return {
      sites,
      successSites: [],
      opaque,
      byOperation: new Map(),
      problems: ['no sources were scanned'],
    };
  }

  for (const { file, text } of sources) {
    const registrations = routeRegistrations(text);
    const guards = orcaBetaGuardRanges(text, file, problems);

    for (const match of text.matchAll(/\breply\s*\.\s*code\s*\(/g)) {
      const open = match.index + match[0].length - 1;
      const balanced = readBalanced(text, open);
      if (!balanced) continue;
      const args = balanced.inner.trim();
      const where = { file, line: lineOf(text, match.index) };

      const ternary = args.match(/^([\s\S]+?)\?\s*(\d{3})\s*:\s*(\d{3})$/);
      let byDialect = null;
      let form = null;

      if (ternary && mentionsOrcaBeta(ternary[1])) {
        const dialect = dialectOf(ternary[1]);
        if (!dialect) {
          problems.push(
            `${file}:${where.line}: \`reply.code(${args})\` branches on the \`orca-beta\` dialect ` +
              'in a shape this scan does not recognize. Rewrite the condition or extend ' +
              '`dialectOf`; guessing which arm belongs to which dialect is how a status ends up ' +
              'attributed to the wrong client.',
          );
          continue;
        }
        form = 'ternary';
        byDialect =
          dialect === 'orca-beta'
            ? { 'orca-beta': ternary[2], default: ternary[3] }
            : { 'orca-beta': ternary[3], default: ternary[2] };
      } else if (/^\d{3}$/.test(args)) {
        const guard = guards.find(({ start, end }) => match.index > start && match.index < end);
        if (guard) {
          // A guard states the status for its own dialect only. What the other
          // dialect answers happens elsewhere in the handler, or is Fastify's
          // implicit 200 — either way this site does not say, so it is left null
          // rather than assumed.
          form = 'guard';
          byDialect = { 'orca-beta': null, default: null, [guard.dialect]: args };
        }
      } else {
        // Neither a bare literal nor a dialect ternary: a status this scan
        // cannot resolve. Counted, never silently skipped.
        opaque.push({ ...where, argument: args });
      }

      if (!form) continue;

      const enclosing = registrations
        .filter((route) => match.index > route.start && match.index < route.end)
        .sort((a, b) => a.end - a.start - (b.end - b.start))[0];
      if (!enclosing) {
        problems.push(
          `${file}:${where.line}: a dialect-conditional \`reply.code\` that is not inside any ` +
            'route registration in the same file. The scan cannot say which operation it belongs ' +
            'to, and an unattributed site is worse than no scan: it reads as covered.',
        );
        continue;
      }
      sites.push({
        ...where,
        form,
        method: enclosing.method,
        routePath: enclosing.path,
        operation: `${enclosing.method} ${enclosing.path}`,
        key: operationKey(enclosing.method, enclosing.path),
        orcaBetaCode: byDialect['orca-beta'],
        defaultCode: byDialect.default,
      });
    }
  }

  const describe = (site) =>
    `${site.operation} → orca-beta ${site.orcaBetaCode ?? '—'} / default ` +
    `${site.defaultCode ?? '—'} (${site.file}:${site.line})`;
  const successSites = sites.filter((site) => /^2\d\d$/.test(site.orcaBetaCode ?? ''));

  if (sites.length !== EXPECTED_STATUS_SITES) {
    problems.push(
      `expected ${EXPECTED_STATUS_SITES} dialect-conditional status sites, found ` +
        `${sites.length}${sites.length ? `: ${sites.map(describe).join(', ')}` : ''}. ` +
        'If a site was added, update EXPECTED_STATUS_SITES *and* check that the new status is ' +
        'declared in the contract — the count is what stops new conditional code from exempting ' +
        'itself from the success-code invariant. If a site was written in a form this scan does ' +
        'not recognize, extend the scan rather than the pin.',
    );
  }
  if (successSites.length !== EXPECTED_SUCCESS_STATUS_SITES) {
    problems.push(
      `expected ${EXPECTED_SUCCESS_STATUS_SITES} sites whose \`orca-beta\` status is a 2xx, ` +
        `found ${successSites.length}: ${successSites.map(describe).join(', ') || '—'}. ` +
        'This is the subset the success-code invariant treats as a permitted divergence, so it ' +
        'is pinned on its own rather than only inside the total.',
    );
  }
  if (opaque.length !== EXPECTED_OPAQUE_SITES) {
    problems.push(
      `expected ${EXPECTED_OPAQUE_SITES} \`reply.code(<expression>)\` sites the scan cannot ` +
        `resolve, found ${opaque.length}: ` +
        `${opaque.map((s) => `${s.file}:${s.line} \`${s.argument}\``).join(', ')}. ` +
        'Each one is a status this scan is blind to; the count is pinned so the blind spot ' +
        'cannot grow without someone noticing.',
    );
  }

  // Keyed on `(method, normalized path)` so a caller can join against either
  // spec. The normalizer is shared with the differ on purpose: it is the naming
  // bridge between `/v1/memory_stores/:id` and `/v1/memory_stores/{id}`, not the
  // property under test, which is the status code itself.
  const byOperation = new Map();
  for (const site of successSites) {
    const entry = byOperation.get(site.key) ?? { operation: site.operation, orcaBetaCodes: [] };
    entry.orcaBetaCodes = [...new Set([...entry.orcaBetaCodes, site.orcaBetaCode])].sort();
    byOperation.set(site.key, entry);
  }

  return { sites, successSites, opaque, byOperation, problems };
}
