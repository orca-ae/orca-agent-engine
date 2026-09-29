// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Source-pinning helper for the runner↔registry constants that are DECLARED TWICE.
//
// The runner is a separate process that does not link the registry service, so a
// handful of cross-component literals (wire paths, header names, event-kind
// strings) exist once on each side. Two hand-written copies are not a contract —
// they are a coincidence that holds until someone edits one of them, and the two
// sides land in different PRs, so no reviewer ever sees them together.
//
// The fix is to assert the runner's copy against the registry's SOURCE rather than
// against a second hand-copied literal in a test. Asserting
// `expect(RUNNER_X).toBe('literal')` passes just as happily when the REGISTRY side
// drifts, which is the failure mode worth catching; reading the registry file and
// extracting its literal fails on a one-sided change from EITHER direction.
//
// The file is read as TEXT, never imported. That is deliberate: importing the
// registry module would pull its whole dependency graph into a runner unit test —
// the very coupling the duplicated constants exist to avoid.
//
// Registry sources that have not landed yet: a branch can carry the runner half
// before the registry half, so a pinned file can legitimately be absent on that
// branch. `registrySourcesPresent` reports that so
// a caller can `describe.skipIf` the pin; it re-activates by itself the moment the
// registry file appears. Absence is skipped; a PRESENT file that no longer holds
// the constant is a hard failure, never a skip.
//
// Some of the vocabulary the registry owns has since moved OUT of the service and
// into `@orca/agent-event-contract`, which the registry imports. That package is a
// permanent workspace fixture, not a file waiting to land, so `contractConst` /
// `contractConstMember` have no absent-source escape hatch: a missing or renamed
// contract literal is a hard failure. The contract also declares most kinds as
// members of a frozen object rather than as top-level consts, which is why the
// member extractor exists alongside the plain one.

import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** `services/registry-service-ts/src`, resolved from this file's own location. */
const REGISTRY_SRC_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../../registry-service-ts/src',
);

/** Absolute path of a registry source file, given its path under `registry-service-ts/src`. */
export function registrySourcePath(relative: string): string {
  return resolve(REGISTRY_SRC_ROOT, relative);
}

/** True only when EVERY named registry source is present on this branch. */
export function registrySourcesPresent(relatives: readonly string[]): boolean {
  return relatives.every((relative) => existsSync(registrySourcePath(relative)));
}

/**
 * Extract a `const <NAME> = '<value>';` single-quoted string literal from a registry
 * source file.
 *
 * The `export` keyword is OPTIONAL on purpose. Most of these constants are exported,
 * but at least one — `NDJSON_CONTENT_TYPE` in `tunnel/session-skills-delivery.ts` —
 * is module-private on the registry side. A private declaration is still the
 * registry's single source of truth for that literal, and pinning against it still
 * catches the drift that matters (the registry changing the value the runner must
 * match). Requiring `export` would have silently pinned nothing there.
 */
export function registryConst(relative: string, name: string): string {
  return constLiteral(registrySourcePath(relative), name);
}

/** `packages/agent-event-contract/src`, resolved from this file's own location. */
const CONTRACT_SRC_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../../../packages/agent-event-contract/src',
);

/** Absolute path of a contract source file, given its path under the package's `src`. */
export function contractSourcePath(relative: string): string {
  return resolve(CONTRACT_SRC_ROOT, relative);
}

/** Extract a top-level `const <NAME> = '<value>'` literal from `@orca/agent-event-contract`. */
export function contractConst(relative: string, name: string): string {
  return constLiteral(contractSourcePath(relative), name);
}

/**
 * Extract `<member>: '<value>'` from a `const <OBJECT> = { … } as const;` declaration in
 * `@orca/agent-event-contract` — the shape the contract uses for its kind vocabularies
 * (`SessionThreadEventKind.created`, `AgentThreadEventKind.messageSent`, …).
 */
export function contractConstMember(relative: string, object: string, member: string): string {
  const path = contractSourcePath(relative);
  const source = readFileSync(path, 'utf8');
  const body = new RegExp(
    `(?:export\\s+)?const ${object}\\s*=\\s*\\{([\\s\\S]*?)\\}\\s*as const;`,
  ).exec(source);
  if (body === null) {
    throw new Error(`contract object ${object} not found in ${path}`);
  }
  const match = new RegExp(`^\\s*${member}\\s*:\\s*'([^']*)'`, 'm').exec(body[1]!);
  if (match === null) {
    throw new Error(`contract constant ${object}.${member} not found in ${path}`);
  }
  return match[1]!;
}

/** Shared single-quoted `const` extraction, used for both source roots. */
function constLiteral(path: string, name: string): string {
  const source = readFileSync(path, 'utf8');
  const match = new RegExp(`(?:export\\s+)?const ${name}\\s*=\\s*'([^']*)'`).exec(source);
  if (match === null) {
    throw new Error(`constant ${name} not found in ${path}`);
  }
  return match[1]!;
}
