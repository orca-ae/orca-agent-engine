// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Cross-component wire pin for `src/client.ts`.
//
// Every route this CLI calls and every event body it posts is a SECOND, hand-written
// copy of something the registry declares in `services/registry-service-ts/src`. The
// two sides are different packages that do not link, and they land in different PRs,
// so nothing puts them in front of one reviewer together.
//
// The rest of this package's tests drive `OrcaClient` against `test/fakes/registry.ts`
// — a fake that answers whatever the client asks for. That proves the client is
// self-consistent and says nothing about whether the registry agrees. It is exactly
// what let two bugs ship green:
//
//   - `stream()` called `GET /v1/sessions/:id/stream`. The registry has never served
//     that route; it serves `/v1/sessions/:id/events/stream`. Every `run` and every
//     `attach --session` would have 404'd on its first turn.
//   - `postToolConfirmation()` sent `{ tool_use_id, decision }`. The registry's
//     `user.tool_confirmation` input is `.strict()` and requires `result`, so
//     `decision` was an unknown key AND `result` was missing — a 400 on every
//     allow/deny verdict.
//
// So this spec asserts the client's literals against the registry SOURCE rather than
// against a second hand-copied literal here. `expect(path).toBe('/v1/…')` passes just
// as happily when the REGISTRY side drifts, which is the failure worth catching.
//
// The registry files are read as TEXT, never imported: importing the contract would
// pull zod and ts-rest into a CLI unit test, and pnpm's strict layout does not even
// resolve them from this package. Same reasoning, and same shape, as
// `services/session-runner/test/unit/support/registry-source-pin.ts`.

import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, it, expect } from 'vitest';
import { OrcaClient } from '../../src/client.js';
import { fakeRegistry, scriptSse } from '../fakes/registry.js';

const HERE = dirname(fileURLToPath(import.meta.url));

/** `services/registry-service-ts/src`, resolved from this file's own location. */
const REGISTRY_SRC = resolve(HERE, '../../../../services/registry-service-ts/src');

const SESSIONS_CONTRACT = resolve(REGISTRY_SRC, 'contracts/sessions.contract.ts');
const SESSIONS_ROUTES = resolve(REGISTRY_SRC, 'api/sessions.routes.ts');
const CLIENT_SRC = resolve(HERE, '../../src/client.ts');

/**
 * The registry half can legitimately be absent while the decomposition is in flight —
 * this package ships in a PR beneath the one that adds the routes. Absence SKIPS; a
 * present file that no longer holds the literal is a hard failure, never a skip.
 */
const registryPresent = [SESSIONS_CONTRACT, SESSIONS_ROUTES].every((p) => existsSync(p));

const read = (p: string): string => readFileSync(p, 'utf8');

/** The `path:` literal the sessions contract declares for one operation. */
function declaredPath(operation: string): string {
  const match = new RegExp(`${operation}:\\s*\\{[\\s\\S]*?path:\\s*'([^']+)'`).exec(
    read(SESSIONS_CONTRACT),
  );
  expect(match, `sessions.contract.ts no longer declares a path for ${operation}`).not.toBeNull();
  return match![1]!;
}

const BASE = 'http://registry.test';
const client = (fetchImpl: OrcaClientFetch): OrcaClient =>
  new OrcaClient({ baseURL: BASE, apiKey: 'orca_k' }, fetchImpl);

type OrcaClientFetch = ConstructorParameters<typeof OrcaClient>[1];

describe.skipIf(!registryPresent)('the client calls the routes the registry serves', () => {
  it('streams from the path `streamEvents` declares', async () => {
    const declared = declaredPath('streamEvents');
    const concrete = declared.replace(':id', 'ses_1');
    const reg = fakeRegistry({ [`GET ${concrete}`]: { stream: scriptSse([]) } });

    for await (const _frame of client(reg.fetch).stream('ses_1')) void _frame;

    expect(new URL(reg.requests[0]!.url).pathname).toBe(concrete);
  });

  it('appends events to the path `appendEvents` declares', async () => {
    const declared = declaredPath('appendEvents');
    const concrete = declared.replace(':id', 'ses_1');
    const reg = fakeRegistry({ [`POST ${concrete}`]: { json: { events: [] } } });

    await client(reg.fetch).postUserMessage('ses_1', 'hi');

    expect(new URL(reg.requests[0]!.url).pathname).toBe(concrete);
  });

  it('mounts the streaming path the contract declares', () => {
    // A contract entry the server never mounts is the same outage as a wrong path in
    // the client, and the contract on its own cannot catch it.
    expect(read(SESSIONS_ROUTES)).toContain(`'${declaredPath('streamEvents')}'`);
  });
});

describe.skipIf(!registryPresent)('user.tool_confirmation matches the registry schema', () => {
  /** The `user.tool_confirmation` member of `sessionEventInputSchema`, as source text. */
  function confirmationSchema(): string {
    // Anchored at the discriminating `type:` so the captured block spans EVERY key
    // of the member, `type` included — not just the ones after the literal.
    const block = /type:\s*z\.literal\('user\.tool_confirmation'\)[\s\S]*?\.strict\(\)/.exec(
      read(SESSIONS_CONTRACT),
    );
    expect(
      block,
      'sessions.contract.ts no longer declares a strict user.tool_confirmation',
    ).not.toBeNull();
    return block![0];
  }

  it('sends every field the schema requires, under the names it requires', async () => {
    const schema = confirmationSchema();
    const reg = fakeRegistry({
      'POST /v1/sessions/ses_1/events': [{ json: { events: [] } }, { json: { events: [] } }],
    });
    const c = client(reg.fetch);
    await c.postToolConfirmation('ses_1', 'tool_abc', 'deny', 'not on a shared host');
    await c.postToolConfirmation('ses_1', 'tool_xyz', 'allow');

    const posted = reg
      .requestsFor('POST /v1/sessions/ses_1/events')
      .map((r) => (r.body as { events: Array<Record<string, unknown>> }).events[0]!);
    expect(posted).toHaveLength(2);

    for (const field of ['type', 'tool_use_id', 'result']) {
      expect(schema, `the schema no longer declares ${field}`).toContain(`${field}:`);
      for (const event of posted) expect(Object.keys(event)).toContain(field);
    }

    // `.strict()` means a key the schema does not declare is a 400, not an ignored extra.
    for (const event of posted) {
      for (const key of Object.keys(event)) {
        expect(schema, `the client sends '${key}', which the schema does not declare`).toContain(
          `${key}:`,
        );
      }
    }

    // `deny_message` is superRefined to deny-only, so an allow must never carry it.
    expect(Object.keys(posted[1]!)).not.toContain('deny_message');
  });

  it('does not send the field name the schema rejects', () => {
    // The original bug, pinned by name from both sides.
    expect(confirmationSchema()).not.toContain('decision:');
    expect(read(CLIENT_SRC)).not.toContain('decision');
  });
});
