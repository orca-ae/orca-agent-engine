// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Cross-component wire pin for `src/protocol.ts`.
//
// Every constant in `src/protocol.ts` is a runner-side COPY of a literal the
// registry owns: the registry PUSHES requests over the runner tunnel and the runner
// serves them at the matching path, with the matching header names. The two
// declarations sit in different services — and can change in different PRs — so
// nothing ever puts them in front of one reviewer at the same time. Nine literals
// kept in agreement by hand is not a contract; a silent one-sided edit breaks turn
// delivery, snapshot delivery, skills delivery, recovery replay, tool confirmation,
// and interrupt, all at runtime.
//
// So the runner's copies are asserted against the registry SOURCE, not against a
// second hand-written literal in this file. See `support/registry-source-pin.ts` for
// why the file is read as text rather than imported, and why an absent registry
// source skips instead of failing.
//
// NOT pinned here, because there is nothing to pin them to:
//   - `RUNNER_TERMINAL_ATTACH_PATH`, `RUNNER_TUNNEL_TOKEN_HEADER` and the runner
//     env-var names are single-sourced in `@orca/harness-tunnel`, which BOTH sides
//     import. A shared symbol cannot drift, which is the outcome this spec is a
//     substitute for.

import { describe, it, expect } from 'vitest';
import {
  NDJSON_CONTENT_TYPE,
  RUNNER_CONFIRMATION_PATH,
  RUNNER_CUSTOM_TOOL_RESULT_PATH,
  RUNNER_INTERRUPT_PATH,
  RUNNER_REPLAY_PATH,
  RUNNER_RESUME_CURSOR_HEADER,
  RUNNER_SESSION_HEADER,
  RUNNER_SKILLS_PATH,
  RUNNER_SNAPSHOT_PATH,
  RUNNER_TURN_PATH,
  RUNNER_RESOURCES_PATH,
  RUNNER_RESOURCE_CHANGES_PATH,
  RUNNER_RESOURCE_ACK_PATH,
} from '../../src/protocol.js';
import { registryConst, registrySourcesPresent } from './support/registry-source-pin.js';

/** The registry files that own the other half of each literal, under `registry-service-ts/src`. */
const EVENT_BRIDGE = 'tunnel/session-event-bridge.ts';
const SNAPSHOT_DELIVERY = 'tunnel/session-snapshot-delivery.ts';
const RECOVERY = 'tunnel/session-recovery.ts';
const SKILLS_DELIVERY = 'tunnel/session-skills-delivery.ts';

const RESOURCES_DELIVERY = 'tunnel/session-resources-delivery.ts';

const PINNED_SOURCES = [
  EVENT_BRIDGE,
  SNAPSHOT_DELIVERY,
  RECOVERY,
  SKILLS_DELIVERY,
  RESOURCES_DELIVERY,
] as const;

/**
 * Every duplicated wire literal: the runner's value, and where the registry declares
 * the same one. A new constant in `src/protocol.ts` belongs on this table.
 */
const PINS: ReadonlyArray<{
  readonly what: string;
  readonly runner: string;
  readonly file: string;
  readonly name: string;
}> = [
  ...[
    ['RUNNER_RESOURCES_PATH', RUNNER_RESOURCES_PATH],
    ['RUNNER_RESOURCE_CHANGES_PATH', RUNNER_RESOURCE_CHANGES_PATH],
    ['RUNNER_RESOURCE_ACK_PATH', RUNNER_RESOURCE_ACK_PATH],
  ].map(([name, runner]) => ({
    what: name!,
    name: name!,
    runner: runner!,
    file: RESOURCES_DELIVERY,
  })),
  {
    what: 'custom tool result route',
    runner: RUNNER_CUSTOM_TOOL_RESULT_PATH,
    file: EVENT_BRIDGE,
    name: 'RUNNER_CUSTOM_TOOL_RESULT_PATH',
  },
  { what: 'turn route', runner: RUNNER_TURN_PATH, file: EVENT_BRIDGE, name: 'RUNNER_TURN_PATH' },
  {
    what: 'confirmation route',
    runner: RUNNER_CONFIRMATION_PATH,
    file: EVENT_BRIDGE,
    name: 'RUNNER_CONFIRMATION_PATH',
  },
  {
    what: 'interrupt route',
    runner: RUNNER_INTERRUPT_PATH,
    file: EVENT_BRIDGE,
    name: 'RUNNER_INTERRUPT_PATH',
  },
  {
    what: 'session header',
    runner: RUNNER_SESSION_HEADER,
    file: EVENT_BRIDGE,
    name: 'RUNNER_SESSION_HEADER',
  },
  {
    what: 'snapshot route',
    runner: RUNNER_SNAPSHOT_PATH,
    file: SNAPSHOT_DELIVERY,
    name: 'RUNNER_SNAPSHOT_PATH',
  },
  { what: 'replay route', runner: RUNNER_REPLAY_PATH, file: RECOVERY, name: 'RUNNER_REPLAY_PATH' },
  {
    what: 'resume-cursor header',
    runner: RUNNER_RESUME_CURSOR_HEADER,
    file: RECOVERY,
    name: 'RUNNER_RESUME_CURSOR_HEADER',
  },
  {
    what: 'skills route',
    runner: RUNNER_SKILLS_PATH,
    file: SKILLS_DELIVERY,
    name: 'RUNNER_SKILLS_PATH',
  },
  // Module-private on the registry side (`session-skills-delivery.ts` declares it
  // without `export` and `session-snapshot-delivery.ts` inlines the same string on
  // its push). Still the registry's own declaration of the content type both sides
  // must agree on, so it is pinned like the rest — see `registryConst` on why the
  // `export` keyword is optional.
  {
    what: 'NDJSON content type',
    runner: NDJSON_CONTENT_TYPE,
    file: SKILLS_DELIVERY,
    name: 'NDJSON_CONTENT_TYPE',
  },
];

const REGISTRY_SRC_PRESENT = registrySourcesPresent(PINNED_SOURCES);

describe.skipIf(!REGISTRY_SRC_PRESENT)(
  'runner-tunnel wire constants mirror the registry EXACTLY (pinned to the registry SOURCE)',
  () => {
    it.each(PINS)('$what: $name matches the registry literal', ({ runner, file, name }) => {
      expect(runner).toBe(registryConst(file, name));
    });
  },
);

describe('runner-tunnel wire constants (values the runner serves)', () => {
  it('pins every constant `src/protocol.ts` exports', async () => {
    // The PINS table is hand-maintained, and a hand-maintained inclusion list cannot
    // fail loudly — the failure is a line nobody wrote. A new wire constant added to
    // `protocol.ts` without a row there would be an unpinned copy that LOOKS pinned,
    // which is the exact condition this spec exists to remove. Runs unconditionally,
    // including on a branch where the registry source is still absent: the table's
    // completeness is a property of this package alone.
    const protocolModule = await import('../../src/protocol.js');
    const exported = Object.keys(protocolModule).sort();
    const pinned = PINS.map(({ name }) => name).sort();
    expect(exported, 'add the new constant to PINS in this file').toEqual(pinned);
  });

  // These always run. They are NOT a substitute for the source pin above — a
  // hand-copied expectation passes when the registry drifts, which is why the pin
  // exists — but they do keep the runner's own served surface from changing
  // silently on a branch where the registry source is not yet present.
  it('serves the six push routes the owner pod addresses', () => {
    expect(RUNNER_TURN_PATH).toBe('/v1/runner/turn');
    expect(RUNNER_SNAPSHOT_PATH).toBe('/v1/runner/snapshot');
    expect(RUNNER_SKILLS_PATH).toBe('/v1/runner/skills');
    expect(RUNNER_REPLAY_PATH).toBe('/v1/runner/replay');
    expect(RUNNER_CONFIRMATION_PATH).toBe('/v1/runner/confirmation');
    expect(RUNNER_INTERRUPT_PATH).toBe('/v1/runner/interrupt');
  });

  it('reads the two push headers in their canonical case', () => {
    expect(RUNNER_SESSION_HEADER).toBe('X-Orca-Session-Id');
    expect(RUNNER_RESUME_CURSOR_HEADER).toBe('X-Orca-Resume-Cursor');
  });

  it('frames every NDJSON body with the shared content type', () => {
    expect(NDJSON_CONTENT_TYPE).toBe('application/x-ndjson');
  });
});
