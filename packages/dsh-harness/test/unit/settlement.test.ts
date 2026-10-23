// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { settlementFailures } from '../../src/settlement.js';

const complete = { started: 2, completed: 2 };

it('accepts known usage for parent-handled child interruption or failure', () => {
  for (const reason of ['aborted', 'max-tokens', 'completed']) {
    expect(
      settlementFailures(
        false,
        'root',
        new Map([
          ['root', complete],
          ['child', complete],
        ]),
        new Map([
          ['root', 'completed'],
          ['child', reason],
        ]),
      ),
    ).toEqual([]);
  }
  // A host-targeted child follow-up need not make a root provider request.
  expect(
    settlementFailures(
      false,
      'root',
      new Map([['child', complete]]),
      new Map([['child', 'completed']]),
    ),
  ).toEqual([]);
});

it('reports every existing guard branch with bounded static labels', () => {
  expect(settlementFailures(true, 'root', new Map(), new Map())).toEqual([
    'root_interrupted',
    'no_usage',
  ]);
  expect(
    settlementFailures(
      false,
      'root',
      new Map([['root', complete]]),
      new Map([['root', 'aborted']]),
    ),
  ).toEqual(['root_terminal_unsuccessful']);
  expect(settlementFailures(false, 'root', new Map([['root', complete]]), new Map())).toEqual([
    'root_terminal_unsuccessful',
    'missing_terminal',
  ]);
  expect(settlementFailures(false, 'root', new Map([['child', complete]]), new Map())).toEqual([
    'missing_terminal',
  ]);
  for (const completed of [0, 3]) {
    expect(
      settlementFailures(
        false,
        'root',
        new Map([['private-child-id', { started: 2, completed }]]),
        new Map([['private-child-id', 'aborted']]),
      ),
    ).toEqual(['provider_requests_unmatched']);
  }
});
