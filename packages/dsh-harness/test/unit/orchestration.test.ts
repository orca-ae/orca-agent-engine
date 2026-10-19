// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { orchestrationArguments } from '../../src/orchestration.js';

it('normalizes only the explicit native operation fields without grant expansion', () => {
  expect(orchestrationArguments('spawn', { prompt: 'task', description: 'child' })).toEqual({
    description: 'child',
    prompt: 'task',
  });
  expect(orchestrationArguments('fork', { description: 'child', prompt: 'task' })).toEqual({
    description: 'child',
    prompt: 'task',
  });
  expect(orchestrationArguments('send_message', { message: 'hello', agent_id: 'target' })).toEqual({
    agent_id: 'target',
    message: 'hello',
  });
  expect(orchestrationArguments('interrupt_agent', { agent_id: 'target' })).toEqual({
    agent_id: 'target',
  });
  expect(() =>
    orchestrationArguments('spawn', { description: 'child', prompt: 'task', cwd: '/other' }),
  ).toThrow('expanded');
  expect(() => orchestrationArguments('send_message', { agent_id: 'target', message: 1 })).toThrow(
    'arguments',
  );
});
