// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import {
  orchestrationArguments,
  inspectionArguments,
  nativeArguments,
  nativeTools,
  orchestration,
} from '../../src/orchestration.js';

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

it('keeps inspection separate from controls and validates only the basic cursor envelope', () => {
  const agent_id = '11111111-1111-4111-8111-111111111111';
  const continuation = JSON.stringify({ generation: 'forged', position: -1, target: 'other' });
  expect(inspectionArguments('list_child_sessions', {})).toEqual({});
  expect(nativeArguments('read_child_session', { agent_id, continuation })).toEqual({
    agent_id,
    continuation,
  });
  expect(nativeArguments('list_child_sessions', { continuation })).toEqual({ continuation });
  expect(nativeTools.has('read_child_session')).toBe(true);
  expect(orchestration.has('read_child_session')).toBe(false);
  expect(() => orchestrationArguments('read_child_session', { agent_id })).toThrow('Unsupported');
  expect(() => nativeArguments('unknown', {})).toThrow('Unsupported');
  expect(() => inspectionArguments('list_child_sessions', { agent_id })).toThrow('expanded');
  for (const value of [null, [], 'child', {}, { agent_id: 'sth_public' }, { agent_id, cwd: '/' }])
    expect(() => inspectionArguments('read_child_session', value)).toThrow();
  for (const cursor of ['', 'not json', 'null', '1', '[]', '{}'.repeat(1025), 1, undefined])
    expect(() =>
      inspectionArguments('read_child_session', { agent_id, continuation: cursor }),
    ).toThrow();
});
