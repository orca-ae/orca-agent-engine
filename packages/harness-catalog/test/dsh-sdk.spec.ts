// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import {
  DSH_MODEL,
  DSH_LLM_ROUTE,
  resolveHarnessAnnotation,
  harnessToProvider,
  validateHarnessDeployment,
  resolveExecutionOwner,
  validateHarnessModel,
  validateHarnessFeatures,
  validateHarnessGuardrails,
  validateSdkCheckpoint,
} from '../src/index.js';

const selection = { harness: 'dsh_sdk', mode: 'colocated' } as const;
const checkpoint = {
  version: 1,
  format: 'dsh_sdk',
  sdkVersion: '0.2.1-alpha.2',
  threadId: '11111111-1111-1111-1111-111111111111',
  instructionsSha256: 'a'.repeat(64),
  files: {
    'orca.json': 'e30=',
    '_no-cwd/11111111-1111-1111-1111-111111111111/session.v4.jsonl': 'e30=',
  },
};

it('admits only cloud/colocated DSH with the fixed Messages model', () => {
  expect(resolveHarnessAnnotation({ harness: 'dsh_sdk' })).toEqual(selection);
  expect(resolveHarnessAnnotation({ harness: 'dsh_sdk', mode: 'separate' })).toHaveProperty(
    'error',
  );
  expect(harnessToProvider('dsh_sdk')).toBe('dsh-sdk');
  expect(validateHarnessDeployment('cloud', selection)).toBeNull();
  expect(validateHarnessDeployment('self_hosted', selection)).toMatch(/does not support/);
  expect(resolveExecutionOwner('cloud', selection)).toBe('harness-server');
  expect(DSH_LLM_ROUTE).toBe('llm-pi-deepseek-anthropic-messages');
  expect(validateHarnessModel(selection, { provider: 'deepseek', id: DSH_MODEL })).toBeNull();
  expect(validateHarnessModel(selection, { provider: 'deepseek', id: 'deepseek-flash' })).toMatch(
    /not supported/,
  );
  expect(validateHarnessFeatures(selection, { multiagent: {} })).toMatch(/multiagent/);
  expect(validateHarnessFeatures(selection, { skills: ['skill'] })).toMatch(/Skills/);
  expect(
    validateHarnessGuardrails(selection, [{ id: 'rule', phases: ['tool_call'], stateful: false }]),
  ).toBeNull();
});

it('admits only implemented DSH tool phases and explicit cumulative spawn bounds', () => {
  expect(
    validateHarnessGuardrails(selection, [
      { id: 'native-output', phases: ['tool_call', 'tool_result'], stateful: false },
    ]),
  ).toMatch(/phase tool_result is not supported/);
  expect(
    validateHarnessGuardrails(selection, [
      {
        id: 'bounds',
        phases: ['tool_call'],
        stateful: true,
        rule: { kind: 'builtin', builtin: 'spawn_bounds' },
      },
    ]),
  ).toBeNull();
  for (const builtin of ['headless_subagent_purpose_guard', 'max_tool_calls_per_session'])
    expect(
      validateHarnessGuardrails(selection, [
        {
          id: builtin,
          phases: ['tool_call'],
          stateful: builtin !== 'headless_subagent_purpose_guard',
          rule: { kind: 'builtin', builtin },
        },
      ]),
    ).toMatch(/adapter/);
  expect(
    validateHarnessGuardrails(selection, [
      { id: 'response', phases: ['response'], stateful: false },
    ]),
  ).toMatch(/not supported/);
});

it('binds checkpoints to DSH and rejects malformed envelope, traversal and noncanonical base64', () => {
  expect(() => validateSdkCheckpoint(checkpoint, 'dsh_sdk')).not.toThrow();
  for (const harness of ['codex_sdk', 'pi_sdk', 'unknown'])
    expect(() => validateSdkCheckpoint(checkpoint, harness)).toThrow();
  for (const bad of [
    { ...checkpoint, version: 2 },
    { ...checkpoint, threadId: '../escape' },
    { ...checkpoint, sdkVersion: '0.2.1' },
    { ...checkpoint, instructionsSha256: '' },
    { ...checkpoint, format: 'unknown' },
    ...['../escape', '/absolute', 'a/../b', 'a\\b', 'a//b', 'a/./b'].map((path) => ({
      ...checkpoint,
      files: { [path]: 'e30=' },
    })),
    ...['Zh==', 'e31=', 'e30', '===='].map((data) => ({
      ...checkpoint,
      files: { 'session.json': data },
    })),
  ])
    expect(() => validateSdkCheckpoint(bad)).toThrow();
});

it('bounds aggregate decoded checkpoint bytes', () => {
  const data = Buffer.alloc(8 * 1024 * 1024 + 1).toString('base64');
  expect(() =>
    validateSdkCheckpoint({
      ...checkpoint,
      files: {
        'orca.json': data,
        '_no-cwd/11111111-1111-1111-1111-111111111111/session.v4.jsonl': data,
      },
    }),
  ).toThrow(/size limit/);
});

it('requires explicit effective denial of DSH host-side web_fetch without changing other SDKs', () => {
  const tools = [{ type: 'agent_toolset' }];
  expect(validateHarnessFeatures(selection, { tools })).toMatch(/web_fetch.*always_deny/);
  expect(validateHarnessFeatures({ harness: 'pi_sdk' }, { tools })).toBeNull();
  expect(validateHarnessFeatures(selection, { tools: [] })).toBeNull();
  expect(
    validateHarnessFeatures(selection, {
      tools: [
        {
          type: 'agent_toolset',
          configs: [{ name: 'web_fetch', enabled: false }],
        },
      ],
    }),
  ).toBeNull();
  expect(
    validateHarnessFeatures(selection, {
      tools: [
        {
          type: 'agent_toolset',
          default_config: { enabled: false },
          configs: [{ name: 'bash', enabled: true, permission_policy: { type: 'always_allow' } }],
        },
      ],
    }),
  ).toBeNull();
});
