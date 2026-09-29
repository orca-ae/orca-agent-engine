// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  resolveHarnessAnnotation,
  harnessToProvider,
  validateHarnessDeployment,
  resolveExecutionOwner,
  validateHarnessModel,
  validateHarnessUpdate,
  validateSdkCheckpoint,
  piLlmRoute,
  piModelEfforts,
} from '../src/index.js';
const checkpoint = {
  version: 1,
  format: 'pi_sdk',
  sdkVersion: '0.87.0',
  threadId: 'thread-1',
  instructionsSha256: 'a'.repeat(64),
  files: { 'session.json': 'e30=' },
};
describe('managed Pi selection and checkpoint admission', () => {
  it('defaults to separate and keeps its provider identity', () => {
    expect(resolveHarnessAnnotation({ harness: 'pi_sdk' })).toEqual({
      harness: 'pi_sdk',
      mode: 'separate',
    });
    expect(harnessToProvider('pi_sdk')).toBe('pi-sdk');
    expect(validateHarnessUpdate({ harness: 'pi_sdk' }, { harness: 'pi' })).toMatch(/immutable/);
  });
  it.each([
    ['cloud', 'separate', 'harness-server'],
    ['cloud', 'colocated', 'harness-server'],
    ['self_hosted', 'colocated', 'registry'],
  ] as const)('admits %s/%s with owner %s', (target, mode, owner) => {
    expect(validateHarnessDeployment(target, { harness: 'pi_sdk', mode })).toBeNull();
    expect(resolveExecutionOwner(target, { harness: 'pi_sdk', mode })).toBe(owner);
  });
  it('refuses self-hosted separate and models outside the pinned policy', () => {
    expect(
      validateHarnessDeployment('self_hosted', { harness: 'pi_sdk', mode: 'separate' }),
    ).toMatch(/does not support/);
    expect(
      validateHarnessModel({ harness: 'pi_sdk' }, { provider: 'openai', id: 'gpt-5.4' }),
    ).toBeNull();
    expect(
      validateHarnessModel({ harness: 'pi_sdk' }, { provider: 'openai', id: 'unknown' }),
    ).toMatch(/not supported/);
  });
  it.each([
    ['anthropic', 'claude-sonnet-4-6', 'high'],
    ['openai', 'gpt-5.4', 'low'],
    ['deepseek', 'deepseek-flash', 'high'],
  ])('admits native %s models and rejects mismatched controls', (provider, id, effort) => {
    expect(validateHarnessModel({ harness: 'pi_sdk' }, { provider, id, effort })).toBeNull();
    expect(validateHarnessModel({ harness: 'pi_sdk' }, { provider, id, speed: 'fast' })).toMatch(
      /speed/,
    );
    expect(
      validateHarnessModel({ harness: 'pi_sdk' }, { provider, id, effort: 'invalid' }),
    ).toMatch(/effort/);
  });
  it('does not admit OAuth-only providers or arbitrary provider model pairs', () => {
    expect(
      validateHarnessModel({ harness: 'pi_sdk' }, { provider: 'openai-codex', id: 'gpt-5.4' }),
    ).toMatch(/providers/);
    expect(
      validateHarnessModel({ harness: 'pi_sdk' }, { provider: 'deepseek', id: 'gpt-5.4' }),
    ).toMatch(/not supported/);
  });
  it('prevents cross-provider history and path injection', () => {
    expect(() => validateSdkCheckpoint(checkpoint, 'pi_sdk')).not.toThrow();
    expect(() =>
      validateSdkCheckpoint({ ...checkpoint, sdkVersion: '0.87.1' }, 'pi_sdk'),
    ).not.toThrow();
    for (const harness of ['codex_sdk', 'claude_code'])
      expect(() => validateSdkCheckpoint(checkpoint, harness)).toThrow();
    for (const bad of [
      { ...checkpoint, sdkVersion: 'unknown' },
      { ...checkpoint, files: { '../auth.json': 'e30=' } },
      { ...checkpoint, instructionsSha256: '' },
      { ...checkpoint, files: { 'session.json': 'invalid' } },
    ])
      expect(() => validateSdkCheckpoint(bad, 'pi_sdk')).toThrow();
  });
});

it('maps each supported Pi provider to its exact protocol route', () => {
  expect(piLlmRoute('openai', 'gpt-5.4')).toBe('llm-pi-openai-openai-responses');
  expect(piLlmRoute('anthropic', 'claude-sonnet-4-6')).toBe('llm-pi-anthropic-anthropic-messages');
  expect(piLlmRoute('deepseek', 'deepseek-flash')).toBe('llm-pi-deepseek-openai-completions');
  expect(() => piLlmRoute('google', 'unsupported')).toThrow('Unsupported Pi');
  expect(piModelEfforts('unknown', 'model')).toBeUndefined();
});
