// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  bindStoredHarness,
  validateHarnessModel,
  validateHarnessUpdate,
  defaultHarnessModelProvider,
  validateCodexCheckpoint,
} from '../src/harness-models.js';
import { harnessToProvider } from '../src/catalog.js';

describe('managed SDK harness selection', () => {
  it('keeps SDK and app-server identities distinct', () => {
    expect(harnessToProvider('codex_sdk')).toBe('codex-sdk');
    expect(harnessToProvider('codex')).toBe('codex');
    expect(defaultHarnessModelProvider('codex_sdk')).toBe('openai');
  });
  it('freezes the selected and implicit legacy harness', () => {
    expect(validateHarnessUpdate({}, { harness: 'codex_sdk' })).toMatch(/immutable/);
    expect(validateHarnessUpdate({ harness: 'codex_sdk' }, {})).toMatch(/immutable/);
    expect(
      validateHarnessUpdate({ harness: 'codex_sdk' }, { harness: 'claude_agent_sdk' }),
    ).toMatch(/immutable/);
    expect(validateHarnessUpdate({ harness: 'claude_agent_sdk' }, {})).toMatch(/immutable/);
    expect(validateHarnessUpdate({}, { tag: 'keep' })).toBeNull();
    expect(
      validateHarnessUpdate({ harness: 'codex_sdk' }, { harness: 'codex_sdk', tag: 'keep' }),
    ).toBeNull();
  });
  it('checks provider, exact model capability, and effort', () => {
    expect(
      validateHarnessModel(
        { harness: 'codex_sdk' },
        { provider: 'openai', id: 'gpt-5.4', effort: 'high' },
      ),
    ).toBeNull();
    expect(
      validateHarnessModel({ harness: 'codex_sdk' }, { provider: 'openai', id: 'gpt-not-a-model' }),
    ).toMatch(/not supported/);
    expect(
      validateHarnessModel(
        { harness: 'codex_sdk' },
        { provider: 'anthropic', id: 'claude-opus-5' },
      ),
    ).toMatch(/providers/);
    expect(
      validateHarnessModel(
        { harness: 'codex_sdk' },
        { provider: 'openai', id: 'gpt-5.4', effort: 'ultra' },
      ),
    ).toMatch(/effort/);
    expect(validateHarnessModel({}, { provider: 'openai', id: 'gpt-5.4' })).toMatch(/providers/);
    expect(validateHarnessModel({}, { provider: 'anthropic', id: 'gpt-5.4' })).toMatch(
      /not supported/,
    );
    expect(
      validateHarnessModel({}, { provider: 'anthropic', id: 'claude-opus-5-5', effort: 'max' }),
    ).toBeNull();
    expect(
      validateHarnessModel(
        { harness: 'pi_sdk' },
        { provider: 'anthropic', id: 'claude-opus-5-5', effort: 'xhigh' },
      ),
    ).toBeNull();
    expect(
      validateHarnessModel({}, { provider: 'anthropic', id: 'claude-opus-4-6-20260205' }),
    ).toBeNull();
  });
  it('preserves operator-defined model contracts for custom harnesses', () => {
    expect(defaultHarnessModelProvider('custom')).toBe('anthropic');
    expect(
      validateHarnessModel(
        { harness: 'custom' },
        { provider: 'private-provider', id: 'private-model', effort: 'custom', speed: 'custom' },
      ),
    ).toBeNull();
  });
  it('accepts standard Codex speed and rejects unsupported speed overrides', () => {
    for (const speed of [undefined, 'standard']) {
      expect(
        validateHarnessModel(
          { harness: 'codex_sdk' },
          { provider: 'openai', id: 'gpt-5.4', ...(speed ? { speed } : {}) },
        ),
      ).toBeNull();
    }
    expect(
      validateHarnessModel(
        { harness: 'codex_sdk' },
        { provider: 'openai', id: 'gpt-5.4', speed: 'fast' },
      ),
    ).toMatch(/model.speed.*not supported/);
  });
  it('rejects invalid annotations before validating models, updates, or stored bindings', () => {
    const invalid = { harness: 'codex_sdk', mode: 'unsupported' };
    expect(validateHarnessModel(invalid, { provider: 'openai', id: 'gpt-5.4' })).toMatch(/mode/);
    expect(validateHarnessUpdate(invalid, { harness: 'codex_sdk' })).toMatch(/mode/);
    expect(validateHarnessUpdate({ harness: 'codex_sdk' }, invalid)).toMatch(/mode/);
    expect(() => bindStoredHarness(invalid, 'codex_sdk', 'codex_sdk')).toThrow(/mode/);
  });
  it('binds legacy and versioned configurations to the immutable identity', () => {
    expect(bindStoredHarness({}, 'claude_agent_sdk')).toEqual({ harness: 'claude_agent_sdk' });
    expect(bindStoredHarness({}, 'codex_sdk', 'codex_sdk')).toEqual({ harness: 'codex_sdk' });
    expect(bindStoredHarness({}, 'codex_sdk')).toEqual({ harness: 'claude_agent_sdk' });
    expect(bindStoredHarness({ harness: 'claude_code' }, 'claude_agent_sdk')).toEqual({
      harness: 'claude_code',
    });
    expect(() =>
      bindStoredHarness({ harness: 'codex_sdk' }, 'claude_agent_sdk', 'codex_sdk'),
    ).toThrow(/conflicts/);
    expect(() => bindStoredHarness({ harness: 'unknown' }, 'claude_agent_sdk')).toThrow();
    expect(() =>
      bindStoredHarness({ harness: 'claude_agent_sdk' }, 'codex_sdk', 'codex_sdk'),
    ).toThrow(/conflicts/);
  });
  it('rejects private checkpoint traversal and accepts a session rollout', () => {
    expect(() =>
      validateCodexCheckpoint({ version: 1, threadId: 'tid', files: { '../auth.json': 'e30=' } }),
    ).toThrow();
    expect(() =>
      validateCodexCheckpoint({
        version: 1,
        threadId: 'tid',
        files: { 'sessions/2026/09/16/rollout-tid.jsonl': 'e30=' },
      }),
    ).not.toThrow();
  });
  it.each([
    null,
    'checkpoint',
    [],
    { version: 2, threadId: 'tid', files: {} },
    { version: 1, threadId: '../tid', files: {} },
    { version: 1, threadId: 'tid', files: [] },
    { version: 1, threadId: 'tid', files: {} },
    { version: 1, threadId: 'tid', files: { 'sessions/2026/09/16/rollout-other.jsonl': 'e30=' } },
    { version: 1, threadId: 'tid', files: { 'sessions/2026/09/16/rollout-tid.jsonl': 42 } },
    {
      version: 1,
      threadId: 'tid',
      files: { 'sessions/2026/09/16/rollout-tid.jsonl': 'not base64' },
    },
  ])('rejects malformed checkpoint %j', (state) => {
    expect(() => validateCodexCheckpoint(state)).toThrow(/invalid Codex checkpoint/);
  });
  it('admits legacy checkpoints and validates optional instruction fingerprints', () => {
    const checkpoint = {
      version: 1,
      threadId: 'tid',
      files: { 'sessions/2026/09/16/rollout-tid.jsonl': 'e30=' },
    };
    expect(() => validateCodexCheckpoint(checkpoint)).not.toThrow();
    expect(() =>
      validateCodexCheckpoint({ ...checkpoint, instructionsSha256: 'a'.repeat(64) }),
    ).not.toThrow();
    for (const instructionsSha256 of [null, 42, '', 'a'.repeat(63), 'A'.repeat(64)]) {
      expect(() => validateCodexCheckpoint({ ...checkpoint, instructionsSha256 })).toThrow(
        'invalid Codex checkpoint',
      );
    }
  });
  it('bounds both checkpoint file count and aggregate size', () => {
    const files = Object.fromEntries(
      Array.from({ length: 32 }, (_, i) => [`sessions/2026/09/16/rollout-tid-${i}.jsonl`, 'e30=']),
    );
    expect(() => validateCodexCheckpoint({ version: 1, threadId: 'tid', files })).not.toThrow();
    files['sessions/2026/09/16/rollout-tid-32.jsonl'] = 'e30=';
    expect(() => validateCodexCheckpoint({ version: 1, threadId: 'tid', files })).toThrow(
      /invalid Codex checkpoint files/,
    );

    const limitFiles = {
      'sessions/2026/09/16/rollout-tid-first.jsonl': Buffer.alloc(8 * 1024 * 1024).toString(
        'base64',
      ),
      'sessions/2026/09/16/rollout-tid-second.jsonl': Buffer.alloc(8 * 1024 * 1024).toString(
        'base64',
      ),
    };
    expect(() =>
      validateCodexCheckpoint({ version: 1, threadId: 'tid', files: limitFiles }),
    ).not.toThrow();
    limitFiles['sessions/2026/09/16/rollout-tid-second.jsonl'] = Buffer.alloc(
      8 * 1024 * 1024 + 1,
    ).toString('base64');
    expect(() =>
      validateCodexCheckpoint({ version: 1, threadId: 'tid', files: limitFiles }),
    ).toThrow(/exceeds size limit/);
  });
});
