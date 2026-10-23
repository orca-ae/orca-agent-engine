// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import {
  NATIVE_USAGE_SOURCE,
  parseNativeCompletion,
  nativeCompletionDigest,
  nativeCompletionIsComplete,
  supportsNativeFileCompletion,
} from '../../src/native-completion.js';
const report = (id: string) => ({
  id,
  provider: 'anthropic',
  model: 'actual-model',
  semantics: 'delta',
  complete: true,
  usage: {
    input_tokens: 13,
    output_tokens: 7,
    cache_read_input_tokens: 2,
    cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 0 },
  },
});
const receipt = () => ({
  version: 1,
  source: NATIVE_USAGE_SOURCE,
  turn_id: 'evt_turn',
  execution_id: 'exec_1',
  placement_id: 'placement_1',
  outcome: 'success',
  report_set_sealed: true,
  expected_reports: ['report_b', 'report_a'],
  reports: [report('report_b'), report('report_a')],
});

it('canonicalizes evidence and includes report payload and placement in the digest', () => {
  const input = receipt();
  expect(nativeCompletionIsComplete(parseNativeCompletion(input))).toBe(true);
  const reversed = structuredClone(input);
  reversed.expected_reports.reverse();
  reversed.reports.reverse();
  expect(nativeCompletionDigest(parseNativeCompletion(input))).toBe(
    nativeCompletionDigest(parseNativeCompletion(reversed)),
  );
  reversed.reports[0]!.usage.output_tokens++;
  expect(nativeCompletionDigest(parseNativeCompletion(input))).not.toBe(
    nativeCompletionDigest(parseNativeCompletion(reversed)),
  );
});

it('rejects missing counters and duplicate identities without coercing them to zero', () => {
  const input = receipt();
  input.reports[0]!.usage.input_tokens = -1;
  expect(() => parseNativeCompletion(input)).toThrow('usage');
  const duplicate = receipt();
  duplicate.reports.push(report('report_a'));
  expect(() => parseNativeCompletion(duplicate)).toThrow('report set');
});

it('distinguishes missing reports and a no-model denial from an invented empty successful turn', () => {
  const missing = receipt();
  missing.reports.pop();
  expect(nativeCompletionIsComplete(parseNativeCompletion(missing))).toBe(false);
  const empty = { ...receipt(), expected_reports: [], reports: [] };
  expect(nativeCompletionIsComplete(parseNativeCompletion(empty))).toBe(false);
  expect(nativeCompletionIsComplete(parseNativeCompletion({ ...empty, outcome: 'failed' }))).toBe(
    true,
  );
});

const snapshot = {
  provider: 'pi',
  model: { provider: 'anthropic' },
  allowed_tool_names: ['read', 'write'],
  allowed_mcp_server_names: [],
};
it.each(['bash', 'sys_terminal_launch', 'delegate_to_agent', 'custom_tool', 'mcp__remote__read'])(
  'refuses the complete effective surface when it includes %s',
  (tool) => {
    expect(
      supportsNativeFileCompletion({
        ...snapshot,
        allowed_tool_names: [...snapshot.allowed_tool_names, tool],
      }),
    ).toBe(false);
    expect(supportsNativeFileCompletion(snapshot)).toBe(true);
  },
);
it('does not admit external servers, skills, custom tools or multiagent', () => {
  expect(
    supportsNativeFileCompletion({ ...snapshot, allowed_mcp_server_names: ['external'] }),
  ).toBe(false);
  expect(supportsNativeFileCompletion({ ...snapshot, skills: [{}] })).toBe(false);
  expect(supportsNativeFileCompletion({ ...snapshot, custom_tools: [{}] })).toBe(false);
  expect(supportsNativeFileCompletion({ ...snapshot, multiagent: {} })).toBe(false);
});
