// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  OUTPUT_CAPTURE_DIRECTORY,
  OUTPUT_CAPTURE_INSTRUCTION,
  withOutputCaptureInstruction,
} from '../../src/sandbox/outputs/output-instructions.js';

describe('output capture instructions', () => {
  it('uses the directory that OutputIndexer scans', () => {
    expect(OUTPUT_CAPTURE_INSTRUCTION).toContain(OUTPUT_CAPTURE_DIRECTORY);
  });

  it('preserves the agent system prompt before the platform instruction', () => {
    expect(withOutputCaptureInstruction('Be concise.')).toBe(
      `Be concise.\n\n${OUTPUT_CAPTURE_INSTRUCTION}`,
    );
  });

  it('provides an instruction when the agent has no system prompt', () => {
    expect(withOutputCaptureInstruction(undefined)).toBe(OUTPUT_CAPTURE_INSTRUCTION);
  });
});
