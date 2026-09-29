// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Canonical sandbox path for artifacts a session exposes through the Files API. */
export const OUTPUT_CAPTURE_DIRECTORY = '/mnt/session/outputs';

/**
 * Platform instruction added only when output capture is active for a runner.
 * Keep its wording aligned with `outputCaptureSystemPrompt` in
 * `services/sandbox-harness/src/subprocess-entry.ts`; the two deployables
 * cannot share a runtime import.
 * Scratch files may still live anywhere else in the sandbox; files intended for
 * the user must be placed in this directory so the OutputIndexer can register
 * them as downloadable `agent_output` records.
 */
export const OUTPUT_CAPTURE_INSTRUCTION = [
  'When you create a file that the user should receive or download,',
  `write it under ${OUTPUT_CAPTURE_DIRECTORY}.`,
  'Files written elsewhere are sandbox scratch files and are not returned to the user.',
].join(' ');

export function withOutputCaptureInstruction(system: string | undefined): string {
  return system && system.length > 0
    ? `${system}\n\n${OUTPUT_CAPTURE_INSTRUCTION}`
    : OUTPUT_CAPTURE_INSTRUCTION;
}
