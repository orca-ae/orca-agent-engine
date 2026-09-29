// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { decodeSkillBundle, encodeSkillBundle } from '../../src/codec.js';
import {
  SkillBundleIntegrityError,
  SkillBundleValidationError,
} from '../../src/types.js';

function decodeEnvelope(envelope: Record<string, unknown>): ReturnType<typeof decodeSkillBundle> {
  const bytes = Buffer.from(JSON.stringify(envelope));
  const digest = createHash('sha256').update(bytes).digest('hex');
  return decodeSkillBundle(bytes, digest);
}

function canonicalFile(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  const content = Buffer.from('a');
  return {
    path: 'SKILL.md',
    mode: 0o644,
    mimeType: 'text/markdown',
    sha256: createHash('sha256').update(content).digest('hex'),
    contentBase64: content.toString('base64'),
    ...overrides,
  };
}

describe('skill bundle codec integrity boundaries', () => {
  it.each([
    [
      'envelope',
      {
        format: 'orca.skill-bundle.v1',
        files: [canonicalFile()],
        unexpected: true,
      },
      /envelope contains unknown fields/,
    ],
    [
      'file entry',
      {
        format: 'orca.skill-bundle.v1',
        files: [canonicalFile({ unexpected: true })],
      },
      /invalid skill bundle file entry/,
    ],
  ])('rejects unknown fields in the %s', (_scope, envelope, message) => {
    expect(() => decodeEnvelope(envelope)).toThrowError(message);
  });

  it.each([
    ['malformed', 'YQ=', /invalid base64 content/],
    ['non-canonical pad bits', 'YR==', /non-canonical base64 content/],
  ])('rejects %s base64', (_case, contentBase64, message) => {
    const envelope = {
      format: 'orca.skill-bundle.v1',
      files: [canonicalFile({ contentBase64 })],
    };
    expect(() => decodeEnvelope(envelope)).toThrowError(message);
    expect(() => decodeEnvelope(envelope)).toThrowError(SkillBundleIntegrityError);
  });

  it('rejects a path whose total UTF-8 length exceeds 4000 bytes', () => {
    const path = Array.from({ length: 16 }, () => 'a'.repeat(250)).join('/');
    expect(Buffer.byteLength(path, 'utf8')).toBeGreaterThan(4000);
    expect(() => encodeSkillBundle([{ path, content: Buffer.from('x') }])).toThrowError(
      SkillBundleValidationError,
    );
  });
});
