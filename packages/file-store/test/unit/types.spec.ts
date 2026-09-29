// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { FileNotFoundError, FileSizeMismatchError } from '../../src/types.js';

describe('error types', () => {
  it('FileNotFoundError carries the fileId', () => {
    const e = new FileNotFoundError('file_abc');
    expect(e.fileId).toBe('file_abc');
    expect(e.name).toBe('FileNotFoundError');
    expect(e.message).toContain('file_abc');
  });
  it('FileSizeMismatchError carries expected + actual', () => {
    const e = new FileSizeMismatchError(100, 200);
    expect(e.expected).toBe(100);
    expect(e.actual).toBe(200);
    expect(e.message).toContain('100');
    expect(e.message).toContain('200');
  });
});
