// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  Event as TranscriptEvent,
  TranscriptStoreClient,
} from '../src/generated/transcript_store.js';
import { FileStoreClient } from '../src/generated/file_store.js';
import { MemoryStoreClient } from '../src/generated/memory_store.js';

describe('generated stubs (registry-service-ts consumer)', () => {
  it('exports TranscriptStoreClient class', () => {
    expect(typeof TranscriptStoreClient).toBe('function');
  });
  it('round-trips optional transcript user attribution', () => {
    const withUser = TranscriptEvent.decode(
      TranscriptEvent.encode(TranscriptEvent.create({ userId: 'user_proto_1' })).finish(),
    );
    const withoutUser = TranscriptEvent.decode(
      TranscriptEvent.encode(TranscriptEvent.create()).finish(),
    );

    expect(withUser.userId).toBe('user_proto_1');
    expect(withoutUser.userId).toBeUndefined();
  });
  it('exports FileStoreClient class', () => {
    expect(typeof FileStoreClient).toBe('function');
  });
  it('exports MemoryStoreClient class', () => {
    expect(typeof MemoryStoreClient).toBe('function');
  });
});
