// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe } from 'vitest';
import { InMemoryMemoryMetadataStore } from '../../src/metadata/in-memory.js';
import { deletionContract } from '../support/deletion-contract.js';

describe('in-memory deletion checkpoint contract', () => {
  let metadata: InMemoryMemoryMetadataStore;
  beforeEach(() => {
    metadata = new InMemoryMemoryMetadataStore();
  });
  deletionContract(() => metadata);
});
