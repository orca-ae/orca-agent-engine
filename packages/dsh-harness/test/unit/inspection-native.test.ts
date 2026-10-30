// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { it } from 'vitest';
import { exerciseInspectionTree } from '../support/inspection-tree.js';

it('pages attached descendants and leaves completed and restored children unavailable', async () => {
  await exerciseInspectionTree();
}, 60000);
