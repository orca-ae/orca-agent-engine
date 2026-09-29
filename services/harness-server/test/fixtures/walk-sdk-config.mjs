// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readdir } from 'node:fs/promises';
import { join } from 'node:path';

export async function walkSdkConfig(root) {
  const files = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      try {
        files.push(...(await walkSdkConfig(path)));
      } catch (error) {
        // SDK lock directories can disappear between listing and descent.
        // Missing roots and all other filesystem failures still fail the probe.
        if (error.code !== 'ENOENT') throw error;
      }
    } else if (entry.isFile()) files.push(path);
  }
  return files;
}
