// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { build } from 'tsup';
import { expect, it } from 'vitest';

it('loads the bundled index and native worker in a plain Node process without a test loader', async () => {
  const root = fileURLToPath(new URL('../../', import.meta.url));
  // Remain beneath the package so normal production dependency resolution is
  // exercised, rather than injecting a native driver path into the smoke test.
  const outDir = await mkdtemp(join(root, '.disk-index-build-'));
  try {
    await build({
      entry: [join(root, 'src/kafka-disk-index.ts')],
      outDir,
      format: ['esm'],
      target: 'node22',
      dts: false,
      silent: true,
    });
    const moduleUrl = pathToFileURL(join(outDir, 'kafka-disk-index.js')).href;
    const { stdout } = await promisify(execFile)(process.execPath, [
      '--input-type=module',
      '--eval',
      `import { KafkaDiskIndex } from ${JSON.stringify(moduleUrl)};
       const failures = [];
       const index = await KafkaDiskIndex.open({ onFailure: (error) => failures.push(error.message) });
       let result;
       let count;
       try {
         const prefix = "I/" + "a".repeat(64) + "/";
         const key = prefix + "identity";
         await index.apply([{ key, value: "native-built", offset: "1" }]);
         result = await index.read([key], { key, value: "native-built" });
         count = await index.countPrefix(prefix);
       } finally { await index.close(); }
       const normalCloseFailures = [...failures];
       const idle = await KafkaDiskIndex.open({ onFailure: (error) => {
         failures.push(error.message);
         throw new Error("observer cannot derail cleanup");
       } });
       await idle.worker.terminate();
       await idle.close();
       console.log(JSON.stringify({ result, count, normalCloseFailures, failures }));`,
    ]);
    expect(JSON.parse(stdout)).toEqual({
      result: ['native-built'],
      count: 1,
      normalCloseFailures: [],
      failures: ['Kafka disk index worker unavailable'],
    });
  } finally {
    await rm(outDir, { recursive: true, force: true });
  }
}, 30_000);
