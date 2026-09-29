// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createInterface } from 'node:readline';
import { PiSdkWorker } from '@orca/pi-harness';
import type { WorkerCommand } from '@orca/sdk-harness';
const worker = new PiSdkWorker((event) => process.stdout.write(JSON.stringify(event) + '\n'));
const lines = createInterface({ input: process.stdin });
lines.on('line', (line) => {
  void worker.handle(JSON.parse(line) as WorkerCommand).catch(() => {
    process.stdout.write(
      JSON.stringify({ type: 'failure', message: 'Pi SDK worker request failed' }) + '\n',
    );
  });
});
lines.on('close', () => {
  void worker.close().finally(() => process.exit(0));
});
process.on('SIGTERM', () => {
  void worker.close().finally(() => process.exit(0));
});
