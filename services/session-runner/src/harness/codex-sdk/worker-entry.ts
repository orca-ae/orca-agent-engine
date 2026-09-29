// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createInterface } from 'node:readline';
import { CodexSdkWorker } from '@orca/codex-harness';
import type { WorkerCommand } from '@orca/codex-harness';
const worker = new CodexSdkWorker((event) => process.stdout.write(JSON.stringify(event) + '\n'));
const lines = createInterface({ input: process.stdin });
lines.on('line', (line) => {
  void worker.handle(JSON.parse(line) as WorkerCommand).catch(() => {
    process.stdout.write(
      JSON.stringify({ type: 'failure', message: 'Codex SDK worker request failed' }) + '\n',
    );
  });
});
lines.on('close', () => {
  void worker.close().finally(() => process.exit(0));
});
process.on('SIGTERM', () => {
  void worker.close().finally(() => process.exit(0));
});
