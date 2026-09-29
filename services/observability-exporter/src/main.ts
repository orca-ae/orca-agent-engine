// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { loadConfig } from './config.js';

async function main(): Promise<void> {
  const config = loadConfig();
  if (config.stateBackend === 'postgres') {
    const { runPostgresExporter } = await import('./postgres-main.js');
    await runPostgresExporter(config);
  } else {
    const { runBrokerExporter } = await import('./broker-main.js');
    await runBrokerExporter(config);
  }
}

main().catch((error: unknown) => {
  if (error instanceof Error) console.error(error.message);
  else console.error('observability-exporter failed to start');
  process.exitCode = 1;
});
