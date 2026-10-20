// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Mode 1 — LOCAL ENGINE. Everything on this laptop; you own every process.
//
//   make self-hosted-up
//   pnpm --filter @orca/example-laptop-agent local
//
// `make self-hosted-up` started Postgres + RustFS, the registry, and an
// `oeadm worker` attached to a freshly created `target=self_hosted` Environment. It
// wrote that environment's id and its one-time Env Key to
// `services/dev/run/self-hosted.env`, and this reads the id from there.
//
// Reading the file rather than creating an environment of our own is not a shortcut,
// it is the requirement: a session must target the environment a worker has CLAIMED.
// Create a second environment and nothing is attached to it, so the session waits for
// a runner that will never come -- with no error, because that is a legal state.
//
// The only difference from `shared.ts` is those few lines. Same client, same chat
// loop, same API.

import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { stdout } from 'node:process';
import { EngineClient } from './client.js';
import { chat } from './chat.js';
import { bold, dim } from './render.js';
import { parseArgs, agentSpecFor, describeAgent } from './agent-spec.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const SELF_HOSTED_ENV_FILE = resolve(HERE, '../../../services/dev/run/self-hosted.env');

/** Read one `KEY=value` out of the file `start-self-hosted.sh` wrote. */
function readSelfHostedValue(key: string): string | null {
  let text: string;
  try {
    text = readFileSync(SELF_HOSTED_ENV_FILE, 'utf8');
  } catch {
    return null;
  }
  for (const line of text.split('\n')) {
    const eq = line.indexOf('=');
    if (eq > 0 && line.slice(0, eq).trim() === key) return line.slice(eq + 1).trim();
  }
  return null;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  const environmentId =
    args.environmentId ?? readSelfHostedValue('ORCA_SELF_HOSTED_ENVIRONMENT_ID');
  if (environmentId === null) {
    throw new Error(
      `no environment id.\n` +
        `  Expected ORCA_SELF_HOSTED_ENVIRONMENT_ID in ${SELF_HOSTED_ENV_FILE},\n` +
        `  which \`make self-hosted-up\` writes. Run that first, or pass --environment <id>.`,
    );
  }

  const apiKey = process.env['ORCA_API_KEY'] ?? readSelfHostedValue('ORCA_SELF_HOSTED_API_KEY');
  if (apiKey === null || apiKey.length === 0) {
    throw new Error(
      'no api key. Set ORCA_API_KEY, or re-run `make self-hosted-up` with a build that\n' +
        '  records ORCA_SELF_HOSTED_API_KEY in its env file.',
    );
  }

  const baseUrl = (process.env['ORCA_BASE_URL'] ?? 'http://localhost:8080').replace(/\/+$/, '');
  const client = new EngineClient({ baseUrl, apiKey });

  const stats = await client.workStats(environmentId);
  if (!stats.workerConnected) {
    throw new Error(
      `environment ${environmentId} has no connected worker.\n` +
        '  That worker is what makes this laptop the place the agent runs.\n' +
        '  `make self-hosted-down && make self-hosted-up` will re-attach one.',
    );
  }

  stdout.write(`${bold('engine')} ${baseUrl} ${dim('(local — you started it)')}\n`);
  stdout.write(`${bold('env')}    ${environmentId} ${dim('(this laptop, worker connected)')}\n`);

  const spec = agentSpecFor(args.harness, args.toolPolicy);
  const agentId = await client.createAgent({
    name: `laptop-agent-local-${Date.now()}`,
    ...spec,
  });
  stdout.write(`${bold('agent')}  ${agentId} ${dim(`(${describeAgent(args.harness)})`)}\n\n`);

  await chat({
    client,
    agentId,
    environmentId,
    ...(args.prompt === undefined ? {} : { firstPrompt: args.prompt }),
    once: args.once,
  });
}

main().catch((err: unknown) => {
  process.stderr.write(`\nerror: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exitCode = 1;
});
