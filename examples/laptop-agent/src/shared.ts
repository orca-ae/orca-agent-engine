// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Mode 2 — SHARED ENGINE. The engine runs somewhere else; the agent still runs here.
//
//   ORCA_BASE_URL=https://engine.example ORCA_API_KEY=orca_… \
//     pnpm --filter @orca/example-laptop-agent shared
//
// This creates a `target=self_hosted` Environment on that engine and prints the
// `oeadm worker` command that attaches THIS machine to it. Run that command in a
// second terminal; this process waits until the engine reports the worker connected,
// then runs the same chat loop.
//
// What that buys, and what mode 1 cannot show:
//
//   - your code and your model credentials never leave this machine. The engine
//     coordinates the session; it does not run the agent.
//   - the transcript lives on the engine. Kill this process mid-turn and the turn
//     keeps going; `oeadm attach --session <id>` from anywhere rejoins it.
//   - the worker DIALS OUT. Nothing listens on this laptop and no inbound port is
//     opened, which is why this works from behind NAT.
//
// Everything below the wiring is identical to `local.ts`: same client, same chat loop.

import { stdout } from 'node:process';
import { EngineClient } from './client.js';
import { chat } from './chat.js';
import { bold, dim, yellow } from './render.js';
import { parseArgs, agentSpecFor, describeAgent } from './agent-spec.js';

/** Poll `work_stats` until a worker claims the environment, or give up with advice. */
async function waitForWorker(
  client: EngineClient,
  environmentId: string,
  runnerCommand: string,
): Promise<void> {
  const first = await client.workStats(environmentId);
  if (first.workerConnected) return;

  stdout.write(`\n${yellow('Run this in another terminal, on this machine:')}\n\n`);
  stdout.write(`  ${runnerCommand}\n\n`);
  stdout.write(dim('waiting for the worker to connect… (ctrl-c to give up)\n'));

  const deadline = Date.now() + 10 * 60_000;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 2_000));
    const stats = await client.workStats(environmentId);
    if (stats.workerConnected) {
      stdout.write(dim('worker connected.\n\n'));
      return;
    }
  }
  throw new Error(`no worker connected to ${environmentId} within 10 minutes`);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  const baseUrl = process.env['ORCA_BASE_URL']?.replace(/\/+$/, '');
  const apiKey = process.env['ORCA_API_KEY'];
  if (
    baseUrl === undefined ||
    baseUrl.length === 0 ||
    apiKey === undefined ||
    apiKey.length === 0
  ) {
    throw new Error(
      'set ORCA_BASE_URL and ORCA_API_KEY to point at the engine.\n' +
        '  For a local engine instead, use `pnpm --filter @orca/example-laptop-agent local`.',
    );
  }

  const client = new EngineClient({ baseUrl, apiKey });
  stdout.write(`${bold('engine')} ${baseUrl} ${dim('(shared — someone else runs it)')}\n`);

  let environmentId = args.environmentId;
  let envKey: string | null = null;
  if (environmentId === undefined) {
    const env = await client.createSelfHostedEnvironment(`laptop-${Date.now()}`);
    environmentId = env.id;
    envKey = env.envKey;
    stdout.write(`${bold('env')}    ${environmentId} ${dim('(created; needs a worker)')}\n`);
  } else {
    stdout.write(`${bold('env')}    ${environmentId} ${dim('(existing)')}\n`);
  }

  // The Env Key is echoed exactly once, at create, and is not recoverable — so the
  // command is printed here or not at all. `--env-key` is deliberately NOT used:
  // a credential in argv is visible in `ps` to every user on the host.
  const runnerCommand =
    envKey === null
      ? `ENVIRONMENT_KEY=<the env key> oeadm worker --environment ${environmentId} \\\n` +
        `    --registry ${baseUrl} --workspace-dir /var/tmp/orca-workspaces \\\n` +
        `    --runner-command "node services/session-runner/dist/main.js"`
      : `ENVIRONMENT_KEY=${envKey} oeadm worker --environment ${environmentId} \\\n` +
        `    --registry ${baseUrl} --workspace-dir /var/tmp/orca-workspaces \\\n` +
        `    --runner-command "node services/session-runner/dist/main.js"`;

  await waitForWorker(client, environmentId, runnerCommand);

  const spec = agentSpecFor(args.harness, args.toolPolicy);
  const agentId = await client.createAgent({
    name: `laptop-agent-shared-${Date.now()}`,
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
