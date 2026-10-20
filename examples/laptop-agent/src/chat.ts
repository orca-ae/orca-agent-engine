// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The terminal chat loop: send a turn, stream it back, answer approval gates.
//
// This file, `client.ts` and `render.ts` are IDENTICAL in both modes. `local.ts` and
// `shared.ts` differ only in where the engine is and who started it -- which is the
// whole claim the example exists to make. If you ever find yourself branching on the
// mode in here, the claim has stopped being true.

import { createInterface } from 'node:readline/promises';
import { stdin, stdout } from 'node:process';
import type { EngineClient, SessionState } from './client.js';
import { bold, cyan, dim, red, renderFrame, toolNameOf, toolUseIdOf, yellow } from './render.js';

export interface ChatOptions {
  readonly client: EngineClient;
  readonly agentId: string;
  readonly environmentId: string;
  /** Sent as the first turn instead of prompting. Used by `--prompt` and by CI. */
  readonly firstPrompt?: string;
  /** With a first prompt and no TTY, run one turn and exit. */
  readonly once?: boolean;
}

/**
 * Wait until the registry has actually dispatched the session to a runner.
 *
 * Worth doing explicitly rather than just sending a turn: an environment with no
 * connected worker leaves the session `pending` indefinitely and no error is ever
 * raised, because "waiting for a worker" is a legitimate state. Failing here, with
 * the environment id in the message, turns a mysterious hang into a sentence.
 */
async function waitForRunner(
  client: EngineClient,
  session: SessionState,
  environmentId: string,
): Promise<SessionState> {
  if (session.distributionState === 'assigned') return session;

  const deadline = Date.now() + 60_000;
  let latest = session;
  while (Date.now() < deadline) {
    latest = await client.getSession(session.id);
    if (latest.distributionState === 'assigned') return latest;
    if (latest.distributionState === 'failed') {
      throw new Error(`the registry failed to launch a runner for session ${session.id}`);
    }
    await new Promise((r) => setTimeout(r, 500));
  }

  const stats = await client.workStats(environmentId);
  throw new Error(
    stats.workerConnected
      ? `a worker is connected to ${environmentId} but no runner was assigned within 60s`
      : `no worker is connected to ${environmentId}. Start one — this is the machine the ` +
          `agent runs on, and without it the session waits for ever rather than failing.`,
  );
}

/**
 * The cursor to ASK FOR, given the last one already handled.
 *
 * `from_cursor` is INCLUSIVE — the store predicates `seq >= cursor`, so the frame at
 * that cursor is redelivered. Resuming a second turn from the last frame of the first
 * therefore replays that turn's `agent.turn_completed`, and the drain below returns
 * instantly on someone else's terminal frame: turn two sends, and renders nothing at
 * all. Advancing by one is what makes "resume" mean "after", which is what the caller
 * means. A non-numeric cursor is left alone and simply costs one duplicate frame.
 */
function resumeAfter(cursor: string | undefined): string | undefined {
  if (cursor === undefined) return undefined;
  return /^\d+$/.test(cursor) ? (BigInt(cursor) + 1n).toString() : cursor;
}

/**
 * Drain one turn: print frames until the turn ends, answering approval gates.
 *
 * Streams from cursor 0 on the first call so a turn that finishes faster than the
 * subscribe round-trip is not missed, then resumes AFTER the last cursor seen.
 */
async function runTurn(
  client: EngineClient,
  sessionId: string,
  fromCursor: string | undefined,
  ask: (question: string) => Promise<string | null>,
): Promise<string | undefined> {
  let cursor = fromCursor;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10 * 60_000);

  // A model credential the runner cannot use does NOT surface as an error. The
  // provider starts, reports its configuration, and then produces nothing at all --
  // the session sits `running` for ever and this loop waits with it. That is a very
  // expensive thing to debug from silence, so say something after a minute.
  let sawAgentFrame = false;
  const stallHint = setTimeout(() => {
    if (sawAgentFrame) return;
    stdout.write(
      dim(
        '\n  (no output yet. The provider started but has produced nothing, which is what\n' +
          '   an unusable model credential looks like from here — it fails silently rather\n' +
          "   than erroring. Check the key the WORKER holds, not this process's:\n" +
          '   it reaches the runner only if named in ORCA_RUNNER_ENV_PASSTHROUGH.)\n\n',
      ),
    );
  }, 60_000);

  try {
    const from = resumeAfter(cursor);
    for await (const frame of client.streamSession(sessionId, {
      ...(from === undefined ? {} : { fromCursor: from }),
      signal: controller.signal,
    })) {
      if (frame.cursor !== null) cursor = frame.cursor;
      if (frame.type.startsWith('agent.')) sawAgentFrame = true;

      const line = renderFrame(frame);
      if (line !== null) stdout.write(`${line}\n`);

      if (frame.type === 'agent.requires_action') {
        const toolUseId = toolUseIdOf(frame.body);
        if (toolUseId === null) {
          stdout.write(red('  an approval was requested with no tool_use_id; denying\n'));
          continue;
        }
        const name = toolNameOf(frame.body);
        const answer = await ask(yellow(`  allow ${bold(name)}? [y/N] `));
        // No input left to answer with: DENY. Failing closed is the only safe reading
        // of "nobody is there" for a gate whose whole purpose is human authorization.
        const allow = answer !== null && ['y', 'yes'].includes(answer.trim().toLowerCase());
        await client.confirmTool(
          sessionId,
          toolUseId,
          allow ? 'allow' : 'deny',
          allow ? undefined : 'denied by the operator',
        );
        stdout.write(dim(`  ${allow ? 'allowed' : 'denied'} ${name}\n`));
      }

      if (frame.type === 'agent.turn_completed' || frame.type === 'agent.turn_failed') {
        return cursor;
      }
    }

    // The stream ended without a terminal frame, so fall back to the durable transcript.
    //
    // This is defensive, and honestly labelled as such: a turn was once observed whose
    // `agent.message` and `agent.turn_completed` were both in the transcript while the
    // stream rendered nothing. The likely mechanism is a catch-up/tail race on a turn
    // that finishes in milliseconds — but it did NOT reproduce in 20 further attempts
    // with this fallback removed, so treat that as a hypothesis, not a diagnosis.
    //
    // Reading the transcript is worth doing regardless of the cause: the stream is the
    // live view and the transcript is the durable one, and a client that only ever
    // trusts the stream has no way to notice it missed something.
    return await reconcileFromTranscript(client, sessionId, cursor);
  } finally {
    clearTimeout(timer);
    clearTimeout(stallHint);
  }
}

/**
 * Render whatever the live stream missed, reading the persisted transcript.
 *
 * Deliberately one shot and not a retry loop: if the events are not there either, the
 * turn genuinely produced nothing and saying so is more useful than waiting longer.
 */
async function reconcileFromTranscript(
  client: EngineClient,
  sessionId: string,
  cursor: string | undefined,
): Promise<string | undefined> {
  const seen = cursor === undefined ? -1n : BigInt(cursor);
  let latest = cursor;
  for (const frame of await client.listEvents(sessionId)) {
    if (frame.cursor !== null && BigInt(frame.cursor) <= seen) continue;
    if (frame.cursor !== null) latest = frame.cursor;
    const line = renderFrame(frame);
    if (line !== null) stdout.write(`${line}\n`);
  }
  return latest;
}

export async function chat(opts: ChatOptions): Promise<void> {
  const { client, agentId, environmentId } = opts;

  const created = await client.createSession(agentId, environmentId);
  stdout.write(dim(`session ${bold(created.id)}\n`));

  const session = await waitForRunner(client, created, environmentId);
  stdout.write(
    dim(`runner  ${bold(session.runnerId ?? 'unknown')} — your machine picked up the work\n\n`),
  );

  const rl = createInterface({ input: stdin, output: stdout });

  // End-of-input is a normal way to finish, not a crash. Ctrl-D closes readline, and
  // so does the last line of a piped script — after which every further `question()`
  // rejects with "readline was closed". Left unhandled that surfaced as an error and
  // a non-zero exit for what is really just "the operator is done talking".
  let inputClosed = false;
  rl.on('close', () => {
    inputClosed = true;
  });

  /** Ask, or return null once there is no more input to read. */
  const ask = async (q: string): Promise<string | null> => {
    if (inputClosed) return null;
    try {
      return await rl.question(q);
    } catch {
      inputClosed = true;
      return null;
    }
  };

  let cursor: string | undefined;
  let turn = 0;
  try {
    for (;;) {
      let prompt: string;
      if (turn === 0 && opts.firstPrompt !== undefined) {
        prompt = opts.firstPrompt;
        stdout.write(`${cyan('you')} ${prompt}\n`);
      } else {
        if (opts.once === true) break;
        const line = await ask(`${cyan('you')} `);
        if (line === null) break; // EOF — same as /quit
        prompt = line.trim();
        if (prompt.length === 0) continue;
        if (prompt === '/quit' || prompt === '/exit') break;
      }
      turn += 1;

      await client.sendMessage(session.id, prompt, `laptop-agent-${session.id}-${turn}`);
      cursor = await runTurn(client, session.id, cursor, ask);
      stdout.write('\n');

      if (opts.once === true) break;
    }
  } finally {
    rl.close();
  }

  stdout.write(dim(`\nsession ${session.id} left running. Delete it with:\n`));
  stdout.write(
    dim(
      `  curl -X DELETE -H "x-api-key: $ORCA_API_KEY" $ORCA_BASE_URL/v1/sessions/${session.id}\n`,
    ),
  );
}
