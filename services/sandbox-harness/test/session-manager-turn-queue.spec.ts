// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { access, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { createManagedSession } from '../src/session-manager.js';

const sessions: Array<ReturnType<typeof createManagedSession>> = [];
const tempDirs: string[] = [];

afterEach(async () => {
  for (const session of sessions.splice(0)) session.kill();
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error('timed out waiting for child fixture');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function exists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

describe('managed session turn queue', () => {
  it('holds turns for matching turn_complete markers only', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'sandbox-harness-turn-'));
    tempDirs.push(dir);
    const fixture = join(dir, 'child.mjs');
    const inputs = join(dir, 'inputs');
    const firstReady = join(dir, 'first-ready');
    const secondReady = join(dir, 'second-ready');
    const releaseFirst = join(dir, 'release-first');
    const staleFirst = join(dir, 'stale-first');
    const wrongSecond = join(dir, 'wrong-second');
    const missingSecond = join(dir, 'missing-second');
    const releaseSecond = join(dir, 'release-second');
    const staleDone = join(dir, 'stale-done');
    const wrongDone = join(dir, 'wrong-done');
    const missingDone = join(dir, 'missing-done');
    await writeFile(
      fixture,
      `import { appendFile, access, writeFile } from 'node:fs/promises';
import { createInterface } from 'node:readline';
let firstId, secondId, turns = 0;
const output = (frame) => process.stdout.write(JSON.stringify(frame) + '\\n');
const once = (trigger, done, action) => {
  const timer = setInterval(async () => {
    try { await access(trigger); } catch { return; }
    clearInterval(timer); await action(); await writeFile(done, 'done');
  }, 2);
};
createInterface({ input: process.stdin }).on('line', async (line) => {
  const message = JSON.parse(line);
  if (message.type !== 'user') return;
  turns += 1;
  await appendFile(process.env.INPUTS, message.message.content + '\\n');
  output({ type: 'result', session_id: 'sess_child', is_error: false, usage: {}, total_cost_usd: 0 });
  if (turns === 1) {
    firstId = message.turn_id; await writeFile(process.env.FIRST_READY, 'ready');
    once(process.env.RELEASE_FIRST, process.env.FIRST_DONE, async () => {
      output({ type: 'system', subtype: 'turn_complete', session_id: 'sess_child', turn_id: firstId });
    });
  } else {
    secondId = message.turn_id; await writeFile(process.env.SECOND_READY, 'ready');
    once(process.env.STALE_FIRST, process.env.STALE_DONE, async () => {
      output({ type: 'system', subtype: 'turn_complete', session_id: 'sess_child', turn_id: firstId });
    });
    once(process.env.WRONG_SECOND, process.env.WRONG_DONE, async () => {
      output({ type: 'system', subtype: 'turn_complete', session_id: 'sess_child', turn_id: 'wrong' });
    });
    once(process.env.MISSING_SECOND, process.env.MISSING_DONE, async () => {
      output({ type: 'system', subtype: 'turn_complete', session_id: 'sess_child' });
    });
    once(process.env.RELEASE_SECOND, process.env.SECOND_DONE, async () => {
      output({ type: 'system', subtype: 'turn_complete', session_id: 'sess_child', turn_id: secondId });
    });
  }
});
`,
    );

    const session = createManagedSession({
      sessionId: 'session_parent',
      subprocessEntryPath: fixture,
      spawnArgs: { agent: 'fixture', args: [], replayEnv: {} },
      env: {
        ...process.env,
        INPUTS: inputs,
        FIRST_READY: firstReady,
        SECOND_READY: secondReady,
        RELEASE_FIRST: releaseFirst,
        FIRST_DONE: join(dir, 'first-done'),
        STALE_FIRST: staleFirst,
        STALE_DONE: staleDone,
        WRONG_SECOND: wrongSecond,
        WRONG_DONE: wrongDone,
        MISSING_SECOND: missingSecond,
        MISSING_DONE: missingDone,
        RELEASE_SECOND: releaseSecond,
        SECOND_DONE: join(dir, 'second-done'),
      },
      emit: () => {},
    });
    sessions.push(session);
    session.start();

    const first = session.sendUserMessage('first');
    const second = session.sendUserMessage('second');
    let secondSettled = false;
    void second.then(() => {
      secondSettled = true;
    });

    await waitFor(() => exists(firstReady));
    expect(await readFile(inputs, 'utf8')).toBe('first\n');
    await writeFile(releaseFirst, 'release');
    await first;
    await waitFor(() => exists(secondReady));
    expect(await readFile(inputs, 'utf8')).toBe('first\nsecond\n');

    for (const [trigger, done] of [
      [staleFirst, staleDone],
      [wrongSecond, wrongDone],
      [missingSecond, missingDone],
    ]) {
      await writeFile(trigger, 'emit');
      await waitFor(() => exists(done));
      await new Promise((resolve) => setImmediate(resolve));
      expect(secondSettled).toBe(false);
    }

    await writeFile(releaseSecond, 'release');
    await second;
    expect(secondSettled).toBe(true);
  });

  it('force-settles pending turn when child exits without a marker', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'sandbox-harness-exit-'));
    tempDirs.push(dir);
    const fixture = join(dir, 'child.mjs');
    await writeFile(
      fixture,
      `import { createInterface } from 'node:readline';
createInterface({ input: process.stdin }).on('line', (line) => {
  if (JSON.parse(line).type === 'user') process.exit(1);
});
`,
    );
    const events: Array<{ type: string }> = [];
    const session = createManagedSession({
      sessionId: 'session_parent',
      subprocessEntryPath: fixture,
      spawnArgs: { agent: 'fixture', args: [], replayEnv: {} },
      env: process.env,
      emit: (event) => events.push(event),
    });
    sessions.push(session);
    session.start();

    await expect(session.sendUserMessage('first')).resolves.toBeUndefined();
    expect(events).toContainEqual(expect.objectContaining({ type: 'session.status_error' }));
  });
});
