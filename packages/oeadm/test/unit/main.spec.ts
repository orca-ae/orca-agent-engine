// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { buildDispatchers, dispatch, run, type CliIo, type Dispatchers } from '../../src/main.js';
import { noColor } from '../../src/colors.js';

function recordingDispatchers(): { calls: string[]; dispatchers: Dispatchers } {
  const calls: string[] = [];
  const dispatchers: Dispatchers = {
    async run(args) {
      calls.push(`run ${args.join(' ')}`);
    },
    async attach(args) {
      calls.push(`attach ${args.join(' ')}`);
    },
    async env(args) {
      calls.push(`env ${args.join(' ')}`);
    },
    async worker(args) {
      calls.push(`worker ${args.join(' ')}`);
    },
  };
  return { calls, dispatchers };
}

describe('dispatch', () => {
  it('routes `run` to the run handler with the remaining args', async () => {
    const { calls, dispatchers } = recordingDispatchers();
    const code = await dispatch(['run', '--agent', 'a1'], dispatchers, () => {});
    expect(code).toBe(0);
    expect(calls).toEqual(['run --agent a1']);
  });

  it('routes `attach` to the attach handler', async () => {
    const { calls, dispatchers } = recordingDispatchers();
    await dispatch(['attach', '--session', 's1'], dispatchers, () => {});
    expect(calls).toEqual(['attach --session s1']);
  });

  it('routes `env` (with its subcommand left intact) to the env handler', async () => {
    const { calls, dispatchers } = recordingDispatchers();
    await dispatch(['env', 'create', '--name', 'x'], dispatchers, () => {});
    expect(calls).toEqual(['env create --name x']);
  });

  it('routes `worker` to the worker handler', async () => {
    const { calls, dispatchers } = recordingDispatchers();
    await dispatch(['worker', '--environment', 'e1'], dispatchers, () => {});
    expect(calls).toEqual(['worker --environment e1']);
  });

  it('prints usage and exits non-zero for an unknown command', async () => {
    const { dispatchers } = recordingDispatchers();
    const lines: string[] = [];
    const code = await dispatch(['frobnicate'], dispatchers, (t) => lines.push(t));
    expect(code).toBe(1);
    expect(lines.join('\n').toLowerCase()).toContain('usage');
  });

  it('prints usage and exits zero when no command is given', async () => {
    const { dispatchers } = recordingDispatchers();
    const lines: string[] = [];
    const code = await dispatch([], dispatchers, (t) => lines.push(t));
    expect(code).toBe(0);
    expect(lines.join('\n').toLowerCase()).toContain('usage');
  });

  it('prints usage for help flags', async () => {
    const { dispatchers } = recordingDispatchers();
    const lines: string[] = [];
    const code = await dispatch(['--help'], dispatchers, (t) => lines.push(t));
    expect(code).toBe(0);
    expect(lines.join('\n').toLowerCase()).toContain('usage');
  });

  it('names ENVIRONMENT_KEY, not --env-key, as the way to supply the Env Key', async () => {
    const { dispatchers } = recordingDispatchers();
    const lines: string[] = [];
    await dispatch(['--help'], dispatchers, (t) => lines.push(t));
    const usage = lines.join('\n');

    // A key passed as an argument is visible in `ps` and in shell history, so the
    // usage must not advertise that as the path.
    expect(usage).toContain('ENVIRONMENT_KEY');
    expect(usage.split('\n').find((l) => l.startsWith('  oeadm worker'))).not.toContain(
      '--env-key',
    );
  });
});

/** A recording {@link CliIo} plus the streams it wrote to. */
function recordingIo(): { io: CliIo; out: string[]; err: string[]; exitCodes: number[] } {
  const out: string[] = [];
  const err: string[] = [];
  const exitCodes: number[] = [];
  return {
    out,
    err,
    exitCodes,
    io: {
      stdout: (t) => out.push(t),
      stderr: (t) => err.push(t),
      colors: noColor,
      setExitCode: (code) => exitCodes.push(code),
    },
  };
}

/**
 * The CLI's failure contract, which nothing pinned.
 *
 * `run`'s `return 1` could be changed to `return 0` and the whole suite stayed
 * green: every subcommand that threw would have reported SUCCESS to the shell.
 * The unknown-command path above was covered, which is what made the gap look
 * closed — a wrong command exited 1 while a failed one exited 0.
 */
describe('run — error mapping', () => {
  it('maps a thrown subcommand error to exit 1 and prints it on stderr', async () => {
    const { io, out, err } = recordingIo();
    const dispatchers: Dispatchers = {
      run: async () => {
        throw new Error('agent not found');
      },
      attach: async () => {},
      env: async () => {},
      worker: async () => {},
    };

    const code = await run(['run', '--agent', 'missing'], dispatchers, io);

    expect(code).toBe(1);
    expect(err.join('\n')).toContain('error: agent not found');
    // The failure goes to stderr, never stdout — a script piping stdout must not
    // see an error as though it were output.
    expect(out.join('\n')).not.toContain('agent not found');
  });

  it('maps a non-Error throw to exit 1 too', async () => {
    const { io, err } = recordingIo();
    const dispatchers: Dispatchers = {
      run: async () => {
        throw 'plain string blew up';
      },
      attach: async () => {},
      env: async () => {},
      worker: async () => {},
    };

    expect(await run(['run'], dispatchers, io)).toBe(1);
    expect(err.join('\n')).toContain('plain string blew up');
  });

  it.each(['attach', 'env', 'worker'] as const)(
    'maps a thrown %s error to exit 1 as well',
    async (command) => {
      const { io, err } = recordingIo();
      const dispatchers: Dispatchers = {
        run: async () => {},
        attach: async () => {},
        env: async () => {},
        worker: async () => {},
        ...{
          [command]: async () => {
            throw new Error(`${command} failed`);
          },
        },
      };

      expect(await run([command], dispatchers, io)).toBe(1);
      expect(err.join('\n')).toContain(`error: ${command} failed`);
    },
  );

  it('returns the dispatcher code untouched when nothing throws', async () => {
    const { io, err } = recordingIo();
    const dispatchers: Dispatchers = {
      run: async () => {},
      attach: async () => {},
      env: async () => {},
      worker: async () => {},
    };

    expect(await run(['run'], dispatchers, io)).toBe(0);
    expect(err).toEqual([]);
  });
});

/**
 * `oeadm worker` is the one subcommand that reports failure with a CODE rather
 * than a throw: it proxies a long-running child whose exit status is the
 * operator's result. Deleting the propagation left the suite green.
 */
describe('buildDispatchers — worker exit-code propagation', () => {
  it('propagates the worker child’s non-zero exit code', async () => {
    const { io, exitCodes } = recordingIo();
    const dispatchers = buildDispatchers(io, async () => 3);

    await dispatchers.worker(['--environment', 'env_1']);

    expect(exitCodes).toEqual([3]);
  });

  it('leaves the exit code alone when the worker exits cleanly', async () => {
    const { io, exitCodes } = recordingIo();
    const dispatchers = buildDispatchers(io, async () => 0);

    await dispatchers.worker([]);

    expect(exitCodes).toEqual([]);
  });

  it('passes the subcommand args through to the worker launcher', async () => {
    const { io } = recordingIo();
    const seen: string[][] = [];
    const dispatchers = buildDispatchers(io, async (args) => {
      seen.push(args);
      return 0;
    });

    await dispatchers.worker(['--environment', 'env_9', '--registry', 'wss://r']);

    expect(seen).toEqual([['--environment', 'env_9', '--registry', 'wss://r']]);
  });

  // End to end through `run`: a worker that fails reaches the shell as a failure
  // even though the dispatcher itself resolves.
  it('surfaces a failing worker through run() as a recorded exit code', async () => {
    const { io, exitCodes } = recordingIo();
    const dispatchers = buildDispatchers(io, async () => 42);

    await run(['worker', '--environment', 'env_1'], dispatchers, io);

    expect(exitCodes).toEqual([42]);
  });
});
