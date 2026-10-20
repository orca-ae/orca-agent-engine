// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Which harness the example asks for, and the flags that select it.
//
// Shared by both modes so they cannot drift into demonstrating different agents.

/** The harnesses this example knows how to ask for. */
export type HarnessChoice = 'claude-code' | 'claude-sdk' | 'mock';

export interface Args {
  readonly harness: HarnessChoice;
  readonly toolPolicy: 'always_ask' | 'always_allow';
  readonly prompt?: string;
  readonly environmentId?: string;
  readonly once: boolean;
}

const USAGE = `laptop-agent — run an agent on this machine, through the Orca Agent Engine.

  --harness <claude-code|claude-sdk|mock>   default: claude-code
  --prompt <text>                            send this as the first turn
  --once                                     run one turn and exit (implies --prompt)
  --environment <env_...>                    target an environment explicitly
  --allow-tools                              skip the approval prompt (always_allow)
`;

export function parseArgs(argv: readonly string[]): Args {
  let harness: HarnessChoice = 'claude-code';
  let prompt: string | undefined;
  let environmentId: string | undefined;
  let once = false;
  let toolPolicy: 'always_ask' | 'always_allow' = 'always_ask';

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    // `pnpm run <script> -- --flag` forwards the `--` itself, so a bare one is not an
    // error — it is what every documented invocation of this example produces.
    if (arg === '--') continue;
    switch (arg) {
      case '--harness': {
        const value = argv[++i];
        if (value !== 'claude-code' && value !== 'claude-sdk' && value !== 'mock') {
          throw new Error(`--harness must be claude-code, claude-sdk or mock (got ${value})`);
        }
        harness = value;
        break;
      }
      case '--prompt':
        prompt = argv[++i];
        break;
      case '--environment':
        environmentId = argv[++i];
        break;
      case '--once':
        once = true;
        break;
      case '--allow-tools':
        toolPolicy = 'always_allow';
        break;
      case '--help':
      case '-h':
        process.stdout.write(USAGE);
        process.exit(0);
      // eslint-disable-next-line no-fallthrough
      default:
        throw new Error(`unknown argument ${arg}\n\n${USAGE}`);
    }
  }

  if (once && prompt === undefined) throw new Error('--once needs --prompt');
  return {
    harness,
    once,
    toolPolicy,
    ...(prompt === undefined ? {} : { prompt }),
    ...(environmentId === undefined ? {} : { environmentId }),
  };
}

interface AgentSpec {
  readonly model: string;
  readonly system?: string;
  readonly harness: string;
  readonly mode: string;
  readonly tools?: unknown[];
}

/**
 * The agent body for each choice.
 *
 * `mode: 'colocated'` is what puts the loop AND its tools on the machine the worker
 * is running on. `separate` would run the loop in harness-server instead and is not
 * what this example is about.
 *
 * The `bash` toolset is granted with `always_ask` so the approval gate is visible:
 * the agent asks, the terminal prompts, and `user.tool_confirmation` answers. Set it
 * to `always_allow` and the demo is quieter but shows less.
 */
export function agentSpecFor(
  choice: HarnessChoice,
  policy: 'always_ask' | 'always_allow' = 'always_ask',
): AgentSpec {
  const toolset = [
    {
      type: 'agent_toolset_20260401',
      default_config: { permission_policy: { type: policy } },
    },
  ];

  switch (choice) {
    case 'claude-code':
      return {
        // `claude_code` maps to the runner's native-CLI provider: the real `claude`
        // binary, headless, driven over stream-json. It needs `claude` on the
        // runner's PATH and a credential named in ORCA_RUNNER_ENV_PASSTHROUGH.
        model: 'claude-sonnet-4-5',
        system: 'You are running on the user’s own laptop. Be concise.',
        harness: 'claude_code',
        mode: 'colocated',
        tools: toolset,
      };
    case 'claude-sdk':
      return {
        // The in-process Agent SDK loop. Same engine underneath Claude Code, but the
        // SDK drives the turn rather than the CLI. `claude_agent_sdk` is `separate`.
        model: 'claude-sonnet-4-5',
        system: 'You are running on the user’s own laptop. Be concise.',
        harness: 'claude_agent_sdk',
        mode: 'separate',
        tools: toolset,
      };
    case 'mock':
      return {
        // Deterministic and LLM-free: it echoes `mock: <your text>`. No credential,
        // no network. The right first run — it proves the plumbing without asking
        // whether a model key works.
        model: 'mock',
        harness: 'mock',
        mode: 'colocated',
      };
  }
}

export function describeAgent(choice: HarnessChoice): string {
  switch (choice) {
    case 'claude-code':
      return 'the real `claude` binary, on this machine';
    case 'claude-sdk':
      return 'the Claude Agent SDK loop';
    case 'mock':
      return 'the LLM-free mock harness';
  }
}
