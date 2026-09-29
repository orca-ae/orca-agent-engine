#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// Child-process entrypoint.
//
// session-manager (the parent, in index.ts) spawns this file by path:
//
//   node dist/subprocess-entry.js --input-format stream-json \
//        --output-format stream-json --agent claude [--model ...]
//
// It is intentionally a SEPARATE tsup entry from index.ts so the parent can
// spawn it by a stable filename rather than re-entering its own bundle. The
// flow is deliberately tiny and fail-fast:
//
//   1. parse the claude-CLI-style launch flags (parseLaunchArgs);
//   2. decode the optional replay preamble from the environment;
//   3. resolve the provider for options.agent;
//   4. build a Session, wiring the decoded replay in as a preamble;
//   5. start a StreamJsonServer over stdin/stdout and hand off.
//
// Any failure in steps 1-3 is fatal: we write the reason to stderr and exit
// with code 2. The manager treats a non-zero child exit as a crash and emits
// `session.status_error` upstream with the tail of this stderr, so the message
// written here is the operator-visible cause.
// ---------------------------------------------------------------------------
import { parseLaunchArgs, StreamJsonServer } from './protocol.js';
import { decodeCanonicalBase64 } from './base64.js';
import { resolveProvider } from './providers/index.js';
import {
  AGENTS_ENV_VAR,
  ALLOWED_TOOLS_ENV_VAR,
  CUSTOM_TOOLS_ENV_VAR,
  FORWARD_SUBAGENT_TEXT_ENV_VAR,
  MODEL_EFFORT_ENV_VAR,
  MODEL_SPEED_ENV_VAR,
  REPLAY_ENV_VAR,
  RUNTIME_TOOLS_ENV_VAR,
  SYSTEM_PROMPT_ENV_VAR,
  TOOLS_ENV_VAR,
} from './session-manager.js';
import { Session, type ReplayEntry } from './session.js';
import {
  MODEL_EFFORTS,
  MODEL_SPEEDS,
  type CustomToolDefinition,
  type ModelEffort,
  type ModelSpeed,
  type RuntimeAgentDefinitions,
} from './providers/index.js';

/** Exit code the manager interprets as "child crashed" (vs. clean shutdown). */
const FATAL_EXIT_CODE = 2;

// The resume-preamble env key is OWNED BY session-manager.ts (the parent/writer);
// we import REPLAY_ENV_VAR rather than re-declare the string so the writer and
// this reader can never drift. base64 is used by the parent because the payload
// is free-form conversation text (newlines, quotes, unicode) that must survive
// transport as a single environment-variable string. When unset, the child
// starts with no prior context.

/** Env var overriding the default agent id when `--agent` is omitted. */
const DEFAULT_AGENT_ENV = 'SANDBOX_HARNESS_DEFAULT_AGENT';

/** Env var overriding the default SDK permission mode for managed sessions. */
const DEFAULT_PERMISSION_MODE_ENV = 'SANDBOX_HARNESS_DEFAULT_PERMISSION_MODE';

/** Env var overriding the default working directory for managed sessions. */
const DEFAULT_CWD_ENV = 'SANDBOX_HARNESS_DEFAULT_CWD';

/** Built-in default agent when neither the flag nor {@link DEFAULT_AGENT_ENV} is set. */
const FALLBACK_AGENT = 'claude';

/** Set by harness-server when the session's local output directory is indexed. */
const OUTPUT_CAPTURE_DIRECTORY_ENV = 'ORCA_OUTPUT_CAPTURE_DIRECTORY';

function outputCaptureSystemPrompt(env: NodeJS.ProcessEnv): string | undefined {
  const directory = env[OUTPUT_CAPTURE_DIRECTORY_ENV]?.trim();
  if (!directory) return undefined;
  // Keep this wording aligned with OUTPUT_CAPTURE_INSTRUCTION in
  // services/harness-server/src/sandbox/outputs/output-instructions.ts.
  return [
    'When you create a file that the user should receive or download,',
    `write it under ${directory}.`,
    'Files written elsewhere are sandbox scratch files and are not returned to the user.',
  ].join(' ');
}

function composeSystemPrompt(...parts: Array<string | undefined>): string | undefined {
  const present = parts.filter((part): part is string => part !== undefined && part.length > 0);
  return present.length > 0 ? present.join('\n\n') : undefined;
}

/**
 * Fail-fast helper: report `reason` on stderr and exit so the manager surfaces
 * it as `session.status_error`. Typed `never` so callers can use it as a
 * terminal branch without convincing the type-checker a value still flows.
 */
function die(reason: string): never {
  process.stderr.write(`${reason}\n`);
  process.exit(FATAL_EXIT_CODE);
}

/**
 * Decode the resume preamble from {@link REPLAY_ENV_VAR}. Returns `undefined`
 * when the var is absent/empty (the common fresh-start case). A
 * present-but-malformed value is a hard error — silently dropping it would let a
 * "resumed" session start with no context, which is worse than crashing loudly.
 */
function decodeReplay(env: NodeJS.ProcessEnv): ReplayEntry[] | undefined {
  const raw = env[REPLAY_ENV_VAR];
  if (raw === undefined || raw.trim() === '') return undefined;

  let parsed: unknown;
  try {
    parsed = JSON.parse(decodeCanonicalBase64(raw).toString('utf8'));
  } catch (err) {
    throw new Error(
      `invalid ${REPLAY_ENV_VAR}: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (!Array.isArray(parsed)) {
    throw new Error(`invalid ${REPLAY_ENV_VAR}: expected a JSON array of replay messages`);
  }
  return parsed as ReplayEntry[];
}

function decodeAgents(env: NodeJS.ProcessEnv): RuntimeAgentDefinitions | undefined {
  const raw = env[AGENTS_ENV_VAR];
  if (raw === undefined || raw.trim() === '') return undefined;

  let parsed: unknown;
  try {
    parsed = JSON.parse(decodeCanonicalBase64(raw).toString('utf8'));
  } catch (err) {
    throw new Error(
      `invalid ${AGENTS_ENV_VAR}: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`invalid ${AGENTS_ENV_VAR}: expected a JSON object of agent definitions`);
  }
  return parsed as RuntimeAgentDefinitions;
}

function decodeForwardSubagentText(env: NodeJS.ProcessEnv): boolean | undefined {
  const raw = env[FORWARD_SUBAGENT_TEXT_ENV_VAR];
  if (raw === undefined || raw.trim() === '') return undefined;
  return raw === '1' || raw.toLowerCase() === 'true';
}

function decodeCustomTools(env: NodeJS.ProcessEnv): CustomToolDefinition[] | undefined {
  const raw = env[CUSTOM_TOOLS_ENV_VAR];
  if (raw === undefined || raw.trim() === '') return undefined;

  let parsed: unknown;
  try {
    parsed = JSON.parse(decodeCanonicalBase64(raw).toString('utf8'));
  } catch (err) {
    throw new Error(
      `invalid ${CUSTOM_TOOLS_ENV_VAR}: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (!Array.isArray(parsed)) {
    throw new Error(`invalid ${CUSTOM_TOOLS_ENV_VAR}: expected a JSON array of custom tools`);
  }
  return parsed as CustomToolDefinition[];
}

function decodeBase64String(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const raw = env[key];
  if (raw === undefined) return undefined;
  try {
    return decodeCanonicalBase64(raw).toString('utf8');
  } catch (err) {
    throw new Error(`invalid ${key}: ${err instanceof Error ? err.message : String(err)}`);
  }
}

function decodeStringArray(env: NodeJS.ProcessEnv, key: string): string[] | undefined {
  const raw = env[key];
  if (raw === undefined || raw.trim() === '') return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(decodeCanonicalBase64(raw).toString('utf8'));
  } catch (err) {
    throw new Error(`invalid ${key}: ${err instanceof Error ? err.message : String(err)}`);
  }
  if (!Array.isArray(parsed) || !parsed.every((item) => typeof item === 'string')) {
    throw new Error(`invalid ${key}: expected a JSON string array`);
  }
  return parsed;
}

function decodeModelSpeed(env: NodeJS.ProcessEnv): ModelSpeed | undefined {
  const raw = env[MODEL_SPEED_ENV_VAR];
  if (raw === undefined || raw.trim() === '') return undefined;
  if (!MODEL_SPEEDS.includes(raw as ModelSpeed)) {
    throw new Error(`invalid ${MODEL_SPEED_ENV_VAR}: ${raw}`);
  }
  return raw as ModelSpeed;
}

function decodeModelEffort(env: NodeJS.ProcessEnv): ModelEffort | undefined {
  const raw = env[MODEL_EFFORT_ENV_VAR];
  if (raw === undefined || raw.trim() === '') return undefined;
  if (!MODEL_EFFORTS.includes(raw as ModelEffort)) {
    throw new Error(`invalid ${MODEL_EFFORT_ENV_VAR}: ${raw}`);
  }
  return raw as ModelEffort;
}

async function main(): Promise<void> {
  const env = process.env;

  // Step 1 + 2: launch args and replay decode both happen here, before any
  // provider/session is built, so a bad config crashes before we hold resources.
  let options;
  let replay: ReplayEntry[] | undefined;
  let agents: RuntimeAgentDefinitions | undefined;
  let forwardSubagentText: boolean | undefined;
  let customTools: CustomToolDefinition[] | undefined;
  let systemPrompt: string | undefined;
  let tools: string[] | undefined;
  let allowedTools: string[] | undefined;
  let runtimeTools: string[] | undefined;
  let modelSpeed: ModelSpeed | undefined;
  let modelEffort: ModelEffort | undefined;
  try {
    options = parseLaunchArgs(process.argv.slice(2), {
      agent: env[DEFAULT_AGENT_ENV] || FALLBACK_AGENT,
      permissionMode: env[DEFAULT_PERMISSION_MODE_ENV] || 'default',
      cwd: env[DEFAULT_CWD_ENV] || process.cwd(),
    });
    replay = decodeReplay(env);
    agents = decodeAgents(env);
    forwardSubagentText = decodeForwardSubagentText(env);
    customTools = decodeCustomTools(env);
    systemPrompt = composeSystemPrompt(
      decodeBase64String(env, SYSTEM_PROMPT_ENV_VAR),
      outputCaptureSystemPrompt(env),
    );
    tools = decodeStringArray(env, TOOLS_ENV_VAR);
    allowedTools = decodeStringArray(env, ALLOWED_TOOLS_ENV_VAR);
    runtimeTools = decodeStringArray(env, RUNTIME_TOOLS_ENV_VAR);
    modelSpeed = decodeModelSpeed(env);
    modelEffort = decodeModelEffort(env);
  } catch (err) {
    die(err instanceof Error ? err.message : String(err));
  }

  // Step 3: resolve the provider for the (possibly defaulted) agent id.
  let provider;
  try {
    provider = await resolveProvider(options.agent);
  } catch (err) {
    die(err instanceof Error ? err.message : String(err));
  }

  // Step 4 + 5: construct the session with the decoded replay preamble, then
  // hand stdin/stdout to the wire. start() is non-blocking; the process stays
  // alive on the stdin readline stream until the parent closes it.
  const session = new Session({
    provider,
    // parseLaunchArgs yields `null` when `--model` is absent; Session treats an
    // omitted model as "use the provider default", so map the `null` sentinel to
    // an omitted property rather than passing `null` (rejected under
    // exactOptionalPropertyTypes).
    ...(options.model !== null ? { model: options.model } : {}),
    ...(modelSpeed !== undefined ? { modelSpeed } : {}),
    ...(modelEffort !== undefined ? { modelEffort } : {}),
    permissionMode: options.permissionMode,
    cwd: options.cwd,
    ...(systemPrompt !== undefined ? { systemPrompt } : {}),
    ...(tools !== undefined ? { tools } : {}),
    ...(allowedTools !== undefined ? { allowedTools } : {}),
    ...(runtimeTools !== undefined ? { runtimeTools } : {}),
    env,
    stderr: process.stderr,
    ...(replay !== undefined ? { replay } : {}),
    ...(agents !== undefined ? { agents } : {}),
    ...(forwardSubagentText !== undefined ? { forwardSubagentText } : {}),
    ...(customTools !== undefined ? { customTools } : {}),
  });

  new StreamJsonServer({ session }).start();
}

main().catch((err: unknown) => {
  die(err instanceof Error ? err.message : String(err));
});
