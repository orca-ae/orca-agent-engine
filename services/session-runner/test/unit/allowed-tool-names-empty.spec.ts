// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The ONE home for what `allowed_tool_names` means at each boundary it crosses.
//
// Three readings of the same field are one boolean apart and read OPPOSITELY:
//
//   `['bash']` → exactly those tools
//   `[]`       → DENY-ALL. An ordinary composition result, not a degenerate one: an
//                mcp-only agent (`tools: [{ type: 'mcp_toolset', … }]`) canonicalizes
//                to the `mcp_toolset` token, which is not a directly-runnable tool
//                name and expands to nothing. `buildOrcaSdkTools` is the baseline
//                (`allowedLogicalNames ? new Set(…) : null` — `[]` is truthy → an
//                allowlist permitting nothing).
//   absent     → NO RESTRICTION — the full built-in tool surface.
//
// The five native-CLI providers used to length-guard the value before pushing
// `--allowed-tools`, so `[]` omitted the flag, the bridge child read "absent" as "no
// restriction", and the SAME agent got the full orca surface on five providers and
// zero tools on the sixth. The flag is now pushed with an EMPTY value for `[]`, and
// `parseAllowedToolsFlag` keeps the two apart.
//
// That leaves one question the argv layer cannot answer: what does a WIRE snapshot
// that omits the field mean? Nothing can know — deny-all and allow-all are both
// defensible and both silent — so `parseSnapshotBody` refuses to guess and throws.
// The unrestricted state stays reachable where a caller states it deliberately, by
// leaving the optional field off a `SessionStartInput` it constructs itself.
//
// This suite pins all of it: the wire parser, the snapshot→boot-input projection, the
// argv writer, the argv reader, and the tool set each ends up selecting.

import { describe, it, expect } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import {
  BRIDGE_ALLOWED_TOOLS_FLAG,
  parseAllowedToolsFlag,
  pushAllowedToolsFlag,
} from '../../src/harness/claude-code/bridge-runtime.js';
import { buildOrcaSdkTools } from '../../src/harness/claude/mcp-tools.js';
import { ClaudeCodeCliHarness } from '../../src/harness/claude-code/index.js';
import { CursorCliHarness } from '../../src/harness/cursor/index.js';
import { CodexCliHarness } from '../../src/harness/codex/index.js';
import { CustomCliHarness } from '../../src/harness/custom/index.js';
import { PiCliHarness } from '../../src/harness/pi/index.js';
import { buildSessionStartInput } from '../../src/harness/provider.js';
import { parseSnapshotBody, SnapshotParseError } from '../../src/snapshot.js';
import type { SandboxHandle } from '../../src/sandbox/seam.js';
import type { SessionStartInput } from '../../src/harness/agent-harness.js';

/** The private argv builder every native-CLI provider uses for its bridge child. */
type BridgeCommandBuilder = {
  buildBridgeCommand(sandbox: SandboxHandle, input: SessionStartInput): { args: string[] };
};

const HARNESS_OPTS = {
  modelDefault: 'model-x',
  workspaceId: 'ws_1',
  sessionId: 'ses_1',
  bridgePrefixArgs: ['/bridge/entry.js'] as string[],
} as const;

/** Every native-CLI provider, each built with the same minimal options. */
function nativeCliHarnesses(): Array<{ name: string; harness: BridgeCommandBuilder }> {
  return [
    { name: 'claude-code', harness: new ClaudeCodeCliHarness({ ...HARNESS_OPTS }) },
    { name: 'cursor', harness: new CursorCliHarness({ ...HARNESS_OPTS }) },
    { name: 'codex', harness: new CodexCliHarness({ ...HARNESS_OPTS }) },
    { name: 'custom', harness: new CustomCliHarness({ ...HARNESS_OPTS }) },
    { name: 'pi', harness: new PiCliHarness({ ...HARNESS_OPTS }) },
  ] as unknown as Array<{ name: string; harness: BridgeCommandBuilder }>;
}

/** Encode a snapshot the way the owner pod delivers it: one JSON line + newline. */
function deliver(snapshot: unknown): Uint8Array {
  return new TextEncoder().encode(`${JSON.stringify(snapshot)}\n`);
}

/**
 * The PRODUCTION path, end to end: a wire snapshot → `parseSnapshotBody` →
 * `buildSessionStartInput` → the boot input a provider's `start` consumes.
 *
 * Every allowlist case a producer can actually put on the wire is driven through this
 * rather than hand-constructed, so a defect anywhere in the chain — a lossy coercion in
 * the parser, a dropped field in the projection, a length-guard in the argv writer — is
 * caught here instead of being papered over by a fixture that skips the broken layer.
 */
function startInputFromWire(snapshotFields: Record<string, unknown>): SessionStartInput {
  const snapshot = parseSnapshotBody(
    deliver({
      model: { provider: 'anthropic', id: 'model-x' },
      provider: 'claude-code',
      system: 'You are Orca.',
      allowed_mcp_server_names: [],
      egress: { mode: 'gateway' },
      ...snapshotFields,
    }),
  );
  return buildSessionStartInput(snapshot, { workspaceId: 'ws_1', sessionId: 'ses_1' });
}

/** The value following `--allowed-tools`, or `undefined` when the flag is absent. */
function flagValue(args: string[]): string | undefined {
  const i = args.indexOf(BRIDGE_ALLOWED_TOOLS_FLAG);
  return i >= 0 ? args[i + 1] : undefined;
}

describe('allowed_tool_names on the wire: absent is a parse error, not a default', () => {
  it('rejects a snapshot that OMITS allowed_tool_names', () => {
    // Defaulting to `[]` would de-tool the agent on every provider; defaulting to
    // "unrestricted" would hand it the full built-in surface. Both are silent, and
    // only the producer knows which it meant — so neither is chosen. `JSON.stringify`
    // drops an `undefined` value, so this body genuinely omits the key.
    expect(() => startInputFromWire({ allowed_tool_names: undefined })).toThrow(SnapshotParseError);
    expect(() => startInputFromWire({ allowed_tool_names: undefined })).toThrow(
      /missing or non-array allowed_tool_names/,
    );
  });

  it('rejects a null allowed_tool_names', () => {
    expect(() => startInputFromWire({ allowed_tool_names: null })).toThrow(
      /missing or non-array allowed_tool_names/,
    );
  });

  it('rejects a non-array allowed_tool_names (string / number / object / boolean)', () => {
    for (const bad of ['bash', 7, { bash: true }, true]) {
      expect(() => startInputFromWire({ allowed_tool_names: bad }), JSON.stringify(bad)).toThrow(
        /missing or non-array allowed_tool_names/,
      );
    }
  });

  it('accepts an array, dropping non-string entries', () => {
    const input = startInputFromWire({ allowed_tool_names: ['bash', 7, null, 'read'] });
    expect(input.agentSnapshot.allowed_tool_names).toEqual(['bash', 'read']);
  });

  it('rejects a ROSTER MEMBER snapshot that omits it, naming the member', () => {
    // A coordinator's members are parsed by the same rules; a member that de-tooled
    // itself silently would be even harder to see than a top-level one.
    expect(
      () =>
        startInputFromWire({
          allowed_tool_names: [],
          multiagent: {
            type: 'coordinator',
            primary_thread_id: 'sth_1',
            agents: [{ agent_name: 'researcher', snapshot: { provider: 'claude' } }],
          },
        }),
      // `.*` bridges the nested `snapshot parse failed:` prefix the re-wrap re-adds.
    ).toThrow(/agents\[0\] snapshot: .*missing or non-array allowed_tool_names/);
  });
});

describe('allowed_tool_names: [] is deny-all, not "no restriction"', () => {
  it('the claude provider denies every tool for [] and allows every tool for undefined', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = (await runtime.acquire({})) as unknown as SandboxHandle;
    // The baseline semantics the five native-CLI providers must match.
    expect(buildOrcaSdkTools(sandbox as never, []).length).toBe(0);
    expect(buildOrcaSdkTools(sandbox as never, undefined).length).toBeGreaterThan(0);
  });

  it('every native-CLI provider emits --allowed-tools with an EMPTY value for a wire []', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = (await runtime.acquire({})) as unknown as SandboxHandle;
    const input = startInputFromWire({ allowed_tool_names: [] });
    // The projection must carry the empty array through — dropping it here would look
    // exactly like "absent" to every provider downstream.
    expect(input.agentSnapshot.allowed_tool_names).toEqual([]);
    for (const { name, harness } of nativeCliHarnesses()) {
      const { args } = harness.buildBridgeCommand(sandbox, input);
      expect(args, `${name} must pass the allowlist flag`).toContain(BRIDGE_ALLOWED_TOOLS_FLAG);
      expect(flagValue(args), `${name} must pass an empty allowlist value`).toBe('');
      // …and what the bridge child then parses is an allowlist permitting nothing.
      expect(parseAllowedToolsFlag(args), `${name} round-trip`).toEqual([]);
      expect(buildOrcaSdkTools(sandbox as never, parseAllowedToolsFlag(args)).length).toBe(0);
    }
  });

  it('every native-CLI provider carries a non-empty wire allowlist verbatim', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = (await runtime.acquire({})) as unknown as SandboxHandle;
    const input = startInputFromWire({ allowed_tool_names: ['bash', 'read'] });
    for (const { name, harness } of nativeCliHarnesses()) {
      const { args } = harness.buildBridgeCommand(sandbox, input);
      expect(flagValue(args), name).toBe('bash,read');
      expect(parseAllowedToolsFlag(args), name).toEqual(['bash', 'read']);
      expect(buildOrcaSdkTools(sandbox as never, parseAllowedToolsFlag(args)).length).toBe(2);
    }
  });

  it('every native-CLI provider OMITS the flag for a boot input that states no allowlist', async () => {
    // The reachable unrestricted case: NOT a wire omission (that throws above) but a
    // caller constructing the boot input itself and deliberately leaving the OPTIONAL
    // `agentSnapshot.allowed_tool_names` off — the shape `SessionStartInput` declares,
    // which every provider must read as "no restriction".
    const runtime = new InMemorySandboxRuntime();
    const sandbox = (await runtime.acquire({})) as unknown as SandboxHandle;
    const input: SessionStartInput = {
      workspaceId: 'ws_1',
      sessionId: 'ses_1',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'model-x',
        system: 'You are Orca.',
      },
    } as unknown as SessionStartInput;
    for (const { name, harness } of nativeCliHarnesses()) {
      const { args } = harness.buildBridgeCommand(sandbox, input);
      expect(args, `${name} must not restrict an absent allowlist`).not.toContain(
        BRIDGE_ALLOWED_TOOLS_FLAG,
      );
      expect(parseAllowedToolsFlag(args), `${name} round-trip`).toBeUndefined();
      expect(
        buildOrcaSdkTools(sandbox as never, parseAllowedToolsFlag(args)).length,
      ).toBeGreaterThan(0);
    }
  });
});

describe('pushAllowedToolsFlag / parseAllowedToolsFlag', () => {
  it('round-trips absent, empty, and populated allowlists distinctly', () => {
    const absent: string[] = [];
    pushAllowedToolsFlag(absent, undefined);
    expect(absent).toEqual([]);
    expect(parseAllowedToolsFlag(absent)).toBeUndefined();

    const empty: string[] = [];
    pushAllowedToolsFlag(empty, []);
    expect(empty).toEqual([BRIDGE_ALLOWED_TOOLS_FLAG, '']);
    expect(parseAllowedToolsFlag(empty)).toEqual([]);

    const populated: string[] = [];
    pushAllowedToolsFlag(populated, ['bash']);
    expect(populated).toEqual([BRIDGE_ALLOWED_TOOLS_FLAG, 'bash']);
    expect(parseAllowedToolsFlag(populated)).toEqual(['bash']);
  });
});
