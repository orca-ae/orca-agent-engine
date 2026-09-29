// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the generic custom-provider LAUNCH-ARG builder.
//
// The `custom` provider boots ANY operator-declared CLI via the native-CLI launcher. This
// builder turns a parsed {@link CustomAgentSpec} + a session's boot context into the
// {@link NativeCliLaunchConfig} the launcher runs: it substitutes the argv/env placeholders
// (`{sessionId}` / `{workspaceId}` / `{model}` / `{systemPrompt}` / `{sandboxRoot}` /
// `{bridgeCommand}` / `{bridgeArgsJson}`) and layers the spec env over the credential-free
// gateway egress. This suite pins that substitution contract:
//   1. argv placeholders are substituted from the boot context;
//   2. env placeholders are substituted + the gateway LLM egress rides the env credential-free;
//   3. the bridge command/args placeholders resolve to the native-CLI tool-bridge launch;
//   4. an unknown placeholder is left intact (a literal the CLI may want);
//   5. the binary path + cwd thread through, and a test prefix arg precedes the spec argv.
//
// The builder is PURE (no I/O).

import { describe, it, expect } from 'vitest';
import { parseCustomAgentSpec } from '../../src/harness/custom/spec.js';
import { buildCustomLaunchConfig } from '../../src/harness/custom/launch-args.js';

const bridge = { command: '/usr/bin/node', args: ['/pkg/bridge-entry.js', '--root', '/work'] };

describe('custom launch-args builder', () => {
  it('substitutes argv placeholders from the boot context', () => {
    const spec = parseCustomAgentSpec({
      command: 'my-agent',
      argv: [
        'run',
        '--session',
        '{sessionId}',
        '--workspace',
        '{workspaceId}',
        '--model',
        '{model}',
      ],
    });
    const config = buildCustomLaunchConfig({
      spec,
      bridge,
      sessionId: 'ses_1',
      workspaceId: 'ws_1',
      model: 'my-model',
      sandboxRoot: '/work',
    });
    expect(config.cmd).toBe('my-agent');
    expect(config.args).toEqual([
      'run',
      '--session',
      'ses_1',
      '--workspace',
      'ws_1',
      '--model',
      'my-model',
    ]);
  });

  it('substitutes the system-prompt + sandbox-root placeholders', () => {
    const spec = parseCustomAgentSpec({
      command: 'my-agent',
      argv: ['--system', '{systemPrompt}', '--root', '{sandboxRoot}'],
    });
    const config = buildCustomLaunchConfig({
      spec,
      bridge,
      sessionId: 'ses_1',
      workspaceId: 'ws_1',
      system: 'You are Orca.',
      sandboxRoot: '/work',
    });
    expect(config.args).toEqual(['--system', 'You are Orca.', '--root', '/work']);
  });

  it('resolves the bridge command/args placeholders to the native-CLI tool-bridge launch', () => {
    const spec = parseCustomAgentSpec({
      command: 'my-agent',
      argv: ['--mcp-cmd', '{bridgeCommand}', '--mcp-args', '{bridgeArgsJson}'],
    });
    const config = buildCustomLaunchConfig({
      spec,
      bridge,
      sessionId: 'ses_1',
      workspaceId: 'ws_1',
      sandboxRoot: '/work',
    });
    expect(config.args?.[0]).toBe('--mcp-cmd');
    expect(config.args?.[1]).toBe('/usr/bin/node');
    expect(config.args?.[2]).toBe('--mcp-args');
    // The args placeholder resolves to a JSON array the CLI can parse.
    expect(JSON.parse(config.args?.[3] ?? 'null')).toEqual(bridge.args);
  });

  it('substitutes env placeholders + carries the gateway LLM egress credential-free', () => {
    const spec = parseCustomAgentSpec({
      command: 'my-agent',
      env: { MY_AGENT_SESSION: '{sessionId}', MODE: 'headless' },
    });
    const config = buildCustomLaunchConfig({
      spec,
      bridge,
      sessionId: 'ses_1',
      workspaceId: 'ws_1',
      sandboxRoot: '/work',
      baseURL: 'https://gw/anthropic',
      apiKey: 'scoped-jwt',
    });
    expect(config.env?.['MY_AGENT_SESSION']).toBe('ses_1');
    expect(config.env?.['MODE']).toBe('headless');
    // The gateway egress is exposed under the neutral ORCA_* names for a generic CLI to read.
    expect(config.env?.['ORCA_LLM_BASE_URL']).toBe('https://gw/anthropic');
    expect(config.env?.['ORCA_LLM_API_KEY']).toBe('scoped-jwt');
  });

  it('leaves an unknown placeholder intact (a literal the CLI may want)', () => {
    const spec = parseCustomAgentSpec({ command: 'my-agent', argv: ['--x', '{notAKnownToken}'] });
    const config = buildCustomLaunchConfig({
      spec,
      bridge,
      sessionId: 'ses_1',
      workspaceId: 'ws_1',
      sandboxRoot: '/work',
    });
    expect(config.args).toEqual(['--x', '{notAKnownToken}']);
  });

  it('threads the cwd through + prepends a test prefix arg before the spec argv', () => {
    const spec = parseCustomAgentSpec({ command: 'my-agent', argv: ['serve'], cwd: 'sub' });
    const config = buildCustomLaunchConfig({
      spec,
      bridge,
      sessionId: 'ses_1',
      workspaceId: 'ws_1',
      sandboxRoot: '/work',
      cliPath: '/opt/my-agent',
      prefixArgs: ['/fake/cli.mjs'],
    });
    expect(config.cmd).toBe('/opt/my-agent');
    expect(config.args?.[0]).toBe('/fake/cli.mjs');
    expect(config.args).toContain('serve');
    expect(config.cwd).toBe('sub');
  });

  it('omits the LLM env entirely when no egress is supplied', () => {
    const spec = parseCustomAgentSpec({ command: 'my-agent' });
    const config = buildCustomLaunchConfig({
      spec,
      bridge,
      sessionId: 'ses_1',
      workspaceId: 'ws_1',
      sandboxRoot: '/work',
    });
    expect(config.env?.['ORCA_LLM_API_KEY']).toBeUndefined();
    expect(config.env?.['ORCA_LLM_BASE_URL']).toBeUndefined();
  });
});
