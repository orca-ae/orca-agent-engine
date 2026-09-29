// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  OS_READ_TOOLS,
  OS_SHELL_TOOLS,
  OS_TOOLS,
  OS_WRITE_TOOLS,
  SUBAGENT_DISPATCH_TOOLS,
  matchesAnyToolPattern,
  matchesToolPattern,
  mcpServerWildcard,
  mcpToolName,
  parseMcpToolName,
} from '../../src/tool-names.js';

/**
 * A guardrail that names a tool the runtime does not have compiles fine, passes
 * a careless test, and then never fires — a security control that is silently a
 * no-op. These assertions are the fixture that stops that: they pin the preset
 * groups to the literal names tools actually carry when a session runs.
 */
describe('operating-system tool presets', () => {
  it('names the shell execution tools under both names shell is exposed as', () => {
    expect(OS_SHELL_TOOLS).toContain('Bash');
    expect(OS_SHELL_TOOLS).toContain('mcp__orca__bash');
  });

  it('names the filesystem read tools under both names reads are exposed as', () => {
    for (const name of ['Read', 'Glob', 'Grep']) {
      expect(OS_READ_TOOLS).toContain(name);
    }
    for (const name of [
      'mcp__orca__read',
      'mcp__orca__glob',
      'mcp__orca__grep',
      'mcp__orca__list',
    ]) {
      expect(OS_READ_TOOLS).toContain(name);
    }
  });

  it('names the filesystem mutation tools under both names mutations are exposed as', () => {
    for (const name of ['Write', 'Edit', 'NotebookEdit']) {
      expect(OS_WRITE_TOOLS).toContain(name);
    }
    for (const name of ['mcp__orca__write', 'mcp__orca__edit', 'mcp__orca__delete']) {
      expect(OS_WRITE_TOOLS).toContain(name);
    }
  });

  it('names the subagent dispatch tool', () => {
    expect(SUBAGENT_DISPATCH_TOOLS).toContain('Agent');
  });

  it('covers every tool the runtime exposes under its own server, with none invented', () => {
    const covered = OS_TOOLS.filter((name) => name.startsWith('mcp__')).sort();
    expect(covered).toEqual(
      [
        'mcp__orca__bash',
        'mcp__orca__delete',
        'mcp__orca__edit',
        'mcp__orca__glob',
        'mcp__orca__grep',
        'mcp__orca__list',
        'mcp__orca__read',
        'mcp__orca__write',
      ].sort(),
    );
  });

  it('is the union of read, write and shell with no duplicates', () => {
    const union = [...OS_READ_TOOLS, ...OS_WRITE_TOOLS, ...OS_SHELL_TOOLS];
    expect([...OS_TOOLS].sort()).toEqual([...new Set(union)].sort());
    expect(OS_TOOLS.length).toBe(new Set(OS_TOOLS).size);
  });

  it.each([
    ['read', OS_READ_TOOLS],
    ['write', OS_WRITE_TOOLS],
    ['shell', OS_SHELL_TOOLS],
    ['subagent dispatch', SUBAGENT_DISPATCH_TOOLS],
  ])('lists the %s preset without duplicates', (_label, preset) => {
    expect(preset.length).toBe(new Set(preset).size);
  });
});

describe('MCP tool keys', () => {
  it('builds the canonical key from a server and tool name', () => {
    expect(mcpToolName('github', 'create_issue')).toBe('mcp__github__create_issue');
  });

  it('builds the whole-server wildcard key', () => {
    expect(mcpServerWildcard('github')).toBe('mcp__github__*');
  });

  it.each([
    ['orca', 'read'],
    ['github', 'create_issue'],
    ['a-server', 'tool'],
  ])('round-trips %s/%s through build and parse', (serverName, toolName) => {
    expect(parseMcpToolName(mcpToolName(serverName, toolName))).toEqual({ serverName, toolName });
  });

  it('keeps underscores in the tool name by splitting on the first separator', () => {
    expect(parseMcpToolName('mcp__srv__my_tool')).toEqual({
      serverName: 'srv',
      toolName: 'my_tool',
    });
  });

  it('gives the whole remainder to the tool name when it contains a separator', () => {
    expect(parseMcpToolName('mcp__srv__a__b')).toEqual({ serverName: 'srv', toolName: 'a__b' });
  });

  it('returns null for a plain tool name', () => {
    expect(parseMcpToolName('Bash')).toBeNull();
  });

  it.each([
    ['no separator after the server', 'mcp__srv'],
    ['an empty tool name', 'mcp__srv__'],
    ['an empty server name', 'mcp____read'],
    ['nothing after the prefix', 'mcp__'],
    ['an empty string', ''],
    ['a prefix that only looks similar', 'mcpx__srv__read'],
  ])('returns null for %s', (_label, name) => {
    expect(parseMcpToolName(name)).toBeNull();
  });
});

describe('tool pattern matching', () => {
  it('matches an exact name', () => {
    expect(matchesToolPattern('Bash', 'Bash')).toBe(true);
  });

  it('is anchored, so a pattern does not match a longer name', () => {
    expect(matchesToolPattern('Bashful', 'Bash')).toBe(false);
    expect(matchesToolPattern('mcp__orca__read', 'read')).toBe(false);
  });

  it('is case sensitive, because case distinguishes two different tools', () => {
    expect(matchesToolPattern('bash', 'Bash')).toBe(false);
  });

  it('matches everything with a bare wildcard', () => {
    for (const name of ['Bash', 'mcp__orca__read', '']) {
      expect(matchesToolPattern(name, '*')).toBe(true);
    }
  });

  it('scopes a server wildcard to that server only', () => {
    expect(matchesToolPattern('mcp__github__create_issue', 'mcp__github__*')).toBe(true);
    expect(matchesToolPattern('mcp__gitlab__create_issue', 'mcp__github__*')).toBe(false);
  });

  it('treats regex metacharacters in a pattern as literal text', () => {
    expect(matchesToolPattern('axb', 'a.b')).toBe(false);
    expect(matchesToolPattern('a.b', 'a.b')).toBe(true);
    expect(matchesToolPattern('a+b', 'a+b')).toBe(true);
    expect(matchesToolPattern('ab', 'a+b')).toBe(false);
    expect(matchesToolPattern('(Bash)', '(Bash)')).toBe(true);
  });

  it('matches when any pattern in the list matches', () => {
    expect(matchesAnyToolPattern('Bash', ['Read', 'Bash'])).toBe(true);
    expect(matchesAnyToolPattern('mcp__orca__read', ['Bash', 'mcp__orca__*'])).toBe(true);
  });

  it('matches nothing when the pattern list is empty', () => {
    expect(matchesAnyToolPattern('Bash', [])).toBe(false);
    expect(matchesAnyToolPattern('', [])).toBe(false);
  });

  it('matches nothing when no pattern in the list matches', () => {
    expect(matchesAnyToolPattern('Bash', ['Read', 'mcp__orca__*'])).toBe(false);
  });
});
