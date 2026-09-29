// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the in-process `orca` SDK MCP server that binds the per-session
// sandbox to the Claude Agent SDK's tool-dispatch surface — the runner's own copy
// (the claude provider is otherwise LLM-only; this is what gives it bash/read/write).
//
// These tests exercise each tool handler directly against an
// `InMemorySandboxRuntime` (a tmpdir-backed test double from `@orca/sandbox-runtime`)
// so we can assert:
//
//   1. The server metadata: name = "orca", version set, every expected tool present
//      with the prefix `mcp__orca__*`.
//   2. Each tool handler dispatches into `SandboxHandle.run` / `SandboxHandle.files.*`
//      (the per-session sandbox), never the runner host.
//   3. The CallToolResult shape matches what the SDK's MCP wire expects:
//      `content: [{ type:'text', text:string }]` plus `isError:boolean`.

import { execFileSync } from 'node:child_process';
import { describe, it, expect } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import {
  buildDelegateOnlySdkMcpServer,
  buildDelegateSdkTool,
  buildOrcaSdkMcpServer,
  buildOrcaSdkTools,
  DELEGATE_TO_AGENT_MCP_TOOL_NAME,
  DELEGATE_TO_AGENT_TOOL_NAME,
  ORCA_MCP_SERVER_NAME,
  ORCA_MCP_TOOL_NAMES,
} from '../../src/harness/claude/mcp-tools.js';
import { TmuxSandboxRuntime } from '../../src/sandbox/tmux-sandbox.js';
import type { SandboxHandle } from '../../src/sandbox/seam.js';
import type { DelegateRequest, DelegateResult } from '../../src/harness/agent-harness.js';

/** True when a usable `tmux` binary is on PATH (the pane-capable legs skip otherwise). */
function tmuxAvailable(): boolean {
  try {
    execFileSync('tmux', ['-V'], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

async function withSandbox<T>(fn: (sb: SandboxHandle) => Promise<T>): Promise<T> {
  const rt = new InMemorySandboxRuntime();
  const sb = await rt.acquire({});
  try {
    return await fn(sb);
  } finally {
    await sb.destroy();
  }
}

/**
 * The SDK's `tool()` helper expects the MCP wire shape: `content: [{type:'text',
 * text:'…'}]` plus `isError`. This helper types-checks the assertion site so a
 * contract drift (an unrelated key added) trips the test rather than the SDK.
 */
function assertCallToolResult(
  r: unknown,
): asserts r is { content: Array<{ type: 'text'; text: string }>; isError: boolean } {
  expect(r).toBeTruthy();
  const obj = r as { content?: unknown; isError?: unknown };
  expect(Array.isArray(obj.content)).toBe(true);
  for (const block of obj.content as Array<{ type?: unknown; text?: unknown }>) {
    expect(block.type).toBe('text');
    expect(typeof block.text).toBe('string');
  }
  expect(typeof obj.isError).toBe('boolean');
}

describe('orca SDK MCP server (session-runner)', () => {
  it('exposes server metadata and every expected tool name', async () => {
    await withSandbox(async (sb) => {
      const server = buildOrcaSdkMcpServer(sb);
      // SDK config block — `type: 'sdk'` is the SDK's discriminator.
      expect(server.type).toBe('sdk');
      expect(server.name).toBe(ORCA_MCP_SERVER_NAME);
      expect(server.instance).toBeDefined();

      const tools = buildOrcaSdkTools(sb);
      const bareNames = tools.map((t) => t.name).sort();
      expect(bareNames).toEqual(
        ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'read', 'write'].sort(),
      );
      expect([...ORCA_MCP_TOOL_NAMES].sort()).toEqual(
        bareNames.map((n) => `mcp__orca__${n}`).sort(),
      );
    });
  });

  it('filters the tool set to the allowed logical names when supplied', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb, ['bash', 'read']);
      expect(tools.map((t) => t.name).sort()).toEqual(['bash', 'read']);
    });
  });

  it('bash dispatches into SandboxHandle.run with stdout text + non-error', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      const bash = tools.find((t) => t.name === 'bash')!;
      const result = await bash.handler({ command: 'echo orca-sandbox-marker-XYZ' }, {});
      assertCallToolResult(result);
      expect(result.isError).toBe(false);
      expect(result.content[0]!.text).toContain('orca-sandbox-marker-XYZ');
      expect(result.content[0]!.text).toContain('[exit_code] 0');
    });
  });

  it('bash surfaces non-zero exit as isError=true', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      const bash = tools.find((t) => t.name === 'bash')!;
      const result = await bash.handler({ command: 'exit 7' }, {});
      assertCallToolResult(result);
      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain('[exit_code] 7');
    });
  });

  it('write + read round-trips a file through SandboxHandle.files', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      const write = tools.find((t) => t.name === 'write')!;
      const read = tools.find((t) => t.name === 'read')!;

      const w = await write.handler({ path: '/data/hi.txt', content: 'hello world' }, {});
      assertCallToolResult(w);
      expect(w.isError).toBe(false);
      expect(w.content[0]!.text).toContain('wrote /data/hi.txt');

      const r = await read.handler({ path: '/data/hi.txt' }, {});
      assertCallToolResult(r);
      expect(r.isError).toBe(false);
      expect(r.content[0]!.text).toBe('hello world');

      // Belt-and-braces: the bytes ALSO landed on the sandbox FS via the direct
      // `files.read` API — proves the MCP path wrote into the sandbox, not the host.
      const direct = await sb.files.read('/data/hi.txt');
      expect(direct.toString('utf8')).toBe('hello world');
    });
  });

  it('read returns isError=true on a missing file', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      const read = tools.find((t) => t.name === 'read')!;
      const r = await read.handler({ path: '/does/not/exist.txt' }, {});
      assertCallToolResult(r);
      expect(r.isError).toBe(true);
      expect(r.content[0]!.text).toContain('read failed');
    });
  });

  it('edit replaces all occurrences and reports the count', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/f.txt', content: 'foo bar foo baz foo' }, {});

      const e = await tools
        .find((t) => t.name === 'edit')!
        .handler({ path: '/f.txt', find: 'foo', replace: 'qux' }, {});
      assertCallToolResult(e);
      expect(e.isError).toBe(false);
      expect(e.content[0]!.text).toContain('replaced 3 occurrence');

      const r = await tools.find((t) => t.name === 'read')!.handler({ path: '/f.txt' }, {});
      assertCallToolResult(r);
      expect(r.content[0]!.text).toBe('qux bar qux baz qux');
    });
  });

  it('edit no-ops with a friendly message when pattern not found', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/f.txt', content: 'hello' }, {});
      const e = await tools
        .find((t) => t.name === 'edit')!
        .handler({ path: '/f.txt', find: 'nope', replace: 'X' }, {});
      assertCallToolResult(e);
      expect(e.isError).toBe(false);
      expect(e.content[0]!.text).toContain('no occurrences');
    });
  });

  it('list returns one-level-deep entries from sandbox.files.list', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/srv/a.txt', content: 'a' }, {});
      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/srv/b.txt', content: 'b' }, {});
      const l = await tools.find((t) => t.name === 'list')!.handler({ path: '/srv' }, {});
      assertCallToolResult(l);
      expect(l.isError).toBe(false);
      const lines = l.content[0]!.text.split('\n').sort();
      expect(lines).toEqual(['a.txt', 'b.txt']);
    });
  });

  it('list reports `(empty)` for empty directories rather than a blank text block', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      // Create a directory with no entries via the sandbox's bash tool, using a
      // relative path so InMemory's bash (cwd=root) lands it under the sandbox root;
      // `sandbox.files.list('/empty-dir')` then resolves against the same root.
      await sb.run({ tool: 'bash', args: { command: 'mkdir -p empty-dir' } });
      const l = await tools.find((t) => t.name === 'list')!.handler({ path: '/empty-dir' }, {});
      assertCallToolResult(l);
      expect(l.isError).toBe(false);
      expect(l.content[0]!.text).toBe('(empty)');
    });
  });

  it('delete is idempotent — a second call against a removed path is not an error', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      await tools.find((t) => t.name === 'write')!.handler({ path: '/gone.txt', content: 'x' }, {});

      const d1 = await tools.find((t) => t.name === 'delete')!.handler({ path: '/gone.txt' }, {});
      assertCallToolResult(d1);
      expect(d1.isError).toBe(false);

      const d2 = await tools.find((t) => t.name === 'delete')!.handler({ path: '/gone.txt' }, {});
      assertCallToolResult(d2);
      expect(d2.isError).toBe(false);
    });
  });

  it('glob lists matching paths via SandboxHandle.run({tool:"glob"})', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/srv/a.txt', content: 'a' }, {});
      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/srv/b.txt', content: 'b' }, {});
      const g = await tools
        .find((t) => t.name === 'glob')!
        .handler({ pattern: '*.txt', root: '/srv' }, {});
      assertCallToolResult(g);
      expect(g.isError).toBe(false);
      const lines = g.content[0]!.text.split('\n').sort();
      expect(lines).toEqual(['a.txt', 'b.txt']);
    });
  });

  it('grep returns matching lines via SandboxHandle.run({tool:"grep"})', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/srv/a.txt', content: 'hello world\n' }, {});
      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/srv/b.txt', content: 'goodbye\n' }, {});
      const g = await tools
        .find((t) => t.name === 'grep')!
        .handler({ pattern: 'hello', root: '/srv' }, {});
      assertCallToolResult(g);
      expect(g.isError).toBe(false);
      expect(g.content[0]!.text).toContain('a.txt');
      expect(g.content[0]!.text).not.toContain('b.txt');
    });
  });

  it('every tool handler returns the SDK-required CallToolResult shape', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      const probes: Record<string, Record<string, unknown>> = {
        bash: { command: 'true' },
        read: { path: '/missing.txt' },
        write: { path: '/probe.txt', content: 'x' },
        edit: { path: '/probe.txt', find: 'nope', replace: 'X' },
        list: { path: '/' },
        delete: { path: '/probe.txt' },
        glob: { pattern: '*.txt', root: '/' },
        grep: { pattern: 'X', root: '/' },
      };
      for (const t of tools) {
        const input = probes[t.name]!;
        const result = await t.handler(input, {});
        assertCallToolResult(result);
      }
    });
  });
});

// The Orca-superset sys_terminal_* tools are added to the SAME `orca` MCP server
// the claude provider builds, but ONLY when the sandbox can host interactive
// panes (a TerminalHost). This proves the claude provider's tool surface gains
// them for a pane-capable handle (tmux) and omits them for one that cannot back
// them (the InMemory tmpdir runtime) — the graceful-degradation contract.
describe('orca SDK MCP server — sys_terminal_* composition', () => {
  /**
   * The bare tool names registered on a `createSdkMcpServer` result. The SDK keeps
   * them on the underlying server's `_registeredTools` map; reading its keys is the
   * least-invasive way to assert the composed surface without standing up a full
   * MCP transport (which the native-cli-bridge spec already exercises end to end).
   */
  function registeredToolNames(server: { instance?: unknown }): string[] {
    const inst = server.instance as { _registeredTools?: Record<string, unknown> } | undefined;
    return inst?._registeredTools ? Object.keys(inst._registeredTools) : [];
  }

  const SYS_TERMINAL = [
    'sys_terminal_launch',
    'sys_terminal_send',
    'sys_terminal_read',
    'sys_terminal_list',
    'sys_terminal_close',
  ];

  // tmux is the only reusable TerminalHost, so the pane-capable legs need the
  // binary. Gate them as a SKIP (not an early `return` inside the body, which
  // reports a green test that asserted nothing) so an absent tmux shows up in the
  // skip count — the same posture as every sibling gate in this suite.
  const maybeTmux = tmuxAvailable() ? it : it.skip;

  maybeTmux(
    'adds sys_terminal_* to the orca server for a pane-capable (tmux) sandbox',
    async () => {
      const rt = new TmuxSandboxRuntime();
      const sb = await rt.acquire({});
      try {
        const names = registeredToolNames(buildOrcaSdkMcpServer(sb));
        // The file/exec built-ins are still present…
        for (const logical of ['bash', 'read', 'write', 'edit', 'list', 'delete', 'glob', 'grep']) {
          expect(names).toContain(logical);
        }
        // …and the interactive-terminal tools are composed in alongside them.
        for (const t of SYS_TERMINAL) {
          expect(names).toContain(t);
        }
      } finally {
        await sb.destroy();
      }
    },
  );

  it('omits sys_terminal_* for a sandbox that cannot host panes (InMemory)', async () => {
    await withSandbox(async (sb) => {
      const names = registeredToolNames(buildOrcaSdkMcpServer(sb));
      expect(names).toContain('bash'); // built-ins present…
      for (const t of SYS_TERMINAL) {
        expect(names).not.toContain(t); // …but no terminal tools (no pane host).
      }
    });
  });

  maybeTmux('honors the tool allowlist for sys_terminal_* on a pane-capable sandbox', async () => {
    const rt = new TmuxSandboxRuntime();
    const sb = await rt.acquire({});
    try {
      // Allow only one terminal tool + one file tool; the rest are withheld.
      const names = registeredToolNames(buildOrcaSdkMcpServer(sb, ['bash', 'sys_terminal_launch']));
      expect(names.sort()).toEqual(['bash', 'sys_terminal_launch'].sort());
    } finally {
      await sb.destroy();
    }
  });
});

// The provider-side DELEGATION tool — the `mcp__orca__delegate_to_agent` tool the
// coordinator's model calls to spawn a roster subagent's thread. This is what closes
// the runner-thread-orchestration gap: previously NO real provider exposed a tool that
// invokes the `delegate` seam, so the whole thread choreography was unreachable with a
// real provider. These tests pin the tool builder + its composition onto the `orca`
// server (folded in for a sandboxed coordinator; standalone for a chat-only one).
describe('orca SDK MCP server — delegate_to_agent tool', () => {
  /** The bare tool names registered on a `createSdkMcpServer` result. */
  function registeredToolNames(server: { instance?: unknown }): string[] {
    const inst = server.instance as { _registeredTools?: Record<string, unknown> } | undefined;
    return inst?._registeredTools ? Object.keys(inst._registeredTools) : [];
  }

  it('invokes the delegate seam with the roster agent + prompt and returns its result', async () => {
    const calls: DelegateRequest[] = [];
    const delegate = async (req: DelegateRequest): Promise<DelegateResult> => {
      calls.push(req);
      return { sessionThreadId: 'sth_x', result: `did:${req.agentName}` };
    };
    const t = buildDelegateSdkTool(delegate);
    expect(t.name).toBe(DELEGATE_TO_AGENT_TOOL_NAME);

    const res = await t.handler({ agent_name: 'researcher', prompt: 'find X' }, {});
    assertCallToolResult(res);
    expect(res.isError).toBe(false);
    expect(res.content[0]!.text).toBe('did:researcher');
    // The model's tool args mapped straight onto the seam's DelegateRequest.
    expect(calls).toEqual([{ agentName: 'researcher', prompt: 'find X' }]);
  });

  it('surfaces a delegation refusal as a tool error (isError=true), not a throw', async () => {
    const delegate = async (): Promise<DelegateResult> => {
      throw new Error('delegation refused: the concurrency limit of 25 session threads is reached');
    };
    const t = buildDelegateSdkTool(delegate);
    const res = await t.handler({ agent_name: 'worker', prompt: 'go' }, {});
    assertCallToolResult(res);
    // A refused delegation is a clean tool error the model can react to.
    expect(res.isError).toBe(true);
    expect(res.content[0]!.text).toContain('delegation failed');
    expect(res.content[0]!.text).toContain('concurrency limit');
  });

  it('is folded into the full orca server for a sandboxed coordinator (alongside the file tools)', async () => {
    await withSandbox(async (sb) => {
      const delegate = async (): Promise<DelegateResult> => ({ sessionThreadId: 's', result: '' });
      const names = registeredToolNames(buildOrcaSdkMcpServer(sb, undefined, delegate));
      // The file/exec built-ins are still present…
      for (const logical of ['bash', 'read', 'write', 'edit', 'list', 'delete', 'glob', 'grep']) {
        expect(names).toContain(logical);
      }
      // …and the delegation tool is composed in alongside them.
      expect(names).toContain(DELEGATE_TO_AGENT_TOOL_NAME);
    });
  });

  it('is ABSENT from the orca server for a single-agent harness (no delegate seam)', async () => {
    await withSandbox(async (sb) => {
      const names = registeredToolNames(buildOrcaSdkMcpServer(sb));
      expect(names).toContain('bash');
      expect(names).not.toContain(DELEGATE_TO_AGENT_TOOL_NAME);
    });
  });

  it('is NOT narrowed by the tool allowlist (the roster is a coordinator capability)', async () => {
    await withSandbox(async (sb) => {
      const delegate = async (): Promise<DelegateResult> => ({ sessionThreadId: 's', result: '' });
      // The allowlist restricts the file tools to `bash`, but the delegation tool must
      // survive regardless — it is not a per-skill sandbox tool the allowlist narrows.
      const names = registeredToolNames(buildOrcaSdkMcpServer(sb, ['bash'], delegate));
      expect(names.sort()).toEqual(['bash', DELEGATE_TO_AGENT_TOOL_NAME].sort());
    });
  });

  it('buildDelegateOnlySdkMcpServer carries ONLY the delegation tool (chat-only coordinator)', async () => {
    const delegate = async (): Promise<DelegateResult> => ({ sessionThreadId: 's', result: '' });
    const server = buildDelegateOnlySdkMcpServer(delegate);
    expect(server.type).toBe('sdk');
    expect(server.name).toBe(ORCA_MCP_SERVER_NAME);
    // A chat-only coordinator has no sandbox → the server carries the delegation tool
    // and no file/exec tools.
    expect(registeredToolNames(server)).toEqual([DELEGATE_TO_AGENT_TOOL_NAME]);
  });

  it('the qualified tool name is the mcp__orca__ form the model calls', () => {
    expect(DELEGATE_TO_AGENT_MCP_TOOL_NAME).toBe(`mcp__orca__${DELEGATE_TO_AGENT_TOOL_NAME}`);
  });
});
