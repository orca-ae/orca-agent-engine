// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// native-CLI tool-bridge — an MCP server over STDIO that binds the runner's
// built-in tools to a per-session sandbox.
//
// A native coding CLI launched by the framework is given this bridge as its MCP
// server and connects to it over stdio; its tool calls then execute inside the
// Orca sandbox rather than on the CLI's own host. This suite proves that wiring
// end to end with a REAL MCP `Client` speaking to a FAKE native-CLI host
// subprocess (which stands up the bridge over its stdio against a real sandbox) —
// no real Codex / Claude-Code binary required. It asserts:
//
//   1. the bridge advertises the SAME built-in tool surface the claude provider's
//      in-process `orca` MCP server exposes (bash/read/write/edit/glob/grep/…),
//   2. a `bash` call runs INSIDE the sandbox (its write lands in the sandbox root,
//      not the client's cwd),
//   3. `write` + `read` round-trip a file through the sandbox filesystem.
//
// The sys_terminal_* tools are additionally advertised when the sandbox is a tmux
// pane host; that leg self-skips without `tmux`.

import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { afterEach, beforeEach, describe, it, expect } from 'vitest';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { ORCA_MCP_TOOL_LOGICAL_NAMES } from '../../src/harness/claude/mcp-tools.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const HOST = join(HERE, 'support', 'fake-native-cli-bridge-host.ts');
const TSX = join(HERE, '..', '..', 'node_modules', '.bin', 'tsx');

function tmuxAvailable(): boolean {
  try {
    execFileSync('tmux', ['-V'], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

/** The text of a callTool result's single text block. */
function textOf(res: { content?: Array<{ type?: string; text?: string }> }): string {
  const first = res.content?.[0];
  return typeof first?.text === 'string' ? first.text : '';
}

describe('native-CLI tool-bridge (STDIO MCP → per-session sandbox)', () => {
  let workdir: string;
  let rootFile: string;

  beforeEach(() => {
    workdir = mkdtempSync(join(tmpdir(), 'orca-bridge-spec-'));
    rootFile = join(workdir, 'sandbox-root.txt');
  });

  afterEach(() => {
    rmSync(workdir, { recursive: true, force: true });
  });

  /**
   * Spawn the fake native-CLI host (which serves the bridge over its stdio) and
   * connect a real MCP `Client` to it, exactly as a native CLI subprocess would.
   * The client's cwd is set to a throwaway dir so a tool that (wrongly) executed
   * on the client host instead of in the sandbox would be caught.
   */
  async function withBridgeClient(
    runtime: 'in-memory' | 'tmux',
    fn: (client: Client, sandboxRoot: () => string) => Promise<void>,
  ): Promise<void> {
    const transport = new StdioClientTransport({
      command: TSX,
      args: [HOST, '--runtime', runtime, '--root-file', rootFile],
      cwd: workdir,
      stderr: 'inherit',
    });
    const client = new Client({ name: 'fake-native-cli', version: '1.0.0' });
    await client.connect(transport);
    try {
      // The host writes the sandbox root once acquired; give the read a helper
      // that reflects the current file contents (available by the time the MCP
      // handshake — which happens after acquire+serve — has completed).
      await fn(client, () => readFileSync(rootFile, 'utf8').trim());
    } finally {
      await client.close();
    }
  }

  it('advertises the orca built-in tool surface (same as the claude provider)', async () => {
    await withBridgeClient('in-memory', async (client) => {
      const { tools } = await client.listTools();
      const names = tools.map((t) => t.name);
      // Every logical orca tool the in-process claude MCP server exposes is here.
      for (const logical of ORCA_MCP_TOOL_LOGICAL_NAMES) {
        expect(names).toContain(logical);
      }
    });
  }, 30_000);

  it('a fake CLI runs bash over the bridge and the write lands IN THE SANDBOX', async () => {
    await withBridgeClient('in-memory', async (client, sandboxRoot) => {
      const marker = 'orca-bridge-bash-marker';
      const res = await client.callTool({
        name: 'bash',
        arguments: { command: `echo ${marker} > from-bash.txt && echo ${marker}` },
      });
      expect(textOf(res as { content?: Array<{ text?: string }> })).toContain(marker);

      // The file the bash tool wrote is in the SANDBOX root (reported by the
      // host), NOT in the client's cwd — proof the call executed in the sandbox.
      const inSandbox = join(sandboxRoot(), 'from-bash.txt');
      expect(existsSync(inSandbox)).toBe(true);
      expect(readFileSync(inSandbox, 'utf8')).toContain(marker);
      // And it did NOT leak into the client process's working directory.
      expect(existsSync(join(workdir, 'from-bash.txt'))).toBe(false);
    });
  }, 30_000);

  it('a fake CLI write + read round-trips a file through the sandbox filesystem', async () => {
    await withBridgeClient('in-memory', async (client, sandboxRoot) => {
      const w = await client.callTool({
        name: 'write',
        arguments: { path: '/notes/hello.txt', content: 'bridged-through-mcp' },
      });
      expect(textOf(w as { content?: Array<{ text?: string }> })).toContain('wrote');

      const r = await client.callTool({
        name: 'read',
        arguments: { path: '/notes/hello.txt' },
      });
      expect(textOf(r as { content?: Array<{ text?: string }> })).toBe('bridged-through-mcp');

      // Belt-and-braces: the bytes physically landed under the sandbox root.
      const onDisk = join(sandboxRoot(), 'notes', 'hello.txt');
      expect(readFileSync(onDisk, 'utf8')).toBe('bridged-through-mcp');
    });
  }, 30_000);

  it('surfaces a tool error (read of a missing file) without crashing the bridge', async () => {
    await withBridgeClient('in-memory', async (client) => {
      const r = (await client.callTool({
        name: 'read',
        arguments: { path: '/does/not/exist.txt' },
      })) as { isError?: boolean; content?: Array<{ text?: string }> };
      expect(r.isError).toBe(true);
      expect(textOf(r)).toContain('read failed');

      // The bridge is still alive and serving after a tool-level error.
      const ok = await client.callTool({
        name: 'bash',
        arguments: { command: 'echo still-alive' },
      });
      expect(textOf(ok as { content?: Array<{ text?: string }> })).toContain('still-alive');
    });
  }, 30_000);

  const maybeTmux = tmuxAvailable() ? it : it.skip;
  maybeTmux(
    'additionally advertises the sys_terminal_* tools when the sandbox hosts panes (tmux)',
    async () => {
      await withBridgeClient('tmux', async (client) => {
        const { tools } = await client.listTools();
        const names = tools.map((t) => t.name);
        // The orca built-ins are still present…
        for (const logical of ORCA_MCP_TOOL_LOGICAL_NAMES) {
          expect(names).toContain(logical);
        }
        // …and the Orca-superset interactive-terminal tools are exposed too.
        for (const t of [
          'sys_terminal_launch',
          'sys_terminal_send',
          'sys_terminal_read',
          'sys_terminal_list',
          'sys_terminal_close',
        ]) {
          expect(names).toContain(t);
        }
      });
    },
    30_000,
  );

  maybeTmux(
    'drives a real interactive pane over the bridge: launch → send → read → close',
    async () => {
      await withBridgeClient('tmux', async (client) => {
        const fakeCli = join(HERE, 'support', 'fake-interactive-cli.mjs');
        const launched = (await client.callTool({
          name: 'sys_terminal_launch',
          arguments: { command: `${process.execPath} ${fakeCli}` },
        })) as { content?: Array<{ text?: string }> };
        const id = (JSON.parse(textOf(launched)) as { terminal_id: string }).terminal_id;
        expect(id).toBeTruthy();

        // Poll read until the program's banner renders in the pane.
        const readPane = async (): Promise<string> => {
          const r = (await client.callTool({
            name: 'sys_terminal_read',
            arguments: { terminal_id: id },
          })) as { content?: Array<{ text?: string }> };
          return textOf(r);
        };
        await waitFor(async () => (await readPane()).includes('READY'), 8000);

        // Type a line + Enter; the program echoes it back into the pane.
        await client.callTool({
          name: 'sys_terminal_send',
          arguments: { terminal_id: id, text: 'over-the-bridge', enter: true },
        });
        await waitFor(async () => (await readPane()).includes('you said: over-the-bridge'), 8000);

        // Close it; it drops out of the list.
        await client.callTool({ name: 'sys_terminal_close', arguments: { terminal_id: id } });
        const listed = (await client.callTool({
          name: 'sys_terminal_list',
          arguments: {},
        })) as { content?: Array<{ text?: string }> };
        const entries = JSON.parse(textOf(listed)) as Array<{ terminal_id: string }>;
        expect(entries.map((e) => e.terminal_id)).not.toContain(id);
      });
    },
    30_000,
  );
});

/** Poll `cond` until it resolves true or the timeout elapses. */
async function waitFor(cond: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await cond()) {
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error('waitFor timed out');
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}
