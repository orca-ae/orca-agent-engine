// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Unit tests for the SDK MCP-server wrapper that binds the per-session
 * sandbox to the Claude Agent SDK's tool-dispatch surface.
 *
 * These tests exercise each tool handler directly against an
 * `InMemorySandboxRuntime` (a tmpdir-backed test double) so we can assert:
 *
 *   1. The server's metadata: name = "orca", version is set, every expected
 *      tool is present with the prefix `mcp__orca__*`.
 *   2. Each tool's handler dispatches into `SandboxHandle.run` /
 *      `SandboxHandle.files.*` rather than the harness host.
 *   3. The CallToolResult shape matches what the SDK's MCP wire expects:
 *      `content: [{type:'text', text:string}]` plus `isError:boolean`.
 *
 * The Layer B integration tests (`real-agent-loop.spec.ts`) prove the same
 * binding survives the full SDK → query() → registerTool path with real
 * Claude traffic. This file's job is to lock the unit-level contract so a
 * regression in the wrapper's plumbing fails fast in `pnpm -F harness test`,
 * not after a 60-second SSE round-trip.
 */
import { describe, it, expect, vi } from 'vitest';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import {
  buildClientExecutedSdkTools,
  buildCustomSdkTools,
  buildOrcaSdkMcpServer,
  buildOrcaSdkTools,
  ORCA_MCP_SERVER_NAME,
  ORCA_MCP_TOOL_NAMES,
} from '../../src/harness/claude/mcp-tools.js';
import type { SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';

async function withSandbox<T>(fn: (sb: SandboxHandle) => Promise<T>): Promise<T> {
  const rt = new InMemorySandboxRuntime();
  const sb = await rt.acquire({});
  const rawReadUtf8Page = sb.files.readUtf8Page.bind(sb.files);
  sb.files.readUtf8Page = async (path, input, constraint) =>
    await rawReadUtf8Page(path, input, constraint ?? { readableRoots: ['/'] });
  try {
    return await fn(sb);
  } finally {
    await sb.destroy();
  }
}

/**
 * The SDK's `tool()` helper rejects calls whose handler returns a plain
 * `{content,error}` result (the legacy agent-toolset shape). It expects the
 * MCP wire shape: `content: [{type:'text', text:'…'}]` plus `isError`.
 * Helper that types-checks the assertion site so a contract drift (someone
 * adds an unrelated key) trips the test rather than the SDK.
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

function readMetadata(text: string): {
  truncation: boolean;
  next_offset: number | null;
  offset_unit: string;
} {
  const prefix = '[orca_read ';
  const start = text.lastIndexOf(prefix);
  expect(start).toBeGreaterThanOrEqual(0);
  expect(text.endsWith(']')).toBe(true);
  return JSON.parse(text.slice(start + prefix.length, -1)) as {
    truncation: boolean;
    next_offset: number | null;
    offset_unit: string;
  };
}

describe('orca SDK MCP server', () => {
  it('routes self-hosted agent tools through a client result callback', async () => {
    const request = vi.fn(async (name: string) => ({
      tool_use_id: 'evt_client_tool',
      content: [{ type: 'text', text: `${name} completed` }],
      is_error: false,
    }));
    const tools = buildClientExecutedSdkTools(['bash'], request);

    expect(tools.map((definition) => definition.name)).toEqual(['bash']);
    const result = await tools[0]!.handler({ command: 'pwd' }, {});

    expect(request).toHaveBeenCalledWith('bash', { command: 'pwd' });
    expect(result).toEqual({
      content: [{ type: 'text', text: 'bash completed' }],
      isError: false,
    });
  });

  it('exposes server metadata and every expected tool name', async () => {
    await withSandbox(async (sb) => {
      const server = buildOrcaSdkMcpServer(sb);
      // SDK config block — `type: 'sdk'` is the SDK's discriminator.
      expect(server.type).toBe('sdk');
      expect(server.name).toBe(ORCA_MCP_SERVER_NAME);
      expect(server.instance).toBeDefined();

      const tools = buildOrcaSdkTools(sb);
      // The MCP wire prefix is applied by the SDK runtime
      // (`mcp__<server>__<tool>`); the tool definitions themselves use the
      // bare name. Assert both: the prefixed surface (what the model sees)
      // and the unprefixed slot (what registerTool sees).
      const bareNames = tools.map((t) => t.name).sort();
      expect(bareNames).toEqual(
        ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'read', 'write'].sort(),
      );
      expect([...ORCA_MCP_TOOL_NAMES].sort()).toEqual(
        bareNames.map((n) => `mcp__orca__${n}`).sort(),
      );
    });
  });

  it('does not register custom tools that collide with sandbox logical tool names', async () => {
    await withSandbox(async (sb) => {
      const server = buildOrcaSdkMcpServer(
        sb,
        undefined,
        [
          { name: 'bash', description: 'Reserved collision.' },
          { name: 'lookup_ticket', description: 'Look up a support ticket.' },
        ],
        async () => ({ custom_tool_use_id: 'evt_custom_1', content: 'ok' }),
      );
      const tools = (server.instance as unknown as { _registeredTools: Record<string, unknown> })
        ._registeredTools;
      const names = Object.keys(tools);
      expect(names.filter((name) => name === 'bash')).toHaveLength(1);
      expect(names).toContain('lookup_ticket');
    });
  });

  it('falls back to unknown for custom tool enum schemas with non-primitive members', () => {
    const tools = buildCustomSdkTools(
      [
        {
          name: 'lookup_ticket',
          input_schema: {
            type: 'object',
            properties: {
              selector: {
                enum: [{ field: 'status' }],
              },
            },
          },
        },
      ],
      async () => ({ custom_tool_use_id: 'evt_custom_1', content: 'ok' }),
    );
    const schema = (
      tools[0] as {
        inputSchema: Record<string, { parse: (value: unknown) => unknown }>;
      }
    ).inputSchema;
    expect(() => schema.selector!.parse({ field: 'status' })).not.toThrow();
  });

  it('bash dispatches into SandboxHandle.run with stdout text + non-error', async () => {
    await withSandbox(async (sb) => {
      const tools = buildOrcaSdkTools(sb);
      const bash = tools.find((t) => t.name === 'bash')!;
      const result = await bash.handler({ command: 'echo orca-srt-marker-XYZ' }, {});
      assertCallToolResult(result);
      expect(result.isError).toBe(false);
      // The tool's output text concatenates stdout, optional stderr, and the
      // exit code marker. Assert against the marker so the test pins the
      // actual sandbox-level execution, not just a generic "not empty" check.
      expect(result.content[0]!.text).toContain('orca-srt-marker-XYZ');
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
      expect(r.content[0]!.text).toMatch(/^hello world\n\[orca_read /);

      // Belt-and-braces: the bytes ALSO landed on the sandbox FS via the
      // direct `files.read` API — proves the MCP path didn't write
      // somewhere weird.
      const direct = await sb.files.read('/data/hi.txt');
      expect(direct.toString('utf8')).toBe('hello world');
    });
  });

  it('reads past 100k with bounded UTF-8 byte pages and next_offset', async () => {
    await withSandbox(async (sb) => {
      const tailMarker = 'ORCA_SKILL_TAIL_MARKER';
      const bytes = Buffer.from(`${'a'.repeat(99_999)}界${'b'.repeat(500)}${tailMarker}`, 'utf8');
      await sb.files.write('/data/large-skill.md', bytes);
      const wholeRead = vi.spyOn(sb.files, 'read').mockRejectedValue(new Error('whole read used'));
      const read = buildOrcaSdkTools(sb).find((t) => t.name === 'read')!;
      const schema = (
        read as {
          inputSchema: Record<string, { parse: (value: unknown) => unknown }>;
        }
      ).inputSchema;
      expect(() => schema.offset!.parse(0)).not.toThrow();
      expect(() => schema.limit!.parse(100_000)).not.toThrow();
      expect(() => schema.limit!.parse(100_001)).toThrow();

      const first = await read.handler({ path: '/data/large-skill.md' }, {});
      assertCallToolResult(first);
      const firstMetadata = readMetadata(first.content[0]!.text);
      expect(first).not.toHaveProperty('structuredContent');
      expect(first.content[0]!.text).not.toContain(tailMarker);
      expect(first.content[0]!.text).not.toContain('\uFFFD');
      expect(firstMetadata).toMatchObject({
        truncation: true,
        next_offset: 4096,
        offset_unit: 'utf8_bytes',
      });

      let second = first;
      let metadata = firstMetadata;
      while (metadata.next_offset !== null) {
        const next = await read.handler(
          { path: '/data/large-skill.md', offset: metadata.next_offset },
          {},
        );
        assertCallToolResult(next);
        second = next;
        metadata = readMetadata(second.content[0]!.text);
      }
      assertCallToolResult(second);
      expect(second.content[0]!.text).toContain(`界${'b'.repeat(500)}${tailMarker}`);
      expect(second.content[0]!.text).not.toContain('\uFFFD');
      expect(readMetadata(second.content[0]!.text)).toMatchObject({
        truncation: false,
        next_offset: null,
      });

      const splitCodePoint = await read.handler(
        { path: '/data/large-skill.md', offset: 100_000 },
        {},
      );
      assertCallToolResult(splitCodePoint);
      expect(splitCodePoint.isError).toBe(true);
      expect(splitCodePoint.content[0]!.text).toContain('not a UTF-8 code point boundary');
      expect(wholeRead).not.toHaveBeenCalled();
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
      expect(r.content[0]!.text).toMatch(/^qux bar qux baz qux\n\[orca_read /);
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

  it('edit refuses files above the bounded-read limit without a whole-file read', async () => {
    await withSandbox(async (sb) => {
      await sb.files.write('/large.txt', Buffer.alloc(100_001, 0x61));
      const wholeRead = vi.spyOn(sb.files, 'read').mockRejectedValue(new Error('whole read used'));
      const edit = buildOrcaSdkTools(sb).find((tool) => tool.name === 'edit')!;

      const result = await edit.handler({ path: '/large.txt', find: 'a', replace: 'b' }, {});

      assertCallToolResult(result);
      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain('exceeds the 100000-byte edit limit');
      expect(wholeRead).not.toHaveBeenCalled();
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
      // Make a directory with no entries via the sandbox's bash tool. We
      // create it via a relative path (no leading `/`) so InMemory's bash
      // (which spawns `bash` with `cwd=root`) lands the dir under the
      // sandbox root rather than the host's `/`. `sandbox.files.list` then
      // resolves `/empty-dir` against the same root.
      await sb.run({ tool: 'bash', args: { command: 'mkdir -p empty-dir' } });
      const l = await tools.find((t) => t.name === 'list')!.handler({ path: '/empty-dir' }, {});
      assertCallToolResult(l);
      expect(l.isError).toBe(false);
      expect(l.content[0]!.text).toBe('(empty)');
    });
  });

  it('delete is idempotent — second call against a removed path is not an error', async () => {
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
    // Belt-and-braces: drive every handler with a benign input and check the
    // shape. Catches the case where someone adds a new tool but forgets to
    // wrap the failure path in `{content:[{type:'text',text:…}], isError:true}`.
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
