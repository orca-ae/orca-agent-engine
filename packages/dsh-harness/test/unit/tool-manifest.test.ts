// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it, vi } from 'vitest';
import { Context } from '@deepseek-ai/cordis';
import Tools from '@deepseek-ai/dsh-tools';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { FrozenToolManifest } from '../../src/tool-manifest.js';

it('freezes the real pinned Tools service without granting a later registration', async () => {
  const ctx = new Context();
  ctx.provide('systemPrompt', { tools: () => () => {} } as never);
  new Tools(ctx, { mode: 'native' });
  const body = vi.fn(async () => 'ok');
  ctx.tools.register({
    name: 'real_probe',
    description: 'real registry',
    parameters: { type: 'object' },
    output: {
      schema: { type: 'string' },
      render: (_args, value) => [{ type: 'text', text: String(value) }],
    },
    execute: body,
  });
  const frozen = new FrozenToolManifest(ctx);
  const agent = {} as Agent;
  try {
    frozen.freeze(agent);
    const definition = ctx.tools.get('real_probe', agent)!;
    expect(await definition.execute({}, { signal: new AbortController().signal } as never)).toBe(
      'ok',
    );
    const call = {
      name: 'real_probe',
      arguments: {},
      agent,
      callId: 'fixture_call',
      signal: new AbortController().signal,
    };
    expect((await ctx.tools.execute(call as never)).isError).not.toBe(true);
    ctx.tools.register({
      name: 'late_probe',
      description: 'late',
      parameters: { type: 'object' },
      output: { schema: {}, render: () => [] },
      execute: async () => null,
    });
    expect(ctx.tools.get('late_probe', agent)).toBeUndefined();
    expect(ctx.tools.schemas(agent).map((tool) => tool.name)).toEqual(['real_probe']);
    expect(() => frozen.assertCurrent()).toThrow('manifest changed');
    await expect(
      definition.execute({}, { signal: new AbortController().signal } as never),
    ).rejects.toThrow('manifest changed');
    expect((await ctx.tools.execute({ ...call, name: 'late_probe' } as never)).isError).toBe(true);
    expect(body).toHaveBeenCalledTimes(2);
  } finally {
    frozen.close();
    await ctx.fiber.dispose();
  }
});

it('freezes dispatch and argument schemas, rejects registry drift and excludes external schemas', async () => {
  const execute = vi.fn(async () => 'original');
  const definition = {
    name: 'business_probe',
    description: 'probe',
    parameters: {
      type: 'object',
      properties: { n: { type: 'integer' } },
      required: ['n'],
      additionalProperties: false,
    },
    execute,
  };
  const schemas = [
    {
      name: definition.name,
      description: definition.description,
      parameters: definition.parameters,
    },
  ];
  const tools = { get: vi.fn(() => definition), schemas: () => structuredClone(schemas) };
  const ctx = { tools } as unknown as Context;
  const agent = {} as Agent;
  const frozen = new FrozenToolManifest(ctx);
  frozen.freeze(agent);
  const dispatch = ctx.tools.get('business_probe', agent)!;
  expect(ctx.tools.get('new_tool', agent)).toBeUndefined();
  expect(frozen.manifest(agent, new Set(['business_probe']))).toBe('[]');
  definition.execute = vi.fn(async () => 'replaced');
  const exec = { signal: new AbortController().signal } as never;
  expect(await dispatch.execute({ n: 2 }, exec)).toBe('original');
  await expect(dispatch.execute({ n: '2' }, exec)).rejects.toThrow('frozen tool arguments');
  expect(execute).toHaveBeenCalledTimes(1);
  schemas.push({ ...schemas[0]!, name: 'late_registration' });
  expect(() => frozen.assertCurrent()).toThrow('manifest changed');
  await expect(dispatch.execute({ n: 2 }, exec)).rejects.toThrow('manifest changed');
  schemas.pop();
  schemas[0]!.parameters.properties.n.type = 'string';
  expect(() => frozen.assertCurrent()).toThrow('manifest changed');
  schemas.length = 0;
  expect(() => frozen.assertCurrent()).toThrow('manifest changed');
  frozen.close();
});

it('rejects children that introduce a name or change a schema', () => {
  let names = ['parent'];
  const tools = {
    get: () => ({ execute: async () => '' }),
    schemas: () => names.map((name) => ({ name, parameters: { type: 'object' } })),
  };
  const frozen = new FrozenToolManifest({ tools } as unknown as Context);
  frozen.freeze({} as Agent);
  names = ['parent', 'expanded'];
  expect(() => frozen.freeze({} as Agent)).toThrow('expands root');
  frozen.close();
});

it('keeps Session MCP names outside the profile identity without changing their existing syntax', () => {
  const definition = {
    name: 'mcp__docs__search.page',
    parameters: { type: 'object' },
    execute: async () => null,
  };
  const tools = {
    get: () => definition,
    schemas: () => [{ name: definition.name, parameters: definition.parameters }],
  };
  const external = new Set([definition.name]);
  const frozen = new FrozenToolManifest({ tools } as unknown as Context, external);
  const agent = {} as Agent;
  frozen.freeze(agent);
  expect(frozen.manifest(agent, external)).toBe('[]');
  frozen.close();
});
