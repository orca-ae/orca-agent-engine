// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import {
  assertSupportedJsonSchema,
  validateJsonSchemaValue,
  type ToolDefinition,
  type ToolRunContext,
} from '@deepseek-ai/dsh-tools';

function canonicalJson(value: unknown): string {
  return JSON.stringify(value, (_key, item) =>
    item && typeof item === 'object' && !Array.isArray(item)
      ? Object.fromEntries(
          Object.keys(item)
            .sort()
            .map((key) => [key, item[key]]),
        )
      : item,
  );
}

/** Freeze the native registry's dispatch definitions as well as its model-visible schemas. */
export class FrozenToolManifest {
  private readonly scopes = new Map<
    Agent,
    { schemas: string; definitions: Map<string, ToolDefinition> }
  >();
  private rootSchemas?: Map<string, string>;
  private readonly originalGet;
  private readonly originalSchemas;

  constructor(
    private readonly ctx: Context,
    private readonly platform: ReadonlySet<string> = new Set(),
  ) {
    this.originalGet = ctx.tools.get.bind(ctx.tools);
    this.originalSchemas = ctx.tools.schemas.bind(ctx.tools);
    const original = this.originalGet;
    ctx.tools.get = (name, scope) => {
      const frozen = this.scopes.get(scope as Agent);
      return frozen ? frozen.definitions.get(name) : original(name, scope);
    };
    ctx.tools.schemas = (scope) => {
      const frozen = this.scopes.get(scope as Agent);
      return frozen ? JSON.parse(frozen.schemas) : this.originalSchemas(scope);
    };
  }

  freeze(agent: Agent): void {
    const schemas = this.originalSchemas(agent).sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
    );
    const names = new Set<string>();
    const definitions = new Map<string, ToolDefinition>();
    for (const schema of schemas) {
      if (
        (!this.platform.has(schema.name) && !/^[A-Za-z0-9_-]{1,128}$/.test(schema.name)) ||
        names.has(schema.name)
      )
        throw new Error('Invalid or colliding DSH tool name');
      names.add(schema.name);
      // Native/bridged tools keep their existing argument validators (including cursor bounds).
      if (!this.platform.has(schema.name)) assertSupportedJsonSchema(schema.parameters);
      if (this.rootSchemas && this.rootSchemas.get(schema.name) !== canonicalJson(schema))
        throw new Error('DSH child tool schema expands root manifest');
      const definition = this.originalGet(schema.name, agent);
      if (!definition) throw new Error('DSH tool definition unavailable');
      const execute = definition.execute.bind(definition);
      definitions.set(
        schema.name,
        Object.freeze({
          ...definition,
          parameters: structuredClone(schema.parameters),
          execute: async (args: unknown, exec: ToolRunContext) => {
            this.assertCurrent(agent);
            const errors = this.platform.has(schema.name)
              ? []
              : validateJsonSchemaValue(schema.parameters, args);
            if (errors.length)
              throw new Error('DSH frozen tool arguments invalid: ' + errors.join('; '));
            exec.signal.throwIfAborted();
            return execute(args, exec);
          },
        }),
      );
    }
    this.rootSchemas ??= new Map(schemas.map((schema) => [schema.name, canonicalJson(schema)]));
    this.scopes.set(agent, { schemas: canonicalJson(schemas), definitions });
  }

  assertCurrent(agent?: Agent): void {
    for (const [scope, frozen] of this.scopes) {
      if (agent && scope !== agent) continue;
      const live = this.originalSchemas(scope).sort((a, b) =>
        a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
      );
      if (canonicalJson(live) !== frozen.schemas)
        throw new Error('DSH tool manifest changed after initialization');
    }
  }

  manifest(agent: Agent, external: ReadonlySet<string>): string {
    const frozen = this.scopes.get(agent);
    if (!frozen) throw new Error('DSH tools are not initialized');
    return canonicalJson(
      JSON.parse(frozen.schemas).filter((schema: { name: string }) => !external.has(schema.name)),
    );
  }

  forget(agent: Agent): void {
    this.scopes.delete(agent);
  }

  close(): void {
    this.ctx.tools.get = this.originalGet;
    this.ctx.tools.schemas = this.originalSchemas;
    this.scopes.clear();
  }
}
