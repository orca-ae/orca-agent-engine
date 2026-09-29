// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { isAppRoute, isAppRouteMutation } from '@ts-rest/core';
import { z } from 'zod';
import {
  sessionAppendEventsBodySchema,
  sessionCreateBodySchema,
  sessionResourceInputSchema,
  sessionUpdateBodySchema,
  sessionsContract,
} from '../../src/contracts/sessions.contract.js';

describe('sessions contract', () => {
  it('validates and retains generic metadata filters with the existing metadata limits', () => {
    const route = sessionsContract.list!;
    if (!isAppRoute(route)) throw new Error('expected the Session list route');
    const query = route.query as z.ZodType<Record<string, unknown>>;
    for (const input of [
      {},
      { metadata_AGENT_TRIGGER: 'local-trigger', metadata_team: 'ops' },
      { metadata_empty: '' },
      { [`metadata_${'k'.repeat(64)}`]: 'v'.repeat(512) },
      { metadata___proto__: 'literal', metadata_constructor: 'literal' },
      Object.fromEntries(Array.from({ length: 16 }, (_, i) => [`metadata_key${i}`, 'v'])),
    ]) {
      const result = query.safeParse(input);
      expect(result.success).toBe(true);
      if (result.success) expect(result.data).toMatchObject(input);
    }
    for (const input of [
      { metadata_: 'value' },
      { [`metadata_${'k'.repeat(65)}`]: 'value' },
      { metadata_team: 'v'.repeat(513) },
      { metadata_team: ['first', 'second'] },
      { metadata_team: null },
      { metadata_team: 123 },
      Object.fromEntries(Array.from({ length: 17 }, (_, i) => [`metadata_key${i}`, 'v'])),
    ]) {
      expect(query.safeParse(input).success, JSON.stringify(input)).toBe(false);
    }
  });

  it('requires environment_id when creating a session', () => {
    const result = sessionCreateBodySchema.safeParse({ agent: 'agt_test' });

    expect(result.success).toBe(false);
  });

  it('accepts a valid environment_id', () => {
    const result = sessionCreateBodySchema.safeParse({
      agent: 'agt_test',
      environment_id: 'env_test',
    });

    expect(result.success).toBe(true);
  });

  it('accepts Claude agent_with_overrides and ordered initial events', () => {
    const result = sessionCreateBodySchema.safeParse({
      agent: {
        type: 'agent_with_overrides',
        id: 'agent_test',
        version: 2,
        model: { id: 'claude-opus-4-8', speed: 'fast' },
        system: null,
        tools: [],
        mcp_servers: [],
        skills: [],
      },
      environment_id: 'env_test',
      initial_events: [
        { type: 'user.message', content: [{ type: 'text', text: 'start' }] },
        {
          type: 'user.define_outcome',
          description: 'finish the task',
          rubric: { type: 'text', content: 'all requirements are met' },
        },
      ],
    });

    expect(result.success).toBe(true);
  });

  it('caps primary Agent skill overrides at 500 entries', () => {
    const skills = Array.from({ length: 501 }, (_, index) => ({
      type: 'anthropic',
      skill_id: `catalog-${index}`,
    }));
    const input = {
      agent: {
        type: 'agent_with_overrides',
        id: 'agent_test',
        skills,
      },
      environment_id: 'env_test',
    } as const;

    expect(
      sessionCreateBodySchema.safeParse({
        ...input,
        agent: { ...input.agent, skills: skills.slice(0, 500) },
      }).success,
    ).toBe(true);
    expect(sessionCreateBodySchema.safeParse(input).success).toBe(false);
  });

  it('rejects non-initial event kinds and more than 50 initial events', () => {
    const base = { agent: 'agent_test', environment_id: 'env_test' };

    expect(
      sessionCreateBodySchema.safeParse({
        ...base,
        initial_events: [{ type: 'user.interrupt' }],
      }).success,
    ).toBe(false);
    expect(
      sessionCreateBodySchema.safeParse({
        ...base,
        initial_events: Array.from({ length: 51 }, () => ({
          type: 'user.message',
          content: [{ type: 'text', text: 'start' }],
        })),
      }).success,
    ).toBe(false);
  });

  it('models metadata null and the reserved vault_ids update field', () => {
    expect(sessionUpdateBodySchema.safeParse({ metadata: null }).success).toBe(true);
    expect(sessionUpdateBodySchema.safeParse({ vault_ids: ['vlt_test'] }).success).toBe(true);
  });

  it('uses the strict seven-event Claude input union', () => {
    expect(sessionAppendEventsBodySchema.safeParse({ events: [] }).success).toBe(true);
    expect(
      sessionAppendEventsBodySchema.safeParse({
        events: [{ type: 'user.tool_result', tool_use_id: 'toolu_1' }],
      }).success,
    ).toBe(true);
    expect(
      sessionAppendEventsBodySchema.safeParse({
        events: [{ type: 'agent.message', content: [{ type: 'text', text: 'forged' }] }],
      }).success,
    ).toBe(false);
  });

  it('strictly validates resource add bodies while retaining documented legacy fields', () => {
    expect(
      sessionResourceInputSchema.safeParse({
        type: 'file',
        file_id: 'file_test',
        access: 'read_only',
        mount_strategy: 'tarball_prefetch',
      }).success,
    ).toBe(true);
    expect(
      sessionResourceInputSchema.safeParse({
        type: 'file',
        file_id: 'file_test',
        unknown_field: true,
      }).success,
    ).toBe(false);
    expect(
      sessionResourceInputSchema.safeParse({
        type: 'memory_store',
        file_id: 'file_wrong_variant',
      }).success,
    ).toBe(false);
  });

  it('exposes every supported resource variant on the attach route contract', () => {
    const attachResource = sessionsContract.attachResource;
    if (!attachResource || !isAppRoute(attachResource) || !isAppRouteMutation(attachResource)) {
      throw new Error('sessionsContract.attachResource is not a mutation route');
    }
    if (!(attachResource.body instanceof z.ZodType)) {
      throw new Error('sessionsContract.attachResource has no zod body schema');
    }
    expect(
      attachResource.body.safeParse({
        type: 'memory_store',
        memory_store_id: 'mems_test',
      }).success,
    ).toBe(true);
    expect(
      attachResource.body.safeParse({
        type: 'github_repository',
        url: 'https://github.com/orca-ae/example',
        authorization_token: 'secret',
      }).success,
    ).toBe(true);
  });

  it('declares canonical 400 responses for validated list, stream, thread, and resource routes', () => {
    for (const route of [
      sessionsContract.list,
      sessionsContract.listEvents,
      sessionsContract.streamEvents,
      sessionsContract.listThreads,
      sessionsContract.listThreadEvents,
      sessionsContract.streamThread,
      sessionsContract.listResources,
      sessionsContract.attachResource,
      sessionsContract.updateResource,
      sessionsContract.detachResource,
    ]) {
      if (!route) throw new Error('sessionsContract is missing an expected route');
      expect(route.responses).toHaveProperty('400');
    }
  });
});
