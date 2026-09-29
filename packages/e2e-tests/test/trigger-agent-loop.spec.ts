// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer B cron Trigger coverage.
 *
 * This spec exercises the live Registry planner/dispatcher, the ordinary
 * Session lifecycle outbox, the configured TranscriptStore backend, Harness,
 * and the selected model. Direct SQL is used only to make the Trigger due immediately;
 * neither reconciler is imported or invoked by the test.
 */
import { REAL_AGENT, requireRealAgentKey } from './real-agent-config.js';
import { randomUUID } from 'node:crypto';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';

interface AgentResponse {
  id: string;
  version: number;
}

interface TriggerResponse {
  id: string;
  name: string;
  session_mode: 'SESSION_PER_EVENT';
  source: {
    type: 'cron';
    schedule: string;
    timezone: string;
    payload: string;
  };
  session: {
    environment_id: string;
    title_template: string | null;
    metadata: Record<string, string>;
    vault_ids: string[];
  };
  replicas: 1;
  status: 'active' | 'paused' | 'archived';
}

interface SessionResponse {
  id: string;
  title: string | null;
  status: string;
}

interface SessionEvent {
  type: string;
  content?: Array<{ type?: string; text?: string }>;
  [key: string]: unknown;
}

const FAILURE_EVENT_TYPES = new Set([
  'agent.turn_failed',
  'session.setup_failed',
  'session.status_error',
  'session.error',
]);
const REAL_CLAUDE_RETRY = Number(process.env['ORCA_E2E_REAL_CLAUDE_RETRY'] ?? '2');

describe(`Layer B: cron Trigger agent loop (${REAL_AGENT.harness})`, () => {
  let cfg: OrcaClientConfig;
  let workspaceId: string;
  let environmentId: string;
  let pool: Pool;
  const triggerIds: string[] = [];
  const agentIds: string[] = [];

  beforeAll(async () => {
    requireRealAgentKey();

    const seeded = await seedWorkspaceApiKey();
    workspaceId = seeded.workspaceId;
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    pool = new Pool({
      connectionString:
        process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
      max: 1,
    });
    environmentId = await createTestEnvironment(cfg, 'trigger-agent-loop-env');
  });

  afterEach(async () => {
    for (const triggerId of triggerIds.splice(0)) {
      await cleanupTrigger(triggerId).catch((error) => {
        console.warn(`trigger-agent-loop: cleanup of Trigger ${triggerId} failed`, error);
      });
    }
    for (const agentId of agentIds.splice(0)) {
      await apiCall(cfg, `/v1/agents/${agentId}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
  });

  afterAll(async () => {
    if (cfg && environmentId) {
      await deleteTestEnvironment(cfg, environmentId).catch(() => {});
    }
    await pool?.end().catch(() => {});
  });

  it(
    'creates one Session per fire and delivers the payload through the selected Harness',
    async () => {
      const marker = `TRIGGER_E2E_OK_${randomUUID().replaceAll('-', '')}`;
      const triggerName = `trigger-agent-loop-${Date.now()}`;
      const payload = `Reply with exactly ${marker} and no other text.`;
      // Keep the next natural occurrence well outside this real-agent test.
      // The first occurrence is accelerated below, so the test still exercises
      // the live planner without creating extra Sessions while Claude runs.
      const schedule = '0 0 1 1 *';

      const agentResult = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `${triggerName}-agent`,
          model: REAL_AGENT.model,
          system: `Reply with exactly ${marker} and no other text.`,
          tools: [],
          mcp_servers: [],
          skills: [],
          metadata: REAL_AGENT.metadata,
        }),
      });
      expect(agentResult.status, agentResult.text).toBe(200);
      const agent = agentResult.json<AgentResponse>();
      agentIds.push(agent.id);

      const createResult = await apiCall(cfg, '/v1/triggers', {
        method: 'POST',
        body: JSON.stringify({
          name: triggerName,
          agent: { type: 'agent', id: agent.id, version: agent.version },
          session_mode: 'SESSION_PER_EVENT',
          source: {
            type: 'cron',
            schedule,
            timezone: 'Etc/UTC',
            payload,
          },
          session: {
            environment_id: environmentId,
            title_template: '${trigger.name}: ${payload}',
            metadata: { suite: 'trigger-agent-loop' },
            vault_ids: [],
          },
          replicas: 1,
        }),
      });
      expect(createResult.status, createResult.text).toBe(200);
      const trigger = createResult.json<TriggerResponse>();
      triggerIds.push(trigger.id);
      expect(trigger).toMatchObject({
        name: triggerName,
        session_mode: 'SESSION_PER_EVENT',
        source: { type: 'cron', schedule, timezone: 'Etc/UTC', payload },
        session: {
          environment_id: environmentId,
          title_template: '${trigger.name}: ${payload}',
          metadata: { suite: 'trigger-agent-loop' },
          vault_ids: [],
        },
        replicas: 1,
        status: 'active',
      });

      // Avoid a wall-clock minute wait while still exercising the live
      // Registry timers. Database time keeps this deterministic on CI hosts.
      const accelerated = await pool.query(
        `UPDATE agent_triggers
         SET next_fire_at = now(), updated_at = now()
         WHERE workspace_id = $1 AND id = $2 AND status = 'active'
         RETURNING id`,
        [workspaceId, trigger.id],
      );
      expect(accelerated.rowCount).toBe(1);

      const session = await waitForTriggerSession(trigger.id, 30_000);
      expect(session.title).toBe(`${triggerName}: ${payload}`);
      expect(['running', 'idle']).toContain(session.status);

      const events = await waitForAgentReply(session.id, marker, 120_000);
      expect(eventText(events, 'user.message')).toContain(marker);
      expect(eventText(events, 'agent.message')).toContain(marker);

      const history = await apiCall(cfg, `/v1/triggers/${trigger.id}/sessions?limit=1`, {
        method: 'GET',
      });
      expect(history.status, history.text).toBe(200);
      expect(history.json<{ data: SessionResponse[]; next_page: string | null }>()).toMatchObject({
        data: [{ id: session.id }],
        next_page: null,
      });
    },
    { timeout: 180_000, retry: REAL_CLAUDE_RETRY },
  );

  async function waitForTriggerSession(
    triggerId: string,
    timeoutMs: number,
  ): Promise<SessionResponse> {
    const deadline = Date.now() + timeoutMs;
    let lastBody = '';
    while (Date.now() < deadline) {
      const response = await apiCall(cfg, `/v1/triggers/${triggerId}/sessions?limit=1`, {
        method: 'GET',
      });
      lastBody = response.text;
      if (response.status !== 200) {
        throw new Error(`Trigger Session history failed: ${response.status} ${response.text}`);
      }
      const [session] = response.json<{ data: SessionResponse[] }>().data;
      if (session) return session;
      await delay(500);
    }
    throw new Error(
      `Trigger ${triggerId} did not create a Session within ${timeoutMs}ms; last response: ${lastBody}`,
    );
  }

  async function waitForAgentReply(
    sessionId: string,
    marker: string,
    timeoutMs: number,
  ): Promise<SessionEvent[]> {
    const deadline = Date.now() + timeoutMs;
    let lastEvents: SessionEvent[] = [];
    while (Date.now() < deadline) {
      const response = await apiCall(cfg, `/v1/sessions/${sessionId}/events?limit=1000`, {
        method: 'GET',
      });
      if (response.status !== 200) {
        throw new Error(`Session event listing failed: ${response.status} ${response.text}`);
      }
      lastEvents = response.json<{ data: SessionEvent[] }>().data;
      const failure = lastEvents.find((event) => FAILURE_EVENT_TYPES.has(event.type));
      if (failure) {
        throw new Error(`Trigger Session ${sessionId} failed: ${JSON.stringify(failure)}`);
      }
      if (
        eventText(lastEvents, 'user.message').includes(marker) &&
        eventText(lastEvents, 'agent.message').includes(marker)
      ) {
        return lastEvents;
      }
      await delay(500);
    }
    throw new Error(
      `Trigger Session ${sessionId} did not produce marker ${marker} within ${timeoutMs}ms; ` +
        `saw event types: ${lastEvents.map((event) => event.type).join(', ')}`,
    );
  }

  async function cleanupTrigger(triggerId: string): Promise<void> {
    await apiCall(cfg, `/v1/triggers/${triggerId}`, {
      method: 'DELETE',
      body: JSON.stringify({}),
    }).catch(() => {});

    const linkedSessions = await pool.query<{ session_id: string | null }>(
      `SELECT session_id
       FROM agent_trigger_fires
       WHERE workspace_id = $1 AND trigger_id = $2 AND session_id IS NOT NULL`,
      [workspaceId, triggerId],
    );

    // Remove the ledger before Sessions. The final schema intentionally has
    // no fire-to-Session FK so public Session deletion remains independent,
    // but this ordering also cleans old local dev volumes created by an early
    // Trigger migration draft that did include that FK.
    await pool.query(
      'DELETE FROM agent_trigger_fires WHERE workspace_id = $1 AND trigger_id = $2',
      [workspaceId, triggerId],
    );
    for (const row of linkedSessions.rows) {
      if (!row.session_id) continue;
      await apiCall(cfg, `/v1/sessions/${row.session_id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }

    // Trigger delete is intentionally archival in the public API. Remove the
    // test-owned Trigger row so repeated local runs do not accumulate fixtures.
    await pool.query('DELETE FROM agent_triggers WHERE workspace_id = $1 AND id = $2', [
      workspaceId,
      triggerId,
    ]);
  }
});

function eventText(events: SessionEvent[], type: string): string {
  return events
    .filter((event) => event.type === type)
    .flatMap((event) => (Array.isArray(event.content) ? event.content : []))
    .filter((block) => block.type === 'text' && typeof block.text === 'string')
    .map((block) => block.text!)
    .join('\n');
}

async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
