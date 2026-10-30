// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { expect, it } from 'vitest';
import { apiCall, buildClientFromConfig, ensureStackReachable } from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';
import { downloadFileText, waitForScopedOutputFile } from './output-file-helpers.js';
import { COLOCATED_AGENT, REAL_AGENT, requireRealAgentKey } from './real-agent-config.js';
import { collectSseFrames, type SseFrame } from './session-observation.js';

/** Uses the same live Registry, sandbox image, Gateway and output helpers as B.1. */
it.runIf(REAL_AGENT.harness === 'dsh_sdk')(
  'runs DSH colocated tools, retained context and child inspection through the Gateway',
  async () => {
    expect(REAL_AGENT.harness, 'Select ORCA_E2E_AGENT_HARNESS=dsh_sdk').toBe('dsh_sdk');
    expect(process.env['ORCA_E2E_SANDBOX_HARNESS']).toBe('1');
    requireRealAgentKey();
    const apiKey = process.env['ORCA_API_KEY'] ?? (await seedWorkspaceApiKey()).apiKey;
    const cfg = buildClientFromConfig({ apiKey });
    await ensureStackReachable(cfg);
    const environmentId = await createTestEnvironment(cfg, 'dsh-colocated-smoke');
    let agentId: string | undefined;
    let sessionId: string | undefined;
    let fileId: string | undefined;
    const marker = `dsh-${randomUUID()}`;
    const filename = `${marker}.txt`;
    const textOf = (frames: SseFrame[]): string =>
      frames
        .filter((frame) => frame.type === 'agent.message')
        .flatMap((frame) => (Array.isArray(frame.content) ? frame.content : []))
        .filter(
          (block): block is { type: string; text: string } =>
            block?.type === 'text' && typeof block.text === 'string',
        )
        .map((block) => block.text)
        .join(String.fromCharCode(10));
    try {
      const agent = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `DSH smoke ${marker}`,
          model: COLOCATED_AGENT.model,
          metadata: COLOCATED_AGENT.metadata,
          system:
            'Follow the user instructions exactly. Use the bash tool when asked to create a file. Do not claim a tool succeeded unless its result confirms it.',
          tools: [
            {
              type: 'agent_toolset_20260401',
              default_config: { enabled: false },
              configs: [
                { name: 'bash', enabled: true, permission_policy: { type: 'always_allow' } },
              ],
            },
          ],
        }),
      });
      expect(agent.status, agent.text).toBe(200);
      agentId = agent.json<{ id: string }>().id;
      const session = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
      });
      expect(session.status, session.text).toBe(200);
      sessionId = session.json<{ id: string }>().id;
      const prompts = [
        `Remember the codeword ${marker}. Use bash to run exactly: printf '%s' '${marker}' > /mnt/session/outputs/${filename}. After the tool succeeds, reply FILE_SAVED.`,
        'Without using any tools, reply with only the codeword I gave you in the previous turn.',
        `Use spawn to create one child with task: Reply CHILD_${marker}. Wait for the child to complete, then call list_child_sessions and read_child_session on that child's agent_id. Completed children detach their history: verify the read reports unavailable with no records. Do not substitute messaging for reading or claim to have read the child's reply. Finally reply INSPECTED_UNAVAILABLE.`,
      ];
      for (const [index, text] of prompts.entries()) {
        const submitted = await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
          method: 'POST',
          body: JSON.stringify({
            request_id: `dsh-smoke-${marker}-${index}`,
            events: [{ type: 'user.message', content: [{ type: 'text', text }] }],
          }),
        });
        expect(submitted.status, submitted.text).toBe(200);
        const frames = await collectSseFrames(cfg, sessionId, {
          deadlineMs: 420_000,
          until: (items) =>
            items.some((item) => item.type === 'session.error') ||
            items.filter(
              (item) =>
                item.type === 'session.status_idle' &&
                (item.stop_reason as { type?: string } | undefined)?.type === 'end_turn',
            ).length >=
              index + 1,
        });
        expect(frames.filter((frame) => frame.type === 'session.error')).toEqual([]);
        expect(
          frames.filter(
            (frame) =>
              frame.type === 'session.status_idle' &&
              (frame.stop_reason as { type?: string } | undefined)?.type === 'end_turn',
          ),
        ).toHaveLength(index + 1);
        if (index === 0) {
          expect(
            frames.some(
              (frame) => frame.type === 'agent.tool_use' || frame.type === 'agent.mcp_tool_use',
            ),
          ).toBe(true);
          expect(
            frames.some(
              (frame) =>
                frame.type === 'agent.tool_result' || frame.type === 'agent.mcp_tool_result',
            ),
          ).toBe(true);
          const file = await waitForScopedOutputFile(cfg, sessionId, filename, 60_000);
          fileId = file.id;
          expect(await downloadFileText(cfg, file.id)).toBe(marker);
        } else if (index === 1) {
          // Check the last reply, not an earlier assistant echo of the codeword.
          const replies = frames.filter((frame) => frame.type === 'agent.message');
          expect(textOf(replies.slice(-1)).trim()).toBe(marker);
        } else {
          const resultText = (frame: SseFrame): string => {
            expect(Array.isArray(frame.content)).toBe(true);
            return (frame.content as Array<{ type: string; text?: string }>)
              .filter((block) => block.type === 'text')
              .map((block) => block.text ?? '')
              .join('');
          };
          const spawns = frames.filter(
            (frame) => frame.type === 'agent.tool_use' && frame.name === 'spawn',
          );
          expect(spawns).toHaveLength(1);
          const spawned = frames.filter(
            (frame) =>
              frame.type === 'agent.tool_result' &&
              frame.tool_use_id === (spawns[0]!.id ?? spawns[0]!.event_id),
          );
          expect(spawned).toHaveLength(1);
          expect(spawned[0]!.is_error).not.toBe(true);
          // Pinned DSH delegation renders its native ID in this text result.
          const match = /^started subagent ([a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12})$/.exec(
            resultText(spawned[0]!),
          );
          expect(match).not.toBeNull();
          const childId = match![1]!;
          for (const name of ['list_child_sessions', 'read_child_session']) {
            const uses = frames.filter(
              (frame) => frame.type === 'agent.tool_use' && frame.name === name,
            );
            expect(uses.length).toBeGreaterThan(0);
            for (const use of uses) {
              const results = frames.filter(
                (frame) =>
                  frame.type === 'agent.tool_result' &&
                  frame.tool_use_id === (use.id ?? use.event_id),
              );
              expect(results).toHaveLength(1);
            }
          }
          const readUses = frames.filter(
            (frame) => frame.type === 'agent.tool_use' && frame.name === 'read_child_session',
          );
          const reads = frames.filter(
            (frame) =>
              frame.type === 'agent.tool_result' &&
              readUses.some((use) => (use.id ?? use.event_id) === frame.tool_use_id) &&
              frame.is_error !== true,
          );
          const decoded = reads.map((frame) => {
            const use = readUses.find(
              (candidate) => (candidate.id ?? candidate.event_id) === frame.tool_use_id,
            )!;
            const page = JSON.parse(resultText(frame));
            expect(page.child.agent_id).toBe((use.input as { agent_id: string }).agent_id);
            return { use, page };
          });
          const inspected = decoded.find(
            ({ page }) =>
              page.child.agent_id === childId && page.child.history_availability === 'unavailable',
          );
          expect(inspected).toBeDefined();
          expect(inspected!.page).toMatchObject({
            child: { agent_id: childId, state: 'dormant', history_reason: 'source_not_attached' },
            history_source: null,
            records: [],
            continuation: null,
          });
          const readIndex = frames.indexOf(inspected!.use);
          const completedIndex = frames.findIndex(
            (frame) =>
              frame.type === 'session.thread_status_idle' &&
              frame.native_session_id === childId &&
              frame.stop_reason === 'completed',
          );
          expect(completedIndex).toBeGreaterThanOrEqual(0);
          expect(completedIndex).toBeLessThan(readIndex);
          const listUses = frames.filter(
            (frame) =>
              frame.type === 'agent.tool_use' &&
              frame.name === 'list_child_sessions' &&
              frames.indexOf(frame) > completedIndex &&
              frames.indexOf(frame) < readIndex,
          );
          const lists = frames.filter(
            (frame) =>
              frame.type === 'agent.tool_result' &&
              frame.is_error !== true &&
              frames.indexOf(frame) < readIndex &&
              listUses.some((use) => (use.id ?? use.event_id) === frame.tool_use_id),
          );
          expect(
            lists.some((frame) =>
              JSON.parse(resultText(frame)).children.some(
                (child: { agent_id: string }) => child.agent_id === childId,
              ),
            ),
          ).toBe(true);
          const replies = frames.filter((frame) => frame.type === 'agent.message');
          expect(textOf(replies.slice(-1)).trim()).toBe('INSPECTED_UNAVAILABLE');
        }
      }
      const assigned = await apiCall(cfg, `/v1/sessions/${sessionId}`, {
        method: 'GET',
        headers: { 'orca-beta': '1' },
      });
      expect(assigned.status, assigned.text).toBe(200);
      const execution = assigned.json<{ sandbox_handle_id?: string }>();
      expect(execution.sandbox_handle_id).toEqual(expect.any(String));
      await expect
        .poll(
          async () => {
            const response = await apiCall(cfg, `/v1/sessions/${sessionId}`, { method: 'GET' });
            expect(response.status, response.text).toBe(200);
            const usage = response.json<{
              usage?: { input_tokens?: number; output_tokens?: number };
            }>().usage;
            return (usage?.input_tokens ?? 0) > 0 && (usage?.output_tokens ?? 0) > 0;
          },
          { timeout: 60_000 },
        )
        .toBe(true);
    } finally {
      // Clean up only resources created by this smoke.
      for (const [kind, id] of [
        ['sessions', sessionId],
        ['agents', agentId],
        ['files', fileId],
      ] as const) {
        if (!id) continue;
        const deleted = await apiCall(cfg, `/v1/${kind}/${id}`, { method: 'DELETE', body: '{}' });
        expect([200, 204, 404], deleted.text).toContain(deleted.status);
      }
      await deleteTestEnvironment(cfg, environmentId);
    }
  },
  1_440_000,
);
