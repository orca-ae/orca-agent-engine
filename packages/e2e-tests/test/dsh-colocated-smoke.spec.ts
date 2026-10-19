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
  'runs two DSH colocated turns through managed tools, Gateway and durable usage',
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
        } else {
          // Check the last reply, not an earlier assistant echo of the codeword.
          const replies = frames.filter((frame) => frame.type === 'agent.message');
          expect(textOf(replies.slice(-1)).trim()).toBe(marker);
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
  960_000,
);
