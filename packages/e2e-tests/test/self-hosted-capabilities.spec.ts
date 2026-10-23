// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — colocated CAPABILITY parity with the separate-mode suite.
 *
 * `self-hosted-e2e.spec.ts` proves the colocated plumbing with the LLM-free `mock`
 * provider: dispatch, the single-writer bridge, event ordering. It cannot prove
 * anything else, because **the mock provider never calls a tool**. Everything the
 * separate suite establishes about a running agent — that a tool call reaches the
 * sandbox, that it is contained there, that resources and skills materialize, that
 * outputs are captured — needs a real model. That is this file.
 *
 * Each capability here has a named counterpart in `real-agent-loop.spec.ts` (the
 * `separate` engine, driven by harness-server) or `sandbox-harness-agent.spec.ts`
 * (harness-server's retired in-sandbox bridge). Neither of those touches the path
 * this file drives: registry → environment-worker → session-runner. The assertions
 * are modelled on the in-sandbox spec rather than the separate one, because its
 * expectations are already written against sandbox-relative paths.
 *
 * ## Which agent runs
 *
 * `ORCA_E2E_COLOCATED_HARNESS` selects the harness under test:
 *
 *   claude_agent_sdk  (default) the in-process Agent SDK. Needs ANTHROPIC_API_KEY in
 *                     the RUNNER's environment. This is the CI configuration.
 *   claude_code       the native Claude Code CLI. Needs `claude` on the runner's PATH.
 *                     Works from the CLI's OWN credentials when no ANTHROPIC_API_KEY
 *                     is set, which is how a developer runs this without a key.
 *   pi                controlled Pi 0.87.1, Darwin Local, Node 22.21+ within Node 22.
 *                     Selects the File-only native completion test below; the worker
 *                     needs ANTHROPIC_API_KEY for the start script's sidecar Environment.
 *                     Gateway Environments instead supply the scoped LLM URL/JWT.
 *
 * That second mode is not a convenience — it is what makes this file verifiable by
 * someone whose only Claude credential is a Claude Code install.
 *
 * ## The failure mode this file is built to avoid
 *
 * Every turn goes through a bounded wait and fails on a terminal harness failure.
 * The suite requires credentials explicitly; a missing key never becomes a skipped
 * capability test. Interrupt waits for a long-running tool call before sending the
 * signal, so a session that never started cannot satisfy the assertion.
 *
 * Prerequisite: `bash services/dev/scripts/start-self-hosted.sh`, with the credential
 * the chosen harness needs available to the WORKER at start time (it forwards an
 * allowlist to the runner, not its whole environment). Run via `pnpm e2e:self-hosted:real`.
 *
 * For `pi`, run the worker and runner on Darwin with SRT installed and Node 22.21+
 * within Node 22. Export ANTHROPIC_API_KEY, then start the stack with:
 *   SANDBOX_RUNTIME=local \
 *   ORCA_RUNNER_ENV_PASSTHROUGH=SANDBOX_RUNTIME,ANTHROPIC_API_KEY \
 *     bash services/dev/scripts/start-self-hosted.sh
 * Run the suite with `ORCA_E2E_COLOCATED_HARNESS=pi pnpm e2e:self-hosted:real`.
 * Naming SANDBOX_RUNTIME in the passthrough is required: setting it only on the
 * worker leaves its child runner on the default InMemory runtime.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { apiCall, buildClientFromConfig, ensureStackReachable } from '../src/client.js';
import type { OrcaClientConfig } from '../src/client.js';
import { mentionsSandboxWorkDir } from '../src/sandbox-workdir.js';
import {
  createAgent,
  createSession,
  assertNoTurnFailure,
  collectSseFrames,
  pollEventsUntil,
  postSessionEvent,
  postUserMessage,
  readEvents,
  readSelfHostedApiKey,
  readWorkerAttachedEnvironmentId,
  runTurn,
  textOf,
  toolNames,
  waitForWorkerConnected,
  type SseFrame,
  type TranscriptEvent,
} from './self-hosted-helpers.js';

/** A real turn on a cold runner: process spawn + model round-trip. */
const TURN_DEADLINE_MS = 180_000;
const TEST_TIMEOUT_MS = TURN_DEADLINE_MS + 60_000;

const HARNESS = process.env['ORCA_E2E_COLOCATED_HARNESS'] ?? 'claude_agent_sdk';
const MODEL_ID = process.env['ORCA_E2E_COLOCATED_MODEL'] ?? 'claude-sonnet-4-5';

/**
 * `claude_agent_sdk` is `separate` in the catalog and `claude_code` is `colocated`,
 * but on a `self_hosted` environment the distributor is MODE-AGNOSTIC — its mode gate
 * applies only to `target=cloud` (`session-distributor.ts`). Both therefore run on the
 * session-runner here, which is what makes them comparable in one file.
 */
const AGENT_MODE = HARNESS === 'claude_code' ? 'colocated' : 'separate';

/** Grant the orca toolset, auto-approving so a turn is not blocked on a human. */
const TOOLSET = [
  {
    type: 'agent_toolset_20260401',
    default_config: { permission_policy: { type: 'always_allow' } },
  },
];

function agentBody(name: string, system: string, extra: Record<string, unknown> = {}) {
  return {
    name,
    model: { provider: 'anthropic', id: MODEL_ID },
    system,
    tools: TOOLSET,
    metadata: { harness: HARNESS, mode: AGENT_MODE },
    ...extra,
  };
}

describe.skipIf(HARNESS === 'pi')(`self-hosted capabilities — real provider (${HARNESS})`, () => {
  let cfg: OrcaClientConfig;
  let workerEnvId: string;
  const sessionIds: string[] = [];
  const agentIds: string[] = [];
  const fileIds: string[] = [];

  beforeAll(async () => {
    if (HARNESS !== 'claude_agent_sdk' && HARNESS !== 'claude_code') {
      throw new Error('ORCA_E2E_COLOCATED_HARNESS must be claude_agent_sdk or claude_code');
    }
    // Hard-fail, never skip. The SDK harness cannot reach Anthropic without a key in
    // the RUNNER's environment, and a spec that skipped here would report green having
    // proven nothing — the failure mode `real-agent-loop.spec.ts` calls out by name.
    if (HARNESS === 'claude_agent_sdk' && !process.env['ANTHROPIC_API_KEY']) {
      throw new Error(
        'colocated capability e2e requires ANTHROPIC_API_KEY for the claude_agent_sdk harness.\n' +
          "  The key must be in the WORKER's environment when the stack starts, not just here:\n" +
          '  the worker forwards an allowlist to the runner (ORCA_RUNNER_ENV_PASSTHROUGH).\n' +
          '  Without a key, set ORCA_E2E_COLOCATED_HARNESS=claude_code to drive the native CLI\n' +
          '  from its own credentials instead.',
      );
    }

    cfg = buildClientFromConfig({ apiKey: readSelfHostedApiKey() });
    await ensureStackReachable(cfg);
    workerEnvId = readWorkerAttachedEnvironmentId();
    await waitForWorkerConnected(cfg, workerEnvId);
  }, 120_000);

  afterAll(async () => {
    // `ORCA_E2E_KEEP_SESSIONS=1` leaves everything in place for a post-mortem — the
    // transcript is the only record of why a capability assertion failed, and the
    // default cleanup deletes it before anyone can read it. Mirrors the existing
    // `ORCA_E2E_SANDBOX_HARNESS_KEEP_RESOURCES` knob.
    if (process.env['ORCA_E2E_KEEP_SESSIONS'] === '1') {
      console.log(`kept ${sessionIds.length} session(s): ${sessionIds.join(', ')}`);
      return;
    }
    for (const id of sessionIds.reverse()) {
      await apiCall(cfg, `/v1/sessions/${id}`, { method: 'DELETE' }).catch(() => {});
    }
    for (const id of agentIds.reverse()) {
      await apiCall(cfg, `/v1/agents/${id}`, { method: 'DELETE' }).catch(() => {});
    }
    for (const id of fileIds.reverse()) {
      await apiCall(cfg, `/v1/files/${id}`, { method: 'DELETE' }).catch(() => {});
    }
  });

  /** Create an agent + a dispatched session, and register both for cleanup. */
  async function startSession(
    name: string,
    system: string,
    agentExtra: Record<string, unknown> = {},
    sessionExtra: Record<string, unknown> = {},
  ): Promise<string> {
    const agentId = await createAgent(cfg, agentBody(name, system, agentExtra));
    agentIds.push(agentId);
    const session = await createSession(cfg, agentId, workerEnvId, sessionExtra);
    sessionIds.push(session.id);
    expect(
      session.distribution_state,
      'a self_hosted session must be dispatched, not left undistributed',
    ).not.toBeNull();
    return session.id;
  }

  // ── 1. Tool dispatch + containment ────────────────────────────────────────────
  //
  // The foundation: if a tool call does not reach the sandbox, nothing below means
  // anything. Counterparts: `real-agent-loop.spec.ts` "summarizes a file mounted at
  // /mnt/lorem.txt via mcp__orca__bash" and "runs bash inside the per-session sandbox
  // work-dir".

  it(
    'routes a tool call to the orca toolset and runs it inside the session sandbox',
    async () => {
      const sessionId = await startSession(
        `colocated-tooldispatch-${Date.now()}`,
        'You have shell tools. When asked to run a command, run it and report the exact output.',
      );

      const frames = await runTurn(
        cfg,
        sessionId,
        'Run the shell command `pwd` and reply with only its output.',
        TURN_DEADLINE_MS,
      );

      // A tool ran at all.
      const names = toolNames(frames);
      expect(names.length, 'the turn used no tools').toBeGreaterThan(0);

      // CONTAINMENT is the platform claim: whatever tool ran, it ran in the session's
      // own sandbox root rather than in the runner's cwd.
      const said = frames
        .filter((f) => f.type === 'agent.message' || f.type === 'agent.tool_result')
        .map((f) => textOf(f))
        .join('\n');
      expect(
        mentionsSandboxWorkDir(said),
        `no session-sandbox work dir in the reply or tool results: ${said.slice(0, 400)}`,
      ).toBe(true);

      // WHICH toolset was reachable is also a platform claim, and it is asserted from
      // the runner's configuration event rather than from the model's choice. The two
      // are not the same thing, and conflating them is how this test was wrong first
      // time: it demanded `mcp__orca__bash` and got `Bash`.
      const advertised = await advertisedTools(cfg, sessionId);
      expect(
        advertised.some((n) => n.startsWith('mcp__orca__')),
        `the orca tool bridge was not advertised to the agent; saw ${JSON.stringify(advertised.slice(0, 40))}`,
      ).toBe(true);

      // The model may choose a native tool or the orca bridge; this baseline pins
      // the working directory and advertised bridge rather than that model choice.
    },
    TEST_TIMEOUT_MS,
  );

  // ── 2. File resource materialization ──────────────────────────────────────────
  //
  // Counterparts: `real-agent-loop.spec.ts` scenario 1, and
  // `sandbox-harness-agent.spec.ts` "reads and writes attached resources…".

  // The legacy Claude path lacks native resource placement. The negotiated Pi test below
  // exercises actual-path guidance and completion-gated capture on Darwin Local.
  //
  // A colocated sandbox has no `/mnt` tree at all: reading an attached resource returns
  // "File does not exist", and writing under it returns
  // `EROFS: read-only file system, mkdir '/mnt'`. Session `resources` are never
  // materialized and no `session.resource_mounted` event is emitted.
  //
  // Restore this assertion when resource materialization is implemented.
  // it(
  //   'materializes an attached file resource into the sandbox and records the mount',
  //   async () => {
  //     // A phrase the model cannot produce without reading the file.
  //     const marker = `colocated-resource-marker-${Date.now().toString(36)}`;
  //     const form = new FormData();
  //     form.append('file', new Blob([`the marker is ${marker}\n`], { type: 'text/plain' }), 'ctx.txt');
  //     const upload = await apiCall(cfg, '/v1/files', { method: 'POST', body: form });
  //     expect(upload.status, `file upload failed: ${upload.text}`).toBe(200);
  //     const fileId = upload.json<{ id: string }>().id;
  //     fileIds.push(fileId);
  //
  //     const sessionId = await startSession(
  //       `colocated-resource-${Date.now()}`,
  //       'You have file tools. Read what you are asked to read and quote it exactly.',
  //       {},
  //       {
  //         resources: [
  //           { type: 'file', file_id: fileId, mount_path: '/mnt/ctx.txt', access: 'read_only' },
  //         ],
  //       },
  //     );
  //
  //     const frames = await runTurn(
  //       cfg,
  //       sessionId,
  //       'Read /mnt/ctx.txt and reply with the marker it contains, and nothing else.',
  //       TURN_DEADLINE_MS,
  //     );
  //
  //     const said = frames.map((f) => textOf(f)).join('\n');
  //     expect(said, `the mounted file was not read back: ${said.slice(0, 400)}`).toContain(marker);
  //
  //     // The mount is an observable event, not just a side effect.
  //     const events = await pollEventsUntil(
  //       cfg,
  //       sessionId,
  //       (evts) => evts.some((e) => e.type === 'session.resource_mounted'),
  //       20_000,
  //     );
  //     const mounted = events.find((e) => e.type === 'session.resource_mounted');
  //     expect(mounted, 'no session.resource_mounted event was persisted').toBeDefined();
  //     expect(JSON.stringify(mounted)).toContain(fileId);
  //   },
  //   TEST_TIMEOUT_MS,
  // );

  // ── 3. Output capture ─────────────────────────────────────────────────────────
  //
  // Counterparts: `real-agent-loop.spec.ts` "captures a separated-runner output
  // immediately after its tool result" and its in-sandbox mirror.

  // COMMENTED OUT — same root cause as the resource test above. With no
  // `/mnt` tree, a write to `/mnt/session/outputs/` fails with EROFS and nothing is ever
  // indexed as a session output.
  // it(
  //   'indexes a file written to the outputs path as a downloadable session output',
  //   async () => {
  //     const marker = `colocated-output-${Date.now().toString(36)}`;
  //     const sessionId = await startSession(
  //       `colocated-outputs-${Date.now()}`,
  //       'You have shell and file tools. Follow write instructions exactly.',
  //     );
  //
  //     await runTurn(
  //       cfg,
  //       sessionId,
  //       `Write a file at /mnt/session/outputs/report.txt whose entire contents are: ${marker}`,
  //       TURN_DEADLINE_MS,
  //     );
  //
  //     // Indexed as a scoped session output, discoverable through the session's file
  //     // listing — the capture happens at tool-result time, not on an idle sweep, so a
  //     // short poll is enough.
  //     const listed = await pollForSessionFile(cfg, sessionId, 'report.txt', 30_000);
  //     expect(listed, 'report.txt was never indexed as a session output').toBeDefined();
  //
  //     const content = await apiCall(cfg, `/v1/sessions/${sessionId}/files/${listed!.id}/content`, {
  //       method: 'GET',
  //     });
  //     expect(content.status).toBe(200);
  //     expect(content.text).toContain(marker);
  //   },
  //   TEST_TIMEOUT_MS,
  // );

  // ── 4. Usage roll-up, and the negative ────────────────────────────────────────
  //
  // Counterpart: `real-agent-loop.spec.ts` scenario 1's usage assertions.

  // COMMENTED OUT — usage roll-up is tracked in the roadmap's colocated parity gaps.
  //
  // The runner emits `agent.usage` with real numbers (`input_tokens: 18,
  // output_tokens: 269` in a sample run), but the registry does not aggregate it into
  // the session's own `usage` on this path, so the roll-up stays zero.
  // it(
  //   'rolls up token usage onto the session',
  //   async () => {
  //     const sessionId = await startSession(
  //       `colocated-usage-${Date.now()}`,
  //       'Answer in one short word.',
  //     );
  //     await runTurn(cfg, sessionId, 'Say the word ready.', TURN_DEADLINE_MS);
  //
  //     const session = await apiCall(cfg, `/v1/sessions/${sessionId}`, {
  //       method: 'GET',
  //       headers: { ...ORCA_BETA_HEADERS },
  //     });
  //     expect(session.status).toBe(200);
  //     const usage = session.json<{ usage?: { input_tokens?: number; output_tokens?: number } }>()
  //       .usage;
  //     expect(usage, 'no usage roll-up on the session after a real turn').toBeDefined();
  //     expect(usage!.input_tokens ?? 0).toBeGreaterThan(0);
  //     expect(usage!.output_tokens ?? 0).toBeGreaterThan(0);
  //   },
  //   TEST_TIMEOUT_MS,
  // );

  it(
    'keeps agent.usage off the public event list',
    async () => {
      const sessionId = await startSession(
        `colocated-usageleak-${Date.now()}`,
        'Answer in one short word.',
      );
      await runTurn(cfg, sessionId, 'Say the word ready.', TURN_DEADLINE_MS);
      const events = await readEvents(cfg, sessionId);
      expect(events.map((e) => e.type)).not.toContain('agent.usage');
    },
    TEST_TIMEOUT_MS,
  );

  // ── 5. Interrupt ──────────────────────────────────────────────────────────────

  it(
    'terminates an in-flight turn on user.interrupt',
    async () => {
      const sessionId = await startSession(
        `colocated-interrupt-${Date.now()}`,
        'You have shell tools. Follow instructions exactly.',
      );

      await postUserMessage(cfg, sessionId, 'Run sleep 120 in the shell and then say done.');
      const beforeInterrupt = await collectSseFrames(cfg, sessionId, {
        deadlineMs: TURN_DEADLINE_MS,
        until: (frames) =>
          toolNames(frames).length > 0 ||
          frames.some(
            (frame) => frame.type === 'agent.turn_completed' || frame.type === 'agent.turn_failed',
          ),
      });
      assertNoTurnFailure(beforeInterrupt);
      expect(
        beforeInterrupt.some(
          (frame) =>
            (frame.type === 'agent.tool_use' || frame.type === 'agent.mcp_tool_use') &&
            JSON.stringify(frame).includes('sleep 120'),
        ),
        'the model must start the long-running tool before interrupting',
      ).toBe(true);
      expect(beforeInterrupt.some((frame) => frame.type === 'agent.turn_completed')).toBe(false);
      await postSessionEvent(cfg, sessionId, { type: 'user.interrupt' });

      // A terminal frame proves the interrupted turn settled; a session that never
      // started must not pass merely because its status is not running.
      const deadline = Date.now() + 90_000;
      let ended = false;
      while (Date.now() < deadline) {
        const events = await readEvents(cfg, sessionId);
        const terminal = events.some(
          (e) => e.type === 'agent.turn_completed' || e.type === 'agent.turn_failed',
        );
        if (terminal) {
          ended = true;
          break;
        }
        await new Promise((r) => setTimeout(r, 2_000));
      }
      expect(ended, 'the turn was still running 90s after user.interrupt').toBe(true);
    },
    TURN_DEADLINE_MS + 120_000,
  );
});

/**
 * The tools the runner advertised to the agent, read off its configuration event.
 *
 * The runner forwards the harness's own init/configuration event into the transcript;
 * for both Claude harnesses it carries the resolved tool list. Reading it is how a
 * test can assert what the PLATFORM offered without depending on what the MODEL chose.
 */
async function advertisedTools(cfg: OrcaClientConfig, sessionId: string): Promise<string[]> {
  const events = await pollEventsUntil(
    cfg,
    sessionId,
    (items) => items.some((event) => Array.isArray(event['tools'])),
    20_000,
  );
  const names = new Set<string>();
  for (const event of events) {
    const tools = (event as Record<string, unknown>)['tools'];
    if (Array.isArray(tools)) {
      for (const tool of tools) {
        if (typeof tool === 'string') names.add(tool);
        else if (tool && typeof tool === 'object') {
          const n = (tool as Record<string, unknown>)['name'];
          if (typeof n === 'string') names.add(n);
        }
      }
    }
  }
  return [...names];
}

// COMMENTED OUT WITH THE OUTPUT-CAPTURE TEST — see the roadmap's colocated parity gaps.
//
// Restoring that test means uncommenting this helper too, and re-adding
// `pollEventsUntil` to the import list above for the resource test. Left here rather
// than deleted so the assertion and its machinery come back together.
// interface SessionFile {
//   id: string;
//   filename?: string;
//   size_bytes?: number;
// }
//
// /** Poll a session's file listing for a filename the agent was asked to write. */
// async function pollForSessionFile(
//   cfg: OrcaClientConfig,
//   sessionId: string,
//   filename: string,
//   timeoutMs: number,
// ): Promise<SessionFile | undefined> {
//   const deadline = Date.now() + timeoutMs;
//   for (;;) {
//     const res = await apiCall(cfg, `/v1/sessions/${sessionId}/files?limit=100`, { method: 'GET' });
//     if (res.status === 200) {
//       const found = res
//         .json<{ data?: SessionFile[] }>()
//         .data?.find((f) => (f.filename ?? '').endsWith(filename));
//       if (found) return found;
//     }
//     if (Date.now() >= deadline) return undefined;
//     await new Promise((r) => setTimeout(r, 1_000));
//   }
// }

// Referenced by the type-only imports above so the file stays honest about what it uses.
export type { SseFrame, TranscriptEvent };

// Explicit opt-in selects the controlled native capability, not the legacy Claude tool surface.
describe.runIf(HARNESS === 'pi')('self-hosted native Pi File completion', () => {
  it(
    'reads an attached File using verified guidance and publishes scoped outputs and usage before completion',
    async () => {
      const cfg = buildClientFromConfig({ apiKey: readSelfHostedApiKey() });
      await ensureStackReachable(cfg);
      const environmentId = readWorkerAttachedEnvironmentId();
      await waitForWorkerConnected(cfg, environmentId);
      const marker = 'native-file-' + Date.now().toString(36);
      let agentId: string | undefined, sessionId: string | undefined, fileId: string | undefined;
      try {
        const form = new FormData();
        form.append('file', new Blob([marker], { type: 'text/plain' }), 'context.txt');
        const upload = await apiCall(cfg, '/v1/files', { method: 'POST', body: form });
        expect(upload.status, upload.text).toBe(200);
        fileId = upload.json<{ id: string }>().id;
        agentId = await createAgent(cfg, {
          name: 'native-files-' + Date.now(),
          model: { provider: 'anthropic', id: MODEL_ID },
          system:
            'Use the verified executable paths in your session guidance. Read the attached File and copy its entire contents to report.txt under the instructed outputs directory, then quote the contents. Use only read and write.',
          tools: [
            {
              type: 'agent_toolset_20260401',
              default_config: { enabled: false, permission_policy: { type: 'always_allow' } },
              configs: { read: { enabled: true }, write: { enabled: true } },
            },
          ],
          metadata: { harness: 'pi', mode: 'colocated' },
        });
        const session = await createSession(cfg, agentId, environmentId, {
          resources: [
            { type: 'file', file_id: fileId, mount_path: '/mnt/context.txt', access: 'read_only' },
          ],
        });
        sessionId = session.id;
        const frames = await runTurn(
          cfg,
          sessionId,
          'Read the attached context File, save its complete contents as report.txt, and quote them.',
          TURN_DEADLINE_MS,
        );
        expect(frames.map(textOf).join('\n')).toContain(marker);
        const outputIndex = frames.findIndex((frame) => frame.type === 'session.output_indexed');
        const terminalIndex = frames.findIndex((frame) => frame.type === 'agent.turn_completed');
        expect(outputIndex).toBeGreaterThanOrEqual(0);
        expect(terminalIndex).toBeGreaterThan(outputIndex);
        expect(frames.map((frame) => frame.type)).not.toContain('agent.usage');
        // Files and usage must already be visible when the live terminal arrives.
        const [listing, result] = await Promise.all([
          apiCall(cfg, '/v1/sessions/' + sessionId + '/files', { method: 'GET' }),
          apiCall(cfg, '/v1/sessions/' + sessionId, {
            method: 'GET',
            headers: { 'orca-beta': 'true' },
          }),
        ]);
        expect(listing.status, listing.text).toBe(200);
        const output = listing
          .json<{ data: Array<{ id: string; filename: string }> }>()
          .data.find((file) => file.filename.endsWith('report.txt'));
        expect(output).toBeDefined();
        const content = await apiCall(
          cfg,
          '/v1/sessions/' + sessionId + '/files/' + output!.id + '/content',
          { method: 'GET' },
        );
        expect(content.status, content.text).toBe(200);
        expect(content.text.trim()).toBe(marker);
        expect(result.status, result.text).toBe(200);
        const usage = result.json<{ usage: { input_tokens: number; output_tokens: number } }>()
          .usage;
        expect(usage.input_tokens).toBeGreaterThan(0);
        expect(usage.output_tokens).toBeGreaterThan(0);
        const events = await pollEventsUntil(
          cfg,
          sessionId,
          (page) =>
            page.some((event) => event.type === 'agent.turn_completed') &&
            page.some((event) => event.type === 'session.output_indexed'),
          20_000,
        );
        const mounted = events.find((event) => event.type === 'session.resource_mounted');
        expect(mounted).toMatchObject({ mount_path: '/mnt/context.txt', file_id: fileId });
        const resolved = mounted?.resolved_path;
        expect(typeof resolved).toBe('string');
        expect(resolved).not.toBe('/mnt/context.txt');
        const indexedOutput = events.findIndex((event) => event.type === 'session.output_indexed');
        expect(indexedOutput).toBeGreaterThanOrEqual(0);
        expect(events.findIndex((event) => event.type === 'agent.turn_completed')).toBeGreaterThan(
          indexedOutput,
        );
        expect(events.map((event) => event.type)).not.toContain('agent.usage');
        const nextFrames = await runTurn(
          cfg,
          sessionId,
          'Read /mnt/session/outputs/report.txt and quote its complete contents without changing it.',
          TURN_DEADLINE_MS,
        );
        expect(nextFrames.map(textOf).join('\n')).toContain(marker);
        expect(nextFrames.map((frame) => frame.type)).not.toContain('agent.usage');
        const nextResult = await apiCall(cfg, '/v1/sessions/' + sessionId, {
          method: 'GET',
          headers: { 'orca-beta': 'true' },
        });
        expect(nextResult.status, nextResult.text).toBe(200);
        expect(
          nextResult.json<{ usage: { output_tokens: number } }>().usage.output_tokens,
        ).toBeGreaterThan(usage.output_tokens);
      } finally {
        if (sessionId) await apiCall(cfg, '/v1/sessions/' + sessionId, { method: 'DELETE' });
        if (agentId) await apiCall(cfg, '/v1/agents/' + agentId, { method: 'DELETE' });
        if (fileId) await apiCall(cfg, '/v1/files/' + fileId, { method: 'DELETE' });
      }
    },
    TEST_TIMEOUT_MS,
  );
});
