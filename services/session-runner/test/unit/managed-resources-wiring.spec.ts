// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { fileURLToPath } from 'node:url';
import type { CodexCheckpoint } from '@orca/codex-harness';
import { createResponsesFixture } from '../../../../packages/codex-harness/test/support/responses-fixture.js';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHash } from 'node:crypto';
import { mkdtemp, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { RunnerResources } from '../../src/resources.js';
import { SessionLoop } from '../../src/session-loop.js';
import { ProviderRegistry } from '../../src/harness/provider.js';
import { CodexSdkHarness } from '../../src/harness/codex-sdk/index.js';
import {
  createRunnerSandboxRuntime,
  resourceManifestDigest,
  type MaterializableSkillDescriptor,
  type SandboxHandle,
  type ResourceManifest,
} from '../../src/sandbox/seam.js';
import { registerSessionHandlers } from '../../src/register-handlers.js';
import { RouteDispatcher } from '../../src/tunnel/request-dispatch.js';
import {
  RUNNER_RESOURCES_PATH,
  RUNNER_RESOURCE_CHANGES_PATH,
  RUNNER_RESOURCE_ACK_PATH,
  RUNNER_SESSION_HEADER,
  RUNNER_SNAPSHOT_PATH,
  RUNNER_TURN_PATH,
} from '../../src/protocol.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';
import { FakeAgentHarness } from './support/fake-agent-harness.js';
import { scriptedNativeCliWithDeath } from './support/failed-native-cli.js';

const hash = (s: string) => createHash('sha256').update(s).digest('hex');
const session = 'ses_managed';
const revision = hash('resources');
const body = (value: unknown) => Buffer.from(JSON.stringify(value));
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.unstubAllEnvs();
});

async function setup(realProvider = false, nativeOptions?: { apiKey: string; baseUrl: string }) {
  const dir = await mkdtemp(join(tmpdir(), 'orca-runner-wiring-'));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  const runtime = createRunnerSandboxRuntime({ kind: 'in-memory' });
  const roots: SandboxHandle[] = [];
  const resources = new RunnerResources({
    workspaceDir: dir,
    acquire: async () => {
      const raw = await runtime.acquire({});
      roots.push(raw);
      return raw;
    },
    enforce: async (raw) => raw,
  }); // Controller mechanics only; kernel enforcement has its own probe.
  const rawWorkers: SandboxHandle[] = [];
  const acquire = vi.fn(async () => {
    const raw = await runtime.acquire({});
    rawWorkers.push(raw);
    return raw;
  });
  const native = scriptedNativeCliWithDeath({
    respond: (line) => {
      const frame = JSON.parse(line);
      if (frame.type === 'start') return [JSON.stringify({ type: 'ready' })];
      if (frame.type === 'submit')
        return [
          JSON.stringify({
            type: 'tool_call',
            id: 'call1',
            name: 'write',
            arguments: { path: '/mnt/session/outputs/report.txt', content: 'saved' },
          }),
        ];
      if (frame.type === 'tool_result') return [JSON.stringify({ type: 'done' })];
      return [];
    },
  });
  const harnesses: FakeAgentHarness[] = [];
  const providers = new ProviderRegistry();
  providers.register(
    'managed-test',
    (snapshot) => {
      if (nativeOptions)
        return new CodexSdkHarness({
          ...nativeOptions,
          ...(snapshot.harness_state
            ? { checkpoint: snapshot.harness_state as CodexCheckpoint }
            : {}),
          workerArgs: [
            fileURLToPath(new URL('../../dist/harness/codex-sdk/worker-entry.js', import.meta.url)),
          ],
          timeoutMs: 20000,
        });
      if (realProvider)
        return new CodexSdkHarness({
          apiKey: 'scoped-jwt',
          launch: () => native.cli,
          timeoutMs: 1000,
        });
      const harness = Object.assign(new FakeAgentHarness(), { preserveSandboxOnRefresh: true });
      harnesses.push(harness);
      return harness;
    },
    { managedResources: true },
  );
  providers.register('unsupported', () => new FakeAgentHarness());
  const loop = new SessionLoop({
    workspaceId: 'ws',
    providers,
    resources,
    skillsWorkspaceDir: dir,
    sandboxRuntime: { acquire, capabilities: runtime.capabilities },
  });
  cleanups.push(() => loop.stop());
  const dispatcher = registerSessionHandlers(new RouteDispatcher(), loop);
  const post = (path: string, value: unknown, id = session) =>
    dispatcher.dispatch({
      method: 'POST',
      path,
      headers: [[RUNNER_SESSION_HEADER, id]],
      queryString: '',
      body: body(value),
    });
  const stage = async (next = revision) => {
    const manifest: ResourceManifest = { version: 1, revision: next, resources: [] };
    expect((await post(RUNNER_RESOURCES_PATH, { type: 'manifest', manifest })).status).toBe(200);
    expect(
      (
        await post(RUNNER_RESOURCES_PATH, {
          type: 'commit',
          revision: next,
          manifest_sha256: resourceManifestDigest(manifest),
        })
      ).status,
    ).toBe(200);
  };
  const snapshot = (extra: Record<string, unknown> = {}) => ({
    provider: 'managed-test',
    model: { provider: 'openai', id: 'gpt-5.4' },
    allowed_tool_names: ['write', 'read', 'bash'],
    default_tool_permission: 'always_allow',
    managed_resources: { version: 1, revision },
    ...extra,
  });
  return {
    dir,
    resources,
    roots,
    rawWorkers,
    harnesses,
    native,
    loop,
    post,
    stage,
    snapshot,
    acquire,
  };
}

async function responseJson(res: Awaited<ReturnType<RouteDispatcher['dispatch']>>) {
  const chunks = [];
  for await (const chunk of res.body) chunks.push(Buffer.from(chunk));
  return JSON.parse(Buffer.concat(chunks).toString());
}

function skillsPush(name = 'guide') {
  const content = '# exact instructions';
  const descriptor: MaterializableSkillDescriptor = {
    id: 'sv1',
    skill_id: 'sk1',
    source: 'custom',
    version_identifier: '1',
    name,
    description: 'A managed guide',
    entrypoint: 'SKILL.md',
    package_sha256: hash('package'),
    package_size_bytes: 100,
  };
  const file = {
    path: 'SKILL.md',
    sizeBytes: Buffer.byteLength(content),
    sha256: hash(content),
    mode: 0o644,
    mimeType: 'text/markdown',
  };
  const manifest = {
    type: 'skills_manifest',
    dir: 'skills-plugin',
    skills: [name],
    descriptors: [descriptor],
    bundles: {
      [name]: {
        sha256: descriptor.package_sha256,
        sizeBytes: descriptor.package_size_bytes,
        files: [file],
      },
    },
  };
  return {
    descriptor,
    content,
    bytes: Buffer.from(
      [
        JSON.stringify(manifest),
        JSON.stringify({
          type: 'skill_file',
          skill: name,
          path: file.path,
          mode: file.mode,
          mime_type: file.mimeType,
          content_base64: Buffer.from(content).toString('base64'),
        }),
      ].join('\n'),
    ),
  };
}

describe('managed runner resource delivery', () => {
  it.each(['Pinned Agent instructions', ''])(
    'refreshes block_skills across native resume without losing history (base=%s)',
    async (system) => {
      const api = await createResponsesFixture({ sessionId: session });
      cleanups.push(api.close);
      const f = await setup(false, { apiKey: api.apiKey, baseUrl: api.url });
      await f.stage();
      const skill = skillsPush();
      await f.loop.applySkills(session, skill.bytes);
      let checkpoint: CodexCheckpoint | undefined;
      let threadId: string | undefined;
      for (const [index, blocked] of [false, true, false].entries()) {
        await f.loop.applySnapshot(
          session,
          body(
            f.snapshot({
              system,
              skills: [skill.descriptor],
              ...(checkpoint ? { harness_state: checkpoint } : {}),
              guardrails: blocked
                ? [
                    {
                      id: 'grd_block',
                      name: 'block guide',
                      tier: 'workspace',
                      phases: ['tool_call'],
                      stateful: false,
                      rule: {
                        kind: 'builtin',
                        builtin: 'block_skills',
                        params: { blocked: ['guide'] },
                      },
                    },
                  ]
                : [],
            }),
          ),
        );
        if (blocked)
          await expect(
            f.resources.toolSandbox!.files.read('/workspace/skills/guide/SKILL.md'),
          ).rejects.toThrow();
        else
          expect(
            (
              await f.resources.toolSandbox!.files.read('/workspace/skills/guide/SKILL.md')
            ).toString(),
          ).toBe(skill.content);
        const frames: Record<string, unknown>[] = [];
        for await (const chunk of f.loop.runTurn(
          session,
          body({
            id: `evt_skill_${index}`,
            type: 'user.message',
            content: `Remember managed turn ${index}`,
          }),
          new AbortController().signal,
        )) {
          frames.push(
            ...Buffer.from(chunk)
              .toString()
              .trim()
              .split('\n')
              .filter(Boolean)
              .map((line) => JSON.parse(line)),
          );
        }
        expect(
          frames.filter((frame) => frame.type === 'agent.error' || frame.type === 'session.error'),
        ).toEqual([]);
        const state = frames.find((frame) => frame.type === 'orca.harness_checkpoint');
        expect(state).toBeDefined();
        checkpoint = state!.state as CodexCheckpoint;
        if (index === 0 && system) {
          // Existing pre-fingerprint native history also supports this transition.
          const { instructionsSha256: _digest, ...legacy } = checkpoint;
          checkpoint = legacy;
        }
        threadId ??= checkpoint.threadId;
        expect(checkpoint.threadId).toBe(threadId);
        expect(api.requests).toHaveLength(index + 1);
        const request = api.requests.at(-1)!;
        const developer = (request.input as { role?: string }[]).filter(
          (item) => item.role === 'developer',
        );
        expect(JSON.stringify(developer).includes('/workspace/skills/guide/SKILL.md')).toBe(
          !blocked,
        );
        if (system) expect(JSON.stringify(developer)).toContain(system);
        expect(JSON.stringify(request.input)).toContain('Remember managed turn 0');
      }
    },
    60000,
  );

  it('requires committed revision and provider capability, rejects malformed and cross-session requests', async () => {
    const f = await setup();
    expect((await f.post(RUNNER_SNAPSHOT_PATH, f.snapshot())).status).toBe(500);
    expect(
      (
        await f.post(
          RUNNER_SNAPSHOT_PATH,
          f.snapshot({ managed_resources: { version: 1, revision: 'bad' } }),
        )
      ).status,
    ).toBe(400);
    await f.stage();
    expect(
      (await f.post(RUNNER_SNAPSHOT_PATH, f.snapshot({ managed_resources: undefined }))).status,
    ).toBe(500);
    expect(
      (await f.post(RUNNER_SNAPSHOT_PATH, f.snapshot({ provider: 'unsupported' }))).status,
    ).toBe(500);
    expect(f.acquire).not.toHaveBeenCalled();
    for (const path of [
      RUNNER_RESOURCES_PATH,
      RUNNER_RESOURCE_CHANGES_PATH,
      RUNNER_RESOURCE_ACK_PATH,
    ]) {
      expect((await f.post(path, { type: 'pending' }, 'ses_other')).status).toBe(400);
    }
    expect((await f.post(RUNNER_SNAPSHOT_PATH, f.snapshot(), 'ses_other')).status).toBe(500);
    expect((await f.post(RUNNER_SNAPSHOT_PATH, f.snapshot())).status).toBe(200);
    expect(
      (await f.post(RUNNER_TURN_PATH, { type: 'user.message', content: 'hi' }, 'ses_other')).status,
    ).toBe(500);
  });

  it('preserves worker and tool state on refresh, keeps new staged resources, and closes both on stop', async () => {
    const f = await setup();
    await f.stage();
    await f.loop.applySnapshot(session, body(f.snapshot()));
    const tools = f.resources.toolSandbox!;
    await tools.files.write('keep.txt', Buffer.from('tool state'));
    const worker = f.rawWorkers[0]!;
    await worker.files.write('private.txt', Buffer.from('worker state'));
    await f.loop.applySnapshot(session, body(f.snapshot()));
    expect(f.acquire).toHaveBeenCalledTimes(1);
    expect(f.harnesses[1]!.startInput!.toolSandbox).toBe(tools);
    expect((await tools.files.read('keep.txt')).toString()).toBe('tool state');
    expect((await worker.files.read('private.txt')).toString()).toBe('worker state');
    const next = hash('reconfigure');
    await f.stage(next);
    const staged = f.resources.toolSandbox!;
    await f.loop.applySnapshot(
      session,
      body(f.snapshot({ managed_resources: { version: 1, revision: next } })),
    );
    expect(f.harnesses[2]!.startInput!.toolSandbox).toBe(staged);
    await staged.files.write('alive.txt', Buffer.from('alive'));
    const destroyTools = vi.spyOn(f.roots[1]!, 'destroy');
    const destroyWorker = vi.spyOn(f.rawWorkers[1]!, 'destroy');
    await f.loop.stop();
    expect(destroyTools).toHaveBeenCalledOnce();
    expect(destroyWorker).toHaveBeenCalledOnce();
  });

  it('waits for an in-progress configuration before stopping both owned sandboxes', async () => {
    const f = await setup();
    await f.stage();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const acquire = f.acquire.getMockImplementation()!;
    f.acquire.mockImplementationOnce(async () => {
      await gate;
      return acquire();
    });
    const snapshot = f.loop.applySnapshot(session, body(f.snapshot()));
    await vi.waitFor(() => expect(f.acquire).toHaveBeenCalledOnce());
    const stopping = f.loop.stop();
    release();
    await snapshot;
    await stopping;
    expect(f.loop.hasHarness()).toBe(false);
    expect(f.resources.toolSandbox).toBeUndefined();
    expect(f.harnesses[0]!.stopReason).toBe('replica.shutting_down');
    await expect(f.loop.applySnapshot(session, body(f.snapshot()))).rejects.toThrow('busy');
  });

  it('refuses another snapshot after a checkpoint scan failed without an acknowledgeable copy', async () => {
    const f = await setup();
    await f.stage();
    const root = (f.roots[0] as SandboxHandle & { rootDir(): string }).rootDir();
    await symlink('/dev/null', join(root, 'mnt/session/outputs/unsafe'));
    await expect(
      f.resources.runTool(
        async () => undefined,
        () => {},
        new AbortController().signal,
      ),
    ).rejects.toThrow('link or device');
    await expect(f.loop.applySnapshot(session, body(f.snapshot()))).rejects.toThrow(
      'not committed',
    );
    expect(f.harnesses).toHaveLength(0);
  });

  it.each(['manifest', 'snapshot', 'stop'] as const)(
    'revalidates a deferred response body after %s changes the captured configuration',
    async (change) => {
      const f = await setup();
      await f.stage();
      await f.loop.applySnapshot(session, body(f.snapshot()));
      const original = f.harnesses[0]!;
      // Dispatch returns the lazy body before the transport sends response.head.
      const response = await f.post(RUNNER_TURN_PATH, {
        type: 'user.message',
        content: 'deferred',
      });
      expect(response.status).toBe(200);
      expect(f.loop.hasActiveWork()).toBe(false);
      if (change === 'manifest') {
        await f.loop.applyResources(session, {
          type: 'manifest',
          manifest: { version: 1, revision: hash('next'), resources: [] },
        });
        expect(f.resources.ready).toBe(false);
      } else if (change === 'snapshot') {
        await f.loop.applySnapshot(session, body(f.snapshot()));
        expect(f.resources.ready).toBe(true);
      } else await f.loop.stop();
      await expect(response.body[Symbol.asyncIterator]().next()).rejects.toThrow('not ready');
      expect(original.submitted).toEqual([]);
      expect(f.harnesses.every((harness) => harness.submitted.length === 0)).toBe(true);
      expect(f.loop.hasActiveWork()).toBe(false);
    },
  );

  it('invalidates deferred bodies for same-revision configuration and rejects consumption while it is queued', async () => {
    const f = await setup();
    await f.stage();
    await f.loop.applySnapshot(session, body(f.snapshot()));
    const response = await f.post(RUNNER_TURN_PATH, { type: 'user.message', content: 'deferred' });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const push = f.resources.push.bind(f.resources);
    vi.spyOn(f.resources, 'push').mockImplementationOnce(async (...args) => {
      await gate;
      return push(...args);
    });
    const configuration = f.loop.applyResources(session, {
      type: 'manifest',
      manifest: { version: 1, revision, resources: [] },
    });
    // The controller is still ready: only the loop's configuration reservation protects this gap.
    expect(f.resources.ready).toBe(true);
    try {
      await expect(response.body[Symbol.asyncIterator]().next()).rejects.toThrow('not ready');
    } finally {
      release();
      await configuration;
    }
    expect(f.harnesses[0]!.submitted).toEqual([]);

    const nextResponse = await f.post(RUNNER_TURN_PATH, {
      type: 'user.message',
      content: 'also deferred',
    });
    await f.loop.applyResources(session, {
      type: 'manifest',
      manifest: { version: 1, revision, resources: [] },
    });
    // Same harness, same resource handle, same revision, but the capture belongs to an old epoch.
    await expect(nextResponse.body[Symbol.asyncIterator]().next()).rejects.toThrow('not ready');
    expect(f.harnesses[0]!.submitted).toEqual([]);
  });

  it('occupies the turn slot before delegation and keeps queued configuration out until the body closes', async () => {
    const f = await setup();
    await f.stage();
    await f.loop.applySnapshot(session, body(f.snapshot()));
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    f.harnesses[0]!.scriptTurn({
      gate,
      events: [{ kind: 'agent.message', payload: { content: 'answer' } }],
    });
    const response = await f.post(RUNNER_TURN_PATH, { type: 'user.message', content: 'run' });
    const second = await f.post(RUNNER_TURN_PATH, { type: 'user.message', content: 'duplicate' });
    const iterator = response.body[Symbol.asyncIterator]();
    const pending = iterator.next();
    expect(f.loop.hasActiveWork()).toBe(true);
    try {
      await expect(
        f.loop.applyResources(session, {
          type: 'manifest',
          manifest: { version: 1, revision, resources: [] },
        }),
      ).rejects.toThrow('during a turn');
      await expect(f.loop.applySnapshot(session, body(f.snapshot()))).rejects.toThrow('busy');
      await expect(second.body[Symbol.asyncIterator]().next()).rejects.toThrow('not ready');
    } finally {
      release();
    }
    expect((await pending).done).toBe(false);
    expect(f.loop.hasActiveWork()).toBe(true);
    while (!(await iterator.next()).done) {
      /* finish the response */
    }
    expect(f.loop.hasActiveWork()).toBe(false);
    expect(f.harnesses[0]!.submitted).toHaveLength(1);
    await f.loop.applySnapshot(session, body(f.snapshot()));
  });

  it.each(['return', 'break', 'throw'] as const)(
    'closes an early response via %s without emitting completion or retaining its turn slot',
    async (close) => {
      const f = await setup();
      await f.stage();
      await f.loop.applySnapshot(session, body(f.snapshot()));
      const harness = f.harnesses[0]!;
      harness.scriptTurn({
        events: [
          { kind: 'agent.message', payload: { text: 'first' } },
          { kind: 'agent.message', payload: { text: 'abandoned' } },
        ],
      });
      harness.scriptTurn({ events: [{ kind: 'agent.message', payload: { text: 'next turn' } }] });
      const response = await f.post(RUNNER_TURN_PATH, { type: 'user.message', content: 'start' });
      const seen: unknown[] = [];
      if (close === 'break') {
        for await (const line of response.body) {
          seen.push(JSON.parse(Buffer.from(line).toString()));
          break;
        }
      } else {
        const iterator = response.body[Symbol.asyncIterator]();
        const first = await iterator.next();
        expect(first.done).toBe(false);
        seen.push(JSON.parse(Buffer.from(first.value!).toString()));
        expect(f.loop.hasActiveWork()).toBe(true);
        if (close === 'return')
          expect(await iterator.return!()).toEqual({ value: undefined, done: true });
        else
          await expect(iterator.throw!(new Error('disconnected'))).rejects.toThrow('disconnected');
        expect(await iterator.next()).toEqual({ value: undefined, done: true });
      }
      expect(seen).toEqual([{ type: 'agent.message', text: 'first' }]);
      expect(harness.interruptCount).toBe(1);
      expect(f.loop.hasActiveWork()).toBe(false);
      const next: Array<{ type: string; text?: string }> = [];
      for await (const line of f.loop.runTurn(
        session,
        body({ type: 'user.message', content: 'again' }),
        new AbortController().signal,
      ))
        next.push(JSON.parse(Buffer.from(line).toString()));
      expect(next).toEqual([
        { type: 'agent.message', text: 'next turn' },
        { type: 'agent.turn_completed' },
      ]);
      await f.loop.applySnapshot(session, body(f.snapshot()));
    },
  );

  it('interrupts and drains a native tool parked on its resource ACK when the response closes', async () => {
    const f = await setup(true);
    await f.stage();
    await f.loop.applySnapshot(session, body(f.snapshot()));
    const killed = vi.spyOn(f.native.cli, 'kill');
    const response = await f.post(RUNNER_TURN_PATH, { type: 'user.message', content: 'write' });
    const iterator = response.body[Symbol.asyncIterator]();
    const received: string[] = [];
    for (;;) {
      const item = await iterator.next();
      expect(item.done).toBe(false);
      const event = JSON.parse(Buffer.from(item.value!).toString());
      received.push(event.type);
      if (event.type === 'orca.resource_checkpoint') break;
    }
    const pending = await responseJson(
      await f.post(RUNNER_RESOURCE_CHANGES_PATH, { type: 'pending' }),
    );
    expect(f.loop.hasActiveWork()).toBe(true);
    expect(await iterator.return!()).toEqual({ value: undefined, done: true });
    expect(f.loop.hasActiveWork()).toBe(false);
    expect(killed).toHaveBeenCalled();
    expect(f.native.frames.some((line) => JSON.parse(line).type === 'interrupt')).toBe(true);
    expect(f.native.frames.some((line) => JSON.parse(line).type === 'tool_result')).toBe(false);
    expect(received).not.toContain('agent.turn_completed');
    expect(await iterator.next()).toEqual({ value: undefined, done: true });
    // The interrupted checkpoint is still recoverable over the independent control routes.
    expect(
      (
        await f.post(RUNNER_RESOURCE_ACK_PATH, {
          checkpoint_id: pending.checkpoint.checkpoint_id,
          manifest_sha256: pending.manifest_sha256,
        })
      ).status,
    ).toBe(200);
    expect(f.resources.ready).toBe(true);
    await f.stage(hash('after closed response'));
  });

  it('serves checkpoint chunks and ACK while a real provider turn waits for persistence', async () => {
    const f = await setup(true);
    await f.stage();
    await f.loop.applySnapshot(session, body(f.snapshot()));
    const events: AgentEvent[] = [];
    let completed = false;
    const drain = (async () => {
      for await (const line of f.loop.runTurn(
        session,
        body({ type: 'user.message', content: [{ type: 'text', text: 'write' }] }),
        new AbortController().signal,
      )) {
        const value = JSON.parse(Buffer.from(line).toString());
        events.push({ kind: value.type, payload: value });
      }
      completed = true;
    })();
    await vi.waitFor(() =>
      expect(events.some((event) => event.kind === 'orca.resource_checkpoint')).toBe(true),
    );
    expect(completed).toBe(false);
    expect(events.findIndex((e) => e.kind === 'agent.tool_result')).toBeLessThan(
      events.findIndex((e) => e.kind === 'orca.resource_checkpoint'),
    );
    expect(f.native.frames.some((line) => JSON.parse(line).type === 'tool_result')).toBe(false);
    await expect(f.rawWorkers[0]!.files.read('/mnt/session/outputs/report.txt')).rejects.toThrow();
    const pending = await responseJson(
      await f.post(RUNNER_RESOURCE_CHANGES_PATH, { type: 'pending' }),
    );
    const ack = {
      checkpoint_id: pending.checkpoint.checkpoint_id,
      manifest_sha256: pending.manifest_sha256,
    };
    const chunk = await responseJson(
      await f.post(RUNNER_RESOURCE_CHANGES_PATH, {
        type: 'file_chunk',
        checkpoint_id: ack.checkpoint_id,
        resource_id: pending.checkpoint.files[0].resource_id,
        path: 'report.txt',
        offset: 0,
      }),
    );
    expect(Buffer.from(chunk.content_base64, 'base64').toString()).toBe('saved');
    expect(
      (await f.post(RUNNER_RESOURCE_ACK_PATH, { ...ack, manifest_sha256: hash('wrong') })).status,
    ).toBe(400);
    expect(completed).toBe(false);
    expect((await f.post(RUNNER_RESOURCE_ACK_PATH, ack)).status).toBe(200);
    await drain;
    expect(f.native.frames.some((line) => JSON.parse(line).type === 'tool_result')).toBe(true);
    expect(
      (await responseJson(await f.post(RUNNER_RESOURCE_CHANGES_PATH, { type: 'pending' })))
        .checkpoint,
    ).toBeNull();
  });

  it('installs verified Skills, advertises exact pins, applies block_skills, and clears removed Skills', async () => {
    const f = await setup();
    await f.stage();
    const skill = skillsPush();
    await f.loop.applySkills(session, skill.bytes);
    await f.loop.applySnapshot(session, body(f.snapshot({ skills: [skill.descriptor] })));
    const input = f.harnesses.at(-1)!.startInput!;
    expect(input.agentSnapshot.skills_plugin_dir).toBeUndefined();
    expect(input.agentSnapshot.system).toContain('/workspace/skills/guide/SKILL.md');
    expect(input.agentSnapshot.system).toContain(skill.descriptor.package_sha256);
    expect(
      (await f.resources.toolSandbox!.files.read('/workspace/skills/guide/SKILL.md')).toString(),
    ).toBe(skill.content);
    await f.loop.applySnapshot(
      session,
      body(
        f.snapshot({
          skills: [skill.descriptor],
          guardrails: [
            {
              id: 'grd_block',
              name: 'block guide',
              tier: 'workspace',
              phases: ['tool_call'],
              stateful: false,
              rule: { kind: 'builtin', builtin: 'block_skills', params: { blocked: ['guide'] } },
            },
          ],
        }),
      ),
    );
    expect(f.harnesses.at(-1)!.startInput!.agentSnapshot.system).not.toContain('available_skills');
    await expect(
      f.resources.toolSandbox!.files.read('/workspace/skills/guide/SKILL.md'),
    ).rejects.toThrow();
    await f.loop.applySnapshot(session, body(f.snapshot({ skills: [skill.descriptor] })));
    await f.loop.applySkills(
      session,
      body({
        type: 'skills_manifest',
        dir: 'skills-plugin',
        skills: [],
        descriptors: [],
        bundles: {},
      }),
    );
    await f.loop.applySnapshot(session, body(f.snapshot()));
    await expect(
      f.resources.toolSandbox!.files.read('/workspace/skills/guide/SKILL.md'),
    ).rejects.toThrow();
  });

  it.each(['extra', 'different', 'missing', 'empty', 'duplicate'] as const)(
    'compares every Skill descriptor binding before materialization deduplication: %s',
    async (mismatch) => {
      const f = await setup();
      await f.stage();
      const skill = skillsPush();
      const other = {
        ...skill.descriptor,
        id: 'sv2',
        skill_id: 'sk2',
        description: 'different binding',
      };
      const lines = skill.bytes.toString().split('\n');
      const manifest = JSON.parse(lines[0]!);
      manifest.descriptors =
        mismatch === 'missing'
          ? [skill.descriptor]
          : [skill.descriptor, mismatch === 'duplicate' ? skill.descriptor : other];
      lines[0] = JSON.stringify(manifest);
      await f.loop.applySkills(session, Buffer.from(lines.join('\n')));
      const expected =
        mismatch === 'different'
          ? [skill.descriptor, { ...other, description: 'expected metadata' }]
          : mismatch === 'missing'
            ? [skill.descriptor, other]
            : mismatch === 'empty'
              ? []
              : [skill.descriptor];
      await expect(
        f.loop.applySnapshot(session, body(f.snapshot({ skills: expected }))),
      ).rejects.toThrow('pins differ');
      expect(f.harnesses).toHaveLength(0);
    },
  );

  it.each([false, true])(
    'accepts matching full Skill binding multisets with shared bytes (identical=%s)',
    async (identical) => {
      const f = await setup();
      await f.stage();
      const skill = skillsPush();
      const other = identical
        ? { ...skill.descriptor }
        : { ...skill.descriptor, id: 'sv2', skill_id: 'sk2', description: 'another binding' };
      const lines = skill.bytes.toString().split('\n');
      const manifest = JSON.parse(lines[0]!);
      manifest.descriptors = [skill.descriptor, other];
      lines[0] = JSON.stringify(manifest);
      await f.loop.applySkills(session, Buffer.from(lines.join('\n')));
      await f.loop.applySnapshot(session, body(f.snapshot({ skills: [other, skill.descriptor] })));
      expect(f.harnesses).toHaveLength(1);
      expect(
        f.harnesses[0]!.startInput!.agentSnapshot.system!.match(
          /\/workspace\/skills\/guide\/SKILL.md/g,
        ),
      ).toHaveLength(1);
      expect(
        (await f.resources.toolSandbox!.files.read('/workspace/skills/guide/SKILL.md')).toString(),
      ).toBe(skill.content);
      if (!identical) {
        const next = hash('replacement with Skills');
        await f.stage(next);
        await f.loop.applySnapshot(
          session,
          body(
            f.snapshot({
              skills: [other, skill.descriptor],
              managed_resources: { version: 1, revision: next },
            }),
          ),
        );
      }
      await f.loop.stop();
      expect(f.resources.toolSandbox).toBeUndefined();
    },
  );

  it('rejects mismatched Skill bytes and snapshot pins before provider start', async () => {
    const f = await setup();
    await f.stage();
    const skill = skillsPush();
    await expect(
      f.loop.applySkills(
        session,
        Buffer.from(
          skill.bytes
            .toString()
            .replace(
              Buffer.from(skill.content).toString('base64'),
              Buffer.from('tampered').toString('base64'),
            ),
        ),
      ),
    ).rejects.toThrow('mismatch');
    await f.loop.applySkills(session, skill.bytes);
    await expect(
      f.loop.applySnapshot(
        session,
        body(f.snapshot({ skills: [{ ...skill.descriptor, package_sha256: hash('other') }] })),
      ),
    ).rejects.toThrow('pins differ');
    expect(f.harnesses).toHaveLength(0);
  });
});

describe('Codex trusted worker and managed tools', () => {
  it('runs Bash, Read, and Write through the managed tool handle while launching only on the worker handle', async () => {
    const runtime = createRunnerSandboxRuntime({ kind: 'in-memory' });
    const worker = await runtime.acquire({});
    const tools = await runtime.acquire({});
    cleanups.push(
      () => worker.destroy(),
      () => tools.destroy(),
    );
    await tools.files.write('seed.txt', Buffer.from('tool-root-only'));
    const calls = [
      { name: 'read', arguments: { path: 'seed.txt' } },
      { name: 'bash', arguments: { command: 'printf managed > bash.txt' } },
      { name: 'write', arguments: { path: 'write.txt', content: 'managed-write' } },
    ];
    let next = 0;
    const native = scriptedNativeCliWithDeath({
      respond: (line) => {
        const cmd = JSON.parse(line);
        if (cmd.type === 'start') return [JSON.stringify({ type: 'ready' })];
        if (cmd.type === 'submit' || cmd.type === 'tool_result') {
          const call = calls[next++];
          return [
            JSON.stringify(
              call ? { type: 'tool_call', id: `call${next}`, ...call } : { type: 'done' },
            ),
          ];
        }
        return [];
      },
    });
    const launch = vi.fn((_handle: SandboxHandle) => native.cli);
    const harness = new CodexSdkHarness({ apiKey: 'scoped', launch });
    cleanups.push(() => harness.stop('client.archived'));
    let barriers = 0;
    await harness.start({
      workspaceId: 'ws',
      sessionId: session,
      sandbox: worker,
      toolSandbox: tools,
      runToolWithResources: async (op) => {
        barriers++;
        return op();
      },
      toolPermissions: { policyFor: () => 'always_allow' },
      agentSnapshot: {
        model_provider: 'openai',
        model_id: 'gpt-5.4',
        allowed_tool_names: ['read', 'write', 'bash'],
      },
    });
    await harness.submit({ kind: 'user.message', payload: { content: 'tools' } });
    expect(launch.mock.calls[0]![0]).toBe(worker);
    expect(barriers).toBe(3);
    expect((await tools.files.read('bash.txt')).toString()).toBe('managed');
    expect((await tools.files.read('write.txt')).toString()).toBe('managed-write');
    await expect(worker.files.read('bash.txt')).rejects.toThrow();
    await expect(worker.files.read('write.txt')).rejects.toThrow();
    expect(native.frames.find((line) => line.includes('tool-root-only'))).toBeDefined();
  });

  it('strips ambient runner/store/Git secrets and launch hooks from the actual worker environment', async () => {
    for (const key of [
      'ORCA_RUNNER_BINDING_TOKEN',
      'AWS_SECRET_ACCESS_KEY',
      'GIT_PASSWORD',
      'DATABASE_URL',
      'OPENAI_API_KEY',
      'NODE_OPTIONS',
    ])
      vi.stubEnv(key, key === 'NODE_OPTIONS' ? '--no-warnings' : 'secret-canary');
    const worker = await createRunnerSandboxRuntime({ kind: 'in-memory' }).acquire({});
    cleanups.push(() => worker.destroy());
    const script = `const fs=require('fs'); const readline=require('readline'); fs.writeFileSync('observed.json',JSON.stringify({env:process.env,cwd:process.cwd()})); readline.createInterface({input:process.stdin}).on('line',line=>{const c=JSON.parse(line);if(c.type==='start')process.stdout.write(JSON.stringify({type:'ready'})+'\\n');});`;
    const harness = new CodexSdkHarness({
      apiKey: 'scoped-jwt-control-pipe',
      workerArgs: ['-e', script],
      timeoutMs: 2000,
    });
    cleanups.push(() => harness.stop('client.archived'));
    await harness.start({
      workspaceId: 'ws',
      sessionId: session,
      sandbox: worker,
      agentSnapshot: { model_provider: 'openai', model_id: 'gpt-5.4', allowed_tool_names: [] },
    });
    const observed = JSON.parse(
      (await worker.files.read('.orca-worker/cwd/observed.json')).toString(),
    );
    for (const key of [
      'ORCA_RUNNER_BINDING_TOKEN',
      'AWS_SECRET_ACCESS_KEY',
      'GIT_PASSWORD',
      'DATABASE_URL',
      'OPENAI_API_KEY',
      'NODE_OPTIONS',
    ])
      expect(observed.env[key]).toBeUndefined();
    expect(Object.keys(observed.env).sort()).toEqual(
      expect.arrayContaining(['HOME', 'PATH', 'TMPDIR']),
    );
    expect(observed.env.HOME).toContain('/.orca-worker/home');
    expect(observed.env.TMPDIR).toContain('/.orca-worker/tmp');
    expect(observed.cwd).toContain('/.orca-worker/cwd');
    expect(JSON.stringify(observed)).not.toContain('scoped-jwt');
  });

  it('fails the turn and never returns a tool response to the SDK after resource persistence fails', async () => {
    const f = await setup(true);
    await f.stage();
    vi.spyOn(f.resources, 'runTool').mockImplementation(async (operation) => {
      await operation();
      throw new Error('resource persistence unavailable');
    });
    await f.loop.applySnapshot(session, body(f.snapshot()));
    const events: AgentEvent[] = [];
    for await (const line of f.loop.runTurn(
      session,
      body({ type: 'user.message', content: [{ type: 'text', text: 'write' }] }),
      new AbortController().signal,
    )) {
      const value = JSON.parse(Buffer.from(line).toString());
      events.push({ kind: value.type, payload: value });
    }
    expect(events.some((event) => event.kind === 'agent.error')).toBe(true);
    expect(events.some((event) => event.kind === 'agent.tool_result')).toBe(true);
    expect(f.native.frames.some((line) => JSON.parse(line).type === 'tool_result')).toBe(false);
    expect(events.at(-1)!.kind).toBe('agent.turn_completed');
  });
});
