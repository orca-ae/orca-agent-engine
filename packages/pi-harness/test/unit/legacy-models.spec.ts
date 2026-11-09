// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { WorkerEvent } from '@orca/sdk-harness';
import { PiSdkWorker } from '../../src/index.js';
import { legacyPiModel } from '../../src/legacy-models.js';
import { createProviderFixture } from '../support/provider-fixture.js';

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of cleanup.splice(0).reverse()) await close();
});

it.each([
  ['together', 'openai/gpt-oss-20b', 'deepseek'],
  ['opencode-go', 'qwen3.7-plus', 'deepseek'],
  ['opencode-go', 'qwen3.8-max', 'deepseek'],
  ['opencode', 'claude-sonnet-4-6', 'anthropic'],
  ['kimi-coding', 'k3', 'anthropic'],
] as const)(
  'preserves %s/%s native endpoint and protocol across the SDK upgrade',
  async (provider, id, fixtureProvider) => {
    const model = legacyPiModel(provider, id)!;
    const api = await createProviderFixture(fixtureProvider);
    const requestPath =
      fixtureProvider === 'anthropic' ? '/v1/messages?beta=true' : '/chat/completions';
    cleanup.push(api.close);
    const send = globalThis.fetch;
    const urls: string[] = [];
    vi.spyOn(globalThis, 'fetch').mockImplementation((input, init) => {
      const request = new Request(input, init);
      urls.push(request.url);
      // Observe the real native URL before routing its bytes to a local fixture.
      const fixturePath = fixtureProvider === 'anthropic' ? requestPath : '/v1' + requestPath;
      return send(new Request(new URL(fixturePath, api.url), request));
    });
    const root = await mkdtemp(join(tmpdir(), 'pi-legacy-model-'));
    cleanup.push(() => rm(root, { recursive: true, force: true }));
    const events: WorkerEvent[] = [];
    const worker = new PiSdkWorker((event) => events.push(event));
    cleanup.push(() => worker.close());
    await worker.handle({
      type: 'start',
      root,
      sessionId: 'ses_sdk',
      modelProvider: provider,
      model: id,
      system: 'Managed tools only',
      apiKey: api.apiKey,
      tools: [],
    });
    await worker.handle({ type: 'submit', text: 'Keep my pinned model' });
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(urls).toEqual([model.baseUrl.replace(/\/$/, '') + requestPath]);
    expect(api.requests[0]?.model).toBe(id);
    expect(events).toContainEqual(expect.objectContaining({ type: 'checkpoint' }));
  },
);
