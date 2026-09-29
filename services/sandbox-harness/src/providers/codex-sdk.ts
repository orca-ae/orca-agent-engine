// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { PiSdkWorker } from '@orca/pi-harness';
import type { SdkWorker } from '@orca/sdk-harness';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CodexSdkWorker, type WorkerCommand } from '@orca/codex-harness';
import { z } from 'zod';
import type { Provider, Runtime } from './types.js';

const options = z.object({
  apiKey: z.string().min(1),
  baseUrl: z.string().url().optional(),
  piGatewayUrl: z.string().url().optional(),
  system: z.string().optional(),
});
const commandSchema = z.discriminatedUnion('type', [
  options.extend({
    type: z.literal('start'),
    root: z.string(),
    sessionId: z.string().min(1),
    model: z.string().min(1),
    modelProvider: z.string().optional(),
    system: z.string(),
    effort: z.string().optional(),
    tools: z.array(
      z
        .object({
          name: z.string(),
          inputSchema: z.object({ type: z.literal('object') }).passthrough(),
        })
        .passthrough(),
    ),
    checkpoint: z
      .object({
        version: z.literal(1),
        format: z.literal('pi_sdk').optional(),
        sdkVersion: z.string().optional(),
        threadId: z.string(),
        files: z.record(z.string(), z.string()),
        instructionsSha256: z.string().optional(),
      })
      .optional(),
  }),
  options.extend({ type: z.literal('refresh') }),
  z.object({ type: z.literal('submit'), text: z.string() }),
  z.object({
    type: z.literal('tool_result'),
    id: z.string(),
    result: z.object({ content: z.array(z.unknown()) }).passthrough(),
  }),
  z.object({ type: z.literal('interrupt') }),
  z.object({ type: z.literal('stop') }),
]);

/** The host adapter owns policies, accounting and durable turn receipts in both topologies. */
export class CodexSdkRuntime implements Runtime {
  constructor(readonly model: 'codex-sdk' | 'pi-sdk' = 'codex-sdk') {}
  private worker: SdkWorker | undefined;
  private root: string | undefined;

  async handleSdkCommand(raw: unknown, emit: (event: unknown) => void): Promise<void> {
    const command = commandSchema.parse(raw);
    if (command.type === 'start') {
      if (this.worker) throw new Error('SDK already started');
      if (this.model === 'pi-sdk' && !command.piGatewayUrl)
        throw new Error('colocated Pi requires an explicit native Gateway URL');
      this.worker = this.model === 'pi-sdk' ? new PiSdkWorker(emit) : new CodexSdkWorker(emit);
      // A host path must never become the SDK's cwd inside the sandbox. Keep
      // project configuration and ambient account credentials out of discovery.
      this.root = await mkdtemp(join(tmpdir(), 'orca-codex-work-'));
      await this.worker.handle({ ...command, root: this.root } as WorkerCommand);
      return;
    }
    if (!this.worker) throw new Error('SDK not started');
    if (command.type === 'refresh') {
      await this.worker.refreshOptions(command);
    } else {
      await this.worker.handle(command as WorkerCommand);
      if (command.type === 'stop' && this.root)
        await rm(this.root, { recursive: true, force: true });
    }
  }

  runTurn(): AsyncGenerator<never> {
    throw new Error('codex-sdk requires the managed SDK command channel');
  }
}

export const codexSdkProvider: Provider = {
  id: 'codex-sdk',
  harnessId: 'codex_sdk',
  displayName: 'Codex SDK',
  createRuntime: () => new CodexSdkRuntime(),
};

export const piSdkProvider: Provider = {
  id: 'pi-sdk',
  harnessId: 'pi_sdk',
  displayName: 'Pi SDK',
  createRuntime: () => new CodexSdkRuntime('pi-sdk'),
};
