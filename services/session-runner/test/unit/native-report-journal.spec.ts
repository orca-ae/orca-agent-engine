// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { appendFile, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { RunnerResources } from '../../src/resources.js';

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return { ...actual, appendFile: vi.fn(actual.appendFile), writeFile: vi.fn(actual.writeFile) };
});

const directories: string[] = [];
afterEach(async () => {
  for (const directory of directories.splice(0))
    await rm(directory, { recursive: true, force: true });
});

async function fixture() {
  const workspaceDir = await mkdtemp(join(tmpdir(), 'orca-report-journal-'));
  directories.push(workspaceDir);
  const controller = new RunnerResources({ workspaceDir });
  // Exercise the journal independently of the platform-specific placement probe.
  Object.assign(controller, { active: { root: workspaceDir } });
  vi.spyOn(controller, 'placement', 'get').mockReturnValue({
    id: 'placement_fixture',
  } as NonNullable<RunnerResources['placement']>);
  vi.spyOn(controller, 'revision', 'get').mockReturnValue('a'.repeat(64));
  await controller.beginNativeTurn('evt_turn');
  const path = join(workspaceDir, 'private-native-turn/evt_turn.reports.ndjson');
  return { controller, workspaceDir, path };
}

it('appends exactly one bounded entry per event through the 10,000-event cap', async () => {
  const { controller, path } = await fixture();
  vi.mocked(appendFile).mockClear();
  vi.mocked(writeFile).mockClear();
  let previous = '';
  for (let index = 0; index < 10000; index++) {
    await controller.retainNativeReports({
      events: [{ type: 'agent.message', id: 'evt_' + index, content: 'text' }],
    });
    if (index === 4999) previous = await readFile(path, 'utf8');
  }
  const log = await readFile(path, 'utf8');
  expect(log.startsWith(previous)).toBe(true);
  expect(appendFile).toHaveBeenCalledTimes(10000);
  expect(writeFile).not.toHaveBeenCalled();
  const bytesWritten = vi
    .mocked(appendFile)
    .mock.calls.reduce((bytes, [, data]) => bytes + Buffer.byteLength(data as string), 0);
  expect(bytesWritten).toBe(Buffer.byteLength(log));
  expect(log.split('\n')).toHaveLength(10001);
  expect(Buffer.byteLength(log)).toBeLessThan(2 * Buffer.byteLength(previous) + 10000);
  await expect(
    controller.retainNativeReports({ events: [{ type: 'agent.message', id: 'evt_extra' }] }),
  ).rejects.toThrow('exceeds limit');
  expect((await stat(path)).size).toBe(Buffer.byteLength(log));
}, 30000);

it('counts the serialized journal framing against the exact 8 MiB limit', async () => {
  const { controller, path } = await fixture();
  const entry = { events: [{ type: 'agent.message', id: 'evt_large', content: '' }] };
  const overhead = Buffer.byteLength(JSON.stringify({ ...entry, turnId: 'evt_turn' }) + '\n');
  entry.events[0]!.content = 'x'.repeat(8 * 1024 * 1024 - overhead);
  await controller.retainNativeReports(entry);
  expect((await stat(path)).size).toBe(8 * 1024 * 1024);
  await expect(controller.retainNativeReports({ closed: true })).rejects.toThrow('exceeds limit');
});

it.each([false, true])(
  'recovers complete journal entries as incomplete evidence and preserves truncated logs (%s)',
  async (truncated) => {
    const { controller, workspaceDir, path } = await fixture();
    await controller.retainNativeReports({
      expected: [],
      reports: [],
      outcome: 'interrupted',
      events: [{ type: 'agent.message', id: 'evt_partial' }],
    });
    const bytes = await readFile(path, 'utf8');
    if (truncated) await writeFile(path, bytes.slice(0, -1));
    const replacement = new RunnerResources({ workspaceDir });
    const pending = await replacement.changes('ses_fixture', { type: 'pending' });
    if (truncated) expect(pending).toEqual({ checkpoint: null });
    else
      expect(pending).toMatchObject({
        incomplete_completion: {
          outcome: 'interrupted',
          report_set_sealed: false,
          events: [{ id: 'evt_partial' }],
        },
      });
    expect(await readFile(path, 'utf8')).toBe(truncated ? bytes.slice(0, -1) : bytes);
    await replacement.close();
  },
);

it('keeps prior JSON evidence readable without deleting it or inventing completion', async () => {
  const { workspaceDir } = await fixture();
  const path = join(workspaceDir, 'private-native-turn/evt_turn.reports.json');
  const retained = JSON.stringify({
    turnId: 'evt_turn',
    expected: [],
    reports: [],
    events: [],
    outcome: 'failed',
  });
  await writeFile(path, retained);
  const replacement = new RunnerResources({ workspaceDir });
  expect(await replacement.changes('ses_fixture', { type: 'pending' })).toMatchObject({
    incomplete_completion: { outcome: 'failed', report_set_sealed: false },
  });
  expect(await readFile(path, 'utf8')).toBe(retained);
  expect(replacement.readyForTurn).toBe(false);
  await replacement.close();
});

it.each([false, true])(
  'merges report deltas across entries and refuses conflicting retained identities (%s)',
  async (conflicting) => {
    const { controller, workspaceDir, path } = await fixture();
    const report = {
      id: 'report_1',
      provider: 'anthropic' as const,
      semantics: 'delta' as const,
      complete: false,
    };
    await controller.retainNativeReports({ expected: ['report_1'] });
    await controller.retainNativeReports({ reports: [report] });
    await controller.retainNativeReports({
      reports: [{ ...report, ...(conflicting ? { model: 'changed' } : {}) }],
    });
    const bytes = await readFile(path, 'utf8');
    const replacement = new RunnerResources({ workspaceDir });
    const pending = await replacement.changes('ses_fixture', { type: 'pending' });
    if (conflicting) expect(pending).toEqual({ checkpoint: null });
    else
      expect(pending).toMatchObject({
        incomplete_completion: {
          expected_reports: ['report_1'],
          reports: [report],
          report_set_sealed: false,
        },
      });
    expect(replacement.readyForTurn).toBe(false);
    expect(await readFile(path, 'utf8')).toBe(bytes);
    await replacement.close();
  },
);

it('recovers prior complete report entries when process loss tears the final append', async () => {
  const { controller, workspaceDir, path } = await fixture();
  const report = {
    id: 'report_1',
    provider: 'anthropic' as const,
    semantics: 'delta' as const,
    complete: false,
  };
  await controller.retainNativeReports({ expected: ['report_1', 'missing'] });
  await controller.retainNativeReports({ reports: [report] });
  const bytes = (await readFile(path, 'utf8')) + '{"turnId":"evt_turn","reports":[';
  await writeFile(path, bytes);
  const replacement = new RunnerResources({ workspaceDir });
  expect(await replacement.changes('ses_fixture', { type: 'pending' })).toMatchObject({
    incomplete_completion: {
      expected_reports: ['missing', 'report_1'],
      reports: [report],
      report_set_sealed: false,
    },
  });
  expect(replacement.readyForTurn).toBe(false);
  expect(await readFile(path, 'utf8')).toBe(bytes);
  await replacement.close();
});
