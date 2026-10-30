// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { afterEach, expect, it, vi } from 'vitest';
import { inspectDshInstallation } from '../../src/installation.js';

vi.mock('node:fs', async (original) => {
  const fs = await original<typeof import('node:fs')>();
  return { ...fs, readFileSync: vi.fn(fs.readFileSync) };
});
afterEach(() => vi.mocked(readFileSync).mockReset());

it.each(['@deepseek-ai/dsh', '@deepseek-ai/cordis', '@earendil-works/pi-ai'])(
  'rejects an incompatible installed %s before launch',
  async (name) => {
    const actual = await vi.importActual<typeof import('node:fs')>('node:fs');
    vi.mocked(readFileSync).mockImplementation(((path: string, encoding: 'utf8') => {
      const text = actual.readFileSync(path, encoding);
      if (path.endsWith('/' + name + '/package.json')) {
        const manifest = JSON.parse(text);
        return JSON.stringify({ ...manifest, version: '0.0.0' });
      }
      return text;
    }) as typeof readFileSync);
    expect(() => inspectDshInstallation()).toThrow(
      'Incompatible installed DSH dependency: ' + name,
    );
  },
);

it.each(['@deepseek-ai/new-plugin', '@deepseek-ai/dsh-new-plugin'])(
  'rejects unknown relevant dependency %s without inferring a pin',
  async (name) => {
    const actual = await vi.importActual<typeof import('node:fs')>('node:fs');
    vi.mocked(readFileSync).mockImplementation(((path: string, encoding: 'utf8') => {
      const text = actual.readFileSync(path, encoding);
      if (path.endsWith('/packages/dsh-harness/package.json')) {
        const manifest = JSON.parse(text);
        manifest.dependencies[name] = '1.1.7-alpha.1';
        return JSON.stringify(manifest);
      }
      return text;
    }) as typeof readFileSync);
    expect(() => inspectDshInstallation()).toThrow('Unknown DSH dependency: ' + name);
  },
);
