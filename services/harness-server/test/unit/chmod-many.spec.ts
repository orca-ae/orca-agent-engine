// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, statSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  buildSandboxChmodManyCommand,
  serializeSandboxFileModes,
} from '../../src/sandbox/chmod-many.js';

describe('sandbox batch chmod helper', () => {
  it.skipIf(process.platform !== 'linux')(
    'executes permission restoration and rejects special bits in the sandbox manifest',
    () => {
      const root = mkdtempSync(join(tmpdir(), 'chmod-many-'));
      const manifest = join(root, 'manifest.json');
      const entries = [0o444, 0o555, 0o640, 0o644, 0o755].map((mode) => ({
        path: join(root, `file '${mode}`),
        mode,
      }));
      try {
        for (const entry of entries) writeFileSync(entry.path, 'fixture');
        writeFileSync(manifest, serializeSandboxFileModes(root, entries));
        const command = buildSandboxChmodManyCommand(manifest, root);
        execFileSync('sh', ['-c', command]);
        for (const entry of entries) {
          expect(statSync(entry.path).mode & 0o7777).toBe(entry.mode);
        }
        expect(existsSync(manifest)).toBe(false);
        // Bypass host validation to exercise the sandbox-side trust boundary.
        writeFileSync(manifest, JSON.stringify([{ path: entries[0]!.path, mode: 0o4755 }]));
        expect(() => execFileSync('sh', ['-c', command], { stdio: 'pipe' })).toThrow(
          /invalid entry/,
        );
        expect(statSync(entries[0]!.path).mode & 0o7777).toBe(0o444);
        expect(existsSync(manifest)).toBe(false);
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
  );

  it('serializes validated absolute paths and normalized modes', () => {
    expect(
      JSON.parse(
        serializeSandboxFileModes('/workspace/skills/example', [
          { path: '/workspace/skills/example/SKILL.md', mode: 0o444 },
          { path: '/workspace/skills/example/scripts/run.sh', mode: 0o555 },
        ]).toString('utf8'),
      ),
    ).toEqual([
      { path: '/workspace/skills/example/SKILL.md', mode: 0o444 },
      { path: '/workspace/skills/example/scripts/run.sh', mode: 0o555 },
    ]);
  });

  it('rejects relative paths, NULs, and invalid modes before writing a manifest', () => {
    expect(() =>
      serializeSandboxFileModes('/workspace/skills', [{ path: 'relative', mode: 0o444 }]),
    ).toThrow(/absolute/);
    expect(() =>
      serializeSandboxFileModes('/workspace/skills', [
        { path: '/workspace/skills/a\0b', mode: 0o444 },
      ]),
    ).toThrow(/NUL/);
    expect(() =>
      serializeSandboxFileModes('/workspace/skills', [
        { path: '/workspace/skills/example', mode: 0o1000 },
      ]),
    ).toThrow(/invalid file mode/);
    expect(() =>
      serializeSandboxFileModes('/workspace/skills', [{ path: '/etc/passwd', mode: 0o444 }]),
    ).toThrow(/outside root/);
  });

  it.each([0o000, 0o600, 0o640, 0o644, 0o755, 0o777])(
    'accepts ordinary permission bits (%s)',
    (mode) => {
      const entries = [{ path: '/workspace/repo/file', mode }];
      expect(JSON.parse(serializeSandboxFileModes('/workspace/repo', entries).toString())).toEqual(
        entries,
      );
    },
  );

  it.each([-1, 1.5, NaN, Infinity, 0o1000, 0o2644, 0o4755, 0o100644])(
    'rejects special bits and invalid numeric modes (%s)',
    (mode) => {
      expect(() =>
        serializeSandboxFileModes('/workspace/repo', [{ path: '/workspace/repo/file', mode }]),
      ).toThrow(/invalid file mode/);
    },
  );

  it('runs Node with the same cleared PATH guaranteed by sandbox templates', () => {
    const command = buildSandboxChmodManyCommand(
      "/tmp/orca-chmod-'quoted'.json",
      '/workspace/skills/example',
    );
    expect(command).toContain('/usr/bin/env -i PATH=/usr/local/bin:/usr/bin node -e');
    expect(command).toContain("'/tmp/orca-chmod-'\\''quoted'\\''.json'");
  });

  it('rejects a root whose opened descriptor resolves to an aliased path', () => {
    const command = buildSandboxChmodManyCommand(
      '/tmp/orca-chmod.json',
      '/workspace/skills/example',
    );
    expect(command).toContain('if (openedRoot !== root) throw new Error');
    expect(command).toContain('chmod root resolves outside its declared path');
  });
});
