// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Mirrors services/harness-server/test/unit/chmod-many.spec.ts: the service
// suite exercises its own physical copy of this logic, so the canonical
// package copy needs the safety net in-package.
import { describe, expect, it } from 'vitest';
import { buildSandboxChmodManyCommand, serializeSandboxFileModes } from '../../src/chmod-many.js';

describe('sandbox batch chmod helper', () => {
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
