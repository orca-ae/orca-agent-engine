// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';

describe('sandbox-harness image hardening', () => {
  it('drops privilege escalation paths before starting Node', async () => {
    const entrypoint = await readFile(new URL('../docker-entrypoint.sh', import.meta.url), 'utf8');
    const dockerfile = await readFile(new URL('../Dockerfile', import.meta.url), 'utf8');
    const gvisorBwrap = await readFile(new URL('../orca-gvisor-bwrap', import.meta.url), 'utf8');

    expect(entrypoint).toContain('--no-new-privs --bounding-set=-all');
    expect(entrypoint).toContain('--dev /dev');
    expect(entrypoint).not.toContain('chown -R');
    expect(dockerfile).toMatch(/\bfuse3\b/);
    expect(dockerfile).toMatch(/\bs3fs\b/);
    expect(dockerfile.match(/^FROM .*@sha256:[0-9a-f]{64} AS [a-z-]+$/gm)).toHaveLength(3);
    expect(dockerfile).toContain("s3fs --help 2>&1 | grep -F 'compat_dir'");
    expect(dockerfile).toContain('id -u node');
    expect(dockerfile).toContain('orca-s3fs-mount');
    expect(dockerfile).toContain('orca-gvisor-bwrap');
    expect(dockerfile).toContain('command -v unshare');
    expect(dockerfile).toContain('ARG BUBBLEWRAP_VERSION=0.12.0');
    expect(dockerfile).toContain(
      'ARG BUBBLEWRAP_SHA256=9760d007363e3abba7c747489910f9f82d9fca53ba3bd3282e396fa3c97a3314',
    );
    expect(dockerfile).toContain("-Dassume_kernel=''");
    expect(dockerfile).toContain('dpkg-deb --build --root-owner-group');
    expect(dockerfile).toContain("'Package: bubblewrap'");
    expect(dockerfile).toContain('dpkg-query -W');
    expect(dockerfile).toContain('/usr/share/doc/bubblewrap/copyright');
    expect(dockerfile).toContain('dpkg -V bubblewrap');
    expect(dockerfile).not.toMatch(/^\s+bubblewrap\s+\\$/m);
    expect(dockerfile).toContain('${package_root}/usr/lib/orca/bwrap.real');
    expect(dockerfile).toContain('/usr/local/bin/orca-gvisor-bwrap');
    expect(dockerfile).not.toMatch(/\bsudo\b(?=\s*\\)/);
    expect(dockerfile).not.toContain('/etc/sudoers.d');
    expect(gvisorBwrap).toContain('--map-user=${uid}');
    expect(gvisorBwrap).toContain('--map-group=${gid}');
    expect(gvisorBwrap).toContain('--unshare-net');
    expect(gvisorBwrap).toContain('--unshare-cgroup | --unshare-cgroup-try');
    expect(gvisorBwrap).toContain('grep -qi gvisor /proc/sys/kernel/osrelease');
    expect(gvisorBwrap).toContain('--debug-opt=force-openat-fallback');
    expect(gvisorBwrap).toContain('REAL_BWRAP=/usr/lib/orca/bwrap.real');
  });
});
