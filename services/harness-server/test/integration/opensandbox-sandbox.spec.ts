// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { OpenSandboxRuntime } from '../../src/sandbox/opensandbox/runtime.js';
import {
  buildS3fsCredentialEnvironment,
  buildS3fsMountCommand,
} from '../../src/sandbox/s3fs-mount.js';
import {
  buildSandboxWritePolicy,
  createPolicyEnforcedSandbox,
} from '../../src/sandbox/write-policy.js';

const domain = process.env['OPEN_SANDBOX_DOMAIN'];
const image = process.env['OPEN_SANDBOX_IMAGE'];
const entrypoint = process.env['OPEN_SANDBOX_ENTRYPOINT'];
const fileOwner = process.env['OPEN_SANDBOX_FILE_OWNER'] ?? 'ubuntu';
const protocol = (process.env['OPEN_SANDBOX_PROTOCOL'] ?? 'http') as 'http' | 'https';
const apiKey = process.env['OPEN_SANDBOX_API_KEY'];
const s3Endpoint = process.env['S3_ENDPOINT'];
const s3Bucket = process.env['S3_BUCKET'];
const s3AccessKey = process.env['S3_ACCESS_KEY_ID'] ?? process.env['S3_ACCESS_KEY'];
const s3SecretKey = process.env['S3_SECRET_ACCESS_KEY'] ?? process.env['S3_SECRET_KEY'];
const resourceLimits = {
  cpu: process.env['OPEN_SANDBOX_RESOURCE_CPU'] ?? '1',
  memory: process.env['OPEN_SANDBOX_RESOURCE_MEMORY'] ?? '2Gi',
};

const skip = !domain || !image;
const skipReason = skip
  ? `OPEN_SANDBOX_DOMAIN${domain ? '' : ' (missing)'} OPEN_SANDBOX_IMAGE${image ? '' : ' (missing)'}`
  : '';

describe.skipIf(skip)(`OpenSandboxRuntime (live ${domain})`, () => {
  if (skip) {
    console.warn(`opensandbox-sandbox: skipping — ${skipReason}`);
  }

  it('write + read + list + bash round-trip in a real sandbox', async () => {
    const runtime = new OpenSandboxRuntime({
      domain: domain!,
      protocol,
      apiKey,
      image: image!,
      ...(entrypoint ? { entrypoint: [entrypoint] } : {}),
      timeoutSeconds: 1800,
      useServerProxy: true,
      requestTimeoutSeconds: 30,
      resourceLimits,
    });
    const sb = await runtime.acquire({
      fileUploadOwnership: { owner: fileOwner, group: fileOwner },
    });
    try {
      await sb.files.write('/tmp/orca-opensandbox.txt', Buffer.from('hello opensandbox'));
      const back = await sb.files.read('/tmp/orca-opensandbox.txt');
      expect(back.toString('utf8')).toBe('hello opensandbox');

      const entries = await sb.files.list('/tmp');
      expect(entries).toContain('orca-opensandbox.txt');

      const r = await sb.run({ tool: 'bash', args: { command: 'cat /tmp/orca-opensandbox.txt' } });
      expect(r.stdout?.trim()).toBe('hello opensandbox');
      const ownership = await sb.run({
        tool: 'bash',
        args: { command: "stat -c '%u:%g' /tmp/orca-opensandbox.txt" },
      });
      expect(ownership.stdout?.trim()).toBe('1000:1000');

      const fuse = await sb.runPrivileged(
        'test -c /dev/fuse && command -v s3fs && command -v fusermount3',
      );
      expect(fuse.exit_code, fuse.stderr).toBe(0);
    } finally {
      await sb.destroy();
    }
  }, 120_000);

  it.skipIf(!s3Endpoint || !s3Bucket || !s3AccessKey || !s3SecretKey)(
    'mounts S3 through s3fs without exposing credentials to the agent',
    async () => {
      const runtime = new OpenSandboxRuntime({
        domain: domain!,
        protocol,
        apiKey,
        image: image!,
        ...(entrypoint ? { entrypoint: [entrypoint] } : {}),
        timeoutSeconds: 1800,
        useServerProxy: true,
        requestTimeoutSeconds: 60,
        resourceLimits,
      });
      const sb = await runtime.acquire({
        fileUploadOwnership: { owner: fileOwner, group: fileOwner },
      });
      const nonce = `${Date.now()}-${process.pid}`;
      const mountPath = `/mnt/memory/orca-opensandbox-fuse-${nonce}`;
      const probeKey = `.orca-opensandbox-fuse-${nonce}`;
      const probeValue = `orca-fuse-${nonce}`;
      const credentialSentinel = `orca-credential-sentinel-${nonce}`;
      const mountCommand = buildS3fsMountCommand({
        bucketAndPrefix: s3Bucket!,
        mountPath,
        options: `url=${s3Endpoint},use_path_request_style,use_cache=,ensure_diskfree=0,allow_other,compat_dir,uid=1000,gid=1000,umask=0022`,
      });
      const mountEnvs = {
        ...buildS3fsCredentialEnvironment({
          accessKeyId: s3AccessKey!,
          secretAccessKey: s3SecretKey!,
        }),
        ORCA_CREDENTIAL_SENTINEL: credentialSentinel,
      };
      try {
        const mkdir = await sb.run({ tool: 'bash', args: { command: `mkdir -p '${mountPath}'` } });
        expect(mkdir.exit_code, mkdir.stderr).toBe(0);

        const firstMount = await sb.runPrivileged(mountCommand, { envs: mountEnvs });
        expect(firstMount.exit_code, firstMount.stderr).toBe(0);

        const write = await sb.run({
          tool: 'bash',
          args: { command: `printf '%s' '${probeValue}' > '${mountPath}/${probeKey}' && sync` },
        });
        expect(write.exit_code, write.stderr).toBe(0);

        const scan = await sb.run({
          tool: 'bash',
          args: {
            command: `test ! -e /root/.aws/credentials && test ! -e /run/orca-s3fs-credentials.lock && for f in /proc/[0-9]*/environ; do if [ -r "$f" ] && tr '\\0' '\\n' < "$f" | grep -Fq -- '${credentialSentinel}'; then exit 1; fi; done`,
          },
        });
        expect(scan.exit_code, scan.stderr).toBe(0);

        // Exercise uploaded files as the agent, not as execd root. This catches
        // a nested UID 1000 that is incorrectly mapped to the outer root user.
        const repoPath = `/workspace/repo-${nonce}`;
        const readonlyPath = `/workspace/reference-${nonce}.txt`;
        await sb.files.write(`${repoPath}/README.md`, Buffer.from('original'));
        await sb.files.write(readonlyPath, Buffer.from('read-only'));
        const agentSandbox = await createPolicyEnforcedSandbox(
          sb,
          buildSandboxWritePolicy([
            { path: repoPath, kind: 'github_repository', access: 'read_write' },
            { path: mountPath, kind: 'memory_store', access: 'read_write' },
            { path: readonlyPath, kind: 'file', access: 'read_only' },
          ]),
        );
        const resourceWrite = await agentSandbox.run({
          tool: 'bash',
          args: {
            command: [
              `test "$(stat -c '%u:%g' '${repoPath}/README.md')" = 1000:1000`,
              `printf appended >> '${repoPath}/README.md'`,
              `test "$(cat '${repoPath}/README.md')" = originalappended`,
              `printf new > '${repoPath}/new.txt'`,
              `rm '${repoPath}/new.txt'`,
              `printf '%s' '${probeValue}' > '${mountPath}/${probeKey}'`,
              'printf output > /mnt/session/outputs/identity-probe.txt',
              `! (printf denied >> '${readonlyPath}')`,
              `! chmod u+w '${readonlyPath}'`,
              '! (printf denied > /etc/orca-identity-probe)',
            ].join(' && '),
          },
        });
        expect(resourceWrite.exit_code, resourceWrite.stderr).toBe(0);
        expect((await sb.files.read(`${repoPath}/README.md`)).toString()).toBe('originalappended');
        expect((await sb.files.read(readonlyPath)).toString()).toBe('read-only');
        await sb.files.delete('/mnt/session/outputs/identity-probe.txt');
        await expect(agentSandbox.files.read('/proc/1/environ')).rejects.toThrow(/read denied/);
        const isolation = await agentSandbox.run({
          tool: 'bash',
          args: {
            command: [
              'test "$(id -u):$(id -g)" = 1000:1000',
              'test ! -e /dev/fuse',
              `awk '$1 == "CapEff:" { exit $2 == "0000000000000000" ? 0 : 1 }' /proc/self/status`,
              `awk '$1 == "CapBnd:" { exit $2 == "0000000000000000" ? 0 : 1 }' /proc/self/status`,
              `awk '$1 == "NoNewPrivs:" { exit $2 == "1" ? 0 : 1 }' /proc/self/status`,
              'test "$PPID" -eq 1',
              'if command -v sudo >/dev/null 2>&1 && sudo -n /bin/sh -c id >/dev/null 2>&1; then exit 1; fi',
              `! grep -R -F -- '${credentialSentinel}' /proc/[0-9]*/environ 2>/dev/null`,
            ].join(' && '),
          },
        });
        expect(isolation.exit_code, isolation.stderr).toBe(0);

        const firstUnmount = await sb.runPrivileged(`umount -- '${mountPath}'`);
        expect(firstUnmount.exit_code, firstUnmount.stderr).toBe(0);

        const secondMount = await sb.runPrivileged(mountCommand, { envs: mountEnvs });
        expect(secondMount.exit_code, secondMount.stderr).toBe(0);
        const readBack = await sb.run({
          tool: 'bash',
          args: {
            command: `test "$(cat '${mountPath}/${probeKey}')" = '${probeValue}' && rm -f '${mountPath}/${probeKey}' && sync`,
          },
        });
        expect(readBack.exit_code, readBack.stderr).toBe(0);
      } finally {
        await sb.runPrivileged(`umount -- '${mountPath}'`).catch(() => undefined);
        await sb.run({
          tool: 'bash',
          args: { command: `rmdir '${mountPath}' 2>/dev/null || true` },
        });
        await sb.destroy();
      }
    },
    180_000,
  );

  it('rejects pause and resume while gVisor FUSE mounts cannot be restored', async () => {
    const runtime = new OpenSandboxRuntime({
      domain: domain!,
      protocol,
      apiKey,
      image: image!,
      ...(entrypoint ? { entrypoint: [entrypoint] } : {}),
      timeoutSeconds: 1800,
      useServerProxy: true,
      requestTimeoutSeconds: 30,
      resourceLimits,
    });
    const sb = await runtime.acquire({});
    try {
      await expect(sb.pause()).rejects.toThrow(/pause is disabled for gVisor in-sandbox FUSE/);
      await expect(sb.resume()).rejects.toThrow(/resume is disabled for gVisor in-sandbox FUSE/);
    } finally {
      await sb.destroy();
    }
  }, 120_000);

  it('destroy is idempotent', async () => {
    const runtime = new OpenSandboxRuntime({
      domain: domain!,
      protocol,
      apiKey,
      image: image!,
      ...(entrypoint ? { entrypoint: [entrypoint] } : {}),
      timeoutSeconds: 1800,
      useServerProxy: true,
      requestTimeoutSeconds: 30,
      resourceLimits,
    });
    const sb = await runtime.acquire({});
    await sb.destroy();
    await sb.destroy(); // second destroy must not throw
  }, 120_000);
});
