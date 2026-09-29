// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  buildS3fsCredentialEnvironment,
  buildS3fsMountCommand,
} from '../../src/sandbox/s3fs-mount.js';

const e2bStartUrl = new URL('../../sandbox-templates/orca-default/start.sh', import.meta.url);

function runE2bStart(forcePathStyle: string): string {
  return execFileSync(
    '/bin/bash',
    [
      '-c',
      `
mountpoint() { return 1; }
sudo() { printf 'sudo %s\\n' "$*"; }
source "$1"
`,
      'bash',
      fileURLToPath(e2bStartUrl),
    ],
    {
      encoding: 'utf8',
      env: {
        ...process.env,
        ORCA_FUSE_OUTPUTS_ENABLE: '1',
        S3_BUCKET: 'orca-files',
        S3_ENDPOINT: 'https://s3.example.com',
        SESSION_OUTPUT_PREFIX: 'outputs/test',
        AWS_ACCESS_KEY_ID: 'test-access',
        AWS_SECRET_ACCESS_KEY: 'test-secret',
        S3_FORCE_PATH_STYLE: forcePathStyle,
      },
    },
  );
}

describe('s3fs mount credential isolation', () => {
  it('delegates to the image-baked constrained mount helper', () => {
    const command = buildS3fsMountCommand({
      bucketAndPrefix: 'orca-files:/prefix/',
      mountPath: '/mnt/memory/store',
      options: 'allow_other,url=http://minio:9000',
    });

    expect(command).toContain('/usr/local/bin/orca-s3fs-mount');
    expect(command).toContain("'orca-files:/prefix/'");
    expect(command).toContain("'/mnt/memory/store'");
    expect(command).not.toContain('actual-access-key');
    expect(command).not.toContain('actual-secret-key');
  });

  it('scrubs the daemon environment and removes the temporary profile', async () => {
    const helper = await readFile(
      new URL('../../sandbox-templates/orca-default/orca-s3fs-mount', import.meta.url),
      'utf8',
    );

    expect(helper).toContain("printf 'aws_session_token = %s\\n'");
    expect(helper).toContain('} >"${credentials_target}"');
    expect(helper).toContain('env -i PATH="${PATH}" HOME=/root');
    expect(helper).toContain('-o "${options},profile=orca-session"');
    expect(helper).toContain(': >"${credentials_target}"');
    expect(helper).toContain('rm -f "${credentials_target}"');
    expect(helper).toContain('refusing to replace existing ${credentials_target}');
    expect(helper).toContain('unsupported s3fs option');
    expect(helper).toContain('#!/bin/bash -p');
    expect(helper).toContain('readonly SAFE_PATH=');
    expect(helper).toContain('non-canonical s3fs prefix');
    expect(helper).toContain('/mnt/session/outputs | /mnt/memory/?*');
    expect(helper).not.toContain('/mnt/session/outputs | /mnt/*');
    expect(helper).not.toContain('mount --bind');
    expect(helper).not.toContain('credlib=');
  });

  it('packages the helper in both OpenSandbox and E2B images', async () => {
    const [openSandboxDockerfile, e2bDockerfile, e2bStart] = await Promise.all([
      readFile(
        new URL('../../sandbox-templates/orca-opensandbox/Dockerfile', import.meta.url),
        'utf8',
      ),
      readFile(
        new URL('../../sandbox-templates/orca-default/e2b.Dockerfile', import.meta.url),
        'utf8',
      ),
      readFile(new URL('../../sandbox-templates/orca-default/start.sh', import.meta.url), 'utf8'),
    ]);

    expect(openSandboxDockerfile).toContain('orca-s3fs-mount');
    expect(openSandboxDockerfile).toMatch(
      /^FROM docker\.io\/opensandbox\/code-interpreter:[^@\s]+@sha256:[0-9a-f]{64}$/m,
    );
    expect(openSandboxDockerfile).toContain("s3fs --help 2>&1 | grep -F 'compat_dir'");
    expect(openSandboxDockerfile).toContain('id -u ubuntu');
    expect(e2bDockerfile).toContain('orca-s3fs-mount');
    expect(e2bDockerfile).toMatch(/^FROM e2bdev\/code-interpreter:[^@\s]+@sha256:[0-9a-f]{64}$/m);
    expect(e2bDockerfile).toContain("s3fs --help 2>&1 | grep -F 'compat_dir'");
    expect(e2bDockerfile).toContain('id -u user');
    expect(e2bDockerfile).toContain(
      'env_keep += "ORCA_S3_ACCESS_KEY_ID ORCA_S3_SECRET_ACCESS_KEY ORCA_S3_SESSION_TOKEN"',
    );
    expect(e2bDockerfile).toContain("'user ALL=(root) NOPASSWD: /usr/local/bin/orca-s3fs-mount'");
    expect(e2bDockerfile).toContain("'user ALL=(root) NOPASSWD: /usr/bin/umount, /bin/umount'");
    expect(e2bDockerfile).not.toContain('NOPASSWD:SETENV');
    expect(e2bDockerfile).toContain('chown -R user:user /mnt/inputs /mnt/memory /mnt/session');
    expect(e2bDockerfile).toContain(
      'git config --system credential.helper /usr/local/bin/orca-git-creds',
    );
    expect(e2bDockerfile).toContain("sed -E -i '/^[[:space:]]*user");
    expect(e2bDockerfile).toMatch(/USER user\s*$/);
    expect(e2bDockerfile).not.toContain('sudo git config');
    expect(e2bStart).toContain('/usr/local/bin/orca-s3fs-mount');
    expect(e2bStart).toContain('S3_FORCE_PATH_STYLE');
    expect(e2bStart).toContain('path_style_opt="use_path_request_style,"');
    expect(e2bStart).not.toContain('credential.helper');
    expect(e2bStart).not.toContain('ORCA_GIT_CREDS_');
    expect(e2bStart).not.toContain('.passwd-s3fs');
  });

  it('parses mixed-case path-style values consistently with service config', () => {
    expect(runE2bStart('TRUE')).toContain('use_path_request_style');
    expect(runE2bStart('False')).not.toContain('use_path_request_style');
  });

  it('omits an empty session token from transport envs', () => {
    expect(
      buildS3fsCredentialEnvironment({
        accessKeyId: 'actual-access-key',
        secretAccessKey: 'actual-secret-key',
        sessionToken: '',
      }),
    ).toEqual({
      BASH_ENV: '/dev/null',
      ENV: '/dev/null',
      PATH: '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
      ORCA_S3_ACCESS_KEY_ID: 'actual-access-key',
      ORCA_S3_SECRET_ACCESS_KEY: 'actual-secret-key',
    });
  });
});
