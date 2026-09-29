// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface S3fsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
}

export interface S3fsMountCommandInput {
  bucketAndPrefix: string;
  mountPath: string;
  options: string;
}

const S3FS_MOUNT_HELPER = '/usr/local/bin/orca-s3fs-mount';
const PRIVILEGED_SAFE_PATH = '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin';

/**
 * Build transport envs for the short-lived privileged shell that starts s3fs.
 * The daemon itself is launched under `env -i`; these names are consumed only
 * while writing a root-only AWS credentials file and never reach its environ.
 */
export function buildS3fsCredentialEnvironment(creds: S3fsCredentials): Record<string, string> {
  const envs: Record<string, string> = {
    // Override shell startup hooks before execd/E2B invokes the root helper.
    // E2B sudo preserves only the three ORCA_S3_* values; PATH then falls
    // back to sudoers secure_path inside the helper.
    BASH_ENV: '/dev/null',
    ENV: '/dev/null',
    PATH: PRIVILEGED_SAFE_PATH,
    ORCA_S3_ACCESS_KEY_ID: creds.accessKeyId,
    ORCA_S3_SECRET_ACCESS_KEY: creds.secretAccessKey,
  };
  if (creds.sessionToken) envs['ORCA_S3_SESSION_TOKEN'] = creds.sessionToken;
  return envs;
}

/**
 * Start s3fs without leaving STS credentials in its long-lived process env.
 * Image-baked helper validates the mount target, creates a temporary AWS
 * profile, starts s3fs under a scrubbed environment, then removes the profile.
 */
export function buildS3fsMountCommand(input: S3fsMountCommandInput): string {
  return [
    S3FS_MOUNT_HELPER,
    shellQuote(input.bucketAndPrefix),
    shellQuote(input.mountPath),
    shellQuote(input.options),
  ].join(' ');
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}
