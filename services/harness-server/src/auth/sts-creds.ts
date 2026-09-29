// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Execution-scoped S3 credential minter.
 *
 * Production credentials are constrained to one runner generation's output
 * prefix and to the explicitly attached memory stores. File blobs are not
 * exposed to the sandbox through these credentials.
 *
 * AssumeRole uses explicitly configured static source credentials when present,
 * otherwise the AWS default credential chain (including IRSA). When no STS role
 * is configured, local development can still use the same static S3 credentials
 * directly. That fallback cannot enforce the inline policy and must not be used
 * in production; a warning is emitted once per minter instance.
 */
import { AssumeRoleCommand, STSClient } from '@aws-sdk/client-sts';
import { createHash } from 'node:crypto';
import { harnessStsMintTotal } from '../metrics.js';

export interface SessionS3Creds {
  accessKeyId: string;
  secretAccessKey: string;
  /** Empty string when minted in dev-fallback mode (static IAM keys). */
  sessionToken: string;
  /** Unix-seconds when the creds expire. Set in the future for dev-fallback. */
  expiresAt: number;
}

export interface SessionCredsConfig {
  bucket: string;
  /** Root before `workspaces/{workspaceId}/sessions/...`; may be empty. */
  outputsRoot: string;
  /** Root before `workspaces/{workspaceId}/memory-stores/...`; may be empty. */
  memoryRoot: string;
  /** When set, AssumeRole; when unset, use the static dev credentials. */
  stsRoleArn?: string;
  /**
   * Optional source credentials for STS AssumeRole. When stsRoleArn is unset,
   * the same pair is used directly by the explicitly enabled dev fallback.
   * When omitted in AssumeRole mode, STS uses the AWS default credential chain.
   */
  staticAccessKey?: string;
  staticSecretKey?: string;
  /** Optional STS endpoint override, for example a local MinIO endpoint. */
  stsEndpoint?: string;
  /** Optional region override; defaults to "us-east-1". */
  region?: string;
}

export type MemoryStoreAccess = 'read_only' | 'read_write';

export interface MemoryStoreGrant {
  storeId: string;
  access: MemoryStoreAccess;
}

export interface MintInput {
  workspaceId: string;
  sessionId: string;
  generationId: string;
  memoryStores: MemoryStoreGrant[];
  /** Default 3600 = 1 hour. STS minimum is 900. */
  durationSeconds?: number;
}

const DEFAULT_DURATION_SECONDS = 3600;
const DEFAULT_REGION = 'us-east-1';
const ROLE_SESSION_NAME_MAX = 64;
const ROLE_SESSION_COMPONENT_MAX = 14;
const SAFE_ID_SEGMENT = /^[A-Za-z0-9_-]+$/;
const SAFE_ROOT_SEGMENT = /^[A-Za-z0-9._-]+$/;

function assertSafeIdSegment(name: string, value: unknown): asserts value is string {
  if (typeof value !== 'string' || !SAFE_ID_SEGMENT.test(value)) {
    throw new Error(`SessionCredsMinter: invalid ${name} object-key segment: "${String(value)}"`);
  }
}

/**
 * Normalize an operator-configured namespace root and reject ambiguous or IAM
 * wildcard-bearing segments. An empty root is valid.
 */
function normalizeObjectKeyRoot(name: string, root: string): string {
  if (typeof root !== 'string') {
    throw new Error(`SessionCredsMinter: invalid ${name}: "${String(root)}"`);
  }
  if (root === '') return '';
  if (root.startsWith('/') || root.includes('\\') || root.includes('\0')) {
    throw new Error(`SessionCredsMinter: invalid ${name}: "${root}"`);
  }

  const withoutTrailingSlash = root.endsWith('/') ? root.slice(0, -1) : root;
  const segments = withoutTrailingSlash.split('/');
  if (
    segments.length === 0 ||
    segments.some(
      (segment) =>
        segment.length === 0 ||
        segment === '.' ||
        segment === '..' ||
        !SAFE_ROOT_SEGMENT.test(segment),
    )
  ) {
    throw new Error(`SessionCredsMinter: invalid ${name}: "${root}"`);
  }
  return `${withoutTrailingSlash}/`;
}

/** Canonical S3 prefix shared by the STS policy and output FUSE mount. */
export function buildExecutionOutputPrefix(
  outputsRoot: string,
  input: Pick<MintInput, 'workspaceId' | 'sessionId' | 'generationId'>,
): string {
  assertSafeIdSegment('workspaceId', input.workspaceId);
  assertSafeIdSegment('sessionId', input.sessionId);
  assertSafeIdSegment('generationId', input.generationId);
  const root = normalizeObjectKeyRoot('outputsRoot', outputsRoot);
  return (
    `${root}workspaces/${input.workspaceId}/sessions/${input.sessionId}/` +
    `executions/${input.generationId}/outputs/`
  );
}

/** Canonical live prefix for one workspace-owned memory store. */
export function buildMemoryStoreLivePrefix(
  memoryRoot: string,
  workspaceId: string,
  storeId: string,
): string {
  assertSafeIdSegment('workspaceId', workspaceId);
  assertSafeIdSegment('storeId', storeId);
  const root = normalizeObjectKeyRoot('memoryRoot', memoryRoot);
  return `${root}workspaces/${workspaceId}/memory-stores/${storeId}/live/`;
}

function validateMintInput(input: MintInput): void {
  assertSafeIdSegment('workspaceId', input.workspaceId);
  assertSafeIdSegment('sessionId', input.sessionId);
  assertSafeIdSegment('generationId', input.generationId);
  if (!Array.isArray(input.memoryStores)) {
    throw new Error('SessionCredsMinter: memoryStores must be an array');
  }

  const storeIds = new Set<string>();
  for (const grant of input.memoryStores) {
    if (grant === null || typeof grant !== 'object') {
      throw new Error('SessionCredsMinter: invalid memory store grant');
    }
    assertSafeIdSegment('storeId', grant.storeId);
    if (grant.access !== 'read_only' && grant.access !== 'read_write') {
      throw new Error(
        `SessionCredsMinter: invalid memory store access for "${grant.storeId}": "${String(grant.access)}"`,
      );
    }
    if (storeIds.has(grant.storeId)) {
      throw new Error(`SessionCredsMinter: duplicate memory store grant: "${grant.storeId}"`);
    }
    storeIds.add(grant.storeId);
  }
}

/** Build the exact execution + memory grants accepted by STS AssumeRole. */
function buildPolicyDocument(config: SessionCredsConfig, input: MintInput): string {
  const outputGlob = `${buildExecutionOutputPrefix(config.outputsRoot, input)}*`;
  const memoryGrants = input.memoryStores.map((grant) => ({
    ...grant,
    glob: `${buildMemoryStoreLivePrefix(config.memoryRoot, input.workspaceId, grant.storeId)}*`,
  }));
  const readMemoryResources = memoryGrants.map(
    ({ glob }) => `arn:aws:s3:::${config.bucket}/${glob}`,
  );
  const writeMemoryResources = memoryGrants
    .filter(({ access }) => access === 'read_write')
    .map(({ glob }) => `arn:aws:s3:::${config.bucket}/${glob}`);

  const statements: object[] = [
    {
      Sid: 'ReadWriteExecutionOutputs',
      Effect: 'Allow',
      Action: ['s3:GetObject', 's3:PutObject', 's3:DeleteObject'],
      Resource: [`arn:aws:s3:::${config.bucket}/${outputGlob}`],
    },
  ];

  if (readMemoryResources.length > 0) {
    statements.push({
      Sid: 'ReadMemoryStores',
      Effect: 'Allow',
      Action: ['s3:GetObject'],
      Resource: readMemoryResources,
    });
  }
  if (writeMemoryResources.length > 0) {
    statements.push({
      Sid: 'WriteMemoryStores',
      Effect: 'Allow',
      Action: ['s3:PutObject', 's3:DeleteObject'],
      Resource: writeMemoryResources,
    });
  }

  statements.push({
    Sid: 'ListExecutionAndMemoryPrefixes',
    Effect: 'Allow',
    Action: ['s3:ListBucket'],
    Resource: [`arn:aws:s3:::${config.bucket}`],
    Condition: {
      StringLike: {
        's3:prefix': [outputGlob, ...memoryGrants.map(({ glob }) => glob)],
      },
    },
  });

  return JSON.stringify({ Version: '2012-10-17', Statement: statements });
}

function buildRoleSessionName(
  workspaceId: string,
  generationId: string,
  sessionId: string,
): string {
  const digest = createHash('sha256')
    .update(`${workspaceId}\0${generationId}\0${sessionId}`)
    .digest('hex')
    .slice(0, 12);
  const name = [
    'orca',
    workspaceId.slice(0, ROLE_SESSION_COMPONENT_MAX),
    generationId.slice(0, ROLE_SESSION_COMPONENT_MAX),
    sessionId.slice(0, ROLE_SESSION_COMPONENT_MAX),
    digest,
  ].join('-');
  return name.slice(0, ROLE_SESSION_NAME_MAX);
}

export class SessionCredsMinter {
  private readonly config: SessionCredsConfig;
  private readonly stsClient: STSClient | null;
  private warnedOnceInDev = false;

  constructor(config: SessionCredsConfig) {
    this.config = {
      ...config,
      outputsRoot: normalizeObjectKeyRoot('outputsRoot', config.outputsRoot),
      memoryRoot: normalizeObjectKeyRoot('memoryRoot', config.memoryRoot),
    };

    const hasStaticAccessKey = Boolean(config.staticAccessKey);
    const hasStaticSecretKey = Boolean(config.staticSecretKey);
    if (hasStaticAccessKey !== hasStaticSecretKey) {
      throw new Error(
        'SessionCredsMinter: staticAccessKey and staticSecretKey must be configured together',
      );
    }

    if (config.stsRoleArn) {
      const clientOpts: ConstructorParameters<typeof STSClient>[0] = {
        region: config.region ?? DEFAULT_REGION,
      };
      if (config.stsEndpoint !== undefined) clientOpts.endpoint = config.stsEndpoint;
      if (config.staticAccessKey && config.staticSecretKey) {
        clientOpts.credentials = {
          accessKeyId: config.staticAccessKey,
          secretAccessKey: config.staticSecretKey,
        };
      }
      this.stsClient = new STSClient(clientOpts);
    } else {
      if (!hasStaticAccessKey || !hasStaticSecretKey) {
        throw new Error(
          'SessionCredsMinter: when stsRoleArn is unset, both staticAccessKey and staticSecretKey must be provided',
        );
      }
      this.stsClient = null;
    }
  }

  async mint(input: MintInput): Promise<SessionS3Creds> {
    validateMintInput(input);
    const durationSeconds = input.durationSeconds ?? DEFAULT_DURATION_SECONDS;

    if (!this.config.stsRoleArn) {
      const creds = this.mintDevFallback(durationSeconds);
      harnessStsMintTotal.inc({ result: 'dev_fallback' });
      return creds;
    }

    try {
      const creds = await this.mintWithSts(input, durationSeconds);
      harnessStsMintTotal.inc({ result: 'ok' });
      return creds;
    } catch (err) {
      harnessStsMintTotal.inc({ result: 'error' });
      throw err;
    }
  }

  private mintDevFallback(durationSeconds: number): SessionS3Creds {
    if (!this.warnedOnceInDev) {
      this.warnedOnceInDev = true;
      console.warn('SessionCredsMinter: STS_ROLE_ARN unset — using static IAM keys. DEV ONLY.');
    }

    return {
      accessKeyId: this.config.staticAccessKey!,
      secretAccessKey: this.config.staticSecretKey!,
      sessionToken: '',
      expiresAt: Math.floor(Date.now() / 1000) + durationSeconds,
    };
  }

  private async mintWithSts(input: MintInput, durationSeconds: number): Promise<SessionS3Creds> {
    const command = new AssumeRoleCommand({
      RoleArn: this.config.stsRoleArn,
      RoleSessionName: buildRoleSessionName(input.workspaceId, input.generationId, input.sessionId),
      DurationSeconds: durationSeconds,
      Policy: buildPolicyDocument(this.config, input),
    });

    const response = await this.stsClient!.send(command);
    const creds = response.Credentials;
    if (!creds || !creds.AccessKeyId || !creds.SecretAccessKey || !creds.Expiration) {
      throw new Error('SessionCredsMinter: STS AssumeRole returned incomplete credentials');
    }

    return {
      accessKeyId: creds.AccessKeyId,
      secretAccessKey: creds.SecretAccessKey,
      sessionToken: creds.SessionToken ?? '',
      expiresAt: Math.floor(creds.Expiration.getTime() / 1000),
    };
  }
}
