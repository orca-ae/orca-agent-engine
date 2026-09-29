// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { AssumeRoleCommand, STSClient } from '@aws-sdk/client-sts';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  buildExecutionOutputPrefix,
  SessionCredsMinter,
  type MemoryStoreAccess,
  type MintInput,
  type SessionCredsConfig,
} from '../../src/auth/sts-creds.js';

const stsMock = mockClient(STSClient);
const ROLE_ARN = 'arn:aws:iam::123456789012:role/orca-harness';

function config(overrides: Partial<SessionCredsConfig> = {}): SessionCredsConfig {
  return {
    bucket: 'orca-files',
    outputsRoot: 'outputs/',
    memoryRoot: 'memory/',
    stsRoleArn: ROLE_ARN,
    ...overrides,
  };
}

function input(overrides: Partial<MintInput> = {}): MintInput {
  return {
    workspaceId: 'ws_alpha',
    sessionId: 'ses_beta',
    generationId: 'run_gamma',
    memoryStores: [],
    ...overrides,
  };
}

function configureSuccessfulAssumeRole(opts?: { expiration?: Date; sessionToken?: string }) {
  stsMock.on(AssumeRoleCommand).resolves({
    Credentials: {
      AccessKeyId: 'ASIAEXAMPLE',
      SecretAccessKey: 'secretFromSts',
      SessionToken: opts?.sessionToken ?? 'token-from-sts',
      Expiration: opts?.expiration ?? new Date(Date.now() + 3600 * 1000),
    },
  });
}

function assumeRoleInput() {
  const calls = stsMock.commandCalls(AssumeRoleCommand);
  expect(calls).toHaveLength(1);
  return calls[0]!.args[0].input;
}

describe('SessionCredsMinter', () => {
  beforeEach(() => {
    stsMock.reset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('production STS policy', () => {
    it('grants only one execution output prefix and the explicitly attached memory stores', async () => {
      configureSuccessfulAssumeRole();
      const minter = new SessionCredsMinter(config());

      await minter.mint(
        input({
          memoryStores: [
            { storeId: 'mem_ro', access: 'read_only' },
            { storeId: 'mem_rw', access: 'read_write' },
          ],
        }),
      );

      const args = assumeRoleInput();
      expect(args.RoleArn).toBe(ROLE_ARN);
      expect(args.DurationSeconds).toBe(3600);
      expect(args.RoleSessionName).toMatch(/^orca-ws_alpha-run_gamma-ses_beta-[0-9a-f]{12}$/);
      expect(args.RoleSessionName!.length).toBeLessThanOrEqual(64);

      const policy = JSON.parse(args.Policy!);
      expect(policy).toEqual({
        Version: '2012-10-17',
        Statement: [
          {
            Sid: 'ReadWriteExecutionOutputs',
            Effect: 'Allow',
            Action: ['s3:GetObject', 's3:PutObject', 's3:DeleteObject'],
            Resource: [
              'arn:aws:s3:::orca-files/outputs/workspaces/ws_alpha/sessions/ses_beta/' +
                'executions/run_gamma/outputs/*',
            ],
          },
          {
            Sid: 'ReadMemoryStores',
            Effect: 'Allow',
            Action: ['s3:GetObject'],
            Resource: [
              'arn:aws:s3:::orca-files/memory/workspaces/ws_alpha/memory-stores/mem_ro/live/*',
              'arn:aws:s3:::orca-files/memory/workspaces/ws_alpha/memory-stores/mem_rw/live/*',
            ],
          },
          {
            Sid: 'WriteMemoryStores',
            Effect: 'Allow',
            Action: ['s3:PutObject', 's3:DeleteObject'],
            Resource: [
              'arn:aws:s3:::orca-files/memory/workspaces/ws_alpha/memory-stores/mem_rw/live/*',
            ],
          },
          {
            Sid: 'ListExecutionAndMemoryPrefixes',
            Effect: 'Allow',
            Action: ['s3:ListBucket'],
            Resource: ['arn:aws:s3:::orca-files'],
            Condition: {
              StringLike: {
                's3:prefix': [
                  'outputs/workspaces/ws_alpha/sessions/ses_beta/' +
                    'executions/run_gamma/outputs/*',
                  'memory/workspaces/ws_alpha/memory-stores/mem_ro/live/*',
                  'memory/workspaces/ws_alpha/memory-stores/mem_rw/live/*',
                ],
              },
            },
          },
        ],
      });

      expect(args.Policy).not.toContain('ReadWorkspaceBlobs');
      expect(args.Policy).not.toContain('/files/blobs/');
      expect(args.Policy).not.toContain('/memory-stores/*');
    });

    it('omits memory object statements when no memory store is attached', async () => {
      configureSuccessfulAssumeRole();
      const minter = new SessionCredsMinter(config());

      await minter.mint(input());

      const policy = JSON.parse(assumeRoleInput().Policy!);
      expect(policy.Statement.map((statement: { Sid: string }) => statement.Sid)).toEqual([
        'ReadWriteExecutionOutputs',
        'ListExecutionAndMemoryPrefixes',
      ]);
      expect(policy.Statement[1].Condition.StringLike['s3:prefix']).toEqual([
        'outputs/workspaces/ws_alpha/sessions/ses_beta/executions/run_gamma/outputs/*',
      ]);
    });

    it('isolates different generations of the same session', async () => {
      configureSuccessfulAssumeRole();
      const minter = new SessionCredsMinter(config());

      await minter.mint(input({ generationId: 'run_first' }));
      const firstPolicy = JSON.parse(
        stsMock.commandCalls(AssumeRoleCommand)[0]!.args[0].input.Policy!,
      );
      await minter.mint(input({ generationId: 'run_second' }));
      const secondPolicy = JSON.parse(
        stsMock.commandCalls(AssumeRoleCommand)[1]!.args[0].input.Policy!,
      );

      expect(firstPolicy.Statement[0].Resource[0]).toContain('/executions/run_first/outputs/*');
      expect(secondPolicy.Statement[0].Resource[0]).toContain('/executions/run_second/outputs/*');
      expect(firstPolicy.Statement[0].Resource).not.toEqual(secondPolicy.Statement[0].Resource);
    });

    it('scopes policies and audit session names to one workspace', async () => {
      configureSuccessfulAssumeRole();
      const minter = new SessionCredsMinter(config());

      await minter.mint(input());
      await minter.mint(input({ workspaceId: 'ws_bravo' }));

      const calls = stsMock.commandCalls(AssumeRoleCommand);
      const first = calls[0]!.args[0].input;
      const second = calls[1]!.args[0].input;
      expect(first.Policy).toContain('/workspaces/ws_alpha/');
      expect(first.Policy).not.toContain('/workspaces/ws_bravo/');
      expect(second.Policy).toContain('/workspaces/ws_bravo/');
      expect(second.Policy).not.toContain('/workspaces/ws_alpha/');
      expect(first.RoleSessionName).toContain('-ws_alpha-');
      expect(second.RoleSessionName).toContain('-ws_bravo-');
      expect(first.RoleSessionName).not.toBe(second.RoleSessionName);
    });

    it('normalizes configured roots before building the policy', async () => {
      configureSuccessfulAssumeRole();
      const minter = new SessionCredsMinter(
        config({ outputsRoot: 'tenant/outputs', memoryRoot: 'tenant/memory' }),
      );

      await minter.mint(input({ memoryStores: [{ storeId: 'mem_a', access: 'read_only' }] }));

      const policy = JSON.parse(assumeRoleInput().Policy!);
      expect(policy.Statement[0].Resource).toEqual([
        'arn:aws:s3:::orca-files/tenant/outputs/workspaces/ws_alpha/sessions/ses_beta/' +
          'executions/run_gamma/outputs/*',
      ]);
      expect(policy.Statement[1].Resource).toEqual([
        'arn:aws:s3:::orca-files/tenant/memory/workspaces/ws_alpha/memory-stores/mem_a/live/*',
      ]);
    });

    it('honors durationSeconds and maps the returned STS credentials', async () => {
      const expiration = new Date('2030-01-01T00:00:00Z');
      configureSuccessfulAssumeRole({ expiration, sessionToken: 'sts-token' });
      const minter = new SessionCredsMinter(config());

      const creds = await minter.mint(input({ durationSeconds: 900 }));

      expect(assumeRoleInput().DurationSeconds).toBe(900);
      expect(creds).toEqual({
        accessKeyId: 'ASIAEXAMPLE',
        secretAccessKey: 'secretFromSts',
        sessionToken: 'sts-token',
        expiresAt: Math.floor(expiration.getTime() / 1000),
      });
    });

    it('rejects incomplete credentials returned by STS', async () => {
      stsMock.on(AssumeRoleCommand).resolves({});
      const minter = new SessionCredsMinter(config());

      await expect(minter.mint(input())).rejects.toThrow(/incomplete credentials/);
    });
  });

  describe('object-key validation', () => {
    it.each([
      ['workspaceId', { workspaceId: 'ws/escape' }],
      ['sessionId', { sessionId: 'ses*' }],
      ['generationId', { generationId: '../run' }],
    ])('rejects an unsafe %s before calling STS', async (_name, overrides) => {
      const minter = new SessionCredsMinter(config());

      await expect(minter.mint(input(overrides))).rejects.toThrow(/object-key segment/);
      expect(stsMock.calls()).toHaveLength(0);
    });

    it('rejects unsafe, duplicate, and invalid-access memory grants', async () => {
      const minter = new SessionCredsMinter(config());

      await expect(
        minter.mint(input({ memoryStores: [{ storeId: 'mem/escape', access: 'read_only' }] })),
      ).rejects.toThrow(/storeId object-key segment/);
      await expect(
        minter.mint(
          input({
            memoryStores: [
              { storeId: 'mem_a', access: 'read_only' },
              { storeId: 'mem_a', access: 'read_write' },
            ],
          }),
        ),
      ).rejects.toThrow(/duplicate memory store grant/);
      await expect(
        minter.mint(
          input({
            memoryStores: [{ storeId: 'mem_a', access: 'owner' as unknown as MemoryStoreAccess }],
          }),
        ),
      ).rejects.toThrow(/invalid memory store access/);
      expect(stsMock.calls()).toHaveLength(0);
    });

    it('does not accept the old session-only mint input', async () => {
      const minter = new SessionCredsMinter(config());
      const oldInput = { workspaceId: 'ws_alpha', sessionId: 'ses_beta' } as MintInput;

      await expect(minter.mint(oldInput)).rejects.toThrow(/generationId object-key segment/);
      expect(stsMock.calls()).toHaveLength(0);
    });

    it.each([
      ['outputsRoot', { outputsRoot: '../outputs' }],
      ['outputsRoot', { outputsRoot: 'outputs/*/' }],
      ['memoryRoot', { memoryRoot: '/memory/' }],
      ['memoryRoot', { memoryRoot: 'memory//private' }],
    ])('rejects an unsafe %s at construction', (name, overrides) => {
      expect(() => new SessionCredsMinter(config(overrides))).toThrow(
        new RegExp(`invalid ${name}`),
      );
    });

    it('uses the same canonical output prefix builder exported to the mount layer', () => {
      expect(buildExecutionOutputPrefix('outputs', input())).toBe(
        'outputs/workspaces/ws_alpha/sessions/ses_beta/executions/run_gamma/outputs/',
      );
      expect(() =>
        buildExecutionOutputPrefix('outputs/', input({ sessionId: 'ses/other' })),
      ).toThrow(/sessionId object-key segment/);
    });
  });

  describe('dev fallback', () => {
    it('returns static credentials, warns once, and still validates execution scope', async () => {
      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
      const minter = new SessionCredsMinter(
        config({
          stsRoleArn: undefined,
          staticAccessKey: 'AKIAFAKEDEV',
          staticSecretKey: 'dev-secret',
        }),
      );
      const before = Math.floor(Date.now() / 1000);

      const creds = await minter.mint(input({ durationSeconds: 1800 }));
      await minter.mint(input({ generationId: 'run_second' }));

      expect(creds.accessKeyId).toBe('AKIAFAKEDEV');
      expect(creds.secretAccessKey).toBe('dev-secret');
      expect(creds.sessionToken).toBe('');
      expect(creds.expiresAt).toBeGreaterThanOrEqual(before + 1800);
      expect(warn).toHaveBeenCalledTimes(1);
      await expect(minter.mint(input({ workspaceId: '../escape' }))).rejects.toThrow(
        /workspaceId object-key segment/,
      );
      expect(stsMock.calls()).toHaveLength(0);
    });
  });

  describe('constructor validation', () => {
    it('uses configured static credentials as the STS AssumeRole source identity', async () => {
      const minter = new SessionCredsMinter(
        config({ staticAccessKey: 'AKIASTS', staticSecretKey: 'sts-source-secret' }),
      );
      const stsClient = (minter as unknown as { stsClient: STSClient }).stsClient;

      await expect(stsClient.config.credentials()).resolves.toMatchObject({
        accessKeyId: 'AKIASTS',
        secretAccessKey: 'sts-source-secret',
      });
    });

    it('requires both static credentials when no STS role is configured', () => {
      expect(
        () =>
          new SessionCredsMinter(
            config({ stsRoleArn: undefined, staticAccessKey: 'AKIA', staticSecretKey: undefined }),
          ),
      ).toThrow(/staticAccessKey and staticSecretKey/);
    });

    it('rejects a partial static source credential pair when an STS role is configured', () => {
      expect(
        () =>
          new SessionCredsMinter(config({ staticAccessKey: 'AKIA', staticSecretKey: undefined })),
      ).toThrow(/staticAccessKey and staticSecretKey/);
    });

    it('does not require static credentials when an STS role is configured', () => {
      expect(() => new SessionCredsMinter(config())).not.toThrow();
    });
  });
});
