// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { parseKafkaTranscriptConfig } from '../../../src/kafka/config.js';
import { KafkaTranscriptCodecError } from '../../../src/kafka/codec-error.js';

const registryEnv = { KAFKA_SCHEMA_REGISTRY_URL: 'https://registry.example/prefix/' };
const companions = {
  KAFKA_SCHEMA_REGISTRY_SUBJECT: 'custom.subject',
  KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER: 'false',
  KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'none',
  KAFKA_SCHEMA_REGISTRY_USERNAME: 'public',
  KAFKA_SCHEMA_REGISTRY_PASSWORD: 'secret-token',
  KAFKA_SCHEMA_REGISTRY_CA_FILE: '/private/ca.pem',
  KAFKA_SCHEMA_REGISTRY_CERT_FILE: '/private/cert.pem',
  KAFKA_SCHEMA_REGISTRY_KEY_FILE: '/private/key.pem',
  KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS: '5000',
};

function configurationError(
  env: Readonly<Record<string, string | undefined>>,
  backend?: string,
): Error {
  try {
    parseKafkaTranscriptConfig(env, backend);
  } catch (error) {
    expect(error).toBeInstanceOf(KafkaTranscriptCodecError);
    expect(error).toMatchObject({ code: 'configuration', retryable: false });
    return error as Error;
  }
  throw new Error('Expected configuration to be rejected');
}

describe('parseKafkaTranscriptConfig', () => {
  const directories: string[] = [];
  afterEach(() => {
    vi.unstubAllEnvs();
    for (const directory of directories.splice(0))
      rmSync(directory, { recursive: true, force: true });
  });

  it('given no codec settings, keeps raw encoding without consulting process.env', () => {
    vi.stubEnv('KAFKA_TRANSCRIPT_ENCODING', 'avro');
    vi.stubEnv('KAFKA_SCHEMA_REGISTRY_URL', registryEnv.KAFKA_SCHEMA_REGISTRY_URL);
    expect(parseKafkaTranscriptConfig(Object.freeze({}))).toEqual({ encoding: 'raw' });
  });

  it('treats empty env values as unset and leaves the readonly input unchanged', () => {
    const env = Object.freeze({
      KAFKA_TRANSCRIPT_ENCODING: '',
      KAFKA_SCHEMA_REGISTRY_URL: '',
      ...Object.fromEntries(Object.keys(companions).map((key) => [key, ''])),
    });
    expect(parseKafkaTranscriptConfig(env)).toEqual({ encoding: 'raw' });
    expect(env.KAFKA_TRANSCRIPT_ENCODING).toBe('');
  });

  it('given only a Registry URL, enables dual reading but retains raw writing', () => {
    expect(parseKafkaTranscriptConfig(registryEnv)).toEqual({
      encoding: 'raw',
      schemaRegistry: {
        url: registryEnv.KAFKA_SCHEMA_REGISTRY_URL,
        subject: 'orca.transcript.TranscriptEvent',
        autoRegister: true,
        requestTimeoutMs: 5000,
      },
    });
  });

  it('parses the explicit Avro writer and exact-schema lookup settings', () => {
    expect(
      parseKafkaTranscriptConfig(
        {
          ...registryEnv,
          KAFKA_TRANSCRIPT_ENCODING: 'avro',
          KAFKA_SCHEMA_REGISTRY_SUBJECT: 'deployment.TranscriptEvent',
          KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER: 'false',
          KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS: '1200',
        },
        'kafka',
      ),
    ).toEqual({
      encoding: 'avro',
      schemaRegistry: {
        url: registryEnv.KAFKA_SCHEMA_REGISTRY_URL,
        subject: 'deployment.TranscriptEvent',
        autoRegister: false,
        requestTimeoutMs: 1200,
      },
    });
  });

  it('accepts explicit auto registration and unauthenticated local HTTP', () => {
    expect(
      parseKafkaTranscriptConfig({
        KAFKA_SCHEMA_REGISTRY_URL: 'http://localhost:8081',
        KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'none',
        KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER: 'true',
      }).schemaRegistry,
    ).toMatchObject({ autoRegister: true });
  });

  it.each(['postgres', 'pulsar'])(
    'allows disabled codec settings for the %s backend',
    (backend) => {
      expect(parseKafkaTranscriptConfig({}, backend)).toEqual({ encoding: 'raw' });
      expect(parseKafkaTranscriptConfig({ KAFKA_TRANSCRIPT_ENCODING: 'raw' }, backend)).toEqual({
        encoding: 'raw',
      });
    },
  );

  it.each(['postgres', 'pulsar'])('rejects enabled Kafka settings on %s', (backend) => {
    configurationError(registryEnv, backend);
    configurationError({ KAFKA_TRANSCRIPT_ENCODING: 'avro' }, backend);
  });

  it('rejects Avro writing without a Registry', () => {
    configurationError({ KAFKA_TRANSCRIPT_ENCODING: 'avro' });
  });

  it.each(Object.entries(companions))('rejects %s without a Registry URL', (key, value) => {
    configurationError({ [key]: value });
  });

  it.each([
    ['KAFKA_TRANSCRIPT_ENCODING', 'json'],
    ['KAFKA_TRANSCRIPT_ENCODING', 'AVRO'],
    ['KAFKA_TRANSCRIPT_ENCODING', ' raw '],
    ['KAFKA_SCHEMA_REGISTRY_AUTH_MODE', 'bearer'],
    ['KAFKA_SCHEMA_REGISTRY_AUTH_MODE', 'Basic'],
    ['KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER', 'yes'],
    ['KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER', '1'],
    ['KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER', 'TRUE'],
  ])('rejects invalid enum %s=%s', (key, value) => {
    configurationError({ ...registryEnv, [key]: value });
  });

  it.each([
    '0',
    '-1',
    '1.5',
    '5000ms',
    'NaN',
    'Infinity',
    '1e3',
    '0x10',
    ' 5000 ',
    '9007199254740992',
  ])('rejects invalid request timeout %s', (value) => {
    configurationError({ ...registryEnv, KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS: value });
  });

  it('preserves Basic credentials verbatim and never reuses broker SASL credentials', () => {
    const env = {
      ...registryEnv,
      KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'basic',
      KAFKA_SCHEMA_REGISTRY_USERNAME: 'public',
      KAFKA_SCHEMA_REGISTRY_PASSWORD: ' raw.jwt.secret ',
      KAFKA_SASL_PASSWORD: 'token:different.jwt',
    };
    expect(parseKafkaTranscriptConfig(env).schemaRegistry?.auth).toEqual({
      username: 'public',
      password: ' raw.jwt.secret ',
    });
    expect(
      parseKafkaTranscriptConfig({ ...registryEnv, KAFKA_SASL_PASSWORD: 'token:jwt' })
        .schemaRegistry,
    ).not.toHaveProperty('auth');
    configurationError({ ...env, KAFKA_SCHEMA_REGISTRY_PASSWORD: undefined });
  });

  it.each([
    { KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'basic' },
    { KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'basic', KAFKA_SCHEMA_REGISTRY_USERNAME: 'public' },
    { KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'basic', KAFKA_SCHEMA_REGISTRY_PASSWORD: 'secret' },
    { KAFKA_SCHEMA_REGISTRY_USERNAME: 'public', KAFKA_SCHEMA_REGISTRY_PASSWORD: 'secret' },
    { KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'none', KAFKA_SCHEMA_REGISTRY_PASSWORD: 'secret' },
  ])('rejects incomplete or disabled Basic credentials %#', (env) => {
    configurationError({ ...registryEnv, ...env });
  });

  it.each([
    'not-a-url',
    'ftp://registry.example',
    'https://user:secret-token@registry.example',
    'https://registry.example?token=secret-token',
    'https://registry.example#secret-token',
  ])('delegates unsafe Registry URL validation without leaking input %#', (url) => {
    const error = configurationError({ KAFKA_SCHEMA_REGISTRY_URL: url });
    expect(error.stack).not.toContain(url);
    expect(error.stack).not.toContain('secret-token');
  });

  it('rejects Basic authentication over HTTP without exposing credentials', () => {
    const error = configurationError({
      KAFKA_SCHEMA_REGISTRY_URL: 'http://registry.example',
      KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'basic',
      KAFKA_SCHEMA_REGISTRY_USERNAME: 'private-user',
      KAFKA_SCHEMA_REGISTRY_PASSWORD: 'private-password',
    });
    expect(error.stack).not.toContain('private-user');
    expect(error.stack).not.toContain('private-password');
  });

  it('loads CA and paired mTLS files as Buffers', () => {
    const directory = mkdtempSync(join(tmpdir(), 'transcript-config-'));
    directories.push(directory);
    const env: Record<string, string> = { ...registryEnv };
    const expected: Record<string, Buffer> = {};
    for (const field of ['ca', 'cert', 'key']) {
      const path = join(directory, field);
      expected[field] = Buffer.from(`${field}-pem`);
      writeFileSync(path, expected[field]!);
      env[`KAFKA_SCHEMA_REGISTRY_${field.toUpperCase()}_FILE`] = path;
    }
    expect(parseKafkaTranscriptConfig(env).schemaRegistry?.tls).toEqual(expected);
    const caOnly = parseKafkaTranscriptConfig({
      ...registryEnv,
      KAFKA_SCHEMA_REGISTRY_CA_FILE: join(directory, 'ca'),
    });
    expect(caOnly.schemaRegistry?.tls).toEqual({ ca: expected.ca });
    configurationError({ ...env, KAFKA_SCHEMA_REGISTRY_URL: 'http://registry.example' });
  });

  it.each(['CERT', 'KEY'])('rejects an unpaired mTLS %s before trying to read it', (field) => {
    const error = configurationError({
      ...registryEnv,
      [`KAFKA_SCHEMA_REGISTRY_${field}_FILE`]: '/private/missing',
    });
    expect(error.message).toContain('CERT_FILE');
    expect(error.message).toContain('KEY_FILE');
  });

  it.each(['CA', 'CERT', 'KEY'])(
    'sanitizes unreadable %s file failures without attaching a raw cause',
    (field) => {
      const env = {
        ...registryEnv,
        [`KAFKA_SCHEMA_REGISTRY_${field}_FILE`]: '/private/secret-path/missing.pem',
      };
      if (field !== 'CA') {
        Object.assign(env, {
          KAFKA_SCHEMA_REGISTRY_CERT_FILE: '/private/secret-path/cert.pem',
          KAFKA_SCHEMA_REGISTRY_KEY_FILE: '/private/secret-path/key.pem',
        });
      }
      const error = configurationError(env);
      expect(error.stack).not.toContain('/private/secret-path');
      expect(error).not.toHaveProperty('cause');
      expect(error.message).toContain('KAFKA_SCHEMA_REGISTRY_');
    },
  );

  it('never includes invalid enum, timeout or backend values in errors', () => {
    for (const key of [
      'KAFKA_TRANSCRIPT_ENCODING',
      'KAFKA_SCHEMA_REGISTRY_AUTH_MODE',
      'KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER',
      'KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS',
    ]) {
      expect(configurationError({ ...registryEnv, [key]: 'secret-sentinel' }).stack).not.toContain(
        'secret-sentinel',
      );
    }
    expect(configurationError(registryEnv, 'secret-sentinel').stack).not.toContain(
      'secret-sentinel',
    );
  });
});
