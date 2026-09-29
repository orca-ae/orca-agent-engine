// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import type { KafkaTranscriptCodecOptions } from './codec.js';
import { KafkaTranscriptCodecError } from './codec-error.js';
import { validateSchemaRegistryOptions } from './schema-registry.js';

const registryCompanions = [
  'KAFKA_SCHEMA_REGISTRY_SUBJECT',
  'KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER',
  'KAFKA_SCHEMA_REGISTRY_AUTH_MODE',
  'KAFKA_SCHEMA_REGISTRY_USERNAME',
  'KAFKA_SCHEMA_REGISTRY_PASSWORD',
  'KAFKA_SCHEMA_REGISTRY_CA_FILE',
  'KAFKA_SCHEMA_REGISTRY_CERT_FILE',
  'KAFKA_SCHEMA_REGISTRY_KEY_FILE',
  'KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS',
] as const;

// Messages contain only static setting names, never env values, paths or underlying errors.
function invalid(message: string): never {
  const error = new KafkaTranscriptCodecError('configuration');
  error.message = message;
  throw error;
}

function readTlsFile(path: string, setting: string): Buffer {
  try {
    return readFileSync(path);
  } catch {
    return invalid(`Unable to read ${setting}`);
  }
}

/** Parse only the supplied env map; broker credentials and process.env are intentionally unrelated. */
export function parseKafkaTranscriptConfig(
  env: Readonly<Record<string, string | undefined>>,
  backend = 'kafka',
): KafkaTranscriptCodecOptions {
  // Deployment templates commonly supply empty strings for disabled optional settings.
  const value = (key: string): string | undefined => (env[key] === '' ? undefined : env[key]);
  const encoding = value('KAFKA_TRANSCRIPT_ENCODING') ?? 'raw';
  if (encoding !== 'raw' && encoding !== 'avro') {
    invalid('KAFKA_TRANSCRIPT_ENCODING must be raw or avro');
  }

  const url = value('KAFKA_SCHEMA_REGISTRY_URL');
  const companion = registryCompanions.find((key) => value(key) !== undefined);
  if (backend !== 'kafka' && (encoding !== 'raw' || url !== undefined || companion !== undefined)) {
    invalid('Kafka transcript encoding and Schema Registry settings require the kafka backend');
  }
  if (url === undefined) {
    if (companion !== undefined) invalid(`${companion} requires KAFKA_SCHEMA_REGISTRY_URL`);
    if (encoding === 'avro')
      invalid('KAFKA_TRANSCRIPT_ENCODING=avro requires KAFKA_SCHEMA_REGISTRY_URL');
    return { encoding };
  }

  const autoRegister = value('KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER') ?? 'true';
  if (autoRegister !== 'true' && autoRegister !== 'false') {
    invalid('KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER must be true or false');
  }
  const authMode = value('KAFKA_SCHEMA_REGISTRY_AUTH_MODE') ?? 'none';
  if (authMode !== 'none' && authMode !== 'basic') {
    invalid('KAFKA_SCHEMA_REGISTRY_AUTH_MODE must be none or basic');
  }
  const username = value('KAFKA_SCHEMA_REGISTRY_USERNAME');
  const password = value('KAFKA_SCHEMA_REGISTRY_PASSWORD');
  if (authMode === 'basic' && (username === undefined || password === undefined)) {
    invalid(
      'Basic Schema Registry authentication requires KAFKA_SCHEMA_REGISTRY_USERNAME and KAFKA_SCHEMA_REGISTRY_PASSWORD',
    );
  }
  if (authMode === 'none' && (username !== undefined || password !== undefined)) {
    invalid('Schema Registry credentials require KAFKA_SCHEMA_REGISTRY_AUTH_MODE=basic');
  }

  const timeout = value('KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS') ?? '5000';
  const requestTimeoutMs = Number(timeout);
  if (!/^\d+$/.test(timeout) || !Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs <= 0) {
    invalid('KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS must be a positive safe integer');
  }
  const caFile = value('KAFKA_SCHEMA_REGISTRY_CA_FILE');
  const certFile = value('KAFKA_SCHEMA_REGISTRY_CERT_FILE');
  const keyFile = value('KAFKA_SCHEMA_REGISTRY_KEY_FILE');
  if ((certFile === undefined) !== (keyFile === undefined)) {
    invalid(
      'KAFKA_SCHEMA_REGISTRY_CERT_FILE and KAFKA_SCHEMA_REGISTRY_KEY_FILE must be configured together',
    );
  }

  const schemaRegistry: NonNullable<KafkaTranscriptCodecOptions['schemaRegistry']> = {
    url,
    subject: value('KAFKA_SCHEMA_REGISTRY_SUBJECT') ?? 'orca.transcript.TranscriptEvent',
    autoRegister: autoRegister === 'true',
    requestTimeoutMs,
    ...(authMode === 'basic' && username !== undefined && password !== undefined
      ? { auth: { username, password } }
      : {}),
    ...(caFile !== undefined || certFile !== undefined || keyFile !== undefined
      ? {
          tls: {
            ...(caFile !== undefined
              ? { ca: readTlsFile(caFile, 'KAFKA_SCHEMA_REGISTRY_CA_FILE') }
              : {}),
            ...(certFile !== undefined
              ? { cert: readTlsFile(certFile, 'KAFKA_SCHEMA_REGISTRY_CERT_FILE') }
              : {}),
            ...(keyFile !== undefined
              ? { key: readTlsFile(keyFile, 'KAFKA_SCHEMA_REGISTRY_KEY_FILE') }
              : {}),
          },
        }
      : {}),
  };
  validateSchemaRegistryOptions(schemaRegistry);
  return { encoding, schemaRegistry };
}
