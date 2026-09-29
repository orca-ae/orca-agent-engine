// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { describe, expect, it } from 'vitest';

const { load } = createRequire(import.meta.url)('js-yaml');
const root = new URL('../../../../../', import.meta.url);
const read = (path: string) => readFileSync(new URL(path, root), 'utf8');
const compose = load(read('services/dev/docker-compose.transcript-avro.yml'));
const runner = read('services/dev/scripts/test-transcript-avro.sh');
const workflow = load(read('.github/workflows/test-transcript-avro.yml'));

describe('isolated Avro Schema Registry fixture', () => {
  it('runs exactly an independent Apache Kafka broker and Schema Registry', () => {
    expect(Object.keys(compose.services).sort()).toEqual(['kafka', 'schema-registry']);
    expect(compose.services.kafka.image).toBe(
      'apache/kafka:3.7.1@sha256:ed74d7d115968d5e8b00ba6822ac6a384cbaaf54ca38991828647000d7089b68',
    );
    expect(compose.services['schema-registry'].image).toBe(
      'confluentinc/cp-schema-registry:7.7.1@sha256:e8c6de70b76f78d33238f5f972da17fc93e5b96b00d08ffed07d9f5f03fbc1ce',
    );
  });

  it('uses single-broker KRaft with internal and loopback host listeners', () => {
    const kafka = compose.services.kafka;
    expect(kafka.environment).toMatchObject({
      KAFKA_NODE_ID: 1,
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093',
      KAFKA_LISTENERS:
        'INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:19092,CONTROLLER://0.0.0.0:29093',
      KAFKA_ADVERTISED_LISTENERS: 'INTERNAL://kafka:29092,EXTERNAL://127.0.0.1:19092',
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
        'INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
      KAFKA_INTER_BROKER_LISTENER_NAME: 'INTERNAL',
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1,
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1,
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1,
    });
    expect(kafka.ports).toEqual(['127.0.0.1:19092:19092']);
    const registry = compose.services['schema-registry'];
    expect(registry.environment).toMatchObject({
      SCHEMA_REGISTRY_HOST_NAME: 'schema-registry',
      SCHEMA_REGISTRY_LISTENERS: 'http://0.0.0.0:8081',
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'PLAINTEXT://kafka:29092',
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 1,
    });
    expect(registry.ports).toEqual(['127.0.0.1:18081:8081']);
    expect(registry.depends_on).toEqual({ kafka: { condition: 'service_healthy' } });
  });

  it('bounds readiness and memory for both services', () => {
    for (const key of ['kafka', 'schema-registry']) {
      const service = compose.services[key];
      expect(service.mem_limit).toMatch(/^[1-2]g$/);
      expect(service.healthcheck.test.length).toBeGreaterThan(1);
      expect(service.healthcheck.interval).toBe('5s');
      expect(service.healthcheck.timeout).toBe('5s');
      expect(service.healthcheck.retries).toBe(30);
    }
  });

  it('runs the schema registry integration with isolated cleanup and a KEEP override', () => {
    expect(runner).toContain(
      'node packages/transcript-store/test/integration/avro-schema-registry.mjs',
    );
    expect(runner).toContain(
      'docker compose -p orca-transcript-avro -f services/dev/docker-compose.transcript-avro.yml',
    );
    expect(runner).toContain('up -d --wait --wait-timeout 180');
    expect(runner).toContain('trap cleanup EXIT');
    expect(runner).toContain('${KEEP_TRANSCRIPT_AVRO:-0}');
    expect(runner).toContain('down --volumes --remove-orphans');
    expect(Object.keys(workflow.jobs).sort()).toEqual(['avro-schema-registry', 'changes']);
    const job = workflow.jobs['avro-schema-registry'];
    expect(job['timeout-minutes']).toBe(15);
    expect(job.steps).toContainEqual({ run: 'bash services/dev/scripts/test-transcript-avro.sh' });
    expect(job.steps).toContainEqual({
      name: 'Clean isolated fixture',
      if: 'always()',
      run: 'bash services/dev/scripts/test-transcript-avro.sh down',
    });
  });
});
