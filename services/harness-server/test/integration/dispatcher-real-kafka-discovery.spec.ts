// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { Kafka, KafkaJSError, PartitionAssigners, type Admin, type Consumer } from 'kafkajs';
import {
  KafkaTranscriptStore,
  sessionTopicName,
  type Event,
  type ReadOptions,
  type TailOptions,
  type TranscriptStore,
} from '@orca/transcript-store';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import {
  GuardrailUsageUnavailableError,
  SessionEventKind,
  withCanonicalAgentEventEnvelope,
  type AgentEvent,
  type AgentEventInput,
  type AgentHarness,
  type SessionStartInput,
  type SubmitHooks,
  type TerminationReason,
  type UserEvent,
} from '../../src/harness/agent-harness.js';

const KAFKA_BROKER = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';
const DISCOVERY_INTERVAL_MS = 100;
const STAGE_DEADLINE_MS = 90_000;

describe('Dispatcher real Kafka topic discovery regression', () => {
  it('retires a crashed consumer without an automatic restart joining after replacement', async () => {
    const suffix = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const workspaceId = `ws_dispatcher_restart_${suffix}`;
    const sessionId = `ses_restart_${suffix}`;
    const groupId = `harness-dispatcher-restart-${suffix}`;
    const outputs = new RecordedOutputs();
    const executions = new ExecutionLedger();
    const topics = new Set<string>();
    const consumers: Consumer[] = [];
    const kafka = createKafka(`dispatcher-restart-${suffix}`);
    const createConsumer = kafka.consumer.bind(kafka);
    const restartAssignmentEntered = deferred();
    const releaseRestartAssignment = deferred();
    let crashRestart: boolean | undefined;
    let retired = false;
    let retiredGroupJoins = 0;
    let injected = false;

    // Use real KafkaJS consumers throughout: throw from its actual fetch loop,
    // then gate the public assigner while automatic start() is in joinAndSync.
    // In KafkaJS 2.2.4 Runner.running is still false here, so stop() does not
    // wait for that start; a completed disconnect alone cannot fence its join.
    kafka.consumer = (config) => {
      const first = consumers.length === 0;
      const consumer = createConsumer({
        ...config,
        sessionTimeout: 6_000,
        heartbeatInterval: 1_000,
        rebalanceTimeout: 6_000,
        retry: { ...config.retry, initialRetryTime: 1, retries: 0 },
        partitionAssigners: [
          (args) => {
            const assigner = PartitionAssigners.roundRobin(args);
            return {
              ...assigner,
              assign: async (group) => {
                const assignment = await assigner.assign(group);
                if (first && injected && crashRestart === true) {
                  restartAssignmentEntered.resolve();
                  await releaseRestartAssignment.promise;
                }
                return assignment;
              },
            };
          },
        ],
      });
      consumers.push(consumer);
      if (first) {
        consumer.on(consumer.events.CRASH, ({ payload }) => {
          crashRestart = payload.restart;
        });
        consumer.on(consumer.events.GROUP_JOIN, () => {
          if (retired) retiredGroupJoins += 1;
        });
        const run = consumer.run.bind(consumer);
        consumer.run = async (options) => {
          await run({
            ...options,
            eachMessage: async (payload) => {
              if (crashRestart === undefined) {
                injected = true;
                throw new KafkaJSError('PR263 real fetch-loop retriable crash', {
                  retriable: true,
                });
              }
              await options?.eachMessage?.(payload);
            },
          });
        };
        const disconnect = consumer.disconnect.bind(consumer);
        consumer.disconnect = async () => {
          await disconnect();
          // KafkaJS onCrash calls its private closure, not this public method.
          retired = true;
          releaseRestartAssignment.resolve();
        };
      } else {
        const connect = consumer.connect.bind(consumer);
        consumer.connect = async () => {
          await eventually('real CRASH decision', () => crashRestart !== undefined, 10_000);
          if (crashRestart) {
            await eventually(
              'automatic restart enters assignment',
              () => restartAssignmentEntered.settled,
              10_000,
            );
          }
          await connect();
        };
      }
      return consumer;
    };

    const store = new RecordingTranscriptStore(outputs);
    const dispatcher = new Dispatcher({
      kafka,
      groupId,
      store,
      topicPattern: workspaceTopicPattern(workspaceId),
      topicRediscoverIntervalMs: DISCOVERY_INTERVAL_MS,
      kafkaGroupJoinTimeoutMs: 30_000,
      sessionIdleTimeoutMs: 300_000,
      anthropicApiKey: 'unused-real-kafka-restart-regression',
      modelDefault: 'fake',
      harnessFactory: (_workspaceId, id) => new AcceptanceRecordingHarness(id, executions),
    });
    const producer = new KafkaTranscriptStore({ kafka: createKafka(`registry-restart-${suffix}`) });
    const admin = createKafka(`admin-restart-${suffix}`).admin();
    try {
      await admin.connect();
      await producer.ensureConnected();
      const first = makeMessages(workspaceId, [sessionId], 'crash-replay', suffix);
      await appendConcurrently(producer, first, topics);
      await dispatcher.start();
      await eventually(
        'replacement joins a stable broker group',
        async () => {
          if (!retired || consumers.length < 2 || !dispatcher.readiness().ready) return false;
          const { groups } = await admin.describeGroups([groupId]);
          return groups[0]?.state === 'Stable';
        },
        30_000,
      );
      const { groups } = await admin.describeGroups([groupId]);
      expect({ members: groups[0]?.members.length, retiredGroupJoins }).toEqual({
        members: 1,
        retiredGroupJoins: 0,
      });
      await waitForStage({
        label: 'crashed input replay',
        admin,
        groupId,
        dispatchers: [dispatcher],
        newMessages: first,
        expectedMessages: first,
        outputs,
        executions,
      });
      const next = makeMessages(workspaceId, [sessionId], 'next-turn', suffix);
      const fresh = makeMessages(workspaceId, [`ses_fresh_${suffix}`], 'fresh-topic', suffix);
      await appendConcurrently(producer, [...next, ...fresh], topics);
      await waitForStage({
        label: 'same-session next turn and fresh topic after crash',
        admin,
        groupId,
        dispatchers: [dispatcher],
        newMessages: [...next, ...fresh],
        expectedMessages: [...first, ...next, ...fresh],
        outputs,
        executions,
      });
      // Assert the actual second offset, independently of the producer's seq mutation.
      expect(await groupOffsetAtLeast(admin, groupId, next[0]!.topic, 2)).toBe(true);
      expect(retiredGroupJoins).toBe(0);
      expect((await admin.describeGroups([groupId])).groups[0]?.members).toHaveLength(1);
      await dispatcher.stop();
      await eventually(
        'shutdown leaves no broker group members',
        async () => (await admin.describeGroups([groupId])).groups[0]?.members.length === 0,
      );
    } finally {
      releaseRestartAssignment.resolve();
      await dispatcher.stop().catch(() => {});
      // Also reap any orphan exposed by the unfixed implementation.
      await Promise.allSettled(consumers.map((consumer) => consumer.disconnect()));
      await producer.close().catch(() => {});
      await admin.deleteGroups([groupId]).catch(() => {});
      try {
        await deleteTopicsAndWait(admin, topics);
      } finally {
        await admin.disconnect().catch(() => {});
        await store.close();
      }
    }
  }, 120_000);

  it('discovers auto-created topics, survives group changes, and catches up after producer restart', async () => {
    const suffix = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const workspaceId = `ws_dispatcher_discovery_${suffix}`;
    const groupId = `harness-dispatcher-discovery-${suffix}`;
    const topicPattern = workspaceTopicPattern(workspaceId);
    const outputs = new RecordedOutputs();
    const executions = new ExecutionLedger();
    const topics = new Set<string>();
    const members: DispatcherMember[] = [];
    let registryStore: KafkaTranscriptStore | null = null;
    let admin: Admin | null = null;

    try {
      const adminKafka = createKafka(`admin-${suffix}`);
      admin = adminKafka.admin();
      await admin.connect();

      registryStore = new KafkaTranscriptStore({ kafka: createKafka(`registry-${suffix}`) });
      // Connect producer before concurrent auto-create appends. This makes
      // test race about dispatcher discovery, not producer initialization.
      await registryStore.ensureConnected();

      const first = createDispatcherMember({
        name: 'first',
        suffix,
        groupId,
        topicPattern,
        outputs,
        executions,
      });
      members.push(first);

      // No topic exists for this unique workspace when dispatcher takes its
      // first discovery snapshot. Old regex/snapshot behavior stays empty
      // forever after this point.
      expect((await matchingTopics(admin, topicPattern)).length).toBe(0);
      await first.dispatcher.start();
      await waitForDispatchersReady('empty initial snapshot', [first.dispatcher]);

      const initialSessions = sessionIds('initial', suffix, 20);
      const initialMessages = makeMessages(workspaceId, initialSessions, 'initial', suffix);
      await appendConcurrently(registryStore, initialMessages, topics);
      await waitForStage({
        label: 'fresh auto-created topics',
        admin,
        groupId,
        dispatchers: [first.dispatcher],
        newMessages: initialMessages,
        expectedMessages: initialMessages,
        outputs,
        executions,
      });

      // A second process in same stable group forces a real group rebalance.
      const second = createDispatcherMember({
        name: 'second',
        suffix,
        groupId,
        topicPattern,
        outputs,
        executions,
      });
      members.push(second);
      await second.dispatcher.start();
      await waitForDispatchersReady('two-member GROUP_JOIN', [first.dispatcher, second.dispatcher]);

      const secondMessages = makeMessages(workspaceId, initialSessions, 'second', suffix);
      await appendConcurrently(registryStore, secondMessages, topics);
      const throughSecond = [...initialMessages, ...secondMessages];
      await waitForStage({
        label: 'two-member rebalance',
        admin,
        groupId,
        dispatchers: [first.dispatcher, second.dispatcher],
        newMessages: secondMessages,
        expectedMessages: throughSecond,
        outputs,
        executions,
      });

      // Graceful leave must restore readiness on remaining group member
      // before it accepts third-turn work.
      await stopDispatcherMember(first);
      await waitForDispatchersReady('remaining member GROUP_JOIN', [second.dispatcher]);

      const thirdMessages = makeMessages(workspaceId, initialSessions, 'third', suffix);
      await appendConcurrently(registryStore, thirdMessages, topics);
      const throughThird = [...throughSecond, ...thirdMessages];
      await waitForStage({
        label: 'single member after graceful leave',
        admin,
        groupId,
        dispatchers: [second.dispatcher],
        newMessages: thirdMessages,
        expectedMessages: throughThird,
        outputs,
        executions,
      });

      // Registry producer restarts while no dispatcher is running. Preserve
      // group id and append both existing-topic turns and fresh-topic turns
      // during downtime; replacement must consume every durable input once.
      await stopDispatcherMember(second);
      await registryStore.close();
      registryStore = new KafkaTranscriptStore({
        kafka: createKafka(`registry-restarted-${suffix}`),
      });
      await registryStore.ensureConnected();

      const downtimeExisting = makeMessages(
        workspaceId,
        initialSessions,
        'downtime-existing',
        suffix,
      );
      const downtimeSessions = sessionIds('downtime', suffix, 5);
      const downtimeFresh = makeMessages(workspaceId, downtimeSessions, 'downtime-fresh', suffix);
      const downtimeMessages = [...downtimeExisting, ...downtimeFresh];
      await appendConcurrently(registryStore, downtimeMessages, topics);

      const replacement = createDispatcherMember({
        name: 'replacement',
        suffix,
        groupId,
        topicPattern,
        outputs,
        executions,
      });
      members.push(replacement);
      await replacement.dispatcher.start();
      await waitForDispatchersReady('replacement GROUP_JOIN', [replacement.dispatcher]);

      const allMessages = [...throughThird, ...downtimeMessages];
      await waitForStage({
        label: 'downtime catch-up after registry producer restart',
        admin,
        groupId,
        dispatchers: [replacement.dispatcher],
        newMessages: downtimeMessages,
        expectedMessages: allMessages,
        outputs,
        executions,
      });
    } finally {
      await Promise.allSettled(members.map((member) => stopDispatcherMember(member)));
      if (registryStore) await registryStore.close().catch(() => {});
      if (admin) {
        try {
          await admin.deleteGroups([groupId]);
        } catch {
          // Kafka rejects deleting an already-reaped / unknown group. Topics
          // remain independently cleanup-safe and have unique names.
        }
        try {
          await deleteTopicsAndWait(admin, topics);
        } finally {
          await admin.disconnect().catch(() => {});
        }
      }
    }
  }, 180_000);

  it('discovers and commits a fresh topic while an older turn is still running', async () => {
    const suffix = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const workspaceId = `ws_dispatcher_inflight_${suffix}`;
    const groupId = `harness-dispatcher-inflight-${suffix}`;
    const topicPattern = workspaceTopicPattern(workspaceId);
    const outputs = new RecordedOutputs();
    const executions = new ExecutionLedger();
    const topics = new Set<string>();
    const firstSession = `ses_inflight_first_${suffix}`;
    const secondSession = `ses_inflight_second_${suffix}`;
    const blockingHarness = new BlockingAcceptanceHarness(firstSession, executions);
    const store = new RecordingTranscriptStore(outputs);
    const dispatcher = new Dispatcher({
      kafka: createKafka(`dispatcher-inflight-${suffix}`),
      groupId,
      store,
      topicPattern,
      topicRediscoverIntervalMs: DISCOVERY_INTERVAL_MS,
      kafkaGroupJoinTimeoutMs: 30_000,
      sessionIdleTimeoutMs: 300_000,
      anthropicApiKey: 'unused-real-kafka-inflight-regression',
      modelDefault: 'fake',
      harnessFactory: (_workspaceId, sessionId) =>
        sessionId === firstSession
          ? blockingHarness
          : new AcceptanceRecordingHarness(sessionId, executions),
    });
    const registryStore = new KafkaTranscriptStore({
      kafka: createKafka(`registry-inflight-${suffix}`),
    });
    const admin = createKafka(`admin-inflight-${suffix}`).admin();

    try {
      await admin.connect();
      await registryStore.ensureConnected();
      await dispatcher.start();
      await waitForDispatchersReady('in-flight empty snapshot', [dispatcher]);

      const [first] = makeMessages(workspaceId, [firstSession], 'inflight-first', suffix);
      const [second] = makeMessages(workspaceId, [secondSession], 'inflight-second', suffix);
      await appendConcurrently(registryStore, [first!], topics);
      await eventually('first turn accepted and blocked', () => blockingHarness.submitStarted);
      expect(await groupOffsetsCoverMessages(admin, groupId, [first!])).toBe(false);

      await appendConcurrently(registryStore, [second!], topics);
      await eventually('second fresh topic completes while first is blocked', async () => {
        if (!dispatcher.readiness().ready) return false;
        if (!outputs.hasTransitionsFor([second!])) return false;
        return await groupOffsetsCoverMessages(admin, groupId, [second!]);
      });
      executions.assertExactlyOnce([second!]);
      expect(await groupOffsetsCoverMessages(admin, groupId, [first!])).toBe(false);

      blockingHarness.releaseSubmit();
      await waitForStage({
        label: 'first turn completes after release',
        admin,
        groupId,
        dispatchers: [dispatcher],
        newMessages: [first!],
        expectedMessages: [first!, second!],
        outputs,
        executions,
      });
    } finally {
      blockingHarness.releaseSubmit();
      await dispatcher.stop().catch(() => {});
      await registryStore.close().catch(() => {});
      try {
        await admin.deleteGroups([groupId]);
      } catch {
        // Unknown/already-reaped group is cleanup-safe.
      }
      try {
        await deleteTopicsAndWait(admin, topics);
      } finally {
        await admin.disconnect().catch(() => {});
      }
      await store.close();
    }
  }, 120_000);

  it('delivers a queued session deletion after a usage failure instead of retaining its runner', async () => {
    const suffix = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const workspaceId = `ws_dispatcher_usage_failure_${suffix}`;
    const sessionId = `ses_usage_failure_${suffix}`;
    const groupId = `harness-dispatcher-usage-failure-${suffix}`;
    const topics = new Set<string>();
    const producerStore = new KafkaTranscriptStore({
      kafka: createKafka(`usage-producer-${suffix}`),
    });
    const store = new KafkaLoopbackRecordingStore(new RecordedOutputs(), producerStore);
    const harness = new AcceptanceRecordingHarness(sessionId, new ExecutionLedger());
    let releaseFailure!: () => void;
    const blocked = new Promise<void>((resolve) => {
      releaseFailure = resolve;
    });
    let accepted = false;
    vi.spyOn(harness, 'submit').mockImplementation(async (_event, hooks) => {
      await hooks?.onAccepted();
      accepted = true;
      await blocked;
      throw new GuardrailUsageUnavailableError('Registry session disappeared during usage flush');
    });
    const stopped = vi.spyOn(harness, 'stop');
    const dispatcher = new Dispatcher({
      kafka: createKafka(`usage-dispatcher-${suffix}`),
      groupId,
      store,
      topicPattern: workspaceTopicPattern(workspaceId),
      topicRediscoverIntervalMs: DISCOVERY_INTERVAL_MS,
      kafkaGroupJoinTimeoutMs: 30_000,
      sessionIdleTimeoutMs: 300_000,
      anthropicApiKey: 'unused-real-kafka-usage-regression',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });
    const admin = createKafka(`usage-admin-${suffix}`).admin();
    try {
      await admin.connect();
      await producerStore.ensureConnected();
      await dispatcher.start();
      const [message] = makeMessages(workspaceId, [sessionId], 'usage-failure', suffix);
      await appendConcurrently(producerStore, [message!], topics);
      await eventually('guarded turn accepted', () => accepted);
      expect(await groupOffsetAtLeast(admin, groupId, message!.topic, 1)).toBe(false);

      await producerStore.append(workspaceId, sessionId, [
        {
          ...message!.event,
          id: `evt_delete_${suffix}`,
          kind: 'session.deleted',
          producedBy: 'transcript-store',
          payload: new Uint8Array(),
        },
      ]);
      const offsets = await admin.fetchTopicOffsets(message!.topic);
      const deletionOffset = Number(offsets[0]!.high);
      expect(stopped).not.toHaveBeenCalled();

      releaseFailure();
      await eventually(
        'queued delete releases the failed runner and commits',
        async () =>
          stopped.mock.calls.some(([reason]) => reason === 'client.archived') &&
          (await groupOffsetAtLeast(admin, groupId, message!.topic, deletionOffset)),
        30_000,
      );
    } finally {
      releaseFailure();
      await dispatcher.stop().catch(() => {});
      await producerStore.close().catch(() => {});
      await admin.deleteGroups([groupId]).catch(() => {});
      try {
        await deleteTopicsAndWait(admin, topics);
      } finally {
        await admin.disconnect().catch(() => {});
      }
    }
  }, 90_000);

  it('does not pause on Harness loopback backlog before the next user turn', async () => {
    const suffix = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const workspaceId = `ws_dispatcher_loopback_${suffix}`;
    const sessionId = `ses_dispatcher_loopback_${suffix}`;
    const groupId = `harness-dispatcher-loopback-${suffix}`;
    const topicPattern = workspaceTopicPattern(workspaceId);
    const outputs = new RecordedOutputs();
    const topics = new Set<string>();
    const producerStore = new KafkaTranscriptStore({
      kafka: createKafka(`registry-loopback-${suffix}`),
    });
    const store = new KafkaLoopbackRecordingStore(outputs, producerStore);
    const harness = new BurstTerminalHarness();
    const dispatcher = new Dispatcher({
      kafka: createKafka(`dispatcher-loopback-${suffix}`),
      groupId,
      store,
      topicPattern,
      topicRediscoverIntervalMs: DISCOVERY_INTERVAL_MS,
      kafkaGroupJoinTimeoutMs: 30_000,
      sessionIdleTimeoutMs: 300_000,
      anthropicApiKey: 'unused-real-kafka-loopback-regression',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });
    const admin = createKafka(`admin-loopback-${suffix}`).admin();

    try {
      await admin.connect();
      await producerStore.ensureConnected();
      await dispatcher.start();
      await waitForDispatchersReady('loopback empty snapshot', [dispatcher]);

      const [first] = makeMessages(workspaceId, [sessionId], 'loopback-first', suffix);
      await appendConcurrently(producerStore, [first!], topics);
      await eventually('first loopback turn commits', async () => {
        return (
          harness.submitCount >= 1 && (await groupOffsetAtLeast(admin, groupId, first!.topic, 1))
        );
      });

      const [second] = makeMessages(workspaceId, [sessionId], 'loopback-second', suffix);
      await appendConcurrently(producerStore, [second!], topics);
      await eventually(
        'second user turn passes Harness loopback backlog',
        async () =>
          harness.submitCount >= 2 && (await groupOffsetAtLeast(admin, groupId, second!.topic, 2)),
        30_000,
      );
    } finally {
      await dispatcher.stop().catch(() => {});
      await producerStore.close().catch(() => {});
      try {
        await admin.deleteGroups([groupId]);
      } catch {
        // Unknown/already-reaped group is cleanup-safe.
      }
      try {
        await deleteTopicsAndWait(admin, topics);
      } finally {
        await admin.disconnect().catch(() => {});
      }
    }
  }, 90_000);
});

interface DispatcherMember {
  dispatcher: Dispatcher;
  store: RecordingTranscriptStore;
  stopped: boolean;
}

function deferred(): { promise: Promise<void>; resolve: () => void; settled: boolean } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  const gate = {
    promise,
    settled: false,
    resolve: () => {
      gate.settled = true;
      resolve();
    },
  };
  return gate;
}

interface ExpectedMessage {
  key: string;
  event: Event;
  topic: string;
}

interface StageWaitInput {
  label: string;
  admin: Admin;
  groupId: string;
  dispatchers: Dispatcher[];
  newMessages: ExpectedMessage[];
  expectedMessages: ExpectedMessage[];
  outputs: RecordedOutputs;
  executions: ExecutionLedger;
}

function createDispatcherMember(args: {
  name: string;
  suffix: string;
  groupId: string;
  topicPattern: RegExp;
  outputs: RecordedOutputs;
  executions: ExecutionLedger;
}): DispatcherMember {
  // Input delivery remains real Kafka. This store records only dispatcher and
  // harness writes so per-session transcript reads cannot serialize the 20
  // topic race behind throwaway consumer-group joins.
  const store = new RecordingTranscriptStore(args.outputs);
  return {
    store,
    stopped: false,
    dispatcher: new Dispatcher({
      kafka: createKafka(`dispatcher-${args.name}-${args.suffix}`),
      groupId: args.groupId,
      store,
      topicPattern: args.topicPattern,
      topicRediscoverIntervalMs: DISCOVERY_INTERVAL_MS,
      kafkaGroupJoinTimeoutMs: 30_000,
      sessionIdleTimeoutMs: 300_000,
      anthropicApiKey: 'unused-real-kafka-regression',
      modelDefault: 'fake',
      harnessFactory: (_workspaceId, sessionId) =>
        new AcceptanceRecordingHarness(sessionId, args.executions),
    }),
  };
}

async function stopDispatcherMember(member: DispatcherMember): Promise<void> {
  if (member.stopped) return;
  member.stopped = true;
  try {
    await member.dispatcher.stop();
  } finally {
    await member.store.close();
  }
}

function createKafka(clientId: string): Kafka {
  return new Kafka({
    clientId: `dispatcher-real-kafka-${clientId}`,
    brokers: [KAFKA_BROKER],
  });
}

function sessionIds(phase: string, suffix: string, count: number): string[] {
  return Array.from({ length: count }, (_, index) => `ses_${phase}_${suffix}_${index}`);
}

function workspaceTopicPattern(workspaceId: string): RegExp {
  return new RegExp(`^orca\\.${escapeRegex(workspaceId)}\\.sessions\\.[A-Za-z0-9_-]+\\.events$`);
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function makeMessages(
  workspaceId: string,
  sessionIdsForPhase: string[],
  phase: string,
  suffix: string,
): ExpectedMessage[] {
  return sessionIdsForPhase.map((sessionId, index) => {
    const key = `${phase}:${sessionId}`;
    const event: Event = {
      id: `evt_dispatcher_discovery_${suffix}_${phase}_${index}`,
      workspaceId,
      sessionId,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.message',
      payload: Buffer.from(JSON.stringify({ content: [{ type: 'text', text: key }] }), 'utf8'),
      idempotencyKey: '',
    };
    return { key, event, topic: sessionTopicName(workspaceId, sessionId) };
  });
}

async function appendConcurrently(
  store: KafkaTranscriptStore,
  messages: ExpectedMessage[],
  topics: Set<string>,
): Promise<void> {
  for (const message of messages) topics.add(message.topic);
  await Promise.all(
    messages.map(async ({ event }) => {
      await store.append(event.workspaceId, event.sessionId, [event]);
    }),
  );
}

async function waitForDispatchersReady(label: string, dispatchers: Dispatcher[]): Promise<void> {
  await eventually(`${label}: dispatcher readiness after GROUP_JOIN`, () =>
    dispatchers.every((dispatcher) => dispatcher.readiness().ready),
  );
}

async function waitForStage(input: StageWaitInput): Promise<void> {
  await eventually(`${input.label}: committed offsets and recorded harness output`, async () => {
    if (!input.dispatchers.every((dispatcher) => dispatcher.readiness().ready)) return false;
    if (!(await groupOffsetsCoverMessages(input.admin, input.groupId, input.newMessages)))
      return false;
    return input.outputs.hasTransitionsFor(input.expectedMessages);
  });

  input.executions.assertExactlyOnce(input.expectedMessages);
  input.outputs.assertExactlyOnce(input.expectedMessages);
}

async function groupOffsetsCoverMessages(
  admin: Admin,
  groupId: string,
  messages: ExpectedMessage[],
): Promise<boolean> {
  const requiredOffsets = new Map<string, number>();
  for (const { event, topic } of messages) {
    const required = event.seq + 1;
    requiredOffsets.set(topic, Math.max(requiredOffsets.get(topic) ?? 0, required));
  }
  const committed = await admin.fetchOffsets({ groupId, topics: [...requiredOffsets.keys()] });
  const committedByTopic = new Map(
    committed.map(({ topic, partitions }) => [topic, Number(partitions[0]?.offset ?? '-1')]),
  );
  return [...requiredOffsets].every(
    ([topic, requiredOffset]) => (committedByTopic.get(topic) ?? -1) >= requiredOffset,
  );
}

async function groupOffsetAtLeast(
  admin: Admin,
  groupId: string,
  topic: string,
  requiredOffset: number,
): Promise<boolean> {
  const committed = await admin.fetchOffsets({ groupId, topics: [topic] });
  return Number(committed[0]?.partitions[0]?.offset ?? '-1') >= requiredOffset;
}

async function matchingTopics(admin: Admin, pattern: RegExp): Promise<string[]> {
  return (await admin.listTopics()).filter((topic) => {
    pattern.lastIndex = 0;
    return pattern.test(topic);
  });
}

async function deleteTopicsAndWait(admin: Admin, topics: Set<string>): Promise<void> {
  if (topics.size === 0) return;
  const existing = new Set(await admin.listTopics());
  const deletable = [...topics].filter((topic) => existing.has(topic));
  if (deletable.length > 0) await admin.deleteTopics({ topics: deletable, timeout: 10_000 });
  await eventually(
    'topic deletion',
    async () => {
      const remaining = new Set(await admin.listTopics());
      return [...topics].every((topic) => !remaining.has(topic));
    },
    30_000,
  );
}

/** Bounded condition polling, never an unconditional timing sleep. */
async function eventually(
  label: string,
  predicate: () => boolean | Promise<boolean>,
  deadlineMs = STAGE_DEADLINE_MS,
): Promise<void> {
  const deadline = Date.now() + deadlineMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  const detail = lastError instanceof Error ? ` Last error: ${lastError.message}` : '';
  throw new Error(`${label} did not converge within ${deadlineMs}ms.${detail}`);
}

class RecordingTranscriptStore implements TranscriptStore {
  constructor(private readonly outputs: RecordedOutputs) {}

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    void workspaceId;
    void sessionId;
    for (const event of events) {
      if (event.producedBy === 'harness') this.outputs.record(event);
    }
    return events.map((event) => event.id);
  }

  read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    void workspaceId;
    void sessionId;
    void opts;
    return emptyEvents();
  }

  tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event> {
    void workspaceId;
    void sessionId;
    void opts;
    return emptyEvents();
  }

  async archive(workspaceId: string, sessionId: string): Promise<void> {
    void workspaceId;
    void sessionId;
  }

  async close(): Promise<void> {}
}

class KafkaLoopbackRecordingStore extends RecordingTranscriptStore {
  constructor(
    outputs: RecordedOutputs,
    private readonly delegate: KafkaTranscriptStore,
  ) {
    super(outputs);
  }

  override async append(
    workspaceId: string,
    sessionId: string,
    events: Event[],
  ): Promise<string[]> {
    await super.append(workspaceId, sessionId, events);
    return await this.delegate.append(workspaceId, sessionId, events);
  }
}

function emptyEvents(): AsyncIterable<Event> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<Event> {
      return {
        next: async (): Promise<IteratorResult<Event>> => ({
          done: true,
          value: undefined as never,
        }),
      };
    },
  };
}

class RecordedOutputs {
  private readonly events: Event[] = [];

  record(event: Event): void {
    this.events.push({ ...event, payload: new Uint8Array(event.payload) });
  }

  hasTransitionsFor(messages: ExpectedMessage[]): boolean {
    const expectedBySession = messageCountsBySession(messages);
    return (
      messages.every(({ event }) => this.processedCount(event.id) >= 1) &&
      [...expectedBySession].every(
        ([sessionId, expected]) =>
          this.countKind(sessionId, SessionEventKind.statusRunning) >= expected &&
          this.countKind(sessionId, SessionEventKind.statusIdle) >= expected,
      ) &&
      messages.every(({ key, event }) => this.messageCount(event.sessionId, key) >= 1)
    );
  }

  assertExactlyOnce(messages: ExpectedMessage[]): void {
    for (const { event, key } of messages) {
      expect(this.processedCount(event.id)).toBe(1);
      expect(this.messageCount(event.sessionId, key)).toBe(1);
    }
    for (const [sessionId, expected] of messageCountsBySession(messages)) {
      expect(this.countKind(sessionId, SessionEventKind.statusRunning)).toBe(expected);
      expect(this.countKind(sessionId, SessionEventKind.statusIdle)).toBe(expected);
    }
  }

  private processedCount(userEventId: string): number {
    return this.events.filter((event) => {
      if (event.kind !== 'session.user_event_processed') return false;
      const payload = parsePayload(event);
      return (
        payload !== null &&
        typeof payload === 'object' &&
        (payload as { user_event_id?: unknown }).user_event_id === userEventId
      );
    }).length;
  }

  private countKind(sessionId: string, kind: string): number {
    return this.events.filter((event) => event.sessionId === sessionId && event.kind === kind)
      .length;
  }

  private messageCount(sessionId: string, key: string): number {
    return this.events.filter((event) => {
      if (event.sessionId !== sessionId || event.kind !== 'agent.message') return false;
      const payload = parsePayload(event);
      if (!payload || typeof payload !== 'object') return false;
      const content = (payload as { content?: unknown }).content;
      return (
        Array.isArray(content) &&
        content.some(
          (part) =>
            part !== null &&
            typeof part === 'object' &&
            (part as { type?: unknown }).type === 'text' &&
            (part as { text?: unknown }).text === `ack:${key}`,
        )
      );
    }).length;
  }
}

class ExecutionLedger {
  private readonly counts = new Map<string, number>();

  record(key: string): void {
    this.counts.set(key, (this.counts.get(key) ?? 0) + 1);
  }

  assertExactlyOnce(messages: ExpectedMessage[]): void {
    const expected = new Set(messages.map(({ key }) => key));
    for (const key of expected) expect(this.counts.get(key)).toBe(1);
    expect([...this.counts.keys()].filter((key) => !expected.has(key))).toEqual([]);
  }
}

class AcceptanceRecordingHarness implements AgentHarness {
  private readonly queue: AgentEvent[] = [];
  private readonly waiters: Array<(event: AgentEvent | null) => void> = [];
  private stopped = false;

  constructor(
    private readonly sessionId: string,
    private readonly executions: ExecutionLedger,
  ) {}

  async start(_input: SessionStartInput): Promise<void> {
    void _input;
  }

  async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (this.stopped || event.kind !== 'user.message') return;
    const key = userMessageText(event.payload);
    if (!key) throw new Error(`missing text content for ${this.sessionId}`);
    if (!hooks) throw new Error(`missing acceptance hook for ${this.sessionId}/${key}`);

    await hooks.onAccepted();
    if (this.stopped) return;
    this.executions.record(key);
    this.emit({ kind: SessionEventKind.statusRunning, payload: { status: 'running' } });
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: `ack:${key}` }] },
    });
    this.emit({
      kind: SessionEventKind.statusIdle,
      payload: { stop_reason: { type: 'end_turn' } },
    });
  }

  async stop(_reason: TerminationReason): Promise<void> {
    void _reason;
    this.stopped = true;
    for (const resolve of this.waiters.splice(0)) resolve(null);
  }

  async *events(): AsyncIterable<AgentEvent> {
    for (;;) {
      const event = await this.next();
      if (event === null) return;
      yield event;
    }
  }

  private next(): Promise<AgentEvent | null> {
    const queued = this.queue.shift();
    if (queued) return Promise.resolve(queued);
    if (this.stopped) return Promise.resolve(null);
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  protected emit(event: AgentEventInput): void {
    if (this.stopped) return;
    const canonicalEvent = withCanonicalAgentEventEnvelope(event);
    const resolve = this.waiters.shift();
    if (resolve) resolve(canonicalEvent);
    else this.queue.push(canonicalEvent);
  }
}

class BlockingAcceptanceHarness implements AgentHarness {
  private readonly queue: AgentEvent[] = [];
  private readonly waiters: Array<(event: AgentEvent | null) => void> = [];
  private stopped = false;
  private release: (() => void) | null = null;
  private readonly released = new Promise<void>((resolve) => {
    this.release = resolve;
  });
  submitStarted = false;

  constructor(
    private readonly sessionId: string,
    private readonly executions: ExecutionLedger,
  ) {}

  async start(_input: SessionStartInput): Promise<void> {
    void _input;
  }

  async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (this.stopped || event.kind !== 'user.message') return;
    const key = userMessageText(event.payload);
    if (!key) throw new Error(`missing text content for ${this.sessionId}`);
    if (!hooks) throw new Error(`missing acceptance hook for ${this.sessionId}/${key}`);
    await hooks.onAccepted();
    this.submitStarted = true;
    await this.released;
    if (this.stopped) return;
    this.executions.record(key);
    this.emit({ kind: SessionEventKind.statusRunning, payload: { status: 'running' } });
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: `ack:${key}` }] },
    });
    this.emit({
      kind: SessionEventKind.statusIdle,
      payload: { stop_reason: { type: 'end_turn' } },
    });
  }

  async stop(_reason: TerminationReason): Promise<void> {
    void _reason;
    this.stopped = true;
    this.release?.();
    for (const resolve of this.waiters.splice(0)) resolve(null);
  }

  async *events(): AsyncIterable<AgentEvent> {
    for (;;) {
      const event = await this.next();
      if (event === null) return;
      yield event;
    }
  }

  releaseSubmit(): void {
    this.release?.();
  }

  private next(): Promise<AgentEvent | null> {
    const queued = this.queue.shift();
    if (queued) return Promise.resolve(queued);
    if (this.stopped) return Promise.resolve(null);
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  private emit(event: AgentEventInput): void {
    if (this.stopped) return;
    const canonicalEvent = withCanonicalAgentEventEnvelope(event);
    const resolve = this.waiters.shift();
    if (resolve) resolve(canonicalEvent);
    else this.queue.push(canonicalEvent);
  }
}

class BurstTerminalHarness extends AcceptanceRecordingHarness {
  submitCount = 0;

  constructor() {
    super('loopback-session', new ExecutionLedger());
  }

  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.kind !== 'user.message') return;
    this.submitCount += 1;
    await hooks?.onAccepted();
    this.emitBurst(this.submitCount === 1 ? 32 : 1);
  }

  private emitBurst(messageCount: number): void {
    this.emit({ kind: SessionEventKind.statusRunning, payload: { status: 'running' } });
    for (let index = 0; index < messageCount; index += 1) {
      this.emit({
        kind: 'agent.message',
        payload: { content: [{ type: 'text', text: `loopback-${this.submitCount}-${index}` }] },
      });
    }
    this.emit({
      kind: SessionEventKind.statusIdle,
      payload: { stop_reason: { type: 'end_turn' } },
    });
  }
}

function messageCountsBySession(messages: ExpectedMessage[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const { event } of messages) {
    counts.set(event.sessionId, (counts.get(event.sessionId) ?? 0) + 1);
  }
  return counts;
}

function userMessageText(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object') return null;
  const content = (payload as { content?: unknown }).content;
  if (!Array.isArray(content)) return null;
  const text = content.find(
    (part) =>
      part !== null &&
      typeof part === 'object' &&
      (part as { type?: unknown }).type === 'text' &&
      typeof (part as { text?: unknown }).text === 'string',
  ) as { text: string } | undefined;
  return text?.text ?? null;
}

function parsePayload(event: Event): unknown | null {
  try {
    return JSON.parse(Buffer.from(event.payload).toString('utf8'));
  } catch {
    return null;
  }
}
