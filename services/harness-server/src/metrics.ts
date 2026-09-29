// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry, prefix: 'harness_server_' });

export const harnessActiveSessions = new Gauge({
  name: 'harness_server_active_sessions',
  help: 'Number of sessions currently bound to a runner on this replica',
  registers: [registry],
});

export const harnessEventSubmitTotal = new Counter({
  name: 'harness_server_event_submit_total',
  help: 'User events received from Kafka and forwarded to a runner',
  labelNames: ['workspace_id', 'kind'] as const,
  registers: [registry],
});

export const harnessEventEmitTotal = new Counter({
  name: 'harness_server_event_emit_total',
  help: 'Harness events appended via TranscriptStore',
  labelNames: ['workspace_id', 'kind'] as const,
  registers: [registry],
});

// Kafka dispatcher health. These intentionally carry no topic, session, or
// prompt labels: topic cardinality grows with every managed-agent session.
export const harnessKafkaDiscoveredTopics = new Gauge({
  name: 'harness_kafka_discovered_topics',
  help: 'Session topics in the latest successful Kafka admin discovery snapshot.',
  registers: [registry],
});

export const harnessKafkaActiveSubscribedTopics = new Gauge({
  name: 'harness_kafka_active_subscribed_topics',
  help: 'Session topics confirmed by the active consumer GROUP_JOIN event.',
  registers: [registry],
});

export const harnessKafkaAssignedTopics = new Gauge({
  name: 'harness_kafka_assigned_topics',
  help: 'Topics with one or more partitions assigned to this consumer.',
  registers: [registry],
});

export const harnessKafkaAssignedPartitions = new Gauge({
  name: 'harness_kafka_assigned_partitions',
  help: 'Partitions assigned to this consumer by the latest GROUP_JOIN event.',
  registers: [registry],
});

export const harnessKafkaConsumerReady = new Gauge({
  name: 'harness_kafka_consumer_ready',
  help: 'Whether the active Kafka consumer has completed GROUP_JOIN and is not rebalancing.',
  registers: [registry],
});

export const harnessKafkaOffsetLag = new Gauge({
  name: 'harness_kafka_offset_lag',
  help: 'Latest END_BATCH_PROCESS offset lag for this consumer; zero with no assignment.',
  registers: [registry],
});

export const harnessKafkaDiscoveryTotal = new Counter({
  name: 'harness_kafka_discovery_total',
  help: 'Kafka topic discovery attempts by outcome.',
  labelNames: ['result'] as const,
  registers: [registry],
});

export const harnessKafkaTransitionTotal = new Counter({
  name: 'harness_kafka_transition_total',
  help: 'Kafka consumer activation and fallback transitions by outcome.',
  labelNames: ['result'] as const,
  registers: [registry],
});

export const harnessRunnerSpawnAttemptsTotal = new Counter({
  name: 'harness_runner_spawn_attempts_total',
  help: 'Session runner spawn attempts.',
  registers: [registry],
});

export const harnessRunnerSpawnFailuresTotal = new Counter({
  name: 'harness_runner_spawn_failures_total',
  help: 'Session runner spawn failures excluding intentionally dropped stale events.',
  registers: [registry],
});

export const harnessAcceptedEventToStatusRunningSeconds = new Histogram({
  name: 'harness_accepted_event_to_status_running_seconds',
  help: 'Latency from client event produced_at to persisted session.status_running.',
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600],
  registers: [registry],
});

// FUSE / output-capture / STS observability.

export const harnessFuseMountTotal = new Counter({
  name: 'harness_fuse_mount_total',
  help: 'Count of resource mount attempts. Labels: strategy=tarball_prefetch (the resolved strategy name), result=ok|error.',
  labelNames: ['strategy', 'result'] as const,
  registers: [registry],
});

export const harnessOutputIndexLagSeconds = new Histogram({
  name: 'harness_output_index_lag_seconds',
  help: 'Wall-clock seconds from an output-index trigger to completion. Labels: trigger=tool_result|shutdown. p95 SLO target: < 5s.',
  labelNames: ['trigger'] as const,
  buckets: [0.1, 0.5, 1, 2, 5, 10, 30, 60],
  registers: [registry],
});

export const harnessOutputFilesIndexedTotal = new Counter({
  name: 'harness_output_files_indexed_total',
  help: 'Count of files registered by the output indexer. Labels: workspace_id, result=ok|error|skipped.',
  labelNames: ['workspace_id', 'result'] as const,
  registers: [registry],
});

export const harnessSandboxWriteDeniedTotal = new Counter({
  name: 'harness_sandbox_write_denied_total',
  help: 'Agent filesystem writes rejected by the session policy. Labels: runtime, kind.',
  labelNames: ['runtime', 'kind'] as const,
  registers: [registry],
});

export const harnessSandboxWritePolicySetupTotal = new Counter({
  name: 'harness_sandbox_write_policy_setup_total',
  help: 'Write-policy installation attempts. Labels: runtime, result=ok|error.',
  labelNames: ['runtime', 'result'] as const,
  registers: [registry],
});

export const harnessStsMintTotal = new Counter({
  name: 'harness_sts_mint_total',
  help: 'Count of SessionCredsMinter.mint() calls. Labels: result=ok|error|dev_fallback.',
  labelNames: ['result'] as const,
  registers: [registry],
});

// Memory write watcher observability.

export const harnessMemoryWriteLagSeconds = new Histogram({
  name: 'harness_memory_write_lag_seconds',
  help: 'Wall-clock seconds from S3 LastModified (or sandbox poll start for InMemory) to memory_version registered. p95 SLO target: <3s.',
  buckets: [0.1, 0.5, 1, 2, 3, 5, 10, 30],
  registers: [registry],
});

export const harnessMemoryVersionsRecordedTotal = new Counter({
  name: 'harness_memory_versions_recorded_total',
  help: 'Memory versions recorded by the watcher. Labels: workspace_id, result=ok|conflict|error.',
  labelNames: ['workspace_id', 'result'] as const,
  registers: [registry],
});

export const harnessMemoryWatcherPollTotal = new Counter({
  name: 'harness_memory_watcher_poll_total',
  help: 'Watcher poll cycles. Labels: result=ok|error.',
  labelNames: ['result'] as const,
  registers: [registry],
});

// github_repository clone observability.

export const harnessGitCloneSeconds = new Histogram({
  name: 'harness_git_clone_seconds',
  help: 'Wall-clock seconds for the GitCloneStrategy.activate() flow (clone + walk + sandbox stream). p95 SLO target: < 30 s for repos <= 100 MB.',
  buckets: [0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300],
  registers: [registry],
});

export const harnessGitCloneTotal = new Counter({
  name: 'harness_git_clone_total',
  help: 'GitCloneStrategy.activate() invocations. Labels: workspace_id, result=ok|error.',
  labelNames: ['workspace_id', 'result'] as const,
  registers: [registry],
});
