// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import { Counter, Histogram } from 'prom-client';
import { registry } from '../metrics.js';
export const policyAppends = new Counter({
  name: 'orca_registry_policy_appends_total',
  help: 'Policy append transaction results',
  labelNames: ['result'],
  registers: [registry],
});
export const policyPruned = new Counter({
  name: 'orca_registry_policy_events_pruned_total',
  help: 'Expired policy event rows pruned',
  registers: [registry],
});
export const policyEvaluations = new Counter({
  name: 'orca_registry_dogwood_evaluations_total',
  help: 'Raw final Dogwood evaluations',
  labelNames: ['verdict'],
  registers: [registry],
});
export const policyEvaluationLatency = new Histogram({
  name: 'orca_registry_dogwood_evaluation_seconds',
  help: 'Final Dogwood worker latency',
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 5],
  registers: [registry],
});
