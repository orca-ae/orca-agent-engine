// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Worker } from 'node:worker_threads';
import { createRequire } from 'node:module';
import type { EvaluationRequest, EvaluationResult } from '@runorca/dogwood';
const artifact = createRequire(import.meta.url).resolve('@runorca/dogwood');
/** Actual pinned WASM in a disposable, bounded worker, never a JavaScript policy approximation. */
export function evaluateDogwood(
  request: EvaluationRequest,
  timeoutMs = 5000,
): Promise<EvaluationResult> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(
      `const {parentPort,workerData}=require('node:worker_threads');try{parentPort.postMessage({result:require(workerData.artifact).evaluate(workerData.request)});}catch(e){parentPort.postMessage({error:String(e)});}`,
      {
        eval: true,
        workerData: { artifact, request },
        resourceLimits: { maxOldGenerationSizeMb: 256 },
      },
    );
    const timer = setTimeout(() => {
      void worker.terminate();
      reject(new Error('Dogwood evaluation timed out'));
    }, timeoutMs);
    let settled = false;
    const done = () => {
      settled = true;
      clearTimeout(timer);
      void worker.terminate();
    };
    worker.once('message', (message: { result?: EvaluationResult; error?: string }) => {
      done();
      if (message.result) resolve(message.result);
      else reject(new Error(message.error ?? 'Dogwood worker failed'));
    });
    worker.once('error', (error) => {
      done();
      reject(error);
    });
    worker.once('exit', (code) => {
      clearTimeout(timer);
      if (!settled) reject(new Error(`Dogwood worker exited without a result (${code})`));
    });
  });
}

/** Operational failures are raw denies; observe/enforce folding stays with the enforcer. */
export async function evaluateDogwoodDecision(
  request: EvaluationRequest,
  timeoutMs = 5000,
): Promise<EvaluationResult> {
  try {
    return await evaluateDogwood(request, timeoutMs);
  } catch {
    return {
      verdict: 'deny',
      deciding_rules: [],
      errors: ['Dogwood evaluator unavailable'],
      valid_before: null,
      policy_hash: '',
      events: { allow: [], deny: [] },
    };
  }
}
