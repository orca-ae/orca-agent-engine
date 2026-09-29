// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the in-memory, per-replica WorkerRegistry + WorkerConnection.
//
// The in-process worker-connection registry tracks which environment workers have
// a live worker tunnel on THIS replica (the durable, cross-replica source of truth
// is the environment-claims table). The registry carries only control frames
// (launch/stop a runner + the
// filesystem ops), so it is simpler than the runner TunnelRegistry — no
// per-request HTTP reassembly, just the outbound send queue, the per-request
// pending-result waiter maps, online tracking, and newest-wins replacement.
//
// Every public method and state field of the registry is exercised here with no
// network and no DB — plain synchronous mutation on the one event loop.

import { describe, it, expect } from 'vitest';
import {
  WorkerFrameKind,
  type WorkerHelloFrame,
  type WorkerSocketMessage,
  type WorkerWebSocket,
} from '@orca/harness-tunnel';
import { WorkerRegistry, WorkerConnectionReplacedError } from '../../src/tunnel/worker-registry.js';

/** A minimal WorkerWebSocket fake: records sends, never receives, swallows close. */
class FakeWorkerSocket implements WorkerWebSocket {
  readonly sent: string[] = [];
  closed: { code?: number; reason?: string } | undefined;

  receive(): Promise<WorkerSocketMessage> {
    // The registry never pulls inbound; the route's receive loop owns that.
    return new Promise<WorkerSocketMessage>(() => {});
  }

  sendText(data: string): Promise<void> {
    this.sent.push(data);
    return Promise.resolve();
  }

  close(code?: number, reason?: string): void {
    this.closed = {
      ...(code !== undefined ? { code } : {}),
      ...(reason !== undefined ? { reason } : {}),
    };
  }
}

function helloFrame(name = 'workstation-01', runners: string[] = []): WorkerHelloFrame {
  return {
    kind: WorkerFrameKind.Hello,
    version: '0.1.0-test',
    frameProtocolVersion: 1,
    name,
    runners,
    configuredHarnesses: null,
  };
}

const ENV_ID = 'env_test_001';

describe('tunnel/WorkerRegistry — register + lookup', () => {
  it('registers a connection and exposes it via get + onlineWorkerIds', () => {
    const registry = new WorkerRegistry();
    const ws = new FakeWorkerSocket();
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(registry.onlineWorkerIds()).toEqual([]);

    const conn = registry.register(ENV_ID, ws, helloFrame('laptop', ['runner_a']), {
      owner: 'ws_acme',
    });

    expect(registry.get(ENV_ID)).toBe(conn);
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);
    expect(conn.workerId).toBe(ENV_ID);
    expect(conn.owner).toBe('ws_acme');
    expect(conn.hello.name).toBe('laptop');
    expect(conn.hello.runners).toEqual(['runner_a']);
    expect(conn.connectedAt).toBeGreaterThan(0);
    expect(conn.lastFrameAt).toBeGreaterThan(0);
  });

  it('initializes every pending-request waiter map empty', () => {
    const registry = new WorkerRegistry();
    const conn = registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    expect(conn.owner).toBeUndefined();
    expect(conn.pendingLaunches.size).toBe(0);
    expect(conn.pendingStops.size).toBe(0);
    expect(conn.pendingStats.size).toBe(0);
    expect(conn.pendingListDirs.size).toBe(0);
    expect(conn.pendingCreateWorktrees.size).toBe(0);
    expect(conn.pendingRemoveWorktrees.size).toBe(0);
    expect(conn.pendingCreateDirs.size).toBe(0);
  });

  it('returns undefined from get for an unknown worker', () => {
    const registry = new WorkerRegistry();
    expect(registry.get('env_missing')).toBeUndefined();
  });
});

describe('tunnel/WorkerRegistry — deregister', () => {
  it('removes a registered connection', () => {
    const registry = new WorkerRegistry();
    registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    expect(registry.get(ENV_ID)).toBeDefined();

    registry.deregister(ENV_ID);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(registry.onlineWorkerIds()).toEqual([]);
  });

  it('is a no-op for an unknown worker (idempotent)', () => {
    const registry = new WorkerRegistry();
    expect(() => registry.deregister('env_missing')).not.toThrow();
  });

  it('pushes the stop sentinel onto the outbound queue so a parked sender unblocks', async () => {
    const registry = new WorkerRegistry();
    const conn = registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    // A sender parked on `outboundQueue.get()` must wake with the null sentinel.
    const next = conn.outboundQueue.get();
    registry.deregister(ENV_ID);
    expect(await next).toBeNull();
  });
});

describe('tunnel/WorkerRegistry — newest-wins replacement', () => {
  it('replaces a stale connection and poisons its outbound queue', async () => {
    const registry = new WorkerRegistry();
    const first = registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame('old'), {});
    const parked = first.outboundQueue.get();

    const second = registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame('new'), {});

    expect(registry.get(ENV_ID)).toBe(second);
    expect(second).not.toBe(first);
    // The superseded connection's sender loop is unblocked with the stop sentinel.
    expect(await parked).toBeNull();
  });

  it('keeps onlineWorkerIds at one entry across a same-worker replacement', () => {
    const registry = new WorkerRegistry();
    registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);
  });

  it('tracks multiple distinct hosts independently in insertion order', () => {
    const registry = new WorkerRegistry();
    registry.register('env_a', new FakeWorkerSocket(), helloFrame(), {});
    registry.register('env_b', new FakeWorkerSocket(), helloFrame(), {});
    expect(registry.onlineWorkerIds()).toEqual(['env_a', 'env_b']);
  });
});

describe('tunnel/WorkerRegistry — sendText', () => {
  it('enqueues a frame on the connection outbound queue', async () => {
    const registry = new WorkerRegistry();
    const conn = registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    registry.sendText(conn, 'frame-1');
    expect(await conn.outboundQueue.get()).toBe('frame-1');
  });

  it('throws WorkerConnectionReplacedError when the connection has been replaced', () => {
    const registry = new WorkerRegistry();
    const first = registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    // Newest-wins replacement makes `first` stale.
    registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    expect(() => registry.sendText(first, 'frame')).toThrow(WorkerConnectionReplacedError);
  });

  it('throws WorkerConnectionReplacedError when the worker was deregistered', () => {
    const registry = new WorkerRegistry();
    const conn = registry.register(ENV_ID, new FakeWorkerSocket(), helloFrame(), {});
    registry.deregister(ENV_ID);
    expect(() => registry.sendText(conn, 'frame')).toThrow(WorkerConnectionReplacedError);
  });
});
