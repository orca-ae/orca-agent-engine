// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Pool } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import {
  hashTranscriptEnvelope,
  ObservabilityExporterRepository,
  stableJson,
  type ClaimedOutboxItem,
} from '../../src/persistence.js';
import { eventIdentityKey } from '../../src/event-identity.js';
import type { OtlpDeliveryOutcome } from '../../src/types.js';
import { TRANSCRIPT_SECRET, event } from '../support/events.js';

describe('ObservabilityExporterRepository inbox boundary', () => {
  it('commits only envelope metadata plus a payload hash before accepting Kafka delivery', async () => {
    const client = new AcceptingClient();
    const repository = new ObservabilityExporterRepository({
      connect: async () => client,
    } as unknown as Pool);
    const source = event(
      7,
      'user.message',
      { content: TRANSCRIPT_SECRET },
      { id: 'evt_inbox_hash', producedBy: 'client' },
    );

    await expect(repository.acceptEvent(source)).resolves.toBe('accepted');
    expect(client.released).toBe(true);
    expect(client.queries.map((query) => query.text)).toContain('COMMIT');
    const inbox = client.queries.find((query) =>
      query.text.includes('INSERT INTO observability_exporter_event_inbox'),
    );
    expect(inbox?.values).toEqual([
      source.workspaceId,
      source.sessionId,
      eventIdentityKey(source.id),
      '7',
      expect.stringMatching(/^[0-9a-f]{64}$/),
      false,
    ]);
    expect(JSON.stringify(client.queries)).not.toContain(TRANSCRIPT_SECRET);
  });

  it('uses stable content-free hashes for duplicate and conflict detection', () => {
    const source = event(
      4,
      'user.message',
      { content: TRANSCRIPT_SECRET },
      { id: 'evt_hash', producedBy: 'client' },
    );
    const replay = { ...source, payload: Buffer.from(source.payload) };
    const changedPayload = { ...source, payload: Buffer.from('changed transcript payload') };

    expect(hashTranscriptEnvelope(source)).toBe(hashTranscriptEnvelope(replay));
    expect(hashTranscriptEnvelope({ ...source, seq: 99 })).toBe(hashTranscriptEnvelope(source));
    expect(hashTranscriptEnvelope(source)).not.toBe(hashTranscriptEnvelope(changedPayload));
    expect(hashTranscriptEnvelope(source)).toMatch(/^[0-9a-f]{64}$/);
    expect(stableJson({ b: 1, a: ['x'] })).toBe('{"a":["x"],"b":1}');
  });
});

describe('ObservabilityExporterRepository outbox selection boundary', () => {
  it('returns immediately when no binding is ready', async () => {
    const { repository, connect, query, release } = contendedOutboxFixture();
    query.mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(repository.claimOutbox('idle-delivery', 30_000)).resolves.toBeNull();
    expect(connect).toHaveBeenCalledOnce();
    expect(query).not.toHaveBeenCalledWith(expect.stringContaining('pg_advisory_xact_lock'), [
      'aob_contended',
    ]);
    expect(query).toHaveBeenLastCalledWith('COMMIT');
    expect(release).toHaveBeenCalledOnce();
  });

  it('bounds repeated claim misses and commits/releases before every new selection', async () => {
    const { repository, connect, query, release } = contendedOutboxFixture();

    await expect(repository.claimOutbox('contended-delivery', 30_000)).resolves.toBeNull();
    expect(connect).toHaveBeenCalledTimes(8);
    expect(release).toHaveBeenCalledTimes(8);
    const commits = query.mock.calls.flatMap(([sql], index) =>
      sql === 'COMMIT' ? [query.mock.invocationCallOrder[index]!] : [],
    );
    expect(commits).toHaveLength(8);
    for (let index = 0; index < commits.length; index += 1) {
      expect(commits[index]).toBeLessThan(release.mock.invocationCallOrder[index]!);
      if (index + 1 < commits.length) {
        expect(release.mock.invocationCallOrder[index]).toBeLessThan(
          connect.mock.invocationCallOrder[index + 1]!,
        );
      }
    }
  });

  it('does not start a claim when already aborted', async () => {
    const { repository, connect } = contendedOutboxFixture();
    const controller = new AbortController();
    controller.abort(new Error('test shutdown'));

    await expect(
      repository.claimOutbox('aborted-delivery', 30_000, controller.signal),
    ).rejects.toThrow('test shutdown');
    expect(connect).not.toHaveBeenCalled();
  });

  it('does not retry a lost selection after shutdown', async () => {
    const { repository, connect, release } = contendedOutboxFixture();
    const controller = new AbortController();
    release.mockImplementation(() => controller.abort(new Error('test shutdown')));

    await expect(
      repository.claimOutbox('aborted-delivery', 30_000, controller.signal),
    ).rejects.toThrow('test shutdown');
    expect(connect).toHaveBeenCalledOnce();
    expect(release).toHaveBeenCalledOnce();
  });

  it('rolls back and releases rather than retrying database errors', async () => {
    const { repository, connect, query, release } = contendedOutboxFixture();
    query.mockRejectedValueOnce(new Error('database unavailable'));

    await expect(repository.claimOutbox('failed-delivery', 30_000)).rejects.toThrow(
      'database unavailable',
    );
    expect(connect).toHaveBeenCalledOnce();
    expect(query).toHaveBeenLastCalledWith('ROLLBACK');
    expect(release).toHaveBeenCalledOnce();
  });
});

describe('ObservabilityExporterRepository delivery retry boundary', () => {
  it('atomically increments attempt count and schedules a fenced positive delay', async () => {
    const queries: RecordedQuery[] = [];
    const query = vi.fn(async (text: string, values?: readonly unknown[]) => {
      queries.push({ text, values });
      return { rows: [{ scheduled: true }], rowCount: 1 };
    });
    const repository = new ObservabilityExporterRepository({ query } as unknown as Pool);
    const item = outboxClaimStub();

    await expect(repository.scheduleOutboxRetry(item, 0, 'otlp_http_503')).rejects.toThrow(
      'retry delay is invalid',
    );
    await expect(
      repository.scheduleOutboxRetry(item, 1_250, 'Retry-After: secret'),
    ).rejects.toThrow('error code is invalid');
    expect(query).not.toHaveBeenCalled();

    await repository.scheduleOutboxRetry(item, 1_250, 'otlp_http_503');

    expect(query).toHaveBeenCalledOnce();
    expect(queries[0]?.text).toContain('delivery_attempt_count = delivery_attempt_count + 1');
    expect(queries[0]?.text).toContain(
      "available_at = now() + ($4::bigint * interval '1 millisecond')",
    );
    expect(queries[0]?.text).toContain('INSERT INTO observability_exporter_binding_cooldowns');
    expect(queries[0]?.values).toEqual([
      item.id,
      item.leaseOwner,
      item.leaseGeneration,
      1_250,
      'otlp_http_503',
    ]);
  });

  it('keeps shutdown claim release separate from attempt accounting', async () => {
    const queries: RecordedQuery[] = [];
    const query = vi.fn(async (text: string, values?: readonly unknown[]) => {
      queries.push({ text, values });
      return { rows: [], rowCount: 1 };
    });
    const repository = new ObservabilityExporterRepository({ query } as unknown as Pool);

    await repository.releaseOutboxClaim(outboxClaimStub());

    expect(queries[0]?.text).toContain('available_at = now()');
    expect(queries[0]?.text).not.toContain('delivery_attempt_count');
  });
});

describe('ObservabilityExporterRepository delivery outcome boundary', () => {
  const warning = {
    kind: 'accepted_with_warning',
    rejectedSpans: '0',
    messageBytes: 1,
    messageSha256: 'a'.repeat(64),
  };
  const partial = { ...warning, kind: 'partial_rejection', rejectedSpans: '1' };

  it.each([
    { outcome: { kind: 'accepted' }, expected: ['accepted', '0', null, null] },
    {
      outcome: {
        kind: 'accepted_with_warning',
        rejectedSpans: '0',
        messageBytes: 1,
        messageSha256: 'a'.repeat(64),
      },
      expected: ['accepted_with_warning', '0', 1, 'a'.repeat(64)],
    },
    {
      outcome: {
        kind: 'partial_rejection',
        rejectedSpans: '9223372036854775807',
        messageBytes: 0,
        messageSha256: 'b'.repeat(64),
      },
      expected: ['partial_rejection', '9223372036854775807', 0, 'b'.repeat(64)],
    },
  ])(
    'persists only safe scalar metadata for $outcome.kind with the existing lease fence',
    async ({ outcome, expected }) => {
      const query = vi.fn(async (_text: string, _values?: readonly unknown[]) => ({
        rows: [],
        rowCount: 1,
      }));
      const repository = new ObservabilityExporterRepository({ query } as unknown as Pool);
      const item = outboxClaimStub();
      const untrusted = Object.assign(outcome, {
        errorMessage: TRANSCRIPT_SECRET,
        rawMessage: TRANSCRIPT_SECRET,
        headers: { authorization: 'sk-outcome-secret' },
        credential: 'sk-outcome-secret',
      });

      await repository.completeOutboxDelivery(item, untrusted as OtlpDeliveryOutcome);

      expect(query).toHaveBeenCalledOnce();
      const [sql, values] = query.mock.calls[0]!;
      expect(values).toEqual([item.id, item.leaseOwner, item.leaseGeneration, ...expected]);
      expect(sql).toContain("status = 'pending'");
      expect(sql).toContain('lease_owner = $2 AND lease_generation = $3::bigint');
      expect(sql).toContain('lease_until > now()');
      expect(sql).toContain('delivery_rejected_spans = $5::bigint');
      expect(JSON.stringify(query.mock.calls)).not.toContain(TRANSCRIPT_SECRET);
      expect(JSON.stringify(query.mock.calls)).not.toContain('sk-outcome-secret');
    },
  );

  it.each([
    { name: 'null', outcome: null },
    { name: 'array', outcome: [] },
    { name: 'missing kind', outcome: {} },
    { name: 'unknown kind', outcome: { kind: TRANSCRIPT_SECRET } },
    {
      name: 'accepted with contradictory count',
      outcome: { kind: 'accepted', rejectedSpans: '1' },
    },
    {
      name: 'accepted with message metadata',
      outcome: { kind: 'accepted', messageSha256: TRANSCRIPT_SECRET },
    },
    ...[undefined, '1', 0, null].map((rejectedSpans, index) => ({
      name: `warning count ${index}`,
      outcome: { ...warning, rejectedSpans },
    })),
    ...[
      undefined,
      null,
      '',
      '0',
      '-1',
      '+1',
      '01',
      ' 1',
      '1 ',
      '1\n',
      '1.0',
      '1e2',
      '9223372036854775808',
      1,
      1n,
      { toPostgres: () => TRANSCRIPT_SECRET },
    ].map((rejectedSpans, index) => ({
      name: `partial count ${index}`,
      outcome: { ...partial, rejectedSpans },
    })),
    { name: 'empty warning message', outcome: { ...warning, messageBytes: 0 } },
    ...[
      undefined,
      null,
      -1,
      0.5,
      65_537,
      NaN,
      Infinity,
      '1',
      { toPostgres: () => TRANSCRIPT_SECRET },
    ].map((messageBytes, index) => ({
      name: `message bytes ${index}`,
      outcome: { ...partial, messageBytes },
    })),
    ...[
      undefined,
      null,
      'a'.repeat(63),
      'A'.repeat(64),
      'g'.repeat(64),
      'a'.repeat(64) + '\n',
      TRANSCRIPT_SECRET,
      { toPostgres: () => TRANSCRIPT_SECRET },
    ].map((messageSha256, index) => ({
      name: `message digest ${index}`,
      outcome: { ...partial, messageSha256 },
    })),
  ])(
    'rejects malformed safe metadata without querying or echoing input: $name',
    async ({ outcome }) => {
      const query = vi.fn(async () => ({ rows: [], rowCount: 1 }));
      const repository = new ObservabilityExporterRepository({ query } as unknown as Pool);

      await expect(
        repository.completeOutboxDelivery(outboxClaimStub(), outcome as OtlpDeliveryOutcome),
      ).rejects.toThrow(new Error('observability exporter delivery outcome is invalid'));
      expect(query).not.toHaveBeenCalled();
    },
  );

  it('reports a lost terminal fence rather than retrying or writing outside it', async () => {
    const query = vi.fn(async () => ({ rows: [], rowCount: 0 }));
    const repository = new ObservabilityExporterRepository({ query } as unknown as Pool);

    await expect(
      repository.completeOutboxDelivery(outboxClaimStub(), { kind: 'accepted' }),
    ).rejects.toThrow('lease lost');
    expect(query).toHaveBeenCalledOnce();
  });
});

function contendedOutboxFixture() {
  const query = vi.fn(async (sql: string, _values?: readonly unknown[]) => ({
    rows: sql.includes('SELECT o.binding_id') ? [{ binding_id: 'aob_contended' }] : [],
    rowCount: sql.includes('SELECT o.binding_id') ? 1 : 0,
  }));
  const release = vi.fn((): void => undefined);
  const connect = vi.fn(async () => ({ query, release }));
  const repository = new ObservabilityExporterRepository({ connect } as unknown as Pool);
  return { repository, connect, query, release };
}

interface RecordedQuery {
  text: string;
  values: readonly unknown[] | undefined;
}

class AcceptingClient {
  readonly queries: RecordedQuery[] = [];
  released = false;

  async query<T = Record<string, never>>(
    text: string,
    values?: readonly unknown[],
  ): Promise<{ rows: T[]; rowCount: number }> {
    this.queries.push({ text, values });
    if (text.includes('SELECT next_seq::text')) {
      return { rows: [{ next_seq: '0' }] as T[], rowCount: 1 };
    }
    if (text.includes('INSERT INTO observability_exporter_event_inbox')) {
      return { rows: [{ event_key: 'a'.repeat(64) }] as T[], rowCount: 1 };
    }
    return { rows: [], rowCount: 1 };
  }

  release(): void {
    this.released = true;
  }
}

function outboxClaimStub(): ClaimedOutboxItem {
  return {
    id: '7',
    attemptCount: 0,
    leaseOwner: 'worker-delivery',
    leaseGeneration: '3',
  } as ClaimedOutboxItem;
}
