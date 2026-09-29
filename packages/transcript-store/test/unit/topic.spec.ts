// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  sessionTopicName,
  matchSessionTopic,
  parseSessionTopic,
  sessionTopicPattern,
  validateTopicPrefix,
  parseCursor,
  formatCursor,
} from '../../src/kafka/topic.js';

describe('sessionTopicName', () => {
  it('selects disjoint raw and avro topic sets, including prefixed and KoP names', () => {
    const raw = 'orca.ws_a.sessions.s_1.events';
    const avro = 'orca.ws_a.sessions.s_1.events-avro';
    for (const prefix of ['', 'public.default.']) {
      expect(sessionTopicName('ws_a', 's_1', prefix, 'avro')).toBe(prefix + avro);
      expect(sessionTopicName('ws_a', 's_1', prefix, 'raw')).toBe(prefix + raw);
      for (const listedPrefix of ['', prefix]) {
        expect(matchSessionTopic(listedPrefix + avro, prefix, 'avro')).toEqual({
          workspaceId: 'ws_a',
          sessionId: 's_1',
          canonicalTopic: prefix + avro,
        });
        expect(matchSessionTopic(listedPrefix + raw, prefix, 'avro')).toBeNull();
        expect(matchSessionTopic(listedPrefix + avro, prefix)).toBeNull();
      }
    }
    expect(parseSessionTopic(avro, 'avro')).toEqual({ workspaceId: 'ws_a', sessionId: 's_1' });
    expect(parseSessionTopic(raw, 'avro')).toBeNull();
    expect(parseSessionTopic(avro)).toBeNull();
    expect(sessionTopicPattern('avro').test(avro)).toBe(true);
    expect(sessionTopicPattern('avro').test(raw)).toBe(false);
    expect(sessionTopicPattern().test(raw)).toBe(true);
    expect(sessionTopicPattern().test(avro)).toBe(false);
    expect(sessionTopicPattern('avro').test(avro + '.extra')).toBe(false);
  });
  it('builds the per-session topic name', () => {
    expect(sessionTopicName('ws_abc', 'ses_123')).toBe('orca.ws_abc.sessions.ses_123.events');
  });
  it('prepends a configured topic prefix', () => {
    expect(sessionTopicName('ws_abc', 'ses_123', 'public.default.')).toBe(
      'public.default.orca.ws_abc.sessions.ses_123.events',
    );
  });
  it('rejects invalid topic prefixes', () => {
    expect(() => sessionTopicName('ws_x', 'ses_x', 'public.default')).toThrow(
      /invalid topic prefix/,
    );
    expect(() => sessionTopicName('ws_x', 'ses_x', '.public.')).toThrow(/invalid topic prefix/);
    expect(() => sessionTopicName('ws_x', 'ses_x', 'pub lic.')).toThrow(/invalid topic prefix/);
  });
  it('rejects workspace ids containing forbidden characters', () => {
    expect(() => sessionTopicName('ws/bad', 'ses_x')).toThrow(/invalid workspace_id/);
    expect(() => sessionTopicName('ws abc', 'ses_x')).toThrow(/invalid workspace_id/);
  });
  it('rejects session ids containing forbidden characters', () => {
    expect(() => sessionTopicName('ws_x', 'ses bad')).toThrow(/invalid session_id/);
  });
});

describe('validateTopicPrefix', () => {
  it('accepts empty and dot-terminated prefixes', () => {
    expect(() => validateTopicPrefix('')).not.toThrow();
    expect(() => validateTopicPrefix('public.default.')).not.toThrow();
    expect(() => validateTopicPrefix('tenant-1.ns_2.')).not.toThrow();
  });
  it('rejects prefixes without a trailing dot or with empty segments', () => {
    expect(() => validateTopicPrefix('public.default')).toThrow(/invalid topic prefix/);
    expect(() => validateTopicPrefix('public..')).toThrow(/invalid topic prefix/);
    expect(() => validateTopicPrefix('.')).toThrow(/invalid topic prefix/);
  });
});

describe('matchSessionTopic', () => {
  it('matches a bare session topic without a prefix', () => {
    expect(matchSessionTopic('orca.ws_a.sessions.s_1.events')).toEqual({
      workspaceId: 'ws_a',
      sessionId: 's_1',
      canonicalTopic: 'orca.ws_a.sessions.s_1.events',
    });
  });
  it('matches a BARE listed name when a prefix is configured (KoP listing asymmetry)', () => {
    expect(matchSessionTopic('orca.ws_a.sessions.s_1.events', 'public.default.')).toEqual({
      workspaceId: 'ws_a',
      sessionId: 's_1',
      canonicalTopic: 'public.default.orca.ws_a.sessions.s_1.events',
    });
  });
  it('matches a fully prefixed name when a prefix is configured (plain Kafka listing)', () => {
    expect(
      matchSessionTopic('public.default.orca.ws_a.sessions.s_1.events', 'public.default.'),
    ).toEqual({
      workspaceId: 'ws_a',
      sessionId: 's_1',
      canonicalTopic: 'public.default.orca.ws_a.sessions.s_1.events',
    });
  });
  it('returns null for non-session topics', () => {
    expect(matchSessionTopic('other.topic', 'public.default.')).toBeNull();
    expect(matchSessionTopic('public.default.other.topic', 'public.default.')).toBeNull();
    expect(matchSessionTopic('orca.ws_a.sessions.s_1.events.extra', '')).toBeNull();
  });
  it('rejects invalid topic prefixes', () => {
    expect(() => matchSessionTopic('orca.ws_a.sessions.s_1.events', 'public.default')).toThrow(
      /invalid topic prefix/,
    );
  });
});

describe('parseCursor / formatCursor', () => {
  it('round-trips a numeric offset', () => {
    expect(parseCursor('42')).toBe(42n);
    expect(formatCursor(42n)).toBe('42');
  });
  it('returns null for empty cursor (consume from beginning)', () => {
    expect(parseCursor('')).toBeNull();
  });
  it('rejects non-numeric cursors', () => {
    expect(() => parseCursor('not-a-number')).toThrow(/invalid cursor/);
  });
});
