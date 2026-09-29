// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { validateGitUploadPack } from '../../src/domain/git-upload-pack.js';
const pkt = (line: string) =>
  `${(Buffer.byteLength(line) + 4).toString(16).padStart(4, '0')}${line}`;
const oid = 'a'.repeat(40);
const v2 = (command: string, ...args: string[]) =>
  Buffer.from(pkt(`command=${command}\n`) + '0001' + args.map(pkt).join('') + '0000');

describe('Git upload-pack grammar', () => {
  it.each([
    v2('ls-refs', 'peel\n', 'symrefs\n', 'ref-prefix refs/heads/main\n'),
    v2('fetch', `want ${oid}\n`, 'thin-pack\n', 'ofs-delta\n', 'deepen 1\n', 'done\n'),
  ])('accepts native v2 read commands', (body) => {
    expect(() => validateGitUploadPack(body, 'version=2')).not.toThrow();
  });
  it('accepts v0/v1 shallow clone and fetch negotiation', () => {
    const body = Buffer.from(
      pkt(`want ${oid} multi_ack_detailed side-band-64k thin-pack ofs-delta agent=git/2.50\n`) +
        pkt('deepen 1\n') +
        '0000' +
        pkt(`have ${oid}\n`) +
        pkt('done\n'),
    );
    expect(() => validateGitUploadPack(body)).not.toThrow();
    expect(() => validateGitUploadPack(body, 'version=1')).not.toThrow();
  });
  it.each([
    Buffer.from('private session data'),
    Buffer.from('fffftruncated'),
    Buffer.from('0003'),
    Buffer.from('0002'),
    Buffer.from('00000000'),
    Buffer.from('0004'),
    v2('receive-pack', `want ${oid}\n`),
    v2('ls-refs', 'private session data\n'),
    v2('fetch', 'want secret\n'),
    v2('fetch', `want ${oid}\nsecret\n`),
    v2('fetch', `want ${oid}\0`),
    v2('fetch', 'done\n'),
    Buffer.concat([v2('ls-refs', 'peel\n'), Buffer.from('secret')]),
    Buffer.from(pkt('command=ls-refs\n') + pkt('server-option=secret\n') + '00010000'),
    Buffer.from(pkt('command=fetch\n') + '0000'),
  ])('rejects malformed framing, arbitrary bytes, writes and unknown extensions', (body) => {
    expect(() => validateGitUploadPack(body, 'version=2')).toThrow(
      'Invalid Git upload-pack request',
    );
  });
  it.each([
    pkt(`want ${oid} arbitrary=secret\n`) + '0000',
    pkt(`want ${oid}\n`) + pkt('private data\n') + '0000',
    pkt(`want ${oid}\n`) + '0000' + pkt('private data\n'),
    pkt(`want ${oid}\n`) + '0001',
  ])('rejects invalid legacy requests', (body) => {
    expect(() => validateGitUploadPack(Buffer.from(body))).toThrow();
  });
});
