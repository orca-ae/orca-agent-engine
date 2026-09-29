// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Accept only the smart-HTTP read subset, never arbitrary bytes or receive-pack.
// https://git-scm.com/docs/gitprotocol-pack and gitprotocol-v2 define these frames.
const oid = '(?:[0-9a-f]{40}|[0-9a-f]{64})';
const objectLine = new RegExp(`^(want|have|shallow) ${oid}$`);
const capabilities = new Set([
  'multi_ack',
  'multi_ack_detailed',
  'thin-pack',
  'side-band',
  'side-band-64k',
  'ofs-delta',
  'shallow',
  'deepen-since',
  'deepen-not',
  'deepen-relative',
  'no-progress',
  'include-tag',
  'no-done',
  'filter',
]);
const ref = /^[A-Za-z0-9_./*-]{1,1024}$/;
function invalid(): never {
  throw new Error('Invalid Git upload-pack request');
}
function capability(value: string): boolean {
  return (
    capabilities.has(value) ||
    /^agent=[A-Za-z0-9._/+()-]{1,128}$/.test(value) ||
    /^object-format=(sha1|sha256)$/.test(value)
  );
}
function fetchArgument(value: string): boolean {
  return (
    objectLine.test(value) ||
    [
      'done',
      'thin-pack',
      'no-progress',
      'include-tag',
      'ofs-delta',
      'deepen-relative',
      'wait-for-done',
      'sideband-all',
    ].includes(value) ||
    /^deepen(?:-since)? [1-9][0-9]{0,9}$/.test(value) ||
    (value.startsWith('deepen-not ') && ref.test(value.slice(11))) ||
    /^filter (?:blob:none|blob:limit=[0-9]{1,10}|tree:[0-9]{1,5})$/.test(value)
  );
}

export function validateGitUploadPack(body: Buffer, protocol?: string): void {
  const packets: Array<string | number> = [];
  for (let offset = 0; offset < body.length; ) {
    const header = body.subarray(offset, offset + 4).toString('latin1');
    if (!/^[0-9a-f]{4}$/.test(header)) invalid();
    const size = parseInt(header, 16);
    offset += 4;
    if (size === 0 || size === 1) {
      packets.push(size);
      continue;
    }
    if (size < 5 || size > 65520 || offset + size - 4 > body.length) invalid();
    const bytes = body.subarray(offset, offset + size - 4);
    if (
      bytes.some(
        (byte, index) => (byte < 32 || byte > 126) && !(byte === 10 && index === bytes.length - 1),
      )
    )
      invalid();
    packets.push(bytes.toString('ascii').replace(/\n$/, ''));
    offset += size - 4;
  }
  if (packets.length < 2) invalid();
  if (protocol === 'version=2') {
    const command = packets.shift();
    if (command !== 'command=ls-refs' && command !== 'command=fetch') invalid();
    const delimiter = packets.indexOf(1);
    if (delimiter < 0 || packets.at(-1) !== 0) invalid();
    if (
      !packets
        .slice(0, delimiter)
        .every(
          (line) =>
            typeof line === 'string' &&
            (/^agent=[A-Za-z0-9._/+()-]{1,128}$/.test(line) ||
              /^object-format=(sha1|sha256)$/.test(line)),
        )
    )
      invalid();
    const args = packets.slice(delimiter + 1, -1);
    if (
      !args.every(
        (line) =>
          typeof line === 'string' &&
          (command === 'command=fetch'
            ? fetchArgument(line)
            : ['peel', 'symrefs', 'unborn'].includes(line) ||
              (line.startsWith('ref-prefix ') && ref.test(line.slice(11)))),
      )
    )
      invalid();
    if (
      command === 'command=fetch' &&
      !args.some((line) => typeof line === 'string' && line.startsWith('want '))
    )
      invalid();
    return;
  }
  // v0/v1: wants and shallow/deepen requests, flush, then negotiation/done.
  const first = packets.shift();
  if (typeof first !== 'string') invalid();
  const [want, hash, ...caps] = first.split(' ');
  if (want !== 'want' || !new RegExp(`^${oid}$`).test(hash ?? '') || !caps.every(capability))
    invalid();
  const flush = packets.indexOf(0);
  if (flush < 0) invalid();
  if (
    !packets
      .slice(0, flush)
      .every(
        (line) =>
          typeof line === 'string' &&
          !line.startsWith('have ') &&
          line !== 'done' &&
          fetchArgument(line),
      )
  )
    invalid();
  const negotiation = packets.slice(flush + 1);
  if (
    !negotiation.every((line, index) =>
      typeof line === 'string'
        ? new RegExp(`^have ${oid}$`).test(line) ||
          (line === 'done' && index === negotiation.length - 1)
        : line === 0 && index === negotiation.length - 1,
    )
  )
    invalid();
}
