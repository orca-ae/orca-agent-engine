// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Structural admission only; the pinned runtime codec validates native payload semantics. */
export function validateDshCheckpoint(state: unknown): void {
  if (!state || typeof state !== 'object' || Array.isArray(state))
    throw new Error('invalid DSH checkpoint');
  const value = state as Record<string, unknown>;
  if (
    value.version !== 1 ||
    value.format !== 'dsh_sdk' ||
    value.sdkVersion !== '0.2.1-alpha.2' ||
    typeof value.threadId !== 'string' ||
    !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value.threadId) ||
    typeof value.instructionsSha256 !== 'string' ||
    !/^[a-f0-9]{64}$/.test(value.instructionsSha256) ||
    !value.files ||
    typeof value.files !== 'object' ||
    Array.isArray(value.files)
  )
    throw new Error('invalid DSH checkpoint');
  const entries = Object.entries(value.files);
  if (entries.length < 2 || entries.length > 129 || !Object.hasOwn(value.files, 'orca.json'))
    throw new Error('invalid DSH checkpoint files');
  let size = 0;
  for (const [path, data] of entries) {
    if (
      path.length > 512 ||
      (path !== 'orca.json' &&
        !/^[A-Za-z0-9_.~%-]+[/][a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}[/]session[.]v4[.]jsonl$/.test(
          path,
        )) ||
      path.split('/').some((part) => part === '.' || part === '..') ||
      typeof data !== 'string' ||
      !data.length ||
      data.length % 4 !== 0 ||
      !/^[A-Za-z0-9+/]*={0,2}$/.test(data)
    )
      throw new Error('invalid DSH checkpoint file');
    size += (data.length / 4) * 3 - (data.endsWith('==') ? 2 : data.endsWith('=') ? 1 : 0);
    if (size > 16 * 1024 * 1024) throw new Error('DSH checkpoint exceeds size limit');
    // Reject nonzero padding bits without decoding or allocating the payload.
    const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    if (
      (data.endsWith('==') && (alphabet.indexOf(data[data.length - 3]!) & 15) !== 0) ||
      (data.endsWith('=') &&
        !data.endsWith('==') &&
        (alphabet.indexOf(data[data.length - 2]!) & 3) !== 0)
    )
      throw new Error('invalid DSH checkpoint encoding');
  }
}
