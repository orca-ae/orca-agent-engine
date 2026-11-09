// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  DSH_PROFILE_DIRECTORY,
  DSH_PROFILE_FILENAMES,
  parseDshProfileDescriptor,
  type DshProfileDescriptor,
} from '@orca/harness-catalog';

export function prepareDshProfilePlan(
  value: DshProfileDescriptor | undefined,
  networking?: Record<string, unknown>,
  managedNetwork = false,
): DshProfileDescriptor | undefined {
  if (value === undefined) return undefined;
  if (!managedNetwork && networking?.type !== undefined && networking.type !== 'unrestricted')
    throw new Error('DSH profile preparation requires unrestricted Environment networking');
  return parseDshProfileDescriptor(value);
}

/** Verify the actual returned sandbox, including every fixed subPath mount. */
export function buildDshProfileProbeCommand(value: DshProfileDescriptor): string {
  const profile = parseDshProfileDescriptor(value);
  const script = [
    "const fs = require('node:fs'); const crypto = require('node:crypto');",
    'const root = ' + JSON.stringify(DSH_PROFILE_DIRECTORY) + ';',
    'const names = ' + JSON.stringify(DSH_PROFILE_FILENAMES) + ';',
    "const mounts = fs.readFileSync('/proc/self/mountinfo', 'utf8').split(String.fromCharCode(10)).map(line => line.split(' '));",
    "for (const name of [...names, 'node_modules', '.orca-profile-revision']) {",
    "  const path = root + '/' + name;",
    "  if (!mounts.some(fields => fields[4] === path && fields[5].split(',').includes('ro')))",
    "    throw new Error('DSH profile requires read-only configuration and node_modules mounts');",
    '  const stat = fs.lstatSync(path);',
    "  if (stat.isSymbolicLink() || (name === 'node_modules' ? !stat.isDirectory() : !stat.isFile()))",
    "    throw new Error('invalid DSH profile mount');",
    '}',
    "if (fs.readFileSync(root + '/.orca-profile-revision', 'utf8') !== " +
      JSON.stringify(profile.config_sha256) +
      ')',
    "  throw new Error('DSH profile revision mismatch');",
    'const texts = names.map(name => {',
    "  const path = root + '/' + name;",
    "  if (fs.statSync(path).size > 1048576) throw new Error('DSH configuration exceeds limit');",
    "  return [name, fs.readFileSync(path, 'utf8')];",
    '});',
    "if (crypto.createHash('sha256').update(JSON.stringify([1, texts])).digest('hex') !== " +
      JSON.stringify(profile.config_sha256) +
      ')',
    "  throw new Error('DSH profile configuration mismatch');",
  ].join(String.fromCharCode(10));
  const quote = String.fromCharCode(39);
  return (
    'env -i PATH=/usr/local/bin:/usr/bin node -e ' +
    quote +
    script.replaceAll(quote, quote + '"' + quote + '"' + quote) +
    quote
  );
}
