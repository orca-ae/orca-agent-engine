// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { posix } from 'node:path';

export interface SandboxEgressGitClone {
  url: string;
  mountPath: string;
  checkout?: { type: 'branch' | 'commit'; value: string };
}

function quote(value: string): string {
  return "'" + value.replaceAll("'", "'\\''") + "'";
}

/** Run inside an enforced sandbox after bootstrap; no credential appears in argv. */
export function buildSandboxEgressGitCloneCommand(input: SandboxEgressGitClone): string {
  let url: URL;
  try {
    url = new URL(input.url);
  } catch {
    throw new Error('sandbox egress Git clone requires a canonical HTTPS URL');
  }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
    throw new Error('sandbox egress Git clone requires a canonical HTTPS URL');
  }
  const target = input.mountPath;
  if (
    !target.startsWith('/') ||
    target === '/' ||
    posix.normalize(target) !== target ||
    target.includes('\0') ||
    /[\r\n]/.test(target)
  ) {
    throw new Error('sandbox egress Git clone requires a canonical mount path');
  }
  const ref = input.checkout?.value;
  if (
    ref !== undefined &&
    (!ref || ref.startsWith('-') || ref.includes('\0') || /[\r\n]/.test(ref))
  ) {
    throw new Error('invalid sandbox egress Git ref');
  }
  if (input.checkout?.type === 'commit' && !/^[a-fA-F0-9]{40}$/.test(ref!)) {
    throw new Error('sandbox egress Git commit requires a full SHA');
  }
  const args = ['git', '-c', 'http.followRedirects=false', 'clone', '--filter=blob:none'];
  if (input.checkout?.type !== 'commit') args.push('--depth=1');
  if (input.checkout?.type === 'branch') args.push('--branch', ref!);
  args.push('--', input.url);
  return [
    'set -eu',
    'target=' + quote(target),
    'parent=' + quote(posix.dirname(target)),
    'test ! -L "$parent" && test ! -L "$target"',
    'mkdir -p -- "$parent"',
    'stage=$(mktemp -d "$parent/.orca-git-XXXXXXXX")',
    'trap \'rm -rf -- "$stage"\' EXIT',
    args.map(quote).join(' ') + ' "$stage/repo" >/dev/null 2>&1',
    ...(input.checkout?.type === 'commit'
      ? ['git -C "$stage/repo" checkout --detach ' + quote(ref!) + ' >/dev/null 2>&1']
      : []),
    // Execd prepares as root; the trusted managed images run tools as UID/GID 1000.
    'if [ "$(id -u)" = 0 ]; then chown -hR 1000:1000 "$stage/repo"; fi',
    // GNU mv replaces an empty preparation directory, but refuses a nonempty mount.
    'mv -T -- "$stage/repo" "$target"',
  ].join('\n');
}
