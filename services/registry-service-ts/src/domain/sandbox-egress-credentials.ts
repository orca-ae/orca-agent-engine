// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { egressHost } from '../contracts/sandbox-egress.contract.js';

export type EgressCredentialSource =
  | { kind: 'git_resource'; resourceId: string }
  | { kind: 'vault_environment_variable'; vaultId: string; credentialId: string };
export interface EgressCredential {
  repositoryUrl?: string;
  marker: string;
  environmentName: string;
  source: EgressCredentialSource;
  targets: string[];
}

export const protectedEgressHeaders = new Set([
  'host',
  'connection',
  'content-length',
  'transfer-encoding',
  'proxy-authorization',
  'proxy-authenticate',
  'proxy-connection',
  'upgrade',
  'trailer',
  'te',
  'keep-alive',
  'cookie',
  'set-cookie',
]);

export function egressMarkers(value: string): RegExpMatchArray[] {
  return [...value.matchAll(/orca_(?:vault|git)_[A-Za-z0-9_-]{43}/g)];
}

/** latin1 preserves every original credential byte, including non-UTF-8 bytes. */
export function decodeEgressBasic(value: string): string | null {
  const match = /^Basic ([A-Za-z0-9+/]+={0,2})$/i.exec(value);
  if (!match) return null;
  const decoded = Buffer.from(match[1]!, 'base64');
  if (decoded.toString('base64') !== match[1] || !decoded.includes(0x3a)) return null;
  return decoded.toString('latin1');
}

/** Raw paths only: reject decoding ambiguity before URL normalisation can hide traversal. */
export function gitEgressOperation(
  repoUrl: string,
  host: string,
  method: string,
  pathAndQuery: string,
  restEnabled: boolean,
): boolean {
  if (
    !pathAndQuery.startsWith('/') ||
    pathAndQuery.startsWith('//') ||
    /[%\\#\p{Cc}\s]/u.test(pathAndQuery)
  )
    return false;
  const [path, query = ''] = pathAndQuery.split('?');
  if (
    pathAndQuery.split('?').length > 2 ||
    path!.split('/').some((part) => part === '.' || part === '..')
  )
    return false;
  const repo = new URL(repoUrl);
  const repoPath = repo.pathname.replace(/\/+$/, '').replace(/\.git$/, '');
  if (
    host === repo.hostname &&
    [repoPath, `${repoPath}.git`].some(
      (base) =>
        (method === 'GET' && path === `${base}/info/refs` && query === 'service=git-upload-pack') ||
        (method === 'POST' && path === `${base}/git-upload-pack` && !query),
    )
  )
    return true;
  if (
    !restEnabled ||
    host !== 'api.github.com' ||
    repo.hostname !== 'github.com' ||
    method !== 'GET'
  )
    return false;
  const prefix = `/repos${repoPath}`;
  if (!path!.startsWith(prefix)) return false;
  const suffix = path!.slice(prefix.length);
  if (
    !/^(?:|\/branches(?:\/[A-Za-z0-9_.-]+)?|\/commits(?:\/[A-Za-z0-9_.-]+)?|\/contents(?:\/[A-Za-z0-9_.-]+)+|\/issues(?:\/[1-9][0-9]*)?|\/pulls(?:\/[1-9][0-9]*)?)$/.test(
      suffix,
    )
  )
    return false;
  const params = new URLSearchParams(query);
  const seen = new Set<string>();
  for (const [key, value] of params) {
    if (seen.has(key)) return false;
    seen.add(key);
    if (['page', 'per_page'].includes(key)) {
      if (!/^[1-9][0-9]{0,5}$/.test(value)) return false;
    } else if (key === 'state' && ['open', 'closed', 'all'].includes(value)) continue;
    else if (key === 'ref' && /^[A-Za-z0-9_./-]{1,256}$/.test(value)) continue;
    else return false;
  }
  return true;
}

export function vaultEgressTargets(networking: unknown, authConfig: unknown): string[] | null {
  const net = networking as { type?: unknown; allowed_hosts?: unknown };
  const config = authConfig as { injection_location?: { header?: unknown; body?: unknown } };
  const location = config?.injection_location ?? { header: true, body: false };
  if (
    location.header !== true ||
    location.body !== false ||
    net?.type !== 'limited' ||
    !Array.isArray(net.allowed_hosts) ||
    !net.allowed_hosts.length ||
    net.allowed_hosts.length > 16
  )
    return null;
  if (!net.allowed_hosts.every((host) => egressHost.safeParse(host).success)) return null;
  return [...new Set(net.allowed_hosts as string[])];
}
