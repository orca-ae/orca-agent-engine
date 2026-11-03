// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { INTERNAL_WS_ORIGIN } from '@orca/harness-tunnel';

/** Shared by runner/worker Origin checks and runner identity resolution. */
export function hostnameIsLoopback(host: string | undefined): boolean {
  if (host === undefined) return false;
  if (host === 'localhost' || host === '::1') return true;
  if (isIpv4LoopbackLiteral(host)) return true;
  const mapped = host.toLowerCase().match(/:ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  return mapped !== null && isIpv4LoopbackLiteral(mapped[1]!);
}

function isIpv4LoopbackLiteral(host: string): boolean {
  const parts = host.split('.');
  if (parts.length !== 4) return false;
  const octets = parts.map((part) => Number(part));
  if (octets.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) return false;
  // Reject octets such as "01" or "" that Number would accept.
  if (parts.some((part) => part.length === 0 || (part.length > 1 && part.startsWith('0'))))
    return false;
  return octets[0] === 127;
}

/** Missing Origin is normal for non-browser clients; explicit allow-lists always apply. */
export function websocketOriginAllowed(
  origin: string | undefined,
  localMode: boolean,
  extraAllowed: ReadonlySet<string>,
): boolean {
  if (origin === undefined || origin === INTERNAL_WS_ORIGIN || extraAllowed.has(origin))
    return true;
  if (!localMode) return extraAllowed.size === 0;
  try {
    const host = new URL(origin).hostname;
    // URL wraps IPv6 hosts in brackets; socket addresses are bare literals.
    return hostnameIsLoopback(
      host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host,
    );
  } catch {
    return false;
  }
}
