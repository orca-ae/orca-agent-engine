// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { lookup } from 'node:dns/promises';
import { BlockList, isIP } from 'node:net';

// Destinations the registry must never dial when probing user-controlled
// URLs (mcp_server_url / token_endpoint): loopback, link-local (incl. cloud
// metadata at 169.254.169.254), RFC1918 + CGNAT private ranges, ULA, and
// unspecified/benchmarking ranges. Reaching any of these would turn the
// validate diagnostic into an SSRF-to-exfiltration primitive.
const blockedRanges = new BlockList();
blockedRanges.addSubnet('0.0.0.0', 8, 'ipv4');
blockedRanges.addSubnet('10.0.0.0', 8, 'ipv4');
blockedRanges.addSubnet('100.64.0.0', 10, 'ipv4');
blockedRanges.addSubnet('127.0.0.0', 8, 'ipv4');
blockedRanges.addSubnet('169.254.0.0', 16, 'ipv4');
blockedRanges.addSubnet('172.16.0.0', 12, 'ipv4');
blockedRanges.addSubnet('192.168.0.0', 16, 'ipv4');
blockedRanges.addSubnet('198.18.0.0', 15, 'ipv4');
blockedRanges.addSubnet('::', 128, 'ipv6');
blockedRanges.addSubnet('::1', 128, 'ipv6');
blockedRanges.addSubnet('fc00::', 7, 'ipv6');
blockedRanges.addSubnet('fe80::', 10, 'ipv6');
// NAT64 (RFC 6052 well-known prefix + RFC 8215 local-use prefix) and 6to4
// (RFC 3056, deprecated by RFC 7526 but still routable on networks that keep
// a relay up) re-encode an arbitrary IPv4 destination inside an IPv6
// literal, the same trick as the two forms handled below. Unlike
// ::ffff:0:0/96, these don't collide with node:net's BlockList ipv4/ipv6
// mixing bug (verified on Node 22.19.0), so they're safe to add directly
// here alongside the ipv4 subnets above.
blockedRanges.addSubnet('64:ff9b::', 96, 'ipv6'); // RFC 6052 NAT64 well-known prefix
blockedRanges.addSubnet('64:ff9b:1::', 48, 'ipv6'); // RFC 8215 NAT64 local-use prefix
blockedRanges.addSubnet('2002::', 16, 'ipv6'); // 6to4 — bytes 2-5 embed the IPv4 destination
// Deprecated "IPv4-compatible IPv6" form (::x.x.x.x, RFC 4291 §2.5.5.1) lets
// an attacker re-encode a blocked IPv4 literal (e.g. ::127.0.0.1, ::10.0.0.1)
// as IPv6 and fall outside every ipv4 range above. No legitimate client
// emits it, so block the full ::/96 (this also re-covers ::/128 and ::1/128,
// already listed explicitly above). Safe to add to the shared list — see the
// ::ffff:0:0/96 comment below for why the *other* IPv4-in-IPv6 form isn't.
blockedRanges.addSubnet('::', 96, 'ipv6');

// IPv4-mapped IPv6 (::ffff:0:0/96) has the same re-encoding problem as the
// deprecated form above, in either the dotted form (::ffff:127.0.0.1) or the
// hex form (::ffff:7f00:1) — the two are textually different but
// address-equal, and BlockList.check() parses both to the same 128-bit value
// before matching, so one subnet entry covers every textual spelling. This
// lives in its *own* BlockList rather than folded into `blockedRanges`:
// node:net's BlockList has a bug (confirmed on Node 22.19.0) where adding an
// ::ffff:0:0/96 subnet to a list that also holds IPv4 subnets makes every
// subsequent `check(ipv4Address, 'ipv4')` against that list return true
// unconditionally, regardless of address — mixing them would silently block
// all outbound IPv4 traffic. We block the whole /96 rather than unmapping to
// the embedded IPv4 and re-checking it against the ipv4 list: a legitimate
// mcp_server_url/token_endpoint is never authored as an IPv4-mapped IPv6
// literal, so there is no real destination this disallows, and it avoids
// hand-rolling IPv6 text parsing (hex groups, "::" compression, mixed
// dotted/hex forms) in a security-sensitive path.
const mappedIPv4Ranges = new BlockList();
mappedIPv4Ranges.addSubnet('::ffff:0:0', 96, 'ipv6');

export function isBlockedAddress(address: string): boolean {
  const family = isIP(address);
  if (family === 4) return blockedRanges.check(address, 'ipv4');
  if (family === 6) {
    return mappedIPv4Ranges.check(address, 'ipv6') || blockedRanges.check(address, 'ipv6');
  }
  // Not a parseable IP at all: fail closed.
  return true;
}

export async function assertPublicHost(hostname: string): Promise<void> {
  // WHATWG URL keeps brackets around IPv6 literals in `hostname`.
  const host = hostname.replace(/^\[|\]$/g, '');
  if (isIP(host) !== 0) {
    if (isBlockedAddress(host)) {
      throw new Error(`egress blocked: ${host} is not a public address`);
    }
    return;
  }
  const addresses = await lookup(host, { all: true });
  if (addresses.length === 0) throw new Error(`egress blocked: ${host} did not resolve`);
  if (addresses.some(({ address }) => isBlockedAddress(address))) {
    throw new Error(`egress blocked: ${host} resolves to a non-public address`);
  }
}

/**
 * Wraps a fetch implementation with a pre-flight guard that rejects
 * non-public destinations, and disables redirect following so a public host
 * cannot 302 the request to an internal one.
 *
 * Limitation: the check uses a DNS lookup separate from the one the socket
 * connect performs, so an attacker running a sub-TTL DNS rebind retains a
 * TOCTOU window; closing it fully needs connect-time address pinning via a
 * custom dispatcher. Acceptable for this low-frequency diagnostic endpoint.
 */
export function createGuardedFetch(fetchImpl: typeof fetch): typeof fetch {
  const guarded = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    await assertPublicHost(url.hostname);
    return fetchImpl(input as never, { ...init, redirect: 'manual' });
  };
  return guarded as typeof fetch;
}
