// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { createGuardedFetch, isBlockedAddress } from '../../src/api/egress-guard.js';

describe('egress guard for mcp_oauth_validate outbound calls', () => {
  it('flags non-public addresses', () => {
    const blocked = [
      '0.0.0.0',
      '10.1.2.3',
      '100.64.0.1',
      '127.0.0.1',
      '169.254.169.254',
      '172.16.0.1',
      '192.168.1.1',
      '198.18.0.1',
      '::',
      '::1',
      'fc00::1',
      'fd12:3456::1',
      'fe80::1',
      // Dotted-form IPv4-mapped IPv6.
      '::ffff:127.0.0.1',
      '::ffff:10.0.0.1',
      // Hex-form IPv4-mapped IPv6 — same addresses as above, textually
      // different, previously bypassed the guard (see egress-guard.ts).
      '::ffff:7f00:1',
      '::ffff:a00:1',
      // Deprecated IPv4-compatible IPv6 form (RFC 4291 ::x.x.x.x).
      '::127.0.0.1',
      '::10.0.0.1',
      // NAT64 (RFC 6052 well-known + RFC 8215 local-use prefixes) and 6to4
      // (RFC 3056) re-encode an embedded IPv4 destination — reachable on
      // IPv6-only hosts behind a NAT64/DNS64 gateway or a 6to4 relay.
      '64:ff9b::7f00:1', // NAT64, embeds 127.0.0.1
      '64:ff9b::a00:1', // NAT64, embeds 10.0.0.1
      '64:ff9b:1::7f00:1', // NAT64 local-use, embeds 127.0.0.1
      '2002:7f00:1::', // 6to4, embeds 127.0.0.1
      '2002:a9fe:a9fe::', // 6to4, embeds 169.254.169.254
      'not-an-ip',
    ];
    for (const ip of blocked) expect(isBlockedAddress(ip), ip).toBe(true);

    const allowed = ['8.8.8.8', '93.184.216.34', '172.32.0.1', '2606:4700::6810:84e5'];
    for (const ip of allowed) expect(isBlockedAddress(ip), ip).toBe(false);

    // We block the whole ::ffff:0:0/96 range rather than unmapping and
    // re-checking the embedded IPv4 (see the comment on that subnet in
    // egress-guard.ts), so a mapped *public* address is blocked too even
    // though the embedded IPv4 (8.8.8.8) is not — no legitimate
    // mcp_server_url/token_endpoint is authored as a mapped-IPv6 literal.
    expect(isBlockedAddress('::ffff:8.8.8.8')).toBe(true);
    expect(isBlockedAddress('::ffff:808:808')).toBe(true);

    // Same blanket-block rationale applies to NAT64/6to4: a mapped *public*
    // address (8.8.8.8) is blocked too, since no legitimate
    // mcp_server_url/token_endpoint is authored as one of these literals.
    expect(isBlockedAddress('64:ff9b::808:808')).toBe(true);
    expect(isBlockedAddress('2002:808:808::')).toBe(true);
  });

  it('rejects fetches to blocked IP literals without dialing', async () => {
    const inner = vi.fn();
    const guarded = createGuardedFetch(inner as unknown as typeof fetch);
    await expect(guarded('http://169.254.169.254/latest/meta-data/')).rejects.toThrow(
      /egress blocked/,
    );
    await expect(guarded('http://127.0.0.1:8080/oauth/token')).rejects.toThrow(/egress blocked/);
    await expect(guarded('http://[::1]:9200/')).rejects.toThrow(/egress blocked/);
    // Bracketed hex-form IPv4-mapped IPv6 URL host (== 127.0.0.1).
    await expect(guarded('http://[::ffff:7f00:1]:9200/')).rejects.toThrow(/egress blocked/);
    // Bracketed NAT64 URL host (== 169.254.169.254, cloud metadata).
    await expect(guarded('http://[64:ff9b::a9fe:a9fe]:9200/')).rejects.toThrow(/egress blocked/);
    expect(inner).not.toHaveBeenCalled();
  });

  it('rejects hostnames that resolve to loopback', async () => {
    const inner = vi.fn();
    const guarded = createGuardedFetch(inner as unknown as typeof fetch);
    await expect(guarded('http://localhost:9200/')).rejects.toThrow(/egress blocked/);
    expect(inner).not.toHaveBeenCalled();
  });

  it('passes public IP literals through and disables redirect following', async () => {
    const inner = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) => new Response('ok', { status: 200 }),
    );
    const guarded = createGuardedFetch(inner as unknown as typeof fetch);
    const res = await guarded('http://93.184.216.34/', { method: 'POST' });
    expect(res.status).toBe(200);
    expect(inner).toHaveBeenCalledTimes(1);
    const init = inner.mock.calls[0]![1]!;
    expect(init.method).toBe('POST');
    expect(init.redirect).toBe('manual');
  });
});
