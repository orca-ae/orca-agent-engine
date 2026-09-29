// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { FastifyRequest } from 'fastify';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const verify = vi.hoisted(() => vi.fn());

vi.mock('jose', () => ({
  createRemoteJWKSet: vi.fn(() => ({ mocked: true })),
  jwtVerify: verify,
}));

/**
 * Wedges the read of one path the way a hung mount does: `open` for that path
 * does not return until the test releases it. Every other path is passed
 * straight through, so the rest of the suite keeps reading real files.
 *
 * A FIFO with no writer reproduces this for real, but it blocks a libuv
 * threadpool thread for the life of the worker; injecting the stall here keeps
 * the suite fast and leaves nothing behind.
 */
const stall = vi.hoisted(() => {
  let wedged = '';
  let release = () => {};
  let gate = Promise.resolve();
  return {
    arm(path: string) {
      wedged = path;
      gate = new Promise<void>((resolve) => {
        release = resolve;
      });
    },
    /** Lets a wedged open through, as a mount coming back would. */
    clear() {
      wedged = '';
      release();
    },
    async waitIfWedged(path: string) {
      if (path === wedged) await gate;
    },
  };
});

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    open: async (path: string, flags?: string) => {
      await stall.waitIfWedged(path);
      return actual.open(path, flags);
    },
  };
});

import { buildAdminOidcAuth, buildOidcAuth, buildPlatformOidcAuth } from '../../src/auth/oidc.js';

/**
 * Captured before any test installs fake timers, so a real deadline is still
 * available while the fake clock is driving the code under test.
 */
const realSetTimeout = globalThis.setTimeout;

const BEARER = { headers: { authorization: 'Bearer signed-token' } } as FastifyRequest;

const REVOKED = 'jti-revoked-0001';

const workspacePayload = (jti?: string) => ({
  ...(jti === undefined ? {} : { jti }),
  workspace_id: 'ws_denylist',
  sub: 'user_denylist',
  scope: 'memory:write',
});
const adminPayload = (jti?: string) => ({
  ...(jti === undefined ? {} : { jti }),
  organization_id: 'org-denylist',
  sub: 'admin_denylist',
  scope: 'org:admin',
});
const platformPayload = (jti?: string) => ({
  ...(jti === undefined ? {} : { jti }),
  sub: 'platform_denylist',
  scope: 'platform:admin',
});

/** The audience of the organization the resolving workspace plane looks up. */
const ORG_AUDIENCE = 'https://denylist.example/orca';

/** An organization-scoped token: no workspace claim, only its audience. */
const resolvedPayload = (jti: string) => ({
  jti,
  aud: ORG_AUDIENCE,
  sub: 'user_denylist',
  scope: 'memory:write',
});

let tempDir: string;
let fileSeq = 0;
let warn: ReturnType<typeof vi.spyOn>;

/**
 * Writes a denylist document and returns its path.
 *
 * Every call gets a fresh path: `oidc.ts` caches the parsed list per file path
 * in module state for the life of the process, so reusing a path would leak one
 * test's list into the next.
 */
async function writeDenylist(document: unknown): Promise<string> {
  const path = join(tempDir, `denylist-${++fileSeq}.json`);
  await writeFile(path, typeof document === 'string' ? document : JSON.stringify(document), 'utf8');
  return path;
}

function unwrittenPath(): string {
  return join(tempDir, `missing-${++fileSeq}.json`);
}

/**
 * Pins `Date.now` so the reload interval can be crossed without a real wait.
 * Returns an advance function; must be installed before the first load so the
 * recorded "last checked" stamp comes from the pinned clock too.
 */
function pinClock(): (advanceMs: number) => void {
  let now = Date.now();
  vi.spyOn(Date, 'now').mockImplementation(() => now);
  return (advanceMs: number) => {
    now += advanceMs;
  };
}

/** Mirrors `DENIED_JTI_READ_DEADLINE_MS` in `oidc.ts`. */
const DEADLINE_MS = 1_000;

/** Mirrors `DENIED_JTI_MAX_BYTES` in `oidc.ts`. */
const MAX_BYTES = 1024 * 1024;

/**
 * Fake timers are the only clock driving the deadline, so a request that fails
 * to observe it would hang the run rather than fail it. Racing a real timer
 * turns that into an ordinary assertion failure.
 */
function orStuck<T>(request: Promise<T>): Promise<T | 'still waiting'> {
  return Promise.race([
    request,
    new Promise<'still waiting'>((resolve) => {
      realSetTimeout(() => resolve('still waiting'), 250);
    }),
  ]);
}

/**
 * Yields to the real event loop until `check` holds. Used where the fake clock
 * is driving the deadline but the read settling behind it is real fs I/O.
 */
async function until(check: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 200 && !check(); attempt += 1) {
    await new Promise((resolve) => {
      realSetTimeout(resolve, 1);
    });
  }
  expect(check()).toBe(true);
}

function warnedAbout(fragment: string): boolean {
  return warn.mock.calls.some((call) => String(call[0]).includes(fragment));
}

/**
 * Lets a wedged read through and waits for it to land. Every stall test ends
 * with this: a read still in flight when the test ends would log into the next
 * test's console spy.
 */
async function releaseStall(): Promise<void> {
  stall.clear();
  await until(() => warnedAbout('finished the stalled read'));
}

describe('OIDC denied-JTI list', () => {
  beforeEach(async () => {
    verify.mockReset();
    warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    tempDir = await mkdtemp(join(tmpdir(), 'oidc-denied-jti-'));
  });

  afterEach(async () => {
    // Order matters: a wedged open has to be released before the directory it
    // is holding open is removed.
    stall.clear();
    vi.useRealTimers();
    vi.restoreAllMocks();
    await rm(tempDir, { recursive: true, force: true });
  });

  it('rejects a revoked token on the workspace plane', async () => {
    verify.mockResolvedValue({ payload: workspacePayload(REVOKED) });
    const deniedJtiFile = await writeDenylist([{ key: REVOKED, exp: 1_764_000_000 }]);

    const authenticate = buildOidcAuth({
      allowedIssuers: ['https://issuer.example'],
      audience: 'orca-managed-agents',
      deniedJtiFile,
    });

    await expect(authenticate(BEARER)).resolves.toBeNull();
  });

  it('rejects a revoked token on a workspace plane resolving by organization audience', async () => {
    // Resolution moves the audience check, not the revocation check: the denied
    // list is still consulted the moment the signature verifies, before any
    // organization is looked up.
    const deniedJtiFile = await writeDenylist([{ key: REVOKED, exp: 1_764_000_000 }]);
    const config = {
      allowedIssuers: ['https://issuer.example'],
      // Empty on purpose: a resolving plane refuses to build carrying a static
      // audience it would never verify.
      audience: '',
      deniedJtiFile,
      resolveWorkspaceByAudience: true,
    };
    const lookups = {
      organizationsForAudiences: async () => [{ id: 'org_denylist', audience: ORG_AUDIENCE }],
      organizationForWorkspace: async () => ({ id: 'org_denylist', audience: ORG_AUDIENCE }),
      activeWorkspaceIds: async () => ['ws_denylist'],
    };
    const authenticate = buildOidcAuth(config, lookups);

    verify.mockResolvedValue({ payload: resolvedPayload(REVOKED) });
    await expect(authenticate(BEARER)).resolves.toBeNull();

    // The control: the same token, revoked only by its jti, authenticates once
    // that jti is not on the list — so the null above is the denylist and not a
    // resolution that never worked.
    verify.mockResolvedValue({ payload: resolvedPayload('jti-still-valid') });
    await expect(authenticate(BEARER)).resolves.toMatchObject({ workspaceId: 'ws_denylist' });
  });

  it('rejects a revoked token on the admin plane', async () => {
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });
    const deniedJtiFile = await writeDenylist([{ key: REVOKED, exp: 1_764_000_000 }]);

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });

    await expect(authenticate(BEARER)).resolves.toBeNull();
  });

  it('rejects a revoked token on the platform plane', async () => {
    verify.mockResolvedValue({ payload: platformPayload(REVOKED) });
    const deniedJtiFile = await writeDenylist([{ key: REVOKED, exp: 1_764_000_000 }]);

    const authenticate = buildPlatformOidcAuth({
      allowedIssuers: ['https://platform-issuer.example'],
      audience: 'orca-managed-agents-platform',
      deniedJtiFile,
    });

    await expect(authenticate(BEARER)).resolves.toBeNull();
  });

  it('admits a token whose jti is not on the list', async () => {
    verify.mockResolvedValue({ payload: adminPayload('jti-still-valid') });
    const deniedJtiFile = await writeDenylist([{ key: REVOKED, exp: 1_764_000_000 }]);

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });

    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
  });

  it('admits a token that carries no jti claim, since the list cannot name it', async () => {
    verify.mockResolvedValue({ payload: adminPayload() });
    const deniedJtiFile = await writeDenylist([{ key: REVOKED, exp: 1_764_000_000 }]);

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });

    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
  });

  it('is disabled when no file is configured', async () => {
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
    });

    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
    expect(warn).not.toHaveBeenCalled();
  });

  it('accepts a plain JSON array of token ids', async () => {
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });
    const deniedJtiFile = await writeDenylist(['jti-other', REVOKED]);

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });

    await expect(authenticate(BEARER)).resolves.toBeNull();
  });

  it('reads a null document as an empty list', async () => {
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });
    const deniedJtiFile = await writeDenylist('null');

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });

    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
    expect(warn).not.toHaveBeenCalled();
  });

  it('keeps usable entries when the list contains unrecognised records', async () => {
    const deniedJtiFile = await writeDenylist([
      REVOKED,
      42,
      null,
      ['nested'],
      { key: 'jti-record-form' },
      { unexpected: 'shape' },
    ]);
    const config = {
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    };
    const authenticate = buildAdminOidcAuth(config);

    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });
    await expect(authenticate(BEARER)).resolves.toBeNull();

    verify.mockResolvedValue({ payload: adminPayload('jti-record-form') });
    await expect(authenticate(BEARER)).resolves.toBeNull();

    verify.mockResolvedValue({ payload: adminPayload('jti-untouched') });
    await expect(authenticate(BEARER)).resolves.not.toBeNull();
  });

  it('picks up entries added to the file after the reload interval', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    await expect(authenticate(BEARER)).resolves.not.toBeNull();

    await writeFile(deniedJtiFile, JSON.stringify([{ key: REVOKED, exp: 1 }]), 'utf8');

    // Still served from cache until the reload interval elapses.
    await expect(authenticate(BEARER)).resolves.not.toBeNull();

    advance(60_000);
    await expect(authenticate(BEARER)).resolves.toBeNull();
  });

  it('reloads rather than freezing the list when the wall clock steps backward', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    await expect(authenticate(BEARER)).resolves.not.toBeNull();

    await writeFile(deniedJtiFile, JSON.stringify([REVOKED]), 'utf8');

    // NTP corrects a fast clock backwards, so the "last checked" stamp is now
    // in the future. A raw subtraction reads that as a fresh cache and keeps
    // serving the stale list until wall time catches back up.
    advance(-60_000);
    await expect(authenticate(BEARER)).resolves.toBeNull();
  });

  it('keeps serving the last good list when the file becomes unreadable', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([REVOKED]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    await expect(authenticate(BEARER)).resolves.toBeNull();

    await rm(deniedJtiFile);
    advance(60_000);

    await expect(authenticate(BEARER)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('last good list of 1 entries'),
      expect.anything(),
    );
  });

  it('keeps the last-good list when a non-empty replacement has no recognized entries', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([REVOKED]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    await expect(authenticate(BEARER)).resolves.toBeNull();

    // Wrong record shape ("jti" instead of "key"): a non-empty document with
    // zero recognized entries is a wrong or corrupted file, and must keep the
    // last-good list instead of silently re-admitting the revoked token.
    await writeFile(deniedJtiFile, JSON.stringify([{ jti: REVOKED }]), 'utf8');
    advance(60_000);
    await expect(authenticate(BEARER)).resolves.toBeNull();
    expect(warn).toHaveBeenCalled();

    // An explicitly empty array remains a valid "revoke nothing" decision.
    await writeFile(deniedJtiFile, JSON.stringify([]), 'utf8');
    advance(60_000);
    await expect(authenticate(BEARER)).resolves.not.toBeNull();
  });

  it('keeps denying a token whose entry a partially unrecognized reload dropped, and warns', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([REVOKED]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    await expect(authenticate(BEARER)).resolves.toBeNull();

    // Partial drift: "other" is recognized, so the document is applied, but the
    // wrongly shaped record for REVOKED is unreadable — and an unreadable entry
    // is never a removal. This pins the chosen semantics: a dirty document is
    // unioned onto the last-good list, so the revoked token STAYS denied until
    // the file parses cleanly, with the warning naming the deferral.
    await writeFile(deniedJtiFile, JSON.stringify([{ jti: REVOKED }, 'other']), 'utf8');
    advance(60_000);

    await expect(authenticate(BEARER)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('dropped 1 unrecognized entries'));
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('Removals in this document are not applied until it parses cleanly'),
    );
  });

  it('enforces an addition made by a partially unrecognized document', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([REVOKED]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload('jti-newly-revoked') });

    await expect(authenticate(BEARER)).resolves.not.toBeNull();

    // The document is dirty, but the new revocation in it is readable:
    // additions never wait for the producer to correct the file.
    await writeFile(
      deniedJtiFile,
      JSON.stringify([REVOKED, 'jti-newly-revoked', { jti: 'jti-drifted' }]),
      'utf8',
    );
    advance(60_000);

    await expect(authenticate(BEARER)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('dropped 1 unrecognized entries'));
  });

  it('applies a removal held over a dirty window once the document parses cleanly', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([REVOKED]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    await expect(authenticate(BEARER)).resolves.toBeNull();

    // Removed from the file, but while the file is dirty: the removal is held.
    await writeFile(
      deniedJtiFile,
      JSON.stringify(['jti-someone-else', { jti: 'jti-drifted' }]),
      'utf8',
    );
    advance(60_000);
    await expect(authenticate(BEARER)).resolves.toBeNull();

    // Producer fixes the record shape. The clean document replaces the enforced
    // set wholesale, so the held removal lands — the union does not freeze it.
    await writeFile(deniedJtiFile, JSON.stringify(['jti-someone-else', 'jti-drifted']), 'utf8');
    advance(60_000);

    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
  });

  it('honours a removal made by a fully recognized replacement document', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([REVOKED]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    await expect(authenticate(BEARER)).resolves.toBeNull();

    // Replace, not merge: dropping an entry from the file un-denies its token
    // on the next reload, and a clean document says so without a warning.
    await writeFile(deniedJtiFile, JSON.stringify(['jti-someone-else']), 'utf8');
    advance(60_000);

    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
    expect(warn).not.toHaveBeenCalled();
  });

  it('denies nothing but warns loudly when the file has never loaded', async () => {
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile: unwrittenPath(),
    });

    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('NO token is being denied'),
      expect.anything(),
    );
  });

  it('warns and denies nothing when the document is not an array', async () => {
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });
    const deniedJtiFile = await writeDenylist({ revoked_tokens: [{ key: REVOKED }] });

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });

    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('NO token is being denied'),
      expect.any(Error),
    );
  });

  it('is consulted only after the signature verifies', async () => {
    // The list is never read for a token that fails verification: a missing
    // file would otherwise log on every rejected request. This pins the
    // ordering required by the feature — deny after verify, never before.
    verify.mockRejectedValue(new Error('bad signature'));

    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile: unwrittenPath(),
    });

    await expect(authenticate(BEARER)).resolves.toBeNull();
    expect(warn).not.toHaveBeenCalled();
  });

  it('shares one loaded list across planes configured with the same file', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([REVOKED]);
    const shared = { deniedJtiFile };

    const workspaceAuth = buildOidcAuth({
      allowedIssuers: ['https://issuer.example'],
      audience: 'orca-managed-agents',
      ...shared,
    });
    const platformAuth = buildPlatformOidcAuth({
      allowedIssuers: ['https://platform-issuer.example'],
      audience: 'orca-managed-agents-platform',
      ...shared,
    });

    verify.mockResolvedValue({ payload: workspacePayload(REVOKED) });
    await expect(workspaceAuth(BEARER)).resolves.toBeNull();

    // Removing the file must not change the answer: the platform plane reads
    // the same cached list rather than loading its own copy.
    await rm(deniedJtiFile);
    verify.mockResolvedValue({ payload: platformPayload(REVOKED) });
    await expect(platformAuth(BEARER)).resolves.toBeNull();
    expect(warn).not.toHaveBeenCalled();

    advance(60_000);
    await expect(platformAuth(BEARER)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it('keeps the last-good list when the file grows past the read cap', async () => {
    const advance = pinClock();
    const deniedJtiFile = await writeDenylist([REVOKED]);
    const authenticate = buildAdminOidcAuth({
      allowedIssuers: ['https://admin-issuer.example'],
      audience: 'orca-managed-agents-admin',
      deniedJtiFile,
    });
    verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

    await expect(authenticate(BEARER)).resolves.toBeNull();

    // Well formed, and one byte past the cap, so the reload can only be
    // rejected for its size. The padding is ASCII, so characters are bytes;
    // `["…"]` is the four bytes of punctuation around the entry.
    const oversized = JSON.stringify(['j'.repeat(MAX_BYTES + 1 - 4)]);
    expect(oversized.length).toBe(MAX_BYTES + 1);
    await writeFile(deniedJtiFile, oversized, 'utf8');
    advance(60_000);

    // An oversized document is a failed reload, not an empty list: the token
    // stays denied and the existing alarm is raised.
    await expect(authenticate(BEARER)).resolves.toBeNull();
    // Pinned to the cap's own message: a document this size must be refused for
    // its size, not read and then rejected as unparseable.
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('last good list of 1 entries'),
      expect.objectContaining({
        message: expect.stringContaining(`${MAX_BYTES} byte limit`),
      }),
    );

    // And the cap poisons nothing: the next document that fits loads normally.
    await writeFile(deniedJtiFile, JSON.stringify(['jti-someone-else']), 'utf8');
    advance(60_000);
    await expect(authenticate(BEARER)).resolves.toMatchObject({ organizationId: 'org-denylist' });
  });

  it('serves the list in hand when a read stalls past the deadline', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      const advance = pinClock();
      const deniedJtiFile = await writeDenylist([REVOKED]);
      const authenticate = buildAdminOidcAuth({
        allowedIssuers: ['https://admin-issuer.example'],
        audience: 'orca-managed-agents-admin',
        deniedJtiFile,
      });
      verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

      await expect(authenticate(BEARER)).resolves.toBeNull();

      // The mount wedges. The reload is issued and does not come back, so the
      // request must stop waiting for it rather than hang on it.
      stall.arm(deniedJtiFile);
      advance(60_000);

      const request = orStuck(authenticate(BEARER));
      await vi.advanceTimersByTimeAsync(DEADLINE_MS);

      // Answered from the last good list: the revoked token is still denied.
      await expect(request).resolves.toBeNull();
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining(`timed out after ${DEADLINE_MS}ms`),
      );

      await releaseStall();
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not make later requests wait again once a stalled read has latched', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      const advance = pinClock();
      const deniedJtiFile = await writeDenylist([REVOKED]);
      const authenticate = buildAdminOidcAuth({
        allowedIssuers: ['https://admin-issuer.example'],
        audience: 'orca-managed-agents-admin',
        deniedJtiFile,
      });
      verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

      await expect(authenticate(BEARER)).resolves.toBeNull();

      stall.arm(deniedJtiFile);
      advance(60_000);
      const first = orStuck(authenticate(BEARER));
      await vi.advanceTimersByTimeAsync(DEADLINE_MS);
      await expect(first).resolves.toBeNull();

      // The latch is set, so this one is answered without a deadline of its
      // own: the fake clock is not moved, and a request that armed another
      // timer would still be waiting when the real one below fires.
      await expect(orStuck(authenticate(BEARER))).resolves.toBeNull();
      expect(vi.getTimerCount()).toBe(0);

      // One stall, one alarm — not one per request caught by it.
      expect(warn.mock.calls.filter((call) => String(call[0]).includes('timed out'))).toHaveLength(
        1,
      );

      await releaseStall();
    } finally {
      vi.useRealTimers();
    }
  });

  it('resumes normal waiting once the stalled read finally settles', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      const advance = pinClock();
      const deniedJtiFile = await writeDenylist([REVOKED]);
      const authenticate = buildAdminOidcAuth({
        allowedIssuers: ['https://admin-issuer.example'],
        audience: 'orca-managed-agents-admin',
        deniedJtiFile,
      });
      verify.mockResolvedValue({ payload: adminPayload(REVOKED) });

      await expect(authenticate(BEARER)).resolves.toBeNull();

      stall.arm(deniedJtiFile);
      advance(60_000);
      const stalledRequest = orStuck(authenticate(BEARER));
      await vi.advanceTimersByTimeAsync(DEADLINE_MS);
      await expect(stalledRequest).resolves.toBeNull();

      // The mount comes back: the read that everyone stopped waiting for
      // completes, the latch clears, and that is said out loud so a stall does
      // not read as permanent.
      await writeFile(deniedJtiFile, JSON.stringify(['jti-someone-else']), 'utf8');
      await releaseStall();

      // Back to normal: the next request waits for a fresh read and sees the
      // removal the settled one brought in.
      advance(60_000);
      await expect(orStuck(authenticate(BEARER))).resolves.toMatchObject({
        organizationId: 'org-denylist',
      });
    } finally {
      vi.useRealTimers();
    }
  });
});
