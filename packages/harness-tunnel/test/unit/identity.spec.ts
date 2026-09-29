// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for runner + host identity helpers.
//
// Two surfaces:
//   1. Runner identity — the token-bound runner id derivation (server and runner
//      must derive the same id from the same binding token), the stable on-disk
//      runner id, and the auth-secret stripping applied at every runner→child
//      spawn boundary.
//   2. Host identity — load-or-create against an on-disk config file, with an
//      env-var override path for server-managed (disposable) sandbox hosts.
//
// The `runner_token_` / `runner_` / `host_` id prefixes and the 32-hex digest
// slice are the load-bearing cross-component assertions: a peer (the registry
// server) derives the same id from the same secret, so the derivation must stay
// stable byte-for-byte.

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createHash, createHmac } from 'node:crypto';
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir, hostname, homedir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  PEER_ID_ENV_VAR,
  RUNNER_PARENT_PID_ENV_VAR,
  RUNNER_ADOPT_SIGNAL,
  RUNNER_WORKSPACE_ENV_VAR,
  RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
  RUNNER_TUNNEL_TOKEN_HEADER,
  RUNNER_ISOLATE_SESSION_ENV_VAR,
  RUNNER_AUTH_SECRET_ENV_VARS,
  INTERNAL_WS_ORIGIN,
  HOST_TOKEN_ENV_VAR,
  HOST_ID_ENV_VAR,
  HOST_NAME_ENV_VAR,
  HOST_TUNNEL_TOKEN_HEADER,
  stripRunnerAuthSecrets,
  getStableRunnerId,
  tokenBoundRunnerId,
  loadOrCreateRunnerId,
  loadOrCreateHostIdentity,
  type HostIdentity,
} from '../../src/identity.js';

// ── Token-bound runner id ────────────────────────────────

describe('token-bound runner id', () => {
  it('is stable across calls and scoped to the secret token', () => {
    // Deterministic per token, trim-normalized, and distinct across tokens — the
    // server and runner derive the same id from the same binding token, so one
    // authenticated caller can never claim another caller's runner id.
    const first = tokenBoundRunnerId('tok-one');
    const second = tokenBoundRunnerId('tok-two');

    expect(first).toBe(tokenBoundRunnerId(' tok-one '));
    expect(first.startsWith('runner_token_')).toBe(true);
    expect(first.length).toBe('runner_token_'.length + 32);
    expect(first).not.toBe(second);
    // The id never leaks the secret token verbatim.
    expect(first.includes('tok-one')).toBe(false);
  });

  it('produces a lowercase-hex digest slice', () => {
    // The 32-char tail is a hex digest slice — pin it so a future refactor can't
    // swap in a non-hex encoding that a peer's matcher would reject.
    const id = tokenBoundRunnerId('some-token');
    const tail = id.slice('runner_token_'.length);
    expect(tail).toMatch(/^[0-9a-f]{32}$/);
  });

  it('is a plain SHA-256 over the colon-joined "orca-runner:" + token', () => {
    // CROSS-COMPONENT CONTRACT — the load-bearing assertion. The registry server
    // re-derives this id from the same binding token; if the runner and server
    // disagree on the construction the token is mis-bound or rejected. So pin the
    // *exact* digest, not just its shape:
    //   1. a frozen known-answer value (catches any change to algorithm, prefix,
    //      joiner, or slice width — a value a peer can be checked against), and
    //   2. an independent recompute proving the construction is a PLAIN SHA-256
    //      of `"orca-runner:" + token` (domain prefix concatenated *inside* the
    //      hashed message) and is NOT an HMAC keyed on the domain string. These
    //      two constructions produce different digests; a regression to either
    //      HMAC or a different prefix/joiner trips this test.
    const token = 'binding-token-fixture';

    // (1) Known-answer: frozen output for a fixed token.
    expect(tokenBoundRunnerId(token)).toBe('runner_token_7490700398661c14885e959904eb393b');

    // (2) Construction proof: equals plain SHA-256 of the salted string, sliced.
    const plain = createHash('sha256').update(`orca-runner:${token}`).digest('hex');
    expect(tokenBoundRunnerId(token)).toBe(`runner_token_${plain.slice(0, 32)}`);

    // ...and is explicitly NOT the HMAC-keyed-on-'orca-runner' construction, so a
    // future "rename the key" edit that reintroduces HMAC cannot pass.
    const hmac = createHmac('sha256', 'orca-runner').update(token).digest('hex');
    expect(tokenBoundRunnerId(token)).not.toBe(`runner_token_${hmac.slice(0, 32)}`);
  });

  it('rejects an empty token', () => {
    // Missing token values fail loud instead of inventing a runner id.
    expect(() => tokenBoundRunnerId('   ')).toThrow(/tunnel binding token must not be empty/);
    expect(() => tokenBoundRunnerId('')).toThrow(/tunnel binding token must not be empty/);
  });
});

// ── strip_runner_auth_secrets ────────────────────────────

describe('strip runner auth secrets', () => {
  it('strips every name in the registry and pins the binding token is registered', () => {
    // Guards the contract between the registry set and the helper: a name added
    // to RUNNER_AUTH_SECRET_ENV_VARS but not stripped (or vice versa) fails here.
    // Also pins that the binding token — the known control-plane secret — is in
    // the set, so a future edit that empties it is caught.
    expect(RUNNER_AUTH_SECRET_ENV_VARS.has(RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR)).toBe(true);

    const seeded: Record<string, string> = {};
    for (const name of RUNNER_AUTH_SECRET_ENV_VARS) {
      seeded[name] = `secret-${name}`;
    }
    seeded['KEEP_ME'] = 'keep';

    const result = stripRunnerAuthSecrets(seeded);

    expect(new Set(Object.keys(result))).toEqual(new Set(['KEEP_ME']));
    expect(result['KEEP_ME']).toBe('keep');
  });

  it('removes the binding token and keeps every other var', () => {
    // Asserts the exact surviving mapping (not just the token's absence) so a
    // regression that also dropped legitimate vars (PATH, creds) fails too.
    const source: Record<string, string> = {
      PATH: '/usr/bin',
      ANTHROPIC_API_KEY: 'sk-keep-me',
      [RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR]: 'bug-binding-token-secret',
    };

    const result = stripRunnerAuthSecrets(source);

    expect(result).toEqual({ PATH: '/usr/bin', ANTHROPIC_API_KEY: 'sk-keep-me' });
    expect(Object.values(result)).not.toContain('bug-binding-token-secret');
  });

  it('does not mutate the input mapping', () => {
    // The runner process itself must retain the token in its own environment (it
    // reuses it for request auth); only the child's copy is filtered. A mutating
    // implementation would strip the token from the live runner environment.
    const source: Record<string, string> = {
      [RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR]: 'tok',
      HOME: '/home/x',
    };

    const result = stripRunnerAuthSecrets(source);

    expect(source).toEqual({ [RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR]: 'tok', HOME: '/home/x' });
    expect(result).toEqual({ HOME: '/home/x' });
    expect(result).not.toBe(source);
  });
});

// ── getStableRunnerId ────────────────────────────────────

describe('get stable runner id', () => {
  const savedRunnerId = process.env[PEER_ID_ENV_VAR];
  // os.homedir() reads $HOME on POSIX and $USERPROFILE on Windows; redirect both
  // so the on-disk fallback test below resolves the default cache path under a
  // throwaway temp home instead of writing into the developer's real ~/.orca.
  const savedHome = process.env.HOME;
  const savedUserProfile = process.env.USERPROFILE;
  let home: string;

  beforeEach(() => {
    home = mkdtempSync(join(tmpdir(), 'orca-stable-runner-home-'));
    process.env.HOME = home;
    process.env.USERPROFILE = home;
  });

  afterEach(() => {
    rmSync(home, { recursive: true, force: true });
    restoreEnv(PEER_ID_ENV_VAR, savedRunnerId);
    restoreEnv('HOME', savedHome);
    restoreEnv('USERPROFILE', savedUserProfile);
  });

  it('returns the env override verbatim, trimmed', () => {
    // Parent processes set the id so child server + runner agree before either
    // touches the on-disk cache. Surrounding whitespace is trimmed.
    process.env[PEER_ID_ENV_VAR] = '  runner_envset  ';
    expect(getStableRunnerId()).toBe('runner_envset');
  });

  it('rejects an empty env override', () => {
    // An explicitly-empty override is a launcher bug — fail loud rather than fall
    // through to the on-disk cache.
    process.env[PEER_ID_ENV_VAR] = '   ';
    expect(() => getStableRunnerId()).toThrow(new RegExp(`${PEER_ID_ENV_VAR} must not be empty`));
  });

  it('falls back to the on-disk cache under ~/.orca when no env override is set', () => {
    // No env override → getStableRunnerId() must mint/read the id from the default
    // path `~/.orca/peers/peer_id`. This is the integration branch the unit
    // tests for loadOrCreateRunnerId can't reach: a regression that mis-wired the
    // default path (wrong dir, wrong env) would mint the id in the wrong place and
    // be caught here. homedir() points at the temp `home` set in beforeEach.
    delete process.env[PEER_ID_ENV_VAR];
    const expectedPath = join(home, '.orca', 'peers', 'peer_id');

    const id = getStableRunnerId();

    expect(id.startsWith('runner_')).toBe(true);
    expect(id.slice('runner_'.length)).toMatch(/^[0-9a-f]{32}$/);
    // It was actually persisted to the default on-disk location, not just minted.
    expect(existsSync(expectedPath)).toBe(true);
    expect(readFileSync(expectedPath, 'utf-8')).toBe(id);
    // The id is the stable on-disk one — a second call reads the same file back.
    expect(getStableRunnerId()).toBe(id);
    // Sanity-check the redirect actually took: homedir resolves to our temp home.
    expect(homedir()).toBe(home);
  });
});

// ── loadOrCreateRunnerId ─────────────────────────────────

describe('load or create runner id', () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'orca-runner-id-'));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it('creates a fresh runner id on first use and persists it', () => {
    // No cache file yet → mint `runner_{uuid hex}`, write it through, and create
    // any missing parent directories.
    const path = join(dir, 'nested', 'peer_id');
    const id = loadOrCreateRunnerId(path);

    expect(id.startsWith('runner_')).toBe(true);
    const hexPart = id.slice('runner_'.length);
    expect(hexPart).toMatch(/^[0-9a-f]{32}$/);
    expect(existsSync(path)).toBe(true);
    expect(readFileSync(path, 'utf-8')).toBe(id);
  });

  it('loads an existing id and is stable across calls', () => {
    const path = join(dir, 'peer_id');
    const first = loadOrCreateRunnerId(path);
    const second = loadOrCreateRunnerId(path);
    expect(first).toBe(second);
  });

  it('trims surrounding whitespace in the cache file', () => {
    const path = join(dir, 'peer_id');
    writeFileSync(path, '  runner_cached \n');
    expect(loadOrCreateRunnerId(path)).toBe('runner_cached');
  });

  it('rejects an empty cache file', () => {
    // A present-but-empty file is corruption — fail loud rather than return ''.
    const path = join(dir, 'peer_id');
    writeFileSync(path, '   \n');
    expect(() => loadOrCreateRunnerId(path)).toThrow(/runner id file is empty/);
  });
});

// ── Constants + wire contract ────────────────────────────

describe('identity constants', () => {
  it('exposes the Orca-native env var names', () => {
    expect(PEER_ID_ENV_VAR).toBe('ORCA_PEER_ID');
    expect(RUNNER_PARENT_PID_ENV_VAR).toBe('ORCA_RUNNER_PARENT_PID');
    expect(RUNNER_WORKSPACE_ENV_VAR).toBe('ORCA_RUNNER_WORKSPACE');
    expect(RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR).toBe('ORCA_RUNNER_TUNNEL_BINDING_TOKEN');
    expect(RUNNER_ISOLATE_SESSION_ENV_VAR).toBe('ORCA_RUNNER_ISOLATE_SESSION');
    expect(HOST_TOKEN_ENV_VAR).toBe('ORCA_HOST_TOKEN');
    expect(HOST_ID_ENV_VAR).toBe('ORCA_HOST_ID');
    expect(HOST_NAME_ENV_VAR).toBe('ORCA_HOST_NAME');
  });

  it('exposes the dedicated tunnel-token headers', () => {
    // Dedicated headers (not Authorization) so the credential can't be confused
    // with a user Bearer token by intermediate proxies or the auth provider.
    expect(RUNNER_TUNNEL_TOKEN_HEADER).toBe('X-Orca-Runner-Tunnel-Token');
    expect(HOST_TUNNEL_TOKEN_HEADER).toBe('X-Orca-Host-Token');
  });

  it('uses a non-HTTP scheme for the internal WS origin sentinel', () => {
    // A browser computes Origin from the page URL and can never emit this value,
    // so the server's CSWSH origin guard can allow the project's own non-browser
    // WebSocket clients by matching this sentinel exactly.
    expect(INTERNAL_WS_ORIGIN).toBe('orca://internal');
    expect(INTERNAL_WS_ORIGIN.startsWith('http')).toBe(false);
  });

  it('registers only the binding token as a runner auth secret', () => {
    expect([...RUNNER_AUTH_SECRET_ENV_VARS]).toEqual([RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR]);
  });

  it('exposes the adopt signal as a portable signal name (or null)', () => {
    // A parent sends this signal to detach a runner from its parent-pid watch.
    // Exposed as the NodeJS.Signals *name* (what process.on / process.kill take),
    // never a numeric code, so callers stay platform-portable. On platforms that
    // define SIGUSR1 (POSIX) it is 'SIGUSR1'; where it is undefined (native
    // Windows) it is null and callers skip adopt signaling.
    if (process.platform === 'win32') {
      // Windows has no SIGUSR1 — the constant degrades to null rather than a code.
      expect(RUNNER_ADOPT_SIGNAL === null || RUNNER_ADOPT_SIGNAL === 'SIGUSR1').toBe(true);
    } else {
      expect(RUNNER_ADOPT_SIGNAL).toBe('SIGUSR1');
    }
    // Whatever it is, it is never a number (the bug we are guarding against).
    expect(typeof RUNNER_ADOPT_SIGNAL === 'string' || RUNNER_ADOPT_SIGNAL === null).toBe(true);
  });
});

// ── Import isolation (dependency-light foundational module) ──

describe('identity module import isolation', () => {
  // Guards the "importing identity does not pull in the app stack" invariant.
  // identity is imported at every runner→child spawn boundary (incl. a sandbox
  // launcher that re-execs a fresh interpreter per spawn), so it must stay
  // dependency-light: importing it must NOT drag in the tunnel transport/codec
  // siblings or any third-party package.
  //
  // The ESM-native, loader-independent check is a static import-graph assertion
  // over the module source: every module specifier identity.ts pulls in must be
  // a `node:` builtin. This fails the moment someone adds `import { ... } from
  // './transport.js'` (or any npm dep) to the foundational module — the exact
  // regression this guards.
  it('imports only node: builtins — no transport/frames or third-party deps', () => {
    const identitySrc = readFileSync(
      fileURLToPath(new URL('../../src/identity.ts', import.meta.url)),
      'utf-8',
    );

    // Collect every `import … from '…'`, `export … from '…'`, dynamic `import('…')`,
    // and `require('…')` specifier in the module source.
    const specifiers = new Set<string>();
    const fromRe = /(?:import|export)\b[^;]*?\bfrom\s*['"]([^'"]+)['"]/g;
    const dynRe = /\bimport\(\s*['"]([^'"]+)['"]\s*\)/g;
    const reqRe = /(?:^|[^.\w])require\(\s*['"]([^'"]+)['"]\s*\)/g;
    for (const re of [fromRe, dynRe, reqRe]) {
      for (const m of identitySrc.matchAll(re)) {
        specifiers.add(m[1]!);
      }
    }

    // Sanity: the module really does import something (guards a broken regex that
    // would make the assertion below vacuously pass).
    expect(specifiers.size).toBeGreaterThan(0);

    for (const spec of specifiers) {
      // Every dependency must be a Node builtin. A relative sibling like
      // './frames.js' / './transport.js' (the heavy tunnel codec + transport) or
      // a bare package specifier (a third-party dep) trips this.
      expect(spec.startsWith('node:'), `identity.ts must not import '${spec}'`).toBe(true);
    }
  });
});

// ── Host identity ────────────────────────────────────────

describe('host identity', () => {
  let dir: string;
  const savedHostId = process.env[HOST_ID_ENV_VAR];
  const savedHostName = process.env[HOST_NAME_ENV_VAR];

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'orca-host-id-'));
    delete process.env[HOST_ID_ENV_VAR];
    delete process.env[HOST_NAME_ENV_VAR];
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
    restoreEnv(HOST_ID_ENV_VAR, savedHostId);
    restoreEnv(HOST_NAME_ENV_VAR, savedHostName);
  });

  it('creates a host section when no config exists', () => {
    // Missing config → generate `host_{uuid hex}`, default the name to the
    // machine hostname, and write the config through.
    const configPath = join(dir, 'config.json');
    const identity = loadOrCreateHostIdentity(configPath);

    expect(existsSync(configPath)).toBe(true);
    expect(identity.hostId.startsWith('host_')).toBe(true);
    const hexPart = identity.hostId.slice('host_'.length);
    expect(hexPart.length).toBe(32);
    expect(hexPart).toMatch(/^[0-9a-f]{32}$/);
    // Name defaults to the machine hostname.
    expect(identity.name).toBe(hostname());
  });

  it('loads an existing host section', () => {
    const configPath = join(dir, 'config.json');
    writeFileSync(
      configPath,
      JSON.stringify({
        server: 'http://example.com',
        host: { host_id: 'host_aabbccdd', name: 'my-laptop' },
      }),
    );

    const identity = loadOrCreateHostIdentity(configPath);

    expect(identity.hostId).toBe('host_aabbccdd');
    expect(identity.name).toBe('my-laptop');
  });

  it('is stable across calls', () => {
    // The host section is read on the second call, not regenerated.
    const configPath = join(dir, 'config.json');
    const first = loadOrCreateHostIdentity(configPath);
    const second = loadOrCreateHostIdentity(configPath);

    expect(first.hostId).toBe(second.hostId);
    expect(first.name).toBe(second.name);
  });

  it('preserves existing config keys when creating the host section', () => {
    // Adding the host section must not clobber existing keys (the write merges,
    // it does not overwrite).
    const configPath = join(dir, 'config.json');
    writeFileSync(configPath, JSON.stringify({ server: 'http://example.com', profile: 'oss' }));

    const identity = loadOrCreateHostIdentity(configPath);
    const data = JSON.parse(readFileSync(configPath, 'utf-8'));

    expect(data.host.host_id).toBe(identity.hostId);
    expect(data.host.name).toBe(identity.name);
    expect(data.server).toBe('http://example.com');
    expect(data.profile).toBe('oss');
  });

  it('preserves pre-existing nested config sections when creating the host section', () => {
    // Pins the preserve-all-keys contract: the serializer fully preserves nested
    // objects. The host-section write must deep-merge, not run keys through a flat
    // top-level allowlist: a pre-existing NESTED object (e.g. an
    // `auth: { token, scopes }` block) must survive the round-trip with every
    // inner key intact. A replacer-array write emits such a section as `{}`,
    // silently dropping `auth.token` / `auth.scopes`.
    const configPath = join(dir, 'config.json');
    writeFileSync(
      configPath,
      JSON.stringify({
        server: 'http://example.com',
        auth: { token: 'secret-tok', scopes: ['read', 'write'] },
        connection: { z: 1, a: 2 },
      }),
    );

    const identity = loadOrCreateHostIdentity(configPath);
    const data = JSON.parse(readFileSync(configPath, 'utf-8'));

    // Host section was added alongside the existing data.
    expect(data.host.host_id).toBe(identity.hostId);
    expect(data.host.name).toBe(identity.name);
    // The nested `auth` block survives intact — inner keys are not dropped.
    expect(data.auth).toEqual({ token: 'secret-tok', scopes: ['read', 'write'] });
    // A second nested mapping survives too (not just the first one).
    expect(data.connection).toEqual({ z: 1, a: 2 });
    // The scalar sibling is preserved as well.
    expect(data.server).toBe('http://example.com');
  });

  it('writes the config with keys sorted at every nesting depth', () => {
    // Pins the deterministic key-sorted write: repeated writes are stable because
    // keys are sorted per-object at every depth (not just top level), so a future
    // serializer change can't reintroduce nondeterministic key order.
    const configPath = join(dir, 'config.json');
    writeFileSync(
      configPath,
      JSON.stringify({ server: 'http://example.com', auth: { token: 't', scopes: ['s'] } }),
    );

    loadOrCreateHostIdentity(configPath);
    const raw = readFileSync(configPath, 'utf-8');
    const data = JSON.parse(raw);

    // Top-level keys are sorted: auth < host < server.
    expect(Object.keys(data)).toEqual(['auth', 'host', 'server']);
    // Nested `auth` keys are sorted too: scopes < token.
    expect(Object.keys(data.auth)).toEqual(['scopes', 'token']);
    // Nested `host` keys are sorted: host_id < name.
    expect(Object.keys(data.host)).toEqual(['host_id', 'name']);
  });

  it('persists the config as canonical JSON that the same loader re-reads', () => {
    // This config is persisted as JSON (documented in the module header) to keep
    // this foundational package free of a YAML parser. That choice is only safe if
    // the on-disk contract is self-consistent: whatever loadOrCreateHostIdentity
    // *writes* must be exactly what loadOrCreateHostIdentity *reads back*. Pin
    // that end-to-end so a future second reader/writer that disagrees on the
    // format (e.g. reintroduces YAML) fails loud here.
    const configPath = join(dir, 'config.json');

    // First call mints + writes the identity. The bytes on disk must be JSON
    // (JSON.parse succeeds) — not YAML and not some other encoding.
    const created = loadOrCreateHostIdentity(configPath);
    const raw = readFileSync(configPath, 'utf-8');
    expect(() => JSON.parse(raw)).not.toThrow();
    // Trailing newline: a POSIX-friendly text file, and proof the writer owns the
    // exact byte layout.
    expect(raw.endsWith('}\n')).toBe(true);
    const parsed = JSON.parse(raw);
    expect(parsed.host).toEqual({ host_id: created.hostId, name: created.name });

    // Round-trip: the same loader reads its own JSON back to an identical identity
    // and does NOT rewrite the file (a second read that re-minted would change the
    // host_id). Pin both the value and the bytes.
    const reread = loadOrCreateHostIdentity(configPath);
    expect(reread).toEqual(created);
    expect(readFileSync(configPath, 'utf-8')).toBe(raw);
  });

  it('returns the env override without touching the config file', () => {
    // A server-managed sandbox host gets its identity from env vars and must not
    // read or write the config file (managed sandboxes are disposable; the
    // server owns their identity).
    process.env[HOST_ID_ENV_VAR] = 'host_env_override';
    process.env[HOST_NAME_ENV_VAR] = 'managed-env';
    const configPath = join(dir, 'config.json');

    const identity = loadOrCreateHostIdentity(configPath);

    expect(identity.hostId).toBe('host_env_override');
    expect(identity.name).toBe('managed-env');
    // The identity file must not be materialized by the env path.
    expect(existsSync(configPath)).toBe(false);
  });

  it('requires both identity env vars together', () => {
    // Setting only one identity env var is a launcher bug — fail loud instead of
    // mixing a server-chosen id with a generated name.
    process.env[HOST_ID_ENV_VAR] = 'host_env_override';
    delete process.env[HOST_NAME_ENV_VAR];

    expect(() => loadOrCreateHostIdentity(join(dir, 'config.json'))).toThrow(
      /must be set together/,
    );
  });

  it('requires both identity env vars together (name without id)', () => {
    // The mirror case: only the name set is equally a launcher bug.
    delete process.env[HOST_ID_ENV_VAR];
    process.env[HOST_NAME_ENV_VAR] = 'managed-env';

    expect(() => loadOrCreateHostIdentity(join(dir, 'config.json'))).toThrow(
      /must be set together/,
    );
  });

  it('regenerates the host section when it is partial (missing name)', () => {
    // A config whose host section lacks a required field is treated as absent —
    // a fresh, complete identity is generated and written through.
    const configPath = join(dir, 'config.json');
    writeFileSync(configPath, JSON.stringify({ host: { host_id: 'host_partial' } }));

    const identity = loadOrCreateHostIdentity(configPath);
    const data = JSON.parse(readFileSync(configPath, 'utf-8'));

    expect(identity.hostId.startsWith('host_')).toBe(true);
    expect(identity.hostId).not.toBe('host_partial');
    expect(data.host.host_id).toBe(identity.hostId);
    expect(data.host.name).toBe(identity.name);
  });

  it('returns a HostIdentity shape with hostId and name', () => {
    const configPath = join(dir, 'config.json');
    const identity: HostIdentity = loadOrCreateHostIdentity(configPath);
    expect(typeof identity.hostId).toBe('string');
    expect(typeof identity.name).toBe('string');
  });
});

function restoreEnv(name: string, saved: string | undefined): void {
  if (saved === undefined) {
    delete process.env[name];
  } else {
    process.env[name] = saved;
  }
}
