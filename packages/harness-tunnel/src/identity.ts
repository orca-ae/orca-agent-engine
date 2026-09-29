// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Runner + host identity helpers.
//
// Two surfaces share this module because they share the same handshake contract:
//
//   1. Runner identity — the token-bound runner id derivation, the stable
//      on-disk runner id, the adopt signal name (`RUNNER_ADOPT_SIGNAL`), and
//      the set of auth-secret env vars stripped at every runner→child spawn
//      boundary.
//   2. Host identity — load-or-create against an on-disk config file, with an
//      env-var override path for server-managed (disposable) sandbox hosts.
//
// Wire/contract notes:
//   - The `runner_token_`, `runner_`, and `host_` id prefixes and the 32-char
//     hex digest slice are the cross-component contract: the registry server
//     derives the *same* runner id from the *same* binding token a runner holds,
//     so the derivation is stable and reproduced byte-for-byte on both sides.
//   - Env var names use the `ORCA_` prefix and tunnel-token headers use the
//     `X-Orca-*` prefix, matching the rest of the stack.
//   - The host config file is local, single-reader on-disk state (no peer reads
//     it), so it is persisted as JSON via the Node standard library rather than
//     pulling a YAML parser into this foundational package. The env-override
//     path the server uses for managed sandboxes never touches the file at all.

import { createHash, randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { constants as osConstants, homedir, hostname } from 'node:os';
import { dirname, join } from 'node:path';

// ── Runner env var names ─────────────────────────────────

/** Parent-set override forcing child server + runner onto one peer id. */
export const PEER_ID_ENV_VAR = 'ORCA_PEER_ID';
/**
 * Parent (worker) pid seeded into the runner's environment. The session-runner
 * parses and validates it but does not watch it.
 */
export const RUNNER_PARENT_PID_ENV_VAR = 'ORCA_RUNNER_PARENT_PID';
/**
 * Name of the "adopt" signal. The session-runner installs no handler for it and
 * watches no parent pid. `SIGUSR1` is unused elsewhere in the runner.
 *
 * Exposed as the `NodeJS.Signals` name (the string Node's `process.on(...)` and
 * `process.kill(pid, ...)` accept) rather than a numeric code, so callers stay
 * platform-portable. Some platforms (notably native Windows) do not define
 * `SIGUSR1`; there it is `null`, and callers skip adopt signaling.
 */
export const RUNNER_ADOPT_SIGNAL: NodeJS.Signals | null =
  osConstants.signals.SIGUSR1 !== undefined ? 'SIGUSR1' : null;
/** Runner workspace root. */
export const RUNNER_WORKSPACE_ENV_VAR = 'ORCA_RUNNER_WORKSPACE';
/** Per-run random tunnel binding token; also the runner-side request auth token. */
export const RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR = 'ORCA_RUNNER_TUNNEL_BINDING_TOKEN';
/**
 * Registry runner-tunnel base URL the spawned runner dials back.
 *
 * The fourth member of the runner-wiring family above, and it lives here for the
 * same reason the other three do: the PRODUCER (the environment-worker, which
 * seeds the runner's environment) and the CONSUMER (the session-runner, which
 * reads it) are separate packages that never import each other, so the only way
 * the two sides cannot drift is a name they both import from this shared
 * boundary. Declaring it twice — once per side — is a hand-copied contract, which
 * is exactly what this block exists to avoid.
 */
export const RUNNER_REGISTRY_URL_ENV_VAR = 'ORCA_RUNNER_REGISTRY_URL';
/**
 * `"1"` enables per-session workspace isolation so each session gets its own
 * subdirectory. Set by shared-host servers; single-user flows leave it unset.
 */
export const RUNNER_ISOLATE_SESSION_ENV_VAR = 'ORCA_RUNNER_ISOLATE_SESSION';

// ── Tunnel handshake headers + origin ────────────────────

/**
 * WebSocket upgrade header carrying the runner's tunnel binding token. A
 * dedicated header (not `Authorization`) so the credential can't be confused
 * with a user Bearer token by intermediate proxies or the auth provider.
 */
export const RUNNER_TUNNEL_TOKEN_HEADER = 'X-Orca-Runner-Tunnel-Token';

/**
 * Sentinel `Origin` header that the project's own non-browser WebSocket clients
 * (runner→server tunnel, host/daemon→server tunnel, terminal-attach) set on
 * their handshakes so the server's CSWSH origin guard allows them. The non-HTTP
 * scheme is deliberate: a browser computes `Origin` from the page URL and can
 * never emit this value.
 */
export const INTERNAL_WS_ORIGIN = 'orca://internal';

/**
 * Base wire path a remote terminal-attach tunnel WS-channel opens on. The registry
 * opens `${RUNNER_TERMINAL_ATTACH_PATH}/<terminalId>` toward the runner over the
 * runner tunnel; the runner registers its pty-bridge handler on the `:terminalId`
 * template of the SAME base. Both sides source this single symbol so the wire
 * contract is one literal, not two hand-kept-in-sync copies — while neither
 * service depends on the other (both already depend on `@orca/harness-tunnel`,
 * the home of the tunnel wire protocol).
 */
export const RUNNER_TERMINAL_ATTACH_PATH = '/v1/runner/terminal/attach';

// ── Runner auth-secret registry ──────────────────────────

/**
 * Env vars carrying the runner's control-plane auth secret. The tunnel binding
 * token is seeded into the runner process by the launcher and reused as the
 * runner-side request auth token, but must never reach a spawned child: the
 * agent payload there could use it to impersonate the runner. Stripped at every
 * runner→child spawn boundary via {@link stripRunnerAuthSecrets}.
 */
export const RUNNER_AUTH_SECRET_ENV_VARS: ReadonlySet<string> = new Set([
  RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
]);

/**
 * Return a copy of `env` with runner-auth secrets removed.
 *
 * Applied at every boundary where the runner spawns a child it does not fully
 * trust with its control-plane credentials — harness subprocesses and sandboxed
 * tool targets. See {@link RUNNER_AUTH_SECRET_ENV_VARS} for the rationale.
 *
 * The source mapping is read-only — not mutated — so the runner process retains
 * the token in its own environment (it reuses it for request auth); only the
 * child's copy is filtered.
 */
export function stripRunnerAuthSecrets(env: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(env)) {
    if (!RUNNER_AUTH_SECRET_ENV_VARS.has(key)) {
      out[key] = value;
    }
  }
  return out;
}

// ── Runner id ────────────────────────────────────────────

/**
 * Return the stable runner id for this local machine.
 *
 * Parent processes may set {@link PEER_ID_ENV_VAR} when they need child server
 * and runner processes to agree on one id before either process touches the
 * on-disk cache. Without that override, the id is loaded from
 * `~/.orca/peers/peer_id` or created there on first use.
 *
 * @throws Error if {@link PEER_ID_ENV_VAR} is set to an empty value.
 */
export function getStableRunnerId(): string {
  const envRunnerId = process.env[PEER_ID_ENV_VAR];
  if (envRunnerId !== undefined) {
    const runnerId = envRunnerId.trim();
    if (!runnerId) {
      throw new Error(`${PEER_ID_ENV_VAR} must not be empty`);
    }
    return runnerId;
  }
  return loadOrCreateRunnerId(defaultRunnerIdPath());
}

/**
 * Return the runner id authorized by a tunnel binding token.
 *
 * Remote server-backed sessions sit behind an auth proxy. Binding the tunnel
 * runner id to a per-run random token prevents one authenticated caller from
 * claiming another caller's runner id on the same shared server. The derivation
 * is deterministic and reproduced identically by the server, so both sides agree
 * on the id for a given token.
 *
 * Derivation: a plain SHA-256 over the domain-prefixed message
 * `"orca-runner:" + <trimmed token>`; the first 32 hex chars of the digest form
 * the id suffix. The domain prefix is concatenated *inside* the hashed message
 * (not used as an HMAC key): this is the exact construction the registry server
 * computes, so the id is reproduced byte-for-byte on both sides. It must stay a
 * plain `createHash('sha256')` over the colon-joined string — switching to an
 * HMAC (or any other keying) changes the digest and breaks the cross-component
 * contract the moment a peer re-derives the id.
 *
 * @throws Error if `token` is empty.
 */
export function tokenBoundRunnerId(token: string): string {
  const stripped = token.trim();
  if (!stripped) {
    throw new Error('tunnel binding token must not be empty');
  }
  const digest = createHash('sha256')
    .update(`${RUNNER_ID_DERIVATION_PREFIX}${stripped}`)
    .digest('hex');
  return `runner_token_${digest.slice(0, 32)}`;
}

/**
 * Domain-separation prefix concatenated ahead of the token in the hashed
 * message for {@link tokenBoundRunnerId}. Prefixed inline (colon-joined), not
 * used as an HMAC key — see that function's note. Both sides of the handshake
 * hardcode this exact string.
 */
const RUNNER_ID_DERIVATION_PREFIX = 'orca-runner:';

/**
 * Load a runner id from `path`, creating one if needed.
 *
 * @throws Error if the cache file exists but is empty.
 */
export function loadOrCreateRunnerId(path: string): string {
  if (existsSync(path)) {
    const runnerId = readFileSync(path, 'utf-8').trim();
    if (!runnerId) {
      throw new Error(`runner id file is empty: ${path}`);
    }
    return runnerId;
  }
  mkdirSync(dirname(path), { recursive: true });
  const runnerId = `runner_${randomUUID().replace(/-/g, '')}`;
  writeFileSync(path, runnerId);
  return runnerId;
}

/** Return the default runner id cache path: `~/.orca/peers/peer_id`. */
function defaultRunnerIdPath(): string {
  return join(homedir(), '.orca', 'peers', 'peer_id');
}

// ── Host identity ────────────────────────────────────────

/** Default host config path: `~/.orca/config.json`. */
export const CONFIG_PATH = join(homedir(), '.orca', 'config.json');

/**
 * Env vars a server-managed sandbox host is launched with. The server provisions
 * the sandbox, generates the identity + launch token, and injects all three so
 * the host registers under the server-chosen identity without persisting
 * anything to the sandbox's config (managed sandboxes are disposable).
 * `HOST_TOKEN` is the tunnel credential (see {@link HOST_TUNNEL_TOKEN_HEADER});
 * `HOST_ID` / `HOST_NAME` override the identity file and must be set together.
 */
export const HOST_TOKEN_ENV_VAR = 'ORCA_HOST_TOKEN';
export const HOST_ID_ENV_VAR = 'ORCA_HOST_ID';
export const HOST_NAME_ENV_VAR = 'ORCA_HOST_NAME';

/**
 * WebSocket upgrade header carrying a server-provisioned sandbox host's launch
 * token. Follows the runner tunnel's {@link RUNNER_TUNNEL_TOKEN_HEADER} pattern:
 * a dedicated header (not `Authorization`) so the credential can't be confused
 * with a user Bearer token by intermediate proxies or the auth provider.
 */
export const HOST_TUNNEL_TOKEN_HEADER = 'X-Orca-Host-Token';

/** Identity of a host machine. */
export interface HostIdentity {
  /**
   * Stable identifier, e.g. `"host_a1b2c3d4e5f67890abcdef1234567890"`. Format:
   * `host_{uuid4_hex}` (32-char hex portion).
   */
  hostId: string;
  /**
   * Human-readable name displayed in the web UI host picker, e.g.
   * `"my-laptop"`.
   */
  name: string;
}

/** The on-disk shape of the persisted `host:` section. */
interface HostConfigSection {
  host_id: string;
  name: string;
}

/**
 * Load host identity from the config file, or create it if absent.
 *
 * Reads the `host` section from the config file. If the section does not exist
 * (or is partial), generates a fresh `host_id`, sets `name` to the machine's
 * hostname, writes the section back (preserving every other key), and returns
 * the identity.
 *
 * Environment override: when {@link HOST_ID_ENV_VAR} and {@link HOST_NAME_ENV_VAR}
 * are both set (a server-managed sandbox host), that identity is returned
 * directly without reading or writing the config file — managed sandboxes are
 * disposable and the server owns their identity. Setting only one of the two is
 * a launcher bug and fails loud.
 *
 * @throws Error if exactly one of the identity env vars is set.
 */
export function loadOrCreateHostIdentity(path: string = CONFIG_PATH): HostIdentity {
  const envHostId = process.env[HOST_ID_ENV_VAR];
  const envName = process.env[HOST_NAME_ENV_VAR];
  if ((envHostId === undefined) !== (envName === undefined)) {
    throw new Error(
      `${HOST_ID_ENV_VAR} and ${HOST_NAME_ENV_VAR} must be set together ` +
        '(a server-provisioned sandbox host sets both)',
    );
  }
  if (envHostId !== undefined && envName !== undefined) {
    return { hostId: envHostId, name: envName };
  }

  let cfg: Record<string, unknown> = {};
  if (existsSync(path)) {
    const parsed = JSON.parse(readFileSync(path, 'utf-8') || '{}') as unknown;
    if (isObject(parsed)) {
      cfg = parsed;
    }
  }

  const hostSection = cfg['host'];
  if (isHostSection(hostSection)) {
    return { hostId: hostSection.host_id, name: hostSection.name };
  }

  const identity: HostIdentity = {
    hostId: `host_${randomUUID().replace(/-/g, '')}`,
    name: hostname(),
  };

  cfg['host'] = { host_id: identity.hostId, name: identity.name } satisfies HostConfigSection;
  mkdirSync(dirname(path), { recursive: true });
  // Canonical on-disk format is JSON (see module header, lines 19-22): this is
  // the single reader/writer of this file, so the format is self-contained. Any
  // future second component that touches this config MUST read/write JSON here —
  // never YAML — to stay round-trip-consistent with this writer. Pinned by the
  // 'persists the config as canonical JSON that the same loader re-reads' test.
  writeFileSync(path, `${JSON.stringify(deepSortKeys(cfg), null, 2)}\n`);

  return identity;
}

function isObject(val: unknown): val is Record<string, unknown> {
  return typeof val === 'object' && val !== null && !Array.isArray(val);
}

function isHostSection(val: unknown): val is HostConfigSection {
  return isObject(val) && typeof val['host_id'] === 'string' && typeof val['name'] === 'string';
}

/**
 * Return a deep copy of `value` with the keys of every plain object sorted, so
 * the persisted config serializes deterministically across repeated writes.
 *
 * Keys are sorted *per object at every nesting depth*, and all other data —
 * nested objects, arrays, scalars — is preserved verbatim. It is applied to the
 * whole config before `JSON.stringify(..., null, 2)`.
 *
 * The earlier implementation passed a flat key-allowlist *array* as the
 * `JSON.stringify` replacer. A replacer array filters keys at every nesting
 * depth, so any pre-existing nested object whose keys were not in the (top-level
 * + `host`) allowlist — e.g. an `auth: { token, scopes }` block — was emitted as
 * `{}`, silently dropping its inner keys on the next host-identity round-trip.
 * Deep-copying with per-object key sorting preserves every key instead, honoring
 * the documented "preserving every other key" contract above.
 */
function deepSortKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(deepSortKeys);
  }
  if (isObject(value)) {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = deepSortKeys(value[key]);
    }
    return out;
  }
  return value;
}

/** Private runner control record; consumed by Registry before transcript mapping. */
export const HARNESS_CHECKPOINT_EVENT = 'orca.harness_checkpoint';
