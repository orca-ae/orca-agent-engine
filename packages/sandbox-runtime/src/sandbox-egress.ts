// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { isIP } from 'node:net';
import { posix } from 'node:path';
import type { SandboxHandle } from './sandbox-runtime.js';

export const SANDBOX_NETWORK_EXTENSION = 'orca.sandbox.network';
export const SANDBOX_NETWORK_BINDING_ANNOTATION = 'orca.ai/network-binding-id';
export const SANDBOX_NETWORK_GENERATION_ANNOTATION = 'orca.ai/network-generation';
export const SANDBOX_NETWORK_DIRECTORY = '/run/orca/sandbox-network';
export const SANDBOX_NETWORK_CONTEXT_PATH = SANDBOX_NETWORK_DIRECTORY + '/context.json';
export const SANDBOX_NETWORK_TRUST_PATH = SANDBOX_NETWORK_DIRECTORY + '/ca-bundle.crt';
export const SANDBOX_NETWORK_TIMEOUT_MS = 70_000;
export const SANDBOX_EGRESS_ENABLED_ENV = 'SANDBOX_EGRESS_ENABLED';
export const SESSION_EGRESS_BOOTSTRAP_PATH = '/run/orca/session-egress/bootstrap.sh';

export interface SandboxNetworkRequest {
  binding_id: string;
  generation: string;
}

export interface SandboxNetworkContext extends SandboxNetworkRequest {
  proxy_url: string;
  proxy_capability: string;
  ca_bundle_path: string;
  internal_bypass: string[];
}

export interface SessionEgressGrant extends SandboxNetworkRequest {
  network_binding_id: string;
  proxy_capability: string;
  placeholders: Record<string, string>;
  git?: { marker: string; username: string };
}

function record(item: unknown): Record<string, unknown> {
  if (!item || typeof item !== 'object' || Array.isArray(item))
    throw new Error('invalid sandbox network value');
  return item as Record<string, unknown>;
}

function string(item: unknown): string {
  if (typeof item !== 'string' || !item || item.length > 16_384 || /[\0\r\n]/.test(item))
    throw new Error('invalid sandbox network value');
  return item;
}

function exact(raw: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(raw).some((key) => !keys.includes(key)))
    throw new Error('unexpected sandbox network field');
}

export function parseSandboxNetworkRequest(value: unknown): SandboxNetworkRequest {
  const raw = record(value);
  exact(raw, ['binding_id', 'generation']);
  return { binding_id: string(raw.binding_id), generation: string(raw.generation) };
}

export function parseSandboxNetworkContext(value: unknown): SandboxNetworkContext {
  const raw = record(value);
  exact(raw, [
    'binding_id',
    'generation',
    'proxy_url',
    'proxy_capability',
    'ca_bundle_path',
    'internal_bypass',
  ]);
  if (
    !Array.isArray(raw.internal_bypass) ||
    raw.internal_bypass.length > 128 ||
    raw.ca_bundle_path !== SANDBOX_NETWORK_TRUST_PATH
  )
    throw new Error('invalid sandbox network context');
  const context: SandboxNetworkContext = {
    binding_id: string(raw.binding_id),
    generation: string(raw.generation),
    proxy_url: string(raw.proxy_url),
    proxy_capability: string(raw.proxy_capability),
    ca_bundle_path: SANDBOX_NETWORK_TRUST_PATH,
    internal_bypass: raw.internal_bypass.map(string),
  };
  buildSandboxNetworkEnvironment(context);
  return context;
}

/** Validate only at the authenticated resource boundary; never persist this delivery. */
export function parseSessionEgressGrant(value: unknown): SessionEgressGrant {
  const raw = record(value);
  exact(raw, [
    'network_binding_id',
    'binding_id',
    'generation',
    'proxy_capability',
    'placeholders',
    'git',
  ]);
  const placeholders = record(raw.placeholders);
  if (Object.keys(placeholders).length > 128) {
    throw new Error('invalid sandbox egress delivery');
  }
  const git = raw.git === undefined ? undefined : record(raw.git);
  if (git) exact(git, ['marker', 'username']);
  const result: SessionEgressGrant = {
    network_binding_id: string(raw.network_binding_id),
    binding_id: string(raw.binding_id),
    generation: string(raw.generation),
    proxy_capability: string(raw.proxy_capability),
    placeholders: Object.fromEntries(
      Object.entries(placeholders).map(([name, marker]) => [name, string(marker)]),
    ),
    ...(git ? { git: { marker: string(git.marker), username: string(git.username) } } : {}),
  };
  buildEnvironment({
    proxyUrl: 'https://validation.invalid',
    capability: result.proxy_capability,
    proxyCaPath: SANDBOX_NETWORK_TRUST_PATH,
    upstreamCaPath: SANDBOX_NETWORK_TRUST_PATH,
    internalBypass: [],
    executionDirectory: '/run/orca/validation',
    placeholders: result.placeholders,
    ...(result.git ? { git: result.git } : {}),
  });
  return result;
}

export interface SandboxEgressConfig {
  proxyUrl: string;
  internalBypass: string[];
  owner: { namespace: string; name: string; uid: string };
}

/** Public trust paths must exist before processes receive this environment. */
interface EgressEnvironmentInput {
  proxyUrl: string;
  capability: string;
  internalBypass: readonly string[];
  proxyCaPath: string;
  upstreamCaPath: string;
  executionDirectory: string;
  git?: { marker: string; username: string };
  placeholders: Readonly<Record<string, string>>;
}

// A local helper: no Registry requests, upstream secrets, or gh dependency.
export const SANDBOX_EGRESS_GIT_HELPER =
  '!f() { if [ "$1" = get ]; then printf "username=%s\\npassword=%s\\n" "$ORCA_GIT_MARKER_USERNAME" "$ORCA_GIT_MARKER"; fi; }; f';

const conflicting =
  /^(?:all_proxy|https?_proxy|no_proxy|git_config.*|git_askpass|ssh_askpass|git_ssl.*|git_proxy_ssl.*|curl_home|curl_ca_bundle|curl_proxy_cainfo|ssl_cert_.*|node_extra_ca_certs|node_tls_reject_unauthorized|requests_ca_bundle|pip_cert|npm_config_.*|cargo_http_.*|apt_config|gh_.*|github_token|orca_git_.*|orca_egress_bootstrap_.*)$/i;

function absolutePath(path: string): string {
  if (
    !path.startsWith('/') ||
    path === '/' ||
    posix.normalize(path) !== path ||
    path.includes('\0') ||
    /[\r\n]/.test(path)
  ) {
    throw new Error('sandbox egress requires canonical absolute trust and execution paths');
  }
  return path;
}

/** The execution owner verifies network enforcement separately, before issuance. */
function buildEnvironment(
  input: EgressEnvironmentInput,
  existing: Readonly<Record<string, string>> = {},
): Record<string, string> {
  for (const name of Object.keys(existing)) {
    if (conflicting.test(name)) throw new Error('sandbox egress conflicts with client environment');
  }
  let proxy: URL;
  try {
    proxy = new URL(input.proxyUrl);
  } catch {
    throw new Error('sandbox egress requires an HTTPS proxy URL');
  }
  if (
    proxy.protocol !== 'https:' ||
    !proxy.hostname ||
    proxy.username ||
    proxy.password ||
    proxy.pathname !== '/' ||
    proxy.search ||
    proxy.hash
  )
    throw new Error('sandbox egress requires an HTTPS proxy origin without credentials');
  if (!input.capability || input.capability.includes('\0') || /[\r\n]/.test(input.capability)) {
    throw new Error('invalid sandbox egress capability');
  }
  for (const host of input.internalBypass) {
    // DNS NO_PROXY suffix matching varies by client; Cilium enforces destination/port scope.
    if (
      isIP(host) !== 4 &&
      !/^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/.test(
        host,
      )
    ) {
      throw new Error('sandbox egress bypass requires canonical DNS names or IPv4 addresses');
    }
  }
  const proxyCa = absolutePath(input.proxyCaPath);
  const upstreamCa = absolutePath(input.upstreamCaPath);
  const directory = absolutePath(input.executionDirectory);
  const env: Record<string, string> = { ...existing };
  for (const [name, marker] of Object.entries(input.placeholders)) {
    if (
      !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ||
      conflicting.test(name) ||
      name in env ||
      marker.includes('\0') ||
      /[\r\n]/.test(marker)
    ) {
      throw new Error('sandbox egress placeholder conflicts with platform environment');
    }
    env[name] = marker;
  }
  const authenticatedProxy =
    'https://orca:' + encodeURIComponent(input.capability) + '@' + proxy.host + '/';
  Object.assign(env, {
    HTTPS_PROXY: authenticatedProxy,
    ALL_PROXY: '',
    all_proxy: '',
    HTTP_PROXY: authenticatedProxy,
    https_proxy: authenticatedProxy,
    http_proxy: authenticatedProxy,
    NO_PROXY: [...new Set(input.internalBypass)].join(','),
    no_proxy: [...new Set(input.internalBypass)].join(','),
    CURL_HOME: SANDBOX_NETWORK_DIRECTORY,
    CURL_CA_BUNDLE: upstreamCa,
    REQUESTS_CA_BUNDLE: upstreamCa,
    PIP_CERT: upstreamCa,
    NPM_CONFIG_CAFILE: upstreamCa,
    CARGO_HTTP_CAINFO: upstreamCa,
    SSL_CERT_FILE: upstreamCa,
    NODE_EXTRA_CA_CERTS: upstreamCa,
    GIT_CONFIG_NOSYSTEM: '1',
    GIT_CONFIG_PARAMETERS: '',
    GIT_SSL_NO_VERIFY: 'false',
    GIT_SSL_CAINFO: upstreamCa,
    GIT_PROXY_SSL_CAINFO: proxyCa,
    GIT_PROXY_SSL_VERIFY: 'true',
    GIT_CONFIG_GLOBAL: '/dev/null',
    GIT_TERMINAL_PROMPT: '0',
    GIT_CONFIG_COUNT: '3',
    GIT_CONFIG_KEY_0: 'http.proxySSLCAInfo',
    GIT_CONFIG_VALUE_0: proxyCa,
    GIT_CONFIG_KEY_1: 'http.sslCAInfo',
    GIT_CONFIG_VALUE_1: upstreamCa,
    GIT_CONFIG_KEY_2: 'credential.helper',
    GIT_CONFIG_VALUE_2: '',
    GH_CONFIG_DIR: directory + '/gh',
  });
  if (input.git) {
    if (
      !input.git.marker ||
      !input.git.username ||
      (input.git.marker + input.git.username).includes('\0') ||
      /[\r\n]/.test(input.git.marker + input.git.username)
    ) {
      throw new Error('invalid sandbox egress Git placeholder');
    }
    Object.assign(env, {
      ORCA_GIT_MARKER: input.git.marker,
      ORCA_GIT_MARKER_USERNAME: input.git.username,
      GH_TOKEN: input.git.marker,
      GH_HOST: 'github.com',
      GIT_CONFIG_COUNT: '6',
      GIT_CONFIG_KEY_3: 'credential.helper',
      GIT_CONFIG_VALUE_3: SANDBOX_EGRESS_GIT_HELPER,
      GIT_CONFIG_KEY_4: 'credential.useHttpPath',
      GIT_CONFIG_VALUE_4: 'true',
      GIT_CONFIG_KEY_5: 'http.proactiveAuth',
      GIT_CONFIG_VALUE_5: 'basic',
    });
  }
  return env;
}

export function buildSandboxNetworkEnvironment(
  context: SandboxNetworkContext,
): Record<string, string> {
  return buildEnvironment({
    proxyUrl: context.proxy_url,
    capability: context.proxy_capability,
    internalBypass: context.internal_bypass,
    proxyCaPath: context.ca_bundle_path,
    upstreamCaPath: context.ca_bundle_path,
    executionDirectory: SANDBOX_NETWORK_DIRECTORY,
    placeholders: {},
  });
}

export function buildSessionEgressEnvironment(
  context: SandboxNetworkContext,
  grant: SessionEgressGrant,
  existing: Readonly<Record<string, string>> = {},
): Record<string, string> {
  parseSandboxNetworkContext(context);
  parseSessionEgressGrant(grant);
  if (grant.network_binding_id !== context.binding_id)
    throw new Error('Session egress parent does not match local sandbox network');
  return buildEnvironment(
    {
      proxyUrl: context.proxy_url,
      capability: grant.proxy_capability,
      internalBypass: context.internal_bypass,
      proxyCaPath: context.ca_bundle_path,
      upstreamCaPath: context.ca_bundle_path,
      executionDirectory: '/home/node/.orca-egress/' + encodeURIComponent(grant.binding_id),
      placeholders: grant.placeholders,
      ...(grant.git ? { git: grant.git } : {}),
    },
    existing,
  );
}

/** Project the Session grant into its private process; never rewrite shared network context. */
export async function prepareSessionEgress(
  sandbox: SandboxHandle,
  issued: SessionEgressGrant,
  colocated: boolean,
  existing: Record<string, string> = {},
): Promise<void> {
  if (!sandbox.configureSessionEgress)
    throw new Error('runtime cannot install sandbox egress bootstrap');
  const context = parseSandboxNetworkContext(
    JSON.parse((await sandbox.files.read(SANDBOX_NETWORK_CONTEXT_PATH)).toString('utf8')),
  );
  await sandbox.files.delete('/usr/local/bin/orca-git-creds');
  const env = buildSessionEgressEnvironment(context, issued, existing);
  await sandbox.configureSessionEgress(env);
  if (colocated) {
    const script =
      Object.entries(env)
        .map(([name, value]) => 'export ' + name + "='" + value.replaceAll("'", "'\\''") + "'")
        .join('\n') + '\n';
    await sandbox.files.write(SESSION_EGRESS_BOOTSTRAP_PATH, Buffer.from(script));
    await sandbox.files.chmod(SESSION_EGRESS_BOOTSTRAP_PATH, 0o600);
  }
}
