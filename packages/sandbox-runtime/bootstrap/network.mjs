// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Baked into trusted images. Only the bootstrap init receives the Pod-bound token.
import { readFile, writeFile, rename, mkdir } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { isIP } from 'node:net';
import { pathToFileURL } from 'node:url';

const root = '/run/orca/sandbox-network';
const ca = root + '/ca-bundle.crt';
const tokenPath = '/var/run/orca/sandbox-network/token';
const managedCa = '/etc/orca/sandbox-egress/ca-bundle.crt';

function clean(value) {
  if (typeof value !== 'string' || !value || value.length > 16384 || /[\0\r\n]/.test(value))
    throw new Error('invalid sandbox network value');
  return value;
}

function origin(value, protocols) {
  const url = new URL(clean(value));
  if (
    !protocols.includes(url.protocol) ||
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash
  )
    throw new Error('invalid sandbox network origin');
  return url;
}

export function networkEnvironment(context) {
  if (
    !context ||
    Object.keys(context).sort().join(',') !==
      'binding_id,ca_bundle_path,generation,internal_bypass,proxy_capability,proxy_url' ||
    context.ca_bundle_path !== ca ||
    !Array.isArray(context.internal_bypass) ||
    context.internal_bypass.length > 128
  )
    throw new Error('invalid sandbox network context');
  clean(context.binding_id);
  clean(context.generation);
  const proxy = origin(context.proxy_url, ['https:']);
  const capability = clean(context.proxy_capability);
  for (const host of context.internal_bypass)
    if (
      typeof host !== 'string' ||
      (isIP(host) !== 4 &&
        !/^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/.test(
          host,
        ))
    )
      throw new Error('invalid sandbox network bypass');
  const authenticated = 'https://orca:' + encodeURIComponent(capability) + '@' + proxy.host + '/';
  const bypass = [...new Set(context.internal_bypass)].join(',');
  return {
    HTTPS_PROXY: authenticated,
    HTTP_PROXY: authenticated,
    https_proxy: authenticated,
    http_proxy: authenticated,
    ALL_PROXY: '',
    all_proxy: '',
    NO_PROXY: bypass,
    no_proxy: bypass,
    CURL_HOME: root,
    CURL_CA_BUNDLE: ca,
    SSL_CERT_FILE: ca,
    REQUESTS_CA_BUNDLE: ca,
    PIP_CERT: ca,
    NODE_EXTRA_CA_CERTS: ca,
    NPM_CONFIG_CAFILE: ca,
    CARGO_HTTP_CAINFO: ca,
    GIT_SSL_CAINFO: ca,
    GIT_PROXY_SSL_CAINFO: ca,
    GIT_SSL_NO_VERIFY: 'false',
    GIT_PROXY_SSL_VERIFY: 'true',
    APT_CONFIG: root + '/apt.conf',
  };
}

export async function readNetworkEnvironment() {
  return networkEnvironment(JSON.parse(await readFile(root + '/context.json', 'utf8')));
}

const quote = (value) => "'" + value.replaceAll("'", "'\\''") + "'";

export async function bootstrap(registryOrigin, binding, generation) {
  const registry = origin(registryOrigin, ['http:', 'https:']);
  clean(binding);
  clean(generation);
  let existing;
  try {
    existing = await readFile(root + '/context.json', 'utf8');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
  if (existing !== undefined) {
    const context = JSON.parse(existing);
    networkEnvironment(context);
    if (context.binding_id !== binding || context.generation !== generation)
      throw new Error('sandbox network context generation mismatch');
    return;
  }
  const token = (await readFile(tokenPath, 'utf8')).trim();
  clean(token);
  const deadline = Date.now() + 60000;
  let response;
  while (Date.now() < deadline) {
    try {
      response = await fetch(
        new URL(
          '/internal/v1/sandbox-networks/' + encodeURIComponent(binding) + '/bootstrap',
          registry,
        ),
        {
          method: 'POST',
          redirect: 'error',
          signal: AbortSignal.timeout(Math.min(5000, deadline - Date.now())),
          headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' },
          body: JSON.stringify({ generation }),
        },
      );
    } catch {
      response = undefined;
    }
    if (response?.ok) break;
    if (response && ![409, 425, 429, 500, 502, 503, 504].includes(response.status))
      throw new Error('sandbox network bootstrap denied');
    await response?.body?.cancel();
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  if (!response?.ok) throw new Error('sandbox network bootstrap timed out');
  // Bound the response without retaining service diagnostics or logging capabilities.
  let body = '';
  for await (const bytes of response.body) {
    body += Buffer.from(bytes).toString('utf8');
    if (body.length > 65536) throw new Error('sandbox network response exceeds limit');
  }
  const value = JSON.parse(body);
  if (
    !value ||
    Object.keys(value).sort().join(',') !==
      'binding_id,generation,internal_bypass,proxy_capability,proxy_url' ||
    value.binding_id !== binding ||
    value.generation !== generation
  )
    throw new Error('sandbox network bootstrap identity mismatch');
  const context = { ...value, ca_bundle_path: ca };
  const env = networkEnvironment(context);
  const certificates = await Promise.all([
    readFile('/etc/ssl/certs/ca-certificates.crt', 'utf8'),
    readFile(managedCa, 'utf8'),
  ]);
  if (certificates.some((text) => !text.includes('-----BEGIN CERTIFICATE-----')))
    throw new Error('invalid sandbox network trust');
  await mkdir(root, { recursive: true, mode: 0o755 });
  await writeFile(ca, certificates.join('\n'), { mode: 0o444 });
  await writeFile(root + '/.curlrc', 'proxy-cacert = "' + ca + '"\n', { mode: 0o444 });
  // apt does not consistently consume the generic proxy environment.
  await writeFile(
    root + '/apt.conf',
    'Acquire::http::Proxy ' +
      JSON.stringify(env.HTTP_PROXY) +
      ';\n' +
      'Acquire::https::Proxy ' +
      JSON.stringify(env.HTTPS_PROXY) +
      ';\n' +
      'Acquire::https::CaInfo ' +
      JSON.stringify(ca) +
      ';\n',
    { mode: 0o444 },
  );
  await writeFile(
    root + '/environment.sh',
    Object.entries(env)
      .map(([key, value]) => 'export ' + key + '=' + quote(value))
      .join('\n') + '\n',
    { mode: 0o444 },
  );
  await writeFile(root + '/context.json.tmp', JSON.stringify(context), { mode: 0o444 });
  await rename(root + '/context.json.tmp', root + '/context.json');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.argv[2] === 'bootstrap') await bootstrap(...process.argv.slice(3));
    else if (process.argv[2] === 'exec' && process.argv[3]) {
      const child = spawn(process.argv[3], process.argv.slice(4), {
        stdio: 'inherit',
        env: { ...process.env, ...(await readNetworkEnvironment()) },
      });
      for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => child.kill(signal));
      child.once('error', () => {
        process.exitCode = 1;
      });
      child.once('exit', (code) => {
        process.exitCode = code ?? 1;
      });
    } else throw new Error('invalid sandbox network operation');
  } catch {
    process.stderr.write('Sandbox network initialization failed\n');
    process.exitCode = 1;
  }
}
