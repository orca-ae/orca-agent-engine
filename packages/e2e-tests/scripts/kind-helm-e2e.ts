// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Black-box Kind Helm verification for split managed-agents/gateway deployment.
 * Workflow builds and loads local images first. This script owns fixtures,
 * releases, public API traffic, and a session-JWT trust-anchor rotation.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '../../..');
const namespace = process.env['KIND_HELM_NAMESPACE'] ?? 'orca-kind-e2e';
const configuredManagedChartDir = process.env['ORCA_MANAGED_CHART_DIR'];
const configuredGatewayChartDir = process.env['ORCA_GATEWAY_CHART_DIR'];
const managedChartDir =
  configuredManagedChartDir === undefined
    ? join(repoRoot, 'charts/orca-managed-agents')
    : resolve(repoRoot, configuredManagedChartDir);
const gatewayChartDir =
  configuredGatewayChartDir === undefined ? '' : resolve(repoRoot, configuredGatewayChartDir);
const infraPath = join(managedChartDir, 'test/kind/infra.yaml');
const timeoutMs = Number(process.env['KIND_HELM_E2E_TIMEOUT_MS'] ?? '180000');

const registryPort = Number(process.env['KIND_HELM_REGISTRY_PORT'] ?? '18080');
const registryInternalPort = Number(process.env['KIND_HELM_REGISTRY_INTERNAL_PORT'] ?? '18081');
const registryAdminPort = Number(process.env['KIND_HELM_REGISTRY_ADMIN_PORT'] ?? '18082');
const gatewayPort = Number(process.env['KIND_HELM_GATEWAY_PORT'] ?? '18090');
const gatewayAdminPort = Number(process.env['KIND_HELM_GATEWAY_ADMIN_PORT'] ?? '18099');
const fixturePort = Number(process.env['KIND_HELM_FIXTURE_PORT'] ?? '18091');

const registryUrl = `http://127.0.0.1:${registryPort}`;
const registryInternalUrl = `http://127.0.0.1:${registryInternalPort}`;
const registryAdminUrl = `http://127.0.0.1:${registryAdminPort}`;
const gatewayUrl = `http://127.0.0.1:${gatewayPort}`;
const fixtureUrl = `http://127.0.0.1:${fixturePort}`;

const managedRelease = 'managed';
const gatewayRelease = 'gateway';
const registryDeployment = 'registry';
const harnessDeployment = 'harness';
const gatewayDeployment = 'gateway';
const managedRegistryConfigMap = 'managed-registry-config';
const managedHarnessConfigMap = 'managed-harness-config';
const gatewayConfigMap = 'gateway-config';
const managedSecret = 'managed-secrets';
const sessionJwtVersionAnnotation = 'orca.ai/session-jwt-key-version';
const kubectlRequestTimeout = '6m';
const gatewayPublicKeyDirectory = '/var/run/secrets/orca/session-jwt-public-key';
const gatewayPublicKeyPath = `${gatewayPublicKeyDirectory}/public.pem`;
const registryInternalTokenDirectory = '/var/run/secrets/orca/registry-internal';
const registryInternalTokenPath = `${registryInternalTokenDirectory}/token`;
const fixtureMcpBearer = 'kind-helm-e2e-secret';

const portForwards: ChildProcess[] = [];
let kubeContext = '';
let kubeconfigPath = '';

interface CommandOptions {
  cwd?: string;
  input?: string;
  quiet?: boolean;
  env?: NodeJS.ProcessEnv;
  display?: string;
  timeoutMs?: number;
}

interface CommandResult {
  stdout: string;
  stderr: string;
}

interface HttpResult {
  status: number;
  body: unknown;
  headers: Headers;
  text: string;
}

interface KeyPair {
  privateKey: string;
  publicKey: string;
}

interface BootstrapCredentials {
  adminKey: string;
  platformKey: string;
}

interface TurnResources {
  agentId: string;
  credentialId: string;
  environmentId: string;
  memoryContent: string;
  memoryId: string;
  memoryStoreId: string;
  sessionId: string;
  vaultId: string;
  workspaceId: string;
  workspaceKey: string;
}

interface LlmUsageResources {
  agentId: string;
  sessionId: string;
}

interface ApplicationPodUids {
  gateway: string;
  harness: string;
  registry: string;
}

interface ApplicationConfigChecksums {
  gateway: string;
  harness: string;
  registry: string;
}

function assert(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}

function record(value: unknown, context: string): Record<string, unknown> {
  assert(
    value !== null && typeof value === 'object' && !Array.isArray(value),
    `${context} is not an object`,
  );
  return value as Record<string, unknown>;
}

function array(value: unknown, context: string): unknown[] {
  assert(Array.isArray(value), `${context} is not an array`);
  return value;
}

function stringField(value: Record<string, unknown>, field: string, context: string): string {
  const candidate = value[field];
  assert(typeof candidate === 'string' && candidate.length > 0, `${context}.${field} is missing`);
  return candidate;
}

function numberField(value: Record<string, unknown>, field: string, context: string): number {
  const candidate = value[field];
  assert(
    typeof candidate === 'number' && Number.isFinite(candidate),
    `${context}.${field} is not a number`,
  );
  return candidate;
}

function arrayField(value: Record<string, unknown>, field: string, context: string): unknown[] {
  return array(value[field], `${context}.${field}`);
}

function stringArrayField(
  value: Record<string, unknown>,
  field: string,
  context: string,
): string[] {
  return arrayField(value, field, context).map((entry, index) => {
    assert(typeof entry === 'string', `${context}.${field}[${index}] is not a string`);
    return entry;
  });
}

function parseJson(text: string, context: string): unknown {
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new Error(`${context} is not valid JSON`);
  }
}

function yamlBlock(value: string, indentation: number): string {
  const prefix = ' '.repeat(indentation);
  return value
    .replace(/\n$/, '')
    .split('\n')
    .map((line) => `${prefix}${line}`)
    .join('\n');
}

async function command(
  executable: string,
  args: string[],
  options: CommandOptions = {},
): Promise<CommandResult> {
  console.log(`$ ${options.display ?? [executable, ...args].join(' ')}`);
  return await new Promise<CommandResult>((resolvePromise, reject) => {
    const child = spawn(executable, args, {
      cwd: options.cwd,
      env: { ...process.env, ...options.env },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    let forceKillTimer: NodeJS.Timeout | undefined;
    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
      forceKillTimer = setTimeout(() => child.kill('SIGKILL'), 5_000);
    }, options.timeoutMs ?? 120_000);
    const clearTimers = (): void => {
      clearTimeout(timeout);
      if (forceKillTimer !== undefined) clearTimeout(forceKillTimer);
    };
    child.stdout.on('data', (chunk: Buffer) => {
      const text = chunk.toString('utf8');
      stdout += text;
      if (!options.quiet) process.stdout.write(text);
    });
    child.stderr.on('data', (chunk: Buffer) => {
      const text = chunk.toString('utf8');
      stderr += text;
      if (!options.quiet) process.stderr.write(text);
    });
    child.on('error', (error) => {
      clearTimers();
      reject(error);
    });
    child.on('close', (code, signal) => {
      clearTimers();
      if (timedOut) {
        reject(new Error(`${executable} timed out after ${options.timeoutMs ?? 120_000}ms`));
        return;
      }
      if (code === 0) {
        resolvePromise({ stdout, stderr });
        return;
      }
      const output = options.quiet ? 'command output suppressed' : stderr || stdout;
      reject(
        new Error(
          `${executable} exited ${code ?? 'null'}${signal ? ` (${signal})` : ''}\n${output}`,
        ),
      );
    });
    if (options.input !== undefined) child.stdin.end(options.input);
    else child.stdin.end();
  });
}

async function kubectl(args: string[], options?: CommandOptions): Promise<CommandResult> {
  assert(kubeContext.length > 0 && kubeconfigPath.length > 0, 'Kind kubeconfig is not initialized');
  return await command(
    'kubectl',
    [
      '--kubeconfig',
      kubeconfigPath,
      '--context',
      kubeContext,
      `--request-timeout=${kubectlRequestTimeout}`,
      '-n',
      namespace,
      ...args,
    ],
    options,
  );
}

async function applyManifest(path: string): Promise<void> {
  const manifest = (await readFile(path, 'utf8')).replaceAll('__NAMESPACE__', namespace);
  await command(
    'kubectl',
    [
      '--kubeconfig',
      kubeconfigPath,
      '--context',
      kubeContext,
      `--request-timeout=${kubectlRequestTimeout}`,
      'apply',
      '-f',
      '-',
    ],
    { input: manifest },
  );
}

async function writeValues(directory: string, name: string, contents: string): Promise<string> {
  const path = join(directory, name);
  await writeFile(path, contents);
  return path;
}

async function sleep(milliseconds: number): Promise<void> {
  await new Promise((resolvePromise) => setTimeout(resolvePromise, milliseconds));
}

async function helmUpgrade(release: string, chart: string, values: string): Promise<void> {
  await command(
    'helm',
    [
      'upgrade',
      '--install',
      release,
      chart,
      '--kubeconfig',
      kubeconfigPath,
      '--kube-context',
      kubeContext,
      '--namespace',
      namespace,
      '--create-namespace',
      '--values',
      values,
      '--wait',
      '--atomic',
      '--timeout',
      '7m',
    ],
    { timeoutMs: 8 * 60_000 },
  );
}

async function helmTest(release: string, options: { logs?: boolean } = {}): Promise<void> {
  const args = [
    'test',
    release,
    '--kubeconfig',
    kubeconfigPath,
    '--kube-context',
    kubeContext,
    '--namespace',
    namespace,
  ];
  if (options.logs !== false) args.push('--logs');
  args.push('--timeout', '5m');
  await command('helm', args, { timeoutMs: 6 * 60_000 });
}

async function getDeployment(name: string): Promise<Record<string, unknown>> {
  const result = await kubectl(['get', `deployment/${name}`, '-o', 'json'], { quiet: true });
  return record(parseJson(result.stdout, `deployment/${name}`), `deployment/${name}`);
}

async function deploymentNames(): Promise<string[]> {
  const result = await kubectl(['get', 'deployments', '-o', 'json'], { quiet: true });
  const deployments = record(parseJson(result.stdout, 'deployment list'), 'deployment list');
  const items = arrayField(deployments, 'items', 'deployment list');
  const names = items.map((item, index) => {
    const deployment = record(item, `deployment list item ${index}`);
    return stringField(
      record(deployment['metadata'], `deployment list item ${index}.metadata`),
      'name',
      'deployment',
    );
  });
  assert(names.length > 0, `namespace ${namespace} has no Deployments`);
  return names.sort();
}

async function waitForDeployment(name: string): Promise<void> {
  await kubectl(['rollout', 'status', `deployment/${name}`, '--timeout=5m'], {
    timeoutMs: 330_000,
  });
  await kubectl(['wait', '--for=condition=Available', `deployment/${name}`, '--timeout=5m'], {
    timeoutMs: 330_000,
  });
  const deployment = await getDeployment(name);
  const spec = record(deployment['spec'], `deployment/${name}.spec`);
  const status = record(deployment['status'], `deployment/${name}.status`);
  const replicas = numberField(spec, 'replicas', `deployment/${name}.spec`);
  assert(
    Number.isInteger(replicas) && replicas >= 0,
    `deployment/${name}.spec.replicas is invalid`,
  );
  assert(
    numberField(status, 'availableReplicas', `deployment/${name}.status`) === replicas,
    `deployment/${name} availableReplicas does not equal replicas`,
  );
  assert(
    numberField(status, 'readyReplicas', `deployment/${name}.status`) === replicas,
    `deployment/${name} readyReplicas does not equal replicas`,
  );
}

async function waitForAllDeployments(): Promise<void> {
  for (const name of await deploymentNames()) await waitForDeployment(name);
}

async function startPortForward(
  service: string,
  localPort: number,
  remotePort: number,
  healthPath?: string,
): Promise<void> {
  console.log(
    `$ kubectl -n ${namespace} port-forward service/${service} ${localPort}:${remotePort}`,
  );
  const child = spawn(
    'kubectl',
    [
      '--kubeconfig',
      kubeconfigPath,
      '--context',
      kubeContext,
      '--request-timeout=0',
      '-n',
      namespace,
      'port-forward',
      `service/${service}`,
      `${localPort}:${remotePort}`,
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );
  let diagnostics = '';
  child.stdout.on('data', (chunk: Buffer) => {
    diagnostics += chunk.toString('utf8');
  });
  child.stderr.on('data', (chunk: Buffer) => {
    diagnostics += chunk.toString('utf8');
  });
  child.on('error', (error) => {
    diagnostics += `\n${error.message}`;
  });
  portForwards.push(child);

  const healthUrl =
    healthPath === undefined ? undefined : `http://127.0.0.1:${localPort}${healthPath}`;
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`port-forward for ${service} exited early`);
    }
    const forwarded = diagnostics.includes('Forwarding from ');
    if (forwarded && healthUrl === undefined) return;
    if (forwarded && healthUrl !== undefined) {
      try {
        const response = await fetch(healthUrl, { signal: AbortSignal.timeout(1_000) });
        if (response.ok) return;
      } catch {
        // Local listener is bound, but target Service is not reachable yet.
      }
    }
    await sleep(250);
  }
  throw new Error(`timed out starting port-forward for ${service}: ${diagnostics}`);
}

async function stopPortForward(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  await new Promise<void>((resolvePromise) => {
    const timeout = setTimeout(() => {
      if (child.exitCode === null) child.kill('SIGKILL');
      resolvePromise();
    }, 5_000);
    child.once('close', () => {
      clearTimeout(timeout);
      resolvePromise();
    });
    child.kill('SIGTERM');
  });
}

async function stopPortForwards(): Promise<void> {
  await Promise.all(portForwards.splice(0).map(stopPortForward));
}

async function startPortForwards(): Promise<void> {
  await startPortForward('registry', registryPort, 8080, '/healthz');
  await startPortForward('registry-internal', registryInternalPort, 8081, '/healthz');
  await startPortForward('registry-admin', registryAdminPort, 8082, '/healthz');
  await startPortForward('gateway', gatewayPort, 8080);
  // Gateway health lives only on admin listener. Do not probe data plane.
  await startPortForward('gateway', gatewayAdminPort, 9099, '/healthz');
  await startPortForward('fixture', fixturePort, 8080, '/healthz');
}

async function request(baseUrl: string, path: string, init: RequestInit = {}): Promise<HttpResult> {
  const headers = new Headers(init.headers);
  if (init.body !== undefined && !headers.has('content-type'))
    headers.set('content-type', 'application/json');
  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers,
    signal: AbortSignal.timeout(30_000),
  });
  const text = await response.text();
  let body: unknown = null;
  if (text !== '') {
    try {
      body = JSON.parse(text) as unknown;
    } catch {
      body = text;
    }
  }
  return { status: response.status, body, headers: response.headers, text };
}

async function expectedRequest(
  baseUrl: string,
  path: string,
  expectedStatus: number | number[],
  init: RequestInit = {},
): Promise<Record<string, unknown>> {
  const result = await request(baseUrl, path, init);
  const expected = Array.isArray(expectedStatus) ? expectedStatus : [expectedStatus];
  assert(
    expected.includes(result.status),
    `${init.method ?? 'GET'} ${path}: expected ${expected.join('/')} got ${result.status}`,
  );
  return record(result.body, `${init.method ?? 'GET'} ${path} response`);
}

function workspaceHeaders(apiKey: string): Record<string, string> {
  return {
    'anthropic-beta': 'managed-agents-2026-04-01',
    'x-api-key': apiKey,
  };
}

function gatewayConfig(marker: string): string {
  return `# kind-helm-config-marker: ${marker}
server:
  listen: "0.0.0.0:8080"
  admin_listen: "0.0.0.0:9099"

identity:
  scope_dims: [workspace_id, session_id, org_id, llm_routes, llm_models, mcp_server_names, vault_ids, credential_ids]
  validators:
    - name: orca-session-jwt
      kind: jwt
      static_public_key_pem: "${gatewayPublicKeyPath}"
      issuers: ["orca-registry"]
      audiences: ["ai-gateway"]
      scope_from:
        jwt_claims:
          workspace_id: workspace_id
          session_id: session_id
          org_id: org_id
          llm_routes: llm_routes
          llm_models: llm_models
          mcp_server_names: mcp_server_names
          vault_ids: vault_ids
          credential_ids: credential_ids

vaults:
  - name: anthropic-fixture
    resolver: env
    env_var: ANTHROPIC_API_KEY
    scheme: api_key
  - name: registry-vaults
    resolver: http
    url_template: "http://registry-internal:8081/internal/v1/workspaces/{scope.workspace_id}/sessions/{scope.session_id}/vault-credentials/{credential_id}/resolve"
    bearer_token_file: "${registryInternalTokenPath}"
    timeout_ms: 5000
    cache:
      ttl_secs: 30
      max_entries: 128

destinations:
  anthropic-proxy:
    kind: anthropic
    base_url: "http://fixture:8080"
    credentials:
      vault: anthropic-fixture
  "*":
    kind: mcp
    destination_resolver:
      kind: http
      url_template: "http://registry-internal:8081/internal/v1/workspaces/{scope.workspace_id}/sessions/{scope.session_id}/mcp-destination/resolve"
      bearer_token_file: "${registryInternalTokenPath}"
      timeout_ms: 5000
      egress_policy:
        allowed_private_hosts: ["fixture"]
        dns_timeout_ms: 2000
        connect_timeout_ms: 5000
        response_timeout_ms: 30000
    credentials:
      vault: registry-vaults

routes:
  - name: llm-messages
    match:
      path: "/v1/messages"
    idempotency_safe: true
    strategy:
      mode: fallback
      targets:
        - destination: anthropic-proxy
  - name: mcp-forward
    match:
      path: "/v1/mcp"
    idempotency_safe: true
    strategy:
      mode: fallback
      targets:
        - destination: "*"

plugins:
  authorizers:
    - name: session-scope-allowlist
      kind: yaml_acl
      required: true
      failure_mode: deny
      rules:
        - effect: allow
          action: invoke
          resource:
            kind: model
            provider: anthropic
          conditions:
            - resource_name_in_scope_list:
                scope: llm_models
        - effect: allow
          action: invoke
          resource:
            kind: route
          conditions:
            - resource_name_in_scope_list:
                scope: llm_routes
        - effect: allow
          action: mcp
          resource:
            kind: mcp_server
            header: X-Orca-Backend
          conditions:
            - header_in_scope_list:
                header: X-Orca-Backend
                scope: mcp_server_names
            - header_in_scope_list:
                header: X-Orca-Credential-Id
                scope: credential_ids
                if_missing: allow
  audit_sinks:
    - name: kind-helm-audit
      kind: kafka
      required: true
      failure_mode: deny
      brokers: "kafka:9092"
      topic_template: "orca.{scope.workspace_id}.audit.ai-gateway"
      client_id: "kind-helm-e2e"
      message_timeout_ms: 5000
  usage_sinks:
    - name: registry-session-usage
      kind: registry
      base_url: "http://registry-internal:8081"
      bearer_token_file: "${registryInternalTokenPath}"
      timeout_ms: 3000
      buffer: 10000
`;
}

function managedValues(
  pair: KeyPair,
  keyVersion: string,
  heartbeatMs: string,
  idleTimeoutMs: string,
): string {
  return `fullnameOverride: managed

images:
  registry:
    repository: orca-registry-service-ts-kind
    tag: e2e
    pullPolicy: IfNotPresent
  harness:
    repository: orca-harness-server-kind
    tag: e2e
    pullPolicy: IfNotPresent

secrets:
  values:
    databaseUrl: postgres://orca:orca@postgres:5432/registry
    transcriptStoreDatabaseUrl: postgres://orca:orca@postgres:5432/transcriptstore
    filestoreDatabaseUrl: postgres://orca:orca@postgres:5432/filestore
    memorystoreDatabaseUrl: postgres://orca:orca@postgres:5432/memorystore
    sessionJwtPrivateKeyPem: |
${yamlBlock(pair.privateKey, 6)}
    sessionJwtPublicKeyPem: |
${yamlBlock(pair.publicKey, 6)}
    anthropicApiKey: fixture-only-not-a-real-anthropic-key
    s3AccessKeyId: minioadmin
    s3SecretAccessKey: minioadmin

transcriptStore:
  backend: kafka
  kafka:
    brokers: kafka:9092
    connectionMode: plaintext
    topicRediscoverIntervalMs: "500"

objectStorage:
  endpoint: http://rustfs:9000
  forcePathStyle: true
  bucket: orca-files
  region: us-east-1
  keyPrefix: managed-agents/
  stsRoleArn: ""
  allowStaticCredentials: true

sessionJwt:
  issuer: orca-registry
  audience: ai-gateway
  ttlSecs: "3600"
  llmRoutes: [llm-messages]
  llmModels: [claude-sonnet-4-5-20250929]
  publicKeySecret:
    name: managed-secrets
    key: SESSION_JWT_PUBLIC_KEY_PEM

secretStore:
  mode: kubernetes

internalAuth:
  mode: kubernetes_service_account
  audience: orca-registry-internal
  tokenExpirationSeconds: 600

registry:
  replicaCount: 1
  nameOverride: registry
  podAnnotations:
    ${sessionJwtVersionAnnotation}: "${keyVersion}"
  sse:
    heartbeatMs: "${heartbeatMs}"
  resources:
    requests:
      cpu: 100m
      memory: 192Mi
    limits:
      memory: 512Mi

harness:
  replicaCount: 1
  nameOverride: harness
  podAnnotations:
    ${sessionJwtVersionAnnotation}: "${keyVersion}"
  aiGatewayUrl: http://gateway:8080/v1/mcp
  anthropicBaseUrl: http://fixture:8080
  anthropicModelDefault: claude-sonnet-4-5-20250929
  sandboxRuntime: in-memory
  nodeEnv: test
  sessionIdleTimeoutMs: "${idleTimeoutMs}"
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      memory: 768Mi

aiGateway:
  enabled: false
  registryUsage:
    enabled: true
  serviceAccount:
    create: false
    name: gateway

toolset:
  enabled: false

migrations:
  registry:
    initContainer:
      enabled: true
    job:
      enabled: false

tests:
  image:
    repository: busybox
    tag: "1.36"
    pullPolicy: IfNotPresent
`;
}

function standaloneGatewayValues(marker: string, keyVersion: string): string {
  const config = gatewayConfig(marker);
  return `fullnameOverride: gateway
replicaCount: 1

image:
  repository: orca-ai-gateway-kind
  tag: e2e
  pullPolicy: IfNotPresent

serviceAccount:
  create: true
  name: gateway

podAnnotations:
  ${sessionJwtVersionAnnotation}: "${keyVersion}"

service:
  port: 8080
  adminPort: 9099

pdb:
  enabled: false
topologySpreadConstraints: []
podAntiAffinity:
  type: none
terminationGracePeriodSeconds: 10
lifecycle: {}
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    memory: 512Mi

env:
  - name: ANTHROPIC_API_KEY
    value: kind-helm-fixture-key

config:
  source: file
  file:
    content: |
${yamlBlock(config, 6)}

extraVolumes:
  - name: session-jwt-public-key
    secret:
      secretName: ${managedSecret}
      items:
        - key: SESSION_JWT_PUBLIC_KEY_PEM
          path: public.pem
  - name: registry-internal-token
    projected:
      defaultMode: 288
      sources:
        - serviceAccountToken:
            path: token
            audience: orca-registry-internal
            expirationSeconds: 600
extraVolumeMounts:
  - name: session-jwt-public-key
    mountPath: ${gatewayPublicKeyDirectory}
    readOnly: true
  - name: registry-internal-token
    mountPath: ${registryInternalTokenDirectory}
    readOnly: true
`;
}

async function generateKeyPair(directory: string, name: string): Promise<KeyPair> {
  const privatePath = join(directory, `${name}-private.pem`);
  const publicPath = join(directory, `${name}-public.pem`);
  await command(
    'openssl',
    ['genpkey', '-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:2048', '-out', privatePath],
    {
      quiet: true,
      display: `openssl genpkey -algorithm RSA -out ${privatePath}`,
    },
  );
  await command('openssl', ['pkey', '-in', privatePath, '-pubout', '-out', publicPath], {
    quiet: true,
    display: `openssl pkey -pubout -out ${publicPath}`,
  });
  return {
    privateKey: await readFile(privatePath, 'utf8'),
    publicKey: await readFile(publicPath, 'utf8'),
  };
}

async function secretValue(secret: string, key: string): Promise<string> {
  const result = await kubectl(['get', 'secret', secret, '-o', 'json'], { quiet: true });
  const parsed = record(parseJson(result.stdout, `secret/${secret}`), `secret/${secret}`);
  const data = record(parsed['data'], `secret/${secret}.data`);
  return Buffer.from(stringField(data, key, `secret/${secret}.data`), 'base64').toString('utf8');
}

async function configMapValue(configMap: string, key: string): Promise<string> {
  const result = await kubectl(['get', 'configmap', configMap, '-o', 'json'], { quiet: true });
  const parsed = record(
    parseJson(result.stdout, `configmap/${configMap}`),
    `configmap/${configMap}`,
  );
  const data = record(parsed['data'], `configmap/${configMap}.data`);
  return stringField(data, key, `configmap/${configMap}.data`);
}

async function deploymentTemplateAnnotations(name: string): Promise<Record<string, unknown>> {
  const deployment = await getDeployment(name);
  const spec = record(deployment['spec'], `deployment/${name}.spec`);
  const template = record(spec['template'], `deployment/${name}.spec.template`);
  const metadata = record(template['metadata'], `deployment/${name}.spec.template.metadata`);
  return record(metadata['annotations'], `deployment/${name}.spec.template.metadata.annotations`);
}

async function applicationConfigChecksums(): Promise<ApplicationConfigChecksums> {
  const [registryAnnotations, harnessAnnotations, gatewayAnnotations] = await Promise.all([
    deploymentTemplateAnnotations(registryDeployment),
    deploymentTemplateAnnotations(harnessDeployment),
    deploymentTemplateAnnotations(gatewayDeployment),
  ]);
  return {
    gateway: stringField(gatewayAnnotations, 'checksum/config', 'Gateway pod annotations'),
    harness: stringField(harnessAnnotations, 'checksum/config', 'Harness pod annotations'),
    registry: stringField(registryAnnotations, 'checksum/config', 'Registry pod annotations'),
  };
}

function assertApplicationConfigChecksumsChanged(
  before: ApplicationConfigChecksums,
  after: ApplicationConfigChecksums,
): void {
  assert(before.registry !== after.registry, 'Registry checksum/config did not change');
  assert(before.harness !== after.harness, 'Harness checksum/config did not change');
  assert(before.gateway !== after.gateway, 'Gateway checksum/config did not change');
}

function assertApplicationConfigChecksumsUnchanged(
  before: ApplicationConfigChecksums,
  after: ApplicationConfigChecksums,
): void {
  assert(
    before.registry === after.registry,
    'Registry checksum/config changed during key rotation',
  );
  assert(before.harness === after.harness, 'Harness checksum/config changed during key rotation');
  assert(before.gateway === after.gateway, 'Gateway checksum/config changed during key rotation');
}

async function podUidForDeployment(name: string): Promise<string> {
  const deployment = await getDeployment(name);
  const spec = record(deployment['spec'], `deployment/${name}.spec`);
  const selector = record(spec['selector'], `deployment/${name}.spec.selector`);
  const labels = record(selector['matchLabels'], `deployment/${name}.spec.selector.matchLabels`);
  const labelSelector = Object.entries(labels)
    .map(([key, value]) => {
      assert(typeof value === 'string', `deployment/${name} selector ${key} is not a string`);
      return `${key}=${value}`;
    })
    .join(',');
  assert(labelSelector.length > 0, `deployment/${name} has no selector labels`);

  const result = await kubectl(['get', 'pods', '-l', labelSelector, '-o', 'json'], { quiet: true });
  const pods = record(
    parseJson(result.stdout, `pods for deployment/${name}`),
    `pods for deployment/${name}`,
  );
  const livePods: Record<string, unknown>[] = [];
  for (const [index, item] of arrayField(pods, 'items', `pods for deployment/${name}`).entries()) {
    const pod = record(item, `pod ${index} for deployment/${name}`);
    const metadata = record(pod['metadata'], `pod ${index} metadata for deployment/${name}`);
    if (metadata['deletionTimestamp'] === undefined || metadata['deletionTimestamp'] === null)
      livePods.push(pod);
  }
  assert(livePods.length === 1, `expected one live pod for deployment/${name}`);
  const pod = livePods[0];
  assert(pod !== undefined, `live pod for deployment/${name} is missing`);
  return stringField(
    record(pod['metadata'], `pod metadata for deployment/${name}`),
    'uid',
    `pod for deployment/${name}`,
  );
}

async function applicationPodUids(): Promise<ApplicationPodUids> {
  const [registry, harness, gateway] = await Promise.all([
    podUidForDeployment(registryDeployment),
    podUidForDeployment(harnessDeployment),
    podUidForDeployment(gatewayDeployment),
  ]);
  return { gateway, harness, registry };
}

function assertApplicationPodsRolled(
  before: ApplicationPodUids,
  after: ApplicationPodUids,
  context: string,
): void {
  assert(after.registry !== before.registry, `Registry pod UID did not change during ${context}`);
  assert(after.harness !== before.harness, `Harness pod UID did not change during ${context}`);
  assert(after.gateway !== before.gateway, `Gateway pod UID did not change during ${context}`);
}

async function deploymentEnvValue(deployment: string, variable: string): Promise<string> {
  const script = `process.stdout.write(process.env[${JSON.stringify(variable)}] ?? '')`;
  const result = await kubectl(
    ['exec', `deployment/${deployment}`, '--', 'node', '--eval', script],
    {
      quiet: true,
      display: `kubectl exec deployment/${deployment} -- read ${variable}`,
    },
  );
  return result.stdout.trim();
}

async function restartKafkaAndAssertTranscript(
  resources: TurnResources,
  marker: string,
): Promise<void> {
  const moduleCheckScript = `
await import('kafkajs');
await import('@orca/transcript-store');
process.stdout.write('TRANSCRIPT_MODULES_OK\\n');
`;
  try {
    const result = await kubectl(
      [
        'exec',
        `deployment/${harnessDeployment}`,
        '--',
        'node',
        '--input-type=module',
        '--eval',
        moduleCheckScript,
      ],
      {
        display: 'kubectl exec deployment/harness -- verify transcript modules',
      },
    );
    assert(
      result.stdout.trim() === 'TRANSCRIPT_MODULES_OK',
      'Harness transcript module check returned unexpected output',
    );
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(
      'Harness image must bundle resolvable "kafkajs" and "@orca/transcript-store" modules ' +
        `for Kafka transcript verification: ${detail}`,
    );
  }

  const kafkaUidBefore = await podUidForDeployment('kafka');
  await kubectl(['rollout', 'restart', 'deployment/kafka']);
  await waitForDeployment('kafka');
  const kafkaUidAfter = await podUidForDeployment('kafka');
  assert(kafkaUidAfter !== kafkaUidBefore, 'Kafka pod UID did not change during restart');

  const script = `
import { Kafka, logLevel } from 'kafkajs';
import { KafkaTranscriptStore } from '@orca/transcript-store';

const store = new KafkaTranscriptStore({
  kafka: new Kafka({
    clientId: 'kind-helm-transcript-check',
    brokers: ['kafka:9092'],
    logLevel: logLevel.NOTHING,
  }),
});
let found = false;
try {
  for await (const event of store.read(
    ${JSON.stringify(resources.workspaceId)},
    ${JSON.stringify(resources.sessionId)},
    { fromCursor: '', maxEvents: 1000, subpath: '*' },
  )) {
    if (Buffer.from(event.payload).toString('utf8').includes(${JSON.stringify(marker)})) {
      found = true;
      break;
    }
  }
} finally {
  await store.close();
}
if (!found) process.exitCode = 2;
else process.stdout.write('KAFKA_TRANSCRIPT_OK');
`;

  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    try {
      const result = await kubectl(
        [
          'exec',
          `deployment/${harnessDeployment}`,
          '--',
          'node',
          '--input-type=module',
          '--eval',
          script,
        ],
        {
          quiet: true,
          display: 'kubectl exec deployment/harness -- verify Kafka transcript marker',
        },
      );
      if (result.stdout.trim() === 'KAFKA_TRANSCRIPT_OK') return;
    } catch {
      // Harness and Gateway consumers may still be reconnecting to the broker.
    }
    await sleep(2_000);
  }
  throw new Error(`Kafka transcript did not retain ${marker} after broker restart`);
}

async function helmValues(release: string): Promise<Record<string, unknown>> {
  const result = await command(
    'helm',
    [
      'get',
      'values',
      release,
      '--kubeconfig',
      kubeconfigPath,
      '--kube-context',
      kubeContext,
      '--namespace',
      namespace,
      '--all',
      '--output',
      'json',
    ],
    { quiet: true },
  );
  return record(
    parseJson(result.stdout, `Helm values for ${release}`),
    `Helm values for ${release}`,
  );
}

async function assertHelmRevision(release: string, expectedRevision: number): Promise<void> {
  const result = await command(
    'helm',
    [
      'history',
      release,
      '--kubeconfig',
      kubeconfigPath,
      '--kube-context',
      kubeContext,
      '--namespace',
      namespace,
      '--output',
      'json',
    ],
    {
      quiet: true,
    },
  );
  const history = array(
    parseJson(result.stdout, `Helm history for ${release}`),
    `Helm history for ${release}`,
  );
  assert(history.length > 0, `Helm history for ${release} is empty`);
  const latest = history.at(-1);
  assert(latest !== undefined, `Helm history for ${release} has no latest revision`);
  const revision = record(latest, `Helm history latest for ${release}`)['revision'];
  assert(
    Number(revision) === expectedRevision,
    `Helm release ${release} revision is not ${expectedRevision}`,
  );
}

async function assertGatewayTrustAnchor(): Promise<void> {
  const deployment = await getDeployment(gatewayDeployment);
  const spec = record(deployment['spec'], 'gateway deployment.spec');
  const template = record(spec['template'], 'gateway deployment.spec.template');
  const podSpec = record(template['spec'], 'gateway deployment.spec.template.spec');
  const volumes = arrayField(podSpec, 'volumes', 'gateway pod spec');
  const publicKeyVolume = volumes.find(
    (entry) => record(entry, 'gateway volume')['name'] === 'session-jwt-public-key',
  );
  assert(publicKeyVolume !== undefined, 'Gateway public-key volume is missing');
  const secret = record(
    record(publicKeyVolume, 'Gateway public-key volume')['secret'],
    'Gateway public-key secret',
  );
  assert(
    secret['secretName'] === managedSecret,
    'Gateway does not mount managed-secrets as trust anchor',
  );
  const items = arrayField(secret, 'items', 'Gateway public-key Secret items');
  assert(items.length === 1, 'Gateway trust-anchor Secret exposes more than public key');
  const item = items[0];
  assert(item !== undefined, 'Gateway trust-anchor Secret item is missing');
  const publicKeyItem = record(item, 'Gateway public-key Secret item');
  assert(
    publicKeyItem['key'] === 'SESSION_JWT_PUBLIC_KEY_PEM' && publicKeyItem['path'] === 'public.pem',
    'Gateway trust-anchor Secret item is not SESSION_JWT_PUBLIC_KEY_PEM',
  );

  const tokenVolume = volumes.find(
    (entry) => record(entry, 'gateway volume')['name'] === 'registry-internal-token',
  );
  assert(tokenVolume !== undefined, 'Gateway projected Registry token volume is missing');
  const projected = record(
    record(tokenVolume, 'Gateway projected token volume')['projected'],
    'Gateway projected token',
  );
  const tokenSource = arrayField(projected, 'sources', 'Gateway projected token sources').find(
    (entry) => {
      const source = record(entry, 'Gateway projected token source');
      return source['serviceAccountToken'] !== undefined;
    },
  );
  assert(tokenSource !== undefined, 'Gateway projected ServiceAccount token source is missing');
  assert(
    record(
      record(tokenSource, 'Gateway projected token source')['serviceAccountToken'],
      'Gateway ServiceAccount token',
    )['audience'] === 'orca-registry-internal',
    'Gateway projected ServiceAccount token uses wrong audience',
  );

  const containers = arrayField(podSpec, 'containers', 'gateway pod spec');
  const gatewayContainer = containers.find(
    (entry) => record(entry, 'gateway container')['name'] === 'orca-gateway',
  );
  assert(gatewayContainer !== undefined, 'Gateway container is missing');
  const mounts = arrayField(
    record(gatewayContainer, 'gateway container'),
    'volumeMounts',
    'gateway container',
  );
  const publicKeyMount = mounts.find(
    (entry) => record(entry, 'Gateway volume mount')['name'] === 'session-jwt-public-key',
  );
  const tokenMount = mounts.find(
    (entry) => record(entry, 'Gateway volume mount')['name'] === 'registry-internal-token',
  );
  assert(
    publicKeyMount !== undefined &&
      record(publicKeyMount, 'Gateway public-key mount')['mountPath'] === gatewayPublicKeyDirectory,
    'Gateway public key is not mounted below /var/run/secrets/orca',
  );
  assert(
    tokenMount !== undefined &&
      record(tokenMount, 'Gateway token mount')['mountPath'] === registryInternalTokenDirectory,
    'Gateway Registry token is not mounted below /var/run/secrets/orca',
  );

  const staleSessionSecretExists = await kubectl(['get', 'secret', 'session-jwt'], { quiet: true })
    .then(() => true)
    .catch(() => false);
  assert(!staleSessionSecretExists, 'Gateway must not use an extra session-jwt Secret');
}

async function assertManagedTopology(): Promise<void> {
  const values = await helmValues(managedRelease);
  const aiGateway = record(values['aiGateway'], 'managed Helm aiGateway values');
  assert(aiGateway['enabled'] === false, 'managed Helm release has aiGateway.enabled=true');

  const names = await deploymentNames();
  assert(
    !names.includes('managed-ai-gateway'),
    'managed release rendered managed-ai-gateway Deployment',
  );

  const gateway = await getDeployment(gatewayDeployment);
  const metadata = record(gateway['metadata'], 'gateway Deployment metadata');
  const annotations = record(metadata['annotations'], 'gateway Deployment annotations');
  assert(
    annotations['meta.helm.sh/release-name'] === gatewayRelease &&
      annotations['meta.helm.sh/release-namespace'] === namespace,
    'Gateway Deployment is not owned by standalone gateway Helm release',
  );
  await assertGatewayTrustAnchor();
}

async function assertKeyVersionAnnotations(expectedVersion: string): Promise<void> {
  for (const deployment of [registryDeployment, harnessDeployment, gatewayDeployment]) {
    const annotations = await deploymentTemplateAnnotations(deployment);
    assert(
      annotations[sessionJwtVersionAnnotation] === expectedVersion,
      `deployment/${deployment} session-JWT key-version annotation is not ${expectedVersion}`,
    );
  }
}

async function assertGatewayConfig(marker: string): Promise<void> {
  const config = await configMapValue(gatewayConfigMap, 'config.yaml');
  assert(
    config.includes(`kind-helm-config-marker: ${marker}`),
    'Gateway config marker did not update',
  );
  assert(
    config.includes(`static_public_key_pem: "${gatewayPublicKeyPath}"`),
    'Gateway uses wrong JWT public-key path',
  );
  assert(
    config.includes(`bearer_token_file: "${registryInternalTokenPath}"`),
    'Gateway uses wrong projected-token path',
  );
  assert(
    config.includes('\n      ttl_secs: 30\n') && !config.includes('default_ttl_secs'),
    'Gateway config lacks modern cache.ttl_secs',
  );
  assert(
    config.includes('allowed_private_hosts: ["fixture"]'),
    'Gateway dynamic resolver does not allow fixture host',
  );
  assert(
    config.includes('kind: kafka') && config.includes('required: true'),
    'Gateway Kafka audit sink is not required',
  );
  assert(
    config.includes('kind: registry') &&
      config.includes('base_url: "http://registry-internal:8081"'),
    'Gateway Registry usage sink is not configured',
  );
  assert(
    !config.includes('/tmp/session-jwt') && !config.includes('/tmp/registry-internal-token'),
    'Gateway overlaps chart /tmp mount',
  );
}

function decodeSessionJwt(token: string): Record<string, unknown> {
  const parts = token.split('.');
  const payload = parts[1];
  assert(typeof payload === 'string' && payload.length > 0, 'session JWT has no payload');
  return record(
    parseJson(Buffer.from(payload, 'base64url').toString('utf8'), 'session JWT payload'),
    'session JWT payload',
  );
}

function assertSessionJwtTtl(token: string, context: string): void {
  const payload = decodeSessionJwt(token);
  const issuedAt = numberField(payload, 'iat', context);
  const expiresAt = numberField(payload, 'exp', context);
  assert(expiresAt - issuedAt >= 3600, `${context} TTL is below 3600 seconds`);
}

function sessionJwtExpiresAt(token: string, context: string): number {
  return numberField(decodeSessionJwt(token), 'exp', context);
}

function assertSessionJwtNotExpired(expiresAt: number, context: string): void {
  assert(
    expiresAt > Math.floor(Date.now() / 1000) + 60,
    `${context} expires too soon to attribute rejection to trust-anchor rotation`,
  );
}

async function bootstrapAdmin(): Promise<BootstrapCredentials> {
  const credentials: BootstrapCredentials = {
    adminKey: `orca_admin_${randomBytes(36).toString('base64url')}`,
    platformKey: `orca_platform_${randomBytes(36).toString('base64url')}`,
  };
  await kubectl(
    [
      'exec',
      `deployment/${registryDeployment}`,
      '--',
      'env',
      `ORCA_BOOTSTRAP_ADMIN_API_KEY=${credentials.adminKey}`,
      `ORCA_BOOTSTRAP_PLATFORM_API_KEY=${credentials.platformKey}`,
      'node',
      'dist/bootstrap-admin.js',
    ],
    {
      quiet: true,
      display:
        'kubectl exec deployment/registry -- node dist/bootstrap-admin.js (test credentials redacted)',
    },
  );
  return credentials;
}

async function seedWorkspaceKey(adminKey: string): Promise<{ id: string; key: string }> {
  const headers = { 'x-api-key': adminKey };
  const workspace = await expectedRequest(registryAdminUrl, '/v1/organizations/workspaces', 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({ name: `Kind Helm E2E ${Date.now()}` }),
  });
  const id = stringField(workspace, 'id', 'workspace');
  const key = await expectedRequest(
    registryAdminUrl,
    `/v1/organizations/workspaces/${id}/api_keys`,
    201,
    {
      method: 'POST',
      headers,
      body: JSON.stringify({ name: 'kind-helm-e2e' }),
    },
  );
  return { id, key: stringField(key, 'key', 'workspace API key') };
}

async function createTurnResources(): Promise<TurnResources> {
  const bootstrap = await bootstrapAdmin();
  const workspace = await seedWorkspaceKey(bootstrap.adminKey);
  const headers = workspaceHeaders(workspace.key);

  const environment = await expectedRequest(registryUrl, '/v1/environments', 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({ name: `kind-helm-env-${Date.now()}`, config: { type: 'cloud' } }),
  });
  const environmentId = stringField(environment, 'id', 'environment');

  const vault = await expectedRequest(registryUrl, '/v1/vaults', 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({ display_name: `kind-helm-vault-${Date.now()}` }),
  });
  const vaultId = stringField(vault, 'id', 'vault');
  const credential = await expectedRequest(registryUrl, `/v1/vaults/${vaultId}/credentials`, 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      display_name: 'Kind Helm MCP bearer',
      auth: {
        type: 'static_bearer',
        token: fixtureMcpBearer,
        mcp_server_url: 'http://fixture:8080/mcp',
      },
    }),
  });
  const credentialId = stringField(credential, 'id', 'credential');

  const memoryContent = `memory-survives-upgrade-${randomBytes(12).toString('hex')}`;
  const memoryStore = await expectedRequest(registryUrl, '/v1/memory_stores', 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({ name: `kind-helm-memory-${Date.now()}` }),
  });
  const memoryStoreId = stringField(memoryStore, 'id', 'memory store');
  const memory = await expectedRequest(
    registryUrl,
    `/v1/memory_stores/${memoryStoreId}/memories?view=full`,
    200,
    {
      method: 'POST',
      headers,
      body: JSON.stringify({ path: '/upgrade.txt', content: memoryContent }),
    },
  );
  const memoryId = stringField(memory, 'id', 'memory');

  const agent = await expectedRequest(registryUrl, '/v1/agents', 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      name: `kind-helm-agent-${Date.now()}`,
      model: { provider: 'anthropic', id: 'claude-sonnet-4-5-20250929' },
      system:
        'Use the kind_e2e remote MCP echo tool exactly once whenever the user supplies a KIND_HELM marker. Then report its result.',
      tools: [
        {
          type: 'mcp_toolset',
          mcp_server_name: 'kind_e2e',
          default_config: { permission_policy: { type: 'always_allow' } },
        },
      ],
      mcp_servers: [{ name: 'kind_e2e', url: 'http://fixture:8080/mcp' }],
      skills: [],
      metadata: { suite: 'kind-helm-e2e' },
    }),
  });
  const agentId = stringField(agent, 'id', 'agent');

  const session = await expectedRequest(registryUrl, '/v1/sessions', 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      environment_id: environmentId,
      agent_id: agentId,
      vault_ids: [vaultId],
      resources: [{ type: 'memory_store', memory_store_id: memoryStoreId }],
    }),
  });
  return {
    agentId,
    credentialId,
    environmentId,
    memoryContent,
    memoryId,
    memoryStoreId,
    sessionId: stringField(session, 'id', 'session'),
    vaultId,
    workspaceId: workspace.id,
    workspaceKey: workspace.key,
  };
}

async function createLlmUsageResources(resources: TurnResources): Promise<LlmUsageResources> {
  const headers = workspaceHeaders(resources.workspaceKey);
  // This fixture deliberately has no guardrail. Registry persists shared
  // runtime usage facts independently of the policies currently attached, so
  // a guardrail added later starts from the authoritative Session total.
  const agent = await expectedRequest(registryUrl, '/v1/agents', 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      name: `kind-helm-usage-agent-${Date.now()}`,
      model: { provider: 'anthropic', id: 'claude-sonnet-4-5-20250929' },
      system: 'Reply briefly.',
      tools: [],
      mcp_servers: [],
      skills: [],
      metadata: {
        harness: 'claude_code',
        mode: 'colocated',
        suite: 'kind-helm-registry-usage-e2e',
      },
    }),
  });
  const session = await expectedRequest(registryUrl, '/v1/sessions', 200, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      environment_id: resources.environmentId,
      agent_id: stringField(agent, 'id', 'LLM usage agent'),
    }),
  });
  return {
    agentId: stringField(agent, 'id', 'LLM usage agent'),
    sessionId: stringField(session, 'id', 'LLM usage session'),
  };
}

async function submitTurn(resources: TurnResources, marker: string): Promise<void> {
  await expectedRequest(registryUrl, `/v1/sessions/${resources.sessionId}/events`, 200, {
    method: 'POST',
    headers: workspaceHeaders(resources.workspaceKey),
    body: JSON.stringify({
      request_id: `kind-helm-${marker}`,
      events: [
        {
          type: 'user.message',
          content: [{ type: 'text', text: `Use the kind_e2e echo tool with marker ${marker}.` }],
        },
      ],
    }),
  });
}

function eventHasMarker(event: unknown, type: string, marker: string): boolean {
  const candidate = record(event, 'session event');
  return candidate['type'] === type && JSON.stringify(candidate).includes(marker);
}

function eventHasSuccessfulMcpToolResult(event: unknown, marker: string): boolean {
  const candidate = record(event, 'session event');
  if (candidate['type'] !== 'agent.mcp_tool_result' || candidate['is_error'] !== false)
    return false;
  const content = candidate['content'];
  if (!Array.isArray(content)) return false;
  return content.some(
    (block) =>
      block !== null &&
      typeof block === 'object' &&
      !Array.isArray(block) &&
      (block as Record<string, unknown>)['type'] === 'text' &&
      (block as Record<string, unknown>)['text'] === `MCP_OK ${marker}`,
  );
}

async function waitForTurn(resources: TurnResources, marker: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const page = await expectedRequest(
      registryUrl,
      `/v1/sessions/${resources.sessionId}/events?limit=1000`,
      200,
      {
        headers: workspaceHeaders(resources.workspaceKey),
      },
    );
    const events = page['data'];
    if (Array.isArray(events)) {
      const failureType = events
        .map((event) => record(event, 'session event')['type'])
        .find(
          (type) =>
            type === 'session.error' ||
            type === 'session.status_error' ||
            type === 'session.setup_failed',
        );
      assert(failureType === undefined, `agent turn emitted ${String(failureType)}`);
      const complete =
        events.some((event) => eventHasMarker(event, 'agent.mcp_tool_use', marker)) &&
        events.some((event) => eventHasSuccessfulMcpToolResult(event, marker)) &&
        events.some((event) => eventHasMarker(event, 'agent.message', `AGENT_OK ${marker}`));
      if (complete) {
        const session = await expectedRequest(
          registryUrl,
          `/v1/sessions/${resources.sessionId}`,
          200,
          {
            headers: workspaceHeaders(resources.workspaceKey),
          },
        );
        if (session['status'] === 'idle') return;
      }
    }
    await sleep(750);
  }
  throw new Error(`timed out waiting for complete idle agent turn for ${marker}`);
}

async function harnessToken(): Promise<string> {
  const result = await kubectl(
    [
      'exec',
      `deployment/${harnessDeployment}`,
      '--',
      'cat',
      '/var/run/secrets/orca/registry-internal/token',
    ],
    {
      quiet: true,
      display: 'kubectl exec deployment/harness -- cat projected Registry token (redacted)',
    },
  );
  const token = result.stdout.trim();
  assert(token.length > 32, 'Harness projected ServiceAccount token is empty');
  return token;
}

async function mintSessionJwt(resources: TurnResources, token: string): Promise<string> {
  const body = await expectedRequest(
    registryInternalUrl,
    `/internal/v1/workspaces/${resources.workspaceId}/sessions/${resources.sessionId}/mint-jwt`,
    200,
    {
      method: 'POST',
      headers: { authorization: `Bearer ${token}` },
      body: JSON.stringify({
        mcp_server_names: ['kind_e2e'],
        vault_ids: [resources.vaultId],
      }),
    },
  );
  return stringField(body, 'token', 'mint-jwt');
}

async function mintLlmSessionJwt(
  resources: TurnResources,
  sessionId: string,
  token: string,
): Promise<string> {
  const body = await expectedRequest(
    registryInternalUrl,
    `/internal/v1/workspaces/${resources.workspaceId}/sessions/${sessionId}/mint-jwt`,
    200,
    {
      method: 'POST',
      headers: { authorization: `Bearer ${token}` },
      body: JSON.stringify({}),
    },
  );
  return stringField(body, 'token', 'LLM mint-jwt');
}

async function preparedExecution(
  resources: TurnResources,
  sessionId: string,
  token: string,
): Promise<Record<string, unknown>> {
  return await expectedRequest(
    registryInternalUrl,
    `/internal/v1/workspaces/${resources.workspaceId}/sessions/${sessionId}/executions:prepare`,
    200,
    {
      method: 'POST',
      headers: { authorization: `Bearer ${token}` },
      body: JSON.stringify({}),
    },
  );
}

async function gatewayLlmRequest(
  resources: TurnResources,
  sessionId: string,
  token: string,
  marker: string,
): Promise<HttpResult> {
  return await request(gatewayUrl, '/v1/messages', {
    method: 'POST',
    headers: {
      authorization: `Bearer ${token}`,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
      'idempotency-key': `kind-helm-usage-${marker}`,
      'x-orca-session-id': sessionId,
      'x-request-id': `kind-helm-usage-${marker}`,
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-5-20250929',
      max_tokens: 64,
      stream: true,
      messages: [{ role: 'user', content: [{ type: 'text', text: marker }] }],
    }),
  });
}

async function waitForExactUsage(
  resources: TurnResources,
  sessionId: string,
  expectedInput: number,
  expectedOutput: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const session = await expectedRequest(registryUrl, `/v1/sessions/${sessionId}`, 200, {
      headers: workspaceHeaders(resources.workspaceKey),
    });
    const usage = record(session['usage'], 'LLM usage session usage');
    if (usage['input_tokens'] === expectedInput && usage['output_tokens'] === expectedOutput)
      return;
    await sleep(500);
  }
  throw new Error(
    `timed out waiting for exact Registry usage input=${expectedInput} output=${expectedOutput}`,
  );
}

async function assertRegistryUsageClosure(
  resources: TurnResources,
  harnessServiceToken: string,
): Promise<void> {
  const separatePrepared = await preparedExecution(
    resources,
    resources.sessionId,
    harnessServiceToken,
  );
  assert(
    record(separatePrepared['session'], 'separate prepared session')['usage_writer'] === 'harness',
    'separate session did not remain Harness-owned',
  );

  const llm = await createLlmUsageResources(resources);
  const prepared = await preparedExecution(resources, llm.sessionId, harnessServiceToken);
  assert(
    record(prepared['session'], 'colocated prepared session')['usage_writer'] === 'ai-gateway',
    'colocated session did not select AI Gateway as usage writer',
  );
  assert(
    Array.isArray(prepared['guardrails']) && prepared['guardrails'].length === 0,
    'guardrail-free usage fixture unexpectedly received a policy',
  );

  const sessionJwt = await mintLlmSessionJwt(resources, llm.sessionId, harnessServiceToken);
  const claims = decodeSessionJwt(sessionJwt);
  assert(
    stringArrayField(claims, 'llm_routes', 'LLM session JWT').includes('llm-messages'),
    'LLM session JWT does not allow the llm-messages route',
  );
  assert(
    stringArrayField(claims, 'llm_models', 'LLM session JWT').includes(
      'claude-sonnet-4-5-20250929',
    ),
    'LLM session JWT does not allow the fixture model',
  );

  const marker = `KIND_HELM_USAGE_${randomBytes(8).toString('hex')}`;
  const response = await gatewayLlmRequest(resources, llm.sessionId, sessionJwt, marker);
  assert(
    response.status === 200,
    `Gateway LLM request returned ${response.status}: ${response.text}`,
  );
  assert(response.text.includes(marker), 'Gateway LLM response did not contain fixture marker');

  await waitForExactUsage(resources, llm.sessionId, 1, 5);
  await sleep(1_000);
  await waitForExactUsage(resources, llm.sessionId, 1, 5);

  const refreshed = await preparedExecution(resources, llm.sessionId, harnessServiceToken);
  assert(
    record(refreshed['guardrail_state'], 'refreshed guardrail state')['total_tokens'] === 6,
    'prepared execution did not refresh the Gateway-written token total',
  );

  const rejectedHarnessWrite = await request(
    registryInternalUrl,
    `/internal/v1/workspaces/${resources.workspaceId}/sessions/${llm.sessionId}/usage`,
    {
      method: 'POST',
      headers: {
        authorization: `Bearer ${harnessServiceToken}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ usage: { input_tokens: 100 } }),
    },
  );
  assert(
    rejectedHarnessWrite.status === 403,
    `Harness usage write returned ${rejectedHarnessWrite.status}, expected 403`,
  );
}

async function gatewayMcpRequest(
  resources: TurnResources,
  token: string,
  requestId: string,
  body: Record<string, unknown>,
  mcpSessionId?: string,
): Promise<HttpResult> {
  const headers: Record<string, string> = {
    accept: 'application/json, text/event-stream',
    authorization: `Bearer ${token}`,
    'content-type': 'application/json',
    'idempotency-key': requestId,
    'x-orca-backend': 'kind_e2e',
    'x-orca-session-id': resources.sessionId,
    'x-request-id': requestId,
  };
  if (mcpSessionId !== undefined) headers['mcp-session-id'] = mcpSessionId;
  return await request(gatewayUrl, '/v1/mcp', {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
}

async function initializeGatewayMcp(
  resources: TurnResources,
  token: string,
  requestId: string,
): Promise<string> {
  const result = await gatewayMcpRequest(resources, token, requestId, {
    jsonrpc: '2.0',
    id: 1,
    method: 'initialize',
    params: {
      protocolVersion: '2025-06-18',
      capabilities: {},
      clientInfo: { name: 'kind-helm-e2e', version: '1.0.0' },
    },
  });
  assert(result.status === 200, `Gateway MCP initialize returned ${result.status}`);
  const sessionId = result.headers.get('mcp-session-id')?.trim() ?? '';
  assert(sessionId.length > 0, 'Gateway MCP initialize returned no mcp-session-id');
  return sessionId;
}

async function gatewayMcpSequence(
  resources: TurnResources,
  token: string,
  requestIdPrefix: string,
): Promise<string> {
  const sessionId = await initializeGatewayMcp(resources, token, `${requestIdPrefix}-initialize`);
  const initialized = await gatewayMcpRequest(
    resources,
    token,
    `${requestIdPrefix}-initialized`,
    { jsonrpc: '2.0', method: 'notifications/initialized' },
    sessionId,
  );
  assert(
    initialized.status === 202,
    `Gateway MCP initialized notification returned ${initialized.status}`,
  );
  const tools = await gatewayMcpRequest(
    resources,
    token,
    `${requestIdPrefix}-tools-list`,
    { jsonrpc: '2.0', id: 2, method: 'tools/list' },
    sessionId,
  );
  assert(tools.status === 200, `Gateway MCP tools/list returned ${tools.status}`);
  return sessionId;
}

async function assertFixtureTraffic(markers: string[]): Promise<void> {
  const summary = await expectedRequest(fixtureUrl, '/captured', 200);
  const anthropic = record(summary['anthropic'], 'fixture anthropic summary');
  const mcp = record(summary['mcp'], 'fixture MCP summary');
  assert(
    numberField(anthropic, 'requests', 'fixture anthropic') > 0,
    'fixture received no Anthropic requests',
  );
  assert(
    numberField(anthropic, 'streaming', 'fixture anthropic') > 0,
    'fixture received no streaming Anthropic requests',
  );
  assert(
    numberField(anthropic, 'toolUseRounds', 'fixture anthropic') > 0,
    'fixture received no Anthropic tool-use round',
  );
  assert(
    numberField(anthropic, 'toolResultRounds', 'fixture anthropic') > 0,
    'fixture received no Anthropic tool-result round',
  );
  assert(
    numberField(mcp, 'authorized', 'fixture MCP') > 0,
    'fixture received no authorized MCP request',
  );
  assert(
    numberField(mcp, 'unauthorized', 'fixture MCP') === 0,
    'fixture received unauthorized MCP traffic',
  );
  assert(
    numberField(mcp, 'validAccepts', 'fixture MCP') > 0,
    'fixture received no valid Streamable HTTP Accept header',
  );
  assert(
    numberField(mcp, 'invalidAccepts', 'fixture MCP') === 0,
    'fixture received an invalid Streamable HTTP Accept header',
  );
  assert(
    numberField(mcp, 'validSessionIds', 'fixture MCP') > 0,
    'fixture received no valid MCP session ID',
  );
  assert(
    numberField(mcp, 'missingSessionIds', 'fixture MCP') === 0,
    'fixture received an MCP request without a session ID',
  );
  assert(
    numberField(mcp, 'invalidSessionIds', 'fixture MCP') === 0,
    'fixture received an invalid MCP session ID',
  );
  const methods = stringArrayField(mcp, 'methods', 'fixture MCP');
  for (const method of ['initialize', 'notifications/initialized', 'tools/list', 'tools/call']) {
    assert(methods.includes(method), `fixture did not capture ${method}`);
  }
  const anthropicMarkers = stringArrayField(anthropic, 'markers', 'fixture anthropic');
  const mcpMarkers = stringArrayField(mcp, 'markers', 'fixture MCP');
  for (const marker of markers) {
    assert(anthropicMarkers.includes(marker), `fixture Anthropic summary is missing ${marker}`);
    assert(mcpMarkers.includes(marker), `fixture MCP summary is missing ${marker}`);
  }
}

async function assertPersistence(resources: TurnResources, marker: string): Promise<void> {
  const headers = workspaceHeaders(resources.workspaceKey);
  const agent = await expectedRequest(registryUrl, `/v1/agents/${resources.agentId}`, 200, {
    headers,
  });
  assert(agent['id'] === resources.agentId, 'Agent did not survive Helm upgrade');
  await expectedRequest(registryUrl, `/v1/environments/${resources.environmentId}`, 200, {
    headers,
  });
  await expectedRequest(
    registryUrl,
    `/v1/vaults/${resources.vaultId}/credentials/${resources.credentialId}`,
    200,
    { headers },
  );
  const session = await expectedRequest(registryUrl, `/v1/sessions/${resources.sessionId}`, 200, {
    headers,
  });
  assert(
    session['id'] === resources.sessionId && session['status'] === 'idle',
    'Session did not survive Helm upgrade idle',
  );
  const events = await expectedRequest(
    registryUrl,
    `/v1/sessions/${resources.sessionId}/events?limit=1000`,
    200,
    {
      headers,
    },
  );
  assert(
    arrayField(events, 'data', 'persisted session events').some((event) =>
      JSON.stringify(event).includes(marker),
    ),
    'Session transcript content did not survive Helm upgrade',
  );
  const memory = await expectedRequest(
    registryUrl,
    `/v1/memory_stores/${resources.memoryStoreId}/memories/${resources.memoryId}?view=full`,
    200,
    { headers },
  );
  assert(
    memory['content'] === resources.memoryContent,
    'MemoryStore content changed during Helm upgrade',
  );
}

async function assertFreshTarget(): Promise<void> {
  const accessArgs = [
    '--kubeconfig',
    kubeconfigPath,
    '--context',
    kubeContext,
    '--request-timeout=30s',
  ];
  const existingNamespace = await command(
    'kubectl',
    [...accessArgs, 'get', 'namespace', namespace, '--ignore-not-found', '--output=name'],
    { quiet: true },
  );
  assert(
    existingNamespace.stdout.trim() === '',
    `namespace ${namespace} already exists; use a fresh disposable Kind cluster`,
  );

  const existingPersistentVolumes = await command(
    'kubectl',
    [
      ...accessArgs,
      'get',
      'persistentvolume',
      'kind-helm-e2e-postgres',
      'kind-helm-e2e-kafka',
      'kind-helm-e2e-rustfs',
      '--ignore-not-found',
      '--output=name',
    ],
    { quiet: true },
  );
  assert(
    existingPersistentVolumes.stdout.trim() === '',
    'Kind Helm E2E persistent volumes already exist; use a fresh disposable Kind cluster',
  );

  const existingBinding = await command(
    'kubectl',
    [
      ...accessArgs,
      'get',
      'clusterrolebinding',
      `managed-${namespace}-registry-tokenreview`,
      '--ignore-not-found',
      '--output=name',
    ],
    { quiet: true },
  );
  assert(
    existingBinding.stdout.trim() === '',
    'managed Registry TokenReview binding already exists; use a fresh disposable Kind cluster',
  );
}

async function checkPrerequisites(tempDir: string): Promise<void> {
  assert(gatewayChartDir.length > 0, 'ORCA_GATEWAY_CHART_DIR is required');
  assert(Number.isFinite(timeoutMs) && timeoutMs > 0, 'KIND_HELM_E2E_TIMEOUT_MS must be positive');
  for (const [binary, args] of [
    ['helm', ['version', '--short']],
    ['kubectl', ['version', '--client=true', '--output=yaml']],
    ['kind', ['version']],
    ['openssl', ['version']],
  ] as const) {
    await command(binary, [...args], { quiet: true });
  }
  await Promise.all([
    readFile(join(managedChartDir, 'Chart.yaml'), 'utf8'),
    readFile(join(gatewayChartDir, 'Chart.yaml'), 'utf8'),
    readFile(infraPath, 'utf8'),
  ]);
  const context = (
    await command('kubectl', ['config', 'current-context'], { quiet: true })
  ).stdout.trim();
  assert(
    context.startsWith('kind-'),
    `current kube-context must be Kind, got ${context || '(empty)'}`,
  );
  const clusterName = context.slice('kind-'.length);
  assert(clusterName.length > 0, 'current Kind context has no cluster name');
  const clusters = (await command('kind', ['get', 'clusters'], { quiet: true })).stdout
    .split(/\r?\n/)
    .map((entry) => entry.trim())
    .filter((entry) => entry !== '');
  assert(
    clusters.includes(clusterName),
    `current context ${context} does not match a running Kind cluster`,
  );

  const kindKubeconfig = await command('kind', ['get', 'kubeconfig', '--name', clusterName], {
    quiet: true,
  });
  kubeContext = context;
  kubeconfigPath = join(tempDir, 'kind-kubeconfig.yaml');
  await writeFile(kubeconfigPath, kindKubeconfig.stdout, { mode: 0o600 });
  const isolatedContext = (
    await command('kubectl', ['--kubeconfig', kubeconfigPath, 'config', 'current-context'], {
      quiet: true,
    })
  ).stdout.trim();
  assert(isolatedContext === kubeContext, 'generated Kind kubeconfig has an unexpected context');

  const nodeList = await command(
    'kubectl',
    [
      '--kubeconfig',
      kubeconfigPath,
      '--context',
      kubeContext,
      '--request-timeout=30s',
      'get',
      'nodes',
      '--output=json',
    ],
    { quiet: true },
  );
  const nodes = arrayField(
    record(parseJson(nodeList.stdout, 'Kind node list'), 'Kind node list'),
    'items',
    'Kind node list',
  );
  assert(
    nodes.length === 1,
    `Kind Helm E2E requires exactly one node because hostPath persistence is node-local; got ${nodes.length}`,
  );
}

async function main(): Promise<void> {
  const tempDir = await mkdtemp(join(tmpdir(), 'orca-kind-helm-e2e-'));
  try {
    await checkPrerequisites(tempDir);
    await assertFreshTarget();
    const namespaceManifest = await command(
      'kubectl',
      [
        '--kubeconfig',
        kubeconfigPath,
        '--context',
        kubeContext,
        '--request-timeout=30s',
        'create',
        'namespace',
        namespace,
        '--dry-run=client',
        '--output=yaml',
      ],
      { quiet: true },
    );
    await command(
      'kubectl',
      [
        '--kubeconfig',
        kubeconfigPath,
        '--context',
        kubeContext,
        '--request-timeout=30s',
        'apply',
        '-f',
        '-',
      ],
      {
        input: namespaceManifest.stdout,
        quiet: true,
      },
    );
    await applyManifest(infraPath);
    await waitForAllDeployments();
    await kubectl(['wait', '--for=condition=complete', 'job/rustfs-bootstrap', '--timeout=5m'], {
      timeoutMs: 330_000,
    });

    const initialPair = await generateKeyPair(tempDir, 'initial');
    const initialManagedValues = await writeValues(
      tempDir,
      'managed-initial.yaml',
      managedValues(initialPair, 'initial', '15000', '300000'),
    );
    const initialGatewayValues = await writeValues(
      tempDir,
      'gateway-initial.yaml',
      standaloneGatewayValues('initial', 'initial'),
    );

    await helmUpgrade(managedRelease, managedChartDir, initialManagedValues);
    await helmUpgrade(gatewayRelease, gatewayChartDir, initialGatewayValues);
    await waitForAllDeployments();
    await helmTest(managedRelease);
    // Pinned Gateway chart deletes successful test Pods before Helm can fetch `--logs`.
    await helmTest(gatewayRelease, { logs: false });
    await assertManagedTopology();
    await assertKeyVersionAnnotations('initial');
    await assertGatewayConfig('initial');
    assert(
      (await configMapValue(managedRegistryConfigMap, 'SESSION_JWT_TTL_SECS')) === '3600',
      'managed sessionJwt.ttlSecs is below 3600',
    );
    assert(
      (await configMapValue(managedRegistryConfigMap, 'AI_GATEWAY_REGISTRY_USAGE_ENABLED')) ===
        'true',
      'managed Registry usage authority is not enabled for E2E',
    );
    assert(
      (await secretValue(managedSecret, 'SESSION_JWT_PUBLIC_KEY_PEM')) === initialPair.publicKey,
      'managed initial session-JWT public key is wrong',
    );

    await startPortForwards();
    const resources = await createTurnResources();
    const firstMarker = `KIND_HELM_INITIAL_${randomBytes(8).toString('hex')}`;
    await submitTurn(resources, firstMarker);
    await waitForTurn(resources, firstMarker);

    const initialHarnessToken = await harnessToken();
    const oldSessionJwt = await mintSessionJwt(resources, initialHarnessToken);
    assertSessionJwtTtl(oldSessionJwt, 'initial session JWT');
    const oldSessionJwtExpiration = sessionJwtExpiresAt(oldSessionJwt, 'initial session JWT');
    await gatewayMcpSequence(resources, oldSessionJwt, `direct-before-${Date.now()}`);
    await assertFixtureTraffic([firstMarker]);

    const initialPodUids = await applicationPodUids();
    const initialConfigChecksums = await applicationConfigChecksums();
    const configManagedValues = await writeValues(
      tempDir,
      'managed-config-only.yaml',
      managedValues(initialPair, 'initial', '7000', '240000'),
    );
    const configGatewayValues = await writeValues(
      tempDir,
      'gateway-config-only.yaml',
      standaloneGatewayValues('config-only', 'initial'),
    );

    // Exercise ConfigMap checksums independently: key material and explicit
    // key-version annotations remain unchanged for every workload.
    await helmUpgrade(managedRelease, managedChartDir, configManagedValues);
    await helmUpgrade(gatewayRelease, gatewayChartDir, configGatewayValues);
    await waitForAllDeployments();
    await stopPortForwards();
    await startPortForwards();
    await helmTest(managedRelease);
    await helmTest(gatewayRelease, { logs: false });

    const configPodUids = await applicationPodUids();
    const configChecksums = await applicationConfigChecksums();
    assertApplicationPodsRolled(initialPodUids, configPodUids, 'config-only upgrade');
    assertApplicationConfigChecksumsChanged(initialConfigChecksums, configChecksums);
    await assertManagedTopology();
    await assertKeyVersionAnnotations('initial');
    await assertGatewayConfig('config-only');
    assert(
      (await configMapValue(managedRegistryConfigMap, 'SSE_HEARTBEAT_MS')) === '7000',
      'managed Registry heartbeat did not update',
    );
    assert(
      (await configMapValue(managedHarnessConfigMap, 'SESSION_IDLE_TIMEOUT_MS')) === '240000',
      'managed Harness idle timeout did not update',
    );
    assert(
      (await deploymentEnvValue(registryDeployment, 'SSE_HEARTBEAT_MS')) === '7000',
      'Registry process did not load updated SSE_HEARTBEAT_MS',
    );
    assert(
      (await deploymentEnvValue(harnessDeployment, 'SESSION_IDLE_TIMEOUT_MS')) === '240000',
      'Harness process did not load updated SESSION_IDLE_TIMEOUT_MS',
    );
    assert(
      (await secretValue(managedSecret, 'SESSION_JWT_PUBLIC_KEY_PEM')) === initialPair.publicKey,
      'config-only upgrade changed the session-JWT public key',
    );
    await assertHelmRevision(managedRelease, 2);
    await assertHelmRevision(gatewayRelease, 2);
    const preRotationMcpSessionId = await gatewayMcpSequence(
      resources,
      oldSessionJwt,
      `direct-config-after-${Date.now()}`,
    );

    const rotatedPair = await generateKeyPair(tempDir, 'rotated');
    const rotatedManagedValues = await writeValues(
      tempDir,
      'managed-rotated.yaml',
      managedValues(rotatedPair, 'rotated', '7000', '240000'),
    );
    const rotatedGatewayValues = await writeValues(
      tempDir,
      'gateway-rotated.yaml',
      standaloneGatewayValues('config-only', 'rotated'),
    );

    // Keep ConfigMaps byte-identical during key rotation. No client traffic
    // between releases: Registry rotates first, then Gateway adopts its key.
    await helmUpgrade(managedRelease, managedChartDir, rotatedManagedValues);
    await helmUpgrade(gatewayRelease, gatewayChartDir, rotatedGatewayValues);
    await waitForAllDeployments();
    await stopPortForwards();
    await startPortForwards();
    await helmTest(managedRelease);
    await helmTest(gatewayRelease, { logs: false });

    const rotatedPodUids = await applicationPodUids();
    const rotatedConfigChecksums = await applicationConfigChecksums();
    assertApplicationPodsRolled(configPodUids, rotatedPodUids, 'session-JWT key rotation');
    assertApplicationConfigChecksumsUnchanged(configChecksums, rotatedConfigChecksums);
    await assertManagedTopology();
    await assertKeyVersionAnnotations('rotated');
    await assertGatewayConfig('config-only');
    assert(
      (await configMapValue(managedRegistryConfigMap, 'SSE_HEARTBEAT_MS')) === '7000',
      'managed Registry heartbeat changed during key rotation',
    );
    assert(
      (await configMapValue(managedHarnessConfigMap, 'SESSION_IDLE_TIMEOUT_MS')) === '240000',
      'managed Harness idle timeout changed during key rotation',
    );
    assert(
      (await deploymentEnvValue(registryDeployment, 'SSE_HEARTBEAT_MS')) === '7000',
      'Registry process lost updated SSE_HEARTBEAT_MS during key rotation',
    );
    assert(
      (await deploymentEnvValue(harnessDeployment, 'SESSION_IDLE_TIMEOUT_MS')) === '240000',
      'Harness process lost updated SESSION_IDLE_TIMEOUT_MS during key rotation',
    );
    assert(
      (await secretValue(managedSecret, 'SESSION_JWT_PUBLIC_KEY_PEM')) === rotatedPair.publicKey,
      'managed session-JWT public key did not rotate',
    );
    await assertHelmRevision(managedRelease, 3);
    await assertHelmRevision(gatewayRelease, 3);
    await restartKafkaAndAssertTranscript(resources, firstMarker);

    // Assert expiry before interpreting a 401 as trust-anchor rotation.
    assertSessionJwtNotExpired(oldSessionJwtExpiration, 'old session JWT');
    const oldTokenAfterRotation = await gatewayMcpRequest(
      resources,
      oldSessionJwt,
      `direct-old-after-${Date.now()}`,
      { jsonrpc: '2.0', id: 3, method: 'tools/list' },
      preRotationMcpSessionId,
    );
    assert(
      oldTokenAfterRotation.status === 401,
      `old session JWT returned ${oldTokenAfterRotation.status}, expected 401`,
    );

    const rotatedHarnessToken = await harnessToken();
    const newSessionJwt = await mintSessionJwt(resources, rotatedHarnessToken);
    assertSessionJwtTtl(newSessionJwt, 'rotated session JWT');
    await gatewayMcpSequence(resources, newSessionJwt, `direct-new-after-${Date.now()}`);
    await assertRegistryUsageClosure(resources, rotatedHarnessToken);

    await assertPersistence(resources, firstMarker);
    const secondMarker = `KIND_HELM_ROTATED_${randomBytes(8).toString('hex')}`;
    await submitTurn(resources, secondMarker);
    await waitForTurn(resources, secondMarker);
    await assertFixtureTraffic([firstMarker, secondMarker]);

    console.log(
      'Kind Helm E2E passed: turn, rollout, TokenReview, JWT rotation, and Registry usage closure verified.',
    );
  } finally {
    await stopPortForwards();
    await rm(tempDir, { recursive: true, force: true });
  }
}

void main().catch((error: unknown) => {
  console.error(error instanceof Error ? (error.stack ?? error.message) : error);
  process.exitCode = 1;
});
