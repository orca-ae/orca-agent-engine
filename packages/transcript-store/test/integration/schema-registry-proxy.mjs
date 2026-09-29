// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Test-only HTTPS/auth/fault proxy forwarding schemas from the real Schema Registry.
import http from 'node:http';
import https from 'node:https';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export async function registryProxy() {
  const directory = mkdtempSync(join(tmpdir(), 'orca-transcript-avro-'));
  const key = join(directory, 'key.pem');
  const cert = join(directory, 'cert.pem');
  let ca;
  let keyData;
  try {
    execFileSync(
      'openssl',
      [
        'req',
        '-x509',
        '-newkey',
        'rsa:2048',
        '-nodes',
        '-days',
        '1',
        '-subj',
        '/CN=localhost',
        '-addext',
        'subjectAltName=DNS:localhost,IP:127.0.0.1',
        '-keyout',
        key,
        '-out',
        cert,
      ],
      { stdio: 'ignore', timeout: 10000 },
    );
    ca = readFileSync(cert);
    keyData = readFileSync(key);
  } catch (error) {
    rmSync(directory, { recursive: true, force: true });
    throw error;
  }
  const auth = { username: 'fixture', password: 'local-fixture-only' };
  const expected = 'Basic ' + Buffer.from(auth.username + ':' + auth.password).toString('base64');
  const state = { unavailable: false, requests: [], auth, ca };
  const handler = (req, res) => {
    state.requests.push({
      method: req.method,
      path: req.url,
      authenticated: req.headers.authorization === expected,
    });
    if (req.headers.authorization !== expected) {
      res.writeHead(401, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error_code: 401, message: 'fixture authentication required' }));
      return;
    }
    if (state.unavailable) {
      res.writeHead(503, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error_code: 503, message: 'fixture registry outage' }));
      return;
    }
    const upstream = http.request(
      'http://127.0.0.1:18081' + req.url,
      {
        method: req.method,
        headers: { ...req.headers, host: '127.0.0.1:18081' },
        timeout: 5000,
      },
      (response) => {
        res.writeHead(response.statusCode, response.headers);
        response.pipe(res);
      },
    );
    upstream.on('error', () => {
      if (!res.headersSent) res.writeHead(502);
      res.end();
    });
    upstream.on('timeout', () => upstream.destroy());
    res.on('close', () => upstream.destroy());
    req.pipe(upstream);
  };
  const secure = https.createServer({ key: keyData, cert: state.ca }, handler);
  try {
    await new Promise((resolve, reject) => {
      secure.once('error', reject);
      secure.listen(18085, '127.0.0.1', resolve);
    });
  } catch (error) {
    secure.close();
    rmSync(directory, { recursive: true, force: true });
    throw error;
  }
  return {
    ...state,
    state,
    async close() {
      await new Promise((resolve) => {
        secure.close(resolve);
        secure.closeAllConnections();
      });
      rmSync(directory, { recursive: true, force: true });
    },
  };
}
