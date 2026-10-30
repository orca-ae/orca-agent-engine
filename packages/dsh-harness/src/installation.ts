// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { createRequire } from 'node:module';
import { basename, dirname, join } from 'node:path';
import { DSH_SDK_VERSION } from './checkpoint.js';

const dependencyPins: Record<string, string> = {
  '@deepseek-ai/dsh': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-llm-pi-ai': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-subagent': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-subagent-spawn-in-process': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-subagent-fork-in-process': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-tool-subagent': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-tool-subagent-control': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-working-directory': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-fs-local': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-session-query': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-session-format-catalog': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-sdk-protocol': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-agent': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-agent-loop': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-llm-deepseek': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-llm': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-session': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-session-projection': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-session-persistence-jsonl': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-system-prompt': DSH_SDK_VERSION,
  '@deepseek-ai/dsh-tools': DSH_SDK_VERSION,
  '@deepseek-ai/cordis': '4.0.5-alpha.1',
  '@deepseek-ai/cordis-plugin-timer': '1.1.7-alpha.1',
  '@earendil-works/pi-ai': '1.1.0',
};

/** Validate the packaged profile and every pinned native runtime dependency before launch. */
export function inspectDshInstallation(): string[] {
  const require = createRequire(import.meta.url);
  const profile = require.resolve('@orca/dsh-harness/profile/orca-managed/package.json');
  const root = dirname(dirname(dirname(profile)));
  const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
  const paths = [root];
  for (const [name, pin] of Object.entries(manifest.dependencies as Record<string, string>)) {
    if (!name.startsWith('@deepseek-ai/') && name !== '@earendil-works/pi-ai') continue;
    const expected = dependencyPins[name];
    if (!expected) throw new Error('Unknown DSH dependency: ' + name);
    // Read the installed manifest even for import-only packages with no manifest export.
    const candidate = require.resolve
      .paths(name)
      ?.map((base) => join(base, name, 'package.json'))
      .find((path) => existsSync(path));
    if (!candidate) throw new Error('Missing installed DSH dependency: ' + name);
    const packagePath = realpathSync(candidate);
    const installed = JSON.parse(readFileSync(packagePath, 'utf8'));
    if (installed.name !== name || installed.version !== expected || pin !== expected)
      throw new Error('Incompatible installed DSH dependency: ' + name);
    paths.push(dirname(packagePath));
    // Frozen pnpm installs resolve transitive plugin imports through the shared store.
    for (let parent = dirname(packagePath); dirname(parent) !== parent; parent = dirname(parent))
      if (basename(parent) === 'node_modules') paths.push(parent);
  }
  const packaged = JSON.parse(readFileSync(profile, 'utf8'));
  if (packaged.name !== 'orca-managed-profile' || packaged.dsh?.profile?.bundles?.length !== 0)
    throw new Error('Incompatible packaged DSH profile');
  return [...new Set(paths)];
}
