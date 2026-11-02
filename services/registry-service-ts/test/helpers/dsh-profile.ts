// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { DshProfileFiles } from '@orca/harness-catalog';

export function nativeProfileFiles(): DshProfileFiles {
  const lines = (...values: string[]) => values.join(String.fromCharCode(10));
  return {
    'package.json': JSON.stringify({
      private: true,
      dependencies: { business: '1.0.0' },
      dsh: { profile: { bundles: ['business'] } },
    }),
    'pnpm-workspace.yaml': lines('packages: [.]', 'nodeLinker: hoisted', 'autoInstallPeers: false'),
    'pnpm-lock.yaml': lines(
      'lockfileVersion: "9.0"',
      'settings: {autoInstallPeers: false, excludeLinksFromLockfile: false}',
      'importers:',
      '  .:',
      '    dependencies:',
      '      business: {specifier: 1.0.0, version: 1.0.0}',
      'packages:',
      '  business@1.0.0:',
      '    resolution: {integrity: sha512-YQ==}',
      'snapshots:',
      '  business@1.0.0: {}',
    ),
    'cordis.patch.yml': '[]',
  };
}
