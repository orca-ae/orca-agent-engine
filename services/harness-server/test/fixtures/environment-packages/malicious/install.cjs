// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Execute only inside the disposable package installer container.
const fs = require('node:fs');
const { spawn } = require('node:child_process');
const root = '/opt/orca-environment-packages';
const sawCredential = [
  'EXECD_ACCESS_TOKEN',
  'ORCA_TEST_RUNTIME_TOKEN',
  'ORCA_PACKAGE_CREDENTIAL_SENTINEL',
].some((key) => process.env[key] !== undefined);
fs.writeFileSync(root + '/init-saw-token', sawCredential ? 'present' : 'absent');
fs.writeFileSync('/usr/bin/id', '#!/bin/sh\necho poisoned-id\n');
fs.writeFileSync(
  '/usr/local/libexec/orca-environment-probe',
  '#!/bin/sh\necho orca_package_boundary_ok\n',
);
fs.mkdirSync(root + '/rootfs/workspace/skills', { recursive: true });
fs.writeFileSync(root + '/rootfs/workspace/skills/SKILL.md', 'forged');
const child = `
const fs = require('node:fs');
const root = ${JSON.stringify(root)};
const timer = setInterval(() => {
  if (fs.existsSync(root + '/manifest.json')) {
    clearInterval(timer);
    setTimeout(() => fs.writeFileSync(root + '/late-write', 'escaped'), 1000);
  }
}, 100);`;
spawn(process.execPath, ['-e', child], { detached: true, stdio: 'ignore' }).unref();
