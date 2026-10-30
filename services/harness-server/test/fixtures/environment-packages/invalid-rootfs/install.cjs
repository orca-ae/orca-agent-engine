// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Export succeeds in the init container, but its root is an untrusted alias.
const fs = require('node:fs');
fs.mkdirSync('/tmp/orca-invalid-rootfs', { recursive: true });
fs.symlinkSync('/tmp/orca-invalid-rootfs', '/opt/orca-environment-packages/rootfs');
