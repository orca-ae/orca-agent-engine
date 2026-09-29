// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  COPYRIGHT_LINE,
  LEGACY_COPYRIGHT_LINE,
  LICENSE_LINE,
  addHeader,
  commentPrefixFor,
  fixHeader,
  hasHeader,
  shebangPrefixFor,
} from './check-license-headers.mjs';

test('extension-less scripts are recognized by their shebang', () => {
  assert.equal(shebangPrefixFor('#!/bin/bash\nset -e\n'), '#');
  assert.equal(shebangPrefixFor('#!/usr/bin/env bash\n'), '#');
  assert.equal(shebangPrefixFor('#!/usr/bin/env python3\n'), '#');
  assert.equal(shebangPrefixFor('#!/usr/bin/env node\n'), '//');
  assert.equal(shebangPrefixFor('plain text\n'), null);
  assert.equal(shebangPrefixFor('#!/usr/bin/env ruby\n'), null);
});

test('comment prefix follows the file type', () => {
  assert.equal(commentPrefixFor('services/registry-service-ts/src/server.ts'), '//');
  assert.equal(commentPrefixFor('scripts/check-env-docs.mjs'), '//');
  assert.equal(commentPrefixFor('services/proto/orca/v1/transcript_store.proto'), '//');
  assert.equal(commentPrefixFor('.github/scripts/changed-areas.sh'), '#');
  assert.equal(commentPrefixFor('charts/orca-managed-agents/values.yaml'), '#');
  assert.equal(commentPrefixFor('services/harness-server/Dockerfile'), '#');
  assert.equal(commentPrefixFor('services/environment-image/e2b.Dockerfile'), '#');
  assert.equal(commentPrefixFor('Makefile'), '#');
});

test('prose, data and exempt paths are not checked', () => {
  assert.equal(commentPrefixFor('README.md'), null);
  assert.equal(commentPrefixFor('package.json'), null);
  assert.equal(commentPrefixFor('services/registry-service-ts/src/generated/common.ts'), null);
  assert.equal(commentPrefixFor('charts/orca-managed-agents/templates/registry.yaml'), null);
  assert.equal(commentPrefixFor('charts/opensandbox-patches/files/batchsandbox_provider.py'), null);
  assert.equal(commentPrefixFor('.github/ISSUE_TEMPLATE/bug_report.yml'), null);
  assert.equal(commentPrefixFor('packages/skill-store/src/unicode-case-fold-data.ts'), null);
  assert.equal(commentPrefixFor('LICENSE'), null);
});

test('a header is recognized only near the top', () => {
  assert.equal(hasHeader(`// ${COPYRIGHT_LINE}\n// ${LICENSE_LINE}\n\nexport {};\n`), true);
  assert.equal(hasHeader(`// ${LICENSE_LINE}\nexport {};\n`), false);
  const buried = `${'\n'.repeat(20)}// ${COPYRIGHT_LINE}\n// ${LICENSE_LINE}\n`;
  assert.equal(hasHeader(buried), false);
});

test('the header goes after a shebang and keeps one blank line before the code', () => {
  const fixed = addHeader('#!/usr/bin/env node\nimport x from "y";\n', '//');
  assert.equal(
    fixed,
    `#!/usr/bin/env node\n// ${COPYRIGHT_LINE}\n// ${LICENSE_LINE}\n\nimport x from "y";\n`,
  );
  assert.equal(hasHeader(fixed), true);
});

test('Dockerfile parser directives stay first', () => {
  const fixed = addHeader('# syntax=docker/dockerfile:1\nFROM node:22\n', '#');
  assert.equal(fixed.split('\n')[0], '# syntax=docker/dockerfile:1');
  assert.equal(fixed.split('\n')[1], `# ${COPYRIGHT_LINE}`);
});

test('an existing blank line is not doubled', () => {
  const fixed = addHeader('\nexport {};\n', '//');
  assert.equal(fixed, `// ${COPYRIGHT_LINE}\n// ${LICENSE_LINE}\n\nexport {};\n`);
});

test('the header is two OpenTelemetry-style lines with no year', () => {
  assert.equal(
    addHeader('export {};\n', '//'),
    '// Copyright The Orca Authors\n// SPDX-License-Identifier: Apache-2.0\n\nexport {};\n',
  );
  assert.equal(
    addHeader('FROM node:22\n', '#'),
    '# Copyright The Orca Authors\n# SPDX-License-Identifier: Apache-2.0\n\nFROM node:22\n',
  );
});

test('an old copyright line fails the check and is rewritten in place', () => {
  const old = `#!/usr/bin/env node\n// ${LEGACY_COPYRIGHT_LINE}\n// ${LICENSE_LINE}\n\nexport {};\n`;
  assert.equal(hasHeader(old), false);
  const fixed = fixHeader(old, '//');
  assert.equal(
    fixed,
    `#!/usr/bin/env node\n// ${COPYRIGHT_LINE}\n// ${LICENSE_LINE}\n\nexport {};\n`,
  );
  assert.equal(hasHeader(fixed), true);
  // A file with no header at all gets the whole header, as with addHeader.
  assert.equal(fixHeader('export {};\n', '//'), addHeader('export {};\n', '//'));
});
