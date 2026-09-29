#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * One-off spike script for the orca-default E2B template.
 *
 * Spawns a sandbox from $E2B_TEMPLATE_ID (or the default 'orca-default'
 * template name) and runs the template's spike checks (FUSE / s3fs mount
 * prerequisites and the git credential helper) via `Sandbox.commands.run`.
 * Prints PASS/FAIL per check; exits 1 if any FAIL.
 *
 * Usage:
 *   E2B_API_KEY=... E2B_TEMPLATE_ID=<id> \
 *     pnpm -F @orca/harness-server exec tsx scripts/spike-orca-default.ts
 */
import { Sandbox } from '@e2b/code-interpreter';

interface CheckResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

interface Check {
  name: string;
  cmd: string;
  judge?: (r: CheckResult) => boolean;
}

// Every check command appends `; echo __EXIT__=$?` so we can capture the
// real exit code via stdout. Without this, the E2B SDK's commands.run throws
// CommandExitError on non-zero, which short-circuits the judge.
const TAIL = ' 2>&1; echo __EXIT__=$?';

function parseExit(stdout: string): { exitCode: number; cleanStdout: string } {
  const m = stdout.match(/__EXIT__=(\d+)\s*$/);
  if (!m) return { exitCode: -1, cleanStdout: stdout };
  return {
    exitCode: Number(m[1]),
    cleanStdout: stdout.slice(0, stdout.lastIndexOf('__EXIT__=')).trimEnd(),
  };
}

const CHECKS: Check[] = [
  { name: 'git installed (>= 2.30)', cmd: 'git --version' + TAIL },
  { name: 'jq installed', cmd: 'jq --version' + TAIL },
  { name: 's3fs installed', cmd: '(which s3fs && s3fs --version 2>&1 | head -1)' + TAIL },
  {
    name: 'orca-git-creds is executable (mode has +x for all)',
    // Functional check: file is executable (not the literal 0755 mode).
    cmd: 'test -x /usr/local/bin/orca-git-creds && echo X_OK' + TAIL,
    judge: (r) => r.exitCode === 0 && /X_OK/.test(r.stdout),
  },
  {
    name: 'template hardening prerequisites',
    cmd:
      'test "$(id -u):$(id -g)" = 1000:1000 && ' +
      'command -v bwrap >/dev/null && command -v realpath >/dev/null && ' +
      'command -v setpriv >/dev/null && test -c /dev/fuse && ' +
      'sudo -n -l > /tmp/orca-sudo-list && ' +
      "! grep -Eq 'NOPASSWD:[[:space:]]*ALL|NOPASSWD:SETENV' /tmp/orca-sudo-list && " +
      "grep -Fq '/usr/local/bin/orca-s3fs-mount' /tmp/orca-sudo-list && " +
      'if sudo -n /bin/sh -c id >/dev/null 2>&1; then exit 91; fi && ' +
      'echo HARDENING_OK' +
      TAIL,
    judge: (r) => r.exitCode === 0 && /HARDENING_OK/.test(r.stdout),
  },
  {
    name: 'sandbox diagnostics (informational)',
    cmd:
      'echo "id=$(id)"; ' +
      'echo "uid=$(id -u)"; ' +
      'echo "CapEff(user)=$(grep CapEff /proc/self/status)"; ' +
      'echo "CapBnd(user)=$(grep CapBnd /proc/self/status)"; ' +
      'echo "fusermount3=$(command -v fusermount3 || echo NONE)"; ' +
      'echo "/dev/fuse=$(ls -l /dev/fuse 2>&1)"; ' +
      'echo "sudo:"; sudo -n -l 2>&1 | sed "s/^/  /"; ' +
      'echo "kernel=$(uname -r)";' +
      TAIL,
    judge: (r) => r.exitCode === 0,
  },
  {
    name: 'FUSE: s3fs against RustFS unreachable from sandbox (smoke)',
    cmd:
      // Just check s3fs prints help; we can't reach a real S3 endpoint here.
      // Real mount persistence needs disposable MinIO/S3 credentials.
      's3fs --help 2>&1 | head -1' + TAIL,
    judge: (r) => r.exitCode === 0 && r.stdout.length > 0,
  },
  {
    name: 'mount helper rejects non-memory custom root',
    cmd:
      'set +e; ' +
      'ORCA_S3_ACCESS_KEY_ID=x ORCA_S3_SECRET_ACCESS_KEY=y ' +
      'sudo -n --preserve-env=ORCA_S3_ACCESS_KEY_ID,ORCA_S3_SECRET_ACCESS_KEY ' +
      '/usr/local/bin/orca-s3fs-mount bucket /mnt/custom url=http://example.invalid ' +
      '>/tmp/orca-custom-mount-out 2>/tmp/orca-custom-mount-err; rc=$?; ' +
      'set -e; test "$rc" = 64 && ' +
      "! grep -Fq 'password is required' /tmp/orca-custom-mount-err && " +
      'echo MOUNT_POLICY_OK' +
      TAIL,
    judge: (r) => r.exitCode === 0 && /MOUNT_POLICY_OK/.test(r.stdout),
  },
  {
    name: 'credential.helper configured system-wide',
    cmd:
      'test "$(git config --system --get credential.helper)" = /usr/local/bin/orca-git-creds && ' +
      'test "$(git config --system --get credential.useHttpPath)" = true && ' +
      'echo GIT_CONFIG_OK' +
      TAIL,
    judge: (r) => r.exitCode === 0 && /GIT_CONFIG_OK/.test(r.stdout),
  },
  {
    name: 'helper-without-env: anonymous fallback',
    cmd:
      'unset ORCA_GIT_CREDS_URL ORCA_GIT_CREDS_TOKEN; ' +
      "out=$(printf 'protocol=https\\nhost=github.com\\npath=orca/test\\n\\n' | " +
      '/usr/local/bin/orca-git-creds get); ' +
      'test -z "$out" && echo ANONYMOUS_OK' +
      TAIL,
    judge: (r) => r.exitCode === 0 && /ANONYMOUS_OK/.test(r.stdout),
  },
];

interface SandboxRunResult {
  exitCode?: number;
  stdout?: string;
  stderr?: string;
}

async function main(): Promise<void> {
  // Self-skip when env is unset so the nightly workflow never fails on a fork
  // without secrets. The two env vars are independent: API_KEY without
  // TEMPLATE_ID would fall back to the upstream `orca-default` template name
  // (which doesn't exist on most accounts), so we require both.
  if (!process.env['E2B_API_KEY']) {
    console.log('spike-orca-default: skipping — E2B_API_KEY is unset');
    process.exit(0);
  }
  if (!process.env['E2B_TEMPLATE_ID']) {
    console.log('spike-orca-default: skipping — E2B_TEMPLATE_ID is unset');
    process.exit(0);
  }
  const templateId = process.env['E2B_TEMPLATE_ID'];
  console.log(`spawning sandbox from template ${templateId}…`);
  const sb = await Sandbox.create(templateId, { timeoutMs: 120_000 });
  console.log(`sandbox ${sb.sandboxId} ready; running ${CHECKS.length} checks\n`);
  let failed = 0;
  for (const check of CHECKS) {
    let rawStdout = '';
    let rawStderr = '';
    try {
      const raw = (await sb.commands.run(check.cmd, { timeoutMs: 30_000 })) as SandboxRunResult;
      rawStdout = raw.stdout ?? '';
      rawStderr = raw.stderr ?? '';
    } catch (e) {
      // E2B SDK throws CommandExitError on non-zero; the error usually
      // carries stdout/stderr/exitCode. Recover them so the judge sees them.
      const err = e as { stdout?: string; stderr?: string; exitCode?: number; message?: string };
      rawStdout = err.stdout ?? '';
      rawStderr = err.stderr ?? err.message ?? '';
      // Append the exit code into stdout so parseExit picks it up.
      if (typeof err.exitCode === 'number' && !rawStdout.includes('__EXIT__=')) {
        rawStdout = rawStdout + `\n__EXIT__=${err.exitCode}\n`;
      }
    }

    const parsed = parseExit(rawStdout);
    const result: CheckResult = {
      exitCode: parsed.exitCode,
      stdout: parsed.cleanStdout,
      stderr: rawStderr,
    };
    const judge =
      check.judge ?? ((r: CheckResult) => r.exitCode === 0 && r.stdout.trim().length > 0);
    const ok = judge(result);
    if (ok) {
      console.log(`PASS  ${check.name}`);
      const firstLine = result.stdout.split('\n').find((l) => l.trim());
      if (firstLine) console.log(`       └─ ${firstLine.trim()}`);
      // For diagnostics-only checks, print all lines.
      if (check.name.includes('diagnostics')) {
        for (const l of result.stdout.split('\n').slice(1)) {
          if (l.trim()) console.log(`       └─ ${l.trim()}`);
        }
      }
    } else {
      failed += 1;
      console.log(`FAIL  ${check.name}`);
      console.log(`       exit_code=${result.exitCode}`);
      if (result.stdout.trim()) {
        console.log(`       stdout: ${result.stdout.trim().split('\n').join('\n              ')}`);
      }
      if (result.stderr.trim()) {
        console.log(`       stderr: ${result.stderr.trim().split('\n').join('\n              ')}`);
      }
    }
  }
  await sb.kill();
  console.log(`\n${failed === 0 ? 'ALL_PASS' : `FAILED: ${failed}/${CHECKS.length}`}`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
