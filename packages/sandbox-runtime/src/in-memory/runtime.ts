// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import {
  chmodSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { parseToolCall } from '../sandbox-runtime.js';
import { killSandboxProcessTree } from '../kill-process-tree.js';
import type {
  EnvironmentSpec,
  SandboxCapabilities,
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SandboxRuntime,
  SpawnHandle,
  ToolCall,
  ToolResult,
  SandboxWritePolicy,
} from '../sandbox-runtime.js';
import type { ReadPage, ReadPageInput } from '../read-page.js';
import { readUtf8FilePage, resolveSandboxPathUnderRoot } from '../read-page.js';
import { canonicalizeVirtualPathUnderRoot } from '../write-policy.js';

let nextId = 0;

/**
 * Test double + dev sandbox. Backs files with a tmpdir; runs `bash -c` via
 * `child_process.spawn`. pause/resume are no-ops (the FS persists trivially
 * because we never delete the tmpdir until destroy()).
 *
 * Not safe for production — runs commands directly on the harness host.
 */
export class InMemorySandboxRuntime implements SandboxRuntime {
  /**
   * No FUSE: this runtime is a tmpdir + `child_process.spawn` on the
   * harness host. There's no privilege boundary, no libfuse, and (more
   * importantly) no isolation — running `mount` or `sudo s3fs` here would
   * touch the host's namespace. The strategy factory falls back to
   * `tarball_prefetch` whenever `supportsFuse` is false.
   */
  readonly capabilities: SandboxCapabilities = {
    supportsFuse: false,
    supportsLocalMemory: true,
    supportsWritePolicy: true,
  };

  async acquire(_env: EnvironmentSpec): Promise<SandboxHandle> {
    const root = mkdtempSync(join(tmpdir(), 'orca-sandbox-'));
    const id = `sbx_inmem_${++nextId}_${Date.now().toString(36)}`;
    return new InMemorySandboxHandle(id, root);
  }
}

class InMemoryFiles implements SandboxFiles {
  constructor(private readonly root: string) {}

  private resolve(path: string): string {
    if (path.startsWith('/')) {
      return join(this.root, path.slice(1));
    }
    return join(this.root, path);
  }

  async write(path: string, content: Buffer | NodeJS.ReadableStream): Promise<void> {
    const full = this.resolve(path);
    mkdirSync(dirname(full), { recursive: true });
    if (Buffer.isBuffer(content)) {
      writeFileSync(full, content);
      return;
    }
    // Drain a stream into a buffer, then write. Acceptable for v1 — files are
    // bounded by the upstream caller.
    const chunks: Buffer[] = [];
    for await (const chunk of content) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
    }
    writeFileSync(full, Buffer.concat(chunks));
  }

  async read(path: string): Promise<Buffer> {
    const full = this.resolve(path);
    return readFileSync(full);
  }

  async readUtf8Page(
    path: string,
    input: ReadPageInput,
    constraint?: SandboxReadConstraint,
  ): Promise<ReadPage> {
    const mappedConstraint = constraint
      ? {
          readableRoots: constraint.readableRoots.map((root) =>
            resolveSandboxPathUnderRoot(this.root, root),
          ),
        }
      : undefined;
    return await readUtf8FilePage(
      resolveSandboxPathUnderRoot(this.root, path),
      mappedConstraint,
      input,
    );
  }

  async list(path: string): Promise<string[]> {
    const full = this.resolve(path);
    return readdirSync(full);
  }

  async chmod(path: string, mode: number): Promise<void> {
    chmodSync(this.resolve(path), mode);
  }

  async delete(path: string): Promise<void> {
    const full = this.resolve(path);
    rmSync(full, { recursive: true, force: true });
  }
}

class InMemorySandboxHandle implements SandboxHandle {
  readonly files: SandboxFiles;
  private destroyed = false;
  private destroyComplete = false;

  constructor(
    public readonly id: string,
    private readonly root: string,
  ) {
    this.files = new InMemoryFiles(root);
  }

  async run(call: ToolCall): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const parsed = parseToolCall(call);
    if (parsed.tool === 'invalid') {
      return { exit_code: 2, stderr: parsed.error };
    }

    if (parsed.tool === 'bash') {
      const args = parsed.args;
      return await runBash(args.command, this.root, args.timeout_ms);
    }

    if (parsed.tool === 'glob') {
      const args = parsed.args;
      const cwd = args.root ? join(this.root, args.root.replace(/^\//, '')) : this.root;
      // No `|| true`: compgen exits 1 for zero matches (a real empty result);
      // anything above 1 — including runBash's synthesized 127 for a bad root —
      // is an error the agent must see, not a false empty success.
      const result = await runBash(`compgen -G '${args.pattern.replace(/'/g, "'\\''")}'`, cwd);
      const matches = (result.stdout ?? '').split('\n').filter((s) => s.length > 0);
      if ((result.exit_code ?? 0) > 1) {
        // Keep any partial matches alongside the fault (see grep below).
        return { exit_code: result.exit_code ?? 1, stderr: result.stderr ?? '', output: matches };
      }
      return { output: matches };
    }

    if (parsed.tool === 'grep') {
      const args = parsed.args;
      const cwd = args.root ? join(this.root, args.root.replace(/^\//, '')) : this.root;
      const safe = args.pattern.replace(/'/g, "'\\''");
      // grep exits 0 on match, 1 on no match, ≥2 on error (bad pattern,
      // unreadable root). Only the first two are successes.
      const result = await runBash(`grep -rn '${safe}' .`, cwd);
      if ((result.exit_code ?? 0) > 1) {
        // grep exits 2 on ANY error even when it also matched — surface the
        // fault WITH the partial matches, not instead of them.
        return {
          exit_code: result.exit_code ?? 1,
          stderr: result.stderr ?? '',
          output: result.stdout ?? '',
        };
      }
      return { output: result.stdout ?? '' };
    }

    return { exit_code: 127, stderr: `unknown tool: ${parsed.name}` };
  }

  /**
   * Spawn a long-lived `bash -c` process on the host, returning its live
   * stdio. Runs inside the session tmpdir (or `opts.cwd` resolved under it)
   * so spawned processes see the same FS as {@link run}. No sandbox isolation
   * — same caveat as the rest of this runtime, so (unlike the Local runtime)
   * the host's own env is inherited verbatim: there is no privilege boundary
   * here for a scrubbed env to protect.
   */
  async spawn(
    cmd: string,
    opts?: { env?: Record<string, string>; cwd?: string },
  ): Promise<SpawnHandle> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const cwd = opts?.cwd ? join(this.root, opts.cwd.replace(/^\//, '')) : this.root;
    const proc = spawn('bash', ['-c', cmd], {
      cwd,
      detached: true,
      env: opts?.env ? { ...process.env, ...opts.env } : process.env,
    });
    // The 'error' listener is load-bearing: without one, a spawn failure (e.g.
    // a caller-supplied cwd that does not exist — resolved above without an
    // existence check) emits an unhandled 'error' event that crashes the whole
    // harness process. The pre-attached no-op catch keeps an unobserved exited
    // promise from surfacing the same failure as an unhandled rejection.
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve, reject) => {
        proc.on('error', (err) => reject(err));
        proc.on('close', (code, signal) => resolve({ code, signal }));
      },
    );
    exited.catch(() => {});
    return {
      stdout: proc.stdout,
      stdin: proc.stdin,
      kill: (signal?: NodeJS.Signals) => {
        killSandboxProcessTree(proc, signal ?? 'SIGTERM');
      },
      exited,
    };
  }

  async prepareFilesystemRoots(_paths: readonly string[]): Promise<void> {
    // This runtime maps every sandbox path below a newly-created private
    // tmpdir, so an image cannot pre-install aliases or mounts there.
  }

  async prepareWritePolicy(_policy: SandboxWritePolicy): Promise<void> {
    // Test-only runtime: direct file operations are guarded by the policy
    // wrapper. Arbitrary Bash is denied below because this runtime has no OS
    // isolation boundary and must not pretend shell parsing is sufficient.
  }

  async runWithWritePolicy(call: ToolCall, _policy: SandboxWritePolicy): Promise<ToolResult> {
    if (call.tool === 'bash') {
      return {
        exit_code: 126,
        stderr: 'bash disabled: InMemorySandboxRuntime cannot isolate subprocess writes',
      };
    }
    return await this.run(call);
  }

  async canonicalizePathForPolicy(path: string): Promise<string> {
    return canonicalizeVirtualPathUnderRoot(this.root, path);
  }

  // `runPrivileged` is deliberately ABSENT: this runtime has no privilege
  // boundary — running `sudo s3fs` here would either fail (no libfuse) or,
  // worse, touch the harness host's mount namespace. FUSE-based strategies
  // MUST NOT be selected against this runtime; the strategy factory enforces
  // that, and the missing method is the feature-detection signal (same
  // convention as `spawn` / `endpoint`).

  async pause(): Promise<void> {
    /* no-op: tmpdir persists across calls */
  }

  async resume(): Promise<void> {
    /* no-op */
  }

  async destroy(): Promise<void> {
    if (this.destroyComplete) return;
    // Two latches (mirroring LocalSandboxHandle): `destroyed` flips FIRST so
    // run()/spawn() refuse new work even when cleanup fails — a handle must
    // never stay operational over a partially-deleted tmpdir — while the
    // idempotent early-return gates on `destroyComplete`, which only a
    // successful rmSync sets, keeping a failed destroy retryable (EACCES under
    // a 0o555-hardened skill directory is the realistic thrower).
    this.destroyed = true;
    rmSync(this.root, { recursive: true, force: true });
    this.destroyComplete = true;
  }

  /** Test helper: returns the underlying tmpdir root (for in-process inspection). */
  rootDir(): string {
    return this.root;
  }
}

function runBash(command: string, cwd: string, timeoutMs?: number): Promise<ToolResult> {
  return new Promise((resolve) => {
    // Detached => group leader, so the timeout kill below reaches the whole
    // tree: SIGKILL'ing only the outer bash orphans its forked command, which
    // keeps running AND holds the stdio pipes open so 'close' never fires.
    const proc = spawn('bash', ['-c', command], { cwd, detached: true });
    const out: Buffer[] = [];
    const err: Buffer[] = [];
    let timer: NodeJS.Timeout | undefined;
    let timedOut = false;
    if (timeoutMs && timeoutMs > 0) {
      timer = setTimeout(() => {
        timedOut = true;
        killSandboxProcessTree(proc, 'SIGKILL');
      }, timeoutMs);
    }
    // A spawn failure (nonexistent cwd — glob/grep pass a caller-supplied root
    // as cwd — or EMFILE under load) emits 'error'; without this listener the
    // event is an uncaught exception that kills the whole harness process and
    // the promise never settles. 127 ("could not run") keeps it distinguishable
    // from the command itself exiting 1.
    proc.on('error', (spawnErr: Error) => {
      if (timer) clearTimeout(timer);
      resolve({
        stdout: '',
        stderr: `[orca: spawn failed: ${spawnErr.message}]`,
        exit_code: 127,
      });
    });
    proc.stdout.on('data', (b: Buffer) => out.push(b));
    proc.stderr.on('data', (b: Buffer) => err.push(b));
    proc.on('close', (code, signal) => {
      if (timer) clearTimeout(timer);
      // A `code ?? 0` here would report the SIGKILL timeout path as success:
      // close fires with (null, 'SIGKILL'). Mirror the local runtime — signal
      // exits resolve 124 with an explicit marker so a timed-out command can
      // never masquerade as exit 0 with partial output.
      const stderrText = Buffer.concat(err).toString('utf8');
      resolve({
        stdout: Buffer.concat(out).toString('utf8'),
        stderr:
          signal !== null
            ? `${stderrText}${stderrText.length > 0 ? '\n' : ''}[orca: killed by ${signal}${timedOut ? ' after timeout' : ''}]`
            : stderrText,
        exit_code: code ?? (signal !== null ? 124 : 0),
      });
    });
  });
}

/** Test helper exported separately so tests can downcast safely. */
export function asInMemoryHandle(h: SandboxHandle): InMemorySandboxHandle {
  if (!(h instanceof InMemorySandboxHandle)) {
    throw new Error('not an InMemorySandboxHandle');
  }
  return h;
}
