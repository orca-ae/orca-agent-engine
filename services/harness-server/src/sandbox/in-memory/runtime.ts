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
import type {
  EnvironmentSpec,
  SandboxCapabilities,
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SandboxRuntime,
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

  constructor(
    public readonly id: string,
    private readonly root: string,
  ) {
    this.files = new InMemoryFiles(root);
  }

  async run(call: ToolCall): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);

    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      return await runBash(args.command, this.root, args.timeout_ms);
    }

    if (call.tool === 'glob') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ? join(this.root, args.root.replace(/^\//, '')) : this.root;
      const result = await runBash(
        `compgen -G '${args.pattern.replace(/'/g, "'\\''")}' || true`,
        cwd,
      );
      const matches = (result.stdout ?? '').split('\n').filter((s) => s.length > 0);
      return { output: matches };
    }

    if (call.tool === 'grep') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ? join(this.root, args.root.replace(/^\//, '')) : this.root;
      const safe = args.pattern.replace(/'/g, "'\\''");
      const result = await runBash(`grep -rn '${safe}' . || true`, cwd);
      return { output: result.stdout ?? '' };
    }

    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
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

  /**
   * The in-memory runtime has no privilege boundary — running `sudo s3fs`
   * here would either fail (no libfuse) or, worse, touch the harness host's
   * mount namespace. FUSE-based strategies (MemoryFuseStrategy) MUST NOT be
   * selected against this runtime; the strategy factory enforces that.
   * Throwing here is the second line of defense.
   */
  async runPrivileged(cmd: string, opts?: { envs?: Record<string, string> }): Promise<ToolResult> {
    void cmd;
    void opts;
    throw new Error('InMemorySandboxRuntime does not support privileged operations');
  }

  async pause(): Promise<void> {
    /* no-op: tmpdir persists across calls */
  }

  async resume(): Promise<void> {
    /* no-op */
  }

  async destroy(): Promise<void> {
    if (this.destroyed) return;
    this.destroyed = true;
    rmSync(this.root, { recursive: true, force: true });
  }

  /** Test helper: returns the underlying tmpdir root (for in-process inspection). */
  rootDir(): string {
    return this.root;
  }
}

function runBash(command: string, cwd: string, timeoutMs?: number): Promise<ToolResult> {
  return new Promise((resolve) => {
    const proc = spawn('bash', ['-c', command], { cwd });
    const out: Buffer[] = [];
    const err: Buffer[] = [];
    let timer: NodeJS.Timeout | undefined;
    if (timeoutMs && timeoutMs > 0) {
      timer = setTimeout(() => proc.kill('SIGKILL'), timeoutMs);
    }
    proc.stdout.on('data', (b: Buffer) => out.push(b));
    proc.stderr.on('data', (b: Buffer) => err.push(b));
    proc.on('close', (code) => {
      if (timer) clearTimeout(timer);
      resolve({
        stdout: Buffer.concat(out).toString('utf8'),
        stderr: Buffer.concat(err).toString('utf8'),
        exit_code: code ?? 0,
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
