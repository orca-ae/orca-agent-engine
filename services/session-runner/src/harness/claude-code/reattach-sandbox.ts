// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Reattach a {@link SandboxHandle} to an EXISTING per-session work-dir root — the glue
// that lets the native-CLI tool-bridge, running in a SEPARATE process the headless CLI
// spawned, bind to the SAME sandbox tree the runner acquired.
//
// **Why this exists.** The `claude-code` provider boots the headless `claude` binary
// INSIDE the per-session sandbox (via {@link launchNativeCli}), and the CLI spawns its
// MCP server (the native-CLI tool-bridge) as its OWN child — a grandchild in the same
// sandbox tree. That bridge child is a distinct process; it cannot share the runner's
// in-process {@link SandboxHandle} object. But both reusable runtimes' handles are just
// wrappers over a host-side work-dir ROOT (files rooted at `root`; `run`/`spawn` via
// `child_process`/tmux in `root`), so a handle REATTACHED to the same `root` operates on
// the SAME files. This module builds that reattached handle from a root path, so the
// bridge serves tools bound to the session's real work-dir — the model's bash/read/write
// then land in the session sandbox, not on the bridge process's cwd.
//
// **Runtime shape.** When a tmux socket is provided (the local-attach transport) the
// reattached handle is a reconstructed {@link TmuxSandboxHandle} over the same root — it
// derives the same socket from `root`, so it reattaches to the live tmux server AND
// carries the {@link TerminalHost} capability (sys_terminal_* still resolve through the
// bridge). Otherwise it is a compact {@link LocalRootSandbox}: files via `node:fs` and
// `bash`/`glob`/`grep` via `node:child_process`, rooted at `root` — the SAME semantics as
// the in-memory runtime, so the bridge behaves identically on the dev/in-memory path.
//
// This is PROVIDER GLUE, not a runtime change: it composes against the {@link SandboxHandle}
// seam and reuses the exported {@link TmuxSandboxHandle}.

import { spawn } from 'node:child_process';
import { chmodSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { readUtf8FilePage, resolveSandboxPathUnderRoot } from '@orca/sandbox-runtime';
import type {
  ReadPage,
  ReadPageInput,
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SpawnHandle,
  ToolCall,
  ToolResult,
} from '../../sandbox/seam.js';
import { TmuxSandboxHandle } from '../../sandbox/tmux-sandbox.js';

/**
 * Build a {@link SandboxHandle} bound to the existing work-dir `root`. When `tmuxSocket`
 * is set, a {@link TmuxSandboxHandle} reconstructed over `root` is returned (it reattaches
 * to the live tmux server derived from `root` and carries the {@link TerminalHost}
 * capability); otherwise a compact {@link LocalRootSandbox} over `root` is returned. The
 * `id` is informational (surfaced on the handle) and defaults to a stable derived value.
 */
export function reattachSessionSandbox(opts: {
  root: string;
  tmuxSocket?: string;
  id?: string;
}): SandboxHandle {
  const id = opts.id ?? `sbx_reattach_${hashRoot(opts.root)}`;
  if (opts.tmuxSocket !== undefined && opts.tmuxSocket.length > 0) {
    // The tmux handle derives its socket from `root/.orca-tmux/server.sock`, so
    // reconstructing over the same root reattaches to the same live tmux server —
    // giving files + run + spawn + TerminalHost over the shared session tree.
    return new TmuxSandboxHandle(id, opts.root);
  }
  return new LocalRootSandbox(id, opts.root);
}

/** Filesystem ops rooted at `root` (mirrors the in-memory runtime's `SandboxFiles`). */
class LocalRootFiles implements SandboxFiles {
  constructor(private readonly root: string) {}

  private resolve(path: string): string {
    return path.startsWith('/') ? join(this.root, path.slice(1)) : join(this.root, path);
  }

  async write(path: string, content: Buffer | NodeJS.ReadableStream): Promise<void> {
    const full = this.resolve(path);
    mkdirSync(dirname(full), { recursive: true });
    if (Buffer.isBuffer(content)) {
      writeFileSync(full, content);
      return;
    }
    const chunks: Buffer[] = [];
    for await (const chunk of content) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
    }
    writeFileSync(full, Buffer.concat(chunks));
  }

  async read(path: string): Promise<Buffer> {
    return readFileSync(this.resolve(path));
  }

  /**
   * Bounded UTF-8 page read, mirroring `InMemoryFiles.readUtf8Page` /
   * `LocalSandboxFiles.readUtf8Page` in `@orca/sandbox-runtime`: map both the
   * target path and the caller's trusted roots into this handle's root via
   * {@link resolveSandboxPathUnderRoot}, then delegate to the package's
   * `readUtf8FilePage` for the open/authorize/pread. Fails closed (throws) when
   * `constraint` is absent, matching the interface contract.
   */
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
    return readdirSync(this.resolve(path));
  }

  async chmod(path: string, mode: number): Promise<void> {
    chmodSync(this.resolve(path), mode);
  }

  async delete(path: string): Promise<void> {
    rmSync(this.resolve(path), { recursive: true, force: true });
  }
}

/**
 * A {@link SandboxHandle} over an existing host-side `root`, with NO isolation — files
 * via `node:fs`, `bash`/`glob`/`grep` + `spawn` via `node:bash -c` in `root`. It mirrors
 * the in-memory runtime's tool semantics exactly (identical `glob`/`grep` shell shapes),
 * so the bridge's tools behave the same whether the session ran on the in-memory or the
 * local-attach transport. `destroy` is a no-op — the reattached handle does NOT own the
 * root (the runner's original handle does), so tearing the bridge process down must never
 * delete the session's work-dir.
 */
class LocalRootSandbox implements SandboxHandle {
  readonly files: SandboxFiles;

  constructor(
    public readonly id: string,
    private readonly root: string,
  ) {
    this.files = new LocalRootFiles(root);
  }

  async run(call: ToolCall): Promise<ToolResult> {
    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      return runBash(args.command, this.root, args.timeout_ms);
    }
    if (call.tool === 'glob') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ? join(this.root, args.root.replace(/^\//, '')) : this.root;
      const result = await runBash(`compgen -G '${shellSingleQuote(args.pattern)}' || true`, cwd);
      const matches = (result.stdout ?? '').split('\n').filter((s) => s.length > 0);
      return { output: matches };
    }
    if (call.tool === 'grep') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ? join(this.root, args.root.replace(/^\//, '')) : this.root;
      const result = await runBash(`grep -rn '${shellSingleQuote(args.pattern)}' . || true`, cwd);
      return { output: result.stdout ?? '' };
    }
    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
  }

  async spawn(
    cmd: string,
    opts?: { env?: Record<string, string>; cwd?: string },
  ): Promise<SpawnHandle> {
    const cwd = opts?.cwd ? join(this.root, opts.cwd.replace(/^\//, '')) : this.root;
    const proc = spawn('bash', ['-c', cmd], {
      cwd,
      env: opts?.env ? { ...process.env, ...opts.env } : process.env,
    });
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
        proc.kill(signal ?? 'SIGTERM');
      },
      exited,
    };
  }

  async runPrivileged(): Promise<ToolResult> {
    throw new Error('LocalRootSandbox does not support privileged operations');
  }

  async pause(): Promise<void> {
    /* no-op: the reattached root persists */
  }

  async resume(): Promise<void> {
    /* no-op */
  }

  async destroy(): Promise<void> {
    /* no-op: the reattached handle does NOT own the root (the runner's handle does). */
  }

  /** The reattached work-dir root (the same accessor the reusable runtimes expose). */
  rootDir(): string {
    return this.root;
  }
}

/** Run one `bash -c` command rooted at `cwd`, collecting stdout/stderr/exit code. */
function runBash(command: string, cwd: string, timeoutMs?: number): Promise<ToolResult> {
  return new Promise((resolve) => {
    const proc = spawn('bash', ['-c', command], { cwd });
    const out: Buffer[] = [];
    const err: Buffer[] = [];
    let timer: NodeJS.Timeout | undefined;
    let settled = false;
    const settle = (result: ToolResult): void => {
      if (settled) {
        return;
      }
      settled = true;
      if (timer) {
        clearTimeout(timer);
      }
      resolve(result);
    };
    if (timeoutMs && timeoutMs > 0) {
      timer = setTimeout(() => proc.kill('SIGKILL'), timeoutMs);
    }
    proc.stdout.on('data', (b: Buffer) => out.push(b));
    proc.stderr.on('data', (b: Buffer) => err.push(b));
    // A spawn failure (`bash` unresolved, etc.) emits `error` — surface it as a
    // non-zero ToolResult rather than letting the rejection tear the bridge down.
    proc.on('error', (e: Error) => {
      settle({ stdout: '', stderr: `spawn failed: ${e.message}`, exit_code: 127 });
    });
    proc.on('close', (code) => {
      settle({
        stdout: Buffer.concat(out).toString('utf8'),
        stderr: Buffer.concat(err).toString('utf8'),
        exit_code: code ?? 0,
      });
    });
  });
}

/** POSIX single-quote-escape a string for safe interpolation inside `'...'`. */
function shellSingleQuote(s: string): string {
  return s.replace(/'/g, "'\\''");
}

/** A short, stable, filesystem-safe digest of a root path (for the informational id). */
function hashRoot(root: string): string {
  let h = 0;
  for (let i = 0; i < root.length; i += 1) {
    h = (Math.imul(31, h) + root.charCodeAt(i)) | 0;
  }
  return (h >>> 0).toString(36);
}
