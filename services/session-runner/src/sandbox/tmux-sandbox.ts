// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// TmuxSandboxHandle — a socket-scoped tmux session exposed as a runner SandboxHandle.
//
// The runner's native-CLI providers launch a real CLI as a long-lived child and
// stream its stdio through the extracted `SandboxHandle.spawn` primitive (see
// `sandbox/seam.ts`). This runtime backs that boundary with tmux: every acquired
// handle owns its OWN tmux server on a private unix socket, and each `spawn`
// starts a detached session (`tmux -S <socket> new-session -d`) whose pane runs
// the requested command. The advantage over a bare `child_process.spawn` is the
// LOCAL ATTACH story: an operator can run `tmux -S <socket> attach` against the
// same socket to watch — and even drive — the pane by hand, at zero cost and with
// no cloud sandbox. That makes this the natural transport for driving a native
// coding CLI on the runner host during development and self-hosted operation.
//
// How each primitive maps onto tmux, and why:
//   - stdout: the pane is a PTY, so capturing the pane directly would hand the
//     consumer `\r\n`-rendered, width-wrapped bytes — useless for a line-oriented
//     stream-json normalizer. Instead the wrapper pipes the process's OWN output
//     through `tee` into a private FIFO: `<cmd> 2>&1 | tee <fifo>`. This runtime
//     reads the FIFO (clean, raw, `\n`-terminated process bytes) while the SAME
//     bytes still render in the pane for an attached operator. The FIFO read end
//     is opened with `fs.createReadStream` (default `O_RDONLY`): the blocking
//     open runs on the libuv threadpool (never stalls the event loop) and the
//     stream sees a natural EOF when `tee` closes as the process exits.
//   - stdin: `tmux send-keys` types into the pane's TTY, exactly what a
//     hand-attaching operator would do. Writes to the returned stdin stream are
//     translated line-by-line into `send-keys -l <line>` followed by an `Enter`
//     key, so a `write("{...}\n")` from a stream-json driver submits one line to
//     the CLI. Long lines are chunked to stay under tmux's per-command length cap.
//     Closing the stream (`end()`) types a final `Ctrl-D`, which the pane's line
//     discipline raises as a real EOF for the foreground reader (a `cat`-style
//     child that reads to EOF then terminates). That makes `end()` a genuine
//     best-effort end-of-input over the local-attach transport, not just a local
//     close — see `TmuxSandboxHandle.sendEof` for the line-boundary caveat.
//   - kill: `tmux kill-session -t <session>` tears the pane (and its process) down.
//   - run: a one-shot pane whose command is run to completion, then its exit code
//     + captured stdout/stderr are returned — bit-compatible with the InMemory and
//     Local runtimes' `run`.
//   - files: the pane host's filesystem, rooted at a per-handle tmpdir (same shape
//     as InMemory) so mount strategies + the toolset read/write there.
//   - pause/resume: no-ops (the session + tmpdir persist between calls).
//   - runPrivileged: unsupported — this transport has no privilege boundary, so it
//     throws, matching InMemory + Local.
//
// The implementation is entirely first-party: it shells out to the stock `tmux`
// binary and Node's `fs`/`child_process`. `supportsFuse=false`.

import { execFile, spawnSync } from 'node:child_process';
import {
  chmodSync,
  createReadStream,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { Writable } from 'node:stream';
import { readUtf8FilePage, resolveSandboxPathUnderRoot } from '@orca/sandbox-runtime';
import type {
  EnvironmentSpec,
  ReadPage,
  ReadPageInput,
  SandboxCapabilities,
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SandboxRuntime,
  SpawnHandle,
  ToolCall,
  ToolResult,
} from './seam.js';
import type {
  AttachedTerminal,
  AttachTerminalOptions,
  LaunchTerminalOptions,
  ReadTerminalOptions,
  SendTerminalOptions,
  TerminalDescriptor,
  TerminalHost,
} from '../tools/sys-terminal.js';

// Re-export the boundary types so this module's consumers (specs, providers) can
// import them from one place alongside the concrete runtime.
export type { SandboxHandle, SandboxRuntime, SpawnHandle, ToolCall, ToolResult } from './seam.js';

/**
 * `send-keys -l` types literal characters, but a single invocation over roughly
 * tmux's per-command buffer fails with "command too long". We chunk literal text
 * well under that ceiling; stream-json control lines are far shorter than this,
 * so chunking only matters for pathological inputs.
 */
const SEND_KEYS_LITERAL_CHUNK = 480;

let nextId = 0;

/**
 * A tmux-backed {@link SandboxRuntime}. Each {@link acquire} stands up a handle
 * that owns a private tmux server (its socket lives under a per-handle tmpdir);
 * commands run as detached sessions on that socket. No FUSE (no privilege
 * boundary), so the strategy factory falls back to `tarball_prefetch`.
 */
export class TmuxSandboxRuntime implements SandboxRuntime {
  readonly capabilities: SandboxCapabilities = { supportsFuse: false };

  async acquire(_env: EnvironmentSpec): Promise<SandboxHandle> {
    const root = mkdtempSync(join(tmpdir(), 'orca-tmux-sandbox-'));
    const id = `sbx_tmux_${++nextId}_${Date.now().toString(36)}`;
    return new TmuxSandboxHandle(id, root);
  }
}

/** Filesystem ops rooted at the per-handle tmpdir (mirrors the InMemory runtime). */
class TmuxSandboxFiles implements SandboxFiles {
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
   * Bounded UTF-8 page read. Files are plain host `node:fs` calls rooted at
   * the per-handle tmpdir (same as `read`/`write` above — there is no tmux
   * transport for file ops, only for `run`/`spawn`), so this mirrors
   * `InMemoryFiles.readUtf8Page` / `LocalSandboxFiles.readUtf8Page` in
   * `@orca/sandbox-runtime` directly rather than going through `read` + slice:
   * map the target path and the caller's trusted roots into this handle's
   * root via {@link resolveSandboxPathUnderRoot}, then delegate to the
   * package's `readUtf8FilePage` for the open/authorize/pread. Fails closed
   * (throws) when `constraint` is absent, matching the interface contract.
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
 * A {@link SandboxHandle} backed by a private tmux server. Owns the socket + a
 * per-handle work-dir; each {@link spawn}/{@link run} opens a detached session on
 * the socket. {@link destroy} kills the whole server (all sessions) and removes
 * the work-dir.
 *
 * It ALSO implements {@link TerminalHost}: on the same private socket it can open
 * interactive panes for the sys_terminal_* toolset (launch a foreground program,
 * send keystrokes/chords, `capture-pane` the rendered grid, list, close). That is
 * a distinct concern from {@link spawn} — spawn pipes through a FIFO for a
 * line-oriented stream, terminals are driven by hand and read as they render — so
 * it is a separate, feature-detectable capability (see `asTerminalHost`).
 */
export class TmuxSandboxHandle implements SandboxHandle, TerminalHost {
  readonly files: SandboxFiles;
  private destroyed = false;
  private spawnSeq = 0;
  private terminalSeq = 0;
  private attachSeq = 0;
  /** Sessions this handle has started, so `destroy` can be thorough. */
  private readonly sessions = new Set<string>();
  /**
   * Interactive terminals launched via the {@link TerminalHost} surface, keyed by
   * their public id (== the tmux session name). Distinct from {@link sessions}
   * (the spawn/run bookkeeping) because these are the panes sys_terminal_* drives:
   * this map is the source of truth for {@link listTerminals} (the launch command
   * is not recoverable from tmux) and lets {@link closeTerminal} be idempotent.
   */
  private readonly terminals = new Map<string, { command: string }>();
  private readonly socketPath: string;
  private readonly runtimeDir: string;

  constructor(
    public readonly id: string,
    private readonly root: string,
  ) {
    // Keep the socket + FIFOs OUT of the file root so they never show up in
    // `files.list('/')` and cannot collide with materialized resources.
    this.runtimeDir = join(root, '.orca-tmux');
    mkdirSync(this.runtimeDir, { recursive: true });
    this.socketPath = join(this.runtimeDir, 'server.sock');
    this.files = new TmuxSandboxFiles(root);
  }

  /**
   * Spawn `cmd` in a fresh detached tmux session and hand back live stdio. The
   * pane runs `<cmd> 2>&1 | tee <fifo>`: this runtime reads the FIFO for clean
   * process bytes while the same output renders in the pane for an attached
   * operator. `stdin` writes are translated to `send-keys`; closing `stdin`
   * (`end()`) types a final `Ctrl-D` for a best-effort EOF; `kill` runs
   * `kill-session`.
   */
  async spawn(
    cmd: string,
    opts?: { env?: Record<string, string>; cwd?: string },
  ): Promise<SpawnHandle> {
    if (this.destroyed) {
      throw new Error(`sandbox ${this.id} destroyed`);
    }
    const seq = ++this.spawnSeq;
    const session = `s${seq}`;
    const fifoPath = join(this.runtimeDir, `${session}.out`);
    makeFifo(fifoPath);

    const cwd = opts?.cwd ? join(this.root, opts.cwd.replace(/^\//, '')) : this.root;
    // The pane runs the caller's command in a group with stderr folded into
    // stdout, piped through `tee` into the FIFO — so this runtime reads the raw
    // process bytes off the FIFO while the SAME bytes render in the pane for an
    // attached operator. The command sits on the LEFT of the pipe, so its stdin is
    // the pane TTY and `send-keys` reaches it. A brace group (not a subshell)
    // keeps the command as the pane's foreground job; tmux runs this single
    // shell-command token via `sh -c`.
    const inner = `{ ${cmd} ; } 2>&1 | tee ${shellQuote(fifoPath)}`;

    // Open the FIFO read end BEFORE the writer starts. `createReadStream` opens
    // on the threadpool (the event loop keeps running) and the stream sees EOF
    // when `tee` closes on process exit.
    const stdout = createReadStream(fifoPath);

    await this.tmux([
      'new-session',
      '-d',
      '-s',
      session,
      '-x',
      '200',
      '-y',
      '50',
      '-c',
      cwd,
      ...envAssignments(opts?.env),
      inner,
    ]);
    this.sessions.add(session);

    const stdin = new TmuxSendKeysStdin(
      (chunk) => this.sendKeys(session, chunk),
      () => this.sendEof(session),
    );
    let killed = false;
    const kill = (_signal?: NodeJS.Signals): void => {
      if (killed) {
        return;
      }
      killed = true;
      // `kill-session` tears down the pane + its process; the FIFO writer (`tee`)
      // closes, ending `stdout` via EOF. Fire-and-forget: a missing session
      // (already gone) is a no-op we deliberately swallow.
      this.tmux(['kill-session', '-t', session]).catch(() => {});
      this.sessions.delete(session);
      // Also tear the reader down directly. This closes `stdout` even in the race
      // where the pane's `tee` had not yet opened the FIFO's write end (so no EOF
      // would ever arrive on its own), and makes `kill` deterministic regardless
      // of how far the child had progressed.
      stdout.destroy();
    };

    // The pane's `tee` closes the FIFO write end on process exit, so `stdout`
    // hits EOF then 'close'; `kill` destroys it too. tmux does not surface the
    // pane process's exit code through the pipe, so the code/signal are null —
    // the channel still tells a caller the process is gone (SpawnHandle.exited).
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve) => {
        stdout.once('close', () => resolve({ code: null, signal: null }));
      },
    );
    exited.catch(() => {});

    return { stdout, stdin, kill, exited };
  }

  /**
   * Run a tool call to completion in a one-shot session. `bash` runs the command;
   * `glob`/`grep` compose the same shell shapes the InMemory + Local runtimes use,
   * so behavior is identical across runtimes.
   */
  async run(call: ToolCall): Promise<ToolResult> {
    if (this.destroyed) {
      throw new Error(`sandbox ${this.id} destroyed`);
    }

    if (call.tool === 'bash') {
      const args = call.args as { command: string; timeout_ms?: number };
      return this.runOneShot(args.command, this.root, args.timeout_ms);
    }

    if (call.tool === 'glob') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ? join(this.root, args.root.replace(/^\//, '')) : this.root;
      const result = await this.runOneShot(`compgen -G ${shellQuote(args.pattern)} || true`, cwd);
      const matches = (result.stdout ?? '').split('\n').filter((s) => s.length > 0);
      return { output: matches };
    }

    if (call.tool === 'grep') {
      const args = call.args as { pattern: string; root?: string };
      const cwd = args.root ? join(this.root, args.root.replace(/^\//, '')) : this.root;
      const result = await this.runOneShot(`grep -rn ${shellQuote(args.pattern)} . || true`, cwd);
      return { output: result.stdout ?? '' };
    }

    return { exit_code: 127, stderr: `unknown tool: ${call.tool}` };
  }

  /**
   * The tmux transport runs panes as ordinary host processes with no privilege
   * boundary; a `sudo`/FUSE mount here would touch the host namespace. The
   * strategy factory never selects a FUSE strategy against `supportsFuse=false`
   * runtimes; throwing here is the second line of defense (matches InMemory).
   */
  async runPrivileged(
    _cmd: string,
    _opts?: { envs?: Record<string, string> },
  ): Promise<ToolResult> {
    throw new Error('TmuxSandboxRuntime does not support privileged operations');
  }

  async pause(): Promise<void> {
    /* no-op: the tmux session + work-dir persist across calls */
  }

  async resume(): Promise<void> {
    /* no-op */
  }

  async destroy(): Promise<void> {
    if (this.destroyed) {
      return;
    }
    this.destroyed = true;
    // Kill the whole server (every session on this socket) in one shot; ignore a
    // "no server running" error when nothing was ever spawned.
    await this.tmux(['kill-server']).catch(() => {});
    this.sessions.clear();
    this.terminals.clear();
    rmSync(this.root, { recursive: true, force: true });
  }

  /** Test helper: the per-handle work-dir root (mirrors InMemory `rootDir`). */
  rootDir(): string {
    return this.root;
  }

  // --- TerminalHost: interactive panes for the sys_terminal_* toolset ---------
  //
  // Where `spawn` runs a program piped through `tee` into a FIFO (clean bytes for
  // a stream-json normalizer), an interactive terminal must be driven by hand and
  // read as it RENDERS. So these launch the program as the pane's FOREGROUND job
  // (no pipe — so `send-keys` reaches its stdin and a `C-c` chord raises SIGINT in
  // it) and read it back with `capture-pane` (the rendered grid), not a FIFO.

  /**
   * Launch an interactive program in a fresh detached tmux session and register
   * it as a terminal. The command runs directly as the pane's foreground process,
   * so keystrokes/chords sent later reach it. Returns the session name as the
   * public terminal id.
   */
  async launchTerminal(opts: LaunchTerminalOptions): Promise<{ terminalId: string }> {
    if (this.destroyed) {
      throw new Error(`sandbox ${this.id} destroyed`);
    }
    const terminalId = `t${++this.terminalSeq}`;
    const cwd = opts.cwd ? join(this.root, opts.cwd.replace(/^\//, '')) : this.root;
    await this.tmux([
      'new-session',
      '-d',
      '-s',
      terminalId,
      '-x',
      String(opts.cols ?? 200),
      '-y',
      String(opts.rows ?? 50),
      '-c',
      cwd,
      ...envAssignments(opts.env),
      opts.command,
    ]);
    this.sessions.add(terminalId);
    this.terminals.set(terminalId, { command: opts.command });
    return { terminalId };
  }

  /**
   * Deliver text and/or key chords to a terminal's pane. Literal `text` is typed
   * first (chunked under tmux's per-command cap), then each `keys` entry is sent
   * as a NAMED tmux key (`C-c`, `Enter`, `Up`, …) — the split is what lets a caller
   * both type characters and press control chords. `enter: true` appends an Enter
   * after the text to submit a line.
   */
  async sendTerminalKeys(terminalId: string, opts: SendTerminalOptions): Promise<void> {
    this.assertTerminal(terminalId);
    if (opts.text !== undefined && opts.text.length > 0) {
      await this.sendLiteral(terminalId, opts.text);
    }
    if (opts.enter === true) {
      await this.tmux(['send-keys', '-t', terminalId, 'Enter']);
    }
    for (const key of opts.keys ?? []) {
      // A named key is sent WITHOUT `-l`, so tmux interprets `C-c`/`Enter`/`Up`
      // as the key chord rather than typing those characters literally.
      await this.tmux(['send-keys', '-t', terminalId, key]);
    }
  }

  /**
   * Capture the rendered pane as plain text. `capture-pane -p` prints the visible
   * grid to stdout; `-S -<N>` extends the capture N lines back into the pane's
   * history so output that scrolled off-screen is recovered. No `-e`, so the text
   * is free of terminal escape sequences.
   */
  async readTerminal(terminalId: string, opts?: ReadTerminalOptions): Promise<string> {
    this.assertTerminal(terminalId);
    const args = ['capture-pane', '-p', '-t', terminalId];
    const scrollback = opts?.scrollbackLines ?? 0;
    if (scrollback > 0) {
      // `-S` is the start line; a negative value counts back from the visible top.
      args.push('-S', `-${scrollback}`);
    }
    return this.tmuxCapture(args);
  }

  /**
   * List the terminals this handle launched, with liveness. The launch command
   * comes from {@link terminals} (tmux does not retain it); `alive` is a
   * `has-session` probe, so a program that exited on its own reads `alive:false`
   * until it is closed.
   */
  async listTerminals(): Promise<TerminalDescriptor[]> {
    const out: TerminalDescriptor[] = [];
    for (const [terminalId, meta] of this.terminals) {
      out.push({ terminalId, command: meta.command, alive: await this.hasSession(terminalId) });
    }
    return out;
  }

  /**
   * Close a terminal: kill its session and forget it. Idempotent — an unknown or
   * already-gone id is a no-op (the `kill-session` error is swallowed), matching
   * the `delete`/`kill` idempotency the rest of the toolset provides.
   */
  async closeTerminal(terminalId: string): Promise<void> {
    if (!this.terminals.has(terminalId)) {
      return;
    }
    await this.tmux(['kill-session', '-t', terminalId]).catch(() => {});
    this.sessions.delete(terminalId);
    this.terminals.delete(terminalId);
  }

  /**
   * Attach to a terminal's LIVE pty and stream its raw output bytes.
   *
   * Uses `tmux pipe-pane -O` to tee the pane's OUTPUT into a private FIFO: this
   * runtime reads the FIFO (the exact bytes a locally-attached operator's terminal
   * would render — escape sequences and all) and hands each chunk to `onData`. That
   * is deliberately DIFFERENT from {@link readTerminal}, which returns the
   * capture-pane rendered grid: an interactive remote attach needs the live pty
   * byte stream, not a periodic snapshot.
   *
   * The returned {@link AttachedTerminal} lets the caller type raw input bytes
   * (`send-keys -H`, faithful hex so control bytes and non-ASCII pass through
   * unmangled), resize the pane (`resize-window`), and detach (toggle `pipe-pane`
   * off + close the reader) WITHOUT killing the terminal.
   */
  async attachTerminal(terminalId: string, opts: AttachTerminalOptions): Promise<AttachedTerminal> {
    this.assertTerminal(terminalId);
    const fifoPath = join(this.runtimeDir, `${terminalId}.attach.${++this.attachSeq}`);
    makeFifo(fifoPath);

    // Open the FIFO read end BEFORE starting the writer so no early bytes are
    // lost. `createReadStream` opens on the libuv threadpool (the event loop keeps
    // running) and stays open across writer opens/closes (a FIFO reader does not
    // EOF just because one writer closed) until we destroy it on detach.
    const reader = createReadStream(fifoPath);
    // The `'data'` event types its chunk as `Buffer | string` (a stream *could*
    // be string-mode via `setEncoding`), but this FIFO reader is never given an
    // encoding, so every chunk is a raw `Buffer` — normalize defensively so the
    // handler stays type-safe and always hands `onData` bytes.
    reader.on('data', (chunk: Buffer | string) => {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      opts.onData(new Uint8Array(bytes));
    });
    reader.on('error', () => {
      /* the FIFO reader is torn down on detach; a late error is benign */
    });

    // `pipe-pane -O <cmd>` runs <cmd> with the pane's OUTPUT on its stdin; append
    // it to the FIFO so this runtime reads the live pane bytes.
    await this.tmux(['pipe-pane', '-O', '-t', terminalId, `cat >> ${shellQuote(fifoPath)}`]);

    let detached = false;
    const write = (bytes: Uint8Array): void => {
      if (detached || this.destroyed || bytes.length === 0) {
        return;
      }
      // `send-keys -H` takes SPACE-separated hex byte values, so every raw input
      // byte (control chars, UTF-8 continuation bytes) reaches the pty verbatim —
      // no literal-vs-key ambiguity. Fire-and-forget; a gone pane is swallowed.
      const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0'));
      this.tmux(['send-keys', '-H', '-t', terminalId, ...hex]).catch(() => {});
    };
    const resize = async (cols: number, rows: number): Promise<void> => {
      if (detached || this.destroyed) {
        return;
      }
      await this.tmux([
        'resize-window',
        '-t',
        terminalId,
        '-x',
        String(cols),
        '-y',
        String(rows),
      ]).catch(() => {});
    };
    const detach = async (): Promise<void> => {
      if (detached) {
        return;
      }
      detached = true;
      // `pipe-pane` with NO command toggles the pane's pipe OFF, stopping the tee.
      await this.tmux(['pipe-pane', '-t', terminalId]).catch(() => {});
      reader.destroy();
      rmSync(fifoPath, { force: true });
    };

    return { write, resize, detach };
  }

  /** Throw a clear error for an unknown terminal id (send/read address-by-id). */
  private assertTerminal(terminalId: string): void {
    if (this.destroyed) {
      throw new Error(`sandbox ${this.id} destroyed`);
    }
    if (!this.terminals.has(terminalId)) {
      throw new Error(`unknown terminal: ${terminalId}`);
    }
  }

  /** Whether a tmux session is still alive (0 exit from `has-session`). */
  private hasSession(session: string): Promise<boolean> {
    return new Promise((resolve) => {
      execFile('tmux', [...this.tmuxBase(), 'has-session', '-t', session], (err) => {
        resolve(err === null);
      });
    });
  }

  /**
   * Run a command to completion in a detached, self-closing session and collect
   * its stdout/stderr/exit code. The pane writes three artifacts under the
   * runtime dir — stdout, stderr, and a `rc` file — then the session ends; this
   * polls for `rc` (written last, so its presence means the command finished),
   * then reads the buffers back. This keeps `run` fully sandbox-local: no reliance
   * on tmux's own capture buffer, which is PTY-rendered.
   */
  private async runOneShot(command: string, cwd: string, timeoutMs?: number): Promise<ToolResult> {
    const seq = ++this.spawnSeq;
    const session = `r${seq}`;
    const outPath = join(this.runtimeDir, `${session}.out`);
    const errPath = join(this.runtimeDir, `${session}.err`);
    const rcPath = join(this.runtimeDir, `${session}.rc`);
    // Run the command in a SUBSHELL with stdout/stderr redirected to files, then
    // record the exit code LAST so `rcPath` existing implies completion. The
    // subshell is load-bearing: a command that calls `exit` must terminate only
    // the subshell, leaving the pane's shell alive to write the rc sentinel.
    const wrapped =
      `( ${command} ) >${shellQuote(outPath)} 2>${shellQuote(errPath)}; ` +
      `printf '%s' "$?" >${shellQuote(rcPath)}`;
    await this.tmux(['new-session', '-d', '-s', session, '-c', cwd, wrapped]);
    this.sessions.add(session);

    const deadline = timeoutMs && timeoutMs > 0 ? Date.now() + timeoutMs : undefined;
    // Poll for the rc file. tmux itself is not consulted for output; we only wait
    // for the sentinel the wrapper writes when the command has finished.
    for (;;) {
      let rcText: string | undefined;
      try {
        rcText = readFileSync(rcPath, 'utf8');
      } catch {
        rcText = undefined;
      }
      if (rcText !== undefined && rcText.length > 0) {
        this.sessions.delete(session);
        return {
          stdout: safeReadText(outPath),
          stderr: safeReadText(errPath),
          exit_code: Number.parseInt(rcText, 10) || 0,
        };
      }
      if (deadline !== undefined && Date.now() >= deadline) {
        break;
      }
      await delay(20);
    }

    // The loop only breaks after the deadline passes (the completion path returns
    // above), so reaching here always means a timeout: kill the session and surface
    // exit code 124, matching the Local runtime's timeout convention.
    await this.tmux(['kill-session', '-t', session]).catch(() => {});
    this.sessions.delete(session);
    return {
      stdout: safeReadText(outPath),
      stderr: `${safeReadText(errPath)}\n[orca: command timed out after ${String(timeoutMs)}ms]\n`,
      exit_code: 124,
    };
  }

  /** Deliver one write chunk to a session's pane via `send-keys`, line by line. */
  private async sendKeys(session: string, chunk: string): Promise<void> {
    if (this.destroyed) {
      return;
    }
    // A write may contain zero or more complete lines plus a trailing partial.
    // Each newline becomes an `Enter` key (a real submit); the partial is typed
    // literally without an Enter so a subsequent write can complete it.
    let start = 0;
    for (let i = 0; i < chunk.length; i++) {
      if (chunk[i] === '\n') {
        const line = chunk.slice(start, i);
        await this.sendLiteral(session, line);
        await this.tmux(['send-keys', '-t', session, 'Enter']);
        start = i + 1;
      }
    }
    if (start < chunk.length) {
      await this.sendLiteral(session, chunk.slice(start));
    }
  }

  /**
   * Deliver a real end-of-input to a session's pane by typing `Ctrl-D` — the
   * exact keystroke a hand-attaching operator would press. On the pane's line
   * discipline `C-d` at a line boundary raises EOF for the foreground reader, so
   * a child that reads its stdin to EOF (e.g. `cat`) actually terminates. This is
   * how {@link TmuxSendKeysStdin._final} turns the returned stdin's `end()` into
   * more than a local close: over the local-attach transport it is a genuine
   * best-effort EOF, not a no-op.
   *
   * Caveat, faithful to a real TTY: `C-d` only raises EOF when the input buffer is
   * empty (at a line start). Stream-json drivers always write whole `\n`-terminated
   * lines, so by the time `end()` runs the pane is at a line boundary and the EOF
   * lands. If a partial (un-`\n`-terminated) line were still buffered, this single
   * `C-d` would flush that partial to the reader without EOF — exactly as a real
   * terminal behaves — and stream-json CLIs are driven by newline messages rather
   * than EOF regardless, so this stays a best-effort courtesy signal.
   */
  private async sendEof(session: string): Promise<void> {
    if (this.destroyed) {
      return;
    }
    // A missing/already-gone session is a no-op we swallow: `end()` must never
    // throw just because the child had already exited (mirrors `kill`).
    await this.tmux(['send-keys', '-t', session, 'C-d']).catch(() => {});
  }

  /** Type literal text into the pane, chunked under tmux's per-command cap. */
  private async sendLiteral(session: string, text: string): Promise<void> {
    if (text.length === 0) {
      return;
    }
    for (let i = 0; i < text.length; i += SEND_KEYS_LITERAL_CHUNK) {
      const part = text.slice(i, i + SEND_KEYS_LITERAL_CHUNK);
      await this.tmux(['send-keys', '-l', '-t', session, part]);
    }
  }

  /** The base argv for every tmux call against this handle's private socket. */
  private tmuxBase(): string[] {
    // `-f /dev/null` ignores any operator ~/.tmux.conf so the transport is
    // deterministic regardless of the host's tmux configuration.
    return ['-S', this.socketPath, '-f', '/dev/null'];
  }

  /** Run one tmux command; reject with stderr on a non-zero exit. */
  private tmux(args: string[]): Promise<void> {
    return new Promise((resolve, reject) => {
      execFile('tmux', [...this.tmuxBase(), ...args], (err, _stdout, stderr) => {
        if (err) {
          reject(new Error(`tmux ${args[0] ?? ''} failed: ${stderr || err.message}`));
          return;
        }
        resolve();
      });
    });
  }

  /**
   * Run one tmux command and RETURN its stdout — used by `capture-pane`, whose
   * whole purpose is the captured text (the `tmux()` helper discards stdout). A
   * generous `maxBuffer` covers a large scrollback capture.
   */
  private tmuxCapture(args: string[]): Promise<string> {
    return new Promise((resolve, reject) => {
      execFile(
        'tmux',
        [...this.tmuxBase(), ...args],
        { maxBuffer: 64 * 1024 * 1024 },
        (err, stdout, stderr) => {
          if (err) {
            reject(new Error(`tmux ${args[0] ?? ''} failed: ${stderr || err.message}`));
            return;
          }
          resolve(stdout);
        },
      );
    });
  }
}

/**
 * A {@link Writable} that turns process stdin into tmux `send-keys`. Writes are
 * serialized through a promise chain so lines reach the pane strictly in order
 * even though each `send-keys` is an async subprocess. Closing the stream
 * (`end()`) types a final `Ctrl-D` via {@link _final}, delivering a best-effort
 * EOF to the pane's foreground reader (see {@link TmuxSandboxHandle.sendEof}).
 */
class TmuxSendKeysStdin extends Writable {
  private chain: Promise<void> = Promise.resolve();

  constructor(
    private readonly deliver: (chunk: string) => Promise<void>,
    private readonly deliverEof: () => Promise<void>,
  ) {
    super();
  }

  override _write(
    chunk: Buffer | string,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    const text = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk);
    // Append to the ordered chain; only THIS write's completion calls back, so
    // backpressure is honored per chunk.
    this.chain = this.chain.then(() => this.deliver(text));
    this.chain.then(
      () => callback(),
      (err: Error) => callback(err),
    );
  }

  /**
   * On `end()`, type a `Ctrl-D` AFTER every queued write has been delivered, so
   * the EOF keystroke lands at a line boundary behind all prior input. Chaining
   * it onto the same promise as `_write` preserves that strict ordering. EOF
   * delivery is best-effort and never rejects (a gone session is swallowed
   * upstream), so `end()` cannot throw on a child that had already exited.
   */
  override _final(callback: (error?: Error | null) => void): void {
    this.chain = this.chain.then(() => this.deliverEof());
    this.chain.then(
      () => callback(),
      (err: Error) => callback(err),
    );
  }
}

/** Create a FIFO at `path` via `mkfifo` (POSIX; not exposed by `node:fs`). */
function makeFifo(path: string): void {
  const res = spawnSync('mkfifo', [path], { encoding: 'utf8' });
  if (res.status !== 0) {
    throw new Error(`mkfifo failed for ${path}: ${res.stderr ?? ''}`);
  }
}

/** Read a text file, returning '' if it is missing (used by the one-shot path). */
function safeReadText(path: string): string {
  try {
    return readFileSync(path, 'utf8');
  } catch {
    return '';
  }
}

/** `KEY=VALUE` args for `new-session`'s environment (tmux `-e` flags). */
function envAssignments(env?: Record<string, string>): string[] {
  if (!env) {
    return [];
  }
  const out: string[] = [];
  for (const [k, v] of Object.entries(env)) {
    out.push('-e', `${k}=${v}`);
  }
  return out;
}

/** POSIX single-quote a string for safe interpolation into a shell command. */
function shellQuote(s: string): string {
  return `'${s.replace(/'/g, "'\\''")}'`;
}

/** A cancellable-free delay used by the one-shot poll loop. */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
