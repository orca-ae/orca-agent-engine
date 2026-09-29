// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the worker tunnel frame protocol.
//
// Worker frames are the control + filesystem-op messages the registry sends a
// connected worker over the worker tunnel (launch/stop runner, runner-exited
// reports, stat / list-dir / worktree / create-dir ops), plus the worker hello.
// Each frame kind round-trips through encode → decode unchanged; decode rejects
// malformed input cleanly. The dot-separated wire `kind` strings (e.g.
// `"worker.launch_runner"`) and the snake_case wire keys are the cross-component
// contract and are asserted byte-for-byte.

import { describe, it, expect } from 'vitest';
import {
  HARNESS_NOT_CONFIGURED_ERROR_CODE,
  WorkerFrameKind,
  type WorkerCreateDirFrame,
  type WorkerCreateDirResultFrame,
  type WorkerCreateWorktreeFrame,
  type WorkerCreateWorktreeResultFrame,
  type WorkerFrame,
  type WorkerHelloFrame,
  type WorkerLaunchRunnerFrame,
  type WorkerLaunchRunnerResultFrame,
  type WorkerListDirFrame,
  type WorkerListDirResultFrame,
  type WorkerRemoveWorktreeFrame,
  type WorkerRemoveWorktreeResultFrame,
  type WorkerRunnerExitedFrame,
  type WorkerStatFrame,
  type WorkerStatResultFrame,
  type WorkerStopRunnerFrame,
  type WorkerStopRunnerResultFrame,
  decodeWorkerFrame,
  encodeWorkerFrame,
} from '../../src/worker-frames.js';

// ── worker.hello ───────────────────────────────────────────

describe('worker.hello', () => {
  it('hello frame round trips', () => {
    // If any field is dropped or garbled, the worker tunnel would register with
    // wrong capabilities or fail to reconcile runners on reconnect.
    const original: WorkerHelloFrame = {
      kind: WorkerFrameKind.Hello,
      version: '0.1.0',
      frameProtocolVersion: 1,
      name: 'workstation-01',
      runners: ['runner_token_aaa', 'runner_token_bbb'],
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original));
    expect(decoded.kind).toBe(WorkerFrameKind.Hello);
    const h = decoded as WorkerHelloFrame;
    expect(h.version).toBe('0.1.0');
    expect(h.frameProtocolVersion).toBe(1);
    expect(h.name).toBe('workstation-01');
    expect(h.runners).toEqual(['runner_token_aaa', 'runner_token_bbb']);
  });

  it('hello frame with no runners decodes to an empty list', () => {
    // First connect has no runners; the field must default cleanly.
    const original: WorkerHelloFrame = {
      kind: WorkerFrameKind.Hello,
      version: '0.1.0',
      frameProtocolVersion: 1,
      name: 'laptop',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerHelloFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.Hello);
    expect(decoded.runners).toEqual([]);
  });

  it('hello frame configured_harnesses map round trips with exact values', () => {
    // If a key or bool is dropped/garbled, the server would persist a wrong
    // readiness map and the web picker would warn about the wrong harnesses (or
    // miss a real warning).
    const original: WorkerHelloFrame = {
      kind: WorkerFrameKind.Hello,
      version: '0.1.0',
      frameProtocolVersion: 1,
      name: 'workstation-01',
      configuredHarnesses: { 'claude-sdk': true, codex: false },
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerHelloFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.Hello);
    // Exact map equality: both the true and the false must survive — false is
    // the actionable "warn the user" value.
    expect(decoded.configuredHarnesses).toEqual({ 'claude-sdk': true, codex: false });
  });

  it('legacy hello payload (no configured_harnesses key) decodes the field as null', () => {
    // If this decoded to {} or raised, every pre-upgrade worker would either fail
    // its handshake or read as "nothing configured" and spuriously warn on all
    // agents. null means "unknown", never a map.
    const legacy = JSON.stringify({
      kind: 'worker.hello',
      version: '0.1.0',
      frame_protocol_version: 1,
      name: 'old-laptop',
      runners: [],
    });
    const decoded = decodeWorkerFrame(legacy) as WorkerHelloFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.Hello);
    expect(decoded.configuredHarnesses).toBeNull();
  });

  it('non-object configured_harnesses value decodes as null instead of raising', () => {
    // The hello is the handshake frame — a peer sending a bad value for this
    // advisory field must not break the whole tunnel connection.
    const malformed = JSON.stringify({
      kind: 'worker.hello',
      version: '0.1.0',
      frame_protocol_version: 1,
      name: 'laptop',
      runners: [],
      configured_harnesses: ['claude-sdk'],
    });
    const decoded = decodeWorkerFrame(malformed) as WorkerHelloFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.Hello);
    expect(decoded.configuredHarnesses).toBeNull();
  });
});

// ── worker.launch_runner ───────────────────────────────────

describe('worker.launch_runner', () => {
  it('launch_runner frame round trips', () => {
    // If binding_token is garbled, the runner would connect with a wrong
    // identity and the session binding would fail.
    const original: WorkerLaunchRunnerFrame = {
      kind: WorkerFrameKind.LaunchRunner,
      requestId: 'req_001',
      bindingToken: 'secret_token_xyz',
      workspace: '/home/dev/projects/frontend',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerLaunchRunnerFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.LaunchRunner);
    expect(decoded.requestId).toBe('req_001');
    expect(decoded.bindingToken).toBe('secret_token_xyz');
    expect(decoded.workspace).toBe('/home/dev/projects/frontend');
  });

  it('launch_runner harness field survives encode → decode', () => {
    // If harness is dropped, the worker's pre-spawn configuration check silently
    // never runs (null skips it) and unconfigured launches regress to dying
    // inside the executor.
    const original: WorkerLaunchRunnerFrame = {
      kind: WorkerFrameKind.LaunchRunner,
      requestId: 'req_001',
      bindingToken: 'secret_token_xyz',
      workspace: '/home/dev/projects/frontend',
      harness: 'claude-sdk',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerLaunchRunnerFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.LaunchRunner);
    expect(decoded.harness).toBe('claude-sdk');
  });

  it('legacy launch payload (no harness key) decodes harness=null (fail open)', () => {
    // If this raised, a new worker could not serve launches from an older server
    // at all.
    const legacy = JSON.stringify({
      kind: 'worker.launch_runner',
      request_id: 'req_001',
      binding_token: 'tok',
      workspace: '/w',
    });
    const decoded = decodeWorkerFrame(legacy) as WorkerLaunchRunnerFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.LaunchRunner);
    expect(decoded.harness).toBeNull();
  });
});

// ── worker.launch_runner_result ────────────────────────────

describe('worker.launch_runner_result', () => {
  it('success result round trips', () => {
    // The server awaits this frame to confirm the runner was spawned. If status
    // or runner_id is wrong, the binding flow stalls or binds the wrong runner.
    const original: WorkerLaunchRunnerResultFrame = {
      kind: WorkerFrameKind.LaunchRunnerResult,
      requestId: 'req_001',
      status: 'launched',
      runnerId: 'runner_token_abc',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerLaunchRunnerResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.LaunchRunnerResult);
    expect(decoded.requestId).toBe('req_001');
    expect(decoded.status).toBe('launched');
    expect(decoded.runnerId).toBe('runner_token_abc');
    expect(decoded.error).toBeNull();
  });

  it('failure result preserves the error message', () => {
    // If error is dropped, the server can't report why the launch failed to the
    // user.
    const original: WorkerLaunchRunnerResultFrame = {
      kind: WorkerFrameKind.LaunchRunnerResult,
      requestId: 'req_001',
      status: 'failed',
      error: 'workspace path does not exist',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerLaunchRunnerResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.LaunchRunnerResult);
    expect(decoded.status).toBe('failed');
    expect(decoded.runnerId).toBeNull();
    expect(decoded.error).toBe('workspace path does not exist');
  });

  it('error_code survives encode → decode', () => {
    // The server keys its 412 mapping on this exact string — if it's dropped, an
    // unconfigured-harness refusal degrades to the generic warn-and-return-200
    // path and the user never sees the setup recommendation.
    const original: WorkerLaunchRunnerResultFrame = {
      kind: WorkerFrameKind.LaunchRunnerResult,
      requestId: 'req_001',
      status: 'failed',
      error: "harness 'codex' is not configured",
      errorCode: HARNESS_NOT_CONFIGURED_ERROR_CODE,
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerLaunchRunnerResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.LaunchRunnerResult);
    expect(decoded.errorCode).toBe('harness_not_configured');
    expect(decoded.error).toBe("harness 'codex' is not configured");
  });

  it('legacy result payload (no error_code key) decodes error_code=null', () => {
    // null must mean "uncategorized failure" so the server keeps the existing
    // generic failure handling for pre-upgrade hosts.
    const legacy = JSON.stringify({
      kind: 'worker.launch_runner_result',
      request_id: 'req_001',
      status: 'failed',
      error: 'boom',
    });
    const decoded = decodeWorkerFrame(legacy) as WorkerLaunchRunnerResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.LaunchRunnerResult);
    expect(decoded.errorCode).toBeNull();
  });

  it('exports the harness_not_configured error-code constant byte-for-byte', () => {
    // Shared by the daemon (producer), server, and tests — the literal string is
    // the cross-component contract.
    expect(HARNESS_NOT_CONFIGURED_ERROR_CODE).toBe('harness_not_configured');
  });
});

// ── worker.stop_runner ─────────────────────────────────────

describe('worker.stop_runner', () => {
  it('stop_runner frame round trips', () => {
    // If runner_id is garbled, the worker would kill the wrong process.
    const original: WorkerStopRunnerFrame = {
      kind: WorkerFrameKind.StopRunner,
      requestId: 'req_002',
      runnerId: 'runner_token_abc',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerStopRunnerFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.StopRunner);
    expect(decoded.requestId).toBe('req_002');
    expect(decoded.runnerId).toBe('runner_token_abc');
  });

  it('stop_runner_result frame round trips', () => {
    const original: WorkerStopRunnerResultFrame = {
      kind: WorkerFrameKind.StopRunnerResult,
      requestId: 'req_002',
      status: 'stopped',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerStopRunnerResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.StopRunnerResult);
    expect(decoded.requestId).toBe('req_002');
    expect(decoded.status).toBe('stopped');
    expect(decoded.error).toBeNull();
  });
});

// ── worker.runner_exited ───────────────────────────────────

describe('worker.runner_exited', () => {
  it('runner_exited frame round trips', () => {
    // This frame carries the failure cause (exit code + log tail) from the worker
    // daemon to the server. A lossy round-trip means a crashed runner's error is
    // mangled or dropped before it ever reaches the waiting client.
    const original: WorkerRunnerExitedFrame = {
      kind: WorkerFrameKind.RunnerExited,
      runnerId: 'runner_abc123',
      error:
        'runner process exited with code 1 (log on worker: ~/x.log)\n' +
        '--- runner log tail ---\nRuntimeError: boom',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerRunnerExitedFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.RunnerExited);
    expect(decoded.runnerId).toBe('runner_abc123');
    // The multi-line error (including the log tail) must survive intact.
    expect(decoded.error).toBe(original.error);
  });

  it('runner_exited frame without error fails to decode', () => {
    // error is the entire payload of this report — accepting a frame without it
    // would record an empty cause and the client would fail with a blank message.
    expect(() =>
      decodeWorkerFrame('{"kind": "worker.runner_exited", "runner_id": "runner_abc123"}'),
    ).toThrow(/missing required string field/);
  });
});

// ── Generic decode / encode failure modes ────────────────

describe('decode / encode failure modes', () => {
  it('rejects an unknown frame kind', () => {
    // The worker tunnel must reject frames it doesn't understand rather than
    // silently ignoring them.
    expect(() => decodeWorkerFrame('{"kind": "worker.unknown_frame"}')).toThrow(
      /unknown worker frame kind/,
    );
  });

  it('rejects a frame without a kind field', () => {
    // A kindless frame is malformed — it must not parse as any frame type.
    expect(() => decodeWorkerFrame('{"version": "0.1.0"}')).toThrow(/missing 'kind' field/);
  });

  it('rejects a frame missing a required field', () => {
    // If required fields aren't validated, a frame with missing data would build
    // an object with null where a str is expected, causing downstream crashes.
    expect(() =>
      decodeWorkerFrame('{"kind": "worker.hello", "frame_protocol_version": 1, "name": "laptop"}'),
    ).toThrow(/missing required string field/);
  });

  it('rejects malformed JSON', () => {
    expect(() => decodeWorkerFrame('not json at all')).toThrow(/not valid JSON/);
  });

  it('rejects a non-object root', () => {
    expect(() => decodeWorkerFrame('[1, 2, 3]')).toThrow(/must be a JSON object/);
  });

  it('rejects an unknown frame type on encode', () => {
    expect(() => encodeWorkerFrame({ kind: 'not.a.frame' } as unknown as WorkerFrame)).toThrow(
      /unknown worker frame type/,
    );
  });

  it('rejects a result status outside its literal union', () => {
    // Status discriminators are membership-CHECKED at decode, like `kind`: a
    // typo'd or future status ('lauched', 'ok' on a launch result) must fail
    // loudly, not decode into a value the type claims is a known literal and
    // turn every launch into a silent failure branch downstream.
    expect(() =>
      decodeWorkerFrame(
        JSON.stringify({
          kind: 'worker.launch_runner_result',
          request_id: 'r',
          status: 'lauched',
          runner_id: 'runner_x',
        }),
      ),
    ).toThrow(/"status" must be "launched" \| "failed"/);
    expect(() =>
      decodeWorkerFrame(
        JSON.stringify({ kind: 'worker.stop_runner_result', request_id: 'r', status: 'ok' }),
      ),
    ).toThrow(/"status" must be/);
    expect(() =>
      decodeWorkerFrame(
        JSON.stringify({
          kind: 'worker.stat_result',
          request_id: 'r',
          status: 'success',
          exists: true,
        }),
      ),
    ).toThrow(/"status" must be/);
  });

  it('rejects a launched result without runner_id', () => {
    // The discriminated union's structural invariant: a success MUST name the
    // runner it spawned (the registry stores it against the session).
    expect(() =>
      decodeWorkerFrame(
        JSON.stringify({
          kind: 'worker.launch_runner_result',
          request_id: 'r',
          status: 'launched',
        }),
      ),
    ).toThrow(/must carry 'runner_id'/);
  });

  it('normalizes cross-arm fields: error on launched and runner_id on failed decode to null', () => {
    const launched = decodeWorkerFrame(
      JSON.stringify({
        kind: 'worker.launch_runner_result',
        request_id: 'r',
        status: 'launched',
        runner_id: 'runner_x',
        error: 'stray warning text',
      }),
    ) as WorkerLaunchRunnerResultFrame;
    expect(launched.error).toBeNull();
    const failed = decodeWorkerFrame(
      JSON.stringify({
        kind: 'worker.launch_runner_result',
        request_id: 'r',
        status: 'failed',
        runner_id: 'runner_should_not_be_here',
        error: 'boom',
      }),
    ) as WorkerLaunchRunnerResultFrame;
    expect(failed.runnerId).toBeNull();
  });

  it('rejects a filesystem entry type outside directory|file|other', () => {
    expect(() =>
      decodeWorkerFrame(
        JSON.stringify({
          kind: 'worker.stat_result',
          request_id: 'r',
          status: 'ok',
          exists: true,
          type: 'symlink',
        }),
      ),
    ).toThrow(/"type" must be/);
    expect(() =>
      decodeWorkerFrame(
        JSON.stringify({
          kind: 'worker.list_dir_result',
          request_id: 'r',
          status: 'ok',
          entries: [{ name: 'x', path: '/x', type: 'socket', bytes: null, modified_at: 1 }],
        }),
      ),
    ).toThrow(/"type" must be/);
  });
});

// ── worker.stat frames ─────────────────────────────────────

describe('worker.stat', () => {
  it('stat request frame round trips', () => {
    // Pins the wire shape that session-create validation relies on: a single
    // `path` field that may be absolute or tilde-prefixed. If this field name or
    // type drifts, the validation flow can't talk to the worker.
    const original: WorkerStatFrame = {
      kind: WorkerFrameKind.Stat,
      requestId: 'req_stat_1',
      path: '/home/dev/universe',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerStatFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.Stat);
    expect(decoded.requestId).toBe('req_stat_1');
    expect(decoded.path).toBe('/home/dev/universe');
  });

  it('stat request round-trips a tilde-prefixed path verbatim', () => {
    // The worker (not the server) is the source of truth for `~` expansion. The
    // frame must therefore preserve tildes through the wire so the worker's stat
    // handler can do the expansion. If the encoder silently expands tildes,
    // server-side resolution would diverge from the worker's process owner.
    const original: WorkerStatFrame = {
      kind: WorkerFrameKind.Stat,
      requestId: 'req_stat_tilde',
      path: '~/projects',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerStatFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.Stat);
    expect(decoded.path).toBe('~/projects');
  });

  it('stat_result for an existing directory round trips', () => {
    // Three properties matter for the server-side validator: exists is true,
    // type is "directory", and canonical_path carries the realpath. The
    // workspace boundary check operates on canonical_path; if that field is
    // dropped, every worker-launched session would be rejected as "outside
    // boundary".
    const original: WorkerStatResultFrame = {
      kind: WorkerFrameKind.StatResult,
      requestId: 'req_stat_2',
      status: 'ok',
      exists: true,
      type: 'directory',
      canonicalPath: '/home/dev/universe',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerStatResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.StatResult);
    expect(decoded.requestId).toBe('req_stat_2');
    expect(decoded.status).toBe('ok');
    expect(decoded.exists).toBe(true);
    expect(decoded.type).toBe('directory');
    expect(decoded.canonicalPath).toBe('/home/dev/universe');
    expect(decoded.error).toBeNull();
  });

  it('stat_result for a non-existent path round trips', () => {
    // When exists is false, type and canonical_path must both be null. If a
    // stale canonical_path carried over (e.g. from the input path), the server
    // might store a session row pointing at a phantom directory — exactly the
    // orphan-session scenario session-create validation is meant to prevent.
    const original: WorkerStatResultFrame = {
      kind: WorkerFrameKind.StatResult,
      requestId: 'req_stat_3',
      status: 'ok',
      exists: false,
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerStatResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.StatResult);
    expect(decoded.exists).toBe(false);
    expect(decoded.type).toBeNull();
    expect(decoded.canonicalPath).toBeNull();
    expect(decoded.error).toBeNull();
  });

  it('stat_result survives encode → decode for I/O failures', () => {
    // status: "failed" is reserved for unexpected errors (EIO, etc.). EACCES and
    // ENOENT both fold into status: "ok", exists: false per the design. The error
    // message must survive so the server can surface it.
    const original: WorkerStatResultFrame = {
      kind: WorkerFrameKind.StatResult,
      requestId: 'req_stat_4',
      status: 'failed',
      exists: false,
      error: 'I/O error reading filesystem',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerStatResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.StatResult);
    expect(decoded.status).toBe('failed');
    expect(decoded.error).toBe('I/O error reading filesystem');
  });

  it('stat_result without exists raises', () => {
    // exists is the load-bearing bit for validation. A frame that omits it would
    // cause silent false defaulting and every legitimate path would fail
    // validation. Decoding must fail loud instead.
    expect(() =>
      decodeWorkerFrame('{"kind": "worker.stat_result", "request_id": "r", "status": "ok"}'),
    ).toThrow(/missing required bool field/);
  });

  it('stat request without path raises', () => {
    // Without path the worker has no way to know what to stat; a default-to-empty
    // would silently stat the worker process's cwd and return misleading data.
    // Failing loud preserves safety.
    expect(() => decodeWorkerFrame('{"kind": "worker.stat", "request_id": "r"}')).toThrow(
      /missing required string field/,
    );
  });
});

// ── worker.list_dir frames ─────────────────────────────────

describe('worker.list_dir', () => {
  it('list_dir request frame round trips', () => {
    // Pins the wire shape used by the directory picker: `path` plus pagination
    // fields (limit / after / before).
    const original: WorkerListDirFrame = {
      kind: WorkerFrameKind.ListDir,
      requestId: 'req_list_1',
      path: '/home/dev/projects',
      limit: 20,
      after: null,
      before: null,
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerListDirFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.ListDir);
    expect(decoded.requestId).toBe('req_list_1');
    expect(decoded.path).toBe('/home/dev/projects');
    expect(decoded.limit).toBe(20);
    expect(decoded.after).toBeNull();
    expect(decoded.before).toBeNull();
  });

  it('list_dir pagination cursors round trip', () => {
    // Without round-tripping, the Web UI's "next page" / "prev page" cursors
    // would silently degrade (always returning the first page).
    const original: WorkerListDirFrame = {
      kind: WorkerFrameKind.ListDir,
      requestId: 'req_list_2',
      path: '/foo',
      limit: 10,
      after: '/foo/m',
      before: null,
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerListDirFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.ListDir);
    expect(decoded.limit).toBe(10);
    expect(decoded.after).toBe('/foo/m');
  });

  it('list_dir tilde-prefixed path round trips verbatim', () => {
    // The worker (not the server) is the source of truth for `~` — same rules as
    // worker.stat. The frame must preserve tildes so the worker's list_dir handler
    // can expand against its own process owner.
    const original: WorkerListDirFrame = {
      kind: WorkerFrameKind.ListDir,
      requestId: 'req_list_tilde',
      path: '~/projects',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerListDirFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.ListDir);
    expect(decoded.path).toBe('~/projects');
  });

  it('list_dir defaults limit to 20 and cursors to null when omitted', () => {
    // The picker's first page omits pagination; the field must default cleanly.
    const decoded = decodeWorkerFrame(
      JSON.stringify({ kind: 'worker.list_dir', request_id: 'r', path: '/p' }),
    ) as WorkerListDirFrame;
    expect(decoded.limit).toBe(20);
    expect(decoded.after).toBeNull();
    expect(decoded.before).toBeNull();
  });

  it('list_dir_result round trips with multiple entry types', () => {
    // Each entry must carry name, absolute path, type, optional bytes, and
    // modified_at. If any field is dropped or mis-typed, the Web UI's tree view
    // would render with missing data.
    const original: WorkerListDirResultFrame = {
      kind: WorkerFrameKind.ListDirResult,
      requestId: 'req_list_3',
      status: 'ok',
      entries: [
        {
          name: 'src',
          path: '/home/dev/foo/src',
          type: 'directory',
          bytes: null,
          modifiedAt: 1779980000,
        },
        {
          name: 'README.md',
          path: '/home/dev/foo/README.md',
          type: 'file',
          bytes: 1234,
          modifiedAt: 1779980100,
        },
      ],
      hasMore: false,
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerListDirResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.ListDirResult);
    expect(decoded.status).toBe('ok');
    expect(decoded.hasMore).toBe(false);
    // `entries` is optional on the type (the encoder substitutes `[]`) but the
    // decoder always returns it populated; narrow to a definite array before
    // indexing so the element assertions stay type-sound.
    const entries = decoded.entries ?? [];
    expect(entries.length).toBe(2);
    expect(entries[0]!.type).toBe('directory');
    expect(entries[0]!.bytes).toBeNull();
    expect(entries[1]!.type).toBe('file');
    expect(entries[1]!.bytes).toBe(1234);
  });

  it('list_dir_result with an empty entry list round trips', () => {
    // Empty directories are common (a fresh project, a clean checkout). If the
    // encoder drops empty arrays, the Web UI would crash trying to iterate null.
    const original: WorkerListDirResultFrame = {
      kind: WorkerFrameKind.ListDirResult,
      requestId: 'req_list_empty',
      status: 'ok',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerListDirResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.ListDirResult);
    expect(decoded.entries).toEqual([]);
    expect(decoded.hasMore).toBe(false);
  });

  it('list_dir_result failure survives encode → decode with the error intact', () => {
    // Without the error message, the route layer can't surface "path does not
    // exist" etc. to the user — it would have to fall back to a generic 500.
    const original: WorkerListDirResultFrame = {
      kind: WorkerFrameKind.ListDirResult,
      requestId: 'req_list_fail',
      status: 'failed',
      error: 'scandir failed: I/O error',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerListDirResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.ListDirResult);
    expect(decoded.status).toBe('failed');
    expect(decoded.error).toBe('scandir failed: I/O error');
    expect(decoded.entries).toEqual([]);
  });

  it('list_dir request without path raises', () => {
    // Without path the worker has nothing to list; a default-to-cwd fallback would
    // silently stat the worker process's working dir and return misleading data.
    expect(() => decodeWorkerFrame('{"kind": "worker.list_dir", "request_id": "r"}')).toThrow(
      /missing required string field/,
    );
  });

  it('list_dir_result entry without modified_at raises', () => {
    // The Web UI sorts entries by mtime; a missing field would make the sort
    // silently inconsistent across pages.
    const bad =
      '{"kind": "worker.list_dir_result", "request_id": "r", "status": "ok", ' +
      '"entries": [{"name": "x", "path": "/x", "type": "file", "bytes": 1}], ' +
      '"has_more": false}';
    expect(() => decodeWorkerFrame(bad)).toThrow(/modified_at/);
  });
});

// ── worker.create_worktree / worker.remove_worktree frames ───

describe('worker.create_worktree / worker.remove_worktree', () => {
  it('create_worktree frame round trips', () => {
    // A garbled repo_path or branch_name would create the worktree in the wrong
    // place or with the wrong branch.
    const original: WorkerCreateWorktreeFrame = {
      kind: WorkerFrameKind.CreateWorktree,
      requestId: 'req_wt_1',
      repoPath: '/home/dev/projects/myrepo',
      branchName: 'feature/login',
      baseBranch: 'main',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerCreateWorktreeFrame;
    expect(decoded).toEqual(original);
  });

  it('create_worktree base_branch is nullable and round-trips as null', () => {
    const original: WorkerCreateWorktreeFrame = {
      kind: WorkerFrameKind.CreateWorktree,
      requestId: 'req_wt_2',
      repoPath: '/repo',
      branchName: 'wip',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerCreateWorktreeFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.CreateWorktree);
    expect(decoded.baseBranch).toBeNull();
  });

  it('create_worktree_result round trips', () => {
    // The server stores worktree_path as the session workspace; a dropped field
    // would persist a session with no workspace.
    const original: WorkerCreateWorktreeResultFrame = {
      kind: WorkerFrameKind.CreateWorktreeResult,
      requestId: 'req_wt_1',
      status: 'ok',
      worktreePath: '/home/dev/projects/myrepo-worktrees/feature-login',
      branch: 'feature/login',
    };
    const decoded = decodeWorkerFrame(
      encodeWorkerFrame(original),
    ) as WorkerCreateWorktreeResultFrame;
    // Decode populates every nullable field; the unset `error` comes back null.
    expect(decoded).toEqual({ ...original, error: null });
  });

  it('failed create_worktree result carries its error', () => {
    const original: WorkerCreateWorktreeResultFrame = {
      kind: WorkerFrameKind.CreateWorktreeResult,
      requestId: 'req_wt_1',
      status: 'failed',
      error: "branch 'x' already exists",
    };
    const decoded = decodeWorkerFrame(
      encodeWorkerFrame(original),
    ) as WorkerCreateWorktreeResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.CreateWorktreeResult);
    expect(decoded.worktreePath).toBeNull();
    expect(decoded.error).toBe("branch 'x' already exists");
  });

  it('remove_worktree frame round trips', () => {
    // A dropped delete_branch flag would silently change cleanup behavior (delete
    // the branch when the user didn't ask, or vice versa).
    const original: WorkerRemoveWorktreeFrame = {
      kind: WorkerFrameKind.RemoveWorktree,
      requestId: 'req_rm_1',
      worktreePath: '/home/dev/projects/myrepo-worktrees/feature-login',
      branch: 'feature/login',
      deleteBranch: true,
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerRemoveWorktreeFrame;
    expect(decoded).toEqual(original);
  });

  it('non-bool delete_branch is rejected, not coerced', () => {
    // Coercing a truthy string to true would delete a branch the user didn't ask
    // to delete.
    const bad =
      '{"kind": "worker.remove_worktree", "request_id": "r", ' +
      '"worktree_path": "/x", "delete_branch": "yes"}';
    expect(() => decodeWorkerFrame(bad)).toThrow(/delete_branch/);
  });

  it('remove_worktree_result round trips', () => {
    const original: WorkerRemoveWorktreeResultFrame = {
      kind: WorkerFrameKind.RemoveWorktreeResult,
      requestId: 'req_rm_1',
      status: 'ok',
    };
    const decoded = decodeWorkerFrame(
      encodeWorkerFrame(original),
    ) as WorkerRemoveWorktreeResultFrame;
    // The unset `error` comes back null after decode.
    expect(decoded).toEqual({ ...original, error: null });
  });
});

// ── worker.create_dir frames ───────────────────────────────

describe('worker.create_dir', () => {
  it('create_dir request frame round trips', () => {
    // Pins the wire shape used by the picker's "New folder" action: request_id
    // plus the directory path to create.
    const original: WorkerCreateDirFrame = {
      kind: WorkerFrameKind.CreateDir,
      requestId: 'req_mkdir_1',
      path: '/home/dev/projects/new-app',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerCreateDirFrame;
    expect(decoded).toEqual(original);
  });

  it('create_dir tilde-prefixed path round trips verbatim', () => {
    // The worker (not the server) expands `~`, same rules as worker.list_dir — so the
    // tilde must survive the wire.
    const original: WorkerCreateDirFrame = {
      kind: WorkerFrameKind.CreateDir,
      requestId: 'req_mkdir_tilde',
      path: '~/scratch',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerCreateDirFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.CreateDir);
    expect(decoded.path).toBe('~/scratch');
  });

  it('create_dir request without path raises', () => {
    // Without path the worker has nothing to create; failing loud beats silently
    // creating something under the process cwd.
    expect(() => decodeWorkerFrame('{"kind": "worker.create_dir", "request_id": "r"}')).toThrow(
      /missing required string field/,
    );
  });

  it('create_dir_result success round trips with the created absolute path', () => {
    // The picker navigates into `path` after creating it; a dropped field would
    // leave the user staring at the old directory.
    const original: WorkerCreateDirResultFrame = {
      kind: WorkerFrameKind.CreateDirResult,
      requestId: 'req_mkdir_2',
      status: 'ok',
      path: '/home/dev/projects/new-app',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerCreateDirResultFrame;
    // The unset `error` comes back null after decode.
    expect(decoded).toEqual({ ...original, error: null });
  });

  it('create_dir_result expected filesystem error round trips with path left null', () => {
    // The route maps a non-empty error to a 409 so the picker can show "directory
    // already exists" — that hinges on the message surviving the wire.
    const original: WorkerCreateDirResultFrame = {
      kind: WorkerFrameKind.CreateDirResult,
      requestId: 'req_mkdir_3',
      status: 'ok',
      error: 'directory already exists',
    };
    const decoded = decodeWorkerFrame(encodeWorkerFrame(original)) as WorkerCreateDirResultFrame;
    expect(decoded.kind).toBe(WorkerFrameKind.CreateDirResult);
    expect(decoded.status).toBe('ok');
    expect(decoded.path).toBeNull();
    expect(decoded.error).toBe('directory already exists');
  });
});

// ── Wire compatibility ───────────────────────────────────
//
// The dot-separated `kind` strings and the snake_case wire keys are the
// cross-component contract; assert them byte-for-byte so a rename of a TS
// identifier can never silently break a peer outside this test suite.

describe('wire compatibility', () => {
  it('WorkerFrameKind enum values match the contract strings', () => {
    expect(WorkerFrameKind.Hello).toBe('worker.hello');
    expect(WorkerFrameKind.LaunchRunner).toBe('worker.launch_runner');
    expect(WorkerFrameKind.LaunchRunnerResult).toBe('worker.launch_runner_result');
    expect(WorkerFrameKind.StopRunner).toBe('worker.stop_runner');
    expect(WorkerFrameKind.StopRunnerResult).toBe('worker.stop_runner_result');
    expect(WorkerFrameKind.RunnerExited).toBe('worker.runner_exited');
    expect(WorkerFrameKind.Stat).toBe('worker.stat');
    expect(WorkerFrameKind.StatResult).toBe('worker.stat_result');
    expect(WorkerFrameKind.ListDir).toBe('worker.list_dir');
    expect(WorkerFrameKind.ListDirResult).toBe('worker.list_dir_result');
    expect(WorkerFrameKind.CreateWorktree).toBe('worker.create_worktree');
    expect(WorkerFrameKind.CreateWorktreeResult).toBe('worker.create_worktree_result');
    expect(WorkerFrameKind.RemoveWorktree).toBe('worker.remove_worktree');
    expect(WorkerFrameKind.RemoveWorktreeResult).toBe('worker.remove_worktree_result');
    expect(WorkerFrameKind.CreateDir).toBe('worker.create_dir');
    expect(WorkerFrameKind.CreateDirResult).toBe('worker.create_dir_result');
  });

  it('encodes hello with snake_case wire keys and full field set', () => {
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.Hello,
        version: '0.1.0',
        frameProtocolVersion: 2,
        name: 'laptop',
        runners: ['r1'],
        configuredHarnesses: { 'claude-sdk': true },
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.hello',
      version: '0.1.0',
      frame_protocol_version: 2,
      name: 'laptop',
      runners: ['r1'],
      configured_harnesses: { 'claude-sdk': true },
    });
  });

  it('encodes hello with null configured_harnesses when omitted', () => {
    // Encode always carries every field; an unset advisory map serializes as null
    // ("unknown"), never as {}.
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.Hello,
        version: '0.1.0',
        frameProtocolVersion: 1,
        name: 'laptop',
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.hello',
      version: '0.1.0',
      frame_protocol_version: 1,
      name: 'laptop',
      runners: [],
      configured_harnesses: null,
    });
  });

  it('encodes launch_runner with snake_case wire keys', () => {
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.LaunchRunner,
        requestId: 'req_001',
        bindingToken: 'tok',
        workspace: '/w',
        harness: 'claude-sdk',
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.launch_runner',
      request_id: 'req_001',
      binding_token: 'tok',
      workspace: '/w',
      harness: 'claude-sdk',
    });
  });

  it('encodes launch_runner_result with snake_case error_code wire key', () => {
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.LaunchRunnerResult,
        requestId: 'req_001',
        status: 'failed',
        error: 'boom',
        errorCode: 'harness_not_configured',
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.launch_runner_result',
      request_id: 'req_001',
      status: 'failed',
      runner_id: null,
      error: 'boom',
      error_code: 'harness_not_configured',
    });
  });

  it('encodes runner_exited with snake_case runner_id wire key', () => {
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.RunnerExited,
        runnerId: 'runner_abc',
        error: 'boom',
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.runner_exited',
      runner_id: 'runner_abc',
      error: 'boom',
    });
  });

  it('encodes stat_result with snake_case canonical_path wire key', () => {
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.StatResult,
        requestId: 'req_stat_2',
        status: 'ok',
        exists: true,
        type: 'directory',
        canonicalPath: '/home/dev/universe',
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.stat_result',
      request_id: 'req_stat_2',
      status: 'ok',
      exists: true,
      type: 'directory',
      canonical_path: '/home/dev/universe',
      error: null,
    });
  });

  it('encodes list_dir_result entries with snake_case modified_at wire key', () => {
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.ListDirResult,
        requestId: 'req_list_3',
        status: 'ok',
        entries: [
          { name: 'src', path: '/foo/src', type: 'directory', bytes: null, modifiedAt: 1779980000 },
        ],
        hasMore: false,
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.list_dir_result',
      request_id: 'req_list_3',
      status: 'ok',
      entries: [
        { name: 'src', path: '/foo/src', type: 'directory', bytes: null, modified_at: 1779980000 },
      ],
      has_more: false,
      error: null,
    });
  });

  it('encodes create_worktree with snake_case repo_path / branch_name / base_branch keys', () => {
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.CreateWorktree,
        requestId: 'req_wt_1',
        repoPath: '/repo',
        branchName: 'feature/login',
        baseBranch: 'main',
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.create_worktree',
      request_id: 'req_wt_1',
      repo_path: '/repo',
      branch_name: 'feature/login',
      base_branch: 'main',
    });
  });

  it('encodes remove_worktree with snake_case worktree_path / delete_branch keys', () => {
    const wire = JSON.parse(
      encodeWorkerFrame({
        kind: WorkerFrameKind.RemoveWorktree,
        requestId: 'req_rm_1',
        worktreePath: '/wt',
        branch: 'feature/login',
        deleteBranch: true,
      }),
    );
    expect(wire).toEqual({
      kind: 'worker.remove_worktree',
      request_id: 'req_rm_1',
      worktree_path: '/wt',
      branch: 'feature/login',
      delete_branch: true,
    });
  });

  it('decode ignores unknown extra keys on a frame', () => {
    // A forward-compatible peer that adds a field must not break older decoders.
    const decoded = decodeWorkerFrame(
      JSON.stringify({ kind: 'worker.stat', request_id: 'r', path: '/p', extra: 'x' }),
    );
    expect((decoded as unknown as Record<string, unknown>).extra).toBeUndefined();
    expect(decoded).toEqual({ kind: WorkerFrameKind.Stat, requestId: 'r', path: '/p' });
  });
});
