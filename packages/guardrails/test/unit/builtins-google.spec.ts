// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { BUILTIN_EVALUATORS } from '../../src/builtins/index.js';
import type { EvaluatorContext } from '../../src/engine.js';
import { applyStateUpdate } from '../../src/state.js';
import type { GuardrailEvent, StateUpdate } from '../../src/types.js';

function ctx(
  params: Record<string, unknown>,
  event: Partial<GuardrailEvent> = {},
  state: Record<string, unknown> = {},
): EvaluatorContext {
  return {
    params,
    state,
    event: {
      phase: 'tool_call',
      sessionId: 'ses_1',
      tool: { name: 'mcp__google_drive__get_file_metadata', input: {} },
      ...event,
    },
    guardrail: {
      id: 'grd_1',
      name: 'test',
      enabled: true,
      phases: ['tool_call', 'tool_result'],
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: 'x', params },
    },
  };
}

function run(
  name: string,
  params: Record<string, unknown>,
  event?: Partial<GuardrailEvent>,
  state?: Record<string, unknown>,
) {
  const evaluator = BUILTIN_EVALUATORS.get(name);
  if (!evaluator) throw new Error(`no evaluator registered for ${name}`);
  return evaluator(ctx(params, event, state));
}

/** Fold the updates an evaluator asked for into the state a later call reads. */
function applied(updates: readonly StateUpdate[] | undefined): Record<string, unknown> {
  const state: Record<string, unknown> = {};
  for (const update of updates ?? []) applyStateUpdate(state, update);
  return state;
}

const call = (name: string, input: Record<string, unknown>): Partial<GuardrailEvent> => ({
  phase: 'tool_call',
  tool: { name, input },
});

const result = (
  name: string,
  input: Record<string, unknown>,
  payload: unknown = {},
): Partial<GuardrailEvent> => ({
  phase: 'tool_result',
  tool: { name, input },
  result: payload,
});

describe('gdrive_policy reads', () => {
  it('abstains on a read while reads are unrestricted', () => {
    expect(
      run('gdrive_policy', {}, call('mcp__google_drive__read_file_content', { file_id: 'doc-2' })),
    ).toBeUndefined();
  });

  it('denies a read of a file outside the readable list', () => {
    const out = run(
      'gdrive_policy',
      { read_all: false, read_files: ['doc-1'] },
      call('mcp__google_drive__read_file_content', { file_id: 'doc-2' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a read of a listed file', () => {
    const out = run(
      'gdrive_policy',
      { read_all: false, read_files: ['doc-1'] },
      call('mcp__google_drive__read_file_content', { file_id: 'doc-1' }),
    );
    expect(out).toBeUndefined();
  });

  it('asks when a restricted read names no file it can check', () => {
    const out = run(
      'gdrive_policy',
      { read_all: false, read_files: ['doc-1'] },
      call('mcp__google_drive__search_files', { query: 'budget' }),
    );
    expect(out?.verdict).toBe('ask');
  });

  it('covers the document and spreadsheet servers, not just Drive itself', () => {
    for (const name of ['mcp__google_docs__get_document', 'mcp__google_sheets__read_values']) {
      const out = run(
        'gdrive_policy',
        { read_all: false, read_files: ['doc-1'] },
        call(name, { document_id: 'doc-2', spreadsheet_id: 'doc-2' }),
      );
      expect(out?.verdict, name).toBe('deny');
    }
  });
});

describe('gdrive_policy writes', () => {
  it('denies creating a file by default', () => {
    const out = run('gdrive_policy', {}, call('mcp__google_drive__create_file', { name: 'notes' }));
    expect(out?.verdict).toBe('deny');
  });

  it('allows creating once creation is turned on', () => {
    const out = run(
      'gdrive_policy',
      { allow_create: true },
      call('mcp__google_drive__create_file', { name: 'notes' }),
    );
    expect(out).toBeUndefined();
  });

  it('denies modifying a file that is not writable', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { file_id: 'doc-2' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows modifying a listed file', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { file_id: 'doc-1' }),
    );
    expect(out).toBeUndefined();
  });

  it('allows modifying a file the session created', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { file_id: 'doc-9' }),
      { gdrive_created: ['doc-9'] },
    );
    expect(out).toBeUndefined();
  });

  it('asks when a write names no file it can check', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { content: 'new text' }),
    );
    expect(out?.verdict).toBe('ask');
  });

  it('denies a comment on a file that is on no list', () => {
    const out = run(
      'gdrive_policy',
      { comment_files: ['doc-1'] },
      call('mcp__google_drive__create_comment', { file_id: 'doc-2' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a comment on a listed file', () => {
    const out = run(
      'gdrive_policy',
      { comment_files: ['doc-1'] },
      call('mcp__google_drive__create_comment', { file_id: 'doc-1' }),
    );
    expect(out).toBeUndefined();
  });

  it('reads comments without needing comment permission', () => {
    const out = run(
      'gdrive_policy',
      {},
      call('mcp__google_drive__list_comments', { file_id: 'doc-2' }),
    );
    expect(out).toBeUndefined();
  });
});

describe('gdrive_policy containment', () => {
  const params = {
    write_files: ['public-1', 'secret-1'],
    confidential_files: ['secret-*'],
  };

  it('records a confidential read at the result phase, not before it happened', () => {
    const before = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__read_file_content', { file_id: 'secret-1' }),
    );
    expect(before?.stateUpdates ?? []).toHaveLength(0);

    const after = run(
      'gdrive_policy',
      params,
      result('mcp__google_drive__read_file_content', { file_id: 'secret-1' }),
    );
    expect(after?.stateUpdates?.[0]).toMatchObject({
      scope: 'session',
      action: 'append',
      value: 'secret-1',
    });
  });

  it('records confidential files identified only by a successful read result', () => {
    const observed = run(
      'gdrive_policy',
      params,
      result(
        'mcp__google_drive__search_files',
        { query: 'secret' },
        { files: [{ id: 'secret-1' }] },
      ),
    );
    const state = applied(observed?.stateUpdates);
    expect(state['gdrive_confidential_read']).toContain('secret-1');

    const write = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__update_file', { file_id: 'public-1' }),
      state,
    );
    expect(write?.verdict).toBe('deny');
  });

  it('denies a write to a lower tier once the session has read a confidential file', () => {
    const state = applied(
      run(
        'gdrive_policy',
        params,
        result('mcp__google_drive__read_file_content', { file_id: 'secret-1' }),
      )?.stateUpdates,
    );
    const out = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__update_file', { file_id: 'public-1' }),
      state,
    );
    expect(out?.verdict).toBe('deny');
  });

  it('asks instead when the policy says to ask', () => {
    const asking = { ...params, write_down_action: 'ask' };
    const state = applied(
      run(
        'gdrive_policy',
        asking,
        result('mcp__google_drive__read_file_content', { file_id: 'secret-1' }),
      )?.stateUpdates,
    );
    const out = run(
      'gdrive_policy',
      asking,
      call('mcp__google_drive__update_file', { file_id: 'public-1' }),
      state,
    );
    expect(out?.verdict).toBe('ask');
  });

  it('still allows writing back to a confidential file, which moves nothing down', () => {
    const state = applied(
      run(
        'gdrive_policy',
        params,
        result('mcp__google_drive__read_file_content', { file_id: 'secret-1' }),
      )?.stateUpdates,
    );
    const out = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__update_file', { file_id: 'secret-1' }),
      state,
    );
    expect(out).toBeUndefined();
  });

  it('denies a new file after a confidential read, which is where content leaves', () => {
    const creating = { ...params, allow_create: true };
    const state = applied(
      run(
        'gdrive_policy',
        creating,
        result('mcp__google_drive__read_file_content', { file_id: 'secret-1' }),
      )?.stateUpdates,
    );
    const out = run(
      'gdrive_policy',
      creating,
      call('mcp__google_drive__create_file', { name: 'summary' }),
      state,
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies copying a confidential file even with no earlier read', () => {
    const out = run(
      'gdrive_policy',
      { ...params, allow_create: true },
      call('mcp__google_drive__copy_file', { file_id: 'secret-1', name: 'copy of secret' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('checks a copy source against restricted read access before allowing creation', () => {
    const denied = run(
      'gdrive_policy',
      { read_all: false, read_files: ['public-*'], allow_create: true },
      call('mcp__google_drive__copy_file', { file_id: 'secret-1', name: 'copy' }),
    );
    expect(denied?.verdict).toBe('deny');

    const allowed = run(
      'gdrive_policy',
      { read_all: false, read_files: ['public-*'], allow_create: true },
      call('mcp__google_drive__duplicate_file', { file_id: 'public-1', name: 'copy' }),
    );
    expect(allowed).toBeUndefined();
  });

  it('leaves ordinary writes alone while nothing confidential has been read', () => {
    const out = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__update_file', { file_id: 'public-1' }),
    );
    expect(out).toBeUndefined();
  });

  it('records the file a create produced so later writes to it are allowed', () => {
    const out = run(
      'gdrive_policy',
      { allow_create: true },
      result('mcp__google_drive__create_file', { name: 'notes' }, { id: 'doc-9' }),
    );
    const state = applied(out?.stateUpdates);
    expect(state['gdrive_created']).toContain('doc-9');

    const write = run(
      'gdrive_policy',
      { allow_create: true },
      call('mcp__google_drive__update_file', { file_id: 'doc-9' }),
      state,
    );
    expect(write).toBeUndefined();
  });

  it('does not record a copy source as created, so the source stays access-checked', () => {
    const out = run(
      'gdrive_policy',
      { allow_create: true },
      result('mcp__google_drive__copy_file', { file_id: 'ceo-doc' }, { id: 'copy-1' }),
    );
    const state = applied(out?.stateUpdates);
    // The new file is created and writable; the source it was copied from is not.
    expect(state['gdrive_created']).toContain('copy-1');
    expect((state['gdrive_created'] as string[] | undefined) ?? []).not.toContain('ceo-doc');

    // A later write to the source is still denied — it was never "created" here.
    const write = run(
      'gdrive_policy',
      {},
      call('mcp__google_drive__update_file', { file_id: 'ceo-doc' }),
      state,
    );
    expect(write?.verdict).toBe('deny');
  });

  it('does not record reads or created ids from failed results', () => {
    const failedRead = run(
      'gdrive_policy',
      params,
      result(
        'mcp__google_drive__read_file_content',
        { file_id: 'secret-1' },
        { is_error: true, error: 'permission denied' },
      ),
    );
    expect(failedRead).toBeUndefined();

    const failedCreate = run(
      'gdrive_policy',
      { allow_create: true },
      result(
        'mcp__google_drive__create_file',
        { name: 'notes' },
        { success: false, error: 'failed', id: 'not-created' },
      ),
    );
    expect(failedCreate).toBeUndefined();
  });

  it('abstains at the result phase for a tool from another integration', () => {
    expect(run('gdrive_policy', params, result('mcp__slack__send_message', {}))).toBeUndefined();
  });
});

describe('gdrive_policy multi-file access', () => {
  it('denies a multi-file call where one id is permitted but another is not', () => {
    // A permitted id must not clear a call that also names a forbidden file.
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__trash_files', { file_ids: ['doc-1', 'ceo-salaries'] }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a multi-file call once every id is permitted', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1', 'doc-2'] },
      call('mcp__google_drive__trash_files', { file_ids: ['doc-1', 'doc-2'] }),
    );
    expect(out).toBeUndefined();
  });

  it('still allows a single file named by both its id and its name', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { file_id: 'doc-1', name: 'Renamed' }),
    );
    expect(out).toBeUndefined();
  });

  it('denies a multi-file comment where one file is off the list', () => {
    const out = run(
      'gdrive_policy',
      { comment_files: ['doc-1'] },
      call('mcp__google_drive__create_comments', { file_ids: ['doc-1', 'doc-2'] }),
    );
    expect(out?.verdict).toBe('deny');
  });
});

describe('gdrive_policy rename laundering', () => {
  it('denies renaming a forbidden file into an allowlisted name', () => {
    // The `name` on a mutation is the value being assigned, not an identity, so
    // it must not be matched against the access list.
    const out = run(
      'gdrive_policy',
      { write_files: ['Weekly Notes'] },
      call('mcp__google_drive__update_file', { file_id: 'ceo-salaries', name: 'Weekly Notes' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows renaming a file whose own id is permitted', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { file_id: 'doc-1', name: 'anything' }),
    );
    expect(out).toBeUndefined();
  });

  it('still treats a title as identity when no id names the file', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { title: 'doc-1' }),
    );
    expect(out).toBeUndefined();
  });
});

describe('gdrive_policy containment against a decoy field', () => {
  const params = {
    write_files: ['public-1', 'secret-1'],
    confidential_files: ['secret-*'],
  };

  it('denies a write-down even when a decoy title matches the confidential set', () => {
    const state = applied(
      run(
        'gdrive_policy',
        params,
        result('mcp__google_drive__read_file_content', { file_id: 'secret-1' }),
      )?.stateUpdates,
    );
    const out = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__update_file', { file_id: 'public-1', title: 'secret-decoy' }),
      state,
    );
    expect(out?.verdict).toBe('deny');
  });
});

describe('gdrive_policy id-key laundering', () => {
  // Different id-type keys carrying different values are different files; a
  // permitted id must not clear a forbidden one beside it.
  it('denies a write that names a forbidden file beside a permitted id key', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { file_id: 'ceo-salaries', document_id: 'doc-1' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies a read that names a forbidden file beside a permitted id key', () => {
    const out = run(
      'gdrive_policy',
      { read_all: false, read_files: ['doc-1'] },
      call('mcp__google_drive__read_file_content', {
        file_id: 'ceo-salaries',
        document_id: 'doc-1',
      }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('does not exempt a write-down through a decoy id field', () => {
    // Both ids are writable (access passes), but the real destination is not
    // confidential, so the write-down is still denied — a confidential-looking
    // decoy id cannot buy the exemption.
    const params = { write_files: ['public-1', 'secret-*'], confidential_files: ['secret-*'] };
    const state = applied(
      run(
        'gdrive_policy',
        params,
        result('mcp__google_drive__read_file_content', { file_id: 'secret-1' }),
      )?.stateUpdates,
    );
    const out = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__update_file', { file_id: 'public-1', document_id: 'secret-decoy' }),
      state,
    );
    expect(out?.verdict).toBe('deny');
  });

  it('collapses true aliases that carry the same id value', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google_drive__update_file', { file_id: 'doc-1', document_id: 'doc-1' }),
    );
    expect(out).toBeUndefined();
  });
});

describe('gdrive_policy copy does not launder its source', () => {
  // A copy's input identifier is its *source*; recording it as created would
  // exempt a read-only source from later access checks. Only the new file's
  // result id may be recorded.
  const params = { write_files: ['doc-1'], allow_create: true };

  it('does not mark a copied source file as created', () => {
    const state = applied(
      run(
        'gdrive_policy',
        params,
        result('mcp__google_drive__copy_file', { file_id: 'ceo-doc' }, { id: 'new-doc' }),
      )?.stateUpdates,
    );
    expect(state['gdrive_created']).not.toContain('ceo-doc');

    const out = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__update_file', { file_id: 'ceo-doc' }),
      state,
    );
    expect(out?.verdict).toBe('deny');
  });

  it('still marks the new file a copy produced as created', () => {
    const state = applied(
      run(
        'gdrive_policy',
        params,
        result('mcp__google_drive__copy_file', { file_id: 'ceo-doc' }, { id: 'new-doc' }),
      )?.stateUpdates,
    );
    expect(state['gdrive_created']).toContain('new-doc');

    const out = run(
      'gdrive_policy',
      params,
      call('mcp__google_drive__update_file', { file_id: 'new-doc' }),
      state,
    );
    expect(out).toBeUndefined();
  });
});

describe('integration recognition by tool name', () => {
  it('applies the gmail policy to a unified-server send tool', () => {
    for (const name of ['mcp__google_workspace__gmail_send_message', 'mcp__gsuite__send_email']) {
      const out = run('gmail_policy', { allow_send: false }, call(name, { to: 'a@example.com' }));
      expect(out?.verdict, name).toBe('deny');
    }
  });

  it('applies the drive policy to a unified-server drive tool', () => {
    const out = run(
      'gdrive_policy',
      { write_files: ['doc-1'] },
      call('mcp__google__drive_update_file', { file_id: 'doc-2' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('applies the calendar policy to a unified-server calendar tool', () => {
    const out = run(
      'gcalendar_policy',
      {},
      call('mcp__google__calendar_create_event', { summary: 'sync' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('classifies operations after a unified-server service prefix', () => {
    expect(
      run(
        'gcalendar_policy',
        { allow_create_events: false, allow_modify_events: true },
        call('mcp__google__calendar_create_event', { summary: 'sync' }),
      )?.verdict,
    ).toBe('deny');
    expect(
      run(
        'gdrive_policy',
        { allow_create: false, write_files: ['doc-1'] },
        call('mcp__google__drive_copy_file', { file_id: 'doc-1' }),
      )?.verdict,
    ).toBe('deny');
    expect(
      run(
        'gdrive_policy',
        { read_all: false, read_files: [], write_files: ['doc-1'] },
        call('mcp__google__drive_get_file', { file_id: 'doc-1' }),
      )?.verdict,
    ).toBe('deny');
    expect(
      run(
        'gmail_policy',
        { allow_read: false, allow_modify: true },
        call('mcp__google__gmail_list_messages', {}),
      )?.verdict,
    ).toBe('deny');
  });
});

describe('gmail_policy', () => {
  it('abstains on a read by default', () => {
    expect(
      run('gmail_policy', {}, call('mcp__gmail__search_threads', { q: 'invoice' })),
    ).toBeUndefined();
  });

  it('denies a read once reading is turned off', () => {
    const out = run(
      'gmail_policy',
      { allow_read: false },
      call('mcp__gmail__get_message', { id: 'm1' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies sending by default', () => {
    const out = run('gmail_policy', {}, call('mcp__gmail__send_message', { to: 'a@example.com' }));
    expect(out?.verdict).toBe('deny');
  });

  it('allows sending once it is turned on', () => {
    const out = run(
      'gmail_policy',
      { allow_send: true },
      call('mcp__gmail__send_message', { to: 'a@example.com' }),
    );
    expect(out).toBeUndefined();
  });

  it('allows drafts by default', () => {
    expect(
      run('gmail_policy', {}, call('mcp__gmail__create_draft', { to: 'a@example.com' })),
    ).toBeUndefined();
  });

  it('denies drafts once they are turned off', () => {
    const out = run(
      'gmail_policy',
      { allow_drafts: false },
      call('mcp__gmail__update_draft', { id: 'd1' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('treats sending a draft as sending rather than drafting', () => {
    const out = run(
      'gmail_policy',
      { allow_drafts: true, allow_send: false },
      call('mcp__gmail__send_draft', { id: 'd1' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies modifying existing mail by default', () => {
    const out = run('gmail_policy', {}, call('mcp__gmail__label_message', { id: 'm1' }));
    expect(out?.verdict).toBe('deny');
  });

  it('allows modification once it is turned on', () => {
    const out = run(
      'gmail_policy',
      { allow_modify: true },
      call('mcp__gmail__label_message', { id: 'm1' }),
    );
    expect(out).toBeUndefined();
  });

  it('treats an unrecognised mutation as a modification rather than allowing it', () => {
    const out = run('gmail_policy', {}, call('mcp__gmail__archive_thread', { id: 't1' }));
    expect(out?.verdict).toBe('deny');
  });

  it('never asks, because there is no mail verdict a client could answer', () => {
    const out = run('gmail_policy', {}, call('mcp__gmail__send_message', {}));
    expect(out?.verdict).toBe('deny');
  });

  it('abstains on another integration', () => {
    expect(run('gmail_policy', {}, call('mcp__slack__send_message', {}))).toBeUndefined();
  });

  it('gates a unified Google server whose mail tools say only "message"', () => {
    // No `mail` in the server name; `messages` carries the family only because the
    // server is Google's own unified deployment.
    expect(run('gmail_policy', {}, call('mcp__google__send_messages', {}))?.verdict).toBe('deny');
    expect(
      run('gmail_policy', { allow_read: false }, call('mcp__gsuite__list_messages', { id: 'm1' }))
        ?.verdict,
    ).toBe('deny');
  });

  it('does not reach into a non-Google server that also uses "message"', () => {
    // The generic word must not turn `gmail_policy` into a Slack messaging guard.
    expect(run('gmail_policy', {}, call('mcp__slack__list_messages', {}))).toBeUndefined();
    expect(run('gmail_policy', {}, call('mcp__discord__send_message', {}))).toBeUndefined();
  });
});

describe('gcalendar_policy', () => {
  it('abstains on a read by default', () => {
    expect(
      run('gcalendar_policy', {}, call('mcp__google_calendar__list_events', {})),
    ).toBeUndefined();
  });

  it('denies a read once reading is turned off', () => {
    const out = run(
      'gcalendar_policy',
      { allow_read: false },
      call('mcp__google_calendar__search_events', { q: 'standup' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies creating an event by default', () => {
    const out = run(
      'gcalendar_policy',
      {},
      call('mcp__google_calendar__create_event', { summary: 'sync' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows creating once it is turned on', () => {
    const out = run(
      'gcalendar_policy',
      { allow_create_events: true },
      call('mcp__google_calendar__create_event', { summary: 'sync' }),
    );
    expect(out).toBeUndefined();
  });

  it('denies modifying or deleting an event by default', () => {
    for (const name of [
      'mcp__google_calendar__update_event',
      'mcp__google_calendar__delete_event',
    ]) {
      expect(run('gcalendar_policy', {}, call(name, { event_id: 'e1' }))?.verdict, name).toBe(
        'deny',
      );
    }
  });

  it('allows modification once it is turned on', () => {
    const out = run(
      'gcalendar_policy',
      { allow_modify_events: true },
      call('mcp__google_calendar__update_event', { event_id: 'e1' }),
    );
    expect(out).toBeUndefined();
  });

  it('treats responding to an invitation as modifying an event', () => {
    const out = run(
      'gcalendar_policy',
      {},
      call('mcp__google_calendar__respond_to_event', { event_id: 'e1' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('creating an event is not covered by permission to modify one', () => {
    const out = run(
      'gcalendar_policy',
      { allow_modify_events: true },
      call('mcp__google_calendar__create_event', { summary: 'sync' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('abstains on another integration', () => {
    expect(run('gcalendar_policy', {}, call('mcp__slack__send_message', {}))).toBeUndefined();
  });

  it('gates a unified Google server whose calendar tools say only "event"', () => {
    expect(
      run('gcalendar_policy', { allow_read: false }, call('mcp__google__list_events', {}))?.verdict,
    ).toBe('deny');
    expect(run('gcalendar_policy', {}, call('mcp__gsuite__create_events', {}))?.verdict).toBe(
      'deny',
    );
  });

  it('does not reach into a non-Google server that also uses "event"', () => {
    expect(
      run('gcalendar_policy', { allow_read: false }, call('mcp__analytics__list_events', {})),
    ).toBeUndefined();
  });
});

describe('gdrive_policy read id-key laundering', () => {
  it('denies a batch read naming a forbidden file beside a permitted one', () => {
    const out = run(
      'gdrive_policy',
      { read_all: false, read_files: ['public'] },
      call('mcp__google_drive__read_file_content', { file_ids: ['secret', 'public'] }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies a read with a decoy permitted id-key beside a forbidden file', () => {
    const out = run(
      'gdrive_policy',
      { read_all: false, read_files: ['public'] },
      call('mcp__google_drive__read_file_content', { file_id: 'secret', document_id: 'public' }),
    );
    expect(out?.verdict).toBe('deny');
  });
});

describe('gdrive_policy stem tool recognition', () => {
  it('does not apply to a tool with no Drive family stem in its name', () => {
    // A genuinely unrelated tool (no drive/doc/sheet/slide stem) is not swept in.
    expect(
      run('gdrive_policy', { write_files: [] }, call('mcp__slack__send_message', { text: 'x' })),
    ).toBeUndefined();
  });

  it('gates the document, spreadsheet and presentation tools on a unified server', () => {
    // The server name carries no family hint, so recognition must come from the
    // tool word. Under-matching here would leave Google's own tools ungated; a
    // shared word like `document` also matching an unrelated integration is the
    // accepted over-match.
    for (const name of [
      'mcp__google__get_document',
      'mcp__gsuite__update_spreadsheet',
      'mcp__google_workspace__get_presentation',
    ]) {
      const out = run(
        'gdrive_policy',
        { read_all: false, read_files: [] },
        call(name, { document_id: 'secret', spreadsheet_id: 'secret', presentation_id: 'secret' }),
      );
      expect(out?.verdict, name).toBe('deny');
    }
  });
});

describe('gdrive_policy create does not launder a name into the created set', () => {
  it('records only the server-assigned id, not the name the create asserted', () => {
    // An attacker names a new file after a victim's id; the create must not mark
    // that name as created, or a later write to the victim would be exempt.
    const state = applied(
      run(
        'gdrive_policy',
        { allow_create: true },
        result('mcp__google_drive__create_file', { name: 'ceo-comp' }, { id: 'new-real-id' }),
      )?.stateUpdates,
    );
    expect(state['gdrive_created']).toEqual(['new-real-id']);
    expect((state['gdrive_created'] as string[]) ?? []).not.toContain('ceo-comp');

    const write = run(
      'gdrive_policy',
      { allow_create: true },
      call('mcp__google_drive__update_file', { file_id: 'ceo-comp' }),
      state,
    );
    expect(write?.verdict).toBe('deny');
  });
});

describe('gdrive_policy read decoy and result poisoning', () => {
  it('denies a read that hides a forbidden title behind a permitted id decoy', () => {
    // A read never renames, so its title is a file addressed by name.
    const out = run(
      'gdrive_policy',
      { read_all: false, read_files: ['Team Handbook'] },
      call('mcp__google_drive__search_files', { name: 'Board Comp 2026', id: 'Team Handbook' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('records only the top-level create id, not one echoed in nested structure', () => {
    const state = applied(
      run(
        'gdrive_policy',
        { allow_create: true },
        result(
          'mcp__google_drive__create_file',
          { name: 'x' },
          { id: 'new_1', appProperties: { id: 'VICTIM' } },
        ),
      )?.stateUpdates,
    );
    expect(state['gdrive_created']).toEqual(['new_1']);
    const write = run(
      'gdrive_policy',
      { allow_create: true },
      call('mcp__google_drive__update_file', { file_id: 'VICTIM' }),
      state,
    );
    expect(write?.verdict).toBe('deny');
  });
});
