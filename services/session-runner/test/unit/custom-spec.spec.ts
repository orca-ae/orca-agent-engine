// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the generic custom-provider SPEC PARSER.
//
// The `custom` provider lets an operator register ANY CLI agent via a declarative spec carried
// on the agent snapshot (the credential-free `custom_spec` block). This suite pins the parser
// that turns that opaque block into a typed {@link CustomAgentSpec}:
//   1. the minimal spec (just a `command`) parses, defaulting argv/env/cwd + the plain-text
//      stdout mapping (every stdout line is agent text);
//   2. an argv template with placeholders is preserved verbatim (substitution is the launcher's
//      job, not the parser's);
//   3. a json-line stdout mapping (field → text / tool_use / tool_result / turn_completed +
//      an optional approval rule) parses into the typed rule shape;
//   4. the approvals opt-in (+ its stdin response template) parses;
//   5. a malformed spec (absent, non-object, missing/blank command, bad stdout mode) fails
//      fast with a {@link CustomSpecError} rather than silently degrading.
//
// The parser is PURE (no I/O).

import { describe, it, expect } from 'vitest';
import {
  parseCustomAgentSpec,
  CustomSpecError,
  type CustomAgentSpec,
} from '../../src/harness/custom/spec.js';

describe('custom-provider spec parser', () => {
  it('parses a minimal spec (command only): defaults argv/env/cwd + plain-text stdout', () => {
    const spec = parseCustomAgentSpec({ command: 'my-agent' });
    expect(spec.command).toBe('my-agent');
    expect(spec.argv).toEqual([]);
    expect(spec.env).toEqual({});
    expect(spec.cwd).toBeUndefined();
    // Default stdout mapping: treat every stdout line as agent text.
    expect(spec.stdout.mode).toBe('text');
    // No approval opt-in by default.
    expect(spec.approvals).toBeUndefined();
  });

  it('preserves an argv template with placeholders verbatim (substitution is the launcher job)', () => {
    const spec = parseCustomAgentSpec({
      command: 'my-agent',
      argv: ['run', '--session', '{sessionId}', '--prompt', '{userText}'],
    });
    expect(spec.argv).toEqual(['run', '--session', '{sessionId}', '--prompt', '{userText}']);
  });

  it('parses env + cwd', () => {
    const spec = parseCustomAgentSpec({
      command: 'my-agent',
      env: { MY_AGENT_MODE: 'headless', LOG: '{sessionId}' },
      cwd: 'work',
    });
    expect(spec.env).toEqual({ MY_AGENT_MODE: 'headless', LOG: '{sessionId}' });
    expect(spec.cwd).toBe('work');
  });

  it('parses a json-line stdout mapping into typed field rules', () => {
    const raw = {
      command: 'my-agent',
      stdout: {
        mode: 'jsonLine',
        text: { type_equals: 'assistant', text_field: 'content' },
        tool_use: {
          type_equals: 'tool_call',
          name_field: 'tool',
          input_field: 'args',
          id_field: 'id',
        },
        tool_result: {
          type_equals: 'tool_result',
          id_field: 'id',
          content_field: 'output',
          error_field: 'is_error',
        },
        turn_completed: { type_equals: 'done' },
      },
    };
    const spec = parseCustomAgentSpec(raw);
    expect(spec.stdout.mode).toBe('jsonLine');
    if (spec.stdout.mode !== 'jsonLine') {
      throw new Error('expected jsonLine mode');
    }
    expect(spec.stdout.text).toEqual({ type_equals: 'assistant', text_field: 'content' });
    expect(spec.stdout.tool_use).toEqual({
      type_equals: 'tool_call',
      name_field: 'tool',
      input_field: 'args',
      id_field: 'id',
    });
    expect(spec.stdout.tool_result).toEqual({
      type_equals: 'tool_result',
      id_field: 'id',
      content_field: 'output',
      error_field: 'is_error',
    });
    expect(spec.stdout.turn_completed).toEqual({ type_equals: 'done' });
  });

  it('parses the approvals opt-in with its stdin response template', () => {
    const spec = parseCustomAgentSpec({
      command: 'my-agent',
      stdout: {
        mode: 'jsonLine',
        approval_request: {
          type_equals: 'approval',
          id_field: 'request_id',
          name_field: 'tool',
        },
      },
      approvals: {
        response: { type: 'approval_response', id: '{requestId}', decision: '{decision}' },
        allow_value: 'allow',
        deny_value: 'deny',
      },
    });
    expect(spec.approvals).toBeDefined();
    expect(spec.approvals?.response).toEqual({
      type: 'approval_response',
      id: '{requestId}',
      decision: '{decision}',
    });
    expect(spec.approvals?.allow_value).toBe('allow');
    expect(spec.approvals?.deny_value).toBe('deny');
    if (spec.stdout.mode !== 'jsonLine') {
      throw new Error('expected jsonLine mode');
    }
    // `input_field` defaults to `input` when the rule omits it.
    expect(spec.stdout.approval_request).toEqual({
      type_equals: 'approval',
      id_field: 'request_id',
      name_field: 'tool',
      input_field: 'input',
    });
  });

  it('supports a bridge-injection placeholder flag so the spec can wire the orca MCP bridge', () => {
    // The spec opts the bridge command/args into its argv via placeholders; the parser keeps
    // them verbatim. `{bridgeCommand}` / `{bridgeArgsJson}` are substituted by the launcher.
    const spec: CustomAgentSpec = parseCustomAgentSpec({
      command: 'my-agent',
      argv: ['--mcp', '{bridgeCommand}', '--mcp-args', '{bridgeArgsJson}'],
    });
    expect(spec.argv).toEqual(['--mcp', '{bridgeCommand}', '--mcp-args', '{bridgeArgsJson}']);
  });

  it('fails fast on an absent spec', () => {
    expect(() => parseCustomAgentSpec(undefined)).toThrow(CustomSpecError);
    expect(() => parseCustomAgentSpec(null)).toThrow(CustomSpecError);
  });

  it('fails fast on a non-object spec', () => {
    expect(() => parseCustomAgentSpec('my-agent')).toThrow(CustomSpecError);
    expect(() => parseCustomAgentSpec(['my-agent'])).toThrow(CustomSpecError);
  });

  it('fails fast on a missing / blank command', () => {
    expect(() => parseCustomAgentSpec({})).toThrow(CustomSpecError);
    expect(() => parseCustomAgentSpec({ command: '' })).toThrow(CustomSpecError);
    expect(() => parseCustomAgentSpec({ command: 42 })).toThrow(CustomSpecError);
  });

  it('fails fast on an unknown stdout mode', () => {
    expect(() => parseCustomAgentSpec({ command: 'x', stdout: { mode: 'bananas' } })).toThrow(
      CustomSpecError,
    );
  });

  it('fails fast on a `stdout.approval_request` rule with NO `approvals` block (would hang a blocking CLI)', () => {
    // The rule routes a CLI-raised approval to the human gate; without an `approvals` block there is
    // no stdin response frame to write back, so a CLI awaiting the decision frame would stall the
    // turn. The two halves are one loop and must be declared together — surface it fail-fast.
    expect(() =>
      parseCustomAgentSpec({
        command: 'my-agent',
        stdout: {
          mode: 'jsonLine',
          approval_request: { type_equals: 'approval', id_field: 'request_id', name_field: 'tool' },
        },
      }),
    ).toThrow(CustomSpecError);
  });

  it('fails fast on an `approvals` block with NO `stdout.approval_request` rule (dead config)', () => {
    // The inverse half: an `approvals` response template with no rule to route an approval frame to
    // the gate is dead config — nothing ever fires it. Reject it rather than silently accept.
    expect(() =>
      parseCustomAgentSpec({
        command: 'my-agent',
        stdout: { mode: 'jsonLine', text: { type_equals: 'assistant', text_field: 'content' } },
        approvals: {
          response: { type: 'approval_response', id: '{requestId}', decision: '{decision}' },
        },
      }),
    ).toThrow(CustomSpecError);
  });

  it('accepts the paired approval loop (a `stdout.approval_request` rule + an `approvals` block)', () => {
    // The well-formed pairing parses — both halves present. (The standalone-halves cases above are
    // the fail-fast; this pins that the paired form is NOT over-rejected.)
    const spec = parseCustomAgentSpec({
      command: 'my-agent',
      stdout: {
        mode: 'jsonLine',
        approval_request: { type_equals: 'approval', id_field: 'request_id', name_field: 'tool' },
      },
      approvals: {
        response: { type: 'approval_response', id: '{requestId}', decision: '{decision}' },
      },
    });
    expect(spec.approvals).toBeDefined();
    if (spec.stdout.mode !== 'jsonLine') {
      throw new Error('expected jsonLine mode');
    }
    expect(spec.stdout.approval_request).toBeDefined();
  });

  it('drops non-string argv entries rather than passing them to the shell', () => {
    const spec = parseCustomAgentSpec({ command: 'x', argv: ['a', 5, null, 'b'] });
    expect(spec.argv).toEqual(['a', 'b']);
  });
});
