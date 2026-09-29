// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import { completedProjectedTrace } from '../support/events.js';
import { toolSpan } from '../support/tools.js';

const schema = 'orca.observability.projected-trace.v2';
const version = 'orca.observability.raw-io.v1';

describe('Langfuse raw observation I/O', () => {
  it('exports root and tool I/O without turning a model summary into a generation', () => {
    const trace = completedProjectedTrace();
    Object.assign(trace, { schemaVersion: schema });
    Object.assign(trace.root, {
      io: {
        version,
        input: { json: JSON.stringify('Find the failed build') },
        output: { json: JSON.stringify(['The build failed its unit tests.']) },
        outputScope: 'turn_messages',
      },
    });
    const tool = toolSpan(trace, 'success');
    Object.assign(tool, {
      io: {
        version,
        toolName: 'read_build',
        input: { json: '{"build":42}' },
        output: { json: '{"status":"failed"}' },
      },
    });
    trace.spans.push(tool);
    const [root, summary, wireTool] =
      encodeLangfuseOtlpJson(trace).resourceSpans[0]!.scopeSpans[0]!.spans;
    expect(root?.attributes).toEqual(
      expect.arrayContaining([
        { key: 'langfuse.observation.input', value: { stringValue: '"Find the failed build"' } },
        {
          key: 'langfuse.observation.output',
          value: { stringValue: '["The build failed its unit tests."]' },
        },
        {
          key: 'langfuse.observation.metadata.orca.io.output_scope',
          value: { stringValue: 'turn_messages' },
        },
      ]),
    );
    expect(wireTool).toMatchObject({ name: 'read_build', parentSpanId: trace.root.spanId });
    expect(wireTool?.attributes).toEqual(
      expect.arrayContaining([
        { key: 'langfuse.observation.input', value: { stringValue: '{"build":42}' } },
        { key: 'langfuse.observation.output', value: { stringValue: '{"status":"failed"}' } },
        { key: 'langfuse.observation.type', value: { stringValue: 'tool' } },
        { key: 'gen_ai.tool.name', value: { stringValue: 'read_build' } },
      ]),
    );
    expect(summary?.attributes).toContainEqual({
      key: 'langfuse.observation.type',
      value: { stringValue: 'span' },
    });
    expect(summary?.attributes.some(({ key }) => key === 'langfuse.observation.input')).toBe(false);
  });

  it('does not export content smuggled into a legacy metadata-only trace', () => {
    const trace = completedProjectedTrace();
    Object.assign(trace.root, { io: { version, input: { json: '"must-stay-private"' } } });
    const tool = toolSpan(trace, 'success');
    Object.assign(tool, { io: { version, toolName: 'private-tool-name' } });
    trace.spans.push(tool);
    const encoded = JSON.stringify(encodeLangfuseOtlpJson(trace));
    expect(encoded).not.toContain('must-stay-private');
    expect(encoded).not.toContain('private-tool-name');
    expect(encoded).not.toContain('langfuse.observation.input');
  });

  it('marks omitted or truncated content rather than inventing empty input/output', () => {
    const trace = completedProjectedTrace();
    Object.assign(trace, { schemaVersion: schema });
    Object.assign(trace.root, {
      io: {
        version,
        input: { omitted: 'unsupported' },
        output: { json: '"preview"', truncated: true },
      },
    });
    const root = encodeLangfuseOtlpJson(trace).resourceSpans[0]!.scopeSpans[0]!.spans[0]!;
    expect(root.attributes.some(({ key }) => key === 'langfuse.observation.input')).toBe(false);
    expect(root.attributes).toEqual(
      expect.arrayContaining([
        {
          key: 'langfuse.observation.metadata.orca.io.input.omitted',
          value: { stringValue: 'unsupported' },
        },
        {
          key: 'langfuse.observation.metadata.orca.io.output.truncated',
          value: { boolValue: true },
        },
      ]),
    );
  });
});
