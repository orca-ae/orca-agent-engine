// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { canonicalize, preprocessForDiff } from '../../scripts/lib/schema-diff.mjs';

/**
 * The parts of the oasdiff adapter that are ours rather than the tool's.
 *
 * oasdiff is well exercised by its own suite; what needs testing here is the
 * three wrappers around it, each of which exists because running it against the
 * real specs showed it was necessary:
 *
 *   - it knows nothing of Anthropic's `?beta=true` path keys;
 *   - it matches responses by literal status code, so a 200-vs-201 divergence
 *     hides every difference in the bodies underneath it;
 *   - it matches response content by literal media type, with the same effect —
 *     declaring `text/event-stream` on the streaming routes turned a 36-leaf
 *     schema difference into a one-leaf media difference that read like
 *     progress;
 *   - its output ordering is unstable, which a committed artifact cannot absorb.
 */

const operation = (responses: Record<string, unknown>) => ({ responses });

describe('preprocessForDiff', () => {
  it("collapses Anthropic's ?beta=true path keys, preferring the beta variant", () => {
    const { base } = preprocessForDiff(
      {
        paths: {
          '/v1/agents': { get: operation({ 200: { description: 'ga' } }) },
          '/v1/agents?beta=true': { get: operation({ 200: { description: 'beta' } }) },
        },
      },
      { paths: {} },
    );

    expect(Object.keys(base.paths)).toEqual(['/v1/agents']);
    expect(base.paths['/v1/agents'].get.responses['200'].description).toBe('beta');
  });

  it('aligns a lone 201 onto 200 so the two success bodies are compared', () => {
    // Without this, oasdiff reports "200 deleted, 201 added" and never looks at
    // either body — hiding exactly the differences this comparison exists to
    // find, on the six creates where we answer 201.
    const { revision } = preprocessForDiff(
      { paths: { '/v1/agents': { post: operation({ 200: { description: 'theirs' } }) } } },
      { paths: { '/v1/agents': { post: operation({ 201: { description: 'ours' } }) } } },
    );

    const responses = revision.paths['/v1/agents'].post.responses;
    expect(Object.keys(responses)).toEqual(['200']);
    expect(responses['200'].description).toBe('ours');
  });

  it('aligns only the matching operation and leaves a shared 201 alone', () => {
    const { revision } = preprocessForDiff(
      {
        paths: {
          '/needs-align': { post: operation({ 200: {} }) },
          '/already-201': { post: operation({ 201: {} }) },
        },
      },
      {
        paths: {
          '/needs-align': { post: operation({ 201: {} }) },
          '/already-201': { post: operation({ 201: {} }) },
        },
      },
    );

    expect(Object.keys(revision.paths['/needs-align'].post.responses)).toEqual(['200']);
    expect(Object.keys(revision.paths['/already-201'].post.responses)).toEqual(['201']);
  });

  it('does not overwrite an existing 200', () => {
    const { revision } = preprocessForDiff(
      { paths: { '/v1/x': { post: operation({ 200: {} }) } } },
      { paths: { '/v1/x': { post: operation({ 200: { description: 'keep' }, 201: {} }) } } },
    );

    expect(revision.paths['/v1/x'].post.responses['200'].description).toBe('keep');
  });

  it('aligns a differing success media type so the two bodies are compared', () => {
    const { revision } = preprocessForDiff(
      {
        paths: {
          '/v1/sessions/{session_id}/stream': {
            get: operation({
              200: { content: { 'application/json': { schema: { type: 'object' } } } },
            }),
          },
        },
      },
      {
        paths: {
          // Their `{session_id}` is our `{id}`: matched on the normalized path,
          // because a literal lookup finds nothing and looks like agreement.
          '/v1/sessions/{id}/stream': {
            get: operation({
              200: { content: { 'text/event-stream': { schema: { type: 'object' } } } },
            }),
          },
        },
      },
    );

    const content = revision.paths['/v1/sessions/{id}/stream'].get.responses['200'].content;
    expect(Object.keys(content)).toEqual(['application/json']);
    expect(content['application/json'].schema).toEqual({ type: 'object' });
  });

  it('leaves a body offering more than one representation alone', () => {
    // Which of two representations to line up is a decision, and guessing is how
    // a comparison quietly stops meaning anything.
    const { revision } = preprocessForDiff(
      { paths: { '/v1/x': { get: operation({ 200: { content: { 'application/json': {} } } }) } } },
      {
        paths: {
          '/v1/x': {
            get: operation({ 200: { content: { 'text/event-stream': {}, 'text/plain': {} } } }),
          },
        },
      },
    );

    expect(Object.keys(revision.paths['/v1/x'].get.responses['200'].content)).toEqual([
      'text/event-stream',
      'text/plain',
    ]);
  });

  it('does not mutate the specs it is given', () => {
    const orca = {
      paths: {
        '/v1/x': { post: operation({ 201: { content: { 'text/event-stream': {} } } }) },
      },
    };
    preprocessForDiff(
      { paths: { '/v1/x': { post: operation({ 200: { content: { 'application/json': {} } } }) } } },
      orca,
    );

    const responses = orca.paths['/v1/x'].post.responses;
    expect(Object.keys(responses)).toEqual(['201']);
    expect(Object.keys(responses['201'].content)).toEqual(['text/event-stream']);
  });
});

describe('canonicalize', () => {
  it('orders object keys and array members so repeated runs agree', () => {
    // oasdiff emits Go map order, which differs between runs on identical
    // input. Three consecutive runs produced three different byte sequences;
    // canonicalized, they are the same document.
    const one = canonicalize({ b: 1, a: [3, 1, 2] });
    const other = canonicalize({ a: [2, 3, 1], b: 1 });

    expect(JSON.stringify(one)).toBe(JSON.stringify(other));
    expect(Object.keys(one as object)).toEqual(['a', 'b']);
  });

  it('preserves scalars and nesting', () => {
    expect(canonicalize({ z: { y: 'x' }, n: 1, t: true })).toEqual({
      n: 1,
      t: true,
      z: { y: 'x' },
    });
  });
});
