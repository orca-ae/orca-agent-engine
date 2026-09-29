// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// One pixel PNG. Used to assert the installed SDK receives real image bytes,
// rather than accepting only an MCP-shaped object in a fake worker.
export const pngBase64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=';

export const customResultCases = [
  {
    name: 'text',
    content: [{ type: 'text', text: 'CALLBACK_TICKET_42' }],
    proof: 'CALLBACK_TICKET_42',
  },
  {
    name: 'text document',
    content: [
      {
        type: 'document',
        title: 'Ticket',
        context: 'Support record',
        source: { type: 'text', data: 'CUSTOM_DOCUMENT_PROOF' },
      },
    ],
    proof: 'CUSTOM_DOCUMENT_PROOF',
  },
  {
    name: 'base64 UTF-8 document',
    content: [
      {
        type: 'document',
        title: 'Ticket',
        source: {
          type: 'base64',
          media_type: 'text/plain',
          data: Buffer.from('UTF8_DOCUMENT_PROOF').toString('base64'),
        },
      },
    ],
    proof: 'UTF8_DOCUMENT_PROOF',
  },
  {
    name: 'search result',
    content: [
      {
        type: 'search_result',
        title: 'Ticket result',
        source: 'https://example.test/ticket',
        content: [{ type: 'text', text: 'SEARCH_RESULT_PROOF' }],
      },
    ],
    proof: 'SEARCH_RESULT_PROOF',
  },
  {
    name: 'inline image',
    content: [
      { type: 'image', source: { type: 'base64', media_type: 'image/png', data: pngBase64 } },
    ],
    proof: `data:image/png;base64,${pngBase64}`,
  },
];
