// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { idString } from './common.js';
import { OutcomeEvaluation } from './sessions.contract.js';

const c = initContract();

export const outcomesContract = c.router({
  get: {
    method: 'GET',
    path: '/v1/sessions/:id/outcome',
    pathParams: z.object({ id: idString('ses') }),
    // The latest outcome verdict, or null when none has been evaluated.
    responses: { 200: OutcomeEvaluation.nullable() },
  },
});
