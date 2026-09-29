// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import type { DbClient } from '../persistence/postgres/client.js';
import { toInternalId } from '../contracts/id-prefix.js';
import { getSessionOutcome } from '../domain/session-outcome.js';

export function registerOutcomesRoutes(app: FastifyInstance, db: DbClient): void {
  app.get('/v1/sessions/:id/outcome', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const view = await getSessionOutcome(db, auth.workspaceId, id);
    reply.send(view.outcome);
  });
}
