-- Copyright The Orca Authors
-- SPDX-License-Identifier: Apache-2.0
-- Native Dogwood state-CAS scope; no imported migration.
ALTER TABLE "guardrail_policy_heads" DROP CONSTRAINT "policy_head_scope";--> statement-breakpoint
ALTER TABLE "guardrail_policy_heads" ADD CONSTRAINT "policy_head_scope" CHECK ("guardrail_policy_heads"."scope" in ('session','principal','state'));