-- Copyright The Orca Authors
-- SPDX-License-Identifier: Apache-2.0
-- Native guardrail-mode migration; no imported schema or data. Existing rules retain enforce semantics.
ALTER TABLE "guardrails" ADD COLUMN "mode" text DEFAULT 'enforce' NOT NULL;--> statement-breakpoint
ALTER TABLE "guardrails" ADD CONSTRAINT "guardrails_mode_check" CHECK ("guardrails"."mode" in ('enforce', 'observe'));