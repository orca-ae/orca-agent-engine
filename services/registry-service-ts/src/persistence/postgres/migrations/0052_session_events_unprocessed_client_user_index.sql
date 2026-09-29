-- Keep schema/snapshot metadata in this journal checkpoint. src/migrate.ts
-- creates this index concurrently after Drizzle commits its transaction.
SELECT 1;
