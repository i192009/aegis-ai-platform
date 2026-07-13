BEGIN;

ALTER TABLE evaluation_results DROP COLUMN IF EXISTS result_payload;
ALTER TABLE evaluation_jobs DROP COLUMN IF EXISTS parameters;

COMMIT;
