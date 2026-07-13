BEGIN;

ALTER TABLE evaluation_jobs
    ADD COLUMN parameters JSONB NOT NULL DEFAULT '{}';

ALTER TABLE evaluation_results
    ADD COLUMN result_payload JSONB NOT NULL DEFAULT '{}';

COMMIT;
