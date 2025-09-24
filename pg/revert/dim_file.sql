-- Revert FileDimension:dim_file from pg

BEGIN;

DROP TABLE dim_file;

COMMIT;
