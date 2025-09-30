-- Revert FileDimension:fix/unique_root_constraint from pg

BEGIN;

DROP INDEX public.idx_dim_file_unique_root;

COMMIT;
