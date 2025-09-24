-- Revert FileDimension:data/seed_root_directory from pg

BEGIN;

DELETE FROM public.dim_file
WHERE parent_id IS NULL AND file_name = '/';

COMMIT;
