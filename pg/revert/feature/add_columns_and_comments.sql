-- Revert FileDimension:feature/add_columns_and_comments from pg

BEGIN;

ALTER TABLE public.dim_file
  DROP COLUMN modified_at,
  DROP COLUMN mimetype;

COMMIT;
