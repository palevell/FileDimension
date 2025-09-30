-- Deploy FileDimension:fix/unique_root_constraint to pg

BEGIN;

-- First, clean up any existing duplicate root entries, keeping only the one with the lowest ID.
DELETE FROM public.dim_file
WHERE id IN (
    SELECT id FROM (
        SELECT id, ROW_NUMBER() OVER (PARTITION BY file_name ORDER BY id) as rn
        FROM public.dim_file
        WHERE parent_id IS NULL
    ) t WHERE rn > 1
);

-- Now, create a partial unique index to enforce that only one row can have a NULL parent_id.
CREATE UNIQUE INDEX idx_dim_file_unique_root
ON public.dim_file (parent_id)
WHERE parent_id IS NULL;

COMMENT ON INDEX public.idx_dim_file_unique_root
  IS 'Ensures that only one entry in the table can be the root (have a NULL parent_id).';

COMMIT;
