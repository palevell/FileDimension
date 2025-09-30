-- Verify FileDimension:fix/unique_root_constraint on pg

-- Verify that the partial unique index for the root directory exists.
-- The query should return a single row with a "true" value to pass.
SELECT TRUE
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname = 'idx_dim_file_unique_root';
