-- Verify FileDimension:feature/add_columns_and_comments on pg

-- The query should return a single row with a "true" value to pass.
SELECT COUNT(*) = 2 AS columns_exist
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name   = 'dim_file'
  AND column_name  IN ('modified_at', 'mimetype');
