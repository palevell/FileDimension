-- Verify FileDimension:feature/auto_update_timestamp on pg

-- Verify that the trigger exists on the dim_file table
SELECT 1 FROM information_schema.triggers
WHERE event_object_table = 'dim_file'
  AND trigger_name = 'trigger_dim_file_updated_at'
  AND trigger_schema = 'public';
