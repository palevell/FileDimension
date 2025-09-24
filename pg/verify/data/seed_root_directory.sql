-- Verify FileDimension:data/seed_root_directory on pg

BEGIN;

SELECT  id, parent_id, file_name, is_directory, file_hash, file_size,
	device_id, inode, created_at, updated_at
FROM    dim_file
WHERE   file_name = '/';

ROLLBACK;
