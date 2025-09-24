-- Verify FileDimension:dim_file on pg

BEGIN;

SELECT  id, parent_id, file_name, is_directory, file_hash, file_size,
	device_id, inode, created_at, updated_at
FROM    dim_file
WHERE   false;

ROLLBACK;
