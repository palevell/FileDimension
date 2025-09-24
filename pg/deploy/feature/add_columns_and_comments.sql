-- Deploy FileDimension:feature/add_columns_and_comments to pg

BEGIN;

-- Add the new columns to the table
ALTER TABLE public.dim_file
  ADD COLUMN modified_at TIMESTAMPTZ,
  ADD COLUMN mimetype TEXT;

-- Add comments to the table and its columns for documentation
COMMENT ON TABLE public.dim_file
  IS 'Dimension table for files and directories, representing a hierarchical filesystem structure.';

COMMENT ON COLUMN public.dim_file.id
  IS 'Primary key for the file or directory record.';
COMMENT ON COLUMN public.dim_file.parent_id
  IS 'The ID of the parent directory. NULL for the root entry.';
COMMENT ON COLUMN public.dim_file.file_name
  IS 'The name of the file or directory.';
COMMENT ON COLUMN public.dim_file.is_directory
  IS 'A boolean flag indicating if the entry is a directory (true) or a file (false).';
COMMENT ON COLUMN public.dim_file.file_hash
  IS 'The SHA-384 hash of the file''s content. NULL for directories.';
COMMENT ON COLUMN public.dim_file.file_size
  IS 'The size of the file in bytes. NULL for directories.';
COMMENT ON COLUMN public.dim_file.device_id
  IS 'The identifier for the device the file resides on (from stat.st_dev).';
COMMENT ON COLUMN public.dim_file.inode
  IS 'The inode number of the file on its device (from stat.st_ino). Used with device_id to detect hard links.';
COMMENT ON COLUMN public.dim_file.created_at
  IS 'Timestamp of when the record was first inserted into this table.';
COMMENT ON COLUMN public.dim_file.updated_at
  IS 'Timestamp of when the record was last updated or verified by a scan (the "as of" date).';
COMMENT ON COLUMN public.dim_file.modified_at
  IS 'The actual modification time of the file on the filesystem (from stat.st_mtime).';
COMMENT ON COLUMN public.dim_file.mimetype
  IS 'The detected MIME type of the file.';

COMMIT;
