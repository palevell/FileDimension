-- Deploy FileDimension:dim_file to pg

BEGIN;

-- This would go into a SQitch deployment script, e.g., deploy/dim_file.sql

CREATE TABLE dim_file (
	-- Primary Key: A unique identifier for this database entry
	id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

	-- Recursive Relationship: Points to the parent directory within this table
	parent_id BIGINT REFERENCES dim_file(id),

	-- File Metadata
	file_name TEXT NOT NULL,
	is_directory BOOLEAN NOT NULL DEFAULT false,

	-- Content & System Identifiers
	file_hash BYTEA, -- Stores the raw SHA-384 hash (48 bytes). More efficient than text.
	file_size BIGINT,  -- Size in bytes. NULL for directories.
	device_id BIGINT,  -- The ID of the device the file resides on (from stat.st_dev).
	inode BIGINT,	   -- The inode number (from stat.st_ino).

	-- Timestamps for auditing
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

	-- Constraints
	-- A file/directory must be unique within its parent directory.
	UNIQUE(parent_id, file_name)
);

-- Index for efficiently looking up children of a directory
CREATE INDEX idx_file_dimension_parent_id ON dim_file(parent_id);

-- Index for finding files by hash
CREATE INDEX idx_file_dimension_file_hash ON dim_file(file_hash) WHERE file_hash IS NOT NULL;

-- Index for finding hard links
CREATE INDEX idx_file_dimension_inode ON dim_file(device_id, inode) WHERE inode IS NOT NULL;

COMMIT;
