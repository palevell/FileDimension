-- Deploy FileDimension:data/seed_root_directory to pg

BEGIN;

INSERT INTO public.dim_file (
	parent_id,
	file_name,
	is_directory,
	file_hash,
	file_size,
	device_id,
	inode
) VALUES (
	NULL, -- This is the root, so it has no parent.
	'/',  -- A common convention for the root's name.
	TRUE, -- It is a directory.
	NULL,
	NULL,
	NULL,
	NULL
);

COMMIT;
