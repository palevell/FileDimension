# src/file_dimension/database.py

import json
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from .config import DATABASE_URL, logger  # Import from our new config module

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def ensure_path_exists(db: Session, absolute_path: str, cache: dict) -> int:
	"""
	Ensures a directory path exists, using a cache to avoid redundant lookups.
	"""
	# 1. If the full path is already cached, we're done.
	if absolute_path in cache:
		return cache[absolute_path]

	path_obj = Path(absolute_path)
	if not path_obj.is_absolute():
		raise ValueError("Path must be absolute.")

	# 2. Find the one true root directory. This is the crucial first step.
	try:
		root_id = db.execute(
			text("SELECT id FROM public.dim_file WHERE parent_id IS NULL")
		).scalar_one()
		cache['/'] = root_id
	except Exception as e:
		logger.critical("Could not find the single root directory '/' in the database.")
		raise e

	parent_id = root_id

	# 3. Walk the path parts, using the cache as we go
	for i in range(1, len(path_obj.parts)):
		current_path_str = str(Path(*path_obj.parts[:i + 1]))

		if current_path_str in cache:
			parent_id = cache[current_path_str]
			continue

		part = path_obj.parts[i]
		child_id_result = db.execute(
			text("""
                SELECT id FROM public.dim_file
                WHERE parent_id = :parent_id AND file_name = :name AND is_directory = TRUE
            """),
			{"parent_id": parent_id, "name": part}
		).scalar_one_or_none()

		if child_id_result:
			parent_id = child_id_result
		else:
			new_id_result = db.execute(
				text("""
                    INSERT INTO public.dim_file (parent_id, file_name, is_directory)
                    VALUES (:parent_id, :name, TRUE)
                    RETURNING id
                """),
				{"parent_id": parent_id, "name": part}
			).scalar_one()
			parent_id = new_id_result

		cache[current_path_str] = parent_id

	return parent_id


def find_duplicate_sets(db: Session, limit: int = 25) -> list:
	"""
	Finds duplicate files based on their content hash.

	Args:
		db: The SQLAlchemy session.
		limit: The maximum number of duplicate sets to return.

	Returns:
		A list of rows, each containing the hash, count, and total size.
	"""
	logger.info(f"Querying for top {limit} duplicate file sets...")
	query = text("""
        SELECT
            encode(file_hash, 'hex') as file_hash_hex,
            COUNT(*) AS duplicate_count,
            SUM(file_size) AS total_wasted_space
        FROM
            public.dim_file
        WHERE
            file_hash IS NOT NULL
        GROUP BY
            file_hash
        HAVING
            -- This is key: it ensures we are counting unique files (by inode/device)
            -- that share the same hash, not just multiple references (hard links).
            COUNT(DISTINCT (device_id, inode)) > 1
        ORDER BY
            total_wasted_space DESC
        LIMIT :limit;
    """)
	result = db.execute(query, {"limit": limit})
	return result.mappings().all()


def get_full_path_for_id(db: Session, file_id: int, cache: dict) -> str:
	"""Recursively builds the full path for a given file ID, using a cache."""
	if file_id in cache:
		return cache[file_id]

	row = db.execute(
		text("SELECT parent_id, file_name FROM public.dim_file WHERE id = :file_id"),
		{"file_id": file_id}
	).first()

	if row is None:
		return ""

	if row.parent_id is None:  # This is the root
		path = "/"
	else:
		# Recursively get the parent path and append this file's name
		parent_path = get_full_path_for_id(db, row.parent_id, cache)
		path = str(Path(parent_path) / row.file_name)

	cache[file_id] = path
	return path


def initialize_database_old(session):
	"""
	Ensures the foundational data, like the root directory, exists.
	This is an idempotent operation.
	"""
	logger.info("Verifying database initialization...")
	session.execute(
		text("""
            INSERT INTO public.dim_file (parent_id, file_name, is_directory)
            VALUES (NULL, '/', TRUE)
            ON CONFLICT (parent_id, file_name) DO NOTHING;
        """)
	)
# Note: We need a unique constraint on (parent_id, file_name) for this to work.
# Your schema already has this, but parent_id can be NULL.
# A better constraint for the root is a unique index where parent_id IS NULL.
# For now, this will work as long as only one entry has a NULL parent.


def prune_database(db: Session):
	"""
	Scans all file records in the database and removes any that no longer
	exist on the filesystem.
	"""
	logger.info("Starting pre-scan database prune...")

	# A cache is essential for performance here
	path_cache = {}

	# Get all records that are files, not directories
	all_files = db.execute(
		text("SELECT id FROM public.dim_file WHERE is_directory = FALSE")
	).fetchall()

	ids_to_delete = []
	total_files = len(all_files)
	logger.info(f"Verifying existence of {total_files} file records...")

	for i, file_record in enumerate(all_files):
		if (i + 1) % 1000 == 0:
			logger.info(f"Checked {i + 1}/{total_files} files...")

		full_path = get_full_path_for_id(db, file_record.id, path_cache)
		if not os.path.exists(full_path):
			logger.warning(f"File not found, marking for deletion: {full_path}")
			ids_to_delete.append(file_record.id)

	if not ids_to_delete:
		logger.success("Prune complete. No missing files found.")
		return

	logger.info(f"Deleting {len(ids_to_delete)} missing file records...")

	# Delete all missing records in a single query
	db.execute(
		text("DELETE FROM public.dim_file WHERE id = ANY(:ids)"),
		{"ids": ids_to_delete}
	)
	db.commit()
	logger.success(f"Successfully deleted {len(ids_to_delete)} records.")



