# src/file_dimension/processor.py

from sqlalchemy import text
from sqlalchemy.orm import Session

from .config import logger  # , MAX_FILES
from .database import ensure_path_exists  # , initialize_database
from .files import find_files, calculate_sha384


# Use logger.catch for clean exception handling
@logger.catch
def process_directory(db: Session, root_directory: str, max_files_override: int):
	"""
	Scans a directory and populates the file dimension table, processing
	up to a maximum number of files defined by the MAX_FILES env variable.
	"""
	max_files = max_files_override

	# with SessionLocal() as session:
	try:
		# logger.info(f"Initializing database . . .")
		# initialize_database(db)

		# Create a cache for this run to store resolved directory IDs
		path_cache = {}

		file_generator = find_files(root_directory=root_directory)

		logger.info(f"Starting processing. Max files to process: {max_files if max_files > 0 else 'All'}")

		# --- Main Processing Loop with MAX_FILES limit ---
		for i, info_dict in enumerate(file_generator):
			# Your break logic
			if max_files > 0 and i >= max_files:
				logger.info(f"Reached MAX_FILES limit of {max_files}. Stopping.")
				break

			# Only show the first 25 files, and then every 100th
			if i < 25 or i % 100 == 99:
				logger.info(f"Processing ({i + 1:,d}): {info_dict['full_path']}")

			# 1. Ensure the parent directory exists in the DB and get its ID
			# parent_id = ensure_path_exists(db, info_dict['parent_path'])
			# Pass the cache to the function
			parent_id = ensure_path_exists(db, info_dict['parent_path'], path_cache)

			# 2. Calculate the file's hash
			file_hash = calculate_sha384(info_dict['full_path'])
			logger.info(f"file_hash -> {file_hash}")

			# 3. Check if the file record already exists
			existing_file = db.execute(
				text("""
								SELECT id, file_hash, modified_at FROM public.dim_file
								WHERE parent_id = :parent_id AND file_name = :file_name
							"""),
				{"parent_id": parent_id, "file_name": info_dict['file_name']}
			).first()

			# 4. INSERT or UPDATE logic
			if existing_file is None:
				# INSERT new record
				logger.debug(f"INSERTING new record for {info_dict['file_name']}")
				db.execute(
					text("""
									INSERT INTO public.dim_file (
										parent_id, file_name, is_directory, file_hash, file_size,
										device_id, inode, modified_at, mimetype
									) VALUES (
										:parent_id, :file_name, FALSE, :file_hash, :file_size,
										:device_id, :inode, :modified_at, :mimetype
									)
								"""),
					{
						"parent_id": parent_id,
						"file_name": info_dict['file_name'],
						"file_hash": file_hash,
						"file_size": info_dict['file_size'],
						"device_id": info_dict['device_id'],
						"inode": info_dict['inode'],
						"modified_at": info_dict['modified_at'],
						"mimetype": info_dict['mimetype']
					}
				)
			else:
				# UPDATE existing record if necessary (e.g., if hash or mtime changed)
				# Convert the database's datetime back to a float timestamp for a reliable comparison
				existing_mtime_ts = existing_file.modified_at.timestamp()

				logger.debug(f"modified_at: {existing_file.modified_at} vs {info_dict['modified_at']}")
				logger.debug(f"mtime   : {existing_mtime_ts} vs {info_dict['mtime_ts']}")
				logger.debug(f"hash    : {existing_file.file_hash.decode()} vs {file_hash}")
				# Compare the raw float timestamps
				if (existing_file.file_hash.decode() != file_hash or
					int(existing_mtime_ts) != int(info_dict['mtime_ts'])):

					logger.debug(f"UPDATING record for {info_dict['file_name']}")
					db.execute(
						text("""
							UPDATE public.dim_file SET
								file_hash = :file_hash,
								file_size = :file_size,
								modified_at = :modified_at,
								mimetype = :mimetype,
								inode = :inode,
								device_id = :device_id
							WHERE id = :file_id
						"""),
						{
							"file_hash": file_hash,
							"file_size": info_dict['file_size'],
							"modified_at": info_dict['modified_at'],  # Still insert the datetime object
							"mimetype": info_dict['mimetype'],
							"inode": info_dict['inode'],
							"device_id": info_dict['device_id'],
							"file_id": existing_file.id
						}
					)
				else:
					logger.debug(f"Skipping unchanged file: {info_dict['file_name']}")
			if i % 250 == 0:
				db.commit()
		db.commit()
		logger.success("Processing complete. Changes committed.")
	except Exception as e:
		logger.critical(f"An error occurred: {e}")
		db.rollback()  # Roll back the passed-in session
		raise  # Re-raise the exception so the caller knows something went wrong
