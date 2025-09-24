# main_2.py

from loguru import logger
from sqlalchemy import text

from core import SessionLocal, ensure_path_exists, env
from finder import find_files

# --- Loguru Configuration ---
# Configure logger to show file and line number for easier debugging
logger.add("file_dimension.log", rotation="10 MB", level="INFO")
logger.info("Logger initialized.")


# ----------------------------

def calculate_sha384(file_path: str) -> str | None:


# ... (function remains the same) ...

# Use logger.catch for clean exception handling
@logger.catch
def process_directory(root_directory: str):
	"""
	Scans a directory and populates the file dimension table, processing
	up to a maximum number of files defined by the MAX_FILES env variable.
	"""
	max_files = env.int("MAX_FILES", default=1000)

	with SessionLocal() as session:
		file_generator = find_files(root_directory=root_directory)

		logger.info(f"Starting processing. Max files to process: {max_files if max_files > 0 else 'All'}")

		for i, info_dict in enumerate(file_generator):
			if max_files > 0 and i >= max_files:
				logger.info(f"Reached MAX_FILES limit of {max_files}. Stopping.")
				break

			logger.info(f"Processing ({i + 1}): {info_dict['full_path']}")

			# 1. Ensure the parent directory exists
			parent_id = ensure_path_exists(session, info_dict['parent_path'])

			# 2. Calculate hash
			file_hash = calculate_sha384(info_dict['full_path'])

			# 3. Check for existing file
			existing_file = session.execute(
				text("""
                    SELECT id, file_hash, modified_at FROM public.dim_file
                    WHERE parent_id = :parent_id AND file_name = :file_name
                """),
				{"parent_id": parent_id, "file_name": info_dict['file_name']}
			).first()

			# 4. INSERT or UPDATE logic
			if existing_file is None:
				logger.debug(f"INSERTING new record for {info_dict['file_name']}")
				# ... (INSERT logic remains the same) ...
			else:
				if (existing_file.file_hash != file_hash or
					existing_file.modified_at != info_dict['modified_at']):
					logger.debug(f"UPDATING record for {info_dict['file_name']}")
					# ... (UPDATE logic remains the same) ...
				else:
					logger.debug(f"Skipping unchanged file: {info_dict['file_name']}")

		session.commit()
		logger.success("Processing complete. Changes committed.")


if __name__ == "__main__":
	target_directory = "/path/to/your/test/directory"  # <-- IMPORTANT: Change this
	process_directory(target_directory)
