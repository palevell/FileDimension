# main.py

import hashlib
import os.path

from sqlalchemy import text

from core import SessionLocal, ensure_path_exists, env  # Import env from core
from finder import find_files


def calculate_sha384(file_path: str) -> str | None:
	"""Calculates the SHA-384 hash of a file."""
	sha384_hash = hashlib.sha384()
	try:
		with open(file_path, "rb") as f:
			# Read the file in chunks to handle large files efficiently
			for chunk in iter(lambda: f.read(4096), b""):
				sha384_hash.update(chunk)
		return sha384_hash.hexdigest()
	except (FileNotFoundError, IsADirectoryError):
		return None


def process_directory(root_directory: str):
	"""
	Scans a directory and populates the file dimension table, processing
	up to a maximum number of files defined by the MAX_FILES env variable.
	"""
	# Get the file limit from the environment
	max_files = env.int("MAX_FILES", default=1000)  # Default to 1000 if not set

	with SessionLocal() as session:
		try:
			file_generator = find_files(root_directory=root_directory)

			print(f"Starting processing. Max files to process: {max_files if max_files > 0 else 'All'}")

			# --- Main Processing Loop with MAX_FILES limit ---
			for i, info_dict in enumerate(file_generator):
				# Your break logic
				if max_files > 0 and i >= max_files:
					print(f"\nReached MAX_FILES limit of {max_files}. Stopping.")
					break

				print(f"Processing ({i + 1}): {info_dict['full_path']}")

				# 1. Ensure the parent directory exists in the DB and get its ID
				parent_id = ensure_path_exists(session, info_dict['parent_path'])

				# 2. Calculate the file's hash
				file_hash = calculate_sha384(info_dict['full_path'])

				# 3. Check if the file record already exists
				existing_file = session.execute(
					text("""
			                        SELECT id, file_hash, modified_at FROM public.dim_file
			                        WHERE parent_id = :parent_id AND file_name = :file_name
			                    """),
					{"parent_id": parent_id, "file_name": info_dict['file_name']}
				).first()

				# 4. INSERT or UPDATE logic
				if existing_file is None:
					# INSERT new record
					session.execute(
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
					if (existing_file.file_hash != file_hash or
						existing_file.modified_at != info_dict['modified_at']):
						session.execute(
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
								"modified_at": info_dict['modified_at'],
								"mimetype": info_dict['mimetype'],
								"inode": info_dict['inode'],
								"device_id": info_dict['device_id'],
								"file_id": existing_file.id
							}
						)
			session.commit()
			print("\nProcessing complete. Changes committed.")

		except Exception as e:
			print(f"An error occurred: {e}")
			session.rollback()
			print("Transaction rolled back.")


if __name__ == "__main__":
	# Example: Scan the user's home directory (use a smaller one for testing!)
	# target_directory = os.path.expanduser("~")
	target_directory = os.path.expanduser("~/Pictures/Gemini")
	process_directory(target_directory)

"""

def calculate_sha384(file_path: str) -> str | None:
    # ... (function remains the same) ...


"""
