# src/file_dimension/database.py

from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .config import DATABASE_URL, logger # Import from our new config module

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def ensure_path_exists(session, absolute_path: str) -> int:
	"""
	Ensures a directory path exists in the dim_file table, creating missing
	directories as needed.

	Args:
		session: The SQLAlchemy session object for database communication.
		absolute_path: The absolute directory path (e.g., '/home/user/Pictures').

	Returns:
		The integer ID of the final directory in the path.
	"""
	# Use pathlib for robust path manipulation
	path_obj = Path(absolute_path)
	if not path_obj.is_absolute():
		raise ValueError("Path must be absolute.")

	# 1. Get the ID of the root ('/') directory
	root_id_result = session.execute(
		text("SELECT id FROM public.dim_file WHERE parent_id IS NULL")
	).scalar_one_or_none()

	if root_id_result is None:
		raise RuntimeError("Database is not seeded with a root directory.")

	parent_id = root_id_result

	# 2. Walk the path components, skipping the root
	for part in path_obj.parts[1:]:
		# Find the ID of the child directory
		child_id_result = session.execute(
			text("""
                SELECT id FROM public.dim_file
                WHERE parent_id = :parent_id AND file_name = :name AND is_directory = TRUE
            """),
			{"parent_id": parent_id, "name": part}
		).scalar_one_or_none()

		if child_id_result:
			# If found, it becomes the parent for the next iteration
			parent_id = child_id_result
		else:
			# If not found, create it and get the new ID
			print(f"Creating directory entry: '{part}' under parent_id {parent_id}")
			new_id_result = session.execute(
				text("""
                    INSERT INTO public.dim_file (parent_id, file_name, is_directory)
                    VALUES (:parent_id, :name, TRUE)
                    RETURNING id
                """),
				{"parent_id": parent_id, "name": part}
			).scalar_one()
			parent_id = new_id_result

	# 3. Return the ID of the final directory in the path
	return parent_id

