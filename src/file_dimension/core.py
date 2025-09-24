# core.py

from pathlib import Path

from environs import Env
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# --- Your environs logic for layered config loading ---
_basedir = Path(__file__).resolve().parent
env = Env(expand_vars=True)
env.read_env(str(_basedir / ".env"))

APP_CONFIG = env.str("APP_CONFIG", default="production").lower()
if APP_CONFIG != "production":
	envfilename = _basedir / ".envs" / f"{APP_CONFIG}.env"
	if envfilename.exists():
		print(f"Loading override config from: {envfilename}")
		env.read_env(str(envfilename), override=True)
# ----------------------------------------------------

# Now, read the final DATABASE_URL from the configured environment
DATABASE_URL = env.str("DATABASE_URL", default=None)
if not DATABASE_URL:
	raise ValueError("DATABASE_URL environment variable is not set.")

# Create the database engine
engine = create_engine(DATABASE_URL)

# Session maker is a factory for creating new Session objects
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


# Example of how to use it
if __name__ == "__main__":
	test_path = "/home/patrick/Pictures"

	# Use a session context manager
	with SessionLocal() as db_session:
		try:
			print(f"Ensuring path '{test_path}' exists...")
			final_id = ensure_path_exists(db_session, test_path)
			print(f"Path '{test_path}' exists with final ID: {final_id}")

			# Commit the changes if any directories were created
			db_session.commit()
		except Exception as e:
			print(f"An error occurred: {e}")
			db_session.rollback()
