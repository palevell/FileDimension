# tests/test_processor.py

from datetime import datetime, timedelta

# In tests/test_processor.py
import pytest

from src.file_dimension import processor
from src.file_dimension.database import SessionLocal, engine, text


@pytest.fixture
def db_session():
	# This fixture provides a clean, isolated, and correctly seeded transaction.
	connection = engine.connect()
	transaction = connection.begin()
	session = SessionLocal(bind=connection)

	try:
		# 1. WIPE THE SLATE CLEAN.
		session.execute(text("TRUNCATE TABLE public.dim_file RESTART IDENTITY CASCADE"))

		# 2. SEED THE REQUIRED DATA for the test to run.
		session.execute(
			text("INSERT INTO public.dim_file (file_name, is_directory, parent_id) VALUES ('/', TRUE, NULL)")
		)

		# 3. Provide the prepared session to the test.
		yield session

	finally:
		session.close()
		transaction.rollback()
		connection.close()


def db_session_old():
	# This fixture now has one job: provide a clean, isolated transaction.
	connection = engine.connect()
	transaction = connection.begin()
	session = SessionLocal(bind=connection)

	try:
		# WIPE THE SLATE CLEAN before each test.
		session.execute(text("TRUNCATE TABLE public.dim_file RESTART IDENTITY CASCADE"))

		# DO NOT seed the root here. Let the application do it.

		yield session  # Provide the clean session to the test.

	finally:
		session.close()
		transaction.rollback()
		connection.close()


# --- The Test Function ---
# def test_process_directory_inserts_new_file(db_session, mocker):
def test_process_directory_inserts_new_file(db_session, mocker):
	"""
	Verify that process_directory correctly inserts a new file record
	when it encounters a file not in the database.
	"""
	# 1. Arrange: Create fake file data for our generator to return
	fake_file_data = {
		"full_path": "/test/foo.txt",
		"parent_path": "/test",
		"file_name": "foo.txt",
		"file_size": 123,
		"mimetype": "text/plain",
		"modified_at": datetime.now().astimezone(),
		"mtime_ts": datetime.now().timestamp(),
		"device_id": 1,
		"inode": 101,
	}

	# 2. Mock: Hijack the filesystem functions to control their output
	# Mock find_files to return our one fake file
	mocker.patch('src.file_dimension.processor.find_files', return_value=[fake_file_data])
	# Mock calculate_sha384 to return a fixed hash
	mocker.patch('src.file_dimension.processor.calculate_sha384', return_value='fake_hash_string')

	# 3. Act: Run the processor on a dummy directory
	# processor.process_directory("/test")
	processor.process_directory(db=db_session,
								root_directory="/test",
								max_files_override=-1,  # <-- Add the missing argument here
								)

	# 4. Assert: Check the database to see if the record was created correctly
	result = db_session.execute(
		text("SELECT * FROM public.dim_file WHERE file_name = 'foo.txt'")
	).first()

	assert result is not None
	assert result.file_name == "foo.txt"
	assert result.file_hash == b'fake_hash_string'  # Driver encodes it to bytes
	assert result.file_size == 123


def test_process_directory_updates_existing_file(db_session, mocker):
	"""
	Verify that process_directory correctly updates a record for a file
	that has changed since the last scan.
	"""
	# 1. Arrange: Seed the database with the "old" file record.
	#    First, create its parent directory.
	parent_id = db_session.execute(
		text("INSERT INTO public.dim_file (parent_id, file_name, is_directory) VALUES (1, 'test', TRUE) RETURNING id")
	).scalar_one()

	old_file_name = "foo.txt"
	old_hash = "old_hash_string"
	old_mtime = datetime.now().astimezone() - timedelta(days=1)

	# Insert the initial file record
	insert_result = db_session.execute(
		text("""
            INSERT INTO public.dim_file (parent_id, file_name, file_hash, modified_at, is_directory)
            VALUES (:parent_id, :name, :hash, :mtime, FALSE)
            RETURNING id
        """),
		{"parent_id": parent_id, "name": old_file_name, "hash": old_hash.encode(), "mtime": old_mtime}
	).scalar_one()

	# 2. Mock: Create "new" data for the same file that has been "modified".
	new_hash = "new_hash_string"
	new_mtime = datetime.now().astimezone()

	new_file_data = {
		"full_path": "/test/foo.txt",
		"parent_path": "/test",
		"file_name": old_file_name,
		"file_size": 456,
		"mimetype": "text/plain",
		"modified_at": new_mtime,
		"mtime_ts": new_mtime.timestamp(),
		"device_id": 1,
		"inode": 101,
	}

	mocker.patch('src.file_dimension.processor.find_files', return_value=[new_file_data])
	mocker.patch('src.file_dimension.processor.calculate_sha384', return_value=new_hash)

	# 3. Act: Run the processor.
	processor.process_directory(
		db=db_session,
		root_directory="/test",
		max_files_override=-1
	)

	# 4. Assert: Verify the original record was updated with the new data.
	updated_record = db_session.execute(
		text("SELECT * FROM public.dim_file WHERE id = :file_id"),
		{"file_id": insert_result}
	).first()

	assert updated_record is not None
	assert updated_record.file_hash.decode() == new_hash
	assert int(updated_record.modified_at.timestamp()) == int(new_mtime.timestamp())
	assert updated_record.file_size == 456


