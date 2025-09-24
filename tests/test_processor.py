# tests/test_processor.py

from datetime import datetime

# In tests/test_processor.py
import pytest

from src.file_dimension import processor
from src.file_dimension.database import SessionLocal, engine, text


@pytest.fixture
def db_session():
	connection = engine.connect()
	# 1. Start a single transaction for the entire test
	transaction = connection.begin()
	session = SessionLocal(bind=connection)

	try:
		# 2. Seed the database. This data is now part of the transaction
		#    and is visible to this session. DO NOT COMMIT.
		session.execute(
			text("INSERT INTO public.dim_file (file_name, is_directory, parent_id) VALUES ('/', TRUE, NULL)")
		)

		# 3. Hand the pre-loaded session over to the test function
		yield session

	finally:
		# 4. This rollback is guaranteed to run and will wipe out
		#    everything done in this test, including the seeded root dir.
		session.close()
		transaction.rollback()
		connection.close()


# --- The Test Function ---
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
	processor.process_directory("/test")

	# 4. Assert: Check the database to see if the record was created correctly
	result = db_session.execute(
		text("SELECT * FROM public.dim_file WHERE file_name = 'foo.txt'")
	).first()

	assert result is not None
	assert result.file_name == "foo.txt"
	assert result.file_hash == b'fake_hash_string'  # Driver encodes it to bytes
	assert result.file_size == 123
