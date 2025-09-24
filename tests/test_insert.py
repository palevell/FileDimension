# tests/test_insert.py
# Tuesday, September 23, 2025

"""
This test module verifies the INSERT functionality for the 'dim_file' table
in the SQA database. It uses SQLAlchemy for database interaction, Faker
for generating realistic test data, and is structured for Python's built-in
unittest framework.

This test is designed to be non-destructive. It assumes the database schema
is already managed (e.g., by Sqitch) and wraps each test in a transaction
that is rolled back, ensuring the database state is unchanged after tests run.
"""

import hashlib
import unittest
from datetime import datetime

from faker import Faker
from sqlalchemy import (create_engine, MetaData, Table, Column,
						BigInteger, Text, Boolean, LargeBinary, DateTime,
						ForeignKey, insert, select)

# --- Test Configuration ---
DATABASE_URI = "postgresql+psycopg://localhost/patrick_dev"

# Initialize Faker for generating test data
fake = Faker()

# --- Database Setup ---
engine = create_engine(DATABASE_URI, echo=False)
metadata = MetaData()

# Define the 'dim_file' table structure using SQLAlchemy's Table object.
# This reflects the table, but the test will not create or drop it.
dim_file = Table(
	'dim_file',
	metadata,
	Column('id', BigInteger, primary_key=True),
	Column('parent_id', BigInteger, ForeignKey('dim_file.id')),
	Column('file_name', Text, nullable=False),
	Column('is_directory', Boolean, nullable=False, default=False),
	Column('file_hash', LargeBinary),
	Column('file_size', BigInteger),
	Column('device_id', BigInteger),
	Column('inode', BigInteger),
	Column('created_at', DateTime(timezone=True), default=datetime.utcnow),
	Column('updated_at', DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
)


class TestDimFileInsertion(unittest.TestCase):
	"""
	Test suite for inserting data into the dim_file table.
	"""

	def setUp(self):
		"""
		Begins a transaction before each test. This creates a sandbox for the
		test to run in, isolated from the database's permanent state.
		"""
		self.connection = engine.connect()
		self.transaction = self.connection.begin()
		print("\n--- Transaction started ---")

	def tearDown(self):
		"""
		Rolls back the transaction after each test. This undoes any changes
		the test made, leaving the database clean for the next test.
		"""
		self.transaction.rollback()
		self.connection.close()
		print("--- Transaction rolled back, connection closed ---")

	def test_insert_directory_and_file(self):
		"""
		Tests the insertion of a parent directory followed by a file within it.
		This verifies the recursive relationship and basic data integrity.
		"""
		# --- Step 1: Insert a root directory ---
		dir_name = fake.word()
		dir_insert_stmt = insert(dim_file).values(
			file_name=dir_name,
			is_directory=True,
			file_hash=None,
			file_size=None,
			device_id=fake.random_int(min=1, max=100),
			inode=fake.random_int(min=1000, max=9999)
		).returning(dim_file.c.id)

		dir_result = self.connection.execute(dir_insert_stmt)
		parent_dir_id = dir_result.scalar_one()
		print(f"Inserted directory '{dir_name}' with ID: {parent_dir_id}")

		self.assertIsNotNone(parent_dir_id)

		# --- Step 2: Insert a file into the new directory ---
		file_name = fake.file_name(category='text')
		file_content = fake.paragraph(nb_sentences=10).encode('utf-8')
		file_hash_bytes = hashlib.sha3_384(file_content).digest()
		file_size_bytes = len(file_content)

		file_insert_stmt = insert(dim_file).values(
			parent_id=parent_dir_id,
			file_name=file_name,
			is_directory=False,
			file_hash=file_hash_bytes,
			file_size=file_size_bytes,
			device_id=fake.random_int(min=1, max=100),
			inode=fake.random_int(min=1000, max=9999)
		)
		self.connection.execute(file_insert_stmt)
		print(f"Inserted file '{file_name}' into directory ID {parent_dir_id}")

		# --- Step 3: Verify the insertions ---
		dir_select_stmt = select(dim_file).where(dim_file.c.id == parent_dir_id)
		dir_row = self.connection.execute(dir_select_stmt).first()
		self.assertIsNotNone(dir_row)
		self.assertEqual(dir_row.file_name, dir_name)
		self.assertTrue(dir_row.is_directory)
		self.assertIsNone(dir_row.parent_id)

		file_select_stmt = select(dim_file).where(dim_file.c.parent_id == parent_dir_id)
		file_row = self.connection.execute(file_select_stmt).first()
		self.assertIsNotNone(file_row)
		self.assertEqual(file_row.file_name, file_name)
		self.assertFalse(file_row.is_directory)
		self.assertEqual(file_row.file_hash, file_hash_bytes)
		self.assertEqual(file_row.file_size, file_size_bytes)

		print("Verification successful for both directory and file.")


# --- Standard boilerplate to run tests directly ---
if __name__ == '__main__':
	unittest.main()
