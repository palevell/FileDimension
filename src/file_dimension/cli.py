# src/file_dimension/cli.py

import json
import typer

from datetime import datetime
from . import processor
from .config import logger, MAX_FILES as MAX_FILES_FROM_ENV  # Rename for clarity
from .database import SessionLocal, find_duplicate_sets, get_full_path_for_id
from sqlalchemy import text

app = typer.Typer()


# --- Helper function for formatting bytes ---
def format_bytes(size: int) -> str:
	"""Formats a size in bytes into a human-readable string."""
	power = 1024
	n = 0
	power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
	while size > power and n < len(power_labels):
		size /= power
		n += 1
	return f"{size:.2f} {power_labels[n]}B"


@app.command()
def find_dupes(
	output: str = typer.Option(
		None,
		"--output",
		"-o",
		help="Output filename for the JSONL report."
	),
	limit: int = typer.Option(
		25, "--limit", "-l", help="The maximum number of duplicate sets to list."
	)
):
	"""
	Finds duplicate files and saves a detailed report in JSONL format.
	"""
	if output is None:
		timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		report_name = f"duplicates-{timestamp}.jsonl"

	logger.info(f"Finding duplicate files, report will be saved to '{report_name}'")

	with SessionLocal() as db_session, open(report_name, "w") as f:
		duplicate_sets = find_duplicate_sets(db=db_session, limit=limit)

		if not duplicate_sets:
			logger.info("No duplicate files found.")
			return

		path_cache = {}  # Cache for reconstructing paths efficiently
		for dupe_set in duplicate_sets:
			file_hash = dupe_set['file_hash_hex']

			# Get all file records for this hash
			file_records = db_session.execute(
				text("SELECT id FROM public.dim_file WHERE encode(file_hash, 'hex') = :hash"),
				{"hash": file_hash}
			).fetchall()

			# Reconstruct the full path for each file
			filenames = [get_full_path_for_id(db_session, record.id, path_cache) for record in file_records]

			# Create the JSON object and write it to the file as a new line
			report_line = {
				"hash": file_hash,
				"count": dupe_set['duplicate_count'],
				"wasted_space": int(dupe_set['total_wasted_space']),
				"filenames": sorted(filenames)  # Sort for consistent output
			}
			f.write(json.dumps(report_line) + "\n")

	logger.success(f"Successfully wrote duplicate file report to '{report_name}'.")


@app.command()
def scan(
	directory: str = typer.Argument(..., help="The root directory to scan."),
	max_files: int = typer.Option(
		MAX_FILES_FROM_ENV,  # Default to the value from the .env file
		"--max-files",
		"-m",
		help="Maximum number of files to process. Overrides MAX_FILES env var. Use -1 for unlimited."
	),
):
	"""
	Scans a directory and populates the File Dimension table in the database.
	"""
	logger.info(f"Starting scan on directory: {directory}")

	# Pass the final max_files value to the processor
	with SessionLocal() as db_session:
		processor.process_directory(
			db=db_session,
			root_directory=directory,
			max_files_override=max_files
		)

	logger.success("Scan completed successfully.")


# --- Add this new, simple command for testing ---
@app.command()
def hello(name: str):
	"""
	A simple test command.
	"""
	print(f"Hello {name}")


def main():
	app()


if __name__ == "__main__":
	main()
