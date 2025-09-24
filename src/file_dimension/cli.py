# src/file_dimension/cli.py

import typer

from . import processor
from .config import logger

app = typer.Typer()


@app.command()
def scan(
	directory: str = typer.Argument(..., help="The root directory to scan."),
):
	"""
	Scans a directory and populates the File Dimension table in the database.
	"""
	logger.info(f"Starting scan on directory: {directory}")
	try:
		processor.process_directory(directory)
		logger.success("Scan completed successfully.")
	except Exception as e:
		logger.critical(f"A critical error occurred: {e}")
		raise typer.Exit(code=1)


# --- Add this new, simple command for testing ---
@app.command()
def hello(name: str):
    """
    A simple test command.
    """
    print(f"Hello {name}")
# --------------------------------------------------


def main():
	app()


if __name__ == "__main__":
	main()
