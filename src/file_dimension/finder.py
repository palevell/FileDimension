# finder.py

import mimetypes
import os
from datetime import datetime
from typing import Iterator, Dict, Any

import magic


def get_magic_mime_type(filename: os.PathLike) -> str | None:
	"""Gets the MIME type by reading the file's magic numbers."""
	try:
		with open(filename, "rb") as fp:
			buffer = fp.read(2048)
			return magic.from_buffer(buffer, mime=True)
	except FileNotFoundError:
		return None


def find_files(
	root_directory: str,
	mimetype: str | None = None,
	since_dt: datetime | None = None,
	until_dt: datetime | None = None,
	min_size: int | None = None,
) -> Iterator[Dict[str, Any]]:
	"""
	Scans a directory tree and yields timezone-aware metadata for files
	that match the criteria.
	"""
	mimetypes.init()

	since_ts = since_dt.timestamp() if since_dt else None
	until_ts = until_dt.timestamp() if until_dt else None

	for dirpath, _, filenames in os.walk(root_directory):
		for filename in filenames:
			full_path = os.path.join(dirpath, filename)

			try:
				stats = os.stat(full_path)
				file_mtime_ts = stats.st_mtime

				# Filter Logic
				if min_size is not None and stats.st_size < min_size:
					continue
				if since_ts is not None and file_mtime_ts < since_ts:
					continue
				if until_ts is not None and file_mtime_ts > until_ts:
					continue

				detected_mimetype, _ = mimetypes.guess_type(full_path)
				if detected_mimetype is None:
					detected_mimetype = get_magic_mime_type(full_path)

				if mimetype is not None and detected_mimetype != mimetype:
					continue

				# --- Yield Timezone-Aware Metadata ---
				yield {
					"full_path": full_path,
					"file_name": filename,
					"parent_path": dirpath,
					"file_size": stats.st_size,
					"mimetype": detected_mimetype,
					"modified_at": datetime.fromtimestamp(file_mtime_ts).astimezone(),  # Clear name
					"device_id": stats.st_dev,
					"inode": stats.st_ino,
				}

			except FileNotFoundError:
				continue
			except Exception as e:
				print(f"Error processing {full_path}: {e}")
				continue


"""

"""
