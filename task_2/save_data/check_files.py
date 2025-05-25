import os
import json

TRACKING_FILE = "processed_files_task_two.json"

def is_already_processed(file_path):
    """
    Checks if the given file has already been processed.

    It does so by comparing the last modified timestamp of the file
    against the stored metadata in `processed_files.json`.

    Returns:
        True if the file exists in the tracking file and has not changed since last run.
        False otherwise.
    """
    if not os.path.exists(TRACKING_FILE):
        return False

    with open(TRACKING_FILE, "r") as f:
        processed = json.load(f)

    last_modified = os.path.getmtime(file_path)
    file_name = os.path.basename(file_path)

    return file_name in processed and processed[file_name] == last_modified

def mark_as_processed(file_path):
    """
    Records that the file has been processed by storing its
    last modified time into `processed_files.json`.

    This helps avoid re-processing the same file in future ETL runs.
    """
    file_name = os.path.basename(file_path)
    last_modified = os.path.getmtime(file_path)

    if os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, "r") as f:
            processed = json.load(f)
    else:
        processed = {}

    processed[file_name] = last_modified
    with open(TRACKING_FILE, "w") as f:
        json.dump(processed, f, indent=2)
