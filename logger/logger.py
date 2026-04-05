"""
logger.py

Configures the shared logger used across the CCDG scoring utility.

Two handlers are attached to every run:
  - File handler:    logs/YYYY-MM-DD.log  (one file per calendar day)
  - Console handler: prints INFO and above to the terminal so admins can
                     see what's happening without tailing the log file

Log format: 2026-03-15 10:23:45,123 [INFO] - message text

Log files are kept in the logs/ directory at the project root.
logs/ is gitignored but is backed up to Google Drive on each run.
"""

import logging
import os
from logging import FileHandler, StreamHandler, Formatter
from datetime import datetime


_HERE    = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(_HERE, '..', 'logs')
LOG_DIR  = os.path.normpath(LOG_DIR)

LOG_FORMAT   = "%(asctime)s [%(levelname)s] - %(message)s"
DATE_FORMAT  = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL    = logging.INFO

# One log file per calendar day — date-stamped name makes them easy to archive.
log_filename = datetime.now().strftime('%Y-%m-%d') + '.log'
LOG_FILE     = os.path.join(LOG_DIR, log_filename)

os.makedirs(LOG_DIR, exist_ok=True)

formatter = Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

# File handler — full record of every run
_file_handler = FileHandler(LOG_FILE)
_file_handler.setLevel(LOG_LEVEL)
_file_handler.setFormatter(formatter)

# Console handler — visible in the terminal during interactive runs
_console_handler = StreamHandler()
_console_handler.setLevel(LOG_LEVEL)
_console_handler.setFormatter(formatter)

logger_gen = logging.getLogger('ccdg')
logger_gen.setLevel(LOG_LEVEL)

# Guard against duplicate handlers if this module is imported more than once
if not logger_gen.handlers:
    logger_gen.addHandler(_file_handler)
    logger_gen.addHandler(_console_handler)


def delete_log_files() -> None:
    """Delete all log files in the logs/ directory.

    Subdirectories are not affected.  Useful for housekeeping at the
    start of a new season.
    """
    if not os.path.isdir(LOG_DIR):
        logger_gen.warning(f"Log directory not found: {LOG_DIR}")
        return

    for filename in os.listdir(LOG_DIR):
        file_path = os.path.join(LOG_DIR, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                logger_gen.info(f"Deleted log file: {file_path}")
            except Exception as e:
                logger_gen.error(f"Failed to delete {file_path}: {e}")
