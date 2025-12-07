'''
Creates log files locally.  There are two:
- General Logs
    Both process_inbox.py and generate_standings.py will write to the same file each time the main program is run
    Has datastamp in the filename
- PLAYERS.log
    Is a season-long log file that details:
      - new players each week (for checking that they paid to join)
      - Divisional changes
      - any errors updating database

These logs will be synced to google with each run so they may be accessed remotely by admins.
'''

import logging, os
from logging import FileHandler
from logging import Formatter
from datetime import datetime


# https://blog.muya.co.ke/configuring-multiple-loggers-python/

LOG_DIR = os.path.join(os.path.abspath(__file__), ".." ,"..",'logs')
LOG_FORMAT = ("%(asctime)s [%(levelname)s] - %(message)s ")
LOG_LEVEL = logging.INFO
DT_FORMAT = '%Y-%m-%d'
GENERAL_LOG_FILE = os.path.join(LOG_DIR, f'{datetime.now().strftime(DT_FORMAT)}.log')


if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)

# general logger - writes a file whenever this runs
logger_gen = logging.getLogger('CCDG_csv_util.gen_logging')
logger_gen.setLevel(LOG_LEVEL)
logger_gen_file_handler = FileHandler(GENERAL_LOG_FILE)
logger_gen_file_handler.setLevel(LOG_LEVEL)
logger_gen_file_handler.setFormatter(Formatter(LOG_FORMAT))
logger_gen.addHandler(logger_gen_file_handler)

# # Players - one file per whole season
# logger_players = logging.getLogger('CCDG_csv_util.player_logging')
# logger_players.setLevel(LOG_LEVEL)
# players_logger_file_handler = FileHandler(PLAYERS_LOG_FILE)
# players_logger_file_handler.setLevel(LOG_LEVEL)
# players_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
# logger_players.addHandler(players_logger_file_handler)

def delete_log_files():
    """
    Deletes all files in the specified directory. Subdirectories are not affected.
    """
    directory = LOG_DIR  # Specify the directory you want to delete files from
    if not os.path.isdir(directory):
        logging.warning(f"{directory} is not a valid directory.")
        return

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Deleted file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete {file_path}: {e}")