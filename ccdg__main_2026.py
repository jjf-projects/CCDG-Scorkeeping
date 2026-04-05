"""
ccdg__main_2026.py

Entry point for the CCDG Weekly Scoring Utility.

Run this file each week (manually or via scheduler) after the Sunday play
window closes.  It will:
  1. Sync the schedule and division definitions from Google Sheets → DB
  2. Sync player registrations → DB
  3. Download and store scores for any completed, unscored periods
  4. Back up the database to Google Drive
  5. Regenerate and publish standings to the public Google Sheets

For travel rounds (no UDisc event URL), set local_xlsx_path inside the
scoring loop below before running.  See get_udisc_scores() for details.

DEV_MODE: controlled via .env (DEV_MODE=true/false).  When true, uses the
          dev database and dev standings sheet without touching the live
          public spreadsheet.
"""

import os
import time
import traceback
from sqlalchemy import func, select

import ccdg_settings as settings_module
from sql_db import ccdg_db
from sql_db.models import Score
from ccdg import ccdg_schedule, ccdg_scores, ccdg_players, ccdg_standings, ccdg_summary
import google_apis.google_tasks as g
from logger.logger import logger_gen as logger


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Read from .env — set DEV_MODE=false there for a live production run.
DEV_MODE = os.environ.get('DEV_MODE', 'true').strip().lower() in ('true', '1', 'yes')

CONFIG = settings_module.Configuration(
    settings_module.Settings_2026_dev if DEV_MODE else settings_module.Settings_2026
)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main(exe_dir: str = os.path.dirname(os.path.abspath(__file__))) -> None:
    """Orchestrate the weekly scoring run."""
    mode = "DEV" if DEV_MODE else "PROD"
    logger.info(f"Running in {mode} mode.")

    db_path = os.path.join(exe_dir, CONFIG.DATABASE['DB_DIR'], CONFIG.DATABASE['DB_NAME'])

    with ccdg_db.get_session(db_path, CONFIG.DATABASE['ECHO']) as db:

        # Keep divisions and schedule in sync with Google Sheets on every run
        ccdg_schedule.populate_divisions(db, CONFIG.DIVISIONS)
        ccdg_schedule.update_schedule(db, exe_dir, CONFIG)

        # Sync player registrations — adds new players and assigns any missing divisions
        registration = g.list_to_dict(
            g.read_gsheet_range(CONFIG.G_SVC_CREDS_FILE, CONFIG.G_REGISTRATION)
        )
        for player in registration:
            player['UDisc Full Name'] = ccdg_players.clean_player_name(player['UDisc Full Name'])

        ccdg_players.add_new_players(db, registration)
        ccdg_players.associate_divisions(db, registration, ccdg_schedule.get_current_cycle(db))

        # Score any completed periods that haven't been processed yet
        for period in ccdg_schedule.get_unscored_periods(db, CONFIG.DT_FORMAT['database']):

            # Travel rounds have no event_url — set local_xlsx_path to the
            # scorekeeper-supplied file before running.  Leave as None for
            # regular rounds (scores are fetched from UDisc automatically).
            local_xlsx_path = None
            # local_xlsx_path = r"C:\path\to\travel_round_scores.xlsx"

            logger.info(f"--- Processing period {period} ---")
            period_data = ccdg_scores.get_udisc_scores(db, period, local_xlsx_path)
            if not period_data:
                logger.warning(
                    f"Period {period}: no scores loaded — skipping. "
                    f"Check that the UDisc event URL is set in the schedule sheet."
                )
                continue

            clean = ccdg_scores.clean_score_data(period_data['leaderboard_rows'])
            ccdg_scores.add_scores(db, period, clean)

        # Back up the database to Google Drive
        g.add_file_to_gdrive(CONFIG.G_SVC_CREDS_FILE, db_path, CONFIG.G_DATA_LOGS)

        # Publish standings to Google Sheets
        ccdg_standings.generate_standings(db, registration, CONFIG)

        # Generate a weekly social media summary via Gemini (skipped if no API key).
        # Always summarises the most recently scored period.
        latest_period = db.execute(select(func.max(Score.period))).scalar_one_or_none()
        if latest_period:
            ccdg_summary.generate_weekly_summary(db, latest_period, exe_dir, CONFIG, registration)


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    start = time.time()
    fname = os.path.basename(__file__)
    logger.info(f"### START ### {fname}")

    try:
        main()
        msg = f"### END ### {fname} completed successfully in {time.time() - start:.3f}s"
    except Exception:
        msg = f"### ERROR ### {fname}\n{traceback.format_exc()}"
    finally:
        logger.info(msg)
        print(msg)
