# python & 3rd party
import os
from datetime import date, datetime
from sqlalchemy import select, func, delete
from sqlalchemy.orm import Session
# custom
from sql_db.models import Schedule, Score, Division
import google_apis.google_tasks as g
from logger.logger import logger_gen as logger


'''
ccdg_schedule.py

Functions for managing the season schedule and division definitions.

The Schedule table is the backbone of the system — every score, division
assignment, and standings calculation references period numbers that come
from here.  The schedule itself lives in a Google Sheet maintained by the
league admins; this module reads it and keeps the DB in sync.
'''


def update_schedule(db: Session, exe_dir: str, cfg: object) -> None:
    """Replace the Schedule table with the latest data from Google Sheets.

    Clears and rewrites every row on each run so that any corrections made
    in the sheet (wrong date, added URL, etc.) are always reflected in the DB.

    Schedule sheet column order (A2:I):
        [0] period, [1] saturday, [2] sunday, [3] course, [4] layout,
        [5] travel, [6] <unused>, [7] cycle, [8] event_url

    Args:
        db:      Active SQLAlchemy Session.
        exe_dir: Absolute path to the project root (for resolving the creds file).
        cfg:     Configuration object (from ccdg_settings.py).
    """
    g_creds  = os.path.join(exe_dir, cfg.G_SVC_CREDS_FILE)
    dt_fmt   = cfg.DT_FORMAT['spreadsheet']

    rows = g.read_gsheet_range(g_creds, cfg.G_SCHEDULE)

    db.execute(delete(Schedule))
    db.commit()

    for r in rows:
        db.add(Schedule(
            period    = int(r[0]),
            saturday  = datetime.strptime(r[1], dt_fmt).date(),
            sunday    = datetime.strptime(r[2], dt_fmt).date(),
            course    = r[3],
            layout    = r[4],
            travel    = r[5].strip().lower() in ('true', '1', 'yes'),
            cycle     = int(r[7]),
            event_url = r[8] if len(r) > 8 and r[8] else None,
        ))

    db.commit()
    logger.info(f"Schedule updated — {len(rows)} periods loaded.")


def populate_divisions(db: Session, division_list: list) -> None:
    """Replace the Division table with the divisions defined in settings.

    Called on every main() run to ensure the DB matches config.DIVISIONS.
    Display order follows the list order (index 0 = top division = Alpha).

    Args:
        db:            Active SQLAlchemy Session.
        division_list: Ordered list of division names from cfg.DIVISIONS.
    """
    db.execute(delete(Division))
    db.commit()

    for i, name in enumerate(division_list):
        db.add(Division(div_name=name, display_order=i + 1))

    db.commit()
    logger.info(f"Divisions set: {division_list}")


def get_unscored_periods(db: Session, date_format: str) -> list[int]:
    """Return a list of periods whose play window has closed but have no scores yet.

    A period is considered complete when its Sunday date is in the past.
    Only periods after the last scored period are checked, so already-processed
    periods are never revisited.

    Args:
        db:          Active SQLAlchemy Session.
        date_format: strftime format for date comparisons (e.g. '%Y-%m-%d').
    """
    last_scored = db.execute(select(func.max(Score.period))).scalar() or 0
    today = date.today()

    return [
        period
        for period, sunday in db.execute(select(Schedule.period, Schedule.sunday)).all()
        if _to_date(sunday, date_format) < today and period > last_scored
    ]


def get_current_cycle(db: Session) -> int | None:
    """Return the cycle number of the most recently completed scoring period.

    Subtracts one day from today so that on a Monday (the typical run day)
    the function correctly identifies the cycle that just finished on Sunday.

    Returns None if no periods have closed yet.
    """
    yesterday = date.today().replace(day=date.today().day - 1)

    return db.execute(
        select(Schedule.cycle)
        .where(Schedule.sunday <= yesterday)
        .order_by(Schedule.sunday.desc())
        .limit(1)
    ).scalar_one_or_none()


def get_min_max_periods_for_cycle(db: Session, cycle: int) -> tuple[int | None, int | None]:
    """Return the (first_period, last_period) for a given cycle number.

    Used to set valid_from_period / valid_to_period on PlayerDivision rows
    and to slice points data for cycle totals.

    Returns (None, None) if the cycle has no schedule entries.
    """
    result = db.execute(
        select(
            func.min(Schedule.period).label("min_period"),
            func.max(Schedule.period).label("max_period"),
        ).where(Schedule.cycle == cycle)
    ).one_or_none()

    return result if result and result[0] is not None else (None, None)


# ---------------------------------------------------------------------------
# PRIVATE HELPERS
# ---------------------------------------------------------------------------

def _to_date(value, fmt: str) -> date:
    """Coerce a string or date object to a date."""
    if isinstance(value, date):
        return value
    return datetime.strptime(value, fmt).date()
