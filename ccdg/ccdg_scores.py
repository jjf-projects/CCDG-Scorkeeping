# python & 3rd party
import os
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse, urlunparse
from sqlalchemy import select, func, delete
from sqlalchemy.orm import Session
# custom
from sql_db.models import Schedule, Score, Player
from ccdg import ccdg_players
from logger.logger import logger_gen as logger


'''
ccdg_scores.py

Utility functions for score import and DB reads used to generate standings.
  - Import:  load scores from a UDisc xlsx export (web or local) into the Score table
  - Read:    query the Score table and shape data for standings generation

UDisc export column names (snake_cased by pandas):
  name, division, position, entry_number,
  event_total_score, event_relative_score, round_rating, hole_1 ... hole_N
'''


# ---------------------------------------------------------------------------
# IMPORT — load scores from UDisc xlsx and write to the Score table
# ---------------------------------------------------------------------------

def get_udisc_scores(db: Session, period: int, local_xlsx_path: str | None = None) -> dict[str, object]:
    """Fetch the UDisc leaderboard for a period and return it as a dict.

    For regular rounds the leaderboard is downloaded from the UDisc event URL
    stored in the Schedule table.  For travel rounds no URL exists; the
    scorekeeper manually downloads the combined leaderboard xlsx and passes
    its path via `local_xlsx_path`.

    Args:
        db:               Active SQLAlchemy Session.
        period:           Scoring period number.
        local_xlsx_path:  Absolute path to a local xlsx file.  Required when
                          the schedule row has no event_url (travel rounds).

    Returns:
        {'period': int, 'leaderboard_rows': list[dict]}
        or {} if scores could not be loaded.
    """
    schedule_row = db.execute(
        select(Schedule).where(Schedule.period == period)
    ).scalar_one_or_none()

    if schedule_row is None:
        logger.error(f"Period {period} not found in the Schedule table.")
        return {}

    if schedule_row.event_url:
        # Build the UDisc export URL from the event URL.
        # See: https://forum.udisc.com/t/request-public-api/375/28
        parsed = urlparse(schedule_row.event_url)
        export_path = parsed.path.rstrip('/') + "/export"
        export_url = urlunparse((parsed.scheme, parsed.netloc, export_path, '', '', ''))
        rows = _fetch_web_xlsx(export_url)
    else:
        # Travel round — scorekeeper provides a local xlsx with negated round ratings.
        # See models.py Score.round_rating for the sign convention.
        if not local_xlsx_path:
            logger.error(
                f"Period {period} is a travel round (no event_url) but no "
                f"local_xlsx_path was provided. Pass the path to get_udisc_scores()."
            )
            return {}
        rows = _load_local_xlsx(local_xlsx_path)

    if not rows:
        logger.error(f"No score data loaded for period {period}.")
        return {}

    return {'period': period, 'leaderboard_rows': rows}


def clean_score_data(score_rows: list[dict]) -> list[dict]:
    """Clean and filter raw UDisc leaderboard rows.

    - Normalises player names (strips spaces, title-cases)
    - Keeps only entry_number == 1 (first round) when multiple rounds exist
    - Drops DNF and WITN rows

    Args:
        score_rows: List of row dicts straight from the UDisc xlsx.

    Returns:
        Cleaned list of row dicts.
    """
    # Normalise names first, on the original list
    for row in score_rows:
        row['name'] = ccdg_players.clean_player_name(row['name'])

    # Filter to first round only when entry_number column is present.
    # UDisc omits this column for single-round events.
    # See: https://forum.udisc.com/t/changes-to-export/468387
    if score_rows and 'entry_number' in score_rows[0]:
        score_rows = [r for r in score_rows if r['entry_number'] == 1]

    score_rows = [r for r in score_rows if r.get('position') != 'DNF']
    score_rows = [r for r in score_rows if r.get('division', '').upper() != 'WITN']

    return score_rows


def add_scores(db: Session, period: int, score_rows: list) -> None:
    """Write cleaned leaderboard rows to the Score table.

    Skips unregistered players (not in the Player table) and duplicate
    entries (score already exists for that player/period).

    Args:
        db:          Active SQLAlchemy Session.
        period:      Scoring period number.
        score_rows:  Output of clean_score_data().
    """
    logger.info(f"Adding scores for period {period} ({len(score_rows)} rows)...")
    for row in score_rows:
        player_id = ccdg_players.get_player_id_by_name(db, row['name'])
        if not player_id:
            logger.warning(f"Unregistered player — skipping: {row['name']}")
            continue
        if _score_exists(db, player_id, period):
            continue
        _insert_score(db, player_id, period, row)
    db.commit()


# ---------------------------------------------------------------------------
# READ — query Score table for standings generation
# ---------------------------------------------------------------------------

def get_scores_pivot(db: Session, player_ids: list[int] | None = None) -> list[list]:
    """Return all player scores pivoted so each period is a column.

    This is the single source of truth for score data used by both
    standings generation and end-of-cycle rebalancing.

    Args:
        db:         Active SQLAlchemy Session.
        player_ids: Optional list of player_ids to include.  When provided
                    only those players are returned (used by sidehatch for
                    rebalancing paid players only).  When None, all players
                    with any score are returned.

    Returns:
        List of rows: [player_id, full_name, division, score_p1, score_p2, ...]
        Scores are relative_score (int) or None if the player did not play
        that period.  Division is the player's current division or "Unknown".
    """
    periods = db.execute(
        select(Score.period).distinct().order_by(Score.period)
    ).scalars().all()

    if not periods:
        return []

    # One correlated subquery per period — coalesce to None when player didn't play.
    period_cols = [
        func.coalesce(
            select(Score.relative_score)
            .where(
                (Score.player_id == Player.player_id) &
                (Score.period == p)
            )
            .correlate(Player)
            .scalar_subquery(),
            None
        ).label(f"p{p}")
        for p in periods
    ]

    query = (
        select(Player.player_id, Player.full_name, *period_cols)
        .distinct()
        .join(Score, isouter=True)
        .order_by(Player.full_name)
    )
    db_rows = db.execute(query).all()

    result = []
    for player_id, full_name, *scores in db_rows:
        if player_ids is not None and player_id not in player_ids:
            continue
        division = ccdg_players.get_player_division_for_period(
            db, player_id, periods[-1]
        ) or "Unknown"
        result.append([player_id, full_name, division, *scores])

    return result


def avg_non_zero_vals(vals: list) -> float:
    """Return the average of non-zero values in a list, rounded to 3 decimal places.

    Used to compute weekly average points for rebalancing.
    Zero is treated as 'did not play' and excluded from the average.
    """
    non_zero = [v for v in vals if v]
    return round(sum(non_zero) / len(non_zero), 3) if non_zero else 0.0


# ---------------------------------------------------------------------------
# MAINTENANCE — utilities for corrections and reruns
# ---------------------------------------------------------------------------

def delete_scores_for_period(db: Session, period: int) -> None:
    """Delete all scores for a period.  Use this to correct a bad import.

    Args:
        db:     Active SQLAlchemy Session.
        period: Period number whose scores should be deleted.
    """
    db.execute(delete(Score).where(Score.period == period))
    db.commit()
    logger.warning(f"Deleted all scores for period {period}.")


# ---------------------------------------------------------------------------
# PRIVATE HELPERS
# ---------------------------------------------------------------------------

def _fetch_web_xlsx(url: str) -> list:
    """Download an xlsx from a URL and return rows as a list of dicts."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return pd.read_excel(BytesIO(response.content)).to_dict(orient="records")
    except Exception as e:
        logger.error(f"Failed to fetch xlsx from {url}: {e}")
        return []


def _load_local_xlsx(file_path: str) -> list:
    """Load an xlsx from a local path and return rows as a list of dicts."""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        return pd.read_excel(file_path).to_dict(orient="records")
    except Exception as e:
        logger.error(f"Failed to load local xlsx from {file_path}: {e}")
        return []


def _score_exists(db: Session, player_id: int, period: int) -> bool:
    """Return True if a score already exists for this player/period."""
    exists = db.execute(
        select(Score.score_id).where(
            (Score.player_id == player_id) & (Score.period == period)
        )
    ).scalar_one_or_none()
    if exists:
        logger.warning(f"Score already exists for player_id={player_id} period={period} — skipping.")
    return exists is not None


def _insert_score(db: Session, player_id: int, period: int, score_data: dict) -> None:
    """Insert a single Score row. Caller is responsible for commit.

    Hole scores are extracted from any key starting with 'hole_' and stored
    as a JSON dict on the Score row (e.g. {"hole_1": 3, "hole_2": 4, ...}).
    This handles layouts with any number of holes.

    Args:
        db:          Active SQLAlchemy Session.
        player_id:   FK to the Player table.
        period:      Scoring period number.
        score_data:  Row dict from the UDisc xlsx.
    """
    hole_scores = {k: v for k, v in score_data.items() if k.startswith('hole_')} or None

    db.add(Score(
        player_id      = player_id,
        period         = period,
        total_score    = score_data.get('event_total_score'),
        relative_score = score_data.get('event_relative_score'),
        round_rating   = score_data.get('round_rating'),
        hole_scores    = hole_scores,
    ))
