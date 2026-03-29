# python & 3rd party
from sqlalchemy import select, func
from sqlalchemy.orm import Session
# custom
from sql_db.models import Score, Schedule
from ccdg import ccdg_scores, ccdg_schedule
import google_apis.google_tasks as g
from logger.logger import logger_gen as logger


'''
ccdg_standings.py

Generates and publishes season standings to Google Sheets.

Three sheets are written each run:
  1. Scores      — raw relative scores by player/period
  2. Points      — points earned per period, plus cycle totals and drops
  3. Weekly Avg  — each player's average points per week played

All three sheets share the same three-row header:
  Row 1: period numbers
  Row 2: Saturday dates
  Row 3: course names  (this row also contains the lead column labels)
'''

# Column positions in a points_row after generate_standings builds it.
# Shape: [name, division, total_pts, pts_after_drops, period_1_pts, period_2_pts, ...]
_COL_NAME           = 0
_COL_DIVISION       = 1
_COL_TOTAL_PTS      = 2
_COL_PTS_AFTER_DROPS = 3
_COL_FIRST_PERIOD   = 4   # per-period points start here


def generate_standings(db: Session, player_registration: list[dict], cfg: object) -> None:
    """Compute standings and write all three sheets to Google Sheets.

    Args:
        db:                  Active SQLAlchemy Session.
        player_registration: List of registration row dicts (from Google Sheets).
        cfg:                 Configuration object (from ccdg_settings.py).
    """
    periods_elapsed = db.execute(select(func.max(Score.period))).scalar_one_or_none()
    if not periods_elapsed:
        logger.warning("No scored periods found — standings not written.")
        return

    curr_cycle = ccdg_schedule.get_current_cycle(db)

    # --- Build the filtered, sorted score data used by all three sheets ---

    # score_rows from get_scores_pivot: [player_id, name, division, score_p1, score_p2, ...]
    # Sort by the most recent period score (ascending = best first).
    score_rows = ccdg_scores.get_scores_pivot(db)
    score_rows = sorted(score_rows, key=lambda r: r[-1] if r[-1] is not None else float('inf'))

    # Scores are stored in the DB for every registered player regardless of payment
    # status or division assignment.  Only filter them out of the *published* standings.
    #
    # Exclude: unpaid players, and players with no division assigned yet ("Unknown").
    # Their scores remain in the DB and will appear in standings once they pay/are assigned.
    unpaid_names = {
        p.get("UDisc Full Name")
        for p in player_registration
        if p.get("Payable Status") != "paid"
    }
    if unpaid_names:
        logger.info(f"Excluding {len(unpaid_names)} unpaid player(s) from standings.")

    before = len(score_rows)
    score_rows = [
        r for r in score_rows
        if r[1] not in unpaid_names   # r[1] = name
        and r[2] != "Unknown"         # r[2] = division; "Unknown" means not yet assigned
    ]
    excluded_no_div = before - len(score_rows) - len(unpaid_names)
    if excluded_no_div > 0:
        logger.info(f"Excluding {excluded_no_div} player(s) with no division assigned yet.")

    # --- Sheet 1: Scores ---
    # Strip player_id (col 0) — not for public consumption.
    # Published shape: [name, division, score_p1, score_p2, ...]
    _write_sheet(
        cfg,
        sheet_key='score_sheet',
        header_rows=create_header_rows(db, cfg.LEAD_COLS_SCORES, cfg.DT_FORMAT["spreadsheet"]),
        data_rows=[r[1:] for r in score_rows],
    )
    logger.info("Scores sheet written.")

    # --- Sheet 2: Points ---
    points_rows = _build_points_rows(score_rows, periods_elapsed, curr_cycle, cfg.SCORING)
    _write_sheet(
        cfg,
        sheet_key='points_sheet',
        header_rows=create_header_rows(db, cfg.LEAD_COLS_POINTS, cfg.DT_FORMAT["spreadsheet"]),
        data_rows=sorted(points_rows, key=lambda r: r[_COL_NAME]),
    )
    logger.info("Points sheet written.")

    # --- Sheet 3: Weekly Average Points ---
    _write_sheet(
        cfg,
        sheet_key='weekly_avg_pts',
        header_rows=[['Player', 'Division', 'Weekly Avg Points']],
        data_rows=_build_avg_points_rows(points_rows),
    )
    logger.info("Weekly average points sheet written.")


# ---------------------------------------------------------------------------
# SHEET BUILDERS
# ---------------------------------------------------------------------------

def _build_points_rows(score_rows: list, periods_elapsed: int, curr_cycle: int, scoring: dict) -> list:
    """Compute per-period points for every player and return the full points table.

    Returns rows shaped: [name, division, total_pts, pts_after_drops, p1_pts, p2_pts, ...]
    """
    # Start with [name, division]; period points will be appended.
    points_data = [[r[1], r[2]] for r in score_rows]

    # score_rows col layout: 0=player_id, 1=name, 2=division, 3+=per-period scores
    SCORE_START = 3
    for i in range(periods_elapsed):
        score_col = SCORE_START + i
        # Each entry: [name, score_or_None]
        period_scores = [
            [r[1], r[score_col]]
            for r in score_rows
            if len(r) > score_col
        ]
        period_points = calc_points_for_period(period_scores, scoring)
        pts_by_name = {row[0]: row[1] for row in period_points}

        for player_row in points_data:
            player_row.append(pts_by_name.get(player_row[_COL_NAME], 0))

    # Insert cycle summary columns at positions 2 and 3.
    # After insert: [name, division, total_pts, pts_after_drops, p1_pts, p2_pts, ...]
    for row in points_data:
        period_pts = row[2:]   # everything after name & division, before inserts
        totals = _tally_cycle_totals(period_pts, curr_cycle, scoring["cycle_len"], scoring["keep_periods"])
        row.insert(_COL_TOTAL_PTS,       totals["points_total"])
        row.insert(_COL_PTS_AFTER_DROPS, totals["points_after_drops"])

    return points_data


def _build_avg_points_rows(points_rows: list) -> list:
    """Compute each player's average points per week played.

    Ignores zero-point weeks (player did not play).
    Returns rows sorted alphabetically: [name, division, avg_pts].
    """
    avg_rows = []
    for r in points_rows:
        period_pts = r[_COL_FIRST_PERIOD:]
        played = [pts for pts in period_pts if pts != 0]
        avg = round(sum(played) / len(played), 2) if played else 0
        avg_rows.append([r[_COL_NAME], r[_COL_DIVISION], avg])

    return sorted(avg_rows, key=lambda r: r[0])


def create_header_rows(db: Session, lead_cols: list[str], date_format: str) -> list[list]:
    """Build the three-row header used on the scores and points sheets.

    Row 1: period numbers  (e.g. 1, 2, 3, ...)
    Row 2: Saturday dates  (e.g. 21-Mar, 28-Mar, ...)
    Row 3: course names    (lead_cols in the first N cells, then course names)

    Args:
        db:          Active SQLAlchemy Session.
        lead_cols:   Labels for the fixed left columns (e.g. ['Name', 'Division']).
        date_format: strftime format string for the date row (e.g. '%d-%b').
    """
    schedule = db.execute(
        select(Schedule.period, Schedule.saturday, Schedule.course)
        .order_by(Schedule.period)
    ).all()

    blanks = [""] * len(lead_cols)
    period_row   = blanks + [str(row.period) for row in schedule]
    date_row     = blanks + [row.saturday.strftime(date_format) if row.saturday else "" for row in schedule]
    course_row   = list(lead_cols) + [row.course or "" for row in schedule]

    return [period_row, date_row, course_row]


# ---------------------------------------------------------------------------
# SCORING ALGORITHM
# ---------------------------------------------------------------------------

def calc_points_for_period(period_scores: list, scoring: dict) -> list:
    """Apply the Percentage + Marnie algorithm to one period's scores.

    The algorithm has two components that together sum to a max of 150 pts:
      - Percentage component (120 pts): rewards placement rank as a % of field size
      - Score-based component (30 pts): rewards margin of victory vs the field range

    Ties are handled by averaging the points that would have been awarded across
    the tied positions.

    Args:
        period_scores: List of [name, score] pairs. score is relative_score (int)
                       or None for players who did not play.
        scoring:       Dict with keys: percentage_modifier, score_based_modifier.

    Returns:
        List of [name, points] pairs for players who played (None scores excluded).
    """
    # Remove non-players and sort ascending (lower score = better)
    played = sorted(
        [r for r in period_scores if r[1] is not None],
        key=lambda r: r[1],
    )

    if not played:
        return []

    total      = len(played)
    best_score = int(played[0][1])
    worst_score = int(played[-1][1])
    score_range = worst_score - best_score  # used for score-based component

    result = []
    i = 0
    while i < total:
        score = int(played[i][1])

        # Count how many players share this score (ties)
        tie_count = 1
        while i + tie_count < total and int(played[i + tie_count][1]) == score:
            tie_count += 1

        # Sum the percentage points that would go to each tied position, then split evenly
        pct_pts_sum = sum(
            ((total - (i + offset)) / total) * scoring['percentage_modifier']
            for offset in range(tie_count)
        )
        pct_pts = pct_pts_sum / tie_count

        # Score-based component: 0 when all players tie (score_range == 0)
        if score_range > 0:
            score_pts = (scoring['score_based_modifier'] / score_range) * (worst_score - score)
        else:
            score_pts = 0

        points = round(pct_pts + score_pts, 2)

        for offset in range(tie_count):
            result.append([played[i + offset][0], points])

        i += tie_count

    return result


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _tally_cycle_totals(season_points: list, cycle: int, cycle_len: int, keep_periods: int) -> dict:
    """Sum points for the current cycle and compute points-after-drops.

    Only counts scores from the current cycle (not prior cycles).
    Points-after-drops keeps only the best `keep_periods` scores.

    Args:
        season_points: All of a player's per-period points for the full season.
        cycle:         The cycle number to tally (1-based).
        cycle_len:     Number of periods in a cycle (12).
        keep_periods:  How many scores count toward points-after-drops (6).
    """
    start = (cycle - 1) * cycle_len
    end   = min(start + cycle_len, len(season_points))
    cycle_pts = [p for p in season_points[start:end] if p is not None]

    pts_total = sum(cycle_pts)

    if len(cycle_pts) > keep_periods:
        pts_after_drops = sum(sorted(cycle_pts, reverse=True)[:keep_periods])
    else:
        pts_after_drops = pts_total

    return {
        'points_total':      round(pts_total, 2),
        'points_after_drops': round(pts_after_drops, 2),
    }


def _write_sheet(cfg, sheet_key: str, header_rows: list, data_rows: list) -> None:
    """Clear and rewrite one Google Sheet tab.

    Args:
        cfg:         Configuration object.
        sheet_key:   Key in cfg.G_STANDINGS (e.g. 'score_sheet', 'points_sheet').
        header_rows: List of rows to prepend before the data.
        data_rows:   List of data rows to write.
    """
    g.write_gsheet_range(
        cfg.G_SVC_CREDS_FILE,
        {'file_id': cfg.G_STANDINGS['file_id'], 'sheet_id': cfg.G_STANDINGS[sheet_key]},
        header_rows + data_rows,
    )
