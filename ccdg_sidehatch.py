"""
ccdg_sidehatch.py

A toolkit for semi-regular end-of-cycle tasks that are run manually by the scorekeeper:
  - process_cycle_results_and_rebalance()  Close a cycle, compute final results, assign new divisions
  - apply_new_divisions()                  Write rebalanced divisions back to the database
  - payouts()                              Calculate prize payouts by division and cycle
  - aces()                                 Report all hole-in-ones to a CSV
  - regenerate_summary()                   Re-generate a weekly social media summary via Gemini

HOW TO USE:
  1. Edit main() below to uncomment the task(s) you need.
  2. Run this file directly:  python ccdg_sidehatch.py

NB: The name ccdg_sidehatch was inspired by the classic Tenacious D act that you really 
    should watch - right now. 
    Then just put on a cool 70s groove and get your scorekeeping on.
    https://www.youtube.com/watch?v=212XFe-ICeM  — 
"""

import csv
import math
import os
import time
import traceback

from sqlalchemy import select, func
from sqlalchemy.orm import Session

import ccdg_settings as settings_module
from sql_db import ccdg_db
from sql_db.models import Score, Player, Schedule
from ccdg import ccdg_scores, ccdg_standings, ccdg_schedule, ccdg_players, ccdg_summary
from logger.logger import logger_gen as logger


# ---------------------------------------------------------------------------
# PRIMARY FUNCTIONS
# ---------------------------------------------------------------------------

def process_cycle_results_and_rebalance(cfg, db: Session, cycle: int) -> tuple[list, list]:
    """Close a cycle: compute final results and determine new division assignments.

    This function does NOT write anything to the database.  It returns two
    values for the caller to review before committing:
      - results_csv_rows: ready to write to a CSV for record-keeping
      - new_divisions:    list of [player_id, division_id] to pass to apply_new_divisions()

    Args:
        cfg:    Configuration object (from ccdg_settings.py).
        db:     Active SQLAlchemy Session.
        cycle:  The cycle number to close (1, 2, or 3).

    Returns:
        (results_csv_rows, new_divisions)
    """
    cycle_min, cycle_max = ccdg_schedule.get_min_max_periods_for_cycle(db, cycle)
    all_periods = db.execute(select(func.max(Score.period))).scalar_one_or_none() or 0

    # Get paid player IDs and their scores across all periods
    player_ids = ccdg_players.get_valid_player_ids(db, cfg.G_SVC_CREDS_FILE, cfg.G_REGISTRATION)
    score_rows = ccdg_scores.get_scores_pivot(db, player_ids)
    # score_rows shape: [player_id, name, division, score_p1, score_p2, ...]
    SCORE_START = 3

    # --- Compute per-period points for all players ---
    # points_data shape: [player_id, name, division, pts_p1, pts_p2, ...]
    points_data = [[r[0], r[1], r[2]] for r in score_rows]

    for i in range(all_periods):
        score_col = SCORE_START + i
        # Pass player_id as the key so we can match points back by id (not name)
        period_scores = [
            [r[0], r[score_col]]
            for r in score_rows
            if len(r) > score_col
        ]
        period_points = ccdg_standings.calc_points_for_period(period_scores, cfg.SCORING)
        pts_by_id = {row[0]: row[1] for row in period_points}

        for player_row in points_data:
            player_row.append(pts_by_id.get(player_row[0], 0))

    # --- Gather per-player stats for the closing cycle ---
    # points_data col layout after the loop above:
    #   0=player_id, 1=name, 2=division, 3=pts_p1, 4=pts_p2, ...
    POINTS_START = 3

    player_stats = []   # [player_id, name, weekly_avg, cycle_total, cycle_pad, old_division]
    for row in points_data:
        player_id = row[0]
        name      = row[1]

        # Weekly average points up through the end of the closing cycle
        pts_through_cycle_end = row[POINTS_START : cycle_max + POINTS_START]
        weekly_avg = ccdg_scores.avg_non_zero_vals(pts_through_cycle_end)

        # Cycle totals (only the target cycle's periods)
        cycle_col_start = POINTS_START + (cycle_min - 1)
        cycle_col_end   = POINTS_START + cycle_max
        cycle_pts = row[cycle_col_start : cycle_col_end]
        totals = _tally_cycle_points(cycle_pts, cfg.SCORING["keep_periods"])

        old_div = ccdg_players.get_player_division_for_period(db, player_id, cycle_max) or "Unknown"

        player_stats.append([
            player_id,
            name,
            weekly_avg,
            totals["points_total"],
            totals["points_after_drops"],
            old_div,
        ])

    # --- Rebalance: split players who scored into equal division chunks ---
    # Players with zero weekly average keep their existing division.
    scored = sorted(
        [p for p in player_stats if p[2] > 0],
        key=lambda p: p[2],
        reverse=True,
    )
    unscored = [p for p in player_stats if p[2] == 0]

    chunks = split_list_into_chunks(scored, len(cfg.DIVISIONS))
    new_div_by_id = {}
    for i, chunk in enumerate(chunks):
        div_name = cfg.DIVISIONS[i]     # Alpha = top, Echo = bottom
        for player in chunk:
            new_div_by_id[player[0]] = div_name

    for player in unscored:
        new_div_by_id[player[0]] = player[5]   # keep existing division

    # Warn about any player whose current division isn't in this season's config
    for player in player_stats:
        if player[5] not in cfg.DIVISIONS:
            logger.error(
                f"Player '{player[1]}' (id={player[0]}) has unrecognised division "
                f"'{player[5]}' in cycle {cycle}. Fix via the registration sheet or "
                f"ccdg_players.update_player_division()."
            )

    # --- Build results CSV rows ---
    header = [
        "Name",
        f"Weekly Avg Pts (through C{cycle})",
        f"Total Points C{cycle}",
        f"Points After Drops C{cycle}",
        f"C{cycle} Division",
        f"C{cycle + 1} Division",
    ]
    data_rows = [
        [
            p[1],                       # name
            p[2],                       # weekly avg
            p[3],                       # cycle total
            p[4],                       # cycle pts after drops
            p[5],                       # old division
            new_div_by_id.get(p[0], p[5]),  # new division
        ]
        for p in player_stats
    ]
    # Sort by old division then points after drops descending
    data_rows.sort(key=lambda r: (r[4], -r[3]))
    results_csv_rows = [header] + data_rows

    # --- Resolve new division names to IDs for the DB update ---
    division_defs = ccdg_players.get_division_defs(db)
    div_id_by_name = {d[0]: d[1] for d in division_defs}

    new_divisions = [
        [player_id, div_id_by_name[div_name]]
        for player_id, div_name in new_div_by_id.items()
        if div_name in div_id_by_name
    ]

    logger.info(f"Cycle {cycle} results processed. {len(scored)} players rebalanced.")
    return results_csv_rows, new_divisions


def apply_new_divisions(cfg, db: Session, new_divisions: list, next_cycle: int) -> None:
    """Write rebalanced division assignments to the database for the next cycle.

    Call this after reviewing the output of process_cycle_results_and_rebalance().

    Args:
        cfg:           Configuration object.
        db:            Active SQLAlchemy Session.
        new_divisions: List of [player_id, division_id] from process_cycle_results_and_rebalance().
        next_cycle:    The cycle number the new divisions apply to (closing_cycle + 1).
    """
    from_period, to_period = ccdg_schedule.get_min_max_periods_for_cycle(db, next_cycle)

    for player_id, division_id in new_divisions:
        msg = ccdg_players.update_player_division(db, player_id, division_id, from_period, to_period)
        logger.info(msg)

    logger.info(f"Division assignments updated for {len(new_divisions)} players in cycle {next_cycle}.")


def payouts(results_file: str, divisions: list, cycles: int = 3) -> list:
    """Calculate end-of-season prize payouts by division and cycle.

    Reads a CSV of final cycle results and distributes each division's purse
    among the top ~30% of finishers using a power-curve formula that awards
    more to higher placements.

    Expected CSV columns: Cycle, Name, Cycle Div, Total Points, Points After Drops

    Args:
        results_file: Path to the CSV file with final season results.
        divisions:    List of division names from config (e.g. ['Alpha', ..., 'Echo']).
        cycles:       Number of cycles in the season (default 3).

    Returns:
        List of dicts, one per payout row, ready to write to CSV.
    """
    PURSE_PER_PLAYER = 25   # each player's entry fee contribution to the prize pool ($USD)
    RATIO_WINNERS    = 0.3  # top 30% of each division receive a payout
    DECAY_EXP        = -0.6 # exponent for the power-curve payout distribution

    results = _read_csv_as_dicts(results_file)
    payouts_out = []

    for cycle_num in range(1, cycles + 1):
        for div in divisions:
            div_results = [
                r for r in results
                if int(r['Cycle']) == cycle_num and r['Cycle Div'] == div
            ]
            if not div_results:
                continue

            purse         = len(div_results) * PURSE_PER_PLAYER / cycles
            num_winners   = math.floor(len(div_results) * RATIO_WINNERS)
            if num_winners == 0:
                continue

            # Power-curve weights: rank 1 gets the most, decreasing by DECAY_EXP
            raw_weights   = [(num_winners * (i + 1)) ** DECAY_EXP for i in range(num_winners)]
            weight_sum    = sum(raw_weights)
            award_amounts = [round(w / weight_sum * purse) for w in raw_weights]

            ranked = sorted(div_results, key=lambda r: -float(r['Points After Drops']))

            for rank, amount in enumerate(award_amounts):
                payouts_out.append({
                    "Cycle":           cycle_num,
                    "Division":        div,
                    "Rank":            rank + 1,
                    "Name":            ranked[rank]['Name'],
                    "TotalPoints":     ranked[rank]['Total Points'],
                    "PointsAfterDrops": ranked[rank]['Points After Drops'],
                    "Award":           amount,
                })

    return payouts_out


def aces(db: Session, exe_dir: str, period_list: list | None = None) -> str:
    """Find all aces (hole score == 1) and write them to a CSV in the temp folder.

    Queries Score.hole_scores (a JSON dict) for any hole value equal to 1,
    then joins with Player and Schedule to get the player name, week number,
    and course name.

    Args:
        db:          Active SQLAlchemy Session.
        exe_dir:     Absolute path to the project root (temp folder is relative to this).
        period_list: Optional list of period numbers to restrict the search.
                     When None, all scored periods are searched.

    Returns:
        The path to the written CSV file.
    """
    # Load all score rows that have hole_scores recorded
    query = (
        select(Score.score_id, Score.player_id, Score.period, Score.hole_scores)
        .where(Score.hole_scores.is_not(None))
    )
    if period_list:
        query = query.where(Score.period.in_(period_list))

    score_rows = db.execute(query).all()

    # Build lookups so we avoid per-row DB calls
    players  = {p.player_id: p.full_name  for p in db.execute(select(Player)).scalars()}
    schedule = {s.period: s.course        for s in db.execute(select(Schedule)).scalars()}

    ace_rows = []
    for score_id, player_id, period, hole_scores in score_rows:
        for hole, strokes in hole_scores.items():
            if strokes == 1:
                ace_rows.append({
                    'player': players.get(player_id, f"player_id={player_id}"),
                    'weekNo': period,
                    'course': schedule.get(period, "Unknown"),
                    'hole':   hole,
                })

    # Sort by week then player name for readability
    ace_rows.sort(key=lambda r: (r['weekNo'], r['player']))

    temp_dir = os.path.join(exe_dir, 'temp')
    os.makedirs(temp_dir, exist_ok=True)
    out_path = os.path.join(temp_dir, f'CCDG_{CONFIG.SEASON}_Aces.csv')
    write_dict_as_csv(out_path, ace_rows) if ace_rows else logger.info("No aces found.")

    count = len(ace_rows)
    logger.info(f"Aces report: {count} ace(s) found. Written to {out_path}")
    print(f"{count} ace(s) found — see {out_path}")
    return out_path


def regenerate_summary(
    db: Session,
    exe_dir: str,
    period: int | None = None,
    dry_run: bool = False,
    send_email: bool = False,
) -> None:
    """Re-generate a weekly social media summary using Gemini.

    Reads from the existing database — no Google Sheets sync needed.
    Use this to experiment with prompt changes without running the full
    weekly pipeline.

    Args:
        db:          Active SQLAlchemy Session.
        exe_dir:     Absolute path to the project root.
        period:      Period number to summarise.  Defaults to the latest scored period.
        dry_run:     When True, prints the formatted prompt sent to Gemini without
                     making an API call.  Use this to check the data looks right
                     before spending API tokens.
        send_email:  When True, emails the generated summary after saving it.
                     Requires EMAIL_SENDER, EMAIL_PASSWORD, and EMAIL_RECIPIENTS in .env.
    """
    # Resolve period
    if period is None:
        period = db.execute(select(func.max(Score.period))).scalar_one_or_none()
        if period is None:
            print("No scored periods found in the database.")
            return
        print(f"No period specified — using latest scored period: {period}\n")

    data    = ccdg_summary._collect_period_data(db, period, CONFIG, registration=None)
    context = ccdg_summary._format_context(data, CONFIG)
    prompt  = ccdg_summary._build_prompt(context, exe_dir)

    if dry_run:
        print("=" * 70)
        print("DRY RUN — prompt that would be sent to Gemini:")
        print("=" * 70)
        print(prompt)
        print("=" * 70)
        print("Edit prompts/weekly_summary.txt and re-run to adjust.")
        return

    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        print("ERROR: GEMINI_API_KEY is not set in .env.")
        return

    print(f"Calling Gemini ({CONFIG.GEMINI_MODEL}) for period {period}...\n")
    summary  = ccdg_summary._call_gemini(prompt, api_key, CONFIG)
    out_path = ccdg_summary._save_summary(summary, period, exe_dir, CONFIG)

    print("=" * 70)
    print(summary)
    print("=" * 70)
    print(f"Saved to: {out_path}")

    if send_email:
        subject = f"CCDG {CONFIG.SEASON} — Week {period:02d} Summary"
        ccdg_summary.send_summary_email(subject, summary, exe_dir)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _tally_cycle_points(cycle_pts: list, keep_periods: int) -> dict:
    """Sum points for a single player's cycle and compute points-after-drops.

    Unlike the standings version, this does not raise on incomplete cycles —
    it tallies whatever data is present.  This makes it safe to call mid-season.

    Args:
        cycle_pts:    List of per-period point values for one cycle.
        keep_periods: How many top scores count toward points-after-drops.
    """
    scored = [p for p in cycle_pts if p is not None]
    pts_total = sum(scored)

    if len(scored) > keep_periods:
        pts_after_drops = sum(sorted(scored, reverse=True)[:keep_periods])
    else:
        pts_after_drops = pts_total

    return {
        'points_total':      round(pts_total, 2),
        'points_after_drops': round(pts_after_drops, 2),
    }


def split_list_into_chunks(lst: list, num_chunks: int) -> list:
    """Split a list into num_chunks roughly equal parts.

    Used to divide scored players into division buckets during rebalancing.
    """
    k, m = divmod(len(lst), num_chunks)
    return [lst[i*k + min(i, m) : (i+1)*k + min(i+1, m)] for i in range(num_chunks)]


def _read_csv_as_dicts(file_path: str) -> list:
    """Read a CSV file and return its rows as a list of dicts."""
    with open(file_path, newline='') as f:
        return list(csv.DictReader(f, skipinitialspace=True))


def write_dict_as_csv(fname: str, list_of_dicts: list) -> None:
    """Write a list of dicts to a CSV file."""
    if not list_of_dicts:
        return
    with open(fname, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list_of_dicts[0].keys())
        writer.writeheader()
        writer.writerows(list_of_dicts)


def write_list_of_lists_as_csv(fname: str, rows: list) -> None:
    """Write a list of lists to a CSV file."""
    with open(fname, 'w', newline='') as f:
        csv.writer(f).writerows(rows)


# ---------------------------------------------------------------------------
# MAIN — edit this to run the tasks you need
# ---------------------------------------------------------------------------

DEV_MODE = os.environ.get('DEV_MODE', 'true').strip().lower() in ('true', '1', 'yes')
CONFIG   = settings_module.Configuration(
    settings_module.Settings_2026_dev if DEV_MODE else settings_module.Settings_2026
)


def main():
    exe_dir      = os.path.dirname(os.path.abspath(__file__))
    db_file_path = os.path.join(exe_dir, CONFIG.DATABASE['DB_DIR'], CONFIG.DATABASE['DB_NAME'])

    with ccdg_db.get_session(db_file_path, CONFIG.DATABASE['ECHO']) as db:

        # --- REBALANCING ---
        # Run at the end of each cycle. Review the CSV output before calling apply_new_divisions().
        #
        # CYCLE_TO_CLOSE = 1
        # results, new_divs = process_cycle_results_and_rebalance(CONFIG, db, CYCLE_TO_CLOSE)
        # out_csv = os.path.join(exe_dir, 'temp', f'CCDG_C{CYCLE_TO_CLOSE}_Results.csv')
        # write_list_of_lists_as_csv(out_csv, results)
        # print(f"Results written to {out_csv} — review before applying divisions.")
        #
        # Once reviewed, apply new divisions to the DB:
        # apply_new_divisions(CONFIG, db, new_divs, next_cycle=CYCLE_TO_CLOSE + 1)

        # --- PAYOUTS ---
        # Run once at end of season after all cycle results CSVs are merged.
        #
        # results_file = os.path.join(exe_dir, 'temp', 'CCDG_2026_AllCycleFinalResults.csv')
        # awards = payouts(results_file, CONFIG.DIVISIONS)
        # awards_csv = os.path.join(exe_dir, 'temp', f'CCDG_{CONFIG.SEASON}_Payouts.csv')
        # write_dict_as_csv(awards_csv, awards)

        # --- ACES REPORT ---
        # Finds all hole-in-ones and writes temp/CCDG_2026_Aces.csv
        # Columns: player, weekNo, course, hole
        #
        # aces(db, exe_dir)
        # aces(db, exe_dir, period_list=[1, 2, 3])  # restrict to specific periods

        # --- WEEKLY SUMMARY (Gemini) ---
        # Re-generate the social media summary for any period without a full run.
        # Edit prompts/weekly_summary.txt to tweak tone, length, emphasis, etc.
        # Requires GEMINI_API_KEY in .env.
        #
        # regenerate_summary(db, exe_dir)                                        # latest period
        # regenerate_summary(db, exe_dir, period=5)                              # specific period
        regenerate_summary(db, exe_dir, dry_run=True)                          # preview prompt only, no API call
        # regenerate_summary(db, exe_dir, period=5, dry_run=True)                # combine both
        # regenerate_summary(db, exe_dir, send_email=True)                       # generate + email
        # regenerate_summary(db, exe_dir, period=5, send_email=True)             # specific period + email
        # Requires EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENTS in .env.

        # --- SCHEMA MIGRATION ---
        # Run this once after adding a new column to models.py on a live DB.
        # Safe to run multiple times — skips columns that already exist.
        # (If starting fresh, just delete the .db file instead.)
        #
        # ccdg_db.migrate(db_file_path)

        # --- DATABASE CORRECTIONS ---
        # Delete scores for a bad period and re-run main to re-import:
        #
        # ccdg_scores.delete_scores_for_period(db, period=5)

        # --- MANUAL DIVISION FIX ---
        # Correct a player's division when registration sheet can't be updated:
        #
        # msg = ccdg_players.update_player_division(db, player_id=42, new_division_id=3,
        #                                            valid_from_period=1, valid_to_period=12)
        # print(msg)

        pass   # placeholder to avoid empty block syntax error


if __name__ == "__main__":
    start_time = time.time()
    fname = os.path.basename(__file__)
    logger.info(f"### START ### {fname}")

    try:
        main()
        elapsed = time.time() - start_time
        msg = f"--- {fname} completed successfully in {elapsed:.3f}s ---"
    except Exception:
        msg = f"--- EXECUTION ERROR ---\n{traceback.format_exc()}"
    finally:
        print(msg)
        logger.info(msg)
