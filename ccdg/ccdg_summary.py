"""
ccdg_summary.py

Generates a weekly social media summary using the Google Gemini API.

How it works:
  1. Collects this week's results from the DB (scores, points, aces, standings)
  2. Formats them as a readable text block
  3. Loads the prompt template from prompts/weekly_summary.txt
  4. Calls the Gemini API to generate the summary
  5. Saves the result to temp/CCDG_<season>_Summary_Week<N>.txt for human review

The prompt template is a plain text file checked into git — tweak it freely
without touching any Python code.

Requires GEMINI_API_KEY to be set in .env.  If it is absent the function
logs a warning and returns None — the rest of the weekly run is unaffected.
"""

import os
from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session
from google import genai

from sql_db.models import Schedule, Score, Player, PlayerDivision, Division
from ccdg import ccdg_scores, ccdg_schedule
from ccdg.ccdg_standings import (
    calc_points_for_period,
    _build_points_rows,
    _COL_NAME,
    _COL_DIVISION,
    _COL_PTS_AFTER_DROPS,
)
from logger.logger import logger_gen as logger


def generate_weekly_summary(
    db: Session,
    period: int,
    exe_dir: str,
    cfg: object,
    registration: list[dict] | None = None,
) -> str | None:
    """Generate and save a weekly social media summary for the given period.

    Args:
        db:           Active SQLAlchemy Session.
        period:       The scoring period just completed.
        exe_dir:      Absolute path to the project root.
        cfg:          Configuration object (from ccdg_settings.py).
        registration: Registration row dicts from Google Sheets — used to
                      filter unpaid players from the summary, same as
                      standings.  Pass None to skip the unpaid filter.

    Returns:
        The generated summary text, or None if generation was skipped or failed.
    """
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        logger.warning("GEMINI_API_KEY not set in .env — weekly summary skipped.")
        return None

    try:
        data    = _collect_period_data(db, period, cfg, registration)
        context = _format_context(data, cfg)
        prompt  = _build_prompt(context, exe_dir)
        summary = _call_gemini(prompt, api_key, cfg)
        out     = _save_summary(summary, period, exe_dir, cfg)
        logger.info(f"Weekly summary saved: {out}")
        print(f"Weekly summary saved: {out}")
        return summary

    except Exception as e:
        logger.error(f"Weekly summary generation failed for period {period}: {e}")
        return None


# ---------------------------------------------------------------------------
# DATA COLLECTION
# ---------------------------------------------------------------------------

def _collect_period_data(
    db: Session,
    period: int,
    cfg: object,
    registration: list[dict] | None,
) -> dict:
    """Query the DB and build a structured data dict for the period."""

    # Schedule info for this period
    sched = db.execute(
        select(Schedule).where(Schedule.period == period)
    ).scalar_one()

    # Lookup dicts — built once to avoid N+1 DB queries
    player_names   = _build_player_name_lookup(db)
    player_divs    = _build_division_lookup(db, period)
    unpaid_names   = _build_unpaid_set(registration)

    # Raw scores for this period
    raw_scores = db.execute(
        select(Score.player_id, Score.relative_score, Score.round_rating, Score.hole_scores)
        .where(Score.period == period)
    ).all()

    # Build filtered [name, score] pairs for points calculation.
    # Travel rounds use round_rating; regular rounds use relative_score.
    period_scores = []
    for player_id, rel_score, rnd_rating, _ in raw_scores:
        name = player_names.get(player_id, f"Player {player_id}")
        div  = player_divs.get(player_id, "Unknown")
        if name in unpaid_names or div == "Unknown":
            continue
        score = rnd_rating if sched.travel else rel_score
        if score is not None:
            period_scores.append([name, score, div])

    # Compute points for this period
    points_input  = [[r[0], r[1]] for r in period_scores]
    points_result = calc_points_for_period(points_input, cfg.SCORING)
    points_by_name = {r[0]: r[1] for r in points_result}

    # Group by division and sort ascending (lower score = better)
    division_order = {name: i for i, name in enumerate(cfg.DIVISIONS)}
    divisions: dict = {}
    for name, score, div in period_scores:
        divisions.setdefault(div, []).append({
            'player': name,
            'score':  score,
            'points': points_by_name.get(name, 0.0),
        })
    for div in divisions:
        divisions[div].sort(key=lambda r: r['score'])

    # Order divisions by cfg.DIVISIONS (Alpha first, etc.)
    ordered_divisions = {
        div: divisions[div]
        for div in cfg.DIVISIONS
        if div in divisions
    }

    # Aces — any hole where strokes == 1
    aces = []
    for player_id, _, _, hole_scores in raw_scores:
        if hole_scores:
            name = player_names.get(player_id, f"Player {player_id}")
            for hole, strokes in hole_scores.items():
                if strokes == 1:
                    aces.append({'player': name, 'hole': hole})

    # Season standings leaders (top 3 per division)
    standings = _get_standings_leaders(db, cfg, registration)

    # Cycle position (e.g. "Week 4 of 12 in Cycle 1")
    min_period, _ = ccdg_schedule.get_min_max_periods_for_cycle(db, sched.cycle)
    week_of_cycle = period - (min_period or period) + 1

    return {
        'period':       period,
        'cycle':        sched.cycle,
        'week_of_cycle': week_of_cycle,
        'cycle_len':    cfg.SCORING['cycle_len'],
        'course':       sched.course,
        'date':         sched.saturday.strftime(cfg.DT_FORMAT['spreadsheet']),
        'travel':       sched.travel,
        'field_size':   len(period_scores),
        'divisions':    ordered_divisions,
        'aces':         aces,
        'standings':    standings,
    }


def _get_standings_leaders(
    db: Session,
    cfg: object,
    registration: list[dict] | None,
) -> dict[str, list]:
    """Return the top-3 players per division by points-after-drops for the current cycle.

    Returns an empty dict if no periods have been scored yet.
    """
    periods_elapsed = db.execute(select(func.max(Score.period))).scalar_one_or_none()
    if not periods_elapsed:
        return {}

    curr_cycle  = ccdg_schedule.get_current_cycle(db)
    score_rows  = ccdg_scores.get_scores_pivot(db)
    unpaid_names = _build_unpaid_set(registration)

    score_rows = [
        r for r in score_rows
        if r[1] not in unpaid_names and r[2] != "Unknown"
    ]
    if not score_rows:
        return {}

    points_rows = _build_points_rows(score_rows, periods_elapsed, curr_cycle, cfg.SCORING)

    # Group by division, sort descending by points-after-drops, keep top 3
    leaders: dict = {}
    for row in points_rows:
        div = row[_COL_DIVISION]
        leaders.setdefault(div, []).append({
            'player': row[_COL_NAME],
            'pts':    row[_COL_PTS_AFTER_DROPS],
        })

    return {
        div: sorted(players, key=lambda r: r['pts'], reverse=True)[:3]
        for div, players in leaders.items()
    }


# ---------------------------------------------------------------------------
# FORMATTING
# ---------------------------------------------------------------------------

def _format_context(data: dict, cfg: object) -> str:
    """Convert the period data dict into a compact text block for the prompt.

    Only the top N players per division are included (controlled by
    cfg.GEMINI_SUMMARY_TOP_N) to keep token usage low on the free tier.
    Field size is still reported so Gemini knows the full size of the field.
    """
    top_n = getattr(cfg, 'GEMINI_SUMMARY_TOP_N', 3)

    lines = []
    travel_note = "  ★ TRAVEL ROUND" if data['travel'] else ""
    lines.append(
        f"WEEK {data['period']} — "
        f"Cycle {data['cycle']}, Week {data['week_of_cycle']} of {data['cycle_len']}"
        f"{travel_note}"
    )
    lines.append(f"Course : {data['course']}")
    lines.append(f"Date   : {data['date']}")
    lines.append(f"Field  : {data['field_size']} players across {len(data['divisions'])} divisions")

    # Results by division — top N only, full field size shown in header
    lines.append("\n── RESULTS BY DIVISION (top placings shown) ──")
    for div_name, players in data['divisions'].items():
        lines.append(f"\n{div_name.upper()}  ({len(players)} players)")
        for rank, p in enumerate(players[:top_n], 1):
            score_val = p['score']
            if score_val == 0:
                score_str = "  E"
            elif score_val > 0:
                score_str = f"+{score_val}"
            else:
                score_str = str(score_val)
            lines.append(f"  {rank:>2}.  {p['player']:<26} {score_str:>4}   {p['points']:.1f} pts")
        if len(players) > top_n:
            lines.append(f"      ... and {len(players) - top_n} more")

    # Aces
    lines.append("\n── ACES ──")
    if data['aces']:
        for ace in data['aces']:
            lines.append(f"  {ace['player']}  —  {ace['hole']}  at {data['course']}")
    else:
        lines.append("  None this week.")

    # Season standings
    lines.append("\n── SEASON STANDINGS LEADERS (points after drops) ──")
    if data['standings']:
        for div_name in cfg.DIVISIONS:
            if div_name not in data['standings']:
                continue
            leader_str = "  |  ".join(
                f"{l['player']} ({l['pts']:.1f})"
                for l in data['standings'][div_name]
            )
            lines.append(f"  {div_name:<10} {leader_str}")
    else:
        lines.append("  No standings data yet.")

    return "\n".join(lines)


def _build_prompt(context: str, exe_dir: str) -> str:
    """Load the prompt template and inject the formatted context."""
    template_path = os.path.join(exe_dir, 'prompts', 'weekly_summary.txt')
    if not os.path.exists(template_path):
        raise FileNotFoundError(
            f"Prompt template not found at {template_path}. "
            f"Expected prompts/weekly_summary.txt in the project root."
        )
    with open(template_path, encoding='utf-8') as f:
        template = f.read()
    return template.format(weekly_data=context)


# ---------------------------------------------------------------------------
# GEMINI API
# ---------------------------------------------------------------------------

def _call_gemini(prompt: str, api_key: str, cfg: object) -> str:
    """Send the prompt to Gemini and return the response text."""
    model_name = getattr(cfg, 'GEMINI_MODEL', 'gemini-2.0-flash')
    client   = genai.Client(api_key=api_key)
    response = client.models.generate_content(model=model_name, contents=prompt)
    return response.text


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------

def _save_summary(summary: str, period: int, exe_dir: str, cfg: object) -> str:
    """Write the summary to temp/ and return the file path."""
    temp_dir = os.path.join(exe_dir, 'temp')
    os.makedirs(temp_dir, exist_ok=True)
    filename = f"CCDG_{cfg.SEASON}_Summary_Week{period:02d}.txt"
    out_path = os.path.join(temp_dir, filename)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(summary)
    return out_path


# ---------------------------------------------------------------------------
# PRIVATE HELPERS
# ---------------------------------------------------------------------------

def _build_player_name_lookup(db: Session) -> dict[int, str]:
    """Return {player_id: full_name} for all players."""
    return {
        pid: name
        for pid, name in db.execute(select(Player.player_id, Player.full_name)).all()
    }


def _build_division_lookup(db: Session, period: int) -> dict[int, str]:
    """Return {player_id: div_name} for all players who have a division at this period."""
    rows = db.execute(
        select(PlayerDivision.player_id, Division.div_name)
        .join(Division, PlayerDivision.division_id == Division.division_id)
        .where(
            PlayerDivision.valid_from_period <= period,
            or_(
                PlayerDivision.valid_to_period.is_(None),
                PlayerDivision.valid_to_period >= period,
            ),
        )
    ).all()
    return {player_id: div_name for player_id, div_name in rows}


def _build_unpaid_set(registration: list[dict] | None) -> set[str]:
    """Return the set of names for players whose payment status is not 'paid'.

    Returns an empty set if registration data is not available.
    """
    if not registration:
        return set()
    return {
        p.get("UDisc Full Name")
        for p in registration
        if p.get("Payable Status") != "paid"
    }
