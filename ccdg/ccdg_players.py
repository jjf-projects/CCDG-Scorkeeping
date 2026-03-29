# python & 3rd party
from sqlalchemy.orm import Session
from sqlalchemy import select, or_
from sqlalchemy.exc import NoResultFound
# custom
from sql_db.models import Player, PlayerDivision, Division
import google_apis.google_tasks as g
from ccdg import ccdg_schedule
from logger.logger import logger_gen as logger


'''
ccdg_players.py

Functions for managing players, division assignments, and registration data.

Key design note:
  Player rows hold identity (name, email) only.
  Division is stored in PlayerDivision with valid_from/valid_to period ranges,
  so a player's division can change after each rebalancing cycle without
  losing historical data.
'''


# ---------------------------------------------------------------------------
# REGISTRATION — add players and assign divisions from the registration sheet
# ---------------------------------------------------------------------------

def add_new_players(db: Session, player_data: list) -> None:
    """Add players from the registration sheet who are not yet in the Player table.

    Safe to call on every run — existing players are skipped.

    Args:
        db:          Active SQLAlchemy Session.
        player_data: List of row dicts from the registration Google Sheet.
                     Must contain 'UDisc Full Name' and 'Email Address' keys.
    """
    existing_names = {
        name for (name,) in db.execute(select(Player.full_name)).all()
    }

    new_players = [
        Player(
            full_name = p['UDisc Full Name'],
            email     = p.get('Email Address'),
        )
        for p in player_data
        if p.get('UDisc Full Name') not in existing_names
    ]

    if new_players:
        db.add_all(new_players)
        db.commit()
        logger.info(f"Added {len(new_players)} new player(s): {[p.full_name for p in new_players]}")


def associate_divisions(db: Session, reg_data: list, cycle: int) -> None:
    """Assign divisions to all players who don't yet have one for this cycle.

    Reads the cycle's division column (e.g. 'C1 Div') from the registration
    sheet and creates a PlayerDivision row for each player missing one.
    Safe to call on every run — already-assigned players are skipped.

    Args:
        db:       Active SQLAlchemy Session.
        reg_data: List of row dicts from the registration Google Sheet.
        cycle:    Current cycle number (1, 2, or 3).
    """
    min_period, max_period = ccdg_schedule.get_min_max_periods_for_cycle(db, cycle)
    if min_period is None:
        logger.error(f"Cannot associate divisions: no schedule periods found for cycle {cycle}.")
        return

    cycle_col = f'C{cycle} Div'   # e.g. 'C1 Div', 'C2 Div'

    # Build lookup dicts so we avoid repeated DB queries in the loop
    division_id_by_name = {
        name: div_id
        for name, div_id in db.execute(select(Division.div_name, Division.division_id)).all()
    }
    already_assigned = {
        player_id for (player_id,) in db.execute(
            select(PlayerDivision.player_id).where(
                PlayerDivision.valid_from_period == min_period
            )
        ).all()
    }

    added = 0
    for reg_row in reg_data:
        player_name = reg_row.get('UDisc Full Name')
        division_name = reg_row.get(cycle_col)

        if not division_name:
            logger.warning(f"No division in column '{cycle_col}' for player '{player_name}' — skipping.")
            continue

        player_id = get_player_id_by_name(db, player_name)
        if player_id is None:
            # Player not in DB yet — this shouldn't happen if add_new_players() ran first
            logger.error(f"Player '{player_name}' not found in the Player table. Run add_new_players() first.")
            continue

        if player_id in already_assigned:
            continue  # already has a division for this cycle

        division_id = division_id_by_name.get(division_name)
        if division_id is None:
            logger.error(
                f"Division '{division_name}' for player '{player_name}' not found in the Division table. "
                f"Valid divisions: {list(division_id_by_name.keys())}"
            )
            continue

        db.add(PlayerDivision(
            player_id         = player_id,
            division_id       = division_id,
            valid_from_period = min_period,
            valid_to_period   = max_period,
        ))
        added += 1

    db.commit()
    logger.info(f"Associated divisions for {added} player(s) in cycle {cycle}.")


# ---------------------------------------------------------------------------
# DIVISION LOOKUPS — used by scores, standings, and sidehatch
# ---------------------------------------------------------------------------

def get_player_division_for_period(db: Session, player_id: int, period: int) -> str | None:
    """Return the division name for a player at a given period, or None if not found.

    This is the single authoritative division lookup used across the codebase.
    Pass the current period for standings; pass the last period of a cycle for
    rebalancing / end-of-cycle results.

    Args:
        db:        Active SQLAlchemy Session.
        player_id: PK from the Player table.
        period:    The scoring period to look up.
    """
    result = db.execute(
        select(Division.div_name)
        .join(PlayerDivision, Division.division_id == PlayerDivision.division_id)
        .where(
            PlayerDivision.player_id == player_id,
            PlayerDivision.valid_from_period <= period,
            or_(
                PlayerDivision.valid_to_period.is_(None),
                PlayerDivision.valid_to_period >= period,
            ),
        )
        .order_by(PlayerDivision.valid_from_period.desc())
        .limit(1)
    ).scalar_one_or_none()

    return result


def update_player_division(
    db: Session,
    player_id: int,
    new_division_id: int,
    valid_from_period: int,
    valid_to_period: int | None = None,
) -> str:
    """Change a player's division, closing any overlapping existing assignments.

    Used by sidehatch after rebalancing to apply new cycle division assignments.

    Args:
        db:               Active SQLAlchemy Session.
        player_id:        PK from the Player table.
        new_division_id:  PK from the Division table.
        valid_from_period: First period the new division applies.
        valid_to_period:   Last period (inclusive); None means open-ended.

    Returns:
        A status message string (suitable for logging).
    """
    try:
        # Close any existing assignments that overlap with the new range
        overlapping = db.execute(
            select(PlayerDivision).where(
                PlayerDivision.player_id == player_id,
                or_(
                    PlayerDivision.valid_to_period.is_(None),
                    PlayerDivision.valid_to_period >= valid_from_period,
                ),
                PlayerDivision.valid_from_period <= (valid_to_period or valid_from_period),
            )
        ).scalars().all()

        for existing in overlapping:
            existing.valid_to_period = valid_from_period - 1

        db.add(PlayerDivision(
            player_id         = player_id,
            division_id       = new_division_id,
            valid_from_period = valid_from_period,
            valid_to_period   = valid_to_period,
        ))
        db.commit()
        return (
            f"Player {player_id} assigned to division {new_division_id} "
            f"from period {valid_from_period} to {valid_to_period}."
        )

    except Exception as e:
        db.rollback()
        return f"Error updating division for player {player_id}: {e}"


# ---------------------------------------------------------------------------
# REGISTRATION FILTERS — determine which players appear in standings
# ---------------------------------------------------------------------------

def get_valid_player_ids(db: Session, g_creds_file: str, g_reg_info: object) -> list:
    """Return a list of player_ids for all paid, registered players.

    Reads the latest registration data from Google Sheets each time so that
    mid-season payment status changes are reflected without a DB update.

    Args:
        db:           Active SQLAlchemy Session.
        g_creds_file: Path to the Google service account credentials file.
        g_reg_info:   Google Sheets config object with file_id, sheet_id, range.
    """
    raw = g.read_gsheet_range(g_creds_file, g_reg_info)
    registration = g.list_to_dict(raw)

    player_ids = []
    for p in registration:
        if p.get("Payable Status") != "paid":
            logger.warning(f"Unpaid player excluded from standings: {p.get('UDisc Full Name')}")
            continue
        name = clean_player_name(p.get('UDisc Full Name', ''))
        pid = get_player_id_by_name(db, name)
        if pid is not None:
            player_ids.append(pid)
        else:
            logger.warning(f"Paid player '{name}' not found in the Player table.")

    return player_ids


# ---------------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------------

def get_player_id_by_name(db: Session, player_name: str) -> int | None:
    """Return the player_id for a given full name, or None if not found."""
    return db.execute(
        select(Player.player_id)
        .where(Player.full_name == player_name)
        .limit(1)
    ).scalar_one_or_none()


def clean_player_name(name: str) -> str:
    """Normalise a player name from UDisc or registration input.

    Strips quotes, @ symbols, extra whitespace, and title-cases the result.
    Ensures names match consistently between the UDisc export and the Player table.
    """
    name = name.replace('"', '').replace("'", '').replace('@', '')
    return name.strip().title()


def get_division_defs(db: Session) -> list:
    """Return all divisions as a list of (div_name, division_id, display_order) tuples."""
    return db.execute(
        select(Division.div_name, Division.division_id, Division.display_order)
        .order_by(Division.display_order)
    ).all()
