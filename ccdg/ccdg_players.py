
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from sqlalchemy import select

from sql_db.models import Player, PlayerDivision, Division
import google_apis.google_tasks as g
from ccdg import ccdg_schedule
from logger.logger import logger_gen as logger

'''
ccdg_players.py

A module for working with player data via the CCDG Weekly CSV Scoring Utility.
    The player table does not contain division.
    The relationship b/t a player and a division is many-to-many and is managed in the
    PlayerDivision table so that it may change over time - i.e. after rebalancing.
'''

## DB Load Functions ##

def add_new_players(db: Session, player_data: list) -> list:
    """
    Reads a list of registered players dictionaries and adds only new players to the database.
    
    Args:
        session (Session): SQLAlchemy database session.
        player_data (list): List of dictionaries representing players as defined 
            in registration (e.g., [{col0: val, col1: val, ...}]).
    
    Returns:
        list: The names of new players in a list
    """
    new_players = []
    new_player_names = []

    # Get all existing player names
    existing_names = {name for (name,) in db.execute(select(Player.full_name)).all()}  

    # Iterate through the player data and add new players to the db
    for player in player_data:        
        player_name = player.get('UDisc Full Name')
        if player_name not in existing_names:
            new_players.append(Player(
                full_name=player_name,
                email=player.get("Email Address"))
                )
            new_player_names.append(player_name)
    
    if new_players:
        db.add_all(new_players)
        db.commit()

    # Return the names of new players
    return new_player_names

def associate_divisions(db: Session, reg_data: list, cycle: int):
    '''
    Associates players with divisions based on the registration data and cycle.
    Processes all players in reg_data, skipping any who already have a division
    assigned for this cycle. Safe to call on every run.

    Args:
        db (Session): SQLAlchemy database session.
        reg_data (list): List of dictionaries representing players as defined in registration.
        cycle (int): The cycle number for which to associate divisions.
    Returns:
        None
    '''

    # get the min & max periods for the cycle
    min_period, max_period = ccdg_schedule.get_min_max_periods_for_cycle(db, cycle)

    # cycle column in the registration data
    cycle_col_name = f'C{str(cycle)} Div' # e.g., C1 Division, C2 Division, etc.

    # get divs and IDs
    divisions = db.execute(select(Division.div_name, Division.division_id)).all()

    # get player IDs that already have a division assignment for this cycle
    existing_assignments = {
        row[0] for row in db.execute(
            select(PlayerDivision.player_id).where(
                PlayerDivision.valid_from_period == min_period
            )
        ).all()
    }

    # loop through all players in registration and add missing PlayerDivision associations
    for player_reg_row in reg_data:
        player_name = player_reg_row.get('UDisc Full Name')

        try:
            division = player_reg_row.get(cycle_col_name)    # e.g., "Alpha", "Bravo", etc.
        except KeyError as e:
            raise e

        player_id = get_player_id_by_name(db, player_name)

        if player_id in existing_assignments:
            continue  # already assigned for this cycle

        division_id = next((value for key, value in divisions if key == division), None)

        if player_id and division_id:
            new_division = PlayerDivision(
                player_id = player_id,
                division_id = division_id,
                valid_from_period = min_period,
                valid_to_period = max_period)
            db.add(new_division)
        else:
            logger.error(f'Error: Player ID or Division ID not found for {player_name} (player_id={player_id}, division="{division}", division_id={division_id})')

    db.commit()
    return

## Utility functions ##

def get_valid_player_ids(db: Session, g_creds_file: str, g_reg_info:object) -> list:
    '''
    Returns a list of player_ids for those who should be included in the reults.
      * checks against the gSheet for registration data
      * player names in this list will match what in the ccdg_db Player table 
    '''
    # load latest registration data from google sheets
    player_registration = g.read_gsheet_range(g_creds_file, g_reg_info)
    player_registration = g.list_to_dict(player_registration)

    player_ids = []
    for p in player_registration:
        if p.get("Payable Status") == "paid":
            player_name = clean_player_name(p.get('UDisc Full Name'))
            p_id = get_player_id_by_name(db, player_name)
            if p_id is not None:
                player_ids.append(p_id)
            else:
                logger.warning(f"Player {player_name} not found in database.")
        else:
            logger.warning(f"Unpaid player: {p.get('UDisc Full Name')}")
    return player_ids

def get_player_id_by_name(db: Session, player_name: str) -> Player:
        """
        Retrieves a player from the database by their full name.

        Args:
            db (sessionmaker): SQLAlchemy database session.
            player_name (str): The full name of the player to retrieve.

        Returns:
            Player: The Player object if found, otherwise None.
        """
        player_id = db.execute(
            select(Player.player_id).where(Player.full_name == player_name).limit(1)
        ).scalar_one_or_none()

        return player_id
        
def clean_player_name (name: str) -> str:
    '''Cleans up player names for consistency in the database'''
    # remove any quotes
    name = name.replace('"', '').replace("'", '')
    # remove @ symbols
    name = name.replace('@', '')
    # remove extra spaces and convert to title case
    name = name.strip().title()

    return name

def update_player_division(
    db: Session,
    player_id: int,
    new_division_id: int,
    valid_from_period: int,
    valid_to_period: int | None = None
):
    """Update a player's division with custom from/to periods."""
    try:
        # Find overlapping existing division records
        overlapping = (
            db.query(PlayerDivision)
            .filter(PlayerDivision.player_id == player_id)
            .filter(
                (PlayerDivision.valid_to_period == None) | 
                (PlayerDivision.valid_to_period >= valid_from_period)
            )
            .filter(PlayerDivision.valid_from_period <= (valid_to_period or valid_from_period))
            .all()
        )

        # Optionally close any overlapping divisions
        for division in overlapping:
            division.valid_to_period = valid_from_period - 1

        # Create new division record
        new_division = PlayerDivision(
            player_id=player_id,
            division_id=new_division_id,
            valid_from_period=valid_from_period,
            valid_to_period=valid_to_period
        )
        db.add(new_division)
        db.commit()

        return f"Player {player_id} set to division {new_division_id} from period {valid_from_period} to {valid_to_period}"

    except NoResultFound:
        return f"Player {player_id} not found"

    except Exception as e:
        db.rollback()
        return f"Error updating division: {e}"

def update_player_udisc_name(
    db: Session,
    player_id: int,
    new_udisc_name: str
):
    """Update a player's UDisc name."""
    try:
        player = db.query(Player).filter(Player.player_id == player_id).one()
        player.udisc_name = new_udisc_name
        db.commit()
        return f"Player {player_id} updated with new UDisc name: {new_udisc_name}"
    except NoResultFound:
        return f"Player {player_id} not found"
    except Exception as e:
        db.rollback()
        return f"Error updating UDisc name: {e}"

def get_player_division_for_period(
    db: Session,
    player_id: int,
    period: int
) -> str:
    """Get the player's division for a specific period."""
    try:
        division = (
            db.query(Division.div_name)
            .join(PlayerDivision)
            .filter(PlayerDivision.player_id == player_id)
            .filter(
                (PlayerDivision.valid_from_period <= period) &
                ((PlayerDivision.valid_to_period == None) | (PlayerDivision.valid_to_period >= period))
            )
            .one()
        )
        return division.div_name
    except NoResultFound:
        return "Unknown"
    except Exception as e:
        logger.error(f"Error retrieving division for player {player_id} in period {period}: {e}")
        return "no player matched"

def get_division_defs(db: Session) -> list:
    """
    Get a list of all divisions in the database.
    
    Args:
        db (Session): SQLAlchemy database session.
    
    Returns:
        list: List of division names.
    """
    try:
        divisions = db.execute(select(Division.div_name, Division.division_id, Division.display_order)).all()
        return divisions
    except Exception as e:
        logger.error(f"Error retrieving divisions: {e}")
        return []
pass

