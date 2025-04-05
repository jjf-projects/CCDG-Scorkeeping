
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from sqlalchemy import select


from sql_db.models import Player, PlayerDivision, Division
from ccdg import ccdg_schedule
import logger.logger as logger

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

def associate_divisions(db: Session, reg_data: list, new_players: list, cycle: int):

    # get the min & max periods for the cycle
    min_period, max_period = ccdg_schedule.get_min_max_periods_for_cycle(db, cycle)

    # cycle column in the registration data
    cycle_col_name = f'C{str(cycle)} Division' # e.g., C1 Division, C2 Division, etc.

    # get divs and IDs
    divisions = db.execute(select(Division.div_name, Division.division_id)).all()

    # loop through new players and add PlayerDivision associations
    for player_name in new_players:
        
        # Find the player's division in the registration data
        player_reg_row = [p for p in reg_data if p['UDisc Full Name'] == player_name]

        if player_reg_row:
            if len(player_reg_row) > 1:
                logger.warning(f"WARNING: Multiple rows found for player {player_name}. Using the first one.")
            player_reg_row = player_reg_row[0]
            try:
                division = player_reg_row[cycle_col_name] # e.g., "PRO", "AAA", etc.
                #if you get and err ^ here - make sure the schedule is populated
            except Exception as e:
                print(f"TIP: Looks like the schedule did not get added to db.  Check timings - see ccdg_schedule.py ln 30 \r\n{e}")
                raise e
        else:
            pass # player not found in registration data

        # get new player id from name
        player_id = get_player_id_by_name(db, player_name)
        division_id = next((value for key, value in divisions if key == division), None)
        
        if player_id and division_id:
            # Create a new PlayerDivision instance and add it to the session
            new_division = PlayerDivision(
                player_id = player_id,
                division_id = division_id,
                valid_from_period = min_period, 
                valid_to_period = max_period)
            db.add(new_division)
        else:
            logger.error(f'Error: Player ID or Division ID not found for {player_name} (id={player_id}, div={division_id})')

    db.commit()
    return

## Utility functions ##

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




pass

