# python & 3rd party
from sqlalchemy import select, func, or_, delete
from sqlalchemy.orm import Session, aliased
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse, urlunparse
# custom libs
from sql_db.models import Schedule, Score, HoleScore, Player, PlayerDivision, Division
from ccdg import ccdg_players
from logger.logger import logger_gen as logger


'''
ccdg_scores.py

A module with utility functions for working with scores in the CCDG Weekly CSV Scoring Utility.  Contains functions for both 
    * import from csv or xls and 
    * db reads for generating standings
'''

### IMPORT ###

import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def load_local_xlsx_as_dicts(file_path: str):
    """
    Loads an Excel (.xlsx) file from a local file path and returns data as a list of dictionaries.

    Args:
        file_path (str): The path to the .xlsx file on the local drive.

    Returns:
        list: A list of dictionaries representing the spreadsheet data.
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Read the Excel file into a Pandas DataFrame
        df = pd.read_excel(file_path)

        # Convert DataFrame to a list of dictionaries
        return df.to_dict(orient="records")

    except Exception as e:
        msg = f"Error reading file from path: {file_path} — {str(e)}"
        print(msg)
        logger.error(msg)
        return []

def fetch_web_xlsx_as_dicts(url: str):
    """
    Fetches an Excel (.xlsx) file from a given URL and returns data as a list of dictionaries.

    Args:
        url (str): The URL of the .xlsx file.

    Returns:
        list: A list of dictionaries representing the spreadsheet data.
    """
    try:
        # Download the file
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for failed requests

        # Read the Excel file into a Pandas DataFrame
        xls_data = BytesIO(response.content)  # Convert response content to file-like object
        df = pd.read_excel(xls_data)  # Read into DataFrame

        # Convert DataFrame to a list of dictionaries
        return df.to_dict(orient="records")  

    except Exception as e:
        msg = f"Error fetching file from URL: {url}"
        print(msg)
        logger.error(msg)
        return []

def get_udsic_scores(db: Session, period: int) -> dict:
    '''
    Fetches UDisc scores for a given period from the database and returns them as a dictionary.
    
    Args:
        db (Session): Active SQLAlchemy Session.
        period (int): The period number to fetch scores for.
    Returns:
        dict: A dictionary containing the period and a list of leaderboard rows.
        Each row is a dictionary with player scores and details.
    Raises:
        Exception: If the period is not found in the schedule or if there is an error fetching the scores.
    '''

    # get schedule details for the period
    sched_period_data = db.execute(select(Schedule).where(Schedule.period == period)).scalar()

    # get the score data    
        # - if url is null try local_file, use that instead of fetching from the web 
        # - see: https://docs.google.com/document/d/1M1CVz6wZ5GF-qIN2Q_5neGg64oaPHecThHo-7JwlVtg/edit?tab=t.0
        # be sure to set loca_file to None if you want to fetch from the web after a player's choice week
    url = sched_period_data.event_url
    if url:
        # parse defalt event url to get an export - see https://forum.udisc.com/t/request-public-api/375/28
        parsed_url = urlparse(url) 
        if not parsed_url:
            logger.error(f"Invalid URL found for wk {period}: {url}")
            logger.error(f"Check the sheet and/or Schedule table in the database.")
            return {}
        new_path = parsed_url.path.rstrip('/') + "/export"  # Ensure path ends with "/export"
        url_export = urlunparse((parsed_url.scheme, parsed_url.netloc, new_path, '', '', ''))
        event_results = fetch_web_xlsx_as_dicts(url_export)
    else:
        local_file = None #r"D:\Code\CCDG\CCDG-Scorkeeping2025\temp\chester-county-disc-golf-ccdg-weekly-league-tyler-east-playerChoice-2025-10-13.xlsx"
        event_results = load_local_xlsx_as_dicts(local_file)
    
    # return a dict w/ wkNo and score data    
    if event_results:
        return {
            'period': period,
            'leaderboard_rows': event_results
        }
    else:
        logger.error(f"Error fetching scores for period {period} from URL: {url}")
        return {}

def download_leaderboard(url, filename):
    """
    Downloads a file from the given URL and saves it with the specified filename.

    Args:
        url (str): The URL of the file to download.
        filename (str): The name to save the downloaded file as.

    Returns:
        bool: True if the file was downloaded successfully, False otherwise.
    """

    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully as {filename}")
        return True
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        return False

def clean_score_data(score_rows: list) -> dict:
    '''
    Takes a list of score rows as dicts [{div: AAA, name: Ed, ...}, {..}...]
    * removes trailing spaces from name
    * drops WITN, DUP, DNF rounds
    '''
    for row in score_rows:

        #trim spaces off of names for jagoffs who no type good on phones and shit
        row['name'] = ccdg_players.clean_player_name(row['name'])

        try:
            score_rows = [r for r in score_rows if r['entry_number'] == 1]    # only accept first round
            # udisc exports for events where everyone played only one round will omit this col
            # https://forum.udisc.com/t/changes-to-export/468387
            # note: Position can be DUP, but it is not a deterministic indicator of entry_number
        except:
            pass
        score_rows = [r for r in score_rows if r['position'] != 'DNF']         # did not finish
        score_rows = [r for r in score_rows if r['division'].upper() != 'WITN']  # this should not happen

    return score_rows

def add_scores(db: Session, period: int, score_rows: list) -> None:
    '''
    '''
    print(f"Adding scores for period {period}...")
    for row in score_rows:
        # get player_id from db
        player_id = ccdg_players.get_player_id_by_name(db, row['name'])
        if not player_id:
            msg = f"Unregistered Player: {row['name']}"
            logger.warning(msg)
            continue

        # check if score already exists for this period
        if check_existing_score(db, player_id, period):
            continue

        # add score entry and hole scores
        add_score_entry(db, player_id, period, row)
        db.commit()
    return

def add_score_entry(db, player_id, period, score_data):
    """
    Adds a new score entry and associated hole scores to the database.

    Args:
        db_Session: The database Session to execute queries.
        player: The player object containing player_id.
        period: The period number for the score entry.
        score_data: A dictionary containing score details, including event_total_score, 
                    event_relative_score, and hole scores.

    Returns:
        None
    """
    # Create and add new score entry
    new_score = Score(
        player_id = player_id,
        period=period,
        total_score=score_data['event_total_score'],  # Matches event_total_score
        relative_score=score_data['event_relative_score'],  # Matches event_relative_score
        round_rating=score_data.get('round_rating', None)  # Optional - placeholder for future
    )
    db.add(new_score)
    db.flush()  # Ensure new_score.score_id is available for hole scores

    # Add hole scores
    # get list of holes from score_data
    col_names = list(score_data.keys())
    hole_numbers = [h for h in col_names if h.startswith('hole_')]

    for hole_num in hole_numbers: 
        if hole_num in score_data:
            hole_score = HoleScore(
                score_id=new_score.score_id,
                hole_number=hole_num,
                hole_score=score_data[hole_num]
            )
            db.add(hole_score)

    # Commit all changes
    db.commit()
    return
    
def check_existing_score(db_Session, player_id, period):
    """
    Checks if a score entry already exists for a given player, period, and season.

    Args:
        db_Session: The database Session to execute queries.
        player_id: The player table value for player_id.
        period: The period number to check.
        season: The season number to check.

    Returns:
        bool: True if the score entry exists, False otherwise.
    """
    existing_score = db_Session.execute(
        select(Score).where(
            (Score.player_id == player_id) &
            (Score.period == period)
        )
    ).scalar_one_or_none()

    if existing_score:
        msg = f"Score already exists for player_id {player_id} in period {period}."
        print(msg)
        logger.warning(msg)
        return True
    return False


# ### READ DB ###

def get_player_scores_all_periods_by_player_id(db: Session, player_ids: list) -> list:
    """
    Fetches all player scores for the given season, pivoting periods into columns.

    Args:
        db_Session (Session): Active SQLAlchemy Session.
        player_ids (int): a list of ids to include in the output

    Returns:
        list where each row is a list like [name, division, score1, score2, ..]
    """
       
    # Get all distinct periods scored in the season - from the db
    periods = db.execute(
        select(Score.period).distinct().order_by(Score.period)
    ).scalars().all()

    # Dynamically build case statements to fetch relative_score for each period
    period_cases = [
        func.coalesce(
            select(Score.relative_score)
            .where((Score.player_id == Player.player_id) & (Score.period == period))
            .correlate(Player)
            .scalar_subquery(),
            None
        ).label(f"wk{period}")
        for period in periods
    ]

    # Final query including player_id, name, and all period scores
    query = (
        select(Player.player_id, Player.full_name, *period_cases)
        .distinct()
        .join(Score, isouter=True)
    )
    player_scores_all_periods = db.execute(query).all() # returns [player_id, player_name, wk1_score, wk2_score,...]

    # assemble final rows adding division for each player
    score_rows = []
    for player_id, full_name, *scores in player_scores_all_periods:
        division = None
        if player_id in player_ids:
            score_rows.append([player_id, full_name, division, *scores])

    return score_rows # list

def get_player_scores_all_periods(db: Session) -> list:
    """
    Fetches all player scores for the given season, pivoting periods into columns.
    Note: 
     - These scores are used to assign points for the standings, the data come from the relative_score column in the Score table, 
     - which comes from'event_relative_score' in the UDisc export  See the new_score varuable in add_score_entry() above for full details.

    Args:
        db_Session (Session): Active SQLAlchemy Session.
        season (int): The season to retrieve scores for.

    Returns:
        list where each row is a list like [name, division, score1, score2, ..]
    """
       
    # Get all distinct periods scored in the season - from the db
    periods = db.execute(
        select(Score.period).distinct().order_by(Score.period)
    ).scalars().all()

    # Dynamically build case statements to fetch relative_score for each period
    period_cases = [
        func.coalesce(
            select(Score.relative_score)
            .where((Score.player_id == Player.player_id) & (Score.period == period))
            .correlate(Player)
            .scalar_subquery(),
            None
        ).label(f"wk{period}")
        for period in periods
    ]

    # Final query including player_id, name, and all period scores
    query = (
        select(Player.player_id, Player.full_name, *period_cases)
        .distinct()
        .join(Score, isouter=True)
    )
    player_scores_all_periods = db.execute(query).all() # returns [player_id, player_name, wk1_score, wk2_score,...]

    # assemble final rows adding division for each player
    score_rows = []
    for player_id, full_name, *scores in player_scores_all_periods:
        division = get_player_current_division(db, player_id) or "Unknown"  # Default if no division found
        score_rows.append([full_name, division, *scores])

    #note: filtering out scores for non-registered players is done in ccdg_standings.generate_standings()

    return score_rows # list

def get_player_current_division(db_Session, player_id):
    # Get the most recent period
    max_period_subquery = select(func.max(Score.period)).scalar_subquery()

    # Alias for cleaner joins
    PD = aliased(PlayerDivision)

    # Query to get the current division for the player
    query = (
        select(Division.div_name)
        .join(PD, Division.division_id == PD.division_id)
        .where(
            PD.player_id == player_id,
            PD.valid_from_period <= max_period_subquery,  # Valid from a past period
            (PD.valid_to_period.is_(None) | (PD.valid_to_period >= max_period_subquery))  # Still valid
        )
        .order_by(PD.valid_from_period.desc())  # Get the latest division
        .limit(1)
    )

    result = db_Session.execute(query).scalar()
    return result  # Returns the division name or None if not found

def get_player_division_for_one_period(db_Session, player_id, target_period):
    """
    Get the division for a player for a specific period.
    
    Args:
        db_Session (Session): Active SQLAlchemy Session.
        player_id (int): The ID of the player.
        period (int): The period to check.

    Returns:
        str: The division name or None if not found.
    """
    query = (
        select(Division.div_name)
        .join(PlayerDivision, Division.division_id == PlayerDivision.division_id)
        .where(
            PlayerDivision.player_id == player_id,
            PlayerDivision.valid_from_period <= target_period,
            or_(
                PlayerDivision.valid_to_period == None,
                PlayerDivision.valid_to_period >= target_period
            )
        )
        .order_by(PlayerDivision.valid_from_period.desc())
        .limit(1)
    )

    result = db_Session.execute(query).scalar_one_or_none()
    return result  # Returns the division name or None if not found

def avg_non_zero_vals(vals: list) -> float:
    """
    Calculate the average of non-zero values from a list.

    Args:
        scores (list): A list of values.

    Returns:
        float: The average of non-zero values, rounded to 3 decimal places.
    """
    non_zero = [pts for pts in vals if pts!= 0]
    avg = sum(non_zero) / len(non_zero) if non_zero else 0
    avg = round(avg, 3)
    return avg


### OTHER FUNCTIONS ###
def delete_scores_for_period(db: Session, period: int):
    """
    Deletes all scores and their associated hole scores for a given period.
    
    Args:
        Session (Session): SQLAlchemy Session object.
        period (int): The period for which scores should be deleted.
    
    SQL Commands:
        -- Step 1: Delete from hole_score where the score is in period 11
        DELETE FROM hole_score
        WHERE score_id IN (
            SELECT score_id FROM score WHERE period = 11
        );

        -- Step 2: Delete from score for period 11
        DELETE FROM score
        WHERE period = 11;

    """
    from sql_db.models import Score, HoleScore  # Import models here to avoid circular imports

    # Step 1: Find all Score IDs for the specified period
    score_ids = db.scalars(
        select(Score.score_id).where(Score.period == period)
    ).all()

    # Step 2: Delete HoleScores linked to those Score IDs
    if score_ids:
        db.execute(
            delete(HoleScore).where(HoleScore.score_id.in_(score_ids))
        )

        # Step 3: Delete the Scores themselves
        db.execute(
            delete(Score).where(Score.score_id.in_(score_ids))
        )

        # Commit the changes
        db.commit()
        logger.warning(f"Deleted scores and hole scores for period {period}.")
pass
