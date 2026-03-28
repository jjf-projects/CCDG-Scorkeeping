# python & 3rd party
import os
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func
# custom modules
import sql_db.ccdg_db as ccdg_db
from sql_db.models import Schedule, Score, Division
import google_apis.google_tasks as g
from logger.logger import logger_gen as logger


'''
ccdg_schedule.py

A module for working with schedules via the CCDG Weekly CSV Scoring Utility.
    The whole thing hangs on schedules, so this module is at the heart of the system.
    The schedule is a list of periods - each with a date, course, layout, cycle and 
    **critically**, a link to the UDisc event.  The schedule is read from a Google Sheet
    which only league admins can edit. Example:
    https://docs.google.com/spreadsheets/d/1tv5N3r0F82Oo6zAYBG8i9Xh13mRnbrDXshkTRj73cVE/edit?gid=259080292#gid=259080292
'''


def update_schedule (db: Session, exe_folder: str, config: dict) -> None:

    # settings
    g_creds = os.path.join(exe_folder, config.G_SVC_CREDS_FILE)
    sched = config.G_SCHEDULE
    dt_format = config.DT_FORMAT
    
    # local function
    def update_schedule_table(db: Session, g_creds: str, sched: dict, dt_format: dict) -> None:

        # Read schedule
        schedule_rows = g.read_gsheet_range(g_creds, sched)

        # Clear existing schedule records
        db.query(Schedule).delete()
        db.commit()

        # Loop through new rows and insert into schedule table
        for r in schedule_rows:
            # Format date
            dt_saturday = datetime.strptime(r[1], dt_format['spreadsheet'])
            dt_sunday = datetime.strptime(r[2], dt_format['spreadsheet'])

            # Handle empty URLs if the UDisc event is not set up
            evt_url = r[8] if len(r) > 8 else None

            new_p = Schedule(
                period=int(r[0]),
                saturday=dt_saturday,
                sunday=dt_sunday,
                course=r[3],
                layout=r[4],
                travel=bool(r[5]),
                cycle=int(r[7]),
                event_url=evt_url
            )
            db.add(new_p)
        db.commit()
        logger.info("Schedule updated")

    # check if we need to update the schedule
    update_schedule_table(db, g_creds, sched, dt_format)

    # Note: The following code is commented out because it is too brittle - a smarter check would be to see if we have compete data for weeks to process.
    # schedule_row_count = db.query(func.count(Schedule.period)).scalar()  # always returns an int
    # if schedule_row_count == 0:
    #     update_schedule_table(db, g_creds, sched, dt_format)
    # else:
    #     # compare last update to schedule vs db so we only do this when we need
    #     last_update_db = ccdg_db.get_db_last_update(db)
    #     last_update_schedule = g.get_gdrivefile_metadata(g_creds, sched['file_id'], 'modifiedDate')
    #     last_update_schedule = datetime.strptime(last_update_schedule, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()

    #     if (last_update_schedule > last_update_db):
    #          update_schedule_table(db, g_creds, sched, dt_format)


    return
        
def populate_divisions(db: Session, division_list: list) -> None:
    '''
    Populate the divisions table with the divisions defined in the settings file.

    This is a one-time operation to set up the database with the divisions.
    It should be run only once, or when the divisions change.

    Args
        db (Session): SQLAlchemy session.
        division_list (list): List of divisions that should exsist in the database
                            in the order that they should be presented in the standings

    '''

    # clear existing divisions recs
    db.query(Division).delete()
    db.commit()

    # loop new rows and insert new divisions
    for i in range(len(division_list)):
        d = division_list[i]
        new_d = Division(
            div_name = d,
            display_order = i+1
        )
        db.add(new_d)
    db.commit()
    
def get_unscored_periods(db: Session, date_format_db: str) -> list:
    """
    Finds periods that are complete, but unscored in the database.

    Args:
        session (Session): SQLAlchemy session.

    Returns:
        list: List of schedule periods that need scoring.
    """
    # Find the latest scored period
    max_scored_period = db.execute(select(func.max(Score.period))).scalar()
    if max_scored_period is None:
        max_scored_period = 0  # If no scores exist, assume we start from 0

    # Get a list of all periods that need scoring
    today = datetime.today().date()
    unscored_periods = []

    schedule_periods = db.execute(select(Schedule.period, Schedule.sunday)).all()

    for period, sunday in schedule_periods:
        # Convert sunday (string) to a date object if necessary
        if isinstance(sunday, str):
            sunday = datetime.strptime(sunday, date_format_db).date()
        
        # Check if the Sunday has passed and period is not scored yet
        if sunday < today and period > max_scored_period:
            unscored_periods.append(period)

    return unscored_periods

def get_current_cycle(db: Session):
    """
    Retrieves the most recent cycle from the Schedule table based on today's date.

    Args:
        db_session (Session): Active SQLAlchemy session.

    Returns:
        int or None: The current cycle number, or None if no valid cycle is found.
    """
    today = date.today() - timedelta(days=1)  #-1 because we want to get the last completed cycle and this usually runs on a Monday of the new week

    query = select(Schedule.cycle).where(
        Schedule.sunday <= today  # Find the most recent Sunday before or on today
        ).order_by(Schedule.sunday.desc()).limit(1)  # Get the latest valid period

    result = db.execute(query).scalar_one_or_none()
       
    return result  # Returns the cycle number or None if no valid data exists

def get_min_max_periods_for_cycle(db: Session, cycle: int):
    """
    Retrieves the minimum and maximum periods for a given cycle from the Schedule table.

    Args:
        db_session (Session): Active SQLAlchemy session.
        cycle (int): The cycle number to filter by.

    Returns:
        tuple: (min_period, max_period) or (None, None) if no records found.
    """
    query = select(
        func.min(Schedule.period).label("min_period"),
        func.max(Schedule.period).label("max_period")
    ).where(Schedule.cycle == cycle)

    result = db.execute(query).one_or_none()
    return result if result else (None, None)  # Handle case where no data is found


pass
