# python & 3rd party modules
import time, os, traceback

# custom modules
import ccdg_settings as config
from sql_db import database
from ccdg import ccdg_schedule, ccdg_scores, ccdg_players, ccdg_standings
import google_apis.google_tasks as g
import logger.logger as logger

'''
ccdg__main_app.py

'''

# load settings as collection of constants - see ccdg_settings.py
# CONFIG = config.Configuration(config.Settings_2025_dev)
CONFIG = config.Configuration(config.Settings_2025_dev)  # uncomment for Dev/Testing

 
def main():
    '''
    Main function to run the CCDG Weekly CSV Scoring Utility.
    This function orchestrates the loading of data, processing of scores,
    and updating the database.
    It is designed to be automated.
    '''

    # use the same db session throughout - see sql_db/database.py
    db = database.init_db(CONFIG.DATABASE)
    
    ####  L O A D   D B  ####

    # Insert schedule updates into the database
    ccdg_schedule.update_schedule(CONFIG, db) 

    # load latest registration data from google sheets
    player_registration = g.read_gsheet_range(CONFIG.G_SVC_CREDS_FILE, CONFIG.G_REGISTRATION)
    player_registration = g.list_to_dict(player_registration)
    
    # clean player names
    for player in player_registration:
        player['UDisc Full Name'] = ccdg_players.clean_player_name(player['UDisc Full Name'])

    # add players to db - note this will add any new player who registered since last run
    new_names = ccdg_players.add_new_players(db, player_registration)

    # create player division associations for new players from registration data
    current_cycle = ccdg_schedule.get_current_cycle(db)
    ccdg_players.associate_divisions(db, player_registration, new_names, current_cycle)

    # determine which periods need processing
    periods_to_score = ccdg_schedule.get_unscored_periods(db, CONFIG.DT_FORMAT['database'])

    # for each period, add players, associate divisions, clean scores, and add to db
    for period in periods_to_score:

        # get uDisc exports for that period's data
        period_data = ccdg_scores.get_udsic_scores(db, period)

        # clean up score rows
        clean_scores = ccdg_scores.clean_score_data(period_data['leaderboard_rows'])

        # add scores to db
        ccdg_scores.add_scores(db, period, clean_scores)
        
        pass

    # having processed all the scores, we are done inserting to the db. Copy sqlite file to gdrive for posterity
    db_path = os.path.join(CONFIG.DATABASE['DB_DIR'], CONFIG.DATABASE['DB_NAME'])
    g.add_file_to_gdrive(CONFIG.G_SVC_CREDS_FILE, db_path, CONFIG.G_DATA_LOGS)

    # generate standindgs
    ccdg_standings.generate_standings(db, player_registration, CONFIG)

    # copy logs to GDrive too
    

    return None





###  M A I N  ###
if __name__ == "__main__":

    start_time = time.time()
    fname = os.path.basename(__file__)
    start_msg = f'### START ###\r\n:{fname} started'
    if hasattr(CONFIG, 'CONFIG_NAME'):
        start_msg += f' with settings: {CONFIG.CONFIG_NAME}'
    logger.info(start_msg)

    try:
        main()
        elapsed_time = time.time() - start_time
        msg = f"\r\n### END ###--- {fname} completed sucessfully in {elapsed_time:.3f} seconds ---\n"
    except Exception as e:
        stack_trace_str = traceback.format_exc()
        msg = f' ---- EXECUTION ERROR  -----\n{e}\n{stack_trace_str}'
    finally:
        logger.info(msg)
        print(msg)
        
