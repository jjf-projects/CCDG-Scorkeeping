import os

'''
json dicts to support different configurations as an easy-to-configure and portable object. 
This takes the place of constants in the code.
Types and capitalization matter!  Explainer:

    Google                      # creds and locations of data in google drive 
        G_SVC_CREDS_FILE        # path to the service credentials.  read more:  https://docs.google.com/document/d/1obuwpJykyDmwbKyDOIOFzF-EHP-qpMCDVK81Q01vock/edit
        G_DATA_FOLDER           # must contain subfolders for inbox, processed, logs. Example: https://drive.google.com/drive/u/0/folders/1R3f1zJ-Cx15d7thgIEtzSTntZKB60flJ
        G_REGISTRATION    # the id of a google sheet with a column particular format - see readme.md
        G_SCHEDULE        # the id of a google sheet with a column particular format - see readme.md
            .file_id            # str 
            .sheet_id           # int
            .range              # string - standard spreadsheet notation eg. "A1:B2"
        G_STANDINGS_SHEET       # the id of a google sheet where we write results and standings,  is shared-read-only the the world
            .file_id            # str 
            score_sheet         # int
            points_sheet        # int

    Leauge details              # detail of the current contest
        SEASON                  # used to uniquie id score_preiod in db examples: 21st week of weekly 2024 leauge, week three of Kings. etc.
        DIVISIONS               # list of divsions eg. ['PRO', 'AAA', ...]
        LEAD_COLS_SCORES        # list of column names at left of the scores sheet in the leauge standings results
        LEAD_COLS_POINTS        # same, but for Points
        SCORING                 # a dict for scoring settings/constants
            percentage_modifier     # points to allocate to % component of total points each period
            score_based_modifier    # same for score_based
            keep_periods            # determines points after drops

    Database
        DATABASE                # a dict to hold DB settings
            DB_DIR                  # must contain the sqlite.db file and a .json describng tables & cols
            DB_NAME                 # enables having different versions for testing - note sqlite will create a file like 'DB_NAME' + '.db'

    Other constants
        DT_FORMAT               # enables diff formats for different apps
            .database:          # '%Y-%m-%d' e.g. 2024-01-31
            .spreadsheet:       # '%d-%b-%Y' e.g. 01-Jan-2024

'''

# devinbox - 1otLA2dVEHQmO4c6Hx2gTMik_u5owmjhZ
# devInbox2 - 1re5LZQSHOBog9orNzWe59S5VlIYj2k5u

Settings_2025 = {

    'G_SVC_CREDS_FILE': '.\\google_apis\\google_creds_svc_acct.json',
    'G_DATA_LOGS': '1GFtj5pVNacVuv_GaaZnujMXFgNRzUpcr',
    'G_REGISTRATION': {
        'file_id': '1tYkb03aoEYGeYqpKHQN-UGtEx39-YY44oKPIJaexcCs',
        'sheet_id': '2104782840',
        'range': 'A:K'},
    'G_SCHEDULE': {
        'file_id': '10dcoUkMyd9GwKTZb2RrW6qX1EgRGgN5lDY-NabftAlg',
        'sheet_id': '259080292',
        'range': 'A2:H'}, 
    'G_STANDINGS': {
        'file_id': '1TeDuilz8Clf50uT3GXzTeLbqj8tTzGrbwyHHSga9qSE',
        'score_sheet': 0,
        'points_sheet': 2139620093,
        'weekly_avg_pts': 24541006},

    'SEASON': 2025,           
    'DIVISIONS': ['PRO', 'AAA','BBB', 'CCC', 'DDD', 'EEE'], 
    'LEAD_COLS_SCORES' : ['Name', "Division"],
    'LEAD_COLS_POINTS' : ['Name', 'Division', 'Total Points Cycle', 'Points After Drops Cycle'],
    'SCORING':{
        "percentage_modifier": 120,
        "score_based_modifier": 30,
        "cycle_len": 12,
        "keep_periods": 8}, 
    
    'DATABASE': {
        'DB_DIR': '.\\sql_db',          
        'DB_NAME': '2025.db',
        'ECHO': False},

    'DT_FORMAT': {
        'database': '%Y-%m-%d',
        'spreadsheet': '%d-%b-%Y'}
}



Settings_2025_dev = {

    'G_SVC_CREDS_FILE': '.\\google_apis\\google_creds_svc_acct.json',
    'G_DATA_LOGS': '1PdwBFcbzBJs5-cHoQKRnuV2xe4Pm_wEP',
    'G_REGISTRATION': {
        'file_id': '1tYkb03aoEYGeYqpKHQN-UGtEx39-YY44oKPIJaexcCs',
        'sheet_id': '2104782840',
        'range': 'A:K'},
    'G_SCHEDULE': {
        'file_id': '1tv5N3r0F82Oo6zAYBG8i9Xh13mRnbrDXshkTRj73cVE',
        'sheet_id': '259080292',
        'range': 'A2:H'}, 
    'G_STANDINGS': {
        'file_id': '1D3JFjvyokhD__0jvvFb9EdWaXkL5GD53BXH62Wd9QTg',
        'score_sheet': 0,
        'points_sheet': 2139620093,
        'weekly_avg_pts': 24541006},

    'SEASON': 2025,           
    'DIVISIONS': ['PRO', 'AAA','BBB', 'CCC', 'DDD', 'EEE'], 
    'LEAD_COLS_SCORES' : ['Name', "Division"],
    'LEAD_COLS_POINTS' : ['Name', 'Division', 'Total Points Cycle', 'Points After Drops Cycle'],
    'SCORING':{
        "percentage_modifier": 120,
        "score_based_modifier": 30,
        "cycle_len": 12,
        "keep_periods": 8}, 
    
    'DATABASE': {
        'DB_DIR': '.\\sql_db',          
        'DB_NAME': '2025_dev.db',
        'ECHO': True},

    'DT_FORMAT': {
        'database': '%Y-%m-%d',
        'spreadsheet': '%d-%b-%Y'}
}


class Configuration:
    # must be initialized with a a dict of values - see above
    def __init__(self, settings_dict):
        self.__dict__.update(settings_dict)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            raise AttributeError(f"'Settings' object has no attribute '{attr}'")

if __name__ == "__main__":
    pass