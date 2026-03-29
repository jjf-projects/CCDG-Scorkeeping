import os
import copy
from dotenv import load_dotenv

# Load .env from the repo root (sits next to this file).
# Values in .env override the defaults below.
# .env is gitignored
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

# Absolute path to the repo root — used to build all other paths.
_HERE = os.path.dirname(os.path.abspath(__file__))

'''
json dicts to support different configurations as an easy-to-configure and portable object. 
This takes the place of constants in the code. Types and capitalization matter!  
Explainer:

    Google                      # creds and locations of data in google drive 
        G_SVC_CREDS_FILE        # path to the service credentials.  read more:  https://docs.google.com/document/d/1obuwpJykyDmwbKyDOIOFzF-EHP-qpMCDVK81Q01vock/edit
        G_DATA_FOLDER           # must contain subfolders for inbox, processed, logs. Example: https://drive.google.com/drive/u/0/folders/1R3f1zJ-Cx15d7thgIEtzSTntZKB60flJ
        G_REGISTRATION          # the id of a google sheet with a column particular format - see readme.md
        G_SCHEDULE              # the id of a google sheet with a column particular format - see readme.md
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

# PRODUCTION Settings for the 2026 season
Settings_2026 = {

    'G_SVC_CREDS_FILE': os.environ.get(
        'CCDG_CREDS_FILE',
        os.path.join(_HERE, 'google_apis', 'google_creds_svc_acct.json'),
    ),
    'G_DATA_LOGS': '1GFtj5pVNacVuv_GaaZnujMXFgNRzUpcr',
    'G_REGISTRATION': {
        'file_id': '1K41qy6rIkwUwtuD6McyZnmdSy3qqtMcHQVuRrAl02hw',
        'sheet_id': '1313108348',
        'range': 'A:F'},
    'G_SCHEDULE': {
        'file_id': '1kLxB3cCzuvkYZL3aQFePDQX4DPOSq0mpxPLRw2YOGw0',
        'sheet_id': '259080292',
        'range': 'A2:I'}, 
    'G_STANDINGS': {
        'file_id': '1zRQHjAxyHQzS2zkMAoMjmK3mDJRRbf_qGGDXIUjKWpc',
        'score_sheet': 0,
        'points_sheet': 2139620093,
        'weekly_avg_pts': 24541006},

    'SEASON': 2026,           
    'DIVISIONS': ["Alpha", "Bravo", "Charlie", "Delta", "Echo"],
    'LEAD_COLS_SCORES' : ['Name', "Division"],
    'LEAD_COLS_POINTS' : ['Name', 'Division', 'Total Points Cycle', 'Points After Drops Cycle'],
    'SCORING':{
        "percentage_modifier": 120,
        "score_based_modifier": 30,
        "cycle_len": 12,
        "keep_periods": 6}, 
    
    'DATABASE': {
        'DB_DIR': os.path.join(_HERE, 'sql_db'),
        'DB_NAME': '2026.db',
        'ECHO': False},

    'DT_FORMAT': {
        'database': '%Y-%m-%d',
        'spreadsheet': '%d-%b-%Y'},

    # Gemini model used for weekly social media summaries.
    # gemini-2.0-flash-lite is the lightest available model — best choice for
    # the free tier.  Run regenerate_summary(dry_run=True) to check token usage.
    # See: https://ai.google.dev/gemini-api/docs/models
    'GEMINI_MODEL': 'gemini-3-flash-preview',

    # How many players per division to include in the Gemini prompt.
    # Keeping this small reduces token usage on the free tier.
    # 3 gives Gemini enough context (winner, runner-up, 3rd) for a good summary.
    'GEMINI_SUMMARY_TOP_N': 10,
}

# DEVELOPMENT Settings for the 2026 season in development mode
Settings_2026_dev = copy.deepcopy(Settings_2026)
Settings_2026_dev['DATABASE'] = {
        'DB_DIR': os.path.join(_HERE, 'sql_db'),
        'DB_NAME': '2026_dev.db',
        'ECHO': True}
Settings_2026_dev['G_STANDINGS'] = {
        'file_id': '1D3JFjvyokhD__0jvvFb9EdWaXkL5GD53BXH62Wd9QTg',
        'score_sheet': 0,
        'points_sheet': 2139620093,
        'weekly_avg_pts': 24541006
    }



### Configuration class to hold settings as an object
# Wraps a settings dict and exposes each key as an attribute.
# All required keys are declared explicitly so a typo or missing entry raises
# a clear AttributeError at startup rather than failing silently at runtime.
class Configuration:
    # Required top-level keys — startup fails with a clear error if any are absent.
    _REQUIRED = {
        'G_SVC_CREDS_FILE', 'G_DATA_LOGS', 'G_REGISTRATION', 'G_SCHEDULE',
        'G_STANDINGS', 'SEASON', 'DIVISIONS', 'LEAD_COLS_SCORES',
        'LEAD_COLS_POINTS', 'SCORING', 'DATABASE', 'DT_FORMAT',
        'GEMINI_MODEL', 'GEMINI_SUMMARY_TOP_N',
    }

    def __init__(self, settings_dict: dict):
        missing = self._REQUIRED - settings_dict.keys()
        if missing:
            raise ValueError(f"Settings dict is missing required keys: {missing}")
        self.__dict__.update(settings_dict)

    def __getattr__(self, attr):
        raise AttributeError(f"Configuration has no attribute '{attr}' — check ccdg_settings.py")

if __name__ == "__main__":
    pass