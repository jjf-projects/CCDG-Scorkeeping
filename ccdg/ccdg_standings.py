# python and 3rd party modules
from sqlalchemy import select, func
from sqlalchemy.orm import session

# custom modules
from sql_db.models import Score, Schedule, Division
from ccdg import ccdg_scores, ccdg_schedule
import google_apis.google_tasks as g
from logger.logger import logger_gen as logger

'''
ccdg_standings.py

A module for generating public facing standings via the CCDG Weekly CSV Scoring Utility

'''


def generate_standings(db: session, player_registration: list, cfg: dict) -> None:
    '''
    Write season-to-date results to Google Sheets from the DB.  This includes
    realtive_scores, points, wkly avg_pts

    '''

    # how may periods dictates how many cols we will need
    periods_elapsed = db.execute(select(func.max(Score.period))).scalar_one_or_none()
    curr_cycle = ccdg_schedule.get_current_cycle(db) # in case of more than one week to process

    
    ## Scores sheet ##

    # get a list of score table rows for all players and periods so far; sort by most recent week
    score_rows =ccdg_scores.get_player_scores_all_periods(db) 
    score_rows = sorted(score_rows, key=lambda x: x[-1] if x[-1] is not None else float('inf'))

    # remove unpaid players from the published results - ** note there is a new function in ccdg_players.py to get a list of valid players
    #   note: their scores are still kept in the DB - just need to update the sheet from cfg.G_REGISTRATION
    unpaid_players = [p.get("UDisc Full Name") for p in player_registration if p.get("Payable Status") != "paid"]
    for p in unpaid_players:
        logger.warning(f"Unpaid player: {p}")
    score_rows = [row for row in score_rows if row[0] not in unpaid_players]

    # Get the header rows for the scores sheet & add scores below
    header_rows = create_header_rows(db, cfg.LEAD_COLS_SCORES, cfg.DT_FORMAT["spreadsheet"])
    score_sheet_data = header_rows + score_rows
    
    # replace all the data on the scores worksheet with latest results
    gsheet_target = {
        'file_id': cfg.G_STANDINGS['file_id'],
        'sheet_id': cfg.G_STANDINGS['score_sheet']
        }
    g.write_gsheet_range(cfg.G_SVC_CREDS_FILE, gsheet_target, score_sheet_data)


    ## Points sheet ##
    
    points_data = [[p[0], p[1]] for p in score_rows]  # start with player [name, division]

    # iterate over periods and add points to the points_data list
    for period in range(periods_elapsed):  # period starts counting at 0
        period_index = period + 2  #score_rows starts w/ Name, Division, so +2 to get to the first period
        period_scores = [[row[0], row[period_index]] if len(row) > 2 else [row[0], None] for row in score_rows]
        period_points = percentage_plus_Marnie(period_scores, cfg.SCORING)
        for player_row in points_data:
            pts = [pp[1] for pp in period_points if pp[0] == player_row[0]]
            player_row.append((pts[0] if pts else 0))  # None scores become zero pts

    # insert tot pts and pts-after-drop cols
    for row in points_data:
        tot_col_vals = tally_totals(row[2:], curr_cycle, cfg.SCORING["cycle_len"], cfg.SCORING["keep_periods"])
        row.insert(2, tot_col_vals["points_total"])
        row.insert(3, tot_col_vals["points_after_drops"])
    
    # sort by name
    points_rows = sorted(points_data, key=lambda x: x[0])

     # Get the header rows for the scores sheet & add points rows below
    header_rows = create_header_rows(db, cfg.LEAD_COLS_POINTS, cfg.DT_FORMAT["spreadsheet"])
    points_sheet_data = header_rows + points_rows

    # replace all the data on the points worksheet witht the latest results
    gsheet_target = {
        'file_id': cfg.G_STANDINGS['file_id'],
        'sheet_id': cfg.G_STANDINGS['points_sheet']
        }
    g.write_gsheet_range(cfg.G_SVC_CREDS_FILE, gsheet_target, points_sheet_data)

    
    ##  weekly Avg Points ##
    
    period_avg_points_rows = get_period_avg_points(points_rows) # sorted by name with a header
    gsheet_target = {
        'file_id': cfg.G_STANDINGS['file_id'],
        'sheet_id': cfg.G_STANDINGS['weekly_avg_pts']
        }
    g.write_gsheet_range(cfg.G_SVC_CREDS_FILE, gsheet_target, period_avg_points_rows)

    return

###  Utility  ###

def create_header_rows(db: session, lead_cols: list, date_format: str) -> list:
    """
    Creates a three-row header for the Google Standings sheet.
    Example: https://docs.google.com/spreadsheets/d/1TeDuilz8Clf50uT3GXzTeLbqj8tTzGrbwyHHSga9qSE/edit?gid=0#gid=0
    
    Args:
        db: SQLAlchemy session or connection object.
        lead_cols: A list of lead column names for the table header.
    
    Returns:
        A list of lists representing the table header rows.
    """
    # Fetch all distinct periods and related schedule data
    schedule_data = db.execute(
        select(Schedule.period, Schedule.monday, Schedule.course)
        .order_by(Schedule.period)
    ).all()

    # Extract data into three header rows
    lead_cols_epmty = [""] * len(lead_cols)
    period_row = lead_cols_epmty + [str(row.period) for row in schedule_data]
    monday_row = lead_cols_epmty + [row.monday.strftime('%d-%b') if row.monday else "" for row in schedule_data]
    course_row = lead_cols + [row.course or "" for row in schedule_data]

    return [period_row, monday_row, course_row]

def tally_totals(season_points: list, cycle: int, cycle_len: int, keep_periods: int) -> dict:
    ''' 
    Calculate total points and points-after-drops for a single player at a time

    Arguments:
        cycle_points: a list of a player's point values
        cycle_len: periods in an award cycle
        keep_periods: the periods that count toward pts-after-drops
    Returns:
        dict w/ 2 keys:
        'points_total'
        'points_after_drops'
    '''

    # add points from dict into an easy-to-sum list
    
    start_index = (cycle - 1) * cycle_len  # Calculate start index
    end_index = end_index = min(start_index + cycle_len, len(season_points))
    vals_to_tally = season_points[start_index:end_index]  # Extract the relevant scores
     
    # sum total and points-after-drops
    pts_total = sum(p for p in vals_to_tally if p is not None)
    if len(vals_to_tally) > keep_periods:
        vals_to_tally.sort(reverse=True)
        pad_vals_to_tally = vals_to_tally[:keep_periods]
        pts_after_drops = sum(p for p in pad_vals_to_tally if p is not None)
    else:  
        pts_after_drops = pts_total
    
    # ship it
    point_tots = {
        'points_total': round(pts_total,2),
        'points_after_drops': round(pts_after_drops,2)
    }
    return point_tots

def get_period_avg_points(rows_points:list):
    '''
        returns avg points scored for only the periods played ready to write to csv of gsheet
        * pass a list of lists like [[name, div, pts1,pts2,.],[...]]
        * returns a list of lists like [[name, div, avg],[row2],..]

        Note: some indexes are hardcoded - check the cols in r if input cols are diff
    '''
    period_avg_pts_rows = []    
    for r in rows_points:
        plyr = r[0]
        div = r[1]
        non_zero = [pts for pts in r[4:] if pts!= 0]
        avg = sum(non_zero) / len(non_zero) if non_zero else 0
        avg = round(avg, 2)
        period_avg_pts_rows.append([plyr, div, avg])
    avg_pt_rows_sorted = sorted(period_avg_pts_rows, key=lambda x: x[0])
    avg_pt_rows_sorted.insert(0, ['Player', 'Division', 'Weekly Avg Points']) 
    
    return avg_pt_rows_sorted


###  Point systems  ###

def percentage_plus_Marnie(period_scores, scoring_modifiers: dict):
    '''
    calculate points for a single period
    
    Arguments:
        player_list: list of lists with player name and score for that period
        scoring_modifiers: dict with scoring modifiers for the period
    
    Returns:
        list of lists with player name and points for that period
        '''

    points_rows = []

    # sort lists like [name, score] by score ascending
    period_scores = sorted(period_scores, key=lambda x: x[1]if x[1] is not None else float('inf')) # period_scores[1] is in format ['Name', relative_score]
    
    # remove playesr who didn't play that week
    period_scores = [r for r in period_scores if r[1] is not None]

    # calculate points
    index = 0
    total = len(period_scores)

    best_score = int(period_scores[0][1])
    worst_score = int(period_scores[total - 1][1])

    while index < total:
        score = int(period_scores[index][1])
        #see if we have ties
        subindex = index + 1
        numscores = 1
        while subindex < total:
            if score == int(period_scores[subindex][1]):
                numscores += 1
                subindex += 1
            else:
                break
        
        #now iterate over number of scores and calculate subtotal to be divided
        score_count = 0
        points = 0
        temp_index = index
        while score_count < numscores:
            points += ((total - temp_index)/total)*scoring_modifiers['percentage_modifier']
            temp_index += 1
            score_count += 1
        
        #divide points across ties
        actual_points = points/numscores
        actual_points += (scoring_modifiers['score_based_modifier']/(worst_score - best_score))*(worst_score - score)
        actual_points = round(actual_points, 2)

        offset = 0
        while offset < numscores:
            period_scores[index + offset].append(actual_points)
            offset += 1    

        index += numscores - 1
        index+=1
    
    #we don't need the actual scores anymore, so strip em
    [r.pop(1) for r in period_scores]

    return period_scores




















## Deprecated or never used ##

# def get_global_points(db_settings: dict, season: int, season_periods_elapsed: int, lead_col_pts: list, scoring_modifiers: dict):
    
#     points_by_period = []

#     # generate points by period 
#     for period in range(season_periods_elapsed):
#         wk = period + 1
#         period_scores = ccdg_scores.get_scores_for_period(db_settings, season, wk)
#         period_points = percentage_plus_Marnie(period_scores, scoring_modifiers)
#         points_by_period.append(period_points)
#     point_dicts = transpose_for_output(db_settings, points_by_period, season, lead_col_pts, scoring_modifiers)
    
#     # # sort - pts_after
#     points_by_player = sorted(point_dicts, key=lambda x:(-x['Points After Drops Cycle']))

#     return points_by_player

# def get_divisional_points(db_settings: dict, season_periods_elapsed: int):
#     '''
#     Apply %+Marnie to each division to flatten things out.
#     i.e each division has a max of 150 pts where
#         "percentage_modifier": 120,
#         "score_based_modifier": 30,
    
#     Experiment abandoned - 2024 pre-season in lieu of rebalancing - see ccdg_sidehatch
#     '''
#     pass

#     # Unimplemented after major refactor- revist/debug before running

#     # points_by_period = []
    
#     # for period in range(season_periods_elapsed):
#     #     wk = period + 1
#     #     period_scores = get_scores_for_period(db_settings, wk)
        
#     #     # split into divisions
#     #     period_points = []
#     #     for div_label in DIVISIONS:
#     #         div_scores = [r for r in period_scores if r[1] == div_label]
#     #         div_points = percentage_plus_Marnie(div_scores)
#     #         period_points.extend(div_points)
#     #     points_by_period.append(period_points)

#     # point_dicts = transpose_for_csv_writer(db_file, points_by_period)
#     # # sort - pts_after desc
#     # points_by_player = sorted(point_dicts, key=lambda x:(-x['Points After Drops']))

#     # return points_by_player

# def get_period_avg_points(rows_points:list):
#     '''
#         returns avg points scored for only the periods played ready to write to csv of gsheet
#         * pass a list of lists like [[name, div, pts1,pts2,.],[...]]
#         * returns a list of lists like [[name, div, avg],[row2],..]

#         Note: some indexes are hardcoded - check the cols in r if input cols are diff
#     '''
#     period_avg_pts_rows = []    
#     for r in rows_points:
#         plyr = r[0]
#         div = r[1]
#         non_zero = [pts for pts in r[4:] if pts!= 0]
#         avg = sum(non_zero) / len(non_zero) if non_zero else 0
#         avg = round(avg, 2)
#         period_avg_pts_rows.append([plyr, div, avg])
#     avg_pt_rows_sorted = sorted(period_avg_pts_rows, key=lambda x: x[2], reverse=True)
#     avg_pt_rows_sorted.insert(0, ['Player', 'Division', 'Weekly Avg Points']) 
    
#     return avg_pt_rows_sorted

# def split_chunks(src_list: list, chunks: int):
#     # um...whatevs it works: https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length  
#     # make sure to cast the output to list
#     k, m = divmod(len(src_list), chunks) 
#     return (src_list[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(chunks))







