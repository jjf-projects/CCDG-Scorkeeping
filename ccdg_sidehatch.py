import csv, time, os
import math
from statistics import mean
import traceback

from sqlalchemy.orm import Session
from sqlalchemy import select, func

# custom modules
from sql_db.models import Score, Player, HoleScore

from sql_db import ccdg_db
import ccdg_settings as config
from ccdg import ccdg_scores, ccdg_standings, ccdg_schedule, ccdg_players
import google_apis.google_tasks as g
from logger.logger import logger_gen as logger  

'''
ccdg_sidehatch.py contains smaller scripts useful for semi-regular one-off tasks.

IMPORTANT: Modify main() to call only the functions you need.  Here are the major ones:
    * rebalance() - generates results for a cycle and assigns new divs. Export (do not update database)


NB: The name ccdg_sidehatch was inspired by the classic Tenacious D act that you
really ought to watch, right now: https://www.youtube.com/watch?v=212XFe-ICeM

So, Put on a cool 70's groove..
'''

###  P R I M A R Y  F U N C T I O N S  ###

def process_cycle_results_and_rebalance(config: object, db: Session, cycle_to_process: int, exe_dir: str) -> None:
        # get the first & last weeks in the cycle
        first_last_wks = ccdg_schedule.get_min_max_periods_for_cycle(db, cycle_to_process)
        target_cycle_periods = list(range(first_last_wks[0], first_last_wks[1] + 1))  # inclusive of last week

        # get a list of all periods for which there are scores in the DB
        all_periods_elapsed = db.execute(select(func.max(Score.period))).scalar_one_or_none()
    
        # get all paid player ids
        player_ids = ccdg_players.get_valid_player_ids(db, config.G_SVC_CREDS_FILE, config.G_REGISTRATION)
    
        # get all the scores we have so far this season for those players & assign their div at the end of CYCLE_TO_PROCESS
        #  score_rows is a list like [[player_id, name, division, wk1_score, wk_2score, ...'],...] where division always None (for now)
        score_rows = ccdg_scores.get_player_scores_all_periods_by_player_id(db, player_ids)

        # get points for all those players in all cycles
        points_data = [[p[0], p[1], p[2]] for p in score_rows]  # start with player [p_id, name, division]
        #  iterate over periods and add points to the points_data list
        for period in range(all_periods_elapsed):  # period starts counting at 0
            period_index = period + 3  #score_rows starts w/ id, Name, Division, so +2 to get to the first period col
            period_scores = [[row[0], row[period_index]] if len(row) > 2 else [row[0], None] for row in score_rows]
            period_points = ccdg_standings.percentage_plus_Marnie(period_scores, config.SCORING)
            for player_row in points_data:
                pts = [pp[1] for pp in period_points if pp[0] == player_row[0]]
                player_row.append((pts[0] if pts else 0))  # None scores become zero pts

        # gather all the data we need for rebalancing and target cycle results in separate lists indexed by player_id
        wkly_avg_pts_by_player_id = []
        cycle_pts_by_player_id = []
        old_cycle_divs = []
        for pts_row in points_data:
        
            # average points from start of season through the end of the target cycle
            pts_to_avg = pts_row[3: first_last_wks[1] + 3] 
            weeky_avg_pts = ccdg_scores.avg_non_zero_vals(pts_to_avg)
            wkly_avg_pts_by_player_id.append([pts_row[0], weeky_avg_pts])
        
            # tally tot and pts after drops for the target cycle
            pts_to_tally = pts_row[2+ first_last_wks[0]: first_last_wks[1] + 3]
            tot_col_vals = tally_cycle_points(pts_to_tally, config.SCORING["cycle_len"], config.SCORING["keep_periods"])
            cycle_pts_by_player_id.append([pts_row[0], tot_col_vals["points_total"], tot_col_vals["points_after_drops"]])
        
            # get the division for the player at the end of cycle_to_process
            div = ccdg_players.get_player_division_for_period(db, pts_row[0], first_last_wks[1])
            old_cycle_divs.append([pts_row[0], div if div else "Unknown"])

        # get new divs assignments indexed by player_id
        #  filter out players that won't be rebalanced (PROs, no points, etc) - they will keep their divs
        non_pro_player_ids = [r[0] for r in old_cycle_divs if r[1] != "PRO"]  # filter out pro players
        reblance_ids = [r[0] for r in wkly_avg_pts_by_player_id if r[1] >0 and r[0] in non_pro_player_ids]  # filter out players with no points
        reblance_rows = [r for r in wkly_avg_pts_by_player_id if r[0] in reblance_ids]  # filter wkly avg pts rows to only those that will be rebalanced
        reblance_rows = sorted(reblance_rows, key=lambda r:(-r[1]))  # sort by weekly average points dec
    
        # Assign new divisions
        new_cycle_divs = []
        tot_div_count = len(config.DIVISIONS)-1  # minus one for pro
        div_chunks_players = list(split_list_into_chunks(reblance_rows, tot_div_count))
        # am players with socres
        for i in range(len(div_chunks_players)):
            chunk = div_chunks_players[i]
            new_div_label = chr(i + 65) * 3
            for p_id in chunk:
                new_cycle_divs.append([p_id[0], new_div_label])
        #  pro & am with no scores (no score means no change in div)
        for p in old_cycle_divs:
            if p[0] not in [x[0] for x in new_cycle_divs]:  # if player is not in the reblance list, keep their div
                new_cycle_divs.append([p[0], p[1]]) 
            if p[1] not in config.DIVISIONS:  
                # if player's division is undefined in the cycle that is ending,
                # it needs to be fixed in the DB - check reg form and see ccdg_players.update_player_division()
                logger.error(f'Player {p[0]} has an undefined division {p[1]} in cycle {cycle_to_process}. Please fix in the registration form.')
            

        # build cycle results data rows for output
        cycle_results_output_rows = []
        for p in points_data:
            name = p[1]
            tot_pts = [r[1] for r in cycle_pts_by_player_id if r[0] == p[0]]
            tot_pts_after_drops = [r[2] for r in cycle_pts_by_player_id if r[0] == p[0]]
            weeky_avg_pts = [r[1] for r in wkly_avg_pts_by_player_id if r[0] == p[0]]
            old_div = [r[1] for r in old_cycle_divs if r[0] == p[0]]
            new_div = [r[1] for r in new_cycle_divs if r[0] == p[0]]
            cycle_results_output_rows.append([
                name,                                                       # [0] player name,
                tot_pts[0] if tot_pts else 0,                               # [1] total points for the cycle
                tot_pts_after_drops[0] if tot_pts_after_drops else 0,       # [2] points after drops for the cycle
                weeky_avg_pts[0] if weeky_avg_pts else 0,                   # [3] weekly average points
                old_div[0] if old_div else "Unknown",                       # [4] ending cycle division
                new_div[0] if new_div else "Unknown"                        # [5] new division - to be assigned later
            ])
        # sort results by new division and then pts after drops
        sorted_rows = sorted(cycle_results_output_rows, key=lambda row: (row[4], -row[2]))
        # put pro at the top
        pro_rows = [r for r in sorted_rows if r[4] == "PRO"]
        sorted_rows = pro_rows + [r for r in sorted_rows if r[4] != "PRO"]  # put PROs at the top

        # add header rows
        old_div_header = f'C{cycle_to_process} Division'
        new_div_header = f'C{cycle_to_process+1} Division'
        header_row = [
            "Name",
            f'Total Points C{cycle_to_process}',
            f'Points After Drops C{cycle_to_process}', 
            f'Weekly Avg Points C{cycle_to_process}',
            old_div_header,
            new_div_header
        ]

        # prep the output data
        points_sheet_data = [header_row] + sorted_rows
        
        # prep new cycle divs data
        divisions = ccdg_players.get_division_defs(db)  # returns tuples of (div_name: str, div_id: int, display_order: int)
        new_cycle_divs_by_div_id = []
        for r in new_cycle_divs:
            player_id = r[0]
            div_name = r[1]
            div_id = next((d[1] for d in divisions if d[0] == div_name), None)
            if div_id is not None:
                new_cycle_divs_by_div_id.append([player_id, div_id])

        # ship it all out
        return points_sheet_data, new_cycle_divs_by_div_id

def payouts(results_file) -> list:
    '''
        Export a csv of the winners by division and cycle.
        2024 Awards sheet: https://docs.google.com/spreadsheets/d/1fKC4LYePnw59rJeYU72Z_LNQfFF8BTAyZZ4Q53spd2U/edit?gid=0#gid=0
        Expected INPUT file format: https://docs.google.com/spreadsheets/d/1KFeoKhSd8Z-k_gD2y_9Rd9fWsJx7UvUXCgXE92qrgm0/edit?gid=0#gid=0
    '''
    # setup
    DIVS = CONFIG.DIVISIONS
    CYCLES = 3
    PURSE_PER_PLAYER = 25      # portion of entry fee for entire season in $USD
    CYCLE_DIV_RESULTS =  results_file                   
    RATIO_WINNERS = .3
    EXP = -.6

    # read in results - 5-col csv:
    #  - Cycle,
    #  - Name,
    #  - Cycle Div,
    #  - Total Points,
    #  - Points After Drops
    results = read_csv_as_dict(CYCLE_DIV_RESULTS)
    results_with_pay = []

    # for each div/cycle combo
    for c in range(CYCLES):
        for d in DIVS:
            
            # filter for cycle & division
            cd_results = [
                r for r in results
                if int(r['Cycle']) == c+1 and r['Cycle Div'] == d
            ]
            # print(f'p:C{c+1}:{d}:{len(cd_results)}')
            
            # compute purses, winners, payouts
            cd_purse = len(cd_results) * PURSE_PER_PLAYER / CYCLES
            count_winners = math.floor(len(cd_results) * RATIO_WINNERS)
            # print(f'w:C{c+1}:{d}:{count_winners}')

            pcts_pay = [None] * count_winners
            for i in range(count_winners):
                # see explainer to understand this math
                pcts_pay[i] = (count_winners * (i+1)) ** EXP
            
            # normalize pcts & assign $ values
            s = sum(pcts_pay)
            normalized = list(map(lambda p: p/s, pcts_pay))
            payouts_dollars = list(map(lambda p: p * cd_purse, normalized))

            # sort results desc
            cd_results_ranked = sorted(cd_results, key=lambda x:(-1 * float(x['Points After Drops'])))

            # assign awards by player, add to list for output
            for r in range(len(payouts_dollars)):
                row = {
                        "Cycle" : c+1,
                        "Division" : d,
                        'Rank' : r+1, 
                        'Name' : cd_results_ranked[r]['Name'],
                        "TotalPoints" : cd_results_ranked[r]['Total Points'],
                        "PointsAfterDrops" : cd_results_ranked[r]['Points After Drops'],
                        "Award" : round(payouts_dollars[r])
                }
                results_with_pay.append(row)

    return results_with_pay
 
def model_point_systems():
    '''
        write_list_of_lists_as_csvor testing how points compare under two different scoring system for multiple layout consideration efore 2025 season.
        Compare points awarded for two actual rounds using different scoring modifiers
    '''

    #set up - ABN Challenge -2024wk14
    db_settings = {
        'DB_DIR': 'D:\\Code\\CCDG\\CCDG_csv_leauge_util\\database',          
        'DB_NAME': '2024'}
    season = 2024
    wk = 14
    scoring_modifiers_short = {
            "percentage_modifier": 80,
            "score_based_modifier": 20,
            "keep_periods": 8}
    scoring_modifiers_long = {
            "percentage_modifier": 120,
            "score_based_modifier": 30,
            "keep_periods": 8}
    
    #fetch scores from DB
    period_scores = ccdg_scores.get_scores_for_period(db_settings, season, wk)
    
    # Calulate points for shorts in BBB-EEE @ 100 max
    long_divs = {'PRO', 'AAA'}
    period_scores_short = [d for d in period_scores if d['division'] not in long_divs]   
    period_points_short = ccdg_standings.percentage_plus_Marnie(period_scores_short, scoring_modifiers_short)
    
    # Calulate points for shorts in PRO-AAA @150 max
    period_scores_long = [d for d in period_scores if d['division'] in long_divs]   
    period_points_long = ccdg_standings.percentage_plus_Marnie(period_scores_long, scoring_modifiers_long)
    
    #combine and export
    period_points = period_points_long + period_points_short
    outfile = 'D:\\Code\\CCDG\\CCDG_csv_leauge_util\\temp\\2024_wk14-ProAlong-B-Eshort.csv'
    write_list_of_lists_as_csv(outfile, period_points)

    # # 100 Red-A 2014
    # wk = 26
    # scoring_modifiers = {
    #         "percentage_modifier": 80,
    #         "score_based_modifier": 20,
    #         "keep_periods": 8}
    # period_scores = ccdg_scores.get_scores_for_period(db_settings, season, wk)
    # period_points = ccdg_standings.percentage_plus_Marnie(period_scores, scoring_modifiers)
    # outfile = 'D:\\Code\CCDG\\CCDG_csv_leauge_util\\temp\\2024_wk26-100.csv'
    # write_list_of_lists_as_csv(outfile, period_points)

def aces(db: Session, period_list: list | None = None) -> list:
    """
    Return list of occurrences where hole_score == 1.
    Each item: {"Name": <player full_name>, "Period": <period>, "hole": <hole_number>}
    If period_list is provided it will be used to restrict results to those periods.
    """
    stmt = (
        select(
            Player.full_name.label("Name"),
            Score.period.label("Period"),
            HoleScore.hole_number.label("hole"),
        )
        .select_from(HoleScore)
        .join(Score, HoleScore.score)       # HoleScore -> Score
        .join(Player, Score.player)         # Score -> Player
        .where(HoleScore.hole_score == 1)
    )
    if period_list:
        stmt = stmt.where(Score.period.in_(period_list))

    rows = db.execute(stmt).mappings().all()
    return [ {"Name": r["Name"], "Period": r["Period"], "hole": r["hole"]} for r in rows ]



# region ###  H E L P E R  F U N C T I O N S  ###

def tally_cycle_points(vals_to_tally: list, cycle_len: int, keep_periods: int) -> dict:
    '''
    Tally points for all players in a complete cycle.
    Used to close out cycle results.
    '''
    # Check data - do we have a complete cycle's worth of points in vals_to_tally?
    if len(vals_to_tally) < cycle_len:
        raise ValueError(f"Insufficient data to tally cycle points: expected {cycle_len} periods, got {len(vals_to_tally)}")
     
    # sum total and points-after-drops "pad"
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


def avg_scores_by_course_and_division(db_settings: dict):
    '''
    Creates a csv that shows the average score for each division at each of the non-travel-round courses
    Used to distribute fun stats to the leauge
    '''

    cycles = [1,2]
    qs = f'''
        SELECT 
            Schedule.course AS Course,
            Player.division AS Division,
            ROUND(AVG(Score.relative_score), 2) AS Avg_Rel_Score

        FROM Schedule
        INNER JOIN Score on Score.scoring_period = Schedule.scoring_period
        INNER JOIN Player On Score.name = Player.name

        WHERE 
            Score.season = 2024 AND
            Schedule.cycle IN ({",".join([str(c) for c in cycles])}) AND
            Schedule.travel = 'False'
        
        GROUP BY Schedule.course, Player.division
        ORDER by Schedule.course, Player.division
        '''
    #results = db_utils.fetch_many(db_settings, qs)  # returns list of dicts
    # [print(r) for r in results]

    csv_file = f'AvgScoresByCourseAndDiv_c{"c".join([str(c) for c in cycles])}.csv'
    csv_file = os.path.join('temp', csv_file)
    headers = results[0].keys()
    with open(csv_file, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(results)

    print(f"Data has been written to {csv_file}")
    
def read_csv_as_dict(file_path):
    with open(file_path) as f:
        result = [{k: v for k, v in row.items()}
        for row in csv.DictReader(f, skipinitialspace=True)]
    
    return result

def write_dict_as_csv (fname:str, list_of_dicts: list) -> None:
    fieldnames = list_of_dicts[0].keys()
    with open(fname, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(list_of_dicts)

def write_list_of_lists_as_csv(file, list_of_lists):
    with open(file, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(list_of_lists)

def split_list_into_chunks(lst, num_chunks):
    k, m = divmod(len(lst), num_chunks)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(num_chunks)]

# endregion Helper Functions

###  M A I N  ###

# load settings as collection of constants - see config/settings.py
CONFIG = config.Configuration(config.Settings_2025)

def main():

    ## G E N E R A L  S E T U P ##

    # connect to the database = use the session "db" throughout
    exe_dir = os.path.dirname(os.path.abspath(__file__))
    db_file_path = os.path.join(exe_dir, CONFIG.DATABASE['DB_DIR'], CONFIG.DATABASE['DB_NAME'])
    db = ccdg_db.init_db(db_file_path, CONFIG.DATABASE['ECHO']) # get a db session


    # ## D A T A B A S E  C L E A N U P ##

    # # Delete all scores for a period specified in the 2nd argument
    # periods_to_delete = [34,35]  # periods to delete
    # for period in periods_to_delete:
    #     ccdg_scores.delete_scores_for_period(db, period)  

    #     # Associate divisions with player
    #     player_div_associations = [
    #         #['player_id', 'division_id', 'start_period', 'end_period'],
    #         [124, 3 , 1, None],
    #         [125, 6, 1, None]
    #     ]
    #     for pda in player_div_associations:
    #         msg = ccdg_players.update_player_division(db, pda[0], pda[1], pda[2], pda[3])
    #         logger.info(msg)
    #         print(msg)  



    ##  R E S U L T S  &  R E B A L A N C I N G  ##

    # CYCLE_TO_PROCESS = 3  # *** Note: Hardcoded for now  ***
    # # process cycle results and rebalance players - this returns two things:
    # #  - a list of lists ready to write closing cycle results to a csv file
    # #  - a list of new cycle divisions - for the db update to player-division assignments
    # points_sheet_data, new_cycle_divs = process_cycle_results_and_rebalance(CONFIG, db, CYCLE_TO_PROCESS, exe_dir)

    # # # update player divisions assignments in the database
    # # first_last_wks = ccdg_schedule.get_min_max_periods_for_cycle(db, CYCLE_TO_PROCESS +1)  # get the first & last weeks in the new cycle
    # # for p in new_cycle_divs:
    # #     # what is p? [player_id, new_division]
    # #     msg = ccdg_players.update_player_division(db, p[0], p[1], first_last_wks[0], first_last_wks[1] if first_last_wks[1] else None)
    # #     logger.info(msg)
    # # print("Player divisions updated - see log for update messages")

    # #write as output csv file
    # fname = os.path.join(exe_dir, 'temp', f'CCDG_C{CYCLE_TO_PROCESS}_Results.csv')
    # write_list_of_lists_as_csv(fname, points_sheet_data)


    ## A W A R D S ##

    # results_file = r"D:\Code\CCDG\CCDG-Scorkeeping2025\temp\CCDG_2025_AllCycleFinalResults.csv"  #see payouts function for details
    # awards = payouts(results_file)
    # awards_fname =  os.path.join(exe_dir, 'temp', f'CCDG_{CONFIG.SEASON}_Season_Payouts.csv')
    # write_dict_as_csv(awards_fname, awards)

    ## ACES  ##
    aces_list = aces(db) # note, this does not cross ref against who registered for acepot
    aces_fname =  os.path.join(exe_dir, 'temp', f'CCDG_{CONFIG.SEASON}_Season_Aces.csv') 
    write_dict_as_csv(aces_fname, aces_list)



    ## L E A U G E  S T A T S ###

    # Course averages
    # avg_scores_by_course_and_division(CONFIG.DATABASE)

    
    ## R E S E A R C H ##

    # model_point_systems()






if __name__ == "__main__":
    logger.info(f'### START ###  - {os.path.basename(__file__)}')
    start_time = time.time()
    fname = os.path.basename(__file__)
    start_msg = f'{fname} started'
    print(start_msg)

    try:
        main()
        elapsed_time = time.time() - start_time
        msg = f"--- {fname} completed sucessfully in {elapsed_time:.3f} seconds ---\n"
    except Exception as e:
        stack_trace_str = traceback.format_exc()
        msg = f' ---- EXECUTION ERROR  -----\n{e}\n{stack_trace_str}'
    finally:
        print(msg)
        logger.info(msg)
        