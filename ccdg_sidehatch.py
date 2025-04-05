import csv, time, os
import math
from statistics import mean
import traceback

import ccdg_settings
import ccdg_scores
import ccdg_standings
import database.db_utils as db_utils
import google_apis.google_tasks as g
from logger.logger import logger_players

'''
ccdg_sidehatch.py contains smaller scripts useful for semi-regular one-off tasks.

IMPORTANT: Modify main() to call only the functions you need.  Here are the major ones:
    * rebalance() - generates results for a cycle and assigns new divs. Export (do not update database)
    * update_player_divs() - will update the Player table in the database
    * export_winners_and_payouts() - 

NB: The name ccdg_sidehatch was inspired by the classic Tenacious D act that you
really ought to watch, right now: https://www.youtube.com/watch?v=212XFe-ICeM

So, Put on a cool 70's groove..
'''

# load settings as collection of constants - see config/settings.py
CONFIG = ccdg_settings.Configuration(ccdg_settings.Settings_2024)


###  P R I M A R Y  F U N C T I O N S  ###

def rebalance(cycle_detail, db_settings):
    '''
    reblance divsions - generate a list of players with cols, scores and divs for old and new cycles
    '''

    # get all PRO & Am players as lists of dicts
    qs = '''Select name, division 
            FROM Player 
            WHERE
                division = "PRO" AND
                UD_user_name is NULL;'''
    players_pro = db_utils.fetch_many(db_settings, qs)
    qs = '''Select name, division 
            FROM Player 
            WHERE
                division != "PRO" AND
                UD_user_name is NULL;'''
    players_am = db_utils.fetch_many(db_settings, qs)

    # # get all the scores & generate points_by_period
    season_periods_elapsed = ccdg_scores.get_max_scoring_period(db_settings, cycle_detail['season'])
    points_by_period = []
    for period in range(season_periods_elapsed):
        wk = period + 1
        period_scores = ccdg_scores.get_scores_for_period(db_settings, cycle_detail['season'], wk)
        period_points = ccdg_standings.percentage_plus_Marnie(period_scores, CONFIG.SCORING)
        points_by_period.append(period_points)
    
    # transpose to points_by_player
    point_dicts = ccdg_standings.transpose_for_output(
        db_settings, 
        points_by_period, 
        cycle_detail['season'], 
        CONFIG.LEAD_COLS_POINTS, 
        CONFIG.SCORING)
        
    # elimiate zeros from skipped weeks and average points from rounds that were played - for Am players only
    round_avg_by_player = []
    for d in point_dicts:
        name = d['Name']
        found = any(record.get('name') == name for record in players_am)
        if (found):
            points_cols = len(CONFIG.LEAD_COLS_POINTS)
            points_list = list(d.values())[points_cols:]
            pts_lst_no_zero = [wk for wk in points_list if not wk == 0]
            round_avg_pts = mean(pts_lst_no_zero)
            player_avg_to_rank = {
                'name': name,
                'per_avg_no_zero':round_avg_pts
            }
            round_avg_by_player.append(player_avg_to_rank)

    # rank Am players by avg pts per round played
    round_avg_by_player = sorted(round_avg_by_player, key=lambda x:(-x['per_avg_no_zero']))

    # split into tot_div_count 
    tot_div_count = len(CONFIG.DIVISIONS)-1  # minus one for pro
    div_chunks_players = list(ccdg_standings.split_chunks(round_avg_by_player, tot_div_count))

    # create a lookup list of player, new_div_label as [[name, label], [...], ...]
    new_div_lables = []
    for i in range(len(div_chunks_players)):
        chunk = div_chunks_players[i]
        new_div_label = chr(i + 65) * 3   #65 is the capitalA in ASCII character set
        [new_div_lables.append([row['name'], new_div_label]) for row in chunk]

    # add PROs back to the LUT - one you go pro, you can never go back
    players_pro_lists = [[p['name'], p['division'], 'PRO'] for p in players_pro]
    new_div_lables.extend(players_pro_lists)
    
    # add new_div_label to the typical global_points output as an extra col & sort
    for csv_row in point_dicts:
        new_div_label = [r[1] for r in new_div_lables if r[0] == csv_row['Name']]
        try:
            csv_row['New Division'] = new_div_label[0]
        except:
            pass

    new_csv_rows = sorted(point_dicts, key=lambda x:(-x['Points After Drops Cycle'])) 
    return new_csv_rows

def update_divisions(divsion_list: list, db_settings: dict) -> None:
    '''
    Updates Division column in Player table - mea
 
    '''
    # update Player table
    for p in divsion_list:
        qry_str = f'''UPDATE Player SET division = "{p['New Division']}" WHERE name = "{p['Name']}";'''
        result = db_utils.execute(db_settings, qry_str)
        if result != 1:
            raise Exception(f'problem with player {p["Name"]}')
    pass

def payouts() -> list:
    '''
        Export a csv of the winners by division and cycle.
        Explainer: https://docs.google.com/spreadsheets/d/1fKC4LYePnw59rJeYU72Z_LNQfFF8BTAyZZ4Q53spd2U/edit?gid=0#gid=0
    '''
    # setup
    DIVS = CONFIG.DIVISIONS
    CYCLES = 3
    PURSE_PER_PLAYER = 25                    # portion of entry fee for entire season in $USD
    CYCLE_DIV_RESULTS = './2024_data/CCDG 2024 Leauge - Results - Final.csv'    # flat file with players by div/cycle 
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
            print(f'p:C{c+1}:{d}:{len(cd_results)}')
            
            # compute purses, winners, payouts
            cd_purse = len(cd_results) * PURSE_PER_PLAYER / CYCLES
            count_winners = math.floor(len(cd_results) * RATIO_WINNERS)
            print(f'w:C{c+1}:{d}:{count_winners}')

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
    outfile = 'D:\\Code\CCDG\\CCDG_csv_leauge_util\\temp\\2024_wk14-ProAlong-B-Eshort.csv'
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



###  H E L P E R  F U N C T I O N S  ###

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
    results = db_utils.fetch_many(db_settings, qs)  # returns list of dicts
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
    csv_file = f'./2024_data/Awards.csv'
    fieldnames = list_of_dicts[0].keys()
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(list_of_dicts)

def write_list_of_lists_as_csv(file, list_of_lists):
    with open(file, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(list_of_lists)


###  M A I N  ###

def main():

    model_point_systems()


    # ##  R E B A L A N C I N G  ##
    # # Figure out new Divs
    # closing_cycle = {
    #     'season': 2024,
    #     'cycle': 2
    # }

    ## rebalance from db data - make sure all weeks in the cycle have been imported to db
    # csv_rows = rebalance(closing_cycle, CONFIG.DATABASE)
    # csv_file = f'./temp/{closing_cycle['season']}_C{closing_cycle['cycle']}-results+new_C{closing_cycle['cycle']+1}-_divisions.csv'
    # write_dict_as_csv(csv_file, csv_rows)


    ## Update db
    # csv_rows = rebalance(closing_cycle, CONFIG.DATABASE)
    # update_divisions(csv_rows, CONFIG.DATABASE)


    ## L E A U G E  S T A T S ###
    # Course averages
    # avg_scores_by_course_and_division(CONFIG.DATABASE)

    
    ## A W A R D S ##
    # awards = payouts()
    # awards_fname = './2024_data/awards.csv'
    # write_dict_as_csv(awards_fname, awards)




if __name__ == "__main__":

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