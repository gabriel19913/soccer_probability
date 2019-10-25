import pandas as pd
import numpy as np
from joblib import Parallel, delayed

def get_table(current_round, max_rounds, league, season, flag):
    dfs = pd.read_html(f'https://www.worldfootball.net/schedule/{league}-{season}-{flag}{current_round}', header=0)
    df = dfs[3]
    df.drop(['Team'], inplace=True, axis=1)

    df.rename(index=str, columns={'#': 'pos', 'Team.1': 'team', 'M.': 'current_round', 'W': 'win',
                                  'D': 'draw', 'L': 'loss', 'goals': 'goals',
                                  'Dif.': 'goals_diff', 'Pt.': 'points_for'}, inplace=True)
    aux = df['goals'].str.split(':', n = 1, expand = True)
    df['goals_for'] = aux[0] 
    df['goals_against'] = aux[1] 
    df.drop(columns = ['goals'], inplace = True)
    df = df.apply(pd.to_numeric, errors='ignore')
    df['pos'] =  pd.to_numeric(df.index) + 1
    df['max_rounds'] = max_rounds
    df['rounds_left'] = df['max_rounds'] - df['current_round']
    df['possible_points'] = df['current_round'] * 3
    df['performance'] = np.round((df['points_for'] / df['possible_points']) * 100, 2)
    df['performance'].replace(np.inf, df['points_for'], inplace=True)
    df.fillna(value={'performance': 0}, inplace=True)
    df['total_possible_points'] = df['points_for'] + (df['rounds_left'] * 3)
    df['goals_for_against_ratio'] = df['goals_for'] / df['goals_against']
    df['goals_for_against_ratio'].replace(np.inf, df['goals_for'], inplace=True)
    df.fillna(value={'goals_for_against_ratio': 0}, inplace=True)
    df['goals_for_against_ratio'] = np.round(df['goals_for_against_ratio'], 3)
    df['league'] = league
    df['season'] = season
    columns = ['pos', 'team', 'current_round', 'max_rounds', 'rounds_left', 'win', 'loss', 'draw', 
               'goals_for', 'goals_against', 'goals_diff', 'goals_for_against_ratio', 'points_for', 
               'possible_points', 'total_possible_points', 'performance', 'league', 'season']
    df = df[columns]
    return df

def get_league(max_rounds, league, season, flag):
    table_list = Parallel(n_jobs=38, verbose=0, backend='threading')(delayed(get_table)(current_round, max_rounds, league, season, flag) for current_round in range(1, max_rounds + 1))
    final_table = pd.concat(table_list, ignore_index=True)
    return final_table

def increment_season(season):
    if '-' in season:
        season_years = season.split('-')
        first_year = int(season_years[0]) + 1
        second_year = int(season_years[1]) + 1
        next_season = str(first_year) + '-' + str(second_year)
        return next_season
    else:
        season_year = int(season) + 1
        next_season = str(season_year)
        return next_season

def get_all_leagues(league_dict):
    list_all_seasons = []
    for key in league_dict.keys():
        if (key == 'bra-serie-a') or (key == 'bra-serie-b'):
            while league_dict[key][0] != '2019':
                flag = 'spieltag/'
                print(key)
                print(league_dict[key][0])
                table = get_league(league_dict[key][1], key, league_dict[key][0], flag)
                league_dict[key][0] = increment_season(league_dict[key][0])
                list_all_seasons.append(table)
                print('DONE!')
        else:
            while league_dict[key][0] != '2019-2020':
                if (key == 'esp-primera-division') and (league_dict[key][0] == '2016-2017'):
                    flag = 'spieltag_2/'
                    print(key)
                    print(league_dict[key][0])
                    table = get_league(league_dict[key][1], key, league_dict[key][0], flag)
                    league_dict[key][0] = increment_season(league_dict[key][0])
                    list_all_seasons.append(table)
                    print('DONE!')
                else:
                    flag = 'spieltag/'
                    print(key)
                    print(league_dict[key][0])
                    table = get_league(league_dict[key][1], key, league_dict[key][0], flag)
                    league_dict[key][0] = increment_season(league_dict[key][0])
                    list_all_seasons.append(table)
                    print('DONE!')
    final_table = pd.concat(list_all_seasons, ignore_index=True)
    return final_table

def add_final_points(df):
    df.loc[((df.index == 98550) | (df.index == 98564)) & (df['team'] == 'Grêmio Prudente'), ['team']] = 'Grêmio Barueri - SP'
    df.loc[((df.index == 96999) | (df.index == 97006)), ['current_round', 'rounds_left', 'loss']] = [(38, 0, 10), (38, 0, 12)]
    unique_leagues = df['league'].unique()
    dfs = []
    for loop_league in unique_leagues:
        unique_seasons = df[df['league'] == loop_league]['season'].unique()
        for season_loop in unique_seasons:
            df_of_season = df[(df['league'] == loop_league) & (df['season'] == season_loop)]
            last_round = np.max(df_of_season['current_round'])
            last_round_df = df_of_season[df_of_season['current_round'] == last_round]
            df_of_season = df_of_season.merge(last_round_df[['team', 'points_for']], how='left', on='team')
            dfs.append(df_of_season)
    final_df = pd.concat(dfs)
    final_df.rename(index=str, columns={'points_for_x': 'points_for', 'points_for_y': 'final_points'}, inplace=True)
    return final_df

league_dict = {'eng-premier-league': ['1995-1996', 38], 'esp-primera-division': ['1997-1998', 38], 
                   'fra-ligue-1': ['2002-2003', 38], 'bundesliga': ['1995-1996', 34], 
                   'ita-serie-a': ['2004-2005', 38], 'ned-eredivisie': ['1995-1996', 34], 
                   'bra-serie-a': ['2006', 38], 'bra-serie-b': ['2011', 38]}

final_dataframe = get_all_leagues(league_dict)
print(final_dataframe.sample(5))
print(final_dataframe.info())
df = add_final_points(final_dataframe)
print(df.sample(5))
print(df.info())

df.to_csv('all_leagues_seasons.csv', sep= ';', index = False)
