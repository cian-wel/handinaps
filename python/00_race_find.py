# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 15:08:55 2022

@author: weldo

00_race_find.py

This file finds the equivalent historic races and outputs a file with their datetime and course
"""

#%% imports, data, variables ===================================================
import pyodbc
import pandas as pd
import string
import numpy as np

pf_db_con = pyodbc.connect(
    r'''Driver={SQL Server};
    Server=localhost\PROFORM_RACING;
    Database=PRODB;
    Trusted_Connection=yes;''')

handinaps = pd.DataFrame(columns=['race', 'crse_name', 'dist', 'month'])
handinaps['race'] = ['lincoln', 'queens cup', 'chester cup', 'victoria cup', 'silver bowl', 'edinburgh cup', 'dash', 'catherine', 'royal hunt cup', 'britannia',
                'wokingham', 'northumberland plate', 'old newton', 'coral challenge', 'bet365 handicap', 'bet365 trophy', 'heritage', 'bunbury cup', 'john smiths', 'international',
                'golden mile', 'stewards cup', 'summer', 'wilfrid', 'revival', 'stayers', 'clipper logistics', 'melrose', 'ebor', 'old borough',
                'portland', 'gold cup', 'cambridgeshire', 'old borough', 'challenge', 'old rowley cup', 'cesarewitch', 'coral sprint', 'balmoral', 'november']

handinaps['crse_name'] = ['doncaster', 'musselburgh', 'chester', 'ascot', 'haydock', 'musselburgh', 'epsom', 'york', 'ascot', 'ascot',
                     'ascot', 'newcastle', 'haydock', 'sandown', 'newmarket', 'newmarket', 'ascot', 'newmarket', 'york', 'ascot',
                     'goodwood', 'goodwood', 'goodwood', 'ripon', 'goodwood', 'york', 'york', 'york', 'york', 'haydock',
                     'doncaster', 'ayr', 'newmarket', 'haydock', 'ascot', 'newmarket', 'newmarket', 'york', 'ascot', 'doncaster']

handinaps['dist'] = [8, 14, 19, 7, 8, 12, 5, 6, 8, 8,
                6, 16, 12, 8, 6, 14, 5, 7, 10, 7,
                8, 6, 14, 6, 7, 16, 8, 14, 14, 14,
                6, 6, 9, 14, 7, 12, 18, 6, 8, 12]

handinaps['month'] = [3, 4, 5, 5, 5, 5, 6, 6, 6, 6,
                 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
                 7, 7, 7, 8, 8, 8, 8, 8, 8, 9,
                 9, 9, 9, 9, 10, 10, 10, 10, 10, 11]

#%% db work ===================================================================
runners = pd.read_sql_query(
        '''SELECT RH_RNo, HIR_HNo, HIR_PositionNo, HIR_BSP, HIR_OddsID FROM vw_Races''',
        pf_db_con)
print('runners imported')

runners = runners.rename(columns={
    "RH_RNo" : "race_id",
    "HIR_HNo" : "horse_id",
    "HIR_PositionNo" : "fin_pos",
    "HIR_BSP" : "win_bsp",
    })
runners['win_bsp'] = runners['win_bsp'] + 1

# odds look up
odds_lkup = pd.read_sql_query('''SELECT O_ID, O_Top, O_Bottom FROM OddsLookups''', pf_db_con)
odds_lkup['isp'] = 1 + odds_lkup.O_Top / odds_lkup.O_Bottom
runners = pd.merge(runners, odds_lkup[['O_ID', 'isp']],
                   left_on = 'HIR_OddsID',
                   right_on = 'O_ID',
                   how = 'left')
runners.drop(['O_ID', 'HIR_OddsID'], inplace=True, axis=1)
del odds_lkup

races = pd.read_sql_query(
        '''SELECT RH_RNo, RH_NoOfRunners, RH_DistanceID, RH_DateTime, RH_GoingID, RH_CNo, RH_Name
        FROM NEW_RH''',
        pf_db_con)
print('races imported')

races = races.rename(columns={
    "RH_RNo" : "race_id",
    "RH_NoOfRunners" : "num_runners",
    "RH_DistanceID" : "dist_id",
    "RH_DateTime" : "race_datetime",
    "RH_CNo" : "course_id",
    "RH_Name" : "race_name"
    })

dist_lkup = pd.read_sql_query('''SELECT D_ID, D_TotalYards FROM DistanceLookups''', pf_db_con)
races = pd.merge(races, dist_lkup[['D_ID', 'D_TotalYards']],
                   left_on = 'dist_id',
                   right_on = 'D_ID',
                   how = 'left')
races.drop(['D_ID', 'dist_id'], inplace=True, axis=1)
races = races.rename(columns={
    'D_TotalYards' : 'distance'
    })
del dist_lkup

course_lkup = pd.read_sql_query('''SELECT C_ID, C_Name, C_Country FROM NEW_C''', pf_db_con)
races = pd.merge(races, course_lkup[['C_ID', 'C_Name', 'C_Country']],
                   left_on = 'course_id',
                   right_on = 'C_ID',
                   how = 'left')
races.drop('C_ID', inplace=True, axis=1)
races = races.rename(columns={
    'C_Name' : 'crse_name',
    'C_Country' : 'crse_country'
    })
del course_lkup

runners = pd.merge(runners, races[['race_id', 'num_runners', 'distance', 'race_datetime', 'course_id', 'crse_name', 'race_name', 'crse_country']],
                   on='race_id',
                   how = 'left')
runners['race_datetime'] = pd.to_datetime(runners['race_datetime'])

del races

print('races added')

# horses db 
horses = pd.read_sql_query('''SELECT H_No, H_Name FROM NEW_H''', pf_db_con)
print('horses imported')

horses = horses.rename(columns={
    'H_No' : 'horse_id',
    'H_Name' : 'horse_name'
    })

runners = pd.merge(runners, horses[['horse_id', 'horse_name']],
                   on = 'horse_id',
                   how = 'left')
print('horses added')
        
del horses

runners = runners[runners.win_bsp != 0]
runners = runners[~runners.fin_pos.isna()]
runners.sort_values(['race_datetime', 'fin_pos'], ascending=[True, True], inplace=True)

runners['handinap_id'] = np.nan

#%% race_datetimes =================================================================
#handinaps = handinaps[(handinaps.index == 0) & (handinaps.index < 40)]
for i in handinaps.index :
    print(handinaps.loc[i, 'race'])
    li = pd.DataFrame(runners[(runners.race_name.str.lower().str.translate(str.maketrans('', '', string.punctuation)).str.contains(handinaps.race[i])) & (runners.crse_name.str.lower() == handinaps.crse_name[i].lower()) & (runners.distance > (handinaps.dist[i]-1)*220) & (runners.distance < (handinaps.dist[i]+1)*220)].race_datetime.drop_duplicates())
    
    # dash
    li = li[li.race_datetime != pd.to_datetime('2019-08-26 15:05:00')]
    
    # northumberland
    li = li[li.race_datetime != pd.to_datetime('2016-06-25 15:40:00')]
    li = li[li.race_datetime != pd.to_datetime('2017-07-01 15:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2018-06-30 13:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2019-06-29 15:00:00')]
    
    # bet365 hc
    li = li[li.race_datetime != pd.to_datetime('2011-06-18 14:35:00')]
    li = li[li.race_datetime != pd.to_datetime('2019-04-17 13:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2020-07-09 12:45:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-04-14 13:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-04-15 13:50:00')]
    
    # tote
    li = li[li.race_datetime != pd.to_datetime('2008-07-27 14:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2009-07-26 17:10:00')]
    
    # bunbury cup
    li = li[li.race_datetime != pd.to_datetime('2015-07-10 16:55:00')]
    li = li[li.race_datetime != pd.to_datetime('2016-07-08 17:20:00')]
    li = li[li.race_datetime != pd.to_datetime('2017-07-14 13:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2018-07-13 13:50:00')]
    
    # stewards
    li = li[li.race_datetime != pd.to_datetime('2015-08-01 14:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2016-07-30 14:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2018-08-04 13:50:00')]
    
    # wilfrid
    li = li[li.race_datetime != pd.to_datetime('2012-08-18 14:20:00')]
    li = li[li.race_datetime != pd.to_datetime('2013-08-17 14:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2014-08-16 14:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2015-08-15 15:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2016-08-13 15:25:00')]
    li = li[li.race_datetime != pd.to_datetime('2018-08-18 14:05:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-08-14 15:10:00')]
    
    # stayers handicap
    li = li[li.race_datetime != pd.to_datetime('2018-10-13 17:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2017-10-14 17:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2016-10-08 16:55:00')]
    li = li[li.race_datetime != pd.to_datetime('2015-10-10 16:35:00')]
    li = li[li.race_datetime != pd.to_datetime('2010-06-11 15:25:00')]
    
    # ebor
    li = li[li.race_datetime != pd.to_datetime('2019-06-15 15:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-06-12 15:05:00')]
    
    # ayr gold cup
    li = li[li.race_datetime != pd.to_datetime('2021-06-22 16:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-07-19 13:35:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-07-26 14:15:00')]
    
    # cambridgeshire
    li = li[li.race_datetime != pd.to_datetime('2010-10-01 16:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2011-09-23 16:45:00')]
    li = li[li.race_datetime != pd.to_datetime('2012-09-28 16:35:00')]
    li = li[li.race_datetime != pd.to_datetime('2013-09-27 17:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2014-09-26 17:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2015-09-25 17:25:00')]
    li = li[li.race_datetime != pd.to_datetime('2016-09-23 17:55:00')]
    li = li[li.race_datetime != pd.to_datetime('2017-09-29 17:20:00')]
    li = li[li.race_datetime != pd.to_datetime('2018-09-22 16:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2019-09-21 16:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2019-09-27 17:20:00')]
    li = li[li.race_datetime != pd.to_datetime('2020-09-19 17:35:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-09-18 16:25:00')]
    
    # challenge
    li = li[li.race_datetime != pd.to_datetime('2008-08-09 15:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2009-08-08 15:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2010-08-07 16:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2011-08-06 15:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2014-09-26 17:00:00')]
    li = li[li.race_datetime != pd.to_datetime('2015-09-25 17:25:00')]
    li = li[li.race_datetime != pd.to_datetime('2016-09-23 17:55:00')]
    li = li[li.race_datetime != pd.to_datetime('2017-09-29 17:20:00')]
    li = li[li.race_datetime != pd.to_datetime('2018-09-22 16:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2019-09-21 16:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2019-09-27 17:20:00')]
    li = li[li.race_datetime != pd.to_datetime('2020-09-19 17:35:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-09-18 16:25:00')]
    
    # cesarewitch
    li = li[li.race_datetime != pd.to_datetime('2009-09-19 15:40:00')]
    li = li[li.race_datetime != pd.to_datetime('2010-09-18 15:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2011-09-17 15:30:00')]
    li = li[li.race_datetime != pd.to_datetime('2012-09-22 15:55:00')]
    li = li[li.race_datetime != pd.to_datetime('2013-09-21 15:40:00')]
    li = li[li.race_datetime != pd.to_datetime('2014-09-20 16:15:00')]
    li = li[li.race_datetime != pd.to_datetime('2015-09-19 15:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2016-09-17 15:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2017-09-23 15:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2018-09-22 15:55:00')]
    li = li[li.race_datetime != pd.to_datetime('2019-09-21 15:55:00')]
    li = li[li.race_datetime != pd.to_datetime('2020-09-19 15:50:00')]
    li = li[li.race_datetime != pd.to_datetime('2021-09-18 15:15:00')]
    
    print(len(li))
    handinaps.loc[i, 'race_datetimes'] = [[li]]
    
    for time in li.race_datetime :
        runners.loc[runners.race_datetime == time, 'handinap_id'] = i

#%% finish up and save ========================================================
handinaps.race[29] = 'old borough x'
runners = runners[~runners.handinap_id.isna()]

handinaps.to_pickle('../data/handinaps.df')
runners.to_pickle('../data/runners.df')

del handinaps, i, li, pf_db_con, runners, time
