# -*- coding: utf-8 -*-
"""
Created on Sun Mar 27 20:16:28 2022

@author: Cian

01_sim.py

file sims out handicaps comp
"""

#%% imports, data, variables ===================================================
import pandas as pd
import numpy as np
import pickle
import random
import string
import warnings 
warnings.filterwarnings("ignore") 

handinaps = pickle.load(open("../data/handinaps.df", 'rb'))
runners = pickle.load(open("../data/runners.df", 'rb'))
bf_place = pd.read_feather('../data/bf_place.ftr')

n_sim = 1000
n_partip = 400
edge_avg = 0
edge_std = 0
stake = 2

#%% data prep ==================================================================
runners['no_punc_name'] = runners.horse_name.str.lower().str.translate(str.maketrans('', '', string.punctuation))
bf_place['no_punc_name'] = bf_place.horse_name.str.lower().str.translate(str.maketrans('', '', string.punctuation))
runners = pd.merge(runners, bf_place[['no_punc_name', 'place_bsp', 'num_places', 'race_datetime']], on=['race_datetime', 'no_punc_name'], how='left')

runners['place_bsp'] = runners.place_bsp / runners.num_places * 4
runners['place_isp'] = (runners.isp-1)*0.25+1
runners['place_prob'] = 1/runners.place_isp
runners['place_isp_no_vig'] = 1/(runners.place_prob / (runners.groupby('race_datetime').place_prob.transform('sum')/4))
runners['place_bsp'] = np.where(runners.place_bsp.isna(), runners.place_isp_no_vig, runners.place_bsp)
runners['win_prob'] = 1/runners.win_bsp
runners['place_prob'] = 1/runners.place_bsp

runners.drop(columns=['distance', 'course_id', 'crse_name', 'crse_country', 'no_punc_name', 'num_places', 'place_isp_no_vig'], inplace=True)

races = runners.drop_duplicates(subset='race_datetime')

mx_pl = list()

#%% run through sim  ==========================================================
#handinaps = handinaps[handinaps.index == 0]
# sim iterate
for i in range(0, n_sim) :
    if (i/100).is_integer() : print(round((i/(n_sim))*100,2),'%')
    comp = pd.DataFrame(index=range(n_partip), columns=range(len(handinaps.index)))
    comp.columns = handinaps.race
    comp['edge'] = 0
    
    # through each race
    for j in handinaps.index :
        
        # select race
        if j != 29 :
            x_race = runners[(runners.handinap_id == j) & (runners.race_datetime == races.race_datetime[random.choice(races[races.handinap_id == j].index)])]
        if j == 29 :
            x_race = runners[(runners.handinap_id == 33) & (runners.race_datetime == races.race_datetime[random.choice(races[races.handinap_id == 33].index)])]
        
        # select win and place
        x_race['won'] = False
        x_race.loc[random.choices(x_race.index, weights=x_race.win_prob, k=1), 'won'] = True
        x_race['place'] = False
        x_race.loc[random.choices(x_race[~x_race.won].index, weights=x_race[~x_race.won].place_prob, k=3), 'place'] = True
        x_race.loc[x_race.won, 'place'] = True
        
        # add values to comp
        
        # selections
        comp[handinaps.race[j]] = np.random.choice(x_race.index, comp.shape[0])
        
        comp = pd.merge(comp, x_race[['won', 'place', 'isp', 'place_isp']], left_on=comp[handinaps.race[j]], right_index=True, how='left')
            
        # results
        comp['ew'] = False
        #comp['ew'] = np.random.choice([False, True], comp.shape[0])
        comp.loc[~comp.ew, handinaps.race[j]] = -stake  + (stake * comp.isp * comp.won)
        comp.loc[comp.ew, handinaps.race[j]] = -stake/2  + (stake/2 * comp.isp * comp.won) -stake/2  + (stake/2 * comp.place_isp * comp.place)
        
        comp.drop(columns=['won', 'place', 'isp', 'place_isp'], inplace=True)

    comp.drop(columns=['ew', 'edge'])
    comp['pl'] = comp.sum(axis=1)
    mx_pl.append(comp.pl.max())
    
mx_pl = pd.DataFrame(mx_pl)

del bf_place, comp, edge_avg, edge_std, handinaps, i, j, n_partip, n_sim, races, runners, stake, x_race
