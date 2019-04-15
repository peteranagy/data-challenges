import json
import pandas as pd
import numpy as np
from datetime import datetime
import math
import time
from flask import Flask
from flask import request
from flask import current_app
app = Flask(__name__)

@app.route("/started")
def started():
    return 'FING'


@app.route("/")
def process():
    input_json = json.load(open('input.json','r'))
    input_df = pd.read_pickle(input_json['data_pickle'])
    m_i_df = input_df
    p_i_df=app.p_i_df
    p_v_df=app.p_v_df
    t_i_df=app.t_i_df
    m_o_df=app.m_o_df


    #m_o_df format

    m_o_df = m_o_df.assign( buzijel = lambda df: df['odds'].apply(lambda x: str(x)).apply(lambda x: x[0]))
    m_o_df = m_o_df.assign( buzijel1 = lambda df: df['odds1'].apply(lambda x: str(x)).apply(lambda x: x[0]))
    m_o_df = m_o_df.assign( buzijel2 = lambda df: df['odds2'].apply(lambda x: str(x)).apply(lambda x: x[0]))
    m_o_df = m_o_df.assign( buzijelx = lambda df: df['oddsx'].apply(lambda x: str(x)).apply(lambda x: x[0]))

    m_o_df['buzijel'] = pd.to_numeric(m_o_df['buzijel'], errors = 'coerce')
    m_o_df['buzijel1'] = pd.to_numeric(m_o_df['buzijel1'], errors = 'coerce')
    m_o_df['buzijel2'] = pd.to_numeric(m_o_df['buzijel2'], errors = 'coerce')
    m_o_df['buzijelx'] = pd.to_numeric(m_o_df['buzijelx'], errors = 'coerce')

    m_o_df = m_o_df.dropna()

    m_o_df['odds'] = pd.to_numeric(m_o_df['odds'])
    m_o_df['odds1'] = pd.to_numeric(m_o_df['odds1'])
    m_o_df['odds2'] = pd.to_numeric(m_o_df['odds2'])
    m_o_df['oddsx'] = pd.to_numeric(m_o_df['oddsx'])

    m_o_df['at'] = [x.replace("\xa0","") for x in m_o_df['at']]
    m_o_df['ht'] = [x.replace("\xa0","") for x in m_o_df['ht']]

    # most-games-played-in-same-position-by-player

    a1 = m_i_df.loc[:,['away1_id','away1_pos']]
    a1.rename(columns={'away1_id':'pid','away1_pos':'pos'},inplace=True)

    a2 = m_i_df.loc[:,['away2_id','away2_pos']]
    a2.rename(columns={'away2_id':'pid','away2_pos':'pos'},inplace=True)

    a3 = m_i_df.loc[:,['away3_id','away3_pos']]
    a3.rename(columns={'away3_id':'pid','away3_pos':'pos'},inplace=True)

    a4 = m_i_df.loc[:,['away4_id','away4_pos']]
    a4.rename(columns={'away4_id':'pid','away4_pos':'pos'},inplace=True)

    a5 = m_i_df.loc[:,['away5_id','away5_pos']]
    a5.rename(columns={'away5_id':'pid','away5_pos':'pos'},inplace=True)

    a6 = m_i_df.loc[:,['away6_id','away6_pos']]
    a6.rename(columns={'away6_id':'pid','away6_pos':'pos'},inplace=True)

    a7 = m_i_df.loc[:,['away7_id','away7_pos']]
    a7.rename(columns={'away7_id':'pid','away7_pos':'pos'},inplace=True)

    a8 = m_i_df.loc[:,['away8_id','away8_pos']]
    a8.rename(columns={'away8_id':'pid','away8_pos':'pos'},inplace=True)

    a9 = m_i_df.loc[:,['away9_id','away9_pos']]
    a9.rename(columns={'away9_id':'pid','away9_pos':'pos'},inplace=True)

    a10 = m_i_df.loc[:,['away10_id','away10_pos']]
    a10.rename(columns={'away10_id':'pid','away10_pos':'pos'},inplace=True)

    a11 = m_i_df.loc[:,['away11_id','away11_pos']]
    a11.rename(columns={'away11_id':'pid','away11_pos':'pos'},inplace=True)

    h1 = m_i_df.loc[:,['home1_id','home1_pos']]
    h1.rename(columns={'home1_id':'pid','home1_pos':'pos'},inplace=True)

    h2 = m_i_df.loc[:,['home2_id','home2_pos']]
    h2.rename(columns={'home2_id':'pid','home2_pos':'pos'},inplace=True)

    h3 = m_i_df.loc[:,['home3_id','home3_pos']]
    h3.rename(columns={'home3_id':'pid','home3_pos':'pos'},inplace=True)

    h4 = m_i_df.loc[:,['home4_id','home4_pos']]
    h4.rename(columns={'home4_id':'pid','home4_pos':'pos'},inplace=True)

    h5 = m_i_df.loc[:,['home5_id','home5_pos']]
    h5.rename(columns={'home5_id':'pid','home5_pos':'pos'},inplace=True)

    h6 = m_i_df.loc[:,['home6_id','home6_pos']]
    h6.rename(columns={'home6_id':'pid','home6_pos':'pos'},inplace=True)

    h7 = m_i_df.loc[:,['home7_id','home7_pos']]
    h7.rename(columns={'home7_id':'pid','home7_pos':'pos'},inplace=True)

    h8 = m_i_df.loc[:,['home8_id','home8_pos']]
    h8.rename(columns={'home8_id':'pid','home8_pos':'pos'},inplace=True)

    h9 = m_i_df.loc[:,['home9_id','home9_pos']]
    h9.rename(columns={'home9_id':'pid','home9_pos':'pos'},inplace=True)

    h10 = m_i_df.loc[:,['home10_id','home10_pos']]
    h10.rename(columns={'home10_id':'pid','home10_pos':'pos'},inplace=True)

    h11 = m_i_df.loc[:,['home11_id','home11_pos']]
    h11.rename(columns={'home11_id':'pid','home11_pos':'pos'},inplace=True)

    frames = [a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11]
    pos_df = pd.concat(frames)
    pos_df = pos_df.groupby(['pid','pos']).size().unstack(fill_value = 0)
    pos_df["max_value"] = pos_df.max(axis = 1)

    print('#1 done')

    # most-different-positions-by-player

    pos_df["diff_pos"] = pos_df.gt(0).sum(axis=1)

    print('#2 done')

    # most-different-formations-by-player

    a1 = m_i_df.loc[:,['away1_id','away_formation']]
    a1.rename(columns={'away1_id':'pid','away_formation':'pos'},inplace=True)

    a2 = m_i_df.loc[:,['away2_id','away_formation']]
    a2.rename(columns={'away2_id':'pid','away_formation':'pos'},inplace=True)

    a3 = m_i_df.loc[:,['away3_id','away_formation']]
    a3.rename(columns={'away3_id':'pid','away_formation':'pos'},inplace=True)

    a4 = m_i_df.loc[:,['away4_id','away_formation']]
    a4.rename(columns={'away4_id':'pid','away_formation':'pos'},inplace=True)

    a5 = m_i_df.loc[:,['away5_id','away_formation']]
    a5.rename(columns={'away5_id':'pid','away_formation':'pos'},inplace=True)

    a6 = m_i_df.loc[:,['away6_id','away_formation']]
    a6.rename(columns={'away6_id':'pid','away_formation':'pos'},inplace=True)

    a7 = m_i_df.loc[:,['away7_id','away_formation']]
    a7.rename(columns={'away7_id':'pid','away_formation':'pos'},inplace=True)

    a8 = m_i_df.loc[:,['away8_id','away_formation']]
    a8.rename(columns={'away8_id':'pid','away_formation':'pos'},inplace=True)

    a9 = m_i_df.loc[:,['away9_id','away_formation']]
    a9.rename(columns={'away9_id':'pid','away_formation':'pos'},inplace=True)

    a10 = m_i_df.loc[:,['away10_id','away_formation']]
    a10.rename(columns={'away10_id':'pid','away_formation':'pos'},inplace=True)

    a11 = m_i_df.loc[:,['away11_id','away_formation']]
    a11.rename(columns={'away11_id':'pid','away_formation':'pos'},inplace=True)

    h1 = m_i_df.loc[:,['home1_id','home_formation']]
    h1.rename(columns={'home1_id':'pid','home_formation':'pos'},inplace=True)

    h2 = m_i_df.loc[:,['home2_id','home_formation']]
    h2.rename(columns={'home2_id':'pid','home_formation':'pos'},inplace=True)

    h3 = m_i_df.loc[:,['home3_id','home_formation']]
    h3.rename(columns={'home3_id':'pid','home_formation':'pos'},inplace=True)

    h4 = m_i_df.loc[:,['home4_id','home_formation']]
    h4.rename(columns={'home4_id':'pid','home_formation':'pos'},inplace=True)

    h5 = m_i_df.loc[:,['home5_id','home_formation']]
    h5.rename(columns={'home5_id':'pid','home_formation':'pos'},inplace=True)

    h6 = m_i_df.loc[:,['home6_id','home_formation']]
    h6.rename(columns={'home6_id':'pid','home_formation':'pos'},inplace=True)

    h7 = m_i_df.loc[:,['home7_id','home_formation']]
    h7.rename(columns={'home7_id':'pid','home_formation':'pos'},inplace=True)

    h8 = m_i_df.loc[:,['home8_id','home_formation']]
    h8.rename(columns={'home8_id':'pid','home_formation':'pos'},inplace=True)

    h9 = m_i_df.loc[:,['home9_id','home_formation']]
    h9.rename(columns={'home9_id':'pid','home_formation':'pos'},inplace=True)

    h10 = m_i_df.loc[:,['home10_id','home_formation']]
    h10.rename(columns={'home10_id':'pid','home_formation':'pos'},inplace=True)

    h11 = m_i_df.loc[:,['home11_id','home_formation']]
    h11.rename(columns={'home11_id':'pid','home_formation':'pos'},inplace=True)

    frames = [a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11]
    formation_df = pd.concat(frames)
    formation_df = formation_df.groupby(['pid','pos']).size().unstack(fill_value = 0)
    formation_df["diff_formation"] = formation_df.gt(0).sum(axis=1)

    print('#3 done')

    # largest-odds-overcome-in-game

    m_o_df['win_odds'] = m_o_df[['odds1', 'odds2']].max(axis=1) == m_o_df['odds']

    print('#4 done')

    # team-with-most-wins-as-underdog

    m_o_df = m_o_df.assign( score_at = lambda df: df['score'].apply(lambda x: int(x.split('-')[0])))
    m_o_df = m_o_df.assign( score_ht = lambda df: df['score'].apply(lambda x: int(x.split('-')[1])))
    m_o_df['underdog'] = np.where(m_o_df['odds1'] > m_o_df['odds2'], m_o_df['at'], m_o_df['ht'])
    m_o_df['favorite'] = np.where(m_o_df['odds1'] < m_o_df['odds2'], m_o_df['at'], m_o_df['ht'])
    m_o_df['winner'] = np.where(m_o_df['score_at'] > m_o_df['score_ht'], m_o_df['at'],\
                            np.where(m_o_df['score_at'] == m_o_df['score_ht'], 'draw', m_o_df['ht']))
    m_o_df['loser'] = np.where(m_o_df['score_at'] < m_o_df['score_ht'], m_o_df['at'],\
                            np.where(m_o_df['score_at'] == m_o_df['score_ht'], 'draw', m_o_df['ht']))
    m_o_df['underwinner'] = np.where(m_o_df['underdog'] == m_o_df['winner'], 1, 0)

    print('#5 done')

    # team-with-most-losses-as-favorite
    m_o_df['favorloser'] = np.where(m_o_df['favorite'] == m_o_df['loser'], 1, 0)

    print('#6 done')

    # team-with-lowest-average-odds-of-draw

    t_m_odds = pd.concat([m_o_df['at'], m_o_df['ht']]).to_frame(name = 'teams')
    t_m_odds['draw_odds'] = m_o_df['oddsx']

    print('#7 done')

    #player-with-highest-number-of-games    
    hanymeccs=m_i_df[['away10_id','away11_id','away1_id','away2_id','away3_id','away4_id','away5_id','away6_id','away7_id','away8_id','away9_id','home10_id','home11_id','home1_id','home2_id','home3_id','home4_id','home5_id','home6_id','home7_id','home8_id','home9_id']].unstack().value_counts().sort_index()

    print('#8 done')

    #number-of-players-with-no-games
    nemjatszott=0
    for i in range(len(app.osszesjatekos)):
        if int(app.osszesjatekos.iloc[i,2]) not in hanymeccs.index:
            nemjatszott+=1

    print('#9 done')

    #player-with-highest-number-of-games-where-his-team-didnt-concede
    
    m_i_df['score_home'] = m_i_df['score'].str.split('-').str[0]
    m_i_df['score_away'] = m_i_df['score'].str.split('-').str[1]
    
    concede=m_i_df.drop_duplicates()
    nemkap1=concede[concede['score_home']=='0']
    nemkap2=concede[concede['score_away']=='0']
    df1=nemkap2[['away10_id','away11_id','away1_id','away2_id','away3_id','away4_id','away5_id','away6_id','away7_id','away8_id','away9_id']].unstack().value_counts()
    df2=nemkap1[['home10_id','home11_id','home1_id','home2_id','home3_id','home4_id','home5_id','home6_id','home7_id','home8_id','home9_id']].unstack().value_counts()

    print('#9 done')

    #position-with-largest-average-height
    
    df_height=m_i_df[a]
    print(df_height)
    print(m_i_df)

    frames=[]
    def tablagenerator():
        for i in range(1,12):
            haho[i-1]=df_height[['home%s_id' %i,'home%s_pos' %i]]
            haho[i-1].columns=['playerid','pos']
            frames.append(haho[i-1])
        result = pd.concat(frames,axis=0,ignore_index=False)
        return result
    df_playerid=tablagenerator()

    average_height=pd.merge(df_playerid,app.p_i_df,left_on='playerid',right_on='playerid')
    average_height=average_height.dropna().drop_duplicates()
    average_height['height']=average_height['height'].apply(stringtoint)

    print('#10 done')

    #player-with-most-different-teams

    idk=[]
    idk.append('date')
    for i in range(1,12):
        idk.append('away%s_id' %i)
        idk.append('home%s_id' %i)
    idk
    frames2=[]
    def tablageneratorasd():
        for i in range(1,12):
            idk[i]=m_i_df[['home%s_id' %i,'date','home_team']]
            idk[i].columns=['playerid','date','team']
            frames2.append(idk[i])
        for j in range(1,12):
            idk[j]=m_i_df[['away%s_id' %j,'date','away_team']]
            idk[j].columns=['playerid','date','team']
            frames2.append(idk[j])
            
        result = pd.concat(frames2,axis=0,ignore_index=True)
        return result

    players = tablageneratorasd()

    print('#11 done')

    #team-with-highest-profit-for-losing

    frames3 = []
    meccs1 = m_o_df[['at','odds1']].rename(columns={"at": "team", "odds1": "against_odds"})
    frames3.append(meccs1)

    meccs2 = m_o_df[['ht','odds2']].rename(columns={"ht": "team", "odds2": "against_odds"})
    frames3.append(meccs2)

    bad_odds = pd.concat(frames3)

    print('#12 done')

     #position-with-largest-average-height
    
    df_height=m_i_df[a]
    print(df_height)
    print(m_i_df)

    frames=[]
    def tablagenerator():
        for i in range(1,12):
            haho[i-1]=df_height[['home%s_id' %i,'home%s_pos' %i]]
            haho[i-1].columns=['playerid','pos']
            frames.append(haho[i-1])
        result = pd.concat(frames,axis=0,ignore_index=False)
        return result
    df_playerid=tablagenerator()
    print(df_playerid)

    average_height=pd.merge(df_playerid,p_i_df,left_on='playerid',right_on='playerid')
    average_height=average_height.dropna().drop_duplicates()
    print(average_height)
    average_height['height']=average_height['height'].apply(stringtoint)

    print('#13 done')

    # position with youngest average age
    average_height=average_height.assign(y_as_d = lambda df: pd.to_datetime(average_height['dob'], format = '%Y-%m-%d'))
    youngest_age=average_height.drop_duplicates()
    youngest_age=youngest_age.assign(fiatal = lambda youngest_age: pd.Timestamp.now()-youngest_age['y_as_d']).sort_values('fiatal')
    youngest_age['fiatal']=(youngest_age['fiatal'].dt.days/365)

    print('#14 done')

    # # position with highest average value
    highest_value=pd.merge(average_height, p_v_df)

    print('#15 done')

    # # longest-time-in-days-between-two-games-for-player
    frames2=[]
    def tablagenerator2():
        for i in range(1,12):
            idk[i]=m_i_df[['home%s_id' %i,'date']]
            idk[i].columns=['playerid','date']
            frames2.append(idk[i])
        for j in range(1,12):
            idk[j]=m_i_df[['away%s_id' %j,'date']]
            idk[j].columns=['playerid','date']
            frames2.append(idk[j])
            
        result = pd.concat(frames2,axis=0,ignore_index=True)
        return result
    longest_df=tablagenerator2()
    longest_df=longest_df.assign(y_as_d = lambda df: pd.to_datetime(longest_df['date'], format = '%Y-%m-%d'))
    longest_df=longest_df.sort_values(['playerid','y_as_d'])
    lista = longest_df['y_as_d'].tolist()
    pl = longest_df['playerid'].tolist()
    kul_lista=[0]
    for i in range(len(lista)):
        if i == 0:
            continue
        else:
            if pl[i] == pl[i-1]:
                kul_lista.append(lista[i]- lista[i-1])
            else:
                kul_lista.append(0)

    print('#16 done')

    # # a legtöbb kapott gól nélküli meccset lehozó kapus
    frames=[]
    def tablagenerator5():
        for i in range(1,12):
            haho[i-1]=m_i_df[['home%s_id' %i,'home%s_pos' %i,'score_away','date']]
            haho[i-1].columns=['playerid','pos','kapott_gol','date']
            frames.append(haho[i-1])
        for i in range(1,12):
            haho[i-1]=m_i_df[['away%s_id' %i,'away%s_pos' %i,'score_home','date']]
            haho[i-1].columns=['playerid','pos','kapott_gol','date']
            frames.append(haho[i-1])
        result = pd.concat(frames,axis=0,ignore_index=False)
        return result
    goalkeeper=tablagenerator5()
    goalkeeper=goalkeeper[goalkeeper['pos']=='top: 80%; left: 40%;']
    kapott_nulla=goalkeeper.sort_values(['playerid','date'], ascending=True)
    kapott_nulla=kapott_nulla.reset_index()
    
    print('már majdnem jó')


    def nulla():
        vissza=0
        
        for i, r in kapott_nulla.iterrows():
            jelen=0
            if i==0:
                continue
            else:
                if kapott_nulla.loc[i,'playerid']==kapott_nulla.loc[i-1,'playerid']:
                    
                    if kapott_nulla.loc[i-1,'kapott_gol']=='0':
                        jelen+=1
                        if jelen>vissza:
                            vissza=jelen
                    else:
                        jelen=0
                
        return vissza

    vissza=nulla()

    print('gyerünkkk++')

    print('#17 done')

    # # Átlagosan legtöbb gólt kapó kapus születési dátuma és magassága
    kapott_nulla['kapott_gol']=kapott_nulla['kapott_gol'].apply(stringtoint)
    legtöbb_kapott=kapott_nulla.groupby('playerid').agg({'kapott_gol':'mean'}).sort_values('kapott_gol', ascending=False)

    print('#18 done')

    # Median of winning team average age
    m_i_df['winning_team']=np.where(m_i_df['score_home']<m_i_df['score_away'], m_i_df['away_team'],np.where(m_i_df['score_home']>m_i_df['score_away'], m_i_df['home_team'], float('nan')))
    frames4=[]
    def tablagenerator4():
        for i in range(1,12):
            idk[i]=m_i_df[['home%s_id' %i,'home_team','date','winning_team']]
            idk[i].columns=['playerid','team_id','mdate','winning_team']
            frames4.append(idk[i])
        for j in range(1,12):
            idk[j]=m_i_df[['away%s_id' %j,'away_team','date','winning_team']]
            idk[j].columns=['playerid','team_id','mdate','winning_team']
            frames4.append(idk[j])
           
        result = pd.concat(frames4,axis=0,ignore_index=True)
        return result
    avg_age=tablagenerator4().drop_duplicates()
    avg_age=avg_age.assign(y_as_d = lambda df: pd.to_datetime(avg_age['mdate'], format = '%Y-%m-%d')).drop(columns=['mdate'])
    seged=p_i_df[['dob','playerid']].assign(player_date = lambda df: pd.to_datetime(p_i_df['dob'], format= '%Y-%m-%d')).drop(columns=['dob'])
    avg_age=pd.merge(avg_age,seged)
    avg_age=avg_age.assign(eletkor=lambda df: avg_age['y_as_d']-avg_age['player_date'])
    avg_age['eletkor'] = avg_age['eletkor'].dt.days
    avg_age=avg_age.dropna()
    avg_age['iswinner']=np.where(avg_age['team_id']==avg_age['winning_team'], True, False)

    print('#19 done')

    # # biggest-stadium-capacity-difference-upset - kisebb stadionú csapat nyer
    biggest0=pd.merge(t_i_df,m_i_df[['home_team','away_team', 'score_home','score_away']],left_on='team_id',right_on='home_team')
    biggest=pd.merge(biggest0,t_i_df,left_on='away_team', right_on='team_id')
    biggest['seat_diff']=(biggest['seats_x']-biggest['seats_y'])
    biggest.dropna()[biggest['seat_diff']<0].sort_values('seat_diff')[biggest['score_home']>biggest['score_away']]

    print('#20 done')

    # # Capacity of stadium of team with most games
    home_df=biggest['name_x'].value_counts()
    away_df=biggest['name_y'].value_counts()
    capa=home_df.add(away_df).sort_values(ascending=False).index[0]

    print('#21 done')

    #result = current_app.
    
    out = [{'most-used-formation':input_df[['home_formation','away_formation']].unstack().value_counts().index[0]},
            {'number-of-players-with-no-games':str(nemjatszott)},
          {'player-with-highest-number-of-games':str(hanymeccs.sort_values(ascending=False).index[0])},
          {'player-with-highest-number-of-games-where-his-team-didnt-concede':str(df1.add(df2).sort_values(ascending=False).index[0])},
          {'most-games-played-in-same-position-by-player':str(pos_df.sort_values(by='max_value', ascending = False).head(1).max(axis=1).iloc[0])},
          {'most-different-positions-by-player':str(pos_df.sort_values(by='diff_pos', ascending = False).head(1).max(axis=1).iloc[0])},
          {'most-different-formations-by-player':str(formation_df.sort_values(by='diff_formation', ascending = False).head(1).loc[:,'diff_formation'].iloc[0])},
          {'largest-odds-overcome-in-game':str(m_o_df.loc[m_o_df['win_odds'] == True].sort_values(by='odds2', ascending = False).head(1).loc[:,'odds'].iloc[0])},
          {'team-with-most-wins-as-underdog':str(m_o_df.groupby('underdog').agg({'underwinner':'sum'}).sort_values(by = 'underwinner', ascending = False).head(1).index[0])},
          {'team-with-most-losses-as-favorite':str(m_o_df.groupby('favorite').agg({'favorloser':'sum'}).sort_values(by = 'favorloser', ascending = False).head(1).index[0])},
          {'team-with-lowest-average-odds-of-draw':str(t_m_odds.groupby('teams').mean().sort_values(by = 'draw_odds', ascending = False).head(1).index[0])},
          {'position-with-largest-average-height':str(average_height.groupby('pos').agg({'height':'mean'}).sort_values('height',ascending=False).index[0])},
          {'player-with-most-different-teams':str(players.groupby('playerid').agg({'team':pd.Series.nunique}).sort_values(by = 'team', ascending = False).head(1).index[0])},
          {'team-with-highest-profit-for-losing':str(bad_odds.groupby('team').agg({'against_odds':'sum'}).sort_values(by = "against_odds", ascending = False).head(1).index[0])},
            {'position-with-youngest-average-age':str(youngest_age.groupby('pos').agg({'fiatal':'mean'}).sort_values('fiatal').index[0])},
            {'position-with-highest-average-value':str(highest_value.groupby('pos').agg({'y':'mean'}).sort_values('y',ascending=False).index[0])},
            {'longest-time-in-days-between-two-games-for-player':str(max([f for f in kul_lista if not type(f) == int]))},
            {'goalkeeper-with-most-clean-sheets':str(vissza)},
          {'Átlagosan legtöbb gólt kapó kapus születési dátuma':str(legtöbb_kapott.reset_index().merge(p_i_df).loc[0,'dob'])},
          {'median-of-winning-team-average-age':str(avg_age[avg_age['iswinner']==True].groupby(['team_id','y_as_d']).agg({'eletkor':'mean'}).reset_index()['eletkor'].median()/365)}, 
          {'biggest-stadium-capacity-difference-upset':str(abs(biggest.dropna()[biggest['seat_diff']<0].sort_values('seat_diff')[biggest['score_home']>biggest['score_away']]['seat_diff'].iloc[0]))},
          {'capacity-of-stadium-of-team-with-most-games':str(biggest[biggest['name_x']==capa]['seats_x'].iloc[0])},
          ]
    
    print(out)
    json.dump(out,open('output.json','w'))
    return 'FING'


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

@app.route('/shutdown')
def shutdown():
    shutdown_server()
    return 'Server shutting down...'

args = json.load(open('data.json','r'))

app.p_i_df = pd.read_csv(args['p_info'])
app.t_i_df = pd.read_csv(args['t_info'])
app.p_v_df = pd.read_csv(args['p_values'])
app.m_o_df = pd.read_csv(args['m_odds'])
app.osszesjatekos=app.p_i_df.sort_values(by='playerid')

haho=[]
for i in range(1,12):
    haho.append(('df%s' %i))

a=[]
b=[]
for k in range(1,12):
    a.append('home%s_id' %k)
    a.append('home%s_pos' %k)
    a.append('away%s_id' %k)
    a.append('away%s_pos' %k)
    b.append('home%s_id' %k)

print(a,b)

def stringtoint(a):
    if type(a)==str:
        a=a.replace(';','')
        a=a.replace(' ','')
        a=a.replace(',','')
        a=a.replace('.','')
    try:
        a=int(a)
    except:
        a=float('nan')
    return a

if __name__ == '__main__':
    app.run(debug=True,port=5112)
