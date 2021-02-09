import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import os
import numpy as np

# MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

# 1. fighters Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    fighters = pd.read_sql_table('fighters', con=con)

# 2. html 에서 round 정보를 스크래핑한다.
rounds_columns = ['match_id', 'fighter_id', 'round_number', 'TD_landed', 'TD_attempted',
                  'SUB_attempted', 'REV', 'CTRL_sec', 'KD', 'HEAD_landed',
                  'HEAD_attempted', 'BODY_landed', 'BODY_attempted', 'LEG_landed', 'LEG_attempted',
                  'DISTANCE_landed', 'DISTANCE_attempted', 'CLINCH_landed', 'CLINCH_attempted', 'GROUND_landed',
                  'GROUND_attempted']
rounds = pd.DataFrame(columns=rounds_columns)

totals_columns = ['FIGHTER', 'KD', 'SIG. STR.', 'SIG. STR. %', 'TOTAL STR.', 'TD', 'TD %', 'SUB. ATT', 'REV.', 'CTRL']
strikes_columns = ['FIGHTER', 'SIG. STR', 'SIG. STR. %', 'HEAD', 'BODY', 'LEG', 'DISTANCE', 'CLINCH', 'GROUND']

match_id_without_records = []

path = 'html/'
s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))
for html in s:
    ## 1. match_id
    match_id = int(html.split('_')[-1].split('.')[0])
    ## totals 와 strikes 로 나눈다.
    round_totals = pd.DataFrame(columns=totals_columns+['round_number'])
    round_strikes = pd.DataFrame(columns=strikes_columns+['round_number'])

    soup = BeautifulSoup(open(path+html), 'html.parser')
    ## table 두 개: 0. totals 1. significant strikes
    ## round 개수: len(tables[i].find_all('tbody'))
    try:
        tables = soup.find_all('table', 'b-fight-details__table js-fight-table')
        totals = tables[0]
        strikes = tables[-1]
    except IndexError:
        match_id_without_records.append(match_id)
        # totals_plus_strikes = pd.DataFrame(columns=rounds_columns)
        # rounds = pd.concat([rounds, totals_plus_strikes], ignore_index=True)
        continue

    ## browser 에서 보는 것과 달리 totals.find_all('tbody') 는 len() = 1 이다.
    totals_round = totals.find('tbody')
    round_number = 0
    for tr in totals_round.find_all('tr'):
        array_totals = []
        round_number += 1
        for td in tr.find_all('td'):
            if len(td.find_all('a')) > 1:
                x = [a['href'] for a in td.find_all('a')]
            else:
                x = td.text.strip().split('\n')
                x = [x.strip() for x in x]
            array_totals.append(list(filter(lambda a: a != '', x)))
        each_round = pd.DataFrame(np.asarray(array_totals).transpose(), columns=totals_columns)
        each_round['round_number'] = round_number
        round_totals = pd.concat([round_totals, each_round], ignore_index=True)
        round_totals.replace(r'^-*$', np.nan, inplace=True, regex=True)

    # fighter_id, round_number, TD_landed, TD_attempted, SUB_attempted,
    # REV, CTRL_sec, KD(for strikes)
    round_totals = round_totals[['FIGHTER', 'round_number', 'TD', 'SUB. ATT', 'REV.', 'CTRL', 'KD']]
    round_totals['TD_landed'] = round_totals['TD'].copy().apply(lambda x: int(x.split('of')[0]))
    round_totals['TD_attempted'] = round_totals['TD'].copy().apply(lambda x: int(x.split('of')[-1]))
    round_totals['SUB_attempted'] = pd.to_numeric(round_totals['SUB. ATT'])
    round_totals['REV'] = pd.to_numeric(round_totals['REV.'])
    round_totals['CTRL_sec'] = round_totals['CTRL'].apply(lambda x: x if type(x) == type(np.nan) else(int(x.split(':')[0]) * 60 + int(x.split(':')[-1])))
    round_totals['KD'] = pd.to_numeric(round_totals['KD'])

    round_totals = round_totals[['FIGHTER', 'round_number', 'TD_landed', 'TD_attempted', 'SUB_attempted', 'REV', 'CTRL_sec', 'KD']]
    # 각 column 의 data type 을 설정한다.

    strikes_round = strikes.find('tbody')
    round_number = 0
    for tr in strikes_round.find_all('tr'):
        array_strikes = []
        round_number += 1
        for td in tr.find_all('td'):
            if len(td.find_all('a')) > 1:
                x = [a['href'] for a in td.find_all('a')]
            else:
                x = td.text.strip().split('\n')
                x = [x.strip() for x in x]
            array_strikes.append(list(filter(lambda a: a != '', x)))
        each_round = pd.DataFrame(np.asarray(array_strikes).transpose(), columns=strikes_columns)
        each_round['round_number'] = round_number
        round_strikes = pd.concat([round_strikes, each_round], ignore_index=True)
        round_strikes.replace(r'^-*$', np.nan, inplace=True, regex=True)

    # fighter_id, round_number, HEAD_landed, HEAD_attempted, BODY_landed,
    # BODY_attempted, LEG_landed, LEG_attempted, DISTANCE_landed, DISTANCE_attempted,
    # CLINCH_landed, CLINCH_attempted, GROUND_landed, GROUND_attempted
    round_strikes = round_strikes[['FIGHTER', 'round_number', 'HEAD', 'BODY', 'LEG', 'DISTANCE', 'CLINCH', 'GROUND']]
    round_strikes['HEAD_landed'] = round_strikes['HEAD'].apply(lambda x: int(x.split('of')[0]))
    round_strikes['HEAD_attempted'] = round_strikes['HEAD'].apply(lambda x: int(x.split('of')[-1]))
    round_strikes['BODY_landed'] = round_strikes['BODY'].apply(lambda x: int(x.split('of')[0]))
    round_strikes['BODY_attempted'] = round_strikes['BODY'].apply(lambda x: int(x.split('of')[-1]))
    round_strikes['LEG_landed'] = round_strikes['LEG'].apply(lambda x: int(x.split('of')[0]))
    round_strikes['LEG_attempted'] = round_strikes['LEG'].apply(lambda x: int(x.split('of')[-1]))
    round_strikes['DISTANCE_landed'] = round_strikes['DISTANCE'].apply(lambda x: int(x.split('of')[0]))
    round_strikes['DISTANCE_attempted'] = round_strikes['DISTANCE'].apply(lambda x: int(x.split('of')[-1]))
    round_strikes['CLINCH_landed'] = round_strikes['CLINCH'].apply(lambda x: int(x.split('of')[0]))
    round_strikes['CLINCH_attempted'] = round_strikes['CLINCH'].apply(lambda x: int(x.split('of')[-1]))
    round_strikes['GROUND_landed'] = round_strikes['GROUND'].apply(lambda x: int(x.split('of')[0]))
    round_strikes['GROUND_attempted'] = round_strikes['GROUND'].apply(lambda x: int(x.split('of')[-1]))

    round_strikes = round_strikes[[
        'FIGHTER', 'round_number', 'HEAD_landed', 'HEAD_attempted', 'BODY_landed',
        'BODY_attempted', 'LEG_landed', 'LEG_attempted', 'DISTANCE_landed', 'DISTANCE_attempted',
        'CLINCH_landed', 'CLINCH_attempted', 'GROUND_landed', 'GROUND_attempted'
    ]]

    ## round_totals 와 round_strikes 를 합친다.
    totals_plus_strikes = pd.merge(round_totals, round_strikes, on=['FIGHTER', 'round_number'])
    ## database 의 fighters Table 을 이용해 fighter_id column 을 생성한다.
    totals_plus_strikes = pd.merge(totals_plus_strikes, fighters[['fighter_id', 'fighter_url']], left_on='FIGHTER', right_on='fighter_url')
    ## match_id
    totals_plus_strikes['match_id'] = match_id
    totals_plus_strikes = totals_plus_strikes[rounds_columns]

    rounds = pd.concat([rounds, totals_plus_strikes], ignore_index=True)

    print(match_id +1 , 'is done out of', len(s))

# 2. round 정보가 누락된 match_id 는 파일로 저장한다.
with open('match_without_rounds.txt', 'w') as f:
    for item in match_id_without_records:
        f.write('%s\n' % item)

# 3. rounds Table 을 SQL 에 입력한다.
with engine.connect() as con:
    rounds.to_sql(con=con, name='rounds', if_exists='replace', index=False,
                           dtype= sqlalchemy.types.INT
                         # dtype={'match_id': sqlalchemy.types.INT, 'fighter_id': sqlalchemy.types.INT,
                         #        'round_number': sqlalchemy.types.INT, 'TD_landed': sqlalchemy.types.INT,
                         #        'TD_attempted': sqlalchemy.types.INT, 'SUB_attempted': sqlalchemy.types.INT,
                         #        'REV': sqlalchemy.types.INT, 'CTRL_sec': sqlalchemy.types.INT,
                         #        'KD': sqlalchemy.types.INT, 'HEAD_landed': sqlalchemy.types.INT,
                         #        'HEAD_attempted': sqlalchemy.types.INT, 'BODY_landed': sqlalchemy.types.INT,
                         #        'BODY_attempted': sqlalchemy.types.INT, 'LEG_landed': sqlalchemy.types.INT,
                         #        'LEG_attempted': sqlalchemy.types.INT, 'DISTANCE_landed': sqlalchemy.types.INT,
                         #        'DISTANCE_attempted': sqlalchemy.types.INT, 'CLINCH_landed': sqlalchemy.types.INT,
                         #        'CLINCH_attempted': sqlalchemy.types.INT, 'GROUND_landed': sqlalchemy.types.INT,
                         #        'GROUND_attempted': sqlalchemy.types.INT
                         #        }
                  )
    con.execute('ALTER TABLE `rounds` ADD PRIMARY KEY (`match_id`, `fighter_id`, `round_number`);')
    con.execute('ALTER TABLE `rounds` ADD FOREIGN KEY (`match_id`) REFERENCES `matches`(`match_id`);')
    con.execute('ALTER TABLE `rounds` ADD FOREIGN KEY (`fighter_id`) REFERENCES `fighters`(`fighter_id`);')