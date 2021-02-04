import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import json
import os

# MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

# 1. fighter 정보를 불러온다.
with open('fighter_url.json', 'r') as f:
    json_data = json.loads(f.read())
    fighter_url = pd.DataFrame(json_data)

## 역대 ufc 경기에 참여했던 인원을 순서대로 list 화 하고, 이의 url 을 담는다.
fighter_url_only_list = []
for red_url, blue_url in fighter_url[['fighter_url_red', 'fighter_url_blue']].itertuples(index=False):
    fighter_url_only_list.append(red_url)
    fighter_url_only_list.append(blue_url)
    fighter_url_only_list = list(dict.fromkeys(fighter_url_only_list))

## url 데이터 불러오기
with open("fight_list_url.json", "r") as f:
    json_data = json.loads(f.read())
    fight_list_url = pd.DataFrame(json_data)

## weights Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    weights = pd.read_sql_table('weights', con)

## results Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    results = pd.read_sql_table('results', con)

## times Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    times = pd.read_sql_table('times', con)

## methods Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    methods = pd.read_sql_table('methods', con)

## referees Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    referees = pd.read_sql_table('referees', con)

# 2. matches DataFrame 을 생성한다.
matches = fighter_url[['event_id', 'match_id']].copy()
## fighter_red_id, fighter_blue_id
fighter_red_id = []
fighter_blue_id = []
for red_url, blue_url in fighter_url[['fighter_url_red', 'fighter_url_blue']].itertuples(index=False):
    red_idx = fighter_url_only_list.index(red_url)
    blue_idx = fighter_url_only_list.index(blue_url)
    fighter_red_id.append(red_idx)
    fighter_blue_id.append(blue_idx)

## weight_id, time_duration
weight_id = []
time_duration = []
for weight, duration in fight_list_url[['Weight class', 'Time']].itertuples(index=False):
    weight_id_idx = int(weights[weights['Weight class'] == weight]['weight_id'])
    weight_id.append(weight_id_idx)
    time_duration.append(duration)

## result_id
result_id = []
for result1, result2 in fighter_url[['defeat_red', 'defeat_blue']].itertuples(index=False):
    result_text = result1 + result2
    ## WL, LW, DD 외에 NCNC 가 될 수 있다.
    if result_text == 'NCNC':
        result_text = 'NC'
    result_id.append(int(results[results['result_r_b'] == result_text]['result_id']))

## method_id, time_id, referee_id, detail
time_id = []
method_id = []
referee_id = []
detail = []
path = 'html/'
s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))
for html in s:
    soup = BeautifulSoup(open(path+html), 'html.parser')
    b_fight_details = soup.find_all('i', class_='b-fight-details__label')
    for b in b_fight_details:
        if str(b).find('Time format') > -1:
            time_text = b.parent.find_all(text=True, recursive=False)[-1].strip()
            break
    time_id.append(int(times[times['time_format'] == time_text]['time_id']))

    method_text = soup.find('i', {'style': 'font-style: normal'}).text.strip()
    method_id.append(int(methods[methods['method_long'] == method_text]['method_id']))

    referee = soup.find_all('p', class_='b-fight-details__text')[0].find('span').text.strip()
    referee_id.append(int(referees[referees['referee_name'] == referee]['referee_id']))

    detail_text = soup.find_all('p', class_='b-fight-details__text')[-1].find_all(text=True, recursive=False)
    detail_text = ''.join(detail_text).strip()
    detail.append(detail_text)
    print(html)


matches['fighter_red_id'] = fighter_red_id
matches['fighter_blue_id'] = fighter_blue_id
matches['weight_id'] = weight_id

matches['result_id'] = result_id
matches['method_id'] = method_id

## time_duration 을 초 단위로 변경한다.
time_second = [int(t.split(':')[0]) * 60 + int(t.split(':')[-1]) for t in time_duration]
matches['time_second'] = time_second

matches['time_id'] = time_id
matches['referee_id'] = referee_id
matches['detail'] = detail

# 2. MYSQL 에 데이터를 처음 입력한다.
with engine.connect() as con:
    matches.to_sql(con=con, name='matches', if_exists='replace', index=False,
                             dtype={'event_id': sqlalchemy.types.Integer,
                                    'match_id': sqlalchemy.types.Integer,
                                    'fighter_red_id': sqlalchemy.types.Integer,
                                    'fighter_blue_id': sqlalchemy.types.Integer,
                                    'weight_id': sqlalchemy.types.Integer,
                                    'result_id': sqlalchemy.types.Integer,
                                    'method_id': sqlalchemy.types.Integer,
                                    'time_second': sqlalchemy.types.Integer,
                                    'time_id': sqlalchemy.types.Integer,
                                    'referee_id': sqlalchemy.types.Integer,
                                    'detail': sqlalchemy.types.VARCHAR(length=255)
                                    })
    con.execute('ALTER TABLE `matches` ADD PRIMARY KEY (`match_id`);')
    con.execute('ALTER TABLE `matches` ADD FOREIGN KEY (`event_id`) REFERENCES `events`(`event_id`);')
    con.execute('ALTER TABLE `matches` ADD FOREIGN KEY (`fighter_red_id`) REFERENCES `fighters`(`fighter_id`);')
    con.execute('ALTER TABLE `matches` ADD FOREIGN KEY (`fighter_blue_id`) REFERENCES `fighters`(`fighter_id`);')
    con.execute('ALTER TABLE `matches` ADD FOREIGN KEY (`weight_id`) REFERENCES `weights`(`weight_id`);')
    con.execute('ALTER TABLE `matches` ADD FOREIGN KEY (`result_id`) REFERENCES `results`(`result_id`);')
    con.execute('ALTER TABLE `matches` ADD FOREIGN KEY (`method_id`) REFERENCES `methods`(`method_id`);')
    con.execute('ALTER TABLE `matches` ADD FOREIGN KEY (`time_id`) REFERENCES `times`(`time_id`);')
    con.execute('ALTER TABLE `matches` ADD FOREIGN KEY (`referee_id`) REFERENCES `referees`(`referee_id`);')