import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import json
import os
import requests
import re

# MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

# 1. fighter 정보를 불러온다. json
with open('fighter_url.json', 'r') as f:
    json_data = json.loads(f.read())
    fighter_url = pd.DataFrame(json_data)
## 역대 ufc 경기에 참여했던 인원을 순서대로 list 화 하고, 이의 url 을 담는다.
fighter_url_only_list = []
for red_url, blue_url in fighter_url[['fighter_url_red', 'fighter_url_blue']].itertuples(index=False):
    fighter_url_only_list.append(red_url)
    fighter_url_only_list.append(blue_url)
    fighter_url_only_list = list(dict.fromkeys(fighter_url_only_list))

# 2. fighter 정보를 불러온다. sql
with engine.connect() as con:
    fighters_df = pd.read_sql_table('fighters', con=con)

# 3. fighters Table 에 url 을 추가한다.
fighters_df['fighter_url'] = fighter_url_only_list
fighters_df = fighters_df[['fighter_id', 'fighter_name', 'fighter_nickname', 'fighter_url', 'height',
                           'weight', 'reach', 'stance', 'dob', 'record_win',
                           'record_loss', 'record_draw', 'record_nc', 'record_sl_pm', 'record_str_acc',
                           'record_sa_pm', 'record_str_def', 'record_td_avg', 'record_td_acc', 'record_td_def',
                           'record_sub_avg']]

with engine.connect() as con:
    ## fighters table
    fighters_df.to_sql(con=con, name='fighters', if_exists='replace', index=False,
                       dtype={
                           'fighter_id': sqlalchemy.types.INT,
                           'fighter_name': sqlalchemy.types.VARCHAR(length=255),
                           'fighter_nickname': sqlalchemy.types.VARCHAR(length=255),
                           'fighter_url': sqlalchemy.types.VARCHAR(length=2000),
                            ### metric
                           'height': sqlalchemy.types.DECIMAL(precision=7, scale=4, asdecimal=True),
                            ### lbs
                            'weight': sqlalchemy.types.DECIMAL(precision=5, scale=2, asdecimal=True),
                            ### metric
                            'reach': sqlalchemy.types.DECIMAL(precision=7, scale=4, asdecimal=True),
                            'stance': sqlalchemy.types.VARCHAR(length=255),
                            'dob': sqlalchemy.types.Date(),
                            'record_win': sqlalchemy.types.INT,
                            'record_loss': sqlalchemy.types.INT,
                            'record_draw': sqlalchemy.types.INT,
                            'record_nc': sqlalchemy.types.INT,
                            'record_sl_pm': sqlalchemy.types.DECIMAL(precision=7, scale=4, asdecimal=True),
                            'record_str_acc': sqlalchemy.types.DECIMAL(precision=5, scale=4, asdecimal=True),
                            'record_sa_pm': sqlalchemy.types.DECIMAL(precision=7, scale=4, asdecimal=True),
                            'record_str_def': sqlalchemy.types.DECIMAL(precision=5, scale=4, asdecimal=True),
                            'record_td_avg': sqlalchemy.types.DECIMAL(precision=7, scale=4, asdecimal=True),
                            'record_td_acc': sqlalchemy.types.DECIMAL(precision=5, scale=4, asdecimal=True),
                            'record_td_def': sqlalchemy.types.DECIMAL(precision=5, scale=4, asdecimal=True),
                            'record_sub_avg': sqlalchemy.types.DECIMAL(precision=7, scale=4, asdecimal=True),
                       }
                       )
    con.execute('ALTER TABLE `fighters` ADD PRIMARY KEY (`fighter_id`);')