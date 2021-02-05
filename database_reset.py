# To reset all databases except rounds Table
# 데이터를 pandas 로 불러온 후, DB 에 이상이 생기면 그 때 database 에 저장한다.
import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import json
import os
import requests
import string
import re

path = 'database backup/'
# MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

# 1. 데이터베이스에서 Table 을 불러온다.
## locations 정보를 불러온다. sql
with engine.connect() as con:
    locations = pd.read_sql_table('locations', con=con)

## events 정보를 불러온다. sql
with engine.connect() as con:
    events = pd.read_sql_table('events', con=con)

## weights Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    weights = pd.read_sql_table('weights', con)

## methods Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    methods = pd.read_sql_table('methods', con)

## times Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    times = pd.read_sql_table('times', con)

## results Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    results = pd.read_sql_table('results', con)

## referees Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    referees = pd.read_sql_table('referees', con)

## fighters Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    fighters = pd.read_sql_table('fighters', con=con)

## matches Table 을 MYSQL 에서 불러온다.
with engine.connect() as con:
    matches = pd.read_sql_table('matches', con=con)

# 2. MYSQL 에 데이터를 입력한다.
## location table
with engine.connect() as con:
    locations.to_sql(con=con, name='locations', if_exists='replace', index=False,
                     dtype={'location_id': sqlalchemy.types.INT,
                            'location_name': sqlalchemy.types.VARCHAR(length=255)})
    con.execute('ALTER TABLE `locations` ADD PRIMARY KEY (`location_id`);')
    # con.execute('ALTER TABLE `locations` CHANGE `index` `location_id` INT;')

## events table
with engine.connect() as con:
    events.to_sql(con=con, name='events', if_exists='replace', index=False,
                         dtype={'event_id': sqlalchemy.types.INTEGER(),
                                'event_name': sqlalchemy.types.VARCHAR(length=255),
                                'date': sqlalchemy.types.Date(),
                                'location_id': sqlalchemy.types.INT})
    con.execute('ALTER TABLE `events` ADD PRIMARY KEY (`event_id`);')
    # con.execute('ALTER TABLE `events` CHANGE `index` `event_id` INT;')
    con.execute('ALTER TABLE `events` ADD FOREIGN KEY (`location_id`) REFERENCES `locations`(`location_id`);')

## weights table
with engine.connect() as con:
    ## weights table
    weights.to_sql(con=con, name='weights', if_exists='replace', index=False,
                     dtype={'weight_id': sqlalchemy.types.INT,
                            'weight_name': sqlalchemy.types.VARCHAR(length=255)})
    con.execute('ALTER TABLE `weights` ADD PRIMARY KEY (`weight_id`);')
    # con.execute('ALTER TABLE `weights` CHANGE `index` `weight_id` INT;')

## methods table
with engine.connect() as con:
    methods.to_sql(con=con, name='methods', if_exists='replace', index=False,
                     dtype={
                            'method_id': sqlalchemy.types.INT,
                            'method': sqlalchemy.types.VARCHAR(length=255),
                            'method_long': sqlalchemy.types.VARCHAR(length=255)
                            }
                   )
    con.execute('ALTER TABLE `methods` ADD PRIMARY KEY (`method_id`);')
    # con.execute('ALTER TABLE `methods` CHANGE `index` `method_id` INT;')

## times table
with engine.connect() as con:
    times.to_sql(con=con, name='times', if_exists='replace', index=False,
                   dtype={
                       'time_id': sqlalchemy.types.INT,
                       'time_format': sqlalchemy.types.VARCHAR(length=255)
                   }
                )
    con.execute('ALTER TABLE `times` ADD PRIMARY KEY (`time_id`);')
    # con.execute('ALTER TABLE `times` CHANGE `index` `time_id` INT;')

## results table
with engine.connect() as con:
    results.to_sql(con=con, name='results', if_exists='replace', index=False,
                   dtype={
                       'result_id': sqlalchemy.types.INT,
                       'result_r_b': sqlalchemy.types.VARCHAR(length=255)
                   }
                   )
    con.execute('ALTER TABLE `results` ADD PRIMARY KEY (`result_id`);')
    # con.execute('ALTER TABLE `results` CHANGE `index` `result_id` INT;')

## referees table
with engine.connect() as con:
    referees.to_sql(con=con, name='referees', if_exists='replace', index=False,
                     dtype={
                            'referee_id': sqlalchemy.types.INT,
                            'referee_name': sqlalchemy.types.VARCHAR(length=255)
                            }
                   )
    con.execute('ALTER TABLE `referees` ADD PRIMARY KEY (`referee_id`);')
    # con.execute('ALTER TABLE `referees` CHANGE `index` `referee_id` INT;')

## fighters table
with engine.connect() as con:
    fighters.to_sql(con=con, name='fighters', if_exists='replace', index=False,
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
    # con.execute('ALTER TABLE `fighters` CHANGE `index` `fighter_id` INT;')

## matches table
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