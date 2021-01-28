import sqlalchemy
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pymysql
from time import sleep
import json

# 1. MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

# 2. url 데이터 불러오기
with open("fight_list_url.json", "r") as f:
    json_data = json.loads(f.read())

fight_list_url = pd.DataFrame(json_data)
fight_list_no_url = fight_list_url.drop(["url", "Fighter"], axis=1)

# 3. MYSQL 에 데이터를 처음 입력한다.
# with engine.connect() as con:
#     fight_list_no_url.to_sql(con=con, name='fights', if_exists='replace', index=True,
#                              dtype={None: sqlalchemy.types.INTEGER,
#                                     'event_index': sqlalchemy.types.Integer})
#     con.execute('ALTER TABLE `fights` ADD PRIMARY KEY (`index`);')
#     con.execute('ALTER TABLE `fights` ADD FOREIGN KEY (`event_index`) REFERENCES `events`(`index`);')

# 4. fight 별로 statistics 를 스크래핑한다.
for url in fight_list_url['url'].iloc[::-1]:
    print(url)
    break