import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import json
import os


# . MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

## html 을 저장할 경로 html
path = 'html/'
s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[1].split('.')[0]))
title_n_bonus = []
for html in s:
    soup = BeautifulSoup(open(path+html), 'html.parser')
    div = soup.find('div', class_='b-fight-details__fight')
    img_url = div.find_all('img')
    for img in img_url:
        title_n_bonus.append(img.attrs['src'])
    title_n_bonus = list(dict.fromkeys(title_n_bonus))
    print(html)

title_n_bonus = [''] + title_n_bonus
chance_name = ['', 'belt', 'fight', 'sub', 'ko', 'perf']
chances = pd.DataFrame()
chances['achieve_name'] = chance_name
chances['achieve_url'] = title_n_bonus

chances = chances[chances['achieve_name'] != 'belt']
chances.reset_index(drop=True, inplace=True)
chances['achieve_id'] = range(len(chances))
chances = chances[['achieve_id', 'achieve_name', 'achieve_url']]

## 열 이름을 모두 변경한다.
chances.rename({'achieve_id': 'bonus_id', 'achieve_name': 'bonus_name', 'achieve_url': 'bonus_url'}, axis=1, inplace=True)

## MYSQL 에 데이터를 처음 입력한다: Table methods
with engine.connect() as con:
    chances.to_sql(con=con, name='bonuses', if_exists='replace', index=False,
                     dtype={
                            'bonus_id': sqlalchemy.types.INT,
                            'bonus_name': sqlalchemy.types.VARCHAR(length=255),
                            'bonus_url': sqlalchemy.types.VARCHAR(length=2000)
                            }
                   )
    con.execute('ALTER TABLE `bonuses` ADD PRIMARY KEY (`bonus_id`);')