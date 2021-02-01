import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import json
import os
import string

# MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

# 1. times format 를 DB 에 입력
# times 를 mysql 에 입력하기 위해서 상세 정보를 봐야 한다.
## html 을 저장할 경로 html
path = 'html/'
times_list = list()

i = 0
s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[1].split('.')[0]))
for html_file in s:
    i += 1
    file_soup = BeautifulSoup(open(path + html_file), 'html.parser')
    b_fight_details = file_soup.find_all('i', class_='b-fight-details__label')
    for b in b_fight_details:
        if str(b).find('Time format') > -1:
            times_list.append(b.parent.find_all(text=True, recursive=False)[-1].strip())
            break
    times_list = list(dict.fromkeys(times_list))
    print(i, 'out of', len(os.listdir(path=path)))

times = pd.DataFrame({'times_format': times_list})
## MYSQL 에 데이터를 처음 입력한다: Table times
with engine.connect() as con:
    ## times table
    times.to_sql(con=con, name='times', if_exists='replace', index=True,
                   dtype={
                       None: sqlalchemy.types.INT,
                       'times_format': sqlalchemy.types.VARCHAR(length=255)
                   }
                )
    con.execute('ALTER TABLE `times` ADD PRIMARY KEY (`index`);')
    con.execute('ALTER TABLE `times` CHANGE `index` `time_id` INT;')

# file_soup = BeautifulSoup(open(path + '1.html'), 'html.parser')
# file_soup.find(class_='b-fight-details__persons clearfix').find(class_='b-fight-details__person').find('a').attrs[
#     'href']

# 2. fighters 정보를 ufcstats.com 에서 스크래핑
## 기존에 갖고 있던 matches 관련 html 파일에서 각 fighter 의 url 정보를 얻는다.

fighter_path = 'fighter_html/'
for char in string.ascii_lowercase:
    url = 'http://www.ufcstats.com/statistics/fighters?char={}&page=all'.format(char)