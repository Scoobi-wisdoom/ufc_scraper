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

# 1. matches Table, achieves Table 을 불러온다.
## matches
with engine.connect() as con:
    matches = pd.read_sql_table('matches', con=con)

## achieves
with engine.connect() as con:
    achieves = pd.read_sql_table('achieves', con=con)

# 2. achieveMatches Table 초안을 생성한다.
achieveMatches = pd.DataFrame(columns=['match_id', 'achieve_id'])

# 3. html 파일에서 match_id 와 achieve_id 를 획득한다.
path = 'html/'
s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))
for html in s:
    sub_df = pd.DataFrame()
    achieves_list = list()

    match_id = html.split('_')[-1].split('.')[0]
    soup = BeautifulSoup(open(path+html), 'html.parser')
    img_url = soup.find('div', class_='b-fight-details__fight').find_all('img')

    ## img_url belt 든 bonus img 든 있을 때때
    if len(img_url) > 0:
        for img in img_url:
            achieves_list.append(achieves['achieve_url'].to_list().index(img.attrs['src']))
        sub_df['achieve_id'] = achieves_list
    else:
        sub_df['achieve_id'] = [0]

    sub_df['match_id'] = match_id
    sub_df = sub_df[['match_id', 'achieve_id']]
    achieveMatches = pd.concat([achieveMatches, sub_df], ignore_index=True)
    print(html)

## MYSQL 에 데이터를 처음 입력한다: Table methods
with engine.connect() as con:
    achieveMatches.to_sql(con=con, name='achievematches', if_exists='replace', index=False,
                     dtype= sqlalchemy.types.INT
                   )
    con.execute('ALTER TABLE `achieveMatches` ADD PRIMARY KEY (`match_id`, `achieve_id`);')
    con.execute('ALTER TABLE `achieveMatches` ADD FOREIGN KEY (`match_id`) REFERENCES `matches`(`match_id`);')
    con.execute('ALTER TABLE `achieveMatches` ADD FOREIGN KEY (`achieve_id`) REFERENCES `achieves`(`achieve_id`);')