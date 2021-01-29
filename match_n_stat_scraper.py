import sqlalchemy
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pymysql
from time import sleep
import json

headers = {'User-agent': 'Mozilla/5.0'}

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
## html 을 저장할 경로 html
path = 'html/'
for match_id, fighter, event_id, url in fight_list_url.iloc[1816:,:].itertuples():
    sauce = requests.get(url, headers=headers)
    soup = BeautifulSoup(sauce.text, 'lxml')
    ## 파일 형식: event_id_match_id
    with open(path+ str(event_id) + '_' + str(match_id) +'.html', 'w', encoding='utf-8') as f:
        f.write(str(soup))
    print(match_id, 'is done out of', len(fight_list_url))
    sleep(5)

# file_soup = BeautifulSoup(open(path+'1.html'), 'html.parser')
# file_soup.find(class_='b-fight-details__persons clearfix').find(class_='b-fight-details__person').find('a').attrs['href']