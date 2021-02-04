import sqlalchemy
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pymysql
from time import sleep
import json

headers = {'User-agent': 'Mozilla/5.0'}

# 2. url 데이터 불러오기
with open("fight_list_url.json", "r") as f:
    json_data = json.loads(f.read())

fight_list_url = pd.DataFrame(json_data)
fight_list_no_url = fight_list_url.drop(["url", "Fighter"], axis=1)

# 4. fight 별로 statistics 를 스크래핑한다.
## html 을 저장할 경로 html
path = 'html/'
for match_id, fighter, event_id, url in fight_list_url.iloc[5046:,:].itertuples():
    sauce = requests.get(url, headers=headers)
    soup = BeautifulSoup(sauce.text, 'lxml')
    ## 파일 형식: event_id_match_id
    with open(path+ str(event_id) + '_' + str(match_id) +'.html', 'w', encoding='utf-8') as f:
        f.write(str(soup))
    print(match_id, 'is done out of', len(fight_list_url))